# Large-Key Storage Design

## Status and scope

This document defines Keylane's disk-native representation for Redis values
that are too large to rewrite as one record. List is the first type. Its current
flat segmented implementation proves root publication and crash recovery; the
COW directory defined below replaces that implementation before large-List
support is considered complete. Hash, Set, Sorted Set, and Stream will reuse the
same physical-reference, publication, recovery, and accounting machinery while
supplying type-specific indexes.

The design has four non-negotiable properties:

- a normal request follows direct physical locations and never resolves a
  Page ID through a global page table;
- segment payloads stay disk-resident, while the sparse large-key side table
  permanently retains roots, decoded directory nodes, segment descriptors,
  and object-to-owner edges;
- data blocks are read once during cold recovery; all reachability work after
  that pass is over compact in-memory metadata;
- RDB and AOF are Redis interoperability formats, not Keylane's internal
  storage or recovery mechanism.

The storage format is still version 1 while Keylane is under development.
This layout is not compatible with older development data files; those files
must be recreated rather than upgraded.

The target layout is not an implementation claim. In particular, large-key COW
support must not ship while recovery still breaks equal mutation sequences by
scan order, while whole-graph retirement is absent, or while transaction undo
does not journal newly allocated internal objects. The relocation tie-breaker,
retirement batches, and symmetric commit/abort cleanup below are correctness
gates rather than optional optimizations.

## Generic record marker

`RecordLocation` remains the 48-byte value in every primary-index entry.
`logical_size_` remains 32 bits. The reserved value `UINT32_MAX`, named
`kSegmentedCollection`, means that the value is a segmented collection and
that its exact 64-bit cardinality is owned by a type-specific side table.

| `RecordLocation` field | Meaning for a segmented List |
| --- | --- |
| `block_id_` | Physical 8 MiB block containing the newest List object record. |
| `mutation_sequence_` | Newest published logical mutation of the Redis key. |
| `allocation_epoch_` | Physical block incarnation; changes when a block ID is reused. |
| `expire_at_ms_` | Absolute Redis key expiration, or zero. List segments never have independent TTLs. |
| `logical_size_` | Always `kSegmentedCollection`; `ListState::element_count_` is authoritative in memory. |
| `record_offset_` | Byte offset of the object record inside the physical block. |
| `total_disk_bytes_` | Aligned bytes occupied by that object record. |
| `block_owner_` | Current-process worker owning the block. |
| `in_memory_` | The referenced record is still in its staging buffer. |
| `external_` | False for the fixed-size segmented List root; oversized segment records, not the root, may own extent manifests. |
| `key_external_` | The Redis key bytes are external to the fixed record header. |
| `shielding_` | An older durable key version still needs this version to prevent resurrection. |
| `unclaimed_` | Tomb-raider round state. |
| `kind_` | Value or tombstone. |
| `value_type_` | `ValueType::kList`. |

On disk, `RecordHeader::logical_size_` is also 32 bits. Strings store their
byte length and cannot collide with the marker because Redis-compatible
strings are capped at 512 MiB. Compact collections store an exact cardinality
below the marker. Segmented collections store the marker.

## List representations

A small List uses the existing `KLL1` packed encoding and one top-level value
record. Its `logical_size_` is the exact element count and `LLEN` reads the
primary-index entry without disk IO or a side-table lookup.

A List is promoted when a mutation would encode to more than 64 KiB and returns
to compact form only when the complete value falls to 32 KiB. Promotion avoids
whole-value rewrites while conversion hysteresis prevents oscillation. The
segmented root and every directory level maintain encoded-byte sums, so the
demotion test is O(1). Because a value this small also satisfies every segment
merge limit, the mutation first coalesces it to one segment, reads that segment,
verifies the actual compact encoding is at most 32 KiB, and publishes one
compact top-level record. It never scans an arbitrarily large tree merely to
decide whether demotion is possible.

A segmented List is an immutable copy-on-write tree. The top-level Redis value
record is a small List root. Its direct physical reference leads to ordered
directory nodes, whose leaves reference immutable KLL1 segment records. There
is no logical segment ID, segment mutation sequence, Page ID, or global address
translation table. One concrete version of a node or segment is identified by
its direct physical reference.

Normal segments are ordinary internal records packed together in 8 MiB records
blocks. They never call the extent writer merely because they are segmented.
Only an oversized singleton that cannot fit in a records block uses an external
manifest and payload extents. This avoids wasting one 8 MiB extent block on a
small segment.

### `DirectRecordRef`

Every root, directory, and segment edge uses the same direct reference.

| Field | Meaning |
| --- | --- |
| `block_id_` | Physical records block containing the referenced object. |
| `allocation_epoch_` | Physical block incarnation; rejects a stale reference after block reuse. |
| `record_offset_` | Aligned byte offset of the referenced record within the block. |
| `total_disk_bytes_` | Aligned bytes occupied by the record, used for bounds checking and accounting. |
| `payload_checksum_` | Identity of the immutable referenced payload. |

`block_id_` alone is not an identity because a reclaimed block ID can be
reused. A segment update creates a new `DirectRecordRef`; an unchanged segment
retains its old reference. Split creates two new references and merge creates
one. For an external oversized segment, the segment record itself remains the
directly referenced object and owns the normal `ExtentRef` manifest.

### `ListRootHeader`

The checksummed inline payload of the top-level List record is fixed-size.
`RecordHeader::mutation_sequence_` is the logical publication version for the
entire List. `RecordHeader::lsn_` orders physical copies of the same logical
mutation during recovery; it is not a Redis-visible mutation.

| Field | Meaning |
| --- | --- |
| `magic_` | List-root format discriminator. |
| `version_` | Storage format version. |
| `header_bytes_` | Exact encoded root-header size. |
| `element_count_` | Exact unsigned 64-bit Redis List length. |
| `segment_count_` | Number of leaf segment references. |
| `encoded_bytes_` | Exact sum of the logical packed segment encodings. |
| `tree_height_` | Number of directory levels; zero is invalid for a segmented List. |
| `directory_root_` | Direct reference to the immutable directory root. |
| `reserved_` | Must be zero until assigned a versioned meaning. |

### `ListState`

`WorkerStore::list_states_` remains a sparse side table keyed by the
address-stable primary-index entry, but one state exists per large List rather
than per segment. It is rebuilt from the winning root during recovery.

| Field | Meaning |
| --- | --- |
| `element_count_` | Exact length returned by `LLEN` without disk IO. |
| `segment_count_` | Segment count for validation, metrics, and admission. |
| `encoded_bytes_` | Exact logical packed bytes, used for compact demotion. |
| `tree_height_` | Number of directory reads on an uncached point lookup. |
| `directory_root_` | Direct physical starting point for an on-demand lookup. |
| `owner_id_` | Runtime-only stable handle joining the root, its resident object metadata, and the current `(db,key)` owner record. It is not encoded. |

Directory nodes and segment descriptors are permanently resident for segmented
values. Segment element payloads are not. This is an intentional memory/disk
ratio: with a roughly 512 KiB average segment, spending about 100--150 bytes per
segment plus its directory entry is a small fraction of the payload and avoids
a directory IO on point reads. A 64 KiB segment target would multiply this
metadata approximately eightfold, which is one reason the List target remains
near 512 KiB.

`owner_id_` is worker-striped and unique for the life of the process. The
key-owning worker stores one `ListOwnerRuntime` mapping it to `(db_id, key,
digest, index_generation)`. Each block-owning worker stores
`ListObjectRuntimeMeta` keyed by `DirectRecordIdentity`; it contains the direct
reference, optional decoded directory, optional extent manifest, logical count,
owner ID, and a runtime parent reference plus slot. RENAME changes the single
owner record rather than every object.
The owner/object maps are accelerators and routing metadata, never recovery
authority: the persisted keyed root and its direct references remain
authoritative.

### Directory records

The directory is a high-fanout ordered COW tree. An internal entry contains a
child `DirectRecordRef`, `subtree_element_count_`,
`subtree_segment_count_`, and `subtree_encoded_bytes_`. A leaf entry contains
the segment `DirectRecordRef`, `element_count_`, and `encoded_bytes_`. Child and
leaf order is Redis List order. Directory records are ordinary packed internal
records, not dedicated blocks.

Every directory header records its node kind, level, entry count, total element
count, total segment count, and total encoded bytes. Decoding verifies that
entry sums match the header and ultimately the List root. Directory node split
and merge are local COW operations; their byte and fanout limits are independent
of List segment limits.

`ListDirectoryHeader` fields have the following meanings:

| Field | Meaning |
| --- | --- |
| `magic_` | Directory-node format discriminator. |
| `version_` | Storage format version. |
| `header_bytes_` | Exact encoded header size. |
| `node_kind_` | Internal or leaf node. |
| `level_` | Zero for a leaf; an internal child's level is exactly one lower. |
| `entry_count_` | Number of following child or segment entries. |
| `element_count_` | Sum of all descendant List elements. |
| `segment_count_` | Sum of all descendant segment leaves. |
| `encoded_bytes_` | Sum of all descendant segment encodings. |
| `reserved_` | Must be zero. |

An internal `ListDirectoryChild` contains:

| Field | Meaning |
| --- | --- |
| `subtree_element_count_` | Elements reachable through this child; used for ordinal routing. |
| `subtree_segment_count_` | Segment leaves reachable through this child. |
| `subtree_encoded_bytes_` | Logical packed bytes reachable through this child. |
| `child_` | Direct reference to the immutable child directory record. |

A leaf `ListDirectorySegment` contains:

| Field | Meaning |
| --- | --- |
| `element_count_` | Elements in the referenced KLL1 segment; nonzero. |
| `encoded_bytes_` | Exact KLL1 payload bytes. |
| `segment_` | Direct reference to the immutable segment record. |

Directory and segment records use an internal collection-record role plus
`ValueType::kList`; they are not independently indexed Redis keys. Their
liveness comes only from a winning top-level root.

### Segment sizing, split, and merge

The maximum normal segment encoding is 1 MiB. Growing beyond that limit splits
the affected segment at the element boundary closest to half the encoded bytes,
normally producing two segments near 512 KiB. Elements are never split. A
single element whose encoded form exceeds 1 MiB is a valid oversized singleton
segment and may grow to Redis's 512 MiB element limit.

A segment below 256 KiB attempts to merge with one adjacent segment. Merge is
performed only when the combined encoding is at most 768 KiB; otherwise the
underfull segment is retained rather than rewriting two large neighbors.
Keeping the merge ceiling below the 1 MiB split threshold prevents a small
insert immediately after a merge from splitting the segment again. One
individually large insertion may still cross the gap and split. Empty segments
are removed immediately. These thresholds reduce recovery and directory
metadata by roughly eightfold relative to 64 KiB segments while bounding an
ordinary point update to at most about 1 MiB of segment IO.

## Local mutation and reads

The key lock covers lookup, segment reads, COW writes, root publication, and
runtime state replacement. Operations use subtree element counts to navigate
without materializing unrelated segments:

- `LLEN` reads `RecordLocation` for compact Lists and `ListRootState` for
  segmented Lists without disk IO;
- `LINDEX` and `LSET` read one directory path and one target segment;
- `LPUSH`, `RPUSH`, `LPOP`, and `RPOP` read only an endpoint path and segment;
- `LRANGE` and `LTRIM` locate their boundary paths and read only intersecting
  segments;
- `LINSERT`, `LREM`, and `LPOS` may scan segments because their Redis semantics
  search by value, but only segments actually changed are rewritten.

An ordinary update writes the changed segment, the COW directory path from its
leaf to the directory root, and one new List root. Split or merge replaces only
the affected neighboring leaf entries and their ancestor paths. Every unrelated
segment and subtree keeps its exact old physical reference and is neither read
nor encoded.

### Replication representation

`ListRootHeader` and every `DirectRecordRef` are node-local physical metadata
and must never cross the replication protocol. A partition snapshot or delta
for a segmented List walks the selected immutable tree and emits the portable
`KLL1` logical encoding plus the 64-bit element count and TTL. Values larger
than one replication RPC are transferred with begin/chunk/commit frames; the
frame tracks encoded bytes separately from List cardinality. The receiver
persists that logical value using its own blocks and extents, so it has no
source block IDs in either its runtime side table or its recovery graph. A
later mutation may promote the received value into a local segmented tree.

This full-value delta is the correctness baseline. An operation-level List
delta or a receiver-side streaming tree builder may replace it to reduce
replication read amplification, but it must preserve the same rule: only
logical elements and ordering cross nodes, never physical references.

## Publication, transaction, and defrag

Publication is reachability-based. Writers durably append new segment records,
then new directory nodes from leaf to root, and finally the top-level List root.
Only that final root changes the Redis key's visible version. A crash before root
durability keeps the complete old tree; orphan new records receive no live-byte
charge and are reclaimable. Recovery never chooses the largest version for an
independent segment and therefore cannot splice an uncommitted segment into an
old List.

The root carries the normal transaction ID. Recovery ignores a tagged root
unless the transaction's `kTxCommit` exists. Runtime undo restores the prior
`RecordLocation` and `ListRootState`. Every COW writer also journals two
disjoint cleanup sets: old paths/subtrees to retire only after commit, and every
new physical object allocated by the transaction to retire on abort. Commit
retires the superseded old paths only after the decision is durable. Abort
restores the prior root and retires the new root, nodes, segments, and external
objects; leaving them charged until restart is not permitted. Multiple writes
to one key in the same transaction additionally put superseded intermediate
transaction objects in the commit cleanup set. Each physical reference is
deduplicated before accounting, and unchanged shared subtrees occur in neither
cleanup set.

Defrag relocation of a referenced segment or directory node creates an
equivalent COW parent path and physically relocated root. It preserves the
logical mutation sequence and receives the normal new append LSN, so it is not
a Redis-visible mutation. Recovery orders top-level candidates
lexicographically by `(mutation_sequence_, physical_lsn)`. The greater on-disk
`RecordHeader::lsn_` wins when logical sequences tie. If both fields tie, the
copies must describe the same root and physical graph identity or recovery
reports corruption; scan order is never an arbiter. During startup the scan
finds the maximum durable LSN. Worker `i` starts at `max_lsn + 1 + i` and
advances by `worker_count`, producing disjoint worker-local striped sequences
without a hot-path atomic operation. Every new-boot LSN is above every old-boot
LSN, including after a worker-count change. This relies on all publications of
one key, including a defrag root, running on the current key owner. The
recovery-only tie-breaker does not grow the 48-byte runtime `RecordLocation`;
no separate relocation generation is required. The old path is retired only
after the relocated root is durable.

Defrag obtains the collection object's owner ID from the resident reverse
table, routes to the current key owner, takes the exclusive key lock, and
revalidates the database epoch, replication epoch, index generation, current
root, and source reachability. Validated parent references make the normal path
O(tree height). A parent edge made stale by an unpublished or aborted COW path
is discarded and defrag falls back to a complete walk of the resident
directories. It then copies the source and rewrites only its ancestor path.
Missing owners, deleted keys, or sources no longer reachable
from the current root are benign races and are skipped. The new object's extent
manifest is installed under its new physical identity before publication.

## Runtime graph retirement

Positive reachability reconstructs exact accounting after restart, but runtime
must also subtract objects that a newly published root makes unreachable.
Local point updates already know their replaced leaf and COW ancestor path.
Operations that detach a whole tree or a large subtree create a compact
`GraphRetirementBatch` containing direct references to the detached subtree
roots plus any replaced path records:

- `DEL`, `UNLINK`, active/passive expiration, deletion of the last collection
  element, and compact demotion detach the complete old collection graph;
- `LTRIM`, `XTRIM`, range deletion, and bulk tree surgery detach only the
  dropped complete subtrees plus rewritten boundary paths;
- `XGROUP DESTROY` detaches that group's consumer directory and PEL graphs, not
  the Stream entry tree;
- replacing an existing destination, including `COPY REPLACE`, detaches the
  old destination graph.

Traversal is asynchronous because deleting a multi-terabyte key must not hold
the key lock while reading every directory node. The publishing path registers
the retirement batch before releasing its accounting fence. A standalone batch
becomes runnable only after the replacement root or tombstone is durable; a
transaction batch waits for its `kTxCommit`. An abort batch instead enumerates
the transaction's newly allocated objects and becomes runnable after rollback
has restored the prior root. Until cleanup finishes, over-counting bytes and
objects is safe, but the batch and its outstanding object estimate count
against admission and can apply mutation backpressure.

Expiration follows the same rule: removing an expired large key from the
runtime index is not by itself authority to retire its graph. The expiration
path first publishes the durable suppressing tombstone/root state required by
normal resurrection protection, then releases the graph-retirement batch.

The retirement worker follows immutable direct references with a bounded
decoded-node buffer and a per-batch visited set/bitmap. It validates allocation
epochs and checksums, subtracts every exclusively detached object and extent
exactly once, and queues newly sparse blocks for defrag. Retirement and defrag
serialize physical-object pin/accounting transitions on the owning worker. The
initial correctness-first implementation pauses relocation of internal
collection-object blocks while any graph-retirement batch is traversing; their
still-positive live bytes also prevent direct block reuse. Ordinary top-level
record defrag may continue. A later per-block retirement epoch may narrow this
fence, but defrag may never move a child between decoding its parent reference
and claiming that child for retirement. Exact COW-difference construction is
mandatory: traversing an entire old root after an ordinary update would
incorrectly retire subtrees shared with the new root.

The queue is recovery-optional rather than durability metadata. A crash before
or during traversal loses only cleanup progress; the next recovery starts
live-byte accounting at zero and charges only the new winning root. It cannot
resurrect or lose data. A shielding tombstone remains protected by the normal
Tomb Raider proof until dangerous older top-level records have physically
disappeared, but that does not keep the deleted collection graph logically
live. Retiring its old root promptly makes those blocks eligible for the
defrag that allows Tomb Raider eventually to remove the shielding marker.

## `COPY` ownership

Large-key `COPY` is an O(encoded bytes) deep copy. It may preserve the source
tree's shape while writing new roots, nodes, leaves, external records, and
extents, but the destination shares no physical `DirectRecordRef` or
`ExtentRef` with the source. O(1) root sharing is forbidden because the design
deliberately has no persistent per-object reference counts; shallow sharing
would make deletion or subtree retirement double-subtract shared objects.
Source and destination key locks cover the copy and the destination root is the
only publication point. `COPY REPLACE` retires the previous destination graph
only after the new destination root is durable.

## One-pass recovery

All scanned blocks begin with `live_bytes_ = 0`. The physical scan validates
records and extents once and stores compact recovery metadata, not List element
payloads. Separate contiguous arenas hold root candidates, directory nodes,
directory entries, segment identities, and extent identities; a physical-ref
index maps a `DirectRecordRef` to its arena index.

After the physical scan, recovery performs only memory work:

1. discard stale DB/partition epochs and transaction participants without a
   commit;
2. select the greatest surviving `(mutation_sequence_, physical_lsn)`
   top-level root for each Redis key;
3. traverse each winning directory root through the temporary physical-ref
   graph, validating levels, counts, checksums, allocation epochs, and bounds;
4. use packed visited bitmaps to charge each reachable root, directory record,
   segment record, and external extent exactly once to its block's live bytes;
5. assign a fresh runtime owner ID to each winning segmented root and install
   its `ListState`, `ListOwnerRuntime`, and every reachable
   `ListObjectRuntimeMeta`; decoded directories and segment descriptors remain
   resident, while segment payload bytes do not;
6. release the unreachable recovery graph and its temporary physical-ref
   index.

Unreachable historical paths and crash orphans are never charged; recovery
does not start from committed bytes and subtract dead objects. A zero-live block
is directly reclaimable, a partially live records block is a defrag candidate,
and a highly live block remains in place. Positive reachability accounting plus
visited bitmaps avoids both underflow and double charging of shared immutable
subtrees.

Recovery memory is proportional to the number of physical metadata objects,
not the encoded List bytes. With a 1 MiB split threshold, segments normally
average near 512 KiB, reducing temporary per-segment metadata approximately
eightfold compared with 64 KiB segmentation. Corrupt references, count
mismatches, cycles, truncated nodes, and reused allocation epochs fail startup;
recovery never silently truncates or repairs a List.

## Hash and Set

Hash and Set use one compact packed top-level record while the post-mutation
encoding is at most 64 KiB. A mutation that crosses 64 KiB atomically promotes
the value into the radix directory and bucket representation below. A large
value demotes only after its exact encoded-byte total reaches at most 32 KiB;
the root and radix child counts maintain that total without scanning every
bucket, after which conversion deliberately reads the now-small value. The
64/32 KiB hysteresis is the same as List.

Large Hash and Set values use extensible hashing. The logical directory is
addressed by successive bits of the existing 160-bit digest, but its physical
representation is a path-compressed immutable radix tree rather than a flat
`2^global_depth` pointer array. Increasing the depth therefore COW-writes only
the split path and never doubles or rewrites the complete directory.

The fixed-size `HashRootHeader`/`SetRootHeader` contains:

| Root field | Meaning |
| --- | --- |
| `magic_` | Type-specific root-format discriminator. |
| `version_` | Storage format version. |
| `header_bytes_` | Exact encoded root-header size. |
| `element_count_` | Exact `HLEN` or `SCARD` result. |
| `encoded_bytes_` | Exact logical packed bytes, used for admission and demotion. |
| `hash_depth_` | Greatest digest-prefix depth currently represented. |
| `digest_format_` | Versioned digest and bit-order discriminator. |
| `radix_root_` | Direct reference to the immutable radix root. |
| `reserved_` | Must be zero until assigned a versioned meaning. |

The corresponding runtime root state retains only the counts, depth/format,
and `radix_root_`. `HLEN` and `SCARD` use the resident root count without disk
IO. Radix nodes and buckets are packed internal records read on demand through
the common bounded node cache. Every radix child entry carries exact
`subtree_element_count_` and `subtree_encoded_bytes_` values in addition to its
prefix and direct child reference; decoding verifies their sums against the
root.

Each bucket records these fields:

| Field | Meaning |
| --- | --- |
| `local_depth_` | Digest-prefix bits that identify this bucket. |
| `hash_prefix_` | Value of those prefix bits. |
| `element_count_` | Number of fields or members in the bucket. |
| `encoded_bytes_` | Exact checksummed bucket payload size. |

A Hash bucket stores digest plus field/value entries; a Set bucket stores digest
plus member entries. Full bytes are always compared after the digest, so a
digest collision cannot alias two Redis values. An entry too large for a normal
bucket is stored once in an immutable external entry record and referenced from
a singleton bucket; splitting never divides field, value, or member bytes.

The initial bucket policy is a 512 KiB maximum, a split target near 256 KiB, and
a 128 KiB underflow trigger. Overflow partitions only the affected bucket by
the next distinguishing digest bit and COW-writes the changed radix path. Two
buddy buckets with equal local depth merge only when their combined encoding is
at most 384 KiB, leaving hysteresis below the 512 KiB split threshold. If all
160 digest bits are equal, the entries remain in a
collision bucket instead of attempting an unbounded split.

Consequently `HGET`, `HEXISTS`, and `SISMEMBER` read one radix path and one
bucket. `HSET`, `HDEL`, `SADD`, and `SREM` rewrite one bucket plus one COW path
and the top-level root. Unrelated buckets are never read or rewritten. `HSCAN`
and `SSCAN` traverse unique radix leaves; unlike a traditional flat extensible
hash directory, no directory aliases cause the same bucket to be visited many
times. The normal Redis SCAN allowance for duplicates during concurrent
mutation remains unchanged.

`SRANDMEMBER`, `SPOP`, and `HRANDFIELD` choose an unbiased logical rank in
`[0, element_count_)` and descend by the radix children's element counts; they
never choose a digest branch uniformly because unequal buckets would bias the
result. The terminal bucket selects the corresponding packed entry. Repeated
unique samples and `SPOP` group chosen ranks by bucket so each bucket and COW
path is read or rewritten once; with-replacement forms may reuse the same rank.
`HRANDFIELD WITHVALUES` returns the value stored beside the selected field.

The Hash/Set root is the only publication point. Buckets have no logical ID or
mutation sequence. A bucket or radix node written before a crash but not
reachable from the winning root is an orphan and receives no live-byte charge.
The common recovery graph, transaction publication, and COW defrag rules used
by List apply without modification.

## Sorted Set

A Sorted Set remains one compact packed top-level record through 64 KiB. A
post-mutation encoding above that threshold promotes atomically by building
both indexes before publishing the first large root. Demotion requires an exact
encoded-byte total at most 32 KiB, reads the now-small member population, and
publishes one compact record. Both index roots and all large objects become
retirement inputs only after that compact record is durable.

A large Sorted Set requires two independently routed indexes published by one
fixed-size `ZSetRootHeader`:

| Root field | Meaning |
| --- | --- |
| `magic_` | ZSet-root format discriminator. |
| `version_` | Storage format version. |
| `header_bytes_` | Exact encoded root-header size. |
| `element_count_` | Exact `ZCARD` result. |
| `encoded_bytes_` | Exact logical packed bytes, used for admission and demotion. |
| `member_root_` | Extensible-hash index from member to score and member identity. |
| `score_root_` | Ordered COW tree keyed by `(score, member)`. |
| `score_tree_height_` | Uncached directory depth for score/rank navigation. |
| `hash_depth_` | Current member-directory digest depth. |
| `reserved_` | Must be zero until assigned a versioned meaning. |

The member index uses the Hash bucket design above with a 512 KiB maximum. The
score index is a high-fanout ordered tree. Its internal child entries store a
separator key, child `DirectRecordRef`, and `subtree_element_count_`; score
leaves store ordered `(score, member)` entries. Subtree counts make `ZRANK` and
rank-based `ZRANGE` logarithmic to position, while score separators route
score-range commands directly to the first intersecting leaf.

Score leaves split above 1 MiB at an entry boundary near 512 KiB. A leaf below
256 KiB attempts to merge with one neighbor only when the result is at most
768 KiB, leaving hysteresis below the 1 MiB split threshold. Directory nodes
have a separate 32 KiB maximum and split near half,
keeping COW ancestor writes small and fanout high. A large member is never
allowed to turn every score leaf comparison into a large copy: members up to
256 bytes are stored inline in both indexes; larger members are stored once in
an immutable `ZSetMemberRecord`. Both indexes then carry its length, digest,
32-byte comparison prefix, and direct reference. The full member record is read
only to confirm equality or resolve an equal-score/equal-prefix comparison.

### Point-read amplification

Commands use only the index required by their semantics. `ZSCORE` and the
existence/condition phase of `ZADD` read one member-directory path and one
member bucket; they do not read the score tree. `ZRANK` first obtains the score
from that bucket and then follows one score-tree path to one leaf. A score or
rank range follows one path to its starting leaf and then reads only the leaves
intersecting the requested range. Cached immutable directory nodes are shared
by these operations, but leaf payloads are never permanently resident.

A direct record read covers the referenced aligned record, not its complete
8 MiB containing records block. A cold point lookup can therefore read at most
one 512 KiB member bucket and, only when ordering is needed, one 1 MiB score
leaf plus small directory nodes. This is still bounded read amplification--the
design does not claim that a 1 MiB leaf is free--but it never materializes the
complete ZSet. Large members normally remain external and are not fetched on
an ordinary digest/prefix comparison.

### Point-update amplification

`ZADD` first uses the member index to determine whether the member exists and
obtain its old score. A new member writes one member bucket and one score leaf.
A score change rewrites one member bucket, removes the old score entry, and
inserts the new score entry; the old and new score entries may share one leaf or
occupy two leaves. Both new index roots are then published together in one ZSet
root. A crash cannot recover one new index with one old index. An unchanged
score, or an `NX`/`XX`/`GT`/`LT` condition that rejects the update, publishes
nothing. `ZREM` rewrites one member bucket and one score leaf. Batched commands
group mutations by bucket and leaf and rewrite each affected object once rather
than once per argument.

Thus a point mutation is bounded independently of total ZSet size:

- one member bucket, at most 512 KiB;
- one or two score leaves, at most 1 MiB each;
- short COW paths of at most 32 KiB nodes;
- one fixed-size ZSet root.

Unchanged subtrees and large member records are reused. This is bounded local
write amplification rather than whole-ZSet amplification. The first
implementation deliberately avoids unbounded per-leaf delta chains: they make a
small write cheap but turn reads and one-pass recovery metadata into a function
of mutation history. If benchmarks require lower point-write volume, a later
bounded delta layer may permit at most a small fixed number of deltas before
compaction; it must not change root publication or recovery reachability.

Range reads touch only intersecting score leaves and are necessarily
proportional to the returned or examined range. Range deletes and store-style
commands similarly rewrite the leaves they semantically affect; no index can
make an operation that returns or deletes most of a ZSet constant-cost.

`ZRANDMEMBER` selects an unbiased rank through the score tree's existing
`subtree_element_count_` fields. Sampling by the member hash is also possible,
but the ordered tree reuses the same rank machinery as rank-based `ZRANGE` and
does not require a second random-routing implementation. Unique batches group
selected ranks by score leaf; with-replacement batches may return a member more
than once as Redis permits.

### Recovery memory

Small leaves do increase recovery memory and can cause startup OOM if chosen
without regard to the supported disk-to-memory ratio. This is why online IO
size and recovery object count must be tuned together rather than reducing the
leaf to the 4 KiB device alignment. Recovery retains one
compact physical-object descriptor and outgoing-reference range per bucket,
directory node, score leaf, external member record, and extent--not one object
per ZSet element and never the decoded member payload. Packed arrays and visited
bitmaps are shared with List/Hash recovery.

The 512 KiB member-bucket and 1 MiB score-leaf maxima normally produce objects
near 256 KiB and 512 KiB after splitting. Per TiB belonging to the corresponding
index, that is roughly four million member buckets or two million score leaves
before skew and large external members, rather than sixteen million objects at
a 64 KiB leaf size. The complete ZSet has both indexes, so their measured object
counts and edges are added; the estimates are not alternatives for total ZSet
memory.

Recovery admission is based on the actual scanned object and edge counts. In
conceptual terms its variable budget is
`objects * packed_descriptor_bytes + edges * packed_edge_bytes + bitmaps`, not
the logical member count. Each packed-array growth checks a configured fraction
of available memory and fails startup with a resource error before allocation
would exceed that budget. This keeps the scan single-pass; it does not require a
counting pass followed by a second disk scan. The sizing constants are changed
only together with a documented supported disk-to-memory ratio and
recovery-memory stress tests.

## Stream

A Stream combines an append-optimized ordered entry index with independently
mutable consumer-group state. Rewriting a large ordered leaf for every `XADD`
would be unacceptable, while retaining one permanent object per appended entry
would make recovery metadata proportional to the entry count. The representation
therefore uses large sealed leaves plus one bounded fragmented tail per Stream.

A Stream with no consumer groups remains a compact top-level record while its
scalar metadata plus packed entries fit in 64 KiB. The first `XGROUP CREATE`, or
an entry mutation that crosses 64 KiB, promotes it: existing entries become the
initial packed tail base, the optional group structure is built, and one large
Stream root publishes the result. After the last group is destroyed, a large
Stream may demote only when its complete scalar metadata and entries fit in
32 KiB. Thus a two-entry Stream uses one record rather than a root, tail
descriptor, and fragment. A Stream with any consumer group never uses compact
form, even if its entry list is empty.

### IDs and `StreamRootHeader`

A `StreamID` is the Redis pair of unsigned 64-bit values:

| Field | Meaning |
| --- | --- |
| `milliseconds_` | Millisecond part of the ID. |
| `sequence_` | Sequence part used to order entries in the same millisecond. |

IDs compare lexicographically. Auto-generation occurs while holding the Stream
key's exclusive transaction lock and uses the published `last_generated_id_`.
An orphan tail fragment cannot consume an ID: if its root was not published,
recovery restores the preceding last-generated ID and the uncommitted ID may be
generated again.

The fixed-size top-level Stream payload contains:

| Root field | Meaning |
| --- | --- |
| `magic_` | Stream-root format discriminator. |
| `version_` | Storage format version. |
| `header_bytes_` | Exact encoded root-header size. |
| `element_count_` | Current number of readable entries; authoritative for `XLEN`. |
| `encoded_bytes_` | Exact scalar-plus-entry bytes, used for admission and demotion. |
| `entries_added_` | Redis entries-added counter; `XADD` increments it and `XSETID` may restore it. |
| `first_entry_id_` | First current entry, or zero when the Stream is empty. |
| `last_entry_id_` | Last current entry, or zero when the Stream is empty. |
| `last_generated_id_` | Greatest published generated/explicit ID, including deleted entries. |
| `max_deleted_entry_id_` | Greatest ID removed by `XDEL` or trimming. |
| `entry_tree_height_` | Height of the sealed-entry COW tree. |
| `sealed_leaf_count_` | Number of sealed entry leaves. |
| `entry_root_` | Direct reference to the sealed-entry tree, or null. |
| `tail_root_` | Direct reference to the active tail descriptor, or null. |
| `group_root_` | Direct reference to the consumer-group hash directory, or null. |
| `group_count_` | Exact number of consumer groups. |
| `reserved_` | Must be zero until assigned a versioned meaning. |

The resident `StreamRootState` contains these scalar counters and three root
references, not entry leaves, groups, consumers, or pending entries. `XLEN` and
the scalar portion of `XINFO STREAM` therefore require no data-node IO.
`XSETID` validates its requested ID against the greatest existing entry and
publishes only a new root; its `ENTRIESADDED` and `MAXDELETEDID` options update
the corresponding root counters without rewriting entry leaves.

### Sealed entry tree

Historical entries use a high-fanout ordered COW tree keyed by `StreamID`.
Internal child entries contain the first ID in the child, a `DirectRecordRef`,
`subtree_element_count_`, and encoded byte count. A sealed leaf contains:

| Leaf field | Meaning |
| --- | --- |
| `first_id_` | Smallest entry ID in the leaf. |
| `last_id_` | Greatest entry ID in the leaf. |
| `entry_count_` | Exact number of entries. |
| `encoded_bytes_` | Checksummed bytes in the packed entry payload. |
| `flags_` | Versioned encoding flags; all unknown bits are rejected. |

Entry IDs are prefix/delta encoded within a leaf. Field/value arrays are packed
without imposing a schema across entries. One entry is never divided across
two leaves. An entry whose encoded field/value payload cannot fit in a normal
leaf is stored once in an immutable external `StreamEntryRecord`; the leaf keeps
its ID, field count, encoded length, digest, and direct reference.

Sealed leaves split above 1 MiB at an entry boundary near 512 KiB. A leaf below
256 KiB may merge with one neighbor only if the combined encoding is at most
768 KiB, leaving hysteresis below the 1 MiB split threshold. Internal nodes use
the common 32 KiB maximum. `XRANGE` follows the tree
to the first requested ID and scans only intersecting leaves; `XREVRANGE` does
the same in reverse. Neither command materializes the complete Stream.

### Bounded active tail

The rightmost logical leaf is represented by one `StreamTailDescriptor`, an
optional packed base leaf, and a bounded array of append fragments:

| Tail field | Meaning |
| --- | --- |
| `base_leaf_` | Direct reference to the current packed tail base, or null. |
| `base_first_id_` / `base_last_id_` | ID range of the base. |
| `base_entry_count_` | Entries in the base. |
| `base_encoded_bytes_` | Packed bytes in the base. |
| `fragment_count_` | Number of following fragment descriptors; at most 32. |
| `fragment_encoded_bytes_` | Sum of logical bytes in all fragments. |
| `fragments_[]` | Ordered direct references with first/last ID, count, and bytes. |

An `XADD` normally writes one small immutable `StreamTailFragment`, a new tail
descriptor, and a new fixed-size Stream root. It does not rewrite the base or a
sealed 1 MiB leaf. The new root is published only after the fragment and
descriptor are valid. Readers merge the base and at most 32 ordered fragments;
the descriptor is normally served by the bounded worker-local node cache.

The tail is folded when it reaches 32 fragments or 256 KiB of fragment payload.
If base plus fragments fit in 1 MiB, folding writes one new packed base and
resets the fragment array. If they overflow, folding emits one or more sealed
leaves near 512 KiB, COW-appends those leaves to the entry tree, and retains the
rightmost remainder as the new tail base. A single oversized entry remains an
external-entry stub and does not force an oversized tail.

This policy has two important bounds. Online reads never follow an unbounded
append chain, and a winning root reaches at most 32 tail-fragment objects per
live Stream, not one object per historical entry. For tiny entries, periodically
rewriting the growing tail base amortizes its bytes over up to 32 appends; for
large entries, the 256 KiB byte trigger compacts sooner. The constants are
initial values and must be benchmarked, but neither bound may be removed
independently.

### Deletion and trimming

`XDEL` groups requested IDs by physical leaf. Each affected sealed leaf is
rewritten once and only its COW paths change. IDs in the active tail cause one
tail fold/rewrite rather than one rewrite per argument. Removing the current
first or last entry also finds and publishes the next boundary ID; it does not
move `last_generated_id_` backwards. Pending-entry records refer to Stream IDs,
not entry records, so deleting an entry does not keep its payload live and a
consumer group may correctly retain a pending ID whose payload is now absent.

`XTRIM MAXLEN` and `XTRIM MINID` drop complete prefix leaves by removing their
tree references. Exact trimming rewrites at most the boundary leaf in addition
to COW directory paths. Approximate `~` trimming normally stops at a leaf
boundary and avoids that rewrite. `LIMIT` bounds examined/deleted leaf work as
required by the command. Bulk tree surgery rebuilds each changed directory node
once rather than issuing an independent root update for every removed leaf.
The `MAXLEN`/`MINID` forms attached to `XADD` perform tail append and trimming
under the same key lock and publish one final root; an intermediate untrimmed
Stream version is never visible or recoverable.

### Consumer groups and pending entries

The group directory is the path-compressed extensible-hash structure used by
Hash and Set, keyed by group name. A group-directory bucket stores packed
`StreamGroupRecord` values:

| Group field | Meaning |
| --- | --- |
| `last_delivered_id_` | Greatest ID delivered as a new message to this group. |
| `entries_read_` | Redis lag-accounting value plus an explicit valid/unknown flag. |
| `pending_count_` | Exact group PEL cardinality. |
| `consumer_count_` | Exact number of consumers. |
| `pel_root_` | Ordered group-wide PEL root, or null/inline form. |
| `consumer_root_` | Extensible-hash directory of consumer records. |

The consumer directory is keyed by consumer name. Its packed
`StreamConsumerRecord` contains:

| Consumer field | Meaning |
| --- | --- |
| `seen_time_ms_` | Last time the consumer was observed. |
| `active_time_ms_` | Last successful delivery time. |
| `pending_count_` | Exact number of entries owned by the consumer. |
| `pel_root_` | Ordered per-consumer PEL root, or null/inline form. |

Small group and consumer PELs remain inline in their directory record up to a
small fixed threshold, initially 4 KiB, so an empty group or a consumer with one
pending entry does not allocate a separate tiny tree. Promotion is one-way until
the PEL becomes empty; this avoids representation oscillation.

The group-wide PEL and each promoted consumer PEL are ordered COW trees keyed by
`StreamID`. A PEL entry contains the consumer name identity, last-delivery time,
and delivery count. The fixed delivery metadata is duplicated in the two PEL
indexes so `XPENDING` with or without a consumer filter scans only one ordered
tree. Large consumer names use one immutable external name record shared by the
directory and PEL entries; Stream entry payloads are never duplicated in a PEL.
PEL leaves have a 512 KiB maximum, split near 256 KiB, and merge below 128 KiB
only when the result is at most 384 KiB, leaving hysteresis below the split
threshold.

PEL mutations update all affected indexes beneath one new Stream root:

- `XREADGROUP` with `>` advances group delivery state and, unless `NOACK`,
  inserts each returned ID into the group PEL and the consumer PEL;
- `XACK` removes each ID from both PELs after the group PEL identifies its
  current consumer;
- `XCLAIM` and `XAUTOCLAIM` replace delivery metadata in the group PEL, remove
  the ID from the old consumer PEL, and insert it into the new consumer PEL;
- group/consumer create, delete, and `SETID` operations rewrite only their
  affected hash buckets, PEL leaves, and COW paths.

Arguments are grouped by bucket and PEL leaf, so a batch rewrites each physical
object once. The final Stream root atomically publishes the entry tree, tail,
group directory, and all PEL changes. No command can recover with a group PEL
that names a different consumer version than the consumer PEL.

### Stream recovery and memory

Stream roots, sealed nodes/leaves, tail descriptors/fragments, group and
consumer buckets, PEL nodes/leaves, and external entry/name records all use the
common physical-reference recovery graph. The disk is scanned once. After the
winning committed Stream root is selected, graph traversal positively charges
only reachable objects and extents; an unpublished fragment, a superseded tail,
or half of an uncommitted group mutation receives no live-byte charge.

Recovery never decodes field/value arrays or allocates one in-memory item per
entry or pending ID. Its temporary memory is proportional to physical objects
and edges. Sealed entry leaves normally average about 512 KiB, PEL leaves about
256 KiB, and each winning Stream root reaches at most 32 fragment objects.
Inline small PELs prevent a one-leaf-per-consumer explosion. Nevertheless,
adversarial numbers of Streams, groups, consumers, external entries, or tiny
promoted PELs can exhaust the recovery graph. The common object/edge memory
budget therefore applies during the single scan and fails startup cleanly before
OOM. Runtime admission must enforce the same physical-metadata-object budget so
a data set accepted during normal operation is also recoverable.

The scan can encounter more than 32 obsolete fragments and tail descriptors
per Stream because unreachable objects remain inside partially live records
blocks until defrag reclaims those blocks. A byte-only garbage threshold is not
sufficient for tiny fragments. The runtime therefore counts all allocated
internal objects, including dead-but-not-yet-reclaimed objects, against the
recoverable-object budget. `XADD` applies backpressure and prioritizes defrag
before another fragment would cross that budget. Recovery must never depend on
background cleanup having completed immediately before the crash.

## Required correctness tests

The implementation is incomplete until deterministic tests cover these storage
transitions in addition to ordinary command compatibility:

- relocate a large-key child and its COW parent path, durably reuse the old
  child block, crash at every root/retirement fence, and verify recovery always
  chooses the greatest physical LSN rather than scan order;
- `DEL`, `UNLINK`, active and passive expiration, last-element deletion,
  compact demotion, large `LTRIM`/`XTRIM`, and `XGROUP DESTROY`, both after
  retirement completion and after crashing with a partially completed batch;
- transaction failure after each newly appended root/node/leaf/extent and after
  rollback, proving that abort retires every new object while leaving all old
  shared subtrees live; repeated writes to one key in one transaction cover
  intermediate COW paths;
- transaction commit crashes before and after the commit marker, proving old
  paths retire only after the decision and the two ZSet/PEL indexes never
  recover at different versions;
- boundary sequences around compact promotion/demotion, split, underflow,
  3/4-threshold merge, and one subsequent small insert/delete, proving the
  representation does not oscillate;
- deterministic rank-routing tests for uneven Hash/Set buckets and ZSet leaves,
  plus statistical smoke tests for `SRANDMEMBER`, `SPOP`, `HRANDFIELD`, and
  `ZRANDMEMBER`;
- `COPY` of every large type followed by independent mutation/deletion of source
  and destination, asserting that no physical child or extent reference is
  shared and live-byte accounting is never subtracted twice;
- restart under the minimum supported disk-to-memory ratio with worst-case
  leaf counts, tail fragments, pending retirement objects, transactions, and
  PELs, verifying bounded failure reports `ResourceExhausted` rather than OOM.

## Blocking operations

Blocking List commands use FIFO queues owned by each key's storage worker. Each
worker keeps its queues in a thread-local `absl::flat_hash_map`; there is no
process-wide waiter map or waiter-map mutex. A multi-key command registers the
same shared waiter in every involved owner-local queue. Its monotonic ticket
keeps the ordering consistent even when cross-core registration messages arrive
in a different order.

Registration is the only round-trip operation: after every owner confirms the
registration, the command atomically retries the nonblocking operation, closing
the empty-check/register race. The storage mutation itself runs on the key
owner, but the foreground coroutine normally resumes on its connection worker,
or on a multi-key coordinator, before calling `NotifyBlockingKey`. That helper
notifies locally when the caller is already the key owner and otherwise posts a
one-way message to the owner; it never waits for the waiter to run. Replica
apply and other background publishers that can make a blocking predicate true
must invoke the same owner-local availability hook directly. A conservative
extra notification is harmless because every waiter rechecks under the key
locks.

For a consuming List wait, only the FIFO head is signalled; it stays at the head
until it consumes, times out, or disconnects, so a second notification cannot
let a younger waiter race ahead on another worker. The woken command reacquires
the complete key set and retries atomically. If another nonblocking command
consumed the value first, the waiter keeps its queue position and sleeps again.

Finite deadlines use a timer coroutine rather than command polling. Completed
waits cancel their logical timer immediately; a one-second cancellation check
bounds retention of the sleeping timer frame. Infinite waits allocate no timer.
Coroutine cancellation and normal completion post one-way removals to all key
owners, and removal of a head notifies the next waiter. Keylane's implementation
is independent rather than copied from another server.

`XREAD BLOCK` and `XREADGROUP BLOCK` use the same owner-local registration and
atomic recheck protocol, with one waiter registered on every requested Stream.
Only a published `XADD` whose ID can satisfy a saved cursor sends a one-way
wakeup. Because `XREAD` is non-consuming, every eligible `XREAD` waiter is
signalled and may observe the same new entry; it is not restricted to the FIFO
head. `XREADGROUP` waiters are queued per `(Stream, group)`: the oldest eligible
waiter assigns available entries and atomically publishes group/PEL state, then
wakes the next waiter if deliverable entries remain. If another command removed
the opportunity, the waiter keeps its position and sleeps again. Timeout,
disconnect, and multi-Stream cleanup use the same shared waiter lifetime rules
as blocking List commands.

## Memory and admission

Steady-state permanent metadata is proportional to live top-level large keys
plus the configured directory-cache capacity, not segments, elements, or
historical revisions. The sparse root-state table and bounded cache are included
in process RSS and therefore in the existing `--max-memory` reconciliation.
Graph-retirement batches and their traversal visited sets are transient but
have separate hard queue/object/byte budgets; mutations apply backpressure
rather than allowing an unbounded deletion backlog. Admission must reserve the
small root state before promotion and enforce explicit cache and retirement
budgets rather than relying only on the sampled RSS guard.

Recovery has a separate bounded allowance for its packed object graph,
transaction candidates, physical-ref index, and visited bitmaps. It is released
after root states and block live-byte totals are installed. Startup fails with a
clear resource error if that allowance cannot represent the scanned metadata;
it does not fall back to a second semantic model or silently omit reachability.
