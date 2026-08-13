# Recovery Metadata Layout

## Implemented scope

Keylane reserves a capacity-derived prefix in every configured file or raw
block device. The prefix contains:

- the immutable device label;
- 16 database epochs;
- 16,384 partition replication epochs;
- one recovery scan bit per physical 8 MiB block.

The epoch and bitmap pages use independent A/B slots. There is no metadata
journal, tree, checkpoint, or persistent worker table in this implementation.
Each path's persisted capacity is fixed after initialization, so its complete
bitmap size is known before data allocation starts. Different devices may have
different capacities and therefore different bitmap lengths and data-prefix
boundaries.

The format version remains 1 during development. Existing files must be
cleared when this layout changes.

## Fixed physical layout

All metadata I/O uses checksummed 4 KiB pages:

```text
offset 0
  DeviceLabel

offset 4 KiB
  epoch page 0 slot A
  epoch page 0 slot B
  ...
  epoch page N slot A
  epoch page N slot B

then
  scan-bitmap page 0 slot A
  scan-bitmap page 0 slot B
  ...

then, rounded up to an 8 MiB boundary
  data block 0
  data block 1
  ...
```

`DataBlockBegin(capacity_blocks)` computes the first physical local block that
may hold records. Block IDs continue to contain the real local block number, so
the reserved prefix never needs a separate address translation.

Each metadata page header records its type, logical page index, payload length,
generation, and CRC32C. An update writes the inactive A/B slot and calls
`fdatasync`; only then is that slot published as current in memory. Recovery
selects the valid slot with the highest generation. A zero/zero pair is an
uninitialized page whose logical contents are zero. A nonzero pair with no
valid slot is corruption and startup fails.

At the 1 PiB per-device limit:

```text
1 PiB / 8 MiB                         = 2^27 physical blocks
one scan bit per block                = 16 MiB payload
16 DB + 16,384 partition uint64 epochs = 128.125 KiB payload
```

A/B copies and page headers make the fixed prefix about 32.6 MiB, which rounds
to five 8 MiB blocks. Smaller configured files reserve fewer bitmap pages.

## Epoch metadata

The logical epoch array is laid out as:

```text
[0, 16)       database epochs
[16, 16400)   partition replication epochs, indexed by Redis slot
```

Epoch zero is not used. A fresh metadata area initializes every value to one.
The partition owner decides the next replication epoch and sends the update
directly to every device owner. Each device owner serializes the update with
the other metadata operations for its device, patches its owner-local page
image, writes the inactive A/B slot, and calls `fdatasync`. There is no global
metadata-page owner and worker zero has no special role in this path. The
logical epoch is published only after every device owner succeeds.
If any mirrored write or sync fails, further record writes are stopped until
restart; continuing with an old in-memory epoch would be unsafe when another
device may already contain the new valid page.

Startup reads every device, selects each device's newest valid A/B page, and
takes a component-wise maximum. This is safe because these epochs only increase
and startup requires the complete device set.

`FLUSHDB` persists the new DB epoch before dropping the in-memory DB indexes.
Recovery ignores records with an older DB epoch, so no per-key disk rewrite is
required. After the indexes are cleared, Keylane marks their old locations dead.
If this makes the worker's active append block completely dead, `FLUSHDB` seals
that block immediately and queues it for flush; a mixed active block remains
open so live records from other logical databases are not disturbed. Once the
sealed block is durable, the normal defrag path can return it to the ready pool.

`ResetReplicaPartition` locks the owning worker's append stream, persists the
new partition epoch, publishes it to the partition, and then removes the old
partition contents. Holding the writer lock across the metadata commit prevents
a command from receiving success for an old-epoch record after the new epoch is
durable. Recovery rejects every record whose partition epoch differs from the
persisted epoch.

## Recovery scan bitmap

The scan bitmap is conservative:

- bit 0 means the block has never been activated, or was durably returned to a
  future cold-free pool; recovery skips all I/O for it;
- bit 1 means recovery must inspect the block header;
- a bit-1 block with a zero header is a valid false positive and becomes a
  reusable ready block.

A false positive only costs one 4 KiB header read. A false negative could hide
committed data, so block activation uses strict ordering:

```text
device owner chooses a batch of block IDs
device owner sets their in-memory bits
device owner writes inactive bitmap A/B pages
fdatasync(device)
publish the block IDs to the owner-local ready pool
hand one block ID to a worker
allow record writes
```

Only the device owner can modify its ordinary bitmap vector or A/B page state.
The bitmap is therefore not atomic and allocation never performs a contended
cross-core CAS on a block bit.

The current activation batch is 256 blocks. It amortizes a metadata durability
operation across later standby requests, but it does not allocate 8 MiB memory
buffers for those blocks. They are only IDs in the device owner's ready vector.

Defrag uses the reverse safe ordering:

```text
remove live index references and wait for pins
write a zero block header
fdatasync(device)
return the ID to the device owner's ready pool
```

The bit intentionally remains one on this warm-reuse path. Reassignment then
needs no bitmap write. Consequently, the current bitmap skips pristine space
but may continue scanning historically used, now-free blocks. Batched warm-to-
cold bit clearing can be added later without changing the allocation ordering.

If a bitmap metadata write fails, the device allocator is frozen until restart.
It does not advance to later blocks because the durable state of the failed I/O
is ambiguous. Restart resolves the newest valid A/B page.

## Runtime ownership and queues

Each device has one runtime allocator owner:

```text
owner = persistent_device_id % current_worker_count
```

Ownership is not persistent and therefore changes safely with worker count.
The owner maintains ordinary owner-local containers for:

- the pristine cursor;
- allocation epoch counter;
- ready blocks;
- cold-free blocks;
- bitmap bytes;
- the device's epoch-page image and bitmap/epoch A/B generations.

Another worker requests a block by submitting a coroutine to that owner. There
is no NxN free-lane matrix and no MPMC ready queue.

Partition replication control remains on
`partition_id % current_worker_count`. That partition owner computes the next
epoch, broadcasts the persistence request to all device owners, waits for all
of them, and then installs the new in-memory value. Sharing a 4 KiB epoch page
between many partitions is safe because each device owner serializes all page
read-modify-write operations for its own device.

Each storage worker holds at most one active append block and one standby block
ID. At 75% active-block occupancy it starts a standby request. Promotion is
local. If the active block fills before the standby arrives, only the writing
coroutine waits; it releases the worker store-state mutex for ordinary commands so
unrelated work can continue. Partition reset deliberately keeps the mutex while
waiting because its epoch transition must remain serialized.

The foreground allocator preserves a small per-device defrag reserve. Device
selection retains worker affinity where possible and falls back to other
devices when needed. If no foreground block is immediately available, an
ordinary write waits in its coroutine while a flush or defrag pass is active,
then retries every device. A monotonically increasing reclaim generation closes
the completion race: `ResourceExhausted` is returned only after a stable
observation with no flush/defrag in progress and no newly returned block.
Defrag's own reserve allocation never waits for another defrag, avoiding a
self-deadlock when the protected reserve is genuinely exhausted. An I/O failure
still stops the writer and is reported as `FailedPrecondition`, rather than
being mistaken for capacity exhaustion.

The reserve and scheduler are per device, not per worker. Every device protects
eight ready blocks from foreground allocation and has an independent ready
queue with at most eight active defrag permits. A candidate is queued according
to its source block's device. Releasing a permit wakes work only for that
device, so workers do not continuously contend for reserved block IDs and one
device cannot consume another device's recovery capacity. Actual concurrency
is also naturally capped by the worker count because a worker runs at most one
defrag pass at a time. A candidate that temporarily encounters
`ResourceExhausted` is returned to the queue instead of being lost.

## Startup sequence

The target recovery path runs in this order. Steps 7 through 10 replace the
current incremental `RecoveryRecord` merge and parked-transaction vectors; the
device-label, epoch, bitmap, scan-assignment, and allocator steps are already
implemented:

1. Validate all device labels and reconstruct persistent device ordering.
2. Compute the fixed metadata prefix from the persisted capacity.
3. Load A/B epoch pages from every device and install canonical DB/partition
   epochs.
4. Load each device's A/B scan bitmap.
5. Distribute each device's physical data-block range across the current
   workers using a global linear ordinal; device capacities need not match.
6. Skip bit-0 blocks; inspect bit-1 headers; fully read valid committed blocks.
7. Reject records with stale DB or partition epochs and append the remaining
   facts to the packed transient arenas defined below; do not charge record
   bytes as live while scanning.
8. Finalize the committed-transaction set, select each key's winning top-level
   version, and resolve large-key physical references entirely in memory.
9. Positively charge winning ordinary records and objects reachable from
   winning large-key roots to their blocks' live-byte totals.
10. Install the partition indexes and fixed-size large-key root states, then
    release the complete transient recovery representation.
11. Rebuild allocation high-watermarks and per-device ready pools from the
    resulting zero/nonzero live-byte state.

The scan assignment depends on the current worker count, while block identity
and metadata do not. Restarting with a different number of workers or a
different command-line device order therefore does not rewrite data.

## Packed one-pass record recovery

### Status and objective

This section specifies the target transient representation. It replaces the
current recovery path's heap-heavy `RecoveryRecord` vectors,
`absl::flat_hash_set` of committed transaction IDs, per-candidate key strings,
and decoded per-candidate List descriptor vectors. It is not implemented yet.
It does not add persistent metadata, change `RecordLocation`, or add another
disk scan.

The physical device pass validates each committed record and each large-key
internal object exactly once. Selection, transaction adjudication, physical
reference resolution, and live-byte accounting may make several passes over
the compact in-memory representation. All blocks begin that work with
`live_bytes_ == 0`; recovery proves liveness positively instead of initially
charging every committed byte and attempting to subtract garbage.

The transient representation consists of contiguous arenas:

| Arena | Contents |
| --- | --- |
| `RecoveryKeyArena` | One copy of each logical Redis key and its candidate-run head. |
| `RootCandidateArena` | Top-level ordinary-record and large-key-root versions. |
| `PhysicalObjectArena` | Large-key roots, directories, buckets, leaves, segments, and external object identities. |
| `PhysicalEdgeArena` | Direct references from an object to child objects or extents. |
| `CommitTxidArena` | Valid `kTxCommit` transaction IDs. |
| `ShieldingArena` | Minimal facts about discarded older values that may still suppress resurrection. |

There is no C++ object, `std::string`, `std::vector`, or `shared_ptr` allocation
per scanned version, child edge, or transaction candidate. The key bytes are
owned once by `RecoveryKeyArena`; candidates refer to a key ordinal. External
manifests and large-key child lists are edge ranges, not separately allocated
vectors.

### Framed delta encoding

Each arena is divided into independently decodable frames, initially 128 or
256 entries per frame. A frame header holds suitable bases such as block ID,
allocation epoch, key ordinal, mutation sequence, and the encoded payload
length. Monotonic or locally clustered fields use unsigned delta varints;
non-monotonic signed deltas use zig-zag varints. Small enums and flags are
bit-packed. Random 32-bit checksums remain fixed-width because varint encoding
normally makes them larger.

A root candidate encodes the logical equivalent of:

```text
key_ordinal_delta
mutation_sequence_delta
physical_lsn_delta
txid_delta
physical_object_ordinal
expire_at_delta
kind_and_value_type_flags
```

A physical object encodes the logical equivalent of:

```text
block_ordinal_delta
allocation_epoch_delta
record_offset_delta
total_disk_bytes
object_kind_and_level
edge_begin_delta
edge_count
payload_checksum_u32
```

The exact byte format is versioned with unit golden vectors before it becomes
code. The listed logical fields are mandatory even when compression makes a
different physical grouping smaller. In particular, allocation epoch and
checksum cannot be omitted merely because block ID and offset appear unique.

Frames have a dead-entry bitmap. Candidate pruning marks entries dead without
moving the remainder of a varint stream. A frame whose dead density crosses an
initial 50% threshold is decoded once and compacted into a replacement frame.
The final adjudication pass compacts all remaining candidate frames. A sparse
frame-offset table and a very small decoded-frame cache provide ordinal lookup
during graph traversal without creating a pointer per object.

### Transaction candidates and the definite floor

The scan accepts a valid `kTxCommit` sighting as positive proof that its txid
committed. The absence of a commit cannot be decided until every worker has
contributed its sightings at the recovery barrier. A tagged record must
therefore remain a possible version until that barrier unless a newer version
that is already definitely valid makes it irrelevant.

For each key, recovery maintains this logical frontier:

```text
definite_floor:
  newest valid candidate with txid == 0 or an already-seen valid kTxCommit

pending:
  newest candidate per unresolved txid whose mutation sequence is greater
  than the definite floor
```

A `txid == 0` record is unconditional. After validating its header, checksum,
DB epoch, and partition replication epoch, a candidate with mutation sequence
`S` establishes a definite floor at `S`. Every complete candidate with a
strictly smaller sequence can immediately be discarded, including unresolved
transaction candidates. A lower candidate encountered later is discarded on
arrival. Pending candidates with a sequence greater than `S` remain because a
missing commit may eventually force recovery back to `S`.

The same pruning applies when a positively observed `kTxCommit` makes a tagged
candidate the new definite floor. Within one `(key, txid)`, only the greatest
mutation sequence can win: if the transaction committed all its writes are
valid, and if it did not commit none of them are valid. Equal-sequence physical
copies are ordered by the already-persisted `RecordHeader::lsn_`; the greatest
physical LSN wins. Every append, including a defrag relocation, obtains a new
LSN from the process-wide monotonic allocator, and startup seeds the allocator
above the maximum LSN found on disk. If mutation sequence and LSN both tie, the
copies must describe the same root and graph identity or recovery reports
corruption. Scan order is never a tie-breaker. The LSN exists only in the disk
header and packed recovery candidate, so the 48-byte runtime `RecordLocation`
does not grow. No separate relocation generation participates in recovery
version arbitration.

For example:

```text
sequence 10, txid 31
sequence 11, txid 42
sequence 12, txid 0       definite
sequence 13, txid 51
sequence 14, txid 62
```

After sequence 12 is scanned, the full candidates for 10 and 11 are
unnecessary. If 62 committed, sequence 14 wins; if only 51 committed, sequence
13 wins; if neither committed, sequence 12 wins. This is why merely keeping the
largest mutation sequence is incorrect, but a newer `txid == 0` floor safely
eliminates every smaller candidate.

At the barrier, worker-local commit-ID runs are sorted, deduplicated, and
merged. The final set uses delta-varint frames with sparse checkpoints, or a
sorted fixed-width working vector when that is smaller. Candidate filtering can
binary-search those checkpoints or sort unresolved candidates by txid and
merge them with the commit run. It does not require a permanent hash table.
Once every key is adjudicated, the commit lookup representation is cleared; it
must not remain resident during normal service.

### Shielding facts after candidate pruning

Deleting an older candidate's root, location, and extent range is different
from forgetting that an older physical value exists. A newer tombstone or
expiring version may need shielding until old value bytes disappear from a
partially live block; otherwise removal of the newer record could let the old
value reappear at a later recovery.

When a definitely valid floor eliminates an older definitely valid value, the
candidate is folded into a small shielding summary containing whether an
immortal value exists and the maximum relevant expiration time. An unresolved
tagged value below the floor no longer needs its physical/root candidate, but
retains the minimal packed tuple `(txid, value flag, expire_at)` until commit
adjudication. A committed tuple contributes to shielding and an uncommitted
tuple is discarded. Treating every unresolved tuple as permanently committed
would be safe against resurrection but could retain tombstones forever, so it
is not the normal algorithm.

The final winner receives exactly the shielding state implied by older valid
physical values. Older large-key nodes do not remain live merely because their
root contributes a shielding fact; only the top-level suppressing record needs
that fact while defrag removes unreachable historical objects.

### Large-key roots and physical reachability

Only a top-level Redis-key root participates in mutation-sequence and txid
selection. Immutable List directory nodes and segments, Hash/Set radix nodes
and buckets, ZSet index nodes and leaves, Stream entry/PEL nodes, and external
objects do not independently compete for newest-version status. They are
anonymous physical objects whose meaning comes exclusively from a reachable
root. A transaction tags its top-level root; it does not require a transaction
candidate for every immutable child written before that root.

The scan appends every validated physical object and its outgoing direct
references to `PhysicalObjectArena` and `PhysicalEdgeArena`, even when the root
that might publish it has not been encountered yet. Scan order may therefore
be child, parent, root, and commit without changing the result. An object
written before a crash but not reachable from a winning root is an orphan.

Direct references cannot be resolved through per-object heap hash nodes. After
the scan, recovery sorts packed `(DirectRecordRef, object_ordinal)` identities
and packed unresolved edges, merge-joins them, and replaces each valid edge
with its target ordinal. Missing references, allocation-epoch mismatches,
checksum mismatches, cycles, level/count inconsistencies, and out-of-range
ordinals fail startup. Recovery never falls back to an older root after finding
that a newer definitely valid root is corrupt.

Selecting a newer root discards older root candidates, but it does not delete
physical-object descriptors immediately. COW versions may share children. Only
the final graph traversal can decide whether an object is unused:

```text
old root ---> segment A
new root ---> segment A
          \-> segment B
```

Removing the old root must not remove the descriptor for shared segment A.

The physical scan also reduces the greatest durable LSN. After its barrier,
worker `i` initializes its private append sequence to `max_lsn + 1 + i` and
increments it by `worker_count`. These striped sequences are disjoint and need
no atomic operation on the write path. They remain a valid root tie-breaker
because every physical publication of one key, including defrag relocation,
runs on that key's current owner; after a topology-changing restart every new
sequence begins above every LSN written by the old topology.

### Final filtering and positive block accounting

After the global transaction barrier, recovery performs these memory-only
steps:

1. Discard every remaining candidate whose nonzero txid is absent from the
   finalized commit set.
2. Select the greatest surviving `(mutation_sequence_, physical_lsn)` for each
   Redis key and finish its shielding summary.
3. Charge a winning compact ordinary record directly to its records block and
   charge its reachable external extents.
4. For a winning large-key root, mark the root object and traverse its resolved
   graph using a packed visited bitmap.
5. On the first visit to an object or extent, add its exact physical bytes to
   the owning block's `live_bytes_`; repeated shared references add nothing.
6. Validate the root's cardinalities, subtree counts, levels, and encoded byte
   totals while traversing.
7. Install only the final `RecordLocation` and fixed-size type-specific root
   state in runtime memory.

Unreachable historical roots, uncommitted transaction objects, crash orphans,
and superseded COW paths receive no live-byte charge. A records block with zero
live bytes is directly reclaimable; a partially live block is a defrag
candidate. The disk is not rescanned during this phase.

`kTxCommit` records are special recovery evidence rather than children of a
large-key root. Until transaction GC proves that no physically surviving
tagged record can require a commit, recovery conservatively charges valid
commit markers as live. It must not delete a commit merely because no current
winning key happens to carry that txid; an older tagged record may still be
present in a partially live records block and become relevant if its suppressing
version disappears incorrectly. Commit-marker GC remains a separate background
maintenance proof.

### Memory admission

Recovery reserves explicit budgets for packed key bytes, candidates, objects,
edges, frame indexes, sorting scratch space, commit IDs, shielding tuples, and
visited bitmaps before growing the corresponding arena. The budget is based on
actual scanned object and edge counts and a configured fraction of available
memory. Allocation that would exceed it fails startup with
`ResourceExhausted`; recovery never continues until the process is killed by
OOM and never silently omits an object or edge.

The definite-floor rule bounds ordinary history aggressively: after a newer
unconditional version, only unresolved higher transactions remain as complete
candidates. Pathological histories consisting entirely of distinct unresolved
txids can still be large, as can disks containing huge numbers of small
large-key leaves. Framing and varints reduce their bytes but do not change that
cardinality. Segment/bucket/leaf sizing, transaction-candidate stress tests,
and the supported disk-to-memory ratio must therefore be validated together.

## Crash invariants

- A data block is never writable before its scan bit is durable.
- A DB or partition epoch is never published before its metadata page is
  durable on every configured device.
- A defragged block is never reusable before its old header is durably zero.
- An unused standby may leave a durable bit with a zero header; recovery safely
  returns it to the ready pool.
- Metadata A/B generations make a torn page update fall back to the previous
  valid slot.

## Deliberate limitations

- Corrupt A/B metadata currently fails startup; full-scan reconstruction is not
  implemented.
- Bitmap bit clearing and a bounded warm pool are not implemented yet.
- The bitmap does not avoid record scanning on a nearly full device. Fast
  recovery at high occupancy will eventually require a partitioned persistent
  index checkpoint.
- Device addition requires a restart with the complete old set and zero-label
  new paths. Online addition and device removal are not implemented.
