<!--
Copyright (C) 2026 EloqData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Grouped collections

## Boundary and availability

Grouped storage is a representation inside the storage engine, not a new
Redis keyspace or a separate database. String, Hash, Set, List, Sorted Set and
Stream support compact values and complete, independently addressed group snapshots. Writes
automatically promote compact collections at 16 KiB of encoded size, with an
8 KiB target per group shared by all six types. Indivisible entries and Hash
collisions can exceed that target. A complete update returns to compact storage
when active primary group payloads total less than 8 KiB; deletion and whole
replacement can also end the grouped incarnation. The payload sum bounds the
compact encoding size, so the writer selects that representation before
staging replacement groups.
Streaming collection imports and grouped key transfers construct graphs directly.
Intermediate ingest mutations retain grouped roots. Once an import is complete,
active primary group payloads below 8 KiB are compacted to one root in the
ingest transaction.
Stream RDB and native receiving also ingest logical records page by page. Both
ordinary and Debug builds use the same read, mutation, recovery and maintenance
adapters.

Grouped Strings use fixed 8 KiB byte segments; Hash/Set use a persisted-seed
prefix directory; List uses an ordered-page directory. Newly built Sorted Sets
combine
ordered `(score, member)` pages with a prefix directory mapping each member to
its score. Both directories
belong to one object and share its physical index and transaction lifecycle.
A prefix directory alone does not supply rank or score/member ordering.

Streams use ordered binary record keys for messages, logical macro-node
metadata, groups, consumers and individual pending entries. Their externally
visible length is independent of the internal record count. See
[Streams](12-streams.md) for their access and logical-format contracts.

String writes promote at the same 16 KiB encoded-size threshold as collections,
regardless of key length. Parent keys of at most 2 KiB are inline; longer keys
share one immutable KeyRecord through a UUID in each root and auxiliary header.
Segment payloads never contain copies of the parent key. See
[storage formats and lifetime](04-storage-and-recovery.md) for UUID ownership.
Segments have dense, one-based identifiers derived from byte offsets. All but
the tail contain exactly 8 KiB; extension materializes zero-filled gaps.
The resident String directory and physical index use direct positional indexing,
without binary search or hash routing. Physical index pages share unchanged
entries with snapshot/undo versions. Incremental writes preserve segment IDs;
whole replacement or shrinking may create a fresh incarnation or a compact value.
For grouped Strings, GETRANGE/GETBIT read only intersecting segments;
SETRANGE/SETBIT/APPEND replace intersecting segments and any changed tail link
through the shared grouped publication boundary. General whole-value callbacks
materialize grouped Strings and reuse unchanged segments in their after-image.
Strings below the promotion threshold use compact whole-value reads and writes.
Dedicated KeyRecords store original key bytes whole regardless of this
user-value promotion threshold.

## Identity and ownership

The top-level `RecordIndex` still owns each user key, type, expiry and logical
version. A grouped marker directs collection lookups to that
partition/database's sparse `ScanHashMap` object index. Its immutable view holds
a routing directory, the active primary group payload total, one compact
physical index entry per group, and separately owned extent manifests. Retired
markers and the Sorted Set member index do not contribute to the total.
Recovery rebuilds it from selected checked group payload lengths. The view
does not retain field names or values.

A root's incarnation distinguishes deletion/recreation from updates. The
persisted hash seed determines field routing independently of the process's
top-level lookup seed. Hash and Set share prefix routing, with Set members
represented by fields with empty values. Split parents remain indexed as
retirement records until the incarnation disappears; losing this evidence
could revive an older parent during cold recovery.

Ordered roots identify a doubly linked page chain, aggregate item count and
the next unused page identifier. Page identifiers are not reused inside an
incarnation. Rank metadata locates a page without keeping item values resident.
Splits and removals publish changed neighbour links and retained retirement
records together. Sorted Set directories also retain per-page minimum and
maximum scores, but no member boundaries. Stream directories retain bounded
per-page maximum routing keys and a fixed header with their logical version;
unknown or ambiguous long-name boundaries fall back to checked page reads.
These runtime-only bounds do not change the durable page format. Recovery checks the complete chain,
rank totals, numeric score boundaries and every live extent checksum while
retaining only routing metadata. Full-page decoding validates local item
ordering; Sorted Set materialization also checks
score/binary-member ordering across adjacent pages.

Indexed Sorted Sets persist each member twice: once in an ordered page and
once as a Hash field whose value is an eight-byte little-endian IEEE-754
score. The member index records scores, not ordered page identifiers, so
ordered splits do not invalidate member routing. Neither directory retains
per-member data in memory; the ordered directory's score bounds scale with
pages, not members. Both graphs have the same incarnation and cardinality.
Their revisions need not match: an ordered-only topology change
or compensating root may retain an older, unchanged member graph.

The outer root record retains source command order; its payload separately
persists a local group revision. One native replay envelope may therefore
change the same key repeatedly without treating distinct group versions as
relocation copies. Every group mutation uses a fresh globally allocated
command-batch identifier as its revision; promotion uses that identifier as
the new incarnation. Auxiliary mutation sequences name these local revisions.
Recovery bounds candidates by the winning root revision, preserves strict
same-revision consistency, and advances the identifier allocator from root
and auxiliary metadata even after transaction cleaning removed their tags.

Expiration-only updates append a new root without reading or rewriting group
payloads. The incarnation and value revision remain unchanged; the new root
advances command order and changes expiry while sharing the exact routing
directory and physical index pages. An object's current command sequence comes
from its root version, so a shared directory can retain an earlier command
sequence. Such a root retains a dependency on the preceding decision before an
independent transaction can commit it, and snapshot/undo views retain their
own historical root expiry. Immediate expiration uses the ordinary
tombstone/graph retirement path.

Views share unchanged physical index pages across mutations. Small updates to existing inline records retain a bounded immutable
coordinate overlay before folding into the physical index. Lookup and graph
lifecycle traversal resolve these overrides, including retirement state; they
retain no payload data. Topology changes and external records fold pending
overrides into the index, while positional String indexing retains its
direct page path. The physical
identity trie skips common prefix bits and retains complete identities in its
leaves; inserting outside a skipped prefix adds a branch without changing
pinned older views. Hash routing nodes are persistent; ordered directories share
owner-local metadata chunks across immutable views. Each allocation admits and
accounts its own lifetime, independently of the number of views retaining it.
Local page replacements detach changed chunks. Lists and Sorted Sets update
rank intervals whose counts change; Streams use a persistent partial-sum rank
index to update logarithmic cells per changed count. Count-neutral replacements
share ranks. Unchanged identities and retirement records remain shared. Stream
suffix insertions preserve a validated predecessor prefix and check the
remaining chain and aggregate counts; other topology changes validate the
complete resulting chain. Only recovery selects among competing physical
candidates. Retired identities remain available to GC. Routing and physical-index node references,
including final destruction, remain on
the key owner. Cross-worker readers exchange physical identities or stream
handles that route metadata access and cleanup back to that owner. Retained
directories, index pages, manifests, publication reservations, retirement
receipts and snapshot pin lists participate in memory admission and accounting.
Admission failures before root publication preserve the previous logical view.
A handle retains metadata only: physical coordinates and allocation epochs are
captured before suspension. Whole-object snapshot readers pin the complete
captured graph; Stream range readers pin the root and their selected page
interval before emitting a reply.

View population generations are scoped to one partition and logical database.
Detaching that population advances its generation; resetting another partition
or promoting an already-built candidate does not invalidate its retained
views. The broader worker index generation remains an independent guard for
ordinary suspended storage operations.

Owner-local standalone predecessors enter the commit queue before releasing
key locks. Successors may stage changed pages and publish readable in-memory
roots while those decisions are pending. Receipts retain the predecessor
failure decisions, so the background commit waits for predecessor durability
before appending the successor decision. A shared commit batch flushes its
appended predecessor frontier before processing a dependent successor,
including across block rollover. Failed ancestors prevent descendant commits;
recovery cannot accept a root inheriting pages from an uncommitted ancestor.
Borrowed EXEC/Lua predecessors whose coordinator is not known to be the local
queue retain the foreground dependency wait. Standalone compact demotion also
retains that boundary because it appends an untagged complete value without a
queued receipt. Ordinary compact SET/GET and
pending-buffer reads do not acquire grouped predecessor dependencies.

## Durable graph and publication

A small keyed root and auxiliary keyed group records preserve the user-visible
collection type. Auxiliary identity in the checked outer header includes
incarnation, group identity, field count, retirement state and a nested batch
decision when present. Auxiliary records never enter the user-key winner merge
or Redis key/expiry counts.

The version-1 ordered-root payload has type-checked shapes: 72 bytes describe
only the ordered graph, including String segments; 136 bytes append the 64-byte
Hash root for an indexed Sorted Set; an 80-byte Stream root appends its
user-visible length. A member-index presence flag must agree with the payload
length, so a truncated indexed root cannot decode as an ordered-only root.

The durable ordered kinds are List=1, Sorted Set=2, Stream=3 and String=4.
String root and page counts measure bytes. Its page payload is a checked
64-byte envelope followed by at most 8 KiB of
raw bytes, without per-item framing. Recovery checks dense IDs, fixed segment
lengths, links, and aggregate byte length. String value segments fit ordinary
records even when the parent key is large, because its UUID has fixed size.
Ordered auxiliary identifiers have zero prefix bits and a nonzero opaque page
number. Member auxiliaries use canonical Hash prefixes (including the unsplit
`{0, 0}` root), a disjoint
identity space under the same Sorted Set type and incarnation. Recovery
bounds each graph by its own root revision.

Ordered-only Sorted Sets use the scan-based member path. New keys, compact
promotions and streaming imports build indexed roots; decoding never invents
an absent member index. Both shapes use the current unreleased v1 schema,
without compatibility decoders or migration for earlier development layouts.

Page score bounds are derived runtime metadata, not new durable fields. Writes
derive them from complete replacement pages and publish them with the same
immutable directory/root view. Recovery derives them from the existing entry
headers during the selected pages' checksum pass, skipping member payloads in
bounded space. Physical relocation does not change logical bounds, and old
read/snapshot views retain their own bounds.

Each group contains a complete snapshot, not a mutation log requiring read-time
replay or compaction. An indivisible large field, list item or sorted-set
member occupies an oversized group;
its bounded-state encoder fills ordinary payload extents without constructing
another full-size serialized value. The record payload envelope and the
individual Redis field/value limits remain independently checked.
The logical Hash, Set, List and Sorted Set codecs also represent complete
multi-group values and do not impose a String or physical-record limit on
their aggregate bytes. Each physical group validates its own envelope before
decoding; full-image
consumers remain subject to memory admission and their destination-buffer or
transport limits. Client query-buffer limits apply to assembling requests,
independently of the size accumulated under a collection key.

Changed groups and their root have one logical publication boundary. After all
storage waits, root preparation refreshes current GC coordinates and admits
metadata before staging root bytes. Root index replacement and side-view
publication then occur without suspension. Physical auxiliary writes alone
make no user-visible mutation.

Existing Hash/Set point writers and typed List/Sorted Set writers retain key
intent and database admission while releasing worker store state for page reads
and private mutation planning. This includes both Sorted Set graphs and retained
EXEC/Lua key holds, but not native candidate ingestion. Plans retain their page
admission through commit. Reacquisition checks population and logical root
identity even for no-ops and last-element deletion; physical relocation alone
is allowed. The writer refreshes GC coordinates and assigns durable revisions
under store state. Full-image, promotion and random Set-pop adapters retain
their separate preparation paths and share the same publication boundary.

Ordinary single-key creation prepares initial pages without worker store state,
including the Sorted Set member graph. Private plans own their scratch admission
and contain placeholder incarnations, never published identities. The command
keeps missing-key intent and database admission; before commit it validates
population and command-time logical absence, allowing expired/tombstone
predecessors to be reclaimed. Durable batch admission assigns one fresh
incarnation to the root and every initial page in both graphs. Transactional
creation and promotion of an existing compact value retain their separate
preparation contracts.

Sorted Set full-image mutation planning derives member-index changes from complete
ordered before/after pages, including full-image callback and import paths.
Typed updates verify requested old members against both indexes, then generate
both private plans from those exact changes. The member plan owns its page
strings before pop/range selections are released; no borrowed command member
escapes preparation. Its cardinality and predecessor membership/score checks
still precede any staging. Changed ordered pages, prefix snapshots and split retirements share the same
command-local decision and publish through one root. Failure cannot expose
only one half of the update.

A standalone grouped command uses one transaction decision. A command inside
EXEC/Lua additionally tags its auxiliaries with a command-local batch decision;
recovery requires both decisions. Failure before root publication leaves that
batch uncommitted, even if later commands commit the outer transaction. The
batch decision is durable before the successful command returns to its outer
coordinator. Failure after root staging fail-stops the writer and poisons the
shared outer decision, including when its coordinator runs on another worker.
That protection extends through post-root fence construction and commit-queue
handoff, not just record staging. A rejected queue handoff does not create a
pending-commit count without an owned receipt. Physical allocation failure
terminates the process; recoverable handoff failures use explicit statuses.
An explicitly failed decision also makes its published graph unreadable,
including collection length and checked Redis key metadata. An uncommitted
expiry cannot disguise that failure as an absent key. Internal compensation
alone may inspect the failed physical view to restore or retire it; snapshot
page readers independently recheck the decision after storage waits.

An incremental successor from another transaction waits for its predecessor's
durable decision before inheriting untouched groups. Same-transaction commands
can reuse their view. This causal dependency is separate from the physical
pins that keep prior transaction blocks allocated until retirement.

Before acquiring a standalone transaction's append lease, grouped writes
sample foreground space and coordinate transaction-block cleaning under pressure.
The predecessor decision is durable before that cleaning can run. Borrowed
EXEC/Lua transactions do not wait for their own still-active blocks to
retire. This pressure signal bypasses the periodic cooldown, not explicit
cleaner disabling or the allocator's authoritative capacity checks.

## Command access paths

Whole-Hash replacement (`LAVIK.HREPLACE`) checks existing key/type/expiry and
the preceding grouped decision, but does not load old field payloads. Its
after-image comes entirely from admitted request data. A large replacement
publishes a fresh incarnation through the grouped command decision; a small
replacement may demote to a compact record. The append funnel retires the old
graph after publication while preserving transaction/snapshot ownership. This
does not change the durable format or the incremental HSET/HMSET path.

Hash/Set point operations load each affected prefix group once. Point reads
scan encoded entries and retain only requested values. Multi-field reads group
request positions by routing leaf, fill result slots in command order, and
release each page before loading the next; repeated operands retain repeated results.
These reads validate all entry framing and duplicate occurrences of requested
fields; full decodes additionally validate every field's uniqueness
and route. Hash/Set field updates and removals validate complete affected
leaves using borrowed entry views, then own only changed replacement pages.
Small replacements retain checked encoded bytes until publication; splits and
oversized entries use owned fields and the bounded-state encoder. Other point
writes decode affected groups. All paths replace only changed groups and any
split-parent retirement records, without retaining user payloads in the index.
HSCAN/SSCAN consume one routing leaf per call and use field digests under the
persisted seed as cursors. COUNT is a hint; equal-digest fields remain in one
response, and empty routing leaves still advance the cursor. Deleting earlier
fields does not shift later fields behind a rank-based cursor. Random field
and member reads sample global ranks using directory counts and load only
selected leaves, retaining draw order and duplicate draws. Random Set pops
retain and replace only selected complete pages and replicate the chosen
members as deterministic removals. Whole-value reads still assemble a logical
value and admit its aggregate temporary peak.

Foreground adapters reserve selected-page scratch before decoding. Scanning
List operations and explicit full-image integrations admit their aggregate
temporary peak before retaining pages or creating a serialized copy; metadata
length reads do not require value-sized admission. These temporary reservations
are distinct from persistent side-index charges and from physical read-buffer
ownership. A streamed page result retains its own output capacity charge across
owner hops and reply construction.

Set and Sorted Set multi-key aggregation retains admitted input and output
owners across coordinator hops. These adapters still materialize their logical
inputs and aggregate result and may reject the combined peak with OOM. STORE
prepares operation vectors, encoding headroom and canonical replication effects
before deleting its destination. Failure paths release no-longer-needed
working state before compensation and propagate compensation failure rather
than disguising it as an ordinary admission rejection.

List length uses root metadata. Indexed/range reads validate the corresponding
rank pages completely and copy only requested values into owned replies.
Multi-page ranges overlap a bounded window of page reads, join every launched
read before returning or reporting an error, revalidate the population after
the join, and append results in rank order.
The caller owns the child tasks and aggregate reply admission; each child
releases its physical read lease after decoding. Admission covers one owned
payload plus reply/page headers and the bounded task window, independently
of read-buffer accounting. Indexed reads retain their direct single-page path.
Read-only operations retain shared
key intent and an immutable routing view, release worker store state before
page I/O, and validate population and physical record lifetime in the page
loader. Push, pop and indexed replacement load the affected interval and its
immediate link neighbours; an equal-sized indexed replacement only needs its
target page. Pivot and position searches consume one page at a time and retain
only the result; insertion reloads the located interval.
Value removals, trimming and within-list moves retain the needed logical
contents before forming a replacement interval. Only changed snapshots enter
the writer, and admitted reply buffers retain their charge across owner hops.

Sorted Set operations use a typed storage interface. Cardinality reads root
metadata. Indexed score lookups read only the selected member-prefix pages;
legacy score lookups and member ranks scan admitted ordered pages. Rank ranges
start at the directory's selected pages; score ranges and score counts first
seek their candidate interval using resident score bounds, then read matching
pages in physical order. Range, rank, count, scan, random and pop selection
borrow member bytes from one owned read lease and validate complete page
framing, ordering and uniqueness before consumption. Replies copy only
admitted output members. Mutation source and boundary probes, and rewrite
planning, use owned snapshots.
Mixed-score BYLEX preserves global member ordering without a resident member
index by repeatedly selecting
the next member: its work can scale with the collection size times the offset
and result count. Read-only scans retain shared key intent, yield between pages
and revalidate their population without retaining the store mutex. Add, increment,
remove and GEOADD use the member index, when present, to resolve old scores
before locating ordered source pages and routing final scores against old page
boundaries. Resident score bounds skip unrelated pages; equal-score runs still
require pagewise exact member comparisons. Batched requests coalesce source
intervals and share destination boundary reads against the old logical view.
Only changed pages and structural link neighbours remain decoded during
publication; a score
moving across the set does not retain or rewrite its intervening values.
There is no resident per-member index: equal-score or legacy member searches
can still scale with the collection, while scratch scales with requested
members, selected pages and routing metadata.
Repeated input members and conditional updates are evaluated in request order
before any physical write. Endpoint pops share the typed sparse mutation path across
single-key, multi-key and blocking commands. ZSCAN uses two pagewise passes and
a bounded digest-prefix selection heap, retaining whole collision buckets and
the requested output rather than all members. GEO point reads (`GEOPOS`,
`GEOHASH`, `GEODIST`) share the member-score lookup; all requested scores are
read under one key intent, including both endpoints of a distance query.
Range removal selects members pagewise under an exclusive key intent and
publishes one sparse mutation after selection and admission complete. Scratch
includes the selected members and affected pages; an oversized removal may
still be rejected before any logical change. Ordinary random reads draw ranks
before loading selected ordered pages and preserve duplicate draw order.
Large negative-count streamed random replies retain their admitted snapshot.
GEO area searches retain the full-logical-value callback with aggregate memory
admission and destination-buffer limits. Their physical rewrite planner still
restricts writes to changed pages and structural neighbours.

## Recovery, reclamation and snapshots

Cold scanning gathers roots and auxiliary candidates separately. After
transaction adjudication and root selection, recovery reconstructs each
winning incarnation, including retained parent markers, and checks routing
coverage and aggregate counts. Under the
[full-device expiration policy](04-storage-and-recovery.md#expiry-and-tombstones),
a graph reconstruction data-loss result may discard only an expired,
unshielded winner, and only with startup expiration authority. Recovery never
falls back to an older root. After allocator recovery, it appends a tombstone
and waits for durable deletion before serving. Only confirmed foreground disk
exhaustion permits skipping that detached-root repair; admission and I/O
failures remain fatal. All other reconstruction errors remain fatal; complete
expired graphs retain ordinary accounting and deletion.

Only reachable external group payloads are validated: an obsolete inline-key
group's value extents may already have been reclaimed while its records block
is still scannable. UUID references remain source-block dependencies so
classification can still resolve the original key. Every live group, root and
extent joins physical-owner accounting before orphan reclamation.

For indexed Sorted Sets, reconstruction requires both complete directories
and validates reachable member snapshots and extent checksums as well as the
ordered graph. GC, deletion and snapshot pins cover both identity spaces.
Logical collection streams traverse only ordered pages, emitting each member
once; ingestion reconstructs the destination's member index from those pages.

Native full-sync records and their side views use the candidate partition's
locally mapped database epoch before that epoch becomes globally served.
Source command sequence and local group revision remain separate during
this interval and during subsequent physical relocation.

Native sources pin a complete immutable graph and traverse it twice: first
to measure the compact wire image, then to emit bounded transfer chunks.
This avoids storing another aggregate value or changing the durable root to
carry transport-specific size metadata, at the cost of an additional snapshot
read pass. Source handles retain at most one admitted decoded page, and
cancellation waits for active page reads before releasing exact graph pins.
Pre-publication scans remain registered with shutdown through the handoff to
capture ownership or asynchronous release. Every chunk revalidates its session
and population, including chunks borrowed from an already decoded large page.

Framed native collection ingestion decodes the compact wire format into
admitted pages under one outer transaction. Intermediate roots have distinct
local revisions but the same source command sequence. Its undo retains the
original predecessor and current view, plus physical retirement and pin
receipts, without retaining every intermediate routing directory. The final
frame validates the complete byte/count contract before durable commit;
failure or cancellation rolls back the uncommitted graph and prevents
candidate promotion. Aggregate encoded bytes do not require a contiguous
buffer, but metadata, indivisible items and retained receipts still require
memory admission.

GC compares auxiliary identity and exact physical location against the current
side view. Root relocation publishes its matching view synchronously. Source
retirement waits for destination durability and pins, including transaction
cleaning; clearing transaction tags also removes the nested decision tag.

Replacement, deletion, undo, database detachment and replica partition reset
carry the graph's physical retirements with their existing root/epoch decision.
Unchanged groups are not retired. RDB capture retains the historical view and
one admitted, deduplicated list of exact block pins; release uses that same
list without allocating. Each worker exposes at most one opaque collection
stream token, returning one decoded group at a time with a sequential cursor.
The immutable view and pins remain owned until explicit stream completion or
session cancellation; cancellation waits for admitted page reads before
destroying their metadata. Empty prefix-routing pages still advance the
cursor. The RDB encoder borrows page strings and emits bounded file fragments
under a complete-key output lease, preventing interleaving between workers
without materializing a second full collection or serialized page.
Decoded transfer pages reserve retention before I/O from physical payload
length and checked entry counts. A move-only page owns its resulting capacity
charge through output backpressure, independently of token cancellation;
buffers are destroyed before their allowance is returned. Admission failure
cancels the snapshot and cannot replace a previous successful dump.
Shutdown checkpoints decline grouped populations and leave ordinary durable
recovery available.

RDB collection export and import do not require a single aggregate compact
value. File import, Redis-PSYNC RDB population and RESTORE consume admitted
logical pages through the same atomic collection-ingest contract. Ordinary
collection encodings yield bounded pages; quicklists consume one encoded node
at a time. Packed-node decoding first validates and measures its entries
without allocating their vectors, then admits the decoded node and validation
scratch. Individual strings, including packed nodes, retain their size bound.

Startup RDB import waits for queued grouped transaction commits to finish
before marking the imported dataset as saved or admitting clients. This keeps
asynchronous grouped String restores within the imported persistence baseline.

File import owns an open descriptor and bounded read scratch for checksum
validation and object decoding. Saved input positions can be reread without
retaining prior file buffers. The source file remains immutable until import
finishes; RESTORE borrows its retained request payload instead.

The complete validation pass precedes destructive file replacement or changing
a RESTORE destination. Duplicate Hash fields and Set/Sorted Set members are
checked across pages using admitted digest-to-input-position metadata; digest
collisions compare the original decoded identity rather than equating hashes.
The application pass rewinds the immutable input and feeds pages on the key
owner, preserving page charges until consumption. EOF and optional declared
cardinality are part of the atomic commit contract; quicklist node counts are
not mistaken for element counts. RESTORE retains the ordinary RESP request
size limits. Stream RDB ordering and PEL-owner joins use bounded temporary-file
reordering; see [Streams](12-streams.md). Integrations explicitly requesting a
whole compact image retain their materialization limits.

COPY and RENAME retain key intents and transfer an immutable, exactly pinned
grouped source through a single-pass page reader, rather than a whole-value
buffer. The source owner releases both its physical pins and metadata; that
lifetime remains registered with shutdown even while another worker owns the
reader. Population changes invalidate the reader, while deleting the locked
source does not invalidate its captured graph.

Ordinary collection ingestion uses one command-local auxiliary decision across
all input pages, with a separate revision for each intermediate root. Inside
EXEC/Lua it preserves the preceding command journal and waits for the shared
batch decision to become durable only after complete input validation. Failure
leaves that batch uncommitted. Compensating a grouped predecessor publishes a
newer root revision pointing to its original incarnation and pages; restoring
the old revision could lose recovery winner selection. Retirement settlement
preserves the restored pages and earlier commands' receipts, so a later outer
commit cannot resurrect failed input or reclaim the restored graph.

## Source map

Grouped storage remains part of the storage engine, with implementation units
under `src/storage/engine/grouped/` and internal model/codec headers under
`include/lavik/storage/detail/grouped/`. It shares the engine's `Impl`, append
and recovery services; the directory is not an independent public engine API.
Its unit, command, recovery and fault tests live under `tests/grouped/`.

| Responsibility | Source |
|---|---|
| Prefix snapshots, mutation planning and persistent routing | `include/lavik/storage/detail/grouped/hash.h`, `src/storage/engine/grouped/hash.cpp` |
| Logical collection encodings and per-element validation | `include/lavik/storage/detail/hash_codec.h`, `ordered_compact_codec.h`; `src/storage/engine/hash_codec.cpp`, `ordered_compact_codec.cpp`, `list_tree.cpp`, `src/redis/zset_command.cpp` |
| Bounded Hash/Set random reads and deterministic sparse Set pops | `src/storage/engine/grouped/hash_random.cpp`, `hash_tree.cpp` |
| Sparse object index, group locations and immutable metadata ownership | `include/lavik/storage/detail/grouped/object_index.h`, `src/storage/engine/grouped/object_index.cpp` |
| Physical reads, incremental publication, extent streaming and commit dependencies | `src/storage/engine/grouped/read.cpp`, `src/storage/engine/grouped/write.cpp`, `src/storage/engine/grouped/mutation.cpp`, `write.cpp`; `include/lavik/storage/detail/record_payload_cursor.h`, `include/lavik/storage/detail/grouped/commit.h` |
| Root-only expiration/persistence publication | `src/storage/engine/grouped/metadata.cpp`, `src/storage/engine/grouped/object_index.cpp`, `write.cpp` |
| Fixed String segments and direct byte-range operations | `src/storage/engine/grouped/string.cpp`, `src/redis/string_command.cpp` |
| Foreground scratch admission and pinned key transfers | `include/lavik/storage/detail/grouped/scratch.h`, `src/storage/engine/transfer_api.cpp`, `src/storage/engine/grouped/restore.cpp` |
| Atomic ordinary collection ingestion and command-local compensation | `src/storage/engine/collection_ingest.cpp`, `src/storage/engine/grouped/restore.cpp`, `write.cpp`; `include/lavik/storage/detail/replica_collection_stage.h` |
| Admitted snapshot tokens, sequential page reads and whole-key RDB output ownership | `include/lavik/storage/collection_page.h`, `src/storage/engine/backup.cpp`, `include/lavik/rdb_collection.h`, `src/redis/rdb_collection.cpp`, `src/redis/backup.cpp` |
| Incremental RDB collection parsing, preflight validation and import consumers | `include/lavik/rdb.h`, `src/redis/rdb.cpp`, `src/redis/rdb_import.cpp`, `src/redis/server.cpp`, `src/redis/command.cpp`, `src/replication/replication.cpp` |
| Native compact wire streaming and transactional grouped ingestion | `include/lavik/storage/detail/collection_compact_stream.h`, `replica_collection_stage.h`; `src/storage/engine/collection_compact_stream.cpp`, `src/storage/engine/grouped/replication_source.cpp`, `replica_collection.cpp`, `replication.cpp` |
| Ordered pages, rank routing and collection command adapters | `include/lavik/storage/sorted_set.h`; `include/lavik/storage/detail/grouped/collection.h`, `include/lavik/storage/detail/grouped/sorted_rewrite.h`; `src/storage/engine/grouped/collection.cpp`, `src/storage/engine/grouped/sorted_rewrite.cpp`, `src/storage/engine/grouped/ordered_io.cpp`, `src/storage/engine/grouped/ordered_mutation.cpp`, `src/storage/engine/grouped/list.cpp`, `src/storage/engine/grouped/zset.cpp`, `sorted_set_api.cpp`, `list_tree.cpp`, `compact_api.cpp` |
| Sorted Set member-prefix reads and atomic dual-index planning | `src/storage/engine/grouped/zset.cpp`, `src/storage/engine/grouped/zset_members.cpp`, `src/storage/engine/grouped/ordered_mutation.cpp` |
| Retirement, undo and database detach | `src/storage/engine/grouped/lifecycle.cpp`, `write.cpp`, `flush_db.cpp`, `replication.cpp` |
| Recovery, GC and historical snapshots | `src/storage/engine/recovery.cpp`, `init.cpp`, `defrag.cpp`, `backup.cpp` |
