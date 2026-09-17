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
Redis keyspace or a separate database. Hash, Set, List and Sorted Set support
compact values and complete, independently addressed group snapshots. Writes
automatically promote compact collections at the encoded-size threshold; the
grouped representation remains in use until the key is deleted or replaced.
Streaming collection imports construct grouped graphs directly. Both ordinary
and Debug builds use the same read, mutation, recovery and maintenance adapters.

Hash/Set use a persisted-seed prefix directory; List uses an ordered-page
directory. Newly built Sorted Sets combine ordered `(score, member)` pages
with a prefix directory mapping each member to its score. Both directories
belong to one object and share its physical index and transaction lifecycle.
A prefix directory alone does not supply rank or score/member ordering.

## Identity and ownership

The top-level `RecordIndex` still owns each user key, type, expiry and logical
version. A grouped marker directs collection lookups to that
partition/database's sparse `ScanHashMap` object index. Its immutable view holds
a routing directory, one compact `RecordIndex` entry per group, and separately
owned extent manifests. It does not retain field names or values.

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
maximum scores, but no member boundaries. Recovery checks the complete chain,
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
sequence. Such a root inherits the preceding durable decision before an
independent transaction can publish it, and snapshot/undo views retain their
own historical root expiry. Immediate expiration uses the ordinary
tombstone/graph retirement path.

Views share unchanged physical index pages across mutations. Hash routing
nodes are persistent; the ordered rank directory owns admitted metadata
vectors and reconstructs them for page-content or topology changes. Routing
and physical-index node references, including final destruction, remain on
the key owner. Cross-worker readers exchange physical identities or stream
handles that route metadata access and cleanup back to that owner. Retained
directories, index pages, manifests, publication reservations, retirement
receipts and snapshot pin lists participate in memory admission and accounting.
Admission failures before root publication preserve the previous logical view.
A handle retains metadata only: physical coordinates and allocation epochs are
captured before suspension, and snapshot readers additionally pin the complete
captured graph.

View population generations are scoped to one partition and logical database.
Detaching that population advances its generation; resetting another partition
or promoting an already-built candidate does not invalidate its retained
views. The broader worker index generation remains an independent guard for
ordinary suspended storage operations.

## Durable graph and publication

A small keyed root and auxiliary keyed group records preserve the user-visible
collection type. Auxiliary identity in the checked outer header includes
incarnation, group identity, field count, retirement state and a nested batch
decision when present. Auxiliary records never enter the user-key winner merge
or Redis key/expiry counts.

The version-1 ordered-root payload has two checked shapes: 72 bytes describe
only the ordered graph; 136 bytes append the 64-byte Hash root for an indexed
Sorted Set. A member-index presence flag must agree with the payload length,
so a truncated indexed root cannot decode as an ordered-only root.
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

Sorted Set mutation planning derives member-index changes from complete
ordered before/after pages, including full-image callback and import paths.
Changed ordered pages, prefix snapshots and split retirements share the same
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
handoff, not just record staging. A failed queue allocation does not create a
pending-commit count without an owned receipt.
An explicitly failed decision also makes its published graph unreadable,
including collection length and checked Redis key metadata. An uncommitted
expiry cannot disguise that failure as an absent key. Internal compensation
alone may inspect the failed physical view to restore or retire it; snapshot
page readers independently recheck the decision after storage waits.

An incremental successor from another transaction waits for its predecessor's
durable decision before inheriting untouched groups. Same-transaction commands
can reuse their view. This causal dependency is separate from the physical
pins that keep prior transaction generations recoverable until retirement.

Before acquiring a standalone transaction's generation lease, grouped writes
sample foreground space and coordinate old-generation cleaning under pressure.
The predecessor decision is durable before that cleaning can run. Borrowed
EXEC/Lua transactions do not wait for their own still-active generation to
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

Hash/Set point operations load each affected prefix group once. Their writes
replace only changed complete groups and any split-parent retirement records.
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

List length uses root metadata. Indexed/range reads load the corresponding
rank pages; push, pop and indexed replacement load the affected interval and
its immediate link neighbours. Pivot and position searches consume one page at
a time and retain only the result; insertion reloads the located interval.
Value removals, trimming and within-list moves retain the needed logical
contents before forming a replacement interval. Only changed snapshots enter
the writer, and admitted reply buffers retain their charge across owner hops.

Sorted Set operations use a typed storage interface. Cardinality reads root
metadata. Indexed score lookups read only the selected member-prefix pages;
legacy score lookups and member ranks scan admitted ordered pages. Rank ranges
start at the directory's selected pages; score ranges and score counts first
seek their candidate interval using resident score bounds, then read matching
pages in physical order. Range replies retain only admitted output members.
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
the requested output rather than all members. Range-removal, random and other GEO commands retain
the full-logical-value callback with aggregate memory admission and
destination-buffer limits. Their physical rewrite planner still restricts
writes to changed pages and structural neighbours.

## Recovery, reclamation and snapshots

Cold scanning gathers roots and auxiliary candidates separately. After
transaction adjudication and root selection, recovery reconstructs each
winning incarnation, including retained parent markers, and checks routing
coverage and aggregate counts. Only reachable external group payloads are
validated: an obsolete inline-key group's value extents may already have been
reclaimed while its records block is still scannable. External parent-key
extents instead remain source-block dependencies because classification still
requires the full key. Every live group, root and extent joins physical-owner
accounting before orphan reclamation.

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
size limits, and String/Stream or other integrations that explicitly request a
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

Internal storage model and codec headers live under
`include/lavik/storage/detail/`; they are not an independent public engine
API. Their implementation units remain under `src/storage/engine/`.

| Responsibility | Source |
|---|---|
| Prefix snapshots, mutation planning and persistent routing | `include/lavik/storage/detail/grouped_hash.h`, `src/storage/engine/grouped_hash.cpp` |
| Logical collection encodings and per-element validation | `include/lavik/storage/detail/hash_codec.h`, `ordered_compact_codec.h`; `src/storage/engine/hash_codec.cpp`, `ordered_compact_codec.cpp`, `list_tree.cpp`, `src/redis/zset_command.cpp` |
| Bounded Hash/Set random reads and deterministic sparse Set pops | `src/storage/engine/grouped_hash_random.cpp`, `hash_tree.cpp` |
| Sparse object index, group locations and immutable metadata ownership | `include/lavik/storage/detail/grouped_object_index.h`, `src/storage/engine/grouped_object_index.cpp` |
| Physical reads, incremental publication, extent streaming and commit dependencies | `src/storage/engine/grouped_read.cpp`, `grouped_write.cpp`, `grouped_mutation.cpp`, `write.cpp`; `include/lavik/storage/detail/record_payload_cursor.h`, `grouped_commit.h` |
| Root-only expiration/persistence publication | `src/storage/engine/grouped_metadata.cpp`, `grouped_object_index.cpp`, `write.cpp` |
| Foreground scratch admission and pinned key transfers | `include/lavik/storage/detail/grouped_scratch.h`, `src/storage/engine/transfer_api.cpp`, `grouped_restore.cpp` |
| Atomic ordinary collection ingestion and command-local compensation | `src/storage/engine/collection_ingest.cpp`, `grouped_restore.cpp`, `write.cpp`; `include/lavik/storage/detail/replica_collection_stage.h` |
| Admitted snapshot tokens, sequential page reads and whole-key RDB output ownership | `include/lavik/storage/collection_page.h`, `src/storage/engine/backup.cpp`, `include/lavik/rdb_collection.h`, `src/redis/rdb_collection.cpp`, `src/redis/backup.cpp` |
| Incremental RDB collection parsing, preflight validation and import consumers | `include/lavik/rdb.h`, `src/redis/rdb.cpp`, `src/redis/rdb_import.cpp`, `src/redis/server.cpp`, `src/redis/command.cpp`, `src/replication/replication.cpp` |
| Native compact wire streaming and transactional grouped ingestion | `include/lavik/storage/detail/collection_compact_stream.h`, `replica_collection_stage.h`; `src/storage/engine/collection_compact_stream.cpp`, `grouped_replication_source.cpp`, `replica_collection.cpp`, `replication.cpp` |
| Ordered pages, rank routing and collection command adapters | `include/lavik/storage/sorted_set.h`; `include/lavik/storage/detail/grouped_collection.h`, `grouped_sorted_rewrite.h`; `src/storage/engine/grouped_collection.cpp`, `grouped_sorted_rewrite.cpp`, `grouped_ordered_io.cpp`, `grouped_ordered_mutation.cpp`, `grouped_list.cpp`, `grouped_zset.cpp`, `sorted_set_api.cpp`, `list_tree.cpp`, `compact_api.cpp` |
| Sorted Set member-prefix reads and atomic dual-index planning | `src/storage/engine/grouped_zset.cpp`, `grouped_zset_members.cpp`, `grouped_ordered_mutation.cpp` |
| Retirement, undo and database detach | `src/storage/engine/grouped_lifecycle.cpp`, `write.cpp`, `flush_db.cpp`, `replication.cpp` |
| Recovery, GC and historical snapshots | `src/storage/engine/recovery.cpp`, `init.cpp`, `defrag.cpp`, `backup.cpp` |
