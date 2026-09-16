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

# Storage and recovery

## Responsibility and boundary

The storage subsystem owns Lavik's in-memory top-level key indexes and its
primary durable representation. It routes keys to worker-owned logical
partitions, appends immutable record versions, serves staged or disk-backed
reads, reconstructs indexes and physical accounting at startup, and reclaims
obsolete records and extents online.

`StorageEngine` is the public boundary. In addition to process lifecycle and
ordinary String operations, it exposes typed List, Hash, Set, Sorted Set, and
Stream operations, pre-locked transaction variants, logical-database epoch
changes, snapshot and full-sync support, replica partition resets, and
maintenance and durability statistics. The command layer owns Redis semantics
and request admission; the transaction subsystem owns key arbitration; and the
replication subsystem owns role and session lifecycle. Storage supplies the
durable records and epochs those modules act on.

The top-level key indexes are runtime authority. An optional clean-shutdown
checkpoint serializes them as a one-shot recovery accelerator, but committed
records remain the durable source of truth and recovery falls back to scanning
them whenever a checkpoint is absent or invalid. Collections have compact
complete-value encodings. Hash, Set, List and Sorted Set also have independently
addressable complete group snapshots, selected through a sparse object side index when the
top-level index marks a grouped representation. Their serving, transaction,
recovery and graph-lifecycle boundaries are described in
[Grouped collections](09-grouped-collections.md). Collection writes promote
automatically at the compact-size threshold, while streaming imports construct
grouped graphs directly. Hash/Set use prefix routing; List uses ordered pages.
Newly built Sorted Sets combine ordered pages with a member-to-score prefix
index under one atomic root; legacy ordered-only roots remain supported.
Explicit full-image callbacks retain aggregate
materialization limits; grouped key transfers and collection snapshot streams
instead consume admitted pages.

## Ownership and runtime state

The startup storage selection controls buffer allocation, I/O submission and
device eligibility together. It is fixed before preparation and cannot change
while buffers or workers exist. An executable with SPDK support can also use
io_uring storage; kernel paths require io_uring and `spdk://` paths require SPDK.
The selection does not alter the durable format.

Storage separates three ownership domains:

- A logical partition is one of the 16,384 Redis hash slots. Its current key
  owner is `partition_id % worker_count`; that worker owns the partition's 16
  logical-database indexes, mutation sequence, replication epoch, counts, and
  snapshot state.
- A physical block has one current runtime owner. When the worker topology
  matches the topology recorded in its header, recovery retains the original
  writer if that worker can access the device. Otherwise recovery hashes the
  block ID and allocation epoch across the eligible workers. All workers are
  eligible on io_uring; SPDK eligibility is restricted to workers holding a
  qpair for the block's physical controller.
- Each device has one allocator owner, which serializes that device's ready and
  cold-free pools, scan bitmap, fixed metadata page generations, epoch mirrors,
  and allocation-epoch counter. io_uring initially assigns this role as
  `device_index % worker_count`. SPDK selects it from the controller's qpair
  owners, so metadata and allocation I/O never route to a worker that cannot
  open the namespace.

Every worker has an ordinary append stream and may have one append stream for
each live transaction generation. Physical streams are per worker, not per
logical partition, so active 8 MiB staging buffers scale with workers and live
transaction generations rather than with 16,384 slots. A worker also owns the
`BlockState` objects assigned to it, including committed and live byte counts,
pins, staging identity, flush state, and defrag state. The runtime owner and
allocation epoch form immutable identity after publication and are the only
fields a key-index owner may read directly. The owner is published atomically
after epoch initialization, so an acquire owner read makes the immutable epoch
visible without an extra worker hop. Other state remains owner-local and other
workers use Bycorf cross-core submissions to read or mutate it.

Append-stream creation and rollover are single-flight within each stream. A
writer may release owner-local store state while physical allocation waits,
but it revalidates the current stream before publication and returns any
unused reservation to the device allocator. This prevents duplicate stream
publication without serializing independent transaction generations. Staging
buffers remain tied to active streams, and shutdown or generation retirement
waits for outstanding allocation work before destroying its state.

Logical key locks come from the transaction subsystem. Storage's pre-locked
interfaces require the caller to run on the key owner with the correct shared
or exclusive lock. Background expiry, relocation, snapshot, and replica work
participates in that same arbitration boundary.

## Persistence backends and physical layout

Active storage paths are existing regular files, Linux raw block devices, or
Bycorf SPDK storage paths. `Prepare` probes and validates them; it does not
create, extend, truncate, or preallocate a missing regular file. On the POSIX
io_uring backend, every worker opens the complete configured path table for
direct I/O; registered buffers are used when registration succeeds and the
same aligned memory with plain asynchronous I/O is the fallback. On SPDK, a
worker opens only namespaces whose physical controller assigned it a qpair.
SPDK also requires DMA buffer registration and fails initialization when that
registration or controller-qpair coverage is insufficient.

Every device has a persistent identity and a capacity-derived metadata prefix:

```text
offset 0
  4 KiB device label
  epoch page 0 slot A, epoch page 0 slot B
  ...
  system-state root slot A, system-state root slot B
  allocation-bitmap page 0 slot A, page 0 slot B
  ...
  checkpoint-bitmap page 0 slot A, page 0 slot B
  ...
  round up to the next 8 MiB boundary
  data block at local ID DataBlockBegin(capacity)
  next data block
  ...
```

The checksummed device label contains the storage-set ID, immutable device ID,
persisted capacity, member count, format version, and block size. Block IDs
encode the device ID above a 27-bit device-local block number, limiting one
device to 2^27 8 MiB blocks, or 1 PiB. Startup requires the complete initialized
member set, rejects duplicate or foreign devices, preserves labeled capacity
when a regular file has grown, and rejects a backing object that has shrunk.

Epoch metadata contains 16 database epochs, 16,384 partition replication
epochs, and the published and consumed checkpoint generations plus the
expected checkpoint block and entry counts. The allocation bitmap and the
independent checkpoint-discovery bitmap each have one bit per physical block.
Each logical metadata page has independent 4 KiB A/B slots with a generation
and CRC32C checksum. Readers select the valid higher generation. An all-zero
pair is uninitialized logical zero; a nonzero pair with no valid slot is
corruption.

The system-state root is a separate, process-global A/B pointer at a fixed,
capacity-independent offset before the bitmap ranges and mirrored on every
configured device. It names one copy-on-write manifest extent containing
the complete Function-catalog extent list, local catalog generation and CRC64,
full-sync readiness and population state, and the latest promotion base.
Worker zero is the only manifest writer. A generation is recoverable only when
the same valid root exists on every device; recovery chooses the highest such
generation, so a torn multi-device update falls back to the previous common
state rather than combining fields from different commits.

Every data block is 8 MiB:

```text
4 KiB block-header slot A
4 KiB block-header slot B
records or extent payload
```

The alternating header slots record block identity, writer topology,
allocation epoch, committed boundary, record count, maximum physical LSN,
header sequence, kind, and kind-specific metadata. Slot selection prefers the
higher allocation epoch and then the higher header sequence. Current block
kinds are ordinary records, payload extents, transaction generations,
and checkpoint index chunks; on-disk enum value 3 remains deliberately
unassigned.

Records are 8-byte aligned and carry their database and value type, key
representation, logical and physical sizes, transaction ID, database and
replication epochs, logical mutation sequence, absolute expiration time,
physical LSN, allocation epoch, and payload and header checksums. A key digest
is deliberately absent from authoritative ordinary records: cold recovery
reconstructs it from the complete key under a fresh random seed. An optional
shutdown checkpoint can persist both its snapshot digests and their seed as
non-authoritative acceleration state. Record kinds are value, tombstone, and
keyless transaction commit decision. Both the storage write boundary and
recovery decoder enforce the durable 512 MiB maximum key length; a wider
internal or replication protocol argument limit cannot create a record that a
restart would reject.

The version-1 record wire layout has a 72-byte base header at explicit byte
offsets. A nonzero transaction ID and expiration timestamp each add one aligned
8-byte extension, so ordinary fixed metadata is 72, 80, or 88 bytes. The base packs
record kind, database, value type, external-payload state, external-key state,
grouped-root and auxiliary-group markers, and extension presence into one
16-bit word. An auxiliary group carries an additional 32-byte checked
identity containing its object incarnation, routing identity, retirement bit
and optional nested-command transaction decision.
Its identity is available without reading an external value; group payloads
retain complete snapshots, not read-time mutation logs. The group payload
envelope leaves framing space within the 1 GiB record-payload limit while
preserving the independent 512 MiB limit for each field and value. Hash and Set
use persisted-seed hash prefixes; List uses stable ordered page identities.
Indexed Sorted Sets use both disjoint identity spaces under their collection
type. The version-1 ordered-root payload has a checked presence flag for the
optional appended member Hash root. Roots preserve an independent
group revision, distinct from the source command sequence shared by mutations
in a replay envelope.
Header length is derived from
those flags and key length; total record length is derived from header and
payload length. Neither derived length is stored. The decoded `RecordHeader`
is a runtime view rather than a persisted C++ object representation.

The compact Stream payload uses the current `LXS1` layout, including persisted
macro-node entry counts that preserve approximate-trim boundaries across
restart and RDB export/import. It does not reconstruct those boundaries from
current settings or accept earlier development layouts without node counts.
Lavik-owned storage schemas remain at v1 while unreleased; incompatible
development media is recreated, not migrated. Redis RDB versions follow the
external Redis format independently.

The current version-1 format also includes checkpoint metadata and the
system-state root and manifest. During pre-deployment development this layout
directly replaces earlier layouts that also used version 1; there is no
compatibility decoder. Older media, including the earlier 104-byte record
layout, must be reset before this build starts.

Keys that do not fit the configured inline header limit move into the payload.
Large key/value payloads use a root record containing an extent manifest. Each
extent reference identifies a dedicated extent block by block ID, allocation
epoch, byte count, and payload checksum. The extent block header repeats its
index, length, and checksum so reads and recovery can validate the complete
root-to-child identity.

## Startup and recovery

### Preparation

`Prepare` runs before Bycorf workers start. It validates the worker count,
inline-key limit, flush alignment, per-device defrag concurrency, configured
paths, capacities, and membership. A fresh regular file must be an 8 MiB
multiple. A raw device uses its complete 8 MiB blocks and ignores a shorter
tail. With the current fixed metadata and eight-block per-device defrag reserve,
each device needs at least 80 MiB. A fresh set with no system-state root denotes
the canonical empty Function catalog and consumes no foreground block merely
to record that absence. Catalog bodies and manifests use the ordinary
foreground allocator; insufficient capacity rejects the state-changing
operation without replacing the current durable or runtime catalog.

Fresh paths receive a storage-set label. Startup can add zero-label devices to
a complete initialized set: it derives canonical epochs from existing members,
initializes the added devices' fixed metadata, publishes all new labels, and
only then advances existing labels' member counts. Interrupted expansion is
recognizable and retryable when the same complete set is supplied again.

An explicit reset validates every target before writing and zeroes the fixed
metadata prefix through the first data-block boundary. This is a logical reset,
not a secure erase of the data region. The implementation does not establish a
crash-safe stale-media sanitization guarantee for reset media or a reused
zero-label added device, so this architecture does not make one.

After labels are resolved, preparation loads the newest valid epoch and bitmap
page on every device. Epochs only increase, so the runtime vector is the
component-wise maximum of all device copies; a later mirrored update also
repairs stale fields on a lagging member. It separately selects the highest
system-state root common to all members, reads and validates its manifest and
Function dump, and records whether an interrupted full sync requires startup
to remain fenced.

If fixed metadata names an unconsumed checkpoint, worker 0 first advances its
consumed generation on every device. All recovery workers then walk disjoint
topology-aware stripes of the checkpoint bitmap. A small prefix read first
classifies matching chunks without reading every 8 MiB body. The exact
per-partition, per-database capacities are then validated against the root and
used to allocate each owner-local index's final power-of-two bucket table.
Only after every owner has finished that allocation do scanners double-buffer
the index and accounting bodies. The prefix directory redistributes those
blocks to their durable shard first, so the index owner performs both body I/O
and installation without a cross-worker decoded batch. After validating the
body checksum, each individually validated entry is installed directly from
its pinned I/O buffer. The entry carries its validated logical partition and
runtime digest, so key bytes have no intermediate owning copy and recovery
does not recalculate Redis slots or key digests. A later invalid entry sends
the installed valid prefix
through the ordinary cold-scan merge. io_uring
owners can open every configured path; under SPDK the unchanged worker
topology must also give the shard owner a qpair for the block's controller.
This removes incremental index rehashing, cross-worker installation, decoded
entry batches, and per-key routing hashes from checkpoint recovery while
retaining worker-bounded temporary I/O memory.
Barriers reduce
per-worker block, entry, capacity, and shard totals and verify each loaded
index against its declared size. The root's expected counts make missing
bitmap bits disable the fast path; stale bits are ignored unless their block
header names the
selected generation. Once the complete checkpoint is resident, its discovery
bitmap is zeroed, each scanner returns its validated blocks through the
cold-free lifecycle, and another barrier precedes ordinary recovery. A missing
block, generation or topology mismatch, invalid bound, checksum failure, or
capacity mismatch disables ordinary-body skipping and retains the full record
scan. Entries from an already installed valid checkpoint prefix merge with
that scan rather than requiring an unbounded rollback buffer. Startup also
zeroes stale checkpoint bitmap state when no checkpoint is usable.

### Per-worker initialization and teardown

`InitializeWorker` creates the worker's aligned buffer pool, registers the
fixed-file table, and opens the paths accessible on that backend. Before worker
startup, io_uring assigns weighted home devices for foreground allocation.
SPDK groups namespaces by physical controller, weights controllers by usable
foreground blocks, and distributes available controller qpairs across workers;
startup fails unless every controller and every worker can be covered. Worker
barriers then coordinate one parallel recovery rather than independent
worker-local boots. Request readiness follows successful completion of the
complete recovery and allocator-cleanup sequence; the exact moment a listening
socket exists is not the readiness boundary.

After worker recovery, the Function catalog decodes the selected dump and
stages it independently on every worker before Redis readiness. A missing root
on a fresh set is the canonical empty catalog and needs no durable allocation.
An existing root with a bad manifest, extent identity, checksum, dump, or
inconsistent compile result fails startup; recovery never substitutes an empty
catalog for corrupt durable state.

Recovery proceeds as follows:

1. Workers divide physical scan work across the configured devices. io_uring
   stripes every device across all workers. SPDK scans each namespace only on
   that controller's qpair owners and strides the namespace across those
   owners. A clear allocation bit is authoritative and skips block I/O. A set
   bit leads to a header read.
2. An all-zero or wholly invalid header on an allocated block is a permitted
   activation false positive and becomes reusable. A valid header whose
   embedded block ID does not match its physical location is stale media and
   is ignored without rewriting the bitmap.
3. A valid extent header contributes extent identity. With a validated
   checkpoint, an ordinary record block contributes only its header because
   the checkpoint already supplies its winning index entries. Without one,
   ordinary record blocks are read through their committed boundary.
   Transaction blocks are always read through their committed boundary; zero
   page padding is skipped, while record bounds, allocation epochs, topology,
   keys, and checksums are validated. Recovery computes each winning key's
   runtime digest from the recovered complete key instead of loading one from
   disk.
   Recovered records are routed to key owners in byte-targeted batches. The
   process-wide target is 64 MiB divided across active scan workers, rather
   than an item limit repeated independently by every worker; one indivisible
   record may exceed its worker target and is flushed before another record is
   retained.
4. Records from obsolete database or partition replication epochs are ignored.
   Commit decisions are collected independently of those keyed-record filters.
   Tagged records remain parked until every worker has contributed to the
   global committed-transaction set.
5. Each key owner chooses the highest mutation sequence. Equal sequences are
   physical relocation copies of one logical version, so the higher physical
   LSN wins. Recovery also rebuilds whether the winner shields an older,
   potentially live value.
6. A second cross-worker pass walks each in-memory winner index once and
   charges every winning root and referenced extent exactly once to its
   physical block owner. A resumable stable cursor pauses at the same
   process-wide byte target divided across workers, applies the per-owner
   batches, and then continues at the next entry; it neither rescans storage
   nor restarts an index scan. One external value's manifest remains an
   indivisible unit. The owner also verifies every live manifest against the
   recovered extent headers before the batch is released.
7. Recovery reconstructs ready and cold-free allocator state, reclaims orphan
   extents before it needs new space, and handles expired winners. Expired
   versions participate in winner selection first, then normally receive a
   durable tombstone so an older value cannot reappear after a clock rollback.
8. Recovered blocks remain sealed. Each worker's next physical LSN and the
   global transaction ID counter are seeded above all durable values before
   periodic flush, active expiration, transaction cleaning, and optional Tomb
   Raider work begin.

Changing the worker count does not rewrite data during startup. Old physical
blocks may temporarily be remote from their keys; foreground replacement and
background defrag gradually move live records to current key owners.

After the runtime has torn down a worker's I/O and coroutine frames, the server
calls `StorageEngine::FinalizeWorker` on that worker's native thread. It
destroys the complete `WorkerStore`, including worker-affine `ScanHashMap`
state, before engine-wide storage objects are released.

## Runtime read, write, and flush flows

### Append and index publication

Logical mutations funnel through `AppendLocked`. The partition's mutation
sequence advances, external payloads are prepared if required, and a record is
prepared for the current worker's ordinary or transaction-generation staging
block. Client writes may carry a transport-neutral `MutationPrecondition`;
after every potentially suspending lock, read, snapshot, extent, and block-
allocation step, `WriteRecordLocked` validates it synchronously immediately
before WATCH invalidation and staging/index publication. Background work,
replica replay, and rollback do not depend on client authority. Once admitted,
the record is appended and the in-memory index is updated immediately; it may
point at staged bytes that have not crossed a crash-durability boundary. Staged
reads use that buffer directly.

Opted-in single-key writes to existing small inline Hash, Set, List and Sorted
Set records retain exclusive key intent but release worker store state while
reading and preparing the replacement. Publication reacquires store state and
validates the source's logical version and population; same-version GC relocation
is allowed, and append resolves the current physical predecessor for retirement.
The command's database admission remains held through publication. This applies to HSET/HMSET,
SADD/SREM, ordinary single-key List writes, and ZADD/ZINCRBY/ZREM. Successful
no-ops and transitions to empty or grouped values also validate before returning
or publishing. Multi-key operations, transactions, Stream writes,
native candidate/loading paths and compact external keys/values retain
their separate locking contracts. Ordinary online replay can use the same
guarded single-key paths. Ordinary String GET does not acquire the store-state
mutex; String SET still acquires it for append-state mutation.

Ordinary single-key Hash/Set insertion, List push and opted-in Sorted Set
add/increment also prepare new collections outside worker store state. The
command retains exclusive key intent even for an absent key, together with
database admission. Preparation owns admitted private data, including initial
group pages and both Sorted Set indexes when the new value is large. Publication
revalidates population, writer/authority state and logical absence at the
original command time; expired or tombstone predecessors may have moved or
been reclaimed. Fresh graph incarnations are assigned together under store
state only after durable batch admission. Preparation allocation failure
publishes no key.
Creation within transactions, multi-key and native candidate/loading adapters
retains their separate preparation contracts.

Existing grouped Hash/Set point writes and typed List/Sorted Set writes also
prepare private pages without worker store state, including under retained
EXEC/Lua key intents. They carry immutable predecessor metadata and admitted
page ownership through preparation, then reacquire state and validate population
and logical identity before any publication or successful no-op. Sorted Set
preparation covers both the ordered and member graphs. GC-only physical moves
are refreshed, not treated as conflicting logical writes. Random Set pops,
full-image callbacks, promotion and candidate ingestion retain their separate
preparation contracts; all paths share the same atomic grouped writer.

Runtime indexes retain key identity, logical version, record coordinates, and
the state needed to serve the current value. The physical block's owner and
allocation epoch live once in `BlockState` rather than being repeated for
every key. Before a location crosses an ownership boundary or a suspension,
the key owner reads that published immutable block identity and materializes a
self-contained `RecordLocation`. The physical owner validates the snapshot on
use, preserving block-reuse and ABA protection. These index representations
are runtime-only; durable block and record headers retain the fields needed for
restart validation.

Runtime key digests use one operating-system-seeded SipHash key shared by all
workers and do not affect Redis-slot routing. A cold recovery chooses a fresh
seed; a successful shutdown-checkpoint recovery restores the checkpoint seed
before hashing any key so its serialized top-level digests remain valid.
External keys and decoded Hash or Set fields retain or reconstruct a digest
only as a lookup aid, and collisions are verified against complete keys. The
seed and top-level digests appear only in the one-use checkpoint, never in
authoritative records or replication protocols.

All partition and logical-database indexes on a worker allocate entries from a
shared worker-local arena, so sparse indexes share capacity instead of
stranding it at partition boundaries. A population detached by `FLUSHDB`, a
replica reset, or a replica abort retains ownership of that arena until
reclamation finishes. Replica promotion and abort reuse the same worker-local
detached-index drain as synchronous database flushes, so a completed lifecycle
cannot leave old index arenas or record-block live-byte accounting queued for a
later rebuild. The drain covers the worker FIFO; it is not partitioned by
replication attempt.

Ordinary online growth remains incremental. Shutdown-checkpoint recovery knows
each final population exactly and allocates its settled bucket table before
installing entries, avoiding intermediate rehash tables without changing the
steady-state load factor or representation.
Optional per-key state such as expiration may change an entry's concrete
representation. Those replacements are owner-serialized, and staged flush,
coroutine, and transaction-undo state revalidates or retargets its saved entry
identity before use rather than relying on an object's former lifetime.

Index growth is failure-atomic. Before appending a record whose publication
needs a new or replacement entry, storage admits the required entry and table
capacity from the current worker's memory share. Incremental expansion likewise
prepares its destination capacity before removing source entries. Admission or
index-capacity failure returns `ResourceExhausted` without publishing a record
that cannot enter the index; an admission rejection during expansion leaves
both tables searchable and the maintenance step retryable. Lavik does not
turn a physical allocator failure into a command or recovery status: an actual
`std::bad_alloc` remains unhandled and terminates the process. This keeps
recoverable capacity policy separate from a process that can no longer uphold
its in-memory invariants.

`--max-memory` is divided into fixed worker shares; a worker does not borrow
another worker's unused balance. Retained state is admitted up to 90 percent of
each share. Its accounting ownership remains bound to the allocation's origin,
so destruction credits the same worker even when it occurs elsewhere. INFO and
metrics aggregate those worker-owned counters without changing foreground
ownership.

Ordinary client request buffers use a separate quota, five percent by default,
which `maxmemory-clients` can express as a percentage or absolute size or
disable. Bounded request-time scratch is not admitted allocation by allocation;
protocol and object-size limits bound untrusted inputs, and safe failure paths
report `ResourceExhausted`. Consequently `--max-memory` bounds accumulating
retained state rather than acting as a strict RSS ceiling, while the remaining
headroom absorbs allocator, request, and I/O peaks.

Full-sync coverage scales with the key set and lifetime of a session, so it
pre-reserves reusable credit within the retained-memory boundary. Redis-
compatible RDB snapshots similarly reserve capacity before retaining dirty-key
identities. If snapshot capture cannot be admitted, only that snapshot is
invalidated: the foreground mutation still completes, and materialization
reports `ResourceExhausted` instead of publishing an incomplete cut. Once
explicit admission succeeds, unexpected physical allocation failure follows
the process fail-fast policy rather than becoming a second admission result.

Extent construction is synchronous with the foreground write. Each extent's
payload and unused header slot are written and synchronized before its header
commit slot is written and synchronized. Only after all children are durable
can the root manifest record be appended. Failed construction queues already
created extents for reclamation and fail-stops the writer on storage I/O
failure.

Extent allocation and buffer ownership are registered under worker store state.
The foreground coroutine retains a pin on its unpublished extent and exclusive
ownership of its bounded buffer while encoding and waiting for device writes
and synchronization without that mutex. Completion reacquires store state before
releasing the pin/buffer, observing writer failure or reclaiming an orphan.
The manifest remains unpublished until all child extents are durable; shutdown
drains the owning operation before destroying its storage state.

Standalone replacements do not immediately retire the old durable record.
The old version remains live in physical accounting until the replacement's
flush completes, ensuring recovery always has at least one durable copy.

Multi-key durable writes use transaction-generation blocks. Each participant's
tagged records become durable first. `CommitTxWrites` waits for all participant
durability fences, then appends a keyless `kTxCommit` and requests its flush.
Recovery keeps tagged records only when that decision exists. Superseded
versions remain charged until the commit record itself is durable.

Successful common multi-key, keyed write-capable Lua, and EXEC paths hand their
receipts to a worker-local commit coordinator instead of spawning one coroutine
per transaction. One runner per worker drains at most 256 receipts at a time.
With a backlog it merges fences for the same block incarnation up to the
greatest required committed boundary and requests those unique frontiers in
parallel; each transaction still awaits only its own fences before appending
its own commit decision. A singleton batch retains the direct path. The queue
high watermark is 4096 receipts: crossing it makes the command wait for queue
capacity before replying, but not for commit durability. Direct callers such
as SORT STORE and list-move operations still wait through tagged-record fences
and commit-record append. Even an awaited `CommitTxWrites` only requests the
commit-record flush; it is not a synchronous crash-durability fence.

### Reads and pins

An ordinary single-key GET may resolve its complete inline-key index entry
without registering a shared transaction lock when that logical database's
lock table is empty. The empty-table observation and index lookup do not
suspend, so the lookup is the read's linearization point. A multi-shard writer
registers intents on every participant before any shard executes; an existing
writer therefore makes the table nonempty, while a writer registered after the
lookup overlaps the GET and can be ordered after it. Optimistic GETs do not
populate the table, so a read-only workload keeps this fast path available.
Any existing lock, an external index key that needs asynchronous verification,
or a physical-location race falls back to the original shared-lock path and
rereads. Transaction-owned reads always use that locked path directly.

Tombstones and expired values are invisible; an expired observation can enqueue
a bounded active-expiration candidate. A staged location is copied or framed
from its write buffer. A disk location is read on its physical block owner into
an aligned lease.

Worker-local MGET uses `BatchGetLocked` after the command has acquired its key
locks. It classifies index entries in one coroutine and issues ordinary-size
local inline records in waves paced by the fixed read-buffer pool; oversized
records use aligned overflow leases. One completion barrier covers every I/O in
a wave, and all leases are returned before relocation retries or the next wave
can suspend. Staged, remote, external, external-key, and stale-validation cases
fall back to the complete `GetLocked` state machine, preserving the ordinary
identity and relocation checks without one coroutine per key.

Before returning disk bytes, storage validates block and record allocation
epochs, record identity, database and replication epochs, mutation sequence,
type, key, and checksums. A pin prevents the block from being physically
released while I/O is active. Defrag can change the physical location without
taking the reader's key lock; a stale physical read aborts and follows the
current same-sequence relocation, while a true logical mutation becomes a
normal miss.

External values read and validate every extent on its current owner and
assemble the logical value. A move-only `ReadBufferLease` may cross to the
connection worker; destruction returns a registered slot to its storage owner.

### Flush and durability

A configured periodic interval, a full block, an extent-dependent overwrite,
a transaction fence or commit, defrag, and shutdown can all request a flush.
The flush snapshots the staged prefix, zero-pads its tail to a direct-I/O page,
and advances the append cursor to the following page so an already durable data
page is never modified again.

The durable order is:

```text
write new data pages and, on the first flush, the zeroed unused header slot
fdatasync
write the selected block-header slot as the commit marker
fdatasync
publish the snapshot as disk-backed and settle retired versions
```

System-state commits reuse the extent child-before-root rule. A replacement
catalog body and the complete manifest are written and synchronized first.
The writer then publishes the inactive A/B root and synchronizes it on each
configured device. Catalog and promotion updates always copy forward the
other fields from the last committed manifest, preventing independent writers
from losing each other's state. An ambiguous per-device root result fail-stops
the system-state writer and globally fences request serving. Restart resolves
the result through the highest common generation before service becomes
available again.

The manifest also retains an opaque Replication-owned population identity and
an optional Clean Shutdown Proof. The proof is committed with the final catalog
and population tokens after all workers cross a shutdown durability barrier.
Startup consumes it through the same common-root commit before permitting
mutations. A destructive FULL clears both identity and proof before any reset;
readable records or a PromotionBase alone cannot restore automatic candidacy.
The bounded shutdown metadata allocator can publish this certificate after
foreground allocation closes, without starting refill work or waiting for
reclamation. Optional index checkpoint construction follows this logical
shutdown boundary and does not substitute for the certificate.

The first flush durably clears the header slot not selected for the new
allocation before committing the selected slot. Later header writes alternate
slots. A dirty tail appended while a snapshot is in flight is requeued at the
front when a sealed stream completes, preserving append and copy-on-write
child-before-root ordering.

An ordinary successful command is therefore not necessarily crash-durable at
reply time. `StorageDurabilityStats` reports dirty staging bytes, pending
flushes, and pending transaction decisions for operators and tests that need a
durability fence. Graceful shutdown closes request admission and all
replication target/source transports first; source flow teardown releases
retained backlog cursors that could otherwise keep an accepted publisher
suspended. It drains accepted requests, joins the Meta control client when
configured, joins target apply work, aborts partial replacement roots,
and drains source handshakes, exports, and history. No accepted directive,
replication apply, or source-log transition can therefore mutate storage behind
the checkpoint boundary. Accepted queued and explicitly background transaction
commits receive up to five seconds to append their decisions; timeout fails
shutdown and prohibits a clean certificate.

For the optional index checkpoint, storage performs two freeze-and-drain rounds around transaction cleaning.
The first resets even header-only active streams and drains flushes, extent
reclaims, expiration, and retirement accounting. Worker 0 next forces
transaction cleaning to promote all committed tagged winners into durable
ordinary records. Because that cleaner may append or retire records itself,
every worker seals and drains once more and reaches a second barrier. Only
after a locked check proves that no mutator, queued flush, or runtime failure
remains may each worker serialize its frozen index shard; worker 0 publishes
their discovery bitmap and generation root through fixed metadata when
`shutdown-checkpoint yes` is configured. Transaction cleanup or checkpoint
failure leaves the previous generation consumed and shutdown continues with
authoritative ordinary and transaction records durable. An I/O failure
fail-stops further writes, rejects the clean checkpoint, and retains staging
buffers so already staged reads do not follow recycled memory.

## Allocation and maintenance lifecycles

### Allocation bitmap and cold reuse

The persistent allocation bitmap is the recovery authority, not merely an
allocation hint. Device allocators activate ready IDs in batches and preserve
eight allocatable blocks per device for defrag. Each worker first balances
foreground allocations across its weighted home devices. io_uring can then
fall back to every other configured device; SPDK cannot leave the namespaces of
controllers for which that worker owns qpairs. Foreground allocation may wait
while an active flush, extent reclaim, or runnable defrag can still return
space; it reports exhaustion only after a stable observation with no progress
in flight. A queued defrag while defrag is paused is deliberately not counted
as runnable progress, so a full-device write reports exhaustion instead of
waiting forever. Resuming defrag allows later allocation to wait for and use
the reclaimed block.

Current reclaimed-block lifecycle is:

```text
source has no live references and no pins
clear allocation bit and fdatasync metadata
place block ID in device-owner cold_free
before reuse, zero the stale 8 KiB header and fdatasync
set allocation bit and fdatasync metadata
publish block ID to the ready pool
assign a fresh allocation epoch
```

A destructive replica reset makes the prior records logically obsolete by
epoch; it does not make their blocks immediately allocatable. Partial-attempt
blocks and retired-but-unreclaimed blocks remain excluded from reported
available capacity until they complete this lifecycle. Full rebuild therefore
uses ordinary allocator and retained-memory admission instead of reserving a
second full dataset up front, and it may report resource exhaustion when the
in-place transition has insufficient reclaimable capacity.

A crash before the bit is cleared still scans the old allocation. A crash
afterward skips the stale body. A crash during reactivation sees either a clear
bit or a durably zero header, never a recoverable record from the cold block's
previous allocation. An ambiguous bitmap write freezes that device allocator
until restart selects the newest valid metadata page.

### Defrag and extent reclamation

Extent blocks are reclaimed as whole units; they are never defrag candidates.
An extent owner waits for pins, removes runtime state, and returns the ID through
the cold-free bitmap lifecycle.

An ordinary records block becomes a defrag candidate only when it is durable,
inactive, unpinned, and at most 50 percent live. Its owner scans the committed
image and asks each current key owner to relocate only an index-current record,
preserving mutation sequence, epochs, expiry, and logical type. Concurrent
mutation, epoch change, or prior relocation makes the candidate stale rather
than overwriting newer state.

Every relocation produces a destination durability fence. The source bitmap
bit is not cleared until all fences from the current and any earlier partial
pass have crossed durable destination headers, the source has no live bytes,
and its pins drain. Dependent external-key extents are reclaimed only after the
source retirement is durable. Per-device permits and the protected reserve
bound maintenance concurrency and prevent one device from consuming another's
recovery capacity.

### Expiry and tombstones

Read paths treat an expired value as absent immediately. Each worker starts a
bounded expiration/index-maintenance coroutine after recovery, including when
no keys have TTLs. Only expiration-authority nodes scan for expired values and
process deletion candidates; maps without expiring keys are skipped. Before
acting, the worker rechecks mutation sequence and deadline under the key's
exclusive lock.

The coroutine's interval and per-cycle scan, deletion, and index-maintenance
budgets are process-wide runtime settings exposed through `CONFIG SET/GET`.
Workers sample budgets after waking and retain them for that cycle; changing
the interval affects the next sleep without interrupting an existing one.
These settings neither grant expiration authority nor bypass pause/drain
boundaries. Defaults, ranges, and commands are documented in
[Active expiration tuning](../operations/active-expiration.md).

Standalone authority uses a permanent in-memory expiration capability.
Meta-managed owners instead receive a revocable capability whose absolute
deadline uses Linux `CLOCK_BOOTTIME` and cannot outlive the matching finite
serving lease. Each queued expiration candidate carries the exact capability
through the final storage mutation precondition. Expiry, replacement, or
revocation therefore cancels delayed work without turning that cancellation
into a storage failure; a later grant installs a distinct capability.

`QuiesceExpiration` is a nestable drain independent of authority revocation.
Controlled failover holds it after request mutation drain while the old owner
captures a stable replication frontier, and a replacement of that same pause
transfers the hold without reopening expiration between desired states. The
hold is released only when the source-pause intent disappears. Tomb Raider is
not governed by the finite capability or this pause, but Meta-managed cluster
startup never launches Tomb Raider because it starts without expiration
authority.

The preferred deletion is a durable tombstone, which remains safe if the wall
clock later moves backward. If foreground space is completely exhausted, an
unshielded expired value can be removed only from memory and physical live-byte
accounting; its on-disk deadline still makes it expired at ordinary recovery
time, and the freed blocks can restore write capacity. For grouped values,
the root and side view leave the indexes together and the complete auxiliary
graph is retired; external-key extents remain dependencies of their source
record blocks. A shielding value cannot use this escape valve because an
older durable value could reappear.

Tomb Raider is a separate, optional cleanup loop launched at worker startup
only when the node is then the expiration authority. Cluster startup does not
grant that authority and therefore does not launch the loop. Once
launched, the loop does not recheck authority on its own; the standalone
`REPLICAOF` transition therefore explicitly quiesces it before installing an
upstream. Native FULL mode also quiesces it at the session-wide boundary before
the first destructive reset. It marks tombstones and shielding values as
initially unclaimed, sweeps all ordinary record blocks
including staged prefixes, claims candidates for which an older unexpired
value still exists, and only then clears stale shielding or erases unclaimed
tombstones. The round state is process-local. A crash or shutdown can forfeit a
round because recovery reconstructs the conservative tombstone and shielding
state and the next round repeats the proof.

Index capacity follows removal from the in-memory index: replacing a value with a
tombstone retains its slot, while erasing the entry enables incremental bucket
shrinking. Worker-local maintenance completes pending rehashes even without
foreground requests or expiring keys, including on replicas. It shares the
expiration pause/drain boundary used by stable scans and shutdown checkpoints.
Shrinking preserves entry addresses and the cursor guarantee that continuously
present entries are visited at least once; a cursor can revisit merged buckets.
Memory admission can defer shrinking without preventing entry removal.

The internal `QuiesceTombRaiderForReplica` boundary is stronger than the
user-facing `TOMBRAIDER OFF`: it disables future rounds, requests an in-flight
round to forfeit at its next safe phase or block checkpoint, and waits until no
round is running. The ordinary OFF command continues to let an in-flight round
finish. Standalone role transition and the native FULL-begin barrier invoke
this boundary while client database gates are closed. The callable cluster
rebuild adapter uses that same native FULL path; cluster startup also avoids
the pre-directive race by never launching Tomb Raider while authority is
withheld.

### Database and transaction-generation cleanup

`FLUSHDB` and `FLUSHALL` advance monotonic database epochs. Storage persists the
new epoch values to every device before publishing them in memory and detaching
the affected per-partition indexes. Recovery therefore rejects the old
population even if the process stops before online reclamation finishes. The
database is observably empty after detach; SYNC waits for detached-index
retirement, while ASYNC ensures the same background reclaimer runs without
waiting for it.

Native FULL rebuild uses the analogous partition-epoch boundary for all 16,384
physical partitions, even when the desired cluster manifest is sparse. Each
reset batch persists a fresh target-local candidate epoch before it detaches
the prior index, and returns that epoch to replication so the logical manifest
epoch can be bound to the exact local incarnation at handoff. Snapshot and tail
records populate the new in-place indexes while the server is LOADING; there is
no parallel staging root or retained active root. Promotion verifies every
partition reached handoff, drains replica writes and the worker-local
detached-index queue, then persists database epochs and clears the sync state.
Abort performs the same write drain, detaches the partial attempt, and drains
that queue before it allows a later attempt to reuse runtime index capacity.
Detached-index completion covers index destruction and record-block live-byte
settlement. Value extents continue through the existing asynchronous retirement
path, and external-key extents remain a `retired-unreclaimed` dependency until
the parent record block's allocation bit is durably clear; allocator capacity
excludes both forms of debt until they actually reach the cold-free state.

Epoch invalidation is durable, but cluster readiness is not a storage property.
After restart, storage may recover records from the latest local epochs while
the replication layer constructs a new boot-scoped `NOT_READY` group and keeps
serving closed until a fresh directive proves a complete population.
The target-local epoch persisted here is distinct from both a manifest entry's
logical partition epoch and Meta's committed group-level partition replication
epoch. The latter two fence control-plane population identity; neither can
substitute for the storage epoch used by recovery.

Transaction cleaning rotates record-bearing generations, seals and flushes
their blocks, collects committed decisions, and relocates current committed
tagged winners into ordinary untagged record blocks. A generation is returned
through the cold-free lifecycle only when it is sealed and durable and has no
active transaction leases, live tagged bytes, or dependency pins. When a
transaction block is durably retired, its deferred external-key extent debt is
released through the same asynchronous reclaim path as an ordinary record
block. Standalone grouped writes can coordinate this lifecycle under foreground
space pressure before acquiring a new generation lease; borrowed transaction
writers never wait for their own generation to retire. Online cleaning yields
to shutdown at block boundaries after already-published relocations become
durable, retaining the incomplete generation's decisions and source allocations.
When a shutdown checkpoint is enabled, worker 0 ignores the online cooldown
and completes this lifecycle to a fixed point after commit and flush drain;
failure skips the checkpoint rather than weakening cold recovery.

## Crash-consistency invariants and failure behavior

- A block header is the commit marker for exactly its advertised data prefix;
  the data is synchronized before the selected header slot.
- An allocation bit is set before a block is handed to a writer, and a reused
  cold block's stale header is synchronized to zero before that bit is set.
- A source allocation bit is cleared only after all live relocations have
  durable destinations and all physical pins have drained.
- Extent children become durable before a manifest root can reference them,
  and dependent extents outlive every root record that can still recover.
- Tagged transaction records survive only with a global durable commit
  decision; a missing decision drops the complete transaction at recovery.
- Database and partition epochs are persisted before logical invalidation is
  published, so recovery cannot resurrect a detached population.
- A replica handoff belongs to the target-local epoch returned by its reset;
  logical control-plane epochs never substitute for the storage epoch that
  filters recovery.
- Recovery considers an expired or tombstone winner before older versions;
  cleanup cannot remove its suppression while an older live record remains.
- Meta-managed active expiration carries the exact finite lease capability to
  the storage mutation boundary; deadline expiry, revocation, or replacement
  cannot be bypassed by work queued under an older grant.
- Allocation epochs accompany physical references, reads, accounting, and
  relocations so delayed work cannot affect a later incarnation of one block.
- Fixed-metadata and storage write ambiguity is fail-stop. The first worker or
  node-global writer failure sets one process-wide, irreversible latch and
  immediately fences request serving; no worker continues foreground
  allocation or append after observing it. Worker zero reports that latch to
  the cluster node controller, which withdraws storage readiness and joins
  replication capabilities before the process can claim a completed loss
  barrier. An uncertain cleanup result stops the server without writing a
  clean-shutdown checkpoint. Restart recovery is the only path that can select
  and reopen a provably valid durable state.
- Checkpoint blocks become reachable only after all index chunks and their
  discovery bitmap are durable and the generation root is published. Startup
  consumes a generation before using it, so a later crash cannot reuse a
  snapshot from before that process ran.
- A durable Function catalog is exposed only through a common system-state
  root; a promotion base refers to the exact local catalog and population
  tokens committed in that same manifest lineage.
- Full-sync invalidation is durable before population replacement begins. A
  valid catalog root cannot make an interrupted target readable until the
  matching population and catalog readiness commit completes.

## Observability and verification

The engine exposes durability status, per-device capacity and available block
metrics, filesystem free space for regular-file devices, and Defrag, Tomb
Raider, transaction-cleaner, and transaction-commit coordinator totals. INFO
STATS reports commit batch and transaction counts, input and merged fences,
queue depth and peak, backpressure waits, and the 4096-receipt watermark.
Prometheus also aggregates Bycorf's completed storage read, write, and
`fdatasync` counters and publishes server readiness; recovery reports periodic
progress in the log. Runtime configuration can pause or pace defrag and select
Tomb Raider off, interval, or daily scheduling. Engine snapshots are collected
on the worker or allocator that owns the underlying mutable state.

Current test evidence includes:

| Test | Evidence |
|---|---|
| `tests/storage_format_test.cpp` | Label, metadata, system-state root, block, record, extent, and transaction encoding; version/CRC rejection; A/B winner and torn-slot fallback |
| `tests/multi_exec_e2e_test.cpp` | Function LOAD/RESTORE/DELETE/FLUSH durability and startup recovery |
| `tests/storage_capacity_test.cpp` | Existing-file requirement, alignment and minimum capacity, persisted capacity, expansion membership, foreign-device rejection, and explicit reset |
| `tests/extent_recovery_e2e_test.cpp` | External keys and values, manifest/extent recovery, reclamation, and repeated worker-count changes |
| `tests/flushdb_reclaim_e2e_test.cpp` | Full-device FLUSHDB reclaim, paused-defrag exhaustion and resume, expiry escape valve, stale activated-header handling, and a crash after durable defrag source retirement |
| `tests/ttl_e2e_test.cpp` | TTL mutation, disk-resident rewrite, expired/live restart behavior, and extent-backed values |
| `tests/tomb_raider_e2e_test.cpp` | Runtime scheduling, retain/reap behavior for buried persistent or expired values, user OFF completion semantics, and bounded internal replica quiescence |
| `tests/multikey_e2e_test.cpp`, `tests/tx_cleaner_test.cpp` | Bounded disk MGET waves, commit batching and fence merging, transaction-generation rotation, recovery, FLUSHDB invalidation, rollback, retry, and exact retirement readiness |
| `tests/atomicity_stress_e2e_test.cpp` | Overlapping multi-key serializability and recovery after a graceful durability drain |
| `tests/list_e2e_test.cpp` | Function-catalog body/root/runtime crash windows, multi-device torn-root fallback, and shielded expired-winner behavior under an injected recovery clock rollback |
| `tests/buffer_pool_test.cpp` | Reuse of a waiting storage write-buffer acquisition |
| `tests/grouped_hash_test.cpp` | Group codecs, incremental routing, mutation planning and transaction-adjudicated recovery-model validation |
| `tests/grouped_object_index_test.cpp` | Side-index identity, copy-on-write updates, retirement markers, pre-admitted publication and retained-memory rollback |
| `tests/grouped_recovery_e2e_test.cpp` | Real grouped disk images, child/outer decisions, physical corruption, worker reassignment, GC crash windows, snapshots and graph detachment |
| `LargeHashDurabilityE2eTest` in `tests/list_e2e_test.cpp` | Large Hash extent-write and GC crash recovery, bounded-device reclamation, and RESP OOM atomicity; dedicated grouped suites also cover graph publication and relocation boundaries |
| `tests/device_affinity_test.cpp` | SPDK controller quota and qpair-owner planning across balanced, weighted, and controller-heavy layouts |

## Known gaps and documentation limits

- Reset zeroes only fixed metadata and does not securely erase the data region.
  Current tests do not prove crash-safe stale-media sanitization between bitmap
  activation and first flush for reset media or a reused zero-label added
  device. Operators should provision genuinely empty added media when that
  property matters.
- Tomb Raider does not recheck replication authority independently. Supported
  standalone role transition and the callable cluster rebuild adapter both
  reach its quiesce boundary through native FULL, while Meta-managed startup
  withholds authority from the outset. Any authority transition that bypasses
  those replication paths must invoke the same boundary.
- Storage persists partition/database epochs, population scope and a one-use
  shutdown certificate, but not a Meta-managed ready token or Data control
  authority. Meta control reconnects after every boot, but neither
  its full desired state nor its leases are restored from the data device;
  recovered records alone never authorize cluster serving. A durable
  incomplete-full-sync fence also survives recovery and prevents a mixed
  population from becoming visible.
- The `tx-commit-append` crash hook exists to isolate a transaction after all
  tagged data is durable but before its decision is appended, but no current
  test arms that named hook directly.
- Storage-specific tests use regular files. Raw-device and SPDK behavior is
  implementation-backed but lacks equivalent end-to-end coverage here.
- Expansion tests validate labels, ordering, and set membership, but do not
  perform a data-bearing multi-device expansion and recovery sequence.
- A successful ordinary command or background transaction commit is not an
  implicit synchronous durability fence. Callers that require one must use the
  exposed durability state or a higher-level operation that explicitly drains
  it.
- Replication frame structures share the format source file, but the native
  replication backlog is process-local and is not part of primary recovered
  storage. Its lifecycle belongs in the replication architecture document.

## Related documents

- [System overview](01-overview.md)
- [Transaction coordination](03-transaction-coordination.md)
- [Replication](05-replication.md)
- [Shutdown index checkpoints](06-shutdown-index-checkpoints.md)
- [Recovery metadata layout](../design-docs/recovery-metadata-design.md)
- [Worker-count-independent storage ownership](../design-docs/storage-block-ownership.md)
- [Multi-device storage](../operations/multi-device-storage.md)
- [Tomb Raider scheduling](../operations/tomb-raider.md)
- [Running with SPDK](../design-docs/spdk.md)

The linked design documents are historical references, while the operations
documents are procedural guidance. This focused architecture document and
current source code are authoritative for present storage behavior.

## Source map

| Claim | Repository source |
|---|---|
| Public lifecycle, routing, typed operations, locked transaction contract, snapshots, epochs, maintenance, and durability interfaces | `include/lavik/storage/engine.h` |
| Worker, partition, block, append-stream, allocator, recovery, and background-maintenance state | `src/storage/engine/impl.h` |
| Runtime index representation, shared entry arena, runtime key digests, and asynchronous entry-identity validation | `include/lavik/storage/scan_hash_map.h`, `include/lavik/storage/format.h`, `src/storage/format.cpp`, `src/storage/engine/impl.h`, `src/storage/engine/write.cpp`, `src/storage/engine/flush.cpp` |
| Compact and grouped Hash/Set serving, group identity and object side index | `src/storage/engine/hash_tree.cpp`, `src/storage/engine/hash_codec.cpp`, `src/storage/engine/grouped_hash.cpp`, `src/storage/engine/grouped_object_index.cpp`, [Grouped collections](09-grouped-collections.md) |
| Compact physical index representation shared by user-key and group-location indexes | `include/lavik/storage/detail/record_index.h` |
| Persistent constants, device and block IDs, A/B metadata pages, record and extent layouts, and checksums | `include/lavik/storage/format.h`, `src/storage/format.cpp` |
| Checkpoint serialization, bitmap validation, generation publication and consumption, fallback, and block retirement | `src/storage/engine/checkpoint.cpp`, `src/storage/engine/flush.cpp`, `src/storage/engine/init.cpp`, `src/storage/engine/recovery.cpp` |
| System-state manifest, catalog COW extents, full-sync fence, population token, promotion base, and one-use shutdown certificate | `src/storage/engine/system_state.cpp`, `include/lavik/storage/engine.h` |
| Aligned buffer ownership, registered-I/O fallback, oversized reads, and cross-worker lease return | `include/lavik/storage/buffer_pool.h`, `src/storage/buffer_pool.cpp` |
| Storage-path probing, device-set validation and expansion, controller/qpair affinity, metadata load, worker initialization and native-thread finalization, recovery barriers, and shutdown flush | `src/storage/engine/init.cpp`, `src/storage/engine/device_affinity.h`, `src/storage/engine/impl.h` |
| Device-owner allocation, bitmap activation and cold-free retirement, epoch mirroring, reserves, and allocator fail-stop behavior | `src/storage/engine/alloc.cpp` |
| Parallel scans, block reassignment, epoch filtering, transaction decision collection, winner selection, and recovery accounting | `src/storage/engine/recovery.cpp`, `src/storage/engine/init.cpp` |
| Append streams, mutation precondition, extent construction, WATCH/index publication, replacement accounting, transaction fences, commit batching and backpressure, commit decisions, caller wait policy, and rollback | `include/lavik/storage/engine.h`, `src/storage/engine/write.cpp`, `src/storage/engine/hash_tree.cpp`, `src/redis/command.cpp`, `src/redis/list_command.cpp`, `src/redis/sort_command.cpp` |
| Worker-sharded retained-memory admission and ownership, detached-index reclaim, client-buffer quotas, full-sync reservations, and RDB snapshot admission failure | `include/lavik/memory.h`, `src/memory.cpp`, `include/lavik/storage/scan_hash_map.h`, `src/storage/engine/replication.cpp`, `src/storage/engine/backup.cpp` |
| Staged and disk reads, bounded BatchGet waves, validation, pins, relocation retry, external-value assembly, and disk-backed reply leases | `src/storage/engine/read.cpp`, `include/lavik/storage/engine.h` |
| Periodic flush snapshots, data-before-header ordering, alternating header commits, dirty-tail ordering, and retirement settlement | `src/storage/engine/flush.cpp` |
| Extent reclaim, defrag candidate selection, relocation durability fences, source retirement, and pacing | `src/storage/engine/defrag.cpp` |
| Lazy and active expiration, permanent and finite authority capabilities, nestable quiescence, durable tombstones, and the full-device escape valve | `include/lavik/storage/engine.h`, `src/storage/engine/expire.cpp`, `src/cluster/node_control.cpp`, `src/replication/replication.cpp` |
| Tombstone and shielding mark/sweep/reap lifecycle, startup authority check, internal replica quiescence, and runtime role limitation | `include/lavik/storage/engine.h`, `src/storage/engine/tomb_raider.cpp`, `src/storage/engine/init.cpp`, `src/replication/replication.cpp` |
| Durable database and replica-partition epoch advance, bounded index detach, replica reset/promotion/abort, and detached-index reclaim | `src/storage/engine/flush_db.cpp`, `src/storage/engine/replication.cpp` |
| Transaction-generation rotation, promotion, readiness, and cold retirement | `src/storage/engine/tx_cleaner.cpp`, `include/lavik/storage/tx_cleaner.h` |
| Device, durability, recovery, storage-I/O, Defrag, Tomb Raider, and transaction-cleaner observability | `include/lavik/storage/engine.h`, `src/storage/engine/metrics.cpp`, `src/storage/engine/recovery.cpp`, `src/metrics.cpp` |
| Format, capacity, catalog recovery, crash-window, expiration, reclamation, transaction-cleaner, and buffer-pool verification | `tests/storage_format_test.cpp`, `tests/storage_capacity_test.cpp`, `tests/multi_exec_e2e_test.cpp`, `tests/extent_recovery_e2e_test.cpp`, `tests/flushdb_reclaim_e2e_test.cpp`, `tests/ttl_e2e_test.cpp`, `tests/tomb_raider_e2e_test.cpp`, `tests/multikey_e2e_test.cpp`, `tests/atomicity_stress_e2e_test.cpp`, `tests/list_e2e_test.cpp`, `tests/tx_cleaner_test.cpp`, `tests/buffer_pool_test.cpp` |
