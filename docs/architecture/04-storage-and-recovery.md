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
complete-value encodings. String, Hash, Set, List, Sorted Set and Stream also have
independently addressable complete group snapshots, selected through a sparse
object side index when the top-level index marks a grouped representation.
Their serving, transaction, recovery and graph-lifecycle boundaries are described in
[Grouped collections](09-grouped-collections.md). Collection writes promote
automatically at the compact-size threshold, while streaming imports construct
grouped graphs directly. Hash/Set use prefix routing; List uses ordered pages.
Sorted Sets combine ordered pages with a member-to-score prefix
index under one atomic root.
[Streams](12-streams.md) use ordered logical records for messages, nodes,
consumer groups and PEL state, with an independent user-visible message count.
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
  owner is `partition_id % worker_count`; that worker owns the partition's
  enabled logical-database indexes, mutation sequence, replication epoch,
  counts, and snapshot state. Cluster client mode constructs only DB0 indexes;
  Single client mode constructs all 16, independently of Meta management. Each worker owns contiguous index arrays
  backed by its shared entry arena, with partitions holding fixed views into
  those arrays. Durable epochs and checkpoint capacity tables retain all 16
  database slots in both modes. Recovery rejects current records in disabled
  databases rather than hiding an existing standalone dataset.
- A physical block has one current runtime owner. When the worker topology
  matches the topology recorded in its header, recovery retains the original
  writer if that worker can access the device. Otherwise recovery hashes the
  block ID and allocation epoch across the eligible workers. All workers are
  eligible on io_uring; SPDK eligibility is restricted to workers holding a
  qpair for the block's physical controller.
- Each device has one allocator owner, which serializes that device's ready and
  cold-free pools, scan bitmap, fixed metadata page generations, epoch mirrors,
  and allocation-epoch counter. io_uring initially assigns this role as
  `device_index % worker_count`. SPDK selects it from the controller's home
  workers, all of which hold a qpair, so metadata and allocation I/O never
  route to a worker that cannot open the namespace.

Every worker has one ordinary and one transaction append stream. Physical
streams are per worker, not per logical partition, so active 8 MiB staging
buffers scale with workers rather than with 16,384 slots. A worker also owns the
`BlockState` objects assigned to it, including committed and live byte counts,
staging identity, flush state, and defrag state. Runtime owner and allocation
epoch form immutable identity after publication. A shared atomic pin word
protects physical lifetime and closes admission to connection-worker reads
while maintenance owns the block. The owner is published atomically after
epoch initialization. Other mutable block fields remain owner-local; remote
workers submit accounting and maintenance work to that owner.

Append-stream creation and rollover are single-flight within each stream. A
writer may release owner-local store state while physical allocation waits,
but it revalidates the current stream before publication and returns any
unused reservation to the device allocator. This prevents duplicate stream
publication without serializing ordinary and transaction appends against each
other. Staging buffers remain tied to active streams, and shutdown waits for
outstanding allocation work before destroying its state.

Logical key locks come from the transaction subsystem. Storage's pre-locked
interfaces require the caller to run on the key owner with the correct shared
or exclusive lock. Background expiry, relocation, snapshot, and replica work
participates in that same arbitration boundary.

## Persistence backends and physical layout

Active storage paths are existing regular files, Linux raw block devices, or
Bycorf SPDK storage paths. `Prepare` probes and validates them; it does not
create, extend, truncate, or preallocate a missing regular file. On the POSIX
io_uring backend, every worker opens the complete configured path table for
direct I/O. Its read and write slots share worker-owned allocations;
multiple slots share a registered region while keeping independent lease and
release identities. Buffer-pool capacity is independent of registration success.
If full io_uring registration fails, a finite memlock allowance permits a
best-effort subset registration, prioritizing read slots. Each lease retains
its slot identity and immutable registration status; unregistered slots use
the same aligned memory with plain asynchronous I/O. On SPDK, a
worker opens only namespaces whose physical controller assigned it a qpair.
Weighted home-device assignment controls write placement independently of
qpair access. Each controller reserves qpairs for its home workers, then
assigns its remaining capacity to other workers, up to one qpair per worker.
A controller with enough qpairs is therefore directly accessible from every
connection worker. Namespaces on that controller share each worker's qpair.
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
kinds are ordinary records, payload extents, indirect-key records (kind 3),
transaction records, checkpoint index chunks, and temporary Redis export
backlog blocks. Export blocks use on-disk kind 6 and contain replication frames,
not indexed records.
They belong to the active Redis full-sync session only; recovery returns every
such allocated block to free space without replaying its contents.

Records are 8-byte aligned and carry their database and value type, key
representation, logical and physical sizes, transaction ID, database and
replication epochs, logical mutation sequence, absolute expiration time,
physical LSN, allocation epoch, and a per-record CRC policy with payload and
header checksum fields. A key digest
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
record kind, database, value type, external-payload state, indirect-key state,
grouped-root and auxiliary-group markers, CRC policy, and extension presence into one
16-bit word. An auxiliary group carries an additional 32-byte checked
identity containing its object incarnation, routing identity, retirement bit
and optional nested-command transaction decision.
Its identity is available without reading an external value; group payloads
retain complete snapshots, not read-time mutation logs. The group payload
envelope leaves framing space within the 1 GiB record-payload limit while
preserving the independent 512 MiB limit for each field and value. Hash and Set
use persisted-seed hash prefixes; List uses stable ordered page identities.
Sorted Sets use both disjoint identity spaces under their collection type.
The version-1 ordered-root payload has a checked presence flag for its appended
member Hash root, required for Sorted Sets and absent for other kinds. Roots preserve an independent
group revision, distinct from the source command sequence shared by mutations
in a replay envelope.
Header length is derived from
those flags and key length; total record length is derived from header and
payload length. Neither derived length is stored. The decoded `RecordHeader`
is a runtime view rather than a persisted C++ object representation.

Data record and value/key extent CRC generation is disabled by default and
selected at startup through `record-crc`. Each record persists a `no_crc` bit;
zero retains the checksummed interpretation of earlier records in this layout.
No-CRC records keep both checksum fields zero. Readers, recovery and defrag
honor each stored object's policy independently of the current startup option,
so records from alternating settings coexist. Structural, identity and bounds
validation always applies. Rewritten records use the current write policy;
reused extents retain their own persisted policy, protected by their block
header CRC. Device labels, block headers, fixed metadata, shutdown checkpoints,
system-state/catalog extents and replication frames always retain their CRCs.
Readers predating these flags cannot consume no-CRC objects.

The compact Stream payload uses the current `LXS1` layout, including persisted
macro-node entry counts that preserve approximate-trim boundaries across
restart and RDB export/import. It does not reconstruct those boundaries from
current settings or accept earlier development layouts without node counts.
Lavik-owned storage schemas remain at v1 while unreleased; incompatible
development media is recreated, not migrated. Redis RDB versions follow the
external Redis format independently. Grouped Streams
project their ordered records into the same logical format; the grouped root
and page identities remain local durable metadata.

The current version-1 format also includes checkpoint metadata and the
system-state root and manifest. During pre-deployment development this layout
directly replaces earlier layouts that also used version 1; there is no
compatibility decoder. Older media, including the earlier 104-byte record
layout, must be reset before this build starts.

Keys of at most 2 KiB stay complete in the index and record header. Longer
keys use a stable 16-byte UUID in every root, auxiliary and tombstone header;
the indirect-key flag identifies this representation. A
worker-owned UUID registry resolves the immutable original bytes. It uses
`ScanHashMap` and shares the worker's index entry arena and memory admission;
registry insertion is admitted before writing a new KeyRecord. Periodic
maintenance advances incremental rehash and shrinking even without long-key
traffic. Runtime lookup still hashes the original key and verifies complete
bytes on a digest collision. Small-key requests never consult this registry.

Dedicated `kIndirectKeys` blocks pack UUID-to-original-key records through a
separate, lazily opened append stream. KeyRecords use whole-value storage;
keys exceeding a block's available payload (8 MiB less headers) use ordinary
payload extents. KeyRecords are never grouped. The UUID includes its logical
slot and remains stable when its physical record moves or worker count changes.
A KeyRecord is durable before a referencing user record can enter staging.

Indivisible values and oversized KeyRecords use a record containing an extent
manifest. Each reference identifies a dedicated extent block by block ID,
allocation epoch, byte count, and payload checksum. The extent header repeats
its index, length, and checksum, plus an independent no-CRC flag. User value manifests contain only value bytes;
they never repeat the original long key.

Every allocated user-record block retains its UUID references, including
obsolete versions, expired values and aborted transaction prepares. Removing
user visibility does not release those recovery dependencies: they leave only
after the block's allocation bit is durably cleared. Once no physical user
record or in-flight operation references a UUID, its KeyRecord becomes dead.
The UUID registry tracks all remaining physical KeyRecord copies; shared key
extents are reclaimed only after the last such copy's block is retired.

KeyRecord cleaning has a separate relocation routine and shares device permits,
allocation reserves and durability fences with other storage maintenance. It
updates one UUID-to-location entry after the new copy is durable. Ordinary
record defrag never interprets KeyRecords as user entries. Recovery first
rebuilds the UUID registry and extent owners, then scans user records and
reconstructs block dependencies before reclaiming orphans. A shutdown checkpoint
retains the complete keys and physical UUID dependencies so startup can skip
user-record bodies; the KeyRecord pass still rebuilds the UUID registry and
its physical copies before those dependencies are attached.

User Strings of at least 16 KiB split their value into fixed 8 KiB group records,
regardless of key length; smaller Strings retain their compact representation.
Dedicated KeyRecords retain their whole-value representation independently of
this user-value rule.
Grouped String roots carry byte length and graph identity. The grouped lifecycle,
including direct segment indexing and root-only TTL updates, is described in
[Grouped collections](09-grouped-collections.md).

## Startup and recovery

### Preparation

`Prepare` runs before Bycorf workers start. It validates the worker count,
flush alignment, per-device defrag concurrency, configured
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
the checkpoint bodies. The prefix directory redistributes those
blocks to their durable shard first, so the index owner performs both body I/O
and installation without a cross-worker decoded batch. After validating the
body checksum, each individually validated inline-key entry is installed
directly from its pinned I/O buffer. The entry carries its validated logical partition and
runtime digest, so key bytes have no intermediate owning copy and recovery
does not recalculate Redis slots or key digests. A later invalid entry sends
the installed valid prefix
through the ordinary cold-scan merge. io_uring
owners can open every configured path; under SPDK the unchanged worker
topology must also give the shard owner a qpair for the block's controller.
This removes incremental index rehashing, cross-worker installation, decoded
entry batches, and per-key routing hashes from inline-key checkpoint recovery while
retaining worker-bounded temporary I/O memory. Grouped roots and indirect keys
use sequenced metadata fragments, with admitted scratch for one complete key
and directory. Grouped views are restored without reading collection values;
UUID dependencies are reattached after the KeyRecord pass. On fallback, those
views and dependencies are discarded and rebuilt by the ordinary scan.
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
SPDK groups namespaces by physical controller, assigns weighted home devices,
and expands qpair access with the remaining controller capacity; startup fails
unless every controller and every worker can be covered. Worker barriers then
coordinate one parallel recovery rather than independent
worker-local boots. Request readiness follows successful completion of the
complete recovery and allocator-cleanup sequence; the exact moment a listening
socket exists is not the readiness boundary.

Worker recovery reuses the three fixed maintenance read workspaces before
maintenance starts. Checkpoint discovery and body loading alternate two
workspaces; ordinary block scans reuse one, while nested key/group extent
validation uses the third. Delegated SPDK reads carry the initiating task's
workspace and join before reuse, so another worker's concurrent recovery cannot
overwrite it. Each checkpoint phase drains outstanding prefetch, including
abandoned reads after validation failure, before the next phase or cold-scan
fallback can reuse those images. Recovery barriers precede maintenance reuse.

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
   is ignored without rewriting the bitmap. A valid temporary Redis export
   block is also reclaimed immediately; a new process has no matching PSYNC
   session or continuation cursor.
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
   potentially live value. Grouped winners then undergo
   [graph reconstruction](09-grouped-collections.md#recovery-reclamation-and-snapshots),
   including its expired-root exception, before live-byte accounting.
6. A second cross-worker pass walks each in-memory winner index once and
   charges every surviving root and referenced extent exactly once to its
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
prepared for the current worker's ordinary or transaction staging
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
native candidate/loading paths and compact external values and indirect keys retain
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

Existing grouped Hash/Set point writes, String segment writes and typed
List/Sorted Set/Stream writes also prepare private pages without worker store
state, including under retained EXEC/Lua key intents. They carry immutable
predecessor metadata and admitted page ownership through preparation, then
reacquire state and validate population and logical identity before any
publication or successful no-op. Sorted Set preparation covers both the ordered
and member graphs. GC-only physical moves are refreshed, not treated as
conflicting logical writes. Random Set pops,
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

Primary and auxiliary group indexes share the same compact field layout.
Primary entries store logical sizes in a variable-width tail; auxiliary arrays
own fixed-width values with their logical-size tail included. An update keeps
its entry identity while its existing representation has room. Width growth
or an expiry subtype change replaces the entry. The append path admits
replacement memory and handle capacity before publishing the write;
pointer-keyed side state follows the replacement.

Runtime key digests use one operating-system-seeded SipHash key shared by all
workers and do not affect Redis-slot routing. A cold recovery chooses a fresh
seed; a successful shutdown-checkpoint recovery restores the checkpoint seed
before hashing any key so its serialized top-level digests remain valid.
External keys and decoded Hash or Set fields retain or reconstruct a digest
only as a lookup aid, and collisions are verified against complete keys. The
seed and top-level digests appear only in the one-use checkpoint, never in
authoritative records or replication protocols.

All partition and logical-database indexes on a worker allocate entries from a
shared arena, so sparse indexes share capacity instead of stranding it at
partition boundaries. Startup allocates its complete page directory, charging
16 MiB of descriptor storage to each worker before recovery; entry pages remain
demand-allocated. The directory stays at one address and publishes page layout
atomically. Handle-to-address resolution can run concurrently with owner-side
allocation when the caller protects the resolved entry and arena lifetimes;
it does not pin entries or make the hash tables safe for concurrent access.
Allocator mutation remains owner-local. Private auxiliary arenas retain
incremental directory allocation and owner-only access.
The primary-index object arrays are allocated once for the worker's partitions
and enabled logical databases and charged to that worker, including while
empty. Each primary key index reserves 32 cache-line-isolated atomic
read-admission states within those objects. Its bucket arrays remain demand-allocated,
start at 32 buckets, and never shrink below 32 while nonempty; checkpoint
recovery applies the same floor. Reader admission is explicitly enabled only
for a published map with a fixed arena directory and an externally protected
map lifetime. A remote reader registers atomically in the low-hash stripe,
performs only const lookup, and releases its guard before suspending. Writers
close the affected stripe and suspend their owner coroutine until existing
readers drain, then resolve entry identities again before mutation. The last
reader of a closed stripe notifies the owner; transaction arbitration and
structural writers recheck readiness there.
Shared overflow-directory growth and population replacement close all stripes.
Resizing closes admission before changing table layout, defers behind existing
readers, and stays closed through the final table switch and old-table release.
Owner lookups and periodic maintenance retry deferred resizing; nesting keeps
resize completion from reopening another writer's scope. Recovery and detached
populations remain private, and grouped-object and auxiliary maps retain their
owner-only layout and bucket sizing. Transaction intents retain stripe closure
through queued and executing work, so a remote index snapshot cannot observe
an intermediate multi-key mutation.
A population detached by `FLUSHDB`, a replica reset, or a replica abort retains
ownership of that arena until reclamation finishes. Replica promotion and
abort reuse the same worker-local detached-index drain as synchronous database
flushes, so a completed lifecycle
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
each share during normal operation. Startup recovery can use 100 percent;
its candidate deques, prepared-record vectors, winner maps, and grouped
reconstruction vectors participate in the same accounting. Scan/apply batches
check the accumulated footprint, and retained index growth continues to use
admission. Temporary-container growth is checked at batch boundaries rather
than throwing quota exceptions inside partially modified hash tables. Bounded
I/O scratch, decoded payloads, and allocator overhead still require RSS
headroom. After all storage recovery coroutines have returned and their heaps
have been collected, every worker's remaining footprint must fit its normal
90-percent share before the server permits startup import and serving. A
failed transition stops startup. Recovery does not change the disk scan count.

Storage buffer capacity also participates in retained accounting: fixed read
and write pools and dedicated maintenance scan buffers are charged at
initialization. Redis export owns one process-wide 8 MiB workspace, allocated
and charged at startup, reused for capture writes and subsequent disk replay,
and retained between sessions. Startup preparation allocates worker zero's
maintenance buffers before reading device metadata. Synchronous reads borrow
one of those images for metadata pages and whole system-state/catalog extents;
decoded state and the assembled catalog own ordinary heap memory. Preparation
finishes all synchronous I/O before worker initialization reuses the same
buffers without another allocation or charge. Worker recovery then borrows
the maintenance buffers before their normal scanners start.
Each device allocator owns one 8 KiB metadata write buffer, charged to its
owner worker at startup and retained until engine teardown after I/O drains.
Allocation failure identifies the device and prevents startup. Preparation
uses that same workspace for label, metadata initialization, and reset writes,
splitting larger ranges into buffer-sized chunks. The device's
coroutine mutex serializes workspace use for recycled-block header clearing,
bitmap and epoch pages, and checkpoint/system-state roots. These writes do not
borrow read slots or ordinary write slots and allocate no fallback buffers.
io_uring uses unregistered I/O for this workspace; SPDK uses DMA memory.
Synchronous SPDK startup helpers submit these caller-owned DMA buffers directly
without allocating bounce storage. Temporary metadata queue pairs are released
before worker queue creation.
Oversized reads and reads that find the fixed pool exhausted allocate aligned
or SPDK DMA overflow storage only while leased;
returning a lease frees that storage and removes its charge on the owner worker.
If temporary allocation fails, ordinary reads wait for a
fixed slot in FIFO order. Oversized record and extent reads whose allocation
fails fall back to slot-sized reads when fixed read slots exist, retaining the
block pin through pool waiting and all chunks. Records assemble a private image
in ordinary heap memory and retain their identity, population, checksum, and
relocation-retry checks. Extent paths validate the same header and, when enabled,
the complete payload checksum before exposing the requested slice; fallback
stages that slice in ordinary heap memory. Whole-value assembly uses ordinary
heap memory, including on SPDK: page and extent reads copy into an output that
neither consumes a fixed slot nor needs DMA addressability. Outputs retain reply
framing space and follow the same owner-bound lease accounting and release
lifecycle. These allocations are accounted without
a separate admission gate so reads and reclamation can still progress at the
memory waterline; subsequent retained growth observes their charges. Pool
charges cover allocated slot capacity, not allocator overhead or SPDK's
entire hugepage reservation.

Accounting ownership remains bound to the allocation's origin,
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

Multi-key durable writes use transaction blocks. Each participant's
tagged records become durable first. `CommitTxWrites` waits for all participant
durability fences, then appends a keyless `kTxCommit` and requests its flush.
Recovery keeps tagged records only when that decision exists. Superseded
versions remain charged until the commit record itself is durable.

Successful common multi-key, keyed write-capable Lua, and EXEC paths hand their
receipts to a worker-local commit coordinator instead of spawning one coroutine
per transaction. One runner per worker drains at most 256 receipts at a time.
With a backlog it merges fences for the same block incarnation up to the
greatest required committed boundary and requests those unique frontiers in
parallel. Each transaction awaits its own data fences before appending its
commit decision. A multi-transaction batch appends its decisions before
requesting their shared flush, then waits for durability before publishing
grouped decisions. A singleton batch retains the direct path. The queue
high watermark is 4096 receipts: crossing it makes the command wait for queue
capacity before replying, but not for commit durability. Direct callers such
as SORT STORE and list-move operations still wait through tagged-record fences
and commit-record append. Even an awaited `CommitTxWrites` only requests the
commit-record flush; it is not a synchronous crash-durability fence.

Record and extent writes use only the fixed write pool. Staging-buffer
pressure seals physical append streams before a writer waits for capacity.
Foreground and relocation writers release worker store state
and join the same owner-local FIFO; a returned fixed slot transfers directly
to its oldest waiter, so new borrowers cannot overtake it. While writers
remain queued, record-write exits seal newly installed streams, including
empty streams left by failed revalidation. Live transaction leases and tagged
records survive a pressure seal; later writes or commit decisions may open
another transaction block. Flush completion returns sealed buffers after
their last reader pin drains, including tails that were already durable.
Capacity therefore does not depend on completing transactions that themselves
need a staging buffer to commit. A failed storage writer fails pending slot
acquisitions without recycling its still-referenced staging memory.

Relocation bypasses the foreground physical-allocation gate and retains access
to the defrag disk reserve: a foreground allocator may itself be waiting for
reclamation. After write-slot waiting, relocation revalidates its source before
publication. Each worker owns three independent 8 MiB scan buffers, one each
for defrag, transaction cleaning, and Tomb Raider. Their schedulers permit at
most one scanner of each kind per worker, so scans can retain their own image
across I/O, relocation, and cross-worker waits without a buffer queue. They
never borrow write slots or allocate fallback scan buffers. All three buffers
are allocated at initialization, outside the ordinary pool budget; failure
names the worker and task and prevents startup. They remain owned until
worker teardown after I/O drains. io_uring uses them without registration;
SPDK allocates DMA-addressable memory for them.

### Reads and pins

Standalone single-key reads explicitly admitted by the command table can
read directly on their connection worker after ordinary request, database and
serving-generation admission. GET and read-only Hash, Set, List, Sorted Set/GEO
and Stream commands share this path. Transaction-owned reads, multi-key reads
and blocking operations retain their owner-side coordination. Repeated random
replies retain their existing bounded snapshot streamer.

A short index reader guard on the key owner protects a complete-key compact
location snapshot. Payload reads pin the block and validate its allocation
identity before dropping the index guard. Only durable, non-grouped,
non-external records of the expected type whose block owner is also their key
owner qualify, and the connection worker must have device I/O access. Length
commands can answer from the same protected index metadata without a block pin
or payload buffer. Closed admission, unsupported representations, expired
entries, concurrent admission contention, and an exhausted connection-worker
read-buffer pool whose temporary allocation also fails return to the normal
owner route. Failed oversized allocation also takes that route, where the
record reader can use fixed-slot chunks. Stream and Sorted Set whole-value
callbacks exceeding the bounded inline workspace also retain the owner's
scratch-admission path. Index misses use the same transaction and population
admission checks.

An admitted payload read submits I/O directly from an awaitable in the
requesting coroutine. Completion validates the record and population and
transfers a buffer lease over the immutable encoded payload. GET adds bulk
framing in that buffer; collection handlers share their compact evaluation and
reply logic with owner reads, retaining owned results through reply streaming.
Command parsing precedes consumption of any captured read error. No borrowed
index entry survives suspension.

The index snapshot orders the read before a subsequently admitted writer. The
block pin survives I/O and cancellation cleanup, so writes can append and retire
old live-byte accounting while the read finishes. Defrag closes remote pin
admission before inspecting the count; the last pin on a pending block asks its
owner to reconsider reclamation. Flush uses the same close-before-count order
when retiring a sealed staging buffer: either it releases the buffer immediately
or the last pin notifies the owner to finish the release. Local fallback reads
retain their existing pin rules. Database/replication epoch and population-generation
changes invalidate an in-flight payload read. Worker shutdown drains I/O and
destroys suspended frames on all workers before destroying any remotely
referenced index, arena or block state.

An owner-local ordinary single-key GET may resolve its complete inline-key index entry
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
an aligned lease. `LoadValue` and its single-location `LoadValueLocal` share an
awaitable embedded in the caller's task. Local inline reads, including staged
copies, use that task directly. Buffer contention retains the pool's FIFO
handoff and schedules an owner-local continuation under the caller's task class;
it does not require a waiter coroutine. The awaitable owns the disk-read pin
and buffer through completion and validates record identity, checksums, and
population before returning it. Grouped reads enter their graph reader directly;
cross-owner, external, and relocation flows use lazy helper tasks. A stale
logical read resolves the current same-mutation location before resuming its
caller, while a single-location read returns the stale status to its enclosing
representation-specific retry loop.

Worker-local MGET uses `BatchGetLocked` after the command has acquired its key
locks. It classifies index entries in one coroutine and issues ordinary-size
local inline records in waves paced by the fixed read-buffer pool; oversized
records use aligned overflow leases or fall back to the complete reader if
allocation fails. One completion barrier covers every I/O in a wave, and all
leases are returned before a chunked fallback, relocation retry, or the next
wave can suspend. Staged, remote, external, indirect-key, and stale-validation
cases fall back to the complete `GetLocked` state machine, preserving the ordinary
identity and relocation checks without one coroutine per key.

Before returning disk bytes, storage validates block and record allocation
epochs, record identity, database and replication epochs, mutation sequence,
type, key, and checksums. A pin prevents the block from being physically
released while I/O is active. Defrag can change the physical location without
taking the reader's key lock; a stale physical read aborts and follows the
current same-sequence relocation, while a true logical mutation becomes a
normal miss.

External values read and validate every extent on its current owner and
assemble the logical value in manifest order. Whole-extent and slice reads
share a physical-read awaitable that retains its pin across buffer waiting,
I/O, and validation. Slices validate the entire extent before copying any
requested bytes. Local extent routing and physical reads need no child task;
cross-owner dispatch retains the runtime's task bridge. The output buffer
outlives every extent read, including on error. A move-only
`ReadBufferLease` may cross to the connection worker; destruction returns a
registered slot to its storage owner.

### Flush and durability

A configured dirty-byte threshold or periodic interval, a full block, an
extent-dependent overwrite, a transaction fence or commit, defrag, and shutdown
can all request a flush. The threshold starts work without requiring a full
submission-sized batch; explicit durability requests also flush smaller tails.
The flush snapshots the staged prefix, zero-pads its tail to a direct-I/O page,
and advances the append cursor to the following page so an already durable data
page is never modified again. Appends can accumulate beyond that immutable
snapshot while its I/O is in flight. Submission sizes follow the available
snapshot data within a bounded I/O size, independently of the trigger threshold.

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
slots. A dirty tail appended while a snapshot is in flight is requeued when it
reaches the trigger threshold, without waiting for the periodic timer. A sealed
stream's remaining tail is requeued at the front regardless of size, preserving
append and copy-on-write child-before-root ordering.

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
fall back to every other configured device; SPDK allocates only from its home
devices, even when spare qpairs give it read access to other controllers.
Foreground allocation may wait while an active flush, extent reclaim, or
runnable defrag can still return
space; it reports exhaustion only after a stable observation with no progress
in flight. A queued defrag while defrag is paused is deliberately not counted
as runnable progress, so a full-device write reports exhaustion instead of
waiting forever. Resuming defrag allows later allocation to wait for and use
the reclaimed block.

A foreground writer that has released worker store state also waits for an
already running transaction-cleaner round before treating exhaustion as final.
Returning a block wakes capacity waiters immediately; round completion wakes
them even if leases or pins prevented reclamation. Each allocation joins at
most one round, so the cleaner's persistent retry loop cannot make a writer
wait for its own live transaction. Relocation uses the protected reserve and
does not join this foreground wait.

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
and its pins drain. UUID dependencies are released only after source
retirement is durable. Per-device permits and the protected reserve
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
into a storage failure. Initial installation binds the same atomic lease
deadline used by request and native-source admission. Ordinary same-epoch
renewal updates this shared deadline and preserves queued capabilities;
renewal after expiry or revocation installs a distinct epoch after cleanup.
Workers cache ownership of the shared capability through independent reference
counts; queued candidates retain their captured capability across cache refresh.
Validity is never cached: admission and final mutation checks still observe the
capability's revocation state and shared lease deadline.
Disabling expiration on local owner loss revokes the shared epoch immediately,
so an outstanding Meta Ack cannot renew stale owner authority.

`QuiesceExpiration` is a nestable drain independent of authority revocation.
Controlled failover holds it after request mutation drain while the old owner
captures a stable replication frontier, and a replacement of that same pause
transfers the hold without reopening expiration between desired states. The
hold is released only when the source-pause intent disappears. Tomb Raider is
not governed by the finite capability or this pause: its authority is the
completeness and availability of the local physical population, independently
of request-serving authority.

Deletion normally publishes a durable tombstone before retiring the value,
remaining safe across wall-clock rollback. If foreground space is exhausted,
runtime expiration and startup cleanup may instead remove an unshielded expired
value from the indexes and live-byte accounting to restore write capacity.
Shielding values require a tombstone because an older durable value could
otherwise reappear. For grouped values, the root and side view leave the
indexes together and the auxiliary graph is retired; UUIDs remain dependencies
of their source record blocks. A surviving root can therefore name reclaimed
children; [grouped recovery](09-grouped-collections.md#recovery-reclamation-and-snapshots)
defines when that incomplete graph may be discarded. This full-device exception
relies on the on-disk deadline: clock rollback before durable deletion can make
the graph unrecoverable, but never authorizes returning an older live version.

Storage owns one permanent Tomb Raider scheduler on worker zero. The scheduler
checks the configured schedule and local population gate before admitting a
round. Complete, usable populations are eligible on standalone and Meta-managed
nodes, including replicas disconnected from their source. Owner leases,
physical roles, and transport connectivity do not grant or revoke this local
maintenance capability. Tomb Raider marks tombstones and shielding values as
initially unclaimed, sweeps all ordinary record blocks
including staged prefixes, claims candidates for which an older unexpired
value still exists, and only then clears stale shielding or erases unclaimed
tombstones. Runtime block removal can precede durable allocation-bitmap
retirement, so an absent runtime block alone never proves that its old values
are unrecoverable. Before capturing its local block set, a sweep waits for
pending record-block retirements to become durable. If a captured block is
being retired, the sweep waits for that retirement, then skips the obsolete
allocation and continues the same round. Claims already made from that block
remain conservative: they can retain a tombstone for another round, but cannot
permit premature deletion. The round state is process-local. A crash or shutdown
can forfeit a round because recovery reconstructs the conservative tombstone
and shielding state and the next round repeats the proof.

Index capacity follows removal from the in-memory index: replacing a value with a
tombstone retains its slot, while erasing the entry enables incremental bucket
shrinking. Worker-local maintenance completes pending rehashes even without
foreground requests or expiring keys, including on replicas. It shares the
expiration pause/drain boundary used by stable scans and shutdown checkpoints.
Shrinking preserves entry addresses and the cursor guarantee that continuously
present entries are visited at least once; a cursor can revisit merged buckets.
Memory admission can defer shrinking without preventing entry removal.

The population gate stays closed through startup recovery and import. Storage
uses its private population generation to close the gate, request an active
round to forfeit at a safe checkpoint, and drain it before a destructive FULL
reset or source-less population initialization. Replication reports population
begin, final completion, and joined abort; it does not manage Tomb Raider or
carry maintenance tokens. Only successful completion of the current population
change can reopen the gate; an interrupted rebuild and stale completion
callbacks cannot expose partial data to cleanup. Native FULL
completion includes root promotion and installation of every flow's final cut;
Redis Cluster import includes every source in the session. Shutdown closes and
drains the gate before storage freezes, and a fatal storage fault keeps it
closed. These internal boundaries preserve the configured schedule. The
user-facing `TOMBRAIDER OFF` only prevents future rounds and lets a running
round finish.

### Database and transaction cleanup

`FLUSHDB` and `FLUSHALL` advance monotonic database epochs. Storage persists the
new epoch values to every device before publishing them in memory and detaching
the affected per-partition indexes. Recovery therefore rejects the old
population even if the process stops before online reclamation finishes. The
database is observably empty after detach; SYNC waits for detached-index
retirement, while ASYNC ensures the same background reclaimer runs without
waiting for it. Reclamation is started on every worker before SYNC awaits any
worker, and runs after command gates, publication order and Group drain are
released. A wait or retirement error propagates without repeating the committed
flush or completed retirement. Neither SYNC nor ASYNC promises that every
obsolete disk extent is immediately reusable.

Tomb Raider can overlap FLUSH without a whole-round drain. A round binds its
mark/sweep/reap proof to database epochs and worker-local index generations;
the latter distinguish local detach from the earlier global epoch publication.
Changing either invalidates the whole proof and the next round starts again.
After asynchronous work, cleanup revalidates that context before modifying the
index. Final erasure retains the local key and store-state locks shared with
index replacement, and already-erased records complete retirement accounting
before cancellation. Owned handles and physical allocation-epoch checks protect
record lifetime separately from logical generation checks. Detached-index
reclamation remains the owner of the old FLUSH population.

Managed command callers pass a mutation precondition down to device epoch
persistence. After the device owner acquires its allocator lock and IO buffer,
the first write validates that precondition before changing pending metadata.
The serial device loop shares one commit-start flag: later devices and detach
cannot cancel an already started commit on authority revocation. Standalone,
trusted replay and internal recovery omit the precondition. Short writes and
write/synchronization failures propagate and latch the existing storage fault;
FLUSH adds no retry or online repair. Recovery takes the maximum valid epoch
across devices, including a partial device-set commit, so an IO error does not
prove that the old keyspace survives. The A/B page format is unchanged.

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
path, and UUIDs remain a `retired-unreclaimed` dependency until
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

Transaction cleaning seals a Tx block when its append stream rolls over, has
received no append for one minute, or has a nearly full tail with no active
writer at a cleaner round. Once the block is durable and every transaction
represented in it has settled, the cleaner can relocate its
current committed winners into ordinary untagged blocks and discard aborted
or obsolete versions. It awaits each relevant commit decision's durability
before promotion. A block carrying a commit decision remains allocated until
every Tx block naming that transaction or grouped batch has no live tagged
winner, dependency pin, or unpaid relocation fence. Durable ordinary copies
can survive a crash without the old decision even while their Tx source blocks
remain allocated. Destination durability, source pins, and the allocation
bitmap retirement still order its cold-free return;
UUID and extent dependencies follow the same source-block lifetime.

Promotion and retirement are separate phases. A successful scan can leave
obsolete records charged to the source until their successors' commit
retirements settle. The cleaner remembers completed scans while the relevant
state remains unchanged, checks retirement conditions in memory, and retains
any outstanding destination durability obligations. Accounting or dependency
progress permits another scan; rollback that restores earlier winners also
invalidates remembered scans before those winners can be overlooked.

Each worker keeps one transaction append stream. The active append block does
not count toward the reported sealed Tx-record backlog. That backlog describes
cleanup work rather than a write-admission limit. A sealed block can return its
staging buffer after flushing and releasing read pins while its physical
allocation remains protected by transaction dependencies. Writers can reuse
that memory for a new physical block without awaiting reclamation of the old
block; the buffer pool and allocator enforce their actual capacity limits.

Sealing a block and releasing a transaction's final writer lease request one
coalesced background cleaner. It continues while sealed work remains, backing
off when leases, pins, or retryable failures prevent progress. Periodic flush
also starts pending cleaning, including progress originating outside a worker.
Standalone grouped writes request this work without joining its completion;
serious cleaner errors remain observable by subsequent standalone grouped
writes. Online cleaning can be disabled through its runtime configuration.

Transaction receipts hold a shared lease until the transaction can no longer
append. The cleaner first observes released leases, then takes a second
worker-wide snapshot of block membership and commit decisions. That ordering
makes the membership of eligible transactions complete even when
workers append concurrently. A final snapshot after promotion confirms that
no live tagged winner still needs each decision. Online cleaning yields to
shutdown between record relocations, retaining the source allocation and any
outstanding destination durability fences. Shutdown joins queued and running
background cleaner work before freezing append streams. The shutdown flush
makes staged copies durable before publishing its clean-shutdown proof. When a
shutdown checkpoint is enabled, worker 0 ignores the online cooldown and
completes this lifecycle to a fixed point after commit and flush drain;
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
| `tests/tomb_raider_e2e_test.cpp` | Runtime scheduling, retain/reap behavior for buried persistent or expired values, user OFF completion semantics, population-change drains, and FLUSH generation invalidation |
| `tests/multikey_e2e_test.cpp` | Bounded disk MGET waves, commit batching and fence merging, transaction-block retirement, recovery, FLUSHDB invalidation, rollback, and retry |
| `tests/atomicity_stress_e2e_test.cpp` | Overlapping multi-key serializability and recovery after a graceful durability drain |
| `tests/list_e2e_test.cpp` | Function-catalog body/root/runtime crash windows, multi-device torn-root fallback, and shielded expired-winner behavior under an injected recovery clock rollback |
| `tests/buffer_pool_test.cpp` | Reuse of a waiting storage write-buffer acquisition |
| `tests/grouped/hash_test.cpp` | Group codecs, incremental routing, mutation planning and transaction-adjudicated recovery-model validation |
| `tests/grouped/object_index_test.cpp` | Side-index identity, copy-on-write updates, retirement markers, pre-admitted publication and retained-memory rollback |
| `tests/grouped/recovery_e2e_test.cpp` | Real grouped disk images, child/outer decisions, physical corruption, worker reassignment, GC crash windows, snapshots and graph detachment |
| `LargeHashDurabilityE2eTest` in `tests/list_e2e_test.cpp` | Large Hash extent-write and GC crash recovery, bounded-device reclamation, and RESP OOM atomicity; dedicated grouped suites also cover graph publication and relocation boundaries |
| `tests/device_affinity_test.cpp` | SPDK controller quota and qpair-owner planning across balanced, weighted, and controller-heavy layouts |

## Known gaps and documentation limits

- Reset zeroes only fixed metadata and does not securely erase the data region.
  Current tests do not prove crash-safe stale-media sanitization between bitmap
  activation and first flush for reset media or a reused zero-label added
  device. Operators should provision genuinely empty added media when that
  property matters.
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
- [Running with SPDK](../operations/spdk-storage.md)

The linked design documents are historical references, while the operations
documents are procedural guidance. This focused architecture document and
current source code are authoritative for present storage behavior.

## Source map

| Claim | Repository source |
|---|---|
| Public lifecycle, routing, typed operations, locked transaction contract, snapshots, epochs, maintenance, and durability interfaces | `include/lavik/storage/engine.h` |
| Worker, partition, block, append-stream, allocator, recovery, and background-maintenance state | `src/storage/engine/impl.h` |
| Runtime index representation, shared entry arena, runtime key digests, and asynchronous entry-identity validation | `include/lavik/storage/scan_hash_map.h`, `include/lavik/storage/format.h`, `src/storage/format.cpp`, `src/storage/engine/impl.h`, `src/storage/engine/init.cpp`, `src/storage/engine/write.cpp`, `src/storage/engine/flush.cpp` |
| Compact and grouped Hash/Set serving, group identity and object side index | `src/storage/engine/hash_tree.cpp`, `src/storage/engine/hash_codec.cpp`, `src/storage/engine/grouped/hash.cpp`, `src/storage/engine/grouped/object_index.cpp`, [Grouped collections](09-grouped-collections.md) |
| Compact physical index representation shared by user-key and group-location indexes | `include/lavik/storage/detail/record_location.h`, `include/lavik/storage/detail/record_index.h` |
| Persistent constants, device and block IDs, A/B metadata pages, record and extent layouts, and checksums | `include/lavik/storage/format.h`, `src/storage/format.cpp` |
| Checkpoint serialization, bitmap validation, generation publication and consumption, fallback, and block retirement | `src/storage/engine/checkpoint.cpp`, `src/storage/engine/flush.cpp`, `src/storage/engine/init.cpp`, `src/storage/engine/recovery.cpp` |
| System-state manifest, catalog COW extents, full-sync fence, population token, promotion base, and one-use shutdown certificate | `src/storage/engine/system_state.cpp`, `include/lavik/storage/engine.h` |
| Aligned buffer ownership, registered-I/O fallback, oversized reads, and cross-worker lease return | `include/lavik/storage/buffer_pool.h`, `src/storage/buffer_pool.cpp` |
| Storage-path probing, device-set validation and expansion, controller/qpair affinity, metadata load, worker initialization and native-thread finalization, recovery barriers, and shutdown flush | `src/storage/engine/init.cpp`, `src/storage/engine/device_affinity.h`, `src/storage/engine/impl.h` |
| Device-owner allocation, bitmap activation and cold-free retirement, epoch mirroring, reserves, and allocator fail-stop behavior | `src/storage/engine/alloc.cpp` |
| Temporary Redis export block allocation, framed command stream, readback, release, and restart discard | `src/storage/engine/redis_export_backlog.cpp`, `src/storage/engine/recovery.cpp` |
| Parallel scans, block reassignment, epoch filtering, transaction decision collection, winner selection, and recovery accounting | `src/storage/engine/recovery.cpp`, `src/storage/engine/init.cpp` |
| Append streams, mutation precondition, extent construction, WATCH/index publication, replacement accounting, transaction fences, commit batching and backpressure, commit decisions, caller wait policy, and rollback | `include/lavik/storage/engine.h`, `src/storage/engine/write.cpp`, `src/storage/engine/hash_tree.cpp`, `src/redis/command.cpp`, `src/redis/list_command.cpp`, `src/redis/sort_command.cpp` |
| Worker-sharded retained-memory admission and ownership, detached-index reclaim, client-buffer quotas, full-sync reservations, and RDB snapshot admission failure | `include/lavik/memory.h`, `src/memory.cpp`, `include/lavik/retained_allocator.h`, `include/lavik/storage/scan_hash_map.h`, `src/storage/engine/replication.cpp`, `src/storage/engine/backup.cpp` |
| Staged and disk reads, bounded BatchGet waves, validation, pins, relocation retry, external-value assembly, and disk-backed reply leases | `src/storage/engine/read.cpp`, `include/lavik/storage/engine.h` |
| Periodic flush snapshots, data-before-header ordering, alternating header commits, dirty-tail ordering, and retirement settlement | `src/storage/engine/flush.cpp` |
| Extent reclaim, defrag candidate selection, relocation durability fences, source retirement, and pacing | `src/storage/engine/defrag.cpp` |
| Lazy and active expiration, permanent and finite authority capabilities, nestable quiescence, durable tombstones, and the full-device escape valve | `include/lavik/storage/engine.h`, `src/storage/engine/expire.cpp`, `src/cluster/node_control.cpp`, `src/replication/replication.cpp` |
| Tombstone and shielding mark/sweep/reap lifecycle, storage-owned scheduling, population eligibility, and generation-checked FLUSH concurrency | `include/lavik/storage/engine.h`, `src/storage/engine/tomb_raider.cpp`, `src/storage/engine/init.cpp`, `src/storage/engine/flush_db.cpp`, `src/replication/replication.cpp`, `src/replication/redis_replication.cpp` |
| Durable database and replica-partition epoch advance, bounded index detach, replica reset/promotion/abort, and detached-index reclaim | `src/storage/engine/flush_db.cpp`, `src/storage/engine/replication.cpp` |
| Transaction-block promotion, commit-decision lifetime, and cold retirement | `src/storage/engine/tx_cleaner.cpp` |
| Device, durability, recovery, storage-I/O, Defrag, Tomb Raider, and transaction-cleaner observability | `include/lavik/storage/engine.h`, `src/storage/engine/metrics.cpp`, `src/storage/engine/recovery.cpp`, `src/metrics.cpp` |
| Format, capacity, catalog recovery, crash-window, expiration, reclamation, transaction-cleaner, and buffer-pool verification | `tests/storage_format_test.cpp`, `tests/storage_capacity_test.cpp`, `tests/multi_exec_e2e_test.cpp`, `tests/extent_recovery_e2e_test.cpp`, `tests/flushdb_reclaim_e2e_test.cpp`, `tests/ttl_e2e_test.cpp`, `tests/tomb_raider_e2e_test.cpp`, `tests/multikey_e2e_test.cpp`, `tests/atomicity_stress_e2e_test.cpp`, `tests/list_e2e_test.cpp`, `tests/buffer_pool_test.cpp` |
