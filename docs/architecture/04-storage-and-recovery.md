# Storage and recovery

## Responsibility and boundary

The storage subsystem owns Keylane's in-memory top-level key indexes and its
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

The top-level key indexes are runtime state, not a separate persistent lookup
structure. Recovery rebuilds them from committed records. Collection values
may contain storage-managed tree or compact encodings, but their current root
is still selected through the same top-level record index.

## Ownership and runtime state

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
workers use Celer cross-core submissions to read or mutate it.

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
Celer SPDK storage paths. `Prepare` probes and validates them; it does not
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
  allocation-bitmap page 0 slot A, page 0 slot B
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

Epoch metadata contains 16 database epochs followed by 16,384 partition
replication epochs. The allocation bitmap has one bit per physical block. Each
logical metadata page has independent 4 KiB A/B slots with a generation and
CRC32C checksum. Readers select the valid higher generation. An all-zero pair
is uninitialized logical zero; a nonzero pair with no valid slot is corruption.

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
kinds are ordinary records, payload extents, and transaction generations;
on-disk enum value 3 remains deliberately unassigned.

Records are 8-byte aligned and carry their database and value type, key
representation, logical and physical sizes, transaction ID, database and
replication epochs, logical mutation sequence, absolute expiration time,
physical LSN, allocation epoch, and payload and header checksums. A key digest
is deliberately absent: it is process-random runtime state reconstructed from
the complete key. Record kinds are value, tombstone, and keyless transaction
commit decision. Both the storage write boundary and recovery decoder enforce
the durable 512 MiB maximum key length; a wider internal or replication
protocol argument limit cannot create a record that a restart would reject.

The version-1 record wire layout has a 72-byte base header at explicit byte
offsets. A nonzero transaction ID and expiration timestamp each add one aligned
8-byte extension, so fixed metadata is 72, 80, or 88 bytes. The base packs
record kind, database, value type, external-payload state, external-key state,
and extension presence into one 16-bit word. Header length is derived from
those flags and key length; total record length is derived from header and
payload length. Neither derived length is stored. The decoded `RecordHeader`
is a runtime view rather than a persisted C++ object representation.

This compact version-1 record layout directly replaces the earlier 104-byte
version-1 layout without changing the format number; there is no compatibility
decoder. Media written by the earlier layout must be reset before this build
starts. Compact Hash/Set values likewise retain version 1.

Keys that do not fit the configured inline header limit move into the payload.
Large key/value payloads use a root record containing an extent manifest. Each
extent reference identifies a dedicated extent block by block ID, allocation
epoch, byte count, and payload checksum. The extent block header repeats its
index, length, and checksum so reads and recovery can validate the complete
root-to-child identity.

## Startup and recovery

### Preparation

`Prepare` runs before Celer workers start. It validates the worker count,
inline-key limit, flush alignment, per-device defrag concurrency, configured
paths, capacities, and membership. A fresh regular file must be an 8 MiB
multiple. A raw device uses its complete 8 MiB blocks and ignores a shorter
tail. With the current fixed metadata and eight-block per-device defrag reserve,
each device needs at least 80 MiB to leave one foreground block.

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
repairs stale fields on a lagging member.

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
3. A valid extent header contributes extent identity. A valid records or
   transaction block is read through its committed boundary; zero page padding
   is skipped, while record bounds, allocation epochs, topology, keys, and
   checksums are validated. Recovery computes each winning key's runtime
   digest from the recovered complete key instead of loading one from disk.
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
appended to the current worker's ordinary or transaction-generation staging
block. The in-memory index is updated immediately and may point at staged bytes
that have not crossed a crash-durability boundary. Staged reads use that buffer
directly.

Runtime indexes retain key identity, logical version, record coordinates, and
the state needed to serve the current value. The physical block's owner and
allocation epoch live once in `BlockState` rather than being repeated for
every key. Before a location crosses an ownership boundary or a suspension,
the key owner reads that published immutable block identity and materializes a
self-contained `RecordLocation`. The physical owner validates the snapshot on
use, preserving block-reuse and ABA protection. These index representations
are runtime-only; durable block and record headers retain the fields needed for
restart validation.

Runtime key digests use one operating-system-seeded SipHash key per process.
They are consistent across workers for that process, change on restart, and do
not affect Redis-slot routing. External keys and decoded Hash or Set fields
retain or reconstruct a digest only as a lookup aid; collisions are verified
against complete keys, and process-local fingerprints are never persisted.

All partition and logical-database indexes on a worker allocate entries from a
shared worker-local arena, so sparse indexes share capacity instead of
stranding it at partition boundaries. A population detached by `FLUSHDB`
retains ownership of that arena until asynchronous reclamation finishes.
Optional per-key state such as expiration may change an entry's concrete
representation. Those replacements are owner-serialized, and staged flush,
coroutine, and transaction-undo state revalidates or retargets its saved entry
identity before use rather than relying on an object's former lifetime.

Index growth is failure-atomic. Before appending a record whose publication
needs a new or replacement entry, storage admits the required entry and table
capacity from the current worker's memory share. Incremental expansion likewise
prepares its destination capacity before removing source entries. Admission or
index-capacity failure returns `ResourceExhausted` without publishing a record
that cannot enter the index; allocation failure during expansion leaves both
tables searchable and the maintenance step retryable.

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

Reads hold the key's shared transaction lock while resolving the current index
entry. Tombstones and expired values are invisible; an expired observation can
enqueue a bounded active-expiration candidate. A staged location is copied or
framed from its write buffer. A disk location is read on its physical block
owner into an aligned lease.

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

The first flush durably clears the header slot not selected for the new
allocation before committing the selected slot. Later header writes alternate
slots. A dirty tail appended while a snapshot is in flight is requeued at the
front when a sealed stream completes, preserving append and copy-on-write
child-before-root ordering.

An ordinary successful command is therefore not necessarily crash-durable at
reply time. `StorageDurabilityStats` reports dirty staging bytes, pending
flushes, and pending transaction decisions for operators and tests that need a
durability fence. Graceful shutdown gives accepted queued and explicitly
background transaction commits up to five seconds to append their decisions,
then seals every active stream and drains flushes, extent reclaims, and
retirement accounting. An I/O failure fail-stops further writes and retains
staging buffers so already staged reads do not follow recycled memory.

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

Read paths treat an expired value as absent immediately. On an expiration
authority node, each worker also runs a bounded 10 ms active cycle that scans
at most 256 map steps and deletes at most 64 validated candidates. The worker
rechecks mutation sequence and deadline under the key's exclusive lock before
acting.

The preferred deletion is a durable tombstone, which remains safe if the wall
clock later moves backward. If foreground space is completely exhausted, an
unshielded expired value can be removed only from memory and physical live-byte
accounting; its on-disk deadline still makes it expired at ordinary recovery
time, and the freed block can restore write capacity. A shielding value cannot
use this escape valve because an older durable value could reappear.

Tomb Raider is a separate, optional cleanup loop launched at worker startup
only when the node is then the expiration authority. Once launched, the loop
does not recheck that authority: a later `REPLICAOF` role change can leave it
scheduled on a replica. It marks tombstones and shielding values as initially
unclaimed, sweeps all ordinary record blocks including staged prefixes, claims
candidates for which an older unexpired value still exists, and only then
clears stale shielding or erases unclaimed tombstones. The round state is
process-local. A crash or shutdown can forfeit a round because recovery
reconstructs the conservative tombstone and shielding state and the next round
repeats the proof.

### Database and transaction-generation cleanup

`FLUSHDB` and `FLUSHALL` advance monotonic database epochs. Storage persists the
new epoch values to every device before publishing them in memory and detaching
the affected per-partition indexes. Recovery therefore rejects the old
population even if the process stops before online reclamation finishes. The
database is observably empty after detach; SYNC waits for detached-index
retirement, while ASYNC ensures the same background reclaimer runs without
waiting for it.

Transaction cleaning rotates record-bearing generations, seals and flushes
their blocks, collects committed decisions, and relocates current committed
tagged winners into ordinary untagged record blocks. A generation is returned
through the cold-free lifecycle only when it is sealed and durable and has no
active transaction leases, live tagged bytes, or dependency pins.

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
- Recovery considers an expired or tombstone winner before older versions;
  cleanup cannot remove its suppression while an older live record remains.
- Allocation epochs accompany physical references, reads, accounting, and
  relocations so delayed work cannot affect a later incarnation of one block.
- Fixed-metadata and storage write ambiguity is fail-stop. The engine does not
  continue allocating or appending after it can no longer prove durable state.

## Observability and verification

The engine exposes durability status, per-device capacity and available block
metrics, filesystem free space for regular-file devices, and Defrag, Tomb
Raider, transaction-cleaner, and transaction-commit coordinator totals. INFO
STATS reports commit batch and transaction counts, input and merged fences,
queue depth and peak, backpressure waits, and the 4096-receipt watermark.
Prometheus also aggregates Celer's completed storage read, write, and
`fdatasync` counters and publishes server readiness; recovery reports periodic
progress in the log. Runtime configuration can pause or pace defrag and select
Tomb Raider off, interval, or daily scheduling. Engine snapshots are collected
on the worker or allocator that owns the underlying mutable state.

Current test evidence includes:

| Test | Evidence |
|---|---|
| `tests/storage_format_test.cpp` | Label, metadata, block, record, extent, and transaction encoding; CRC rejection; A/B winner and torn-slot fallback |
| `tests/storage_capacity_test.cpp` | Existing-file requirement, alignment and minimum capacity, persisted capacity, expansion membership, foreign-device rejection, and explicit reset |
| `tests/extent_recovery_e2e_test.cpp` | External keys and values, manifest/extent recovery, reclamation, and repeated worker-count changes |
| `tests/flushdb_reclaim_e2e_test.cpp` | Full-device FLUSHDB reclaim, paused-defrag exhaustion and resume, expiry escape valve, stale activated-header handling, and a crash after durable defrag source retirement |
| `tests/ttl_e2e_test.cpp` | TTL mutation, disk-resident rewrite, expired/live restart behavior, and extent-backed values |
| `tests/tomb_raider_e2e_test.cpp` | Runtime scheduling plus retain/reap behavior for buried persistent or expired values |
| `tests/multikey_e2e_test.cpp`, `tests/tx_cleaner_test.cpp` | Bounded disk MGET waves, commit batching and fence merging, transaction-generation rotation, recovery, FLUSHDB invalidation, rollback, retry, and exact retirement readiness |
| `tests/atomicity_stress_e2e_test.cpp` | Overlapping multi-key serializability and recovery after a graceful durability drain |
| `tests/list_e2e_test.cpp` | Shielded expired-winner behavior under an injected recovery clock rollback |
| `tests/buffer_pool_test.cpp` | Reuse of a waiting storage write-buffer acquisition |
| `tests/device_affinity_test.cpp` | SPDK controller quota and qpair-owner planning across balanced, weighted, and controller-heavy layouts |

## Known gaps and documentation limits

- Reset zeroes only fixed metadata and does not securely erase the data region.
  Current tests do not prove crash-safe stale-media sanitization between bitmap
  activation and first flush for reset media or a reused zero-label added
  device. Operators should provision genuinely empty added media when that
  property matters.
- A runtime role change can revoke expiration authority without stopping an
  already launched Tomb Raider loop; the loop does not currently recheck the
  role before later cleanup rounds.
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
| Public lifecycle, routing, typed operations, locked transaction contract, snapshots, epochs, maintenance, and durability interfaces | `include/keylane/storage/engine.h` |
| Worker, partition, block, append-stream, allocator, recovery, and background-maintenance state | `src/storage/engine/impl.h` |
| Runtime index representation, shared entry arena, process-local key digests, and asynchronous entry-identity validation | `include/keylane/storage/scan_hash_map.h`, `include/keylane/storage/format.h`, `src/storage/format.cpp`, `src/storage/engine/impl.h`, `src/storage/engine/write.cpp`, `src/storage/engine/flush.cpp` |
| Persistent constants, device and block IDs, A/B metadata pages, record and extent layouts, and checksums | `include/keylane/storage/format.h`, `src/storage/format.cpp` |
| Aligned buffer ownership, registered-I/O fallback, oversized reads, and cross-worker lease return | `include/keylane/storage/buffer_pool.h`, `src/storage/buffer_pool.cpp` |
| Storage-path probing, device-set validation and expansion, controller/qpair affinity, metadata load, worker initialization and native-thread finalization, recovery barriers, and shutdown flush | `src/storage/engine/init.cpp`, `src/storage/engine/device_affinity.h`, `src/storage/engine/impl.h` |
| Device-owner allocation, bitmap activation and cold-free retirement, epoch mirroring, reserves, and allocator fail-stop behavior | `src/storage/engine/alloc.cpp` |
| Parallel scans, block reassignment, epoch filtering, transaction decision collection, winner selection, and recovery accounting | `src/storage/engine/recovery.cpp`, `src/storage/engine/init.cpp` |
| Append streams, extent construction, index publication, replacement accounting, transaction fences, commit batching and backpressure, commit decisions, caller wait policy, and rollback | `src/storage/engine/write.cpp`, `src/redis/command.cpp`, `src/redis/list_command.cpp`, `src/redis/sort_command.cpp` |
| Worker-sharded retained-memory admission and ownership, client-buffer quotas, full-sync reservations, and RDB snapshot admission failure | `include/keylane/memory.h`, `src/memory.cpp`, `include/keylane/storage/scan_hash_map.h`, `src/storage/engine/replication.cpp`, `src/storage/engine/backup.cpp` |
| Staged and disk reads, bounded BatchGet waves, validation, pins, relocation retry, external-value assembly, and disk-backed reply leases | `src/storage/engine/read.cpp`, `include/keylane/storage/engine.h` |
| Periodic flush snapshots, data-before-header ordering, alternating header commits, dirty-tail ordering, and retirement settlement | `src/storage/engine/flush.cpp` |
| Extent reclaim, defrag candidate selection, relocation durability fences, source retirement, and pacing | `src/storage/engine/defrag.cpp` |
| Lazy and active expiration, authority and quiescence, durable tombstones, and the full-device escape valve | `src/storage/engine/expire.cpp` |
| Tombstone and shielding mark/sweep/reap lifecycle, startup authority check, and runtime role limitation | `src/storage/engine/tomb_raider.cpp`, `src/storage/engine/init.cpp`, `src/replication/replication.cpp` |
| Durable database epoch advance, bounded index detach, and online detached-index reclaim | `src/storage/engine/flush_db.cpp` |
| Transaction-generation rotation, promotion, readiness, and cold retirement | `src/storage/engine/tx_cleaner.cpp`, `include/keylane/storage/tx_cleaner.h` |
| Device, durability, recovery, storage-I/O, Defrag, Tomb Raider, and transaction-cleaner observability | `include/keylane/storage/engine.h`, `src/storage/engine/metrics.cpp`, `src/storage/engine/recovery.cpp`, `src/metrics.cpp` |
| Format, capacity, recovery, crash-window, expiration, reclamation, transaction-cleaner, and buffer-pool verification | `tests/storage_format_test.cpp`, `tests/storage_capacity_test.cpp`, `tests/extent_recovery_e2e_test.cpp`, `tests/flushdb_reclaim_e2e_test.cpp`, `tests/ttl_e2e_test.cpp`, `tests/tomb_raider_e2e_test.cpp`, `tests/multikey_e2e_test.cpp`, `tests/atomicity_stress_e2e_test.cpp`, `tests/list_e2e_test.cpp`, `tests/tx_cleaner_test.cpp`, `tests/buffer_pool_test.cpp` |
