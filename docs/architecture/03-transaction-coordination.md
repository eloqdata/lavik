# Transaction coordination

## Responsibility and boundary

The transaction subsystem serializes conflicting key access across Keylane's
worker-affine storage shards. It provides shard-local shared/exclusive locking
for ordinary storage operations, a txid-ordered wait queue for contention, and
a coordinator for commands that must hold keys on several workers or across
several callback hops. Storage background work uses the same arbitration path
as client commands, so expiry, replica apply, backup, and reclamation do not
bypass foreground key locks.

This subsystem owns runtime ordering, not stored values or durable commit
decisions. A `tx::Transaction` scheduling id orders queue entries only. Atomic
multi-key writes separately use `storage::TxShardWrites`, undo journals,
durability fences, and `kTxCommit` records owned by the command and storage
subsystems. Both kinds of identifier currently come from
`TxRuntime::next_txid_`, but one allocation does not serve both roles.

## Interfaces and state

`TxRuntime` is created once with one `TxShard` per Celer worker before the
workers start. Each worker binds its shard during `RedisService::Run`; recovery
on worker 0 later advances the shared id counter above every transaction id
found on disk. The shard's lock tables, queue, WATCH table, committed-id
watermark, and counters are mutable only on its owning worker.

The caller-facing interfaces are:

- `TxShard::AcquireKey` and `AcquireKeys`, which return an RAII `Guard` for
  ordinary owner-local work. Direct key-set callers must supply a duplicate-free
  set of `(database, fingerprint)` references.
- `Transaction::AddKey`, `Seal`, `Schedule`, `Execute`, and `Release`, which
  coordinate a command-owned transaction embedded in its coroutine frame.
- `ShardCallback`, which receives the current owner's `ShardSlice` and may
  suspend on storage I/O while locks remain held.
- `SetShardEntryHook`, which runs a non-suspending hook once on each participant
  before its first callback. The command layer uses it to enqueue that shard's
  replication transaction marker before storage I/O; a guard-level hook can
  release an already-acquired publication-order slot after the final
  participant marker enters.
- `Watch`, `WatchClean`, `Unwatch`, `MarkWatched`, and `MarkAllWatched`, which
  maintain owner-local optimistic WATCH marks.

`Seal` groups keys by owner while preserving their argument order within each
owner's slice. Callback slices retain duplicate arguments and full digests.
The lock set is separately deduplicated by `(database, fingerprint)`, with an
exclusive occurrence upgrading a shared occurrence. The fingerprint is the
first eight bytes of the storage digest; callbacks still access storage by full
digest and key, so a collision can add contention but cannot alias records.

Each logical database has a separate lock table on every worker. A lock entry
has two counter layers:

- intents cover both queued and executing operations and prevent a later
  conflicting operation from barging;
- holds cover callbacks that are currently executing or suspended and tell the
  queue when a conflicting predecessor has actually drained.

The queue contains both plain acquisition waiters and persistent transaction
entries. It is sorted by scheduling id. Plain waiters are removed before their
coroutine resumes; transaction entries retain their position until a releasing
hop. Mid-queue cancellation leaves a tombstone that the queue skips when it
reaches the front.

## Single-shard lifecycle

1. The caller adds keys and seals the transaction. `Schedule` returns without
   allocating a scheduling id because only one owner participates.
2. `Execute` submits to that owner and acquires the deduplicated key set through
   `TxShard::AcquireKeys` if the transaction does not already retain a guard.
3. Acquisition records every intent. If all intents are immediately compatible,
   it acquires holds and continues without touching the global id counter or
   shard queue. If any intent conflicts, the awaiter keeps all recorded intents,
   allocates an id, inserts a plain waiter, and suspends until it is the
   hold-compatible queue head.
4. The shard entry hook runs once, then the callback runs. A releasing execution
   resets the guard after the callback; a non-releasing execution keeps the
   same guard on the owner for the next hop.

The no-scheduling-id property applies only to lock scheduling. A multi-key
write may independently allocate a durable storage transaction id even when all
of its keys share one worker.

## Multi-shard lifecycle

1. `Schedule` allocates one scheduling id and starts a non-suspending schedule
   phase on every owner through Celer cross-core requests, invoking the local
   owner directly when applicable.
2. Each shard rejects an id at or below its committed watermark. Otherwise it
   records the transaction's intents. A conflicting transaction also rejects
   insertion before a later live queue tail, because that later position may
   already have influenced execution. Successful participants insert their
   persistent transaction node in id order.
3. If any participant rejects the schedule, a cancel phase visits only the
   successful participants, releases their intents, removes their nodes, and
   repolls. The coordinator then retries with a fresh id. The current retry loop
   is unbounded and counted, with no explicit backoff.
4. `Execute` clears participant statuses and arms every transaction node. A
   participant whose complete intent set was granted during scheduling enters
   the owner-local `bypass_ready_` queue while retaining its txid-ordered queue
   position. Such a participant is conflict-free with every earlier registered
   operation on that shard; its intents also prevent a later conflicting
   transaction from making the same claim. `TxShard::Poll` therefore dispatches
   armed bypass entries before examining the ordered queue head, subject to a
   final hold-compatibility check. This avoids cross-shard wait cycles in which
   several otherwise independent transactions are each hidden behind a lower
   txid waiting on another shard.
5. Plain waiters and transaction participants whose intents were not fully
   granted still run through the ordered queue head. Polling stops if that head
   is unarmed, already running, or conflicts with a suspended holder. Otherwise
   it publishes the entry's id to the committed watermark, acquires holds once,
   and starts the waiter or shard callback. A bypassed transaction keeps its
   ordered position and retained intents and holds under the same rules as any
   other persistent transaction entry.
6. Every participant records its callback status and decrements the transaction
   barrier. The last participant schedules the coordinator coroutine on its
   original worker. The coordinator observes all statuses only after this
   barrier completes.
7. A non-releasing hop leaves holds, intents, and queue positions in place. A
   releasing hop drops holds and intents, removes each node, and repolls its
   shard. `Release` is a no-op callback executed as such a final releasing hop.

The coordinator awaits every round it starts. A participant's barrier
decrement is its final access to the frame-embedded transaction, which prevents
the coordinator frame from disappearing while a shard callback is suspended.

## Durable commit handoff

Transaction scheduling ends when the command has completed its logical hops
and released its locks; crash-atomic storage completion has a separate
worker-local coordinator. Successful common multi-key, keyed write-capable Lua,
and EXEC paths settle their undo state, then transfer their `TxShardWrites`
receipts to the current worker's commit queue. At most one drain coroutine runs
per worker, replacing a detached coroutine per accepted transaction.

The drain coroutine removes at most 256 receipts per batch. When it finds a
backlog, it merges durability fences that name the same block owner, block ID,
and allocation epoch, retaining the largest required committed boundary. It
requests all unique flush frontiers before awaiting them so different storage
owners can progress in parallel. Each transaction still waits only for its own
tagged-record fences before appending its own `kTxCommit` decision; one receipt
on an otherwise idle queue stays on the direct low-latency path.

The queue high watermark is 4096 receipts per worker. Enqueueing always
transfers an accepted receipt, but returns a backpressure indication at or above
that depth; the command waits until the owner-local queue falls below the
watermark before replying. This bounds reply-side backlog without turning a
successful reply into a synchronous durability promise. INFO STATS exposes
batch and transaction counts, input and merged fence counts, current and peak
queue depth, backpressure waits, and the configured watermark.

Accepted queued and explicitly background commit paths increment a shared
pending count. Graceful shutdown gives that count up to five seconds to drain
before freezing append streams and starting the final storage flush. After the
deadline, shutdown proceeds without waiting further; recovery drops all tagged
records atomically whenever their durable commit decision is absent.

## WATCH integration

WATCH registration is split between connection and shard state. The
connection retains each full key, digest, owner, database, fingerprint, and its
WATCH-time liveness. The owner shard retains a sticky dirty bit per connection
under `(database, fingerprint)`; it never dereferences the connection.

Real mutation paths mark the fingerprint at the storage append funnel. Active
expiry paths that can remove a value without that append mark explicitly, and
database detach marks every registration for that database, including keys
that did not exist. Keyed `EXEC` takes its transaction locks before checking
that every shard entry remains clean and every full key's liveness matches its
snapshot. `UNWATCH`, `DISCARD`, an `EXEC` outcome that consumes the queued
transaction, and connection cleanup remove the shard registrations.

Two distinct keys can share a fingerprint and one connection's shard entry.
A write to either then dirties both watches, producing a safe false abort. The
per-key liveness snapshots prevent the shared fingerprint entry from hiding a
passive expiration or creation of the other key.

## Invariants and failure behavior

- Holds are acquired only while compatible and are released before their
  corresponding intents; therefore holds remain a subset of intents.
- Lock, queue, and WATCH mutations run on the owning worker. Cross-worker work
  moves through Celer requests and notifications, and the coordinator resumes
  on its origin worker.
- Queued plain acquisitions and conflicting transaction participants start
  only from the ordered queue head. An uncontended plain acquisition instead
  completes through `TryFastPath` without entering the queue. A fully granted
  multi-shard participant may run from `bypass_ready_` while retaining its
  ordered position; its recorded intents prove compatibility with predecessors
  and prevent a later conflicting bypass. A suspended incompatible holder can
  still delay either queued path.
- Shard callback failures do not short-circuit an active round. All participants
  reach the barrier, after which `Execute` returns the first non-OK status in
  participant order.
- A releasing hop performs lock cleanup even when its callback fails. A failed
  non-releasing hop retains locks, so the caller must issue `Release` or another
  releasing execution. The transaction subsystem does not automatically undo
  storage changes.
- Runtime rollback and crash atomicity for multi-key writes are implemented
  above and below this module: the command layer keeps transaction locks across
  read/write/finish hops, while storage restores undo state or later validates
  tagged records against a durable commit record.
- The shared id counter also supplies durable write ids and is recovery-seeded;
  consequently the `tx_ids_allocated` INFO field counts both scheduling and
  storage allocations.

## Verification and known gaps

`tests/tx_lock_test.cpp` directly covers intent/hold compatibility, queue
ordering and tombstones, the no-id fast path, anti-barging, a conflicting
waiter behind suspended I/O, and progress on an unrelated key. The multi-key
end-to-end test drives many conflict-free cross-shard MSET writers under one
deadline to catch head-only scheduler cycles, and verifies that commit receipts
are neither lost nor all drained as singleton batches and that shared fences
are merged. The multi-key and MULTI/EXEC tests also cover cross-shard and
cross-database behavior, duplicate keys, single-shard hashtag commands, WATCH
invalidation, runtime rollback, and recovery. The atomicity stress test
overlaps writers with MGET and EXEC readers to detect torn snapshots.

There is no focused deterministic unit test for a multi-shard schedule
cancellation/retry, the conflict-free bypass queue, one-shot shard entry hooks,
deliberate fingerprint collisions, or a retained single-shard guard across
several hops. Those paths are implementation-backed and receive indirect
end-to-end coverage, but the specific edge behavior is not isolated by the
current test suite. The scheduler also defines no retry limit, fairness
deadline, or timeout for a slow held predecessor.

## Source map

| Claim | Repository source |
|---|---|
| Lock modes, fingerprint identity, key references, and collision boundary | `include/keylane/tx/fingerprint.h`, `include/keylane/tx/transaction.h` |
| Intent/hold compatibility and the hold-subset-of-intent invariant | `include/keylane/tx/intent_lock.h` |
| Waiter flavors, txid ordering, removal, and tombstones | `include/keylane/tx/tx_queue.h` |
| Shard-local acquisition, ordered and bypass-ready polling, WATCH tables, runtime, and metrics | `include/keylane/tx/tx_shard.h`, `src/tx/tx_shard.cpp` |
| Key grouping, schedule/cancel/arm phases, callbacks, barriers, and release | `include/keylane/tx/transaction.h`, `src/tx/transaction.cpp` |
| Runtime creation, worker binding, and recovery seeding | `src/redis/server.cpp`, `src/storage/engine/init.cpp` |
| Command construction, multi-hop use, WATCH checks, replication entry hook, and INFO fields | `include/keylane/command.h`, `include/keylane/session.h`, `src/redis/command.cpp`, `src/redis/string_command.cpp`, `src/redis/set_command.cpp`, `src/redis/list_command.cpp`, `src/redis/sort_command.cpp`, `src/redis/zset_command.cpp` |
| Owner routing and the pre-locked storage contract | `include/keylane/storage/engine.h`, `src/storage/engine/impl.h` |
| Ordinary and background storage participation in transaction locks | `src/storage/engine/read.cpp`, `src/storage/engine/write.cpp`, `src/storage/engine/expire.cpp`, `src/storage/engine/backup.cpp`, `src/storage/engine/replication.cpp`, `src/storage/engine/tomb_raider.cpp` |
| Mutation and database-wide WATCH marking | `src/storage/engine/write.cpp`, `src/storage/engine/expire.cpp`, `src/storage/engine/flush_db.cpp`, `src/storage/engine/replication.cpp` |
| Durable write ids, undo/commit boundary, worker-local commit batching and backpressure, and storage-owned crash atomicity | `include/keylane/storage/engine.h`, `src/storage/engine.cpp`, `src/storage/engine/write.cpp`, `src/redis/command.cpp`, `src/storage/engine/recovery.cpp`, `src/storage/engine/tx_cleaner.cpp` |
| Direct lock tests and end-to-end transaction coverage | `tests/tx_lock_test.cpp`, `tests/multikey_e2e_test.cpp`, `tests/multi_exec_e2e_test.cpp`, `tests/atomicity_stress_e2e_test.cpp` |
