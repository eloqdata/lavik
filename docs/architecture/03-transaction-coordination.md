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
  before its first callback. The command layer uses this to enter replication
  transaction ordering.
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
4. `Execute` clears participant statuses and arms every transaction node.
   `TxShard::Poll` examines only the live queue head. It stops if that entry is
   unarmed, already running, or still conflicts with a held predecessor.
   Otherwise it publishes the head's id to the committed watermark, acquires
   holds once, and spawns the shard callback.
5. Every participant records its callback status and decrements the transaction
   barrier. The last participant schedules the coordinator coroutine on its
   original worker. The coordinator observes all statuses only after this
   barrier completes.
6. A non-releasing hop leaves holds, intents, and queue positions in place. A
   releasing hop drops holds and intents, removes each node, and repolls its
   shard. `Release` is a no-op callback executed as such a final releasing hop.

The coordinator awaits every round it starts. A participant's barrier
decrement is its final access to the frame-embedded transaction, which prevents
the coordinator frame from disappearing while a shard callback is suspended.

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
- `TxShard::Poll` starts only the queue head. A suspended conflicting holder can
  delay it; unrelated owner-local acquisitions may still take the fast path
  when their intents are compatible.
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
and MULTI/EXEC end-to-end tests cover cross-shard and cross-database behavior,
duplicate keys, single-shard hashtag commands, WATCH invalidation, runtime
rollback, and recovery. The atomicity stress test overlaps writers with MGET
and EXEC readers to detect torn snapshots.

There is no focused deterministic unit test for a multi-shard schedule
cancellation/retry, one-shot shard entry hooks, deliberate fingerprint
collisions, or a retained single-shard guard across several hops. Those paths
are implementation-backed and receive indirect end-to-end coverage, but the
specific edge behavior is not isolated by the current test suite. The scheduler
also defines no retry limit, fairness deadline, or timeout for a slow held
predecessor.

## Source map

| Claim | Repository source |
|---|---|
| Lock modes, fingerprint identity, key references, and collision boundary | `include/keylane/tx/fingerprint.h`, `include/keylane/tx/transaction.h` |
| Intent/hold compatibility and the hold-subset-of-intent invariant | `include/keylane/tx/intent_lock.h` |
| Waiter flavors, txid ordering, removal, and tombstones | `include/keylane/tx/tx_queue.h` |
| Shard-local acquisition, polling state, WATCH tables, runtime, and metrics | `include/keylane/tx/tx_shard.h`, `src/tx/tx_shard.cpp` |
| Key grouping, schedule/cancel/arm phases, callbacks, barriers, and release | `include/keylane/tx/transaction.h`, `src/tx/transaction.cpp` |
| Runtime creation, worker binding, and recovery seeding | `src/redis/server.cpp`, `src/storage/engine/init.cpp` |
| Command construction, multi-hop use, WATCH checks, replication entry hook, and INFO fields | `include/keylane/command.h`, `include/keylane/session.h`, `src/redis/command.cpp`, `src/redis/string_command.cpp`, `src/redis/set_command.cpp`, `src/redis/list_command.cpp`, `src/redis/sort_command.cpp`, `src/redis/zset_command.cpp` |
| Owner routing and the pre-locked storage contract | `include/keylane/storage/engine.h`, `src/storage/engine/impl.h` |
| Ordinary and background storage participation in transaction locks | `src/storage/engine/read.cpp`, `src/storage/engine/write.cpp`, `src/storage/engine/expire.cpp`, `src/storage/engine/backup.cpp`, `src/storage/engine/replication.cpp`, `src/storage/engine/tomb_raider.cpp` |
| Mutation and database-wide WATCH marking | `src/storage/engine/write.cpp`, `src/storage/engine/expire.cpp`, `src/storage/engine/flush_db.cpp`, `src/storage/engine/replication.cpp` |
| Durable write ids, undo/commit boundary, and storage-owned crash atomicity | `include/keylane/storage/engine.h`, `src/storage/engine.cpp`, `src/redis/command.cpp`, `src/storage/engine/recovery.cpp`, `src/storage/engine/tx_cleaner.cpp` |
| Direct lock tests and end-to-end transaction coverage | `tests/tx_lock_test.cpp`, `tests/multikey_e2e_test.cpp`, `tests/multi_exec_e2e_test.cpp`, `tests/atomicity_stress_e2e_test.cpp` |
