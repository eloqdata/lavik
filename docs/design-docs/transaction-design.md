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

# Lavik transaction design

## Context

lavik is a thread-per-core, io_uring, C++20-coroutine disk-backed Redis server (the celer submodule provides the runtime). Today it only has the engine's internal per-key blocking locks (`storage::IntentLockTable`); the only multi-key commands are DEL/EXISTS, executed serially per key with no atomicity, and there is no MULTI/EXEC. Goal: design an **original** transaction scheduling framework based on VLL (Very Lightweight Locking, Ren/Thomson/Abadi), supporting cross-shard atomic multi-key commands (MSET/MGET, atomic DEL/EXISTS) and MULTI/EXEC.

**Hard constraint (user requirement): a single-shard command on the uncontended path must never touch the global atomic txid counter.** Approach: lazy txid allocation + optimistic execution at schedule time (with the mechanism built on celer's SPSC lanes / coroutine model).

**No external code is copied** — only concepts are borrowed: intent-counting locks (Acquire never blocks; it records the intent and reports whether everything was granted), a per-shard txid-ordered TxQueue as the arbiter, all-granted ⇒ out-of-order execution allowed, scheduling may fail but execution never rolls back, locks are held until the concluding hop.

**lavik-specific divergence (the core design innovation):** the reference engine's shard callbacks are non-preemptive and the queue head runs to completion; lavik's engine operations suspend on disk I/O while holding locks. Solution: `IntentLock` extends to **two layers of counters, intent + hold** — intent is the scheduling arbiter (classic VLL semantics), hold marks "a callback is currently executing (possibly suspended)". A suspended holder does not block later non-conflicting transactions (the intent counters keep grant checks valid while the holder sleeps); the queue head only waits for genuinely conflicting holds to drain. This preserves the I/O overlap a disk-backed shard depends on.

WATCH is included in this milestone (user addition): implemented as **version-number-based optimistic validation** with zero write-path hooks — every lavik `RecordLocation` already carries `mutation_sequence` (engine.cpp:3964, incremented on every AppendLocked; EXPIRE is also a whole-record rewrite and increments it; defrag relocation preserves it @5052), and FLUSHDB has `DbEpoch(db_id)` @2837. Snapshot at WATCH time, re-read and compare under the EXEC locks.

## 1. New module: the `tx/` scheduling core

New files (registered in CMakeLists.txt's `lavik_module`):
```
include/lavik/tx/fingerprint.h   LockFp = first 8 bytes of the Digest (SHA-1 already computed for the index; zero extra hashing)
include/lavik/tx/intent_lock.h   IntentLock counters + LockTable
include/lavik/tx/tx_queue.h      per-shard TxQueue
include/lavik/tx/transaction.h   Transaction, ShardData, hop awaiter
include/lavik/tx/tx_shard.h      TxShard (per worker) + TxRuntime (global)
src/tx/tx_shard.cpp, src/tx/transaction.cpp
```
A standalone module rather than part of WorkerStore: scheduling happens at the command layer (above the engine), and the engine's background paths need it too. `TxRuntime` (`vector<unique_ptr<TxShard>>` + `atomic<uint64_t> next_txid`) is created in RunServer before the workers start.

### LockTable (one per worker × 16 logical DBs, single-threaded, no atomics)
```cpp
struct IntentLock {   // absl::flat_hash_map<LockFp, IntentLock, IdentityHash>; entry removed when all four counters are zero
  uint32_t shared_intent, exclusive_intent;   // scheduling intents (queued and running alike)
  uint32_t shared_held,  exclusive_held;      // callback currently executing (possibly suspended on I/O)
};
// Acquire(fp, mode): always records the intent, returns granted:
//   shared: exclusive_intent==0;  exclusive: shared_intent==0 && exclusive_intent==1 (only myself)
// CanHold(fp, mode): shared: exclusive_held==0;  exclusive: both held counters zero
// Plus ReleaseIntent / AcquireHold (asserts CanHold) / ReleaseHold
```
No wait queues — the old table's FIFO waiters are gone; wakeup becomes the TxQueue's Poll. Lock granularity = the whole key (routing still by hashtag slot); fp collisions only cause false contention, correctness is backed by queue ordering plus the engine index's full-digest comparison.

**Why split intent/held (classic VLL implementations have only Cs/Cx):** their execution premise is run-to-completion — once a transaction starts it runs on the partition thread without suspending, so at any scheduling decision point the state "currently executing" is unobservable (anything started earlier either finished with its counters already decremented, or is still in the queue); two counters plus a queue position are complete. lavik's callbacks sleep on disk while holding locks, creating a third state: "not in the queue, not finished, currently suspended" (fast-path transactions never enqueue). With only Cs/Cx the queue head faces an unresolvable ambiguity — it sees a conflicting count of 1 and cannot tell whether that is a **sleeping predecessor already running** (must wait) or an **intent queued behind itself** (must never wait — waiting deadlocks); not waiting means racing the sleeper and tearing data. The held counter supplies exactly that missing bit: `*_intent` keeps classic VLL semantics (counts queued and running alike; all grant/out-of-order decisions use it), `*_held` counts only "callback executing right now (possibly suspended)", with the invariant held ⊆ intent; the queue head's start condition = it is my turn **and** CanHold passes. Rejected alternatives: putting the fast path in the queue too (needs a txid ordering slot → touches the global atomic, or introduces unordered txid=0 entries), or maintaining a set of running-transaction pointers (equivalent to held but heavier, and CanHold stops being O(1)).

**intent/held walkthrough (beat by beat; k1's counters written (Cs_i,Cx_i|Cs_h,Cx_h)):**
```
t0 GET1 fast path: record intent granted + record held, suspend on disk read   (1,0|1,0)
t1 SET arrives: not granted → takes txid, enqueues as head; CanHold(X) sees Cs_h=1 → wait  (1,1|1,0)   ← waiting on the held (a runner)
t2 GET2 arrives: not granted → queues behind SET                               (2,1|1,0)
t3 GET1 finishes: release held+intent → Poll; head's CanHold: held all zero → run  (1,1|0,0)   ← the remaining Cs_i is GET2's intent; ignored
t4 SET executing, GET3 tries the fast path: Cx_i=1 → not granted, enqueues     (1,1|0,1)   ← anti-barging is done by the intent layer
t5 SET releases → dequeues → Poll → GET2 starts; when done all zeros, entry removed
```
The head sees a nonzero shared count twice (t1/t3); the held layer instantly distinguishes "a runner (wait)" from "someone queued behind (don't wait)" — exactly the bit a single Cs/Cx layer loses. The fast path's held acquisition always succeeds: all-granted ⇒ sole intent holder ⇒ (held ⊆ intent) nobody has a hold.

**What a memory-first engine does about SSD offload (tiered storage) and why it does not transfer (verified against their source):** it never lets a transaction callback sleep on disk while holding locks — (1) backstop: `PollExecution` returns immediately when it sees `running_tx_` (engine_shard.cc:620), so if a callback really suspends the whole shard queue stalls (conflicting or not); (2) main path: a GET of an offloaded value only registers a read request inside the shard callback and returns a Future (string_family.cc:75); the transaction concludes and releases locks as usual, and the actual disk wait, `fut.Get()`, happens in the connection fiber's Send (:716), already outside the transaction; consistency is kept by OpManager bookkeeping (no blob reclaim before the read completes, `HasModificationPending` intercepts dirty segments). lavik cannot copy (2): that engine is memory-first — the transaction body only touches RAM and the disk read is a pure data fetch movable outside the locks; lavik is disk-native — the write path (AppendLocked) and read-modify-write (INCR/EXPIRE: load → compute → rewrite) have their disk I/O in the middle of the critical section, and moving it out destroys atomicity. Hence suspending while holding locks must be supported → held. Potential future optimization: pure GETs could adopt (2) (snapshot the RecordLocation under the lock, read the block after releasing, with a block lease against defrag reclaim); this does not affect the necessity of held.

### Transaction (stack-allocated, embedded in the coordinator coroutine's frame; no heap allocation, no reference counting)
```cpp
class Transaction {
  uint8_t db_id;  const CommandContext* ctx;
  struct KeyRef { Digest digest; LockFp fp; uint32_t arg_index; Mode mode; };
  absl::InlinedVector<KeyRef, 2> keys_;            // grouped contiguously by shard
  struct ShardData { celer::RemoteWork msg;        // embedded arm/schedule/cancel message
                     uint16_t shard_id, flags;     // kActive|kGranted|kQueued|kHoldsAcquired|kRanFirstHop|kArmed|kScheduleFailed
                     uint16_t key_begin, key_count; };
  absl::InlinedVector<ShardData, 1> shards_;       // single shard inlined
  uint64_t txid_ = 0;                              // 0 = never allocated (fast path stays 0 forever)
  ShardCallback cb_; bool releasing_; uint8_t phase_;   // callback = function pointer + void* ctx, avoiding std::function heap allocation
  std::atomic<uint32_t> barrier_;                  // the only cross-thread hot word
  std::coroutine_handle<> coord_handle_;  celer::WorkerId coord_worker_;
};
```
Lifetime safety rule: **the coordinator awaits the barrier after every round it starts (schedule/hop/cancel); a shard's fetch_sub on the barrier is its last access to the tx** — the coordinator cannot resume before the final decrement, so the frame cannot dangle (including while a callback is suspended in io_uring: that hop's barrier has not been decremented yet).

**Hop protocol (celer-native, replacing the reference is_armed atomic exchange):** the awaiter of `Execute(cb, release)` first writes `coord_handle_`, then `barrier_.store(n, release)`, then per shard: local shards call `ArmOnShard` directly, remote ones get `PostRequest(&sd.msg)` (the SPSC lane's release/acquire is the ownership transfer). `ArmOnShard`, on the shard thread, sets `msg.reply_deferred = true` (suppressing celer's automatic reply; the cross_core.h/RunRemoteWork contract was verified), marks kArmed, and calls `Poll()`. On completion: the shard whose `barrier_.fetch_sub(1, acq_rel) == 1`, if it is the coordinator's worker, directly `Enqueue(coord_handle_)`; otherwise it `PostNotification`s to the coordinator's worker, whose drain loop enqueues. **Invariant: the coordinator handle is only ever enqueued by its own worker thread.** Callbacks write results into slots in the caller's frame; cross-shard data flows only through the coordinator between hops — callbacks never talk to each other.

### TxShard / TxQueue / the scheduling algorithm
```cpp
struct TxShard { std::array<LockTable, 16> locks; TxQueue queue;   // deque + lazy tombstones (v1 keeps it simple)
                 uint64_t committed_txid = 0; bool polling = false; celer::Worker* worker; /* stats */ };
```
**ScheduleInShard (shard thread, non-suspending section):** (1) `txid_ != 0 && txid_ <= committed_txid` → fail (stale); (2) unconditionally record all intents, note granted; (3) **reorder rule: queue non-empty and my txid < tail txid and !granted → release intents, fail the schedule** (the tail may already have executed out of order; nothing may be inserted ahead of it); (4) insert in txid order.

**Lazy txid allocation:** multi-shard transactions have the coordinator `next_txid.fetch_add(1)` before each scheduling round (all shards must share one txid); if any shard fails → a cancel round (successful shards dequeue + release + Poll) → retry with a larger fresh txid (unbounded retries + a retry counter). **Single-shard transactions keep txid 0 on the fast path; only when the grant check fails does the shard thread fetch_add and enqueue — at that point the new txid is necessarily larger than everything in the queue (fetch_add is globally monotonic + real-time order), so it always inserts at the tail, scheduling never fails, and there are no retries.**

**Poll (replacing the reference PollExecution + the running_tx_ gate):** triggered by arm, every transaction completion (fast-path completions included), cancel, and release. Take the queue head (skipping tombstones); if the head is running / not armed / `!HoldsCompatible(head)`, break (the corresponding event will trigger Poll again); otherwise `committed_txid = max(committed_txid, head->txid_)` (**published before the callback's first possible suspension**, in the same non-suspending section — the worker's cooperative scheduling makes this naturally atomic) → AcquireHolds → `SpawnOnCurrentWorker(RunTx)`. RunTx: `co_await cb(...)` (may suspend) → non-suspending epilogue: if concluding, release holds+intents, dequeue, Poll(); finally decrement the barrier. Multi-hop: after a non-concluding hop the holds and queue position are retained; the next arm sees kRanFirstHop and continues directly (the continuation is a per-tx flag, not a shard field — multiple non-conflicting suspended transactions can be in flight simultaneously).

**Concurrency model of the three execution shapes:** (1) fast-path transactions (all granted, txid 0, never queued) — all-granted ⇒ sole intent holder ⇒ no conflicting hold; while it sleeps its intents stay in the counters fending off newcomers; (2) out-of-order in-queue transactions (multi-shard, all granted) — same argument; (3) the queue head — waits only for suspended holders that started before it to drain (I12: after the head's intents are recorded no new conflicting party can be granted, so the conflict set is finite and must drain).

### The single-shard fast-path ladder (proof that the global atomic is never touched)
- **Rung 1 (connection worker == owner):** the coordinator coroutine directly AcquireAllIntents; all granted → AcquireHolds → directly `co_await GetLocked(...)` → release → Poll. Zero txid, zero queue, zero cross-core, zero spawn.
- **Rung 2 (remote optimistic):** a single embedded msg (kScheduleAndRun); the grant check runs inside the shard's drain loop; on success → spawn and run immediately, **never enqueued, txid stays 0 for life**. Incremental cost over today's SubmitTaskTo: N counter increments/decrements + one empty-queue check.
- **Rung 3 (contention fallback):** grant fails (already on the shard thread) → the shard thread fetch_adds a txid, inserts at the tail, waits for Poll.
`next_txid` is touched only on rung 3 and in multi-shard scheduling; rungs 1–2 touch only the per-shard map, the per-tx barrier, and the SPSC lane — the global counter's cache line is never loaded. ∎

## 2. The command layer

### Command table (new `include/lavik/command_table.h` + `src/redis/command_table.cpp`; behavior-neutral, can merge first)
`CommandSpec{name, kind, arity (Redis convention, negative = minimum), first_key, last_key (negative = from the end), key_step, flags}`; flags: kWrite/kReadOnly/kNoKeys/kMultiShard/kGlobal/kNoTx/kNotQueueable. A constexpr array of ~20 entries, bucketed by length with linear lookup (stays allocation-free). `DetermineKeys(spec, argc) -> KeyIndexView` centralizes arity errors (killing ~10 duplicated checks in ExecuteStorageCommand). Replaces `MatchCommandKind` and the hard-coded mutating/uses_db chains; dispatch stays a switch for this milestone.
New entries: MSET `{-3,1,-1,2,W|MS}` (plus odd-argc validation), MGET `{-2,1,-1,1,RO|MS}`, DEL/EXISTS become MS, MULTI/EXEC/DISCARD `{1,0,0,0,NoTx|NoKeys}`, WATCH `{-2,1,-1,1,RO|MS|NoTx}` (error inside MULTI), UNWATCH `{1,0,0,0,NoKeys}` (queueable).

### Engine API rework (two lock systems must never coexist on one key — the switch is an atomic milestone)
- **Step A (behavior-neutral):** split the 8 client operations into `XxxCore` (everything after the current lock-acquisition line; store_state_mutex still taken internally and released before returning; the key lock → store_state_mutex order unchanged) + wrappers over the old locks. Expose `GetLocked/SetLocked/DeleteLocked/ExistsLocked/IncrementLocked/StringLengthLocked/GetExpirationLocked/UpdateExpirationLocked`, **taking a precomputed Digest** (SHA-1 moves to the coordinator worker; shards stop hashing), with a debug assertion on `TxShard::HoldsKey`.
- **Step B (the VLL switch):** delete the engine's internal `key_locks` acquisition and `storage/intent_lock.h`; every client keyed command goes through a transaction.
- **Background paths** (all single-key, already on the owner worker): SnapshotPartition@~1917 (S per key), ApplyReplicaRecords@~2166 (X per record), ExpireCandidate@~4511 (X), defrag RelocateIfCurrent@~5036 (X) → all switch to `TxShard::RunLocal(db, keys, cb)` (the library form of rung 1: try-grant inline execution, else a stack-embedded internal transaction enqueues). From then on they share one arbitration system with client transactions — fair ordering, no second lock system.
- Callback author rule: store_state_mutex must never be held across a hop boundary (existing engine operations already satisfy this naturally).

### Multi-key commands (single hop, multi-shard; RouteMultiKey deleted)
ShardView exposes the shard's slice as **original argument indices**. MSET: each shard runs SetLocked in argument order (duplicate keys dedup the locks, writes stay ordered → last-wins); MGET: the coordinator preallocates `vector<optional<string>>(n)`, each shard fills its own slots (disjoint + barrier acq_rel ⇒ no race), reply encoded in request order; DEL sums per-shard counts; EXISTS counts occurrences (`EXISTS k k` → 2) while the lock set dedups. The single-shard case (hashtag) automatically takes the fast path.

### MULTI/EXEC (LOCK_AHEAD) + WATCH
- New `include/lavik/session.h`: `ConnectionContext{selected_db, in_multi, multi_dirty, multi_db, vector<CommandRequest> queued, vector<WatchedKey> watched}`; `WatchedKey{uint8 db; string key; Digest digest; WatchStamp stamp}`, `WatchStamp{uint64 db_epoch; uint64 seq; bool live}`. Serve's (server.cpp:405) `selected_db` becomes the ctx, with a `DispatchCommand(ctx, request)` wrapper layer.
- Queueing semantics (Redis-compatible): unknown command / arity / READONLY / kNotQueueable → error + set dirty; nested MULTI and WATCH-inside-MULTI (`-ERR WATCH inside MULTI is not allowed`) → error without setting dirty; SELECT updates multi_db and queues; UNWATCH queues (a no-op inside EXEC); everything else `+QUEUED`. DISCARD/EXEC without MULTI are the standard errors; dirty EXEC → `-EXECABORT`; empty queue → `*0`; runtime errors are inlined in the array and execution continues.
- **WATCH mechanism (user-selected: push-style shard-local mark tables, zero cross-thread shared memory):** one `flat_hash_map<LockFp, WatchItems>` per (worker, db); items are registered by (conn_id, key), conn_id used only as a key and never dereferenced. Four pieces:
  1. **Registration**: WATCH (only legal outside MULTI) `SubmitTo`s each owner shard to create the item (`try_emplace` — a repeated WATCH keeps the original item and marks, sticky, matching Redis); the item records a `live` bit (existed and unexpired at the time);
  2. **Marking**: hooked at the **funnel of real modifications**, not at command classification — `AppendLocked`/the index update points (covering SET/DEL/INCR/EXPIRE/active expiry), the replica-apply write point, and FLUSHDB's detach point (marks every item of that db on the worker, including watches on nonexistent keys); "didn't actually change" cases (`SET NX` that did not apply) do not mark; defrag relocation does not mark. Write-path cost = an empty-table branch (≈ zero);
  3. **Check (EXEC)**: schedule and lock first, then look at the marks. Shards the queued commands touch check their own tables in hop 0 on the way; shards that are only watched, which the transaction never visits, are read via a plain `SubmitTo` (lock-free safe: EXEC does not read that key, so any ordering of the write and the EXEC is a legal serialization). **Passive-expiry hole plugged** (Redis also patches this with isWatchedKeyExpired): compare the item's live bit with the current IsExpired; a change also aborts. Any mark / live change → `Release()` and reply `*-1`; race argument: marking and checking are on the same shard thread — a write either completed before the locks (its mark must be visible) or is ordered behind the transaction by the intents; there is no third case;
  4. **Cleanup**: UNWATCH/DISCARD/EXEC (either outcome) and connection close (the single cleanup point of M3) `PostNotification` the registered shards to erase by conn_id (sent eagerly, applied asynchronously, the mailbox never drops).
- EXEC = one transaction: take the DbOperationGuard first (held for the whole EXEC, avoiding mid-hop TRYAGAIN); the lock set = the union of queued commands' keys (per-key mode: X if any writer touches it) → `InitKeys` with an explicit (db, key, mode) list → `Schedule()` (a single-shard all-granted EXEC takes intent+hold directly, never queues, txid 0 — EXEC enjoys the fast path too) → the watch check (item 3 above) → on pass, execute commands serially: one non-concluding hop per queued command (barrier-separated), handlers refactored as `RunXxx(Transaction&, request)` shared between standalone execution and EXEC; keyless commands (PING/SELECT) run on the coordinator between hops; a final `Release()` guarantees the locks are released exactly once.
- Under the push model the Redis edge cases align naturally: SET-then-DEL with later tombstone reclamation (the mark was set at SET time and is sticky, no pull-style ABA); FLUSHDB invalidates watches on nonexistent keys too (the detach point marks the whole table); a value changed back to the original still invalidates (marks track modification events, not values). **Recorded alternative (evaluated, not chosen): pull-style version snapshots** — WATCH records the `{db_epoch, mutation_sequence, live}` triple (reusing existing seq/epoch, zero new state) and EXEC re-compares under the check hop's locks; drawbacks: watched keys must enter the lock set, one extra validation hop, and tombstone reclamation opens an ABA window (needs a minimum-reclaim-age hardening). Fall back to this if push hits an obstacle.
- Transaction exposes: `InitKeys` / `Schedule()` / `Execute(cb, release)` / `Release()` (renamed from Conclude, 2026-08-08). (Dual-hop RENAME is the validation command for this API, kept as an optional extension.)

### Replication and FLUSHDB (decisions for this milestone)
- Replication: this milestone's per-key-delta design has been superseded by
  `docs/design-docs/replication-design.md`. Transactions publish only after commit. During
  hidden full sync their participant after-images may apply independently, but
  `FULLSYNC_CUT` cannot split a transaction; ONLINE uses one transaction
  envelope and advances all participant cursors atomically. The replica
  read-only check also runs during MULTI queueing.
- FLUSHDB/DBSIZE/SCAN: stay behind g_db_gates; every VLL transaction holds a DbOperationGuard for its whole lifetime; all three are kNotQueueable (a documented deviation from Redis); migrating them to shard-level global transactions is a later milestone.

## 3. Implementation order — small milestones, each independently compiling, all tests green, individually mergeable

| # | Goal | Size | Behavior change |
|---|------|------|-----------------|
| M1 | **Command table**: CommandSpec + FindCommand + DetermineKeys, replacing MatchCommandKind and the hard-coded mutating/uses_db; `tests/command_table_test.cpp` | S | none |
| M2 | **Engine split**: extract `*Locked` variants of the 8 client operations + Digest parameterization, old-lock wrappers retained | M | none |
| M3 | **ConnectionContext** threaded through Serve/Dispatch (MULTI not wired yet); also extract the existing loop into an inner `ServeLoop(stream, ctx)` with the outer `Serve` establishing the single cleanup point `CleanupConnection(ctx)` after `co_await ServeLoop` (every disconnect path passes through it, awaitable — the structural home for WATCH deregistration and other connection-level cleanup) | S | none |
| M4 | **tx/ module**: LockTable (intent+hold) + TxQueue + TxShard + RunLocal + TxRuntime wiring, unit tests only (`tests/tx_lock_test.cpp`, using suspending fake callbacks to test grant/hold/queue/poll), no commands wired | M | none |
| M5 | **The VLL switch** (the single cutover point; by then only mechanical replacement remains): single-key commands take the fast-path ladder; the 4 background paths move to RunLocal; delete `WorkerStore::key_locks` and the old `storage/intent_lock.h`; transactions hold the db gate for their lifetime. Full CTest + e2e + ASan | M | none (semantically equivalent) |
| M6 | **Multi-shard single hop**: scheduling rounds/cancel/retry, hop awaiter + barrier + notification resumption; MSET/MGET + atomic DEL/EXISTS (RouteMultiKey deleted); `tests/multikey_e2e_test.cpp` | M | new feature |
| M7 | **MULTI/EXEC/DISCARD** + Release (then named Conclude) + multi-hop (continuations); `tests/multi_exec_e2e_test.cpp`. EXEC execution model = **serial per-command hops** (user decision; rationale: every command must be supported, and serial hops hold uniformly for all command shapes — single-key, multi-key, RENAME/EVAL-style cross-shard dataflow, future blocking commands — with no segmentation logic): one non-concluding hop per command, barrier-separated, later commands see earlier effects, replies in order; inner commands do not enqueue individually — inter-transaction order belongs to the TxQueue, intra-transaction order to the hop sequence. Deferred optimization (once the command surface stabilizes): segment packing / squashing — the three correctness conditions are already argued (slices determinable up front, per-shard queue order preserved i.e. same key ⇒ same shard, reply slot assembly), cutting segments at cross-shard dataflow commands, one hop per segment | M | new feature |
| M8 | **WATCH/UNWATCH** (push-style shard-local mark tables): the four pieces — registration / marking at modification funnels / post-lock EXEC check (incl. passive-expiry comparison) / eager deregistration; WATCH e2e cases folded into the multi_exec test | M | new feature |
| M9 | **Stress + observability**: `tests/atomicity_stress_e2e_test.cpp`; fastpath/OOO/queued/retries/head_wait counters; replication-gap documentation. (Swapping TxQueue to a vector-ring + pq_pos is an optional optimization) | S | none |

M1–M4 are mutually independent; M5 depends on M2+M4; M6 onward are sequential.

### Single-key command cost note (user concern)
After the VLL switch, single-key commands get **cheaper at runtime, not more expensive**: today it is a `try_emplace` with a 20-byte Digest key plus a waiters-queue check; afterwards it is a `try_emplace` with an 8-byte fp key (identity hash) + two counter increments/decrements + one empty-queue check. Uncontended commands never enqueue, never take a txid, never cross a barrier, and the routing structure (inline or SubmitTaskTo) is unchanged. The framework's complexity concentrates in the multi-key/multi-hop branches; single-key commands traverse only the most trivial path.

## 4. Verification

- Unit tests: command_table (arity / negative indices / flags), tx_lock (S/S grant, S/X conflicting intents, zero-count eviction, RunLocal's fairness toward the queue, suspended holders not blocking non-conflicting transactions).
- E2E (extend RespClient with recursive array-reply parsing; server `--threads 4` guarantees genuine cross-shard, with a `--threads 1` section kept):
  - multikey: out-of-order cross-shard MGET returns in request order, missing keys are nil, MSET duplicate keys last-wins, hashtag fast-path regression.
  - multi_exec: the full RESP semantics matrix (QUEUED / nesting / EXECABORT / DISCARD / empty EXEC / inline runtime errors / SELECT-in-MULTI / state reset).
  - WATCH (two connections): another client modifies the watched key → EXEC `*-1` and the queued commands never ran; modifying it yourself before MULTI → likewise invalidated; no modification → success; value changed back to the original (SET k v; SET k v) still invalidates (version semantics, matching Redis); DEL of the watched key → invalidates; EXPIRE → invalidates; watched key springing into existence → invalidates; FLUSHDB → invalidates; EXEC succeeds after UNWATCH; watches are cleared after EXEC (a second EXEC is unaffected by old watches); WATCH-inside-MULTI errors but the transaction can continue; cross-shard watched keys.
  - **Atomicity stress** (shaped to catch out-of-order/lock-mode/barrier bugs): W1 loops `MSET a v b v`, W2 loops `MSET b u c u` (values carry writer tags; keys verified cross-shard via CRC16); readers (both MGET and MULTI-GET×3-EXEC) assert: b is W1's value ⇒ a==b, b is W2's value ⇒ c==b; plus same-pair (a,b) overlap hammering asserting a==b always. Run 5–10s; on failure dump the triple + server log.
- After every step: `scripts/build_debug.sh && ctest --test-dir build_debug`; from step 5 on, add the ASan build running the e2e suite (build_asan/ already exists).

## 5. Settled risk items

- Blocking commands (future BLPOP) would break the frame-embedded lifetime (a suspended blocking transaction outlives the hop barrier) → keep frame embedding for now; recorded upgrade path: only BLOCKING commands switch to intrusive refcounting + heap allocation.
- Multi-shard retries can theoretically starve → unbounded retries + counters + `celer::Yield` between retries; deal with it if benchmarks show a problem.
- The queue head can be delayed by a slow suspended holder → the inherent cost of disk-backed atomicity; per-fp granularity already limits it to true conflicts; add a head_wait counter.
- Dependence on the `reply_deferred` contract (verified against cross_core.h) → add a comment on the celer side + a cross-worker arm stress test in lavik.

## Key files

Modified: `src/redis/command.cpp` (routing rewrite), `src/redis/server.cpp` (ctx threading), `include/lavik/command.h`, `src/storage/engine.cpp` (`*Locked` split, background-path migration, key_locks removal), `include/lavik/storage/engine.h`, `CMakeLists.txt`; deleted: `include/lavik/storage/intent_lock.h`; added: `include/lavik/tx/*`, `src/tx/*`, `include/lavik/command_table.h`, `src/redis/command_table.cpp`, `include/lavik/session.h`, 5 new tests. Read-only dependency: `celer/include/celer/runtime/cross_core.h` (RemoteWork/PostRequest/PostNotification/reply_deferred).

## M10: storage-failure atomicity for multi-key writes (2PC commit records; finalized 2026-08-09)

**Problem**: VLL has no undo. When a multi-key write (MSET / multi-key DEL / an EXEC body) hits a mid-transaction storage error (disk full / EIO / write-buffer exhaustion), the shards that already appended have taken effect durably while the failing shard reports an error — the client gets an error but half the write remains, and it is still half after recovery.

**Scheme (proposed by the user)**: presumed-abort 2PC. Data records are the prepare; the initiating worker appends a commit record locally **after** every participating shard's data is durable; at recovery, a txid-tagged data record is kept only if its commit record is found.

**Representation**: `RecordHeader::generation` is repurposed and renamed `txid` (a fossil field: the original newest-wins ordinal, left without any reader after 04f10f4 introduced replication_epoch/mutation_sequence; it merely mirrored mutation_sequence). txid==0 means a non-transactional record (the fast path is always 0; next_txid starts at 1), so **no flag bit is needed**. The generation cleaner clears it only while promoting a positively committed winner into an ordinary records block. The earlier version-2 proposal was not adopted; the current storage format version remains 1.

**The commit record**: new `RecordKind::kTxCommit`, header-only with no payload, the txid field holding the committed id. It uses the transaction's generation block stream, riding the existing staging/flush/recovery-scan machinery. Flush ordering uses the same fence as RelocationDurabilityFence: the coordinator gathers each participating shard's (block, committed) high-water marks and appends the commit only after all are durable. Losing the commit itself = the whole transaction is dropped at recovery, which falls within the relaxed-durability promise; the only forbidden outcome is "half of it survives". Generation cleaning removes the tagged records and their commits as one closed block group, avoiding self-pinning.

**Retirement routing**: a transactional write's superseded previous is not retired via the data record's RecordIdentity but via the commit record's identity (commit block flushed = commit durable ⇒ data durable long before ⇒ only then may the previous leave its accounting). Otherwise "data durable, commit not, previous reclaimed, recovery drops the new" loses both ends.

**Recovery**: the scan parks txid-tagged data records and collects the kTxCommit set; after the scan, filter into the index. next_txid is seeded to the maximum txid seen on disk + 1 (aggregated at the recovery barrier), solving cross-boot collisions with zero new metadata.

**Runtime rollback (preserving "readers never see half")**: multi-key **write** commands (MSET/DEL) change from a concluding single hop to "execute hop (release=false) → coordinator checks every shard's status → Release()/rollback hop", 1→2 hops; read commands (MGET/EXISTS) stay at 1 hop. EXEC is already multi-hop, structure unchanged. Rollback = each successful shard, still holding the locks, restores the previous index entries + MarkRecordDead on the new records + counter corrections; the dirty records stay in staging and recovery drops them naturally for lack of a commit.

**Replication interplay**: rollback publishes nothing. Replication publisher
placeholders resolve to `Discard`, and full-sync participant effects are made
visible only by `PublishCommittedFullSyncEffects` after commit; the old
overflow/re-copy compatibility path no longer exists.

**Stages**: ① rename the txid field + thread it through (all writers pass 0, behavior-neutral) → ② kTxCommit + recovery filtering + seeding (nobody writes txids yet, neutral) → ③ transactional tagging + the commit chain + retirement routing → ④ 2-hop + runtime rollback + replication overflow → ⑤ crash tests (crash with data durable but commit not ⇒ all dropped; commit durable ⇒ all present) + a disk-full-triggered runtime rollback e2e (genuine ENOSPC on a small data file).

### M10 progress and the pre-stage-③ open point (historical, 2026-08-09)

Landed: ① the txid field (fossil generation repurposed, all writers stamp 0, behavior-neutral); ② the kTxCommit kind + codec validation, recovery-time "park tagged records → adjudicate after the barrier against the commit set", next_txid recovery seeding (disk-wide max+1, set by worker 0). Both committed green.

**Open point discovered while implementing stage ③: commit-record reclamation.** A commit record counts toward its block's live_bytes but never enters the index: defrag salvage finds no index entry and will not relocate it, and the block never reaches zero because of it — left unhandled, every block eventually degenerates into a permanent "commit records only" leak. The candidates considered at the time were:
- (a) salvage special-cases kTxCommit and relocates unconditionally: blocks stay compactable, but commit records themselves never die, accumulating without bound with total transaction count;
- (b) refcount GC: the coordinator maintains txid → {unretired data-record count, commit-record location}; when a transaction's data record retires (at its fenced MarkRecordDead point) it notifies the coordinator to decrement; at zero, MarkRecordDead the commit record and the block reclaims naturally. After restart, recovery rebuilds the counts (the scan sees both the surviving tagged records and the commit records). Cost: obtaining txid on every retirement, one cross-worker notification per record retirement, and either a wider primary index or a disk-header read;
- (c) rewrite staging before flush to strip the txid (the commit decision is far faster than the periodic flush; most records can be untagged pre-flush with a header-CRC recompute, eliminating the commit record at the root): but it races "block fills → immediate flush", and already-flushed portions still need a commit record as backstop — complexity buying zero bloat on the normal path; viable only as an optimization on top of a complete commit-GC scheme.

Stage ③ shipped with (a) as a correctness-first stopgap. The generation-block design below supersedes the planned refcount scheme. Staging rewrite remains a possible optimization on top, not a correctness requirement.

### M10 complete (2026-08-09; commits 143f5a6/33601dd/288da01/6d627f4)

All four stages landed: ① the txid field; ② kTxCommit + recovery filtering + seeding; ③ tagging + the commit chain + retirement routing (pitfalls: commit records must not enter partitions; shutdown drains commit chains first; standby errors out waiters during shutdown); ④ runtime rollback — TxShardWrites.collect_undo enables the per-shard undo journal (WorkerStore.tx_undo, keyed by txid); single-shard transactions self-roll-back inside their callback keeping 1 hop, multi-shard goes through a second finish hop (rollback/discard under the still-held locks); overwritten keys restore the previous location + MarkRecordDead the new record + counter corrections, freshly created keys get a normal tombstone append; rolled-back writes publish no replication event; EXEC does not enable undo (per-command error reporting is Redis semantics; crash atomicity is still guaranteed by the commit record). LAVIK_FAIL_TX_WRITE injects test faults. Verified: injected mid-transaction failures roll back fully (overwrites / fresh keys / DBSIZE / consistency across restart) + the tx-commit-append crash matrix in both directions.

**Deferred follow-ups**: mid-transaction disk errors of a single command inside
EXEC (e.g. an embedded MSET) remain partially visible (command-level); a full
ASan re-run.

### M10.1: generation transaction blocks and TxCommit retirement (2026-08-19)

The range-based and per-transaction watcher designs were replaced by a simpler
layout: tagged records and their `kTxCommit` decisions share a dedicated
`BlockKind::kTransaction` class. Every transaction block belongs to exactly one
nonzero generation, persisted in `BlockHeader::tx_generation`. Ordinary writes
and promoted records use `BlockKind::kRecords` and generation zero.

This makes a generation the reclamation unit. The cleaner does not merge txid
ranges, retain a watcher per transaction, or mutate individual commit records.
It copies every still-current committed value out with `txid=0`; once no live
tagged value or rollback dependency remains, it removes all blocks in that
generation, including all of its commit records.

#### Generation lifecycle: derived state, no global mutex

```text
generation == current  --atomic rotate-->  generation < current
       Open                                  Closed / cleaner candidate
```

- **Open** is derived from equality with the one atomic current-generation id.
  Each worker owns its own active transaction block and generation metadata.
- **Closed** is derived from `generation < current`. Existing lease holders may
  still finish, commit, or roll back, but new transactions cannot enter it.
- There is no stored Retiring phase. The process-wide `cleaner_running` CAS
  makes the final exact check and owner-by-owner retirement a single-coordinator
  operation; an error leaves the remaining owner-local entries available for a
  later round or triggers the existing fail-stop path.

No process-wide generation map, admission counter, or thread-blocking mutex
exists. `InitializeTxWrites` reads the atomic current id and registers its lease
in one non-suspending worker-local section. After rotation, the cleaner reaches
every owner through a worker task; that task is also a quiescence barrier. An
initializer that already read the old id must finish its local registration on
that worker before the cleaner task can inspect it, while a later initializer
sees the new id. “Draining” is `active_transactions != 0`; “sealed/durable” is
computed from owner block states; and “reclaimable” is the pure predicate below.

#### Transaction write and UNDO lifetime

Before the first prepare write, `InitializeTxWrites` registers one shared lease
in its current worker's generation table and places the generation id in every
participating `TxShardWrites`. The shard receipts share the lease, so moving
them between workers does not multiply `active_transactions`. Its shared
runtime contains an atomic count, allowing the last receipt to release the
lease on any worker without touching a foreign map or taking a mutex. A
rotation only redirects new transactions; an older lease always continues to
write its original generation.

Every tagged data record and the transaction's `kTxCommit` append therefore go
to the transaction-block stream for that same generation. Commit ordering is
unchanged: all prepare fences must be durable before the commit is appended,
and superseded values carried by the commit completion retire only after the
decision is durable.

UNDO needs no new durable log. Until a failed transaction finishes its in-memory
rollback, its generation lease prevents cleaning. If a prepare temporarily
replaces an older record in a transaction block, that older block also receives
a `dependency_pin`; the pin follows the retirement/rollback bookkeeping and is
released only when the dependency is settled. This prevents the cleaner from
moving or deleting a value that rollback may restore. The existing block live
byte accounting remains a conservative second barrier.

#### Periodic cleaner and worker coordination

`CONFIG SET tx-cleaner-cooldown-ms <n>` controls the minimum delay between
rounds; the first automatic round also waits one full cooldown, and zero
disables the cleaner. Changing a nonzero value arms an immediate reevaluation.
Every worker's periodic maintenance may try to start a round. A process-wide
compare-and-swap elects whichever worker wins as that round's coordinator, so
worker 0 is not special and only one round can consume CPU at a time.

The coordinator never reads another worker's mutable block tables directly.
For each generation and owner it submits an owner-affine task; the owner takes
its normal store mutex, operates on its own blocks, and returns a compact
result. The complete flow is:

1. Ask each worker whether its current Open generation contains records. If so,
   atomically increment the current id; subsequent owner tasks form the
   per-worker barrier for initializers that overlapped the rotation.
2. Ask every worker for its local generation ids below current. For each
   candidate, sum owner-local lease counts and merge the committed-txid sets
   recorded by the workers that wrote the commit blocks. A nonzero lease count
   defers that generation.
3. Ask every owner to seal its active block for the generation and wait for its
   exact flush fence. Each owner then reports block identities, live tagged
   bytes, dependency pins, and whether every block is sealed and durable.
4. On each owner, scan only that owner's transaction blocks. Ignore commit
   records and aborted tagged records. For a committed record, revalidate that
   it is still the current index winner; use the existing owner-routing path
   when the key belongs to another worker; append the winner to an ordinary
   records block with `txid=0`.
5. Wait for every destination relocation fence before allowing the tagged
   source record to cease being recovery evidence. Concurrent foreground
   writes are resolved by the same `RelocateIfCurrent` compare-and-update used
   by defrag: either the cleaner moves the exact current version, or it loses
   the race and the foreground write retires that source normally.
6. Ask every owner for a fresh exact inspection. A generation is reclaimable
   only when `active_transactions == 0`, live tagged bytes are zero,
   dependency pins are zero, and all blocks are sealed and durable. Promotion
   is awaited inline, so it needs no separate generation state or counter.
7. Each owner takes its store mutex, revalidates the block id, allocation epoch,
   kind, generation, pin/flush/defrag state and zero counters, destroys those
   exact block states, and durably returns the block ids. After every owner
   succeeds, owner tasks erase their local generation metadata. A failed
   revalidation leaves the generation derived as Closed for a later round; it
   never frees optimistically.

Cleaner failures are background-maintenance failures, not shutdown completion.
The winning periodic worker records the failure, leaves the cleaner dirty, and
continues periodic block flushing; a later cooldown retries the round. Only the
explicit shutdown branch calls the worker's shutdown-completion accounting.

The coordinator messages contain generation ids, immutable commit evidence and
owner-local requests/results. There is no per-record cross-worker notification
stream. Cross-worker value promotion reuses the existing key-owner task path;
generation completion is learned from task replies and durability fences.

#### Crash recovery and replicas

Recovery reads the persisted generation from every transaction block, rebuilds
the owner-local byte/pin-free block summaries and committed-txid sets, and
registers every recovered generation as Closed. It parks tagged records until
commit discovery is complete, exactly as the original 2PC recovery does. The
next Open id is greater than every recovered generation.

A crash before promoted destinations are durable leaves the old transaction
blocks allocated, so recovery still requires their commits. A crash after the
destinations are durable but before generation retirement may retain redundant
transaction blocks for one more round. Retirement only clears allocated blocks
after all destination fences, so recovery never observes missing commit
evidence with an incomplete promoted value.

Canonical replicated transactions execute the same storage transaction path on
a replica and automatically acquire generation leases there. Full-sync snapshot
records remain ordinary `txid=0` records. The periodic cleaner is local storage
maintenance and therefore runs independently on primary and replica without a
replication protocol message or a fixed worker assignment.

#### Required coverage

- The three-state lifecycle and every readiness blocker are unit-tested.
- Multi-worker committed transactions are promoted, remain readable during a
  cleaner round, and survive restart after their transaction blocks retire.
- A fault-injected multi-key transaction rolls back to its prior values while a
  short cleaner cooldown is enabled, and those values survive restart.
- Recovery of a sealed transaction generation resumes from Closed and can be
  cleaned without reusing its generation id.
- Primary and replica continue to apply canonical transactions through the same
  generation path; full-sync snapshot writes remain ordinary records.
