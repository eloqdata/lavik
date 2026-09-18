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

# Cross-Shard Architecture Design

> Storage ownership has evolved beyond the original worker-equals-shard model
> described here. See [Worker-Count-Independent Storage Ownership](storage-block-ownership.md)
> for the current persistent block, recovery, read, and defrag design.
> See [Logical Databases and SELECT](logical-databases.md) for current
> connection-level database selection and persistent `(db_id, key)` identity.

## Problem

Current `thread_local DbShard tls_db_` is a correctness bug: a key written on
worker 0 is invisible to a connection handled by worker 1. Data is split
non-deterministically by which worker accepted the connection, not by key.

## Target Model (ScyllaDB-style shared-nothing)

- **Data is partitioned by key hash**, deterministically: `shard = hash(key) % N`.
- **A shard's data is touched only by its owning worker thread** → no locks.
- **Cross-shard access goes through message passing** (`submit_to`), never direct.
- **Logical DBs (SELECT 0..15) are orthogonal to shards**: each shard holds a
  slice of every logical DB. 32 cores → 32 shards, each holding 1/32 of all 16 DBs.

## Ownership Layering

```
Runtime (celer)
 ├── workers_[0..N]          threads + io_uring + cross-core queues
 └── (generic submit_to)     run a closure on any worker, resume on origin

lavik
 ├── shards_[0..N]           the DATA: each = std::array<HashTable, 16>
 │                           shards_[i] touched only by worker i
 └── services (port+handler) Redis:6379, RPC:7000, Admin:8080
                             every handler references its worker's shard
```

**Data is NOT owned by any service.** Multiple services on the same worker share
that worker's shard (lock-free: same thread). The shard array lives outside all
services so any service — Redis, RPC, admin — reaches the same per-worker data.

## celer: generic cross-core primitive

```cpp
// Runs fn on target worker's thread; suspends caller; resumes caller on its
// own (origin) worker once the result comes back. fn must not block.
template <typename Fn>
auto SubmitTo(unsigned target, Fn fn) -> awaitable<std::invoke_result_t<Fn>>;
```

Mechanism (Seastar model):
- N×N **SPSC** queues (`_q[origin][target]` requests, `_q[target][origin]` replies).
  SPSC chosen over MPSC: key hashing produces all-to-all uniform traffic → MPSC
  would have heavy CAS contention; SPSC writes are contention-free. Core count
  is in SPSC's sweet spot (≤64).
- The work item lives in the awaiter (in the caller's coroutine frame) — no
  separate allocation.
- `await_suspend`: store handle, push work-item ptr into `_q[origin][target]`,
  wake target's eventfd.
- Target worker poll loop: pop work item, run `fn()`, store result, push back into
  `_q[target][origin]`, wake origin's eventfd.
- Origin worker poll loop: pop completion, `handle.resume()` — runs on origin. ✓

Note: the awaiting coroutine never migrates threads — the origin worker resumes
it. Task<T>'s symmetric-transfer stays single-threaded, so Task #10 is not
required for this path.

Fast path: `target == current_worker` → run `fn` inline, no queue, no suspend.

## lavik: shards + routing

```cpp
struct Shard { std::array<HashTable, 16> dbs_; };   // per-worker data slice

// In the Redis handler, current SELECT db = d:
unsigned target = hash(key) % N;
if (target == ctx.worker_id())
  v = shards_[target].dbs_[d].Get(key);                          // local
else
  v = co_await SubmitTo(target, [&]{ return shards_[target].dbs_[d].Get(key); });
```

## Implementation phases

**Phase A — celer cross-core infra**
- `SpscQueue<T>` ring (store-release / load-acquire, no CAS).
- `thread_local Worker* current worker`, worker id.
- Runtime allocates N×N queue matrix, hands each worker its row/column.
- `SubmitTo` awaiter; integrate drain into `Worker::Run` poll loop + eventfd wake.
- Test: trivial cross-core call returns correct result.

**Phase B — lavik shards**
- `Shard { std::array<HashTable,16> }`; Runtime/owner holds `shards_[0..N]`.
- `ShardContext { worker_id(), shards_ }` passed to handlers.

**Phase C — routing**
- Redis handler computes `hash(key) % N`, local vs `SubmitTo`.
- Replace `thread_local DbShard`. Verify multi-thread cross-connection consistency.

## Verification

- `redis-benchmark -t SET,GET --threads 4 -c 100 -P 10` correctness + throughput.
- Cross-connection: SET on conn A (worker 0), GET on conn B (worker 1) → hit.
