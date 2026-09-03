# System overview

## System context

Keylane is a Linux C++23 server that accepts Redis/Valkey-compatible commands.
Connections start with RESP2 reply semantics and can negotiate RESP2 or RESP3
with `HELLO`. Keylane persists the resulting logical data to local files, block
devices, or SPDK NVMe namespaces. The process uses the Celer submodule for its
thread-per-worker coroutine runtime, TCP/TLS transport, cross-core messaging,
HTTP service, and storage I/O backends.

The executable is one process with worker-affine state rather than a collection
of networked services. Redis serving, metrics, replication, transaction
coordination, and storage are composed in `RunServer`; Celer owns the worker and
socket lifecycle underneath those Keylane modules.

```text
Redis/Valkey clients, Sentinels, and replicas
                |
        Celer TCP/TLS services
                |
    RESP2/RESP3 session and command layer
          /                         \
 command handlers and Lua       worker-local Pub/Sub
          |                       registries/fan-out
 transaction coordination -------- replication manager
          |                  Function catalog
          |                    /         \
    storage engine <--- system state   replication log/replay
          |
 file, block-device, or SPDK I/O

Prometheus scrapes a separate Celer HTTP service backed by worker and storage
snapshots.
```

## Component responsibilities

| Component | Responsibility | Main interface |
|---|---|---|
| Process shell | Parse configuration, initialize logging and memory limits, compose modules, start services, and coordinate graceful shutdown | `app/keylane.cpp`, `keylane::RunServer` |
| Celer runtime | Own worker threads, coroutines, cross-core submissions, TCP/TLS sessions, HTTP serving, and I/O backends | `celer::Server`, `celer::TcpService`, `celer::Worker`, `celer::SubmitTaskTo` |
| Request and Redis serving | Parse commands, negotiate RESP reply semantics, retain connection state, run Lua and Pub/Sub, classify and dispatch commands, and encode or stream replies | `RedisService`, `DispatchCommand`, `ExecuteCommand` |
| Transaction coordination | Serialize conflicting key access across workers and execute single- or multi-shard command hops | `tx::TxRuntime`, `tx::Transaction`, `tx::TxShard` |
| Storage and recovery | Own logical indexes and physical blocks, execute reads and appends, recover durable state, and reclaim obsolete data | `storage::StorageEngine` |
| Function catalog | Stage one complete process-global Function definition set on every worker, commit its existing `FUNCTION DUMP` encoding, swap runtimes, and recover it before service readiness | `FunctionCatalog` |
| Replication | Own one replication group, node role and sessions; publish native logs, run full/partial synchronization, interoperate with Redis PSYNC and Sentinel, and apply trusted replay | `ReplicationManager` |
| Observability and limits | Maintain worker-local command, connection, and slow-log state, expose Prometheus snapshots, account retained memory, and enforce admission estimates | `RenderPrometheusMetrics`, `MaybeRecordSlowCommand`, `InitMemoryLimit`, `WouldExceedMemoryLimit` |

## Process lifecycle

1. `main` loads an optional Redis-style config file, applies CLI overrides,
   validates the combined options, and initializes logging.
2. `RunServer` initializes the memory budget, signal handling, storage engine,
   replication manager, command/storage bindings, metrics shards, transaction
   runtime, and Celer service graph.
3. On every worker, `RedisService::Run` binds the memory and transaction shards
   and awaits `StorageEngine::InitializeWorker`. Recovery barriers ensure all
   workers finish recovery and allocator cleanup before the process becomes
   ready.
4. Worker 0 recovers and validates the durable Function catalog on every
   worker, then performs an optional validated RDB import before the readiness
   flag is published. Replication is notified only after worker storage and
   catalog recovery are ready.
5. On a shutdown signal, new requests and accepts are closed, active requests
   and RDB backup work drain, and storage is durably flushed. When configured,
   shutdown transaction cleaning first relocates committed tagged winners into
   durable ordinary records; a shutdown checkpoint then serializes the frozen
   key indexes and publishes them before worker teardown. Celer then stops
   each worker; after that worker's I/O and coroutine frames are gone but
   before its native thread exits and is joined,
   `RedisService::FinalizeWorker` calls `StorageEngine::FinalizeWorker` to
   release worker-owned indexes and storage state.

## Primary flows

### Client command

`RedisService` incrementally parses a connection's byte stream, creates a
`CommandRequest` from static command metadata, and dispatches it with the
connection's explicit logical database and negotiated reply version. Dispatch
handles connection-scoped state such as authentication, `HELLO`, `SELECT`,
`MULTI`/`EXEC`, `WATCH`, Pub/Sub subscriptions, and replica read routing. Keyed
work runs on the owning worker, using transaction coordination when the command
spans keys or requires ordered multi-hop work. MGET holds one shared-lock
transactional view while the participating workers resolve their keys. Replies
use the connection's negotiated protocol, with large results emitted as bounded
streamed chunks.

Lua `EVAL`/`EVALSHA` and stored `FCALL` invocations run in a persistent
worker-local VM. Their declared keys establish the transaction boundary;
`redis.call` re-enters restricted command execution inside those retained
holds, and successful write effects are committed and replicated with the
outer invocation. Script-cache mutations fan out to every worker before the
mutation command returns, while Function invocations and catalog operations
share a catalog barrier that hides staged Function updates. Function mutations
acknowledge only after the complete target dump is crash-durable and the
worker runtimes have swapped; replicas apply that same boundary before
advancing their event cursor.

Pub/Sub keeps subscription registries on each connection's worker. Commands and
cross-worker publications feed bounded per-session output encoded for the
connection's negotiated protocol; a slow subscriber is closed rather than
permitted unbounded output growth.

### Durable write and publication

The command layer performs role, memory, database-gate, and replication
publisher admission before entering the mutation. The transaction and storage
layers keep key arbitration and physical append/recovery state separate.
Committed source writes publish deterministic logical commands or transaction
envelopes to worker-local replication logs; replay re-enters trusted command or
storage paths with publication disabled.

### Recovery and readiness

Storage preparation validates the configured device set before workers start.
Per-worker initialization reconstructs durable metadata, indexes, transaction
evidence, and block accounting before the Redis service advertises readiness.
The metrics service can exist during startup but receives the same readiness
state explicitly.

## Cross-cutting invariants

- Worker-affine mutable state is accessed on its owner worker; cross-worker
  work uses Celer submission primitives. Coroutine coordinators resume on their
  origin worker. Celer may batch cross-worker delivery, but accepted work
  remains discoverable across concurrent posts, drains, and worker wakeups and
  cannot be stranded.
- Logical database identity is carried in each command and durable record; it
  is not inferred from the worker executing a request.
- The command table is the shared classification source for arity, key
  positions, write/read behavior, global fan-out, database gates, and blocking
  behavior.
- Reply encoding follows the connection's current `RespVersion`. An `EXEC`
  freezes its outer aggregate encoding at entry, while a queued `HELLO` can
  change later child replies and the post-`EXEC` connection version. `HELLO`
  validates authentication and client-name options before changing connection
  state, and Pub/Sub changes its frame encoding with the session.
- Conflicting key access uses the transaction module. Storage background work
  participates in the same arbitration boundary rather than maintaining an
  independent client lock system.
- Lua calls cannot escape their declared-key transaction or recursively enter
  administrative, global, or scripting commands. Blocking list and sorted-set
  operations execute one immediate attempt without registering a waiter;
  stream reads allow only the non-`BLOCK` form, and `WAIT` immediately checks
  the caller's pre-script replication watermark. Worker-local Lua executions
  are serialized because cached closures share VM globals. Ordinary commands do
  not take that worker-local execution gate, but once an invocation exceeds
  `lua-time-limit`, a process-wide busy flag rejects ordinary client commands
  until it finishes or an eligible kill succeeds.
- A node configured as a replica does not create authoritative local expiry
  mutations, and replayed commands do not republish themselves.
- Each process owns exactly one replication group. Boot, history, and replica
  incarnation identities are distinct; a process restart creates a new
  history and therefore requires whole-group full synchronization.
- Full-sync start durably fences the prior population, promotion base, and
  Function-catalog readiness. Neither a valid old catalog root nor an
  interrupted replacement is sufficient to reopen service.
- Readiness follows recovery and optional import; shutdown drains admitted
  requests before the final storage flush.
- Replication and full-sync queues use admission/backpressure. They must not
  silently drop an already accepted logical write.
- `maxmemory` admission uses explicit worker-owned retained allocations rather
  than global allocation hooks. RSS and mimalloc committed/reserved statistics
  remain diagnostic, so the retained waterline is not an instantaneous RSS
  hard wall.

## External integrations

| Integration | Boundary |
|---|---|
| Celer | Pinned git submodule compiled into Keylane for runtime, network, TLS, cross-core, HTTP, io_uring, and optional SPDK support |
| mimalloc | Pinned allocator submodule; the official global new/delete override serves ordinary C++ allocations, while retained storage calls mimalloc through explicitly accounted domains |
| OpenSSL | TLS server/client contexts; release builds can link it statically |
| Redis/Valkey clients | RESP2 by default; `HELLO 2`/`HELLO 3` selects connection-level reply semantics, including RESP3 maps, sets, booleans, doubles, nulls, and push frames where handlers expose them |
| Redis Sentinel | Discovers topology through Redis-compatible `INFO`, `ROLE`, client metadata, and Pub/Sub connections; drives failover with `REPLICAOF`, `CONFIG REWRITE`, and client eviction, using `replica-priority` for candidate preference |
| Keylane or Redis upstreams/downstreams | Native replication, Redis PSYNC following, and Redis-compatible export |
| Local storage | Existing files, raw block devices, or `spdk://` namespaces supplied through repeated `--data-file` options |
| RDB files | Startup import and Redis-compatible `SAVE`/`BGSAVE` output through filesystem paths |
| Prometheus/Grafana | Plaintext HTTP `/metrics`; optional Compose deployment under `deploy/monitoring/` |

No production service-manager unit, orchestration manifest for the Keylane
process itself, or supported platform matrix is defined in this repository;
those deployment boundaries remain unknown here.

## Source map

| Claim | Repository source |
|---|---|
| Language level, targets, dependencies, source units, and test entry points | `CMakeLists.txt` |
| CLI/config parsing and top-level process entry | `app/keylane.cpp`, `include/keylane/config.h`, `src/config.cpp` |
| Module construction, worker startup barriers, readiness, and shutdown ordering | `include/keylane/server.h`, `src/redis/server.cpp` |
| Celer runtime and service dependency | `.gitmodules`, `celer/include/celer/runtime/`, `celer/include/celer/net/`, `celer/src/` |
| Request/session/command flow and negotiated RESP semantics | `include/keylane/resp.h`, `include/keylane/resp_version.h`, `include/keylane/session.h`, `include/keylane/command.h`, `src/redis/server.cpp`, `src/redis/resp.cpp` |
| Lua scripts, Function catalog lifecycle, and their transaction boundary | `src/redis/lua_eval.h`, `src/redis/lua_eval.cpp`, `src/redis/function_catalog.h`, `src/redis/function_catalog.cpp`, `src/redis/command.cpp` |
| Pub/Sub sessions, worker-local registries, fan-out, and bounded output | `include/keylane/pubsub.h`, `src/redis/pubsub.cpp`, `src/redis/server.cpp` |
| Transaction boundary | `include/keylane/tx/`, `src/tx/` |
| Storage boundary and focused lifecycle units | `include/keylane/storage/engine.h`, `include/keylane/storage/format.h`, `src/storage/engine/`, `src/storage/format.cpp` |
| Replication manager, protocol, Sentinel-visible role state, and log boundary | `include/keylane/replication.h`, `include/keylane/replication_command.h`, `src/replication/`, `src/storage/engine/replication_log.cpp`, `tests/sentinel_e2e_test.cpp` |
| Memory accounting, slow log, command statistics, and Prometheus service | `include/keylane/memory.h`, `src/memory.cpp`, `include/keylane/metrics.h`, `src/metrics.cpp`, `include/keylane/slowlog.h`, `src/redis/slowlog.cpp` |
| Build, release, and package commands | `scripts/build_debug.sh`, `scripts/build_release.sh`, `scripts/package_release.sh`, `docs/operations/building-and-packaging.md` |
| Keylane process deployment unit or orchestration manifest | Unknown; `deploy/` contains the monitoring stack, not the Keylane process definition |
