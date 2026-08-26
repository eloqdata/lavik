# System overview

## System context

Keylane is a Linux C++23 server that accepts Redis/Valkey-compatible RESP2
traffic and persists the resulting logical data to local files, block devices,
or SPDK NVMe namespaces. The process uses the Celer submodule for its
thread-per-worker coroutine runtime, TCP/TLS transport, cross-core messaging,
HTTP service, and storage I/O backends.

The executable is one process with worker-affine state rather than a collection
of networked services. Redis serving, metrics, replication, transaction
coordination, and storage are composed in `RunServer`; Celer owns the worker and
socket lifecycle underneath those Keylane modules.

```text
Redis/Valkey clients and replicas
                |
        Celer TCP/TLS services
                |
     RESP session and command layer
                |
     transaction coordination -------- replication manager
                |                              |
          storage engine <------------- replication log/replay
                |
       file, block-device, or SPDK I/O

Prometheus scrapes a separate Celer HTTP service backed by worker/storage
snapshots.
```

## Component responsibilities

| Component | Responsibility | Main interface |
|---|---|---|
| Process shell | Parse configuration, initialize logging and memory limits, compose modules, start services, and coordinate graceful shutdown | `app/keylane.cpp`, `keylane::RunServer` |
| Celer runtime | Own worker threads, coroutines, cross-core submissions, TCP/TLS sessions, HTTP serving, and I/O backends | `celer::Server`, `celer::TcpService`, `celer::Worker`, `celer::SubmitTaskTo` |
| Request and Redis serving | Parse RESP, retain connection state, classify and dispatch commands, and encode or stream replies | `RedisService`, `DispatchCommand`, `ExecuteCommand` |
| Transaction coordination | Serialize conflicting key access across workers and execute single- or multi-shard command hops | `tx::TxRuntime`, `tx::Transaction`, `tx::TxShard` |
| Storage and recovery | Own logical indexes and physical blocks, execute reads and appends, recover durable state, and reclaim obsolete data | `storage::StorageEngine` |
| Replication | Manage node role and sessions, publish native logs, run full/partial synchronization, interoperate with Redis PSYNC, and apply trusted replay | `ReplicationManager` |
| Observability and limits | Maintain worker-local command/connection metrics, expose Prometheus snapshots, account process memory, and enforce admission estimates | `RenderPrometheusMetrics`, `InitMemoryLimit`, `WouldExceedMemoryLimit` |

## Process lifecycle

1. `main` applies mimalloc defaults, optionally loads a Redis-style config
   file, parses CLI overrides, validates the combined options, and initializes
   logging.
2. `RunServer` initializes the memory budget, signal handling, storage engine,
   replication manager, command/storage bindings, metrics shards, transaction
   runtime, and Celer service graph.
3. On every worker, `RedisService::Run` binds the memory and transaction shards
   and awaits `StorageEngine::InitializeWorker`. Recovery barriers ensure all
   workers finish recovery and allocator cleanup before the process becomes
   ready.
4. Worker 0 performs an optional validated RDB import before the readiness flag
   is published. Replication is notified only after worker storage is ready.
5. On a shutdown signal, new requests and accepts are closed, active requests
   and RDB backup work drain, storage is durably flushed, and then Celer workers
   are stopped and joined.

## Primary flows

### Client command

`RedisService` incrementally parses a connection's byte stream, creates a
`CommandRequest` from static command metadata, and dispatches it with the
connection's explicit logical database. Dispatch handles connection-scoped
state such as authentication, `SELECT`, `MULTI`/`EXEC`, `WATCH`, and replica
read routing. Keyed work runs on the owning worker, using transaction
coordination when the command spans keys or requires ordered multi-hop work.
The result is encoded into a reusable reply buffer, sent directly from a
storage read lease, or emitted as bounded streamed chunks.

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
  origin worker.
- Logical database identity is carried in each command and durable record; it
  is not inferred from the worker executing a request.
- The command table is the shared classification source for arity, key
  positions, write/read behavior, global fan-out, database gates, and blocking
  behavior.
- Conflicting key access uses the transaction module. Storage background work
  participates in the same arbitration boundary rather than maintaining an
  independent client lock system.
- A node configured as a replica does not create authoritative local expiry
  mutations, and replayed commands do not republish themselves.
- Readiness follows recovery and optional import; shutdown drains admitted
  requests before the final storage flush.
- Replication and full-sync queues use admission/backpressure. They must not
  silently drop an already accepted logical write.
- Process-memory admission is a conservative sampled guard, not an allocator
  hard wall; diagnostics and enforcement have distinct update paths.

## External integrations

| Integration | Boundary |
|---|---|
| Celer | Pinned git submodule compiled into Keylane for runtime, network, TLS, cross-core, HTTP, io_uring, and optional SPDK support |
| mimalloc | Pinned allocator submodule plus Keylane new/delete accounting hooks |
| OpenSSL | TLS server/client contexts; release builds can link it statically |
| Redis/Valkey clients | RESP2 command and reply compatibility at the listener |
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
| Request/session/command flow | `include/keylane/resp.h`, `include/keylane/session.h`, `include/keylane/command.h`, `src/redis/` |
| Transaction boundary | `include/keylane/tx/`, `src/tx/` |
| Storage boundary and focused lifecycle units | `include/keylane/storage/engine.h`, `include/keylane/storage/format.h`, `src/storage/engine/`, `src/storage/format.cpp` |
| Replication manager, protocol, and log boundary | `include/keylane/replication.h`, `include/keylane/replication_command.h`, `src/replication/`, `src/storage/engine/replication_log.cpp` |
| Memory accounting and Prometheus service | `include/keylane/memory.h`, `src/memory.cpp`, `include/keylane/metrics.h`, `src/metrics.cpp` |
| Build, release, and package commands | `scripts/build_debug.sh`, `scripts/build_release.sh`, `scripts/package_release.sh`, `docs/operations/building-and-packaging.md` |
| Keylane process deployment unit or orchestration manifest | Unknown; `deploy/` contains the monitoring stack, not the Keylane process definition |
