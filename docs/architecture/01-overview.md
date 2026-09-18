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

# System overview

## System context

Lavik is a Linux C++23 server that accepts Redis/Valkey-compatible commands.
Connections start with RESP2 reply semantics and can negotiate RESP2 or RESP3
with `HELLO`. Lavik persists the resulting logical data to local files, block
devices, or SPDK NVMe namespaces. The process uses the Bycorf submodule for its
thread-per-worker coroutine runtime, TCP/TLS transport, cross-core messaging,
HTTP service, and storage I/O backends.

The executable is one process with worker-affine state rather than a collection
of networked services. Redis serving, metrics, replication, transaction
coordination, the cluster data plane, and storage are composed in `RunServer`;
Bycorf owns the worker and socket lifecycle underneath those Lavik modules.

Network and storage backends are independently selected at startup and remain
fixed for the process lifetime, including storage preparation before workers
start. The default build includes only Linux TCP and io_uring. The opt-in
`LAVIK_KERNEL_BYPASS` build includes DPDK and SPDK together; it does not
activate them. Startup defaults remain Linux TCP and io_uring, including for
the Meta control plane. Each worker owns one io_uring shared by kernel
network/storage I/O, timers and wakeups; the DPDK adapter borrows that ring.
The optional DPDK network backend serves
plaintext IPv4 TCP through Bycorf's private FreeBSD stack while preserving the
same RESP stream interface and worker ownership. Software packet forwarding
allows connection owners to outnumber hardware queue pairs. File I/O and
cross-worker wakeups retain io_uring, and networking shares one EAL with SPDK.
This integration is an experimental standalone serving profile; its TLS,
replication handoff, and cluster paths are outside the validation scope. See
the [build guide](../operations/building-and-packaging.md#experimental-dpdk-networking)
and Bycorf's [network architecture](../../bycorf/docs/architecture/networking.md).

A separate `lavik-meta` executable runs the [Raft-backed meta control
plane](08-meta-control-plane.md). It owns committed cluster metadata,
leader-local observations, authenticated administration, and coordination
plus process-lifetime Data-control sessions. Six durable stores share one Raft
state machine; Topology owns each Group's sole term, owner, authority and
failover state. Data consumes a complete bootstrap once, then independent
routing, local control and task updates. Initial creation, Meta-member
addition/removal, and per-Group failover are recovered by the current leader,
independent of an Admin client's connection or wait deadline. It links the
pinned NuRaft submodule, whose native Asio service owns Raft peer communication;
Bycorf owns the separate administrative and Data-node sessions. The Raft-free
`lavik-ctl` operator client sends direct administrative commands; its
`cluster-status` command discovers the current Meta leader and reads one stable
cluster-readiness cut through that surface; `failover` submits a durable
controlled transition and `getop` follows its operator-visible outcome. NuRaft
is linked only into `lavik-meta`: the data-plane executable, operator client, their supporting
libraries, and their focused tests never see consensus code, and the build
enforces that boundary at configure time.

```text
Redis/Valkey clients, Sentinels, and replicas
                |
        Bycorf TCP/TLS services
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

Prometheus scrapes a separate Bycorf HTTP service backed by worker and storage
snapshots.

lavik-meta Raft leader <-- framed control session --> Data NodeControl
       committed view       full state / lease /        topology, authority,
                            directive / observation      failover/replication

operator --> lavik-ctl cluster-status / failover / getop
                         |
                    Meta Admin seed --> current Meta leader
                      clusterhead           clusterstatus
```

## Component responsibilities

| Component | Responsibility | Main interface |
|---|---|---|
| Process shell | Parse configuration, initialize logging and memory limits, compose modules, start services, and coordinate graceful shutdown | `app/lavik.cpp`, `lavik::RunServer` |
| Bycorf runtime | Own worker threads, coroutines, cross-core submissions, TCP/TLS sessions, HTTP serving, and I/O backends | `bycorf::Server`, `bycorf::TcpService`, `bycorf::Worker`, `bycorf::SubmitTaskTo` |
| Request and Redis serving | Parse commands, negotiate RESP reply semantics, retain connection state, run Lua and Pub/Sub, classify and dispatch commands, and encode or stream replies | `RedisService`, `DispatchCommand`, `ExecuteCommand` |
| Transaction coordination | Serialize conflicting key access across workers and execute single- or multi-shard command hops | `tx::TxRuntime`, `tx::Transaction`, `tx::TxShard` |
| Storage and recovery | Own logical indexes and physical blocks, execute reads and appends, recover durable state, and reclaim obsolete data | `storage::StorageEngine` |
| Function catalog | Stage one complete process-global Function definition set on every worker, commit its existing `FUNCTION DUMP` encoding, swap runtimes, and recover it before service readiness | `FunctionCatalog` |
| Replication | Own one replication group, node role and sessions; publish native logs, run full/partial synchronization, prepare and activate a Meta-selected successor, follow the committed owner, interoperate with Redis PSYNC and Sentinel, and apply trusted replay | `ReplicationManager` |
| Cluster data plane | Admit, redirect, pause, or refuse requests by Meta-projected slot ownership, failover state, and finite authority; reconcile failover actions and owner following; serve Redis Cluster discovery | `cluster::AuthorityGuard::CaptureAndAdmit` / `RegisterAndRecheck`, `cluster::TopologyCache`, `cluster::NodeControlInstaller`, `cluster::MetaControlClientService` |
| Meta control plane | Replicate metadata commands and typed global Policy, project node-specific desired state, publish leader-scoped Data sessions, detect sustained current-Owner failure, reconcile committed failover transitions, and expose authenticated administration plus stable cluster readiness | `meta::MetaCoordinator`, `meta::MetaStateMachine`, `meta::MetaControlProjector`, `meta::MetaDataControlServer`, `meta::MetaAutomaticFailoverReconciler`, `meta::MetaFailoverReconciler` |
| Observability and limits | Maintain worker-local command, connection, and slow-log state, expose Prometheus snapshots, account retained memory, and enforce admission estimates | `RenderPrometheusMetrics`, `MaybeRecordSlowCommand`, `InitMemoryLimit`, `WouldExceedMemoryLimit` |

## Process lifecycle

1. `main` loads an optional Redis-style config file, applies CLI overrides,
   validates the combined options, and initializes logging.
2. `RunServer` selects Bycorf's process-wide I/O backends before storage probing,
   buffer allocation, or worker startup freezes the selection. It then
   initializes the memory budget, signal handling, storage engine, replication
   manager, cluster topology/authority/node-controller runtime,
   command/storage bindings, metrics shards, transaction runtime, and Bycorf
   service graph. Cluster mode always starts the outbound Meta control client
   fenced; no topology or positive authority is restored locally.
3. On every worker, `RedisService::Run` binds the memory and transaction shards
   and awaits `StorageEngine::InitializeWorker`. Recovery barriers ensure all
   workers finish recovery and allocator cleanup before the process becomes
   ready.
4. Worker 0 recovers and validates the durable Function catalog on every
   worker, then performs an optional validated RDB import before the readiness
   flag is published. Replication is notified only after worker storage and
   catalog recovery are ready.
5. On a shutdown signal, new requests and accepts are closed; Meta control,
   active requests, replication target/source work, and RDB backup work drain
   before storage is durably flushed. When configured, shutdown transaction
   cleaning relocates committed tagged winners into durable ordinary records;
   a second seal/drain then freezes the resulting indexes before a shutdown
   checkpoint publishes them. Bycorf then stops
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

### Meta-managed failover

For each Group, the caught-up Meta leader joins the current typed automatic
failover Policy and committed Owner authority to current authenticated Data
observations. `MetaAutomaticFailoverReconciler` treats stale or causally
incomplete evidence as indeterminate, debounces only exact Owner failures on a
monotonic leader-local clock, and submits one candidate-less automatic
uncontrolled Begin. That Begin's Raft commit atomically advances and fences the
Group term and installs the durable transition. A replacement Meta leader
discards pre-commit detector time; after Begin commits, it needs only the
durable transition described below.

Each Group may carry one committed `FailoverTransition` beside its current
topology and authority. A leader-scoped, level-triggered reconciler derives the
next typed Raft command from that transition plus fresh Data observations, so a
new Meta leader resumes the same transition without recovering process-local
workflow state. Controlled failover keeps the current owner and lease while
Data pauses and drains mutations, catches the chosen replica through a stable
frontier, and prepares it without opening service. Cutover atomically commits
the successor owner, grant, action-bound activation identity, and cleared
transition. Uncontrolled failover fences first, selects the best eligible
observed compatibility domain, and replaces a failed candidate immediately.

The successor activates its boot-local prepared context only under the matching
committed grant and finite lease. Subsequent full desired state makes every
other replica follow the new owner through the existing native continuation or
full-sync path. A former owner retires its old source history and backlog when
it consumes that follow-owner relationship; Meta does not orchestrate backlog
cleanup as another durable phase.

## Cross-cutting invariants

- Worker-affine mutable state is accessed on its owner worker; cross-worker
  work uses Bycorf submission primitives. Coroutine coordinators resume on their
  origin worker. Bycorf may batch cross-worker delivery, but accepted work
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
  requests and every replication storage mutator before the final flush.
- Before the first stable release, Lavik-owned durable and control formats
  use v1 with in-place schema replacement and no compatibility promise for
  earlier development layouts. Incompatible data directories must be recreated.
  Lavik uses its own format signatures, `LAVIK.*` native commands and
  `lavik://` principal URIs; obsolete development identifiers are not
  accepted as aliases. External standards such as Redis RESP/RDB retain their
  own versioning.
- Replication and full-sync queues use admission/backpressure. They must not
  silently drop an already accepted logical write.
- Meta decisions derive from one committed view plus observations accepted by
  the current leader session generation. Observations are never Raft state and
  are purged on role changes or when their committed node, assignment, term,
  manifest, or partition replication epoch anchor becomes stale. Failover
  source-pause, prepared-candidate, and action-failure observations remain
  independent of the steady-state heartbeat role and are re-reported after a
  Meta leader change. Candidate history is checked against the authenticated
  `ClientHello` session; task completion uses an idempotent terminal result
  and Raft commit acknowledgement.
- Data nodes restore no positive serving authority, desired-state checkpoint,
  or directive outcome from their data files. Each restart begins fenced with
  a new boot identity; only a current Meta session and unexpired in-memory
  lease can authorize a local owner, and an authority transition drains the
  request generation it replaces.
- Metadata apply is deterministic and replay-safe by log index. A rejected
  domain command still consumes its index and creates an audit record;
  malformed durable bytes or inconsistent replay fail stop rather than
  allowing replicas to diverge.
- `maxmemory` admission uses explicit worker-owned retained allocations rather
  than global allocation hooks. RSS and mimalloc committed/reserved statistics
  remain diagnostic, so the retained waterline is not an instantaneous RSS
  hard wall.

## External integrations

| Integration | Boundary |
|---|---|
| Bycorf | Pinned git submodule compiled into Lavik for runtime, network, TLS, cross-core, HTTP, io_uring, and optional SPDK support |
| mimalloc | Pinned allocator submodule; the official global new/delete override serves ordinary C++ allocations, while retained storage calls mimalloc through explicitly accounted domains |
| NuRaft | Pinned Raft consensus submodule linked only by `lavik-meta`; its native Asio service owns Raft peer sockets, timers, and TLS and uses the Asio headers shipped in the NuRaft source tree |
| OpenSSL | TLS server/client contexts; release builds can link it statically |
| Redis/Valkey clients | RESP2 by default; `HELLO 2`/`HELLO 3` selects connection-level reply semantics, including RESP3 maps, sets, booleans, doubles, nulls, and push frames where handlers expose them |
| Redis Sentinel | Discovers topology through Redis-compatible `INFO`, `ROLE`, client metadata, and Pub/Sub connections; drives failover with `REPLICAOF`, `CONFIG REWRITE`, and client eviction, using `replica-priority` for candidate preference |
| Lavik or Redis upstreams/downstreams | Native replication, Redis PSYNC following, and Redis-compatible export |
| Local storage | Existing files, raw block devices, or `spdk://` namespaces supplied through repeated `--data-file` options |
| RDB files | Startup import and Redis-compatible `SAVE`/`BGSAVE` output through filesystem paths |
| Prometheus/Grafana | Plaintext HTTP `/metrics`; optional Compose deployment under `deploy/monitoring/` |

No production service-manager unit, orchestration manifest for the Lavik
process itself, or supported platform matrix is defined in this repository;
those deployment boundaries remain unknown here.

## Source map

| Claim | Repository source |
|---|---|
| Language level, targets, dependencies, source units, and test entry points | `CMakeLists.txt` |
| Meta control-plane composition, failover reconciler, and NuRaft layering boundary | `CMakeLists.txt`, `app/lavik_meta.cpp`, `include/lavik/meta/`, `src/meta/`, `.gitmodules` |
| CLI/config parsing and top-level process entry | `app/lavik.cpp`, `include/lavik/config.h`, `src/config.cpp` |
| Module construction, worker startup barriers, readiness, and shutdown ordering | `include/lavik/server.h`, `src/redis/server.cpp` |
| Bycorf runtime and service dependency | `.gitmodules`, `bycorf/include/bycorf/runtime/`, `bycorf/include/bycorf/net/`, `bycorf/src/` |
| Request/session/command flow and negotiated RESP semantics | `include/lavik/resp.h`, `include/lavik/resp_version.h`, `include/lavik/session.h`, `include/lavik/command.h`, `src/redis/server.cpp`, `src/redis/resp.cpp` |
| Lua scripts, Function catalog lifecycle, and their transaction boundary | `src/redis/lua_eval.h`, `src/redis/lua_eval.cpp`, `src/redis/function_catalog.h`, `src/redis/function_catalog.cpp`, `src/redis/command.cpp` |
| Pub/Sub sessions, worker-local registries, fan-out, and bounded output | `include/lavik/pubsub.h`, `src/redis/pubsub.cpp`, `src/redis/server.cpp` |
| Transaction boundary | `include/lavik/tx/`, `src/tx/` |
| Storage boundary and focused lifecycle units | `include/lavik/storage/engine.h`, `include/lavik/storage/format.h`, `src/storage/engine/`, `src/storage/format.cpp` |
| Replication manager, cluster failover/follow-owner adapters, protocol, Sentinel-visible role state, and log boundary | `include/lavik/replication.h`, `include/lavik/replication_command.h`, `src/replication/`, `src/storage/engine/replication_log.cpp`, `tests/cluster/replication_manager_integration_test.cpp`, `tests/sentinel_e2e_test.cpp` |
| Cluster topology, authority, controlled mutation pause, node control, and Meta/Data session | `include/lavik/cluster/`, `src/cluster/`, `src/redis/cluster_gate.h`, `src/redis/command.cpp`, `src/redis/blocking_wait.cpp`, `src/redis/server.cpp` |
| Memory accounting, slow log, command statistics, and Prometheus service | `include/lavik/memory.h`, `src/memory.cpp`, `include/lavik/metrics.h`, `src/metrics.cpp`, `include/lavik/slowlog.h`, `src/redis/slowlog.cpp` |
| Build, release, and package commands | `scripts/build_debug.sh`, `scripts/build_release.sh`, `scripts/package_release.sh`, `docs/operations/building-and-packaging.md` |
| Lavik process deployment unit or orchestration manifest | Unknown; `deploy/` contains the monitoring stack, not the Lavik process definition |
