# Architecture index

This is the only documentation directory maintained as a current explanation
of Keylane's core design. It is a compact model of the implemented system, not
an implementation reference or release history. Source code and tests remain
authoritative when they disagree. Files under `docs/design-docs/` are historical
references rather than current architecture.

## Subsystem map

| Subsystem | Responsibility | Architecture document |
|---|---|---|
| System composition | Process lifecycle, runtime dependency, cross-cutting flows, and integrations | [System overview](01-overview.md) |
| Request and Redis serving | TCP/TLS sessions, RESP2/RESP3 negotiation, command dispatch, connection state, Lua/Functions, Pub/Sub, replies, and command handlers | [Request and Redis serving](02-request-serving.md) |
| Function catalog | Process-global Function ownership, hidden staging, crash-durable catalog commits, startup recovery, and replication integration | [Function catalog](07-function-catalog.md) |
| Transaction coordination | Per-worker intent arbitration, cross-worker scheduling, multi-hop execution, WATCH, and transaction completion | [Transaction coordination](03-transaction-coordination.md) |
| Storage and recovery | Logical indexes, append/read paths, durable format, devices, recovery, shutdown checkpoints, flushing, expiry, and reclamation | [Storage and recovery](04-storage-and-recovery.md), [Grouped collections](09-grouped-collections.md), [Shutdown index checkpoints](06-shutdown-index-checkpoints.md) |
| Replication | Native Keylane replication, Redis PSYNC interoperability, destructive full sync, online logs, Sentinel role changes, replay, and the fail-closed single-group cluster boundary | [Replication](05-replication.md) |
| Cluster data plane | Slot routing, finite authority admission, node-local control installation, Meta discovery/session handling, and Redis Cluster compatibility | [Cluster data plane](06-cluster-data-plane.md) |
| Meta control plane | NuRaft-backed committed metadata, projection and Data-session publishing, leader-local observations, authenticated administration and cluster status, membership identity, and WAL/snapshot recovery | [Meta control plane](08-meta-control-plane.md) |

Metrics, memory accounting, logging, configuration, and the Celer runtime cross
several subsystems and are summarized in the system overview rather than
treated as independent durable modules.

## Authoring standard

Architecture changes are claim-driven. For an implementation change, identify
the current claim or core model that would otherwise become false or materially
incomplete. An update is warranted when the change affects a durable module
boundary, primary control or data flow, ownership or lifecycle, durable or wire
format, external integration, or system-level correctness, safety, or
compatibility invariant. If no such claim exists, leave the architecture
unchanged. Dedicated documentation work may correct an inaccuracy, fill a
known core-design gap, or consolidate existing sediment.

Write the resulting design in the present tense and at the highest useful
level of abstraction. Preserve rationale that explains a stable constraint or
tradeoff. Revise or replace existing prose so the document stands on its own;
a reader should not need the originating commit to understand it.

Use the narrowest durable home for each kind of information:

| Information | Home |
|---|---|
| Core module responsibility, ownership, lifecycle, cross-module flow, durable or wire contract, and system-level invariant | `docs/architecture/` |
| Significant historical decision, alternatives, and superseded design context | `docs/design-docs/` |
| Supported operational procedure, prerequisite, or safety boundary | `docs/operations/` |
| Local algorithm, runtime representation, tuning mechanism, or code-level invariant | Nearby source or API documentation |
| Motivation for this change, before/after behavior, benchmark result, and implementation journey | Pull request or commit |

For example, a record-format change that removes a persisted field and makes
older media unreadable belongs here because it changes a durable contract and
compatibility boundary. The exact byte size of a runtime index entry, the
number of handles in an internal bucket, or a cached rehash plan belongs near
the implementation. Likewise, architecture may state that cross-worker
delivery preserves worker affinity and cannot strand accepted work; the exact
wake-coalescing handshake that currently realizes that invariant belongs near
the messaging code.

## Maintenance rule

Update the relevant focused document and its source map when the authoring test
above passes. Add, split, merge, or remove focused documents when the durable
module map changes. Keep proposals, design history, task investigations, and
change narratives outside this directory; they do not substitute for a current
architecture update.

## Source map

| Claim | Repository source |
|---|---|
| The process composes the `keylane` data-plane executable and one main Keylane library around Celer, plus the separate `keylane-meta` service and Raft-free Meta operator clients | `CMakeLists.txt`, `app/keylane.cpp`, `app/keylane_meta.cpp`, `app/keylane_meta_ctl.cpp`, `app/keylane_cluster.cpp`, `src/redis/server.cpp`, `include/keylane/meta/`, `src/meta/` |
| Request serving has distinct RESP-version, session, command, scripting, Function-catalog, Pub/Sub, and observability boundaries | `include/keylane/resp.h`, `include/keylane/resp_version.h`, `include/keylane/session.h`, `include/keylane/command.h`, `include/keylane/pubsub.h`, `include/keylane/slowlog.h`, `src/redis/` |
| The Function catalog is a durable module with one complete-catalog commit boundary | `src/redis/function_catalog.h`, `src/redis/function_catalog.cpp`, `src/storage/engine/system_state.cpp` |
| Transaction coordination has its own interfaces and implementation lifecycle | `include/keylane/tx/`, `src/tx/` |
| Storage exposes a durable engine boundary with focused implementation units | `include/keylane/storage/`, `src/storage/` |
| Shutdown checkpoints are optional one-shot recovery accelerators published through fixed metadata | `src/storage/engine/checkpoint.cpp`, `src/storage/engine/flush.cpp`, `src/storage/engine/recovery.cpp` |
| Replication has manager, boot-scoped single-group coordination, a callable cluster-rebuild adapter behind fail-closed admission, Sentinel-compatible configuration, and storage-log integration boundaries | `include/keylane/replication.h`, `include/keylane/replication_group.h`, `src/config.cpp`, `src/replication/`, `src/storage/engine/replication_log.cpp`, `tests/replication_group_test.cpp`, `tests/cluster/population_integration_test.cpp`, `tests/cluster/rebuild_protocol_integration_test.cpp` |
| Cluster data plane has topology, authority, node-controller, static-control, Meta-client/session, and Redis gate boundaries | `include/keylane/cluster/`, `src/cluster/`, `src/redis/cluster_command.cpp`, `src/redis/command.cpp`, `src/redis/server.cpp` |
| Meta control plane separates deterministic committed state, pure node projection, volatile observations, leader-scoped Data publishing, authenticated administration and stable cluster status, and NuRaft integration | `include/keylane/meta/`, `src/meta/`, `app/keylane_meta.cpp`, `app/keylane_meta_ctl.cpp`, `app/keylane_cluster.cpp` |
| Celer is a pinned runtime submodule | `.gitmodules`, `CMakeLists.txt`, `celer/include/celer/`, `celer/src/` |
