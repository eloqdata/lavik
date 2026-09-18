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

# Architecture index

This is the only documentation directory maintained as a current explanation
of Lavik's core design. It is a compact model of the implemented system, not
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
| Replication | Native Lavik replication, Redis PSYNC interoperability, destructive full sync, online logs, Sentinel role changes, Meta-managed bounded Candidate Recovery, direct-parent reparent, promotion/follow-owner control, replay, and the fail-closed single-group cluster boundary | [Replication](05-replication.md) |
| Cluster data plane | Slot routing, finite authority admission, controlled-failover write pause, selected local control, independent route/task updates, failover activation and replica following, Meta discovery/session handling, and Redis Cluster compatibility | [Cluster data plane](06-cluster-data-plane.md) |
| Meta control plane | Raft-backed metadata, manifest-bootstrapped multi-member genesis, six durable stores with Topology-owned lifecycle, per-Group term/owner/authority and failover transitions, atomic creation admission, Data-session publishing, cluster status, leader-owned workflow recovery, membership identity, and WAL/snapshots | [Meta control plane](08-meta-control-plane.md) |
| Meta Raft runtime | Single-owner consensus, asynchronous WAL/application dependencies, snapshot recovery, peer authentication and bounded quorum liveness | [Meta Raft runtime](10-meta-raft.md) |

Metrics, memory accounting, logging, configuration, and the Bycorf runtime cross
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
| The process composes the `lavik` data-plane executable and one main Lavik library around Bycorf, plus the separate `lavik-meta` service and Raft-free `lavik-ctl` operator client | `CMakeLists.txt`, `app/lavik.cpp`, `app/lavik_meta.cpp`, `app/lavik_ctl.cpp`, `src/redis/server.cpp`, `include/lavik/meta/`, `src/meta/` |
| Request serving has distinct RESP-version, session, command, scripting, Function-catalog, Pub/Sub, and observability boundaries | `include/lavik/resp.h`, `include/lavik/resp_version.h`, `include/lavik/session.h`, `include/lavik/command.h`, `include/lavik/pubsub.h`, `include/lavik/slowlog.h`, `src/redis/` |
| The Function catalog is a durable module with one complete-catalog commit boundary | `src/redis/function_catalog.h`, `src/redis/function_catalog.cpp`, `src/storage/engine/system_state.cpp` |
| Transaction coordination has its own interfaces and implementation lifecycle | `include/lavik/tx/`, `src/tx/` |
| Storage exposes a durable engine boundary with focused implementation units | `include/lavik/storage/`, `src/storage/` |
| Shutdown checkpoints are optional one-shot recovery accelerators published through fixed metadata | `src/storage/engine/checkpoint.cpp`, `src/storage/engine/flush.cpp`, `src/storage/engine/recovery.cpp` |
| Replication has manager, boot-scoped single-group coordination, callable cluster rebuild/failover/follow-owner adapters behind fail-closed admission, Sentinel-compatible configuration, and storage-log integration boundaries | `include/lavik/replication.h`, `include/lavik/replication_group.h`, `src/config.cpp`, `src/replication/`, `src/storage/engine/replication_log.cpp`, `tests/replication_group_test.cpp`, `tests/cluster/population_integration_test.cpp`, `tests/cluster/replication_manager_integration_test.cpp`, `tests/cluster/rebuild_protocol_integration_test.cpp` |
| Cluster data plane has topology, finite authority, node-controller, Meta-client/session, failover observation, and Redis gate boundaries | `include/lavik/cluster/`, `src/cluster/`, `src/redis/cluster_command.cpp`, `src/redis/command.cpp`, `src/redis/server.cpp` |
| Meta control plane separates deterministic committed state with a topology-owned single-Data-cluster lifecycle and per-Group failover transitions, pure node projection, volatile observations, leader-scoped publishing/reconciliation, authenticated administration, atomic initial creation, stable cluster status, manifest-bootstrapped initial membership, and the C++/Go Raft boundary | `include/lavik/meta/`, `src/meta/`, `app/lavik_meta.cpp`, `app/lavik_ctl.cpp` |
| Bycorf is a pinned runtime submodule | `.gitmodules`, `CMakeLists.txt`, `bycorf/include/bycorf/`, `bycorf/src/` |
