# Architecture index

This is the only documentation directory maintained as a current explanation
of Keylane's code. It describes the system as implemented; source code and tests
remain authoritative when they disagree. Files under `docs/design-docs/` are
historical references rather than current architecture.

## Subsystem map

| Subsystem | Responsibility | Architecture document |
|---|---|---|
| System composition | Process lifecycle, runtime dependency, cross-cutting flows, and integrations | [System overview](01-overview.md) |
| Request and Redis serving | TCP/TLS sessions, RESP2/RESP3 negotiation, command dispatch, connection state, Lua/Functions, Pub/Sub, replies, and command handlers | [Request and Redis serving](02-request-serving.md) |
| Transaction coordination | Per-worker intent arbitration, cross-worker scheduling, multi-hop execution, WATCH, and transaction completion | [Transaction coordination](03-transaction-coordination.md) |
| Storage and recovery | Logical indexes, append/read paths, durable format, devices, recovery, flushing, expiry, and reclamation | [Storage and recovery](04-storage-and-recovery.md) |
| Replication | Native Keylane replication, Redis PSYNC interoperability, full sync, online logs, Sentinel-managed role changes, and replay | [Replication](05-replication.md) |

Metrics, memory accounting, logging, configuration, and the Celer runtime cross
several subsystems and are summarized in the system overview rather than
treated as independent durable modules.

## Maintenance rule

Update the relevant focused document and its source map in the same change as a
module boundary, core flow, lifecycle, durable format, or external integration.
Add, split, merge, or remove focused documents when the durable module map
changes. Keep proposals and design history outside this directory; they do not
substitute for a current architecture update.

## Source map

| Claim | Repository source |
|---|---|
| The process composes one executable and one main Keylane library around Celer | `CMakeLists.txt`, `app/keylane.cpp`, `src/redis/server.cpp` |
| Request serving has distinct RESP-version, session, command, scripting, Pub/Sub, and observability boundaries | `include/keylane/resp.h`, `include/keylane/resp_version.h`, `include/keylane/session.h`, `include/keylane/command.h`, `include/keylane/pubsub.h`, `include/keylane/slowlog.h`, `src/redis/` |
| Transaction coordination has its own interfaces and implementation lifecycle | `include/keylane/tx/`, `src/tx/` |
| Storage exposes a durable engine boundary with focused implementation units | `include/keylane/storage/`, `src/storage/` |
| Replication has manager, Sentinel-compatible configuration, and storage-log integration boundaries | `include/keylane/replication.h`, `src/config.cpp`, `src/replication/`, `src/storage/engine/replication_log.cpp` |
| Celer is a pinned runtime submodule | `.gitmodules`, `CMakeLists.txt`, `celer/include/celer/`, `celer/src/` |
