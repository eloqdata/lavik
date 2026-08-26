# Request and Redis serving

## Responsibility and boundary

This subsystem owns the Redis-facing connection lifecycle: incremental RESP2
parsing, per-connection state, authentication and replication handoff,
command classification, admission and dispatch, Redis command handlers, and
reply encoding or streaming. Celer owns sockets and worker scheduling below the
boundary. Transaction coordination, durable records, and replication sessions
remain separate modules reached through explicit interfaces.

The implementation is split across shared protocol/session interfaces and
focused command families under `src/redis/`. `src/redis/command.cpp` is the
integration point for command admission, cross-worker routing, transactions,
storage calls, role control, and trusted replay.

## Connection lifecycle

`RedisService` is a Celer `TcpService`. Each accepted connection gets one
`ConnectionContext` in its serving coroutine. That context retains the selected
logical database, authentication and cluster-read state, reusable reply buffer,
`MULTI` queue, WATCH registrations, client identity, and MONITOR subscription.
All disconnect paths return through one cleanup point which unregisters client
metadata, monitor state, and WATCH registrations.

The service recognizes authentication and replication handshakes before
ordinary dispatch. An isolated Redis `PSYNC` connection is transferred to the
Redis exporter; an isolated native Keylane handshake is transferred to the
replication manager. Other authenticated traffic enters the request-drain gate
used by graceful shutdown.

## Request pipeline

1. `ReadCommandBatch` reads into a connection-local buffer and feeds
   `RespCommandParser`. Up to 128 parsed commands are retained in wire order.
2. `BuildCommandRequest` resolves case-insensitive static metadata from the
   command table and copies the connection's current database ID into the
   request.
3. `DispatchCommand` handles connection-level transaction and client state,
   then records command metrics around the dispatch result.
4. `ExecuteCommand` and `ExecuteAdmittedCommand` reserve replication publisher
   capacity for source writes before database/key work. Eligible single-key
   writes are moved directly to their owner so admission and mutation share the
   owner-local fast path.
5. `ExecuteCommandBody` enforces replica write policy and memory admission,
   manages database and replication gates, then calls the relevant local,
   storage, transaction, blocking, RDB, or administrative handler.
6. The service writes a normal encoded reply, a direct storage-backed value, or
   bounded chunks. Small pipeline replies are coalesced up to 64 KiB.

## Command metadata and routing

`CommandSpec` is the central static classification for supported commands. It
records arity, key positions, write/read behavior, whether a command can span
workers, whether it fans out globally, whether it participates in a database
gate, whether key locations are argument-dependent, and whether it may block.
`DetermineKeys` validates static and movable key ranges before transaction or
owner routing.

The selected database is request data, never ambient worker state. Storage
chooses an owner from the key's Redis hash-slot partition. Commands with one
owner can execute locally or through one Celer cross-worker submission.
Commands needing atomic access to several keys build a `tx::Transaction` and
execute one or more shard callbacks. Global commands explicitly collect from or
coordinate all workers.

Blocking List, Sorted Set, and Stream commands release database admission while
waiting and reacquire it for each concrete attempt. Their waiter registry and
readiness events are implemented in the Redis subsystem, while storage remains
the source of truth checked after wakeup.

## Session and transaction behavior

- Requests from one connection are dispatched sequentially, so `SELECT` and
  pipelined commands observe wire order.
- `MULTI` queues structurally validated `CommandRequest` objects with their
  database IDs; argument-dependent validation can remain deferred to `EXEC`.
  `EXEC` builds a union lock set and runs queued commands through the
  transaction module while preserving response order.
- WATCH state is registered on key owners and is released after `UNWATCH`,
  `DISCARD`, an `EXEC` outcome that consumes the queued transaction, or
  connection close.
- Commands applied from a replication stream carry `replication_origin_`.
  They bypass client role checks where appropriate and cannot publish another
  replication event.
- The reply builder is connection-owned and valid only until the current
  socket write. Direct disk replies keep their read lease; unbounded replies
  use a chunk source. A 30-second no-progress watchdog closes a connection that
  stalls while a streamed reply holds a database gate.

## Failure and backpressure behavior

Parse errors are returned to the connection and end that malformed session.
Command errors are encoded as Redis errors without stopping unrelated
connections. Storage and transaction failures are converted at the command
boundary; trusted replication replay instead treats command errors as apply
failures so a flow cannot acknowledge partial work.

Replication publisher admission occurs before database gates and key locks so
a slow replica cannot suspend a write while holding state required by
`FLUSHDB` or a full-sync cut. Memory-growing commands use the sampled memory
guard before execution. Graceful shutdown closes admission, drains active
requests, and only then asks storage for its final durable flush.

## Verification

Parser and reply encoding are unit-tested independently. The command-table
tests cover kind lookup, flags, arity, and movable key extraction. E2E binaries
exercise multi-key atomicity, `MULTI`/`EXEC`/WATCH, TTL, collections, metrics,
RDB import/export/backup, replication logs, and Redis PSYNC behavior through the
real server executable.

## Source map

| Claim | Repository source |
|---|---|
| Celer service integration, connection setup/cleanup, parsing loop, batching, reply paths, and handshake transfer | `src/redis/server.cpp` |
| Per-connection database, authentication, MULTI, WATCH, and monitor state | `include/keylane/session.h` |
| Incremental RESP parser and reusable reply builder | `include/keylane/resp.h`, `src/redis/resp.cpp` |
| Command request/reply contracts, dispatch, replay, and gate interfaces | `include/keylane/command.h` |
| Static command classification and key extraction | `include/keylane/command_table.h`, `src/redis/command_table.cpp` |
| Admission, role checks, database/replication gates, transaction integration, routing, and replay | `src/redis/command.cpp` |
| Type-family command handlers | `src/redis/string_command.cpp`, `src/redis/list_command.cpp`, `src/redis/hash_command.cpp`, `src/redis/set_command.cpp`, `src/redis/zset_command.cpp`, `src/redis/stream_command.cpp`, `src/redis/sort_command.cpp` |
| Blocking waiter ownership and wakeups | `src/redis/blocking_wait.h`, `src/redis/blocking_wait.cpp` |
| Redis RDB import/export and backup commands | `include/keylane/rdb.h`, `src/redis/rdb.cpp`, `src/redis/backup.h`, `src/redis/backup.cpp` |
| Parser, metadata, configuration, and end-to-end command coverage | `tests/resp_test.cpp`, `tests/command_table_test.cpp`, `tests/config_test.cpp`, `tests/multikey_e2e_test.cpp`, `tests/multi_exec_e2e_test.cpp`, `tests/list_e2e_test.cpp` |
