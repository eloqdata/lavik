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

# Request and Redis serving

## Responsibility and boundary

This subsystem owns the Redis-facing connection lifecycle: incremental command
parsing, connection-level RESP2/RESP3 reply negotiation, per-connection state,
authentication and replication handoff, command classification, admission and
dispatch, Lua and Pub/Sub execution, Redis command handlers, and reply encoding
or streaming. Bycorf owns sockets and worker scheduling below the boundary.
Transaction coordination, durable records, and replication sessions remain
separate modules reached through explicit interfaces.

The RESP parser/reply builder and default-user password verifier form a small
shared library also used by Meta's Sentinel server. That server owns separate
sessions and its own command allowlist; sharing these primitives does not
share credentials or expose Data dispatch. Data AUTH, HELLO, and RESET remain
local connection operations, including in standalone deployments without Meta.

The implementation is split across shared protocol/session interfaces and
focused command families under `src/redis/`. `src/redis/command.cpp` is the
integration point for command admission, cross-worker routing, transactions,
storage calls, role control, and trusted replay.

## Connection lifecycle

`RedisService` is a Bycorf `TcpService`. Each accepted connection gets one
`ConnectionContext` in its serving coroutine. That context retains the selected
logical database, authentication and cluster-read state, negotiated
`RespVersion` in its reusable reply builder, `MULTI` queue, WATCH registrations,
socket and peer identity, client name, MONITOR subscription, the native
replication watermark used by `WAIT`, and optional `PubSubSession`. A
worker-local client record separately tracks data exposed by
`CLIENT`, including RESP version, client library name/version, subscription
counts, blocking state, and replication-session identity. All disconnect paths
return through one cleanup point which unregisters client, monitor, Pub/Sub, and
WATCH state.

`RedisService` applies one process-wide `maxclients` limit across its plaintext
and TLS endpoints before registering an accepted socket or starting TLS. The
limit and active count belong to the Redis protocol service; other Bycorf TCP
services such as the metrics HTTP endpoint do not participate. A
plaintext connection rejected at the limit receives Redis's max-clients error;
a TLS connection is closed without plaintext output or handshake work. Pending
TLS and replication handshakes consume slots because their eventual role is not
known before protocol negotiation. Every session owns its slot through TLS
setup and serving, so handshake failures and all disconnect paths release it.
`CONFIG SET maxclients` changes admission immediately; lowering it below the
active count leaves existing connections intact and rejects new ones until the
count falls below the new limit. Lavik keeps 256 file descriptors outside the
client budget. Startup attempts to raise `RLIMIT_NOFILE` and reduces the
effective initial limit when the process hard limit is insufficient; a runtime
increase that cannot preserve the reserve fails without changing the live
limit.

Connections start in RESP2. `HELLO 2` or `HELLO 3` can combine protocol
selection with `AUTH` and `SETNAME`; validation finishes before authentication,
name, or reply-version state is changed. The selected version follows every
subsequent `CommandRequest` and `ReplyBuilder`. RESP3 handlers can therefore
emit native nulls, booleans, doubles, maps, sets, and push frames, while the same
logical replies retain their RESP2-compatible encodings on RESP2 connections.
An `EXEC` retains its entry version for the outer aggregate header; a queued
`HELLO` changes later child replies and the connection version that remains
after `EXEC`.
`RESET` returns the connection to RESP2 and clears its selected database,
authentication, cluster-read, transaction, WATCH, native replication
watermark, and name state.

The service recognizes authentication and replication handshakes before
ordinary dispatch. An isolated native Lavik handshake is transferred to the
replication manager. Other authenticated traffic enters the request-drain gate
used by graceful shutdown. That gate stores a closed bit and active count in
one cache-line-isolated shard per worker. Normal traffic therefore mutates only
worker-local state; shutdown closes every shard before it waits, so an entry
racing closure is either rejected or remains visible to the drain.

## Request pipeline

1. `ReadCommandBatch` reads into a connection-local buffer and feeds
   `RespCommandParser`. Up to 128 parsed commands are retained in wire order.
   Ordinary connections collectively hold at most `maxmemory-clients`, which
   accepts an absolute byte size or a percentage of effective maxmemory and
   defaults to 5%. The effective limit is divided into fixed worker shares.
   This abuse-control quota is independent of retained-memory admission; the
   server does not reserve temporary request bytes against `maxmemory` a second
   time. Admission runs once after each socket read;
   parsed wire bytes follow their command until execution finishes, while blank
   input is retired as soon as the parser returns to its idle state. The
   offending connection receives an error and closes when its worker's share is
   exhausted. Zero disables this client-specific limit. As in Valkey, every
   nonzero process-wide client allowance has a 128 KiB
   floor so an intentionally tiny or already-exhausted maxmemory still permits
   administrative and shrinking commands. Commands queued by `MULTI` retain
   their charge until `EXEC`, `DISCARD`, `RESET`, or connection teardown because
   their argument storage remains connection-owned after the `QUEUED` reply.
   Independently, `client-query-buffer-limit` bounds the wire bytes retained
   while one connection incrementally assembles a command. It defaults to 1
   GiB and, like Redis, accepts an absolute value from 1 MiB through
   `LONG_MAX`. Lavik consumes the small socket input window directly into
   argument strings, so this is a parser-retained-byte limit rather than a
   requirement for a second contiguous query buffer. Completed pipelined and
   `MULTI` commands remain governed by `maxmemory-clients` after parser
   ownership ends. `CONFIG SET client-query-buffer-limit` updates a process-wide
   atomic. Existing connections refresh it once per socket-read parsing round,
   avoiding an atomic load for each RESP token while ensuring a partially read
   command is checked against the new limit when more bytes arrive.
2. `BuildCommandRequest` resolves case-insensitive static metadata from the
   command table and copies the connection's current database ID into the
   request.
3. `DispatchCommand` handles connection-level transaction and client state,
   attaches the connection's reply version, then records command metrics and
   eligible slow-log entries around the dispatch result.
4. `DispatchCommandImpl` runs the readiness and ownership admission gates.
   The replication LOADING gate rejects non-whitelisted commands while a
   standalone replica syncs or a cluster node rebuilds its Meta-authorized
   population. When cluster mode is enabled, the cluster admission gate runs
   next and independently decides from the committed `cluster::ServingState`
   whether to serve locally, serve a stale replica read, redirect with MOVED,
   or refuse with CROSSSLOT, CLUSTERDOWN, LOADING, or a controlled-failover
   `TRYAGAIN`. Cluster mode disables the
   standalone replica-MOVED shim: a population source transfers data but never
   supplies client-routing authority. The cluster gate also runs at `MULTI`
   queue time so a rejected command aborts the queued transaction. Managed
   Single uses the same Group authority without redirects. Writes and Single
   multi-shard reads retain their admission snapshot and key slots for execution
   checks; synchronous reads borrow the worker's snapshot.
5. `ExecuteCommand` and `ExecuteAdmittedCommand` reserve replication publisher
   capacity for source writes before database/key work. Eligible single-key
   writes are moved directly to their owner so admission and mutation share the
   owner-local fast path.
6. `ExecuteCommandBody` enforces replica write policy and memory admission,
   manages database and replication gates, then calls the relevant local,
   storage, transaction, blocking, RDB, or administrative handler. Single reads
   recheck Group authority after DB admission and after a worker hop. In either
   managed mode, admitted writes re-check their authority against the current
   `ServingState` after these outer admissions and before the handler runs;
   transactional writes also re-check per shard through a validator hook on
   `tx::Transaction`. Because a handler can still suspend on key/store locks,
   reads, or block allocation, every logical keyspace write carries a
   storage-neutral `MutationPrecondition`. `StorageEngine` evaluates it after
   that preparation and immediately before invalidating WATCH or changing the
   staging buffer/index; rollback, background maintenance, and replica replay
   do not depend on client authority.
7. The service writes a normal encoded reply, a direct storage-backed value, or
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
owner can execute locally or through one Bycorf cross-worker submission.
Commands needing atomic access to several keys build a `tx::Transaction` and
execute one or more shard callbacks. Global commands explicitly collect from or
coordinate all workers.

`LAVIK.HREPLACE key field value [field value ...]` is a Lavik-only RESP
extension: it atomically replaces the entire field set of an existing Hash,
preserves the absolute key expiry, and returns `OK`. Missing/expired keys return
RESP null without a write; another live type returns `WRONGTYPE`. At least one
field/value pair is required, duplicate fields use the last supplied value,
and field/value limits match HSET. It participates in ordinary key routing,
memory admission, WATCH, EXEC/Lua and replication. HSET/HMSET retain their
standard merge semantics. Redis SDKs can invoke the extension through a raw
command API; ordinary Redis does not recognize the command.

When cluster mode is enabled, the same hash slot is also a node-level routing
decision made before any worker routing: every key of a request must hash to
one slot (hashtags keep multi-key commands usable), that slot must belong to a
granted local group, and anything else is redirected to the owning node or
refused. Multi-key commands, `MULTI`/`EXEC` unions, and Lua declared keys all
obey the single-slot constraint. The worker mapping inside the owning node is
unchanged. See [Cluster data plane](06-cluster-data-plane.md).

MGET acquires all requested keys in one shared-lock transaction and preserves
their argument positions in the assembled reply. Each participating shard
passes its local keys to `StorageEngine::BatchGetLocked`. Ordinary-size compact
on-disk values are submitted together in waves paced by that worker's available
fixed read buffers; oversized reads use aligned overflow leases. One awaiter
resumes only after every submitted read in the wave completes. The wave
releases all leases before relocation retries or the next wave, while external,
in-memory, remote-owner, or stale-location cases use the complete single-key
fallback path.

Top-level blocking List and Sorted Set writes and `XREADGROUP` release database
admission while waiting and reacquire it for each concrete attempt. In cluster
mode they also re-admit and register a fresh authority in-flight guard for that
attempt, then release the guard before waiter registration or sleep. Immediate
EXEC and Lua forms do not wait and stay within their enclosing authority
window. Read-only `XREAD` and keyless `WAIT` register no mutation guard. The
waiter registry and readiness events are implemented in the Redis subsystem,
while storage remains the source of truth checked after wakeup. This
attempt-scoped ownership lets fencing drain promptly even when a client waits
without a timeout.

External data commands capture the replication manager's packed
serving-generation/open token at dispatch and revalidate it after obtaining
database admission. This includes ordinary shared-gate commands and commands
that establish their own exclusive cut: FLUSH, KEYS, and SAVE/BGSAVE validate
after closing and draining their target database gates, before mutation, reply
commitment, or snapshot capture. Blocking List and Sorted Set loops and the
Stream loop repeat the check on every wake. A role transition away from a
serving population first closes and advances the generation, then broadcasts a
wake to every worker-local blocking registry. A request admitted against the
old population therefore exits with LOADING or TRYAGAIN instead of examining
the replacement population. WATCH holds shared database admission across its
cross-worker registrations and liveness reads, so reset either waits and then
dirties those registrations or wins first and makes WATCH reject its stale
generation before registering. Each registration also retains that generation;
EXEC treats a later generation mismatch as a watched-key modification even if
the replacement has identical key liveness. Once KEYS has validated and
committed its streamed reply, a later replacement waits for its exclusive
database gate rather than disconnecting it mid-reply. Opening a completed
population publishes the role before the open bit. Trusted replication-origin
commands bypass this client fence because they are the work that constructs the
closed population.

Worker zero owns RDB backup scheduling. `SAVE`, `BGSAVE`, and `LASTSAVE`
requests arriving on other workers submit their control step there, while an
active backup fans snapshot capture and scanning back out to every data worker.
`BGSAVE SCHEDULE` retains at most one immutable follow-up request context and
starts it after the active job retires. Configured `save <seconds> <changes>`
policies are alternatives whose time and change thresholds must both hold. If
at least one policy exists, a one-second worker-zero coroutine evaluates them
using low-frequency collection of the worker-local change counters; no timer
coroutine exists when automatic saves are disabled. Successful snapshots
advance each worker's saved cut, so mutations after the snapshot cut remain
eligible for a later automatic save. Shutdown disables new policy-driven saves
and drains an active or explicitly scheduled job before storage flush.

## Session and transaction behavior

- Requests from one connection are dispatched sequentially, so `SELECT` and
  pipelined commands observe wire order.
- Successful source commands that may publish a native event invalidate the
  connection's cached replication watermark. `WAIT` lazily fences every
  worker publisher and then counts a native replica only when all of its flow
  ACK cursors cross that history-local vector. Repeated waits without another
  write reuse the cut. A blocking `WAIT` uses the keyless client-wait registry
  so deadlines, disconnect cancellation, and `CLIENT UNBLOCK` share the same
  lifecycle as collection waits. Inside `EXEC`, `WAIT` performs only an
  immediate check; blocking on an envelope that cannot publish until the
  transaction commits would deadlock the transaction with itself.
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
  Negative-count random replies retain a charged, immutable command-time
  snapshot (including inside `EXEC`), without retaining DB/key locks during
  socket backpressure. Their encoder limits each output chunk to 1 MiB and
  preserves a bulk-string cursor across chunks; disconnecting destroys the
  snapshot and its retained charges.

## Lua scripts and Functions

Each Bycorf worker lazily owns one persistent `LuaWorkerRuntime`. It retains the
Lua VM, compiled script closures, and locally installed Function libraries;
source bodies and canonical metadata have process-wide ownership. `SCRIPT
LOAD`/`FLUSH` update every worker's script index. Script-cache mutations are
serialized with each other and finish their worker fan-out before replying,
but concurrent `SCRIPT EXISTS` or `EVALSHA` lookups do not join that mutation
guard and can temporarily observe the per-worker transition.

`FunctionCatalog` owns the deeper Function lifecycle. A mutation constructs a
complete hidden runtime on every worker, verifies identical metadata, commits
the canonical `FUNCTION DUMP` to storage, and only then swaps the runtime and
process-global maps. Function invocations, reads, mutations, RDB installation,
and promotion capture share its guard. Startup restores and validates the
durable catalog before Redis readiness when one exists; a fresh set starts
with the canonical empty catalog without allocating a durable root. Full
ownership, persistence, replay, and failure behavior are described in the
[Function catalog](07-function-catalog.md) document.

`EVAL`, `EVALSHA`, their `_RO` variants, `FCALL`, and `FCALL_RO` derive their
key set from `numkeys`. The command layer acquires those declared keys through
one transaction before starting Lua. The `_RO` command variants use shared
holds; ordinary `EVAL`, `EVALSHA`, and `FCALL` use exclusive holds regardless
of the Function's runtime flags. Calls made through `redis.call` or
`redis.pcall` re-enter command metadata and keyed execution inside those
retained holds. Undeclared keys and global, administrative, nested scripting,
or otherwise unsafe commands are rejected. Blocking list and sorted-set pops
and moves reuse the transaction's locked EXEC helpers for one immediate
attempt; they never register a waiter. `XREAD` and `XREADGROUP` similarly use
one locked read when `BLOCK` is absent and reject an explicit `BLOCK` option.
`WAIT` validates its ordinary arguments but never registers a waiter: it
checks the connection's pre-script replication watermark immediately, and
returns zero after any write in the current invocation because that write's
replication envelope cannot publish until Lua returns and commits.
For the same deterministic-replication reason, `SORT` over a Set with a
constant `BY` pattern forces alphabetic ordering in scripts. Pattern-derived
`BY`/`GET` keys remain rejected because they are absent from the declared-key
transaction and cannot be locked after it has started.
Write effects, blocking notifications, durable transaction receipts, and
replication effects remain attached to the outer invocation rather than
becoming independent commands.

Cached closures share VM globals, so Lua invocations are serialized per worker
even when a script yields to execute a command on another owner. The persistent
global table and its lookup metatable are recursively read-only; reads of
absent globals fail instead of returning nil, matching the Redis sandbox and
preventing scripts from probing disabled libraries. Ordinary commands do not
take this worker-local Lua gate. Once an invocation exceeds
`lua-time-limit`, however, a process-wide busy flag makes ordinary non-replay
commands return `BUSY`; the matching `SCRIPT KILL` or `FUNCTION KILL` and
`FUNCTION STATS` bypass the ordinary execution gates and remain admissible.
Lua 5.1 cannot yield across a protected-call C boundary, however, so a script
that repeatedly catches its instruction-hook error can monopolize its worker.
A kill connection accepted by that same worker cannot be read until the
invocation yields; callers must retry through another connection while this
worker-affinity limitation remains. An invocation that has written or came
from replication cannot be killed in a way that would expose partial effects.

## Pub/Sub mode

Channel and pattern indexes are worker-local. `PUBLISH` fans out to every
worker registry and counts live matching subscriptions after membership is
rechecked on the destination worker. Encoded RESP2 and RESP3 message bodies are
shared between recipients where possible; each session receives frames in its
negotiated version. A source-side `EXEC` that must await replication captures
the matching sessions, protocol encodings, and receiver count at the
`PUBLISH` command's position. It makes those captured frames visible only after
replication publication succeeds, so later subscription changes in the same
transaction cannot reorder delivery.

In cluster mode `PUBLISH` derives the channel's hash slot and is a runtime-only
mutation even though it writes no durable key. It therefore participates in
authority admission, in-flight draining, final publication recheck, and EXEC's
mutation union. During a controlled failover it returns `TRYAGAIN Failover in
progress`; a publication that already crossed admission either completes under
the drained old-owner generation or fails its final authority check before
replication publication and local delivery. Subscription delivery itself does
not hold mutation authority.

After the first subscription, the connection enters a two-coroutine serving
mode. A dedicated reader continues parsing allowed commands and enqueues their
replies, while the original serving coroutine is the only socket writer and
drains both command replies and published messages. On exit or failure the
writer closes the session and joins the reader before connection cleanup.
RESP2 subscribed clients are restricted to subscription management, `PING`,
`QUIT`, and `RESET`; RESP3 subscribed clients can continue issuing ordinary
commands while push messages are interleaved by the single writer.

Output is bounded rather than silently dropped: a session queue holds at most
10,000 data frames, and all sessions on one worker share a 128 MiB pending-byte
budget. Exceeding either limit closes the slow subscriber and shuts down its
socket, preventing Pub/Sub delivery from consuming unbounded worker memory.

## Administrative compatibility and observability

SLOWLOG uses one bounded, single-owner ring per worker. Command completion
records eligible non-blocking commands after dispatch, redacts credentials,
and bounds copied arguments; `SLOWLOG GET` merges worker snapshots by a global
entry ID. Its threshold and capacity are runtime-configurable. `CONFIG
RESETSTAT` visits every worker and clears command counters only; it does not
alter data or persistence dirty state. The successful `CONFIG` command is
recorded after the reset traversal returns, but the traversal does not globally
quiesce requests, so concurrent commands can also contribute post-reset
samples.

The external replication command surface accepts only Redis/Redis Cluster
upstreams. `REPLICAOF` and `SLAVEOF host port` complete AUTH/PSYNC before changing
roles or retiring an existing subscription; `ADDREPLICAOF` additionally checks
matching master slot layouts and disjoint slots; callers select the cluster. All Meta-managed nodes reject these
commands, including `NO ONE`, independently of client mode. Lavik native
relationships are established by Meta Follow Owner.

The retained management-only `MULTI`/`EXEC` handling preserves command order and
individual replies; it never bypasses these management checks. `CONFIG REWRITE`
persists the current single-upstream configuration and priority while preserving
startup Meta seeds and identity settings. External Sentinel control of Lavik HA is
outside the supported deployment contract.

`CONFIG GET/SET maxclients` exposes the live connection limit, while `INFO clients`
reports it alongside the active client gauges. The startup
`maxclients` directive and command-line option establish the initial value.

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
requests, and only then asks storage for its final durable flush. After Bycorf
has torn down a worker's I/O and coroutine frames, its native-thread service
finalizer releases that worker's remaining `StorageEngine` state; worker-owned
indexes are never destroyed from the shutdown thread.

A locally admitted write also captures the replication role epoch before
routing. After it acquires ordinary, blocking, multi-database, EXEC, or FLUSH
database admission, it revalidates that epoch before mutation. A write delayed
across demotion and promotion returns `TRYAGAIN`; it cannot enter the new child
history using the publication decision from the old role.

## Verification

Parser and version-aware reply encoding are unit-tested independently. The
command-table tests cover kind lookup, flags, arity, and movable key extraction.
E2E binaries exercise HELLO/RESP3 and Pub/Sub through the Pub/Sub suite, Lua
through the transaction suite, bounded batched MGET through the multi-key
suite, SLOWLOG and `RESETSTAT` through the metrics suite, Sentinel failover,
multi-key atomicity, `MULTI`/`EXEC`/WATCH, TTL, collections, RDB
import/export/backup, replication logs, and Redis PSYNC behavior through the
real server executable.

## Source map

| Claim | Repository source |
|---|---|
| Bycorf service integration, pre-TLS connection admission, connection setup/cleanup, parsing loop, batching, reply paths, and handshake transfer | `bycorf/include/bycorf/net/tcp_service.h`, `bycorf/src/net/tcp_service.cpp`, `src/redis/server.cpp` |
| Per-connection database, authentication, reply version, MULTI, WATCH, monitor, Pub/Sub, and client identity state | `include/lavik/session.h`, `include/lavik/resp_version.h` |
| Incremental RESP parser and version-aware reusable reply builder | `include/lavik/resp.h`, `src/redis/resp.cpp` |
| Command request/reply contracts, dispatch, replay, and gate interfaces | `include/lavik/command.h` |
| Static command classification and key extraction | `include/lavik/command_table.h`, `src/redis/command_table.cpp` |
| Admission, controlled-failover pause, role checks, database/replication gates, transaction integration, PUBLISH fencing, routing, and replay | `src/redis/command.cpp`, `src/redis/blocking_wait.cpp` |
| Cluster admission gate, CLUSTER subcommands, and discovery replies | `include/lavik/cluster/`, `src/cluster/`, `src/redis/cluster_command.cpp` |
| Final logical-mutation precondition and WATCH/publication seam | `include/lavik/storage/engine.h`, `src/storage/engine/write.cpp`, `src/storage/engine/hash_tree.cpp` |
| Type-family command handlers | `src/redis/string_command.cpp`, `src/redis/list_command.cpp`, `src/redis/hash_command.cpp`, `src/redis/set_command.cpp`, `src/redis/zset_command.cpp`, `src/redis/stream_command.cpp`, `src/redis/sort_command.cpp` |
| Blocking waiter ownership and wakeups | `src/redis/blocking_wait.h`, `src/redis/blocking_wait.cpp` |
| Worker-local Lua VM, script cache, Function runtime staging, invocation state, and command re-entry | `src/redis/lua_eval.h`, `src/redis/lua_eval.cpp`, `src/redis/command.cpp` |
| Durable process-global Function catalog lifecycle | `src/redis/function_catalog.h`, `src/redis/function_catalog.cpp`, `src/storage/engine/system_state.cpp` |
| Pub/Sub session queues, worker-local registries, fan-out, and subscribed connection serving | `include/lavik/pubsub.h`, `src/redis/pubsub.cpp`, `src/redis/server.cpp` |
| SLOWLOG shards, command-stat reset, and client/Sentinel administration | `include/lavik/slowlog.h`, `src/redis/slowlog.cpp`, `include/lavik/metrics.h`, `src/metrics.cpp`, `src/redis/command.cpp` |
| Redis RDB import/export and backup commands | `include/lavik/rdb.h`, `src/redis/rdb.cpp`, `include/lavik/rdb_collection.h`, `src/redis/rdb_collection.cpp`, `src/redis/backup.h`, `src/redis/backup.cpp` |
| Parser, metadata, configuration, max-client admission, and end-to-end command coverage | `tests/resp_test.cpp`, `tests/command_table_test.cpp`, `tests/config_test.cpp`, `tests/multikey_e2e_test.cpp`, `tests/multi_exec_e2e_test.cpp`, `tests/pubsub_e2e_test.cpp`, `tests/metrics_e2e_test.cpp`, `tests/redis_follower_smoke.py`, `tests/list_e2e_test.cpp` |
