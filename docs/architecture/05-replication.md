# Replication

## Responsibility and boundary

The replication subsystem owns node role, upstream and downstream session
lifetime, native Keylane synchronization, Redis PSYNC interoperability, and
trusted replay. `ReplicationManager` coordinates connections and role changes;
the Redis command layer captures committed logical effects; and the storage
engine owns the source backlog, full-sync capture state, target rebuild state,
and durable epoch changes.

Replication moves deterministic logical commands, snapshot records, and the
process-global Redis Function catalog, not physical block addresses or record
offsets. The logical stream distinguishes durable keyspace mutations,
cross-flow transactions and control barriers from runtime-only effects such as
`PUBLISH`. Celer owns sockets, TLS, worker scheduling, and cross-core
submissions below this boundary. Normal storage and transaction paths remain
authoritative for mutation ordering and durability. Replication-origin
commands re-enter those paths with client role checks bypassed and publication
disabled.

The source backlog is deliberately not durable. It is process-local memory
used for connected downstreams and bounded reconnects. Target records and the
epochs that fence an in-progress rebuild are durable, but native flow cursors
and Redis replid/offset state are also process-local. A process restart
therefore starts a new native history and requires a fresh Redis RDB per source.

## Roles and lifecycle

The externally reported roles are `master`, `connecting`, `syncing`, and
`online`. `connecting` and `syncing` are LOADING states: ordinary data commands
cannot observe a partial rebuild. A configured replica does not perform
authoritative active expiration; it applies the source's absolute deadlines
and replicated deletion effects instead.

`REPLICAOF host port` replaces the desired upstream under closed and drained
command database gates, disables local expiration authority, cancels the old
flows, and reconnects asynchronously. Ordinary `REPLICAOF` probes the native
protocol first and falls back to Redis discovery when the peer does not support
it. Explicit `redis-replicaof` configuration skips that probe.

`REPLICAOF NO ONE` cancels and drains the active flows before reopening command
admission. After a simple transport loss, it can retain and promote a completed
native or standalone-Redis dataset while the old root remains intact and marked
valid. An explicit source switch clears that validity; issuing `REPLICAOF NO
ONE` before the replacement reaches its cut then discards the partial population
and publishes an empty writable master. A same-source native reconnect that
misses the backlog is a different edge case: destructive full-sync reset can
detach the old indexes without clearing the validity flag, and cancelling that
partial rebuild can consequently publish an empty master despite the stale
flag. Multi-source Redis Cluster promotion has the opposite edge case: any one
valid source currently prevents discard, even while the topology is incomplete
or another source is still syncing, so promotion can expose a partial slot
population. Native reconnects return to LOADING while rebuilding. A standalone
Redis follower whose process-local dataset remains valid can stay online and
serve its local read-only copy during a transient source disconnect.

Redis Sentinel drives these same transitions through Redis-compatible `ROLE`,
`INFO replication`, `REPLICAOF`/`SLAVEOF`, `CONFIG REWRITE`, and `CLIENT KILL`
behavior. `replica-priority` defaults to 100, is runtime mutable, and is
reported as `slave_priority`; Sentinel treats zero as ineligible and otherwise
prefers the lower value. Sentinel-specific transaction handling and config
persistence are described under [Redis interoperability](#sentinel-managed-failover).

## Native control and data flow

Native replication uses authenticated, isolated RESP commands on the ordinary
Redis listener. Worker 0 owns the `KLPSYNC` control connection. The source has
one `KLFLOW` data connection per source worker and adopts each flow socket onto
that worker. A target may have a different worker count; it assigns source flow
`n` to target worker `n % target_worker_count` without changing the source flow
identity.

The control handshake exchanges a session ID, node IDs, the source history ID,
and source worker count. Each flow then presents its requested `(lsn,
fragment_index)` cursor. Mode selection waits for every flow: matching history
and a retained non-initial cursor on every flow selects `CONTINUE`. The initial
`(1, 0)` cursor, any history mismatch, or any backlog miss selects full sync
for every flow. Mixed continue/full sessions are not allowed.

The steady-state source path is:

```text
client write
  -> publisher admission before database/key gates
  -> normal storage or transaction commit
  -> canonical command, transaction envelope, control barrier, or ephemeral event
  -> worker-local publisher FIFO
  -> shared in-memory replication frames
  -> duplex KLFLOW sender and ACK receiver
  -> target reassembly queue (at most 256 complete commands)
  -> strict FIFO trusted apply
  -> target resume-cursor publication
  -> ACK
```

Canonical commands use the KRC1 version-1 encoding: explicit logical database,
argument count and lengths, then argument bytes. Large arguments stream into
the backlog without another complete flattened allocation. One logical event
has one flow-local LSN and may span several CRC-checked frames and 8 MiB memory
blocks. Mutation, transaction, and database-control events are distinct frame
kinds. `kEphemeral` events carry runtime-only effects such as standalone
`PUBLISH` and Function-catalog mutations. They participate in online and
in-progress full-sync streams, but their journal records disappear when the
process-local replication history is replaced. The Function catalog itself is
runtime state captured explicitly at full-sync and RDB boundaries.

Source publishers normally target 2 MiB and are capped at 128 frames per
batch. The first frame is admitted even when it exceeds the byte target, so a
one-frame batch can be larger than 2 MiB. Before a socket write can yield, the
sender appends every complete event in the batch to an expected-ACK FIFO. A
separate receiver coroutine validates ACKs in that order and advances the
session's retained cursor while the sender continues reading and transmitting
later batches. The sender never stops at an arbitrary batch boundary waiting
for a transaction ACK whose other participant may be in another flow's next
batch; socket backpressure bounds outstanding wire output instead. A flow with
no tail data sleeps briefly. A full-sync flow that makes no protocol progress,
or an online flow with unacknowledged work that makes no backlog ACK progress,
for ten minutes is cancelled; a progressing full sync has no overall
wall-clock limit.

## Runtime backlog and backpressure

Each source worker has one heap-backed backlog shared by all downstream native
sessions and the Redis exporter. Downstreams own only their cursors and
retention pins. `repl-backlog-size` is a global quota divided across workers in
8 MiB blocks; allocation is lazy. Per-worker LSNs start at one for a new
history and increase monotonically. The log remains active across downstream
disconnects and is cleared only when disabled, the process exits, or the
history is invalidated.

A connected native downstream pins its first unacknowledged LSN. The publisher
must wait rather than evict required history. At capacity it sleeps until ACKs
raise the retention point beyond the oldest sealed block's last LSN. Reclamation
then removes that block and any immediately following sealed blocks containing
the rest of its final spanning event, and the waiting publisher receives one
block-granular allocation opportunity immediately. There is no percentage or
quarter-window low-water hysteresis. Disconnect releases the pin immediately,
wakes writers, and leaves the remaining capacity as a circular reconnect
window. Shrinking below pinned history establishes a target quota rather than
deleting required events. A single oversized event can own multiple blocks and
temporarily exceed the ordinary quota, but eviction and trim never retain only
part of an event.

Writes reserve the worker publisher queue and all relevant active full-sync
queues before entering database or key gates. Multi-participant requests
normally reserve workers sequentially in worker-ID order. A replicated
standalone `MSET` is the intentional exception: it first acquires the global
cross-flow publication-order slot, then, when it spans more than one worker,
waits for its actual participant workers' publisher credits in parallel. Once
the transaction owns its shard locks and every participant marker has been
enqueued, an entry hook releases the order slot before storage I/O and
durability completion. A one-worker `MSET` releases the slot without a
cross-flow marker. This preserves a common flow order without serializing
unrelated storage work. Queue exhaustion therefore backpressures the client
before commit instead of silently dropping publication. Encoding or backlog
allocation failure after admission marks the history invalid: the primary
dataset remains usable, but the manager assigns a new history ID, cancels
downstream sessions, clears all worker logs, and requires full sync.

## Full-sync lifecycle

Full sync rebuilds one target population in place; there is no parallel serving
root. The source registers a bounded full-sync session queue on every worker
and captures a database epoch vector plus each partition's baseline mutation
sequence. A flow scans the source partitions assigned to it, one logical
database at a time. Each `(partition, database)` moves through `unstarted`,
`scanning`, and `tailing`:

- an unstarted database needs no capture because its later scan sees committed
  state;
- an uncovered key in a scanning database coalesces to its latest after-image
  or tombstone;
- a covered key, or any key in a tailing database, enters the ordered session
  command FIFO.

Baseline values are captured under the key-ordering boundary. Large external
values pin immutable extents and stream bounded chunks; ordinary values are
materialized into bounded record batches. Transactions committed during the
hidden rebuild publish their participant after-images only after the commit
decision. `FLUSHDB` or `FLUSHALL` invalidates an active capture attempt so the
next attempt starts from the new database epochs.

Runtime-only commands published while the key snapshot is in progress enter
the same bounded full-sync command FIFOs without creating snapshot state.
Unlike durable writes, however, direct `PUBLISH` and the ephemeral publication
phase of a `PUBLISH`-only `EXEC` are not protected by the snapshot or database
admission gates. At the native cut, a publish can land in both the full-sync
FIFO and backlog after that FIFO has drained but before the backlog fence is
taken; capture cleanup drops the FIFO copy while the post-event fence cursor
skips the backlog copy. Redis export has no full-sync FIFO: a publish during its
RDB cut but before the corresponding worker's backlog fence is absent from the
RDB and lies before the online cursor. Function libraries also live outside the
storage snapshot. At the final native cut, flow zero sends one exact,
fragmentable `FUNCTION RESTORE ... FLUSH` catalog command after draining its
command FIFO and before fencing the online backlog.

Before target records are accepted, `ResetReplicaPartitions` persists new
candidate replication epochs in batches and then detaches the old indexes.
Recovery can consequently reject both prior-epoch records and records from an
interrupted partial rebuild. Baseline, replacement, and tail records all enter
the same in-place indexes while LOADING. Partition handoff marks that its
databases have reached tailing.

At the final cut, the source closes and drains snapshot-transaction admission,
closes and drains command database gates, drains replacement and session
queues, fences each shared backlog, pins each returned stable cursor, stops
capture on every flow, and reopens foreground admission before waiting for the
network cut ACK. The target waits for every flow's cut, closes its own command
gates, validates every partition, drains staged storage writes, persists the
new database epochs, removes sync state, and only then becomes online. Abort
drains and detaches the partial population rather than exposing it.

## Online apply and rendezvous

The target disables socket read-ahead on native flow connections. Its online
receiver validates frame identity and fragment order, reassembles and decodes
complete KRC1 commands, and places at most 256 commands in an owner-local FIFO.
A separate consumer removes them in receive order and performs strict replay.
Only successful application, including any cross-flow rendezvous, publishes
the next in-memory resume cursor; the consumer then writes the ACK. If that ACK
detects a disconnect, reconnect does not repeat an already applied `APPEND`,
`INCR`, or similar effect. An ingress or apply failure cancels the whole
session and joins the consumer before the flow returns, so no detached apply
coroutine can strand another flow at a transaction barrier.

Multi-participant writes carry identical `__KEYLANE_TX_V1` envelopes on every
participant flow. The target groups arrivals by transaction ID, verifies the
participant set and body, applies the command once after every participant is
present, and advances all participant cursors before any flow ACKs. `FLUSHDB`
and `FLUSHALL` use a shared barrier ID copied to every source flow; the target
waits for all copies, installs one database epoch change, then advances the
whole cursor vector. Transaction/control rendezvous failures and command apply
failures invalidate the target's continuation state so a retry cannot continue
from an uncertain cut. Online ingress protocol, frame identity or ordering,
fragment reassembly, and KRC1 decode failures instead cancel the session
without explicitly invalidating the prior continuation state.

Source-side cross-flow transaction and control publication uses one
process-global ordering slot. Unless its concrete key set lands on a single
shard (see the dynamic admission below), a replicated standalone `MSET`
acquires it before publisher admission or database gating, admits its
participant workers in parallel, and releases it as soon as every participant
marker is queued rather than holding it across storage I/O. `EXEC` and
database-control publication use the same slot for their ordering boundary;
`FLUSH` acquires it before closing database gates, and full-sync cut closes
and drains the snapshot-transaction gate before database gates. The shared
command-layer helper cooperatively yields while the slot is held, but blocking
List and Sorted Set attempt paths still retry the same acquisition after
1 ms sleeps.

Admission onto the slot is dynamic rather than static-flag based. A request
whose command kind carries `kCmdKeyViewComplete` and whose concrete
`DetermineKeys` view maps to a single shard skips the slot entirely: a
single-flow envelope cannot join a cross-flow rendezvous cycle, so ordering it
against other flows buys nothing. Gate-eligible writes whose execution-time
participant sets can exceed their key view keep serializing on it, including
`SORT` with BY/GET patterns, `GEORADIUS` with STORE/STOREDIST, and the
aggregate sorted-set STORE variants; command kinds without the proven-complete
flag fail conservative and always serialize. `EXEC` keeps the slot regardless
of how many shards its queued writes touch.

The MSET pre-acquisition fixes only that command's order-slot/DB inversion. Its
body still acquires database admission before snapshot-transaction admission,
while a full-sync cut closes and drains the snapshot gate before draining the
database gates, so `MSET` can still participate in a DB/snapshot hold-and-wait
cycle. Other multi-shard paths can additionally acquire database admission
before the publication-order slot. `FLUSH` takes the order slot before draining
database gates. These inverse orders have no deterministic regression proof;
the current implementation must not be described as deadlock-free or as
providing a fairness deadline.

## Redis interoperability

### Following Redis

A Redis follower authenticates, sends PING, advertises its listening port and
PSYNC2 capability, and requests either its process-local replid/offset or a
fresh full synchronization. FULLRESYNC receives a length-delimited RDB into a
temporary file. Import is serialized, closes and drains command database
gates, resets the source-owned slots, validates ownership, and restores raw
Redis values. Unsupported self-describing Module values and Module auxiliary
records are skipped with warnings. Redis 7 `FUNCTION2` entries are instead
collected as one catalog, validated before key application begins, and
installed only after the RDB keys have applied successfully. The pre-release
Function opcode is rejected because it is not safely skippable.

The online stream handles `SELECT`, `PING`, `REPLCONF GETACK`, `PUBLISH`,
Function mutations, keyed writes, and strict `MULTI`/`EXEC`. `PUBLISH` is
delivered locally without being forwarded again. The offset advances only
after successful apply.
For Redis Cluster, each source must be a slot-owning master; `ADDREPLICAOF`
accepts only a source with disjoint slots and the same complete topology. The
node becomes online only when registered sources cover all 16,384 slots, the
source count matches the topology's master count, every source dataset is
valid, and the topology is not faulted. Two consecutive incompatible topology
observations fault the topology and return the node to loading.

### Exporting to Redis

Redis export is available only while Keylane is a master, requires the replica
to advertise diskless EOF support, and permits one export connection at a
time. The exporter closes command gates, starts one RDB snapshot per worker,
fences every replication log, reopens writes, and streams a bounded-queue RDB
followed by online backlog events. It snapshots the Function catalog inside
the same cut and emits Redis 7 `FUNCTION2` entries before the key records.

The backlog merger reads one head event per worker. Ready cross-worker
transactions and control barriers take priority over unrelated mutations; a
transaction is emitted once as Redis `MULTI`/`EXEC`, and a flush is emitted
once after matching copies are present on every worker. Without
`redis-export-backpressure`, a slow Redis replica that falls below a backlog
floor is disconnected and must full-sync again. With it enabled, the exporter
pins its cursors and foreground writes inherit the backlog pressure.

### Sentinel-managed failover

Keylane exposes the Redis-shaped status surface Sentinel uses. `ROLE` reports
the master or replica tuple and offsets; `INFO replication` reports downstream
replicas, upstream address and link state, link-down duration, replication
offsets, and `slave_priority`. Native and Redis link progress are mapped onto
these process-local compatibility offsets. Sentinel's named Pub/Sub
connections also appear in `CLIENT LIST` and can be selected by `CLIENT KILL
TYPE pubsub`.

Sentinel normally queues `REPLICAOF`/`SLAVEOF`, `CONFIG REWRITE`, and `CLIENT
KILL TYPE normal|pubsub` in one `MULTI`/`EXEC`. Keylane accepts these commands
as an isolated management batch, preserves their order and individual replies,
and rejects mixing ordinary commands into that batch. It does not stop
unrelated work between the management commands; the role transition itself
provides the storage admission boundary.

`CONFIG REWRITE` preserves unmanaged directives and atomically replaces the
configured upstream mode plus `replica-priority` through a synced temporary
file, rename, and directory sync. It rejects a node with multiple Redis Cluster
upstreams because that topology cannot be represented by one Redis config
directive. Together with priority zero/lower-value semantics, this is the
surface exercised by an external Redis Sentinel quorum during promotion and
reattachment.

## Configuration and observability

| Setting or command | Current scope and behavior |
|---|---|
| `replicaof host port` / `REPLICAOF` | Redis-style config or runtime role change; native-first protocol discovery |
| `redis-replicaof host port` / `--redis-replicaof` | Explicit startup Redis PSYNC source |
| `ADDREPLICAOF host port` | Runtime addition of a disjoint master from the active Redis Cluster |
| `replica-read-only` | Startup write policy; `REPLICAOF NO ONE` is writable regardless |
| `replica-priority` | Startup/runtime Sentinel election priority, default 100; zero is ineligible and lower nonzero values are preferred |
| `CONFIG REWRITE` | Atomically persists the current single upstream mode and `replica-priority`; unavailable without a config file or with multiple Redis Cluster sources |
| `tls-replication`, `masteruser`, `masterauth` | Outgoing control and every data connection; only the `default` user is supported |
| `repl-backlog-size` | Startup/CLI/runtime global backlog, default 1 GiB; at least one 8 MiB block per worker |
| `replication-publish-queue-mb-per-worker` | Startup/CLI/runtime publisher waterline, default 16 MiB per worker |
| `replication-snapshot-batch-size` | Startup/CLI/runtime scan scheduling batch, default 64 |
| `replication-snapshot-read-concurrency` | Runtime-only read concurrency, default 16 and maximum 128 |
| `redis-export-backpressure` | Startup/CLI selection between cursor pinning and disconnect-on-gap |

`INFO replication` reports Redis/Sentinel-compatible role, link and offset
fields alongside upstream identity, native session/flow counts, history IDs,
downstreams, Redis sources, dataset validity, priority, and topology fault
state. Replica and Sentinel Pub/Sub connections are registered in CLIENT
metadata. Prometheus exposes per-worker backlog, publisher queue,
retention/backpressure, full-sync queue/session, and connection metrics.

## Invariants, failures, and current limitations

- Role epoch, history ID, session ID, flow LSN, partition mutation sequence,
  database epoch, and target replication epoch are separate identity domains.
  None can substitute for another.
- All native flows continue together or full-sync together. A target publishes
  online only after all partitions and the all-flow cut validate.
- Replay never republishes itself, and a replica never performs authoritative
  active expiration.
- Runtime-only `kEphemeral` events consume normal publication and retention
  capacity but create no durable keyspace baseline. A replica may still issue a
  local `PUBLISH`; only a source forwards its PUBLISH events downstream.
- Direct `PUBLISH` and the ephemeral publication phase of a `PUBLISH`-only
  `EXEC` are not protected by the gates that close initial synchronization. A
  native event in the FIFO-drain-to-backlog-fence window, or a Redis-export
  event during the RDB cut but before its worker's backlog fence, can therefore
  be absent from the target.
- Publication admission and retention backpressure must delay work rather than
  leave a successful primary write out of an otherwise valid history.
- Native cascading replication is unsupported. Replica record application
  bypasses source-side full-sync subscribers, so an `A -> B -> C` topology can
  stop forwarding changes after C's snapshot baseline.
- Storage record application during replica synchronization bypasses the
  command layer's database gates. The code records that this breaks the
  exclusive, still-keyspace assumption used by KEYS's two-pass response and by
  FLUSHDB drain-then-detach; concurrent apply can therefore make the announced
  KEYS array length disagree with emitted elements or violate FLUSHDB
  exclusivity.
- Online ingress protocol, frame identity or ordering, fragment reassembly, and
  KRC1 decode failures cancel the session without explicitly invalidating the
  target's prior continuation state; rendezvous and command-apply failures do
  invalidate it.
- The global publication-order slot has no fairness deadline. Its marker hook
  shortens replicated `MSET` ownership and its early acquisition fixes the
  MSET order/DB inversion, but `MSET` remains exposed to the DB/snapshot
  inversion and other paths retain DB/order inversions. These gates still admit
  hold-and-wait cycles. Blocking List and Sorted Set attempts also retain 1 ms
  polling rather than cooperative yield.
- Native reset can leave a stale dataset-valid flag after detaching the last
  completed root, while Redis Cluster promotion treats any one valid source as
  sufficient. `REPLICAOF NO ONE` can therefore publish an empty native dataset
  during replacement or a partial Cluster dataset before all sources are ready.
- Redis PSYNC cursors and native continuation cursors do not survive restart.
  Durable target data does not imply crash-resumable replication history.

## Verification and known gaps

`tests/replication_log_e2e_test.cpp` directly exercises source full-sync
capture, replacements and handoff, publisher admission, fragmentation,
capacity changes, retention backpressure, deterministic TTL effects, strict
replay, database barriers, and restart without backlog recovery.
`tests/list_e2e_test.cpp` covers native full sync and tailing, unequal worker
counts, large values, role changes, reconnects, TLS on control and all flows,
flush during full sync, exact-once non-idempotent effects, tight queue
waterlines, multiple replicas, Function-catalog full sync and incremental
mutation, config rewrite, and transaction/control fault injection.

`tests/multikey_e2e_test.cpp` covers concurrent wide replicated `MSET`, early
order-slot release after all participant markers, and changing participant sets
across several wire batches; this is the regression coverage for duplex source
sending and cross-flow rendezvous progress. `tests/pubsub_e2e_test.cpp` covers
local and replicated `PUBLISH`, runtime-only publish transactions, mixed
durable/Pub-Sub transactions, and RESP2/RESP3 subscribers.

`tests/redis_cluster_psync_e2e.sh` covers multi-source Redis Cluster discovery,
slot ownership, online writes, disconnect, and partial resynchronization.
`tests/redis_export_e2e.sh` covers diskless baseline transfer, online writes,
transactions, flush, detach, Function transfer, and both export configuration
modes. `tests/sentinel_e2e_test.cpp` runs a three-Sentinel quorum through
priority selection, promotion, reattachment, and post-failover writes.
`tests/multi_exec_e2e_test.cpp`, `tests/rdb_test.cpp`, and `tests/config_test.cpp`
cover Sentinel management transaction isolation, `FUNCTION2` codecs/catalog
validation, priority parsing, and atomic config rewrite. Unit tests also cover
expiration-effect construction and replication-frame validation.

There is no focused malformed-KRC1 decoder matrix, standalone Redis follower
E2E distinct from the cluster path, forced slow-reader Redis export test, or
deterministic gate-order regression across publication-order and
FLUSH/full-sync interleavings. Two legacy replication tests in
`tests/list_e2e_test.cpp` are disabled and are not current verification.

## Source map

| Claim | Repository source |
|---|---|
| Public roles, options, status, and manager boundary | `include/keylane/replication.h` |
| Native control/data protocol, duplex online flow, role lifecycle, Redis follower/export, topology, Function full sync, and reconnect behavior | `src/replication/replication.cpp` |
| Canonical command format and deterministic expiration effects | `include/keylane/replication_command.h`, `src/replication/command.cpp` |
| REPLICAOF/Sentinel commands, MSET publication admission/order, Function and PUBLISH capture, transaction/control capture, and trusted replay | `include/keylane/command.h`, `src/redis/command.cpp`, `src/redis/command_table.cpp` |
| Authenticated listener handoff and module construction | `src/redis/server.cpp` |
| Redis RDB Function catalog import/export and `FUNCTION2` encoding | `include/keylane/rdb.h`, `src/redis/rdb.cpp`, `src/redis/server.cpp` |
| Pub/Sub delivery and connection/session state used by replicated PUBLISH and Sentinel | `include/keylane/pubsub.h`, `src/redis/pubsub.cpp`, `src/redis/server.cpp` |
| Runtime source-log and target full-sync interfaces | `include/keylane/storage/engine.h` |
| Backlog blocks, publisher/session queues, capture state, and target sync state | `src/storage/engine/impl.h` |
| In-memory append/read/fence/retention, capacity, invalidation, and control publication | `src/storage/engine/replication_log.cpp` |
| Full-sync scanning, replacements, handoff, target reset/apply/promotion/abort, cascade and DB-gate limitations | `src/storage/engine/replication.cpp`, `src/storage/engine/write.cpp` |
| Frame layout, event kinds, fragmentation, and checksums | `include/keylane/storage/format.h`, `src/storage/format.cpp` |
| Startup/runtime replication configuration and atomic CONFIG REWRITE | `app/keylane.cpp`, `include/keylane/server.h`, `src/config.cpp`, `src/redis/command.cpp` |
| Native, log, MSET, Pub/Sub, Sentinel, Redis PSYNC/export, RDB, format, and configuration verification | `tests/replication_log_e2e_test.cpp`, `tests/list_e2e_test.cpp`, `tests/multikey_e2e_test.cpp`, `tests/pubsub_e2e_test.cpp`, `tests/sentinel_e2e_test.cpp`, `tests/redis_cluster_psync_e2e.sh`, `tests/redis_export_e2e.sh`, `tests/multi_exec_e2e_test.cpp`, `tests/rdb_test.cpp`, `tests/replication_command_test.cpp`, `tests/storage_format_test.cpp`, `tests/config_test.cpp` |
