# Replication

## Responsibility and boundary

The replication subsystem owns node role, upstream and downstream session
lifetime, native Keylane synchronization, Redis PSYNC interoperability, and
trusted replay. `ReplicationManager` coordinates connections and role changes;
the Redis command layer captures committed logical effects; and the storage
engine owns the source backlog, full-sync capture state, target rebuild state,
and durable epoch changes.

Replication moves deterministic logical commands and snapshot records, not
physical block addresses or record offsets. Celer owns sockets, TLS, worker
scheduling, and cross-core submissions below this boundary. Normal storage and
transaction paths remain authoritative for mutation ordering and durability.
Replication-origin commands re-enter those paths with client role checks
bypassed and publication disabled.

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
admission. If issued while a rebuild is loading, it discards the partial
population and publishes an empty writable master. If issued from an online
replica, it retains the completed dataset and makes it writable. Native
reconnects return to LOADING even when the prior dataset was complete. A Redis
follower whose process-local dataset is still valid can remain online and
serve its local read-only copy during a transient source disconnect.

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
  -> canonical command, transaction envelope, or control barrier
  -> worker-local publisher FIFO
  -> shared in-memory replication frames
  -> KLFLOW batch
  -> target reassembly and trusted apply
  -> target cursor publication
  -> ACK
```

Canonical commands use the KRC1 version-1 encoding: explicit logical database,
argument count and lengths, then argument bytes. Large arguments stream into
the backlog without another complete flattened allocation. One logical event
has one flow-local LSN and may span several CRC-checked frames and 8 MiB memory
blocks. Mutation, transaction, and database-control events are distinct frame
kinds.

Source publishers normally target 2 MiB and are capped at 128 frames per
batch. The first frame is admitted even when it exceeds the byte target, so a
one-frame batch can be larger than 2 MiB. Publishers then wait for ACKs for the
complete logical events in that batch. This is not per-command stop-and-wait.
A flow with no tail data sleeps briefly; a full-sync flow or online flow that
makes no protocol progress for ten minutes is cancelled. There is no overall
wall-clock limit for a progressing full sync.

## Runtime backlog and backpressure

Each source worker has one heap-backed backlog shared by all downstream native
sessions and the Redis exporter. Downstreams own only their cursors and
retention pins. `repl-backlog-size` is a global quota divided across workers in
8 MiB blocks; allocation is lazy. Per-worker LSNs start at one for a new
history and increase monotonically. The log remains active across downstream
disconnects and is cleared only when disabled, the process exits, or the
history is invalidated.

A connected native downstream pins its first unacknowledged LSN. The publisher
must wait rather than evict required history; after reaching capacity it uses a
low-water threshold before resuming to avoid oscillation. Disconnect releases
the pin immediately, wakes writers, and leaves the remaining capacity as a
circular reconnect window. Shrinking below pinned history establishes a target
quota rather than deleting required events. A single oversized event can own
multiple blocks and temporarily exceed the ordinary quota, but eviction and
trim never retain only part of an event.

Writes reserve the worker publisher queue and all relevant active full-sync
queues before entering database or key gates. Multi-participant requests
reserve workers in sorted order. Queue exhaustion therefore backpressures the
client before commit instead of silently dropping publication. Encoding or
backlog allocation failure after admission marks the history invalid: the
primary dataset remains usable, but the manager assigns a new history ID,
cancels downstream sessions, clears all worker logs, and requires full sync.

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

The target validates frame identity and fragment order, decodes one complete
KRC1 command, and applies it through strict replay. Ordinary non-idempotent
commands publish their next in-memory cursor before writing the ACK. If that
ACK detects a disconnect, reconnect does not repeat an already applied
`APPEND`, `INCR`, or similar effect.

Multi-participant writes carry identical `__KEYLANE_TX_V1` envelopes on every
participant flow. The target groups arrivals by transaction ID, verifies the
participant set and body, applies the command once after every participant is
present, and advances all participant cursors before any flow ACKs. `FLUSHDB`
and `FLUSHALL` use a shared barrier ID copied to every source flow; the target
waits for all copies, installs one database epoch change, then advances the
whole cursor vector. Transaction/control rendezvous failures and command apply
failures invalidate the target's continuation state so a retry cannot continue
from an uncertain cut. An online KRC1 decode error instead returns from the
flow without explicitly invalidating the prior continuation state.

Source-side cross-flow transaction and control publication currently uses one
process-global ordering flag. Contenders poll it at 1 ms intervals. The normal
multi-shard command path first holds database admission and then waits for this
ordering flag and the snapshot-transaction gate; FLUSH holds the ordering flag
before closing and draining database gates; full-sync cut closes and drains the
snapshot gate before database gates. These acquisition orders admit a
hold-and-wait risk and are not covered by a deterministic regression test. The
current implementation must not be described as deadlock-free.

## Redis interoperability

### Following Redis

A Redis follower authenticates, sends PING, advertises its listening port and
PSYNC2 capability, and requests either its process-local replid/offset or a
fresh full synchronization. FULLRESYNC receives a length-delimited RDB into a
temporary file. Import is serialized, closes and drains command database
gates, resets the source-owned slots, validates ownership, and restores raw
Redis values. Unsupported self-describing Module and Function entries are
skipped with warnings.

The online stream handles `SELECT`, `PING`, `REPLCONF GETACK`, keyed writes,
and strict `MULTI`/`EXEC`. The offset advances only after successful apply.
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
followed by online backlog events.

The backlog merger reads one head event per worker. Ready cross-worker
transactions and control barriers take priority over unrelated mutations; a
transaction is emitted once as Redis `MULTI`/`EXEC`, and a flush is emitted
once after matching copies are present on every worker. Without
`redis-export-backpressure`, a slow Redis replica that falls below a backlog
floor is disconnected and must full-sync again. With it enabled, the exporter
pins its cursors and foreground writes inherit the backlog pressure.

## Configuration and observability

| Setting or command | Current scope and behavior |
|---|---|
| `replicaof host port` / `REPLICAOF` | Redis-style config or runtime role change; native-first protocol discovery |
| `redis-replicaof host port` / `--redis-replicaof` | Explicit startup Redis PSYNC source |
| `ADDREPLICAOF host port` | Runtime addition of a disjoint master from the active Redis Cluster |
| `replica-read-only` | Startup write policy; `REPLICAOF NO ONE` is writable regardless |
| `tls-replication`, `masteruser`, `masterauth` | Outgoing control and every data connection; only the `default` user is supported |
| `repl-backlog-size` | Startup/CLI/runtime global backlog, default 1 GiB; at least one 8 MiB block per worker |
| `replication-publish-queue-mb-per-worker` | Startup/CLI/runtime publisher waterline, default 16 MiB per worker |
| `replication-snapshot-batch-size` | Startup/CLI/runtime scan scheduling batch, default 64 |
| `replication-snapshot-read-concurrency` | Runtime-only read concurrency, default 16 and maximum 128 |
| `redis-export-backpressure` | Startup/CLI selection between cursor pinning and disconnect-on-gap |

`INFO replication` reports role, upstream identity, native session/flow counts,
history IDs, downstreams, Redis sources, offsets, dataset validity, and topology
fault state. Replica connections are registered in CLIENT metadata. Prometheus
exposes per-worker backlog, publisher queue, retention/backpressure, full-sync
queue/session, and connection metrics.

## Invariants, failures, and current limitations

- Role epoch, history ID, session ID, flow LSN, partition mutation sequence,
  database epoch, and target replication epoch are separate identity domains.
  None can substitute for another.
- All native flows continue together or full-sync together. A target publishes
  online only after all partitions and the all-flow cut validate.
- Replay never republishes itself, and a replica never performs authoritative
  active expiration.
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
- An online KRC1 decode failure terminates the flow without explicitly
  invalidating the target's prior continuation state; rendezvous and command
  apply failures do invalidate it.
- The global publication ordering and gate acquisition order described above
  have an unresolved hold-and-wait risk. There is no fairness deadline beyond
  1 ms polling.
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
waterlines, multiple replicas, and transaction/control fault injection.

`tests/redis_cluster_psync_e2e.sh` covers multi-source Redis Cluster discovery,
slot ownership, online writes, disconnect, and partial resynchronization.
`tests/redis_export_e2e.sh` covers diskless baseline transfer, online writes,
transactions, flush, detach, and both export configuration modes. Unit tests
cover role/config parsing, expiration-effect construction, and replication
frame validation.

There is no focused malformed-KRC1 decoder matrix, standalone Redis follower
E2E distinct from the cluster path, forced slow-reader Redis export test,
three-flow publication-cycle test, 1 ms contention test, or deterministic
FLUSH/full-sync gate-order regression. Two legacy replication tests in
`tests/list_e2e_test.cpp` are disabled and are not current verification.

## Source map

| Claim | Repository source |
|---|---|
| Public roles, options, status, and manager boundary | `include/keylane/replication.h` |
| Native control/data protocol, role lifecycle, Redis follower/export, topology, and reconnect behavior | `src/replication/replication.cpp` |
| Canonical command format and deterministic expiration effects | `include/keylane/replication_command.h`, `src/replication/command.cpp` |
| REPLICAOF parsing, publication admission/order, transaction/control capture, and trusted replay | `include/keylane/command.h`, `src/redis/command.cpp`, `src/redis/command_table.cpp` |
| Authenticated listener handoff and module construction | `src/redis/server.cpp` |
| Runtime source-log and target full-sync interfaces | `include/keylane/storage/engine.h` |
| Backlog blocks, publisher/session queues, capture state, and target sync state | `src/storage/engine/impl.h` |
| In-memory append/read/fence/retention, capacity, invalidation, and control publication | `src/storage/engine/replication_log.cpp` |
| Full-sync scanning, replacements, handoff, target reset/apply/promotion/abort, cascade and DB-gate limitations | `src/storage/engine/replication.cpp`, `src/storage/engine/write.cpp` |
| Frame layout, event kinds, fragmentation, and checksums | `include/keylane/storage/format.h`, `src/storage/format.cpp` |
| Startup and runtime replication configuration | `app/keylane.cpp`, `include/keylane/server.h`, `src/config.cpp`, `src/redis/command.cpp` |
| Native, log, Redis PSYNC/export, format, and configuration verification | `tests/replication_log_e2e_test.cpp`, `tests/list_e2e_test.cpp`, `tests/redis_cluster_psync_e2e.sh`, `tests/redis_export_e2e.sh`, `tests/replication_command_test.cpp`, `tests/storage_format_test.cpp`, `tests/config_test.cpp` |
