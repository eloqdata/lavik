<!-- Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0. -->

# Emergency Redis export

Use unmodified RedisShake **v4.6.2** (commit
`f20f28e6f2679e71a213904d2c74ceb521e19551`) and destination Redis **7.2.14**.
The test installers in `scripts/install_test_redis.sh` and
`scripts/install_test_redisshake.sh` pin release checksums.

| Source | Export scope |
|---|---|
| Managed Single Primary | DB0–15 and Function catalog |
| Managed Cluster Primary | This Group's DB0 and Function catalog |
| Standalone Primary | DB0–15 and Function catalog |

Select the current ready, authorized Primary. A replica, incomplete population
or expired Owner cannot export. Cluster export does not discover or combine
Groups: arrange a separate isolated target for each Group before any application
routing change. Supported objects are strings, lists, hashes, sets, sorted sets
and Streams (entries, groups, consumers and pending entries). Binary keys and
values, large objects, expiries, deletes, renames, Function changes, FLUSH and
transaction final effects use the same stream. RedisShake's relative TTL
conversion drift is accepted; preserve and verify expiry classification.

## Prepare

Keep the destination empty and isolated from business traffic throughout import.
RedisShake does not guarantee transaction atomic visibility while writing the
destination. An interrupted attempt must not be reused as a baseline.

Check source authentication and independent resource limits:

```sh
redis-cli -h SOURCE -a "$SOURCE_PASSWORD" CONFIG GET repl-backlog-size
redis-cli -h SOURCE -a "$SOURCE_PASSWORD" CONFIG GET redis-export-disk-backlog-size
redis-cli -h SOURCE -a "$SOURCE_PASSWORD" CONFIG GET replication-backlog-backpressure
redis-cli -h SOURCE -a "$SOURCE_PASSWORD" CONFIG SET redis-export-disk-backlog-size 1gb
```

`repl-backlog-size` limits memory history. `redis-export-disk-backlog-size`
limits temporary incremental disk blocks during RDB export, defaults to 1 GiB,
requires at least 8 MiB and rounds down to complete 8 MiB blocks. It does not
limit RDB size. Configure it in the source config file, with
`--redis-export-disk-backlog-size`, or with CONFIG SET. An active session keeps
its starting disk quota. Provision free capacity on Lavik's data devices; this
is separate from RedisShake's spool directory. Disk buffers also require
retained-memory admission.

`replication-backlog-backpressure yes` is the default and applies to both native
replicas and the exporter. A slow exporter can stall business writes. Setting
it to `no` wakes blocked publishers and permits eviction; a lagging export then
fails. Re-enabling it does not restore lost history. Disk quota/device exhaustion
always fails the export and frees its retention rather than waiting indefinitely.

## Run

Write `export.toml`, setting real endpoints, credentials and spool paths:

```toml
[sync_reader]
address = "SOURCE:6379"
password = "SOURCE_PASSWORD"
cluster = false
prefer_replica = false
sync_rdb = true
sync_aof = true
try_diskless = true

[redis_writer]
address = "TARGET:6379"
password = "TARGET_PASSWORD"
cluster = false

[advanced]
dir = "/mnt/local_nvme/redis-export-spool"
log_file = "/mnt/local_nvme/redis-export.log"
status_port = 18080
rdb_restore_command_behavior = "panic"
```

```sh
redis-shake export.toml
curl -s http://127.0.0.1:18080/status
redis-cli -h SOURCE -a "$SOURCE_PASSWORD" INFO replication
```

Only one PSYNC exporter is allowed per source. Each new connection performs
FULLRESYNC. The source sends RDB, waits for the initial ACK, replays disk staging,
then sends the live memory tail. `redis_export_phase` progresses through
`admission`, `rdb`, `waiting-ack`, `disk-replay`, and `online`.
Cancellation withdraws the session identity immediately. Wait for
`redis_export_active:0` before opening a replacement connection; this confirms
the previous session has joined and released its slot and temporary resources.

## Stop writes, verify, cut over

1. Stop all application mutations and scheduled writers to the exported scope.
   Keep target business traffic disabled.
2. Record source `redis_export_session_id`, `redis_export_history_id` and
   `redis_export_source_next_lsns` from INFO replication. This vector crosses
   every source publisher fence and covers committed events before the sample.
3. Wait for phase `online` and each `redis_export_sent_next_lsns` entry to cover
   the corresponding recorded source position. Record `redis_export_offset`.
   Restart verification if the session/history changes or becomes inactive.
4. At RedisShake `/status`, require the reader's `aof_received_offset` and
   `aof_sent_offset` to cover that Redis byte offset. Require every writer's
   `unanswered_entries` and `unanswered_bytes` to reach zero. The tool's
   `consistent` flag or one marker key alone is insufficient.
5. Compare all databases in scope: complete key set (including unwanted extra
   keys), types, values/collections, Stream group/PEL state, persistent versus
   expiring attributes, and `FUNCTION LIST WITHCODE`. Accept the tool's TTL
   conversion drift; do not omit expiry checks. Keep the source session valid
   through this verification.
6. Stop RedisShake, switch application routing to the verified target, then
   enable target business traffic. Preserve the stopped source for rollback
   until the application cutover has been accepted.

## Failure and HA

Controlled Pause permits current valid exports and new admission. Failover does
not wait for the tool to catch up. If exporter backpressure obstructs the pause
drain, the source cancels that session to release the drain. Owner loss, lease
expiry, population/history replacement and shutdown cancel the session even
when its process still reports the Primary role. A disconnected exporter releases
its own retention without changing native replica retention/history.

There is no continuation across connections or failover. On source/tool/network
failure, stop the tool, discard the destination, select a currently valid Primary,
and start a fresh FULLRESYNC into a clean isolated target. Temporary Lavik blocks
are reclaimed on session cleanup or restart. The old target can contain a tail
that the successor never committed; copying only missing keys cannot repair it.
