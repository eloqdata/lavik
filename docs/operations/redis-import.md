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

# Redis import into Meta-managed Lavik

Use RedisShake **v4.6.2**, commit
`f20f28e6f2679e71a213904d2c74ceb521e19551`, with Redis **7.2.14** as the
source baseline. `scripts/install_test_redisshake.sh` installs the
checksum-pinned release. Acceptance uses unmodified redis-py **8.1.0** and
go-redis **9.22.0**, with RESP2 and RESP3.

## Choose the import path

| Path | Target ownership and recovery |
|---|---|
| RedisShake `sync_reader` → `redis_writer` → managed Lavik | Create the target with Meta from the start. Ordinary authenticated writes use Group authority and native replication; Meta owns failover throughout import. |
| Non-Meta Lavik `REPLICAOF` following Redis/Redis Cluster | Existing direct PSYNC consumption, tracked by [#88](https://github.com/eloqdata/lavik/issues/88). It does not acquire the managed path's HA guarantees. |

Do not convert an already populated non-Meta import node into a managed target:
that data-adoption procedure is not supported. The reverse RedisShake export
path has separate scope and validation in
[#122](https://github.com/eloqdata/lavik/issues/122).

## Prepare and start

1. Create an isolated, empty Meta-managed Single or Cluster deployment with
   native replicas, using the [deployment guide](cluster-deployment.md).
   Upgrade every target member to the same build before importing. Keep
   application traffic off the target until cutover. Confirm all target Groups
   are serving and all intended replicas have readable complete populations.
2. Inventory source databases, types, Function libraries, expiry requirements
   and command usage. Single supports DB0–15; Cluster supports DB0 and native
   same-slot restrictions. Filtering/remapping nonzero source databases or
   cross-slot operations is the migration tool/operator's responsibility.
   Source and target node counts need not match: the test matrix uses three
   source Cluster primaries and two target Groups.
3. Give the tool access to every advertised source and target endpoint,
   including credentials. Keep source Cluster topology stable. Use a new
   private working directory for each attempt, with enough capacity for RDB
   and AOF spool files. Place configuration containing passwords under mode 0600.
4. Start the pinned binary with a configuration like this. Substitute addresses,
   passwords and absolute paths; set each `cluster` flag independently.

```toml
[sync_reader]
address = "127.0.0.1:6379"
password = "source-password"
cluster = false
prefer_replica = false
sync_rdb = true
sync_aof = true

[redis_writer]
address = "127.0.0.1:6380"
password = "target-password"
cluster = false

[advanced]
dir = "/mnt/local_nvme/import-attempt-1/spool"
log_file = "/mnt/local_nvme/import-attempt-1/shake.log"
status_port = 18080
log_interval = 1
rdb_restore_command_behavior = "panic"
target_redis_proto_max_bulk_len = 512000000
```

```sh
redis-shake /mnt/local_nvme/import-attempt-1/shake.toml
curl --fail http://127.0.0.1:18080/status
```

Restrict access to the status listener. A process that is alive, a successful
RDB load, or a zero progress difference alone is not a completion proof.

## Object and command compatibility

The default baseline path sends RESTORE. Lavik accepts DUMP/RDB versions up to
11 and the tested Redis 7.2 encodings for strings, lists, hashes, sets, sorted
sets and streams. Tests include binary keys/values, a 2 MiB string, compact and
expanded collection encodings, and stream consumer-group pending state.
RedisShake repackages DUMP with a version-6 footer; that footer does not convert
object encodings or establish compatibility with arbitrary newer Redis objects.
Unsupported types/encodings and module values must fail the rehearsal; do not
skip or silently count them as migrated. A new source version needs its own
compatibility run.

Above `target_redis_proto_max_bulk_len`, RedisShake switches to individual type
commands. Tests force that path at 1024 bytes for the five non-stream types.
This threshold selects a reconstruction path; it is not a guarantee that every
resulting command is smaller than the threshold. The tool's Stream reconstruction
cannot be treated as a lossless replacement for RESTORE's full stream metadata;
keep Streams on the tested RESTORE path. Duplicate handling may differ in the
type-command path, so reruns still require an empty target.

Ordinary incremental HSET and other variadic writes publish through the native
LRC1 command format, with up to 65,535 arguments and a 1 GiB complete-event
budget. These are native format limits, not a promise to accept every possible
Redis request size. Older Lavik receivers capped command arguments at 1024;
upgrade all replicas before using larger batches.

RedisShake converts baseline absolute expiry times into relative RESTORE/PEXPIRE
TTLs at parse time. Destination queue delay can extend expiry; an already
expired snapshot entry can be represented with a minimal positive TTL. Lavik
executes those commands with Redis semantics. Measure absolute `PEXPIRETIME`
differences against a pre-agreed migration tolerance; strict absolute-expiry
preservation requires a tool-side solution or explicit reconciliation before
cutover. Tests bound baseline drift by elapsed import time, and compare
persistent/expiring state independently. Incremental expiry commands are also
verified. Do not infer exact expiry preservation from equal values alone.
Redis 7.2 propagates XREADGROUP as XCLAIM without the `entries-read` counter,
including to native Redis replicas. Exact equality of that diagnostic counter
needs explicit XGROUP SETID ENTRIESREAD or a fresh snapshot; the test fixture
uses SETID. This does not relax checks of message IDs, values or pending entries.

Function libraries use the existing per-Group catalog. FLUSHDB/FLUSHALL on a
Cluster node affect its receiving Group; Single uses the selected DB/all 16 DBs.
RedisShake decides how keyless commands are broadcast and how source libraries
reach target nodes. In particular, source Cluster keyless operations/catalog
conflicts are not resolved by Lavik and cannot be inferred from node-count
matching. Evaluate the tool's mapping against the source workload. The gate
uses live Function/FLUSH changes for a single source and a consistent baseline
catalog for the Cluster-source fixtures; this is fixture coverage, not a new
server requirement that source catalogs be frozen or equal.

## Stop writes, verify and cut over

1. Stop all source application writes, including jobs and administrative
   mutations. Record each source primary's `master_repl_offset` and identity.
   Keep sources available throughout verification.
2. In `/status`, require every reader to reach `syncing aof`, with both
   `aof_received_offset` and `aof_sent_offset` at or beyond that source's recorded
   tail. Require all writers' `unanswered_entries` and `unanswered_bytes` to be
   zero. Treat `consistent` as corroboration, not the sole acceptance condition.
3. Verify every target Group and intended native replica. A fresh connection's
   bare WAIT only counts online replicas. To establish a fence after import,
   use one connection per current Owner to write a private, correctly routed
   marker, delete it, then issue `WAIT <required-replicas> <finite-timeout>` on
   that same connection. Check the returned count. This observes all publisher
   workers in that Group. It does not upgrade asynchronous replication into a
   global commit or guarantee survival under every failover.
4. Scan every source DB/primary and every target Group/replica. Compare the
   complete key sets (including extra target keys), types, logical values,
   absolute expiry times within the agreed tolerance, Function catalog/code,
   and relevant Stream/group/pending state. Use READONLY for direct Cluster
   replica reads. Ensure source identity and target Owner/history have remained
   stable across the proof; any transition invalidates this verification pass.
5. Stop RedisShake, confirm it has stopped writing, and move business clients
   to the target using the normal Single/Sentinel or Cluster discovery path.
   Verify reads and writes through standard clients. Retain the stopped source
   for the agreed rollback window; once target business writes begin, switching
   back is a separate data-reconciliation decision.

## Failure and a fresh baseline

If the tool exits, connectivity fails, a target changes Owner, or verification
fails, stop the attempt and keep business traffic off the target. RedisShake
v4.6.2 has no reliable checkpoint resume for this workflow. A lost reply can
mean a command committed: never replay an uncertain non-idempotent suffix or
assume `RESTORE REPLACE` makes the entire attempt idempotent.

Let Meta finish target failover/recovery, then stop every old writer and drain
its admitted commands. Prefer a newly created isolated target. To reuse the
existing isolated target, explicitly FLUSHALL and FUNCTION FLUSH on **every
current target Owner**, confirm native replication and empty key/catalog state,
and ensure no old tool can write again. These commands destroy target contents;
never use them on a serving business deployment. Start a new tool process and
spool directory, rebuild the entire source baseline, follow the new tail, and
repeat every cutover check. Source writes may continue during this rebuild.

Meta recovery is the existing Group recovery implementation. Replication is
asynchronous: target failover may lose acknowledged writes not present on the
selected replica, and uncertain client outcomes remain uncertain. Reparent FULL
still has the destructive-rebuild availability limits addressed by
[#45](https://github.com/eloqdata/lavik/issues/45). The standalone external-Sentinel
churn gate in [#74](https://github.com/eloqdata/lavik/issues/74) remains separate;
this managed import validation does not certify that deployment.

The tool strips source MULTI/EXEC boundaries. Import provides no cross-shard
global snapshot, exactly-once delivery, automatic resume, or support for source
Cluster topology changes. Do not serve application traffic from a partially
imported dataset.

## Reproduce acceptance

Build with the repository's fault-enabled CI configuration and install its
pinned Redis, RedisShake, redis-py and Go prerequisites. Run:

```sh
ctest --test-dir build_ci -L redis-import --output-on-failure
ctest --test-dir build_ci -R 'meta_integration.gate_managed_wait' --output-on-failure
```

The import matrix covers four topology combinations and forced type-command
reconstruction. Single and Cluster target fault cases interrupt the tool,
writer connection, and target primary during baseline and incremental phases,
then rebuild and verify through both standard clients. Artifacts follow
`LAVIK_TEST_DATA_DIR`; set it to a writable local disk directory before running.
