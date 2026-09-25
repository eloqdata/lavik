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

# Cluster deployment quick start

Data without `meta-seed` starts standalone. With Meta seeds, Data obtains the
immutable `client_mode = "single" | "cluster"` from the committed creation
manifest before initializing storage. Do not configure a local client mode.
A managed node waits for Meta creation and registration without opening Redis;
identity or capability incompatibility fails startup. A cluster's mode cannot
be changed after creation.

Managed Single has exactly one Group covering every slot and all 16 logical
databases. It serves single-key commands, collections, TTL, PUBLISH, cross-slot
multi-key commands, cross-database COPY, List/Sorted Set blocking commands,
DBSIZE/SCAN/RANDOMKEY/KEYS, and FUNCTION LOAD/DELETE/FLUSH/RESTORE through
shared Group authority. Function libraries are shared across DB0–15;
FUNCTION DUMP/LIST work on the Owner and complete readable replicas.
Complete replicas accept ordinary read connections without READONLY. Configure
`replica-serve-stale-data yes|no` in the file or through CONFIG GET/SET; the default
`yes` permits complete stale data during disconnection. `no` returns MASTERDOWN
for data requests while the replication link is down. Initial FULL and invalid
populations return LOADING in either setting; replicas always reject mutations.
Transactions, scripts/FCALL, Stream blocking, WAIT and FLUSHDB/FLUSHALL
remain explicitly unsupported in managed Single. An uncertain Function
catalog root commit or failure to publish an already durable mutation fences
the Data process and closes the initiating connection; restart recovery is
required and a blind retry cannot determine the prior outcome. Cluster retains
DB0, CROSSSLOT, its COPY DB restriction and its READONLY contract.

In Cluster mode, FUNCTION LOAD/DELETE/FLUSH/RESTORE update the receiving
primary's catalog and replicate to its own replicas. Other Groups keep their
own libraries; use separate commands against each primary to deploy across
the cluster, without an atomic cross-Group update. FUNCTION DUMP/LIST inspect
the local catalog on a primary or complete readable replica without READONLY.
Catalog mutations inside managed MULTI remain unsupported.

On an authorized Single Owner, a connection can select a database and copy into
another database without changing its own selection:

```text
SELECT 1
SET example value PX 60000
COPY example example DB 15 REPLACE
SELECT 15
GET example
PTTL example
SCAN 0 MATCH example
```

The copy retains the source's absolute expiry time. To verify replication,
connect to a complete replica, issue `SELECT 15`, and poll `GET example` until
it returns `value`; this is an observation of that key, not a synchronous
durability guarantee. Each connection selects its database independently.
DBSIZE retains its existing index-count semantics, which can include expired
records awaiting cleanup. SCAN is an ordinary cursor traversal, not a snapshot;
restart it from cursor zero after changing databases or replacing the population.

KEYS holds its database gate until the streamed reply completes or the connection
closes. A client that continues reading slowly can delay FULL or promotion cuts
that need every database gate. The existing 30-second watchdog detects stalled
network progress; it is not a total-duration deadline or failover cancellation.
Use bounded SCAN requests for routine inspection of large databases.

No seeds means the existing standalone recovery rules also apply to files last
used by a managed node. There is no detach step or extra persisted management
marker. An incomplete destructive FULL remains protected by the existing
storage integrity fence.

Non-Meta Single retains Redis/Redis Cluster
follower support through `replicaof`, `redis-replicaof`, `REPLICAOF`/`SLAVEOF`,
and `ADDREPLICAOF`. These entry points reject Lavik upstreams before retiring
existing subscriptions or replacing data. Lavik peers use Meta native Follow
Owner. A runtime AUTH/PSYNC handshake failure leaves the old subscription intact.
Preparation times out after ten seconds; retry if Redis is busy with a background
save that delays FULLRESYNC. Startup network failures retry without opening
service, and unsupported upstreams remain
fenced and retry. Cluster source checks compare master counts and slot groups;
operators must select sources from the same cluster. Replica membership changes
do not interrupt subscription, and a new master for the same slots is followed
automatically. Existing process-local Redis offsets support reconnect, not durable
cross-process resume or automatic Meta takeover.

**Configuration change:** `cluster-enabled` and the old `cluster-*` identity,
seed and announce options have been removed, without aliases. Local `client-mode`
and `meta-managed` options are also removed and return migration hints. Use
`meta-seed`, `node-id`, `announce-ip`, `announce-port` and
`announce-tls-port`. Meta-managed nodes require a stable 40-character lowercase
hex node ID and numeric Meta seed endpoints; they reject external upstreams and
`load-rdb`. TLS requirements are unchanged. Old generated launch scripts must be
regenerated or edited before restarting with this version.

Build the three cluster binaries and launch a local cluster:

```bash
./scripts/configure_release.sh
cmake --build build --target lavik lavik-meta lavik-ctl --parallel
./scripts/cluster_local_example.sh bootstrap --root /tmp/lavik-cluster
```

The script creates three Meta nodes, two primary/replica Groups, four fresh
1 GiB Data files, and waits until the cluster is ready. `--root` must not
already exist. Use `--data-size 100G` to change the file size, or
`--bin-dir /path/to/build` to use another build directory.

Verify that clients can follow slot redirects:

```bash
redis-cli -c -h 127.0.0.1 -p 6371 SET greeting 'hello from Lavik cluster'
redis-cli -c -h 127.0.0.1 -p 6372 GET greeting
```

The generated manifest is at `/tmp/lavik-cluster/cluster.toml`; use it as the
starting point for a multi-host deployment. Replace every loopback address
with a stable reachable IP, keep each Meta and Data directory on persistent
storage, and start the same generated process roles on their assigned hosts.
All three initial Meta members use the same manifest. For normal deployment,
place Meta voters and a Group's primary/replica on separate hosts.

Useful commands:

```bash
./scripts/cluster_local_example.sh status --root /tmp/lavik-cluster
./scripts/cluster_local_example.sh down --root /tmp/lavik-cluster
./scripts/cluster_local_example.sh up --root /tmp/lavik-cluster
```

Run `up` without `bootstrap` after a restart; it retains the existing Data and
Meta state. Do not run `init` or `create` again for an existing cluster.

For TLS, Meta membership changes, or controlled failover, use the focused
[Meta control-plane runbook](meta-control-plane.md).

## Cluster client compatibility matrix

CI continuously verifies two pinned, unpatched, standard client libraries
against a Meta-managed two-Group cluster, on both amd64 and arm64. Each client
runs at RESP2 and RESP3 with `requirepass` authentication enabled, and the same
connection pool must survive a controlled failover and a primary `SIGKILL`
without being recreated:

| Client | Pinned version | Pin enforcement |
|---|---|---|
| redis-py `RedisCluster` | 8.1.0 | `tests/meta_integration/requirements-redis-py.txt` sha256, installed with `pip --require-hashes`; the gate asserts `redis.__version__` |
| go-redis `ClusterClient` | 9.22.0 | `tests/meta_integration/cluster_client_go/go.mod`+`go.sum` with `-mod=readonly`; the driver reports the compiled module version and the gate asserts it |

Every cell of that matrix covers: slot discovery through `CLUSTER SLOTS` from a
single seed node, `MOVED` redirection followed at owner change, cross-Group
key isolation, server-side `CROSSSLOT` rejection for multi-key commands, and
read/write recovery on the new owner after controlled failover (lossless) and
after an uncontrolled primary crash (acknowledged writes the replica never
saw may be absent). The matrix also covers the declared authentication and
TLS client-port configurations, including that a TLS client discovers the
advertised TLS endpoints. The gate is
`meta_integration.gate_cluster_client`; to run it locally:

```bash
./scripts/install_test_redis_py.sh /tmp/redis-py
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON \
  -DLAVIK_CLUSTER_CLIENT_REDIS_PY=/tmp/redis-py
cmake --build build --parallel
ctest --test-dir build -R meta_integration.gate_cluster_client --output-on-failure
```

Compatibility boundaries verified by the same gate:

- The Cluster command surface is `CLUSTER SLOTS`, `NODES`, `INFO`, `KEYSLOT`,
  and `MYID`. `CLUSTER SHARDS` and the slot-migration family (`SETSLOT`,
  `ASKING`, `ADDSLOTS`, ...) are rejected by design; both pinned clients
  discover slots through `CLUSTER SLOTS` only, so the rejection never affects
  their routing.
- Cross-slot multi-key handling differs by client and is part of the recorded
  contract: redis-py 8.1.0 rejects a cross-slot `mget` client-side (its
  per-slot fan-out lives in the explicit `mget_nonatomic`), while go-redis
  9.22.0 routes the whole typed command by its first key and surfaces the
  server's `CROSSSLOT` error. Server-side enforcement is proven through each
  library's official pass-through (`execute_command(..., target_nodes=...)` /
  `Do(...)`), and a rejected cross-slot `MSET` applies no partial write.
- Cluster mode serves DB0 only, and there is no slot migration (`ASK`) because
  ownership changes move whole slot ranges at once.
- After an uncontrolled primary kill, rediscovery latency follows the client
  library's own policy: redis-py reinitializes on connection errors, while
  go-redis refreshes a crash-stale slot map once it ages past its
  `ClusterStateReloadInterval` (default 60 s). A dead node answers no
  `MOVED`, so deployments that need faster go-redis rediscovery should lower
  that option. The gate enforces recovery within a 90 s per-client budget
  covering that default and logs each cell's actual rediscovery latency.
  Controlled failover cuts over with the old owner alive, so both clients can
  follow `MOVED` against it; the gate enforces recovery within its 30 s
  budget and logs the actual latencies.

## Exporting with RedisShake ScanReader

Lavik no longer serves PSYNC export; `redis-export-backpressure` is removed.
Use RedisShake ScanReader for one-shot keyspace export to Redis 7.2 or newer
(the destination must accept Lavik's RDB 11 DUMP payloads). A tested RedisShake
v4.6.2 configuration is:

```toml
[scan_reader]
cluster = true
address = "127.0.0.1:6371"
password = ""
scan = true
ksn = false

[redis_writer]
address = "127.0.0.1:6379"

[advanced]
rdb_restore_command_behavior = "rewrite"
```

Run `redis-shake shake.toml`. Set `cluster = false` for Single, which scans all
populated DBs including DB15; Cluster discovers slot owners and scans DB0.
Supply passwords and TLS options appropriate to the deployment. The example
replaces target keys with the same name, so use an empty migration destination.
Stop application writes and keep topology stable while scanning for a complete
migration. Verify the destination before switching clients; this does not
provide a cross-node snapshot or continuous synchronization.

Values and remaining TTLs transfer through SCAN/DUMP/PTTL. Functions, ACLs, and
server configuration require separate migration. Meta-managed sources must
remain ready and authorized for ordinary reads; ScanReader does not bypass
fencing. The supported startup matrix above remains unchanged.
