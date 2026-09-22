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

Managed Single has exactly one Group covering every slot. It serves DB0 basic
single-key commands, collections, TTL and PUBLISH through shared Group authority.
Complete replicas accept ordinary read connections without READONLY. Configure
`replica-serve-stale-data yes|no` in the file or through CONFIG GET/SET; the default
`yes` permits complete stale data during disconnection. `no` returns MASTERDOWN
for data requests while the replication link is down. Initial FULL and invalid
populations return LOADING in either setting; replicas always reject mutations.
Multi-key operations, nonzero DBs, transactions, scripts, blocking operations
and global data/catalog paths remain explicitly unsupported in managed Single.
Cluster retains its current routing and READONLY contract.

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
