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

Data startup uses two independent options:

| Option | Default | Meaning |
|---|---|---|
| `client-mode single\|cluster` | `single` | Redis client semantics and DB range (16 DBs or DB0) |
| `meta-managed yes\|no` | `no` | Meta authority, control session and native replication |

Use the same values on the CLI (`--client-mode cluster --meta-managed yes`).
Configuration files load first; explicitly supplied CLI values override them.
Cluster without Meta and managed Single are rejected before storage preparation;
managed Single is not yet available. Non-Meta Single retains Redis/Redis Cluster
follower support through `replicaof`, `redis-replicaof`, `REPLICAOF`/`SLAVEOF`,
and `ADDREPLICAOF`. These entry points reject Lavik upstreams before retiring
existing subscriptions or replacing data. Lavik peers use Meta native Follow
Owner. A runtime probe failure leaves the old subscription intact; startup
network failures retry without opening service, and unsupported upstreams remain
fenced. Existing process-local Redis offsets support reconnect, not durable
cross-process resume or automatic Meta takeover.

**Configuration change:** `cluster-enabled` and the old `cluster-*` identity,
seed and announce options have been removed, without aliases. Use `client-mode`,
`meta-managed`, `meta-seed`, `node-id`, `announce-ip`, `announce-port` and
`announce-tls-port`. Meta-managed nodes require a stable 40-character lowercase
hex node ID and numeric Meta seed endpoints; they reject external upstreams and
`load-rdb`. TLS requirements are unchanged. Old generated launch scripts must be
regenerated or edited before restarting with this version.

Build the three cluster binaries and launch a local cluster:

```bash
./scripts/build_release.sh
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
