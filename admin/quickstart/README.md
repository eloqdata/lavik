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

# Three-node Docker quick start

This example starts **three Lavik Data nodes: one primary and two replicas**.
It also starts three Meta voters and Lavik Admin, for seven running containers
on one machine. The setup command builds the Linux executables, starts the services,
registers `demo-cluster` in Admin, and initializes it through a durable Admin
job. Each Data node gets a persistent 1 GiB file.

This is a local learning environment. All containers share one physical host,
so it does not provide availability across machine failures. Meta/Data traffic
uses plaintext inside a dedicated Docker network; the browser port and three
Data client ports are published only on localhost. Meta/Data containers enable io_uring through
`seccomp=unconfined`; Admin does not need that setting. The example uses the
build-toolchain image to avoid a separate host compiler or Redis CLI install.
Use the [standalone Admin deployment guide](../../docs/operations/lavik-admin.md)
for remote access, credentials, and the production Admin image.

## 1. Set up everything

Install Git and Docker Desktop on macOS, or Docker Engine with Compose v2 on
Linux. Linux needs kernel 6.1+ with io_uring enabled. From a Lavik source
checkout, run:

```sh
./admin/quickstart/setup.sh
```

This initializes the required source submodules, builds the toolchain and Linux
binaries in Docker, starts Compose, creates **demo-cluster**, and waits for it
to become ready. The first build can take several minutes; allow disk space for
the toolchain, build cache, and three 1 GiB Data files. Subsequent builds reuse
the cache. The command prints the Admin URL and sign-in token when ready.

Open **http://localhost:4173**, sign in, and select **demo-cluster**. There is
no manifest to paste and no separate initialization step. The same command can
be rerun: it retains volumes and observes existing creation jobs instead of
replaying an uncertain creation. If initialization needs attention, inspect
Admin’s Operations view and Compose logs before resuming.

Set `LAVIK_QUICKSTART_PORT=4183` before running if the default Admin port is
occupied. Keep it set for later Compose commands. The example reserves Docker
subnet `172.29.91.0/24`; if it overlaps another network, change the addresses in
`compose.yaml`, `start.sh`, and `cluster.toml` together before first startup.
Bootstrap discovers Meta Admin seeds from the manifest.

The one-command entry point uses the same individual build and Compose
operations as a manual run:

```sh
docker build -f admin/Dockerfile.toolchain -t lavik-admin-toolchain:local .
docker compose -f admin/quickstart/compose.yaml run --rm build
docker compose -f admin/quickstart/compose.yaml up -d
docker compose -f admin/quickstart/compose.yaml exec -T admin node quickstart/bootstrap.mjs
```

## 2. Verify data and the shared CLI catalog

In Admin's **Send command** view, run `SET greeting "hello from Lavik"`, confirm
the write, then run `GET greeting`. The same operations work from Docker:

```sh
docker compose -f admin/quickstart/compose.yaml exec data-1 \
  redis-cli -c -h 172.29.91.21 -p 6379 SET greeting 'hello from Lavik'
docker compose -f admin/quickstart/compose.yaml exec data-2 \
  redis-cli -c -h 172.29.91.22 -p 6379 GET greeting

docker compose -f admin/quickstart/compose.yaml exec admin \
  /build/lavik-ctl --socket /data/lavik-admin/admin.sock fleet-list
docker compose -f admin/quickstart/compose.yaml exec admin \
  /build/lavik-ctl --socket /data/lavik-admin/admin.sock fleet-status demo-cluster
```

`GET` should return `hello from Lavik`, and `fleet-list` should include `demo-cluster`.
Client commands run inside the Docker network so cluster redirects can reach
the advertised node IPs.

### Connecting from the Mac host

Docker Desktop keeps `172.29.91.*` inside its Linux VM. A host command such as
`redis-cli -h 172.29.91.21 -p 6379` cannot reach that private network. Use these
published addresses for direct node access instead:

| Node | Inside Docker | On the host |
|---|---|---|
| `data-1` | `172.29.91.21:6379` | `127.0.0.1:16379` |
| `data-2` | `172.29.91.22:6379` | `127.0.0.1:16380` |
| `data-3` | `172.29.91.23:6379` | `127.0.0.1:16381` |

```sh
redis-cli -h 127.0.0.1 -p 16379 PING
```

For keyed reads and writes, choose the **current primary** shown in Admin's
Topology view. Its host port follows the table above; failover can change
which node is primary. For example, when `data-2` is primary:

```sh
redis-cli -h 127.0.0.1 -p 16380 GET greeting
```

Use the Docker-based `redis-cli -c` commands above for automatic redirects.
Host port publishing does not rewrite `MOVED` replies or `CLUSTER SLOTS`:
they still contain the internal addresses required by Meta, Data, and Admin.
Enabling `-c` in a Mac-hosted client can therefore hang after a redirect to
another node. An application running on the host needs explicit endpoint
mapping support in its cluster client, or should run inside this Docker
network. The selected ports avoid an existing service on host port 6379.

If updating an already-running quick start, applying these new port mappings
recreates the Data containers. Retain their volumes and recreate replicas
before the current primary, waiting for **Healthy** between changes:

```sh
docker compose -f admin/quickstart/compose.yaml up -d --no-deps data-1
```

Repeat for the other nodes in the appropriate order for their current roles.

You can now use **Switch primary** to exercise controlled failover, browse
keys, and inspect the shared Operations view.

## 3. Stop and resume

```sh
docker compose -f admin/quickstart/compose.yaml down
docker compose -f admin/quickstart/compose.yaml up -d
```

Volumes retain the cluster, data, Admin catalog, and token. Sign in again and
wait for **Healthy**; do not initialize the cluster again. The launcher omits
Meta's bootstrap-only manifest option when restarting existing state and
never truncates existing Data files. Do not add `-v` to `down` if you want to
keep the data; that flag deletes the named volumes.

For diagnostics:

```sh
docker compose -f admin/quickstart/compose.yaml logs --tail=100 admin meta-1 data-1
```
