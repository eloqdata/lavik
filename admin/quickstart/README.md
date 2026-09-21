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
on one machine. Docker starts the processes; you initialize their cluster in
the Admin UI. Each Data node gets a persistent 1 GiB file.

This is a local learning environment. All containers share one physical host,
so it does not provide availability across machine failures. Meta/Data traffic
uses plaintext inside a dedicated Docker network; only the browser port is
published, bound to localhost. Meta/Data containers enable io_uring through
`seccomp=unconfined`; Admin does not need that setting. The example uses the
build-toolchain image to avoid a separate host compiler or Redis CLI install.
Use the [standalone Admin deployment guide](../../docs/operations/lavik-admin.md)
for remote access, credentials, and the production Admin image.

## 1. Prepare the new machine

Install Git and [Docker Desktop](https://docs.docker.com/desktop/) on a Mac,
or Docker Engine with the Compose plugin on Linux. Linux hosts need kernel
6.1 or newer with io_uring enabled. Allow space for the toolchain, native build,
and three 1 GiB Data files. The build uses two compiler processes at a time.

Obtain the Lavik source checkout **containing these Admin quick-start files**.
An older checkout or release will not contain this example; locally added
files must be copied or included in the branch checked out on the new machine.
The images below are built locally, not pulled from a published Lavik registry.

From the repository root:

```sh
git submodule update --init bycorf third_party/mimalloc
git -C bycorf submodule update --init third_party/liburing third_party/abseil

docker build -f admin/Dockerfile.toolchain -t lavik-admin-toolchain:local .
docker compose -f admin/quickstart/compose.yaml run --rm build
```

The first build downloads dependencies and compiles all three Linux binaries
inside Docker. It can take several minutes. Later builds reuse the build
volume. Finish this step before starting the services.

## 2. Start Admin and the nodes

```sh
docker compose -f admin/quickstart/compose.yaml up -d
docker compose -f admin/quickstart/compose.yaml ps
docker compose -f admin/quickstart/compose.yaml exec admin \
  cat /data/lavik-admin/token
```

If the token file is not ready immediately, wait a moment and repeat the last
command. Open **http://localhost:4173** and sign in with that token.

If port 4173 is occupied, run `export LAVIK_QUICKSTART_PORT=4183` before `up`
and use http://localhost:4183. Keep this variable set for subsequent Compose
commands. The example reserves Docker subnet `172.29.91.0/24`. If it overlaps
an existing network, choose another private subnet and change the addresses
together in `compose.yaml`, `start.sh`, and `cluster.toml` before first startup.

## 3. Initialize the cluster in the UI

1. Click **Connect cluster**.
2. Enter cluster name **demo**.
3. Enter Meta seeds **172.29.91.11:7200,172.29.91.12:7200,172.29.91.13:7200**.
4. Select the **default** profile and click **Connect cluster**.
5. Open **Topology → Initialize cluster**.
6. Paste the contents of [`cluster.toml`](cluster.toml). To print them:

   ```sh
   cat admin/quickstart/cluster.toml
   ```

7. Type **demo** in the confirmation field and click **Initialize cluster**.
8. Wait for the creation operation to complete and the Dashboard to report
   **Healthy**. Topology shows `group-1`, one primary, and two replicas.

Use the numeric Meta addresses above, not `localhost`: Admin runs inside
Docker. Port 7200 is the Meta Admin endpoint; port 6379 is the Data endpoint.

## 4. Verify data and the shared CLI catalog

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
  /build/lavik-ctl --socket /data/lavik-admin/admin.sock fleet-status demo
```

`GET` should return `hello from Lavik`, and `fleet-list` should include `demo`.
Client commands run inside the Docker network so cluster redirects can reach
the advertised node IPs. The Data ports are not published to the Mac host.

You can now use **Switch primary** to exercise controlled failover, browse
keys, and inspect the shared Operations view.

## 5. Stop and resume

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
