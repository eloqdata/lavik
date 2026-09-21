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

# Lavik Admin

Lavik Admin offers a browser workspace and `lavik-ctl` fleet commands backed
by the same persistent catalog. Each connected Meta deployment retains its
own authoritative cluster state and operation journal.

Run `lavik`, `lavik-meta`, and `lavik-ctl` from the same updated checkout.
Admin uses the Meta endpoint and operation-summary commands included here,
and repeated replica resizing needs the Data removal/rejoin fixes. Updating
the Admin image alone does not update separately deployed Meta or Data nodes.

## Run in Docker

To start a new local cluster as well as Admin, follow the
[three-node Docker quick start](../../admin/quickstart/README.md). It starts
one primary, two replicas, three Meta voters, and Admin, with persistent
volumes and browser-driven initialization. The commands below start the
standalone Admin service for connecting to separately deployed clusters.

Initialize the repository submodules and follow the prerequisites in the
[build guide](building-and-packaging.md). From the repository root:

```sh
docker build -f admin/Dockerfile.toolchain -t lavik-admin-toolchain:local .
docker compose -f admin/compose.yaml up -d --build
docker compose -f admin/compose.yaml exec admin cat /data/lavik-admin/token
```

Open <http://localhost:4173> and sign in with the printed token. The Compose
service binds to the local host only and runs as an unprivileged user. Its
named volume preserves connections, requests, and the token across container
replacement. It does not start or provision Data or Meta nodes.

Choose **Connect cluster**, enter a name and one or more numeric Meta Admin
addresses, and select a connection profile. These are the `--ctl-addr`
endpoints, not the Data client or Meta Raft ports. Every advertised Meta and
Data endpoint must be reachable from inside Admin. A loopback address names
the Admin container itself unless the processes share its network namespace.

For a source run alongside an installed `lavik-ctl`:

```sh
LAVIK_CTL=/path/to/lavik-ctl LAVIK_ADMIN_DATA=/private/path/admin \
  node admin/server.mjs
```

Use Node.js 24.15 or newer. The data directory must be mode 0700. Admin creates
its SQLite database, mode-0600 token file, and mode-0600 `admin.sock` there.
No npm installation is needed for the service itself.

## Shared command-line workspace

Run the CLI on the Admin host, through SSH, or inside its container:

```sh
lavik-ctl --socket /private/path/admin/admin.sock \
  fleet-add production 10.0.0.11:7200,10.0.0.12:7200 production-mtls
lavik-ctl --socket /private/path/admin/admin.sock fleet-list
lavik-ctl --socket /private/path/admin/admin.sock fleet-status production
lavik-ctl --socket /private/path/admin/admin.sock fleet-operations production
```

Fleet replies are `OK` followed by JSON; failures are `ERR` followed by JSON.
The catalog is server-owned. Both interfaces immediately see additions from
the other interface. `fleet-status` reports live Meta state, and
`fleet-operations NAME [AFTER_SEQUENCE]` joins saved Admin requests with up to
100 Meta live-journal summaries. The returned `next` cursor selects another
page. Meta's archived operation summaries remain available through its
existing export procedures.

Direct cluster commands remain available. Against the current Meta leader:

```sh
lavik-ctl --addr 10.0.0.11:7200 listops 0 100
lavik-ctl --addr 10.0.0.11:7200 getop OPERATION_ID
lavik-ctl --addr 10.0.0.11:7200 getgroup GROUP
```

`listops AFTER LIMIT` returns `OK listops-v1`, followed by space-separated
`id:sequence:lifecycle:kind_hex:phase_hex:result_hex` entries. Limit is 1–100;
phase/result previews are at most 512 bytes. The operation sequence is the
immutable submit index. `getgroup` returns the membership revision, term,
Owner, and active-transition flag. `getnode` includes tagged client endpoints.
These metadata views let external tools reuse the same administration surface.

## Connection profiles and remote access

The default profile permits plaintext Meta connections for a trusted local
network. For mTLS, create a private JSON file on the Admin host:

```json
{
  "production-mtls": {
    "ca": "/run/secrets/meta-ca.crt",
    "cert": "/run/secrets/operator.crt",
    "key": "/run/secrets/operator.key",
    "dataCa": "/run/secrets/data-ca.crt",
    "dataCert": "/run/secrets/data-client.crt",
    "dataKey": "/run/secrets/data-client.key",
    "password": "DATA_PASSWORD_IF_CONFIGURED"
  }
}
```

Set `LAVIK_ADMIN_PROFILES` to that file and mount it and its referenced files
read-only in a container. Data credentials are optional according to the
deployment. Keep the profile file private; its credential contents are never
stored in the catalog or returned to the browser. Meta certificates follow
the [Meta authentication guide](meta-control-plane.md).
Profiles containing Data TLS files require `tls://` Data endpoints. Set
`dataTlsRequired: true` when using TLS with the system CA bundle. Neither Meta
nor Data TLS configuration silently falls back to plaintext.

For remote browser access, terminate HTTPS at a reverse proxy, restrict access
to the Admin service, and set `LAVIK_ADMIN_ORIGIN` to its exact public origin,
for example `https://admin.example.com`. This also enables Secure cookies.
`LAVIK_ADMIN_BIND` and `LAVIK_ADMIN_PORT` select the internal listener. The
access token grants operator access to all registered clusters; there is no
per-user RBAC. Rotate the token file and restart Admin to invalidate sessions.
Do not mount a Docker socket into the production Admin container.

## Create and resize clusters

To initialize a new cluster, first start its Meta members and Data nodes using
the same manifest and fresh Data files as described in the
[cluster deployment guide](cluster-deployment.md). Connect the Meta endpoint,
choose **Initialize cluster**, paste the manifest, and confirm the cluster
name. The service invokes `lavik-ctl cluster-create`. Initialization replaces
the listed Data populations and is available only for an uninitialized Meta
deployment. Different manifests can define different numbers of groups,
replicas, and Meta members.

To add a replica, start a Data process with a fresh file, its stable
40-character node ID, and the cluster's Meta seeds. In **Topology**, choose
**Add replica** for an existing group and enter the ID and tagged client
endpoint. The CLI equivalent is:

```sh
lavik-ctl --socket /private/path/admin/admin.sock \
  fleet-replica-add production group-1 NODE_ID tcp://10.0.1.15:6379
```

The operation registers and assigns the node, then waits for current
population, projection, and health observations. Accepted membership is not
reported as completed synchronization.

Choose **Remove** on a replica to shrink the group's redundancy, or run:

```sh
lavik-ctl --socket /private/path/admin/admin.sock \
  fleet-replica-remove production group-1 NODE_ID
```

Removal checks the reviewed membership revision and rejects the current
Owner or an active failover. It retains the host and data files. Completion
means the node is unassigned and remaining group members have converged; the
removed node is not counted in readiness. A later re-add creates a new
membership incarnation and synchronizes the population again.

For a primary change, use **Switch primary** or:

```sh
lavik-ctl --socket /private/path/admin/admin.sock fleet-failover production group-1
```

Meta selects an eligible Candidate and owns the controlled failover. Direct
`lavik-ctl failover` operations also appear in the browser's **Operations**
view. Automation can retain an exact failover identity before sending:

```sh
lavik-ctl failover group-1 --addr 10.0.0.11:7200 \
  --tls-ca /path/ca.crt --tls-cert /path/operator.crt --tls-key /path/operator.key \
  --operation-id HEX32 --deadline-unix-ms ABSOLUTE_DEADLINE
```

Both options must be supplied together and retained unchanged for a retry.

Replica resizing does not change primary-group count or redistribute slots.
Online primary-group expansion/shrink requires a data-migration protocol that
Lavik does not currently implement; the UI does not offer it as an available
action. Provisioning machines, Meta-member resizing in the browser, hot-key
tracking, `COMMANDLOG`, and per-key memory analysis are also outside the
current Admin feature set. Existing Meta membership CLI procedures remain
available.

## Operation recovery and backups

Admin persists intent before sending mutations. A timeout or service restart
may leave an **Uncertain** request; inspect its Meta operation and live
topology. Admin automatically observes outcomes and never blindly repeats a
possibly committed mutation. **Retry original request**, or
`fleet-resume NAME OPERATION_ID`, preserves the original identity, deadline,
and removal revision. An expired deadline or changed membership requires a
new reviewed request.
Creation requests are observed through their retained Meta root and cannot be
resubmitted with the generic retry control.

An idle uncertain replica request can be abandoned with **Abandon request**
or `fleet-abandon NAME OPERATION_ID`. This terminalizes its generic Meta
operation and retains already committed membership changes. It does not undo
an assignment or restore a removed replica. Creation and controlled failover
remain Meta-owned and must be diagnosed through their normal workflows.

Keep the Admin persistent volume as well as every cluster's Meta/Data state.
For a consistent simple backup, stop Admin, copy its complete private data
directory including SQLite sidecar files, then restart it. Restore the directory
with its original permissions. Do not edit the database directly or run two
Admin instances against one directory. Stopping Admin does not stop Data or
Meta; an already committed membership change continues converging there.

## Local Mac verification with Docker

The test toolchain runs ARM64 Linux on Docker Desktop and compiles current
source. Its isolated container needs `seccomp=unconfined` for Linux io_uring;
the production Admin image itself does not need that permission.

```sh
docker build -f admin/Dockerfile.toolchain -t lavik-admin-toolchain:local .
docker run -d --name lavik-admin-dev --security-opt seccomp=unconfined \
  --mount type=bind,src="$PWD",dst=/src,readonly \
  --mount type=volume,src=lavik-admin-build,dst=/build \
  --mount type=volume,src=lavik-admin-testdata,dst=/data \
  -p 127.0.0.1:4173:4173 lavik-admin-toolchain:local sleep infinity
docker exec lavik-admin-dev cmake -S /src -B /build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DLAVIK_ENABLE_OPT=OFF -DBUILD_TESTING=ON
docker exec lavik-admin-dev cmake --build /build \
  --target lavik lavik-meta lavik-ctl -j4
docker exec lavik-admin-dev python3 /src/admin/test/cluster-fixture.py
docker exec -d -e LAVIK_ADMIN_DATA=/data/admin-workspace \
  -e LAVIK_ADMIN_BIND=0.0.0.0 -e LAVIK_CTL=/build/lavik-ctl \
  lavik-admin-dev node /src/admin/server.mjs
```

Wait for `/data/admin-workspace/admin.sock`, then register the fixtures:

```sh
docker exec lavik-admin-dev /build/lavik-ctl \
  --socket /data/admin-workspace/admin.sock fleet-add alpha 127.0.0.1:8200
docker exec lavik-admin-dev /build/lavik-ctl \
  --socket /data/admin-workspace/admin.sock fleet-add beta 127.0.0.1:8600
cd admin
npm ci
npx playwright install chromium
npm test
LAVIK_ADMIN_TEST_DATA=/data/admin-workspace npm run test:browser
```

The suite exercises two cluster sizes, browser/CLI catalog sharing,
key inspection, escaped string editing, repeated replica addition/removal,
primary-removal protection, controlled failover through both interfaces, data
preservation, and mobile navigation. It uses real Meta and Data processes.

To include browser initialization, start a third fresh deployment before the
first browser run, then set the manifest environment variable. Run the Docker
command from the repository root and npm from `admin/`:

```sh
docker exec -e LAVIK_ADMIN_FIXTURE_EMPTY=1 lavik-admin-dev \
  python3 /src/admin/test/cluster-fixture.py /data/admin-create-fixture
LAVIK_ADMIN_TEST_DATA=/data/admin-workspace \
  LAVIK_ADMIN_CREATE_MANIFEST=/data/admin-create-fixture/gamma/cluster.toml \
  npm run test:browser
```

Initialization is intentionally a one-time test against fresh files. Omit that
environment variable on repeat runs. The fixture launcher refuses to reuse an
existing directory. Retained fixture logs and PIDs live under the selected
fixture root; browser failure traces and screenshots live in
`admin/test-results/`. After browser initialization, verify the native protocol
against gamma as well:

```sh
docker exec -e LAVIK_ADMIN_TEST_DATA=/data/admin-workspace lavik-admin-dev \
  python3 /src/admin/test/protocol-smoke.py
```

This checks native pagination, endpoints, primary-removal and revision guards,
replica convergence, rejected-handshake closure, and preservation of the test
key. Stop just these test processes with:

```sh
docker exec lavik-admin-dev python3 /src/admin/test/cluster-fixture.py \
  --stop /data/admin-fixture
docker exec lavik-admin-dev python3 /src/admin/test/cluster-fixture.py \
  --stop /data/admin-create-fixture
```

Stopping fixtures preserves their data for diagnosis. Never start another
fixture on the same ports while these processes are still running.

The stop helper targets only PIDs recorded by that fixture, verifies their
command paths, and terminates stalled disposable processes after a grace
period. Volumes and diagnostic logs are retained.
