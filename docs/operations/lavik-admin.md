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

For a first hands-on run on macOS, follow
[Lavik Admin 101: Docker hosts to a running cluster](lavik-admin-mac-101.md).
It includes exact SSH preparation, form values, node mapping, and client ports.

## Start from a release

Extract any future nightly or tagged Linux release archive and run:

```sh
./lavik-admin
```

Open <http://localhost:4173> and sign in with the token printed in the terminal.
Linux archives include Node.js and require no npm installation. On macOS,
install Node.js 24.15+ and use the same launcher; Data, Meta, and their release's
`lavik-ctl` run on the Linux hosts over SSH. Admin itself needs local OpenSSH
(`ssh` and `scp`). Previously published archives are not modified retroactively.
The archive includes a standalone `LAVIK-ADMIN.md` quick start.

The workspace defaults to `~/.local/share/lavik-admin` (or
`$XDG_DATA_HOME/lavik-admin`). Set `LAVIK_ADMIN_DATA` to use a different private
directory and `LAVIK_ADMIN_PORT` to change the default 4173. Keep Admin running
while using its browser or fleet CLI. You can close it without stopping clusters.

## Prepare SSH hosts and create a cluster

Use existing Linux AMD64 or ARM64 hosts with Python 3, glibc 2.39+, Linux 6.1+,
and io_uring enabled. Ubuntu 24.04 satisfies the userspace requirement. The
Admin computer needs HTTPS access to GitHub; hosts receive verified binaries
over SSH and do not need to download releases themselves.

Choose **Create cluster** to pick **Local demo**, **Set up machines**, or
**Connect existing cluster**. The local path shows the single command
`./admin/quickstart/setup.sh`; see the [Docker quick start](../../admin/quickstart/README.md).
Production setup follows three stages:

1. **Prepare hosts.** Paste one `SSH_HOST[:PORT]` per line, optionally followed
   by its private cluster IP. Set the SSH user and choose **Password** or
   **Private key / SSH agent**. A private-key path refers to the computer
   running Admin; encrypted keys accept a temporary passphrase. Click
   **Prepare hosts** to install Admin's public key and verify fresh key-only
   access. Use separate batches for different logins. Existing authorized keys
   remain intact. First connections trust and save the server's host key;
   changed keys are rejected. Passwords/passphrases are never saved or logged.
   Select prepared hosts and click **Continue to node placement**; saved hosts
   are rechecked and can be reused across clusters without the initial login.
2. **Node placement.** Name the cluster, choose `nightly` or a published GitHub
   release, and set numeric private cluster IPs for the selected hosts. These
   addresses must be bound on the hosts and reachable between them; they may
   differ from the SSH address behind Docker port mappings. Defaults give three
   Meta voters, one primary, and two followers. The table explicitly maps every
   voter/primary/follower to a host; each row has a selector. A host may run
   both Meta and Data. Use separate hosts for redundancy. Changing node counts
   or the selected host count resets placement to even spreading.
   **Tune advanced settings** exposes workers, file size, storage directory,
   base ports and client mode. **Service lifecycle** is shown directly in Node
   placement: choose systemd for persistent hosts or development processes for
   Docker labs. Initial setup supports up to 64 Data nodes and 1, 3, or 5
   Meta voters.
3. **Review & deploy.** **Check hosts & review** checks Python, kernel/libc,
   io_uring, service management, storage, and ports. Resolve reported issues,
   inspect placement and ports, type the cluster name, and choose **Deploy
   cluster**. **Operations** tracks installation, initialization, and readiness.
   The dashboard provides metrics, commands, topology, and slow
   logs. Repeat setup for additional independent clusters in this workspace.

The version field lists releases and accepts exact tags. Discovery uses the
public `github.com/eloqdata/lavik/releases` page and checksummed Linux
packages (minimal for files, standard for SPDK), with a short metadata cache.
It does not use the rate-limited REST API. GitHub page/asset access is still required.

For ordinary Docker lab containers, select **Node placement → Service
lifecycle → Development processes (no automatic restart)** and run **Check
hosts & review** again. If checks report that systemd is unavailable,
**Use development processes & recheck** makes that selection and runs a fresh
review. The lifecycle changes only when you choose it. These containers do not run systemd; enabling lingering
cannot fix that. Process mode does not restart nodes after crashes or reboot.

Default supervision uses systemd user services. On hosts running systemd, an
administrator must enable lingering once for the selected SSH user, then reconnect:

```sh
sudo loginctl enable-linger "$USER"
```

Admin does not change sudo or SSH server authentication policy. Password
onboarding requires that the host already allows that user's password login;
use an existing private key otherwise. Key-only login must work without MFA
or an interactive shell prompt. Use the local browser or a configured HTTPS
reverse proxy when entering credentials. The protected workspace owns
`ssh/identity/id_ed25519` and `ssh/known_hosts`; back them up with the catalog.
Prepare-host SSH connections go directly to the entered hostname and port.
**Admin SSH access verified** means Admin can log in using its workspace key.
A plain terminal `ssh USER@HOST` may still prompt for a password because it
uses your usual SSH identities. For a manual login, select Admin's key with
`ssh -i /PATH/TO/ADMIN_WORKSPACE/ssh/identity/id_ed25519 -p PORT USER@HOST`.
The workspace is the running Admin process's `LAVIK_ADMIN_DATA` value, or
`~/.local/share/lavik-admin` by default; the lab's password-file directory
alone does not select the Admin workspace.

Base ports default to 6379 (Data), 7100 (Meta Raft), 7200 (Meta Admin), and
7300 (Data control). Each host IP's first Data node uses the Data base port;
additional Data nodes on that IP increment it. Thus one Data node per host uses
6379 on every host, even with custom placement or a different host-list order.
Meta's three port ranges increment by voter index. Open reviewed ports between
hosts. Existing deployment plans retain their original ports.
Multiple clusters on the same hosts need nonoverlapping ports and names.

Provisioning uses kernel networking and plaintext cluster traffic on the
private network. File storage uses io_uring and minimal packages; SPDK uses
standard packages and the dedicated-host setup below. SSH protects management access; it
does not automatically configure cluster TLS, firewall rules, cloud machines,
or OS packages. Use the connection-profile flow below for existing TLS clusters.
Admin data tools for SSH deployments reach private Data addresses through SSH;
your Mac does not need direct access to Docker's Linux bridge IPs. External
clients still need their own route or explicit published ports.

Admin installs under `~/.local/share/lavik/clusters/NAME` on each host by default.
Each directory carries ownership metadata, the pinned binaries, original
manifest, and per-node state. Existing unowned directories and conflicting
configuration are refused; existing data files are never overwritten or resized.
Systemd units are named `lavik-NAME-meta-1.service`, `lavik-NAME-data-1.service`,
and so on. Inspect a service on its host with:

```sh
systemctl --user status lavik-NAME-data-1.service
journalctl --user -u lavik-NAME-data-1.service
```

The **Development processes** lifecycle option supports lab containers without
systemd. It writes `console.log` and process identities in each node directory
but does not restart processes after a crash or reboot. Use systemd for ongoing
host deployments.

Released clients run with their matching Meta/Data binaries. Older releases
such as `v0.1.0-beta.1` can be created and inspected; their older APIs do not
support Admin's safe follower resizing and controlled failover. Those controls
are disabled. Select a current release for the full management workflow.

### SPDK on dedicated Data hosts

In Node placement, choose **SPDK · dedicated NVMe controllers** and enter one
`spdk://DOMAIN:BUS:DEVICE.FUNCTION/NAMESPACE_ID` URI for every host assigned a
Data node. Admin resolves the standard release; minimal packages cannot provide
SPDK. Use one Data node and one dedicated, single-namespace NVMe controller per
physical host. Automated setup requires root SSH, systemd, an isolated IOMMU
group, 2 MiB hugepages, `lsblk`, `wipefs`, `fuser`, `modprobe`, `mount`, and
`findmnt`. Root accounts hosting Meta still need the systemd user manager and
lingering required by ordinary Meta services. Standard release runtime libraries
must be installed; executable compatibility is checked before device binding.

Read [SPDK storage](spdk-storage.md) when choosing media. Host checks reject
partitions, filesystems, mounts, swap, holders, open users, nonempty storage
headers, missing IOMMU isolation, insufficient memory, and unsafe no-IOMMU mode.
Admin does not erase media. The review shows each controller and serial number;
confirm dedicated-controller binding as well as the cluster name to deploy.
The serial is rechecked immediately before configuration. Existing controllers
already bound outside this deployment cannot be adopted automatically.

Admin claims the host/controller under `/var/lib/lavik-admin/spdk/`, reserves
hugepages without reducing an existing pool, mounts `/dev/hugepages`, and binds
only the selected controller to `vfio-pci`. The retained
`lavik-CLUSTER-spdk.service` reapplies configuration at boot. SPDK Data nodes use
system services with unlimited memlock and a dependency on that setup service;
Meta keeps its normal user service. The EAL allowlist and memory budget are
written into each Data launcher. Other instances must not share this host’s
SPDK resources. This automated path supports new clusters; add later SPDK
followers through **Use an already-running node** after preparing their hosts.

If setup stops, inspect the reported host error and use Admin’s resume action.
Claims, configurations, and existing populations are retained. A partial driver
transition can require host repair before resume; Admin never resets drivers
or releases hugepages automatically. Before manual decommissioning, stop Data
and disable both the Data and SPDK setup services, then follow the storage
runbook to restore drivers. Keep controller claims until the storage is no
longer owned by this cluster. Hardware replacement requires a new review.

### Monitoring hosts

Select one or more prepared hosts in **Monitoring**. Each runs an independent
Prometheus/Grafana stack scraping every Data node. Install Docker Engine and
Compose v2, enable Docker startup at boot, and grant the SSH account Docker
access. Images must be cached or reachable from those hosts. Admin uploads the
repository’s pinned monitoring Compose configuration and dashboard. Data metrics
ports start at 9100 on each IP and increment for colocated nodes; the review
checks for conflicts. **Monitoring ports** lets you change the metrics base port,
Grafana port, and Prometheus port. Restrict unauthenticated metrics ports to monitoring hosts.

Grafana binds to the selected private IP, defaulting to port 3000. The Dashboard links to
it after deployment. Sign in as `admin`; read the unique generated password
from `BASE_DIR/CLUSTER/monitoring/grafana-password` on that host. This password
is not returned to the browser or stored in the fleet database. Prometheus
defaults to `127.0.0.1:9090` on the monitoring host. For access outside the private
network, provide your own TLS reverse proxy and access controls.

Deployment waits for Grafana and Prometheus and verifies all Prometheus scrape
targets after Meta reports the created cluster ready, because Data listeners
open only after Genesis. A monitoring failure leaves post-creation verification
resumable; resuming never submits cluster creation again. Automatic file-backed
follower additions update all monitoring target lists. Membership removal leaves the running process in the scrape list, matching
Admin’s existing behavior of retaining its service and storage. Inspect and
manage the generated stack with `docker compose --project-name lavik-CLUSTER
--project-directory BASE_DIR/CLUSTER/monitoring -f
BASE_DIR/CLUSTER/monitoring/compose.yaml ...`. Retain its named volumes for history.

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
replacement. It does not run Data or Meta locally. To use SSH deployment from this
container, use **Prepare hosts** with a login password, or mount a bootstrap
private key/agent socket for its unprivileged user. Paths refer to the
container's filesystem; its persistent workspace retains the managed SSH key. Never mount the Docker socket for host provisioning.

Choose **Connect existing cluster**, enter a name and one or more numeric Meta Admin
addresses, and select a connection profile. These are the `--ctl-addr`
endpoints, not the Data client or Meta Raft ports. Every advertised Meta and
Data endpoint must be reachable from inside Admin. A loopback address names
the Admin container itself unless the processes share its network namespace.
Click **Test connection** to discover lifecycle, topology counts, and readiness;
then **Connect cluster** to save the tested connection. Commas, whitespace,
newlines, and `tcp://`/`tls://` endpoint prefixes are accepted by the form.
Changing an input requires another test. An unreachable endpoint leaves the
form intact and does not create a catalog entry. An initialized cluster is
never initialized again by this connection flow. `lavik-ctl cluster-create`
does not register a fleet entry automatically; `lavik-ctl fleet-add` does.

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

For release users, `./lavik-admin ctl` uses the same socket and also runs on
macOS. Run `./lavik-admin ctl fleet-hosts` to list hosts prepared in the UI.
To reuse them in a deployment, save `setup.json` with their returned IDs:

```json
{
  "id": "production",
  "release": "nightly",
  "hosts": [
    {"hostId": "PREPARED_HOST_ID_1", "address": "10.0.0.11"},
    {"hostId": "PREPARED_HOST_ID_2", "address": "10.0.0.12"},
    {"hostId": "PREPARED_HOST_ID_3", "address": "10.0.0.13"}
  ]
}
```

Unspecified settings use the wizard defaults. Existing automation may still
supply full SSH host settings (`host`, `user`, optional `port`, `identityFile`,
`knownHostsFile`) after establishing passwordless access and host trust itself.
Then review and submit:

```sh
./lavik-admin ctl fleet-releases
./lavik-admin ctl fleet-plan ./setup.json
./lavik-admin ctl fleet-deploy REVIEW_TOKEN production
./lavik-admin ctl fleet-operations production
```

`fleet-plan` checks hosts without installing files and returns a 15-minute
review token. SPDK plans additionally require the explicit acknowledgement
`fleet-deploy REVIEW_TOKEN production confirm-spdk` after reviewing controllers
and serials. A restart expires previews; accepted operations and plans remain
durable. Native `lavik-ctl` accepts the same commands, with base64url-encoded
JSON as the `fleet-plan` argument instead of a filename. The launcher helper
performs that encoding. Use `fleet-follower-plan NAME ./follower.json` with
`{"group":"group-1","host":{"host":"10.0.0.14","user":"ubuntu"},"dataPort":6379}`
and then `fleet-follower-deploy NAME REVIEW_TOKEN` for a new follower.

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

## Initialize existing deployments and resize followers

To initialize processes started outside Admin, first start its Meta members and Data nodes using
the same manifest and fresh Data files as described in the
[cluster deployment guide](cluster-deployment.md). Connect the Meta endpoint,
choose **Initialize cluster**, paste the manifest, and confirm the cluster
name. The service invokes `lavik-ctl cluster-create`. Initialization replaces
the listed Data populations and is available only for an uninitialized Meta
deployment. Different manifests can define different numbers of groups,
replicas, and Meta members.

For a cluster deployed by Admin, choose **Topology → Add replica → Deploy on
a host**. Select a prepared host (or prepare a new host), continue to placement,
choose an unused Data port, check prerequisites, and confirm.
The new follower inherits the cluster's exact release, workers, file size, and
service lifecycle. A host already in the cluster keeps its saved SSH user and
cluster IP. Selecting its prepared inventory entry can replace legacy login
paths with Admin’s managed key after host checks succeed.
Admin waits for native population/projection/health convergence before marking
addition complete.

To attach an already-running replica, start a Data process with a fresh file, its stable
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
Owner or an active failover. It retains the process, host, and data files.
After completion, stop and disable an unused provisioned service separately
with `systemctl --user disable --now lavik-NAME-NODE.service` on its host. Completion
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
Cluster creation is observed through its retained Meta root. For SSH setup,
**Retry original request** can continue interrupted installation/service startup
using the same owned paths and node IDs. After the creation checkpoint it only
observes Meta; it never resubmits an uncertain Genesis. Diagnose Meta startup
failures through the per-node service logs. No automatic rollback deletes host
files or stops services when a deployment is interrupted.

An idle uncertain replica request can be abandoned with **Abandon request**
or `fleet-abandon NAME OPERATION_ID`. This terminalizes its generic Meta
operation and retains already committed membership changes. It does not undo
an assignment or restore a removed replica. Creation and controlled failover
remain Meta-owned and must be diagnosed through their normal workflows.

Keep the Admin persistent volume as well as every cluster's Meta/Data state.
The `releases/` cache retains verified archives by digest, so a follower can
use the original nightly after its GitHub assets have moved. Do not delete
archives still referenced by deployments. Only used host architectures are
necessarily cached; adding a different architecture after a nightly moves
requires the original checksummed archive in the cache. Loss of that archive
must fail safely rather than deploy a different binary. Include the managed
`ssh/` identity and trust store in workspace backups. Legacy plans referencing
external SSH files also need protected backups of those files.
For a consistent simple backup, stop Admin, copy its complete private data
directory including SQLite sidecar files, then restart it. Restore the directory
with its original permissions. Do not edit the database directly or run two
Admin instances against one directory. Stopping Admin does not stop Data or
Meta; an already committed membership change continues converging there.

## Local Mac verification with Docker

The SSH provisioning test runs Admin natively on the Mac and creates three
isolated Ubuntu SSH containers. It tests password/private-key/encrypted-key
onboarding, preserved authorized keys, credential-free persistence, and changed
host-key rejection. It downloads official nightly and beta binaries,
creates two independent clusters, checks data/metrics, provisions and removes
a follower, changes primary, checks shared CLI state, restarts Admin, and can
complete a third deployment in Chromium. Only its own temporary containers,
network, keys, and workspace are removed on exit.

```sh
docker build -f admin/test/ssh/Dockerfile -t lavik-admin-ssh-test:local .
(cd admin && npm ci && npx playwright install chromium)
node --test admin/test/*.test.mjs
python3 admin/test/remote_test.py
LAVIK_ADMIN_TEST_BROWSER=1 node admin/test/ssh-smoke.mjs
```

`LAVIK_ADMIN_TEST_RELEASE_CACHE` optionally retains downloads across runs.
`LAVIK_ADMIN_KEEP_TEST=1` retains the isolated fixture for diagnosis. Containers
use development-process supervision; remote helper tests cover the generated
systemd service and pristine-versus-recovered Meta startup contract. Docker
verification does not claim a full host reboot/systemd integration test.

The source-build suite below additionally exercises direct, non-SSH connections.


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
command execution, repeated replica addition/removal,
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
