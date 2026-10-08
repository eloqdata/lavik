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

# Lavik Admin and fleet management

Lavik Admin is a separate Node.js service that owns a persistent fleet catalog,
operator request tracking, SSH deployment plans, browser sessions, and the HTTP UI. A private Unix
socket accepts `lavik-ctl` fleet commands. Both entry points invoke one
application boundary and one database; neither the browser nor the CLI keeps
an independent cluster list.

Each Meta Raft deployment continues to own exactly one Data cluster. The
Admin fleet catalog associates a human-readable name with numeric Meta seeds
and a server-side connection profile. It does not merge separate clusters'
Raft stores or become a source of Data serving authority.

```text
browser -- HTTP + session cookie --+
                                  |
lavik-ctl -- private Unix socket --+--> Fleet application
                                        |         |
                               SQLite worker      +-- lavik-ctl subprocess
                               catalog / requests       |
                               deployment plans    direct or SSH transport
                                                        |
                                                  Meta Admin leader
                                                  topology / operations
                                        |
                                        +-- bounded RESP, direct or SSH --> Data
                                        +-- release cache / SSH helper --> host services
```

## State and ownership

A dedicated worker exclusively owns the SQLite connection. WAL with full
synchronization stores the versioned cluster catalog, request records, and
administrative audit metadata. Schema version 3 stores a reusable prepared-host
inventory alongside per-cluster deployment plans; version-1 and version-2
catalogs migrate without changing connections or requests. Inventory entries
retain SSH endpoints, managed key/trust paths, and the last successful key-only
verification time. They carry no node role or cluster ownership. Plans retain official release URLs and SHA-256 digests, host addresses
and SSH file paths, node identities, placement, storage settings, and an
ownership nonce. Keys and passwords are not stored in these plans. A creation
transaction admits the catalog entry, plan, and initial job together. Follower
admission compares the reviewed plan and updates it in the same transaction
as its job, preserving the one-active-job invariant. Disk synchronization does not block HTTP or
network processing. One private Unix socket per data directory prevents two
Admin processes from owning the same fleet workflow executor. The database
is an operator-service durability boundary, not a replicated consensus store;
its persistent volume requires an operational backup.

Meta remains authoritative for committed cluster lifecycle, membership,
authority, and operation outcomes. The service invokes the actual `lavik-ctl`
binary without a shell, reusing its discovery, TLS verification, deadlines,
and wire validation. Connected clusters use a local client. SSH-deployed
clusters invoke the client from their own retained release on a Meta host;
Admin can therefore run on macOS without executing Linux binaries locally.
Only explicitly classified read commands may retry through another SSH host.
A transport failure never authorizes replay of a mutation. Leader reads expose node endpoints, group membership
revision, and a bounded page of operation summaries. Summaries exclude opaque
intent and directive payloads; their previews are bounded independently of
retained operation size. Meta's live operation journal also includes work
initiated by a direct CLI client; archived records follow existing Meta
retention/export procedures.

## Host deployment and release lifecycle

The deployment coordinator discovers versions from the public GitHub releases
page and resolves minimal packages for file storage or standard packages for
SPDK through its public asset fragment and matching published SHA-256 files. Discovery does not depend on REST API quota.
Only same-repository release links are parsed; HTML is never served to the UI,
and missing/changed metadata fails closed. Short-lived metadata requests are
cached and coalesced; installed bytes remain pinned by digest.

One placement model drives the browser's live node table and the server's
reviewed topology. Default placement spreads Meta and Data independently across
hosts; explicit placement assigns every node to a validated host index before
identities, ports, and the immutable manifest are generated. New plans reuse
one Data base port on each advertised IP and increment it for additional Data
nodes sharing that IP, independent of host-list order or SSH routes. Meta
ports retain their voter-index offsets. Retained plans keep their exact ports
for restart and follower operations; updated planning defaults never rewrite
an installed cluster. Both HTTP and the
fleet CLI submit the same placement map. Host prerequisites are checked through
a fixed Python helper over noninteractive OpenSSH. Browser setup separates
host preparation, node placement, and review/deploy. Preparation generates a
workspace-owned Ed25519 identity, uses the operator's initial password or
private key to append its public key to the remote account, preserves existing
authorized keys, and proves a fresh managed-key-only login. Initial passwords
and passphrases live only in request memory and a temporary private askpass
socket; they are absent from files, subprocess arguments/environment, catalog,
and replies. Direct bootstrap connections use trust on first use; later SSH
calls require the pinned host key, and changed identities always fail closed.
Preparation does not change sshd or sudo policy. The inventory is shared
across clusters and exposed by `fleet-hosts`; UI and CLI plans may reference
its host IDs. Saved hosts are rechecked before browser placement. Legacy CLI
plans may still supply independently prepared SSH settings. Preview performs read-only checks and
retains the reviewed inputs behind a short-lived server token. Deployment
requires that token and the cluster name; browser-supplied replacement asset
URLs, checksums, or node assignments are not accepted.

The private release cache verifies complete archives before publishing them
under their content digest. The original nightly bytes remain available for
restarts and follower additions after GitHub replaces its nightly assets.
Only architectures installed in that cluster are necessarily cached. A later
addition on another architecture needs that pinned asset still available or
restored to the cache. Downloads and SSH/RESP outputs are bounded.

The remote helper claims only a new directory with the retained ownership
nonce. Retries reuse identical configuration and never replace existing Data
files or release digests. It uploads verified archives from Admin, installs
only the runtime executables and notices, allocates new files, and starts Meta
before Data. Meta's state directory is separate from launch/configuration
files; the launch wrapper supplies bootstrap membership only for a pristine
state, including after a host reboot. The default lifecycle uses systemd user
services with lingering; explicit development mode uses detached processes
without automatic restart. Admin uses existing machines and does not change
sudo policy. Optional SPDK setup requires root SSH on dedicated Data hosts: read-only preflight records the
selected controller serial and rejects media in use or unsupported IOMMU/memory
configurations. Explicit deployment confirmation authorizes a root-owned host
and controller claim, hugepage reservation, and VFIO binding. Retained system
services restore configuration before SPDK Data services at boot. Those Data
services have unlimited memlock; Meta retains user-service ownership. Device
claims survive interrupted deployment and prevent cross-deployment reuse; setup
never erases media or enables unsafe no-IOMMU mode. Stopping Admin leaves host
services running.

Optional monitoring placement runs the repository’s Prometheus/Grafana Compose
stack on prepared hosts with Docker access. Admin assigns Data metrics ports and
uploads configuration; each monitor generates and retains its own Grafana password
outside the fleet catalog and browser. Data connectivity and monitoring readiness
are checked after Meta reports the created cluster ready: Data listeners do not
open before Genesis. Interrupted verification requires explicit resume and never
replays cluster creation. Private Grafana listeners, loopback
Prometheus listeners, and network-restricted Data metrics share the deployment’s
trusted-network boundary. Monitoring is ready only after its services and all
scrape targets respond. Follower installation updates the retained target set;
membership removal leaves the still-running process observable. Monitoring
configuration and volumes survive Admin shutdown.

Creation saves a durable checkpoint before submitting `cluster-create` with
the retained manifest. After interruption, installation/start phases require
explicit resume using the same identities and owned paths. Creation phases
only observe Meta and never replay an uncertain Genesis. Follower provisioning
uses the cluster's exact release, then hands its existing job identity to the
native replica-add workflow. Old releases lacking the membership revision and
idempotency APIs permit setup and inspection but not Admin resize/failover.

All Linux release archive variants include the same Admin sources, browser
assets, launcher, a checksummed Node runtime, and its notices. The launcher
keeps mutable workspace state outside the extracted archive. On macOS it uses
an installed compatible Node runtime. The cross-platform launcher CLI and
`lavik-ctl` share the private fleet socket protocol.

## Mutation lifecycle

Admin persists a request identity and its intent before sending a mutation.
A unique database constraint admits one active Admin request per registered
cluster. Meta still validates every mutation and arbitrates with concurrent
direct operators and failover. A failover request retains its operation ID
and absolute deadline as an indivisible idempotency pair. Cluster creation
uses the existing single-cluster Genesis admission and retained Meta root.

Replica changes create a generic Meta operation under the same request ID.
Addition registers the specified node identity/endpoint, assigns it to an
existing group, and observes native Follow Owner synchronization. It completes
only after Meta observes current population, projection, and health. Removal
retains the reviewed membership revision, rejects the current Owner and active
failover, and commits the existing membership-removal command. It completes
when the node is unassigned and the remaining group's topology converges.
Unassigned nodes have no group-runtime acknowledgement. Addition accepts an
already-running node or follows the SSH provisioning workflow above. Removal
changes membership only; it does not stop the process, delete storage, or
reclaim its retained installation record.

The Meta publisher and native Follow Owner relationship drive Data convergence
after membership commits. A removed node retains only remote routing for its
old group; those routes contain no local population manifest. Re-adding that
node creates a new assignment incarnation. Replica resizing leaves primary
groups and slot ownership unchanged. Changing their count requires a separate
data-migration protocol and is not exposed as an available operation.

Admin restarts treat previously running requests as uncertain and observe
committed state. They never blindly repeat a possibly submitted mutation.
Explicit retry preserves the request identity, deadline, and removal revision;
expired or stale removal intent requires a new review. Abandoning an uncertain
replica request terminalizes its generic Meta operation without undoing
committed membership. Typed creation and failover remain Meta-owned workflows.

## Browser, data, and trust boundaries

Browser onboarding distinguishes a local Docker demo, SSH deployment, and an
existing cluster. Existing-cluster discovery is read-only and retains tested
connection inputs behind an expiring server token before catalog registration;
registration never initializes or replaces Meta state. The source-based Docker
entry point builds and starts Compose, registers its manifest’s seeds, and uses
an Admin creation job with a stable request identity. Reruns observe retained
jobs and never replay uncertain creation.

The browser uses HTTP-only, SameSite session cookies with finite lifetime.
An access token in a private file establishes a session; sessions expire on
process restart. Mutation requests require a same-origin custom header and
JSON content type. The CLI socket is mode 0600 inside a mode-0700 directory.
Remote browser deployment uses a TLS reverse proxy and an explicit origin.
Connection profiles retain Meta mTLS and optional Data TLS/password settings
on the server; browser replies and the fleet database do not contain these
credentials. Plaintext Meta profiles rely on the existing trusted-network
boundary.

Data requests use bounded RESP connections to endpoints learned from Meta.
For SSH deployments they run through the node's retained host, or a Meta host
for an already-running addition, so the Admin computer does not need direct
access to cluster-private IPs. Provisioned node traffic uses a trusted private
network; this path does not automatically configure Meta/Data TLS.
Redirects are followed only to endpoints in that cluster's topology. The
console admits an explicit command set and requires write confirmation;
Meta and native replication commands are excluded. Metrics report partial node
failures explicitly and derive throughput from successive counter samples. Slow logs use `SLOWLOG`;
unsupported Valkey-specific observability is not simulated.

## Source map

| Responsibility | Source |
|---|---|
| HTTP, authentication, private CLI entry | `admin/server.mjs` |
| Catalog, workflow admission and observation, data tools | `admin/fleet.mjs` |
| Database owner and schema | `admin/store.mjs` |
| Host inventory, managed SSH identity, transient initial authentication | `admin/hosts.mjs`, `admin/askpass.mjs` |
| Reviewed deployment, storage and monitoring placement, follower installation | `admin/deploy.mjs`, `admin/monitoring.mjs`, `deploy/monitoring/` |
| Official release resolution and immutable cache | `admin/releases.mjs` |
| SSH transport and remote ownership/service lifecycle | `admin/ssh.mjs`, `admin/remote.py` |
| Release launcher, portable fleet client, packaging | `admin/lavik-admin`, `admin/cli.mjs`, `scripts/package_admin.sh` |
| Meta transport reuse | `admin/meta.mjs`, `app/lavik_ctl.cpp` |
| Bounded RESP transport and binary-safe values | `admin/resp.mjs` |
| Browser views and shared host placement model | `admin/public/`, `admin/public/placement.js` |
| Meta summaries and membership removal | `src/meta/ctl_server.cpp`, `src/meta/state_machine.cpp` |
| Docker and browser verification | `admin/test/`, `admin/Dockerfile*`, `admin/compose.yaml` |
