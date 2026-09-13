# Meta control plane operations

## Process and storage prerequisites

Build the separate Meta executable and its operator client with:

```sh
cmake --build <build-dir> --target keylane-meta keylane-ctl
```

A cluster normally has three `keylane-meta` processes. Every process needs:

- A positive, cluster-unique `--id` that never changes for that member.
- A numeric IPv4 or IPv6 `--addr` for its local Raft listener. At genesis it
  must equal the manifest's advertised Raft endpoint; a restart may bind
  behind an explicit transport proxy without changing the durable endpoint.
  For IPv6, use the form accepted by `keylane-meta --help`.
- A distinct numeric `--data-control-addr` for the process-lifetime listener
  used by Data nodes. The durable advertised route comes from membership and
  may instead name a proxy; that route must remain stable across restart.
- A concrete numeric `--ctl-addr` for remote Admin access and leader discovery
  before the cluster grows beyond one voter. This flag is the local bind; the
  durable advertised route may name a proxy. Wildcard hosts and port zero are
  rejected. Changing an advertised route requires removing/retiring the
  member and joining a fresh server id.
- A private `--data-dir`. Never share a directory between members or reuse it
  with another id.
- The required TCP Admin listener plus an optional Unix socket. With no
  explicit `--ctl-socket`, `keylane-meta` also uses
  `<data-dir>/meta-admin.sock`.

Create the data directory as the service account with mode 0700. The control
socket itself is mode 0600 and authenticates the caller with Linux
`SO_PEERCRED`. By default only the process uid is allowed; repeat
`--ctl-allow-uid N` to replace that default with an explicit uid allowlist.
The socket parent must not be group- or world-writable. The required
`--ctl-addr` listener uses plaintext TCP unless all three `--ctl-tls-*`
arguments are supplied. Partial TLS configuration fails startup instead of
silently downgrading. A plaintext listener grants operator access to any
reachable peer, so expose it only on loopback or a trusted private network.
The TCP listener and the explicit or default Unix socket share command
dispatch, authorization, status capture, and limits; failure of either rolls
back process startup.

Before the first start, give every initial Meta process the same canonical
cluster manifest through `--initial-cluster-manifest`. The manifest's complete
`[[meta_members]]` set becomes one NuRaft genesis configuration; one, three,
and five initial voters follow the same path and use ordinary randomized
election. Each process's `--id` and initial Raft address must match its
manifest descriptor. Data-control and Admin process addresses are local binds;
their manifest values are the durable advertised routes.

The manifest is a bootstrap-only input. After `cluster_config.dat` exists,
restart with the same id, data directory, compatible listeners, and TLS mode
but omit `--initial-cluster-manifest`; replaying it is rejected. A pristine
process started without a manifest is instead an election-disabled waiting
joiner for a future `addsrv`. Internally, `initial_bindings.dat` keeps initial
transport recovery enabled only until every genesis identity binding commits;
`initial_bindings_complete.dat` permanently disambiguates that completed state
from the initial two-file publication prefix. A waiting process's
`waiting_joiner.dat` survives pre-add config replay and is removed only after
the installed config includes its local id and it applies all bindings through
that committed membership config. `raft_started.dat` is published before the
first durable vote so a later loss of both vote and WAL cannot make an active
genesis directory look unused. Once either path converges,
`transport_bindings.dat` holds the exact last-converged descriptors and their
state-machine replay watermark; future `addsrv` and `removesrv` changes update
that same reusable baseline transactionally with `cluster_config.dat`. A
transient `transport_bindings.next` is resolved during crash recovery. A late
initial member may catch up through either WAL or a snapshot after membership
has changed. Before opening Raft transport or elections, restart validates the
snapshot and reconciles its embedded membership with the durable config. It
automatically completes interrupted config/baseline/lifecycle writes when that
snapshot supplies the evidence; a later config needs its matching WAL entry
(except an election-disabled joiner's pending invite). Conflicting configurations
or missing recovery evidence prevent startup. Do not
edit or delete these files or other Raft state to turn an old member into a new
one; missing, mismatched, or malformed lifecycle state intentionally prevents
startup.

## Send administrative commands

Direct `keylane-ctl` commands send one LF-terminated request to the
selected member and print its one-line reply. `status` reports that member's
local state. Run the client as an allowed uid when using the local Unix socket:

```sh
keylane-ctl \
  --socket /var/lib/keylane/meta-1/meta-admin.sock status
```

Direct commands exit 0 for an `OK` reply, 2 for an `ERR` reply, and 1 for local,
connection, TLS, timeout, or malformed-protocol failures. The default timeout
is five seconds and `--timeout-ms` changes the whole connect/send/receive
deadline. Query every live member when locating the leader; the leader's reply
contains `leader=1`, while mutation requests sent to a follower return
`ERR not-leader`.

For cluster-wide readiness, use `keylane-ctl cluster-status`. It queries
the seed for the current leader and requests one leader-bracketed status cut;
it does not probe followers or claim their reachability or replication progress:

```sh
keylane-ctl cluster-status \
  --socket /var/lib/keylane/meta-1/meta-admin.sock

keylane-ctl cluster-status --addr 10.0.0.11:7200 \
  --allow-plaintext-admin --json
```

Connection options may also precede `cluster-status`, for example
`keylane-ctl --socket /var/lib/keylane/meta-1/meta-admin.sock cluster-status`.
The first human-readable line is `READY`, `NOT READY`, or `RETRYABLE`.
Corresponding exits are 0, 2, and 3; invalid options, unsafe transport choices,
TLS/identity failures, incompatible wire data, and corrupt status exit 1.
Text and JSON status include `status_explanation` and `next_action`; fatal
errors print equivalent guidance to stderr even when no trustworthy status cut
exists. `cluster-status` requires `--allow-plaintext-admin` for TCP without TLS.
Supplying `--tls-ca`, `--tls-cert`, and `--tls-key` together enables mTLS for
every TCP connection, with the address verified against the server
certificate's IP SAN. These credentials can accompany a Unix seed and secure
the connection to a discovered remote leader. There is no plaintext/TLS
fallback. One absolute deadline, five seconds by default, covers discovery,
redirects, capture, and response I/O.

## Create the first multi-Group cluster

`keylane-ctl cluster-create` is the v1 topology-creation path for a fresh
manifest-bootstrapped Meta cluster and one or more preconfigured Data
processes. It is not an import, expansion, or retry-existing command. One Meta
Raft cluster permanently owns at most one logical Data cluster. Only lifecycle
`uninitialized` accepts creation, and the environment must also contain no
Data identity (including retired identities), Group or Slot state, policy,
grant, population manifest, or legacy creation operation. Meta identity,
configuration, and audit records do not make the environment non-pristine.
Creation is destructive for every declared Data process. Define the initial
Meta set in the same manifest shown below, then start every Meta process from
that file before starting Data:

```sh
keylane-meta --id 1 --addr 127.0.0.1:7101 \
  --data-control-addr 127.0.0.1:7301 \
  --ctl-addr 127.0.0.1:7201 \
  --data-dir /var/lib/keylane/meta-1 \
  --initial-cluster-manifest /etc/keylane/cluster.toml \
  --ctl-socket /var/lib/keylane/meta-1/meta-admin.sock
```

Start every Data process in fail-closed Meta-managed mode with its final node
id, unique client port and storage path. For example, repeat this pattern for
the ids and ports named by the manifest:

```sh
keylane --cluster-enabled \
  --cluster-node-id 1111111111111111111111111111111111111111 \
  --cluster-meta-seed 127.0.0.1:7301 \
  --cluster-announce-ip 127.0.0.1 --port 6371 \
  --data-file /var/lib/keylane/data-primary-1/keylane.data
```

Data reports LOADING while its unregistered control connection retries. Run
the same release on Meta, Data and the CLI. Version 1 names the current
multi-Group topology and full initial Meta directory; it does not provide
compatibility with the earlier scalar Meta development layout. Raft,
Data-control, and Admin advertised addresses belong to each Meta descriptor.
Local bind addresses, storage paths, Unix socket paths, TLS files, and runtime
tuning remain per-process arguments and do not belong in the manifest. A bind
may differ from its advertised route when an explicit proxy or load balancer
owns that route.
An automatically allocated two-Group topology is:

```toml
schema_version = 1
slot_strategy = "contiguous-even"

[[meta_members]]
id = 1
raft_endpoint = "tcp://127.0.0.1:7101"
data_control_endpoint = "tcp://127.0.0.1:7301"
ctl_endpoint = "tcp://127.0.0.1:7201"

[[data_nodes]]
id = "1111111111111111111111111111111111111111"
client_endpoint = "tcp://127.0.0.1:6371"

[[data_nodes]]
id = "2222222222222222222222222222222222222222"
client_endpoint = "tcp://127.0.0.1:6372"

[[data_nodes]]
id = "3333333333333333333333333333333333333333"
client_endpoint = "tcp://127.0.0.1:6373"

[[data_nodes]]
id = "4444444444444444444444444444444444444444"
client_endpoint = "tcp://127.0.0.1:6374"

[[groups]]
id = "group-1"
primary = "1111111111111111111111111111111111111111"
replicas = ["2222222222222222222222222222222222222222"]

[[groups]]
id = "group-2"
primary = "3333333333333333333333333333333333333333"
replicas = ["4444444444444444444444444444444444444444"]
```

To choose Slots explicitly, omit `slot_strategy` and add a complete table:

```toml
[[slot_ranges]]
first = 0
last = 8191
group = "group-1"

[[slot_ranges]]
first = 8192
last = 16383
group = "group-2"
```

The parser rejects files over 64 KiB, unknown or duplicate structure,
noncanonical ids or numeric endpoints, duplicate or cross-Group membership,
Groups without a valid primary, and Slot tables with gaps, overlaps, unknown
Groups, or incomplete coverage. Every declared Data must belong to exactly one
Group and every Group must own at least one Slot. `contiguous-even` sorts
Group ids and assigns boundaries with
`floor(i*16384/N)..floor((i+1)*16384/N)-1`. The CLI displays the fully
normalized order and range table before any mutation.

For mTLS Data control and replication, configure each Data process with the
[Data TLS credentials](#configure-data-nodes), `--tls-replication`, and a
`--tls-port` listener. Add its advertised TLS address to its `[[data_nodes]]`
entry before creating the cluster:

```toml
[[data_nodes]]
id = "1111111111111111111111111111111111111111"
client_endpoint = "tcp://127.0.0.1:6371"
tls_endpoint = "tls://127.0.0.1:16371"
```

Here the Data process listens with `--port 6371 --tls-port 16371`. For a
TLS-only listener, use `--port 0 --tls-port 16371` and omit `client_endpoint`
from that node's manifest entry. At least one endpoint is required; when both
are present they must use the same numeric host and distinct ports. Data socket
addresses must be unique across the manifest, including across transports.
Keep certificate paths and keys in process configuration. Meta and Admin
descriptors retain their `tcp://` address spelling; their TLS mode comes from
the process TLS options, independently of these Data listener tags.

New clients encode creation intents as binary version 4. Meta also reads
version 3 intents for recovery of existing TCP-only creation workflows; run
the updated Meta and CLI together when creating a topology with TLS endpoints.

Repeat `[[meta_members]]` for every first-wave voter. IDs and each endpoint
class must be unique; entries are canonicalized by ID, all members are voters,
and the principal is fixed as `keylane://meta/<id>`. At admission the manifest
set must exactly equal NuRaft's committed descriptors, the identity store, and
the Admin/Data-control directories. Cluster Create never calls `add_srv`.
Its durable root first waits at a fixed Raft barrier until every remote Meta
has recently replied and reports its state machine applied through the root's
submit index; only then does Data registration begin. During that phase the
leader deliberately waits for all Meta state machines. After the root advances,
normal writes return to majority-based completion, so an unavailable minority
does not stall unrelated control-plane mutations.

Run:

```sh
keylane-ctl cluster-create --manifest cluster.toml \
  --socket /var/lib/keylane/meta-1/meta-admin.sock
```

Review the normalized plan and data-erasure warning, then enter exactly
lowercase `yes`. EOF, any other input, or a failed parse exits before any Meta
request. Automation may pass `--yes`. `--timeout-ms` is one absolute deadline
for leader discovery and the Genesis proposal response; its default is 120
seconds. It does not wait for Data initialization or READY. Remote TCP uses
the same mTLS or explicit `--allow-plaintext-admin` policy as
`cluster-status`; `--json` does not apply.

Exit 0 means the root operation and lifecycle `creating` transition committed
atomically. The CLI prints
`Cluster create accepted: genesis committed=<index> operation=<id>` and returns
immediately. Exit 1 is a local manifest/confirmation failure or a connection or
protocol failure known to precede mutation. Exit 2 is an explicit Meta
rejection. Exit 3 means the mutation was sent but its commit outcome is
uncertain; preserve the printed/root operation id and run `cluster-status`
before taking further action.

Do not issue another create after accepted or uncertain outcomes. The leader
continues the accepted root in the background, and restart or leadership
change resumes it from snapshot/WAL. All subsequent creates—whether their
manifest is identical or different—are rejected as `already-created` once the
lifecycle is `creating`, `created`, or `provisioning-failed`. The system has no
attach, retry-existing, reset, or Meta reuse path.

Run `cluster-status` to follow the root id, Genesis index, current phase, and
runtime blockers; use `getop <root-operation-id>` while the operation remains
retained. A non-terminal record includes its phase; `recovery-required` means
the retained v1 intent no longer matches safe execution conditions. Preserve
Meta/Data logs and directories and investigate rather than reinitialize. A
changed Data boot/history, invalidated attempt, or changed topology does not
authorize another destructive reset.

Graceful Meta stop cancels result waits and leaves accepted work intact; it
does not wait for an offline Data node. A deterministic Data failure fences
only that Group before aborting its child and the overall creation. Already
completed Groups are not rolled back. While replica initialization is pending,
a changed source boot/history or a new boot on a replica still awaiting its
result follows this failure path and reports the affected Group and node.
The failure is retained across Meta restart; it does not automatically start
a new destructive attempt. Meta recovery preserves successful
population work and finishes bookkeeping without initializing it again.
Meta-member addition/removal has a separate recovery driver; Data
migration and failover orchestration do not become recoverable merely because
the operation journal exists.

Interpret `cluster_state` independently from `READY / NOT READY / RETRYABLE`:

| `cluster_state` | Meaning | Next action |
|---|---|---|
| `uninitialized` | No Genesis committed and no Data-cluster artifacts exist | Validate the manifest, then run `cluster-create` once |
| `non-pristine` | Lifecycle is uninitialized but Data-cluster artifacts exist | Do not create; inspect the artifacts and rebuild the Meta data directory if this is discarded development state |
| `creating` | Genesis committed and the durable workflow is running or awaiting recovery | Wait and rerun `cluster-status`; inspect the reported phase and Meta logs if it stops advancing |
| `created` | Initial workflow completed; current Data runtime may still be NOT READY | Inspect ordinary blockers and Data sessions until READY |
| `provisioning-failed` | Genesis committed but initialization failed deterministically after fencing | Do not rerun create; preserve data and inspect the failure summary, root operation, and Meta logs |

`status_explanation` summarizes the current condition and `next_action` gives
the same guidance in both text and JSON output. A `RETRYABLE` result means no
stable leader-observed cut was available; verify quorum and Admin connectivity,
then rerun `cluster-status`.

During creation, `cluster-status` distinguishes `meta_catching_up`, a declared
but not yet committed `data_unregistered`, an unregistered Data process whose
Hello is actively retrying (`data_unregistered_retrying`), and a committed
identity that has never contacted the current leader (`data_unobserved`). If a
previously accepted or actively retrying node lacks a current accepted session,
the blocker is `data_session_missing`. Later Group blockers continue to name
missing projection, health, or population evidence.

For plaintext remote administration, configure a listener and connect without
TLS arguments:

```sh
keylane-meta --id 1 --addr 10.0.0.11:7100 \
  --data-control-addr 10.0.0.11:7300 \
  --data-dir /var/lib/keylane/meta-1 \
  --initial-cluster-manifest /etc/keylane/cluster.toml \
  --ctl-addr 10.0.0.11:7200

keylane-ctl --addr 10.0.0.11:7200 status
```

All plaintext peers share the audit actor
`keylane://operator/plaintext`; source IP addresses are not treated as
authenticated identities. Use the mTLS configuration below when distinct,
cryptographically authenticated operators or data nodes need the remote
surface.

## Run a local plaintext cluster

Raft transport is plaintext when no `--tls-*` arguments are present. The
following three-process cluster is suitable for local development; production
addresses and durable directories should be managed by the service manager
instead of background shell jobs:

```sh
META_BIN=./build-clang/keylane-meta
META_ROOT=/tmp/keylane-meta-demo
install -d -m 0700 "$META_ROOT" \
  "$META_ROOT/node1" "$META_ROOT/node2" "$META_ROOT/node3"
INITIAL_MANIFEST="$META_ROOT/cluster.toml"
```

Create `cluster.toml` with the intended Data/Group sections and these three
Meta entries (the parser normalizes them by id):

```toml
[[meta_members]]
id = 1
raft_endpoint = "tcp://127.0.0.1:7101"
data_control_endpoint = "tcp://127.0.0.1:7301"
ctl_endpoint = "tcp://127.0.0.1:7201"

[[meta_members]]
id = 2
raft_endpoint = "tcp://127.0.0.1:7102"
data_control_endpoint = "tcp://127.0.0.1:7302"
ctl_endpoint = "tcp://127.0.0.1:7202"

[[meta_members]]
id = 3
raft_endpoint = "tcp://127.0.0.1:7103"
data_control_endpoint = "tcp://127.0.0.1:7303"
ctl_endpoint = "tcp://127.0.0.1:7203"
```

Start all three first-wave members with that same file:

```sh

"$META_BIN" --id 1 --addr 127.0.0.1:7101 \
  --data-control-addr 127.0.0.1:7301 \
  --ctl-addr 127.0.0.1:7201 \
  --ctl-socket "$META_ROOT/node1/meta-admin.sock" \
  --data-dir "$META_ROOT/node1" \
  --initial-cluster-manifest "$INITIAL_MANIFEST" \
  >"$META_ROOT/node1.log" 2>&1 &

"$META_BIN" --id 2 --addr 127.0.0.1:7102 \
  --data-control-addr 127.0.0.1:7302 \
  --ctl-addr 127.0.0.1:7202 \
  --ctl-socket "$META_ROOT/node2/meta-admin.sock" \
  --data-dir "$META_ROOT/node2" \
  --initial-cluster-manifest "$INITIAL_MANIFEST" \
  >"$META_ROOT/node2.log" 2>&1 &

"$META_BIN" --id 3 --addr 127.0.0.1:7103 \
  --data-control-addr 127.0.0.1:7303 \
  --ctl-addr 127.0.0.1:7203 \
  --ctl-socket "$META_ROOT/node3/meta-admin.sock" \
  --data-dir "$META_ROOT/node3" \
  --initial-cluster-manifest "$INITIAL_MANIFEST" \
  >"$META_ROOT/node3.log" 2>&1 &
```

Query the processes until exactly one reports `leader=1`. No `addsrv` command
is part of this initial startup:

```sh
keylane-ctl --socket "$META_ROOT/node1/meta-admin.sock" status
keylane-ctl --socket "$META_ROOT/node2/meta-admin.sock" status
keylane-ctl --socket "$META_ROOT/node3/meta-admin.sock" status
```

On every later restart, use the same process arguments but remove
`--initial-cluster-manifest`. The durable config, not this genesis file, is
then authoritative.

For later expansion, replacement, or contraction, start a pristine waiting
joiner without a manifest and use the reusable membership workflow below.
`addsrv` and `removesrv` persist a workflow before changing identity or Raft
membership. `OK` means the requested configuration and identity changes have
committed, not just that NuRaft accepted an invite. Before adding the next member, poll the
new member's `status` until it remains alive and its `committed` index reaches
the leader value observed after the add. The current `status` command does not
list the membership set; a replicated write observed on the joiner is the
stronger end-to-end check when an automation needs proof of convergence.

The server bounds the Admin wait using `--client-req-timeout-ms`. An
`ERR uncertain-outcome operation=<id>` reply or disconnected client does not
cancel the accepted task. The current leader retries in the background; after
restart or leader change it finds the task in the recovered journal and
continues without another command. An identical in-flight `addsrv/removesrv`
request waits on that same task, while a conflicting request returns
`ERR config-changing`. Query `getop <id>` for the durable phase/result; Meta
also logs `membership <id> phase=...`. Generic `abortop/completeop` cannot
release a membership reservation while an accepted Raft change may still
commit. A `recovery-required` phase requires investigation of the retained
intent and actual configuration, not a replacement change or automatic
identity reactivation. A direct request to remove the current leader remains
rejected; if a previously authorized removal target later becomes leader,
recovery performs a leadership handoff before continuing its removal.

Plaintext peers still check the claimed Raft source and destination ids against
the configuration and committed identity bindings, but those ids are not
cryptographically authenticated. Use plaintext only where network access and
routing are already trusted.

## Configure Raft mTLS

mTLS uses one CA trusted by the whole Meta cluster and a distinct certificate
and private key for every member. Member `N` must have exactly one URI SAN in
total, the canonical `keylane://meta/N` principal, plus IP or DNS SANs covering
both its Raft and Data-control advertised hosts (one SAN suffices when they
share a host).
The certificate must be usable for both TLS server and TLS client
authentication. Never copy one member's certificate or key to another member.

Use an organization-managed CA in production. The following OpenSSL commands
show the required certificate shape for a disposable development cluster:

```sh
TLS_ROOT=/tmp/keylane-meta-tls
install -d -m 0700 "$TLS_ROOT"
umask 077

openssl req -x509 -newkey rsa:3072 -nodes -sha256 -days 30 \
  -subj '/CN=Keylane Meta Development CA' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign' \
  -keyout "$TLS_ROOT/ca.key" -out "$TLS_ROOT/ca.crt"

issue_meta_cert() {
  member_id=$1
  member_ip=$2
  openssl req -newkey rsa:2048 -nodes -sha256 \
    -subj "/CN=keylane-meta-$member_id" \
    -addext "subjectAltName=IP:$member_ip,URI:keylane://meta/$member_id" \
    -addext 'extendedKeyUsage=serverAuth,clientAuth' \
    -addext 'keyUsage=critical,digitalSignature,keyEncipherment' \
    -keyout "$TLS_ROOT/meta-$member_id.key" \
    -out "$TLS_ROOT/meta-$member_id.csr"
  openssl x509 -req -sha256 -days 30 \
    -in "$TLS_ROOT/meta-$member_id.csr" \
    -CA "$TLS_ROOT/ca.crt" -CAkey "$TLS_ROOT/ca.key" \
    -CAcreateserial -copy_extensions copy \
    -out "$TLS_ROOT/meta-$member_id.crt"
}

issue_meta_cert 1 10.0.0.11
issue_meta_cert 2 10.0.0.12
issue_meta_cert 3 10.0.0.13
```

Protect the CA key offline in a real deployment. Distribute only `ca.crt` and
the matching member leaf/key to each host. Start each process with its own leaf
and the shared CA, for example member 1:

```sh
keylane-meta \
  --id 1 --addr 10.0.0.11:7100 --data-dir /var/lib/keylane/meta-1 \
  --data-control-addr 10.0.0.11:7300 \
  --ctl-addr 10.0.0.11:7200 \
  --initial-cluster-manifest /etc/keylane/cluster.toml \
  --tls-ca /etc/keylane/meta/ca.crt \
  --tls-cert /etc/keylane/meta/meta-1.crt \
  --tls-key /etc/keylane/meta/meta-1.key \
  --ctl-tls-ca /etc/keylane/meta/ca.crt \
  --ctl-tls-cert /etc/keylane/meta/meta-1.crt \
  --ctl-tls-key /etc/keylane/meta/meta-1.key
```

Start every initial member with the equivalent member-specific certificate
and the same manifest. The first election authenticates the complete manifest
peer set; it does not join members sequentially. Omit the manifest from every
later restart. A future dynamic joiner starts with mTLS but without a manifest,
then enters through `addsrv` with all three endpoints.
The Data-control listener reuses the same CA, certificate, and key; there is no
second Data-control TLS option set. `--tls-ca`, `--tls-cert`, and `--tls-key`
are all-or-nothing; partial TLS configuration fails startup. Start the joiner
with mTLS before issuing `addsrv`, and ensure its URI SAN matches the id in that
command.

Do not mix plaintext and mTLS members. Enabling or disabling Raft TLS on an
existing cluster requires a coordinated restart of all members; it does not
change the WAL or snapshot format. `--raft-io-threads` sizes NuRaft's native
Asio pool (default 2); it does not change the single Celer control-session
worker or make WAL synchronization asynchronous.

## Configure Data nodes

A Meta-managed Data process needs its committed 40-character lowercase hex
node id and one or more numeric Data-control seeds. It is the only production
Cluster control mode. Before starting a new Data process, register
that identity and its client endpoint on the Meta leader. The endpoint is
tagged `tcp://` or `tls://`; a dual-listener node may supply one of each, using
the same numeric host:

```sh
keylane-ctl --socket /var/lib/keylane/meta-1/meta-admin.sock \
  registernode 0123456789abcdef0123456789abcdef01234567 \
  keylane://node/0123456789abcdef0123456789abcdef01234567 \
  primary tcp://10.0.1.11:6379
```

The role here is registered identity metadata, not a lease or current group
ownership. Registration alone therefore lets the node authenticate and receive
an empty-topology full state, but it remains fenced/LOADING until later
committed topology, population, and grant state make it ready.

After creating a group, add membership with `assignnode <group-id> <node-id>
<primary|replica>`. The command deliberately has no assignment-id argument:
the trusted Meta proposer generates a fresh nonzero 128-bit value from the OS
CSPRNG for that membership incarnation. Repeating the same desired membership
is idempotent; removing and later re-adding it generates another identity.

The authenticated operator surface also exposes the typed commits needed to
assemble or revoke finite authority. These are low-level, absolute-state
operations intended for controlled bootstrap and recovery workflows:

```text
putpolicy <policy-id> <version> <content>
setslotmap <first> <last> <group-id> <config-epoch>
activateauthority <group-id> <expected-term> <owner-node-id> <lease-ms> \
                  <policy-id> <policy-version> \
                  <new-authority-version> <new-config-epoch>
fencegroup <group-id> <expected-term>
```

`putpolicy` computes the content hash inside the trusted Meta proposer;
`content` is one non-empty, whitespace-free token. `setslotmap` replaces the
entire slot map with one inclusive range—it is not an incremental assignment
command—and sets the named group's absolute config epoch. Both slot endpoints
must be within 0–16383. If the replacement changes a group's slot coverage or
config epoch, every affected source and destination group must first be
fenced; apply rejects the whole map while any such group has an active grant.
Activate fresh authorities only after the complete replacement commits.

`activateauthority` is the atomic owner/grant commit. The term must already
have been established with `begingroupterm`, the owner must hold a current
assignment, and the policy version must already be committed and active. Term,
authority version, config epoch, policy version, and lease duration are
absolute values, not increments. Raft apply checks them against committed
state, rejects stale or conflicting transitions without changing authority,
and may accept an identical domain effect idempotently. Only the cluster-wide
topology epoch is derived by the leader from its committed snapshot.
`fencegroup` removes the grant under the explicit expected-term CAS. Treat
`setslotmap`, `activateauthority`, and `fencegroup` as dangerous: verify the
current leader and intended group/owner before issuing them, and do not retry
an uncertain result with newly invented version values until the committed
state has been checked.

These commits alone do not make a newly assigned Data node population-ready.
Until a reconciliation workflow installs a matching ReadyToken, heartbeats
challenge the committed grant but receive a node-not-ready denial and the Data
node remains fenced/LOADING. Never interpret `activateauthority` returning
`OK` as proof that client writes are enabled.

For a plaintext development deployment, start the registered node with:

```sh
keylane --cluster-enabled \
  --cluster-node-id 0123456789abcdef0123456789abcdef01234567 \
  --cluster-meta-seed 10.0.0.11:7300 \
  --cluster-meta-seed 10.0.0.12:7300 \
  --cluster-meta-seed 10.0.0.13:7300 \
  --data-file /var/lib/keylane/data-1/keylane.data
```

The client first tries its volatile accepted-leader hint, then the latest
committed in-memory Meta directory, then these seeds. It does not persist that
directory, a lease, a term floor, or desired state. Every restart creates a new
boot identity and begins fenced/LOADING until a leader supplies and accepts a
complete projection and finite authority.

Lease expiry is suspend-aware: Data checks deadlines with Linux
`CLOCK_BOOTTIME`, and a new Meta leader or authority identity waits twice the
configured Raft election lower bound before its first otherwise-valid grant.
The second interval is an internally derived cross-host clock margin, not a
separate operator setting. A host suspend therefore consumes an existing lease
instead of extending it. Meta additionally detects suspend against NuRaft's
active clock, closes authority sessions, logs a quarantine warning, and
requests immediate resignation; a sole member must run for one election-lower-
bound interval before accepting authority again. Repeated client reconnects
during that interval are expected and must not be worked around by relaxing
the timing bound. The Meta listener also admits at most 4096 sockets
that have not yet completed TLS/`ClientHello` and either finished a follower
redirect or claimed a leader-side node session slot. A committed node has only
one such leader slot, including while its initial full-state transfer is
stalled; duplicates are rejected until the incumbent exits. Excess sockets are
closed and should be investigated as connection storms or untrusted-network
exposure.
Decoded-plus-encoded projections share a 2 GiB retained-capacity budget, with
1 GiB reserved while each FDS is built. These values are derived from the
512 MiB object cap and the old/new generation overlap. Budget exhaustion closes
the affected setup or publisher session; investigate an abuse-sized committed
projection or excessive concurrent FDS ownership rather than retrying without
first reducing that state.

To enable mTLS, the Data client reuses the existing replication TLS settings;
there are no separate Meta-control certificate flags. Its certificate must
have exactly one URI SAN in total, the canonical
`keylane://node/<node-id>` principal, an IP SAN for the Data endpoint, and both
client/server usages. The URI must equal the active Meta identity binding for
that node. For example:

```sh
keylane --cluster-enabled \
  --cluster-node-id 0123456789abcdef0123456789abcdef01234567 \
  --cluster-meta-seed 10.0.0.11:7300 \
  --cluster-meta-seed 10.0.0.12:7300 \
  --tls-port 6380 --tls-auth-clients yes --tls-replication \
  --tls-ca-cert-file /etc/keylane/data/ca.crt \
  --tls-cert-file /etc/keylane/data/node-01234567.crt \
  --tls-key-file /etc/keylane/data/node-01234567.key \
  --data-file /var/lib/keylane/data-1/keylane.data
```

All Data and Meta certificates used for this connection must chain to the
configured trust roots. Partial TLS inputs fail startup; neither side falls
back to plaintext. In plaintext mode, protect both Raft and Data-control ports
with the same private-network assumptions as the replication path.

Remote administration configures its server certificate independently with
`--ctl-tls-ca`, `--ctl-tls-cert`, and `--ctl-tls-key`. It may reuse that Meta
member's Raft certificate when the control listener uses an IP or DNS already
covered by the certificate and the leaf permits `serverAuth`; otherwise issue
a dedicated server leaf with a SAN covering `--ctl-addr`. The client can select
a DNS SAN instead of the numeric control address with `--tls-server-name` for
direct commands.

Every client certificate must carry exactly one canonical operator URI SAN.
The following development example uses the CA created above to issue
`keylane://operator/admin`; production deployments should use their managed
certificate issuer and normal lifetime/rotation policy:

```sh
umask 077

openssl req -newkey rsa:2048 -nodes -sha256 \
  -subj '/CN=keylane-meta-operator-admin' \
  -addext 'subjectAltName=URI:keylane://operator/admin' \
  -addext 'extendedKeyUsage=clientAuth' \
  -addext 'keyUsage=critical,digitalSignature' \
  -keyout "$TLS_ROOT/operator-admin.key" \
  -out "$TLS_ROOT/operator-admin.csr"

openssl x509 -req -sha256 -days 30 \
  -in "$TLS_ROOT/operator-admin.csr" \
  -CA "$TLS_ROOT/ca.crt" -CAkey "$TLS_ROOT/ca.key" \
  -CAcreateserial -copy_extensions copy \
  -out "$TLS_ROOT/operator-admin.crt"
```

Use that operator identity with the control client:

```sh
keylane-ctl --addr 10.0.0.11:7200 \
  --tls-ca /etc/keylane/meta/ca.crt \
  --tls-cert /etc/keylane/meta/operator-admin.crt \
  --tls-key /etc/keylane/meta/operator-admin.key \
  status

keylane-ctl cluster-status --addr 10.0.0.11:7200 \
  --tls-ca /etc/keylane/meta/ca.crt \
  --tls-cert /etc/keylane/meta/operator-admin.crt \
  --tls-key /etc/keylane/meta/operator-admin.key \
  --json
```

`cluster-status` has no DNS server-name override: every committed Admin route
is numeric and must appear as an IP SAN.

## Add and remove peers

Membership commands are accepted only by the current leader and only one
change may be active at a time. To add a peer:

1. Allocate a never-before-used positive id, private data directory, Raft
   endpoint, distinct Data-control endpoint, and concrete Admin endpoint. With
   mTLS, issue its matching certificate first.
2. Start the new `keylane-meta` process without
   `--initial-cluster-manifest`; its pristine directory becomes a durable,
   election-disabled waiting joiner.
3. On the current leader, run
   `addsrv <id> <raft-endpoint> <data-control-endpoint> <ctl-endpoint>`. A
   fifth principal argument is accepted but may only be the matching canonical
   `keylane://meta/<id>`; omitting it selects that value automatically.
4. Poll the joiner's `status` and verify replicated progress before adding
   another peer or relying on it for quorum.

For example:

```sh
keylane-ctl --socket /var/lib/keylane/meta-1/meta-admin.sock \
  addsrv 4 10.0.0.14:7100 10.0.0.14:7300 10.0.0.14:7200
```

The leader first commits and audits the complete membership intent, then its
background owner binds identity and invokes NuRaft `add_srv`. A successful
invite alone does not complete the task. `ERR config-changing` indicates a
different active workflow; an uncertain-outcome reply retains the original
operation id and the leader continues retrying independently of the client.
`ERR already-exists` may mean an earlier invite committed; verify the joiner
rather than creating a different identity.

To remove a peer, select a follower and run this on the leader:

```sh
keylane-ctl --socket /var/lib/keylane/meta-1/meta-admin.sock removesrv 4
```

On success, Keylane first observes the committed configuration without the peer, then commits and
audits retirement of that member's identity before replying `OK`. Stop the
removed process after the command succeeds. The retired id and principal are
terminal and cannot be reactivated; replacing that machine requires a new id,
fresh data directory, and, with mTLS, a new certificate.

Remove one member at a time and preserve a quorum throughout. Prefer removing
a follower; directly removing the current leader returns `ERR cannot-remove-leader`.
`ERR config-changing` indicates another durable membership/creation task is active.
Never wipe or repurpose a member's data directory before its removal has
committed and the remaining cluster has elected a healthy leader.

## Status and snapshots

Send one LF-terminated command per connection or keep a connection open and
read exactly one reply line per command. `status` reports whether that member
is leader, its server id, committed and snapshot indexes, and current term. It
is a local view; compare all members when diagnosing lag.

Automatic snapshots run according to `--snapshot-distance`; `snapshot` asks
the leader for a commit-serialized capture and returns its cut index. The reply
means capture succeeded, while durable publication and WAL compaction complete
asynchronously. Monitor logs for `snapshot write failed`, snapshot decode or
install errors, and repeated snapshot failures. A leader that exceeds the
uncompacted-WAL or repeated-snapshot-failure guard rejects new proposals with
`RESOURCE_EXHAUSTED` rather than expanding indefinitely.

When that guard fires:

1. Preserve logs. Stop one affected replica at a time and take a filesystem
   snapshot or backup of its data directory before destructive intervention.
2. Confirm a quorum is healthy and compare `committed`, `snapshot_idx`, and
   `term` on every member.
3. Fix filesystem space, permissions, I/O, or snapshot-size pressure. Do not
   delete WAL segments from a running node.
4. Trigger `snapshot` on the leader and wait for `snapshot_idx` to advance and
   the error stream to stop before retrying writes.
5. If one replica remains damaged, remove it from membership before replacing
   its data directory. Retired member identities are terminal, so provision a
   new server id and matching certificate for the replacement. Never wipe a
   quorum simultaneously.

The formal WAL/snapshot format does not migrate prototype `raft_log.dat` or
`LSN1` snapshots. Back up such a directory, then bootstrap a fresh formal
cluster; startup intentionally refuses to guess at a conversion.

## Binary replacement and format compatibility

The physical segmented-WAL container remains v1 while the first release is
unpublished. Cluster lifecycle makes the `MetaTopologyStore` codec and Raft
command envelope v2; older development directories therefore fail loudly at
decode. Other durable stores retain their own exact version markers. Equal
version numbers in any one layer do not make incompatible builds safe to mix.
There is no mixed-format window or in-band format switch. For a binary-only
change that preserves every durable format, replace one follower at a time,
wait for catch-up, and replace the leader last. Before any replacement, back
up every member and record the membership, term, commit index, and snapshot
index.

For an incompatible pre-release format change, stop the old cluster and create
fresh data directories with the new binary. Do not add a new-format process to
an old-format membership or copy old snapshots/WAL into the new directory.
Unknown format markers intentionally fail loudly rather than attempting an
implicit conversion.

## Configure and export audit history

Audit defaults to `bounded-rotate`: service remains available at capacity,
while `status` exposes `audit_dropped_total` and `audit_dropped_through` so an
archival gap cannot be mistaken for complete history. The replicated choices
are `setauditpolicy disabled <attestation>`, `setauditpolicy bounded-rotate
<attestation>`, and `setauditpolicy strict-export <attestation>`. Policy
changes always produce an audit record. Use disabled only under an explicit
operational exception; the missing ordinary records are intentional.

Strict-export is the fail-safe retention mode. Before selecting it, ensure the
window has room for the policy-change record. Choose an index still in the
window and request `exportaudit <through-index>`. Decode and verify the
versioned hash-chain blob and its drop watermarks in the external archival
system, store it durably under an archive-defined deployment namespace, and
deduplicate by Raft log index and record hash. Keylane does not persist a
separate cluster identity. Only after that acknowledgement should an operator
issue `pruneaudit <through-index>`. The prune is replicated and advances the
chain anchor; there is no in-process record of the external acknowledgement.
At a full window, overlapping prune attempts are rejected until the outstanding
prune's Raft outcome resolves, including when its client has already timed out.

Terminal operations may be moved into bounded archive summaries with
`archiveoperations <seq>...`. `exportoperations` returns all current summaries
as a versioned hex blob. After durable external storage, `pruneoperations
<seq>...` removes exactly the listed summaries through a replicated command.
Pruning a summary ends the local late-retry tombstone window for that operation
id, so retention must cover the clients' documented retry horizon.

The current line protocol returns export bytes as hexadecimal on one line.
Protect these responses as audit data and avoid terminal logging that could
copy principals or operation results into an ungoverned sink.

## Recover from the durability fail-safe

Repeated snapshot failure or excessive uncompacted WAL puts the leader into a
durability fail-safe. Ordinary proposals then return `ERR
resource-exhausted`; Data nodes retain no local authority that can bypass this
gate. Stop automated mutators, identify the current leader with `status`, and
correct the underlying disk, permission, or size problem first.

The gate accepts one effect-producing recovery proposal at a time. It rejects a
stale or no-op command before Raft append, even when the command's verb is on
the recovery allowlist. A client timeout does not release the reservation: the
next recovery proposal remains rejected until NuRaft resolves the first one's
actual outcome. Reconcile that outcome from the leader's committed view before
moving to the next step.

If strict-export audit retention is also full, export and durably acknowledge a
large enough prefix first, then run `pruneaudit <through-index>`. The proposed
prune must make the serialized audit window smaller after accounting for the
prune command's own audit record.

For retained live operations, use this bounded sequence. Keep the original
operation id and sequence returned by `submitop`; `getop` does not return the
sequence and is not a linearizable read on a follower.

```sh
keylane-ctl --socket /var/lib/keylane/meta-1/meta-admin.sock \
  abortop 00000001000000000000000000000001
keylane-ctl --socket /var/lib/keylane/meta-1/meta-admin.sock \
  archiveoperations 12345
keylane-ctl --socket /var/lib/keylane/meta-1/meta-admin.sock \
  exportoperations
# Verify and durably store the exported blob before removing its retry tombstone.
keylane-ctl --socket /var/lib/keylane/meta-1/meta-admin.sock \
  pruneoperations 12345
```

The empty `abortop` reason is intentional; `completeop <id>` with an empty
result is the equivalent successful terminalization path. Nonempty result or
reason payloads are rejected while the fail-safe is active. Each command above
must change the named committed record: repeating an already-applied archive or
prune does not consume another log or audit slot.

After enough aggregate state has been removed, run `snapshot` and wait for
`snapshot_idx` to advance. A successful snapshot compacts the WAL and clears
the durability pressure on that member; if the guard remains active, continue
with another known, effect-producing recovery item rather than sending dummy
prunes.
