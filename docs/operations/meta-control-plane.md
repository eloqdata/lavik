# Meta control plane operations

## Process and storage prerequisites

Build the separate Meta executable and its operator client with:

```sh
cmake --build <build-dir> --target keylane-meta keylane-ctl
```

A cluster normally has three `keylane-meta` processes. Every process needs:

- A positive, cluster-unique `--id` that never changes for that member.
- A numeric IPv4 or IPv6 `--addr` used both as its Raft listener and advertised
  endpoint. For IPv6, use the form accepted by `keylane-meta --help`.
- A distinct numeric `--data-control-addr` for the process-lifetime listener
  used by Data nodes. This endpoint is committed with membership and returned
  by every Meta seed; it must remain stable across restart.
- A concrete numeric `--ctl-addr` for remote Admin access and leader discovery
  before the cluster grows beyond one voter. It is both the actual bind and
  committed address; wildcard hosts and port zero are rejected. Changing it
  requires removing/retiring the member and joining a fresh server id.
- A private `--data-dir`. Never share a directory between members or reuse it
  with another id.
- At least one Admin listener. Omit both Admin options to use the local Unix
  socket at `<data-dir>/meta-admin.sock`; use only `--ctl-addr` for a TCP-only
  member, or specify both options to expose both transports.

Create the data directory as the service account with mode 0700. The control
socket itself is mode 0600 and authenticates the caller with Linux
`SO_PEERCRED`. By default only the process uid is allowed; repeat
`--ctl-allow-uid N` to replace that default with an explicit uid allowlist.
The socket parent must not be group- or world-writable. Remote administration
is a separate option: `--ctl-addr` uses plaintext TCP unless all three
`--ctl-tls-*` arguments are supplied. Partial TLS configuration fails startup
instead of silently downgrading. A plaintext listener grants operator access to
any reachable peer, so expose it only on loopback or a trusted private network.
When both `--ctl-socket` and `--ctl-addr` are explicitly provided, both
listeners start and share command dispatch, authorization, status capture, and
limits; failure of either listener rolls back the process startup. Naming only
one enables only that transport. Omitting both enables the default Unix socket.

Only the first process of a new cluster is started with `--bootstrap`. A fresh
process without `--bootstrap` opens its listener and waits to be invited; merely
starting it does not make it a member. On restart, use the same id, address,
data directory, TLS mode, and bootstrap setting originally used for that
member.

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
TLS/identity failures, incompatible wire data, and corrupt status exit 1 with
empty stdout. `cluster-status` requires `--allow-plaintext-admin` for TCP
without TLS.
Supplying `--tls-ca`, `--tls-cert`, and `--tls-key` together enables mTLS for
every TCP connection, with the address verified against the server
certificate's IP SAN. These credentials can accompany a Unix seed and secure
the connection to a discovered remote leader. There is no plaintext/TLS
fallback. One absolute deadline, five seconds by default, covers discovery,
redirects, capture, and response I/O.

For plaintext remote administration, configure a listener and connect without
TLS arguments:

```sh
keylane-meta --id 1 --addr 10.0.0.11:7100 \
  --data-control-addr 10.0.0.11:7300 \
  --data-dir /var/lib/keylane/meta-1 --bootstrap \
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

"$META_BIN" --id 1 --addr 127.0.0.1:7101 \
  --data-control-addr 127.0.0.1:7301 \
  --ctl-addr 127.0.0.1:7201 \
  --ctl-socket "$META_ROOT/node1/meta-admin.sock" \
  --data-dir "$META_ROOT/node1" --bootstrap \
  >"$META_ROOT/node1.log" 2>&1 &

"$META_BIN" --id 2 --addr 127.0.0.1:7102 \
  --data-control-addr 127.0.0.1:7302 \
  --ctl-addr 127.0.0.1:7202 \
  --ctl-socket "$META_ROOT/node2/meta-admin.sock" \
  --data-dir "$META_ROOT/node2" \
  >"$META_ROOT/node2.log" 2>&1 &

"$META_BIN" --id 3 --addr 127.0.0.1:7103 \
  --data-control-addr 127.0.0.1:7303 \
  --ctl-addr 127.0.0.1:7203 \
  --ctl-socket "$META_ROOT/node3/meta-admin.sock" \
  --data-dir "$META_ROOT/node3" \
  >"$META_ROOT/node3.log" 2>&1 &
```

Wait until node 1 reports `leader=1`, then add one waiting member at a time:

```sh
keylane-ctl --socket "$META_ROOT/node1/meta-admin.sock" status
keylane-ctl --socket "$META_ROOT/node1/meta-admin.sock" \
  addsrv 2 127.0.0.1:7102 127.0.0.1:7302 127.0.0.1:7202

keylane-ctl --socket "$META_ROOT/node2/meta-admin.sock" status

keylane-ctl --socket "$META_ROOT/node1/meta-admin.sock" \
  addsrv 3 127.0.0.1:7103 127.0.0.1:7303 127.0.0.1:7203

keylane-ctl --socket "$META_ROOT/node3/meta-admin.sock" status
```

Do not treat `addsrv` returning `OK` as proof that catch-up finished: NuRaft
returns it when the invite is accepted. Before adding the next member, poll the
new member's `status` until it remains alive and its `committed` index reaches
the leader value observed after the add. The current `status` command does not
list the membership set; a replicated write observed on the joiner is the
stronger end-to-end check when an automation needs proof of convergence.

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
  --bootstrap \
  --tls-ca /etc/keylane/meta/ca.crt \
  --tls-cert /etc/keylane/meta/meta-1.crt \
  --tls-key /etc/keylane/meta/meta-1.key \
  --ctl-tls-ca /etc/keylane/meta/ca.crt \
  --ctl-tls-cert /etc/keylane/meta/meta-1.crt \
  --ctl-tls-key /etc/keylane/meta/meta-1.key
```

Use the equivalent member-specific certificate and omit `--bootstrap` for
members 2 and 3, then join them with their Raft, Data-control, and Admin
endpoints.
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
node id and one or more numeric Data-control seeds. Static topology and Meta
control are mutually exclusive. Before starting a new Data process, register
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
2. Start the new `keylane-meta` process without `--bootstrap`.
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

The leader first commits and audits the member identity binding, then invokes
NuRaft `add_srv`. `ERR joining` and `ERR config-changing` mean the caller should
wait, re-check the leader and both members, and retry the same operation.
`ERR already-exists` may mean an earlier invite committed; verify the joiner
rather than creating a different identity.

To remove a peer, select a follower and run this on the leader:

```sh
keylane-ctl --socket /var/lib/keylane/meta-1/meta-admin.sock removesrv 4
```

On success, Keylane first completes NuRaft `remove_srv`, then commits and
audits retirement of that member's identity before replying `OK`. Stop the
removed process after the command succeeds. The retired id and principal are
terminal and cannot be reactivated; replacing that machine requires a new id,
fresh data directory, and, with mTLS, a new certificate.

Remove one member at a time and preserve a quorum throughout. Prefer removing
a follower; removing the current leader may return `ERR cannot-remove-leader`
or trigger a step-down depending on NuRaft state. `ERR leaving` and
`ERR config-changing` indicate another membership change is still active.
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

Meta durable schema and segmented WAL remain v1 while the first release is
unpublished. The current layout replaces earlier development layouts in
place; equal version numbers do not make incompatible builds safe to mix.
There is no mixed-format window or in-band format switch. For a binary-only
change that preserves the format, replace one follower at a time, wait for
catch-up, and replace the leader last. Before any replacement, back up every
member and record the membership, term, commit index, and snapshot index.

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
