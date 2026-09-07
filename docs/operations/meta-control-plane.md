# Meta control plane operations

## Process and storage prerequisites

Build the separate Meta executable and its operator client with:

```sh
cmake --build <build-dir> --target keylane-meta keylane-meta-ctl
```

A cluster normally has three `keylane-meta` processes. Every process needs:

- A positive, cluster-unique `--id` that never changes for that member.
- A numeric IPv4 or IPv6 `--addr` used both as its Raft listener and advertised
  endpoint. For IPv6, use the form accepted by `keylane-meta --help`.
- A private `--data-dir`. Never share a directory between members or reuse it
  with another id.
- A local administrative Unix socket, defaulting to
  `<data-dir>/meta-admin.sock`.

Create the data directory as the service account with mode 0700. The control
socket itself is mode 0600 and authenticates the caller with Linux
`SO_PEERCRED`. By default only the process uid is allowed; repeat
`--ctl-allow-uid N` to replace that default with an explicit uid allowlist.
The socket parent must not be group- or world-writable. Remote administration
is a separate option: `--ctl-addr` uses plaintext TCP unless all three
`--ctl-tls-*` arguments are supplied. Partial TLS configuration fails startup
instead of silently downgrading. A plaintext listener grants operator access to
any reachable peer, so expose it only on loopback or a trusted private network.

Only the first process of a new cluster is started with `--bootstrap`. A fresh
process without `--bootstrap` opens its listener and waits to be invited; merely
starting it does not make it a member. On restart, use the same id, address,
data directory, TLS mode, and bootstrap setting originally used for that
member.

## Send administrative commands

`keylane-meta-ctl` sends one LF-terminated command and prints the one-line
reply. Run it as an allowed uid when using the local Unix socket:

```sh
keylane-meta-ctl \
  --socket /var/lib/keylane/meta-1/meta-admin.sock status
```

The client exits 0 for an `OK` reply, 2 for an `ERR` reply, and 1 for local,
connection, TLS, timeout, or malformed-protocol failures. Its default timeout
is five seconds and `--timeout-ms` changes the whole connect/send/receive
deadline. Query every live member when locating the leader; the leader's reply
contains `leader=1`, while mutation requests sent to a follower return
`ERR not-leader`.

For plaintext remote administration, configure a listener and connect without
TLS arguments:

```sh
keylane-meta --id 1 --addr 10.0.0.11:7100 \
  --data-dir /var/lib/keylane/meta-1 --bootstrap \
  --ctl-addr 10.0.0.11:7200

keylane-meta-ctl --addr 10.0.0.11:7200 status
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
  --data-dir "$META_ROOT/node1" --bootstrap \
  >"$META_ROOT/node1.log" 2>&1 &

"$META_BIN" --id 2 --addr 127.0.0.1:7102 \
  --data-dir "$META_ROOT/node2" \
  >"$META_ROOT/node2.log" 2>&1 &

"$META_BIN" --id 3 --addr 127.0.0.1:7103 \
  --data-dir "$META_ROOT/node3" \
  >"$META_ROOT/node3.log" 2>&1 &
```

Wait until node 1 reports `leader=1`, then add one waiting member at a time:

```sh
keylane-meta-ctl --socket "$META_ROOT/node1/meta-admin.sock" status
keylane-meta-ctl --socket "$META_ROOT/node1/meta-admin.sock" \
  addsrv 2 127.0.0.1:7102

keylane-meta-ctl --socket "$META_ROOT/node2/meta-admin.sock" status

keylane-meta-ctl --socket "$META_ROOT/node1/meta-admin.sock" \
  addsrv 3 127.0.0.1:7103

keylane-meta-ctl --socket "$META_ROOT/node3/meta-admin.sock" status
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
and private key for every member. Member `N` must have exactly one recognized
Keylane URI SAN, `keylane://meta/N`, plus an IP or DNS SAN matching the endpoint
given to its peers. The certificate must be usable for both TLS server and TLS
client authentication. Never copy one member's certificate or key to another
member.

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
  --bootstrap \
  --tls-ca /etc/keylane/meta/ca.crt \
  --tls-cert /etc/keylane/meta/meta-1.crt \
  --tls-key /etc/keylane/meta/meta-1.key
```

Use the equivalent member-specific certificate and omit `--bootstrap` for
members 2 and 3, then join them with the same `addsrv` procedure as the
plaintext example. `--tls-ca`, `--tls-cert`, and `--tls-key` are all-or-nothing;
partial TLS configuration fails startup. Start the joiner with mTLS before
issuing `addsrv`, and ensure its URI SAN matches the id in that command.

Do not mix plaintext and mTLS members. Enabling or disabling Raft TLS on an
existing cluster requires a coordinated restart of all members; it does not
change the WAL or snapshot format. `--raft-io-threads` sizes NuRaft's native
Asio pool (default 2); it does not change the single Celer control-session
worker or make WAL synchronization asynchronous.

Remote administration configures its server certificate independently with
`--ctl-tls-ca`, `--ctl-tls-cert`, and `--ctl-tls-key`. It may reuse that Meta
member's Raft certificate when the control listener uses an IP or DNS already
covered by the certificate and the leaf permits `serverAuth`; otherwise issue
a dedicated server leaf with a SAN covering `--ctl-addr`. The client can select
a DNS SAN instead of the numeric control address with `--tls-server-name`.

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
keylane-meta-ctl --addr 10.0.0.11:7200 \
  --tls-ca /etc/keylane/meta/ca.crt \
  --tls-cert /etc/keylane/meta/operator-admin.crt \
  --tls-key /etc/keylane/meta/operator-admin.key \
  status
```

## Add and remove peers

Membership commands are accepted only by the current leader and only one
change may be active at a time. To add a peer:

1. Allocate a never-before-used positive id, private data directory, and Raft
   endpoint. With mTLS, issue its matching certificate first.
2. Start the new `keylane-meta` process without `--bootstrap`.
3. On the current leader, run `addsrv <id> <endpoint>`. A third principal
   argument is accepted but may only be the matching canonical
   `keylane://meta/<id>`; omitting it selects that value automatically.
4. Poll the joiner's `status` and verify replicated progress before adding
   another peer or relying on it for quorum.

For example:

```sh
keylane-meta-ctl --socket /var/lib/keylane/meta-1/meta-admin.sock \
  addsrv 4 10.0.0.14:7100
```

The leader first commits and audits the member identity binding, then invokes
NuRaft `add_srv`. `ERR joining` and `ERR config-changing` mean the caller should
wait, re-check the leader and both members, and retry the same operation.
`ERR already-exists` may mean an earlier invite committed; verify the joiner
rather than creating a different identity.

To remove a peer, select a follower and run this on the leader:

```sh
keylane-meta-ctl --socket /var/lib/keylane/meta-1/meta-admin.sock removesrv 4
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

Meta has one exact durable format and does not support a mixed-version schema
window or an in-band format switch. For a binary-only change that preserves the
format, replace one follower at a time, wait for catch-up, and replace the
leader last. Before any replacement, back up every member and record the
membership, term, commit index, and snapshot index.

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
system, store it durably, and deduplicate with cluster identity, Raft log
index, and record hash. Only after that acknowledgement should an operator issue
`pruneaudit <through-index>`. The prune is replicated and advances the chain
anchor; there is no in-process record of the external acknowledgement. At a
full window, overlapping prune attempts are rejected until the outstanding
prune's Raft outcome resolves, including when its client has already timed out.

Terminal operations may be moved into bounded archive summaries by the model's
archive command. `exportoperations` returns all current summaries as a
versioned hex blob. After durable external storage, `pruneoperations <seq>...`
removes exactly the listed summaries through a replicated command. Pruning a
summary ends the local late-retry tombstone window for that operation id, so
retention must cover the clients' documented retry horizon.

The current line protocol returns export bytes as hexadecimal on one line.
Protect these responses as audit data and avoid terminal logging that could
copy principals or operation results into an ungoverned sink.
