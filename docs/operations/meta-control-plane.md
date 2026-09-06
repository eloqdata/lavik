# Meta control plane operations

## Provision identities before startup

Run a typical deployment as three separate `keylane_meta` processes with
independent durable data directories. Provision a shared operator-owned CA and
a distinct key and certificate for every Meta member. Member `N` must present
exactly one URI SAN `keylane://meta/N`, plus an IP or DNS SAN covering the
advertised address; do not copy one member certificate to another node.
Protect the CA key offline and restrict each node key and data directory to the
service account.

Start the first member with its own advertised numeric address and `--bootstrap`:

```sh
keylane_meta \
  --id 1 --addr 10.0.0.11:7100 --data-dir /var/lib/keylane/meta-1 \
  --bootstrap \
  --tls-ca /etc/keylane/meta-ca.pem \
  --tls-cert /etc/keylane/meta-1.pem \
  --tls-key /etc/keylane/meta-1-key.pem \
  --ctl-allow-uid 991
```

The local control endpoint defaults to `<data-dir>/meta-admin.sock`, is created
with mode 0600, authenticates through `SO_PEERCRED`, and permits the process
UID unless `--ctl-allow-uid` is repeated explicitly. Use `--ctl-socket` to
choose another path, but keep its parent directory owned by the service account
and not group- or world-writable; startup rejects an unsafe parent. To expose
administration over TCP, replace the Unix endpoint with `--ctl-addr` and
provide all three `--ctl-tls-*` files; plaintext TCP administration is
rejected, including on loopback.

Raft mutual TLS is mandatory in normal startup. The
`--unsafe-allow-plaintext-raft` escape hatch exists only for isolated tests and
must not be used in a deployment. `--raft-io-threads` sizes NuRaft's native
Asio pool (default 2); it does not change the single Celer control-session
worker or make WAL synchronization asynchronous.

Start additional members without `--bootstrap`, then ask the current leader to
add each identity and endpoint:

```text
addsrv 2 10.0.0.12:7100 keylane://meta/2
addsrv 3 10.0.0.13:7100 keylane://meta/3
```

The add operation commits the identity binding before changing Raft membership.
`removesrv N` performs the inverse order: Raft removal first, then retirement
of the binding. Treat `ERR joining` and `ERR config-changing` as retryable only
after checking cluster status; never bypass the identity step manually.

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
