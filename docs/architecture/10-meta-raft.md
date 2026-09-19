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

# Meta Raft runtime

## Boundary and ownership

`lavik-meta` embeds etcd-io/raft through a Go C archive. `MetaRaft` is its C++
ownership boundary: proposals, immutable status, role edges, and owned byte
buffers cross a versioned C ABI; no Go pointer is retained by C++. The C++
`MetaStateMachine` owns the six business stores and deterministic command
semantics. Bycorf continues to own Admin and Data sessions. The Data executable
and `lavik-ctl` do not link Go or Raft.

One Go event loop exclusively owns `RawNode`, its memory log, configuration,
proposal reservations, and quorum proofs. It does not perform filesystem I/O,
wait for sockets, call a C++ state getter, or acquire an application/storage
mutex. Its bounded role callback records an ordered edge and wakes Bycorf.

| Owner | Responsibility |
|---|---|
| Protocol loop | Tick, Step, Ready, memory log, admission, configuration, completion dependencies |
| Ordered append executor | etcd WAL writes/sync, term/vote, snapshot publication markers, join evidence |
| Application executor | Serial C++ apply/advance, exact-cut capture, atomic snapshot installation, identity projection |
| Bulk executor | Prepare both captured and received checksummed snapshot files, rename and directory sync |
| Reclamation executor | Delete covered, unlocked WAL segments and obsolete snapshot files; directory sync |
| Per-peer connection goroutines | Separate control, log, and snapshot streams, encoding, TLS and reconnect |

The append executor may group adjacent ordinary tasks into one durable write.
It preserves entry order and the final HardState, then releases each original
completion in order. Snapshot and join transactions are separate barriers.
No disk-owned lock participates in a heartbeat path. A disk stall may stop
writes or elections that require a new durable term, while an established
term continues exchanging heartbeats.

## Consensus and application dependencies

The integration uses `AsyncStorageWrites`. `MsgStorageAppend` and
`MsgStorageApply` are local executor work, never accepted from the network.
Their attached responses are released only after the corresponding work has
actually completed; `RawNode.Advance` is never used in this mode. Response-only
append tasks retain the same ordered durability barrier. A local persistence
error revokes authority and fails the process before any dependent success.

WAL persistence, committed Raft state, and actual C++ application are distinct
frontiers. `MemoryStorage` advances after real disk completion. A received
snapshot's completion additionally waits for C++ installation. The application
cursor advances for configuration and no-op entries as well as business
commands. Client success includes the actual command verdict; a timeout,
demotion, or cancellation leaves an uncertain outcome that must be reconciled
by the existing operation idempotency key.

RawNode restores its quorum configuration when it accepts an incoming snapshot,
ahead of persistence and C++ installation. Older application jobs still drain
in order, but covered configuration completions cannot modify that restored
configuration. Local snapshot capture waits for installation to publish a
matching application, descriptor, and configuration cut.

A leader becomes usable only after applying an entry from its current term and
obtaining a fresh voter majority. This is an application fence, not perpetual
equality between applied and commit: ordinary pipelining does not repeatedly
withdraw Data authority. Peer application progress is always reported; only
Cluster Create and learner promotion impose their explicit catch-up barriers.
Ordinary writes retain durable majority semantics.

## Transport and authority

Each destination has independent control, log, and snapshot TCP connections and
queues. `LRT1` handshakes bind the source, destination, lane and connection
instance; length-prefixed protobuf frames cannot carry local storage messages
or embedded executor responses. The transport rechecks the immutable authorized
peer view and accepted connection before delivery, and the protocol owner checks
membership again before Step. Removed or retired peers cannot use queued traffic
to regain membership.

Optional mTLS validates the CA chain, exactly one URI SAN naming the canonical
`lavik://meta/<id>` identity,
and the outgoing endpoint certificate name. Plaintext preserves the same
membership checks but relies on network isolation for authentication. Advertised
routes are separate from listener binds and may name proxies.

Heartbeat context preserves Raft's opaque context behind a Lavik header carrying
a boot nonce, sequence, term and actual applied index. The leader accepts a reply
only for an outstanding challenge in the same term and connection generations.
Freshness expires from the original send time; receiving a delayed response
cannot grant a new full interval. Duplicate, old-boot, old-term, expired and
reconnected-stream replies do not renew authority. Published evidence is also
invalidated by authorization revocation or connection replacement. Log responses alone cannot
sustain quorum liveness. Expired authority stays revoked for that leadership
epoch even if delayed traffic arrives later.

The validity interval is `D = heartbeat interval * election ticks`. The C++
Data-control suspend detector and finite-lease `2D` handoff quarantine remain
part of the authority boundary: Go's monotonic clock does not account for host
suspend. Resignation revokes the C++ role immediately. A generation carried through the
C ABI fences older role callbacks until the protocol owner acknowledges it. A sole voter may reopen
only after a full active interval, through the existing Data quarantine.

## Durable root and recovery

Meta owns one current storage layout directly under its configured data
directory. WAL and snapshot files use etcd's native encodings:

| Path | Durable meaning |
|---|---|
| `RAFT` | Immutable local identity and exact initial Meta vector; duplicated in WAL metadata |
| `STARTED` | One-way evidence published before the first consensus write |
| `JOIN` | Optional immutable invitation and committed configuration cut for a waiting joiner |
| `wal/*.wal` | etcd checksummed WAL: entries, HardState and published snapshot markers |
| `snap/*.snap` | etcd checksummed snapshots: Raft index/term/configuration plus the `LMS1` descriptor/application envelope |

A pristine directory with a manifest bootstraps exactly one, three or five
voters. A pristine directory without one is a waiting joiner with no voter
configuration. A manifest is rejected on restart. Missing or contradictory
root/startup evidence fails closed; a nonempty or damaged directory
never becomes fresh genesis automatically.

Capture runs between application jobs at an exact applied index and includes
that cut's Raft configuration and descriptors. The bulk executor stages a
checksummed file, renames it, and syncs its directory. Only then may the ordered
append executor write and sync the WAL snapshot marker. A locally captured cut
cannot exceed durable commit. A received image's marker becomes durable before
the HardState that advances commit past the old log; recovery selects a marker
only when durable commit covers it. If the same append includes a log suffix,
the snapshot cut's commit is durable before writing that suffix, so a partial
suffix cannot leave a gap behind an ineligible recovery image. Memory compaction
and unlock of covered WAL segments follow the complete publication transaction.
Reclamation deletes whole unlocked segments while
retaining the boundary predecessor and the newest two published-or-older images;
it never rewrites the surviving log suffix. A held message owns its snapshot
bytes independently of file reclamation.

Recovery requires the newest published snapshot, verifies its configuration and
checksum, and replays the WAL's persisted committed suffix through C++ before
accepting traffic or ticking. Unpublished prepared files do not become roots.
The uncommitted suffix remains Raft's responsibility and may be replaced.
Snapshot-only log freshness uses the last-included term/index, including after
restart; compaction never makes an older candidate current. Corruption of the
published image does not silently fall back to an older one.

Genesis descriptors substitute for missing identity bindings only until all
initial bindings have been applied. Retired bindings prove completion but never
authorize a connection. A waiting joiner's durable invitation is transport-only
grace below its configuration cut; it does not install voters. Ordinary restart
replays committed identity/configuration state before opening peer ingress.
Snapshot metadata and the installed identity projection close the same grace
windows, so partial installation cannot resurrect old voters or retired IDs.

Dynamic membership keeps the existing C++ durable workflow. Add commits the
identity, adds a learner, waits for durable replication and fresh actual
application, then promotes it. Remove observes the applied configuration before
retiring the identity. A recovered workflow accepts the exact baseline,
expected learner intermediate state, or intended final configuration; it never
infers a different membership request from network input. After prefix compaction,
a configuration change refreshes the snapshot even on an idle cluster: a new
learner cannot restore an older image that omits its member ID.

## Bounds, failure and lifecycle

Input and output lanes have independent byte and count reservations. Application
proposals, pending durable work, retained logs, and executor responses are
bounded separately. One incoming snapshot keeps its reservation through install;
one local capture may be in progress alongside it. The application image cap is
512 MiB. Snapshot encode/decode/copy stages retain a bounded number of images,
so deployments must allow multiple image-sized allocations at that cap.
Saturated log queues cannot consume control reservations. Saturation stops new
storage-dependent work, including new local election terms, while same-term
heartbeat processing remains available. An authenticated higher term revokes
authority immediately even at saturation; one retained maximum-term observation
waits for ordered persistence capacity, without acknowledging the rejected RPC.

Failed local snapshot preparation preserves the previous recovery root and
increments the existing snapshot-failure guard. Publication, WAL, replay or
application failure is fail-stop. GC errors preserve the root, are observable,
and retry asynchronously; they do not report successful reclamation. Status
separates retained logical log bytes, pending durability bytes, snapshot/first
log indices, and transport/GC failures.

Shutdown seals admission and drains existing submitters before resolving queued
work. It revokes role authority, resolves pending local
results, closes transports, joins append/application/bulk/GC owners and their
callbacks, then closes WAL. An uninterruptible filesystem call can delay joining
but cannot preserve authority. C++ drains its foreign-executor producers before
stopping Bycorf; callback owners remain alive throughout that drain.

Meta commands and business-store encodings retain their current semantics.
The Go toolchain and module checksums are pinned; etcd is a Go Module dependency,
not a submodule or an external service.

## Source map

| Claim | Repository source |
|---|---|
| C++/Go lifetime, callbacks and status | `include/lavik/meta/raft*.h`, `src/meta/raft.cpp`, `raft/bridge/` |
| Ordered protocol and executor dependencies | `raft/engine/core.go`, `runtime.go`, `snapshot.go` |
| Durable format, recovery and GC | `raft/engine/storage.go`, `gc.go`, `join.go` |
| Peer authentication, lane isolation and freshness | `raft/engine/transport.go`, `identity.go`, `liveness.go`, `budget.go` |
| Configuration workflow and learner promotion | `raft/engine/membership.go`, `src/meta/membership_reconciler.cpp` |
| Failure, recovery and isolation contracts | `raft/engine/*_test.go`, `tests/meta_state_machine_test.cpp`, `tests/meta_integration/` |
