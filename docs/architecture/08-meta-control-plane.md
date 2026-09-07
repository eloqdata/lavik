# Meta control plane

## Boundary and state model

`keylane-meta` is a separate C++ process for durable cluster metadata. It
embeds NuRaft and uses its native Asio service for Raft peer sockets, timers,
and TLS. One Celer worker owns the administrative and future data-node control
sessions; a bounded proposal executor keeps synchronous NuRaft API entry and
WAL I/O off that worker, and a mailbox returns completions to it. The main
Keylane data-plane executable remains Raft-free. The future
authenticated data-node control session belongs to issue #20; this module
exposes the in-process coordinator and observation seams that session will use.

The state machine owns one `MetaStores` value containing six committed stores:

| Store | Durable responsibility |
|---|---|
| Identity | Data-node certificate principal bindings and retired identities; Meta-member principal bindings |
| Topology | Groups, membership, owners, epochs, manifests, and slot ranges |
| Policy | Versioned, content-addressed policy documents and retirement state |
| Grant | Group terms and authority grants, including fencing and lease parameters |
| Operation | Idempotent operation lifecycle records plus exported/prunable terminal summaries |
| Audit | Log-index-ordered command verdicts in a bounded hash chain |

`ApplyCommitted` is the only mutation path. It dispatches absolute-value and
revision-checked commands and enforces cross-store invariants such as unique
principals, one data group per node, monotonic terms and epochs, and valid
policy/grant/operation references. Membership and endpoint changes advance the
same cluster topology epoch as owner and slot changes. Population-manifest and
partition-replication changes are a single CAS command that also advances that
epoch. Non-terminal operations persist structured policy dependencies and an
optional replication-history binding; phase evidence is accepted only when
its node, operation, group term, manifest, and history match committed facts.
A domain-invalid command consumes its Raft index, leaves the requested domain
state unchanged, and records the rejection in the audit chain. Unknown
commands, corrupt durable bytes, contradictory replay at an existing index,
and impossible apply ordering fail stop. Replaying the same entry at the same
index is idempotent and produces the same verdict and audit record;
correctness does not depend on apply running only once.

All model collections, command fields, snapshots, active operations, archived
summaries, policy bytes, and the audit window have explicit bounds. Snapshot
and uncompacted-WAL bounds reject new proposals with `RESOURCE_EXHAUSTED`.
Audit retention is replicated: the default bounded-rotate mode evicts the
oldest entry and records durable loss watermarks, disabled mode suppresses
ordinary records while retaining policy transitions, and strict-export mode
rejects proposals at capacity until an operator exports and prunes. In strict
mode the coordinator reserves capacity until each Raft proposal actually
resolves, including after a local timeout, and serializes prunes so overlapping
proposals cannot overbook the window.

## Proposal and observation flows

`MetaCoordinator` is the in-process API used by reconcilers and the current
administrative adapter. `Propose` accepts model commands rather than NuRaft
types. On the leader it takes one atomic committed view, applies fail-safe and
registered semantic validation, injects the authenticated principal and a
readable proposal time, encodes the current durable format, submits to
Raft through the proposal executor, and returns the apply result carried by
NuRaft's completion. It never re-reads a record that bounded audit rotation may
already have evicted. Followers return a not-leader status without appending.
Membership workflows hold one exclusive leader-local lease through completion,
so NuRaft never receives overlapping configuration changes.

Committed subscribers atomically receive a complete `CommittedView`, its
cursor, and a bounded ordered subscription. Replay can redeliver an index, so
consumers deduplicate by index. Queue overflow cancels the subscription and
requires resynchronization from a new full view. Role callbacks start
reconcilers only after NuRaft has caught the state machine up; losing leadership
cancels them and waits for completion.

`MetaObservationStore` is deliberately outside `MetaStores`: it is volatile,
leader-local evidence and is never encoded into a command, WAL, snapshot, or
committed subscription. Admission authenticates the tuple `(node identity,
boot incarnation, controller-local session generation)` and accepts only the
current generation. A new generation atomically removes the node's older
observations. Candidate and operation evidence must equal the current
committed term, manifest, history, operation, and registration anchors. The
same committed anchors are rechecked deterministically when evidence is
embedded in an operation-phase command, closing the race between leader-local
validation and Raft apply. Committed changes proactively purge stale evidence,
and queries filter again against one current committed snapshot. Startup and
every leader/follower edge clear sessions, observations, and their local
diagnostic ring.

## Durability and recovery

The durable source of truth is the newest completed state-machine snapshot plus
the following Raft WAL. Snapshot capture is serialized with commit and copies
an exact applied-index cut; a writer thread performs serialization and file I/O
after capture. A snapshot becomes eligible for log compaction only after its
atomic durable publication succeeds. Incoming snapshots are size-bounded,
decoded completely, and installed synchronously as one replacement state.

WAL v2 uses checksum-protected `log-<first-index>.seg` files. Segments roll at
a size trigger. Compaction writes the complete surviving suffix to a synced
`compact-<first-index>.ready` intent before replacing the old segment set;
startup finishes such an intent after a crash. A reported pre-publication
failure leaves both the live index and old segments authoritative. Append
batches become durable at NuRaft's flush hooks;
membership state and vote state use atomic rename plus file and directory
sync. Recovery retains the intact contiguous prefix and truncates a torn tail.
The older prototype's `raft_log.dat` and `LSN1` snapshots are intentionally
incompatible and cause startup to fail with an explicit migration error.

The persisted state-machine watermark is the snapshot index, not every applied
WAL index. After restart, a post-snapshot tail remains invisible until Raft
legally reconfirms it with a current-term quorum; depending on the elected
leader, it is then committed as a prefix or overwritten. With no quorum the
Meta plane is unavailable rather than exposing an unconfirmed decision.
Client timeouts therefore mean an uncertain outcome and must be resolved by
the operation's stable idempotency key.

## Format compatibility

Commands, records, exports, and snapshots carry one exact format marker.
Keylane Meta does not negotiate durable formats between mixed binary versions
and has no in-band schema-switch command. A release that changes an
incompatible format requires coordinated replacement of the Meta cluster;
pre-release data from the superseded format is recreated rather than migrated.
Readers reject unknown markers and trailing bytes so incompatible state fails
at startup or replay instead of being interpreted approximately. The
independent segmented-WAL marker follows the same fail-loudly rule.

## Authentication, membership, and audit

Raft transport is plaintext by default, matching the data-plane deployment
model. It still checks claimed source and destination ids against NuRaft
configuration descriptors and committed identity-store bindings, but those
claims are not cryptographically authenticated; deployments whose network is
not fully trusted enable optional mutual TLS. With mTLS, every member
certificate has exactly one canonical `keylane://meta/<server-id>` URI SAN and
an IP or DNS SAN covering its advertised endpoint. The NuRaft configuration
identity descriptor, the CA-authenticated certificate, and the committed
identity-store binding must all match the claimed source id; neither the
configuration nor the store binding grants membership alone. With mTLS, a
pristine joiner temporarily relies on the authenticated certificate and
invited configuration until it installs the leader's configuration and its
first state-machine entry or snapshot. Plaintext deployments instead rely on
network isolation during that bootstrap interval.

Dynamic membership preserves the applicable configuration and identity-store
bindings. Add commits the member binding before `add_srv`; removal commits
`remove_srv` before retiring the binding.
The retired binding also disambiguates the short interval after removal commits
but before NuRaft publishes its new in-memory configuration. Reactivation of
retired principals is rejected. Data-node identities use canonical
`keylane://node/<node-id>` principals with global one-to-one binding.

Local administration uses a mode-0600 Unix socket and derives a canonical
operator actor from Linux `SO_PEERCRED`, constrained by an explicit UID
allowlist. Its parent directory must not be group- or world-writable, and the
listener records the bound inode so shutdown never unlinks a replacement path.
Remote administration requires mutual TLS even on loopback and uses the peer's
canonical URI SAN. Role-based authorization separates operators, Meta members,
and data-node self-reporting; actor fields on the wire are never trusted.
Certificate validity is enforced by TLS, but online issuance, rotation, CRL,
and OCSP integration are outside this module.
The one-shot `keylane-meta-ctl` operator client speaks the same ordered line
protocol over either transport, verifies the remote server certificate, and
keeps RESP and NuRaft dependencies out of the client.

Every privileged committed command creates a deterministic audit record keyed
by Raft log index. Records include the injected actor, proposal time, command
summary, verdict, and a rolling hash linked to the previous record. Exports
carry the preceding anchor and record hashes so an external archive can verify
continuity and deduplicate by cluster, log index, and record hash. Pruning
advances the committed chain anchor only through an explicitly named record.
`SetAuditPolicy` is itself replicated and always audited, including a
transition into or out of disabled mode.

## Source map

| Claim | Repository source |
|---|---|
| Commands, encoding, store composition, deterministic apply, and bounds | `src/meta/meta_commands.*`, `src/meta/meta_encoding.*`, `src/meta/meta_*_store.*`, `src/meta/meta_state_apply.*` |
| Exact-cut snapshots and committed state-machine lifecycle | `src/meta/meta_state_machine.*` |
| Coordinator proposal, subscription, role, and fail-safe behavior | `src/meta/meta_coordinator.*` |
| Volatile observation admission and freshness | `src/meta/meta_observation_store.*` |
| TLS identity, RBAC, Unix peer credentials, and administrative protocol | `src/meta/meta_identity_verifier.*`, `src/meta/meta_ctl_server.*`, `app/keylane_meta.cpp`, `app/keylane_meta_ctl.cpp`, `celer/src/net/` |
| Raft WAL, vote/config state, native Asio hooks, proposal executor, and Celer completion bridge | `src/meta/nuraft_log_store.*`, `src/meta/nuraft_state_mgr.*`, `src/meta/nuraft_asio_transport.*`, `src/meta/meta_proposal_executor.*`, `src/meta/nuraft_scheduler.*`, `third_party/patches/nuraft/` |
| Recovery, partition, membership, and security gates | `tests/meta_*`, `tests/meta_integration/` |
