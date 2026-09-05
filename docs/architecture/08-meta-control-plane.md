# Meta control plane

## Boundary and state model

`keylane_meta` is a separate C++ process for durable cluster metadata. It
embeds NuRaft but replaces NuRaft's asio networking and timers with Celer
adapters. The main Keylane data-plane executable remains Raft-free. The future
authenticated data-node control session belongs to issue #20; this module
exposes the in-process coordinator and observation seams that session will use.

The state machine owns one `MetaStores` value containing six committed stores
and the active write schema:

| Store | Durable responsibility |
|---|---|
| Identity | Data-node certificate principal bindings and retired identities; Meta-member principal and schema-range bindings |
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
summaries, policy bytes, and the audit window have explicit bounds. The leader
rejects new privileged proposals with `RESOURCE_EXHAUSTED` before an audit,
snapshot, or uncompacted-WAL bound can be crossed. Replicated prune commands
are the escape valves after an operator has durably stored the corresponding
export. The coordinator reserves audit capacity until each Raft proposal
actually resolves, including after a local client timeout, and serializes audit
prunes so overlapping proposals cannot overbook a full window.

## Proposal and observation flows

`MetaCoordinator` is the in-process API used by reconcilers and the current
administrative adapter. `Propose` accepts model commands rather than NuRaft
types. On the leader it takes one atomic committed view, applies fail-safe and
registered semantic validation, injects the authenticated principal and a
readable proposal time, encodes with the committed write schema, submits to
Raft, and returns the committed apply verdict. Followers return a not-leader
status without appending.

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
a size trigger and compaction removes or rewrites the prefix through a durable
snapshot boundary. Append batches become durable at NuRaft's flush hooks;
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

## Schema compatibility

Commands and snapshots carry a schema version. A binary reads the current and
immediately preceding schema, while the committed `active_write_schema`
selects what leaders emit. A new binary continues to write the old schema
during a rolling deployment. `SetSchemaVersion` itself remains encoded in the
oldest readable form and is admitted only when every member descriptor in the
committed Raft configuration attests support for the target. Before any Raft
request, peers exchange their binary-compiled schema ranges; the authenticated
peer range must exactly match its committed descriptor and must contain the
active write schema. Thus operator-supplied membership data cannot make an old
process appear compatible. After the switch, a binary that cannot read the
active schema fails loudly and its transport handshake prevents it from being
added to membership.

## Authentication, membership, and audit

Raft transport requires mutual TLS by default. Every member certificate has
exactly one canonical `keylane://meta/<server-id>` URI SAN. The NuRaft
configuration identity descriptor, the CA-authenticated certificate, and the
committed identity-store binding must all match the claimed source id; neither
the configuration nor the store binding grants membership alone. A pristine
joiner temporarily relies on the configuration descriptor and certificate
until it installs its first state-machine entry or snapshot.

Dynamic membership preserves that conjunction. Add commits the member binding
before `add_srv`; removal commits `remove_srv` before retiring the binding.
Reactivation of retired principals is rejected. Data-node identities use
canonical `keylane://node/<node-id>` principals with global one-to-one binding.

Local administration uses a mode-0600 Unix socket and derives a canonical
operator actor from Linux `SO_PEERCRED`, constrained by an explicit UID
allowlist. Its parent directory must not be group- or world-writable, and the
listener records the bound inode so shutdown never unlinks a replacement path.
Remote administration requires mutual TLS even on loopback and uses the peer's
canonical URI SAN. Role-based authorization separates operators, Meta members,
and data-node self-reporting; actor fields on the wire are never trusted.
Certificate validity is enforced by TLS, but online issuance, rotation, CRL,
and OCSP integration are outside this module.

Every privileged committed command creates a deterministic audit record keyed
by Raft log index. Records include the injected actor, proposal time, command
summary, verdict, and a rolling hash linked to the previous record. Exports
carry the preceding anchor and record hashes so an external archive can verify
continuity and deduplicate by cluster, log index, and record hash. Pruning
advances the committed chain anchor only through an explicitly named record.

## Source map

| Claim | Repository source |
|---|---|
| Commands, encoding, store composition, deterministic apply, and bounds | `src/meta/meta_commands.*`, `src/meta/meta_encoding.*`, `src/meta/meta_*_store.*`, `src/meta/meta_state_apply.*` |
| Exact-cut snapshots and committed state-machine lifecycle | `src/meta/meta_state_machine.*` |
| Coordinator proposal, subscription, role, and fail-safe behavior | `src/meta/meta_coordinator.*` |
| Volatile observation admission and freshness | `src/meta/meta_observation_store.*` |
| TLS identity, RBAC, Unix peer credentials, and administrative protocol | `src/meta/meta_identity_verifier.*`, `src/meta/meta_ctl_server.*`, `src/meta/meta_main.cpp`, `celer/src/net/` |
| Raft WAL, vote/config state, RPC, scheduler, and Celer bridge | `src/meta/nuraft_log_store.*`, `src/meta/nuraft_state_mgr.*`, `src/meta/nuraft_rpc_*`, `src/meta/nuraft_scheduler.*` |
| Recovery, upgrade, partition, membership, and security gates | `tests/meta_*`, `tests/meta_integration/` |
