# Meta control plane

## Boundary and state model

`keylane-meta` is a separate C++ process for durable cluster metadata. It
embeds NuRaft and uses its native Asio service for Raft peer sockets, timers,
and TLS. One Celer worker owns the administrative listener and the
process-lifetime Data-control listener; a bounded proposal executor keeps
synchronous NuRaft API entry and WAL I/O off that worker. NuRaft and
proposal-executor threads return typed notifications or coroutine handles
through Celer's foreign MPSC mailbox, which reuses the worker's normal wake
sequence and eventfd. The main Keylane data-plane executable remains
Raft-free. Followers keep accepting Data connections long enough to return the
committed member directory and leader hint. Only a caught-up leader installs
the publisher that may create sessions, project desired state, evaluate lease
challenges, or accept results. Demotion cancels that publisher and begins
closing all sessions from its leadership generation before the coordinator
reports the transition complete. Shutdown first quiesces those sessions and
all NuRaft/proposal-executor producers, waits for the foreign executor's
accepted prefix to reach the Meta worker, and only then stops the generic Celer
runtime. An active demotion or first shutdown drain is fail-stop if the worker
mailbox cannot accept its notification: reporting success would permit a later
leader epoch to reuse authority that was never revoked. Once shutdown has
synchronously drained the worker, later reconciler cancellation and object
destruction are no-ops and do not depend on a still-running executor.

The state machine owns one `MetaStores` value containing seven committed
stores:

| Store | Durable responsibility |
|---|---|
| Identity | Data-node certificate principal bindings and retired identities; Meta-member principal bindings |
| Topology | Groups, membership, owners, epochs, manifest references, and slot ranges |
| Policy | Versioned, content-addressed policy documents and retirement state |
| Grant | Group terms and authority grants, including fencing and lease parameters |
| Operation | Idempotent operation lifecycle, current directives, durable terminal receipts, and exported/prunable terminal summaries |
| Population manifest | Immutable, content-addressed partition/epoch documents and explicit pruning |
| Audit | Log-index-ordered command verdicts in a bounded hash chain |

`ApplyCommitted` is the only mutation path. It dispatches absolute-value and
revision-checked commands and enforces cross-store invariants such as unique
principals, one data group per node, monotonic terms and epochs, and valid
policy/grant/operation references. Group membership, owner, slot, population-
manifest, partition-replication, and `UpdateNode` endpoint changes advance the
cluster topology epoch. Initial identity registration can be projected at
epoch zero before any group exists. The administrative membership proposer,
not operator input or deterministic apply, generates each assignment
incarnation from the OS CSPRNG; the topology store's bounded last-value index
only catches direct replay. Non-terminal operations persist structured policy
dependencies and an
optional replication-history binding; phase evidence is accepted only when
its exact `(group, node, assignment)` membership incarnation, reporter boot,
operation, group term, manifest, partition replication epoch, and history
match committed facts. Those identity anchors remain in the durable evidence
summary rather than being reconstructed from the node's membership at apply.
Directive validity is a continuously maintained committed invariant, not only
an admission check. After every accepted command, apply deterministically
rechecks each bounded live directive against the exact active source and
target assignments, group term, authority and grant revisions, and population
manifest/partition epoch. It removes only the stale attempts and advances each
affected operation's phase revision once, forcing reconcilers with an older
CAS view to reload. This catches source or target removal/reassignment,
term/authority/grant changes, fencing or revocation, and population changes
without a command-specific cleanup list. Replaying the anchor mutation sees
the directive already absent and is a no-op; snapshot decode rejects a stale
directive as corrupt aggregate state.
A domain-invalid command consumes its Raft index, leaves the requested domain
state unchanged, and records the rejection in the audit chain. Unknown
commands, corrupt durable bytes, contradictory replay at an existing index,
and impossible apply ordering fail stop. Replaying the same entry at the same
index is idempotent and produces the same verdict and audit record;
correctness does not depend on apply running only once.

All model collections, command fields, snapshots, active operations, archived
summaries, policy bytes, and the audit window have explicit bounds. An
unreferenced population-manifest insertion is charged against the exact bytes
remaining in the aggregate snapshot, including headroom for its audit record;
the decode count ceiling is derived from that durable byte limit rather than
an unrelated collection limit. An oversized snapshot fails that snapshot
round. Once uncompacted WAL or consecutive snapshot-failure guards fire, the
coordinator rejects ordinary proposals with `RESOURCE_EXHAUSTED`. It simulates
an explicit recovery command against one committed view and admits exactly one
whose effect advances the bounded recovery chain: empty-result terminalization
of a live operation, movement of terminal records into the archive, removal of
existing archived summaries or terminal receipts, removal of an existing
unreferenced manifest, or an audit prune whose serialized window is smaller
even after its own audit record. A no-op prune, stale revision, nonempty result,
or other nominally whitelisted command is rejected before Raft append.
The recovery reservation follows the actual NuRaft proposal until it resolves,
even if its caller times out, so another recovery step cannot overtake an
uncertain outcome. After enough state is removed, a successful snapshot clears
the snapshot/WAL pressure rather than Data-local state synthesizing authority.
Snapshot decode revalidates group-set and authority-anchor lockstep plus every
active identity, policy, and manifest reference before exposing the recovered
aggregate. An active authority additionally requires nonzero term and config
epochs, matching the Data-control serving representation.
Audit retention is replicated: the default bounded-rotate mode evicts the
oldest entry and records durable loss watermarks, disabled mode suppresses
ordinary records while retaining policy transitions, and strict-export mode
rejects proposals at capacity until an operator exports and prunes. In strict
mode the separate audit-capacity reservation also follows each Raft proposal
until it actually resolves, including after a local timeout, so overlapping
proposals cannot overbook the window.

## Proposal and observation flows

`MetaCoordinator` is the in-process API used by reconcilers, the Data-session
publisher, and the administrative adapter. `Propose` accepts model commands
rather than NuRaft types. On the leader it takes one atomic committed view,
applies fail-safe and
registered semantic validation, injects the transport-authorized actor and a
readable proposal time, encodes the current durable format, submits to
Raft through the proposal executor, and returns the apply result carried by
NuRaft's completion. It never re-reads a record that bounded audit rotation may
already have evicted. Followers return a not-leader status without appending.
Membership workflows hold one exclusive leader-local lease through completion,
so NuRaft never receives overlapping configuration changes.

Committed subscribers atomically receive a complete `CommittedView`, its
cursor, and a bounded ordered subscription. Replay can redeliver an index, so
consumers deduplicate by index. Queue overflow cancels the subscription and
requires resynchronization from a new full view. Each NuRaft role callback
synchronously records its exact edge in `MetaLeadershipRelay` before scheduling
a Celer drain, so a stalled worker or coordinator cannot collapse a rapid
Leader/Follower/Leader sequence into its final role. The relay also preserves
edges racing startup attachment and makes shutdown detachment a lifetime
barrier. `MetaCoordinator` consumes one ordered event queue for reconciler
registration and role edges. A Follower event always cancels and joins every
leader reconciler, then invalidates volatile observations, before a later
Leader event can restart anything. Promotion still waits for NuRaft to catch
the state machine up.

`MetaControlProjector` is a pure function over one atomic committed view. It
produces a canonical node-specific `FullDesiredState`: the global Meta/Data
directories and topology, each group's partition replication epoch, that
node's group/authority policy, referenced population manifests and policies,
and live directives whose explicit
recipient is that node. The source applied index is an ordering/diagnostic
watermark; SHA-256 of the canonical semantic projection is the dependency used
by leases and directives. The publisher sends a full projection on session
acceptance and whenever that hash changes. Protocol v1 has no delta format, so
an index advance with identical content does not create network churn and a
reconnect never depends on retained incremental history.

The Data-control wire protocol has a fixed versioned header, per-direction
sequence, payload length, and CRC32C. Frames are bounded to 16 KiB. Larger
objects use Start/Chunk/End with a declared total length and SHA-256; desired
state is capped at 512 MiB and individual opaque directive, result, or
operation-evidence fields at 256 KiB. These are defensive ceilings rather than
expected object sizes:
16 KiB keeps small authority messages atomic and bounds per-frame latency;
256 KiB equals the durable Meta payload-field cap; and the 512 MiB hard ceiling
matches the snapshot size class so malformed projections cannot allocate
without limit. The writer admits four frame sizes, enough for one outstanding
frame in each authority, reliable, bulk, and soft class. Chunks have no
stop-and-wait acknowledgement. The serialized writer schedules authority and
reliable frames ahead of bulk chunks between kernel writes, while one reader
and disabled socket read-ahead keep each session's memory ownership explicit.
Only one complete-object transfer is active in a direction at a time; queued
transfers retain shared ownership of their encoded bytes, so Start/Chunk/End
sequences cannot interleave or outlive their payload storage.
Connect, handshake, session progress, and individual socket writes are bounded
at ten seconds; this is well above the default 100 ms heartbeat and 300--600 ms
election cadence while still turning a stuck peer into a finite failure.

Heartbeat is the periodic Data-to-Meta observation message. It contains
health, optional boot-scoped candidate progress, and at most one lease
challenge. A session accepts only the next business sequence or an exact replay
of the previous heartbeat; an exact replay gets the cached exact ack. Data
quiesces heartbeat projection reads during an FDS replacement. Any outstanding
ack for the old object is consumed without applying its lease decision, and
production resumes only after the new object is acknowledged.
Candidate progress names the authenticated reporter's exact committed member
assignment, term, manifest, partition replication epoch, and boot history.
Meta rejects reports from active nodes that are not members of the named group
and old assignment proofs after remove/re-add; group queries retain the
reporter identity with each proof. Health/candidate ingestion is independent
of challenge validation, so a bad
renewal request cannot hide useful liveness evidence. Candidate history is
bound to the history announced in `ClientHello` for that boot session.
Challenges name the exact projection and complete group authority anchor.
Meta grants only while it remains the caught-up leader, and caps duration at
both committed policy and the configured leadership-validity bound. An
otherwise-valid first grant for a new group/boot/anchor/leadership identity is
held behind a leader-local monotonic quarantine for that full bound; losing
volatile quarantine evidence restarts the wait rather than recovering an
unsafe wall-clock deadline. Data measures a granted duration from the
monotonic instant immediately before its first heartbeat write, not from ack
receipt.

Directives separate the wire recipient from the rebuild target: rebuild is
delivered to the target, while authorize/revoke is delivered to the source.
The common assignment field always names the target membership incarnation;
the durable directive carries a separate source assignment and the committed
partition replication epoch. Both must exactly match committed topology and
the installed group view.
Operation, durable directive, execution attempt, and assignment-incarnation
identities remain distinct. Assignment ids are proposer-generated
128-bit values that are never reused across incarnations; the topology store
retains the most recent value per node to reject direct remove/re-add replay
without growing an unbounded historical set. Accepted, Started, and Completed
receipts are session observations. A pre-start controller rejection omits
Started and moves directly from Accepted to Completed; Meta requires its result
status to be rejected. Once Started was observed, a non-success result is an
execution failure rather than a rejection. The terminal result is authoritative
only after Meta commits an exact `MetaTerminalReceipt` through Raft and responds
`ResultCommitted`. That first commit advances the live operation revision, so
an already-issued phase mutation cannot pass its old CAS after the terminal
result becomes durable; an identical result replay is idempotent and does not
advance it again, while conflicting content fails closed. Before a first result
is committed, apply revalidates the
matching live directive against that same current aggregate anchor; a result
racing an anchor mutation is therefore rejected even if a future mutation
path were to miss eager cleanup. An already committed receipt remains
immutable history and an exact retry still resolves to its original commit
index after the authority later advances. Terminal receipts are pruned only
through an explicit replicated command after the retry-retention window.

Data sends typed `OperationEvidence` when directive execution starts and
completes. The envelope carries the session and boot, exact reporter
assignment, operation, population and history anchors, phase, and a SHA-256 of
the evidence body. A report that fits one frame uses the soft lane; a larger
report uses the `ObservationEvidence` Start/Chunk/End transfer with a bounded
256 KiB body plus envelope. Meta performs whole-object hash and schema checks
before admission. A report that is structurally valid but stale against the
latest committed view is audited and discarded without disrupting an
otherwise current authority session; malformed session, boot, framing, or
content-hash data closes it.

`MetaObservationStore` is deliberately outside `MetaStores`: it is volatile,
leader-local evidence and is never encoded into a command, WAL, snapshot, or
committed subscription. Admission authenticates the tuple `(node identity,
boot incarnation, controller-local session generation)` and accepts only the
current generation. A new generation atomically removes the node's older
observations. Candidate and operation evidence must equal the current
committed node/group/assignment membership, term, manifest, partition
replication epoch, history, operation, and registration anchors. The
typed candidate/evidence query results include the authenticated reporter boot
alongside node and assignment, so a reconciler never joins a payload to a
second session lookup that could cross a reconnect. The canonical evidence-
summary conversion copies that complete reporter incarnation. The same
committed anchors are rechecked deterministically when evidence is embedded in
an operation-phase command, closing the race between leader-local validation
and Raft apply. A committed epoch-only change therefore invalidates old
candidate and operation evidence even when term and manifest do not move.
Committed changes proactively purge stale evidence,
and queries filter again against one current committed snapshot. Startup and
each entered leader epoch clear soft state; every observed follower edge clears
sessions, observations, and their local diagnostic ring even when another
leader edge is already waiting behind it.

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

Commands, records, exports, and snapshots carry exact schema version 2. Its
pre-release layout includes distinct target and source assignment anchors and
the partition replication epoch in durable directives. Durable operation
evidence includes its exact group id, reporter assignment and boot, population
identity, history, operation id, and evidence hash. Snapshot decoding rejects
malformed identity anchors and evidence that names a missing group or an
impossible future group/population epoch; older committed evidence remains
valid history after a group legitimately advances or the reporter moves.
Version 1 lacks the Data-control fields and seventh store
and is rejected rather than partially decoded or upgraded in place.
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
retired principals is rejected. Every member also commits its numeric
Data-control endpoint in the canonical `IPv4:port` or `[IPv6]:port` spelling,
allowing any seed to return the same directory and making the local leader's
listener identity compare equal to its committed binding.
Data-node identities use canonical `keylane://node/<node-id>` principals with
global one-to-one binding.

Data control follows the Raft transport's optional mTLS mode instead of adding
a second Meta certificate configuration. The listener reuses that member's
Raft CA/certificate/key and its sole `keylane://meta/<server-id>` URI SAN. A
Data client reuses its replication TLS CA/certificate/key and must present its
committed sole `keylane://node/<node-id>` URI SAN. TLS is all-or-none on each
side; plaintext deployments rely on network isolation and never silently
downgrade a partially configured identity.

Local administration uses a mode-0600 Unix socket and derives a canonical
operator actor from Linux `SO_PEERCRED`, constrained by an explicit UID
allowlist. Its parent directory must not be group- or world-writable, and the
listener records the bound inode so shutdown never unlinks a replacement path.
Remote administration is plaintext when no control TLS identity is configured,
matching the Raft transport default. A plaintext listener grants operator
authority to every reachable peer and records the fixed
`keylane://operator/plaintext` actor, so trusted network reachability is its
security boundary. Deployments requiring authenticated peer identity configure
mutual TLS and use the peer's canonical URI SAN. Role-based authorization then
separates operators, Meta members, and data-node self-reporting; actor fields on
the wire are never trusted.
Certificate validity is enforced by TLS, but online issuance, rotation, CRL,
and OCSP integration are outside this module.
The one-shot `keylane-meta-ctl` operator client speaks the same ordered line
protocol over Unix, plaintext TCP, or mTLS TCP; in mTLS mode it verifies the
remote server certificate. It keeps RESP and NuRaft dependencies out of the
client.

Every privileged committed command creates a deterministic audit record keyed
by Raft log index. Records include the injected actor, proposal time, command
summary, verdict, and a rolling hash linked to the previous record. Exports
carry the preceding anchor and record hashes so an external archive can verify
continuity and deduplicate by log index and record hash within its own
deployment namespace; Keylane persists no separate cluster identity. Pruning
advances the committed chain anchor only through an explicitly named record.
`SetAuditPolicy` is itself replicated and always audited, including a
transition into or out of disabled mode.

## Source map

| Claim | Repository source |
|---|---|
| Public Meta boundaries, commands, store composition, and correctness contracts | `include/keylane/meta/` |
| Deterministic apply, stores, coordinator, observations, and administrative protocol implementations | `src/meta/` |
| Pure per-node projection and leader-scoped Data-session publisher | `include/keylane/meta/control_projector.h`, `src/meta/control_projector.cpp`, `include/keylane/meta/data_control_server.h`, `src/meta/data_control_server.cpp` |
| Shared Meta/Data frame, object-transfer, and message formats | `include/keylane/cluster/control_protocol.h`, `include/keylane/cluster/control_transport.h`, `src/cluster/control_protocol.cpp`, `src/cluster/control_transport.cpp` |
| Raft WAL, vote/config state, native Asio hooks, and proposal executor | `include/keylane/meta/nuraft_*`, `src/meta/nuraft_*`, `src/meta/proposal_executor.cpp`, `third_party/patches/nuraft/` |
| Foreign-thread typed completion ingress and worker wakeup | `celer/include/celer/runtime/foreign_executor.h`, `celer/src/runtime/foreign_executor.cpp`, `celer/include/celer/runtime/cross_core.h`, `celer/src/runtime/worker.cpp` |
| TLS identity, RBAC, Unix peer credentials, and administrative clients | `include/keylane/meta/identity_verifier.h`, `include/keylane/meta/ctl_server.h`, `app/keylane_meta.cpp`, `app/keylane_meta_ctl.cpp`, `celer/src/net/` |
| Recovery, partition, membership, and security gates | `tests/meta_*`, `tests/meta_integration/` |
