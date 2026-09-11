# Meta control plane

## Boundary and state model

`keylane-meta` is a separate C++ process for durable cluster metadata. It
embeds NuRaft and uses its native Asio service for Raft peer sockets, timers,
and TLS. One Celer worker owns the configured Unix and/or TCP administrative
listeners and the process-lifetime Data-control listener; a bounded proposal
executor keeps
synchronous NuRaft API entry and WAL I/O off that worker. NuRaft and
proposal-executor threads return typed notifications or coroutine handles
through Celer's foreign MPSC mailbox, which reuses the worker's normal wake
sequence and eventfd. The main Keylane data-plane executable remains
Raft-free. Followers keep accepting Data connections long enough to return the
committed member directory and leader hint. Only a caught-up leader installs
the publisher that may create sessions, project desired state, evaluate lease
challenges, or accept results. Demotion cancels that publisher and begins
closing, then joins, all sessions and leader-scoped tasks from its leadership
generation before the coordinator reports the transition complete. Shutdown
first stops the creation and membership reconcilers without waiting for remote results, then
cancels administrative result waits and drains every listener, then quiesces Data
sessions and
all NuRaft/proposal-executor producers, waits for the foreign executor's
accepted prefix to reach the Meta worker, and only then stops the generic Celer
runtime. An active demotion or first shutdown drain is fail-stop if the worker
mailbox cannot accept its notification: reporting success would permit a later
leader epoch to reuse authority that was never revoked. Once shutdown has
synchronously drained the worker, later reconciler cancellation and object
destruction are no-ops and do not depend on a still-running executor.

The process exposes three independent network responsibilities. `--addr` is
the Raft peer endpoint, `--data-control-addr` accepts Data-node control
sessions, and the optional concrete `--ctl-addr` is the remote operator/Admin
endpoint. Admin defaults to the mode-0600 Unix socket when neither Admin option
is specified; explicitly naming both Unix and TCP starts both over one
dispatcher, authentication policy, capture limiter, and retained-reply budget.
There is no separate bind/advertise Admin identity: `--ctl-addr` is both the
actual bind and the committed route, so wildcard addresses and port zero are
invalid.

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
cluster topology epoch. `SetSlotMap` validates a complete candidate before
publication and rejects any ownership or config-epoch change involving an
active grant. Every affected source and destination group must first be
fenced, preventing a lease for the old projection from spanning the cut.
Initial identity registration can be projected at
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

Failover preparation uses one top-level durable operation whose intent and
phase blobs have strict versioned codecs. The implemented graph ends at
`promotion-prepared`:
old authority excluded, candidate caught up, promotion preparing, then
promotion prepared. A registered proposal-validation hook forbids skipped or
repeated phases, changes to earlier exclusion/frontier proofs, and any prepare
before the committed group is fenced and grantless. The preparing phase owns
one current `promotion-prepare` directive; the prepared phase clears it only
after the exact successful terminal receipt and matching evidence summary are
committed under the same operation id. Authority activation and serving phases
are outside the preparation graph and are rejected by its validator.

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
acceptance and whenever that hash changes. Control protocol v1 has no delta
format, so an index advance with identical content does not create network
churn and a reconnect never depends on retained incremental history.

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
Projection and directive-set validation stream their canonical encodings into
SHA-256 and retain only a bounded vector of directive references; they never
materialize a normalized FDS or one encoded buffer per directive. Receive-side
parsing necessarily overlaps the accumulated wire bytes with the owning
decoded fields, so the 512 MiB value is a protocol abuse ceiling rather than a
512 MiB process-memory promise. The consuming decoder releases the wire buffer
on success or failure before installation can suspend, bounding that overlap
to parsing instead of retaining it across the atomic replacement.
Connect, handshake, session progress, and individual socket writes are bounded
at ten seconds; this is well above the default 100 ms heartbeat and 300--600 ms
election cadence while still turning a stuck peer into a finite failure.
Sockets that have not completed TLS when enabled, a valid `ClientHello`, and a
committed active node/certificate binding also consume one of 4096 pending-
handshake permits. The listener closes excess sockets before starting their
session coroutine. A follower retains its permit through the bounded redirect
write. A leader releases it only after the connection has atomically claimed
the committed node's single post-authentication slot and joined the current
leadership generation; duplicates are rejected until that exact owner exits.
Consequently anonymous/redirect work is capped at 4096, and projection/FDS
holders are capped at one per committed node-record slot (validated active at
claim time) even when a peer stalls or that record retires before session
cleanup.
Those per-node objects also share one weighted 2 GiB projection budget derived
as two overlapping generations times encoded-plus-decoded 512 MiB size
classes. Each build reserves the encoded/decoded pair before projection and
then adjusts to the batch's actual retained string/vector capacities. The
permit follows shared ownership through transfer and live installation, so
4096 small sessions remain possible while a few abuse-sized projections cannot
multiply common topology and manifest data into a TiB-scale allocation.

Heartbeat is the periodic Data-to-Meta observation message. Protocol v1 carries
common health followed by exactly one tagged role payload: no role information,
an authority lease request, or replica candidate progress. A session accepts
only the next business sequence or an exact replay
of the previous heartbeat; an exact replay gets the cached exact ack. Data
quiesces heartbeat projection reads during an FDS replacement. Any outstanding
ack for the old object is consumed without applying its lease decision, and
production resumes only after the new object is acknowledged.
Candidate progress names the authenticated reporter's exact committed member
assignment, term, manifest revision and digest, partition replication epoch,
completed rebuild source lineage, and typed next-LSN vector.
Meta rejects reports from active nodes that are not members of the named group
and old assignment proofs after remove/re-add; group queries retain the
reporter identity with each proof. Reporter-local history is bound to the
history announced in `ClientHello`; source history in candidate progress is an
independent lineage anchor. Health ingestion is independent of challenge
validation, so a bad renewal request cannot hide useful liveness evidence.
Meta derives role from committed FDS facts rather than trusting the tag, and
replaces common health plus candidate state under one observation-store lock.
An authority/no-role heartbeat, or rejected candidate, clears any older
candidate for that node. Session teardown also withdraws the exact
generation's candidate immediately; a stale teardown cannot clear evidence
from a replacement generation.
Challenges name the exact projection and complete group authority anchor.
Meta grants only while it remains the caught-up leader, and caps duration at
both committed policy and the configured leadership-validity bound. An
otherwise-valid first grant for a new group/boot/anchor/leadership identity is
held behind a leader-local `2D` quarantine on Linux `CLOCK_BOOTTIME`, where `D`
is the maximum prior lease and the second `D` is a safety margin derived from
the same Raft election lower bound. Data measures and synchronously rechecks
the lease on that suspend-aware clock, starting immediately before its first
heartbeat write rather than at ack receipt. The `2D` interval tolerates the
Meta clock advancing up to twice as fast as the prior Data clock; scheduling
can only extend the wait. Losing volatile quarantine evidence restarts the
whole interval rather than recovering an unsafe wall-clock deadline.

NuRaft's peer-response expiry uses active `CLOCK_MONOTONIC` time, so a Meta
host suspend can otherwise preserve an old process's cached leader verdict
while other members elect a replacement. The Data-control worker compares
that clock with `CLOCK_BOOTTIME`; accumulated suspend divergence of at least
`D` closes the leadership generation's authority sessions and synchronously
requests immediate NuRaft resignation. In a multi-member cluster the old
generation cannot become eligible again. NuRaft intentionally keeps a sole
member leader, so that case must instead run for another full `D` of active
time; a further suspend restarts the wait. Live FDS boundaries, directives,
result proposals, and grants all pass this barrier. It covers the same-identity
case whose ordinary `2D` handoff entry matured before suspension.

Directives separate the wire recipient from the rebuild target: rebuild and
promotion-prepare are delivered to the target, while authorize/revoke is
delivered to the source.
The common assignment field always names the target membership incarnation;
the durable directive carries a separate source assignment and the committed
partition replication epoch. Both must exactly match committed topology and
the installed group view.
The durable and wire codecs keep bounded `payload`, `preconditions`, and
`force` fields. V1 uses `payload` for `initialize-empty-population`, where it
carries the target Data session's authenticated replication-history id, and
defines versioned payload/precondition bodies for `promotion-prepare`. Other
executable directives require both fields empty, and all kinds require
`force=false`. The prepare bodies bind parent history and
required flow frontier to the committed old-authority exclusion term and hash.
Meta transition apply and Data admission reject malformed or misplaced bodies,
so the replication adapter cannot silently ignore a predicate or override.
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

For promotion prepare, terminal success is opaque only to the generic journal:
the Failover validator decodes `PromotionPreparedEvidence`, requires its parent
history and frozen frontier to satisfy the current phase, and requires the
same bytes and hash in a successful terminal receipt and a TTL-fresh operation
observation from the candidate's current boot/session before constructing the
durable evidence summary. The evidence query applies the TTL boundary itself;
it does not rely on a periodic cleanup sweep. This is the Raft boundary between
Data-local prepare and later authority activation. A control stream may remain
connected across it, but Data must receive later committed FDS/lease authority
before local activation; no single RPC may cross the commit point on Meta's
behalf.

`MetaObservationStore` is deliberately outside `MetaStores`: it is volatile,
leader-local evidence and is never encoded into a command, WAL, snapshot, or
committed subscription. Admission authenticates the tuple `(node identity,
boot incarnation, controller-local session generation)` and accepts only the
current generation. A new generation atomically removes the node's older
observations. Candidate progress must match committed node/group/assignment,
term, manifest revision and digest, and partition replication epoch state.
The compatibility `history` field remains reporter-local; the internal
selector uses the separately stored source assignment, boot, and history.
Operation evidence also
matches the committed operation and its replication-history binding. The
typed candidate/evidence query results include the authenticated reporter boot
alongside node and assignment, so a reconciler never joins a payload to a
second session lookup that could cross a reconnect. The canonical evidence-
summary conversion copies that complete reporter incarnation. Operation
evidence's committed anchors are rechecked deterministically when embedded in
an operation-phase command, closing the race between leader-local validation
and Raft apply. A committed epoch-only change therefore invalidates old
candidate and operation evidence even when term and manifest do not move.
Committed changes proactively purge stale evidence,
and queries filter again against one current committed snapshot. Startup and
each entered leader epoch clear soft state; every observed follower edge clears
sessions, observations, and their local diagnostic ring even when another
leader edge is already waiting behind it.

Soft-state memory is bounded independently of its field validators. The store
admits at most the committed node-domain cap of 4096 session keys and candidate
reporters per group, at most 1024 evidence phases per reporter and per operation
(the durable operation-evidence cap), and at most 65,536 total observations.
Using the full node-domain bound for candidates avoids making report arrival
order an implicit member-selection policy. Exact logical byte accounting covers
retained variable fields and their lookup-key copies: the global 68 MiB budget
is one direct frame plus identifier allowance per maximum node, while a node's
289 KiB share holds one maximum streamed evidence object, one heartbeat frame,
and its index allowance. Replacement, generation purge, revalidation, TTL
expiry, and leader reset update the same counters. Ordinary diagnostic
ingestion keeps its previous latest-wins value on capacity rejection.
Heartbeat candidate replacement is stricter: it clears old role evidence
before admitting the replacement. Soft state cannot grant or restore
authority.

`CandidatePlanFor` is an internal, read-only function seam rather than an
administrative command or RPC. At one fixed receive-time cut it selects only
current-session, current-boot, Ready, healthy, non-draining replicas whose
assignment and population anchors still match committed facts. Each report has
a non-extendable TTL deadline; planning does not depend on a global observation
revision, so unrelated heartbeats cannot restart the calculation. Exact
manifest/source lineage and flow dimension define a compatibility domain;
multiple domains stop the plan. Within one domain the selector removes vectors
strictly dominated component-by-component. A unique greatest vector wins,
equal greatest vectors choose the lowest node id, and incomparable maxima choose
the lowest envelope-deficit tuple `(sum as uint128, max, node id)`. The latter
is an explicit best-effort data-loss policy, not a claim of a lossless latest
node. The self-contained result is returned immediately; this layer adds no
yield/resume revalidation lifecycle.

## Durability and recovery

The durable source of truth is the newest completed state-machine snapshot plus
the following Raft WAL. Snapshot capture is serialized with commit and copies
an exact applied-index cut; a writer thread performs serialization and file I/O
after capture. A snapshot becomes eligible for log compaction only after its
atomic durable publication succeeds. Incoming snapshots are size-bounded,
decoded completely, and installed synchronously as one replacement state.

WAL v1 uses checksum-protected `log-<first-index>.seg` files. Segments roll at
a size trigger. Compaction writes the complete surviving suffix to a synced
`compact-<first-index>.ready` intent before replacing the old segment set;
startup finishes such an intent after a crash. A reported pre-publication
failure leaves both the live index and old segments authoritative. Append
batches become durable at NuRaft's flush hooks;
membership state and vote state use atomic rename plus file and directory
sync. Recovery retains the intact contiguous prefix and truncates a torn tail.
A checksum-valid segment or compact-intent header with an unsupported format
version is rejected before recovery modifies any files.
The older prototype's `raft_log.dat` and `LSN1` snapshots are intentionally
incompatible and cause startup to fail with an explicit format error.

The persisted state-machine watermark is the snapshot index, not every applied
WAL index. After restart, a post-snapshot tail remains invisible until Raft
legally reconfirms it with a current-term quorum; depending on the elected
leader, it is then committed as a prefix or overwritten. With no quorum the
Meta plane is unavailable rather than exposing an unconfirmed decision.
Client timeouts therefore mean an uncertain outcome and must be resolved by
the operation's stable idempotency key.

## Format compatibility

The Meta/Data control wire uses protocol v1 with a pre-release layout. Both
peers must use the same layout; earlier pre-release layouts have no
compatibility or negotiation path. This wire version is independent of the
durable schemas below.

Commands, records, exports, snapshots, and WAL replay carry exact schema
version 1. Each active Meta-member identity may carry one canonical, concrete
numeric Admin endpoint. The endpoint is committed with membership rather than
revised by a separate routing command. A sole UDS-managed member with no
endpoint may complete that field once from its configured `--ctl-addr`; a
nonempty endpoint is immutable, and changing it requires retirement followed
by a fresh server id. Before a configuration can contain multiple voters,
every current and new member must have a unique Admin endpoint. Durable
operation evidence includes its exact group id, reporter assignment and boot,
population identity, history, operation id, and evidence hash. Snapshot decoding rejects
malformed identity anchors and evidence that names a missing group or an
impossible future group/population epoch; older committed evidence remains
valid history after a group legitimately advances or the reporter moves.
The unreleased schema and segmented WAL evolve in place as v1. This label
does not guarantee compatibility with earlier development layouts: their
data directories are recreated rather than migrated. Keylane Meta does not
negotiate durable formats between mixed binary versions and has no in-band
schema-switch command. Incompatible changes require coordinated replacement
of the Meta cluster.
Readers reject unknown markers and trailing bytes so incompatible state fails
at startup or replay instead of being interpreted approximately. The
independent segmented-WAL marker follows the same fail-loudly rule.

## Authentication, membership, and audit

Raft transport is plaintext by default, matching the data-plane deployment
model. It still checks claimed source and destination ids against NuRaft
configuration descriptors and committed identity-store bindings, but those
claims are not cryptographically authenticated; deployments whose network is
not fully trusted enable optional mutual TLS. The Raft verifier requires
exactly one recognized canonical `keylane://meta/<server-id>` URI SAN but
ignores unrelated URI SANs; certificates reused by Data control are subject to
the stricter total-URI rule below. An IP or DNS SAN covers the advertised Raft
endpoint. The NuRaft configuration
identity descriptor, the CA-authenticated certificate, and the committed
identity-store binding must all match the claimed source id; neither the
configuration nor the store binding grants membership alone. With mTLS, a
pristine joiner temporarily relies on the authenticated certificate and
invited configuration until it installs the leader's configuration and its
first state-machine entry or snapshot. Plaintext deployments instead rely on
network isolation during that bootstrap interval.

Dynamic membership is a leader-owned `meta-membership-workflow-v1` operation.
Before either identity or NuRaft mutation, Admin commits a bounded versioned
intent containing the requested target, all three endpoints, principal, and
the baseline peer descriptors and identity bindings. Peer descriptors retain
voter/joiner flags, priority and data-center attributes; election-only config
log indices are not semantic membership changes. A recovered owner accepts
only that exact baseline or requested post-state, never overwrites a changed
peer set or reactivates a retired identity. Legacy partial changes without an
intent are not inferred as authorized workflows.

`MetaMembershipReconciler` scans the recovered non-terminal journal on each
leader transition. Add binds identity before invoking `add_srv`; remove
observes the committed configuration without the member before retiring its
identity. NuRaft's accepted invite/leave result is not a commit certificate.
The owner checks the actual committed configuration, checkpoints each phase,
and completes the operation only after all effects are present. Recovery also
handles a crash between an effect and its phase checkpoint. If the removal
target becomes leader during recovery, it yields leadership before another
leader continues the same removal.

Creation and membership operations share durable admission as well as one
leader-local lease. Identical in-flight membership requests attach to the
existing task; conflicting requests cannot bypass it after timeout or restart.
Generic Admin submit/complete/abort commands cannot create or abandon a
membership workflow. A timed-out invite can still commit, so cancelling its
wait does not release this reservation. Demotion joins only queued/local
NuRaft API entry and local proposals; remote-result callbacks own inert result
storage, not a reconciler or leader context. The next owner retries from the
committed state. Incompatible recovery retains an inspectable
`recovery-required` phase instead of guessing a rollback.
The retired binding also disambiguates the short interval after removal commits
but before NuRaft publishes its new in-memory configuration. Reactivation of
retired principals is rejected. Every member commits numeric Data-control and
Admin endpoints in canonical `IPv4:port` or `[IPv6]:port` spelling. Data seeds
use the former directory; operator discovery uses the latter. Neither
directory implies that every follower is currently reachable.
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
The `keylane-ctl` operator client uses one Raft-free Admin transport for
Unix, plaintext TCP, and mTLS TCP. Direct commands address the selected member;
`status` reports its local state. The `cluster-status` command discovers the
leader and evaluates cluster readiness. The transport handles partial I/O
under one absolute deadline. Direct commands support a TLS server-name
override; cluster discovery verifies each numeric Admin IP against the
certificate IP SAN and never falls back between TLS and plaintext. A Unix
seed can use configured TLS credentials for subsequent remote leader access.

`keylane-ctl cluster-status` normally performs exactly two reads:
`clusterhead 1` against the supplied seed to learn the current committed Admin
directory, then `clusterstatus 1` against the indicated leader. Redirect,
leader-change, busy, and incomplete-catch-up results retry discovery only
within the original deadline. Leader routing stays internal to the operator
API. The client reads this leader-observed cut without probing followers or
reporting their replication progress. A stable
result therefore states only leader-observed Meta availability and committed
membership consistency; a quorum-serving leader can report the cluster ready
while one follower is unreachable.

The leader builds `clusterstatus` from a compact state-machine view captured
under the same mutex as committed apply plus a Data-control runtime snapshot
captured first. Evaluation and encoding run on the bounded proposal executor,
not the Celer worker that drives Data heartbeats. Runtime entries exist only
after Hello, FDS application, and a current-view validation; replacement,
disconnect, demotion, and shutdown remove them. Health is timestamped on
receipt, while a lease decision becomes observable only after its Ack is
written, and replaying a cached Ack does not refresh it. Merging requires the
session, projection, assignment, group term, authority, grant, manifest, and
population anchors to match the committed cut. The result then passes a second
leader-alive, term, leadership-generation/eligibility, Raft-config, and
committed Meta-directory check; a changed bracket returns `cut_changed`
instead of mixed state. Captures are single-flight across both Admin listeners.
Completed replies release the capture permit before sending, share a 256 MiB
retained-reply budget, and a slow receiver loses the connection after five
seconds rather than delaying Data heartbeats. The compact committed cut also
carries whether a non-terminal `cluster-create-workflow-v1` root exists. The
server exposes that fact through the existing `cluster_create_active` blocker,
preserving the `cluster-status` v1 wire layout, and the operator uses it for its
read-only creation preflight without copying the journal. The leader repeats
the check under exclusive creation/membership admission before its first
proposal. Unrelated operation kinds do not make a clean topology appear
occupied.

Readiness uses this leader-observed cut. Meta availability requires a live,
caught-up leader with quorum; membership stability compares its Raft config
with committed Meta identities and the complete unique Admin directory.
Topology convergence requires current projection plus fresh storage and
population facts for each active node referenced by committed groups. Serving
readiness requires complete slot coverage and, for every slot-owning group, a
committed owner/grant, present manifest, active policy, and a recent
successfully written lease grant matching the current session and authority.
Unassigned nodes remain diagnostic only. Empty and partially configured
clusters are stable `NOT READY` results.

`keylane-ctl cluster-create` reuses the same private leader discovery and
status-capture seam and accepts only manifest schema v1. A manifest names one
existing Meta member, one or more canonical Data identities and numeric client
endpoints, and one or more Groups with exactly one primary and optional
replicas. Slots are either generated with `contiguous-even` after sorting Group
ids or supplied as a complete, non-overlapping `0..16383` range table. All
declared Data belongs to exactly one Group and every Group owns at least one
slot. The parser rejects unknown TOML structure and files over 64 KiB, then
sorts nodes, Groups, replicas and ranges and merges adjacent ranges belonging
to the same Group. The CLI renders that canonical plan and requires exact
lowercase `yes` unless `--yes` is present. Only this normalized multi-Group
shape is accepted under version 1; scalar payloads have no compatibility
decoder.

After an empty-topology and no-active-create client check, the CLI sends one
`clustercreate 1` request to the discovered leader. `MetaCtlServer` commits
the entire normalized request as the intent of the existing
`cluster-create-workflow-v1` root operation before changing topology. Apply
reserves pristine state for that operation; a different creation id is
rejected, while exact-id replay remains legal. Admin only waits for the
durable result. A timeout or shutdown cancels the wait, not the operation, and
a repeated CLI invocation does not create a replacement for unfinished work.

`MetaClusterCreateReconciler` runs on the Meta worker after each caught-up
leader transition. Its atomic committed subscription includes the recovered
snapshot/WAL prefix; it scans the one non-terminal creation root and plans one
existing Meta command at a time from retained intent, phase, and actual
committed state. Every effect is checked before its phase checkpoint advances,
including recovery between those commits. Subscription overflow reacquires the
complete view. The reconciler uses the trusted coordinator actor for follow-up
proposals; the original operator remains recorded on the root operation.
Creation and Meta membership changes share admission, with the durable active
operation check covering handoff, timeout, and restart.

The reconciler registers Data in node-id order, creates Groups in Group-id
order, assigns primary before sorted replicas, begins term 1, and replaces the
whole Slot map with one `SetSlotMap` carrying every canonical range and each
Group's config epoch. It installs one shared `keylane.cluster-create-v1`
policy, then uses the existing population store, replication-state command,
and finite authority command to anchor a sparse manifest containing only each
Group's slots and grant that Group to its declared primary. No separate
persistent topology or creation-state model exists.

Only current, boot-bound Data sessions that acknowledge the complete
projection permit population initialization. Each Group receives one stable,
domain-separated `cluster-create-v1` child operation. Its primary first gets
the existing source-less empty-population directive. After that exact success
receipt commits, the child installs one existing `authorize-source` and one
`rebuild` directive per replica in a single revision. The projector sends
the authorization immediately but withholds each rebuild until its matching
authorization success is durable. The rebuild completion means the existing
replication manager has activated the population and established its native
replication session; subsequent heartbeats supply the population-current READY
evidence.

Group children execute in canonical Group order. Exact directive identities,
attempts, boots, histories, assignments, terms, grants, manifests, and
partition epochs make reconnect replay safe without minting another attempt.
A deterministic failure fences only that Group before aborting its child and
then the root; already completed Groups are not rolled back. While population
work is unfinished, a current Data session with a different target boot or
source boot/history invalidates the attempt. The reconciler durably removes
its directives and records the reason before fencing and aborting, so Meta
recovery does not depend on retaining the detecting session. Committed success
receipts remain immutable history; they do not establish readiness for a new
boot. Other incompatible topology changes stop at an inspectable
`recovery-required` phase. Neither path authorizes another destructive
initialization. After every child completes, the root completes and Admin
returns the stable Group ids, child operation ids, and their committed
population proof indices.

Demotion and shutdown cancel the owner and join its local proposal work while
the worker and executor remain live. Already accepted proposals may commit;
cancellation never synthesizes a Data result, rolls back committed topology,
or adds a compensating fence. The next leader re-reads authoritative effects.
Creation and Meta membership have dedicated background drivers. Arbitrary
operation kinds, including full Data migration/failover orchestration, still
require their own recovery policy; journal persistence alone supplies none.

The client then polls the ordinary cluster-status v1 wire until roles,
membership, topology, sparse population state, and recent authority evidence
match the complete manifest. Node failures use existing blockers with
`node:<id>` scope and identify the Group plus missing session, projection,
health, or population conditions. Finally the CLI invokes `redis-cli` to
exercise every Group's primary and verify `MOVED`, `CROSSSLOT`,
`CLUSTER INFO`, `CLUSTER SLOTS`, and normal key operations. This makes
success mean the declared multi-Group data plane is usable, not merely that a
metadata prefix committed.

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
| Volatile candidate replacement and internal deterministic plan selection | `include/keylane/meta/observation_store.h`, `src/meta/observation_store.cpp`, `include/keylane/meta/candidate_plan.h`, `src/meta/candidate_plan.cpp` |
| Pure per-node projection and leader-scoped Data-session publisher | `include/keylane/meta/control_projector.h`, `src/meta/control_projector.cpp`, `include/keylane/meta/data_control_server.h`, `src/meta/data_control_server.cpp` |
| Durable creation admission, leader-owned recovery and shutdown cancellation | `src/meta/ctl_server.cpp`, `include/keylane/meta/cluster_create_reconciler.h`, `src/meta/cluster_create_reconciler.cpp`, `src/meta/operation_store.cpp`, `app/keylane_meta.cpp` |
| Durable Meta membership intent, exact-config recovery, leadership handoff and identity retirement | `include/keylane/meta/membership_reconciler.h`, `src/meta/membership_reconciler.cpp`, `src/meta/ctl_server.cpp`, `src/meta/state_apply.cpp`, `tests/meta_integration/gate_membership_recovery.py` |
| Shared Meta/Data frame, object-transfer, and message formats | `include/keylane/cluster/control_protocol.h`, `include/keylane/cluster/control_transport.h`, `src/cluster/control_protocol.cpp`, `src/cluster/control_transport.cpp` |
| Raft WAL, vote/config state, native Asio hooks, and proposal executor | `include/keylane/meta/nuraft_*`, `src/meta/nuraft_*`, `src/meta/proposal_executor.cpp`, `third_party/patches/nuraft/` |
| Foreign-thread typed completion ingress and worker wakeup | `celer/include/celer/runtime/foreign_executor.h`, `celer/src/runtime/foreign_executor.cpp`, `celer/include/celer/runtime/cross_core.h`, `celer/src/runtime/worker.cpp` |
| TLS identity, RBAC, Unix peer credentials, Admin transport, cluster status, and initial cluster creation | `include/keylane/meta/identity_verifier.h`, `include/keylane/meta/ctl_server.h`, `include/keylane/meta/admin_client.h`, `include/keylane/meta/cluster_status.h`, `include/keylane/meta/cluster_create.h`, `app/keylane_meta.cpp`, `app/keylane_ctl.cpp`, `celer/src/net/` |
| Recovery, partition, membership, and security gates | `tests/meta_*`, `tests/meta_integration/` |
