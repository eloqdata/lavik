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
challenges, or accept results, and starts the leader-local Automatic Failover
Detector. Demotion cancels those owners and begins closing, then joins, all
sessions and leader-scoped tasks from its leadership generation before the
coordinator reports the transition complete. Shutdown first stops the
creation, membership, Automatic Failover Detector, and Failover Transition
reconcilers without waiting for remote results, then
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
the local Raft listener, `--data-control-addr` accepts Data-node control
sessions, and the required concrete `--ctl-addr` is the remote operator/Admin
listener. Admin also defaults to a mode-0600 Unix socket unless an explicit
socket choice is supplied; Unix and TCP share one dispatcher, authentication
policy, capture limiter, and retained-reply budget. The manifest and durable
membership descriptor own the advertised routes, which may name explicit
proxies rather than these local binds. Wildcard Admin binds and port zero are
invalid.

The state machine owns one `MetaStores` value containing seven committed
stores:

| Store | Durable responsibility |
|---|---|
| Identity | Data-node certificate principal bindings and retired identities; Meta-member principal bindings |
| Topology | Single-Data-cluster lifecycle, Groups, membership, owners, epochs, manifest references, slot ranges, and one optional failover transition per Group |
| Policy | Registered cluster-wide Policy families and their immutable consecutive raw JSON versions, exposed through typed current-value accessors |
| Grant | One optional authority Grant per Group Term, activation actions, and fencing |
| Operation | Idempotent operation lifecycle, current directives, durable terminal receipts, and exported/prunable terminal summaries |
| Population manifest | Immutable, content-addressed partition/epoch documents and explicit pruning |
| Audit | Log-index-ordered command verdicts in a bounded hash chain |

`ApplyCommitted` is the only mutation path. It dispatches absolute-value and
revision-checked commands and enforces cross-store invariants such as unique
principals, one data group per node, monotonic terms and epochs, required
current Policy families for a Created cluster, and valid authority/operation
anchors. Each Group Term may install at most one Grant. Fencing advances to a
fresh grantless term, so replacing or reauthorizing an Owner cannot reuse an
authority identity. Group membership, owner, slot, population-
manifest, partition-replication, and `UpdateNode` endpoint changes advance the
cluster topology epoch. `SetSlotMap` validates a complete candidate before
publication and rejects any slot ownership change involving an
active grant. Every affected source and destination group must first be
fenced, preventing a lease for the old projection from spanning the cut.
An active Group failover transition locks its owner, membership, term,
grant, manifest, and population epoch against ordinary
mutations. Only a typed failover command carrying the exact transition id and
latest transition revision may advance that aggregate.
Initial identity registration can be projected at
epoch zero before any group exists. The administrative membership proposer,
not operator input or deterministic apply, generates each assignment
incarnation from the OS CSPRNG; the topology store's bounded last-value index
only catches direct replay. Non-terminal operations persist an optional
replication-history binding but no Policy dependency; current cluster-wide
Policy takes effect at its own family boundary rather than being copied into a
Grant, Operation, or Failover Transition. Phase evidence is accepted only when
its exact `(group, node, assignment)` membership incarnation, reporter boot,
operation, group term, manifest, partition replication epoch, and history
match committed facts. Those identity anchors remain in the durable evidence
summary rather than being reconstructed from the node's membership at apply.
Directive validity is a continuously maintained committed invariant, not only
an admission check. After every accepted command, apply deterministically
rechecks each bounded live directive against the exact active source and
target assignments, Group Term, and population
manifest/partition epoch. It removes only the stale attempts and advances each
affected operation's phase revision once, forcing reconcilers with an older
CAS view to reload. This catches source or target removal/reassignment,
term changes, fencing, and population changes
without a command-specific cleanup list. Replaying the anchor mutation sees
the directive already absent and is a no-op; snapshot decode rejects a stale
directive as corrupt aggregate state.
A domain-invalid command consumes its Raft index, leaves the requested domain
state unchanged, and records the rejection in the audit chain. Unknown
commands, corrupt durable bytes, contradictory replay at an existing index,
and impossible apply ordering fail stop. Replaying the same entry at the same
index is idempotent and produces the same verdict and audit record;
correctness does not depend on apply running only once.

Policy families are compiled into `MetaPolicyStore`; an arbitrary id cannot
introduce a schema at runtime. The current families are
`keylane.automatic-uncontrolled-failover-v1` (fixed matching `kind`, `enabled`,
and `suspect_after_ms`) and `keylane.authority-lease-v1` (fixed matching `kind`
and `duration_ms`).
Each accepts only its exact compact JSON object and typed ranges, rejecting
whitespace, missing, duplicate or unknown fields, alternate escaped spellings,
wrong scalar kinds, and overflow. This deep Module owns its small family parser;
there is no runtime schema registry or general-purpose JSON dependency. A
family starts at version 1 and then admits
only `current + 1`, apart from exact same-version raw replay. It keeps the raw
administrative bytes and at most the newest 32 consecutive versions; adding a
33rd atomically evicts the oldest, and all retained Policy content shares a
16 MiB cap. No content hash is stored; bounded raw-byte equality defines
same-version identity, and the highest version becomes current at its commit.
Typed current-value accessors are the only consumption seam, so neither the
Full Desired State publisher nor the
Automatic Failover Detector reparses generic JSON. Policy retirement, runtime
schema registration, and references from Grants or Operations do not exist.
The Admin Adapter exposes `putpolicy` and `getpolicy`: its outer command/Raft
codec transports the bounded raw bytes and version, while `MetaPolicyStore`
remains the one family-aware validation and typed-decoding Seam. Reads return
the retained raw current version.

The topology store also owns an independently revisioned cluster lifecycle:
`Uninitialized`, `Creating`, `Created`, or `ProvisioningFailed`. A Meta Raft
cluster owns at most one logical Data cluster. The root Cluster Create
`SubmitOperation` and `Uninitialized -> Creating` transition occur on
candidate copies in one apply; root completion or abort similarly updates the
operation and terminal lifecycle together. These transitions do not advance
the topology epoch. The topology record permanently retains the root operation
id, Genesis commit index, terminal outcome, and a bounded non-sensitive failure
summary, while the manifest remains only in the root operation during
creation. Operation archive or pruning therefore cannot reopen creation.
Snapshot decode requires `Creating` to name exactly one matching non-terminal
root whose operation sequence equals the Genesis index; a missing or mismatched
half is fail-stop corruption. Terminal lifecycle permits that root to be live,
archived, or pruned. An `Uninitialized` snapshot containing Data-cluster
artifacts remains decodable and is reported as `non-pristine`. The lifecycle is
part of the topology-store durable format. Keylane is pre-release, so this
format change has no migration path; older development data directories must be
rebuilt. The Raft command/WAL envelope version also advances without adding a
command tag, so a WAL-only directory containing the older opaque creation
intent fails stop at decode rather than replaying a partial lifecycle.

Each Group may own one independently revisioned optional
`MetaFailoverTransition`. It durably records a transition id, controlled or
uncontrolled mode, target term, and an optional candidate action. The action
binds a fresh action id to the exact
candidate node/assignment/boot, source compatibility domain, and optional
one-way preparation authorization with `none` or `unknown` loss. Controlled
state additionally binds the operator operation id and absolute deadline.
Runtime frontiers and prepared contexts are absent: they remain boot/session
observations and must be reported again after Meta leadership changes. The
transition revision is its latest mutating Raft index and is independent of the
Group membership revision.

The generic operation store retains only the controlled request's stable
`(group, absolute deadline)` intent and terminal operator result. It supplies
idempotency and `getop`, but carries no failover phase, directive, receipt, or
candidate progress. All resumable execution state is the Group transition
above.

Eight typed commands are the complete durable transition language:
`BeginControlledFailover`, `BeginUncontrolledFailover`,
`SetUncontrolledCandidate`, `AuthorizeFailoverPrepare`,
`AbortControlledFailover`, `DegradeControlledFailover`,
`CommitControlledFailover`, and `CommitUncontrolledFailover`. Controlled Begin
preserves the current owner, term, and grant while installing a candidate;
uncontrolled Begin atomically advances to the target term, fences authority,
and a manual Begin may install a candidate. An automatic Begin is always
candidate-less, leaving selection and any replacement entirely to the durable
uncontrolled executor. Degrade performs that same fence, terminates the
controlled operation, and retains only an already lossless-authorized healthy
candidate. Data treats this retained controlled-to-uncontrolled projection as
the same attempt only when its transition/action, candidate, source lineage,
and population anchors are exact; it updates the fenced term context in place
so an already prepared child history survives. Candidate replacement is legal
only while uncontrolled and always starts unauthorized. Authorization is
action-scoped and monotonic.

Manual and automatic entry share `BeginUncontrolledFailover`; there is no
parallel automatic transition format. A manual Begin records `manual` and zero
SUSPECT duration. An automatic Begin records one fixed exact failure reason and
the accumulated SUSPECT duration as committed audit provenance, but does not
persist Policy versions or resolved Policy values. It may also name one exact
pristine `Submitted` Controlled Failover operation for the same Group as a CAS
witness. Apply deterministically scans the bounded operation journal, atomically
aborts every untouched Submitted Controlled request for that Group with
`preempted by automatic uncontrolled failover`, and installs the uncontrolled
fence/transition. This closes either Raft ordering around proposal construction;
while the transition exists, new Controlled submissions for the Group are
rejected, although replay of an already-known operation id remains idempotent.
A running Controlled transition, an advanced operation, or a mismatched named
witness cannot be preempted.

Commit requires the exact authorized prepared action and atomically activates
its candidate as owner with the target term's sole Grant, advances the topology
epoch, places the action id on the Grant for Data activation, and
clears the transition. Controlled Commit advances to that term and installs
the Grant in the same cutover; Uncontrolled Commit installs into the grantless
term already reserved by Begin. Controlled Commit also completes its operation
with `failover-completed` and requires loss `none`; Uncontrolled Commit records
the transition's latched loss policy: `none` only for an exact healthy action
retained from lossless controlled authorization, otherwise `unknown`.
Controlled Abort terminates the operation and clears its matching transition.
Its pre-Begin form gives a submitted operation a terminal path when planning
cannot safely install a transition.
Every command validates exact pre-state or exact post-state replay, so a stale
leader proposal cannot advance a replaced action or transition.

All model collections, command fields, snapshots, active operations, archived
summaries, retained Policy bytes, and the audit window have explicit bounds. An
unreferenced population-manifest insertion is charged against the exact bytes
remaining in the aggregate snapshot, including headroom for its audit record;
the decode count ceiling is derived from that durable byte limit rather than
an unrelated collection limit. An oversized snapshot fails that snapshot
round. Once uncompacted WAL or consecutive snapshot-failure guards fire, the
coordinator rejects ordinary proposals with `RESOURCE_EXHAUSTED`. It simulates
an explicit recovery command against one committed view and admits exactly one
whose effect advances the bounded recovery chain: terminalization of a live
operation, movement of terminal records into the archive, removal of
existing archived summaries or terminal receipts, removal of an existing
unreferenced manifest, or an audit prune whose serialized window is smaller
even after its own audit record. Generic terminalization still requires a
bounded empty result. Cluster Create root terminalization is instead simulated
against the complete stores and must atomically produce `Created` with
`cluster-created`, or `ProvisioningFailed` with an abort; validating only the
operation half is not safe recovery. A no-op prune, stale revision, or other
nominally whitelisted command is rejected before Raft append.
The recovery reservation follows the actual NuRaft proposal until it resolves,
even if its caller times out, so another recovery step cannot overtake an
uncertain outcome. After enough state is removed, a successful snapshot clears
the snapshot/WAL pressure rather than Data-local state synthesizing authority.
Snapshot decode revalidates group-set and authority-anchor lockstep, active
identities, the required current registered Policy families, and retained
manifest references before exposing the recovered aggregate. An active
authority additionally requires a nonzero Group Term, matching the
Data-control serving representation.
Audit retention is replicated: the default bounded-rotate mode evicts the
oldest entry and records durable loss watermarks, disabled mode suppresses
ordinary records while retaining audit-mode transitions, and strict-export mode
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

Failover's registered proposal hook also closes the gap between a planner read
and Raft append. At the coordinator's single proposal timestamp it rechecks the
current authenticated session and TTL-fresh exact candidate/source evidence
claimed by Begin, candidate replacement, authorization, retained degradation,
and cutover. A disconnected candidate, expired source heartbeat, withdrawn
pause/progress, failed action, or missing prepared observation therefore rejects the
proposal before append. Source absence remains a grace-derived negative
decision rather than a required certificate, but an exact current-source
heartbeat arriving after Degrade was planned is positive contrary evidence
and rejects that stale proposal. These checks gate leader-local capabilities
only; deterministic transition revision and aggregate preconditions remain
the authoritative conflict check during apply.

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

`MetaFailoverReconciler` is a leader-scoped, level-triggered driver. It starts
from the complete committed view on every eligible leadership epoch, wakes on
commits, and periodically re-evaluates observation TTLs. Each pass derives at
most one of the eight typed commands; proposal completion is only a wakeup, not
workflow state. Demotion or shutdown cancels and joins local planning/proposal
work while leaving the committed transition for the next leader. A leadership
warmup equal to the observation grace prevents a new leader from treating
not-yet-reported boots as failures. The process derives that grace as at least
the Raft election upper bound plus Data's maximum reconnect window, and never
shorter than the observation TTL. This covers a healthy Data process that just
misses the winning election round without delaying explicit disconnect or
typed action-failure evidence.

The absolute Controlled deadline is a leader-side proposal-admission cutoff,
not a clock read during replicated apply. At or after the cutoff the planner
only proposes Abort, and the proposal hook rejects a newly admitted Begin,
authorization, degradation, or Controlled cutover. An entry admitted before
the cutoff and a deadline Abort are still resolved by Raft log order; the
earlier entry may obtain quorum after the wall-clock deadline. This preserves
deterministic apply and replay without introducing a replicated time oracle.

A controlled request selects a candidate in the exact current-source domain,
then preserves the owner while waiting for its paused stable frontier and the
candidate to cover it. Only then does it authorize lossless preparation and
commit a matching prepared action. Deadline expiry aborts the controlled
operation. An explicit Candidate failure while the Source is healthy or still
inside its bounded recovery grace also aborts immediately; it neither waits
for that Candidate to restart nor waits to reinterpret the failure as a Source
failure. Source absence or disconnect beyond the observation grace, or definite
source boot/history replacement, instead degrades the transition to
uncontrolled, fences the old authority, and records the controlled operation
as failed. An uncontrolled
transition never aborts: a missing candidate waits. A failed, disconnected, or
replaced action is removed immediately; Meta installs the best eligible
replacement when one exists, or clears the candidate and waits without relying
on the failed process to restart.

The loss contract matches Redis Cluster's asynchronous replication tier.
Controlled cutover reaches the old owner's drained frontier and records
`loss=none`. A newly selected or replacement uncontrolled action records
`loss=unknown`; an exact healthy action already authorized lossless may retain
`none` when controlled failover degrades. Unknown-loss cutover proceeds from
the best eligible observable replica, so writes acknowledged only by the
unavailable owner may be lost. Controlled failover can still degrade after its
source fails; independently, the Automatic Failover Detector can begin an
uncontrolled transition for an exactly unserviceable Owner.

Owner Serviceability is one pure classification over a self-contained cut: the
committed Owner, assignment, and Group Term; the current
authenticated session generation and boot; the FDS installed on that session;
TTL freshness, draining, storage and population health; and causal lease
confirmation. The exact Unserviceable reason precedence is `session_missing`,
`heartbeat_expired`, `draining`, `storage_unready`, then
`population_unready`. A mismatched session/boot identity or an unjoinable
runtime/observation cut is Indeterminate until a coherent cut arrives; the
detector does not manufacture failure from elapsed time when the possible
lease deadline is unknown. For an exact session, an old FDS anchor or a valid
grant Ack not yet causally confirmed is Indeterminate only while its known
possible Authority Lease remains inside that installed projection's effective
duration. Known expiry becomes `heartbeat_expired` even if the committed and
runtime FDS have already advanced. The observation store retains that old
effective duration with the same atomic heartbeat cut, starts its finite
causal-progress interval at the first heartbeat for an exact session and Owner
projection, and refreshes it only when a strictly higher granted Ack is
causally confirmed. A newer Policy therefore neither shortens nor extends a
lease already installed by Data.

After a new Leader's `2D` authority-handoff quarantine, every first-send Grant
attempt enters a finite possible-lease envelope before the socket write. A
failed write is delivery-ambiguous, so the envelope uses that heartbeat's
steady receive time and the exact effective duration; cached-Ack replay never
refreshes it. The store keeps the maximum unconfirmed deadline for the same
Owner authority across projection or duration replacement. Confirmation of
the latest ordered Grant collapses older possibilities into that exact
installed window; the installed window itself survives later same-authority
FDS replacement. A session/boot, Owner, assignment, or Group Term change
clears both. Thus a Policy/FDS replacement cannot erase a lease
Data may still retain, while confirmation of a shorter replacement Grant does
not unnecessarily preserve an older longer deadline.

The handoff-pending denial is likewise recorded before its first send and bound
to the exact session, authority, and heartbeat sequence. It survives a
same-authority FDS replacement that clears projection-local runtime status. A
Grant attempt proves the suspend-aware handoff deadline elapsed and clears the
marker. An unhealthy exact challenge is passed through the same guard before
Meta publishes `NodeNotReady`, so a same/newer successfully written health
denial can also retire it without bypassing `2D`; older runtime decisions and
other denial kinds cannot. Owner heartbeat and causal-progress receive times
use the detector's steady-clock domain, so wall-clock corrections do not
affect freshness; the exact TTL boundary remains fresh.
Ineligible leadership, leadership observation warmup, authority handoff, and
an existing Failover Transition are explicit control-plane blockers.

The Automatic Failover Detector is a bounded leader-local state machine, not
Committed State and not the Uncontrolled Executor. For every Created Group it
reports `DISABLED`, `HEALTHY`, `SUSPECT`, `BLOCKED`, or `TRIGGERING` and uses
an injected monotonic-millisecond cut (production `steady_clock`) to accumulate
only exact Unserviceable time. A reason change does not clear elapsed time.
Indeterminate evidence for the same complete anchor freezes and later resumes
it; Serviceable evidence clears it. A change
to leadership generation or any revisioned eligibility interruption (including
false-to-true entirely between detector polls), Owner/assignment, Group term,
either current Policy version, enabled value, or
threshold discards it. A new or newly eligible leader first completes the
normal observation warmup and then gives absence or failure a complete fresh
debounce interval. Silent loss therefore needs the observation TTL plus that
full debounce and proposal latency before a fence can commit. Detector state
never changes readiness or serving authority by itself.

At the threshold, only while the current classification is exact
Unserviceable, the detector enters `TRIGGERING`, creates stable request and
transition identities, and proposes the existing candidate-less automatic
`BeginUncontrolledFailover`. Its proposal hook rechecks the current Policy,
leadership, complete authority anchor, current observations, exact reason, and
threshold immediately before a first append. A definite pre-append or domain
rejection discards that attempt. Once admitted for append, later health or
Policy arrival does not retract the decision; log order and the command's
committed authority CAS resolve the race. Once append outcome is uncertain,
retries use the identical command and bounded backoff; committed-state CAS
either installs that transition once or rejects it after the anchor changes.
The ordinary
`MetaFailoverReconciler` then owns the committed transition exactly as it does
for a manual uncontrolled Begin. Demotion cancels and joins detector timers,
pending proposals, and validation admissions; a replacement leader never
inherits SUSPECT time.

`MetaControlProjector` is a pure function over one atomic committed view. It
produces a canonical node-specific `FullDesiredState`: the global Meta/Data
directories and topology, each group's partition replication epoch, the
resolved Authority Lease duration, referenced population manifests, the
optional Group failover transition and grant activation action, the complete
steady owner/membership relationship, and live directives whose explicit
recipient is that node. Each Group's
`steady_replication_enabled` bit
is true only when the committed cluster lifecycle is `Created`; Genesis leaves
explicit population directives as the sole ingress owner. Data defers the
projected follow-owner relationship while an active transition or a current
local initialize/rebuild directive owns ingress and consumes a later complete
projection after that owner disappears. Controlled operation id/deadline
remains Meta-only. Policy ids, versions, and raw documents never cross this
wire seam. The source applied index is an
ordering/diagnostic watermark; SHA-256 of the canonical semantic projection is
the dependency used by leases and directives. The projector takes the current
typed Authority Lease Policy, the leader publisher may only reduce its
duration to the local leadership-validity bound, and that effective duration
is covered by the projection hash and validated by Data. Data derives the
heartbeat interval deterministically as `max(1 ms, duration / 3)`; cadence is
not an independently encoded FDS scalar. The publisher sends a full projection
on session acceptance and whenever that hash changes. Control
protocol v1 has no delta format, so an index advance with identical content
does not create network
churn and a reconnect never depends on retained incremental history.
Superseding an incomplete node projection is an object-local FDS abort and
retry on the same authenticated session. If a direct FDS or streamed
`TransferEnd` is already visible, the publisher first consumes that
projection's exact `FullStateApplied` and adopts it as the session's real
baseline before retrying the newest projection. Only the named abort for the
matching active FDS object is non-terminal; an unknown reason, wrong object
kind, or mismatched object fails closed. This keeps ordinary projection churn
out of candidate-disconnect semantics without weakening real session-loss
handling.

Control v1 is unreleased and its schema is replaced in place. Data and Meta
must use matching layouts; a Policy-bearing FDS, missing resolved lease,
missing Hello flow counts, and untyped rebuild payloads fail validation rather
than being inferred or defaulted locally.

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
Connect, handshake, partial-frame progress, and individual socket writes are
bounded at ten seconds. An established reader instead permits one complete
observation TTL plus that fixed progress budget before declaring the session
idle. This keeps long but valid `D / 3` heartbeat cadences connected, lets the
detector classify heartbeat expiry before transport teardown, and still turns
a stalled partial frame or peer into a finite failure.
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
An accepted Data-control or Admin session borrows its Celer `Connection`
storage before spawning the session coroutine. During frame destruction, its
owner unregisters the task and releases the borrow after body-local Connection
users have unwound. Shutdown, demotion, or a watchdog may retire the transport
immediately, but Celer cannot reclaim the borrowed storage while suspended
session code can still resume and dereference it.
Those per-node objects also share one weighted 2 GiB projection budget derived
as two overlapping generations times encoded-plus-decoded 512 MiB size
classes. Each build reserves the encoded/decoded pair before projection and
then adjusts to the batch's actual retained string/vector capacities. The
permit follows shared ownership through transfer and live installation, so
4096 small sessions remain possible while a few abuse-sized projections cannot
multiply common topology and manifest data into a TiB-scale allocation.

Heartbeat is the periodic Data-to-Meta observation message. Protocol v1 carries
common health followed by exactly one tagged steady-state role payload: no role
information, an authority lease request, or replica candidate progress, plus
an independent optional failover observation. A session accepts
only the next business sequence or an exact replay
of the previous heartbeat; an exact replay gets the cached exact ack. Because
Data sends the next sequence only after processing the preceding Ack, receipt
of heartbeat `N+1` is causal confirmation of Ack `N`. Meta retains that proof
only when `N` granted a nonzero lease for the same authenticated session/boot
and exact installed Owner projection, assignment, and Group Term.
Ack write completion alone is not confirmation, and cached Ack replay does not
advance it. Data
quiesces heartbeat projection reads during an FDS replacement. Any outstanding
ack for the old object is consumed without applying its lease decision, and
production resumes only after the new object is acknowledged, so a prior
projection cannot causally confirm the replacement authority.
Likewise, Data terminates the authenticated session when an otherwise exact
Grant arrives at or after its local deadline. It cannot send the next business
sequence on that session: Meta would interpret the sequence advance as proof
that the expired Grant was installed. A replacement session clears that cached
Ack lineage and may acquire a fresh finite lease normally. This is also a
split-authority safeguard during a duration decrease: falsely confirming the
shorter Grant would discard Meta's older long-lease window while Data could
still hold that older lease, allowing failover before its actual expiry.
Candidate progress names the authenticated reporter's exact committed member
assignment, term, manifest revision and digest, partition replication epoch,
completed rebuild source lineage, and typed next-LSN vector.
Meta rejects reports from active nodes that are not members of the named group
and old assignment observations after remove/re-add; group queries retain the
reporter identity with each observation. Reporter-local history is bound to the
history announced in `ClientHello`; source history in candidate progress is an
independent lineage anchor. The failover payload is one of `SourcePaused`,
`CandidatePrepared`, or `ActionFailed`, bound to the exact transition/action and
current reporter boot. It can coexist with lease renewal or candidate progress,
so transition evidence never suppresses steady role observation. Health
ingestion is independent of challenge validation, so a bad renewal request
cannot hide useful liveness evidence.
An exact uncontrolled target-term fence retains the old owner only as topology
history. Because its grant is absent, that same member may re-enter candidate
selection with a strict self-origin domain: preceding source term, identical
node and assignment, and the current authenticated session's boot and history.
Meta admits this exception only while the uncontrolled transition and fenced
grantless state are both committed; active-grant owners and any mismatched
term, assignment, boot, or history remain ineligible.
Meta derives role from committed FDS facts rather than trusting the tag, and
replaces common health plus candidate state under one observation-store lock.
An authority/no-role heartbeat, or rejected candidate, clears any older
candidate for that node. Session teardown also withdraws the exact
generation's candidate immediately; a stale teardown cannot clear evidence
from a replacement generation.
The same atomic replacement stores the exact candidate-action basis from the
FDS installed on that authenticated session: group term, transition revision,
action, assignment, and boot. The failover planner treats a role omission or
different candidate as terminal only when that basis matches the current
unauthorized action. A heartbeat from an older or unknown projection remains
warmup evidence, so `BeginUncontrolledFailover` cannot invalidate its
preselected candidate merely by advancing the committed term before the new
FDS arrives. Authorization may temporarily suppress the ordinary role during
Data history rotation; thereafter only Prepared, typed ActionFailed, the
action watchdog, or disconnect resolves the attempt.
Challenges name the exact projection and complete group authority anchor.
Meta grants only while it remains the caught-up leader, and caps duration at
both the current Authority Lease Policy and the configured leadership-validity
bound. An
otherwise-valid first grant for a new group/boot/anchor/leadership identity is
held behind a leader-local `2D` quarantine on Linux `CLOCK_BOOTTIME`, where `D`
is the maximum prior lease and the second `D` is a safety margin derived from
the same Raft election lower bound. Data measures and synchronously rechecks
the lease on that suspend-aware clock, starting immediately before its first
heartbeat write rather than at ack receipt. The `2D` interval tolerates the
Meta clock advancing up to twice as fast as the prior Data clock; scheduling
can only extend the wait. Losing volatile quarantine evidence restarts the
whole interval rather than recovering an unsafe wall-clock deadline.
Once granted, Data owns expiry enforcement: `CLOCK_BOOTTIME` expiry
self-fences its in-memory authority without waiting for disconnect detection,
an Automatic Failover Detector decision, or another Meta message. Renewal
synchronously expires an already-due lease before considering a replacement,
so a late Ack cannot revive authority across the deadline.

NuRaft's peer-response expiry uses active `CLOCK_MONOTONIC` time, so a Meta
host suspend can otherwise preserve an old process's cached leader verdict
while other members elect a replacement. A leader-scoped Data-control task
continuously compares that clock with `CLOCK_BOOTTIME`, including when no Data
session is active; accumulated suspend divergence of at least
`D` closes the leadership generation's authority sessions and synchronously
requests immediate NuRaft resignation. In a multi-member cluster the old
generation cannot become eligible again. NuRaft intentionally keeps a sole
member leader, so that case must instead run for another full `D` of active
time; a further suspend restarts the wait. Live FDS boundaries, directives,
result proposals, and grants all pass this barrier. It covers the same-identity
case whose ordinary `2D` handoff entry matured before suspension.

Population directives separate the wire recipient from the rebuild target:
rebuild is delivered to the target, while authorize/revoke is delivered to the
source.
The common assignment field always names the target membership incarnation;
the durable directive carries a separate source assignment and the committed
partition replication epoch. Both must exactly match committed topology and
the installed group view.
The durable and wire codecs keep bounded `payload`, `preconditions`, and
`force` fields. V1 uses `payload` for `initialize-empty-population`, where it
carries the target Data session's authenticated replication-history id.
`rebuild` and `authorize-source` share a versioned payload containing the source
flow count advertised with its boot/history in `ClientHello`. Meta commits that
layout in both directives; projection and replay never infer it from the
recipient's workers or a newer source session. Source authorization and the
native target handshake both check it against the actual source layout.
`revoke-sources` requires both fields empty, and all kinds require
`force=false`.
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

Failover does not use operation evidence or terminal receipts. Its three typed
heartbeat observations are volatile inputs to a typed transition command. The
Raft cutover command, not observation receipt, is the boundary between
Data-local preparation and action-bound activation. A control stream may remain
connected across it, but Data must consume the later committed FDS and a valid
finite lease; no single RPC crosses the commit point on Meta's behalf.

`MetaObservationStore` is deliberately outside `MetaStores`: it is volatile,
leader-local evidence and is never encoded into a command, WAL, snapshot, or
committed subscription. Admission authenticates the tuple `(node identity,
boot incarnation, controller-local session generation)` and accepts only the
current generation. A new generation atomically removes the node's older
observations. An exact generic candidate report may clear a same-boot
disconnect recorded before any action selected that incarnation. Once a
committed action binds it, the disconnect latch is terminal for that action;
same-boot reconnect, generic progress, and newly reported prepared context do
not revive the attempt. Candidate progress must match committed
node/group/assignment, term, manifest revision and digest, and partition
replication epoch state.
The compatibility `history` field remains reporter-local; the internal
selector uses the separately stored source assignment, boot, and history.
Failover source/action observations additionally match the exact live
transition identity, candidate action where applicable, reporter incarnation,
and population anchors. Operation evidence for directive-based workflows also
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

Candidate planning is an internal, read-only function seam rather than an
administrative command or RPC. At one fixed receive-time cut it selects only
current-session, current-boot, Ready, healthy, non-draining replicas whose
assignment and population anchors still match committed facts. Each report has
a non-extendable TTL deadline; planning does not depend on a global observation
revision, so unrelated heartbeats cannot restart the calculation. Exact source
term/node/assignment/boot/history and flow dimension define a compatibility
domain, and per-flow LSNs are never compared across domains. Controlled
failover selects only inside the current source domain. Uncontrolled failover
tries domains in descending source-term order and canonical identity order,
falling back one domain at a time.

Within the chosen domain the selector removes vectors strictly dominated
component-by-component. A unique greatest vector wins, equal greatest vectors
choose the lowest node id, and incomparable maxima choose the lowest
envelope-deficit tuple `(sum as uint128, max, node id)`. The latter is an
explicit best-effort data-loss policy, not a claim of a lossless latest node.
The self-contained result is returned immediately; this layer adds no
yield/resume revalidation lifecycle.

## Durability and recovery

The durable source of truth is the newest completed state-machine snapshot plus
the following Raft WAL. Snapshot capture serializes an exact applied-index cut
under the state-machine mutex, excluding committed apply; a writer thread
performs file I/O after capture. A snapshot becomes eligible for log compaction
only after its atomic durable publication succeeds. Incoming snapshots are
size-bounded, decoded completely, and installed synchronously as one replacement
state.

Election log freshness includes the compacted prefix: RequestVote compares the
last logical log term first, then its index. When no WAL suffix remains, the
snapshot's last-included index and term supply that boundary, including after
restart. A surviving WAL suffix supplies its own last entry instead. Compaction
therefore cannot make a current voter consider an older candidate up to date.

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

Before NuRaft opens its network or election timer, a pristine initial member
loads the canonical full Meta vector from `--initial-cluster-manifest` and
atomically publishes it as `cluster_config.dat`. A one-, three-, or five-voter
genesis differs only in vector length. Every member starts the same ordinary
randomized election; there is no distinguished bootstrap candidate. The
manifest is rejected once any durable config exists and is never read on
restart. `initial_bindings.dat` retains that exact genesis descriptor set
until local committed apply has observed every identity binding. It
survives election-time config copies and restarts, then is atomically removed
after `initial_bindings_complete.dat` is published as a permanent tombstone;
the tombstone prevents a completed zero-index genesis from being mistaken for
the config-first publication crash prefix. A pristine process with no manifest
instead persists
`waiting_joiner.dat` before its singleton placeholder config and disables its
initial election. That marker makes restart and partial dynamic-join catch-up
remain election-disabled, and is durably removed only after the joiner applies
the identity bindings through its committed config entry. An invite may
publish that config before the joiner receives a WAL segment; the marker keeps
that recoverable prefix distinct from an incomplete ordinary member.
The first persisted NuRaft vote also publishes `raft_started.dat` before the
vote itself. For genesis and ordinary members this irreversible boundary makes
loss of both the vote and WAL distinguishable from a process that never opened
Raft; waiting joiners retain their explicitly narrower pre-WAL recovery rule.

After either lifecycle converges, `transport_bindings.dat` records the exact
full descriptor set and the state-machine watermark that proved or followed
its identity bindings. An ordinary restart may use those descriptors only
while replay remains below that watermark. Committed dynamic membership
changes reuse the same baseline: `save_config()` publishes a bounded
`transport_bindings.next`, replaces `cluster_config.dat`, and durably promotes
the candidate. Recovery either discards a candidate paired with the old config
or completes a candidate paired with the new config; any other pairing fails
closed. Snapshot installation uses the validated snapshot's embedded NuRaft
configuration and identity projection to update this same config/baseline pair
and finish local genesis or join catch-up. Retired genesis bindings still prove
that initialization completed; they grant no active membership. Extra bindings
may belong to the normal two-phase membership workflow.

The durable snapshot also supplies redo evidence for an interrupted installation.
Before transport or elections start, recovery validates the complete snapshot
and compares configuration-entry indices. A newer snapshot configuration replaces
an older disk configuration; equal-index configurations must agree. A later disk
configuration must lie beyond the snapshot's applied index and match its exact
surviving WAL entry. Election-disabled waiting joiners retain their narrower
invite-before-WAL exception. A snapshot that precedes binding completion preserves
the matching baseline's later replay watermark. Conflicting or insufficient
evidence fails startup; a covered membership entry can never disappear merely
because compaction removed it from the WAL.

This is Raft transport recovery state, not a Cluster Create operation
or membership workflow record. Config indices, a genesis completion tombstone,
the Raft-started marker, or a transport baseline require the matching server
state and segmented WAL to exist. Missing, oversized, truncated, or
contradictory state fails closed rather than replaying genesis.

The persisted state-machine watermark is the snapshot index, not every applied
WAL index. After restart, a post-snapshot tail remains invisible until Raft
legally reconfirms it with a current-term quorum; depending on the elected
leader, it is then committed as a prefix or overwritten. With no quorum the
Meta plane is unavailable rather than exposing an unconfirmed decision.
Client timeouts therefore mean an uncertain outcome and must be resolved by
the operation's stable idempotency key.

## Format compatibility

The Meta/Data control wire uses protocol v1 with the current pre-release
layout. FDS carries the resolved Authority Lease duration; Data derives its
heartbeat interval as `max(1 ms, duration / 3)`. Stop-and-wait heartbeat
sequencing supplies the causal Ack confirmation used by Owner Serviceability.
Both peers must use the same layout; earlier Policy-bearing layouts have no
compatibility or negotiation path. This wire version is independent of the
durable schemas below.

Commands, stores, records, exports, snapshots, and the physical segmented WAL
carry independent exact format markers. The command envelope and topology
store are respectively v4 and v2; the aggregate snapshot envelope, other
persisted stores, and segmented-WAL container retain their existing v1
markers. Command v4 removes Policy retirement, content hashes, Policy
references, durable successor-grant specification, separate authority/Grant
revision counters, and term-preserving Grant revocation. It adds automatic
`BeginUncontrolledFailover` provenance and a pristine-operation CAS witness;
apply preempts the complete bounded set for the Group. Policy snapshots retain
only registered-family raw histories and typed-decodable current values. None
of these unreleased layouts has a legacy decoder, so development directories
from a different same-marker or older layout are not interchangeable. Every
configured Meta identity has one
canonical concrete numeric Data-control endpoint and one canonical concrete
numeric Admin endpoint. The
NuRaft `srv_config::aux` `KMI2` descriptor carries the server id, derived
principal, and both endpoints; Raft keeps its endpoint in the native field.
The descriptor and committed identity binding must agree exactly. Advertised
Data-control and Admin addresses may route through an explicit proxy instead
of equaling their local process binds; restart can likewise rebind a Raft
listener behind a transport proxy without changing its durable advertised
endpoint. Endpoints are immutable, unique within their
respective directories, and change only through retirement and replacement
with a fresh server id. The previous partial `KMI1` development descriptor is
rejected and is not migrated. Durable
operation evidence includes its exact group id, reporter assignment and boot,
population identity, history, operation id, and evidence hash. Snapshot decoding rejects
malformed identity anchors and evidence that names a missing group or an
impossible future group/population epoch; older committed evidence remains
valid history after a group legitimately advances or the reporter moves.
These unreleased formats evolve independently in place. A shared version
number in one layer does not guarantee compatibility with earlier development
layouts: their data directories are recreated rather than migrated. Keylane
Meta does not negotiate durable formats between mixed binary versions and has
no in-band schema-switch command. Incompatible changes require coordinated
replacement of the Meta cluster.
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
configuration nor the store binding grants membership alone. During
manifest-bootstrapped genesis, the complete config descriptor may temporarily
stand in for a not-yet-applied binding only while the durable initial-binding
marker names that unchanged descriptor set. The membership reconciler commits
missing bindings in server-id order; every member closes its local marker
synchronously when committed log or snapshot apply proves convergence.
Transport checks identity and reads recovery state without advancing that
lifecycle. A dynamic waiting joiner has a second narrow catch-up window only
while its durable waiting marker is
present. Replaying a pre-add config cannot close that window: the marker is
removed only after the installed config includes the local id, every descriptor
binding is visible, and the state machine has applied through that config
index. The config can arrive before the earlier binding command. With
mTLS these windows also require the exact certificate identity; plaintext
deployments rely on network isolation. Ordinary replay uses only the exact
descriptor set in `transport_bindings.dat`, only below its recorded watermark;
at or above that cut a config descriptor never substitutes for a missing or
conflicting binding.

Initial identity recovery is not a Cluster Create child operation. The
existing `MetaMembershipReconciler` holds the shared membership gate, compares
the loaded genesis descriptors with the identity store, and proposes one
idempotent `BindMetaMember` at a time. Matching crash prefixes resume; any
descriptor conflict fails closed. Once this convergence is complete, the
same reconciler handles only ordinary reusable membership workflows.

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

Creating lifecycle and membership operations share durable admission as well
as one leader-local lease. Identical in-flight membership requests attach to
the existing task; conflicting requests cannot bypass it after timeout or restart.
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

`keylane-ctl failover GROUP` uses the same leader discovery, verifies a Created
cluster and committed Group, generates an operation id and absolute transition
deadline, and submits one `failover 1` request. Its success point is the
operation commit, not Data cutover. A caller retrying this mutation must retain
both the operation id and absolute deadline; recomputing either changes the
intent. Timeout, cancellation, or a generic failure after proposal begins is
reported as uncertain together with that operation id; pre-append resource
gates remain definite `ResourceExhausted` rejections. `getop` derives
`submitted` versus `running` from the operation and
matching Group transition in one committed snapshot, then reports the durable
completed or aborted result. Generic `submitop`, `completeop`, and `abortop`
cannot create or terminate failover because its aggregate changes must use the
typed commands above.

The leader builds `clusterstatus` from a compact state-machine view captured
under the same mutex as committed apply plus Data-control runtime and Automatic
Failover Detector diagnostic snapshots captured first. The two volatile cuts
must name the same leadership generation and eligibility-continuity revision;
the detector cut must also name the compact view's exact applied index. This
top-level identity covers even an empty detector batch, so a false-to-true
eligibility ABA cannot splice pre-interruption diagnostics into a later cut.
Evaluation and encoding run on the bounded proposal executor, not the Celer
worker that drives Data heartbeats.
Runtime entries exist only
after Hello, FDS application, and a current-view validation; replacement,
disconnect, demotion, and shutdown remove them. Health is timestamped on
receipt, while a lease decision becomes observable only after its Ack is
written, and replaying a cached Ack does not refresh it. Merging requires the
session, projection, assignment, Group Term, manifest, and population anchors
to match the committed cut. The result then passes a second
leader-alive, term, leadership-generation/eligibility/revision, Raft-config,
and committed Meta-directory check; a changed bracket returns `cut_changed`
instead of mixed state. Captures are single-flight across both Admin listeners.
Completed replies release the capture permit before sending, share a 256 MiB
retained-reply budget, and a slow receiver loses the connection after five
seconds rather than delaying Data heartbeats. The compact committed cut carries
the topology-owned cluster lifecycle, its revision, root operation id, Genesis
commit index, and any creating phase or terminal failure summary. While
`Creating`, the server locates that exact root id and retains only the declared
Data ids needed for diagnostics; it does not scan operation kinds. The
`cluster_create_active` blocker means that durable workflow remains
non-terminal. The server also exposes specific `meta_catching_up`,
`data_unregistered`, `data_unregistered_retrying`, `data_unobserved`, and
`data_session_missing` blockers. The public command remains `clusterstatus 1`;
its incompatible strict inner payload is version 4, and JSON rendering is
schema 3. Every Group includes `automatic_failover_state`, optional
`current_reason`, `suspect_elapsed_ms`, `effective_threshold_ms`, and optional
`blocked_reason`. The state itself records when a Begin is being triggered;
the threshold is a derived scalar rather than Policy identity or content.
These are a bounded leader-local diagnostic cut and do not alter readiness. A Created Group
missing the first complete detector publication is conservatively `blocked`
with `indeterminate_evidence` rather than being reported as disabled.
`data_unobserved` means the current leader has no handshake evidence;
`data_session_missing` means a previously accepted or actively retrying node
has no current accepted session. A rejected,
parsed Data Hello is leader-generation-scoped observational evidence only; it
never authorizes a node. The operator uses the compact cut for its read-only
creation preflight without copying the journal. The leader repeats the check
under exclusive creation/membership admission before its proposal; apply is
the authoritative concurrency boundary. In `Uninitialized`, any Data identity
including retired identities, Group or slot state, grant, population
manifest, or prior creation operation derives `non-pristine` rather than
snapshot corruption. Meta identity/configuration and audit state are not Data
cluster artifacts.

Readiness uses this leader-observed cut. Meta availability requires a live,
caught-up leader with quorum; membership stability compares its Raft config
with committed Meta identities and the complete unique Admin directory.
Topology convergence requires current projection plus fresh storage and
population facts for each active node referenced by committed groups. Serving
readiness requires complete slot coverage and, for every slot-owning group, a
committed owner/grant, present manifest, current Authority Lease Policy, and a
recent successfully written lease grant matching the current session and
authority.
That operational readiness timestamp deliberately remains an Ack-write fact;
the stricter next-heartbeat causal confirmation is an Automatic Failover
Detector input and does not redefine cluster readiness.
Unassigned nodes remain diagnostic only. Lifecycle and runtime readiness are
orthogonal: `Created` means the initial workflow completed, not that current
Data sessions are READY. Empty and partially configured clusters are stable
`NOT READY` results. Every rendered result, including retryable and fatal CLI
errors, includes a status explanation and an operator next action.

`keylane-ctl cluster-create` reuses the same private leader discovery and
status-capture seam and accepts only manifest schema v1. A manifest names the
complete initial Meta vector—id plus canonical numeric Raft, Data-control, and
Admin endpoints—one or more canonical Data identities with advertised
`client_endpoint` (`tcp://`) and/or `tls_endpoint` (`tls://`), and one or more
Groups with exactly one primary and optional replicas. Every Data node has
at least one numeric listener; dual listeners share a host and use distinct
ports, and no two declarations share a Data socket address. Registration
preserves the transport tags through the durable identity store and projection
into separate TCP/TLS ports. TLS replication selects the advertised TLS port
without falling back to plaintext; credentials and listener configuration
remain process-local. Meta entries are sorted by id, all are voters, each
endpoint class is unique, and the principal derives as `keylane://meta/<id>`.
Slots are either generated with `contiguous-even` after sorting Group ids or supplied as a
complete, non-overlapping `0..16383` range table. All
declared Data belongs to exactly one Group and every Group owns at least one
slot. An optional strict `[bootstrap_policy]` table supplies Bootstrap Policy
Defaults for `automatic_uncontrolled_failover_enabled`,
`automatic_uncontrolled_failover_suspect_after_ms`, and
`authority_lease_duration_ms`; omitted values default to `true`, 5000 ms, and
5000 ms respectively and must satisfy the registered family ranges. The parser
rejects unknown TOML structure and files over 64 KiB, then
sorts nodes, Groups, replicas and ranges and merges adjacent ranges belonging
to the same Group. The CLI renders that canonical plan and requires exact
lowercase `yes` unless `--yes` is present. Only this normalized multi-Group
and multi-Meta shape is accepted under version 1; the earlier scalar-Meta
payload has no compatibility decoder. The request and persisted intent use
binary version 5 only. Older binary intents are intentionally unsupported.

After an `Uninitialized` and pristine client check, the CLI generates a root
operation id and sends one `clustercreate 1` request to the discovered leader.
`MetaCtlServer` proposes the existing `SubmitOperation` command with the
normalized manifest as a `cluster-create-workflow-v1` intent. At that Raft
index, `ApplyCommitted` atomically inserts the root and enters `Creating`; this
Genesis commit is the command's success point. The server replies
`OK clustercreate 1 <genesis-index> <root-id>` immediately, without waiting for
workflow phases, Data readiness, or Redis probes. `--timeout-ms` covers leader
discovery and this proposal response only.

Only `Uninitialized` accepts Genesis. `Creating`, `Created`, and
`ProvisioningFailed` reject every later create as `already-created`, without
manifest comparison, attach, or retry-existing behavior. A proposal rejected
before commit leaves both operation and lifecycle unchanged. Once a mutation
has been sent, a transport or proposal ambiguity is `uncertain-outcome` and
retains the caller-generated root id so the operator can resolve it with
`cluster-status`.

`MetaClusterCreateReconciler` runs on the Meta worker after each caught-up
leader transition, but only while lifecycle is `Creating`. Its atomic
committed subscription includes the recovered snapshot/WAL prefix; it loads the
exact root id stored by topology and plans one existing Meta command at a time
from retained intent, phase, and actual
committed state. Every effect is checked before its phase checkpoint advances,
including recovery between those commits. Subscription overflow reacquires the
complete view. The reconciler uses the trusted coordinator actor for follow-up
proposals; the original operator remains recorded on the root operation.
Creation and Meta membership changes share admission, with the durable
`Creating` lifecycle covering handoff, timeout, archive, and restart.

Before changing Data topology, the root remains in `wait-meta-barrier`. Its
submit log index is the fixed barrier `B`: every remote member in the genesis
configuration must have a recent transport-verified response and report a
state-machine commit index at least `B`. NuRaft peer-SM tracking remains
enabled on followers so their responses carry that index. A leader enables it
only while this phase is active, because the same NuRaft switch also delays
ordinary client completion until every peer applies the write; outside the
genesis barrier, leader proposals retain normal majority availability. The
reconciler also requires exact agreement among the manifest Meta vector,
NuRaft descriptors, committed
identity bindings, and both advertised Meta directories. It never calls
`add_srv`; initial membership already exists as the full genesis
configuration. Cluster Create retains the shared membership admission gate
through completion, so ordinary membership changes cannot invalidate this
barrier or the retained creation intent.

The reconciler registers Data in node-id order, creates Groups in Group-id
order, assigns primary before sorted replicas, begins term 1, and replaces the
whole Slot map with one `SetSlotMap` carrying every canonical range.
Before Data topology exists, it installs version 1 of
each registered global Policy family from the retained Bootstrap Policy
Defaults only when that family has no current value; an operator-preseeded
family is never overwritten and Policy state alone does not make an
Uninitialized aggregate non-pristine. After this phase, only the current
committed Policy values are authoritative; retained manifest defaults have no
ongoing control role. It then uses the existing population store,
replication-state command, and finite authority command to anchor a sparse
manifest containing only each Group's slots and grant that Group to its
declared primary. Creation state is
the lifecycle embedded in `MetaTopologyStore`; there is no separate durable
module, singleton Genesis record, or new Raft command.

Only current, boot-bound Data sessions that acknowledge the complete
projection permit population initialization. Each Group receives one stable,
domain-separated `cluster-create-v1` child operation. Its primary first gets
the existing source-less empty-population directive. After that exact success
receipt commits, the child installs one `authorize-source` directive per
replica as a dedicated durable phase. Only after every exact authorization
success receipt commits does the next durable phase retain those directives
byte-for-byte, including their original revisions, and add the matching
`rebuild` directives under a later revision. The projector independently
checks the retained authorization's exact receipt before sending a rebuild.
Source authority therefore remains logically current throughout target
initialization, including Meta replay between the two phases. Data clears the
runtime capabilities at each FDS boundary, but the FDS-projected replay count
temporarily reserves their source history and makes an early target handshake
retryable until the exact level-triggered capabilities are restored. Rebuild
completion means the existing replication manager has activated the population
and established its native replication session; subsequent heartbeats supply
the population-current READY evidence.

Group children execute in canonical Group order. Exact directive identities,
attempts, boots, histories, assignments, terms, grants, manifests,
partition epochs, and source flow layouts make reconnect replay safe without
minting another attempt. Primary and replica worker counts may differ: flow
identity follows the source and native replication maps those flows onto the
target's workers.
A deterministic failure fences only that Group before aborting its child and
then the root; already completed Groups are not rolled back. While population
work is unfinished, a current Data session with a different target boot or
source boot/history invalidates the attempt. The reconciler durably removes
its directives and records the reason before fencing and aborting, so Meta
recovery does not depend on retaining the detecting session. Committed success
receipts remain immutable history; they do not establish readiness for a new
boot. Other incompatible topology changes stop at an inspectable
`recovery-required` phase. Neither path authorizes another destructive
initialization. After every child completes, the reconciler submits the
existing root `CompleteOperation` result `cluster-created`; apply atomically
enters `Created`. A deterministic failure first completes fencing and then
submits root `AbortOperation`, atomically entering `ProvisioningFailed` with a
bounded sanitized summary. Recoverable or uncertain errors remain `Creating`.

Demotion and shutdown cancel the owner and join its local proposal work while
the worker and executor remain live. Already accepted proposals may commit;
cancellation never synthesizes a Data result, rolls back committed topology,
or adds a compensating fence. The next leader re-reads authoritative effects.
Creation, Meta membership, and per-Group failover have dedicated background
drivers. Arbitrary other operation kinds still require their own recovery
mechanism; journal persistence alone supplies none.

Operators separately use `cluster-status` to follow lifecycle and runtime
readiness. `creating` includes the root phase; `provisioning-failed` includes
the durable safe summary; every non-uninitialized state includes the root id
and Genesis index. Node failures use existing blockers with `node:<id>` scope
and identify the Group plus missing session, projection, health, or population
conditions. The CLI has no `redis-cli` dependency and its exit 0 confirms only
the atomic Genesis commit; current Data-plane usability remains the READY
status contract.

Every privileged committed command creates a deterministic audit record keyed
by Raft log index. Records include the injected actor, proposal time, command
summary, verdict, and a rolling hash linked to the previous record. Exports
carry the preceding anchor and record hashes so an external archive can verify
continuity and deduplicate by log index and record hash within its own
deployment namespace; Keylane persists no separate cluster identity. Pruning
advances the committed chain anchor only through an explicitly named record.
`SetAuditPolicy` is itself replicated and always audited, including a
transition into or out of disabled mode.

Accepted failover transition commands normally emit one structured process log:
`failover event=<...> mode=<...> group=<...> transition=<...> action=<...>
loss=<none|unknown|pending> commit_index=<...>`. Candidate and source details
or bounded reasons are appended where relevant. Opaque variable token values
use canonical percent encoding, so embedded whitespace, control bytes, or `=`
cannot change field boundaries; fixed enum, numeric, and hexadecimal values
remain directly readable. Entries whose cut may lose acknowledged writes use
warning severity; lossless transition entries use informational severity.

The line is derived from the pre-apply aggregate and emitted only for an
accepted commit. Process-log collection is at least once, so consumers
deduplicate by `commit_index`. Two exact post-effect replays deliberately
suppress their duplicate state-dependent event: `SetUncontrolledCandidate`
cannot recover whether the original event selected or replaced a candidate,
and a post-Begin `AbortControlledFailover` cannot recover the cleared action
identifier. The deterministic committed audit record remains authoritative,
and the process log reconstructs the election/cutover sequence alongside that
audit chain rather than replacing it.

## Source map

| Claim | Repository source |
|---|---|
| Public Meta boundaries, commands, store composition, and correctness contracts | `include/keylane/meta/` |
| Deterministic apply, stores, coordinator, observations, and administrative protocol implementations | `src/meta/` |
| Per-Group durable failover transition, eight typed commands, exact replay/CAS apply, leader-resumable planner, controlled Admin entry, and structured commit logs | `include/keylane/meta/commands.h`, `include/keylane/meta/failover.h`, `include/keylane/meta/failover_reconciler.h`, `include/keylane/meta/failover_admin.h`, `src/meta/failover.cpp`, `src/meta/failover_reconciler.cpp`, `src/meta/failover_admin.cpp`, `src/meta/state_apply.cpp`, `src/meta/state_machine.cpp`, `src/meta/ctl_server.cpp` |
| Registered typed durable Policy families, strict raw JSON admission/history, and current-value accessors | `include/keylane/meta/policy_store.h`, `src/meta/policy_store.cpp`, `tests/meta_stores_test.cpp` |
| Pure Owner Serviceability cut, causal lease confirmation, leader-local detector state, automatic Begin adapter, and generation-bracketed diagnostics | `include/keylane/meta/owner_serviceability.h`, `src/meta/owner_serviceability.cpp`, `include/keylane/meta/automatic_failover_detector.h`, `src/meta/automatic_failover_detector.cpp`, `include/keylane/meta/automatic_failover_reconciler.h`, `src/meta/automatic_failover_reconciler.cpp` |
| Volatile candidate/failover observations and deterministic compatibility-domain plan selection | `include/keylane/meta/observation_store.h`, `src/meta/observation_store.cpp`, `include/keylane/meta/candidate_plan.h`, `src/meta/candidate_plan.cpp` |
| Pure per-node projection including resolved lease duration, Data-derived heartbeat cadence, and failover/activation/follow-owner state, plus the leader-scoped Data-session publisher and causal heartbeat admission | `include/keylane/meta/control_projector.h`, `src/meta/control_projector.cpp`, `include/keylane/meta/data_control_server.h`, `src/meta/data_control_server.cpp` |
| Manifest-bootstrapped initial Meta configuration, persistent restart/waiting-joiner classification, and Raft durability | `include/keylane/meta/nuraft_state_mgr.h`, `src/meta/nuraft_state_mgr.cpp`, `app/keylane_meta.cpp`, `tests/meta_integration/gate_initial_meta.py` |
| Atomic Genesis lifecycle, strict Bootstrap Policy Defaults, durable creation admission, Meta catch-up barrier, and leader-owned recovery | `include/keylane/meta/cluster_create.h`, `src/meta/cluster_create.cpp`, `include/keylane/meta/topology_store.h`, `src/meta/topology_store.cpp`, `src/meta/state_apply.cpp`, `src/meta/ctl_server.cpp`, `include/keylane/meta/cluster_create_reconciler.h`, `src/meta/cluster_create_reconciler.cpp`, `app/keylane_meta.cpp` |
| Durable post-genesis Meta membership intent, exact-config recovery, leadership handoff, and identity retirement | `include/keylane/meta/membership_reconciler.h`, `src/meta/membership_reconciler.cpp`, `src/meta/ctl_server.cpp`, `src/meta/state_apply.cpp`, `tests/meta_integration/gate_membership_recovery.py` |
| Shared Meta/Data frame, object-transfer, failover observation, transition, and activation formats | `include/keylane/cluster/control_protocol.h`, `include/keylane/cluster/control_transport.h`, `src/cluster/control_protocol.cpp`, `src/cluster/control_transport.cpp` |
| Raft WAL, vote/config state, native Asio hooks, and proposal executor | `include/keylane/meta/nuraft_*`, `src/meta/nuraft_*`, `src/meta/proposal_executor.cpp`, `third_party/patches/nuraft/` |
| Meta session transport retirement and Connection-storage lifetime | `src/meta/ctl_server.cpp`, `src/meta/data_control_server.cpp`, `celer/include/celer/net/connection.h`, `celer/src/runtime/worker.cpp` |
| Foreign-thread typed completion ingress and worker wakeup | `celer/include/celer/runtime/foreign_executor.h`, `celer/src/runtime/foreign_executor.cpp`, `celer/include/celer/runtime/cross_core.h`, `celer/src/runtime/worker.cpp` |
| TLS identity, RBAC, Unix peer credentials, Admin transport, cluster status, controlled failover, and initial cluster creation | `include/keylane/meta/identity_verifier.h`, `include/keylane/meta/ctl_server.h`, `include/keylane/meta/admin_client.h`, `include/keylane/meta/cluster_status.h`, `include/keylane/meta/cluster_create.h`, `include/keylane/meta/failover_admin.h`, `app/keylane_meta.cpp`, `app/keylane_ctl.cpp`, `celer/src/net/` |
| Automatic-failover status wire/model plus JSON and text rendering | `include/keylane/meta/cluster_status.h`, `src/meta/cluster_status.cpp`, `tests/meta_cluster_status_test.cpp` |
| Recovery, partition, membership, failover, and security gates | `tests/meta_*`, `tests/meta_integration/` |
