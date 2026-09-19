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

# Meta control plane

## Boundary and state model

`lavik-meta` is a separate C++ process for durable cluster metadata. Its
[Meta Raft runtime](10-meta-raft.md) embeds etcd-io/raft in a Go C archive and
owns consensus, peer sockets, ordered persistence, application dispatch and
snapshot files. The C++ state machine retains deterministic business semantics.
One Bycorf worker owns Admin and Data-control sessions; bounded proposal work
and cross-runtime completions use the existing foreign executor mailbox.
The Data executable and operator client remain Raft-free.

Followers return the committed member directory and leader hint. Only a leader
with current-term application and fresh quorum evidence may start the publisher
and Automatic Failover Detector. Demotion revokes that authority immediately,
then cancels and joins all owners from its leadership generation. Shutdown stops
workflow owners and ingress, drains proposals and all Go callback producers,
waits for accepted foreign notifications, then stops Bycorf. A failed demotion
notification is fail-stop; no later leader generation may reuse authority whose
revocation was not delivered.

The process exposes three independent network responsibilities. `--addr` is
the local Raft listener, `--data-control-addr` accepts Data-node control
sessions, and the required concrete `--ctl-addr` is the remote operator/Admin
listener. Admin also defaults to a mode-0600 Unix socket unless an explicit
socket choice is supplied; Unix and TCP share one dispatcher, authentication
policy, capture limiter, and retained-reply budget. The manifest and durable
membership descriptor own the advertised routes, which may name explicit
proxies rather than these local binds. Wildcard Admin binds and port zero are
invalid.

The state machine owns one `MetaStores` value containing six committed
stores:

| Store | Durable responsibility |
|---|---|
| Identity | Data-node certificate principal bindings and retired identities; Meta-member principal bindings |
| Topology | Cluster lifecycle and Groups: membership, one term/owner, active authority and activation action, population references, slots, and failover transition |
| Policy | Registered cluster-wide Policy families and their immutable consecutive raw JSON versions, exposed through typed current-value accessors |
| Operation | Idempotent operation lifecycle, current directives, durable terminal receipts, and exported/prunable terminal summaries |
| Population manifest | Immutable, content-addressed partition/epoch documents and explicit pruning |
| Audit | Log-index-ordered command verdicts in a bounded audit window |

`ApplyCommitted` is the only mutation path. It dispatches absolute-value and
revision-checked commands and enforces cross-store invariants such as unique
principals, one data group per node, monotonic terms and epochs, required
current Policy families for a Created cluster, and valid authority/operation
anchors. Apply mutates the command's affected records under the state-machine
write lock. Compound authority, failover, and operation/lifecycle transitions
retain only those records for rollback until post-state validation succeeds;
rejection restores them before the audit verdict or any read can observe the
result. Neither individual stores nor the complete `MetaStores` aggregate are
copied for apply. Apply checks domain capacities and task bounds without constructing or
encoding a node projection. Snapshot-size admission uses count-only writers;
network encoding belongs to the publisher. Topology owns authority directly
inside each Group, with no separate Grant table or snapshot section. Read-only
authority views derive their term and owner from that same Group. Each Group Term may install at most one Grant. Fencing advances to a
fresh grantless term, so replacing or reauthorizing an Owner cannot reuse an
authority identity. Group membership, owner, slot, population-
manifest, partition-replication, and `UpdateNode` endpoint changes advance the
cluster topology epoch. `SetSlotMap` validates the command's slot map before
mutation and rejects any slot ownership change involving an
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
Grant, Operation, or Failover Transition. Directive validity is a continuously maintained committed invariant, not only
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
state unchanged, and records the rejection in the audit window. Unknown
commands, corrupt durable bytes, contradictory replay at an existing index,
and impossible apply ordering fail stop. Replaying the same entry at the same
index is idempotent and produces the same verdict and audit record;
correctness does not depend on apply running only once.

Policy families are compiled into `MetaPolicyStore`; an arbitrary id cannot
introduce a schema at runtime. The current families are
`lavik.automatic-uncontrolled-failover-v1` (fixed matching `kind` and
`suspect_after_ms`), `lavik.authority-lease-v1` (fixed matching `kind`
and `duration_ms`), and `lavik.candidate-recovery-v1` (fixed matching `kind`
and `budget_ms`, default 2000 ms, range 0–86,400,000 ms).
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

The topology store also owns the cluster lifecycle:
`Uninitialized`, `Creating`, `Created`, or `ProvisioningFailed`. A Meta Raft
cluster owns at most one logical Data cluster. The root Cluster Create
`SubmitOperation` and `Uninitialized -> Creating` transition occur in
one atomic apply delta; root completion or abort similarly updates the
operation and terminal lifecycle together. These transitions do not advance
the topology epoch. The topology record permanently retains the root operation
id, Genesis commit index, lifecycle state, and a bounded non-sensitive failure
summary. The public lifecycle revision is derived as 0 for Uninitialized, 1 for
Creating, and 2 for either terminal state; terminal outcome is also derived
from that state, while the manifest remains only in the root operation during
creation. Operation archive or pruning therefore cannot reopen creation.
Snapshot decode requires `Creating` to name exactly one matching non-terminal
root whose operation sequence equals the Genesis index; a missing or mismatched
half is fail-stop corruption. Terminal lifecycle permits that root to be live,
archived, or pruned. An `Uninitialized` snapshot containing Data-cluster
artifacts remains decodable and is reported as `non-pristine`.

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

An explicit operator-recovery action instead carries no source compatibility
domain and permits only `loss=unknown`. The operator selects a current member
with readable recovered data when no automatic Candidate exists. Admission
rechecks current observations and rejects a serving Owner or replacement of a
healthy action. An unfenced Group enters through uncontrolled Begin; an already
fenced, candidate-less transition uses SetUncontrolledCandidate. Both continue
through ordinary committed authorization, preparation, Cutover and lease
quarantine. No local command can bypass Meta quorum or establish authority.

Candidate heartbeats distinguish a clean recovered frontier from a live
frontier, preserving the old source boot/history separately from the newly
authenticated reporter. Recovery without a certificate advertises only scoped
operator availability and is excluded from automatic selection. Neither kind
of recovery restores the prior Owner's lease or serving readiness.

The generic operation store retains only the controlled request's stable
`(group, absolute deadline)` intent and terminal operator result. It supplies
idempotency and `getop`, but carries no failover phase, directive, receipt, or
candidate progress. All resumable execution state is the Group transition
above.

Nine typed commands are the complete durable transition language:
`BeginControlledFailover`, `BeginUncontrolledFailover`,
`SetUncontrolledCandidate`, `StartCandidateRecovery`, `AuthorizeFailoverPrepare`,
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
The recovery reservation follows the actual Raft proposal until it resolves,
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
rather than Raft types. On the leader it takes one atomic committed view,
applies fail-safe and
registered semantic validation, injects the transport-authorized actor and a
readable proposal time, encodes the current durable format, submits to
Raft through the proposal executor, and returns the apply result carried by
Raft's completion. It never re-reads a record that bounded audit rotation may
already have evicted. Followers return a not-leader status without appending.
Membership workflows hold one exclusive leader-local lease through completion,
so Raft never receives overlapping configuration changes.

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
requires resynchronization from a new full view. Each Raft role callback
synchronously records its exact edge in `MetaLeadershipRelay` before scheduling
a Bycorf drain, so a stalled worker or coordinator cannot collapse a rapid
Leader/Follower/Leader sequence into its final role. The relay also preserves
edges racing startup attachment and makes shutdown detachment a lifetime
barrier. `MetaCoordinator` consumes one ordered event queue for reconciler
registration and role edges. A Follower event always cancels and joins every
leader reconciler, then invalidates volatile observations, before a later
Leader event can restart anything. Promotion still waits for Raft to catch
the state machine up.

`MetaFailoverReconciler` is a leader-scoped, level-triggered driver. It starts
from the complete committed view on every eligible leadership epoch, wakes on
commits, and periodically re-evaluates observation TTLs. Each pass derives at
most one of the eight typed commands; proposal completion is only a wakeup, not
workflow state. Demotion or shutdown cancels and joins local planning/proposal
work while leaving the committed transition for the next leader. A leadership
warmup equal to the observation grace prevents a new leader from treating
not-yet-reported boots as failures. The process derives that grace as at least
the Raft election upper bound plus the maximum supported Data reconnect delay,
and never shorter than the observation TTL. The delay bound retains ten seconds
for older Data binaries during rolling upgrades; current clients retry within
120 ms, but the session handshake does not negotiate their retry policy. This
covers a healthy Data process that just misses the winning election round without delaying explicit disconnect or
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

Before an unauthorized uncontrolled action with a comparable source domain can
prepare, Meta first excludes any old finite write lease using its
suspend-aware authority guard. It then
proposes `StartCandidateRecovery` with one absolute Unix-millisecond cutoff.
The proposing leader latches the current recovery budget and preserves that
cutoff across retries. The committed proposal carries its proposal-time cutoff,
so commit and dispatch delay consume the budget. Deterministic apply records it
on the Group transition without reading a clock. Once committed,
Candidate/action replacement, donor loss, policy update, and Meta leadership
replacement cannot extend it. An uncommitted proposal lost with its leader
does not establish a recoverable cutoff. Zero budget skips active gathering,
not normal retention or post-cutover partial reparent. A missing eligible
Candidate continues to wait after expiry; expiry does not terminate failover.

Data discovers donors from the exact FDS member directory and owns all transfer
planning. Meta retains no coverage or transfer journal. Before prepare
authorization, it requires a fresh, authenticated `CandidateRecoveryComplete`
for this action/deadline whose actual complete vector equals current Candidate
Progress. A retained already-authorized lossless action never reopens recovery.
Explicit operator recovery has no historical frontier to compare or gather;
it retains its separate unknown-loss authorization over readable local storage.
The recovery policy is independent of automatic detection; changing only that
family does not reset SUSPECT. Detailed event ownership and reparent semantics
belong to [Replication](05-replication.md).

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
authenticated session generation and boot; the local control installed on that session;
TTL freshness, draining, storage and population health; and causal lease
confirmation. The exact Unserviceable reason precedence is `session_missing`,
`heartbeat_expired`, `draining`, `storage_unready`, then
`population_unready`. A mismatched session/boot identity or an unjoinable
runtime/observation cut is Indeterminate until a coherent cut arrives; the
detector does not manufacture failure from elapsed time when the possible
lease deadline is unknown. For an exact session, an old local control anchor or a valid
grant Ack not yet causally confirmed is Indeterminate only while its known
possible Authority Lease remains inside that installed projection's effective
duration. Known expiry becomes `heartbeat_expired` even if the committed and
runtime local control have already advanced. The observation store retains that old
effective duration with the same atomic heartbeat cut, starts its finite
causal-progress interval at the first heartbeat for an exact session and Owner
projection, and refreshes it only when a strictly higher granted Ack is
causally confirmed. A newer Policy therefore neither shortens nor extends a
lease already installed by Data.

After a new Leader's `2D` authority-handoff quarantine, every first-send Grant
attempt enters a finite possible-lease envelope before the socket write. A
failed write is delivery-ambiguous, so the envelope uses that heartbeat's
steady receive time and the exact effective duration. The store keeps the
maximum unconfirmed deadline for the same Owner authority across projection
or duration replacement. Confirmation of
the latest ordered Grant collapses older possibilities into that exact
installed window; the installed window itself survives later same-authority
local control update. A session/boot, Owner, assignment, or Group Term change
clears both. Thus a Policy/local control update cannot erase a lease
Data may still retain, while confirmation of a shorter replacement Grant does
not unnecessarily preserve an older longer deadline.

The handoff-pending denial is likewise recorded before its first send and bound
to the exact session, authority, and heartbeat sequence. It survives a
same-authority local control update that clears projection-local runtime status. A
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
runs continuously and reports `HEALTHY`, `SUSPECT`, `BLOCKED`, or `TRIGGERING`.
There is no enable/disable Policy; `suspect_after_ms` controls only the finite
debounce interval. The detector uses
an injected monotonic-millisecond cut (production `steady_clock`) to accumulate
only exact Unserviceable time. A reason change does not clear elapsed time.
Indeterminate evidence for the same complete anchor freezes and later resumes
it; Serviceable evidence clears it. A change
to leadership generation or any revisioned eligibility interruption (including
false-to-true entirely between detector polls), Owner/assignment, Group term,
either current Policy version, or threshold discards it. A new or newly
eligible leader first completes the
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

`MetaControlProjector` reads one atomic committed view. Initial connection and
reconnection receive a complete `FullDesiredState` containing cluster discovery,
Group state, referenced manifests, resolved lease duration, and the recipient's
current tasks. Data selects its own control state and releases that input.
It retains global routes and endpoints, only its local Group's assignments,
population manifest and failover control, the Meta directory, and its tasks.
Remote Group execution details have no Data-side owner.

Established sessions receive `NodeControlUpdate`. Routing, local Group control,
and the Meta directory are complete objects with independent session revisions;
tasks use a base/revision delta of upserts and removals. Only changed objects
are sent. A remote rebuild or candidate preparation that leaves routes unchanged
produces no update for this node. Revisions advance only for the affected object;
the bootstrap seed does not require Data to follow Meta's Raft applied index.
A task body is delivered once in bootstrap or an upsert and enters execution
directly, without another full Directive message. Group Term, assignment,
manifest/partition epochs, action and attempt identities retain their own
business scopes. The local control revision binds lease challenges and fencing;
request IDs correlate update acknowledgements, not state versions.

Complete objects ignore older revisions and reject conflicting equal revisions.
A task delta requires its base revision or an identical replay; a gap closes
the session and bootstrap restores current state. Data validates the selected
state before installation. Routing-only updates preserve local execution,
source exports, population readiness and finite leases. Local control or task
changes quiesce heartbeat/control work and reconcile before acknowledgement.
Data derives heartbeat cadence as `max(1 ms, resolved lease duration / 3)`.
Policy documents and versions remain Meta-owned. `steady_replication_enabled`
is true only for a Created cluster; explicit population or failover work
otherwise owns local replication ingress.

Fencing and large-object publication share the serialized priority writer.
An incomplete superseded update is aborted by object ID and retried within the
session. Once Data can apply a direct update or completed transfer, Meta consumes
its exact acknowledgement and adopts that intermediate baseline before retrying.
A partial transfer never changes Data control state. Reconnection starts from
fresh bootstrap and does not depend on retained delta history. Protocol v1
layouts evolve in place for fresh clusters, without migration or mixed-version
negotiation.

The Data-control wire protocol has a fixed versioned header, per-direction
sequence, payload length, and CRC32C. Frames are bounded to 16 KiB. Larger
objects use Start/Chunk/End with object identity, ordered offsets, and a
declared total length; desired state is capped at 512 MiB and individual
opaque directive or result fields at 256 KiB. These are defensive ceilings rather than
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
FDS and object transfers carry no whole-object content hash. Frame CRC32C
checks accidental transport corruption; mutual TLS authenticates the channel
and protects transmitted records. Session identity, local control revision, Group Term,
and assignment enforce message freshness and authority. Structural validation
does not allocate another projection-sized encoded buffer. Receive-side
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
An accepted Data-control or Admin session borrows its Bycorf `Connection`
storage before spawning the session coroutine. During frame destruction, its
owner unregisters the task and releases the borrow after body-local Connection
users have unwound. Shutdown, demotion, or a watchdog may retire the transport
immediately, but Bycorf cannot reclaim the borrowed storage while suspended
session code can still resume and dereference it.
The full projection batches share one weighted 2 GiB budget derived
as two overlapping generations times encoded-plus-decoded 512 MiB size
classes. Each build reserves the encoded/decoded pair before projection and
then adjusts to the batch's actual retained string/vector capacities. The
permit follows shared ownership through transfer and live installation, so
4096 small sessions remain possible while a few abuse-sized projections cannot
multiply common topology and manifest data into a TiB-scale allocation.
The selected per-session state and temporary update objects are additional
allocations bounded by the protocol's object limits; the batch budget is not
a limit on total process memory.

Heartbeat is the periodic Data-to-Meta observation message. Protocol v1 carries
common health followed by exactly one tagged steady-state role payload: no role
information, an authority lease request, or replica candidate progress, plus
an independent optional failover observation. A session accepts
only the next business sequence. Data never retries within a session;
timeout closes it, and a new session starts a fresh sequence. Duplicate,
regressing, and skipped sequences close the session. Because
Data sends the next sequence only after processing the preceding Ack, receipt
of heartbeat `N+1` is causal confirmation of Ack `N`. Meta retains that proof
only when `N` granted a nonzero lease for the same authenticated session/boot
and exact installed Owner projection, assignment, and Group Term.
Ack write completion alone is not confirmation. Data
quiesces heartbeat projection reads during a local control update. Any outstanding
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
`CandidateRecoveryComplete`, `CandidatePrepared`, or `ActionFailed`, bound to the exact transition/action and
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
Meta derives role from committed local control facts rather than trusting the tag, and
replaces common health plus candidate state under one observation-store lock.
An authority heartbeat clears ordinary candidate progress, but may retain an
unready, storage-healthy node's operator-recovery availability with its original
receive time and TTL. Availability still requires the current boot/session and
committed population scope at selection. No-role heartbeats, rejected reports,
changed readiness or unhealthy storage withdraw it. Session teardown also
withdraws the exact generation's candidate immediately; a stale teardown cannot clear evidence
from a replacement generation.
The same atomic replacement stores the exact candidate-action basis from the
local control installed on that authenticated session: group term, transition revision,
action, assignment, and boot. The failover planner treats a role omission or
different candidate as terminal only when that basis matches the current
unauthorized action. A heartbeat from an older or unknown projection remains
warmup evidence, so `BeginUncontrolledFailover` cannot invalidate its
preselected candidate merely by advancing the committed term before the new
local control update arrives. Authorization may temporarily suppress the ordinary role during
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

Raft's peer-response expiry uses active `CLOCK_MONOTONIC` time, so a Meta
host suspend can otherwise preserve an old process's cached leader verdict
while other members elect a replacement. A leader-scoped Data-control task
continuously compares that clock with `CLOCK_BOOTTIME`, including when no Data
session is active; accumulated suspend divergence of at least
`D` closes the leadership generation's authority sessions and synchronously
requests immediate Raft resignation. In a multi-member cluster the old
generation cannot become eligible again. A sole
member may reopen leadership only after running for another full `D` of active
time; a further suspend restarts the wait. Live control boundaries, directives,
result proposals, and grants all pass this barrier. It covers the same-identity
case whose ordinary `2D` handoff entry matured before suspension.

Population directives separate the wire recipient from the rebuild target:
rebuild is delivered to the target, while authorize/revoke is delivered to the
source.
The common assignment field always names the target membership incarnation;
the durable directive carries a separate source assignment and the committed
partition replication epoch. Both must exactly match committed topology and
the installed group view.
The durable and wire codecs retain only the kind-specific bounded `payload`.
Mutation classification follows directive kind, so it cannot disagree with a
sender-supplied flag. V1 uses `payload` for `initialize-empty-population`, where it
carries the target Data session's authenticated replication-history id.
`rebuild` and `authorize-source` share a versioned payload containing the source
flow count advertised with its boot/history in `ClientHello`. Meta commits that
layout in both directives; projection and replay never infer it from the
recipient's workers or a newer source session. Source authorization and the
native target handshake both check it against the actual source layout.
`revoke-sources` requires an empty payload.
Meta transition apply and Data admission reject malformed or misplaced bodies,
so the replication adapter cannot silently ignore a predicate or override.
Operation, durable directive, execution attempt, and assignment-incarnation
identities remain distinct. Assignment ids are proposer-generated
128-bit values that are never reused across incarnations; the topology store
retains the most recent value per node to reject direct remove/re-add replay
without growing an unbounded historical set. `DirectiveResponse` reports whether the exact idempotent request started.
A pre-admission failure produces a rejected result; failure after admission is
an execution failure. There is no retained receipt-stage state machine, and
Meta does not require a progress response before accepting a final result.
The terminal result is authoritative
only after Meta commits an exact `MetaTerminalReceipt` through Raft and responds
`ResultCommitted`. That first commit advances the live operation revision, so
an already-issued phase mutation cannot pass its old CAS after the terminal
result becomes durable; an identical result replay is idempotent and does not
advance it again, while conflicting status or result bytes fail closed.
`ResultCommitted` acknowledges the exact directive attempt identity and original
commit index; the Data-side attempt completion is immutable. Before a first
result is committed, apply revalidates the
matching live directive against that same current aggregate anchor; a result
racing an anchor mutation is therefore rejected even if a future mutation
path were to miss eager cleanup. An already committed receipt remains
immutable history and an exact retry still resolves to its original commit
index after the authority later advances. Terminal receipts are pruned only
through an explicit replicated command after the retry-retention window.

Generic operation evidence has no wire type, observation cache, phase-command
field, or durable summary. Optional progress has explicit business semantics.

Failover uses its Group transition rather than directive terminal receipts. Its three typed
heartbeat observations are volatile inputs to a typed transition command. The
Raft cutover command, not observation receipt, is the boundary between
Data-local preparation and action-bound activation. A control stream may remain
connected across it, but Data must consume the later committed local control and a valid
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
and population anchors. Typed candidate and failover query results retain the authenticated reporter
boot alongside assignment, so reconciliation does not race a second session
lookup. A committed population-epoch change invalidates older observations
even when term and manifest do not move.
Committed changes proactively purge stale evidence,
and queries filter again against one current committed snapshot. Startup and
each entered leader epoch clear soft state; every observed follower edge clears
sessions, observations, and their local diagnostic ring even when another
leader edge is already waiting behind it.

Soft-state memory is bounded independently of its field validators. The store
admits at most the committed node-domain cap of 4096 session keys and candidate
reporters per group and at most 65,536 total observations.
Using the full node-domain bound for candidates avoids making report arrival
order an implicit member-selection policy. Exact logical byte accounting covers
retained variable fields and their lookup-key copies: the global 68 MiB budget
is one direct frame plus identifier allowance per maximum node, while a node's
17 KiB share holds one maximum heartbeat frame and its index allowance. Replacement, generation purge, revalidation, TTL
expiry, and leader reset update the same counters. Ordinary diagnostic
ingestion keeps its previous latest-wins value on capacity rejection.
Rejecting diagnostic health text does not discard the heartbeat's fixed-size
typed owner health used for serviceability.
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

The durable source of truth is the newest published etcd snapshot plus the
following Raft WAL. The [runtime document](10-meta-raft.md#durable-root-and-recovery)
defines publication, compaction, crash recovery, genesis and join evidence.
C++ snapshot capture excludes later application and labels exactly the captured
cut. Installation validates the whole candidate before replacing any store.
The stored application watermark is the snapshot index; recovery replays only
the WAL's persisted committed suffix before opening peer ingress. Uncommitted
entries require subsequent Raft commitment and may instead be replaced.
A timeout remains an uncertain proposal outcome, reconciled by operation ID.

## Format compatibility

The Meta/Data control wire, commands, stores, records, operation intents,
exports, Admin binary payloads, and
cluster-status JSON schema use their current v1 layouts. Before Lavik's
first stable release, development layouts are replaced in place for fresh
clusters without a legacy decoder, migration, or mixed-layout negotiation.
Equal markers do not make earlier development state interchangeable.

The aggregate snapshot contains six stores. Each Topology Group stores its
single term/owner, authority-active bit, and optional activation action.
Operation records and phase commands contain no evidence summaries. Policy
snapshots retain registered-family raw histories and typed-decodable current
values.

Every configured Meta identity has one canonical concrete numeric Data-control
endpoint and one canonical concrete numeric Admin endpoint. The C++ `LMI1`
descriptor carries server id, principal and both endpoints. The bridge carries
these decoded fields and the Raft endpoint in committed configuration contexts
and snapshots. The descriptor
and committed identity binding must agree exactly. Advertised Data-control and
Admin addresses may route through an explicit proxy instead of equaling local
binds; restart may likewise rebind a Raft listener behind a transport proxy
without changing its durable advertised endpoint. Endpoints are immutable and
unique within their respective directories, and change only through retirement
and replacement with a fresh server id. Recovery validates descriptors and the
committed directory together; partial descriptors are rejected.

Incompatible development data directories are recreated. Meta has no in-band
schema-switch command; incompatible changes require coordinated replacement
of communicating binaries. Readers reject unknown markers, malformed fields,
and trailing bytes; the etcd WAL and snapshot readers validate their own framing
and checksums. These checks cannot detect every incompatible same-marker layout.

## Authentication, membership, and audit

Raft transport is plaintext by default and optional mTLS authenticates the
canonical Meta principal against the CA and committed descriptor. Plaintext
membership checks rely on network isolation. The runtime publishes a small
immutable peer authorization view after actual identity/configuration apply;
network and heartbeat paths never copy `MetaStores` or take its state lock.
Genesis and waiting-joiner grace are bounded by durable evidence and the applied
cut described in [Meta Raft](10-meta-raft.md#durable-root-and-recovery).
Retirement always wins over grace and queued connections.

Initial identity recovery is not a Cluster Create child operation. The
existing `MetaMembershipReconciler` holds the shared membership gate, compares
the loaded genesis descriptors with the identity store, and proposes one
idempotent `BindMetaMember` at a time. Matching crash prefixes resume; any
descriptor conflict fails closed. Once this convergence is complete, the
same reconciler handles only ordinary reusable membership workflows.

Dynamic membership is a leader-owned `meta-membership-workflow-v1` operation.
Before either identity or Raft mutation, Admin commits a bounded versioned
intent containing the requested target, all three endpoints, principal, and
the baseline peer descriptors and identity bindings. Peer descriptors retain
voter/joiner flags, priority and data-center attributes; election-only config
log indices are not semantic membership changes. A recovered owner accepts
only that exact baseline, the expected learner intermediate state, or requested post-state, never overwrites a changed
peer set or reactivates a retired identity. Legacy partial changes without an
intent are not inferred as authorized workflows.

`MetaMembershipReconciler` scans the recovered non-terminal journal on each
leader transition. Add binds identity before invoking `add_srv`; remove
observes the committed configuration without the member before retiring its
identity. Learner promotion additionally requires fresh actual application
through the current commit. Submission alone is not a commit certificate.
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
Raft API entry and local proposals; remote-result callbacks own inert result
storage, not a reconciler or leader context. The next owner retries from the
committed state. Incompatible recovery retains an inspectable
`recovery-required` phase instead of guessing a rollback.
The retired binding also disambiguates the short interval after removal commits
but before Raft publishes its new in-memory configuration. Reactivation of
retired principals is rejected. Every member commits numeric Data-control and
Admin endpoints in canonical `IPv4:port` or `[IPv6]:port` spelling. Data seeds
use the former directory; operator discovery uses the latter. Neither
directory implies that every follower is currently reachable.
Data-node identities use canonical `lavik://node/<node-id>` principals with
global one-to-one binding.

Data control follows the Raft transport's optional mTLS mode instead of adding
a second Meta certificate configuration. The listener reuses that member's
Raft CA/certificate/key and its sole `lavik://meta/<server-id>` URI SAN. A
Data client reuses its replication TLS CA/certificate/key and must present its
committed sole `lavik://node/<node-id>` URI SAN. TLS is all-or-none on each
side; plaintext deployments rely on network isolation and never silently
downgrade a partially configured identity.

Local administration uses a mode-0600 Unix socket and derives a canonical
operator actor from Linux `SO_PEERCRED`, constrained by an explicit UID
allowlist. Its parent directory must not be group- or world-writable, and the
listener records the bound inode so shutdown never unlinks a replacement path.
Remote administration is plaintext when no control TLS identity is configured,
matching the Raft transport default. A plaintext listener grants operator
authority to every reachable peer and records the fixed
`lavik://operator/plaintext` actor, so trusted network reachability is its
security boundary. Deployments requiring authenticated peer identity configure
mutual TLS and use the peer's canonical URI SAN. Role-based authorization then
separates operators, Meta members, and data-node self-reporting; actor fields on
the wire are never trusted.
Certificate validity is enforced by TLS, but online issuance, rotation, CRL,
and OCSP integration are outside this module.
The `lavik-ctl` operator client uses one Raft-free Admin transport for
Unix, plaintext TCP, and mTLS TCP. Direct commands address the selected member;
`status` reports its local state. The `cluster-status` command discovers the
leader and evaluates cluster readiness. The transport handles partial I/O
under one absolute deadline. Direct commands support a TLS server-name
override; cluster discovery verifies each numeric Admin IP against the
certificate IP SAN and never falls back between TLS and plaintext. A Unix
seed can use configured TLS credentials for subsequent remote leader access.

`lavik-ctl cluster-status` normally performs exactly two reads:
`clusterhead 1` against the supplied seed to learn the current committed Admin
directory, then `clusterstatus 1` against the indicated leader. Redirect,
leader-change, busy, and incomplete-catch-up results retry discovery only
within the original deadline. Leader routing stays internal to the operator
API. The client reads this leader-observed cut without probing followers or
reporting their replication progress. A stable
result therefore states only leader-observed Meta availability and committed
membership consistency; a quorum-serving leader can report the cluster ready
while one follower is unreachable.

`lavik-ctl failover GROUP` uses the same leader discovery, verifies a Created
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
Evaluation and encoding run on the bounded proposal executor, not the Bycorf
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
its strict inner payload and JSON rendering both use schema v1. Every Group
includes `automatic_failover_state`, optional
`current_reason`, `suspect_elapsed_ms`, `effective_threshold_ms`, and optional
`blocked_reason`. The state itself records when a Begin is being triggered;
the threshold is a derived scalar rather than Policy identity or content.
These are a bounded leader-local diagnostic cut and do not alter readiness. A Created Group
missing the first complete detector publication is conservatively `blocked`
with `indeterminate_evidence`. A pre-Genesis Group without its Policy is also
`BLOCKED`; only that pre-Policy state may have a zero threshold.
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

`lavik-ctl cluster-create` reuses the same private leader discovery and
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
endpoint class is unique, and the principal derives as `lavik://meta/<id>`.
Slots are either generated with `contiguous-even` after sorting Group ids or supplied as a
complete, non-overlapping `0..16383` range table. All
declared Data belongs to exactly one Group and every Group owns at least one
slot. An optional strict `[bootstrap_policy]` table supplies Bootstrap Policy
Defaults for `automatic_uncontrolled_failover_suspect_after_ms`,
`authority_lease_duration_ms`, and `candidate_recovery_budget_ms`; omitted values
default to 5000 ms, 5000 ms, and 2000 ms respectively and must satisfy the
registered family ranges. The parser
rejects unknown TOML structure and files over 64 KiB, then
sorts nodes, Groups, replicas and ranges and merges adjacent ranges belonging
to the same Group. The CLI renders that canonical plan and requires exact
lowercase `yes` unless `--yes` is present. The manifest, request, and persisted
intent use version 1 with this normalized multi-Group and multi-Meta shape
only; earlier development layouts have no compatibility decoder.

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
state-machine commit index at least `B`. Raft heartbeats always report actual
application progress, independently of ordinary durable-majority completion. The
reconciler also requires exact agreement among the manifest Meta vector,
Raft descriptors, committed
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
runtime capabilities at each local execution update, but the selected task replay count
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
summary, and verdict. Exports carry complete records and drop watermarks.
An external archive uses its own deployment namespace and deduplicates by log
index, checking complete record equality on duplicate exports; Lavik persists
no separate cluster identity. Pruning removes a prefix through an explicitly
named record and advances the committed prune floor. Audit is an operational
record, not a cryptographically tamper-evident chain.
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
audit history rather than replacing it.

## Source map

| Claim | Repository source |
|---|---|
| Public Meta boundaries, commands, store composition, and correctness contracts | `include/lavik/meta/` |
| Deterministic apply, stores, coordinator, observations, and administrative protocol implementations | `src/meta/` |
| Per-Group durable failover transition, eight typed commands, exact replay/CAS apply, leader-resumable planner, controlled Admin entry, and structured commit logs | `include/lavik/meta/commands.h`, `include/lavik/meta/failover.h`, `include/lavik/meta/failover_reconciler.h`, `include/lavik/meta/failover_admin.h`, `src/meta/failover.cpp`, `src/meta/failover_reconciler.cpp`, `src/meta/failover_admin.cpp`, `src/meta/state_apply.cpp`, `src/meta/state_machine.cpp`, `src/meta/ctl_server.cpp` |
| Registered typed durable Policy families, strict raw JSON admission/history, and current-value accessors | `include/lavik/meta/policy_store.h`, `src/meta/policy_store.cpp`, `tests/meta_stores_test.cpp` |
| Pure Owner Serviceability cut, causal lease confirmation, leader-local detector state, automatic Begin adapter, and generation-bracketed diagnostics | `include/lavik/meta/owner_serviceability.h`, `src/meta/owner_serviceability.cpp`, `include/lavik/meta/automatic_failover_detector.h`, `src/meta/automatic_failover_detector.cpp`, `include/lavik/meta/automatic_failover_reconciler.h`, `src/meta/automatic_failover_reconciler.cpp` |
| Volatile candidate/failover observations and deterministic compatibility-domain plan selection | `include/lavik/meta/observation_store.h`, `src/meta/observation_store.cpp`, `include/lavik/meta/candidate_plan.h`, `src/meta/candidate_plan.cpp` |
| Pure per-node projection including resolved lease duration, Data-derived heartbeat cadence, and failover/activation/follow-owner state, plus the leader-scoped Data-session publisher and causal heartbeat admission | `include/lavik/meta/control_projector.h`, `src/meta/control_projector.cpp`, `include/lavik/meta/data_control_server.h`, `src/meta/data_control_server.cpp` |
| Manifest-bootstrapped initial Meta configuration, persistent restart/waiting-joiner classification, and Raft durability | `raft/engine/storage.go`, `raft/engine/join.go`, `app/lavik_meta.cpp`, `tests/meta_integration/gate_initial_meta.py` |
| Atomic Genesis lifecycle, strict Bootstrap Policy Defaults, durable creation admission, Meta catch-up barrier, and leader-owned recovery | `include/lavik/meta/cluster_create.h`, `src/meta/cluster_create.cpp`, `include/lavik/meta/topology_store.h`, `src/meta/topology_store.cpp`, `src/meta/state_apply.cpp`, `src/meta/ctl_server.cpp`, `include/lavik/meta/cluster_create_reconciler.h`, `src/meta/cluster_create_reconciler.cpp`, `app/lavik_meta.cpp` |
| Durable post-genesis Meta membership intent, exact-config recovery, leadership handoff, and identity retirement | `include/lavik/meta/membership_reconciler.h`, `src/meta/membership_reconciler.cpp`, `src/meta/ctl_server.cpp`, `src/meta/state_apply.cpp`, `tests/meta_integration/gate_membership_recovery.py` |
| Shared Meta/Data frame, object-transfer, failover observation, transition, and activation formats | `include/lavik/cluster/control_protocol.h`, `include/lavik/cluster/control_transport.h`, `src/cluster/control_protocol.cpp`, `src/cluster/control_transport.cpp` |
| Asynchronous Raft protocol, WAL, snapshots, authentication and quorum liveness | [Meta Raft runtime](10-meta-raft.md), `raft/engine/`, `include/lavik/meta/raft.h`, `src/meta/raft.cpp`, `src/meta/proposal_executor.cpp` |
| Meta session transport retirement and Connection-storage lifetime | `src/meta/ctl_server.cpp`, `src/meta/data_control_server.cpp`, `bycorf/include/bycorf/net/connection.h`, `bycorf/src/runtime/worker.cpp` |
| Foreign-thread typed completion ingress and worker wakeup | `bycorf/include/bycorf/runtime/foreign_executor.h`, `bycorf/src/runtime/foreign_executor.cpp`, `bycorf/include/bycorf/runtime/cross_core.h`, `bycorf/src/runtime/worker.cpp` |
| TLS identity, RBAC, Unix peer credentials, Admin transport, cluster status, controlled failover, and initial cluster creation | `include/lavik/meta/identity_verifier.h`, `include/lavik/meta/ctl_server.h`, `include/lavik/meta/admin_client.h`, `include/lavik/meta/cluster_status.h`, `include/lavik/meta/cluster_create.h`, `include/lavik/meta/failover_admin.h`, `app/lavik_meta.cpp`, `app/lavik_ctl.cpp`, `bycorf/src/net/` |
| Automatic-failover status wire/model plus JSON and text rendering | `include/lavik/meta/cluster_status.h`, `src/meta/cluster_status.cpp`, `tests/meta_cluster_status_test.cpp` |
| Recovery, partition, membership, failover, and security gates | `tests/meta_*`, `tests/meta_integration/` |
