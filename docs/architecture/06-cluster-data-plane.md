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

# Cluster data plane

## Responsibility and boundary

This subsystem makes a Lavik process a Redis Cluster data node: it decides,
for every client command, whether this node may serve it, must redirect it to
the owning node, or must refuse it with a standard cluster error. Ordinary
requests never consult an external control plane; each node answers from its
locally committed view of slot ownership, authority, and readiness.

The module under `include/lavik/cluster/` and `src/cluster/` exposes five
seams:

- `TopologyCache` holds the committed `ServingState` and publishes it
  atomically.
- `AuthorityGuard` is the request-path authority boundary:
  `CaptureAndAdmit` records the routing and finite-lease proof, while
  `RegisterAndRecheck` atomically registers in-flight work and closes the
  publication race. Free `Admit` and `AuthorityUnchanged` are pure routing
  helpers, not request-path substitutes.
- `ClusterRouter` is a set of pure functions over a committed state: slot to
  owning node, MOVED target, and client-facing endpoint selection.
- `NodeControlInstaller` is the only state-changing seam. It validates and
  atomically publishes full state, installs finite leases and fences, drains
  retired or controlled-paused request generations, revokes source
  capabilities, and reconciles typed replication directives, failover actions,
  activation, and the steady follow-owner relationship.
- `MetaControlClientService` owns leader discovery and the one outbound
  Meta-to-Data control session. It maps authenticated wire values into the
  node controller; neither it nor replication writes `TopologyCache`
  directly.

The Redis/storage boundary adds a transport-neutral final seam:
`storage::MutationPrecondition` carries the captured admission through every
suspending storage preparation step and validates it at logical publication.
Storage depends only on a callback and opaque shared context, not on cluster
types.

The module is free of Redis wire concerns. Wire mapping (error texts,
discovery reply shapes) lives in the Redis serving layer, and internal
authority state (terms, grants, fence reasons) never crosses into RESP. The
hash-slot function is shared with storage: keys hash with
`storage::RedisSlot` (CRC16-XMODEM with `{hashtag}` support, modulo 16384),
and a static assertion ties the two slot spaces together. Slot ownership
decides *which node* serves a key; the worker mapping inside a node
(`slot % worker_count`) is unchanged, and transaction coordination keeps its
own arbitration boundary — the cluster layer reaches it only through a
generic per-shard validator hook that knows nothing about clusters.

Cluster mode is a startup-only, process-wide choice. The runtime
(`ClusterRuntime`: topology cache, authority guard, node controller, action
adapter, and resolved announce addresses) is installed
before any listener accepts a client; in standalone mode it is null and every
cluster code path is inert.

## ServingState: the published unit of truth

A `ServingState` is an immutable snapshot built and validated as one unit:
the node table, the shard groups with their slot ranges, per-group authority
(primary identity, term, grant) and readiness (storage and population), and
the computed slot-to-group map. Construction rejects malformed node ids,
duplicate node or group ids, dangling primary or replica references, and
inverted, out-of-range, or overlapping slot ranges. Coverage may be partial:
an unbound slot is a first-class state, not an error.

Publication is a single atomic `shared_ptr` swap, so topology, grants, and
readiness always appear together. A writer mutex serializes the rare
control-plane publications, and an odd/even publication sequence brackets the
snapshot store and version update. Readers accept a snapshot/version pair only
when equal even sequence reads surround it, so they cannot pair a newly stored
snapshot with the preceding version. The cache also carries a monotonic logical
version for publication ordering and tests. A content-identical republication
(decided by a content hash over the semantic state) is a no-op that keeps the
existing snapshot, version, and sequence, so projection replay is invisible.
Authority decisions never consult the global version: admission captures the
snapshot it decided against, and the owner-side re-check compares a per-group
authority token — owner identity, term, grant, and readiness, precomputed at
build time — so an unrelated group's republication does not disturb
in-flight work.

Request-path reads go through a thread-local snapshot cache that re-reads only
the publication sequence per call; a hit returns the last observed snapshot
with no shared-memory writes at all. This keeps the admission gate free of
cross-worker serialization — a direct `atomic<shared_ptr>` load per request
would serialize on the toolchain's internal spin bit.

Readiness is per group and deliberately excludes the grant bit: a fenced
group is not "still loading", it has no safe owner. Keyed requests consult
only the groups their slots map to, so one group's recovery does not stall
traffic owned by healthy groups; keyless commands consult the aggregate.
Meta does not serialize a Data process's boot-local population proof. During
a local control update, NodeControl carries an existing positive proof only across
an exact match of the Group, local and Owner assignments, Group term, manifest,
and partition replication epoch. Explicit readiness loss or any changed anchor
clears it before publication. Consequently a Controlled Failover pause can
close mutation admission without manufacturing a transient LOADING interval or
revoking an otherwise unchanged finite lease.

## Admission and fencing

`Admit` is a pure function over one committed snapshot and a request view
(distinct key slots, write intent, connection READONLY state, loading
whitelist membership), so the full decision matrix is testable offline. Its
evaluation order mirrors Redis `getNodeByQuery`:

The request record retains only the first slot and, when present, one distinct
slot as a CROSSSLOT witness. More distinct slots cannot change the decision,
and every request that proceeds beyond admission therefore carries exactly one
slot without a general-purpose vector allocation or footprint.

1. Loading: no committed snapshot, or an involved group is not ready. Only
   whitelisted commands (health, discovery, configuration, subscription
   management) are served; everything else is LOADING.
2. First-key coverage: the first key's slot has no owner, or its owner is
   this node's fenced group — CLUSTERDOWN. This outranks the cross-slot
   check, matching Redis.
3. Cross-slot: the remaining keys hash to other slots — CROSSSLOT.
4. Ownership: this node's granted group owns the slot — serve, except that a
   write during its controlled-failover mutation pause returns `TRYAGAIN
   Failover in progress`; a READONLY connection on a replica of the owning
   group serves reads locally as an explicitly stale read; otherwise MOVED to
   the primary. A fenced remote primary still receives the redirect: the
   target applies its own grant gate and answers CLUSTERDOWN, so a redirect
   never lands a client on a writable fenced node.

Commands without keys — including commands whose key extraction fails, such
as a malformed `EVAL` numkeys — admit locally (readiness still applies) and
produce their own argument errors, the same treatment Redis gives zero-key
commands. Runtime `PUBLISH` is the exception: it derives a slot from the
channel so controlled failover can pause and drain it like every other source
mutation.

Admission alone cannot fence writes: a request admitted just before a
topology change could mutate afterwards. Two owner-side re-check choke points
close the outer dispatch and transaction-scheduling races after publisher,
database, snapshot, and order-gate waits:

1. Non-transactional writes re-check in `ExecuteCommandBody` after the
   database gate is held. A changed authority means nothing has executed yet,
   so the request is re-admitted against the current snapshot and answered
   honestly with a redirect or error; a benign republication re-arms the
   request with the current snapshot.
2. Transactional writes install a per-shard validator on the
   `tx::Transaction`. The hook runs on the owner shard immediately before
   every shard callback and aborts that shard when authority changed. A
   single-shard transaction provably ran nothing and gets the re-admission
   answer; a multi-shard transaction may have mutated a shard whose check
   raced the fence, so its outcome is undeterminable and the connection
   closes without a fabricated reply.

Handlers can still suspend after either choke point while acquiring key/store
locks, reading an old value, or allocating storage. Every client keyspace write
therefore carries the same admission as a `storage::MutationPrecondition`.
Transactions copy it into each `TxShardWrites` receipt; direct handlers keep it
alive until their storage task completes. `WriteRecordLocked` evaluates the
nonblocking callback on the owning shard after all potentially suspending
preparation and immediately before WATCH invalidation and staging/index
publication. The hash no-op path performs the same check before its observable
WATCH invalidation. Internal rollback explicitly bypasses the inherited check
so it can restore an already-started transaction; background expiration,
maintenance, and replica replay carry no client precondition.

The shared admission records whether any storage publication began and whether
a later final check failed. If the first attempted mutation is rejected, the
command discards its prepared reply, re-admits against current state, and sends
that fresh redirect or error. If an earlier participant already began and a
later one is rejected, the aggregate outcome is indeterminate and the server
closes the connection without fabricating a retryable result. This finalizer
also covers errors swallowed inside Lua and the single-shard EXEC fast path.

`EXEC` re-evaluates the union of its queued commands' slots at execution
time: spanning slots fails the whole transaction with CROSSSLOT, and a write
transaction whose slot moved redirects or refuses as a whole. Its
single-shard fast path, which never builds a `tx::Transaction`, re-checks
after taking the key guard. Lua `redis.call` writes re-check before
dispatch and again on the owner hop; script key access is additionally
confined to the slot set the script was admitted with. Invoking a Function
loaded with the `no-cluster` flag is refused in cluster mode, with Redis's
exact error text. Top-level blocking writes (List and Sorted Set operations and
`XREADGROUP`) re-run admission on every attempt after reacquiring the database
gate. Each concrete storage attempt registers a fresh in-flight authority guard
and releases it before entering the waiter registry or sleeping; a dormant
client therefore cannot delay an authority drain. Read-only `XREAD` and the
keyless `WAIT` carry no mutation authority and register no such guard.
Replication replay is exempt from every re-check: applied commands are
already ordered by the replication stream and carry no client fencing
semantics.

Reads are intentionally not re-checked. A read gated at admission may observe
data committed before a concurrent fence — the same staleness window Redis
Cluster clients accept across failover. Writes have no such window: the
combination of admission, the owner-side choke points, the final storage
precondition, and per-group tokens guarantees a stale topology causes
redirection or temporary unavailability, never a second writer.

Only writes retain ownership of the admitted snapshot across suspension
points. Reads finish their decision while the thread-local cache keeps the
snapshot alive and carry no per-request shared reference afterwards, matching
their intentionally absent owner-side re-check.

Executions register their admitted groups when they pass the re-check and
unregister at completion, so a fence publisher can observe whether any
admitted write is still running for an affected group. The accounting lives
in per-group striped atomic counters owned by the published snapshot: the
request path performs one atomic increment on the registering thread's
stripe, with no lock and no allocation, and aggregation happens off the
request path. Publication shares the replaced snapshot's counter into every
group whose authority token is unchanged, so executions admitted under
token-equal snapshots drain together, while a changed group starts a fresh
counter and fencing drains the replaced snapshot's. Registration is bracketed
by publication-sequence loads — an odd sequence covers the state/version
update and a completed publication changes the even token — so a drain either
observes a concurrent registration or the registrant observes the sequence
change and rolls back.

Ordinary writes retain that registration through their handler. Top-level
blocking List and Sorted Set writes and `XREADGROUP` instead retain it only
across one concrete mutation attempt, because time spent waiting for data
performs no mutation and must not pin the replaced authority generation. Their
immediate EXEC and Lua forms never enter the waiter registry and remain inside
the enclosing transaction or script authority window.

The node controller retains the replaced immutable snapshot as a drain token.
A new lease, source authorization, destructive rebuild, or controlled source
frontier for that group is refused until the old counter reaches zero. An
explicit Fence is acknowledged only after the local fence is published, source
export capabilities are revoked and joined, and the old request generation has
drained. A controlled transition instead keeps the owner/grant serving reads
and established replication, rejects new mutations with TRYAGAIN, drains the
old mutation counter, and then quiesces expiration before capturing its stable
frontier. These acknowledgements and observations describe completed local
barriers rather than mere transport receipt.

## Meta control session and finite authority

A Meta-managed node starts fenced after every process restart. It restores no
serving checkpoint, lease, term floor, directive outcome, leader directory, or
other positive authority from local storage. The only durable authority is the
Meta Raft state; boot, replication-history, session, challenge, and lease state
are process memory. A fresh 160-bit boot identity prevents messages for a prior
process from becoming current even though the durable data files remain.

The Data process connects outward from configured numeric seed endpoints. It
tries the accepted leader hint, the latest in-memory committed directory, then
the configured bootstrap seeds, with full-jitter exponential backoff from one
to ten seconds.
An unresolved seed remains a fallback even when a learned member currently
announces the same endpoint, because endpoint ownership can legitimately change
across reconfiguration. A response replaces the in-memory directory atomically
only after its member identities and the peer's authenticated URI principal
agree. A learned dial target pins that exact prior committed principal against
the new `ServerHello`; only an unresolved configured seed may bootstrap its
binding from the authenticated Hello. The hint and directory are intentionally
not persisted. Connect, TLS, Hello, read progress, and write progress each have
a ten-second bound; backoff resets only after an accepted session has produced
a valid `HeartbeatAck`.

The session uses control protocol v1 with framing independent of TCP packets.
A fixed header carries type, length, per-direction sequence, and CRC32C. Frames
are at most 16 KiB. A frame-sized `FullDesiredState` is sent directly as one
typed frame. Complete objects that exceed one frame use Start/Chunk/End plus
a declared total length, object identity, and ordered chunk offsets; chunks
stream without per-frame application acknowledgements. Bootstrap and control updates get `FullStateApplied` (updates correlate by
request ID), tasks get `DirectiveResponse`, and final results get
`ResultCommitted` or `ResultNoLongerTracked`.
The complete desired-state cap is 512 MiB, while each opaque directive
or result field is capped at 256 KiB. These are abuse
ceilings, not normal sizing goals:
16 KiB keeps one authority frame cheap and indivisible; 256 KiB matches the
durable Meta payload-field bound; 512 MiB matches Meta's snapshot hard ceiling;
and the 64 KiB writer budget provides one frame of headroom for each priority
class while a large object streams one chunk at a time. One reader and one
serialized writer own each direction, read-ahead is disabled, and a ten-second
progress deadline closes a stuck session. That deadline is deliberately much
longer than the default Raft heartbeat/election cadence but still bounds failed
peer detection. Authority and reliable messages take precedence between bulk
frames; priority cannot interrupt bytes already handed to the kernel. Only one
complete-object transfer may be active at a time, so Start/Chunk/End sequences
cannot interleave. Queued transfers retain shared ownership of their encoded
bytes until completion or failure.
Meta also accounts every retained node projection by the capacities of its
encoded string and decoded owning graph under one 2 GiB worker-local budget
(`4 * 512 MiB`). A build first reserves 1 GiB (`2 * 512 MiB`) and then adjusts
to its measured weight; the permit follows the shared batch through initial
send, live installation, and replacement. The two-generation budget is derived
from an installed/replacement overlap, not the 4096 session count. A highly
structural object can therefore be rejected below its wire cap, and aggregate
normal projections cannot multiply the global topology into TiB-scale output
ownership.

FullDesiredState is only an initialization input after connect or reconnect.
Data selects global routing/discovery, its local Group control and referenced
manifest, the Meta directory, and its own tasks, then releases the full input.
It retains no remote full membership-assignment table, manifest, or failover workflow and tracks no
shared FDS applied index. `NodeControlUpdate` replaces changed routing, local
control, or directory objects at their own session revisions and applies task
upserts/removals with a base and target revision. Older complete objects are
ignored; conflicting equal revisions and task gaps are rejected. A reconnect
bootstraps current state rather than replaying incremental history.

Task bodies enter execution directly from bootstrap or an upsert. The sole
local `DesiredClusterControl` owns Group identity, owner/authority, population
and failover control; ServingState is derived for requests. Remote routes keep
only members/endpoints, slots, term, and current serving owner/assignment.
A remote Group's internal rebuild or failover preparation does not trigger an
update here. Routing-only publication preserves local tasks, source exports,
readiness and leases. The local control revision binds lease challenges and
fencing; task/action identities and Group term/assignment fence execution.
Local endpoint changes also advance local control because replication consumes
those endpoints.

Policy documents stay in Meta. Data uses the resolved lease duration, with
heartbeat cadence `max(1 ms, duration / 3)`, and rejects a zero duration or a
cadence beyond the negotiated observation TTL. A duration update affects
future grants without shortening an already installed finite lease.
An exact Grant that reaches Data at or after its local deadline is consumed but
not installed, and Data ends that Meta control session. Reusing the session for
the next heartbeat would falsely make its higher sequence a causal confirmation
of the rejected Grant. Session invalidation revokes any older retained
authority before reauthentication begins a new causal sequence; a later valid
Grant removes the local fence without a separate self-fence report.
Meta sets that bit only when the committed cluster lifecycle is `Created`;
Genesis keeps it false so explicit population directives exclusively own
ingress. Even when true, Data defers follow-owner reconciliation while an
active failover transition or a current local initialize/rebuild directive owns
ingress, then consumes local control after that blocker disappears.
Superseded incomplete updates abort by exact object ID within the same session.
If a direct update or TransferEnd can already be applied, Meta first consumes
its exact acknowledgement and adopts that state as the baseline before sending
the next delta. A partial transfer never mutates serving state. Requests and
acknowledgements do not require an unrelated Meta commit to advance any local
version. Replaying identical state preserves readiness and serving state.
Installation validates and builds the entire immutable state before one
publication; partial transfer never changes serving state. A
group's partition replication epoch cannot regress even when the global
topology epoch advances.
Membership entries carry node and assignment identities only. The committed
group owner and matching owner assignment are projected independently from
the active-grant bit: they classify the heartbeat role while `grant_active`
alone authorizes serving. Protocol v1 has no redundant member-role field that
could disagree with them.

Heartbeat carries common health followed by exactly one tagged steady-state
role payload: no role information, an authority lease request, or replica
candidate progress. A committed owner with an active renewable grant sends
lease requests. A restarted member without a clean shutdown proof alternates
lease requests with operator-recovery availability when it still occupies the
owner assignment; its unready health cannot renew authority. An inactive topology owner normally sends no role
information. The narrow exception is the historical owner retained by an
active uncontrolled target-term fence: with no grant and a coherent live Ready
frontier, it may report candidate progress using its authenticated current
boot/history and its own assignment at the preceding term as source lineage.
Recovered candidates instead retain the exact historical source boot/history
from a consumed clean shutdown proof, while their current boot remains fenced.
Operator-recovery availability contains only the completed local population's
scope, with no source lineage or vector; it cannot enter automatic selection.
This lets a surviving population re-enter selection without treating a
fenced owner as active authority. Other non-owner members with coherent
Ready frontiers also send candidate progress. Candidate
progress includes the exact local membership assignment, manifest revision and
digest, partition replication epoch, completed rebuild source lineage, and a
typed bounded next-LSN vector sampled after successful apply. Candidate and
health are volatile Meta observations, not Raft commands. The challenge names the
exact projection and complete group authority anchor. `sent_at` is captured
immediately before the first socket write, and the granted duration is applied
to that monotonic timestamp, so a delayed response cannot extend authority.
Business heartbeat sequences accept only the next message; duplicates, gaps,
and regressions close the session. Data does not retry within a session: a
missing Ack ends it, and reauthentication begins a new sequence. Data does
not send heartbeat `N+1` until it has processed Ack `N`, so `N+1` is causal
evidence that the immediately preceding lease decision reached Data. Meta
retains it as a confirmed Authority Lease only when Ack `N` granted a nonzero
lease for this authenticated session/boot and the exact installed Owner,
assignment, Group Term, and projection. Ack write alone does not confirm
authority. Meta records Owner heartbeat and
causal-progress receive times on the same steady clock used by detector
debounce, retains the original time when later heartbeats repeat a confirmation,
and advances it only for a strictly higher confirmed Ack. Once that causal
progress is older than the effective lease duration, otherwise-fresh health
traffic is classified as `heartbeat_expired`; wall-clock corrections do not
participate in either Owner freshness decision. Invalid
challenge content changes only the lease decision and cannot suppress common
health observation. Meta derives role from the installed committed projection,
not from the payload tag; an authority/no-role heartbeat atomically clears any
ordinary candidate left from the same node's prior replica role. An unready,
storage-healthy former Owner's lease-only heartbeat retains operator-recovery
availability without refreshing its original TTL; no-role or unhealthy reports
withdraw it. The authenticated
session completion path immediately withdraws the exact generation's
candidate. Generation replacement, boot replacement, committed freshness
changes, and Meta leadership changes independently invalidate it, so the
selector never waits for TTL after a known disconnect or role change.
For an active failover candidate, Meta records the exact group term,
transition revision, action, assignment, and boot from the session's installed
FDS under the same observation-store lock as that heartbeat. A missing or
different role is terminal only when this marker matches the current
unauthorized action; a heartbeat based on an older projection remains warmup
evidence and cannot erase a candidate selected atomically with an uncontrolled
term fence. After authorization, Data legitimately suppresses its ordinary
role while rotating history, so Meta waits for Prepared, typed ActionFailed,
the action watchdog, or disconnect instead of interpreting that gap as loss.

An independent optional failover observation accompanies that role payload.
`SourcePaused` reports the controlled source's exact history and stable
next-LSN vector while the same heartbeat still renews its lease. A candidate
may suppress its ordinary role while rotating history after authorization;
`CandidatePrepared` then reports the exact action and boot-local prepared-
context identity independently. `ActionFailed` reports a bounded class
and detail for that exact action. Candidate action observations are withdrawn
on candidate disconnect or an incompatible projection/incarnation change.
`SourcePaused` is source-lineage scoped and may survive an exact source
disconnect only through its independent grace. All failover observations are
volatile, disappear on Meta leadership change, and are regenerated from
current Data state rather than restored by a new leader.

After the initial projection is applied and validated, Meta also publishes a
compact leader-local runtime record for cluster status and leader-owned
workflows. The record follows the current session and FDS incarnation, retains
the replication-history id authenticated by `ClientHello`, timestamps health
at receive, and records a lease decision only after the corresponding Ack
write succeeds. Replacement,
session close, leadership loss, and shutdown remove the record. This is a
one-way observational feed: Data authority and lease evaluation never read
status state back.

A local control or task update quiesces the heartbeat producer before publishing the new
controller projection. A heartbeat already written under the old projection
is detached from lease authority; its exact ack may still be consumed for wire
progress, but its lease decision is ignored. Heartbeats resume only after the
new desired object is installed and `FullStateApplied` is written, so an old
Ack cannot causally confirm the replacement projection and an old Ready proof
cannot be evaluated against a new assignment, manifest, or population epoch.

Task admission replies with `DirectiveResponse.started`; the asynchronous
`DirectiveResult` is accepted independently of that response. There is no
generic evidence message, receipt-stage chain, or evidence cache. SourcePaused,
CandidatePrepared and ActionFailed remain concrete heartbeat observations,
with authenticated reporter boot and assignment retained for reconciliation.
Reporter-local history remains the compatibility diagnostic `history` value.
The candidate's source boot, assignment, and history are independent fields;
only equal source lineages and equal population anchors/flow dimensions form a
comparison domain.

A committed slot-map cut cannot reuse an existing grant. Meta rejects a slot
ownership change while any affected source or destination
group still has an active grant. The controller must fence all affected
groups, commit the complete replacement map, and then activate fresh
authorities. This committed-state precondition complements the per-session
Fence/FDS drain: a source that has not consumed the replacement can never keep
an old lease while the destination begins serving the same slot.

Finite leases are required only for a Meta-managed local primary. Admission
and the final mutation recheck both prove the current session, group
assignment, Group Term, projection, and unexpired
monotonic deadline. Session loss, expiry, committed authority change, local
storage loss, or an explicit fence invalidates memory authority immediately.
Startup-not-ready and runtime storage loss are distinct controller states:
startup may later publish ready, while runtime loss latches the current boot
failed, publishes storage-unready, joins older directive admission and every
control transition already across the loss cut, then performs a final source
revocation and target-population cancellation and drains retired requests.
Storage writers set a process-wide latch and close the request gate
immediately; worker zero drives this asynchronous controller barrier. An
uncertain cleanup result stops the server without a clean checkpoint. Neither
a later FDS nor a population proof can clear the latch; recovery requires
process restart.

Meta's leadership expiry and every granted lease are bounded by the Raft
election lower bound `D`, and a leader stops sessions synchronously on
demotion. Data constructs and rechecks lease deadlines with Linux
`CLOCK_BOOTTIME`, so suspend time consumes rather than preserves authority.
Expiry self-fences memory authority without waiting for a lost-session signal,
an Automatic Failover Detector decision, or a further Meta message.
The worker's relative timer rechecks that suspend-aware deadline in slices no
larger than 25 ms or one quarter of the granted duration, whichever is
smaller. Request admission therefore rejects at the first post-resume touch,
while the source's native admission check compares the same absolute deadline
under its session-publication mutex and cannot wait for the timer at all. The
timer closes the O(1) source-admission gate within one bounded slice without
discarding current FDS capabilities or already-published population sessions.
Tying the slice to the grant avoids imposing a fixed 25 ms lag on deliberately
short leases.
Before issuing the first otherwise-valid lease for each group, boot, authority
anchor, and leadership generation, Meta waits `2D` on the same suspend-aware
clock: one maximum prior lease plus a second `D` safety margin. This remains
safe under the deliberately loose assumption that the Meta host's elapsed-time
clock advances no more than twice as fast as the prior Data host's; scheduling
delay can only postpone a grant. The leader-local evidence resets on leadership
or process restart, so a later owner cannot overlap a predecessor even when no
Fence acknowledgement is available. Before the first socket send of each
Grant Ack, Meta records a delivery-ambiguous finite window from the exact
heartbeat receive cut. A higher-sequence heartbeat confirming the Grant makes
the Owner serviceable and collapses preceding ordered possibilities into that
exact installed lease window. Meta retains both an unconfirmed maximum and
the confirmed installed window across a same-authority local control or duration
replacement because Data deliberately keeps that lease. Confirmation of a
later shorter Grant replaces the older installed window; a session or
authority change clears both windows.
If confirmation never arrives, the last possible window expires to
`heartbeat_expired`.

The corresponding handoff-pending marker is also session-, authority-, and
heartbeat-sequence-bound and survives same-authority local control update. Health
denials cannot bypass the wait: exact unhealthy challenges mature the same
suspend-aware guard and remain handoff-pending until `2D`; only then may a
written `NodeNotReady` retire the marker. This avoids both a pre-send ABA
window and permanent handoff blocking when the Owner remains unhealthy.

NuRaft's peer-liveness timer uses active `CLOCK_MONOTONIC` time, which does not
advance while a Meta host is suspended. Data control therefore also compares
that clock with `CLOCK_BOOTTIME`. Once their accumulated divergence reaches
`D`, it closes the leadership generation's authority sessions and requests an
immediate NuRaft resignation. A sole member, for which resignation is a no-op,
must run for another full `D` of active monotonic time before authority can be
eligible; a further suspend extends that wait. All control boundaries, directives,
results, and lease grants pass this gate. Thus an old multi-member leader
cannot resume after a replacement election and refresh the same stale identity
through a handoff entry that had already matured before suspension.

Graceful Data shutdown stops new client and control-message admission, then
immediately cancels replication target and source transports. In particular,
closing source flows releases retained backlog cursors before request drain, so
a stopped downstream cannot make shutdown wait forever on an admitted write.
The control session reader observes its stop after the current socket operation
(bounded by the normal progress deadline), synchronously invalidates the
session's memory lease, closes the stream on its owning worker, and joins the
short directive admission lane plus the final fail-closed session-loss
transition. Terminal rebuild observation is separately session-scoped, so
abandoning a dead wire cannot block reconnection; shutdown still cancels and
joins the underlying native attempt before storage teardown.
The process drains admitted client requests before joining the control barrier,
so the retained final population frontier includes every accepted write. It
then joins the already-cancelled replication work and durably flushes storage
before publishing a clean shutdown proof and the optional index checkpoint.
This ordering prevents a completed control-side mutation
from landing after the checkpoint that is supposed to describe the clean
shutdown. If native-flow cancellation, source revocation, or candidate root
retirement has an uncertain outcome, the barrier returns that failure; the
process skips the normal checkpoint and exits unsuccessfully rather than
recording an unsafe state as a clean shutdown.

Population directives carry distinct 128-bit operation, directive, attempt,
target assignment, and source assignment identities. The common assignment
field denotes the target membership incarnation; the explicit source
assignment denotes the exporting membership incarnation. The proposer never
reuses either membership id for a later incarnation, while Meta durably retains
only the current or most recent value per node to catch direct replay. The
other IDs distinguish workflow, durable command, and execution attempt. The
wire recipient is separate from the rebuild target: rebuild runs at the target,
while authorize/revoke runs at the source without rewriting the common
source/target identity used by native replication.

The node controller checks the exact installed projection, full authority
anchor, recipient boot, exact target and source member assignments, local
source/target role, manifest revision/digest/content, partition replication
epoch, and one-group constraint before calling `ReplicationManager`. A
directive that passes the first check holds an admission token across readiness
publication and the possibly suspending manager registration, then rechecks
its generation and all anchors immediately before crossing that action seam.
local control update, fencing, session invalidation, lease expiry, and externally
observed population proof loss advance the generation before publishing their
invalidating boundary. Their async transition waits for every older token to
reject or finish registration. Fencing, population identity loss, and explicit
revocation then perform their required source/target cleanup. Session loss also
retires session-scoped target directives, but preserves the exact live
level-triggered `FollowOwner` attempt whose own history rotation requires the
Meta session to reconnect. Ordinary lease expiry instead closes only new source
admission and preserves current capabilities plus sessions already published
by the source. Thus a directive cannot appear behind the cleanup represented by
`FullStateApplied` or `FenceAck` even when its action adapter suspended after
validation. A
boot-local fence floor also rejects directives through the fenced Group Term:
rebuilds compare the local target assignment, while source authorize/revoke
compares the local source assignment. A fresh assignment or strictly newer
term is therefore distinguishable from replay of fenced authority.
The bounded worker timer may still be queued briefly after a host resume, so a
renewal also compares the old deadline with `CLOCK_BOOTTIME` synchronously. If
the old lease is already due, the renewal path runs that exact expiration
transition first: it advances the authority generation, retires the stale
timer, closes new source admission, joins older directive admissions, and
drains retired requests before considering the replacement grant. A
same-anchor heartbeat can extend only a lease that never expired, so pre-expiry
admissions cannot be revived by a delayed timer.

Population directives carry a kind-specific bounded `payload`; their mutation
classification follows kind and has no independently supplied flag. `initialize-empty-population` uses its
payload for exactly one canonical target replication-history id and has no
source node, source authorization, or replication connection; zero-valued wire
source fields normalize to an empty domain source. Rebuild and
source-authorization payloads freeze the source flow count reported by its
authenticated boot. Meta and NodeControl repeat the typed checks before the
action seam. Empty-population initialization and rebuild require a non-serving
target and retain the existing in-flight drain and mutation exclusion checks. Failover preparation does not use this directive or
receipt channel; its action is embedded in the local Group transition.

Initialization reuses the ordinary directive, FDS, operation receipt, and
population-proof lifecycles. NodeControl first closes readiness and serving,
then passes a source-less `RebuildIdentity` to `ReplicationManager` only after
the current session, boot, target assignment/history, Group Term, projection,
manifest, and partition epoch all match. The manager reuses the durable
full-sync fence, resets and hands off every physical partition, installs an
empty Function catalog, promotes the candidate root, and publishes a
ReadyToken with an empty flow cut. The token remains valid when the completed
directive disappears from the otherwise matching task state. A definite pre-promotion
failure stays LOADING and lets Meta abort/fence the operation; an uncertain
reset, abort, or promotion latches fail-stop until restart and never enables
lease admission.

An idempotent request receives one response after admission. Rejection before
start and failure after start are distinct final statuses. Rebuild admission
returns before readiness, allowing a newer attempt to supersede ongoing work;
a completion observer emits the terminal result without extra completion
receipts or generic evidence. The rebuild's own transition to
not-ready does not invalidate its admission token; concurrent external control
transitions still do. A local control or task update cancels prior completion observers and reconciles
the native target population before its applied acknowledgement; a fence also
cancels any in-progress target attempt and joins its completion observer before
`FenceAck`. A lease grant is admitted only after these cleanup counters clear
and does not disturb a still-current directive. The result reports a truthful
success, precondition rejection, or runtime failure. Meta durably commits the
exact
terminal receipt before returning `ResultCommitted`. With no Data-side journal,
a lost acknowledgement causes the committed directive to be replayed and its
idempotent replication identity to resolve the work again. After current
session, projection, identity and fence checks, NodeControl may return an
exact still-valid population completion through a non-mutating lookup even
while that population is serving. This path neither clears readiness nor
starts work; no match retains all ordinary destructive-admission and drain
checks. No local result record can reopen authority after restart.
The production node controller and replication control state share worker
zero: validation and exact-result lookup do not suspend or acquire a global
state mutex. Other workers request population observations through the
replication owner's asynchronous API, while data-flow progress remains
worker-local and independently sampled.

The local Group failover transition is a separate level-triggered control object. It
names the transition/revision, controlled or uncontrolled mode, target term,
and optional candidate action with exact candidate boot and one-way
authorization. Ordinary actions pin a compatibility domain; Operator Recovery
pins the selected member and population scope with unknown loss and no
historical domain or frontier. NodeControl derives source pause only for the exact
controlled owner and derives candidate work only for the exact named candidate.
The manager catches up through the existing native coordinator and returns a
boot-local prepared-context identity or a typed failure observation. A
changed action cancels and joins the old candidate before the replacement can
report progress. The one legal exception is a retained controlled degradation:
the exact same authorized action survives while its installed authority
context changes to the fenced target term, preserving any prepared child
history for uncontrolled cutover. Any changed candidate, source lineage, or
population anchor still replaces and cleans up the attempt.

After Meta atomically cuts over, local control clears the transition and carries its
action id on the new grant. NodeControl retains only that action's prepared
context and activates it provisionally on the first matching finite lease. It
rechecks FDS/session/deadline around role activation and expiration capability
installation before publishing the request lease. Exact replay is a no-op and
any mismatch remains fenced. There is no separate activation directive or RPC.

The same post-cutover local control supplies the complete owner, endpoint, membership,
manifest, and population epoch as a steady follow-owner relationship. The new
owner authorizes the exact replicas; every non-owner connects directly and the
native handshake chooses CONTINUE or FULL. A former owner fences its role,
revokes export, and retires its old backlog locally before it begins following.
This relation is resent after reconnect or Meta leadership change, so cleanup
and replica attachment do not depend on an ephemeral post-commit message.

A completed population is content-scoped by group, membership assignment,
immutable manifest, and partition replication epoch. `BeginGroupTerm` fences
authority but does not mutate that content, so FDS reconciliation preserves a
matching Ready population across a term-only change only when the epoch remains
unchanged, and reports its candidate proof under the newer committed term. An
epoch-only change first revokes Ready and serving eligibility and requires a
new rebuild before another lease can be granted. Explicit rebuild directives
remain term-scoped and are cancelled unless the FDS still carries their exact
term and rebuild successor. A steady FollowOwner FULL may instead finish across
a forward term-only fence when the physical population anchors and its exact
active follow/session/rebuild ownership remain unchanged; Owner, assignment,
manifest, or partition-epoch replacement still retires it. Grantless groups have
no `ServingState` owner or bound slots, but their committed membership remains
in the controller identity view so this preservation is possible. For any
member incarnation retained across projections, NodeControl also requires the
group's Group Term, manifest revision/digest, and partition
replication epoch to be monotonic even while the group is
ownerless; a full remove-and-reassign identity is the explicit boundary at
which a new incarnation may reset those counters. Once Meta
activates the candidate, the next local control update can publish the retained proof; if no
proof survived (for example after a reboot), the granted-but-unready target may
run a rebuild while it remains unable to serve or obtain a lease.

## Redis wire contract

Cluster routing errors are simple error lines with Redis 7.2 texts; clients
dispatch on the first token, and the encodings are identical under RESP2 and
RESP3. Deliberately unsupported administrative mutations use stable Lavik
`ERR` replies.

| Condition | Reply |
|---|---|
| Slot owned by another node | `-MOVED <slot> <host>:<port>` |
| First key's slot unbound, or self is its fenced primary | `-CLUSTERDOWN Hash slot not served` |
| Keys span multiple slots (including an `EXEC` union) | `-CROSSSLOT Keys in request don't hash to the same slot` |
| No ready committed state | `-LOADING Redis is loading the dataset in memory` |
| Transient retryable condition (e.g. flush in progress) | `-TRYAGAIN <message>` |
| `SELECT 0` | `+OK` (a no-op; every other database index is rejected) |
| `SELECT` with a nonzero index | `-ERR SELECT is not allowed in cluster mode` |
| `COPY` with a `DB` option | `-ERR Copying to another database is not allowed in cluster mode` |
| `REPLICAOF` / `ADDREPLICAOF` | `-ERR REPLICAOF not allowed in cluster mode.` |
| `FLUSHDB`, `FLUSHALL`, or catalog-changing `FUNCTION` subcommands | `-ERR <command> is not allowed in cluster mode` |
| Unknown `CLUSTER` subcommand or wrong arity | `-ERR Unknown CLUSTER subcommand or wrong number of arguments for '<sub>'` |
| Execution outcome undeterminable | No reply; the connection is closed |

MOVED always names the owning node's concrete advertised address; the
empty-host "dial the startup node" convention exists only for the self entry
in discovery replies. Both MOVED and discovery select the port by the
requesting connection's TLS state — TLS connections receive the target's TLS
port, falling back to the plain port when the target offers no TLS — mirroring
Redis `getNodeClientPort`. Discovery replies (`CLUSTER SLOTS`/`NODES`) are
built per request from one committed snapshot and never cache encoded output.

A gate rejection inside `MULTI` marks the transaction dirty, so `EXEC` fails
with EXECABORT as in Redis. `EVAL`/`EVALSHA`/`FCALL` are treated as writes for
admission — a script that only reads still redirects to the primary — while
their `_RO` variants are treated as reads; declared keys must hash to one
slot, and `redis.call` access outside the admitted slot set is rejected with
Redis's non-local-key error. Read-only global commands and process-local
administration (INFO, CONFIG, DBSIZE, SCAN, SCRIPT cache management, and
similar) keep node-local semantics and are governed only by readiness.
Cluster nodes reject `FLUSHDB`, `FLUSHALL`, and catalog-changing
`FUNCTION LOAD`, `DELETE`, `FLUSH`, and `RESTORE`: they mutate durable
process-wide state but carry no slot from which finite authority can derive a
group lease and drain cell. Queuing one in `MULTI` marks the transaction dirty,
so `EXEC` aborts rather than creating a slotless authority exception.
`FUNCTION KILL` and `FUNCTION STATS` remain available while loading so an
executing Function can be stopped or inspected; they do not mutate the catalog.

## CLUSTER subcommands and discovery surface

Cluster mode serves `SLOTS`, `NODES`, `MYID`, `INFO`, and `KEYSLOT` from the
committed state; every other subcommand receives Redis's unknown-subcommand
error. `KEYSLOT` is a pure function of the key and answers even before the
first state is published. `CLUSTER INFO` reports `cluster_state:ok` exactly
when slot coverage is complete, the assigned/ok slot counts, the known-node
count, and the number of slot-serving primaries as `cluster_size`; with no
gossip, the pfail/fail and message counters are
always zero. `CLUSTER NODES` emits nodes.conf-format lines with the `myself`
mark on the local entry and no bus-port semantics (`@0`).

Redis epoch fields are derived from Group Terms, not independently stored
configuration counters. `CLUSTER NODES` reports each member's Group Term in
the `config-epoch` field, including replicas and fenced Groups with no Grant.
`CLUSTER INFO` reports the local member's Group Term as `cluster_my_epoch`
and the maximum Group Term in the complete projection, including empty
Groups, as `cluster_current_epoch`. These are
discovery compatibility fields: Group Terms do not order slot conflicts
between different Groups. Meta owns slot assignment and orders projections
with its separate cluster-wide `topology_epoch`; Lavik does not participate
in Redis gossip or Redis elections.

`HELLO` reports `mode:cluster`, `INFO` reports `redis_mode:cluster` in its
Server section, and a `# Cluster` section carries `cluster_enabled:1`, so
standard clients and Sentinel-style tooling detect the mode. Standalone mode
is unchanged: it keeps the legacy replication-derived `CLUSTER NODES`/`SLOTS`
shim that fakes full coverage, and its replica-redirect MOVED shim never runs
in cluster mode because the two topology sources are mutually exclusive.

## Meta control and configuration

Meta-controlled state enters only through the asynchronous client/session path
and `NodeControlInstaller`, which can wait for replication revocation and
request drains before acknowledging a transition. Production startup never
installs a local topology or positive authority. Read paths still hide expired
values after their absolute deadline; without a valid lease, recovery cannot
append the authoritative tombstone or reclaim the retained winner.

Startup-only directives configure the subsystem: `cluster-enabled` (default
`no`); repeatable `cluster-meta-seed`; required `cluster-node-id`; and
`cluster-announce-ip`, `cluster-announce-port`, and
`cluster-announce-tls-port`. Announce values default to the first non-wildcard
bind address and the corresponding listen ports; a wildcard bind leaves the
announce host empty so discovery self entries keep the startup-node convention.
Cluster mode requires a canonical 40-character lowercase node id and at least
one numeric Meta seed. It refuses coexistence with either replication upstream
directive (two topology sources never mix; runtime `REPLICAOF` is rejected
separately at the command layer), and requires at least one reachable announced
client port so MOVED and discovery can always name an endpoint — a TLS-only
deployment is valid.

Data-to-Meta mTLS is optional and all-or-none. When enabled, the Data client
reuses its existing replication TLS CA/certificate/key and presents the
committed `lavik://node/<node-id>` identity. A Meta listener reuses that
member's existing Raft TLS CA/certificate/key and presents its sole
`lavik://meta/<server-id>` URI SAN. Plaintext mode treats network isolation
as its trust boundary; neither side silently falls back when a TLS identity is
partially configured.

## Current scope limits

The data plane deliberately excludes: ASK/ASKING and the
importing/migrating compatibility flow, the gossip bus protocol, protocol
deltas, Data-side durable control journals, shard Pub/Sub, non-uniform TLS
ports, dynamic node-id allocation, group-scoped authority for persistent
no-key mutations, and the remaining `CLUSTER` management subcommands
(`SETSLOT`, `MEET`, `FAILOVER`, `ADDSLOTS`, and similar).

## Verification

The admission decision matrix, builder and parser validation, content-hash
publication semantics, resolved lease validation and derived-cadence
enforcement, causal heartbeat sequencing, finite lease self-fencing,
controlled mutation pause, failover action/activation, follow-owner
reconciliation, frame and
complete-object codecs, projection validation, drain behavior, TLS-aware
endpoint selection, in-flight counter concurrency and cell sharing, and the
CLUSTER wire texts and reply shapes are unit-tested through pure seams and the
test-only finite-lease topology installer.
A real-process plaintext Data-control gate starts three Meta members and a Data
node, exercising follower-seed redirect, full-state install, heartbeat
observation, leader failure and reconnect, stale-member restart, and graceful
shutdown. Separate single-Meta gates cover mTLS identity and TLS/plaintext mode
selection. The cluster-create process gate starts one bootstrap Meta and two
Groups of initially unregistered, fenced primary/replica Data nodes. It covers
automatic and explicit slot layouts, interactive and `--yes` confirmation,
real sparse-population initialization, native full rebuild and continued
replication, Redis routing, `redis-cli` redirect following, cross-slot and
cluster-command restrictions, READONLY replica access, discovery, and exact
node-level diagnostics when one replica is stopped. Focused TTL and rebuild
tests validate recovery without expiration authority and the durable
incomplete-full-sync fence without a local topology source.

## Source map

| Claim | Repository source |
|---|---|
| ServingState model, builder validation, topology cache, content hash, striped in-flight cells, and routing functions | `include/lavik/cluster/topology.h`, `src/cluster/topology.cpp` |
| Admission decision and owner-side authority re-check | `include/lavik/cluster/authority.h`, `src/cluster/authority.cpp` |
| Final logical-mutation precondition and WATCH/publication seam | `include/lavik/storage/engine.h`, `src/storage/engine/write.cpp`, `src/storage/engine/hash_tree.cpp` |
| Meta/Data protocol framing, resolved Authority Lease field and derived heartbeat cadence, independent failover observations, transition/activation projection, complete-object transfer, and bounded writer scheduling | `include/lavik/cluster/control_protocol.h`, `include/lavik/cluster/control_transport.h`, `src/cluster/control_protocol.cpp`, `src/cluster/control_transport.cpp` |
| Node controller, full-state validation, controlled pause, provisional activation, finite authority, drain, follow-owner reconciliation, and typed replication adaptation | `include/lavik/cluster/node_control.h`, `include/lavik/cluster/meta_control.h`, `src/cluster/node_control.cpp`, `src/cluster/meta_control.cpp`, `include/lavik/replication.h`, `src/replication/replication.cpp` |
| Meta discovery, outbound Data control session, stop-and-wait causal heartbeat cadence, and finite-lease expiry | `include/lavik/cluster/meta_client.h`, `src/cluster/meta_client.cpp` |
| Process-wide runtime installation | `include/lavik/cluster/runtime.h`, `src/cluster/runtime.cpp` |
| Cluster admission gate, controlled TRYAGAIN/PUBLISH handling, owner/final re-check plumbing, outcome finalization, EXEC/Lua/blocking integration, and mode-restricted command policies | `src/redis/command.cpp`, `src/redis/cluster_gate.h`, `src/redis/blocking_wait.cpp` |
| CLUSTER subcommands and discovery replies | `src/redis/cluster_command.cpp`, `src/redis/cluster_command.h` |
| Cluster wire error texts | `include/lavik/resp.h`, `src/redis/resp.cpp` |
| Per-shard transaction validator hook | `include/lavik/tx/transaction.h`, `src/tx/transaction.cpp` |
| Startup wiring, storage-ready publication, and Meta control client ownership | `src/redis/server.cpp` |
| Cluster configuration directives and validation | `include/lavik/server.h`, `src/config.cpp`, `app/lavik.cpp` |
| Decision matrix, parser, publication, finite-lease test installation, failover projection/activation, and concurrency unit tests | `tests/cluster_authority_test.cpp`, `tests/cluster/test_topology_installer.h`, `tests/cluster_topology_test.cpp`, `tests/cluster_command_test.cpp`, `tests/control_protocol_test.cpp`, `tests/meta_client_test.cpp`, `tests/meta_control_test.cpp`, `tests/node_control_test.cpp`, `tests/cluster/replication_manager_integration_test.cpp` |
| Real-process Meta/Data discovery, committed failover, mTLS, initial creation, shutdown proof consumption and operator recovery gates | `tests/meta_integration/gate_data_control.py`, `tests/meta_integration/gate_cluster_create.py`, `tests/meta_integration/gate_failover.py`, `tests/meta_integration/gate_population_recovery.py` |
