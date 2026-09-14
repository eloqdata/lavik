# Cluster data plane

## Responsibility and boundary

This subsystem makes a Keylane process a Redis Cluster data node: it decides,
for every client command, whether this node may serve it, must redirect it to
the owning node, or must refuse it with a standard cluster error. Ordinary
requests never consult an external control plane; each node answers from its
locally committed view of slot ownership, authority, and readiness.

The module under `include/keylane/cluster/` and `src/cluster/` exposes five
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
an FDS replacement, NodeControl carries an existing positive proof only across
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
total length and SHA-256; chunks stream without per-frame application
acknowledgements. A complete FDS gets `FullStateApplied`, directives get typed
receipt stages, and terminal results get `ResultCommitted` or
`NoLongerTracked`; soft `OperationEvidence` has no application ack.
The complete desired-state cap is 512 MiB, while each opaque directive,
result, or operation-evidence field is capped at 256 KiB. These are abuse
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

Control protocol v1 has no delta format. Initial connection, reconnection, and
every semantic projection change transfer a complete `FullDesiredState`. The
object contains global topology plus each group's partition replication epoch
and the receiving node's policies, immutable population manifests, current
directives, optional failover transition, optional grant activation action,
complete membership/owner relationship, and `steady_replication_enabled`.
Meta sets that bit only when the committed cluster lifecycle is `Created`;
Genesis keeps it false so explicit population directives exclusively own
ingress. Even when true, Data defers follow-owner reconciliation while an
active failover transition or a current local initialize/rebuild directive owns
ingress, then consumes the later complete FDS after that blocker disappears.
When a newer committed projection supersedes an incomplete object transfer,
Meta aborts only that matching FDS object and retries on the same authenticated
session. Data treats only the named FDS-supersession reason for its exact active
object as non-terminal; an unknown reason, another object kind, or a mismatched
object fails the session. If a direct FDS frame or streamed `TransferEnd` is
already visible, Meta instead consumes that object's exact `FullStateApplied`
and adopts the intermediate projection as the actual session baseline before
retrying the newest projection. Normal projection churn therefore does not
manufacture a candidate disconnect, while a real session loss remains terminal
for a selected action.
The source applied index is a diagnostic/order watermark; the projection
SHA-256 is the semantic dependency for leases and directives. A higher index
with identical semantic content is therefore harmless. Lower indexes reject,
exact index/object replays are
idempotent, and one index naming different bytes invalidates memory authority
and closes the session. Installation validates and builds the entire immutable
state before one publication; partial transfer never changes serving state. A
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
only the lease request. An inactive topology owner normally sends no role
information. The narrow exception is the historical owner retained by an
active uncontrolled target-term fence: with no grant and a coherent live Ready
frontier, it may report candidate progress using its authenticated current
boot/history and its own assignment at the preceding term as source lineage.
This lets the best surviving population re-enter selection without treating a
fenced owner as active authority. Other non-owner members with coherent live
Ready frontiers also send only candidate progress. Candidate
progress includes the exact local membership assignment, manifest revision and
digest, partition replication epoch, completed rebuild source lineage, and a
typed bounded next-LSN vector sampled after successful apply. Candidate and
health are volatile Meta observations, not Raft commands. The challenge names the
exact projection and complete group authority anchor. `sent_at` is captured
immediately before the first socket write, and the granted duration is applied
to that monotonic timestamp, so a delayed response cannot extend authority.
Business heartbeat sequences accept only the next message or an exact replay
of the preceding message; the latter receives the cached exact ack. Invalid
challenge content changes only the lease decision and cannot suppress common
health observation. Meta derives role from the installed committed projection,
not from the payload tag; an authority/no-role heartbeat atomically clears any
candidate left from the same node's prior replica role. The authenticated
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
context identity/hash independently. `ActionFailed` reports a bounded class
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
write succeeds. Cached Ack replay does not renew that timestamp. Replacement,
session close, leadership loss, and shutdown remove the record. This is a
one-way observational feed: Data authority and lease evaluation never read
status state back.

An FDS replacement quiesces the heartbeat producer before publishing the new
controller projection. A heartbeat already written under the old projection
is detached from lease authority; its exact ack may still be consumed for wire
progress, but its lease decision is ignored. Heartbeats resume only after the
new desired object is installed and `FullStateApplied` is written, so an old
Ready proof cannot be evaluated against a new assignment, manifest, or
population epoch.

Directive execution also emits typed, volatile `OperationEvidence` for the
started and completed phases. Each report is bound to the current session and
boot, authenticated reporter, exact local assignment, operation, population
anchor, and replication history, and carries a SHA-256 of its bounded evidence
body. Reports that fit one frame use the soft-message lane; larger reports use
the same Start/Chunk/End object-transfer machinery under an evidence-specific
cap and are admitted only after whole-object hash and schema validation. Meta
audits and discards a well-formed report that has become stale without closing
the authority session; malformed session, boot, framing, or content-hash data
fails the session.
Meta's typed candidate/evidence query results retain that authenticated boot
beside the reporter and assignment, so later directive/evidence construction
never has to race a second session lookup.
Reporter-local history remains the compatibility diagnostic `history` value.
The candidate's source boot, assignment, and history are independent fields;
only equal source lineages and equal population anchors/flow dimensions form a
comparison domain.

A committed slot-map cut cannot reuse an existing grant. Meta rejects a slot
ownership or config-epoch change while any affected source or destination
group still has an active grant. The controller must fence all affected
groups, commit the complete replacement map, and then activate fresh
authorities. This committed-state precondition complements the per-session
Fence/FDS drain: a source that has not consumed the replacement can never keep
an old lease while the destination begins serving the same slot.

Finite leases are required only for a Meta-managed local primary. Admission
and the final mutation recheck both prove the current session, group
assignment, term, authority/grant revision, projection, and unexpired
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
The worker's relative timer rechecks that suspend-aware deadline in slices no
larger than 25 ms or one quarter of the granted duration, whichever is
smaller. Request admission therefore rejects at the first post-resume touch,
while source-capability invalidation begins within one bounded slice rather
than waiting out the pre-suspend remainder of a long lease. Tying the slice to
the grant avoids imposing a fixed 25 ms lag on deliberately short leases.
Before issuing the first otherwise-valid lease for each group, boot, authority
anchor, and leadership generation, Meta waits `2D` on the same suspend-aware
clock: one maximum prior lease plus a second `D` safety margin. This remains
safe under the deliberately loose assumption that the Meta host's elapsed-time
clock advances no more than twice as fast as the prior Data host's; scheduling
delay can only postpone a grant. The leader-local evidence resets on leadership
or process restart, so a later owner cannot overlap a predecessor even when no
Fence acknowledgement is available.

NuRaft's peer-liveness timer uses active `CLOCK_MONOTONIC` time, which does not
advance while a Meta host is suspended. Data control therefore also compares
that clock with `CLOCK_BOOTTIME`. Once their accumulated divergence reaches
`D`, it closes the leadership generation's authority sessions and requests an
immediate NuRaft resignation. A sole member, for which resignation is a no-op,
must run for another full `D` of active monotonic time before authority can be
eligible; a further suspend extends that wait. All FDS boundaries, directives,
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
Only after the control barrier may the process finish draining client requests.
It then joins the already-cancelled replication work and creates its storage
shutdown checkpoint. This ordering prevents a completed control-side mutation
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
FDS replacement, fencing, session invalidation, lease expiry, and externally
observed population proof loss advance the generation before publishing their
invalidating boundary. Their async transition waits for every older token to
reject or finish registration and only then performs its final revoke/cancel
pass. Thus a directive cannot appear behind the cleanup represented by
`FullStateApplied` or `FenceAck` even when its action adapter suspended after
validation. A boot-local fence floor also rejects directives through the
fenced counter tuple: rebuilds compare the local target assignment, while
source authorize/revoke compares the local source assignment. A fresh
assignment or a strictly newer committed counter tuple is therefore
distinguishable from replay of fenced authority.
The bounded worker timer may still be queued briefly after a host resume, so a
renewal also compares the old deadline with `CLOCK_BOOTTIME` synchronously. If
the old lease is already due, the renewal path runs that exact expiration
transition first: it advances the authority generation, retires the stale
timer, and joins source/directive cleanup before considering the replacement
grant. A same-anchor heartbeat can extend only a lease that never expired, so
pre-expiry admissions cannot be revived by a delayed timer.

The encoded population-directive schema retains bounded `payload`,
`preconditions`, and `force` fields. `initialize-empty-population` uses its
payload for exactly one canonical target replication-history id and has no
source node, source authorization, or replication connection; zero-valued wire
source fields normalize to an empty domain source. Rebuild and
source-authorization payloads freeze the source flow count reported by its
authenticated boot. Meta and NodeControl repeat the typed checks before the
action seam, so the native adapter never treats an unknown predicate or forced
operation as satisfied. Failover preparation does not use this directive or
receipt channel; its action is embedded in the Group transition in FDS.

Initialization reuses the ordinary directive, FDS, operation receipt, and
population-proof lifecycles. NodeControl first closes readiness and serving,
then passes a source-less `RebuildIdentity` to `ReplicationManager` only after
the current session, boot, target assignment/history, authority, grant,
manifest, and partition epoch all match. The manager reuses the durable
full-sync fence, resets and hands off every physical partition, installs an
empty Function catalog, promotes the candidate root, and publishes a
ReadyToken with an empty flow cut. The token remains valid when the completed
directive disappears from otherwise matching FDS. A definite pre-promotion
failure stays LOADING and lets Meta abort/fence the operation; an uncertain
reset, abort, or promotion latches fail-stop until restart and never enables
lease admission.

Receipt stages distinguish acceptance, execution start, and completion. A
controller rejection moves directly from
`Accepted` to `Completed` and reports a rejected result without started
evidence; admitted work emits `Started`, and any later non-success is a failed
execution. For admitted work, completed evidence is written after `Completed`
but before the terminal result, while the operation is still live at Meta.
Rebuild admission returns before readiness so a newer current rebuild can enter
ReplicationManager and supersede the old attempt; an exact completion observer
alone emits `Completed` and the result. The rebuild's own transition to
not-ready does not invalidate its admission token; concurrent external control
transitions still do. FDS then cancels prior completion observers and reconciles
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

The FDS failover transition is a separate level-triggered control object. It
names the transition/revision, controlled or uncontrolled mode, target term,
and optional candidate action with exact candidate boot, compatibility domain,
and one-way authorization. NodeControl derives source pause only for the exact
controlled owner and derives candidate work only for the exact named candidate.
The manager catches up through the existing native coordinator and returns a
boot-local prepared-context identity/hash or a typed failure observation. A
changed action cancels and joins the old candidate before the replacement can
report progress. The one legal exception is a retained controlled degradation:
the exact same authorized action survives while its installed authority
context changes to the fenced target term, preserving any prepared child
history for uncontrolled cutover. Any changed candidate, source lineage, or
population anchor still replaces and cleans up the attempt.

After Meta atomically cuts over, FDS clears the transition and carries its
action id on the new grant. NodeControl retains only that action's prepared
context and activates it provisionally on the first matching finite lease. It
rechecks FDS/session/deadline around role activation and expiration capability
installation before publishing the request lease. Exact replay is a no-op and
any mismatch remains fenced. There is no separate activation directive or RPC.

The same post-cutover FDS projects the complete owner, endpoint, membership,
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
new rebuild before another lease can be granted. In-progress rebuilds remain
term-scoped and are cancelled unless the FDS
still carries their exact term and a rebuild successor. Grantless groups have
no `ServingState` owner or bound slots, but their committed membership remains
in the controller identity view so this preservation is possible. For any
member incarnation retained across projections, NodeControl also requires the
group's config epoch, term, authority version, grant revision, manifest
revision/digest, and partition replication epoch to be monotonic even while
the group is ownerless; a full remove-and-reassign identity is the explicit
boundary at which a new incarnation may reset those counters. Once Meta
activates the candidate, the next FDS can publish the retained proof; if no
proof survived (for example after a reboot), the granted-but-unready target may
run a rebuild while it remains unable to serve or obtain a lease.

## Redis wire contract

Cluster routing errors are simple error lines with Redis 7.2 texts; clients
dispatch on the first token, and the encodings are identical under RESP2 and
RESP3. Deliberately unsupported administrative mutations use stable Keylane
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
count, the number of slot-serving primaries as `cluster_size`, and the
configuration epochs; with no gossip, the pfail/fail and message counters are
always zero. `CLUSTER NODES` emits nodes.conf-format lines with the `myself`
mark on the local entry and no bus-port semantics (`@0`).

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
committed `keylane://node/<node-id>` identity. A Meta listener reuses that
member's existing Raft TLS CA/certificate/key and presents its sole
`keylane://meta/<server-id>` URI SAN. Plaintext mode treats network isolation
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
publication semantics, finite lease lifecycle, controlled mutation pause,
failover action/activation, follow-owner reconciliation, frame and
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
| ServingState model, builder validation, topology cache, content hash, striped in-flight cells, and routing functions | `include/keylane/cluster/topology.h`, `src/cluster/topology.cpp` |
| Admission decision and owner-side authority re-check | `include/keylane/cluster/authority.h`, `src/cluster/authority.cpp` |
| Final logical-mutation precondition and WATCH/publication seam | `include/keylane/storage/engine.h`, `src/storage/engine/write.cpp`, `src/storage/engine/hash_tree.cpp` |
| Meta/Data protocol framing, independent failover observations, transition/activation projection, complete-object transfer, and bounded writer scheduling | `include/keylane/cluster/control_protocol.h`, `include/keylane/cluster/control_transport.h`, `src/cluster/control_protocol.cpp`, `src/cluster/control_transport.cpp` |
| Node controller, full-state validation, controlled pause, provisional activation, finite authority, drain, follow-owner reconciliation, and typed replication adaptation | `include/keylane/cluster/node_control.h`, `include/keylane/cluster/meta_control.h`, `src/cluster/node_control.cpp`, `src/cluster/meta_control.cpp`, `include/keylane/replication.h`, `src/replication/replication.cpp` |
| Meta discovery and outbound Data control session | `include/keylane/cluster/meta_client.h`, `src/cluster/meta_client.cpp` |
| Process-wide runtime installation | `include/keylane/cluster/runtime.h`, `src/cluster/runtime.cpp` |
| Cluster admission gate, controlled TRYAGAIN/PUBLISH handling, owner/final re-check plumbing, outcome finalization, EXEC/Lua/blocking integration, and mode-restricted command policies | `src/redis/command.cpp`, `src/redis/cluster_gate.h`, `src/redis/blocking_wait.cpp` |
| CLUSTER subcommands and discovery replies | `src/redis/cluster_command.cpp`, `src/redis/cluster_command.h` |
| Cluster wire error texts | `include/keylane/resp.h`, `src/redis/resp.cpp` |
| Per-shard transaction validator hook | `include/keylane/tx/transaction.h`, `src/tx/transaction.cpp` |
| Startup wiring, storage-ready publication, and Meta control client ownership | `src/redis/server.cpp` |
| Cluster configuration directives and validation | `include/keylane/server.h`, `src/config.cpp`, `app/keylane.cpp` |
| Decision matrix, parser, publication, finite-lease test installation, failover projection/activation, and concurrency unit tests | `tests/cluster_authority_test.cpp`, `tests/cluster/test_topology_installer.h`, `tests/cluster_topology_test.cpp`, `tests/cluster_command_test.cpp`, `tests/control_protocol_test.cpp`, `tests/meta_client_test.cpp`, `tests/meta_control_test.cpp`, `tests/node_control_test.cpp`, `tests/cluster/replication_manager_integration_test.cpp` |
| Real-process Meta/Data discovery, committed failover, mTLS, initial creation, and shutdown gates | `tests/meta_integration/gate_data_control.py`, `tests/meta_integration/gate_cluster_create.py`, `tests/meta_integration/gate_failover.py` |
