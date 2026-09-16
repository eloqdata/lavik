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

# Replication

## Responsibility and boundary

The replication subsystem owns node role, upstream and downstream session
lifetime, native Lavik synchronization, Redis PSYNC interoperability, and
trusted replay. `ReplicationManager` owns exactly one deep `ReplicationGroup`;
its administrative boundary is directives, peer sockets, and coherent
observation. Multiple Redis Cluster source connections are participants
in that one group, not independent replication groups. The Redis command layer
captures committed logical effects; and the storage engine owns the source
backlog, full-sync capture state, target rebuild state, and durable state.
In cluster mode that deep group also owns one boot-scoped population identity
and readiness proof. Its callable cluster adapter binds the proof to the native
reset, transfer, cut, promotion, activation, follow-owner, and abort paths. The
Data-side node controller is the only caller for Meta-delivered rebuild and
failover desired state, source authorization, revocation, and finite expiration
authority; transport code cannot bypass the replication adapter.

Replication moves deterministic logical commands, snapshot records, and the
process-global Redis Function catalog, not physical block addresses or record
offsets. The logical stream distinguishes durable keyspace mutations,
Function-catalog mutations, cross-flow transactions and control barriers from
runtime-only effects such as `PUBLISH`. Bycorf owns sockets, TLS, worker scheduling, and cross-core
submissions below this boundary. Normal storage and transaction paths remain
authoritative for mutation ordering and durability. Replication-origin
commands re-enter those paths with client role checks bypassed and publication
disabled.

The source backlog is deliberately not durable. It is process-local memory
used for connected downstreams and bounded reconnects. Target records,
Function catalog, full-sync invalidation, population eligibility, and
promotion base are durable. A completed graceful shutdown also certifies its
final source-domain frontier. Startup consumes that certificate durably before
mutation and may reconstruct a fenced Candidate for any former Group role.
Without that proof, automatic eligibility requires whole-group full sync.
Every boot creates a new boot ID, history ID, and replica incarnation; neither
native continuation nor Redis replid/offset state resumes across restart.

## Population recovery

Completed Bootstrap and replica FULL populations commit their Group/member
assignment, manifest revision/digest, partition epoch and local population scope
through Storage's system-state writer. These records establish where readable
local data belongs; they do not certify replication progress.

Graceful shutdown drains accepted commands and target apply before capturing
source history and the complete per-flow frontier. Storage publishes the Clean
Shutdown Proof only after all workers have flushed ordinary and transactional
records and all transaction decisions have drained. The certificate is
independent of the optional index checkpoint. Startup validates the selected
catalog and population, durably consumes the certificate, then rebinds the
recovered population to the new reporter boot while preserving historical
source provenance. A later crash cannot reuse the consumed certificate.

Recovered candidates advertise no serving readiness until an authorized
promotion preparation creates a new durable base and history. An authority-only
term fence preserves their unchanged population scope. Bootstrap assignment,
election ordering, committed Cutover and finite lease activation remain Meta's
responsibility. Downstreams rebuild from the new history.

Readable recovered populations without a clean certificate report operator
recovery availability without a source cursor. A committed operator-recovery
action can establish a fresh base over that population with `loss=unknown`;
its new base cursor never enters automatic historical-domain ranking. Missing
scope, corruption and incomplete destructive FULL are not recoverable through
this action.

## Runtime ownership

Worker zero owns mutable role, upstream configuration, target sessions,
population proofs, source authorizations, and failure state. Control commands
and observations from other workers enter through Bycorf submissions and resume
on their caller's worker. No process-thread mutex serializes this state.
The node controller shares that owner, so validation and non-suspending exact
completion lookup form one uninterrupted decision. Per-worker immutable
upstream caches serve legacy MOVED replies without an owner hop; role and
serving-generation admission remain atomic and independent of that cache.

Data flows remain on their assigned workers and publish progress through their
existing session/frontier boundaries. A malformed or failed flow awaits
owner-side exact-session invalidation before completing teardown; a retired
flow cannot invalidate its replacement. Population heartbeat sampling runs on
the owner without suspending while it captures the proof, and validates the
concurrently published frontier independently. The downstream registry retains
its coroutine-aware cross-worker gate because source flow workers also access
it; it does not block an operating-system worker thread.

## Roles and lifecycle

### Standalone and Sentinel-managed mode

The externally reported roles are `master`, `connecting`, `syncing`, and
`online`. `connecting` and `syncing` are LOADING states: ordinary data commands
cannot observe a partial rebuild. A configured replica does not perform
authoritative active expiration; it applies the source's absolute deadlines
and replicated deletion effects instead.

Only an unfenced master can accept native LVPSYNC/LVFLOW or Redis PSYNC export.
An online follower rejects those source handshakes because cascading is not
implemented. A node detached during an incomplete full sync may report
`role:master`, but its durable LOADING fence also prevents it from exporting
the invalidated or partial population.

`REPLICAOF host port` first joins any automatic session teardown, closes the
reconfiguration gate, and changes the role epoch. The gate prevents a new
coordinator attempt throughout cancellation and promotion; the epoch rejects
continuations from the old attempt. It cancels the old upstream while database
admission is still open, and waits for
any apply that already crossed the replica FIFO boundary. When leaving the
master role, it also retires source sessions before trying to close command
database gates: an in-progress full sync may itself hold those gates while it
waits for a catalog acknowledgement. The old source log remains enabled until
admitted commands have drained, so their reserved events can still publish;
only then is that history disabled. The transition replaces the desired
upstream, disables local expiration authority, and reconnects asynchronously.
Ordinary `REPLICAOF` probes the native protocol first and falls back to Redis
discovery when the peer does not support it. Explicit `redis-replicaof`
configuration skips that probe.

`REPLICAOF NO ONE` and Sentinel promotion use the same role-transition path
and the same private prepare/activate kernel used by Meta-managed promotion.
The group first enters `syncing`, prevents upstream reconnect, and cancels the
transport while database admission remains open. Native commands already
popped for apply, complete registered transactions that entered apply, and a
Redis command already admitted from its serial stream finish and publish their
cursors or offset. Complete
read-ahead frames that never entered apply remain unacknowledged, are discarded,
and stay at or after the frozen frontier. The group then closes and drains
command database admission, takes the Function guard, quiesces expiration,
validates the durable population, captures the parent history and next-event
vector, crosses the storage durability barrier, and commits a `PromotionBase`
containing the population and catalog tokens. This gate-before-Function-guard
order matches FUNCTION/FCALL locking. Only then does it create a child history,
prepare a fresh source backlog, then immediately activate: restore expiration
authority and open writes. The two internal steps are consecutive inside the
one synchronous Redis command because Sentinel is itself the authority
coordinator; no Meta commit point separates them.
Source retirement first joins old downstream flow teardown/history resets and
the idle-history monitor through their final log-disable steps, so that teardown
cannot disable newly enabled child logs. A
catalog change while preparation holds its token aborts the transition.

A failed durability or source-backlog step keeps the node `syncing` and retains
the frozen parent proof for a complete `REPLICAOF NO ONE` retry. Configuring a
new upstream abandons that pending promotion and retires any partially prepared
source history before the replacement full sync starts. No failure path opens
the master write gate directly.

Native cascading is not supported, so a candidate has no downstream session to
reparent during promotion. Other replicas attach to the promoted node's child
history through whole-group full sync. This version does not emit
`HistorySwitch` or attempt partial reparent. Issuing `REPLICAOF NO ONE` while a
full sync is incomplete does not resurrect the invalidated population; the
node remains fenced rather than exposing the pre-sync state.

Redis Sentinel drives these same transitions through Redis-compatible `ROLE`,
`INFO replication`, `REPLICAOF`/`SLAVEOF`, `CONFIG REWRITE`, and `CLIENT KILL`
behavior. `replica-priority` defaults to 100, is runtime mutable, and is
reported as `slave_priority`; Sentinel treats zero as ineligible and otherwise
prefers the lower value. Sentinel-specific transaction handling and config
persistence are described under [Redis interoperability](#sentinel-managed-failover).

Process shutdown has a stronger one-way barrier. Before draining accepted
client writes, a thread-safe request bit prevents reconnect and closes outbound
target plus inbound native/Redis source sockets. Closing source flows releases
retained backlog cursors, so a slow downstream cannot leave an admitted
publisher waiting on its ACK while shutdown waits on that publisher. Worker
zero owns session cancellation and then joins native and Redis target apply,
aborts incomplete replacement roots, joins source exports and control
handshakes, and disables source history. Storage may start its final
freeze/checkpoint only after that barrier
returns successfully. Main accesses only transport cancellation sets, never
the worker-owned session registry or partially initialized session proofs.
Target sockets join both their session set and the process shutdown set before
connect/TLS; late registration after shutdown is rejected. Transport sets retain
descriptor-lifetime mutexes for concurrent registration, close, and shutdown;
normal command admission, progress publication, and heartbeat queries do not
acquire them.

### Meta-managed cluster lifecycle

`cluster-enabled yes` (or `--cluster-enabled`) together with configured Meta
seeds selects the fail-closed population mode for a process that may own at
most one replication group. The manager starts in `connecting`, ordinary reads
and writes return LOADING, and storage starts without expiration authority.
Cluster mode rejects startup `replicaof`, `redis-replicaof`, and `load-rdb`;
runtime `REPLICAOF`/`SLAVEOF` (including `NO ONE`) and `ADDREPLICAOF` are also
rejected, as are unauthenticated native and Redis replication exports. These
restrictions prevent standalone role control or imported data from being
mistaken for an authorized cluster population.

The configured stable data-node identity is also the
ReplicationManager's local node identity. Status, the boot-scoped
`ReplicationGroup`, and every native handshake therefore name the same node;
boot and history identities remain freshly generated process incarnations.
Standalone mode generates the local replication node identity at process
start instead.

`ReplicationManager` exposes a callable boundary to the Data-side node
controller: `cluster_population_status()` reports the local node/boot and the
boot-scoped state or ready/failure evidence plus the currently applied
parent-flow frontier, and `StartClusterRebuildDirective()` admits one authorized
source rebuild and returns its exact completion handle.
`StartEmptyPopulationInitialization()` is the source-less first-population
variant and reuses the same completion and proof ownership instead of exposing
a second runtime. NodeControl observes the handle later to distinguish wire
admission from `ReadyToken`, cancellation, or failure; exact replay shares the
same attempt.
FDS reconciliation retains an in-progress attempt only while a current rebuild
directive still names the same local assignment, term, manifest, and partition
replication epoch. Removing
that directive or changing the population identity closes serving, joins the
native session, aborts partial storage, and retires the proof before
`FullStateApplied`. Control-session loss performs the same barrier for an
in-progress attempt but retains an already completed matching `ReadyToken`, so
a transient reconnect does not itself force a full rebuild.
An ordinary steady FollowOwner FULL has no rebuild directive. Its exact live
follow relationship, replica session, and rebuild context instead own the
attempt; a forward term-only authority fence preserves that copy while group,
assignment, immutable manifest, and partition replication epoch are unchanged.
Replacing the Owner or any of those population anchors still cancels and joins
the old ingress before another source can start.
`AuthorizeClusterRebuildSource()` plus its revocation method control exact
downstream export capabilities. `MetaControlClientService` receives and
normalizes the wire directive, but only `NodeControlInstaller` may call this
boundary after matching it to the installed projection and authority. The
capability ledger and finite-lease admission gate are separate. Installing an
Owner lease opens new `POPULATION` handshakes only until that lease's absolute
`CLOCK_BOOTTIME` deadline and only after request authority itself is installed.
The native handler checks both the exact current capability and `now < deadline`
under the same source mutex that publishes the session, so a late timer cannot
admit work after expiry.

Lease expiry closes only new admission. It retains current FDS capabilities and
every population session already published across the mutex boundary, including
a session that has not reached ONLINE; a later exact lease renewal reopens the
gate in O(1). A live FDS replacement clears and replays capabilities while an
unchanged, unexpired gate may remain open. The authenticated FDS supplies the
number of local `authorize-source` capabilities its directive lane must replay.
That pending count and every installed capability reserve the current source
history; a source-valid handshake during the bounded replay gap is denied data
with `LVLEASESUSPENDED` and uses the target's finite retry path. Each newly
installed capability consumes one replay slot. An FDS with no such directive,
or a strong revoke, drops the reservation. When the replacement proves the
complete source/member/population scope unchanged, every already-published
matching population session survives, including a session between
`LVFULLRESYNC` admission and ONLINE. Control-session replacement instead closes
the gate, carries the outstanding replay count into the replacement session,
and may preserve only an exact already-ONLINE export while its identity is
revalidated. Fence, committed revocation, membership/population change,
readiness loss, or storage loss uses the stronger join-and-revoke barrier. This
lets ordinary lease and projection convergence avoid aborting destructive
population work without allowing a new source session to start
outside finite authority. It also lets a completed initialization operation
retire its one-shot directives without interrupting the continuous replication
session it established. Until a valid directive completes, a Meta-managed
process remains LOADING. `PING` and the management/diagnostic surfaces needed
to observe the process remain available, but recovered keyspace is not made
readable or writable merely because storage initialization succeeded.

Population readiness itself remains boot-local and is never projected by Meta.
A replacement FDS therefore treats its local `population_ready=false` value as
an omitted proof, not as a negative observation: within the same Data boot,
NodeControl carries an already verified positive proof only when the Group,
local and Owner assignments, Group term, manifest revision and digest, and
partition replication epoch are all unchanged. An explicit NOT_READY report or
any change to those anchors clears readiness and revokes the finite lease. A
Controlled Failover transition changes only the mutation gate, so its pause and
removal preserve reads and the existing lease while writes return `TRYAGAIN`.

Each projected Group carries `steady_replication_enabled`, which Meta sets only
after the committed cluster lifecycle is `Created`. It remains false during
Genesis, so the explicit initialize/rebuild directives own replication ingress
exclusively. Once enabled, NodeControl still defers steady follow-owner
reconciliation while an active failover transition or a current local
initialize/rebuild directive owns ingress. The next complete FDS after that
owner disappears installs the steady relationship; no one-shot handoff is
required.

Failover is level-triggered desired state rather than a rebuild directive. On a
controlled source, `ReconcileClusterSourcePause()` is called only after
NodeControl has published the mutation pause and drained requests admitted by
the preceding state. It holds a nestable expiration pause and captures a stable
native next-LSN vector while the old owner continues reads, lease renewal, and
already-established replication flows. Replacing the same pause transfers the
hold; removing it releases that one level.

On the selected candidate, `ReconcileClusterFailoverAction()` first waits for
the exact Ready population and compatibility domain. Authorization is a
one-way, action-scoped gate: after the candidate covers the required controlled
frontier, or immediately under the explicit uncontrolled loss policy, the
manager joins upstream apply and runs the shared durable promotion-preparation
kernel. The resulting boot-local context freezes the parent frontier and
population/catalog proof and creates the child history, but keeps the node
`syncing`, LOADING, unable to expire, and closed to export. A replaced or
removed action withdraws its observation and joins preparation; exact replay
is idempotent. Before storage durability or history mutation begins,
supersession or shutdown cancels the exact preparation after joining its
detached target flows and releases admission. Ordinary supersession preserves
the Ready population and frontier for a successor action; a self-origin
candidate also returns to its fenced primary role. Shutdown keeps that role
closed and retires the population. Once durability mutation begins,
supersession instead joins the known terminal outcome and retires any
non-cutover child history before acknowledging the FDS. Because history
rotation precedes publication of the prepared
observation, the existing authenticated Meta session may bridge only that exact
installed, authorized action: a prepared status must name the resulting child
history. Boot, action, child-history, or desired-state mismatch closes the
session normally. Cutover or cancellation removes the action and therefore
forces reauthentication under the current history; the bridge never rewrites
the session identity used by ordinary progress evidence.

Cutover FDS removes the transition and places the exact action id on the new
owner's committed grant. The manager retains only the matching prepared context
across that boundary. On the first valid finite lease, NodeControl rechecks the
FDS, session, and deadline, calls `ActivateClusterPreparedPromotion()` to open
the prepared role provisionally, installs expiration authority through the
same absolute lease deadline, rechecks again, and only then publishes client
authority. Any mismatch fails closed. No one-shot activation RPC can make an
uncommitted candidate serve.

An eligible steady FDS installs one `DesiredClusterUpstream` relationship. The
owner authorizes the exact non-owner membership incarnations for native export;
each non-owner connects directly to that owner. The ordinary native handshake
learns the source boot/history and chooses CONTINUE when the retained population
covers the live domain, otherwise FULL. A follower keeps its Ready population
until an authenticated owner is actually able to export, avoiding destructive
replacement merely because the endpoint is temporarily unavailable. Scope
replacement cancels and joins the prior relationship. When an old owner becomes
a follower, it first closes its role and expiration authority, revokes exports,
and locally retires its former source history/backlog before following the new
owner. This cleanup is a Data-owned consequence of desired topology, not
another Meta failover phase.

Post-cutover replacements are not staged. Every follower may independently
discover that CONTINUE is impossible and enter destructive FULL against the new
owner at the same time. Once FULL withdraws those followers' old Ready proofs
and before any replacement finishes, a second failure of the new owner can
leave Meta with no eligible Candidate. An uncontrolled transition then waits
for an eligible population instead of manufacturing recovery proof. There is
no Meta rebuild queue or Data-side admission controller that preserves one
follower while the others replace their populations.

## Single-group population coordination contract

A cluster data node has one physical dataset and accepts at most one
replication-group assignment during a process boot. `PopulationManifest`
content-addresses the desired `(partition, logical epoch)` set; it may be
empty, sparse, or complete. Meta and Data derive that identity from the same
domain-separated, versioned, big-endian canonical encoding; a manifest
accepted into committed Meta state therefore has exactly the digest Data
recomputes while installing the projected full state. The committed group
also carries a partition
replication epoch that can advance without changing the manifest. Both values
belong to the control-plane population identity and are distinct from the
target-local replication epochs persisted by storage.

One rebuild identity binds the group, distinct target and source membership
assignments, authority term, directive revision and authority identity, source
node/boot/history, target node/boot,
operation, durable directive, execution attempt, manifest revision and digest,
and partition replication epoch. `BeginRebuild` accepts only the
local target boot and a matching manifest, rejects assignment to another
group, and requires the directive's safe-source assertion before returning a
destructive-reset authorization. Safe source is therefore bound to the
accepted boot-scoped directive, not inferred from a reachable socket or a
source name. Accepted directives advance monotonically by `(term,
directive_revision)`; exact in-progress retries are idempotent, while stale or
conflicting revisions and reused attempt identities are rejected. An exact
retry is idempotent only while its proof remains valid and its source endpoint
is unchanged; after invalidation the control plane must issue a fresh attempt
identity. A newer directive is fully validated before admission closes, then
the old session is cancelled, joined, aborted, and proof-invalidated before
the replacement capability is installed. Every later proof operation must
revalidate the complete identity, so an authorization from an older attempt
cannot complete a replacement attempt.

NodeControl publishes the local population as not ready before entering any
destructive rebuild admission. Admission and terminal observation are
separate: the short serialized admission lane may start a newer rebuild while
the prior attempt's completion observer is pending, allowing
ReplicationManager to run the supersession barrier above. Source authorization
and revocation remain serialized against target admission. Completion
observers are projection- and session-scoped; cancelling an observer never
fabricates a terminal result or cancels the underlying attempt, which is
retired only by the explicit ReplicationManager barriers.

The transfer set and physical reset domain are deliberately different. A
destructive rebuild must reset all 16,384 physical partitions, including
partitions absent from a sparse desired manifest, so no record outside the
desired population can survive. The source scans baseline data only for
manifest members, and the target refuses to install out-of-manifest records or
full-sync tail commands; reset and handoff still cover the full physical domain.
Every reset returns a nonzero target-local storage epoch. Completion evidence
maps each partition's saved logical manifest epoch (zero for a non-member) to
that exact target-local epoch, so a handoff from a prior attempt cannot satisfy
the current proof. It also covers the partitionless Function catalog, one
immutable stable cut for every declared source flow, and storage finalization
before publishing a boot-scoped ready token. A sparse manifest never permits a
sparse physical reset.

`StartClusterRebuildDirective()` consumes this contract in production. It
calls `BeginRebuild`, retains the resulting destructive-reset capability, and
revalidates that capability immediately before each storage reset batch. The
native flows feed reset epochs and manifest handoffs into the same
`ReplicationGroup`. After the all-flow cut rendezvous, flow zero records the
complete cut vector and Function-catalog proof before storage promotion. Only
then does the manager publish its ready token and open the serving generation.
Exact replay of the current completed directive returns that existing result
without calling `BeginRebuild` again. A partial attempt is cancelled and joined
as one session, its candidate population is aborted, and a cluster retry after
proof loss requires a fresh directive identity. An uncertain promotion, cut
installation,
cancellation/join, proof invalidation, or abort enters a group-identity-bound
current-boot failure latch: the process remains LOADING and accepts no later
attempt until restart.

Meta control-session replacement cancels an in-progress population directive
because that session owned its terminal result channel. It preserves an exact
active steady `FollowOwner` FULL, which is level-triggered by the installed FDS
rather than a session-scoped directive. The FULL itself rotates the target's
local replication history and therefore forces the Meta client to reconnect;
cancelling the FDS relationship at that boundary would make a slow follower
rebuild cancel itself. A later FDS owner, assignment, manifest, or partition
epoch replacement still cancels and joins the attempt, and process shutdown
never preserves it.

First population follows the same destructive boundary without inventing a
source. The accepted identity binds the current target replication history and
requires zero flows and no safe-source assertion. The manager durably
invalidates serving, resets and hands off all 16,384 physical partitions using
logical epoch 1 from the complete manifest, commits an empty Function catalog,
and promotes through the normal root path. Only the complete reset, catalog,
handoff, promotion, and proof sequence can publish the boot-scoped ReadyToken;
its cut vector is empty. A matching desired population retains that token after
the initialization directive is removed. Definite failures before promotion
remain fenced, while any uncertain reset/abort/promotion or post-promotion
failure uses the existing fail-stop/restart barrier.

A source-side cluster export is separately fail-closed. The control adapter
may authorize a downstream identity only while the local population has a
valid ready token for the same group, manifest, and partition replication
epoch, the native dataset is valid,
the local role is primary with no upstream, and the directive names the current
source node, membership assignment, boot, history, and flow layout. The common
directive assignment remains target-scoped; a separate source assignment must
exactly match both the installed FDS member and the source's ready token.
`NodeControlInstaller` proves both memberships current before crossing this
boundary. Authorization is exact to the complete rebuild identity and can
be revoked by cancelling and joining the affected source sessions. One
revision may authorize multiple targets only when their common group term,
source membership/boot/history, manifest, partition replication epoch, and flow
layout agree; each target
keeps its own assignment and authority identity. A newer revision
first revokes and joins every older export. Revocation clears active grants but
removes its sessions from the registry, and discards their reconnect leases. A
committed revoke also retains the accepted term/revision as a rejection floor,
so that revision and every older directive cannot resurrect authority; a
revoke before any directive was accepted is an idempotent no-op. Replacing an
authenticated Meta session uses a distinct cleanup path: it joins the
predecessor's process-local exports without advancing that floor, and only
after old-session dispatch is stopped may the replacement session reinstall
the exact live directives from its authenticated full state. Authorization
and both cleanup modes share the source-session registry lock, and grants stay
closed while old flows are joined. The
safe-source assertion alone does not promote an `Online` follower or permit
unsupported native cascading. Cluster-primary activation and upstream
detachment are outside this adapter; without that role transition, source
authorization remains fail-closed.

## Native control and data flow

Native replication uses authenticated, isolated RESP commands on the ordinary
Redis listener. Worker 0 owns the `LVPSYNC` control connection. The source has
one `LVFLOW` data connection per source worker and adopts each flow socket onto
that worker. A target may have a different worker count; it assigns source flow
`n` to target worker `n % target_worker_count` without changing the source flow
identity. Native protocol v1 is the only supported native wire format; there is
no compatibility layout from an earlier deployment. The control hello carries
the group,
replica incarnation, replica boot, requested history context, and complete
Applied vector; the response supplies the source group, boot, history, session,
flow count, and a fresh 160-bit flow capability generated from the OS CSPRNG.
The target captures that vector once for the control handshake and every
`LVFLOW` request in the session. It reuses the vector only when source
group/history and flow layout all match; a missing proof or changed layout
starts every flow at one, never by copying a matching prefix from an older
layout. If retained coverage later forces collective FULL, the target installs
the all-ones vector before destructive reset.
Every `LVFLOW` presents that bearer capability when claiming its flow id; the
source compares it in constant time before binding the socket. A guessed
session number therefore cannot steal a flow from the corresponding control
session. `Applied[flow]` is the next incomplete logical event and starts at
one. A flow socket binds its flow and repeats that component. Mode
selection waits for every flow: identical group/history context, the exact
control-vector component, and complete retained event coverage on every flow
selects `CONTINUE`; any restart, mismatch, gap, or missing event selects full
sync for the whole group. Mixed continue/full sessions are not allowed.
Every native data frame carries a payload CRC32C that the receiver verifies
before parsing, applying, or acknowledging it. A cluster rebuild additionally
carries the complete `POPULATION` identity, including both target and source
assignment ids and the partition replication epoch, in `LVPSYNC`; the source
returns its current boot identity and
accepts the session only when that identity exactly matches its current ready
population, an installed source authorization, and the connecting target node.
A Meta-managed source rejects an anonymous or standalone native export and
accepts only an exactly authorized `POPULATION` handshake inside its current
finite-lease deadline. An exact authorization presented while that admission
gate is closed receives the dedicated `LVLEASESUSPENDED` response before any
target mutation. The target retries only that response, only while the same
immutable rebuild context has no flow and has not crossed its destructive-root
boundary, at one-second intervals for at most three retries; all other protocol
or transport failures remain terminal for the attempt.

The steady-state source path is:

```text
client write
  -> publisher admission before database/key gates
  -> normal storage or transaction commit
  -> canonical command, transaction envelope, control barrier, or ephemeral event
  -> worker-local publisher FIFO
  -> shared in-memory replication frames
  -> duplex LVFLOW sender and ACK receiver
  -> target reassembly queue (at most 256 complete commands)
  -> ordered staging and cross-flow transaction scheduler
  -> trusted apply
  -> target resume-cursor publication
  -> per-flow ordered ACK
```

Canonical commands use the LRC1 command-body encoding: explicit logical database,
argument count and lengths, then argument bytes. Large arguments stream into
the backlog without another complete flattened allocation. The native v1
transport wraps each fragment in a versioned little-endian header containing
magic, header and payload lengths, kind, and payload CRC32C. One logical event
has one flow-local LSN and may span transport fragments and 8 MiB memory blocks,
but target continuation state contains only next-LSNs: fragment position is
source/backlog/wire state and is never published as applied progress. Applied
advances and ACKs only after the complete event succeeds. Mutation,
catalog-mutation, transaction, database-control, and ephemeral events are
distinct kinds. `kEphemeral` is reserved for runtime-only effects such as
standalone `PUBLISH`; Function mutations carry the original Redis command in
the catalog-mutation kind.

Publisher queues, full-sync subscriber queues, backlog blocks, full-sync
coverage, and multi-frame target staging are retained replication state. They
are admitted against the owning worker before they can accumulate. Each active
worker log and full-sync session owns a bounded publisher budget; sessions and
transaction participants share immutable command envelopes rather than
duplicating payloads. Admission reserves every destination before mutation, so
a successful primary write is not silently omitted from a valid history.

One command larger than its publisher-staging waterline may proceed only as the
sole queued item so the queue cannot deadlock on an item that no consumer can
make smaller. The complete canonical event is still bounded by the 1 GiB
native-event limit and the receiving flow's block share of
`repl-backlog-size`; transport fragmentation never weakens that event-level
admission rule. The command layer checks a conservative complete-event bound,
including possible expiration after-images, before mutation. A foreground publication-admission
failure before mutation is returned as Redis OOM. A
recoverable encoding or backlog failure discovered after a mutation invalidates
the affected history or full-sync attempt and requires a new full sync. Once
explicit retained-memory admission succeeds, unexpected physical allocation
failure is process-fatal rather than converted to a second admission result.

The source sends bounded batches while a separate receiver validates ACK order
and advances the retained cursor. Sending can continue across batch boundaries
so every participant of a cross-flow transaction can reach its rendezvous;
socket backpressure bounds outstanding output. A full-sync flow that stalls, or
an online flow with unacknowledged work whose ACK cursor stops advancing, is
eventually cancelled. A progressing full sync has no overall wall-clock limit.

## Runtime backlog and backpressure

Each source worker has one heap-backed backlog shared by all downstream native
sessions and the Redis exporter. Downstreams own only their cursors and
retention pins. `repl-backlog-size` is a global quota divided across workers in
8 MiB blocks, and block ownership is covered by worker-local retained-memory
admission. Per-worker LSNs start at one for a new history and increase
monotonically. The log remains active across a downstream disconnect while that
replica can still resume from the circular reconnect window. Each online native
session records the next LSN after its highest completely written socket batch
on every flow; this is a conservative upper bound even when the final ACK is
lost. Once one flow's floor advances beyond that upper bound, the all-flow
native session can no longer continue. Meta-managed Owners keep their source
sequence active even without consumers so a clean shutdown can certify the
final all-flow frontier; unpinned log entries remain bounded and evictable.
For standalone sources, after every disconnected native replica
reaches that state and no native session, Redis exporter, installed population
capability, or FDS replay reservation is active, the manager closes and drains
command admission, rechecks that the source is still idle, rotates the history
ID, and disables all worker logs. Draining preserves events and fences already
reserved by admitted commands. A later downstream must full-sync and re-enables
a fresh history before its snapshot cut.

A connected native downstream advertises its first unacknowledged LSN as a
coverage claim. By default, the publisher waits at the hard backlog limit until
ACK progress makes a complete event reclaimable; publisher staging then fills
and propagates bounded backpressure to foreground write admission. Runtime
`CONFIG SET replication-backlog-backpressure no` wakes any blocked publisher
and changes capacity conflicts to revoke lagging claims, evict complete old
events, and let affected consumers discover a floor gap and reconnect with
whole-group full sync. Re-enabling it affects the next capacity conflict. A
full-sync session pinned after its cut follows the same selected policy.
Disconnect leaves the remaining capacity as a circular reconnect window.
Shrinking below claimed history establishes a target quota; later publication
waits or revokes according to the current policy. Eviction, trim, and reconnect
coverage operate on complete events and never retain or advertise only a
suffix of one fragmented event.

Client `WAIT` uses the same ACK cursors without changing the native wire
protocol. Because worker LSN domains are independent, the source captures a
publisher fence on every worker after the connection's preceding writes and
retains that vector with the source history ID. An online replica counts only
after every flow reaches its corresponding fence. A history change invalidates
the cached vector and causes the connection to establish a new cut; offsets
from different histories are never compared. The initial connection offset
precedes all events, so every online native replica satisfies it without a
fence. Redis PSYNC export acknowledgements are deliberately excluded from this
native-replica count.

The source-side native session registry and history/continuation leases are
shared by control and flow coroutines on different workers. They are protected
by Bycorf's FIFO `CrossWorkerMutex`: contention suspends only the calling
coroutine and resumes it on its original worker, so an unrelated connection on
that worker is never parked behind a process-thread mutex. The mutex's atomic
guard covers only waiter-list handoff; no replication registry work or
`co_await` runs while that guard is held.

Writes reserve the worker publisher queue and all relevant active full-sync
queues before entering database or key gates. Multi-participant requests
reserve every destination while the cross-flow ordering boundary keeps worker
FIFOs consistent. Queue exhaustion therefore backpressures the client before
commit instead of silently dropping publication. A post-mutation encoding or
backlog failure leaves the primary dataset usable but invalidates the history:
the manager assigns a new history ID, cancels downstream sessions, clears all
worker logs, and requires full sync. An already-admitted head item may drain
above the current waterline to preserve forward progress; new admissions remain
blocked until occupancy falls below it.

## Full-sync lifecycle

Full sync destructively rebuilds one target population in place. It neither
allocates a parallel staging root nor retains the previous active root for
rollback. LOADING admission, rather than a root swap, prevents clients from
observing the indexes while they are reset and repopulated. The source
registers a bounded full-sync session queue on every worker and captures a
database epoch vector plus each partition's baseline mutation sequence. A flow
scans the source partitions assigned to it, one logical database at a time.
Each `(partition, database)` moves through `unstarted`, `scanning`, and
`tailing`:

- an unstarted database needs no capture because its later scan sees committed
  state;
- an uncovered key in a scanning database coalesces to its latest after-image
  or tombstone;
- a covered key, or any key in a tailing database, enters the ordered session
  command FIFO.

Baseline scans tolerate index growth and shrinking: continuously present keys
remain enumerable, and per-key capture state suppresses repeated bucket visits
while preserving concurrent after-images. Baseline values are captured under
the key-ordering boundary. Large external values pin immutable extents and
stream bounded value chunks between `begin`
and `commit` frames. Grouped sources pin an immutable root and its complete
group graph, measure the compact wire length page by page, and traverse the
same graph again to emit chunks. Source state retains one admitted decoded
page; completion and cancellation release the graph only after active reads
have finished. These source-local handles do not change the wire format.
Non-collection targets reserve key plus encoded-value
staging capacity from their worker-local memory share. Hash, Set, List and
Sorted Set targets instead decode the same compact wire image incrementally,
admitting one page at a time and writing complete grouped snapshots under one
outer transaction. The key-ordering guard spans the entire framed value;
partial page roots remain hidden behind LOADING and cannot become a promotable
population. The final frame validates exact bytes and cardinality before the
transaction's durability boundary. Malformed input, cancellation or OOM
rolls back uncommitted pages and invalidates the rebuild. Cleanup errors after
durable commit never roll back that committed decision.
Ordinary values are materialized into bounded record batches. Transactions
committed during the LOADING rebuild publish their participant after-images
only after the commit decision. `FLUSHDB` or `FLUSHALL` invalidates an active
capture attempt so the next attempt starts from the new database epochs.

Grouped target views use partition/database-local population generations.
Reset batches invalidate only the indexes they detach; promotion changes
visibility without changing the candidate's index identity. The worker-wide
generation used by ordinary suspended IO remains separate. See
[Grouped collections](09-grouped-collections.md) for graph ownership,
incremental mutation and snapshot limits.

Before a source session becomes visible, each worker reserves coverage-map
headroom for its largest `(partition, database)` scan, because only one such
map is live at a time. The estimate comes from per-database summaries maintained
with the indexes, so session startup does not scan the dataset. Coverage stores
key identity rather than value bytes; external baseline keys use their digest,
while later replacements can consume additional reserved credit. The
reservation is reused as partitions hand off and released when the session
ends. If post-fence writes introduce more identities than it covers, the
durable foreground mutations remain valid but the lower-priority full-sync
attempt is invalidated and retried.

Native replication frame receive/send buffers, fragmented-command assembly,
decoded record key/value strings and record vectors are bounded
temporary materializations. They rely on protocol size limits and checked
allocation rather than max-memory reservations. State that can accumulate or
survive an individual frame is still admitted against retained memory: this
includes journal/backlog ownership, full-sync coverage and subscriber queues,
and the replica's multi-frame large-value staging buffer. Collection RDB
decoders reserve memory before allocating a plain page or packed node; the
page carries that charge through storage ingestion or output backpressure.

Runtime-only commands published while the key snapshot is in progress enter
the same bounded full-sync command FIFOs without creating snapshot state.
Partition reset epochs are installed on the target in bounded batches, so a
channel-sharded `PUBLISH` may arrive before its transport partition's batch.
The target still validates its partition range, frame order, fragmentation,
and LRC1 body, but only decoded `PUBLISH` is exempt from the installed-epoch
check because it cannot touch the rebuilding dataset. Durable commands and
runtime envelopes that can apply storage effects continue to require that epoch
before replay.
In Meta-managed mode `PUBLISH` also carries slot-scoped mutation authority
through its final replication-publication check. A controlled failover pauses
new publications and drains those already admitted before freezing the source
frontier, so its cut cannot strand an ephemeral publication on the old owner.
Function libraries live outside the storage snapshot. Catalog mutations
therefore remain in online history but are projected out of full-sync command
FIFOs; a mixed EXEC retains its non-Function effects. At the final native cut,
flow zero sends one exact, fragmentable `FUNCTION RESTORE ... FLUSH` catalog
command after draining its command FIFO and before fencing the online backlog.

Before any target partition reset or frame apply, every flow waits for one
idempotent durable full-sync invalidation. It clears the old population token,
promotion base, and catalog readiness and leaves the node `LOADING`. The
synthetic RESTORE then passes through ordinary complete-catalog staging,
overwrites the durable dump, swaps all runtimes, and ACKs. A later population
or activation failure does not roll the catalog back. Final activation clears
the durable fence only when the population token and catalog readiness both
belong to that full-sync session; restart while the fence exists restarts the
whole group instead of serving the old root.

Before any target reset, every flow must select the same FULL mode. Flow zero
then closes command database admission, drains work admitted against the old
serving generation, and quiesces Tomb Raider and expiration while all other
flows wait at a session barrier. Admission reopens only after that boundary so
trusted replica apply can proceed; external commands remain fenced by the
closed serving generation. This makes the first destructive reset, rather than
the final cut, the boundary after which no request admitted against the old
serving generation, Tomb Raider round, or expiration task can mutate or
publish the old logical population. Physical maintenance such as defrag and
transaction cleanup may continue; epoch, index-generation, and record-location
revalidation prevents that stale work from publishing into the replacement.

After that boundary, the native flows collectively call
`ResetReplicaPartitions` for all 16,384 physical partitions, including empty
ones. Each bounded reset batch persists new candidate replication epochs and
then detaches the old indexes. Recovery rejects the old population because its
partition epochs are stale. Candidate records use the next database epochs,
which are persisted only by successful promotion, so recovery also rejects an
interrupted or aborted candidate. Cluster restart independently creates a fresh
`NOT_READY` proof domain and remains LOADING. Baseline, replacement, and tail
records all enter the same indexes while LOADING. For a cluster population, the
source opens partition snapshot capture and scans baseline data only for
partitions present in the authorized manifest; non-member partitions proceed
directly from reset to an empty handoff. The target also discards
out-of-manifest records and full-sync commands as a final guard. Partition
handoff still covers every physical partition: it records the manifest's
logical epoch, including zero for a non-member, against the exact target-local
epoch returned by reset. On the target, every handoff marks the corresponding
empty or populated partition as tailing.

At the final cut, the source closes and drains snapshot-transaction admission,
closes and drains command database gates, drains replacement and session
queues, fences each shared backlog, pins each returned stable cursor, stops
capture on every flow, and reopens foreground admission before waiting for the
network cut ACK. The target waits for every flow's cut, closes its own command
gates, validates every partition, drains staged storage writes, persists the
new database epochs, removes sync state, and only then becomes eligible to go
online. Each flow follows a local protocol phase machine: FULL rebuild frames
are accepted only before its cut, the first online cursor must match the
installed stable cut, and later reset/snapshot/cut frames are rejected;
CONTINUE accepts no reset/snapshot frames and requires its first cursor to
match the exact cursor requested in `LVFLOW`. A cut also rejects an unfinished
command fragment before any promotion work. The target records every flow's
stable next-LSN cut and, after storage promotion,
installs the complete cut vector into shared continuation state before it
releases the all-flow completion barrier or acknowledges any cut. Online cursor
frames must agree with that installed vector, so reconnect cannot retain a
partially updated set of flow cursors. The source's final `LVONLINE` is only a
notification: the target opens its serving generation only after every flow
has completed its cursor handoff, the FULL cut vector is installed (or the
CONTINUE population was already valid), every flow remains connected, and the
session is not cancelled. For a cluster rebuild, the same cut proof marks the
Function catalog complete, records the stable cut, initializes the live Applied
frontier, records
successful storage promotion, and publishes the boot-scoped `ReadyToken`.
Storage promotion first drains each worker's existing detached-index queue, so
successive successful destructive rebuilds cannot accumulate old index arenas
or unsettled record-block accounting. This reuses the storage engine's existing
synchronous reclaim boundary rather than adding per-attempt tracking. These
proofs are runtime state; durable Function-catalog recovery is outside this
boundary.

When an ordinary failure leaves a partial rebuild, teardown cancels the whole
session, resolves every cross-flow rendezvous, joins every detached flow and
transaction apply, and only then aborts and detaches that partial population.
Storage does not report abort complete until each worker's detached-index drain
has reclaimed detached prior-generation and candidate index RAM and settled
record live-block accounting. Value extents may continue through asynchronous
retirement after that boundary; allocator capacity excludes them as
`retired-unreclaimed` debt until their blocks are actually reusable. An
uncertain promotion, cut installation, join, proof invalidation, or abort
instead enters the current-boot terminal latch; the manager cannot safely
start another attempt in the same physical indexes.

Role transitions also fence client work with a packed serving generation.
Closing a population advances the generation and wakes all blocking registries;
commands revalidate after acquiring ordinary database admission or draining a
self-managed exclusive database cut, so queued, blocked, scanning, and backup
requests from the previous population cannot observe or capture the rebuilt
indexes. A self-gated command that validates first retains its gate through the
point that makes its result stable, so a later replacement waits instead of
invalidating an already committed result. Replication-origin apply bypasses
this client fence while the population is closed.

## Online apply and rendezvous

The target disables transport-level socket read-ahead on native flow
connections. Its application receiver nevertheless validates frame identity
and fragment order, reassembles and decodes complete LRC1 commands, and places
at most 256 commands in an owner-local FIFO. A staging coroutine consumes that
FIFO in flow order. Ordinary commands are applied there; transaction markers
are registered with the cross-flow scheduler without waiting for apply, up to
a second bounded completion FIFO of 256 entries. An ordinary command or control
barrier is a hard staging boundary and waits for the preceding transaction on
that flow, preserving its mixed-event order.

Each flow retains its previous registered transaction and submits that
predecessor with the next marker. A transaction ID deterministically selects a
target worker; registration crosses through the runtime's sender-to-owner SPSC
lane, and only that worker accesses the corresponding rendezvous table. Each
flow has at most one registration submission in flight, retaining bounded
pressure without a process-wide transaction lock. Once every marker and the
one canonical payload have arrived, a detached task on the transaction owner
waits those predecessors and applies the command. Transactions with disjoint
participant sets have no dependency and may apply concurrently across owners;
transactions sharing any flow retain source order even when their IDs select
different owners.

A separate ACK coroutine drains the completion FIFO in receive order. A
successful storage apply publishes its next-LSN to a cache-line-isolated,
lock-free atomic slot before it becomes ACK-eligible. The ordinary per-flow hot
path performs one release store and no cross-core mutex or atomic RMW. If an
ACK detects a disconnect, reconnect does not repeat an already
applied `APPEND`, `INCR`, or similar effect. An ingress, staging, ACK,
rendezvous, or apply failure cancels the whole session. Cancellation visits
each rendezvous table on its owner worker, resolves incomplete arrivals, and
joins both flow-local coroutines and every detached transaction task before
replacement, so no task can retain the old stream or storage mutation
lifetime.

Multi-participant writes carry a binary V1 `LTX1` metadata argument on every
participant flow. Its little-endian layout is the four-byte magic, transaction
ID (`u64`), payload flow (`u16`), participant-bitmap length (`u16`), and the
canonical bitmap with no trailing zero byte. The named flow carries the
canonical command as the remaining LRC1 arguments while the other flows carry
only the metadata argument. Payload-flow selection rotates across the
participants so one bounded backlog does not become a hotspot. The target
verifies and groups these markers by transaction ID, applies the command once,
and publishes all participant cursors inside one publisher sequence before any
flow ACKs. Snapshots accept a vector only when every publisher sequence is
unchanged and even, so promotion, reconnect, and heartbeat observation cannot
see half of a transaction or control barrier. Publisher sequences are
single-writer release stores rather than lock-prefixed increments; sequence or
LSN exhaustion poisons the frontier and fails closed. This representation is
shared by every cross-flow command and does not encode a Redis command kind.

Source-side cross-flow transaction and control publication uses one
process-global ordering slot so independent worker FIFOs agree on rendezvous
order. A request whose complete, concrete key set maps to one shard skips the
slot because it cannot participate in a cross-flow cycle. Requests whose
execution can discover additional participants serialize conservatively, as
does `EXEC`.

A keyed `EXEC` containing a successful Function mutation adds flow zero to the
same participant set even when no key maps there. Because the catalog is
already durable at this point, the source waits for every participant marker
to cross a publisher fence before replying; failure globally fences request
serving until restart.

A replicated standalone `MSET` acquires the slot before publisher admission or
database gating, admits its participant workers in parallel, and releases it
as soon as every participant marker is queued rather than holding it across
storage I/O. Database-control publication uses the same ordering boundary:
`FLUSH` acquires it before closing database gates, while a full-sync cut closes
and drains snapshot-transaction admission before database gates. These gates
do not have one global acquisition order, so the remaining hold-and-wait risk
is documented under [current limitations](#invariants-failures-and-current-limitations).

## Redis interoperability

### Following Redis

A Redis follower authenticates, sends PING, advertises its listening port and
PSYNC2 capability, and requests either its process-local replid/offset or a
fresh full synchronization. FULLRESYNC receives a length-delimited RDB into a
temporary file. Import is serialized, closes and drains command database
gates, resets the source-owned slots, validates ownership, and restores values.
Collection input is prevalidated and then consumed as admitted pages on the
key owner, with one atomic ingest decision per complete key rather than a
whole-object compact buffer. Packed collections and quicklist nodes are
measured before decoding; Hash/Set/ZSet duplicate identities are checked
across page boundaries, and ZSet input need not arrive in score order.
Unsupported self-describing Module values and Module auxiliary
records are skipped with warnings. Redis 7 `FUNCTION2` entries are instead
collected as one catalog, validated before key application begins, and
installed through the durable catalog replacement only after the RDB keys
have applied successfully. Before receiving a FULLRESYNC body, the target
durably invalidates its old population, promotion base, and catalog readiness;
that FULLRESYNC starts one group replacement generation and forces every other
registered source to request a fresh full synchronization. All source command
streams wait behind activation, and the fence clears only after every required
Cluster source has installed the same replacement generation. The pre-release
Function opcode is rejected because it is not safely skippable.

The online stream handles `SELECT`, `PING`, `REPLCONF GETACK`, `PUBLISH`,
Function mutations, keyed writes, and strict `MULTI`/`EXEC`. `PUBLISH` is
delivered locally without being forwarded again. The offset advances only
after successful apply.
In standalone Redis PSYNC compatibility mode, one Lavik process can follow
multiple masters of one Redis Cluster; these are sources for one imported
dataset, not multiple cluster-managed replication groups. Each source must be
a slot-owning master, and `ADDREPLICAOF` accepts only a source with disjoint
slots and the same complete topology. The node becomes online only when
registered sources cover all 16,384 slots, the source count matches the
topology's master count, every source dataset is valid, and the topology is not
faulted. Two consecutive incompatible topology observations fault the topology
and return the node to loading.

### Exporting to Redis

Redis export is available only in standalone mode while Lavik is a master,
requires the replica to advertise diskless EOF support, and permits one export
connection at a time. Every cluster mode rejects it because Redis PSYNC cannot
carry the exact population authorization required by the cluster data plane.
The exporter closes command gates, starts one RDB snapshot per worker,
fences every replication log, reopens writes, and streams a bounded-queue RDB
followed by online backlog events. It snapshots the Function catalog inside
the same cut and emits Redis 7 `FUNCTION2` entries before the key records.
Grouped keys retain their exact snapshot graph and stream admitted pages
through the incremental collection encoder. A whole-key lease serializes one
queue producer token across workers: producer-local FIFO alone would not
prevent different keys' fragments from interleaving at the consumer. Output
fragments are at most 1 MiB; page memory and graph pins remain owned across
network backpressure and are released on completion or cancellation.

The backlog merger reads one head event per worker. Ready cross-worker
transactions and control barriers take priority over unrelated mutations; a
transaction is emitted once as Redis `MULTI`/`EXEC`, and a flush is emitted
once after matching copies are present on every worker. Without
`redis-export-backpressure`, a slow Redis replica that falls below a backlog
floor is disconnected and must full-sync again. With it enabled, the exporter
pins its cursors; pressure from those pins follows the global
`replication-backlog-backpressure` wait-or-full-sync policy.

`LAVIK.HREPLACE` uses the native command stream between Lavik nodes.
Redis export lowers each successful replacement to `DEL` followed by `HSET`
inside the enclosing `MULTI`/`EXEC`; source-captured `PERSIST`/`PEXPIREAT`
effects preserve the final absolute expiry. Failed existence conditions do
not publish a replacement. Standard Redis replicas never receive the extension
command, and replacement does not require reading old fields for export.

### Sentinel-managed failover

Lavik exposes the Redis-shaped status surface Sentinel uses. `ROLE` reports
the master or replica tuple and offsets; `INFO replication` reports downstream
replicas, upstream address and link state, link-down duration, replication
offsets, and `slave_priority`. Native and Redis link progress are mapped onto
these process-local compatibility offsets. Sentinel's named Pub/Sub
connections also appear in `CLIENT LIST` and can be selected by `CLIENT KILL
TYPE pubsub`.

Sentinel normally queues `REPLICAOF`/`SLAVEOF`, `CONFIG REWRITE`, and `CLIENT
KILL TYPE normal|pubsub` in one `MULTI`/`EXEC`. Lavik accepts these commands
as an isolated management batch, preserves their order and individual replies,
and rejects mixing ordinary commands into that batch. It does not stop
unrelated work between the management commands; the role transition itself
provides the storage admission boundary.

`CONFIG REWRITE` preserves unmanaged directives and atomically replaces the
configured upstream mode plus `replica-priority` through a synced temporary
file, rename, and directory sync. It rejects a node with multiple Redis Cluster
upstreams because that topology cannot be represented by one Redis config
directive. Together with priority zero/lower-value semantics, this is the
surface exercised by an external Redis Sentinel quorum during promotion and
reattachment.

## Configuration and observability

| Setting or command | Current scope and behavior |
|---|---|
| `cluster-enabled` / `--cluster-enabled` | Meta-managed one-node-one-group, fail-closed startup requiring a stable node id and at least one numeric Meta seed; incompatible with startup `replicaof`, `redis-replicaof`, and `load-rdb`; the public manager also ignores a standalone initial upstream supplied by a direct embedder |
| Cluster control adapter | Node-controller-only source rebuild and source-less first-population admission/completion handles, population status, and source authorize/revoke APIs; Meta transport remains outside `ReplicationManager` |
| `replicaof host port` / `REPLICAOF` | Standalone Redis-style config or runtime role change with native-first discovery; rejected in cluster-managed mode |
| `redis-replicaof host port` / `--redis-replicaof` | Explicit standalone startup Redis PSYNC source; rejected in cluster-managed mode |
| `ADDREPLICAOF host port` | Standalone runtime addition of a disjoint master from the active Redis Cluster; rejected in cluster-managed mode |
| `replica-read-only` | Startup write policy; `REPLICAOF NO ONE` is writable regardless |
| `replica-priority` | Startup/runtime Sentinel election priority, default 100; zero is ineligible and lower nonzero values are preferred |
| `CONFIG REWRITE` | Atomically persists the current single upstream mode and `replica-priority`; unavailable without a config file or with multiple Redis Cluster sources |
| `tls-replication`, `masteruser`, `masterauth` | Outgoing control and every data connection; only the `default` user is supported |
| `repl-backlog-size` | Startup/CLI/runtime global backlog, default 1 GiB; at least one 8 MiB block per worker |
| `replication-backlog-backpressure` | Startup/CLI/runtime retained-history policy, default `yes`; `no` prefers primary write availability by forcing lagging consumers to full-sync at capacity |
| `replication-publish-queue-mb-per-worker` | Startup/CLI/runtime staging waterline, default 16 MiB per active worker log and per active full-sync session |
| `replication-snapshot-batch-size` | Startup/CLI/runtime scan scheduling batch, default 64 |
| `replication-snapshot-read-concurrency` | Runtime-only read concurrency, default 16 and maximum 128 |
| `redis-export-backpressure` | Startup/CLI selection between cursor pinning and disconnect-on-gap |

`INFO replication` reports Redis/Sentinel-compatible role, link and offset
fields alongside group, boot and replica-incarnation IDs, upstream identity,
native session/flow counts, history IDs, local Function-catalog generation and
CRC64, downstreams, Redis sources, dataset validity, priority, and topology
fault state. The callable cluster status separately reports the local node/boot,
`NOT_READY`/`REBUILDING`/`READY`/`FAILED_STOPPED` state, a ready token only in
`READY`, and the terminal failure reason. Replica and Sentinel Pub/Sub
connections are registered in CLIENT metadata. Prometheus exposes per-worker
backlog, publisher queue, retention/backpressure, full-sync queue/session, and
connection metrics.

## Invariants, failures, and current limitations

- Role epoch, history ID, session ID, flow LSN, manifest logical epoch,
  committed partition replication epoch, database epoch, and target-local
  replication epoch are separate identity domains.
  None can substitute for another.
- A cluster-managed process accepts at most one replication-group assignment
  during one boot. Safe-source authority and readiness evidence are bound to
  the complete rebuild directive and target boot. A cluster source accepts
  only an exact authorized population handshake from its named target.
- Controlled failover authorizes preparation only after the candidate covers
  the old owner's drained stable frontier and records loss as `none`.
  An uncontrolled action selected after fencing may proceed without the old
  owner, chooses the best comparable eligible observation available to Meta,
  and records loss as `unknown`. A healthy action already authorized lossless
  by controlled failover may survive degradation with `loss=none`; every new
  or replacement uncontrolled action is `unknown`. Like Redis Cluster's
  asynchronous replication, acknowledged writes not present on the selected
  replica can be lost; no cross-node durable commit quorum is implied.
- All native flows continue together or full-sync together. A target publishes
  online only after all partitions and the all-flow cut validate.
- Applied progress is next-unapplied LSN per logical source flow. Ordinary
  event publication is lock-free and per-flow; cross-flow logical effects use
  bounded sequence validation so readers either obtain the whole vector or no
  snapshot. Heartbeat sampling revalidates the Ready context, token, and
  frontier identity after that lock-free read; a concurrent withdrawal omits
  the candidate. Transport fragments never enter candidate progress.
- A native full sync destructively resets every physical partition in place;
  it has neither a retained old active root nor a separate staging root. The
  target installs the complete all-flow cut vector before acknowledging any
  cut, never one flow cursor at a time.
- Replay never republishes itself, and a replica never performs authoritative
  active expiration.
- Runtime-only `kEphemeral` events consume normal publication and retention
  capacity but create no durable keyspace baseline. A replica may still issue a
  local `PUBLISH`; only a source forwards its PUBLISH events downstream.
- Meta-managed `PUBLISH`, including its `EXEC` form, is a slot-scoped runtime
  mutation. Controlled failover drains its authority guard and rechecks at
  replication publication before freezing the old source frontier. An `EXEC`
  capture freezes local subscriber membership, protocol encoding, and receiver
  count at each `PUBLISH` position, delays delivery until that durable or
  ephemeral publication cut succeeds, then makes one bounded enqueue attempt
  for each captured match even if authority changes while delivery is in
  flight. Later subscription commands cannot join or leave that captured
  publication; session closure and output backpressure retain their ordinary
  delivery behavior, while a rejected final check exposes neither a local
  message nor a replica-backlog event.
- Publication admission must reject before mutation when the complete event
  cannot fit. Retention pressure either waits for ACK progress or, when
  configured not to backpressure, revokes a lagging consumer's coverage and
  forces its full sync; neither policy can leave a successful primary write
  out of the source history.
- Native cascading replication is unsupported. A node with an upstream rejects
  native downstream handshakes and Redis export, and replica application never
  republishes upstream events.
- Meta-managed post-cutover topology asks every non-owner to follow the new
  owner directly. It reuses native CONTINUE/FULL selection and does not depend
  on cascading or a separate rebuild operation. A former owner retires its old
  source history locally before beginning that relationship.
- Storage record application during replica synchronization bypasses the
  command layer's database gates. The code records that this breaks the
  exclusive, still-keyspace assumption used by KEYS's two-pass response and by
  FLUSHDB drain-then-detach; concurrent apply can therefore make the announced
  KEYS array length disagree with emitted elements or violate FLUSHDB
  exclusivity.
- Malformed, gapped, or divergent online command ingress, including frame
  identity/order, fragment reassembly, LRC1 decode, rendezvous, and
  command-apply failures, invalidates the whole continuation domain. Serving
  closes immediately, every flow is cancelled and joined, and a standalone
  target selects a fresh all-flow FULL; a cluster target discards the ready
  proof and waits for a fresh directive identity.
- The global publication-order slot has no fairness deadline. `MSET` can still
  acquire database admission before snapshot-transaction admission, other
  multi-shard paths can acquire database admission before the order slot, and
  `FLUSH` acquires the order slot before draining database gates. These inverse
  orders still admit hold-and-wait cycles, so replication is not documented as
  deadlock-free.
- Redis PSYNC cursors and native continuation cursors do not survive restart.
  Durable target data does not imply crash-resumable replication history.
- A Meta-managed `ReplicationGroup`, its reset capability, and its ready token
  are current-boot state. Every such restart constructs a new `NOT_READY`
  group and remains LOADING. A consumed Clean Shutdown Proof permits fresh
  candidate evidence, while records alone require rebuild or explicit operator
  recovery; old directives, leases and ready tokens cannot reactivate serving.
- The callable `ReplicationManager` adapter does not receive or authenticate
  Meta messages and does not itself publish candidate state to a quorum.
  `MetaControlClientService` owns the authenticated session, while
  `NodeControlInstaller` is the sole topology/replication mutation seam.

## Verification and known gaps

`tests/replication_log_e2e_test.cpp` directly exercises source full-sync
capture, replacements and handoff, publisher admission, fragmentation,
capacity changes, retention backpressure, deterministic TTL effects, strict
replay, database barriers, and restart without backlog recovery.
`tests/list_e2e_test.cpp` covers native full sync and tailing, unequal worker
counts, large values, role changes, reconnects, TLS on control and all flows,
flush during full sync, exact-once non-idempotent effects, tight queue
waterlines, multiple replicas, connection-scoped `WAIT` across flow ACKs,
Function-catalog full sync and incremental mutation, config rewrite, and
transaction/control fault injection and destructive replacement of the
previous population. Cluster model and integration coverage checks boot-scoped
identity, safe-source reset authorization, all-partition reset and handoff,
all-flow cut publication, fail-closed admission and failure, serving fences,
and reclaim accounting. `tests/cluster/README.md` is the cluster test inventory.

`tests/multikey_e2e_test.cpp` covers concurrent wide replicated `MSET`, changing
participant sets, duplex source sending, and cross-flow rendezvous progress.
`tests/pubsub_e2e_test.cpp` covers local and replicated `PUBLISH`, runtime-only
publish transactions, mixed durable/Pub-Sub transactions, and RESP2/RESP3
subscribers.

`tests/redis_cluster_psync_e2e.sh` covers multi-source Redis Cluster discovery,
slot ownership, online writes, disconnect, and partial resynchronization.
`tests/redis_export_e2e.sh` covers diskless baseline transfer, online writes,
transactions, flush, detach, Function transfer, and both export configuration
modes. `tests/sentinel_e2e_test.cpp` runs a three-Sentinel quorum through
priority selection, promotion, reattachment, and post-failover writes.
`tests/multi_exec_e2e_test.cpp`, `tests/rdb_test.cpp`, and `tests/config_test.cpp`
cover Sentinel management transaction isolation, `FUNCTION2` codecs/catalog
validation, priority parsing, and atomic config rewrite. Unit tests also cover
expiration-effect construction and replication-frame validation.

There is no focused malformed-LRC1 decoder matrix, standalone Redis follower
E2E distinct from the cluster path, forced slow-reader Redis export test, or
deterministic gate-order regression across publication-order and
FLUSH/full-sync interleavings. Two legacy replication tests in
`tests/list_e2e_test.cpp` are disabled and are not current verification.

## Source map

| Claim | Repository source |
|---|---|
| Public roles, options, status, and manager boundary | `include/lavik/replication.h` |
| Single-group rebuild identity, safe-source authorization, logical/local epoch mapping, manifest/reset proof, readiness, clean recovery, and fail-stop contract | `include/lavik/replication_group.h`, `src/replication/replication_group.cpp`, `src/replication/population_recovery.h` |
| Callable cluster directive/status/source-authorization, failover prepare/activation, source pause, and follow-owner adapters; native control/data protocol, duplex online flow, role lifecycle, Redis follower/export, topology, Function full sync, and reconnect behavior | `include/lavik/replication.h`, `src/replication/replication.cpp` |
| Lock-free live target Applied frontier and coherent cross-flow snapshots | `src/replication/replica_applied_frontier.h`, `src/replication/replica_applied_frontier.cpp` |
| Canonical command format and deterministic expiration effects | `include/lavik/replication_command.h`, `src/replication/command.cpp` |
| REPLICAOF/Sentinel commands, serving-generation fencing, blocking-wait invalidation, MSET publication admission/order, Function and PUBLISH capture, transaction/control capture, and trusted replay | `include/lavik/command.h`, `src/redis/command.cpp`, `src/redis/blocking_wait.cpp`, `src/redis/command_table.cpp` |
| Authenticated listener handoff and module construction | `src/redis/server.cpp` |
| Redis RDB Function catalog import/export and `FUNCTION2` encoding | `include/lavik/rdb.h`, `src/redis/rdb.cpp`, `src/redis/function_catalog.cpp`, `src/redis/server.cpp` |
| Pub/Sub delivery and connection/session state used by replicated PUBLISH and Sentinel | `include/lavik/pubsub.h`, `src/redis/pubsub.cpp`, `src/redis/server.cpp` |
| Runtime source-log and target full-sync interfaces | `include/lavik/storage/engine.h` |
| Durable catalog, full-sync invalidation, population eligibility, and promotion base | `src/storage/engine/system_state.cpp`, `src/storage/engine/init.cpp` |
| Backlog blocks, publisher/session queues, capture state, and target sync state | `src/storage/engine/impl.h` |
| In-memory append/read/fence/retention, capacity, invalidation, and control publication | `src/storage/engine/replication_log.cpp` |
| Manifest-filtered full-sync scanning, replacements, handoff, target reset/apply/promotion/abort, detached-index reclaim, cascade and DB-gate limitations | `src/storage/engine/replication.cpp`, `src/storage/engine/write.cpp` |
| Frame layout, event kinds, fragmentation, and checksums | `include/lavik/storage/format.h`, `src/storage/format.cpp` |
| Startup/runtime replication configuration, cluster fail-closed admission, and atomic CONFIG REWRITE | `app/lavik.cpp`, `include/lavik/server.h`, `src/config.cpp`, `src/redis/command.cpp`, `src/redis/server.cpp` |
| Native, group-model, cluster-startup/manager/generation/failure/protocol-guard/failover/follow-owner, log, MSET, Pub/Sub, Sentinel, Redis PSYNC/export, RDB, format, and configuration verification | `tests/replication_group_test.cpp`, `tests/cluster/cluster_invariants.cpp`, `tests/cluster/fault_harness_test.cpp`, `tests/cluster/population_integration_test.cpp`, `tests/cluster/replication_manager_integration_test.cpp`, `tests/cluster/serving_generation_integration_test.cpp`, `tests/cluster/rebuild_failure_integration_test.cpp`, `tests/cluster/rebuild_protocol_integration_test.cpp`, `tests/replication_log_e2e_test.cpp`, `tests/list_e2e_test.cpp`, `tests/multikey_e2e_test.cpp`, `tests/pubsub_e2e_test.cpp`, `tests/sentinel_e2e_test.cpp`, `tests/redis_cluster_psync_e2e.sh`, `tests/redis_export_e2e.sh`, `tests/multi_exec_e2e_test.cpp`, `tests/rdb_test.cpp`, `tests/replication_command_test.cpp`, `tests/storage_format_test.cpp`, `tests/config_test.cpp` |
