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

# Lavik

Lavik coordinates Redis-compatible data groups through a replicated Meta
control plane and node-local data-plane enforcement.

## Language

**Committed State**:
The deterministic Meta control-plane state replicated by Raft and restored by
a replacement Meta leader. An in-progress failover must retain enough state
here for the replacement leader to continue it safely.
_Avoid_: Committee Store, Meta KV store

**Owner**:
The Group member named by committed topology as its current primary. Being the
Owner does not by itself grant serving authority.
_Avoid_: Leader

**Group Term**:
The monotonically increasing committed authority epoch of a Group. Cluster
creation reserves term one before installing the initial Grant. Each term may
install at most one serving Grant; fencing advances to the next grantless term,
and every later authorization—including reauthorizing the same Owner—uses that
new term. Owner assignment plus Group Term is therefore the complete committed
identity of an active authority.
_Avoid_: Authority version, Grant revision

**Candidate**:
A compatible Group member selected to receive the next serving authority.
After an Uncontrolled fence, it may be the same node still named as Owner by
the unchanged topology; selection itself grants no authority.
_Avoid_: New primary, pending owner

**Failover Transition**:
The single in-progress, committed handoff for a Group. It identifies the exact
handoff and current Candidate Action but contains no history of prior Candidate
attempts. An Uncontrolled Failover may retain a Failover Transition with no
current Candidate while it waits for an eligible member.
_Avoid_: Failover workflow, recovery record

**Compatibility Domain**:
The Source Group Term, source incarnation, parent replication history, flow
count, and immutable Group replication configuration within which Candidate
progress may be compared. Each ordinary Candidate Action pins exactly one
domain. Operator Recovery has no historical domain or comparable frontier.

**Source Group Term**:
The committed Owner generation from which a population most recently derives.
It orders recovery domains by topology recency but does not prove that a newer
domain contains every write from an older or sibling domain.

**Recovered Population**:
A node's logical dataset reconstructed from durable storage after restart,
whether that node previously served as an Owner or a replica. Automatic
failover candidacy without a new rebuild requires a verified Clean Shutdown
Proof; reconstruction alone restores neither eligibility nor serving authority.
_Avoid_: Restored primary, recovered lease

**Clean Shutdown Proof**:
Durable evidence that a complete population and its Compatibility Domain and
Recovered Frontier were preserved by a successfully completed graceful
shutdown. It permits automatic failover candidacy after validation, not reuse
of the previous boot's serving authority.
_Avoid_: Shutdown request, successful process exit, index checkpoint

**Recovered Frontier**:
The complete per-flow logical boundary within a Compatibility Domain preserved
by a Clean Shutdown Proof and validated against the recovered population. It
grants neither serving authority nor permission to continue an old replication
session.
_Avoid_: Last acknowledged cursor, maximum disk LSN

**Operator Recovery**:
An explicit operator selection of a Group member as the recovery source when
the Group has no automatically eligible Candidate. Selection is not evidence
of a complete prior replication frontier and grants no serving authority by
itself.
_Avoid_: Controlled Failover, automatic candidate selection

**Loss Assessment**:
The terminal statement of whether a failover discarded Source data. `none`
means the handoff is known to cover the Source's controlled pause boundary or
did not change authority; `unknown` means surviving observations cannot bound
an unavailable Owner's unreplicated or non-durable tail. It is not a claim of
global linear consistency.
_Avoid_: Exact proof, bounded loss

**Node Incarnation**:
One assignment and process boot on a particular Data Node, together with the
replication history needed to interpret its progress. A later boot is a new
incarnation even when the node id is unchanged.

**Observation**:
A current Node Incarnation's replaceable report to the Meta Leader. It is
reacquired after Meta Leader replacement and is not Committed State.
_Avoid_: Proof, committed evidence

**Policy**:
A versioned, cluster-wide configuration held in Committed State. Each policy
family defines the decision boundary at which its current version takes effect
for every Group. A new current version does not retroactively cancel an
automatic failover command that has already entered submission.
_Avoid_: Per-Group policy, Grant policy

**Bootstrap Policy Defaults**:
Typed values in the initial cluster manifest used by cluster creation to
register any required global Policy that has not already been pre-seeded.
They never overwrite committed Policy and are not an ongoing configuration
source after cluster creation.
_Avoid_: Active policy, process policy

**Owner Serviceability**:
The Meta Leader's current assessment that the Owner can hold or regain serving
authority from current authenticated Observations and finite-lease evidence.
It is not Committed State or proof that a request has succeeded.
_Avoid_: Owner health, availability proof

**Authority Lease**:
A finite, boot-local serving capability installed by a Data Owner from a Meta
acknowledgement. Its effective duration is bounded by both the current global
Policy and the Meta Leader's local leadership-validity limit; expiry causes
the Data Node to fence itself without a separate report to Meta.
_Avoid_: Authority grant, Leader lease

**Automatic Failover Detector**:
The Meta Leader-local state machine that evaluates Owner Serviceability,
accumulates SUSPECT time, and may submit an Uncontrolled Failover. It neither
persists suspicion nor executes the resulting Failover Transition.
_Avoid_: Uncontrolled Executor

**SUSPECT**:
A Meta Leader-local accumulated interval in which the same committed Owner
authority is assessed as not serviceable. It is discarded on Meta leadership
replacement and never becomes Committed State.
_Avoid_: Failure record, durable suspicion

**Indeterminate**:
The Meta Leader lacks current causal evidence to classify an Owner as either
serviceable or unserviceable. It freezes any SUSPECT time already accumulated
for the same Owner authority. When the uncertainty is a known possible
Authority Lease, it is bounded by that installed lease's effective duration
and known expiry becomes an Unserviceable heartbeat timeout. A torn
runtime/session join has no provable deadline and remains Indeterminate until a
coherent cut arrives; elapsed time never manufactures failure evidence. A
leadership, authority, or relevant Policy change instead discards SUSPECT time.
_Avoid_: Unhealthy, failed

**BLOCKED**:
The automatic failover detector cannot currently advance its decision because
of an explicit control-plane condition or Indeterminate evidence. BLOCKED is a
leader-local diagnostic state and does not itself change cluster readiness or
serving authority.
_Avoid_: Failed, not ready

**Controlled Failover**:
An operator-initiated Failover Transition that preserves service on the
current Owner until its Candidate is ready for cutover.
_Avoid_: Manual Promotion

**Uncontrolled Failover**:
A failure-triggered Failover Transition that restores unavailable service and
may replace a failed Candidate without waiting for it to restart.
_Avoid_: Recovery Handoff

**Source Owner**:
The Owner from which a Controlled Failover's Candidate catches up before
cutover.
_Avoid_: Old primary

**Controlled Pause**:
A reversible, boot-scoped Source Owner write barrier for one Failover
Transition: it drains admitted mutations and blocks every operation that can
mutate the dataset or advance the replication frontier, including expiry and
background work. Frontier-neutral physical cleanup remains allowed; the pause
survives Meta session replacement but not a Data Node restart.

**Failover Observation**:
A Node Incarnation's current progress toward one Failover Transition, reported
again after Meta Leader replacement. It is an Observation, not Committed State.
_Avoid_: Operation evidence, failover proof

**Promotion Preparation**:
The boot-local conversion of a Candidate's replicated population into a
prepared primary population. It grants no serving authority.
_Avoid_: Promotion, cutover

**Candidate Action**:
One selection of a Candidate within a Failover Transition. An ordinary action
pins its Compatibility Domain; an Operator Recovery action instead pins the
selected member and population scope and accepts unknown loss, without claiming
a historical frontier. Every selection, including reselection of the same Node
Incarnation, receives a new immutable action identity.

**Action Failure**:
A terminal, boot-scoped statement that one Candidate Action cannot safely
continue. It makes that population incarnation ineligible until its boot or
population identity changes; transient retries are not Action Failures.

**Preserved Replica**:
A non-Candidate Group member whose usable population is retained until an
Uncontrolled Failover reaches Cutover, including a fenced node still named as
the committed Owner. It remains fenced and avoids destructive replacement,
while compatible replication already in progress may continue best effort.

**Promotion Authorization**:
A one-way committed latch allowing the current Candidate Action to begin
Promotion Preparation. For Controlled Failover, Meta commits it only after
observing that the Candidate has reached the Source Owner's live paused
frontier; the frontier itself remains an Observation.

**Desired-state cleanup**:
Removal or replacement of a Candidate Action in committed state. Data Nodes
reconcile the latest Full Desired State across live and reconnected sessions,
without requiring one-shot abort delivery, and acknowledge it only after the
old action can no longer publish progress or activate; destructive replication
catch-up may continue asynchronously.

**Follow Owner**:
The ordinary desired state in which every non-Owner fences its former role and
preserves usable local data until the new Owner is authenticated and
export-ready. It then continues compatible replication or rebuilds from that
Owner.

**Redundancy Restoration**:
Ordinary post-Cutover reconciliation in which non-Owners Follow Owner and
become usable replicas again; it is neither a Failover Transition phase nor a
Meta operation. Current Observations expose its progress, and Full Desired
State makes it resumable after either Meta or Data reconnects.
_Avoid_: Rebuild operation, failover rebuild phase

**Uncontrolled Executor**:
The resumable transition reconciler that fences a failed Owner, selects and
replaces Candidates, and commits Cutover. It is independent of the automatic
failure detector that decides when to begin an Uncontrolled Failover.

**Cutover**:
The committed change that makes a prepared Candidate the Owner under a new
Group term and authority tied to the originating Candidate Action, preventing
stale prepared state from activating. It removes the Failover Transition;
Redundancy Restoration follows as steady-state reconciliation, and any failure
after Cutover is treated as a new Owner failure.
_Avoid_: Promotion Preparation
