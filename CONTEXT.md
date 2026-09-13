# Keylane

Keylane coordinates Redis-compatible data groups through a replicated Meta
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
progress may be compared. Each Candidate Action pins exactly one domain.

**Source Group Term**:
The committed Owner generation from which a population most recently derives.
It orders recovery domains by topology recency but does not prove that a newer
domain contains every write from an older or sibling domain.

**Loss Assessment**:
The terminal statement of whether a failover discarded Source data. `none`
means the handoff is known to cover the Source's controlled pause boundary or
did not change authority; `unknown` means surviving observations cannot bound
an unavailable Owner's unreplicated tail. It is not a claim of global linear
consistency.
_Avoid_: Exact proof, bounded loss

**Node Incarnation**:
One assignment and process boot on a particular Data Node, together with the
replication history needed to interpret its progress. A later boot is a new
incarnation even when the node id is unchanged.

**Observation**:
A current Node Incarnation's replaceable report to the Meta Leader. It is
reacquired after Meta Leader replacement and is not Committed State.
_Avoid_: Proof, committed evidence

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
A reversible, boot-scoped write barrier held by the Source Owner for one
Failover Transition. It drains admitted mutations and then prevents every
dataset or replication-frontier mutation, including expiry and any background
work that can advance that frontier. Tomb Raider's physical cleanup does not
advance the frontier and is outside this barrier. The pause survives Meta
session replacement but not a Data Node restart.

**Failover Observation**:
A Node Incarnation's current progress toward one Failover Transition, reported
again after Meta Leader replacement. It is an Observation, not Committed State.
_Avoid_: Operation evidence, failover proof

**Promotion Preparation**:
The boot-local conversion of a Candidate's replicated population into a
prepared primary population. It grants no serving authority.
_Avoid_: Promotion, cutover

**Candidate Action**:
One selection of a Candidate and its Compatibility Domain within a Failover
Transition. Every selection, including reselection of the same Node
Incarnation, receives a new immutable action identity.

**Action Failure**:
A terminal, boot-scoped statement that one Candidate Action cannot safely
continue. It makes that population incarnation ineligible until its boot or
population identity changes; transient retries are not Action Failures.

**Preserved Replica**:
A non-Candidate Group member, including a fenced node still named as the
committed Owner, that retains its usable population before an Uncontrolled
Failover reaches Cutover. It remains fenced without starting destructive
replacement, while an already established compatible source flow may continue
best effort.

**Promotion Authorization**:
A one-way committed latch allowing the current Candidate Action to begin
Promotion Preparation. For Controlled Failover, Meta commits it only after
observing that the Candidate has reached the Source Owner's live paused
frontier; the frontier itself remains an Observation.

**Desired-state cleanup**:
Removal or replacement of a Candidate Action in committed state. Data Nodes
converge by reconciling the latest Full Desired State, immediately when
connected or after reconnect, rather than requiring a delivered one-shot abort
message. A Data Node acknowledges the Full Desired State only after the old
action can no longer publish progress or activate; destructive replication
catch-up may continue asynchronously.

**Follow Owner**:
The ordinary desired state for every non-Owner Group member. A member first
fences its former role and preserves its usable local population until the new
Owner is authenticated and export-ready, then uses the native replication
engine to continue when compatible or rebuild otherwise.

**Redundancy Restoration**:
The ordinary post-Cutover reconciliation in which non-Owners Follow Owner and
become usable replicas again. It is not a Failover Transition phase or a Meta
operation; its progress is derived from current Observations, and the full
desired state makes it resumable after either Meta or Data reconnects.
_Avoid_: Rebuild operation, failover rebuild phase

**Uncontrolled Executor**:
The resumable transition reconciler that fences a failed Owner, selects and
replaces Candidates, and commits Cutover. It is independent of the automatic
failure detector that decides when to begin an Uncontrolled Failover.

**Cutover**:
The committed change that makes a prepared Candidate the Owner under a new
Group term and authority. The installed authority retains the Candidate Action
identity that produced it, preventing activation from consuming stale prepared
state. Cutover also removes the Failover Transition; subsequent Redundancy
Restoration is steady-state reconciliation. A failure after Cutover is a new
primary failure.
_Avoid_: Promotion Preparation
