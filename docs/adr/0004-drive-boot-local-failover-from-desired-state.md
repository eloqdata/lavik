# Drive boot-local failover from desired state

## Status

Accepted

Meta projects each committed Failover Transition through Full Desired State,
and Data Nodes report their current response as an optional failover status in
ordinary heartbeats alongside their existing role information. A Controlled
Pause is bound to the transition rather than the Meta session: it first drains
admitted mutations, blocks every path that can change the dataset or
replication frontier, then repeatedly reports a stable frontier across Meta
Leader reconnection. A Data restart changes the boot identity and fails the
controlled attempt. Data reconciles pause, preparation, activation, following,
and cleanup through one Full Desired State entry point. Removing or replacing
an action in committed state is therefore the durable cleanup request: an
online node applies it immediately, and an offline node applies the latest
state after reconnect, without a separate abort directive or durable Data-side
workflow. `FullStateApplied` is the retirement barrier: before acknowledging,
Data disables the old action's admission and activation capabilities, prevents
late tasks from publishing results, and either retires its local state or
atomically hands a winning prepared action to the matching authority. Replica
catch-up remains asynchronous. Transport or acknowledgement failure closes the
session and the next session receives the latest complete state, so periodic
Full Desired State replay and cleanup tombstones are unnecessary.

Before an Uncontrolled Cutover, every non-Candidate member is a Preserved
Replica: it remains fenced, retains and reports its current Ready population,
and does not destructively follow the revoked Owner or the not-yet-authoritative
Candidate. Fencing the former Owner revokes mutation authority but does not
deliberately close already established downstream replication flows. Those
authenticated, same-domain ONLINE flows may continue best effort until they
end naturally or Promotion Preparation retires them. This makes no promise to
open a new flow after disconnection, and ordinary Preserved Replicas do not
start either a new continuation or a destructive full rebuild before Cutover.
A Candidate Action uses a boot-local bounded watchdog. Retryable transport or
resource failures remain inside that action; an expired watchdog or unsafe
local outcome reports a terminal Action Failure and removes that exact
population incarnation from eligibility until its boot or population identity
changes. The overall Uncontrolled Failover has no deadline and may select
another action indefinitely.

After Cutover, the same reconciler derives Follow Owner for the former Owner
and every other replica. It fences the old role immediately but does not
invalidate a usable local population until the new Owner has authenticated and
is export-ready. Native replication then continues when histories are
compatible or performs a full rebuild; old source-backlog cleanup belongs to
this Data-local role transition rather than a Meta cleanup operation.

No committed source-history hold is added. Ordinary replication sessions and
their reconnect leases retain the source-wide backlog while useful; Candidate
replacement does not clear it, Controlled abort returns it to ordinary Owner
management, and an unreachable Source retains only what its boot can preserve
best effort. Uncontrolled recovery never waits for a final Source frontier.
