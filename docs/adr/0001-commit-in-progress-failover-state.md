# Commit in-progress failover state

## Status

Accepted

Keylane records enough state for every in-progress Group failover in Meta's
Raft-replicated committed state. Failover execution is therefore resumed from
committed facts after Meta Leader replacement rather than depending on the
former Leader's memory; this deliberately accepts a small durable transition
model instead of making failover leader-local and simpler but non-resumable.
