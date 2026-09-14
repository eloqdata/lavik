# Degrade a controlled transition in place

## Status

Accepted

When the Source Owner is lost before Cutover, one Raft command fails the
operator's Controlled Failover, advances and fences the Group term, and changes
the same Failover Transition to uncontrolled. Before Promotion Authorization,
Meta reruns ordinary Candidate selection and assigns a new Candidate Action.
After authorization, it may retain the same live, still-best Candidate Action;
the Uncontrolled Executor may then cut over a fresh matching prepared result
with a subsequent `CommitUncontrolledFailover`. Keeping Degrade and Cutover as
separate typed commands lets Degrade deterministically commit the fence without
embedding leader-local observations in that command. This preserves the
strongest recoverable Data state without linking an uncontrolled recovery to
the completed operator Operation. A failed or superseded Candidate is still
replaced immediately and is never awaited across an unbounded restart.
