# Co-locate one failover transition with each Group

## Status

Accepted

ADR 0015 later supersedes only the embedded successor Grant specification and
Policy-reference part of this decision. Transition ownership and lifecycle
remain as described here.

`MetaTopologyStore` owns at most one optional Failover Transition beside each
Group's current topology record. Controlled and uncontrolled failover share
this committed state shape but apply different failure policies, and the
Group's committed Owner changes only at authority activation. This keeps the
transition's lifetime and membership checks local to its Group without adding
an independent committed store or mixing transient handoff state into the
current serving-topology record. An uncontrolled transition may have no current
Candidate so loss of serving authority can be fenced before a replacement is
available. The transition also retains the successor grant specification,
including its policy reference, because beginning an uncontrolled term removes
the active grant. A Candidate Action atomically owns both its selected Data Node
incarnation and Compatibility Domain. An uncontrolled transition with no
Candidate Action has no pinned domain; when observations permit another
attempt, replacement chooses the newest live domain again. Source membership
and immutable replication configuration are read from the locked Group record
instead of being copied into two durable records.

While a transition is active, ordinary commands that could invalidate its
Group, Owner, Candidate, assignment, replication compatibility domain, term, or
grant are rejected. Candidate replacement atomically changes the Candidate,
domain, action identity, and preparation latch; final authority activation uses
the same transition revision precondition. Changes that do not alter these
anchors may continue.
