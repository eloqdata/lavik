# Separate failover intent from live observation

## Status

Accepted

Each Failover Transition has an immutable random identity and a revision used
to reject stale mutations, and it pins the selected Data Node incarnations.
Its progress is derived from committed topology and authority facts plus fresh
observations rather than a committed phase enum. In particular, a controlled
source's paused frontier remains a live observation: if that source incarnation
cannot report after Meta Leader replacement, the controlled attempt fails
instead of preserving a durable source-proof lifecycle. An operator-triggered
transition references its durable Operation, but execution state is not copied
into the Operation journal.
