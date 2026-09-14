# Use typed failover commands

## Status

Accepted

Failover state changes use narrow commands for controlled and uncontrolled
begin, uncontrolled Candidate replacement, Promotion Authorization, controlled
abort, controlled degradation, and mode-specific Cutover. Compound commands
validate and mutate bounded copies of the topology, grant, and Operation stores
before publishing them together. This deliberately uses more command tags
than a generic transition setter so illegal combinations of active grant,
fenced term, Operation terminal state, and Cutover semantics cannot be encoded.
