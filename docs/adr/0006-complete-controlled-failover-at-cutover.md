# Complete controlled failover at committed cutover

## Status

Accepted

An operator's Controlled Failover completes successfully when Raft commits the
Cutover after the Candidate has reported Promotion Preparation, rather than
waiting for a later serving heartbeat. A Candidate failure after that point is
a new Owner failure handled by an independent Uncontrolled Failover. The
controlled deadline is absolute and committed across Meta Leader changes, and
new writes fail fast with `TRYAGAIN` while the reversible Controlled Pause is
active.
