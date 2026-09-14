# Bind promotion to a committed action

## Status

Accepted

Every Candidate selection receives a new immutable action identity, including
when the same Data Node incarnation is selected again. A controlled Candidate
may begin Promotion Preparation only after Meta commits a one-way authorization
latch for that action. The latch records its authorizing Raft revision and the
loss assessment permitted if that exact action reaches Cutover: `none` only
when authorization observed coverage of a Controlled Pause, and `unknown`
otherwise. Later transition revisions leave the Candidate Action identity
unchanged. Meta derives authorization from fresh source and candidate
observations but does not commit the paused frontier. The final grant retains
the winning Candidate Action as its activation identity, and Data activates
only a matching prepared context. This prevents prepared state from an aborted
or replaced attempt being reused after the old Owner has resumed writes, while
preserving loss provenance across controlled degradation without committing
progress observations.

A terminal Action Failure also revokes that action's boot-local preparation
capability and Candidate eligibility for the same population incarnation.
Transient failures are retried inside the existing action instead of minting
new action identities, so Meta needs neither a failed-attempt set nor an
unbounded same-incarnation retry loop.
