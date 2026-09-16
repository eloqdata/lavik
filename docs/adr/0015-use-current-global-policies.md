# Use current global policies

## Status

Accepted

This supersedes ADR 0002's decision to retain a successor Grant specification
and Policy reference inside each Failover Transition. The transition remains
co-located with its Group; only that embedded configuration snapshot is
removed.

Meta Policy is a small, typed cluster-configuration registry in Committed
State, not a per-Group behavior attachment. Each registered family has one
current version whose activation boundary belongs to that family: automatic
failover reads it while making a trigger decision, while Authority Lease
changes flow through new Full Desired State and subsequent lease issuance.
Grants, Operations, and Failover Transitions therefore carry neither Policy
references nor lease duration. This keeps failover recovery independent of
historical configuration while preserving configuration across Meta Leader
replacement.

The initial cluster manifest provides typed Bootstrap Policy Defaults. Cluster
creation installs a missing family but never overwrites a pre-seeded current
value. Later versions become current immediately, are numbered consecutively,
and retain only a bounded history; retirement has no remaining domain meaning.
The raw document itself is the same-version idempotency value, with no separate
content hash in the command or committed store.
An automatic failover command already in submission is not cancelled or
version-gated by a later Policy update. That availability-first race may cause
one failover under the preceding decision, but fencing and authority CAS still
prevent overlapping write authority.
