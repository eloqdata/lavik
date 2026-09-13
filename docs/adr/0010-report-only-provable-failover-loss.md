# Report only provable failover loss

## Status

Accepted

Terminal failover outcomes classify loss as `none` or `unknown`. Controlled
abort without authority movement, normal controlled Cutover, and Cutover of
the same authorized paused-source Candidate Action may report `none`; ordinary
uncontrolled recovery and Candidate replacement report `unknown`. Without a
committed final Source frontier, Keylane does not claim bounded loss, a numeric
RPO, or a proven frontier from replica offsets alone.

Controlled outcomes remain self-contained in the existing Operation result.
Uncontrolled outcomes are fields of the accepted Cutover audit record and its
commit-indexed structured log, not a new Operation or per-Group history record.
Current Group status exposes only an active transition. Long-term history uses
the existing audit export mechanism. Runtime log delivery is at least once and
includes the transition identity, action identity, and Raft commit index for
deduplication; the committed audit record remains authoritative rather than
requiring a durable exactly-once logging outbox.
