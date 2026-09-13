# Order controlled and uncontrolled failover differently

## Status

Accepted

A Controlled Failover keeps the Source Owner's committed authority in place
behind a reversible write pause while the Candidate catches up and completes
Promotion Preparation; only then does one Cutover advance the term and install
the new authority. An Uncontrolled Failover instead advances the term and
fences the unavailable Owner when its transition begins, then may replace
Candidates within that same term. This asymmetry preserves the original Owner
when planned maintenance fails while preventing a failed Owner from regaining
authority during automatic recovery.

The Uncontrolled fence does not intentionally terminate the former Owner's
already established, authenticated downstream replication exports; their
remaining data is useful on a best-effort basis and cannot restore write
authority. Natural flow loss does not create a recovery obligation. If the
former Owner later applies the target-term fence and can produce an ordinary
Ready population proof for its current incarnation and history, it may compete
as a Candidate, including for a Cutover that keeps the same topology node. It
receives authority only through a fresh Candidate Action, Promotion
Preparation, and Cutover; recovery never directly reinstates its excluded
grant.
