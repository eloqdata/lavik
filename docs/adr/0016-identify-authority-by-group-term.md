# Identify authority by Group Term

## Status

Accepted

Serving authority is identified by the Owner assignment and Group Term; there
is no separate authority version or Grant revision. Each Group Term installs
at most one Grant. Fencing advances to a fresh grantless term, so replacing or
reauthorizing an Owner can never reuse the preceding authority identity.
Cluster creation reserves term one and later installs its initial Grant,
Controlled Failover advances and installs atomically at Cutover, and
Uncontrolled Failover reserves its new grantless term at Begin before
installing the Candidate at Cutover. This removes two counters that described
the same lifecycle while keeping stale commands and Data-plane traffic
rejectable by term, assignment, and projection identity.
