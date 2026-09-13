# Order recovery domains by source term

## Status

Accepted

Uncontrolled recovery orders live Compatibility Domains by descending Source
Group Term, then reuses the existing vector selector only among Candidates in
one exact domain. This maps Redis's topology-recency preference onto Keylane's
already committed Group term without adding a durable history graph or domain
epoch. Distinct histories created in one term have equal recency and are tried
in canonical domain-identity order; their LSNs are never compared, and a
fallback Cutover reports loss as `unknown`.

A recovered newer domain does not preempt a healthy Candidate Action. When the
current action ends, Meta selects again from all fresh observations, newest
term first, so there is no committed one-way fallback cursor or permanent
Candidate blacklist. The selected domain and Candidate are committed together
in the new action, which makes this policy resumable after Meta Leader
replacement while leaving the domain population itself observational.
