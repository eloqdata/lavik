<!--
Copyright (C) 2026 EloqData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Order recovery domains by source term

## Status

Accepted

Uncontrolled recovery orders live Compatibility Domains by descending Source
Group Term, then reuses the existing vector selector only among Candidates in
one exact domain. This maps Redis's topology-recency preference onto Lavik's
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
