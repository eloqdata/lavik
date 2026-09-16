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

# Allow operator-selected recovery without automatic eligibility

## Status

Accepted for the recovery work in [issue #44](https://github.com/thweetkomputer/lavik/issues/44).

When a Group has no automatically eligible Candidate, an operator may explicitly
choose a current member's recovered population as the new primary's data base.
This includes a restarted Owner or replica without a valid Clean Shutdown
Proof. It provides a deliberate recovery path when no live Owner can supply a
rebuild, while keeping automatic eligibility restricted to provable populations.

Operator Recovery accepts unknown data loss and gives no assurance that the
chosen population is the newest. It does not invent a prior flow cursor or
enter the chosen node into ordinary progress ranking. The selected data must
pass storage integrity checks and become a fresh durable base before service;
corrupt storage and an incomplete destructive full rebuild remain ineligible.
The recovery decision is explicit and auditable, scoped to one Group and its
selected member. A Data-cluster-wide outage is recovered by making that choice
for each affected Group.

Meta quorum and the committed authority protocol remain required. Recovery
fences the previous authority, binds preparation to the committed recovery
action, and opens service only after Cutover and a valid finite lease with the
normal handoff quarantine. A new history supplies subsequent replication;
followers rebuild from the selected Owner. Old leases, old sessions, and
cross-restart continuation are never restored. This is not a local authority
override when Meta itself is unavailable.
