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

# Require clean shutdown proof for automatic reentry

## Status

Accepted for the recovery work in [issue #44](https://github.com/thweetkomputer/lavik/issues/44).

A restarted Group member may automatically qualify for failover without a new
full rebuild only when it validates a Clean Shutdown Proof for its recovered
population. This applies equally to an initial Bootstrap Owner, a later Owner,
and a replica; first-time Bootstrap still uses Meta's explicit initial Owner
assignment. Crash recovery, incomplete shutdown, and absent or invalid proof
require a full rebuild before automatic eligibility can return. Operator
Recovery is a separate explicit recovery decision, never an automatic fallback.

The proof must establish a complete durable logical boundary and its source
domain, including the final per-flow frontier. A shutdown request or successful
exit code is insufficient. Incomplete full sync, outstanding transaction
decisions, failed durability work, or an uncertain proof publication cannot
produce valid proof. Later boots or population replacement must not be able to
reuse proof predating their changes. An index checkpoint alone is not this
proof.

This chooses a bounded shutdown durability barrier over continuous tracking
and reconstruction of replication progress across arbitrary crashes. The
restarted process still has a new boot and replication history, begins fenced,
and reports fresh observations; it regains service only through Meta's
committed failover and finite authority lease. Neither old leases nor
cross-restart replication continuation are restored.

When no member has valid recovery evidence and no live Owner can provide a
rebuild, automatic recovery remains unavailable even if local records can be
read. The operator must make an explicit recovery choice; the automatic path
does not infer a safe candidate from the largest physical disk sequence.
