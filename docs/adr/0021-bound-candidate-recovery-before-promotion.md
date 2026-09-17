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

# Bound Candidate Recovery before promotion

## Status

Accepted and implemented for the pre-promotion recovery scope of
[#46](https://github.com/eloqdata/lavik/issues/46).

During Uncontrolled Failover, after excluding the former Owner's write
authority, only the selected Candidate gathers missing canonical events from
compatible Recovery Donors before prepare authorization. An already-authorized
action continues its existing preparation rather than reopening recovery;
controlled-to-uncontrolled degradation preserves the valid action's original
loss authorization. The Recovery Target Frontier is a same-domain
goal derived from complete Applied cuts, and every missing range still
requires actual retained coverage. It is neither a cross-history merge nor
proof of the failed Owner's complete acknowledged tail.

Recovery is bounded best effort: reaching the goal or exhausting the recovery
allowance ends the optional gathering step. After safe cancellation and apply
drain, a still-trustworthy Candidate may prepare from its actual complete
frontier even if it missed the goal; uncertain apply results cannot be treated
as successful recovery. Other replicas retain their complete populations and
follow after activation rather than participating in an all-replica completion
barrier. This trades a limited pre-promotion delay for less discarded progress
and fewer divergent-cursor FULLs without requiring the slowest replica to
catch up before service returns. Existing Compatibility Domain ordering and
Uncontrolled Failover loss assessment remain in force.

One fixed Recovery Deadline is shared by the entire Failover Transition and
retained in Committed State. Once the former Owner's write authority is
actually excluded and the first Candidate is selected, Meta fixes the cutoff
when it first initiates recovery. Proposal submission and delivery, donor
discovery, connection, transfer, and retries all consume that allowance; it
does not start at the first received event. Candidate replacement, donor
replacement, and Meta Leader replacement cannot restart or extend it,
preventing successive attempts from turning optional recovery into unbounded
waiting.
Expiry stops optional gathering and leaves required apply drain and promotion
safety checks intact; if no eligible Candidate exists, the transition remains
fenced and waits. Applied progress and Recovery Coverage remain observations,
not a durable transfer journal.

The allowance uses a dedicated Candidate Recovery family in the existing
cluster-wide Meta Policy registry, initially 2000 ms. Automatic failover
triggering and Authority Lease remain separate families: recovery also applies
to manually initiated Uncontrolled Failover, and tuning its budget must not
reset the detector's SUSPECT interval or change the always-on detector's
triggering behavior. The family reuses the existing typed configuration,
versioning, and
bootstrap mechanisms rather than introducing a separate configuration store.
Zero skips active pre-promotion gathering while retaining normal event
retention, Secondary History, and post-Cutover partial reparent. The first
recovery-start proposal reads the current value and carries the resulting
absolute deadline; deterministic replicated apply does not read a clock. The
transition retains that execution cutoff, not a Policy reference or a
configuration snapshot. The proposing leader preserves its submitted cutoff
across retries and Policy updates. Once committed, the cutoff also survives
leadership replacement; a proposal lost before commitment does not establish
a recoverable deadline. The proposal that commits retains its proposal-time
cutoff, so commit and dispatch delay consume its budget. Subsequent Policy
updates affect only transitions whose deadline is not yet fixed. This follows
ADR 0015's decision-boundary model while keeping the optional availability
delay tunable independently of required promotion work.

Recovery Coverage is live availability evidence, distinct from both the target
frontier and the Candidate's actual Applied progress. Donor loss withdraws
unfulfilled coverage and may require another donor for the remaining gaps;
it does not undo completed apply or make already validated local events
depend on the donor staying online. Only complete logical effects with all
dependencies satisfied advance Applied. If coverage remains insufficient,
recovery ends at the actual complete cut established by safe apply drain,
without fabricating a reachable cut from independent per-flow coverage
endpoints or treating donor unavailability alone as Candidate corruption.
