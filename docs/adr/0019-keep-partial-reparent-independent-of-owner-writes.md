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

# Keep partial reparent independent of Owner writes

## Status

Accepted and implemented for [#46](https://github.com/eloqdata/lavik/issues/46). This extends the follow-up optimization anticipated by
[ADR 0013](0013-rebuild-followers-after-promotion.md); population preservation
during fallback is defined by
[ADR 0020](0020-preserve-candidate-eligibility-during-reparent.md).

All eligible native followers retain a bounded suffix of completely applied
canonical events during ordinary following, before Candidate selection. An
initial FULL supplies the complete population and cut; retention starts with
subsequent online events rather than reconstructing history from snapshot
records. Retaining only after selection would lose the earlier events needed
by lagging siblings, so the design accepts ongoing follower cache use.

Parent retained events and the child backlog share the existing
`repl-backlog-size` history quota. Child activation and publication have
priority, and secondary event retention can be evicted to zero rather than
requiring a second history budget or blocking the Owner. This quota covers
history retention; publisher and transport queues keep their existing,
separate accounting.

Partial reparent is opportunistic: retaining or replaying the parent suffix
and retaining the required child prefix must not block the serving Owner's
writes. Missing coverage in either history sends only the affected target to
whole-group FULL. Its trustworthy Active Population survives until FULL
admission; the current destructive fallback then withdraws Ready until rebuild
completes. ADR 0020 defers preservation throughout FULL to isolated staging.
This trades guaranteed partial completion under retention pressure for Owner
write availability during partial reparent. Source-side FULL queues and cut
processing retain their existing backpressure policy; future staged population
replacement preserves Candidate eligibility but does not extend the partial-
reparent write-availability guarantee to FULL.

HistorySwitch binds each history to its own flow layout and installs the
complete child starting cursor only after the target proves that it has
reached the frozen parent frontier. Parent and child flow counts may differ.
Former Owners are included: their fenced and drained source frontier is their
parent-domain Resume Cursor until the switch commits, after which they adopt
the new Owner's authenticated child cursor for that same logical boundary.
The new Owner's current tail cannot substitute for proof of target progress.

The target's atomic installation of the child domain, complete cursor, and
Ready proof is the local HistorySwitch completion point. A lost final ACK
does not undo a proven switch: a same-boot reconnect may use ordinary child
history CONTINUE after fresh authorization, identity, and all-flow coverage
checks, including a valid child starting cursor before any child event has
been applied. An incomplete or unprovable switch still selects FULL under the
population-integrity rules of ADR 0020; neither restart nor a stale connection
can revive the old switch proof.
