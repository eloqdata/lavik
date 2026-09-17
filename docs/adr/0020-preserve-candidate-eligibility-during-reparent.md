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

# Preserve Candidate eligibility during reparent

## Status

Accepted for [#45](https://github.com/eloqdata/lavik/issues/45) and
[#46](https://github.com/eloqdata/lavik/issues/46). Partial replay, HistorySwitch,
and preservation before FULL admission are implemented. The current phase uses
destructive FULL after authenticated source admission, withdrawing old Ready
and Candidate evidence until the replacement completes. This lets #46 complete
replica attachment independently of #45. Isolated staged storage replacement
and joint availability acceptance remain in #45; the first-release availability
concession remains in effect until then.
The completed design will replace the first-release availability concession in
[ADR 0013](0013-rebuild-followers-after-promotion.md), under which concurrent
destructive FULLs could leave no eligible Candidate.

The completed design requires that entering reparent not itself revoke a trustworthy complete population's
Candidate eligibility. Partial replay retains a provable complete applied
frontier in its actual history domain; FULL retains the old Active Population
while building an isolated Staging Population, and a partial-to-FULL fallback
enters staging without first resetting Active. This accepts the bounded
resource cost of retaining both populations so a second Owner failure can
recover from surviving complete populations before any reparent finishes;
capacity pressure cannot be resolved by destroying the trusted Active.

A selected Candidate cancels and safely drains the previous transfer,
isolates staging, and promotes its complete Active Population under the
existing domain ordering and Uncontrolled Failover loss rules. Incomplete
staging is never Candidate evidence, and uncertainty about Active integrity
or safe teardown still invalidates proof or fails closed. The mechanisms may
be developed independently, but their combined availability claim requires
joint fault verification across partial replay, HistorySwitch, and staged
FULL; process restarts do not revive old boot-local proofs.
