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

# Report only provable failover loss

## Status

Accepted

Terminal failover outcomes classify loss as `none` or `unknown`. Controlled
abort without authority movement, normal controlled Cutover, and Cutover of
the same authorized paused-source Candidate Action may report `none`; ordinary
uncontrolled recovery and Candidate replacement report `unknown`. Without a
committed final Source frontier, Lavik does not claim bounded loss, a numeric
RPO, or a proven frontier from replica offsets alone.

Controlled outcomes remain self-contained in the existing Operation result.
Uncontrolled outcomes are fields of the accepted Cutover audit record and its
commit-indexed structured log, not a new Operation or per-Group history record.
Current Group status exposes only an active transition. Long-term history uses
the existing audit export mechanism. Runtime log delivery is at least once and
includes the transition identity, action identity, and Raft commit index for
deduplication; the committed audit record remains authoritative rather than
requiring a durable exactly-once logging outbox.
