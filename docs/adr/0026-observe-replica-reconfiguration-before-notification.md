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

# Publish discovery notifications from observed transitions

## Status

Implemented in [#105](https://github.com/eloqdata/lavik/issues/105).

`+switch-master` reports a change in the publishable Data Primary address.
During one continuous authoritative Meta leadership, a transition from address
A through a period with no publishable Primary to address B retains A as the
old address and emits A-to-B only when B becomes publishable under committed
authority. Losing publication alone emits no switch. Reauthorizing the same
address under a new Group Term also emits no switch; old Data permissions
remain subject to their own revocation boundary.

The first observed publishable Primary establishes a baseline rather than a
synthetic switch. Meta leader replacement establishes a new baseline from
current state: it emits neither a switch merely because Meta leadership
changed nor a replay of Data switches the new observer did not witness.
Events are best-effort refresh hints, not durable history. Demotion or lost
authority freshness revokes old discovery requests and subscriptions; clients
recover through queries after reconnecting. HA correctness cannot depend on
event delivery across that boundary.

The Discovery Entry emits `+replica-reconf-done` only when fresh evidence from
the current Data incarnation establishes that the replica has actually adopted
the current Owner as its replication source. Acknowledging desired state and
having a complete readable population do not establish that fact: following
the new Owner proceeds asynchronously, while a retained population may already
support Stale Replica Reads. Completion promises neither zero replication lag
nor continuing connectivity and does not redefine replica-read admission.

Use existing replication evidence where sufficient and extend observations
only if necessary to establish completion. This narrows the observation
deferral in [ADR 0024](0024-publish-discovery-from-committed-authority.md): a
supported consumer now needs completion evidence, without requiring a general
upstream-link diagnostic surface. Reacquiring observations after Meta leader
replacement is not itself a Data reconfiguration and must not manufacture a
completion event.

Discovery notifications accelerate client recovery but are not its safety
boundary. Revoking stale Discovery Entry requests and subscriptions and
recovering through client seeds belong to discovery availability. Revoking
obsolete Data-session permissions and safely handling already-started requests
remain Data-plane responsibilities, including when a client misses an event or
does not subscribe. The former is delivered in #105; the latter and complete
surviving-Data-connection recovery are delivered in
[#100](https://github.com/eloqdata/lavik/issues/100). Validation uses persistent,
unmodified clients and pools rather than rebuilding them or replacing their
discovery logic with application polling.
