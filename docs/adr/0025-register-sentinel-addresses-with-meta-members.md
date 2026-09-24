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

# Register Sentinel addresses with Meta members

## Status

Implemented in [#105](https://github.com/eloqdata/lavik/issues/105). This decision covers address ownership and deployment
coverage and peer-directory semantics. Notifications and the boundary with Data-session
recovery are addressed in [ADR 0026](0026-observe-replica-reconfiguration-before-notification.md).

The Sentinel address identifies the existing Discovery Entry. Register its
optional advertised address in the existing committed Meta member directory,
with the same registration, retirement, snapshot, and recovery lifecycle as
the member. The address is fixed for that member; enabling, disabling, or
changing its registration requires member replacement. This follows the
existing member-endpoint model rather than introducing independent mutable
discovery configuration, at the cost of member replacement when an existing
deployment adds or changes its registered Sentinel address.

Discovery remains leader-only. A managed Single deployment enabling discovery
HA must configure and run a Discovery Entry on every Meta member eligible to
become leader: an elected leader without an entry would make discovery
unavailable even if every advertised seed were reachable. The optional field
supports deployments without discovery HA; it does not make partial coverage
of eligible leaders sufficient for that guarantee.

The advertised Sentinel address and local listener bind name the same service
and can use the same host and port in a direct deployment. They need not be
textually equal when an explicit proxy is used. This introduces no additional
listener, durable store, or election mechanism.

`SENTINEL SENTINELS` and `num-other-sentinels` derive from the same committed
set: effective, non-retired Meta members with registered Sentinel addresses,
excluding the answering member. Registration does not prove present network
reachability; a temporarily unavailable process remains in the directory
until its membership is removed. Consequently redis-py's
`min_other_sentinels` checks directory size, not a count of live peers.
Authority to answer remains subject to Meta leadership and quorum freshness,
without adding Sentinel peer probes or a separate quorum mechanism.
