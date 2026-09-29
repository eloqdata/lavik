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

# Keep discovery transport out of promotion eligibility

## Status

Implemented in [#109](https://github.com/eloqdata/lavik/issues/109).
This decision extends the publication
boundary in [ADR 0024](0024-publish-discovery-from-committed-authority.md).

## Publication and authority

The Discovery Entry's explicit Data transport selection constrains which
address it may publish; it does not constrain Meta membership admission,
Candidate eligibility, or promotion. Under TLS selection, an Owner without a
valid advertised TLS endpoint has no publishable Primary address. Discovery
uses the existing withdrawn-publication responses, omits replicas without a
valid endpoint for that transport, and never substitutes a plaintext address.
Invalid local TLS configuration remains a configuration error, not permission
to start a plaintext listener instead.

This deliberately permits Meta to complete a failover while TLS discovery has
no publishable Primary. Requiring a TLS endpoint on every potential Owner at
membership admission would instead couple application connection settings to
the shared HA model. A deployment promising client recovery must provision
valid, reachable endpoints and certificates for every node that may become
Owner, and validate discovery and reconnection after failover.

Redis Sentinel selects candidates using observed connection and replication
state, without a separate requirement that every member declare an additional
TLS address. TLS failures can affect those observations because Sentinel itself
connects to the monitored endpoint. Lavik's Meta observations arrive through a
different control channel, so control-plane health cannot establish application
TLS reachability. Preserve Meta's existing authority and recovery rules instead
of treating discovery configuration as a new source of promotion eligibility.

## Local publication configuration

Data transport selection and hostname publication settings belong to each Meta
process's local startup configuration. Advertised endpoint registrations remain
in Committed State, but these publication settings do not introduce a committed
Policy or a cluster-wide configuration-matching admission rule. Deployments must
configure the settings consistently across all potential Meta leaders, just as
their Sentinel credentials and connection requirements must be compatible with
the same clients.

The alternative was to persist a common publication policy and reject discovery
service from a process whose local configuration disagreed with it. That would
detect configuration drift, but add a replicated configuration lifecycle for
an application connection concern. The selected local model accepts that a
misconfigured replacement Meta leader can publish an incompatible address and
interrupt client discovery or reconnection. It does not authorize a TLS client
to fall back to plaintext.

Deployment examples supply consistent local settings. Acceptance switches
leadership through every Meta member and checks discovery replies, notification
addresses, and client reconnection under those settings. This validates the
deployment contract without turning publication configuration into another
source of Meta authority.

Reference: [Redis 7.2.14 candidate selection](https://github.com/redis/redis/blob/7.2.14/src/sentinel.c#L5062-L5104).
