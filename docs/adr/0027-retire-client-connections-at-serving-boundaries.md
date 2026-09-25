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

# Retire client connections at serving boundaries

## Status

Accepted and implemented for [#100](https://github.com/eloqdata/lavik/issues/100).

When a Data Node loses Owner authority or changes serving role, retire existing
ordinary client connections, including idle, blocking and Pub/Sub connections.
Diagnostics remain accessible through newly established connections; existing
management connections on the client endpoint receive no diagnostic exemption.
A connection can alternate between diagnostic and data commands, so preserving
its TCP session would require a separate session contract with no existing
Redis identity. Meta control, native replication and Recovery Donor channels
retain their own lifecycles and are not targets of this client cleanup.

ROLE and INFO replication's role field retain Redis semantics: they report the
node's actual local replication role, independently of its serving authority.
A fenced Owner can therefore still report master until its replication role
changes; a configured replica reports slave even while its upstream is
disconnected. Lease loss alone neither changes ROLE into a MASTERDOWN error
nor fabricates a replica role. Separate diagnostic fields expose authority and
serving status. This preserves the standard role contract instead of making
ROLE a readiness probe; clients must not interpret master as proof that a data
request will be admitted. Data admission enforces activation and finite lease
conditions, and connection retirement triggers client rediscovery. Renewed
authority at the same address and within the same boot must not revive a
connection retired under the previous authority.

Each worker closes all ordinary connections present when it processes the
cleanup notification. A recent reconnect may be closed by that sweep; no
connection generation or ID cutoff protects it. Connections established after
the sweep retain ordinary Redis error semantics.
A data command rejected before mutation returns the applicable admission error;
that rejection alone does not close the connection. Diagnostic commands remain
available. These new connections never held the revoked authority and can
survive same-node reauthorization. Actual later role reconfiguration must still
retire the connections then present, including those accepted after the first
fence. This does not weaken the existing disconnect rule for uncertain writes.

Retirement follows Redis CLIENT KILL's distinction between safe execution
lifetime and reply delivery. Redis serializes ordinary command execution with
CLIENT KILL, but frees a targeted client's pending output without waiting for
the client to receive it; blocked and subscribed clients do not wait for a
future result. Lavik must preserve required internal mutation drain and safe
request cleanup across its concurrent workers, without extending expired
serving authority or waiting for a slow client to consume a complete reply.
Disconnecting does not roll back a completed mutation, and a missing complete
reply does not prove that a mutation failed. Existing uncertain-outcome fencing
continues to close the connection without a fabricated success or retryable
failure.

This is a serving-boundary action, not a reason to disconnect a legal replica
merely because its Meta session or replication link is temporarily unavailable.
The complete-population stale-read contract in ADR 0022 remains in force.

Acceptance includes Pub/Sub recovery with unmodified redis-py 8.1.0 and
go-redis 9.22.0. The application retains its original Pub/Sub object while the
client rediscovers the service, reconnects and restores subscriptions; the test
must observe newly published messages after recovery, not merely a closed old
socket. Messages during the interruption may be lost and are not replayed.
New SUBSCRIBE and PSUBSCRIBE commands remain permitted during the fenced
interval, following Redis subscription semantics. A client using temporarily
stale discovery information can therefore reconnect and subscribe to the old
Owner after the initial fence. The old node's subsequent role reconfiguration
retires that connection again and triggers another discovery attempt. Recovery
acceptance includes completion of that reconfiguration, reachable discovery
and a serviceable new Owner. New-Owner readiness alone does not guarantee
migration of a subscriber attached to an old node that remains isolated from
the control plane. This accepts Redis's reconfiguration-dependent recovery
boundary instead of introducing a serving-authority gate for subscriptions.

References: [Redis CLIENT KILL](https://redis.io/docs/latest/commands/client-kill/),
[Redis ROLE](https://redis.io/docs/latest/commands/role/),
[Redis connection cleanup](https://github.com/redis/redis/blob/8.2/src/networking.c),
[Redis Sentinel reconfiguration](https://github.com/redis/redis/blob/8.2/src/sentinel.c).
