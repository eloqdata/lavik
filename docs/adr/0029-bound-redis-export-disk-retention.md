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

# Bound Redis export resources and session lifetime

## Status

Accepted for [#122](https://github.com/eloqdata/lavik/issues/122); implementation
and end-to-end acceptance remain incomplete.

## Decision

An emergency export session belongs to one currently authorized Primary and
its population/history identity. PSYNC admission uses the existing current,
servable Primary eligibility checks, including readiness and valid authority;
the role label alone is insufficient. Controlled Pause adds no separate
rejection rule: a node that remains an eligible Primary may accept an export.
During Controlled Pause, an existing session may continue through the stable
source frontier while authority remains valid;
the failover does not wait for RedisShake to catch up. If the pause is aborted
without changing that identity or losing authority, the session may continue
as writes resume.

For both Controlled and Uncontrolled Failover, local authority revocation or
lease expiry cancels the export, including when the process still reports the
same primary role. Owner or population/history replacement also cancels it.
Cancellation closes the transport and drains the session's snapshot, ACK and
backlog work without blocking indefinitely on the consumer. It does not
promise immediate consumer-side failure detection across a network blackhole.

Do not carry Redis offsets across connections or Owner changes, discover a
successor on behalf of the consumer, or resume an interrupted export. After
failure, the operator selects the new valid Primary and starts a fresh full
export into a clean dedicated target or a new isolated target. Partial output
from the failed attempt is not a valid baseline: in an Uncontrolled Failover,
it may contain an old-primary tail absent from the successor. This boundary
matches the fixed RedisShake version's lack of resumable transfer and keeps
export recovery out of the Meta failover state machine.

Retain the temporary disk backlog introduced by
[#197](https://github.com/eloqdata/lavik/pull/197): while an export sends its
RDB, it spools concurrent incremental commands on Lavik's data devices, then
replays them before streaming the live tail. Configure this disposable data
with an independent `redis-export-disk-backlog-size` quota, defaulting to
1 GiB. Keep `repl-backlog-size` as the existing in-memory history/backlog
capacity; the two settings are independent limits, not a combined budget or
a memory-to-disk ratio. The disk quota bounds allocated temporary blocks for
the single allowed export session, not the RDB size. Disk staging buffers
still require ordinary retained-memory admission. Exhausting the quota or
available capacity
terminates the export session rather than requiring unbounded retention or
making export completion a prerequisite for business writes. Temporary blocks
are reclaimed on session cleanup or restart; they establish no resumable
replication history or recovery authority.

Exporter memory retention follows `replication-backlog-backpressure`, whose
default is `yes`, through the existing worker policy and wakeups. It may delay
business writes while a slow exporter holds the oldest needed event. Runtime
`no` permits eviction and terminates an exporter whose coverage is lost;
switching back to `yes` cannot restore missing coverage. This replaces the
prior proposal to always disconnect slow exporters. Controlled Pause does not
cancel a healthy session, but the pause drain cancels an exporter whose pins
block publication, releasing its pins before joining tasks. Native consumers'
retention and source history are unaffected by exporter cleanup.

The initial compatibility baseline is unmodified RedisShake v4.6.2
(`f20f28e6f2679e71a213904d2c74ceb521e19551`) writing to Redis 7.2.14.
Accept the TTL drift inherent in that tool's baseline conversion from absolute
RDB expiry times to relative RESTORE/PEXPIRE TTLs, as with Redis-to-Redis use
of the same tool. Do not require an operator-selected tolerance or additional
expiry reconciliation as a cutover condition. Source expiry encoding and
incremental expiry behavior must still be correct; this accepts the tool's
conversion behavior, not loss of expiry metadata.

Native LVPSYNC full-sync use of shared temporary disk storage is tracked
separately in [#205](https://github.com/eloqdata/lavik/issues/205). This decision
does not add disk buffering to native replication or define its future quota
policy.
