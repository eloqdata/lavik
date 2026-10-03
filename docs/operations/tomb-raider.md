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

# Tomb Raider scheduling

Tomb Raider retires tombstones that no surviving disk record needs. Storage
keeps one scheduler running on standalone and Meta-managed nodes, including
replicas. Exactly one of three scheduling modes is configured at a time:

- `TOMBRAIDER OFF` disables future rounds. A round already running completes.
- `TOMBRAIDER INTERVAL <milliseconds>` runs one round after each interval. The
  next interval starts only after the previous round completes or is cancelled.
  If the population is unavailable when an interval expires, one round runs
  when it becomes available again.
- `TOMBRAIDER DAILY <HH:MM[:SS]>` runs once per day at that time in the
  server's local timezone. If a round overlaps a later scheduled time, that
  occurrence is skipped rather than run concurrently. A scheduled time missed
  while the population is unavailable is also skipped.

`TOMBRAIDER ON` restores the schedule active before `OFF`.
`TOMBRAIDER BLOCK-SLEEP <milliseconds>` changes the pause after each scanned
block and takes effect during the current round. Zero disables the pause.
`TOMBRAIDER STATUS` reports the mode, interval, block pause, daily time,
timezone, whether a round is running, and its current eligibility and blocking
reason. Disabled or unavailable schedulers continue checking at most once per
second; they do not start cleanup work.

Runtime changes are not persisted across restarts. Startup uses
`--tomb-raider-interval-ms` and `--tomb-raider-sleep-ms`; an interval of zero
starts in off mode.

## Population availability

Cleanup requires a complete, usable local population. A complete replica can
clean tombstones while disconnected, and neither a role change nor loss of a
Meta Owner lease changes its configured schedule. Active expiration, which
creates logical deletions, retains its separate authority requirements.

Startup recovery and imports hold cleanup until the local population is
complete. Native FULL, Redis FULLRESYNC, and source-less population
initialization cancel and drain any current round before destructive work.
Cleanup resumes automatically only after that population change completes,
including all native flow cuts or all sources in a Redis Cluster import.
Failed or interrupted rebuilds remain blocked. These transitions never turn
the user configuration OFF. Shutdown drains cleanup before freezing storage;
a fatal storage error prevents further rounds.

`FLUSHDB` and `FLUSHALL` may overlap cleanup. Their epoch and index-generation
changes invalidate the current round, which finishes any committed retirement
accounting and stops. The next scheduled round starts with a fresh scan. FLUSH
retains its own detached-index reclamation and SYNC/ASYNC completion semantics.
Concurrent defrag may temporarily delay a sweep while record-block retirement
becomes durable. The sweep then skips that old allocation and continues the
same round, preserving its progress and the configured schedule. A tombstone
already claimed before its older block retired may remain until the next round.

## Inspecting progress

`TOMBRAIDER STATUS` reports `eligible=1` when the configured schedule is enabled
and the local population is usable. This does not mean the next scheduled time
has arrived. `running=1` means a round is currently executing; it can remain one
after OFF while the round finishes. `INFO stats` exposes the same distinction:

| Field | Meaning |
|---|---|
| `tomb_raider_enabled` | The configured scheduling mode is not OFF |
| `tomb_raider_running` | A cleanup round is executing |
| `tomb_raider_eligible` | A round may start when its schedule is due |
| `tomb_raider_blocked_reason` | The current reason a new round cannot start |
| `tomb_raider_rounds` | Successfully completed rounds; cancelled rounds do not count |
| `tomb_raider_reaped` | Tombstones actually removed, including progress before a round is cancelled |
| `tomb_raider_refreshed` | Shielding state actually cleared |

STATUS names the reason `blocked_reason`. Its values are `none`, `user_off`,
`startup`, `population_change`, `incomplete_population`, `storage_failure`, and
`shutdown`. `none` means eligibility is open, even when the scheduler is waiting
for the configured time. Population-related reasons require successful local
recovery or rebuild completion; running `TOMBRAIDER ON` does not bypass them.

To assess memory reclamation, compare indexed tombstone counts and index
allocation with the reaped counter. Replication backlog and other storage
buffers have separate retention rules, so process RSS need not fall with every
completed round.
