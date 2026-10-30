<!-- Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 -->
# Meta / Raft monitoring and fault rehearsal

Use the **Lavik Meta / Raft** dashboard and `lavik-meta` Prometheus job. Start
with target reachability, lifecycle and source availability before interpreting
Raft or application values. A leader ID is a hint; local usable Raft authority
and Data-control eligibility are separate observations. See the
[metric contract](metrics.md#meta--raft-metrics) and
[deployment instructions](../../deploy/monitoring/README.md).

The dashboard shows one cluster at a time. Its summary cards cover every
configured target in that cluster; **Node details** narrows the table and
charts only. The node table keeps down targets visible and marks unavailable
runtime fields as unknown. It supports sorting, column filtering and pagination.
Cluster summaries require successful scrapes and complete coverage of the
relevant source, so a missing member cannot turn a partial zero into a healthy
cluster result. This coverage is about monitoring targets, not Raft membership
or proof of quorum.

The main charts show caller outcomes and latency, pending proposals, local
commit-to-apply lag, leader-observed peer lag and heartbeat evidence age. Expand
the diagnostic rows for persistence/snapshot latency and failures, transport
errors and queue pressure, Data-control sessions, or exporter failures. Terms,
leader IDs, raw log indices and vote-response counts are exported for targeted
queries instead of occupying routine health panels. Vote/pre-vote responses
are not election or leadership-change events; the dashboard does not present
them as election counts. Heartbeat evidence age is not peer RTT, and complete
proposal latency is not isolated Raft commit latency.

## Alert policy

The monitoring stack installs `meta-recording.yml` and `meta-alerts.yml` alongside
the Data rules. Both jobs use the existing optional Alertmanager routes and
inhibition. Configure receivers using the [notification runbook](alerts.md);
installing rules alone does not deliver production notifications. The independent
Meta dashboard adds fixed-size cluster alert counts and a paginated current-alert
table with peer/stage scope, runbook links and metric-panel links. **Node details**
does not hide cluster alerts. These are Prometheus rule states, including pending
and inhibited/silenced alerts, not delivery acknowledgements. No active samples
does not prove health; first check targets and source coverage.

| Alert | Default condition | Severity / `for` |
|---|---|---|
| LavikTargetDown (`job="lavik-meta"`) | Scrape `up=0` | critical / 2m |
| LavikMetaNotRunning | Reachable but running phase is zero | warning / 10m |
| LavikMetaTelemetryUnavailable | Lifecycle absent, or running with missing health inputs/unavailable sources/stale protocol publication | warning / 5m |
| LavikMetaNoLeader | All configured targets observed; sum of usable Raft authority is zero | critical / 2m |
| LavikMetaControlUnavailable | Complete Raft and Data-control observations; no eligible node | critical / 5m |
| LavikMetaVoterUnavailable | Usable leader lacks fresh heartbeat evidence for a current voter | warning / 2m |
| LavikMetaApplyLag | Local commit exceeds actual applied by more than 100 entries | warning / 5m |
| LavikMetaReplicationLag | Usable leader observes a current voter Match more than 100 entries behind commit | warning / 5m |
| LavikMetaWALFailure | A stage's WAL error counter increases in 5m | critical / immediate |
| LavikMetaSnapshotFailures | Snapshot consecutive-failure Gauge remains positive | warning / 10m |
| LavikMetaGCFailures | Reclamation consecutive-failure Gauge remains positive | warning / 10m |

Thresholds and durations are editable starting points, not measured SLOs. All
alerts retain firing state for two minutes after the condition clears. Counter
windows, scrape and rule evaluation intervals add delay. Recovery requires the
specific condition to clear and this hold to expire; resolved notifications do
not prove current health. TargetDown inhibits other alerts on the same
cluster/job/instance. Cluster alerts have no instance and remain cluster-scoped.

Raft health rules require a running, successfully scraped target with the Raft
source available and publication age below its exported leadership validity
window. Cluster absence rules additionally require coverage of every configured
target. Missing/down/recovering targets therefore produce the corresponding
target, telemetry or recovery alarm instead of a false no-leader conclusion.
This is target coverage, not proof that discovery contains all Raft members.
The optional Meta job has no absent-target alarm: old releases and Data-only
deployments legitimately have no Meta exporters. Retain failed targets in
discovery; external inventory is needed to detect an accidentally omitted node.

Peer alarms use current voters, so learners catching up are excluded. They
require the observing node's usable authority; a retired leader's old peer state
cannot keep a new condition active. Peer-applied values may legitimately be
absent and are never replaced by zero. These rules do not page on term, leader
hints, vote responses or zero connections on on-demand lanes. Positive snapshot
or GC streaks mean no subsequent success, not a known retry frequency.

The queries below are diagnostic companions to the installed rules; apply their
coverage/freshness guards before using them as alert policy.

## Target down

Check process status, Prometheus Targets and the private metrics listener from
the monitoring host. A scrape failure cannot distinguish process exit, network
isolation, timeout or collection failure. Restore successful scrapes, then verify
phase, sources and usable authority. A fail-stop WAL error may never reach a
scrape, so target-down coverage must remain enabled.

## Telemetry and recovery

During startup/recovery, unavailable runtime sources and omitted runtime fields
are expected. Investigate when the ten-minute recovery allowance expires; adapt
it to snapshot size and storage. Avoid repeatedly restarting a progressing
recovery. Once running, all three source flags, authority, control eligibility,
commit/applied indices and a fresh protocol publication are required. Inspect
exporter versions, relabel filters and owner progress when these disappear.
The guard does not inventory every peer/stage family. Restore the missing source
or publication; do not fill gaps with healthy zeroes.

## No usable leader

```promql
sum by (cluster) (lavik_meta_raft_leader_authority{job="lavik-meta"}) == 0
```

Require current successful scrapes and available Raft sources; missing series
do not mean zero authority. Inspect one-hot `lavik_meta_phase`,
`lavik_meta_source_available`, protocol `lavik_meta_raft_role`, term and
`lavik_meta_raft_status_age_seconds`. A brief election gap differs from a
persistent loss of quorum. Data-control can remain ineligible while the usable
Raft leader finishes its membership/identity readiness gates.

```promql
up{job="lavik-meta"} == 0
```

This query covers process exit, unavailable endpoints, collection failure and
network timeouts. It is essential after fail-stop because a WAL failure counter
may never reach a subsequent scrape. Discovery must retain stopped targets to
keep this evidence; removing a target is not proof of recovery.

## Reduced voter redundancy

```promql
(
  1 + sum by (cluster, instance) (
    lavik_meta_raft_peer_fresh{job="lavik-meta"}
    * on (cluster, instance, peer_id)
      lavik_meta_raft_member{job="lavik-meta",kind="voter"}
  )
) < on (cluster, instance)
count by (cluster, instance) (
  lavik_meta_raft_member{job="lavik-meta",kind="voter"}
)
```

Evaluate only on a current usable leader with a recent status publication.
The added one represents that leader itself. This query describes observed
voter redundancy, not a replacement for Raft's joint-configuration quorum
rules. Learners are excluded. In a three-voter cluster a single isolated
follower can reduce redundancy while writes continue. Inspect heartbeat age,
control-lane errors and queue rejection. Zero log/snapshot connections is
normal when those lanes have no work.

## Persistent application or replication lag

```promql
min_over_time((
  clamp_min(lavik_meta_raft_commit_index{job="lavik-meta"}
    - lavik_meta_raft_applied_index{job="lavik-meta"}, 0)
)[5m:]) > 100
```

For peer application, use valid heartbeat evidence; never `or vector(0)` to
fill absent applied indices:

```promql
clamp_min(
  lavik_meta_raft_commit_index{job="lavik-meta"}
  - on (cluster, instance) group_right
    lavik_meta_raft_peer_applied_index{job="lavik-meta"}, 0
)
```

A slow apply executor can leave durable and committed indices advancing while
actual applied lags. A slow WAL can increase pending durability bytes and
`operation_age_seconds{stage="persist"}` while an established term continues
heartbeats. Durable is a current-branch prefix and can regress on suffix
replacement; do not assume durable, committed and applied always move together.
A follower's replication Match and actual applied are different observations.

## Persistence and snapshot failures

```promql
sum by (cluster, instance, stage) (
  increase(lavik_meta_raft_wal_errors_total{job="lavik-meta"}[5m])
) > 0
```

```promql
lavik_meta_raft_snapshot_consecutive_failures{job="lavik-meta"} > 0
```

```promql
increase(lavik_meta_raft_gc_errors_total{job="lavik-meta"}[5m]) > 0
```

Successful retries clear consecutive failure Gauges; cumulative Counters keep
the failure history. Use the `capture`, `install`, `prepare`, `publish` stage
outcomes to distinguish C++ state work, file preparation and durable
publication. A prepared snapshot is not a published recovery root. Upstream
snapshot Histogram counts can omit failures, and upstream write/fsync timing
must not be added to complete executor timing.

Proposal `outcome_unknown` means the caller must reconcile using the request's
idempotency key. A timeout followed by actual commit keeps its original unknown
outcome; it does not produce a second success sample.

## Rehearsal matrix

Use a disposable three-member cluster with private loopback ports and temporary
data directories. Preserve logs and metrics with monotonic test timestamps;
never inject storage faults or lose quorum on a production cluster. The
repository's `tests/meta_integration/harness.py` provides per-destination
proxies, process lifecycle, writes and snapshot requests. Go executor tests use
barriers at real persistence/application boundaries; no production sleep flags
are needed.

| Exercise | Expected diagnostic evidence |
|---|---|
| Normal writes | One usable leader; durable/commit/actual applied progress; applied-success samples; fresh voter peers |
| Stop the leader | Its target goes down; survivors elect a later term; authority and Data-control readiness recover |
| Block one follower destination | Peer freshness expires; valid peer-applied disappears; remaining majority can continue; reconnect restores evidence |
| Stop two of three voters | Remaining node loses usable authority; no successful proposals; unavailable targets remain visible |
| Add a learner and promote it | Current member kind, replication Match/Next and actual applied show catch-up; voter redundancy excludes it until promotion |
| Block WAL or C++ apply independently | Persistent stage age or commit/applied gap grows; scrape remains responsive without waiting for that executor |
| Capture/prepare failure then retry | Error stage count and snapshot cumulative failure increase; successful retry clears only the consecutive Gauge |
| Snapshot install and recovery | Install timing/results recorded; recovery phase with unavailable runtime fields until publication; installed snapshot/applied indices agree |
| Concurrent/slow HTTP and shutdown | Bounded admission, 503 or deadline closure; owner progress continues; shutdown cancels connections before destroying sources |

Illustrative three-voter sequence: with member 1 leading, peer 3 starts with
`peer_fresh=1` and a valid applied index. After its heartbeat proof expires,
member 1 reports `peer_fresh=0`, omits peer 3's applied series, and still has
usable authority through member 2. Losing member 2 then revokes authority;
readiness is no longer inferred from the old leader hint. Restoring connectivity
requires fresh proof and, after term retirement, a new election.

## Performance acceptance

The reproducible driver uses fresh three-member clusters and writes raw JSONL
plus latency samples outside the source tree:

```sh
python3 tests/meta_integration/bench_metrics.py \
  --baseline /path/to/baseline/lavik-meta \
  --candidate /path/to/candidate/lavik-meta \
  --output /path/to/results.jsonl
```

A logical operation is an Admin `submitop`/`completeop` pair (two proposals).
The driver reports Meta, client and driver CPU separately, plus whole-host CPU
to expose interference from other work. Retain rejected or interrupted runs;
do not count measurements affected by unrelated builds as acceptance evidence.

Compare Release builds on the same host and storage with identical workload,
member topology and non-default settings. Alternate baseline/candidate order
for at least three pairs, covering endpoint disabled, enabled without scrapes,
5-second scrapes and 32 concurrent scrape clients. Measure throughput,
p99/p99.9, CPU per operation, RSS and shutdown time under light and saturated
load. Record raw samples and exact revisions. Normal collection targets no more
than 3% throughput loss and 5% p99 growth; noisy or conflicting samples require
further paired measurement. A slow external load generator is not evidence
that request-path overhead is absent.
