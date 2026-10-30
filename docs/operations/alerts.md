<!-- Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 -->

# Prometheus alert runbook

The [monitoring stack](../../deploy/monitoring/README.md) loads user-editable
Prometheus rules. Its optional Alertmanager routes notifications; installing the
stack alone does not configure a production receiver. Use the local drill before
relying on delivery. [Metrics](metrics.md) defines the underlying signals.

The independent [Meta / Raft monitoring runbook](meta-monitoring.md#alert-policy)
documents the `lavik-meta` job's rules and dedicated dashboard. Both jobs share
notification routing and target-down inhibition.

## Policy

These conservative defaults are starting points, not measured workload SLOs.
Adapt recovery allowances to dataset size, storage and operating procedures.
Ratios require a positive denominator. Counters use `rate`/`increase` before any
aggregation, so process restarts do not count as errors. No error ratio is derived
from completed-command counters: early rejections do not share that denominator.
No command-traffic minimum applies to capacity or absolute failure counters.

| Alert | Signal and default condition | Severity / `for` | Overview panel |
|---|---|---|---|
| LavikTargetDown | Prometheus `up=0` | critical / 2m | Target Status |
| LavikServerNotReady | `up=1`, `lavik_server_ready=0` | warning / 10m; critical / 30m | Target Status |
| LavikWorkerAdmissionHigh | worker `(retained + pending + fullsync reserved) / memory limit >90%` | warning / 10m | Worker Retained Admission Utilization |
| LavikMemoryRejectedCommands | `lavik_memory_rejected_commands_total` increases in 5m; rate exceeds 1/s | warning / 1m; critical / 5m | Memory Admission Rejections |
| LavikWritableSpaceLow | per-device `lavik_storage_available_bytes / lavik_storage_capacity_bytes <10%` or `<3%` | warning / 10m; critical / 5m | Writable Space by Device |
| LavikWritableSpaceExhausted | `lavik_storage_available_bytes=0` | critical / 30s | Writable Space by Device |
| LavikBacklogPressure | active backlog, pinned cursors, `backpressured=1`, publisher queue at least 80% full | warning / 5m; critical / 15m | Backlog and Publisher Pressure |
| LavikFullSyncQueuePressure | FULL sessions present, `(queued + admitted) / queue capacity >=80%` | warning / 10m | FULL Publication Queue Pressure |
| LavikReplicationStalled | valid connected flow with pending send/apply/ACK/FULL work and no effective progress | warning at 5m; critical at 8m | Native Replication Without Progress |
| LavikFullSyncFailures | at least 3 failed native FULL attempts in 15m | warning | Native FULL Results and Retries |
| LavikPopulationRecoveryStalled | required population unready, no recovery progress for 15m, not waiting for admission/authority | critical | Native Population and Readiness |
| LavikMetaDisconnected | managed Data `lavik_cluster_control_connected=0` | critical / 5m | Meta Connection State |
| LavikMetaProtocolErrors | control protocol errors increase by at least 3 in 5m | warning / 1m | Meta Exceptions |
| LavikLeaseExpirations | locally detected lease expirations increase in 5m; at least 3 for critical | warning or critical / 1m | Meta Exceptions |
| LavikMetricsMissing | successful scrape lacks readiness, or ready target lacks required inputs | warning / 5m | Target Status |
| LavikNoTargets | no `up{job="lavik"}` series | critical / 2m | Target Status |
| LavikTxBacklogNotDraining | positive, growing Tx record backlog over 10m with no retired blocks and enabled cleaner cooldown at most 300s | warning / 5m | Tx Backlog and Command P99 |
| LavikStorageDurabilityNoProgress | durability pending throughout 5m with no completed writes or fdatasync calls | warning / 5m | Dirty Staging and I/O Progress |
| LavikTxCommitBackpressure | commit queue backpressure events exceed 1/s over 5m | warning / 5m | Commit Queue and Backpressure |

The memory limit in the worker denominator already includes the retained-memory
waterline; do not multiply it by 0.9 again. A process average can hide a full
worker. Writable space excludes the defrag reserve and is not filesystem free
space. FULL queue capacity and occupancy are aggregate per worker, so one busy
session may be hidden by other sessions; this is not a per-replica progress SLO.

Every alert remains firing for two minutes after its condition stops matching.
Rate windows also take time to clear. Scrape and evaluation intervals add bounded
detection delay, and recording groups can be observed one evaluation later.
Tx backlog and durability no-progress rules additionally require a sample at the
start of their observation window and no reset of the relevant progress counters
within that window. They are diagnostics, not proofs of a specific blocked owner
or transaction. Both standalone and Meta-managed Data use the same storage policy.
`resolved` means the condition ended, not that service health was proved: check
current target/readiness/missing-data state before declaring recovery.

TargetDown inhibits other Lavik alerts for the same cluster/job/instance.
Critical inhibits warning only for the same alert and worker/device/signal,
including the same replication side/flow/step;
WritableSpaceExhausted inhibits WritableSpaceLow for that device. Alertmanager
uses its standard 30s initial grouping, 5m group updates and 4h repeat interval.
Inhibited alerts remain visible in Alertmanager. Scheduled maintenance should
use bounded Alertmanager silences, with an owner and expiry.

## Dashboard

The Overview dashboard's **Alerts** section is independent of the replication
panels. A compact Canvas title bar places live count badges to the right of
**Alerts**. Critical and Warning count firing alert instances; Pending
counts conditions still waiting for their `for` duration. Counts and the current
alert table use the selected range's end, so a historical range is a historical
snapshot. The instance selector applies to all these panels; `LavikNoTargets`
remains visible because it has no target instance. The badges show `?` when
their query fields are unavailable; this is distinct from a successful zero.

Each table row is a Prometheus alert instance, including severity and any
worker/device/signal scope. Warning and critical rules can both appear for the
same condition. These are rule states, not a count of delivered notifications:
Alertmanager inhibition and silences do not hide rows. **Active for** measures
from the condition's first active evaluation (`ALERTS_FOR_STATE`), including
pending time and `keep_firing_for`; a missing timestamp displays an unknown
value instead of removing the alert. Table headers support sorting and filtering.

**Alert Activity** gives each alert instance/severity its own lane. Blue is
pending, yellow is warning firing, and red is critical firing; custom severities
use purple when firing. Gaps mean no active sample, not confirmed recovery;
missing telemetry, inactive conditions,
and time outside retention cannot be distinguished from `ALERTS` alone. It is
sampled history, with resolution bounded by evaluation/query intervals, rather
than an audit log of state transitions or notifications. Check Target Status and
the missing-metrics rules before concluding that an empty table or zero count
means healthy; query errors must also be resolved. A neutral gray lane labeled
**No active alert samples** appears only when the entire selected window has no
alert samples. It does not assert recovery.

The table's built-in alert-name mapping supplies readable titles, runbook
anchors, and stable metric-panel IDs without changing the rule metrics. If you
rename or add rules, they still appear under their own names, with links to this
policy and Target Status; edit panel 37's mapping when a specific destination is
needed. Threshold and timing changes do not require a dashboard change.

## Target down

Confirm the configured endpoint from the monitoring host and inspect Prometheus
Targets for the scrape error. Check process status, metrics listener, network
reachability and scrape timeouts. A failed scrape does not identify which failed.
Restore the endpoint, then verify current readiness and any dependent alarms.
The alert resolves after successful scrapes and the recovery window.

## Not ready

The endpoint is reachable but storage startup or runtime health is not ready.
Inspect startup/recovery logs and storage failure diagnostics; compare elapsed
recovery with the expected dataset-specific window. Do not repeatedly restart a
recovering process. If a runtime storage failure is latched, diagnose and repair
it before restarting. Recovery requires `lavik_server_ready=1` and the recovery
window. This does not establish authority or FULL population readiness.

## Worker admission

Inspect the named worker's retained bytes, pending admission and FULL reservations,
then compare `INFO memory`, RSS and active imports/replication. A hot worker can
reject growth while other workers have spare memory. Reduce retained growth or
adjust the configured memory budget after checking host capacity. Do not treat
RSS as the admission numerator. Recovery requires utilization at or below 90%
for the recovery window; also check that command rejections have stopped.

## Memory rejections

Check application OOM responses, the worker admission bars and recent workload or
FULL activity. Reads may still work while growth is rejected. Reduce growth or
provide an appropriately sized retained budget. A single low-traffic rejection
can warn; sustained rejection rate is critical. Recovery requires the 5m counter
window/rate to clear plus the recovery window; counter reset alone is not repair.

## Writable space

Inspect the named device, writable blocks and defrag activity/pending state.
Check whether defrag was paused, whether writes exceed reclaim throughput, and
whether pinned records prevent reclamation. Restore reclamation or follow the
supported [storage provisioning guide](multi-device-storage.md). Filesystem free
bytes do not replace Lavik's internal writable capacity. Recovery requires the
applicable threshold to clear; the exhausted alert requires positive writable
space. Preserve data files while diagnosing capacity failures.

## Replication pressure

Inspect the affected worker's pinned cursors, publisher queue and backlog
backpressure together. Check downstream connectivity, throughput and ACK progress
using existing replication diagnostics. The source may intentionally wait to
preserve coverage; do not disable backpressure blindly. Recovery requires the
combined pressure condition to clear. Backlog floor-to-tail distance is not
replica lag. Backpressure-disabled coverage revocations are diagnostic only in
this release: repeated replica-offline state is not yet exported for a safe rule.

## Full sync pressure

Inspect active FULL sessions, downstream storage/network throughput and queued
versus admitted publication bytes. The queue may temporarily exceed capacity for
one oversized command. Reduce competing write load or repair the bottleneck.
Recovery requires queue pressure to clear or the sessions to finish. A full
queue does not distinguish slow progress from stalled progress. Confirm successful
population activation separately; this rule cannot prove it.

## Native replication stalled

Check `INFO replication` through the existing Admin command entry or a Redis
client. Match `lavik_replication_progressN` side, worker and logical flow with the
alert; INFO adds session, history and peer identity. Source metrics aggregate the
maximum distance and age across comparable downstreams, so INFO identifies the
particular peer. `pending_events` is an event-position difference, never bytes.

For `send`, inspect publisher/backlog pressure and the network; valid fragments
count as send progress even before a large event finishes. For `apply`, inspect
target storage, queued complete events and the consistent applied frontier. For
`ack`, distinguish a blocked target write from a source waiting for an observed
ACK. An online ACK does not prove a crash-durable checkpoint. For `full`, inspect
the current locally observed phase and processed data/partitions. Source cannot
report the target's internal promotion phase.

The server samples progress once per second. The rule fires at 300 or 480 seconds
of known pending work, plus scrape/evaluation delay; there is no second five- or
eight-minute `for` window. Idle work has zero age. Disconnected, waiting and
unavailable states do not establish a replication stall. A reconnect starts new
flow timers; do not subtract LSNs across histories or nodes. Check the independent
population recovery alarm if reconnects repeat without useful recovery work.

## Native FULL failures

`lavik_replication_last_full_*` retains the most recent terminal result for this
process. A new attempt leaves that result intact. Correlate side, session and
history in `native FULL` start/phase/result logs. The failure phase and original
reason remain available even when cleanup also fails; inspect cleanup logs too.
Process restart clears this diagnostic record. Use Prometheus result counters
and duration histograms for history across restarts, and logs for each attempt.

The default rule uses `increase(...{result="failure"}[15m]) >= 3`, separately for
source and target, avoiding double counting one attempt seen at both ends.
Cancellation for local role replacement/shutdown is a separate result. FULL busy
and authority waits are retry reasons, not failed attempts. A remote source can
only diagnose the connection outcome it actually observes. Repair the reported
cause before retrying; respect a failed-stopped node's existing operator recovery
procedure. Source and target completion counts need not be identical when the
connection fails around activation.

## Native population recovery

Compare population ready, catalog ready, ONLINE, loading, failed-stopped and data
readability independently. A committed catalog or a promoted root alone does not
prove completion of all-flow cut/finalization and online activation. A failure in
promotion/finalization must not increment success.

This rule requires an unready population that should recover and 15 minutes
without effective recovery progress. It does not fire merely because a large
FULL has a long total duration. Retry/heartbeat activity alone does not establish
recovery progress. Admission/authority waits have their own diagnostic state and
are excluded from this alarm. Inspect Meta authority and source admission when
waiting persists. On recovery verify current population readiness, online state,
read behavior and the terminal result; an alert resolving on missing telemetry
is not evidence that the population recovered.

## Meta disconnected

Applies only to `lavik_mode="meta-managed"`. Check seeds, Meta leader availability,
control network/TLS/authentication and Data control logs. A short election or
reconnect is allowed. Restore an accepted session and wait for the recovery
window. An accepted connection does not itself prove serving authority; use the
existing cluster-status and Data diagnostics to assess serviceability.

## Meta protocol errors

Inspect both sides' control logs for framing/protocol state errors and check
release compatibility and endpoint configuration. Do not use reconnect count
alone as failure evidence. Recovery requires the error window below threshold
and the recovery window. Error text and session IDs belong in logs, not labels.

## Lease expirations

These are actual locally detected expirations that fenced authority, not denied
lease requests. Correlate Meta connectivity, lease policy, control delays and
host/runtime stalls. Restore timely renewal and assess serviceability through
cluster diagnostics. The rule resolves after the event window clears; that alone
does not prove a new lease was granted. Control-step latency has no universal
1 ms failure threshold.

## Missing metrics

Check the alert's finite `signal` label and raw scrape body. While readiness is
zero, only readiness is required. Once ready, the rules require memory, storage,
replication and (for managed targets) control input families, including missing
members relative to existing worker/device anchor series. Check exporter version,
metric relabel filters and incomplete responses. Do not fill missing values with
zero. Recovery requires the expected series to return, or entry into an explicit
not-ready/down state; inspect that replacement state before declaring health.

The rules do not invent an external inventory: simultaneous disappearance of an
entire worker/device cannot always be detected. Missing per-command series before
any command runs and absent filesystem gauges on raw devices are legitimate.

## No targets

Check file-SD contents, configuration mounts and Prometheus discovery errors.
Restore the intended target set. Removing an individual target from discovery
means it is no longer monitored; these rules cannot detect an accidentally omitted
node without an external desired inventory. Recovery requires target series to
return and the recovery window; inspect each target's actual health.

## Coverage boundaries

The Data and Meta rule sets observe their respective exported metrics; they do
not prove Data serving authority. Command error ratios and latency attribution
remain separate work under #391 and #396. Native replication progress and FULL
population recovery are covered; Redis PSYNC keeps its existing diagnostics.
Native snapshots unavailable for five minutes trigger the `LavikMetricsMissing`
native_replication signal. Tx block counts, age and blocking reasons remain
outside these diagnostics under #379. No node_exporter or centralized log platform is
included. Monitoring does not participate in authorization or replication decisions.

## Tx backlog

`LavikTxBacklogNotDraining` warns when sealed unreclaimed Tx record bytes are
positive and grow over ten minutes, with no retired blocks over that window,
for five more minutes. Online cleaning must be enabled throughout the window
and its cooldown must remain at most 300 seconds. Disabled cleaning and longer
cooldowns suppress this default rule; operators using a longer cooldown must
increase the observation window and its matching `offset`, then adjust the
cooldown bound. These editable defaults allow several ordinary cleaner cycles;
they are not a measured workload capacity limit.

Inspect INFO STATS and the Storage / Transactions panels for cleaner failures,
running state, retired-block rate, pending commit chains and durability progress.
Check existing storage failure logs and outstanding transactions. Transaction
leases or pinned dependencies can legitimately keep a block alive. The byte
value excludes active Tx blocks and extents and cannot identify the number,
age, blocking reason or metadata footprint of retained blocks. It is not an
admission threshold. Do not infer memory usage or change Tx admission from it.

The condition clears when growth stops, reclamation progresses or the rule's
configuration gate no longer matches. After the rate and recovery windows clear,
verify actual backlog drainage and service health. A stable positive backlog
alone does not fire this rule; stopping producers does not prove it has drained.

## Storage durability

`LavikStorageDurabilityNoProgress` warns when durability remains pending over
five minutes and neither completed writes nor completed `fdatasync` calls
advance, for another five minutes. Missing inputs suppress the progress judgment
and trigger the missing-metrics warning instead. A reset of either I/O counter
requires a fresh observation window. Thresholds and timing are editable; unusually
long configured flush intervals or maintenance pauses need matching alert policy.

Inspect dirty staging bytes, active flush runners, pending commit chains, device
space, existing I/O error logs and the write/fdatasync rate panels. Flush runners
are not queued block or I/O counts. Queue depth may already be zero while commits
are in flight. Expiration pause count is current nesting depth, not elapsed pause
time or accumulated events, and is not itself a fault.

I/O counters are process-wide progress proxies. Unrelated writes can mask a
stalled transaction, and completed I/O does not prove a specific commit crossed
its durability boundary. These statistics provide neither per-transaction age
nor a durable acknowledgment barrier during concurrent writes. Runtime storage
failure may withdraw readiness, leaving the existing NotReady alert responsible.

The condition clears when pending drains or I/O progress resumes; the recovery
window still applies. Confirm the relevant workload progresses before declaring
recovery. Do not restart or delete storage solely because this warning fires.

## Tx commit pressure

`LavikTxCommitBackpressure` warns when enqueues encounter their local commit
queue watermark more than once per second over five minutes, sustained for five
minutes. This conservative initial rate is editable for the workload. It counts
enqueues encountering pressure, not wait duration, rejected commands, or Tx block
admission. A counter reset is handled by `rate` and is not a pressure spike.

Compare commit batch/transaction and fence rates, queue depth, pending commit
chains, dirty bytes and I/O progress with command p99. Batch counters advance at
dequeue before durability succeeds. Queue depth is the process-wide queued count;
peak is the process-lifetime maximum and does not drop when drained. The exported
4096 watermark applies independently to each worker: dividing global depth by it
would give a misleading utilization percentage. No current metric attributes
this pressure to an individual worker.

The condition clears after the wait rate subsides and the recovery window ends.
Verify current queues and pending chains drain, not that the historical peak
falls. Cleaner failure counts and pause depth remain supporting diagnostics,
without standalone alert policies.

### Storage diagnostics rollout

The new rules require the upgraded Data exporter. Retain old rules during a
rolling upgrade; once all targets expose the documented diagnostics and both I/O
progress series, validate and reload the new rules. Restore the old rules before
rolling exporters back. `LavikMetricsMissing{signal="storage_diagnostics"}` checks
all 20 diagnostic series plus write/fdatasync operation counters on ready targets.
It covers an unsupported old exporter as well as partially missing telemetry;
neither is silently treated as zero. Before readiness these families are omitted
intentionally. The warning shares Target Status and the missing-metrics runbook.
