<!-- Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 -->

# Prometheus alert runbook

The [monitoring stack](../../deploy/monitoring/README.md) loads user-editable
Prometheus rules. Its optional Alertmanager routes notifications; installing the
stack alone does not configure a production receiver. Use the local drill before
relying on delivery. [Metrics](metrics.md) defines the underlying signals.

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
| LavikMetaDisconnected | managed Data `lavik_cluster_control_connected=0` | critical / 5m | Meta Connection State |
| LavikMetaProtocolErrors | control protocol errors increase by at least 3 in 5m | warning / 1m | Meta Exceptions |
| LavikLeaseExpirations | locally detected lease expirations increase in 5m; at least 3 for critical | warning or critical / 1m | Meta Exceptions |
| LavikMetricsMissing | successful scrape lacks readiness, or ready target lacks required inputs | warning / 5m | Target Status |
| LavikNoTargets | no `up{job="lavik"}` series | critical / 2m | Target Status |

The memory limit in the worker denominator already includes the retained-memory
waterline; do not multiply it by 0.9 again. A process average can hide a full
worker. Writable space excludes the defrag reserve and is not filesystem free
space. FULL queue capacity and occupancy are aggregate per worker, so one busy
session may be hidden by other sessions; this is not a per-replica progress SLO.

Every alert remains firing for two minutes after its condition stops matching.
Rate windows also take time to clear. Scrape and evaluation intervals add bounded
detection delay, and recording groups can be observed one evaluation later.
`resolved` means the condition ended, not that service health was proved: check
current target/readiness/missing-data state before declaring recovery.

TargetDown inhibits other Lavik alerts for the same cluster/job/instance.
Critical inhibits warning only for the same alert and worker/device/signal;
WritableSpaceExhausted inhibits WritableSpaceLow for that device. Alertmanager
uses its standard 30s initial grouping, 5m group updates and 4h repeat interval.
Inhibited alerts remain visible in Alertmanager. Scheduled maintenance should
use bounded Alertmanager silences, with an owner and expiry.

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

Rules observe existing Data metrics. Meta/Raft member health, serving-authority
state, command error ratios, detailed storage progress, replica stall detection,
FULL failure history/population loading and latency attribution remain separate
work under #391 and #393–#396. No node_exporter or centralized log platform is
included. Monitoring does not participate in authorization or replication decisions.
