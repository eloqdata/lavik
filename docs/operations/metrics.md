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

# Prometheus metrics

Enable the HTTP endpoint with a separate listen port:

```sh
./lavik --port 6379 --metrics-port 9100 --data-file lavik.data
curl http://127.0.0.1:9100/metrics
```

`--metrics-port=0` disables the HTTP listener. The endpoint is `GET /metrics`
and uses the Prometheus text exposition format.

A ready-to-run Prometheus and Grafana deployment, including a provisioned
dashboard and multi-node discovery, is available in
[`deploy/monitoring`](../../deploy/monitoring/README.md).

`lavik_cluster_control_wait_seconds` measures completed Meta control steps,
with `stage="observation"` for data-state collection and
`stage="lease_installation"` for authority installation. It includes
cross-worker waits and is exported as a histogram in seconds. These timings
are diagnostics, not a 1 ms scheduling guarantee or an automatic warning
threshold. Compare their distribution with the effective heartbeat interval
and lease duration; use `lavik_cluster_control_lease_expirations_total` to
identify actual local lease failures.

The Grafana overview includes a collapsed **Meta Control Plane** row for these step
timings, connection state, lease decisions, expirations, reconnects, and
directive results. The metrics HTTP service runs on the data workers; the
last runtime worker remains dedicated to Meta control tasks.

## Memory limit

`--max-memory` (also accepted as `--maxmemory`) sets the process memory limit
and accepts byte-size suffixes such as `8GiB`. A value of zero, the default,
uses 80% of the smaller of the host memory capacity and the process cgroup
limit. Commands that may grow retained memory return a Redis-compatible OOM
error when their worker's share would cross 90% of the limit. The remaining 10%
is passive headroom, not a separately admitted temporary-memory pool. The
default 5% client-request quota can occupy half of that space. Bounded command,
IO, and protocol scratch may briefly push process memory above `--max-memory`,
so this setting is a retained-memory waterline rather than a strict
instantaneous RSS ceiling.

`--maxmemory-clients` bounds ordinary client request and queued `MULTI` bytes.
It accepts either a percentage such as `5%` or an absolute size such as
`512mb`; the default is `5%`, and `0` disables this client-specific limit.
Every nonzero value has a 128 KiB effective minimum so an accidentally tiny
setting does not make administrative commands unusable. The process-wide value
is divided into fixed worker shares and checked once after each socket read.

`--client-query-buffer-limit` is the independent per-connection hard limit for
an incomplete command. It accepts an absolute Redis-style memory size, defaults
to `1gb`, and must be at least `1mb`. The same
`client-query-buffer-limit 1gb` directive is accepted in Redis-style
configuration files. The connection closes if incremental parsing crosses the
limit; completed commands continue to count against `maxmemory-clients` until
execution or transaction teardown releases them. `CONFIG GET/SET
client-query-buffer-limit` reads or changes the live process-wide value;
existing connections apply a change before their next parsing round.

The process budget is split evenly across workers and unused capacity is not
borrowed across workers. SET/MSET/INCR performs one worker-local retained-growth
check. Only a new record-index arena page or bucket allocation enters a slow
path that temporarily reserves exact headroom. Full-sync coverage, replication
backlog ownership, fixed source-publisher/full-sync FIFO budgets, and
multi-frame replica staging also share the 90% retained budget because they
can accumulate beyond one request. Publisher admission normally stays within
the fixed queue budget. One command larger than that waterline may enter only
as the exclusive item; its surplus is not retained-accounted and can remain
allocated while publication is backpressured. The protocol limit bounds this
exception, but neither the passive 10% nor `maxmemory-clients` guarantees that
it fits. Increasing or disabling the default client quota reduces the assumed
headroom further. Operators requiring the publisher copy to remain inside the
retained boundary should configure
`replication-publish-queue-mb-per-worker` at least as large as their largest
accepted replicated command.
Recovery bounds its avoidable routing and live-accounting allocations with a
64 MiB process-wide batch target divided across scan workers. A single record
or external-value manifest is indivisible and may exceed one worker's share,
but that batch is applied before the worker retains another record.
Ordinary temporary allocations do not reserve headroom:
the official mimalloc global new/delete override performs no Lavik
accounting. RSS and allocator diagnostics remain outside command execution.

## Business metrics

- `lavik_commands_total`: completed Redis commands. QPS is
  `rate(lavik_commands_total[1m])`.
- `lavik_command_calls_total{command}`: completed commands by command name.
- `lavik_command_duration_seconds`: command execution histogram, from
  dispatch through reply construction; socket response writes are excluded.
- `lavik_connections`: all current TCP connections, including Redis clients,
  Prometheus scrapes, replication, and outgoing Meta control streams. The
  process counter is read atomically without scheduling work on the control
  worker. `lavik_cluster_control_connected` separately reports whether Meta
  has accepted the control session.
- `lavik_connected_clients`: current Redis client connections. This is always
  less than or equal to `lavik_connections`.

For example, per-command QPS and aggregate p99 latency are:

```promql
sum by (command) (rate(lavik_command_calls_total[1m]))
histogram_quantile(
  0.99,
  sum by (le) (rate(lavik_command_duration_seconds_bucket[5m]))
)
```

## Cluster control metrics

Meta-managed Data nodes export process-level control health without node,
group, assignment, directive, or operation identifiers as labels:

- `lavik_cluster_control_connected`: 1 while the final runtime worker (the Meta
  control worker) owns an accepted Meta session, otherwise 0.
- `lavik_cluster_control_reconnects_total`: reconnect rounds after the first
  attempt.
- `lavik_cluster_control_protocol_errors_total`: sessions closed for invalid
  framing or protocol state.
- `lavik_cluster_control_full_states_applied_total`: complete desired-state
  projections installed atomically.
- `lavik_cluster_control_lease_decisions_total{decision="granted|denied"}`:
  finite-authority outcomes returned by Meta.
- `lavik_cluster_control_lease_expirations_total`: locally detected lease
  expirations that fenced authority.
- `lavik_cluster_control_directive_results_total{result="success|failure"}`:
  terminal Data-side directive execution outcomes.

Alert on a Meta-managed node remaining disconnected, repeated protocol errors,
or lease expirations. A reconnect count alone is diagnostic: leader changes and
process restarts legitimately increment it.

## Storage metrics

The active defrag tuning values are exported alongside the work gauges so
latency graphs can be correlated with runtime A/B changes:

- `lavik_storage_defrag_max_active_per_device`
- `lavik_storage_defrag_block_sleep_seconds`
- `lavik_storage_defrag_record_sleep_seconds`
- `lavik_storage_defrag_paused`

They can be changed without restarting Lavik:

```text
DEFRAG PAUSE
DEFRAG RESUME
DEFRAG MAX-ACTIVE 1
DEFRAG BLOCK-SLEEP-MS 100
DEFRAG RECORD-SLEEP-US 10
DEFRAG STATUS
```

Reducing concurrency does not cancel relocations already in flight. A block
sleep change applies after the current block; record sleep is loaded at every
record checkpoint. Zero disables either sleep. `PAUSE` prevents new relocation
jobs from starting but lets an already active job finish; queued candidates are
woken by `RESUME`. Pausing defrag indefinitely can eventually prevent writes
from reclaiming space on a nearly full device.

- `lavik_storage_defrag_runs_total{result}`: completed defrag attempts,
  classified as `success`, `resource_exhausted`, or `error`.
- `lavik_storage_defrag_active`: defrag jobs currently running.
- `lavik_storage_defrag_pending`: workers waiting to start a defrag job.
- `lavik_storage_capacity_bytes{device,path}`: usable data capacity.
- `lavik_storage_available_bytes{device,path}`: space available to normal
  writes after preserving the defrag reserve. This is the capacity metric to
  alert on when Lavik is close to rejecting writes.
- `lavik_filesystem_available_bytes{device,path}`: free space reported by
  the filesystem containing a regular data file. It is omitted for raw block
  devices. A preallocated data file can leave this value unchanged while
  `lavik_storage_available_bytes` falls.

Defrag activity can be compared with command latency using:

```promql
sum by (result) (rate(lavik_storage_defrag_runs_total[5m]))
```

## Memory metrics

- `lavik_memory_current_bytes`: cached explicitly retained bytes used for
  limit enforcement.
- `lavik_memory_used_bytes`: explicitly retained bytes. This is not total
  process heap usage; compare RSS and mimalloc diagnostics for that view.
- `lavik_memory_rss_bytes`: resident process memory, refreshed when metrics
  or `INFO memory` is requested.
- `lavik_memory_committed_bytes`: pages committed by mimalloc; diagnostic
  only and valid without per-allocation mimalloc statistics.
- `lavik_memory_reserved_bytes`: virtual address space reserved by mimalloc;
  diagnostic only.
- `lavik_memory_max_bytes`: configured process memory limit.
- `lavik_client_request_buffer_limit_bytes`: effective client request-buffer
  limit after resolving percentages and the nonzero 128 KiB floor.
- `lavik_client_buffered_request_bytes`: wire bytes currently retained by
  ordinary parsers, ready command batches, and queued transactions.
- `lavik_fullsync_reserved_memory_bytes`: reusable retained-memory headroom
  promised to active full-sync coverage maps.
- `lavik_memory_admission_pending_bytes`: short-lived worker-local permits
  held while retained page, bucket, replication-owner, or replica-staging
  ownership is constructed.
- `lavik_memory_rejected_commands_total`: commands rejected by the limit.
- `lavik_worker_retained_memory_bytes{worker}`: retained bytes charged to
  one worker's admission share, including its deterministic share of retained
  allocations created outside a bound worker.
- `lavik_worker_memory_admission_pending_bytes{worker}`: that worker's
  short-lived allocation permits.
- `lavik_worker_fullsync_reserved_memory_bytes{worker}`: that worker's
  reusable full-sync coverage reservation.
- `lavik_worker_client_buffered_request_bytes{worker}`: request bytes charged
  to that worker's independent client-buffer quota.
- `lavik_worker_memory_limit_bytes{worker}`: the worker's fixed share of the
  90% retained-memory waterline.

The provisioned Grafana **Retained Admission Utilization** gauge approximates
the process-wide admission decision as:

```promql
100 * (
  lavik_memory_current_bytes
  + lavik_fullsync_reserved_memory_bytes
  + lavik_memory_admission_pending_bytes
) / (0.9 * lavik_memory_max_bytes)
```

Admission is enforced per worker, so a single worker can still reject growth
before this process-wide aggregate reaches 100%. Oversized publisher surplus,
client buffers, temporary heap usage, and RSS are intentionally absent from
this retained-admission gauge.

The provisioned Grafana dashboard keeps the per-worker retained-admission bars
in a collapsed **Worker Memory** row. Expanding it shows
`100 * (retained + pending + full-sync reserved) / worker limit` for every
selected instance and worker without adding any command-path accounting.

The same values are available through Redis `INFO memory`, including
`used_memory`, `used_memory_rss`, `maxmemory`, and
`oom_rejected_commands`.

Worker 0 sums the cache-line-separated worker retained counters every 100 ms.
Release builds use mimalloc's official global C++ override without Lavik
hooks. Explicit retained allocators query `mi_usable_size` only on their much
rarer allocation/free paths. The hot command path reads its own shard and adds
a conservative retained-size estimate, so it performs no allocator aggregation
or `/proc` I/O. RSS is diagnostic only and is sampled by the explicit
metrics/INFO request.

## Replication and Function catalog

`INFO replication` exposes `lavik_replication_group_id`, the current boot
and replica-incarnation IDs, the local/upstream history, and
`lavik_function_catalog_generation` plus its CRC64. Group ID is the stable
peer/reparent lineage; boot, incarnation, and history are process-scoped.
After a restart, expect a new history and whole-group full sync rather than a
continuation from the old in-memory cursor.

The catalog generation is node-local and must only be compared with that
node's promotion base. Generation zero denotes a fresh set's implicit empty
catalog; the first successful catalog commit makes it nonzero. It is useful for
detecting a successful local Function commit or an unexpected catalog change,
but equal or different generations on two nodes say nothing about replicated
equivalence. The CRC64 identifies the local dump content for diagnosis.

Backlog and full-sync metrics are worker-labelled:

- `lavik_replication_backlog_bytes` and
  `lavik_replication_backlog_capacity_bytes` show the current hard reconnect
  window allocation and quota.
- `lavik_replication_backlog_floor_lsn` and
  `lavik_replication_backlog_tail_lsn` show retained event coverage. A
  consumer below the floor must full-sync.
- `lavik_replication_backlog_pinned_cursors` counts current ACK coverage
  claims. At the hard cap, the default policy waits for cursor progress;
  `replication-backlog-backpressure no` instead revokes lagging claims and
  evicts complete old events without allowing the backlog to grow.
- `lavik_replication_backlog_backpressured` identifies workers currently
  waiting for cursor progress, and
  `lavik_replication_backlog_backpressure_waits_total` counts capacity
  conflicts that entered that state.
- `lavik_replication_backlog_coverage_revocations_total` counts those
  revocations.
- `lavik_replication_publish_queue_bytes` and its capacity expose source
  publication staging. The corresponding `lavik_fullsync_*` gauges expose
  active full-sync sessions, their command queues, admission, and waits.
- `lavik_replication_control_connections` and
  `lavik_replication_flow_connections` distinguish native control sockets
  from per-flow data sockets.

Alert on sustained backlog backpressure together with a publisher queue near
capacity. When backpressure is disabled, alert instead on sustained coverage
revocations together with replicas repeatedly leaving `online`. Also alert on
a full-sync queue remaining near capacity or a node remaining `LOADING` after
a full-sync failure. A valid catalog root does not clear that last condition:
full-sync population activation and catalog readiness must complete together.

## Update model

Each worker owns a cache-line-aligned metrics shard and updates plain integers
only in that shard. Scraping submits a short snapshot operation to each worker
and merges the returned copies. Command execution therefore does not contend
on shared QPS or histogram atomics. HTTP response buffers are retained in a
bounded per-worker pool and reused by later scrapes.
