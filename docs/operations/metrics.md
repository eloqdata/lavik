# Prometheus metrics

Enable the HTTP endpoint with a separate listen port:

```sh
./keylane --port 6379 --metrics-port 9100 --data-file keylane.data
curl http://127.0.0.1:9100/metrics
```

`--metrics-port=0` disables the HTTP listener. The endpoint is `GET /metrics`
and uses the Prometheus text exposition format.

A ready-to-run Prometheus and Grafana deployment, including a provisioned
dashboard and multi-node discovery, is available in
[`deploy/monitoring`](../../deploy/monitoring/README.md).

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
the official mimalloc global new/delete override performs no Keylane
accounting. RSS and allocator diagnostics remain outside command execution.

## Business metrics

- `keylane_commands_total`: completed Redis commands. QPS is
  `rate(keylane_commands_total[1m])`.
- `keylane_command_calls_total{command}`: completed commands by command name.
- `keylane_command_duration_seconds`: command execution histogram, from
  dispatch through reply construction; socket response writes are excluded.
- `keylane_connections`: all current TCP connections, including Redis clients,
  Prometheus scrapes, and replication connections.
- `keylane_connected_clients`: current Redis client connections. This is always
  less than or equal to `keylane_connections`.

For example, per-command QPS and aggregate p99 latency are:

```promql
sum by (command) (rate(keylane_command_calls_total[1m]))
histogram_quantile(
  0.99,
  sum by (le) (rate(keylane_command_duration_seconds_bucket[5m]))
)
```

## Storage metrics

The active defrag tuning values are exported alongside the work gauges so
latency graphs can be correlated with runtime A/B changes:

- `keylane_storage_defrag_max_active_per_device`
- `keylane_storage_defrag_block_sleep_seconds`
- `keylane_storage_defrag_record_sleep_seconds`
- `keylane_storage_defrag_paused`

They can be changed without restarting Keylane:

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

- `keylane_storage_defrag_runs_total{result}`: completed defrag attempts,
  classified as `success`, `resource_exhausted`, or `error`.
- `keylane_storage_defrag_active`: defrag jobs currently running.
- `keylane_storage_defrag_pending`: workers waiting to start a defrag job.
- `keylane_storage_capacity_bytes{device,path}`: usable data capacity.
- `keylane_storage_available_bytes{device,path}`: space available to normal
  writes after preserving the defrag reserve. This is the capacity metric to
  alert on when Keylane is close to rejecting writes.
- `keylane_filesystem_available_bytes{device,path}`: free space reported by
  the filesystem containing a regular data file. It is omitted for raw block
  devices. A preallocated data file can leave this value unchanged while
  `keylane_storage_available_bytes` falls.

Defrag activity can be compared with command latency using:

```promql
sum by (result) (rate(keylane_storage_defrag_runs_total[5m]))
```

## Memory metrics

- `keylane_memory_current_bytes`: cached explicitly retained bytes used for
  limit enforcement.
- `keylane_memory_used_bytes`: explicitly retained bytes. This is not total
  process heap usage; compare RSS and mimalloc diagnostics for that view.
- `keylane_memory_rss_bytes`: resident process memory, refreshed when metrics
  or `INFO memory` is requested.
- `keylane_memory_committed_bytes`: pages committed by mimalloc; diagnostic
  only and valid without per-allocation mimalloc statistics.
- `keylane_memory_reserved_bytes`: virtual address space reserved by mimalloc;
  diagnostic only.
- `keylane_memory_max_bytes`: configured process memory limit.
- `keylane_client_request_buffer_limit_bytes`: effective client request-buffer
  limit after resolving percentages and the nonzero 128 KiB floor.
- `keylane_client_buffered_request_bytes`: wire bytes currently retained by
  ordinary parsers, ready command batches, and queued transactions.
- `keylane_fullsync_reserved_memory_bytes`: reusable retained-memory headroom
  promised to active full-sync coverage maps.
- `keylane_memory_admission_pending_bytes`: short-lived worker-local permits
  held while retained page, bucket, replication-owner, or replica-staging
  ownership is constructed.
- `keylane_memory_rejected_commands_total`: commands rejected by the limit.
- `keylane_worker_retained_memory_bytes{worker}`: retained bytes charged to
  one worker's admission share, including its deterministic share of retained
  allocations created outside a bound worker.
- `keylane_worker_memory_admission_pending_bytes{worker}`: that worker's
  short-lived allocation permits.
- `keylane_worker_fullsync_reserved_memory_bytes{worker}`: that worker's
  reusable full-sync coverage reservation.
- `keylane_worker_client_buffered_request_bytes{worker}`: request bytes charged
  to that worker's independent client-buffer quota.
- `keylane_worker_memory_limit_bytes{worker}`: the worker's fixed share of the
  90% retained-memory waterline.

The provisioned Grafana **Retained Admission Utilization** gauge approximates
the process-wide admission decision as:

```promql
100 * (
  keylane_memory_current_bytes
  + keylane_fullsync_reserved_memory_bytes
  + keylane_memory_admission_pending_bytes
) / (0.9 * keylane_memory_max_bytes)
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
Release builds use mimalloc's official global C++ override without Keylane
hooks. Explicit retained allocators query `mi_usable_size` only on their much
rarer allocation/free paths. The hot command path reads its own shard and adds
a conservative retained-size estimate, so it performs no allocator aggregation
or `/proc` I/O. RSS is diagnostic only and is sampled by the explicit
metrics/INFO request.

## Update model

Each worker owns a cache-line-aligned metrics shard and updates plain integers
only in that shard. Scraping submits a short snapshot operation to each worker
and merges the returned copies. Command execution therefore does not contend
on shared QPS or histogram atomics. HTTP response buffers are retained in a
bounded per-worker pool and reused by later scrapes.
