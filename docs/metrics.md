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
[`deploy/monitoring`](../deploy/monitoring/README.md).

## Memory limit

`--max-memory` (also accepted as `--maxmemory`) sets the process memory limit
and accepts byte-size suffixes such as `8GiB`. A value of zero, the default,
uses 80% of the smaller of the host memory capacity and the process cgroup
limit. Commands that may grow retained memory return a Redis-compatible OOM
error when the cached allocator or RSS measurement would cross the limit.

Allocator and RSS measurements are refreshed every 100ms outside command
execution. The SET/MSET/INCR hot path performs relaxed atomic reads and a
small allocation estimate; it does not query the allocator or `/proc`.

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

- `keylane_memory_current_bytes`: cached allocator usable bytes used for limit
  enforcement.
- `keylane_memory_used_bytes`: allocator usable bytes.
- `keylane_memory_rss_bytes`: resident process memory, refreshed when metrics
  or `INFO memory` is requested.
- `keylane_memory_committed_bytes`: pages committed by mimalloc; diagnostic
  only and valid without per-allocation mimalloc statistics.
- `keylane_memory_reserved_bytes`: virtual address space reserved by mimalloc;
  diagnostic only.
- `keylane_memory_max_bytes`: configured process memory limit.
- `keylane_memory_rejected_commands_total`: commands rejected by the limit.

The same values are available through Redis `INFO memory`, including
`used_memory`, `used_memory_rss`, `maxmemory`, and
`oom_rejected_commands`.

Worker 0 sums the cache-line-separated worker allocation counters every 100 ms.
Release builds keep mimalloc's generic per-allocation statistics disabled and
use Keylane's own lightweight usable-size accounting instead. The hot command
path reads only the cached gauge and adds a conservative request-size estimate,
so it performs no allocator aggregation or `/proc` I/O. RSS is diagnostic only
and is sampled by the explicit metrics/INFO request.

## Update model

Each worker owns a cache-line-aligned metrics shard and updates plain integers
only in that shard. Scraping submits a short snapshot operation to each worker
and merges the returned copies. Command execution therefore does not contend
on shared QPS or histogram atomics. HTTP response buffers are retained in a
bounded per-worker pool and reused by later scrapes.
