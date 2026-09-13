# RAID0 YCSB matrix

The six NVMe devices are `/dev/md0` RAID0. Keylane uses `/dev/md0p1` through
io_uring; Aerospike CE uses `/dev/md0p2` (1 TiB, due to the CE device limit).
Both databases contain 100M records with 10 fields of 128 bytes. Selection is
Uniform. Each matrix cell is a 100K-operation short measurement; the raw YCSB
logs retain average, min, max, p50, p95, p99, p99.9 and p99.99 latency.

The 32-cell baseline matrix covers Workloads A/B/C/D at 64, 128, 256 and 512
workers for Keylane HMSET and Aerospike. `hreplace-256/` contains the matching
256-worker Keylane HREPLACE runs. `summary.csv` contains the parsed baseline
rows; raw logs are grouped by database and workload.

The 256-worker HMSET profiling run completed 2M operations with 999,783 reads
and 1,000,217 updates. Client results are in `hmset-256-2m.log`.

## Profiling limitation

The host exposes `kernel.perf_event_paranoid=4`; `perf record` and `perf stat`
therefore report no samples/counters (`cycles: not supported`). No source
optimization was applied without a profile signal. The next actionable step is
to lower that host restriction or run the benchmark on a host that permits
per-process perf events, then repeat the same 256-worker HMSET run before and
after any mechanical optimization.
