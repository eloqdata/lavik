# Online latency diagnostics: server overhead

**English** | [简体中文](README.zh-CN.md)

Measured 2026-11-01. Final-code measurements use commit `4ba8f0b3` after rebasing onto main `9d745dcc`. They cover always-on pressure metrics, runtime GET/SET stage tracing, and independent file logging. The default 20-record/s/worker logging budget has no consistent additional throughput loss in five short matched runs. Full sampling has a measurable cost; raising the output budget needs a separate assessment.

## Whole feature against main

Each cell is the median of three runs with rotated mode order. Both versions use the same optimized Release build settings and pinned dependencies. The default threshold is 10 ms; diagnostics are normally disabled. All sampled modes enable the independent file logger at its default limit.

| Mode | QPS | Change vs main | p99 ms | p99.9 ms | CPU µs/op | RSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| main baseline | 200,189 | +0.00% | 0.623 | 2.975 | 9.180 | 854.53 |
| Diagnostics off; pressure metrics on | 198,942 | -0.62% | 0.599 | 2.991 | 9.278 | 856.07 |
| 1/64, default threshold | 197,268 | -1.46% | 0.631 | 2.991 | 9.345 | 855.89 |
| 1/1, default threshold | 191,282 | -4.45% | 0.607 | 2.975 | 9.721 | 855.84 |
| 1/1, threshold 0 | 188,290 | -5.94% | 0.599 | 2.975 | 9.870 | 856.04 |

These results support keeping the pressure counters on, and using 1/64 sampling for temporary diagnosis. Full sampling is available when the additional cost is acceptable. A threshold filters completed samples: it saves retention/output work but does not avoid timing selected commands. Smaller QPS does not necessarily increase p99 in a closed-loop client; CPU/op and throughput are relevant alongside latency.

## Incremental file logging at the default limit

Both sides use the same final binary, sampling rate, zero threshold, stage histograms and recent history. Only `latency-diagnostics-log-enabled` changes. Each cell is the median of five rotated runs. Two workers share neither their enqueue budgets nor their queues.

| Sampling | QPS off → on | Change | p99 ms off → on | p99.9 ms off → on | CPU µs/op off → on |
|---|---:|---:|---:|---:|---:|
| 1/64 | 197,730 → 199,061 | +0.67% | 0.623 → 0.623 | 2.991 → 3.023 | 9.299 → 9.264 |
| 1/1 | 189,335 → 188,800 | -0.28% | 0.591 → 0.615 | 3.007 → 2.991 | 9.860 → 9.891 |

Both rates produce 400 records in each ten-second measured logging-on run, approximately 40 records/s across two workers; logging-off produces zero. Most selected traces are intentionally omitted from the file after budget exhaustion, while histograms and recent history still update. Throughput changes have mixed signs. These measurements do not establish a strict <1% overhead bound or a tail-latency SLO; individual paired results are in [comparison.json](logging-final/comparison.json).

## Configured output budgets

`CONFIG SET latency-diagnostics-log-max-per-second N` changes each worker's one-second enqueue budget. The default is 20; 0 removes the rate limit. The 64-entry/worker SPSC queue remains bounded and drops rather than blocking requests. A consumer recovering from a stalled sink can drain a burst; this is not a strict disk-write rate. `latency-diagnostics-log-enabled no` disables new output independently.

The following are **one matched pair per cell**, with 15-second measurements and a zero threshold; they are exploratory observations, not stable estimates. They isolate logging against the same binary's logging-off case.

| Limit / worker / s | Sampling | QPS off → on | Change | Written records/s | Written MB/s | CPU µs/op off → on |
|---|---|---:|---:|---:|---:|---:|
| 200 | 1/64 | 197,433 → 196,100 | -0.68% | 400 | 0.296 | 9.343 → 9.378 |
| 200 | 1/1 | 189,078 → 187,073 | -1.06% | 400 | 0.296 | 9.882 → 9.921 |
| 0 | 1/64 | 197,855 → 197,319 | -0.27% | 3,067 | 2.274 | 9.319 → 9.389 |
| 0 | 1/1 | 187,678 → 189,286 | +0.86% | 6,246 | 4.639 | 9.878 → 9.946 |

Rate-limit and queue/sink omissions contribute to `log_dropped`. An unlimited enqueue budget does not guarantee that every qualifying trace is written. Raw end-of-run diagnostic counters include warmup and are retained separately from the measured file-rate deltas.

## Conditions and limits

- Intel Xeon Platinum 8573C VM, eight logical CPUs / four physical cores, approximately 62.8 GiB RAM; Ubuntu Linux 6.17.0-1018-azure. Server workers use CPUs 0,2; memtier uses 4,6 on separate physical cores. The logger remains unpinned. Builds, other Lavik tests, Grafana, Prometheus, Alloy and Loki are stopped during measurement.
- memtier 2.5.1, loopback TCP, GET:SET 1:1, 512-byte values, 10,000 preloaded keys, two client threads and 16 clients per thread. Every measured GET hits, with zero connection errors. Each fresh server gets a three-second warmup followed by the measurement described above. Warmup includes initial logger/file creation in enabled cases.
- Each server uses its own 2 GiB file on local NVMe RAID (`/dev/md127`, ext4), normal io_uring storage and persistence settings. Files are removed after clean shutdown. This small overwrite workload does not model large disk-resident values, sustained cleaning or a long-running dataset. Large paged GET correctness has separate regression coverage; its performance is not measured here.
- Workers publish fixed-size copies to bounded SPSC queues. A dedicated thread serializes JSON and writes a separate rotating JSONL sink. It polls/flushes every 20 ms without fsync. Disk stalls, remote filesystems and external collectors need their own workload-specific measurements. No log rotation occurs in the final measured cases.
- Logging-off preserves the logger thread and queues. The on/off A/B measures incremental output work, rather than resources saved by removing the module. Linux `/proc/PID/stat` supplies process CPU time, and `/proc/PID/task/TID/schedstat` supplies thread CPU runtime. Kernel writeback and collector CPU are outside process accounting. RSS includes storage buffers and is an observed process snapshot rather than an allocation ledger.
- Quantiles are memtier's client-observed latencies. Diagnostic stage histograms describe a selected slow subset and have different boundaries. Three/five short runs do not provide a confidence interval or an upper bound for other workloads.

A final-build layout probe gives 736 bytes per fixed trace record. The 256-context pool, 128-entry recent history and 64-entry log queue together reserve 322 KiB per worker for those record copies, plus counters, histograms and free-slot bookkeeping. These allocations remain when diagnostics are off. The existing default `CommandReply` remains 72 bytes. Selected paged reads additionally own temporary child timing payloads within their existing I/O window.

## Reproduction and evidence

Candidate SHA-256: `7fe05f9f3aea772b1ecef9dad13b46a1f7ccb756c92bc21f5166244f397ec4d7`. Main baseline SHA-256: `c5462bfd8233b09c79a300d202dfe4b54597a0d528ca5050c4164b588998b9ef`. Build with GCC 13 and Ninja using the normal optimized Release settings. Bycorf is pinned to `9f06006de978bd48bd51773c9d43d5122229aab1`, mimalloc to `acf2fdd329f9dc2a7ffe3f12a133fe7175e39378`. Build the main source snapshot separately with those same dependencies/settings to obtain the baseline path below.

```bash
cmake -S . -B build_perf -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build_perf

python3 microbench/online_latency_benchmark.py build_perf/lavik build_perf/lavik /mnt/local_nvme/lavik-396/logging-final --logging-only --workers 2 --server-cpus 0,2 --client-cpus 4,6 --single-client-cpus 4,6 --seconds 10

python3 microbench/online_latency_benchmark.py /mnt/local_nvme/lavik-396/baseline-build/lavik build_perf/lavik /mnt/local_nvme/lavik-396/feature-final --workers 2 --server-cpus 0,2 --client-cpus 4,6 --single-client-cpus 4,6 --seconds 10 --rounds 3

python3 microbench/online_latency_benchmark.py build_perf/lavik build_perf/lavik /mnt/local_nvme/lavik-396/logging-limit-200 --logging-only --log-limit 200 --workers 2 --server-cpus 0,2 --client-cpus 4,6 --single-client-cpus 4,6 --rounds 1

python3 microbench/online_latency_benchmark.py build_perf/lavik build_perf/lavik /mnt/local_nvme/lavik-396/logging-limit-0 --logging-only --log-limit 0 --workers 2 --server-cpus 0,2 --client-cpus 4,6 --single-client-cpus 4,6 --rounds 1
```

Output directories must be new. [Feature runs](feature-final/runs.json), [feature summary](feature-final/summary.json), [logging runs](logging-final/runs.json), [logging summary](logging-final/summary.json), configured-budget runs ([200](logging-limit-200/runs.json), [0](logging-limit-0/runs.json)), and run metadata with source/binary identities are committed. Each run retains `memtier.json`, `commands.json`, `server-args.json` and diagnostic counters. Exact generated command arrays preserve observed arguments. Complete server logs, scrapes and diagnostic JSONL remain under the matching `/mnt/local_nvme/lavik-396/` directories on the measurement host.

## Earlier snapshot

The [earlier logging data](logging/summary.json) covers one and two workers, five runs per mode. It used binary `a08effe8ed5672c50ed874b0ddfae609181cf7535534bdcf7567f64c84121d7a`, built from `b5726e4a` plus [captured changes](logging/source.patch.gz), before the configurable rate limit and paged-read timing fix. Its median logging-on QPS changes were −1.15% / −0.04% for one worker at 1/64 / 1/1 sampling, and +0.78% / +1.41% for two workers. Paired changes spanned −2.00% to +2.17%; one p99.9 pair increased by 0.448 ms. These are historical observations rather than final-code measurements.

A [30-second thread profile](logging-thread-profile/runs.json) on that earlier snapshot used two workers and full sampling. The dedicated consumer used 15.754 ms with output off and 15.605 ms with output on, approximately 0.052% of one CPU in both cases, including polling wakeups. The incremental difference is below this pair's noise. Process QPS changed by −0.45% and CPU/op by +0.62%. This pair is retained for context and does not substitute for the final-code results above.
