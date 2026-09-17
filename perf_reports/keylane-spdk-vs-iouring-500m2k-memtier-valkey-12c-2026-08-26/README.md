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

# Keylane SPDK vs raw io_uring: 500M keys, fixed 2 KiB, 12 workers

**English** | [简体中文](README.zh-CN.md) | [All reports](../README.md)

> Historical benchmark of Keylane, the project now named Lavik. Product names,
> versions, commands, and measurements describe the original test; this is not a
> measurement of the current Lavik release.
> Restored from the [2026-09-15 archive](https://github.com/eloqdata/lavik/tree/eedb3080d808769519d93e971195b53b38b09c1e/perf_reports).
> The historical `perf_runs/` input directory was not included in the source snapshot;
> this restoration contains the report and its recorded tables, not those raw logs.

Date: 2026-08-26 UTC

## Technical summary

On this 16-logical-CPU host, Keylane used 12 pinned workers on CPUs 0-11 while local user processes and mlx5 IRQs were isolated to CPUs 12-15. Both backends used two 1.92 TB NVMe namespaces, Release builds, pipeline one, 80 client connections, eight client threads, fixed 2048-byte values, 500,000,000 existing keys, unlimited closed-loop load, and 300-second formal windows.

The main backend result is consistent across both clients:

- SPDK is faster for random reads and 1:1 read/write. With memtier it leads raw io_uring by 5.69% on read throughput and 6.72% on mixed throughput. With Valkey it leads raw io_uring by 8.24% on read throughput.
- Pure-write throughput is effectively tied. raw io_uring is 0.49% faster with memtier and 0.19% faster with Valkey, but its deep write tail is higher.
- At essentially identical SPDK read throughput, memtier reports p99.99 `1.039 ms` while Valkey reports `(0.591, 0.623] ms`. The same pattern appears on raw io_uring: memtier `1.079 ms`, Valkey `(0.607, 0.639] ms`. The extra read tail is therefore primarily client-side, not a Keylane throughput difference.
- Every accepted run exited successfully, retained exactly 500,000,000 keys, recorded zero memory rejections, and produced no Keylane error or latency-trace log lines. All memtier GET workloads reported zero misses.

## Headline results

### memtier_benchmark

memtier reports point estimates for the requested percentiles.

| Backend | Workload | QPS | SET QPS | GET QPS | Avg ms | p99 ms | p99.9 ms | p99.99 ms | p99.999 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SPDK | Read | 322,639.86 | 0 | 322,639.86 | 0.248 | 0.447 | 0.543 | 1.039 | 1.727 |
| raw io_uring | Read | 305,282.28 | 0 | 305,282.28 | 0.262 | 0.455 | 0.559 | 1.079 | 1.703 |
| SPDK | Random 1:1 | 370,683.28 | 185,341.70 | 185,341.58 | 0.216 | 0.551 | 1.351 | 1.639 | 4.959 |
| raw io_uring | Random 1:1 | 347,351.57 | 173,675.85 | 173,675.72 | 0.230 | 0.679 | 1.543 | 2.255 | 5.919 |
| SPDK | Write | 419,672.66 | 419,672.66 | 0 | 0.190 | 0.871 | 1.407 | 2.127 | 10.367 |
| raw io_uring | Write | 421,737.32 | 421,737.32 | 0 | 0.189 | 0.903 | 1.495 | 2.623 | 13.439 |

The mixed row counts individual Redis commands. memtier generated an equal long-run SET:GET ratio with independent random keys; it did not issue a two-command transaction or force the read and write to use the same key.

### valkey-benchmark

Valkey was intentionally not used for 1:1 because its custom multi-command sequence path at pipeline one did not scale with connection count. Valkey percentile values below are conservative HDR bucket intervals: the true percentile is greater than the lower endpoint and no greater than the upper endpoint.

| Backend | Workload | QPS | Avg ms | p99 ms | p99.9 ms | p99.99 ms | p99.999 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| SPDK | Read | 322,096.47 | 0.230 | 0.423 | `(0.479, 0.511]` | `(0.591, 0.623]` | `(0.703, 1.271]` |
| raw io_uring | Read | 297,588.91 | 0.251 | 0.439 | `(0.503, 0.527]` | `(0.607, 0.639]` | `(0.727, 3.055]` |
| SPDK | Write | 434,038.25 | 0.163 | 0.839 | `(1.367, 1.479]` | `(1.879, 2.423]` | `(6.503, 7.599]` |
| raw io_uring | Write | 434,877.59 | 0.163 | 0.847 | `(1.367, 1.479]` | `(1.951, 3.175]` | `(8.983, 10.367]` |

## Backend comparison

| Client and workload | SPDK QPS | raw io_uring QPS | Higher backend | Difference |
|---|---:|---:|---|---:|
| memtier read | 322,639.86 | 305,282.28 | SPDK | 5.69% |
| memtier random 1:1 | 370,683.28 | 347,351.57 | SPDK | 6.72% |
| memtier write | 419,672.66 | 421,737.32 | raw io_uring | 0.49% |
| Valkey read | 322,096.47 | 297,588.91 | SPDK | 8.24% |
| Valkey write | 434,038.25 | 434,877.59 | raw io_uring | 0.19% |

SPDK's benefit is concentrated in workloads that wait for random reads. It polls NVMe completions in user space and avoids the raw backend's kernel submission/completion path and managed NVMe IRQs. Pure writes are buffered and batched enough that both backends converge on the same foreground throughput.

The raw backend's write tail remains worse despite equal throughput. With memtier, raw p99.99 is `2.623 ms` versus SPDK `2.127 ms`, and raw p99.999 is `13.439 ms` versus `10.367 ms`. The Valkey HDR upper bounds show the same direction.

## Client comparison

The fixed-size rerun isolates the earlier client discrepancy:

| Backend | Client | Read QPS | Avg ms | p99.9 ms | p99.99 ms |
|---|---|---:|---:|---:|---:|
| SPDK | memtier | 322,639.86 | 0.248 | 0.543 | 1.039 |
| SPDK | Valkey | 322,096.47 | 0.230 | `(0.479, 0.511]` | `(0.591, 0.623]` |
| raw io_uring | memtier | 305,282.28 | 0.262 | 0.559 | 1.079 |
| raw io_uring | Valkey | 297,588.91 | 0.251 | `(0.503, 0.527]` | `(0.607, 0.639]` |

On SPDK the clients differ by only 0.17% in read throughput, yet memtier's p99.99 is at least 66.8% above Valkey's conservative upper bound. On raw io_uring memtier is 2.59% faster, but its p99.99 is at least 68.9% above Valkey's upper bound. This is direct evidence that memtier adds deep-tail delay under closed-loop load even when it does not limit server throughput.

The write comparison is less clean because the memtier pure-write window followed the mixed window on the same dataset, whereas Valkey's pure-write window followed only a read. It is still useful operational evidence, but it should not be used alone to attribute a 3% write-QPS difference to the client implementation.

## The anomalous raw Valkey read

The first io_uring/Valkey read attempt completed but contained a 5.4-second disturbance at elapsed seconds 277-282:

- Tick throughput fell from approximately 290-300K QPS to approximately 51K QPS.
- Tick average latency rose to approximately 1.54 ms.
- The full-window result fell to 290,002.56 QPS with p99.99 `(2.535, 2.735] ms` and maximum `206.463 ms`.
- Keylane remained at approximately 1120% CPU. Keylane journal, kernel journal, and both NVMe SMART logs showed no error; both devices reported zero media errors and zero critical warnings.

An immediate second 300-second read did not reproduce the disturbance and measured 297,588.91 QPS with p99.99 `(0.607, 0.639] ms`. The stable rerun is the main table value. The first run remains in the raw evidence as `iouring/valkey/read-run1-anomalous`; the cause is unresolved and should be investigated with block-layer latency tracing if it recurs.

## Dataset construction

Each client used a separately rebuilt logical dataset because their native random-key formats are incompatible:

- memtier keys were `kv_1` through `kv_500000000`, created once each with parallel sequential `P:P` generation and `--requests=allkeys`.
- Valkey keys were `kv_000000000000` through `kv_000499999999`, created once each with `--sequential -n 500000000 -r 500000000`.

Both datasets contained exactly 500,000,000 keys and fixed 2048-byte values. Each rebuild began after `FLUSHALL SYNC`, `DBSIZE=0`, and `DEFRAG STATUS` reported no active or pending work. The key-format difference changes key length by a few bytes and is a limitation for direct client comparison, but it avoids the much larger error of benchmarking GET misses.

| Backend | Client-format fill | Fill SET/s | Final DBSIZE | Rejections |
|---|---|---:|---:|---:|
| SPDK | memtier | 663,908.40 | 500,000,000 | 0 |
| SPDK | Valkey | 654,454.56 | 500,000,000 | 0 |
| raw io_uring | memtier | 652,767.11 | 500,000,000 | 0 |
| raw io_uring | Valkey | 644,019.50 | 500,000,000 | 0 |

All memtier read and mixed runs reported zero misses. For each Valkey dataset, the first, middle, and last known keys returned `STRLEN=2048`, and a five-second random GET probe succeeded before formal testing.

## Runtime configuration

### CPU and IRQ layout

| Work | CPUs |
|---|---|
| Keylane service cgroup and 12 pinned workers | 0-11 |
| Local user slice, tmux, Codex and SSH launchers | 12-15 |
| mlx5 async IRQ | 15 |
| mlx5 completion IRQs | 12-15 round-robin |
| raw io_uring NVMe managed completion queues | one queue per CPU; worker queues remain on 0-11 |

The final effective state was `user-1000.slice AllowedCPUs=12-15`; mlx5 IRQ 58 was on CPU 15 and IRQs 59-74 were round-robin across 12-15. Linux exposes the raw NVMe I/O vectors as managed IRQs assigned one per CPU, so they cannot all be moved off the worker set. This kernel work is intentionally part of the raw io_uring result.

### Keylane settings

- `--threads=12 --pin-workers`
- `--busy-poll-us=20`
- `--foreground-budget-us=1000`
- SPDK only: `--spdk-max-completions-per-poll=8`
- SPDK devices: `spdk://3cfa:00:00.0/1` and `spdk://fd75:00:00.0/1`
- raw devices: stable `/dev/disk/by-id/nvme-Microsoft_NVMe_Direct_Disk_*` paths
- Release builds with cross-core, read, and SET latency tracing all compiled `OFF`

The raw build successfully recovered the 500-million-key SPDK dataset before it was deliberately cleared, demonstrating storage-format compatibility. Recovery took approximately 3 minutes 55 seconds at roughly 2.7-2.8 million scanned records per second.

### Defrag settings

Defrag remained enabled for every fill and workload:

| Workload | Max active per device | Block sleep | Record sleep |
|---|---:|---:|---:|
| Read | 8 | 0 ms | 0 us |
| Random 1:1 | 2 | 15 ms | 0 us |
| Write | 6 | 0 ms | 0 us |

Pure-write windows observed active defrag jobs and ended with no queued or active jobs. This makes the write result an online-reclamation result rather than an append-only burst.

## Client commands

memtier formal shape, with `RATIO` set to `0:1`, `1:1`, or `1:0`:

```bash
taskset -c 0-15 /usr/bin/memtier_benchmark \
  -s 10.0.0.4 -p 6379 -P redis \
  -t 8 -c 10 --pipeline=1 --run-count=1 --test-time=300 \
  --ratio=RATIO \
  --key-minimum=1 --key-maximum=500000000 \
  --key-pattern=R:R --key-prefix=kv_ --distinct-client-seed \
  --data-size=2048 \
  --print-percentiles=50,95,99,99.9,99.99,99.999
```

Valkey read shape; SET replaced GET for the write run:

```bash
taskset -c 0-15 /tmp/keylane-valkey-offset/valkey-benchmark \
  -h 10.0.0.4 -p 6379 \
  -c 80 --threads 8 -P 1 \
  --warmup 5 --duration 300 --precision 3 --seed 20260826 \
  -r 500000000 -d 2048 -- GET kv___rand_int__
```

Client versions and hashes:

- memtier `2.5.1`, SHA-256 `9b6ee614dae154c64b17a067a10236b3e6532f7ca52c43b547b87101556dd7d4`
- Valkey commit `382a1349`, SHA-256 `f4a6156b7b8f2200d07b5c8e7ba0a30411a1f9ee1e534ca9a3e69c34cccf46b5`

## Validation and limitations

- Keylane commit: `c9f981732539fd32b6ec9000d2608f03e698691b`
- celer commit: `0a70d22086fb14435464253991ca6fc6a90f19d5`
- Every primary run used one 300-second measured window; Valkey additionally used a five-second warmup.
- All primary client exit codes were zero, `DBSIZE` was 500,000,000 before and after, and `keylane_memory_rejected_commands_total` remained zero.
- No primary service-journal window contained error, fatal, OOM, or latency-trace lines.
- This is a single-host, single-run comparison except for the repeated anomalous raw Valkey read. Confidence intervals across independent process restarts were not measured.
- The backends used the same physical devices sequentially, not simultaneously. Temperature stayed below warning thresholds and SMART reported no media errors.
- Separate client-native key formats prevent a perfectly identical client A/B. Backend comparisons within a client are stronger than cross-client comparisons.
- The write order includes real dataset aging. memtier ran read, mixed, then write; Valkey ran read then write. Cross-client write differences therefore combine client behavior with different prior aging.

## Recommended next steps

1. Treat SPDK as the preferred read/mixed backend on this host; its advantage is 5.7-8.2% at the tested scale.
2. Choose raw io_uring when operational simplicity matters more than that read advantage; pure-write throughput is effectively equal.
3. Use Valkey for sub-millisecond deep-tail read characterization. memtier is still suitable for throughput and workload-mix generation, but its p99.99 read value includes measurable client-side delay.
4. If the raw Valkey read disturbance recurs, capture `block:block_rq_issue`, `block:block_rq_complete`, io_uring completion latency, per-worker queue delay, and `/proc/interrupts` over the same interval.
5. Repeat the matrix three times with alternating backend order if a release gate requires confidence intervals rather than a point comparison.

## Evidence

- Compact reviewed results are included in the headline tables above.
- Raw run root: `perf_runs/keylane-500m2k-spdk-vs-iouring-memtier-valkey-12c-20260826/`
