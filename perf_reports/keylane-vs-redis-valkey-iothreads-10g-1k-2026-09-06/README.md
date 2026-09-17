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

# Keylane, Redis, Valkey, Dragonfly, and Garnet: 1 KiB High-Concurrency Performance Comparison

**English** | [简体中文](README.zh-CN.md) | [All reports](../README.md)

> Historical benchmark of Keylane, the project now named Lavik. Product names,
> versions, commands, and measurements describe the original test; this is not a
> measurement of the current Lavik release.
> Restored from the [2026-09-15 archive](https://github.com/eloqdata/lavik/tree/eedb3080d808769519d93e971195b53b38b09c1e/perf_reports).
> Archived charts, CSVs, hash manifests, and helper scripts are included. The scripts
> reference original benchmark hosts and raw-input paths; those raw logs are not bundled.

Test date: September 6, 2026

## Technical summary

This report contains two independent experiment groups. In the
10,000,000-key × 1,024-byte (approximately 10 GB) in-memory comparison,
Keylane on six raw NVMe devices with io_uring reached a best GET throughput of
**828,502 QPS**, **14.1%** below the best tuned Redis 8.8.0 result and **12.6%**
below Valkey 9.1.0. Keylane reached **984,452 QPS** on SET, **12.5%** above
Redis and **23.7%** above Valkey.

In the separate 1,000,000,000-key × 1,024-byte (approximately 1 TB)
storage-tier experiment, Keylane raw io_uring peaked at **784,179 GET QPS**,
**107.0%** above Dragonfly v1.40.2 and **135.7%** above Garnet v2.1.5. Its
**856,523 SET QPS** peak was **44.0%** above Dragonfly and **12.0%** above
Garnet. All three systems regressed at 2,560 connections, with their respective
peaks occurring between 640 and 1,280 connections.

These results support a bounded conclusion: on this machine, with 1 KiB
values, pipeline=1, and one client, Keylane random-read throughput was about
13%–14% below tuned in-memory systems, while its write throughput did not lag.
Keylane also led the comparable large-capacity storage-tier products in both
read and write throughput. The 10 GB and 1 TB groups used different datasets,
run durations, and storage models; they must not be ranked across groups, and
the results do not imply the same gaps on arbitrary hardware or durability
configurations.

Redis and Valkey required enough I/O threads to make the in-memory comparison
meaningful. Their one- and two-thread peaks were only about 144k–159k QPS.
Redis performed best with 16 threads. Valkey GET was also best with 16 threads,
but Valkey SET was best with 8 and regressed by 4.7% at 16.

For a broader comparison of disk-backed Redis-compatible systems and their
different persistence, WAL, and compaction settings, see the
[detailed persistence and storage-tier report](../keylane-vs-dragonfly-tiering-2026-08-11/README.md).

## The gap between Keylane and tuned in-memory systems

![Keylane, Redis, and Valkey QPS by connection count](best-memory-vs-keylane-qps.png)

The in-memory configuration is selected per command from the measured sweep:
Redis uses 16 I/O threads for GET and SET; Valkey uses 16 for GET and 8 for SET.
Keylane always uses 16 workers. This compares each system's best measured
configuration instead of inflating Keylane's advantage with single-threaded
Redis or Valkey.

| Workload | System and configuration | Peak QPS | Connections at peak | Avg | p50 | p99 | p99.9 | p99.99 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| GET | Keylane, 16 workers | 828,502 | 1,280 | 1.544 ms | 1.319 ms | 4.543 ms | 11.199 ms | 21.887 ms |
| GET | Redis, 16 I/O threads | 964,267 | 1,280 | 1.327 ms | 1.111 ms | 4.671 ms | 8.191 ms | 17.407 ms |
| GET | Valkey, 16 I/O threads | 948,300 | 1,280 | 1.349 ms | 1.111 ms | 4.479 ms | 8.383 ms | 17.151 ms |
| SET | Keylane, 16 workers | 984,452 | 1,280 | 1.300 ms | 1.159 ms | 4.575 ms | 7.455 ms | 16.063 ms |
| SET | Redis, 16 I/O threads | 874,879 | 1,280 | 1.463 ms | 1.191 ms | 4.767 ms | 7.839 ms | 17.663 ms |
| SET | Valkey, 8 I/O threads | 796,145 | 1,280 | 1.607 ms | 1.447 ms | 4.319 ms | 9.471 ms | 17.919 ms |

Peak p99 was similar across the three products: 4.48–4.67 ms for GET and
4.32–4.77 ms for SET. Keylane GET had the worst p99.99, while Keylane SET had
better p99.9 and p99.99 than both in-memory controls. The comparable throughput
was not achieved by accepting a substantially worse p99.

### Exact QPS at each connection count

| Workload | Connections | Keylane, 16 workers | Redis, 16 I/O threads | Best Valkey thread count |
|---|---:|---:|---:|---:|
| GET | 80 | 251,872 | 300,320 | 374,308 (16) |
| GET | 160 | 447,569 | 496,011 | 576,207 (16) |
| GET | 320 | 658,224 | 686,043 | 708,992 (16) |
| GET | 640 | 796,395 | 869,620 | 882,881 (16) |
| GET | 1,280 | 828,502 | 964,267 | 948,300 (16) |
| SET | 80 | 338,732 | 298,222 | 362,610 (8) |
| SET | 160 | 566,366 | 476,830 | 579,863 (8) |
| SET | 320 | 764,712 | 657,652 | 682,094 (8) |
| SET | 640 | 964,503 | 798,889 | 775,229 (8) |
| SET | 1,280 | 984,452 | 874,879 | 796,145 (8) |

## 1 TB storage tier: Keylane leads Dragonfly and Garnet

![Keylane, Dragonfly, and Garnet QPS by connection count](storage-tier-comparison-qps.png)

This group expands the dataset to one billion keys, or 1.024 TB of logical
value payload (approximately 953.7 GiB), and is therefore analyzed separately
from the 10 GB in-memory experiment. Keylane directly uses six raw NVMe
devices. Dragonfly Tiered Storage and Garnet Storage Tier use files on a RAID0
and XFS volume built from the same six drives. All three use the same server,
client, 16 memtier threads, pipeline=1, and a 60-second window per point.

Keylane reached 784,179 GET QPS at 1,280 connections: 2.07× Dragonfly's peak
and 2.36× Garnet's. It reached 856,523 SET QPS at 1,280 connections, 44.0%
above Dragonfly and 12.0% above Garnet. Dragonfly GET continued to gain
throughput as concurrency increased, but its p99 at the peak reached 47.615 ms.
Garnet GET had steadier tail latency but began to regress after 640 connections.
Keylane also regressed at 2,560 connections; further concurrency only added
queueing and tail latency.

| Workload | System and configuration | Peak QPS | Connections at peak | Avg | p50 | p99 | p99.9 |
|---|---|---:|---:|---:|---:|---:|---:|
| GET | Keylane, 16 workers, raw NVMe | 784,179 | 1,280 | 1.632 ms | 1.527 ms | 4.095 ms | 8.895 ms |
| GET | Dragonfly, 16 proactors, Tiered Storage | 378,851 | 1,280 | 3.378 ms | 1.439 ms | 47.615 ms | 77.823 ms |
| GET | Garnet, Storage Tier | 332,665 | 640 | 1.923 ms | 1.735 ms | 3.743 ms | 5.503 ms |
| SET | Keylane, 16 workers, raw NVMe | 856,523 | 1,280 | 1.494 ms | 1.015 ms | 8.511 ms | 12.799 ms |
| SET | Dragonfly, 16 proactors, Tiered Storage | 594,941 | 1,280 | 2.151 ms | 1.551 ms | 21.375 ms | 51.199 ms |
| SET | Garnet, Storage Tier | 764,917 | 1,280 | 1.673 ms | 1.399 ms | 5.183 ms | 11.263 ms |

### Exact QPS at each connection count

| Workload | Connections | Keylane raw io_uring | Dragonfly Tiered Storage | Garnet Storage Tier |
|---|---:|---:|---:|---:|
| GET | 80 | 253,251 | 199,548 | 177,016 |
| GET | 160 | 444,808 | 297,609 | 247,875 |
| GET | 320 | 661,401 | 347,584 | 306,277 |
| GET | 640 | 771,496 | 367,956 | 332,665 |
| GET | 1,280 | 784,179 | 378,851 | 318,165 |
| GET | 2,560 | 690,259 | 371,890 | 292,482 |
| SET | 80 | 320,360 | 268,251 | 365,630 |
| SET | 160 | 513,385 | 378,335 | 490,332 |
| SET | 320 | 671,865 | 440,722 | 669,057 |
| SET | 640 | 811,734 | 474,879 | 757,442 |
| SET | 1,280 | 856,523 | 594,941 | 764,917 |
| SET | 2,560 | 790,186 | 537,193 | 720,262 |

Garnet was faster for low-concurrency SET: at 80 connections it led Keylane by
14.1%. Keylane moved ahead at 160 connections and widened the gap through the
640–1,280 range. “Keylane is always fastest on SET” is therefore not supported;
the precise conclusion is that Keylane achieved the highest peak in the tested
high-concurrency saturation range.

## I/O threads determine whether Redis and Valkey approach Keylane

![Redis and Valkey I/O-thread scaling](iothread-scaling-qps.png)

Moving from one to two I/O threads barely helped either system. Throughput
started scaling materially at four threads and entered the 800k QPS range only
at eight or more. Redis GET and SET continued to improve at 16 threads within
the tested range. Valkey GET also improved, while Valkey SET regressed after
eight threads. More threads are not unconditionally better; production settings
must be selected for the workload.

| System | I/O threads | Peak GET QPS @ connections | Peak SET QPS @ connections |
|---|---:|---:|---:|
| Redis 8.8.0 | 1 | 156,477 @ 320 | 146,263 @ 320 |
| Redis 8.8.0 | 2 | 157,452 @ 320 | 147,218 @ 320 |
| Redis 8.8.0 | 4 | 432,526 @ 1,280 | 398,222 @ 640 |
| Redis 8.8.0 | 8 | 841,942 @ 1,280 | 817,501 @ 1,280 |
| Redis 8.8.0 | 16 | 964,267 @ 1,280 | 874,879 @ 1,280 |
| Valkey 9.1.0 | 1 | 154,406 @ 320 | 143,854 @ 320 |
| Valkey 9.1.0 | 2 | 159,368 @ 640 | 148,006 @ 320 |
| Valkey 9.1.0 | 4 | 440,085 @ 640 | 409,011 @ 1,280 |
| Valkey 9.1.0 | 8 | 848,840 @ 1,280 | 796,145 @ 1,280 |
| Valkey 9.1.0 | 16 | 948,300 @ 1,280 | 758,618 @ 1,280 |

## Scope and metric definitions

- Server: `172.16.0.4`, AMD EPYC 9V74, 8 cores / 16 threads, 125 GiB RAM.
  All server threads were restricted to CPUs 0–15.
- Client: `172.16.0.5`, 16 cores, 31 GiB RAM. memtier 2.5.1 used 16 fixed
  threads restricted to CPUs 0–15. Host `.6` was unreachable during this run,
  so only one client was used.
- Datasets: the in-memory group used 10,000,000 decimal numeric keys and 10.24
  GB (9.54 GiB) of logical value payload. The storage-tier group used
  1,000,000,000 keys and 1.024 TB (953.7 GiB) of logical value payload. Every
  value was exactly 1,024 bytes. After loading, Redis reported 12.46G of used
  memory and Valkey reported 12.36G.
- Workloads: GET uniformly sampled the full key range and had a 100% hit rate.
  SET uniformly overwrote existing keys without adding new keys.
- Concurrency: the in-memory group used 80, 160, 320, 640, and 1,280
  connections with a 30-second window per point. The storage-tier group also
  included 2,560 connections and used 60 seconds per point. Both used
  pipeline=1. QPS is completed requests divided by wall-clock time inside the
  formal measurement window.
- Latency: all values are end-to-end observations from the memtier client and
  are reported in milliseconds.

## Methodology

- Keylane used commit
  [`29dc8e6`](https://github.com/thweetkomputer/keylane/commit/29dc8e6b87c40196dc397759690252944f1196f0),
  a Clang 18 Release build with `-march=native`, 16 workers, an io_uring backend
  on six independent raw NVMe devices, and defragmentation paused. The binary's
  SHA-256 was
  `ef8cc3f1b815fe123f408626f8d4dadfc3a509ed7f63ab5de17e3e237b2d82fd`.
- Redis used the official [8.8.0 release](https://github.com/redis/redis/releases/tag/8.8.0),
  and Valkey used the official [9.1.0 release](https://github.com/valkey-io/valkey/releases/tag/9.1.0).
  Both were built from source with their default jemalloc configuration and
  `-O3`, without add-on modules.
- Redis and Valkey ran with AOF and automatic RDB saves disabled. After loading
  exactly 10M keys and validating `DBSIZE`, each system saved a baseline RDB.
  Every I/O-thread variant restarted from this RDB before the formal SET tests
  could modify it, validated the key count and `CONFIG GET io-threads`, and ran
  a 10-second GET warmup before the sweep.
- After disassembling the RAID0, the six Keylane drives were individually
  `blkdiscard`ed. Keylane loaded the 10M keys once, validated `DBSIZE`, ran a
  10-second GET warmup, and followed the same connection-count order.
- The 1 TB group used the same one-billion-key × 1 KiB workload. Keylane raw
  io_uring directly opened the six NVMe devices.
  [Dragonfly v1.40.2](https://github.com/dragonflydb/dragonfly/releases/tag/v1.40.2)
  used 16 proactors, 96 GiB `maxmemory`, and Tiered Storage on RAID0/XFS.
  [Garnet v2.1.5](https://github.com/microsoft/garnet/releases/tag/v2.1.5)
  used 64 GiB of hybrid-log memory, a 32 GiB read cache, a 16 GiB index, and
  Storage Tier on the same RAID0/XFS volume. Dragonfly received an additional
  180-second random-GET warmup before formal GET testing.
- Reproduction entry points are [`run_memory_sweep.sh`](run_memory_sweep.sh)
  and [`run_keylane_sweep.sh`](run_keylane_sweep.sh). Normalization and chart
  generation are implemented by [`build_assets.py`](build_assets.py). The 110
  in-memory points are in [`results.csv`](results.csv); the 36 storage-tier
  points are in [`storage-results.csv`](storage-results.csv). Raw-input hashes
  are recorded in [`raw-SHA256SUMS`](raw-SHA256SUMS) and
  [`storage-raw-SHA256SUMS`](storage-raw-SHA256SUMS).

## Limitations, anomalies, and robustness checks

- Each point currently has one run: 30 seconds for the in-memory group and 60
  seconds for the storage-tier group. The data can distinguish differences in
  the tens of percent or multiples, but not a stable 1%–3% advantage. The
  640- and 1,280-connection points should be interleaved and repeated three
  times.
- One client may still cap peak results. At the in-memory peak points, average
  client CPU use was 11.38 cores for Keylane GET, 14.27 for Keylane SET, 13.29
  for Redis GET, 12.41 for Redis SET, 12.85 for Valkey GET, and 11.16 for
  Valkey SET. Keylane SET was especially close to the client CPU limit, so the
  observed 984k QPS may not be the server limit.
- Persistence was fully disabled during the Redis and Valkey formal windows.
  Keylane wrote values to raw NVMe, but this report does not claim equivalent
  crash-durability semantics among the three. This experiment measures request
  path performance, not equal-durability cost.
- The experiment groups are not interchangeable. Redis and Valkey results use
  a 10 GB working set that fits entirely in DRAM; Dragonfly and Garnet use a
  1 TB storage-tier working set. The storage-tier group is also not a
  single-variable backend experiment: Keylane sees six independent raw devices,
  while Dragonfly and Garnet see RAID0/XFS files and use different memory
  budgets, cache policies, and background maintenance.
- One excluded Keylane preparation run encountered a general-protection fault
  while requesting metrics after recovering an existing 10M-key dataset and
  overwriting it again. The cause has not been established. After fully
  clearing the drives and avoiding active metrics scraping, all ten formal
  Keylane points completed without disconnects. The anomalous log remains in
  the raw result directory and does not establish metrics as the cause.
- All formal GET workloads had zero misses. Each of the 146 formal result files
  contains exactly one complete `Totals` row. All five Redis and Valkey thread
  settings matched their server configuration readback. SHA-256 verification
  passed for all six result directories.

## Recommended next steps

1. Interleave and repeat the three best in-memory configurations three times at
   640 and 1,280 connections, reporting the mean, standard deviation, and worst
   p99.99. Repeat the three storage-tier configurations at the same points.
2. Restore the second client or add client CPU capacity to determine whether
   the current client caps Keylane SET and Redis/Valkey GET.
3. Reproduce and isolate the excluded Keylane general-protection fault by
   separating overwrite-after-recovery, background flush, and metrics scraping.
4. For a production-cost comparison, add Redis and Valkey AOF `everysec` and a
   clearly defined Keylane fsync policy instead of treating the no-persistence
   in-memory results as a production conclusion.
5. Standardize the 1 TB group on either raw devices or RAID0/XFS and use equal
   memory budgets to separate request-path differences from storage topology
   and caching policy.

## Further questions

- Would 12 Redis or Valkey I/O threads outperform 8 or 16 on this 8C/16T host?
- How do the relative results change with 64-byte or 4 KiB values, or a skewed
  hot-key distribution?
- Above 1M QPS with two clients, which resource saturates first: server, client,
  or network?
- If Dragonfly and Garnet used the same raw-device topology where supported, or
  Keylane used the same RAID0/XFS file layout, how much of the 1 TB gap would
  remain?
