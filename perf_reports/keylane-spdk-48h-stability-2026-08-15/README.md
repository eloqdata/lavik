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

# Keylane SPDK 48-hour online stability experiment (2026-08-15)

**English** | [简体中文](README.zh-CN.md) | [All reports](../README.md)

> Historical benchmark of Keylane, the project now named Lavik. Product names,
> versions, commands, and measurements describe the original test; this is not a
> measurement of the current Lavik release.
> Restored from the [2026-09-15 archive](https://github.com/eloqdata/lavik/tree/eedb3080d808769519d93e971195b53b38b09c1e/perf_reports).

## Conclusion

Keylane ran continuously for a strict 48-hour window with the final defrag settings, serving 400 million records with 1–4 KB values at 100,000 QPS and a 95% read / 5% overwrite mix. The window completed approximately 17.280 billion GET/SET operations without a service restart, error, memory rejection, or loss of database entries.

Server-side latency remained stable:

- Mean p99.9 was `0.944 ms`; the highest five-minute point was `2.184 ms`.
- Mean p99.99 was `2.383 ms`; the highest five-minute point was `2.810 ms`.
- Both p99.9 and p99.99 stayed below `3 ms` at all 577 valid five-minute points.

Disk usage followed a sawtooth pattern as blocks containing old versions became eligible for defrag, rather than remaining flat. Combined usage across the two SPDK devices ranged from 1,268.1 to 1,493.1 GiB. It ended at 1,364.7 GiB, down 128.4 GiB from the peak; the final 24 hours had a net decrease of 52.6 GiB. Disk space did not deplete monotonically during the window.

This experiment supports stable operation at 100,000 QPS for 48 hours under this workload and the final defrag settings, with server-side p99.99 below 3 ms. The conclusion applies only to the reported configuration and observation window; it does not establish indefinite steady-state operation for arbitrary workloads.

## Grafana screenshot

<img width="1512" height="949" alt="Grafana dashboard for the 48-hour stability run" src="https://github.com/user-attachments/assets/4783c5a6-80ec-48ec-b048-bb1a7764054d" />

## 48-hour results

Latency is calculated from Keylane Prometheus histograms and measures server-side command execution, excluding network transport and socket response writes. Means and maxima use all five-minute windows within the strict 48-hour interval. Only points with QPS between 90,000 and 110,000 are retained, excluding incomplete samples at the interval boundaries.

| Metric | Result |
| --- | ---: |
| Time range | 2026-08-13 07:06:30–2026-08-15 07:06:30 UTC |
| Valid five-minute windows | 577 |
| Mean QPS | 99,999.9 |
| Minimum five-minute QPS | 99,993.2 |
| Maximum five-minute QPS | 100,002.7 |
| Mean p99.9 | 0.944 ms |
| Minimum p99.9 | 0.487 ms |
| Maximum p99.9 | 2.184 ms |
| Mean p99.99 | 2.383 ms |
| Minimum p99.99 | 1.515 ms |
| Maximum p99.99 | 2.810 ms |

100,000 QPS is a client rate limit, not a peak-throughput measurement. Stability here means keeping up with the prescribed load throughout continuous background reclamation; it does not imply the system's maximum QPS.

## Data integrity and service state

| Check | 48-hour result |
| --- | ---: |
| GET increment | 16,415,992,616 |
| SET increment | 863,999,609 |
| Combined GET/SET increment | 17,279,992,225 |
| Successfully defragmented blocks | 525,153 |
| Defrag `error` | 0 |
| Defrag `resource_exhausted` | 0 |
| Commands rejected for memory | 0 |
| Service restarts | 0 |
| `DBSIZE` after the test | 400,000,000 |

The actual GET:SET count ratio was 95:5. The test used neither DEL nor TTL, and Tomb Raider was dynamically disabled. Defrag reclaimed old physical versions created by overwrites. This validates overwrite reclamation, not cleanup stability under large volumes of tombstones or expired TTL data.

After load generation stopped, Keylane remained `active (running)`, systemd reported `NRestarts=0`, and the test window contained no warning/error journal entries.

## Memory stability

| Metric | Start | End | 48-hour maximum |
| --- | ---: | ---: | ---: |
| RSS | 37.388 GiB | 37.392 GiB | 37.399 GiB |
| Keylane accounted memory | 37.352 GiB | 37.354 GiB | 37.354 GiB |

RSS varied by approximately 11 MiB at most and did not grow with overwrite count. Post-test `INFO memory` reported `oom_rejected_commands=0`; the process used mimalloc.

## Disk space and defrag

Each of the two SPDK namespaces provided 1,920,370,475,008 bytes of usable data capacity, approximately 3.49 TiB in total. Keylane's `storage_available` already excludes the defrag reserve. This report defines used capacity as `capacity - available`; it is not Linux `df` usage.

| Metric | 48-hour result |
| --- | ---: |
| Initial usage | 1,268.1 GiB |
| Final usage | 1,364.7 GiB |
| Minimum usage | 1,268.1 GiB |
| Maximum usage | 1,493.1 GiB |
| Peak-to-end change | -128.4 GiB |
| Net change over the window | +96.6 GiB |
| Net change over the final 24 hours | -52.6 GiB |
| Mean defrag rate | 3.04 blocks/s |
| Maximum five-minute defrag rate | 14.12 blocks/s |

Do not extrapolate this curve linearly from a short interval. SET appends new versions. Blocks containing old versions become defrag candidates only after their live-data ratio falls below 50%, naturally producing growth followed by concentrated reclamation.

The 48-hour window began at a low point after reclamation, so final usage exceeded initial usage by 96.6 GiB. That alone does not establish long-term space balance. More significantly, usage declined after peaking at 1,493.1 GiB, with a further net decline over the final 24 hours; it was not still approaching the capacity limit at the end. This supports controlled space usage within this two-day window. Longer validation should cover multiple complete sawtooth cycles.

The run did not record `defrag_reclaimed_bytes_total` or `defrag_relocated_bytes_total`. Only completed-block counts and capacity changes can be cross-checked. Add both byte counters for a weeks-long steady-state validation.

## Final defrag settings

The following settings were applied dynamically before the strict 48-hour window and remained unchanged throughout it:

```bash
redis-cli -h 10.0.0.4 -p 6379 DEFRAG MAX-ACTIVE 1
redis-cli -h 10.0.0.4 -p 6379 DEFRAG BLOCK-SLEEP-MS 100
redis-cli -h 10.0.0.4 -p 6379 DEFRAG RECORD-SLEEP-US 0
redis-cli -h 10.0.0.4 -p 6379 TOMBRAIDER OFF
```

`MAX-ACTIVE` is a per-device limit, allowing at most two concurrent defrag jobs across the two disks. Block cooldown waits asynchronously after a block completes; it does not block an entire worker with sleep. Record sleep remains zero, while processing still yields cooperatively between records.

## Test environment

| Role | Azure VM size | CPU | Address |
| --- | --- | --- | --- |
| Server | `Standard_L16s_v3` | 16 vCPU, Intel Xeon Platinum 8370C | `10.0.0.4:6379` |
| Client | `Standard_L16s_v3` | 16 vCPU, Intel Xeon Platinum 8370C | `10.0.0.5` |

The server used 12 Keylane workers with systemd `AllowedCPUs=0-11`. IRQs 58–74 were spread over logical CPUs 12–15 to avoid directly competing with Keylane workers. Client memtier used CPUs 0–15.

Storage comprised two independent SPDK NVMe namespaces, without RAID:

```text
spdk://021d:00:00.0/1
spdk://69f9:00:00.0/1
```

Versions:

```text
Keylane: 13dab14e786fa3e9465d5e82780d53b83ba93268
Celer:   e394652ddacffd18854b927367911ba8d283f0ca
Binary SHA-256: 66d3a451f337e7f4852dc3f397757d4415ab680e7ebe892c4ea1c74e4b1cd8a4
```

## Test command

The database had already been loaded with 400 million `kv_` keys and random values of 1,000–4,000 bytes. The sustained workload used eight memtier threads with ten connections each, 80 connections total. Each connection was limited to 1,250 ops/s, giving a total limit of 100,000 ops/s.

```bash
taskset -c 0-15 memtier_benchmark \
  -t 8 -c 10 \
  -s 10.0.0.4 -p 6379 \
  --test-time 259200 \
  --distinct-client-seed \
  --ratio=1:19 \
  --key-prefix="kv_" \
  --key-minimum=1 \
  --key-maximum=400000000 \
  --random-data \
  --data-size-range=1000-4000 \
  --data-size-pattern=R \
  --hide-histogram \
  --print-percentiles="99.9,99.99" \
  --rate-limiting=1250 \
  --randomize
```

memtier actually ran for more than 48 hours. This report selects the final 48 hours after the final settings had stabilized, excluding the earlier tuning period.

## Monitoring definitions and verification queries

Prometheus ran on the client with 30-day retention and scraped Keylane metrics every five seconds. The report used these PromQL queries with five-minute rates as the stability aggregation interval:

```promql
# GET + SET QPS
sum(rate(keylane_command_calls_total{command=~"get|set"}[5m]))

# p99.9 / p99.99: replace P with 0.999 or 0.9999, respectively
histogram_quantile(
  P,
  sum by (le) (
    rate(keylane_command_duration_seconds_bucket{command=~"get|set"}[5m])
  )
)

# Total used capacity across both devices
sum(keylane_storage_capacity_bytes - keylane_storage_available_bytes)

# Successful defrag blocks per second
sum(rate(keylane_storage_defrag_runs_total{result="success"}[5m]))
```

The strict 48-hour interval is `2026-08-13 07:06:30` through `2026-08-15 07:06:30 UTC`.

## Scope and limitations

- This report covers 400M keys, 1–4 KB values, uniformly random keys, a 95:5 read/overwrite mix, 100k QPS, and two SPDK NVMe devices.
- Tomb Raider was disabled, so large DEL, TTL, and tombstone-scan workloads were not validated.
- The results cover one clear reclamation wave, not a weeks-long soak test. “Stable” means no failures, bounded p99.9/p99.99, and no monotonic space depletion within the strict 48-hour window.
- Prometheus quantiles use histogram interpolation and exclude client networking.
- Both VMs were on the same test network. Different CPUs, NVMe devices, key distributions, or write ratios require choosing defrag settings again.
