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

# Keylane and Aerospike: YCSB throughput and tail latency

**English** | [简体中文](README.zh-CN.md) | [All reports](../README.md)

> Historical benchmark of Keylane, the project now named Lavik. Product names,
> versions, commands, and measurements describe the original test; this is not a
> measurement of the current Lavik release.

Test date: 2026-09-16. Both databases were freshly loaded with **100 million records, each containing 10 × 128 B fields**. One client with 256 YCSB worker threads measured the complete A/B/C/D × 100K/unlimited matrix: 16 measurements of 300 seconds each. Warmup and measured phases had zero errors.

**Aerospike used its default performance settings and the host's original scheduling and IRQ configuration; Keylane used 12 workers with a separate 12+4 CPU/IRQ isolation policy.** These were the two requested deployment configurations, with different CPU allocations and database settings. Effective configuration was recorded and checked before and after every phase.

## Hardware, network, and storage

| Item | Configuration |
|---|---|
| Server | Azure `Standard_L16aos_v4`, AMD EPYC 9V74, 16 logical CPUs / 8 physical cores (SMT), approximately 125 GiB RAM |
| Client | Azure `Standard_F16als_v7`, AMD EPYC 9V45, 16 logical CPUs / 16 physical cores, approximately 32 GiB RAM; Java 17 |
| Operating system | Ubuntu 24.04, Linux `6.17.0-1022-azure`, x86_64 |
| Network | One client `172.16.0.5` → server `172.16.0.4`; both databases used kernel networking and the same mlx5 NIC |
| Physical storage | Six Microsoft NVMe Direct Disk v2 devices, Linux RAID0 `/dev/md127`, chunk=512 KiB, approximately 10.48 TiB total |
| Aerospike partition | `/dev/md127p3`, 1,099,494,850,560 B, approximately 1 TiB, raw block device |
| Keylane partition | `/dev/md127p4`, 1,099,494,850,560 B, approximately 1 TiB, raw block device |
| Partition initialization | Both new partitions were aligned to 24 MiB and completely zeroed, without filesystems; existing p1/p2 GET datasets were retained |
| Background monitoring | Prometheus / Grafana remained on the client; the other database's benchmark did not run concurrently with a measurement |

The [machine inventory](evidence/inventory.json) includes VM metadata, CPU topology, kernel, routes, and client JAR SHA values. The [partition record](evidence/storage-after.json) and [zeroing evidence](evidence/zeroing.json) preserve device identities and boundaries. The databases used the same RAID0 sequentially, without concurrent disk contention.

## Dataset and workloads

| Item | Aerospike | Keylane |
|---|---:|---:|
| Records at the start of the measured matrix | 100,000,000 | 100,000,000 |
| Field-value bytes per record | 10 × 128 B = 1280 B | 10 × 128 B = 1280 B |
| Initial field-value payload | 128 GB (approximately 119.21 GiB) | 128 GB (approximately 119.21 GiB) |
| Random read range | Original 100,000,000 keys | Original 100,000,000 keys |
| Records at the end of the complete matrix | 108,328,377 | 109,699,918 |

Payload counts only field values, excluding keys, field names, indexes, and record-format overhead. Both databases used the same hashed key naming and fixed field sizes and were loaded from empty databases. Successful INSERT counts and server-side record increments were both checked against 100 million. After loading, each database was stopped cleanly and recovered. Record counts, lengths of all ten fields, and their SHA256 values at eight distributed keys were verified before warmup and measurement.

| Workload | READ | UPDATE | INSERT | Read distribution |
|---|---:|---:|---:|---|
| A | 50% | 50% | 0 | Uniform |
| B | 95% | 5% | 0 | Uniform |
| C | 100% | 0 | 0 | Uniform |
| D-Uniform | 95% | 0 | 5% | Uniform over the fixed original 100 million keys |

D does not read records inserted during the run, so it is D-Uniform. Both databases started at the same size. Successful D inserts differed with throughput, producing different final record counts. Both used identical, non-overlapping phase insert-ID starting points: 2 billion / 4 billion for 100K warmup / measurement and 6 billion / 8 billion for unlimited warmup / measurement. In D, `recordcount` specifies the starting insert ID, while `insertstart=0, insertcount=100000000` fixes the read domain.

## Host configuration: original Aerospike policy and separate Keylane isolation

| Item | Aerospike | Keylane |
|---|---|---|
| Process CPU availability | 0–15, 16 logical CPUs / 8 physical cores; no additional CPU-affinity options | 0–11, 12 logical CPUs / 6 physical cores; 12 pinned workers |
| systemd placement | Default `system.slice`, without AllowedCPUs/CPUAffinity settings | Dedicated `keylane-bench.slice`, AllowedCPUs/CPUAffinity=0–11 |
| System tasks | No additional CPU restrictions on `system.slice`, `user.slice`, or `init.scope` | These tasks restricted to CPUs 12–15 |
| Unbound workqueue | Original `ffff`, CPUs 0–15 | `f000`, CPUs 12–15 |
| Test NIC IRQs | Original driver layout: 16 completion IRQs on CPUs 0–15 | The NIC's 17 IRQs distributed round-robin over CPUs 12–15 |
| irqbalance | Not originally installed on the host; remained inactive | Also remained inactive |
| RPS/XPS and the other NIC | Original settings | Original settings retained |

All Aerospike thread affinities, `auto-pin=none`, and the original IRQ configuration were checked before and after every phase. Keylane tuning was applied only after all Aerospike tests completed and Aerospike stopped; the original state was restored afterward. Before and after each Keylane warmup and measurement, configured values and effective IRQ placement were checked. Interrupt-count deltas confirmed that this NIC's IRQs did not execute on CPUs 0–11. Fixed per-CPU kernel threads and managed NVMe IRQs are not arbitrarily movable system tasks.

[Original host snapshot](evidence/host-original.json) · [Keylane tuning evidence](evidence/keylane-policy-applied.json) · [Host restoration evidence](evidence/host-restored.json) · [Affinity restoration for newly created helper threads](evidence/inherited-affinity-restored.json)

## Database settings

| Setting | Aerospike | Keylane |
|---|---|---|
| Version | Community Edition 8.1.2.4-4 | `f666837b0038ab65564a17cb3a0bca8530f8e1be` |
| Networking and storage | Kernel networking, `storage-engine device`, raw RAID0 partition | Kernel networking + io_uring, raw RAID0 partition; SPDK disabled |
| Concurrency | Effective defaults `service-threads=80`, `auto-pin=none` | `--threads 12 --pin-workers` |
| Flushing | Defaults `flush-size=1 MiB`, `flush-max-ms=1000` | 128 KiB write chunks, `--flush-max-ms 100` |
| Write and read caches | Defaults `max-write-cache=64 MiB`, `post-write-cache=256 MiB`, `read-page-cache=false` | This revision's default storage settings |
| Index budget | Default `indexes-memory-budget=0`, without a separate budget cap | This revision's default index settings |
| Data and replication | Namespace `ycsb`, single node RF=1; default TTL 0 | Single instance DB0, no replica configured |
| GC/reclamation | Server defaults | GC/defrag enabled, `max_active_per_device=8`, no extra sleep |
| READ | All 10 fields | `HGETALL`, all 10 fields |
| UPDATE | `REPLACE_ONLY`, all 10 fields | `HREPLACE`, all 10 fields |
| INSERT | `CREATE_ONLY` | `HMSET` creates a new Hash, `redis.scanindex=none` |
| Client | YCSB Aerospike binding, Java client 3.1.2 | YCSB Redis binding, Jedis 3.9.0 |

Aerospike configuration specified only the required cluster-name, listening/single-node communication, namespace, RF=1, and block device. It did not override thread count, auto-pin, caches, flushing, index budget, or reclamation. The file-descriptor limit was the package systemd unit's 100,000. The complete [configuration](evidence/aerospike.conf), [actual startup command](evidence/aerospike-measured-server-command.json), and [effective settings](evidence/aerospike-measured-ready.json) are retained. Default values refer to queries from this run.

Keylane reused the original report's binary: GCC 13.3 / Release / O3 / native / LTO, SHA256 `9162d45032c34a9ddffffd1ef8137495237256173092aa931b65890dc12b7266`. The [build evidence](evidence/keylane-build.json) and [startup command](evidence/hreplace-measured-server-command.json) record all settings. 100ms is the periodic flush trigger interval; an ordinary successful write does not promise per-write synchronous durability. Data fdatasync precedes block-header fdatasync. Active expiration used this revision's default budget.

## Load generation and metric definitions

- Both databases used 256 YCSB worker threads. The Redis binding used one Jedis connection per thread; the Aerospike SDK managed its own connection pool. Equal thread counts do not imply equal total TCP connection counts.
- `fieldcount=10`, `fieldlength=128`, `fieldlengthdistribution=constant`, `readallfields=true`, `writeallfields=true`, `requestdistribution=uniform`, `insertorder=hashed`; SCAN and read-modify-write were disabled. Both client timeouts were 10000 ms.
- Aerospike ran first, then Keylane. Each database ran C/A/B/D, with 100K followed by unlimited for each workload. Each case first warmed up for one million operations in a separate JVM, then started a fresh measurement JVM for 300 seconds. Initial JIT/ramp-up was not removed from the measured window. Databases were neither cleared nor restarted between cases.
- 100K means `-target 100000` across all threads and operation types. The measured `operationcount=2000000000` was a loose cap; `maxexecutiontime=300` controlled actual termination.
- QPS = successful operations in the measured window / actual YCSB RunTime. Total QPS combines successful reads and writes; latency is reported separately by operation.
- Tail latency comes from the client's ordinary operation HDR histogram: p999=p99.9 and p9999=p99.99. It uses the complete five-minute window, without averaging interval percentiles. Intended histograms are retained separately in the raw logs.
- Each case is one five-minute measurement. Results describe this dataset state, execution order, and deployment configuration; variability across repeated independent runs was not estimated.

| Database | Measured window (UTC) | Cases |
|---|---|---:|
| Aerospike | 2026-09-16T03:05:56Z – 2026-09-16T03:47:32Z | 8 |
| Keylane | 2026-09-16T03:54:38Z – 2026-09-16T04:35:31Z | 8 |

## Throughput and tail latency

Each cell is **operation QPS; p99 / p999 / p9999 (ms)**. Values are rounded independently, so displayed operation QPS may sum to one more or less than total QPS.

| Workload | Rate limit | Operation | Aerospike (defaults, original host policy) | Keylane (100ms, 12+4 isolation) |
|---|---|---|---:|---:|
| A | Unlimited | Total QPS | 489,819 | 511,169 |
| A | Unlimited | READ | 244,911; 1.331 / 3.427 / 5.599 | 255,555; 2.475 / 4.603 / 6.207 |
| A | Unlimited | UPDATE | 244,908; 1.121 / 2.089 / 3.407 | 255,615; 1.666 / 3.185 / 4.811 |
| A | 100K | Total QPS | 99,905 | 99,963 |
| A | 100K | READ | 49,959; 0.439 / 0.726 / 1.516 | 49,989; 0.467 / 0.797 / 1.268 |
| A | 100K | UPDATE | 49,946; 0.361 / 0.551 / 1.366 | 49,975; 0.378 / 0.668 / 1.502 |
| B | Unlimited | Total QPS | 445,515 | 540,102 |
| B | Unlimited | READ | 423,238; 1.285 / 2.793 / 4.323 | 513,100; 0.818 / 2.937 / 4.643 |
| B | Unlimited | UPDATE | 22,277; 1.093 / 1.843 / 3.321 | 27,002; 0.656 / 1.979 / 3.879 |
| B | 100K | Total QPS | 99,874 | 99,973 |
| B | 100K | READ | 94,878; 0.447 / 0.595 / 1.359 | 94,973; 0.465 / 0.929 / 1.317 |
| B | 100K | UPDATE | 4,996; 0.373 / 0.498 / 1.288 | 4,999; 0.380 / 0.718 / 1.257 |
| C | Unlimited | READ | 445,698; 1.201 / 2.623 / 4.735 | 550,587; 0.740 / 2.719 / 4.411 |
| C | 100K | READ | 99,882; 0.448 / 0.564 / 1.370 | 99,956; 0.469 / 0.638 / 1.317 |
| D | Unlimited | Total QPS | 448,832 | 540,275 |
| D | Unlimited | READ | 426,400; 1.216 / 2.623 / 4.635 | 513,278; 0.772 / 3.127 / 4.903 |
| D | Unlimited | INSERT | 22,432; 1.037 / 1.690 / 2.271 | 26,997; 0.639 / 2.311 / 4.291 |
| D | 100K | Total QPS | 99,911 | 99,970 |
| D | 100K | READ | 94,918; 0.434 / 0.555 / 1.371 | 94,974; 0.425 / 0.559 / 1.275 |
| D | 100K | INSERT | 4,993; 0.373 / 0.485 / 1.269 | 4,996; 0.364 / 0.501 / 1.331 |

Under these deployments, Keylane's unlimited-throughput differences relative to Aerospike were A **+4.36%**, B **+21.23%**, C **+23.53%**, and D **+20.37%**. Per-operation tails are shown above. Approaching the 100K throughput target does not itself imply lower tail latency. CPU allocations, host policies, flushing, caches, and other settings differ, so these results cannot be attributed solely to IRQ tuning or any single parameter.

Compare throughput and tails separately: **Aerospike had lower p999 for every unlimited operation** in this run. **Keylane had lower p99 for B/C/D**, while Aerospike had lower values at all three reported tail percentiles for A READ/UPDATE. Keylane A's [interval throughput](evidence/fresh100m-20260916-hreplace-a-c256-measured.log) was approximately 530–540 thousand QPS early and 470 thousand QPS later. Its [end-of-window snapshot](evidence/fresh100m-20260916-hreplace-a-c256-measured.host-after.json) recorded eight active GC jobs. Table averages and percentiles include this entire progression.

## Raw evidence and offline verification

[Results JSON](results.json) · [Operation CSV](summary.csv) (including average/maximum latency and success counts) · [All phases](evidence/phases.json) · [Initial test runner](evidence/runner.py) · [Keylane continuation script](evidence/resume_keylane.py) · [File checksums](SHA256SUMS)

After Aerospike completed, the first switchover stopped because it immediately checked an IRQ affinity change that would become effective only on the next interrupt. The host was restored automatically; Keylane had not yet started. The switchover check was then corrected to confirm effective IRQ placement after load traffic, retaining all completed Aerospike results. This was a setup-phase failure; no measured window was replaced. Original scripts, [continuation evidence](evidence/resume.json), and failure records are archived.

Offline verification reparses raw logs and checks both fresh 100-million-record loads, all 32 warmup/measured phases, 16 measured windows, 28 operation results, server command/record deltas, and effective host/database configuration in each phase. It also checks both language versions of the results table and archived file hashes. Python 3.11 or newer is sufficient; it does not start databases or modify the system:

```sh
python3 perf_reports/ycsb-rerun-2026-09-13/verify_report.py
```

At completion, the host's original policy had been restored, Keylane had stopped cleanly, and the new default-configured Aerospike instance had recovered with its data verified. Both YCSB partitions and the original p1/p2 GET datasets were retained; see the [completion evidence](evidence/complete.json). Archived benchmark scripts record this particular batch. A rerun requires a new batch and empty data media; consumed D insert ranges cannot be reused.
