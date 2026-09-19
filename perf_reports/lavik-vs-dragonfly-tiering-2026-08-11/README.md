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

# Lavik versus Redis-compatible storage tiers

**English** | [简体中文](README.zh-CN.md)

The downloaded [Lavik](https://github.com/eloqdata/lavik/releases/tag/v0.1.0-beta.1) release is compared with Dragonfly Tiered Storage, Garnet Storage Tier, Apache Kvrocks, Pika, Tendis, and KeyDB On Flash over **200 million keys with random 1–4 KB values**. Each product/backend starts with freshly initialized storage and an independent full load.

Lavik runs with `--flush-max-ms=100`, setting the partial-block flush deadline to **100 ms**. The published executable was tested with this explicit flag.

## Results

![Throughput comparison](tiering-throughput.svg)

| Workload | System | QPS | p99 (ms) | p99.9 (ms) |
|---|---|---:|---:|---:|
| GET | Lavik SPDK | 316,139.85 | 0.383 | 0.607 |
| GET | Lavik raw io_uring | 287,278.40 | 0.423 | 0.679 |
| GET | Lavik per-drive XFS io_uring | 282,293.98 | 0.431 | 0.655 |
| GET | Garnet Storage Tier | 208,347.59 | 2.447 | 4.767 |
| GET | Dragonfly Tiered Storage | 187,153.99 | 3.327 | 14.719 |
| GET | Pika | 93,320.87 | 1.911 | 3.631 |
| GET | Apache Kvrocks | 82,949.14 | 1.863 | 2.399 |
| GET | Tendis | 87,368.36 | 1.279 | 9.663 |
| GET | KeyDB On Flash | 7,633.56 | 13.887 | 19.583 |
| GET + SET (1:1) | Lavik SPDK | 354,235.51 | 0.767 | 1.639 |
| GET + SET (1:1) | Lavik raw io_uring | 330,474.78 | 0.751 | 1.583 |
| GET + SET (1:1) | Lavik per-drive XFS io_uring | 320,144.74 | 0.791 | 1.567 |
| GET + SET (1:1) | Garnet Storage Tier | 287,567.52 | 1.887 | 3.279 |
| GET + SET (1:1) | Dragonfly Tiered Storage | 183,506.95 | 4.383 | 10.943 |
| GET + SET (1:1) | Pika | 76,016.72 | 3.871 | 8.831 |
| GET + SET (1:1) | Apache Kvrocks | 99,210.78 | 1.999 | 2.879 |
| GET + SET (1:1) | Tendis | 110,891.11 | 1.831 | 12.543 |
| GET + SET (1:1) | KeyDB On Flash | 7,048.80 | 17.535 | 23.295 |
| SET | Lavik SPDK | 363,847.67 | 1.399 | 2.671 |
| SET | Lavik raw io_uring | 360,064.39 | 1.399 | 2.703 |
| SET | Lavik per-drive XFS io_uring | 359,652.36 | 1.399 | 2.655 |
| SET | Garnet Storage Tier | 388,682.07 | 1.439 | 3.487 |
| SET | Dragonfly Tiered Storage | 181,607.64 | 5.023 | 10.623 |
| SET | Pika | 76,481.51 | 4.015 | 8.031 |
| SET | Apache Kvrocks | 106,180.99 | 1.679 | 2.383 |
| SET | Tendis | 173,639.10 | 1.231 | 1.927 |
| SET | KeyDB On Flash | 6,672.89 | 19.327 | 24.319 |

![p99.9 latency comparison](tiering-p999.svg)

Lavik SPDK leads GET and mixed throughput at **316,140 / 354,236 QPS**. Garnet leads SET at **388,682 QPS**, compared with **363,848 QPS** for Lavik SPDK. Lavik SPDK GET / mixed / SET p99.9 is **0.607 / 1.639 / 2.671 ms**.

## Workload and hardware

| Role | Azure VM size | Address | CPU | RAM |
|---|---|---|---|---|
| Server | `Standard_L16aos_v4` | `172.16.0.4:6379` | AMD EPYC 9V74 · CPU 0–15 (8 cores / 16 threads) | about 126 GiB |
| Client | `Standard_F16als_v7` | `172.16.0.5` | AMD EPYC 9V45 · CPU 0–15 | about 31 GiB |

The server uses six dedicated 1,919,850,381,312-byte NVMe drives. Lavik SPDK uses six independent namespaces; raw io_uring uses six independent block devices. File io_uring uses one newly formatted XFS filesystem and one fully preallocated 1,600 GiB file on each drive. All other products use RAID0 across the same six drives, formatted with XFS and mounted with `noatime`. RAID and filesystems are recreated for every product. OS and workspace devices are outside the tested storage set.

memtier 2.5.1 loads keys `kv_1` through `kv_200000000` using 16 threads and 640 connections, partitioned sequential keys, random 1,000–4,000-byte values, and distinct client seeds. The formal sequence is GET, 1:1 GET/SET, then SET; each runs for 300 seconds with 8 threads, 80 connections, pipeline 1, uniform random keys, distinct client seeds, and no rate limit. Each configuration receives one full load followed by those three windows. The process and dataset remain intact between workloads. Only Dragonfly receives a 180-second random-GET warmup, matching the original protocol.

## Server cost

All local database configurations use the same `Standard_L16aos_v4` VM in **Japan East**, so their server VM rate is the same. The Linux pay-as-you-go retail rate is **$2.128/hour**, or **$1,553.44/month at 730 hours**. This estimate excludes the benchmark client, managed disks, network charges, and taxes; it applies no reservation, savings-plan, Spot, or negotiated discount.

The rate comes from the [Azure Retail Prices API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices). The exact query, selected meter, retrieval time, and monthly calculation are saved in [pricing.json](pricing.json), with the complete API response in the evidence archive. Azure Managed Redis is outside this rerun, so the previous managed-service price comparison is not carried forward.

## Product settings and reproduction

| Product | Version / configuration |
|---|---|
| Lavik | Published release package; kernel TCP; SPDK, raw io_uring, and per-drive XFS files |
| Dragonfly | 1.40.1; 16 proactors; 64 GiB memory tier; buffered tier files; experimental cooling disabled |
| Garnet | 2.1.3; .NET 10.0.11; 64 GiB hybrid log, 32 GiB read cache, 4 GiB index; Native Libaio |
| Apache Kvrocks | 2.16.0; 16 workers; 80 GiB HCC block cache; BlobDB; no WAL or compression |
| Pika | v4.0.3 source (reports 4.0.2); 16 network / 32 request threads; 3 RocksDB instances; 24 GiB block cache + 32 GiB RTC cache |
| Tendis | 2.8.4-rocksdb-v8.5.3; 16 executors; 10 stores; shared 72 GiB cache; BlobDB; no WAL, binlog, or compression |
| KeyDB On Flash | 6.3.4; 4 server threads; 64 GiB hot tier; RocksDB WAL and automatic compaction retained |

The [SPDK setup guide](../../docs/operations/spdk-storage.md) covers release-package setup, device identity, hugepages, VFIO binding, launch, and restoration. For file/block provisioning, see [multi-device storage](../../docs/operations/multi-device-storage.md). The evidence archive records the exact launch command, environments, device serials and PCI addresses, initialization commands, and host restoration for every configuration under `flush100/runs/<system>/` for Lavik and `baseline/runs/<system>/` for peers.

Lavik uses its default worker count on CPU 0–15. The explicit launch settings are `--bind=172.16.0.4`, the storage backend and device/file paths, `--tomb-raider-interval-ms=0`, a log directory, `--flush-max-ms=100`, and `--spdk-max-completions-per-poll=16` for SPDK. SPDK reserves 8 GiB of hugepages. This dedicated VM requires VFIO no-IOMMU mode; the saved setting and hugepage count are restored after SPDK. Defrag retains the original per-device concurrency / block cooldown: 8 / 0 ms for loading and GET, 2 / 15 ms for mixed, and 6 / 0 ms for SET. Per-record sleep is 0. Exact commands and status are in the acquisition scripts and `*.defrag-before.txt`.

Peer configuration values are in [peer-configs.json](peer-configs.json); actual server commands are in the raw evidence. Kvrocks, Pika, and KeyDB are built from the original report's source tags. Dragonfly, Garnet, and Tendis use official release artifacts. Garnet's native library receives the Ubuntu libaio SONAME/layout adjustment; KeyDB receives GCC 13 header and dependency-link fixes. Binary hashes and adjustments are recorded in [binaries.json](binaries.json).

Every full load contains exactly 200 million completed SET operations, using partitioned sequential keys. Formal client windows report zero connection errors and zero GET misses. The products retain their specified cache, warmup, compaction, and persistence settings; no page cache is cleared between workloads.

[CSV](results.csv) · [Binary identity](binary.json) · [Peer identities](binaries.json) · [Protocol](protocol.json) · [Peer configurations](peer-configs.json) · [Raw evidence](evidence.tar.gz) · [File hashes](raw-SHA256SUMS)

The Lavik rerun additionally records INFO/DEFRAG state every five seconds. The [flush comparison](flush-comparison.csv) retains all nine original 1000 ms versus 100 ms points; the main table consistently uses 100 ms. [Source mappings](sources.json) identify each original memtier JSON in the evidence archive and its SHA-256.

## Rebuild charts

Install Matplotlib 3.11.2 and run `python build_assets.py` in this directory. The renderer checks all 27 CSV points before producing the SVG/PNG figures. `chart-SHA256SUMS` records their checksums.

Run `python3 validate_report.py` to verify the raw-source hashes, measured values, launch configuration, both tables, and figure checksums.
