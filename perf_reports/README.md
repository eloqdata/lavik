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

# Performance reports

These reports record benchmarks of Keylane before the project was renamed Lavik.
Each directory opens with an English `README.md` and provides a Simplified Chinese
`README.zh-CN.md`. Test versions, commands, configurations, and results retain their
historical meaning; they are not measurements of the current Lavik release.

每份报告默认展示英文版，点击“简体中文”进入中文版。报告保留测试时的名称、版本和数据。

| Report | Test date | English | 简体中文 |
|---|---|---|---|
| SPDK 48-hour online stability | 2026-08-15 | [English](keylane-spdk-48h-stability-2026-08-15/README.md) | [简体中文](keylane-spdk-48h-stability-2026-08-15/README.zh-CN.md) |
| One billion keys: SPDK value-size scaling on 16 workers | 2026-08-31 | [English](keylane-spdk-dfly-bench-1b-value-size-limit-16worker-2026-08-31/README.md) | [简体中文](keylane-spdk-dfly-bench-1b-value-size-limit-16worker-2026-08-31/README.zh-CN.md) |
| SPDK vs. raw io_uring: 500M keys, 2 KiB values | 2026-08-26 | [English](keylane-spdk-vs-iouring-500m2k-memtier-valkey-12c-2026-08-26/README.md) | [简体中文](keylane-spdk-vs-iouring-500m2k-memtier-valkey-12c-2026-08-26/README.zh-CN.md) |
| Persistence and storage tiers: Dragonfly, Garnet, Kvrocks, Pika, Tendis, KeyDB, and Azure Managed Redis | 2026-08-11 | [English](keylane-vs-dragonfly-tiering-2026-08-11/README.md) | [简体中文](keylane-vs-dragonfly-tiering-2026-08-11/README.zh-CN.md) |
| High concurrency: Redis/Valkey I/O threads and Dragonfly/Garnet storage tiers | 2026-09-06 | [English](keylane-vs-redis-valkey-iothreads-10g-1k-2026-09-06/README.md) | [简体中文](keylane-vs-redis-valkey-iothreads-10g-1k-2026-09-06/README.zh-CN.md) |
| Keylane–Aerospike YCSB A/B/C/D throughput and tail latency | 2026-09-16 | [English](ycsb-rerun-2026-09-13/README.md) | [简体中文](ycsb-rerun-2026-09-13/README.zh-CN.md) |

The YCSB directory name retains its original batch date; its current report covers
the fresh-data rerun on September 16. Evidence availability and measurement limits
are described in each report. Historical reproduction commands can depend on the
original machines, dedicated devices, software revisions, and external raw logs.
