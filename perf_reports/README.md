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

Lavik benchmark results, test configurations, and reproduction commands.
Each report provides an English `README.md` and a Simplified Chinese
`README.zh-CN.md`.

每份报告默认展示英文版，点击“简体中文”进入中文版。

| Report | Test date | English | 简体中文 |
|---|---|---|---|
| SPDK 48-hour online stability | 2026-08-15 | [English](lavik-spdk-48h-stability-2026-08-15/README.md) | [简体中文](lavik-spdk-48h-stability-2026-08-15/README.zh-CN.md) |
| One billion keys: SPDK value-size scaling on 16 workers | 2026-08-31 | [English](lavik-spdk-dfly-bench-1b-value-size-limit-16worker-2026-08-31/README.md) | [简体中文](lavik-spdk-dfly-bench-1b-value-size-limit-16worker-2026-08-31/README.zh-CN.md) |
| SPDK vs. raw io_uring: 500M keys, 2 KiB values | 2026-08-26 | [English](lavik-spdk-vs-iouring-500m2k-memtier-valkey-12c-2026-08-26/README.md) | [简体中文](lavik-spdk-vs-iouring-500m2k-memtier-valkey-12c-2026-08-26/README.zh-CN.md) |
| Persistence and storage tiers: Dragonfly, Garnet, Kvrocks, Pika, Tendis, KeyDB, and Azure Managed Redis | 2026-08-11 | [English](lavik-vs-dragonfly-tiering-2026-08-11/README.md) | [简体中文](lavik-vs-dragonfly-tiering-2026-08-11/README.zh-CN.md) |
| v0.1.0-beta.1 release: SPDK versus peers | — | [English](lavik-v0.1.0-beta.1-spdk-vs-peers-2026-09-18/README.md) | [简体中文](lavik-v0.1.0-beta.1-spdk-vs-peers-2026-09-18/README.zh-CN.md) |
| Lavik–Aerospike YCSB A/B/C/D throughput and tail latency | 2026-09-16 | [English](ycsb-rerun-2026-09-13/README.md) | [简体中文](ycsb-rerun-2026-09-13/README.zh-CN.md) |

The YCSB directory name retains its original batch date; its current report covers
the fresh-data rerun on September 16. Evidence availability and measurement limits
are described in each report. Historical reproduction commands can depend on the
original machines, dedicated devices, software revisions, and external raw logs.
