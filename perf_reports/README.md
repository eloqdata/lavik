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
| v0.2.0 versus in-memory databases: 2 / 16 vCPUs, up to 1B keys | 2026-10-21 | [English](lavik-v0.2.0-vs-memory-databases-2026-10-21/README.md) | [简体中文](lavik-v0.2.0-vs-memory-databases-2026-10-21/README.zh-CN.md) |
| v0.2.0: 1TB versus Dragonfly, Garnet, Kvrocks, Pika and Tendis | 2026-10-21–2026-10-22 | [English](lavik-v0.2.0-storage-peers-1tb-2026-10-21/README.md) | [简体中文](lavik-v0.2.0-storage-peers-1tb-2026-10-21/README.zh-CN.md) |
| v0.2.0: Aerospike YCSB A/B/C/D throughput and tail latency | 2026-10-21 | [English](lavik-v0.2.0-aerospike-ycsb-2026-10-21/README.md) | [简体中文](lavik-v0.2.0-aerospike-ycsb-2026-10-21/README.zh-CN.md) |
| v0.1.0: 1TB versus Dragonfly, Garnet, Kvrocks, Pika and Tendis | 2026-10-13–14 | [English](lavik-v0.1.0-storage-peers-1tb-2026-10-14/README.md) | [简体中文](lavik-v0.1.0-storage-peers-1tb-2026-10-14/README.zh-CN.md) |
| v0.1.0 on 2 vCPUs: latest Redis / Valkey + Lavik 100M-key capacity | 2026-10-13 | [English](lavik-v0.1.0-2vcpu-latest-capacity-2026-10-13/README.md) | [简体中文](lavik-v0.1.0-2vcpu-latest-capacity-2026-10-13/README.zh-CN.md) |
| v0.1.0 release: Redis 8.10.2 / Valkey 9.1.2 and Lavik 1TB, defrag enabled | 2026-10-13 | [English](lavik-v0.1.0-spdk-vs-latest-2026-10-13/README.md) | [简体中文](lavik-v0.1.0-spdk-vs-latest-2026-10-13/README.zh-CN.md) |
| SPDK 48-hour online stability | 2026-08-15 | [English](lavik-spdk-48h-stability-2026-08-15/README.md) | [简体中文](lavik-spdk-48h-stability-2026-08-15/README.zh-CN.md) |
| v0.1.0 release: Aerospike YCSB A/B/C/D throughput and tail latency, defrag enabled | 2026-10-13 | [English](lavik-v0.1.0-aerospike-ycsb-2026-10-13/README.md) | [简体中文](lavik-v0.1.0-aerospike-ycsb-2026-10-13/README.zh-CN.md) |

Evidence availability and measurement limits are described in each report. Historical reproduction commands can depend on the
original machines, dedicated devices, software revisions, and external raw logs.
