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

Each benchmark topic has one report at a stable path. Refresh that report when
new measurements are available; record the tested version, commit and date
inside the report. Earlier report revisions remain in Git history.

Each report provides an English `README.md` and a Simplified Chinese
`README.zh-CN.md`.

每类测试使用固定目录，更新结果时直接替换同类报告；测试版本、提交和日期写在报告内。

| Report | English | 简体中文 |
|---|---|---|
| In-memory Redis / Valkey: 2 / 16 vCPUs, up to 1B keys | [English](lavik-vs-memory-databases/README.md) | [简体中文](lavik-vs-memory-databases/README.zh-CN.md) |
| 1TB versus Dragonfly, Garnet, Kvrocks, Pika and Tendis | [English](lavik-storage-peers-1tb/README.md) | [简体中文](lavik-storage-peers-1tb/README.zh-CN.md) |
| Aerospike YCSB A/B/C/D throughput and tail latency | [English](lavik-aerospike-ycsb/README.md) | [简体中文](lavik-aerospike-ycsb/README.zh-CN.md) |
| SPDK 48-hour online stability | [English](lavik-spdk-48h-stability/README.md) | [简体中文](lavik-spdk-48h-stability/README.zh-CN.md) |

Evidence availability, reproduction commands and measurement limits are described
in each report.
