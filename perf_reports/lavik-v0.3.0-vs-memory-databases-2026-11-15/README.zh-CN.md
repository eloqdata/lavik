# Lavik v0.3.0 与内存 Redis / Valkey 对比

[English](README.md) · 测试日期：2026-11-15（UTC）

## 结论

16 vCPU、10M keys 下，Lavik 达到 **1,168,681 GET ops/s、959,837 SET ops/s**，均高于本轮两款内存数据库的峰值。2 vCPU、1M keys 下，Lavik GET 峰值 **215,689 ops/s** 低于 Valkey 的 **238,151**，SET 峰值 **239,535 ops/s** 高于两者。另测 Lavik NVMe 上的 100M / 1B keys，容量曲线的数据量是内存对照的 **100 倍**。[逐点数据](https://github.com/eloqdata/lavik-agent/blob/0988de68a0f315269e1a77ec35de736a0bdd6ed0/perf_reports/lavik-v0.3.0-vs-memory-databases-2026-11-15/metadata/memtier-points.csv)。

本轮同机重测 v0.2.0 后，未发现测试矩阵中持续的明显回退。短窗口尾延迟异常及复测在下方单独保留；这不构成统计上的等价性证明。

实测源码提交为 [`dcb2dda96452`](https://github.com/eloqdata/lavik/commit/dcb2dda96452610b9aa2625ff2a26abb3303f9b5)，与 [v0.3.0](https://github.com/eloqdata/lavik/releases/tag/v0.3.0) tag 一致。测试使用发布前构建的标准 x86-64-v2/SPDK 包，版本输出为 `0.3.0-dev`，并非之后上传的 release 二进制。二进制 SHA-256 为 `c442735274c7bcbfaa8b59268b1f5f0f45ac36782ef48314e3b8e49f1857bf10`。CSV 中的 `main` 指这个确切提交。

## 2 vCPU

![2 vCPU throughput and p99.9](2vcpu-combined.svg)

每个对照产品、每种命令选取**完整连接数扫描**中峰值最高的一个 I/O 线程配置，吞吐和延迟曲线均固定使用它，不逐点拼接配置。表格包含全部连接数（包括 1280）。峰值表的延迟与该吞吐峰值来自**同一窗口**。

| Command | Product / keys | Workers / I/O threads | Connections | Peak ops/s | p99.9 ms |
| --- | --- | --- | --- | --- | --- |
| GET | Lavik v0.3.0 / 1M | 2 | 80 | 215,689 | 0.671 |
| GET | Lavik v0.3.0 / 100M | 2 | 80 | 215,536 | 0.647 |
| GET | Redis 8.10.2 / 1M | 2 | 1280 | 208,441 | 18.687 |
| GET | Valkey 9.1.2 / 1M | 2 | 80 | 238,151 | 0.503 |
| SET | Lavik v0.3.0 / 1M | 2 | 160 | 239,535 | 1.599 |
| SET | Lavik v0.3.0 / 100M | 2 | 320 | 230,794 | 3.887 |
| SET | Redis 8.10.2 / 1M | 2 | 32 | 190,106 | 0.255 |
| SET | Valkey 9.1.2 / 1M | 2 | 32 | 220,455 | 0.215 |

### 相同连接数

每格为 **ops/s / p99.9 ms**。

| Command | Connections | Lavik v0.3.0 / 1M | Lavik v0.3.0 / 100M | Redis 8.10.2 / 1M | Valkey 9.1.2 / 1M |
| --- | --- | --- | --- | --- | --- |
| GET | 16 | 127,675 / 0.279 | 126,953 / 0.199 | 164,117 / 0.167 | 217,251 / 0.143 |
| GET | 32 | 214,755 / 0.359 | 215,425 / 0.263 | 195,499 / 0.279 | 233,187 / 0.263 |
| GET | 80 | 215,689 / 0.671 | 215,536 / 0.647 | 204,387 / 0.775 | 238,151 / 0.503 |
| GET | 160 | 215,227 / 1.887 | 214,951 / 2.607 | 205,932 / 1.767 | 227,427 / 2.495 |
| GET | 320 | 215,142 / 3.615 | 215,166 / 3.535 | 205,515 / 4.319 | 224,037 / 5.279 |
| GET | 640 | 212,536 / 11.775 | 215,056 / 6.783 | 205,496 / 9.855 | 222,142 / 9.919 |
| GET | 1280 | 214,363 / 13.951 | 214,874 / 12.159 | 208,441 / 18.687 | 222,936 / 17.663 |
| SET | 16 | 177,915 / 0.207 | 201,847 / 0.167 | 162,613 / 0.167 | 201,029 / 0.151 |
| SET | 32 | 213,108 / 0.295 | 214,558 / 0.311 | 190,106 / 0.255 | 220,455 / 0.215 |
| SET | 80 | 232,442 / 0.759 | 224,756 / 1.151 | 187,409 / 0.719 | 197,883 / 0.895 |
| SET | 160 | 239,535 / 1.599 | 228,609 / 2.815 | 183,348 / 1.959 | 198,552 / 3.007 |
| SET | 320 | 238,512 / 4.895 | 230,794 / 3.887 | 179,628 / 4.799 | 194,950 / 4.735 |
| SET | 640 | 236,808 / 7.423 | 227,535 / 7.359 | 178,303 / 10.687 | 188,992 / 10.367 |
| SET | 1280 | 231,358 / 12.991 | 220,792 / 13.311 | 176,903 / 19.583 | 188,545 / 21.375 |

### 完整 I/O 线程扫描

| Product | I/O threads | GET peak ops/s (connections) | SET peak ops/s (connections) |
| --- | --- | --- | --- |
| Redis 8.10.2 | 1 | 192,048 (160) | 178,727 (160) |
| Redis 8.10.2 | 2 | 208,441 (1280) | 190,106 (32) |
| Valkey 9.1.2 | 1 | 186,115 (160) | 183,241 (1280) |
| Valkey 9.1.2 | 2 | 238,151 (80) | 220,455 (32) |

## 16 vCPU

![16 vCPU throughput and p99.9](16vcpu-combined.svg)

每个对照产品、每种命令选取**完整连接数扫描**中峰值最高的一个 I/O 线程配置，吞吐和延迟曲线均固定使用它，不逐点拼接配置。表格包含全部连接数（包括 1280）。峰值表的延迟与该吞吐峰值来自**同一窗口**。

| Command | Product / keys | Workers / I/O threads | Connections | Peak ops/s | p99.9 ms |
| --- | --- | --- | --- | --- | --- |
| GET | Lavik v0.3.0 / 10M | 16 | 1280 | 1,168,681 | 6.207 |
| GET | Lavik v0.3.0 / 1B | 16 | 1280 | 1,143,839 | 3.887 |
| GET | Redis 8.10.2 / 10M | 16 | 1280 | 971,136 | 7.167 |
| GET | Valkey 9.1.2 / 10M | 16 | 1280 | 919,758 | 7.391 |
| SET | Lavik v0.3.0 / 10M | 16 | 1280 | 959,837 | 7.199 |
| SET | Lavik v0.3.0 / 1B | 16 | 1280 | 929,346 | 5.279 |
| SET | Redis 8.10.2 / 10M | 16 | 1280 | 874,635 | 7.231 |
| SET | Valkey 9.1.2 / 10M | 8 | 1280 | 801,428 | 7.583 |

### 相同连接数

每格为 **ops/s / p99.9 ms**。

| Command | Connections | Lavik v0.3.0 / 10M | Lavik v0.3.0 / 1B | Redis 8.10.2 / 10M | Valkey 9.1.2 / 10M |
| --- | --- | --- | --- | --- | --- |
| GET | 160 | 376,725 / 0.903 | 382,250 / 0.823 | 388,562 / 0.871 | 464,368 / 0.879 |
| GET | 320 | 615,731 / 1.551 | 671,976 / 0.927 | 598,530 / 1.367 | 644,655 / 1.399 |
| GET | 640 | 870,897 / 3.599 | 1,009,460 / 1.719 | 796,158 / 3.439 | 803,926 / 3.663 |
| GET | 1280 | 1,168,681 / 6.207 | 1,143,839 / 3.887 | 971,136 / 7.167 | 919,758 / 7.391 |
| SET | 160 | 426,998 / 0.887 | 443,004 / 0.807 | 389,159 / 0.879 | 464,311 / 0.839 |
| SET | 320 | 632,927 / 1.735 | 692,511 / 0.919 | 583,883 / 1.391 | 648,985 / 1.343 |
| SET | 640 | 826,811 / 3.711 | 877,731 / 2.271 | 734,854 / 3.231 | 754,771 / 3.439 |
| SET | 1280 | 959,837 / 7.199 | 929,346 / 5.279 | 874,635 / 7.231 | 801,428 / 7.583 |

### 完整 I/O 线程扫描

| Product | I/O threads | GET peak ops/s (connections) | SET peak ops/s (connections) |
| --- | --- | --- | --- |
| Redis 8.10.2 | 1 | 153,518 (320) | 144,395 (320) |
| Redis 8.10.2 | 2 | 160,279 (320) | 148,925 (320) |
| Redis 8.10.2 | 4 | 435,077 (1280) | 401,351 (1280) |
| Redis 8.10.2 | 8 | 839,976 (1280) | 828,286 (1280) |
| Redis 8.10.2 | 16 | 971,136 (1280) | 874,635 (1280) |
| Valkey 9.1.2 | 1 | 151,579 (320) | 145,922 (320) |
| Valkey 9.1.2 | 2 | 158,728 (160) | 147,252 (640) |
| Valkey 9.1.2 | 4 | 437,382 (1280) | 398,036 (1280) |
| Valkey 9.1.2 | 8 | 849,172 (1280) | 801,428 (1280) |
| Valkey 9.1.2 | 16 | 919,758 (1280) | 785,046 (1280) |

## 同机 v0.2.0 回归检查

基线为官方 v0.2.0 包，提交 `2908f86f2770e9c6e8acc6585b3bedb4b44d1cd5`，在本轮相同机器上重测。44 个首轮同条件对照的 QPS 差异为 **−2.5% 至 +5.9%**。首轮最大降幅出现在 16 vCPU / 10M / SET / 160 连接；三轮复测中位数为 **428,765 → 431,976 ops/s（+0.75%）**，范围重叠。

两个短窗口 p99.9 复测曾分别升高约 78%（2 vCPU / 1M / GET / 160）和 28%（2 vCPU / 100M / SET / 160）。重新装载数据、每版各测三轮 120 秒后，中位数为 **1.871 → 1.863 ms** 和 **2.063 → 2.063 ms**，QPS 变化为 −0.34% / +3.55%。复测没有替换首轮结果；窗口百分位数的中位数不等于合并样本百分位数。

[44 个同条件对照](https://github.com/eloqdata/lavik-agent/blob/0988de68a0f315269e1a77ec35de736a0bdd6ed0/perf_reports/lavik-v0.3.0-vs-memory-databases-2026-11-15/metadata/memtier-comparison.csv) · [复测中位数和范围](https://github.com/eloqdata/lavik-agent/blob/0988de68a0f315269e1a77ec35de736a0bdd6ed0/perf_reports/lavik-v0.3.0-vs-memory-databases-2026-11-15/metadata/memtier-recheck-summary.csv) · [全部 272 个窗口](https://github.com/eloqdata/lavik-agent/blob/0988de68a0f315269e1a77ec35de736a0bdd6ed0/perf_reports/lavik-v0.3.0-vs-memory-databases-2026-11-15/metadata/memtier-points.csv)。

## 硬件与方法

| Item | 2 vCPU | 16 vCPU |
| --- | --- | --- |
| Server | Standard_L2aos_v5; EPYC 9V45; 1 physical / 2 logical CPUs; 16 GiB | Standard_L16aos_v4; EPYC 9V74; 8 physical / 16 logical CPUs; 128 GiB |
| Region | East US, zone 3 | Japan East, zone 2 |
| Client | Standard_L16aos_v5; CPUs 0–15 | Standard_F32als_v6; CPUs 0–15 (10M), 0–31 (1B) |
| NVMe | 1 × 1,439,887,785,984 bytes | 6 × 1,919,850,381,312 bytes |
| Hugepages | 2 GiB | 8 GiB |
| Client threads / window | 16 / 30s | 16 / 30s (10M); 32 / 60s (1B) |
| Connections | 16 / 32 / 80 / 160 / 320 / 640 / 1280 | 160 / 320 / 640 / 1280 |

各主机为 Ubuntu 24.04 / Linux 7.0.0-1017-azure。服务端和独立客户端位于同一 VNet、可用区，使用加速网络。产品及版本在相同服务端顺序运行。

- memtier 2.5.1 使用十进制键 `1..N`、1,024 字节值、均匀随机 GET 与覆盖 SET，各数据集独立装载。先 GET 后 SET，连接数递增。除 1B 无额外 GET 预热外，各配置先进行 10 秒 GET 预热。2 vCPU 与 1B 使用 `--distinct-client-seed`；10M 沿用历史种子策略。
- Lavik 使用 SPDK、2 / 16 个 worker。非默认设置为 `--tomb-raider-interval-ms=0`、`--spdk-max-completions-per-poll=16` 上表的 hugepage 预留，以及 1B 的 `--shutdown-checkpoint`。completion cap 沿用历史报告；不要与使用默认 cap 的 SPDK YCSB 实验混淆。
- Redis 8.10.2 / Valkey 9.1.2 为优化 jemalloc 构建，`save ""` 关闭自动 RDB 快照。装载后手工 SAVE，各 I/O 线程配置重新载入基线。完整线程扫描见上表。
- THP 为 `never`，overcommit 为 1。每个配置前后检查精确键数和五个样本值长度。272 个正式窗口均通过响应/错误检查，GET 零 miss，服务端采样 steal 均为零。[逐窗口遥测](https://github.com/eloqdata/lavik-agent/blob/0988de68a0f315269e1a77ec35de736a0bdd6ed0/perf_reports/lavik-v0.3.0-vs-memory-databases-2026-11-15/metadata/memtier-telemetry.csv) 包含 RSS 和客户端 CPU；SPDK hugepage 不含在 RSS 中。16 vCPU / 10M 的客户端最高占用约 14.3/16 个分配 CPU，因此峰值不能证明服务器的绝对上限。

相对十月报告，本轮内核由 6.17.0-1022-azure 更新，2 vCPU 的客户端改为 L16aos_v5，对照产品构建不包含 TLS（两轮流量均为明文）。因此回归判断使用**本轮同机 v0.2.0 重测**，不能直接减去旧报告数值。

## 证据与复现

[完整证据](https://github.com/eloqdata/lavik-agent/tree/0988de68a0f315269e1a77ec35de736a0bdd6ed0/perf_reports/lavik-v0.3.0-vs-memory-databases-2026-11-15) 与 v0.2.0 数据一同保存在 **eloqdata/lavik-agent**，链接固定至提交 `0988de68a0f3`。[Metadata](https://github.com/eloqdata/lavik-agent/tree/0988de68a0f315269e1a77ec35de736a0bdd6ed0/perf_reports/lavik-v0.3.0-vs-memory-databases-2026-11-15/metadata) 保留完整 I/O 扫描、首轮点、复测与遥测。CSV 的 `source` 字段对应[原始归档](https://github.com/eloqdata/lavik-agent/tree/0988de68a0f315269e1a77ec35de736a0bdd6ed0/perf_reports/lavik-v0.3.0-vs-memory-databases-2026-11-15/raw)内的 receipt 路径；归档逐字节保留测量计数、延迟直方图、控制台输出、命令、装载/预热记录及主机检查。[来源记录](https://github.com/eloqdata/lavik-agent/blob/0988de68a0f315269e1a77ec35de736a0bdd6ed0/perf_reports/lavik-v0.3.0-vs-memory-databases-2026-11-15/metadata/provenance.json) 包含二进制身份、原始归档哈希和路径映射。

数据仓库的 [SHA-256 清单](https://github.com/eloqdata/lavik-agent/blob/0988de68a0f315269e1a77ec35de736a0bdd6ed0/perf_reports/lavik-v0.3.0-vs-memory-databases-2026-11-15/SHA256SUMS) 校验归档和元数据，[原始文件清单](https://github.com/eloqdata/lavik-agent/blob/0988de68a0f315269e1a77ec35de736a0bdd6ed0/perf_reports/lavik-v0.3.0-vs-memory-databases-2026-11-15/raw/MANIFEST.json) 记录逐文件哈希。[verify.py](https://github.com/eloqdata/lavik-agent/blob/0988de68a0f315269e1a77ec35de736a0bdd6ed0/perf_reports/lavik-v0.3.0-vs-memory-databases-2026-11-15/verify.py) 检查归档字节并复核 272 个测量点、44 个版本对照和八组复测。本仓库仅保留报告和图表，数据与辅助脚本均位于证据仓库。

按上述硬件、版本、装载规模、顺序、CPU 绑定和非默认控制项复现；专用设备准备参见 [SPDK 存储指南](../../docs/operations/spdk-storage.md)。在固定版本的证据目录运行 `python3 plot.py` 可生成两张 SVG（[绘图脚本](https://github.com/eloqdata/lavik-agent/blob/0988de68a0f315269e1a77ec35de736a0bdd6ed0/perf_reports/lavik-v0.3.0-vs-memory-databases-2026-11-15/plot.py)，需要 Matplotlib）。Lavik 默认控制项已对照实测提交的 [`ServerOptions`](../../include/lavik/server.h) 核实，上文仅列出覆盖项。

多数条件仅一个窗口，补测次数单独标明；未给出置信区间，运行顺序未随机化。产品间成功响应不代表相同同步持久化语义。容量比较有意采用不同键数；同数据量请看 1M / 10M 曲线。[历史 v0.2.0 报告](../lavik-v0.2.0-vs-memory-databases-2026-10-21/README.zh-CN.md)。
