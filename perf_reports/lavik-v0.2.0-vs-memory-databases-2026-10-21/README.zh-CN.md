# Lavik v0.2.0 与内存数据库对比：2 vCPU / 16 vCPU

[English](README.md) | **简体中文**

在两种服务器规格上对比 Lavik、Redis 8.10.2 和 Valkey 9.1.2，并展示 Lavik 的 100M / 1B key 容量结果。共 **180 个正式测量点**，value 均为 **1 KiB**，测试日期为 **2026-10-21（UTC）**。

| Server vCPUs | Datasets | Client threads / CPUs | Total connections | Seconds / point | Points |
| --- | --- | --- | --- | --- | --- |
| 2 | Lavik 1M / 100M; Redis / Valkey 1M | 16 / 0–15 | 16 / 32 / 80 / 160 / 320 / 640 / 1280 | 30 | 84 |
| 16 | Lavik / Redis / Valkey 10M | 16 / 0–15 | 160 / 320 / 640 / 1280 | 30 | 88 |
| 16 | Lavik 1B | 32 / 0–31 | 160 / 320 / 640 / 1280 | 60 | 8 |

[完整逐点数据](https://github.com/eloqdata/lavik-agent/blob/bb061b97e9836281e71c219f21570d742313fb8c/perf_reports/lavik-v0.2.0-vs-memory-databases-2026-10-21/metadata/results.csv) · [版本与硬件元数据](https://github.com/eloqdata/lavik-agent/blob/bb061b97e9836281e71c219f21570d742313fb8c/perf_reports/lavik-v0.2.0-vs-memory-databases-2026-10-21/metadata/provenance.json) · [校验结果](https://github.com/eloqdata/lavik-agent/blob/bb061b97e9836281e71c219f21570d742313fb8c/perf_reports/lavik-v0.2.0-vs-memory-databases-2026-10-21/metadata/validation.json) · [原始日志与测量凭据](https://github.com/eloqdata/lavik-agent/tree/bb061b97e9836281e71c219f21570d742313fb8c/perf_reports/lavik-v0.2.0-vs-memory-databases-2026-10-21)

## 2 vCPU：1M 与 100M key

100M 数据集的 GET / SET 峰值为 **215,644 / 231,361 ops/s**，分别为小数据集峰值的 **100.0% / 96.6%**。这些峰值可能来自不同连接数，匹配连接数的结果见下表。 在 1M 基准中，GET 峰值最高的是 Valkey 9.1.2（245,955 ops/s），SET 峰值最高的是 Lavik 0.2.0 / 1M（239,405 ops/s）。

### 吞吐与 p99.9 延迟

<img src="2vcpu-combined.svg" alt="GET and SET throughput and p99.9 latency" width="800">

| Command | Product / keys | Workers / I/O threads | Connections | Peak ops/s | p99.9 ms |
| --- | --- | --- | --- | --- | --- |
| GET | Lavik 0.2.0 / 1M | 2 | 80 | 215,640 | 0.687 |
| GET | Lavik 0.2.0 / 100M | 2 | 640 | 215,644 | 5.535 |
| GET | Redis 8.10.2 | 2 | 160 | 209,958 | 1.311 |
| GET | Valkey 9.1.2 | 2 | 32 | 245,955 | 0.223 |
| SET | Lavik 0.2.0 / 1M | 2 | 320 | 239,405 | 2.527 |
| SET | Lavik 0.2.0 / 100M | 2 | 320 | 231,361 | 2.943 |
| SET | Redis 8.10.2 | 2 | 80 | 188,371 | 0.679 |
| SET | Valkey 9.1.2 | 2 | 80 | 214,117 | 0.511 |

每种命令为每个对比产品选择峰值最高的**一个固定 I/O 线程配置**，整条吞吐和延迟曲线均使用该配置，不逐点拼接。主图展示 16–640 连接，表格和完整配置图保留 1280 连接。

### 相同连接数对比

每格为 **ops/s / p99.9 ms**。

| Command | Connections | Lavik 0.2.0 / 1M | Lavik 0.2.0 / 100M | Redis 8.10.2 | Valkey 9.1.2 |
| --- | --- | --- | --- | --- | --- |
| GET | 16 | 105,536 / 0.343 | 105,464 / 0.223 | 124,422 / 0.167 | 172,900 / 0.167 |
| GET | 32 | 198,832 / 0.335 | 197,851 / 0.271 | 184,549 / 0.271 | 245,955 / 0.223 |
| GET | 80 | 215,640 / 0.687 | 215,643 / 0.639 | 205,073 / 0.567 | 225,845 / 0.543 |
| GET | 160 | 214,971 / 3.583 | 215,639 / 1.111 | 209,958 / 1.311 | 240,260 / 1.007 |
| GET | 320 | 215,632 / 2.303 | 215,644 / 2.159 | 208,124 / 2.831 | 230,637 / 6.367 |
| GET | 640 | 215,452 / 7.583 | 215,644 / 5.535 | 206,284 / 6.559 | 225,932 / 6.399 |
| GET | 1280 | 215,620 / 12.479 | 215,439 / 12.927 | 204,466 / 14.207 | 227,679 / 13.503 |
| SET | 16 | 148,361 / 0.271 | 157,101 / 0.183 | 139,237 / 0.247 | 169,978 / 0.167 |
| SET | 32 | 204,416 / 0.303 | 208,295 / 0.303 | 182,105 / 0.271 | 199,425 / 0.247 |
| SET | 80 | 229,009 / 0.639 | 220,759 / 0.655 | 188,371 / 0.679 | 214,117 / 0.511 |
| SET | 160 | 237,336 / 1.543 | 225,243 / 2.255 | 184,826 / 1.791 | 202,535 / 2.607 |
| SET | 320 | 239,405 / 2.527 | 231,361 / 2.943 | 178,853 / 3.887 | 194,044 / 2.959 |
| SET | 640 | 234,839 / 8.255 | 227,927 / 5.567 | 178,734 / 7.935 | 193,103 / 7.103 |
| SET | 1280 | 231,271 / 13.503 | 220,561 / 13.439 | 175,050 / 17.023 | 193,829 / 13.951 |

### 完整 I/O 线程扫描

| Product | I/O threads | GET peak ops/s (connections) | SET peak ops/s (connections) |
| --- | --- | --- | --- |
| Redis 8.10.2 | 1 | 189,749 (160) | 180,645 (320) |
| Redis 8.10.2 | 2 | 209,958 (160) | 188,371 (80) |
| Valkey 9.1.2 | 1 | 183,408 (32) | 180,624 (160) |
| Valkey 9.1.2 | 2 | 245,955 (32) | 214,117 (80) |

![All measured configurations](2vcpu-all-configurations.svg)

### 内存与客户端诊断

| Product / keys | INFO used_memory (MiB) | Process RSS (MiB) |
| --- | --- | --- |
| Lavik 0.2.0 / 1M | 96.0–96.0 | 739.2–748.6 |
| Lavik 0.2.0 / 100M | 5,069.4–5,069.4 | 5,513.4–5,521.7 |
| Redis 8.10.2 | 1,264.7–1,265.0 | 1,287.9–1,978.4 |
| Valkey 9.1.2 | 1,268.1–1,268.6 | 1,290.6–2,223.3 |

范围来自正式点后的快照。INFO 内存使用各产品自己的统计口径；RSS 为操作系统进程驻留内存。Lavik 另有 **2,048 MiB 大页预留**。客户端最大平均 CPU 使用为 **3.718 核**，单个客户端线程的最大平均使用为 **0.249 核**；测量包围窗口内最大服务端 CPU steal 为 **0.0000%**。

## 16 vCPU：10M 与 1B key

Lavik 10M / 1B 与 Redis、Valkey 10M 使用同一组 160/320/640/1280 总连接。10M 使用 **16 个客户端线程、30 秒窗口**；1B 使用 **32 个客户端线程、60 秒窗口**。1B 的八个点也用于存储产品对比报告。

10M 结果取自已保存的实测数据，排除原实验的 80 连接档；峰值和固定 I/O 线程配置均按保留档位重新计算。每个命令、每种产品的整条曲线使用同一固定配置。

![16 vCPU GET / SET throughput and p99.9](16vcpu-combined.svg)

| Command | Product / keys | Workers / I/O threads | Client threads | Peak connections | Peak ops/s | p99.9 ms |
| --- | --- | --- | --- | --- | --- | --- |
| GET | Lavik 0.2.0 / 10M | 16 | 16 | 1280 | 1,156,956 | 7.103 |
| GET | Lavik 0.2.0 / 1B | 16 | 32 | 1280 | 1,129,597 | 4.015 |
| GET | Redis 8.10.2 | 16 | 16 | 1280 | 976,087 | 7.039 |
| GET | Valkey 9.1.2 | 16 | 16 | 1280 | 913,756 | 7.743 |
| SET | Lavik 0.2.0 / 10M | 16 | 16 | 1280 | 945,384 | 10.175 |
| SET | Lavik 0.2.0 / 1B | 16 | 32 | 1280 | 910,356 | 4.319 |
| SET | Redis 8.10.2 | 16 | 16 | 1280 | 863,860 | 7.391 |
| SET | Valkey 9.1.2 | 8 | 16 | 1280 | 793,809 | 7.743 |

### 相同连接数对比

**ops/s / p99.9 ms**：

| Command | Connections | Lavik 0.2.0 / 10M | Lavik 0.2.0 / 1B | Redis 8.10.2 | Valkey 9.1.2 |
| --- | --- | --- | --- | --- | --- |
| GET | 160 | 377,857 / 0.855 | 375,966 / 0.823 | 408,159 / 0.871 | 459,449 / 0.895 |
| GET | 320 | 612,513 / 1.431 | 652,881 / 0.943 | 603,866 / 1.399 | 640,091 / 1.431 |
| GET | 640 | 876,217 / 3.983 | 985,019 / 1.751 | 800,217 / 3.711 | 791,161 / 3.887 |
| GET | 1280 | 1,156,956 / 7.103 | 1,129,597 / 4.015 | 976,087 / 7.039 | 913,756 / 7.743 |
| SET | 160 | 431,054 / 1.255 | 409,886 / 0.783 | 384,939 / 0.903 | 453,171 / 0.847 |
| SET | 320 | 637,287 / 1.375 | 701,241 / 0.959 | 579,902 / 1.423 | 638,517 / 1.383 |
| SET | 640 | 834,508 / 3.871 | 861,911 / 1.727 | 732,342 / 3.423 | 749,664 / 3.759 |
| SET | 1280 | 945,384 / 10.175 | 910,356 / 4.319 | 863,860 / 7.391 | 793,809 / 7.743 |

### Redis / Valkey I/O 线程扫描（10M）

![All tested configurations](16vcpu-all-configurations.svg)

| Product | I/O threads | GET peak ops/s (connections) | SET peak ops/s (connections) |
| --- | --- | --- | --- |
| Redis 8.10.2 | 1 | 154,244 (320) | 142,910 (320) |
| Redis 8.10.2 | 2 | 158,908 (320) | 147,806 (320) |
| Redis 8.10.2 | 4 | 437,073 (1280) | 398,372 (1280) |
| Redis 8.10.2 | 8 | 838,330 (1280) | 822,820 (1280) |
| Redis 8.10.2 | 16 | 976,087 (1280) | 863,860 (1280) |
| Valkey 9.1.2 | 1 | 152,804 (320) | 143,996 (320) |
| Valkey 9.1.2 | 2 | 159,958 (640) | 148,287 (640) |
| Valkey 9.1.2 | 4 | 436,332 (640) | 395,389 (1280) |
| Valkey 9.1.2 | 8 | 846,926 (1280) | 793,809 (1280) |
| Valkey 9.1.2 | 16 | 913,756 (1280) | 773,060 (1280) |

### 1B 延迟明细

| Command | Connections | Client threads | Ops/s | p50 ms | p99 ms | p99.9 ms | p99.99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GET | 160 | 32 | 375,966 | 0.423 | 0.743 | 0.823 | 0.927 |
| GET | 320 | 32 | 652,881 | 0.495 | 0.815 | 0.943 | 1.471 |
| GET | 640 | 32 | 985,019 | 0.631 | 1.247 | 1.751 | 3.743 |
| GET | 1280 | 32 | 1,129,597 | 1.095 | 2.159 | 4.015 | 8.575 |
| SET | 160 | 32 | 409,886 | 0.391 | 0.687 | 0.783 | 1.055 |
| SET | 320 | 32 | 701,241 | 0.447 | 0.815 | 0.959 | 1.519 |
| SET | 640 | 32 | 861,911 | 0.735 | 1.271 | 1.727 | 3.839 |
| SET | 1280 | 32 | 910,356 | 1.391 | 2.239 | 4.319 | 8.575 |

### 内存与客户端诊断

| Product | Max average client CPU / cores | Max thread average / cores | Max server steal / % | INFO used_memory / MiB | Process RSS / MiB |
| --- | --- | --- | --- | --- | --- |
| Lavik 0.2.0 / 10M | 15.066 | 0.948 | 0.0000 | 716.6–716.6 | 1,486.1–1,499.8 |
| Redis 8.10.2 | 13.187 | 0.840 | 0.0000 | 12,677.9–12,681.3 | 12,797.9–12,877.8 |
| Valkey 9.1.2 | 12.515 | 0.801 | 0.0000 | 12,654.7–12,655.8 | 12,773.3–13,065.9 |

部分测量点接近客户端 CPU 预算，观测到的峰值不能直接视为服务器容量上限。

## 环境与测试方法

### 2 vCPU

#### 版本与硬件

| 项目 | 配置 |
| --- | --- |
| Server | Standard_L2aos_v5; 2 vCPU (1 physical core) / 16 GiB nominal RAM; AMD EPYC 9V45 96-Core Processor |
| Client | Standard_F16as_v7; separate VM; CPUs 0–15 |
| Region | East US, zone 3 |
| OS | Ubuntu 24.04; Linux 6.17.0-1022-azure on both hosts |
| Storage | 1 × 1,439,887,785,984-byte dedicated local NVMe; SPDK/VFIO |
| Network | Kernel TCP; same VNet and availability zone; accelerated networking |
| Lavik | Official standard x86_64 v0.2.0 package; 2908f86f2770e9c6e8acc6585b3bedb4b44d1cd5 |

#### 测试方法

- key 为十进制 `1..N`，无前缀，value 为 1,024 字节，无 TTL。Lavik 1M 和 100M 分别独立装载；Redis、Valkey 各使用 1M。
- memtier 2.5.1 固定在客户端 16 个 CPU 上，pipeline 1，全 key 范围均匀随机 GET 或覆盖 SET。连接数为 16, 32, 80, 160, 320, 640, 1280；每点测一次，基准与 1 亿条组 30 秒。全部定时测量均使用独立客户端随机种子。
- 每个配置先预热 GET 10 秒，然后 GET、SET 各按连接数递增测量。所有产品在同一服务器串行运行，测量前已完成所有构建。
- Lavik 使用 2 个 worker 和独占 NVMe/SPDK。非默认设置为 `--tomb-raider-interval-ms=0`、`--spdk-max-completions-per-poll=16` 和 2 GiB EAL/大页内存。每个测量点前后检查 defrag 保持启用。
- Redis 8.10.2、Valkey 9.1.2 与旧报告版本一致，使用默认优化构建与 jemalloc，分别扫描 1, 2 个 I/O 线程。`save ""` 禁用定时快照，AOF 保持关闭；初次装载后手动 SAVE，各线程配置从相同 RDB 基线恢复。正式写入只进入内存。
- THP 为 `never`，overcommit 为 1，不添加 cgroup 内存上限。每个配置前后核验精确 DBSIZE 和五个 value 长度，并检查装载请求数、零 GET 未命中、连接错误、版本和 CPU 亲和性。

### 16 vCPU：10M

#### 版本与硬件

| 项目 | 配置 |
| --- | --- |
| Server | Standard_L16aos_v4; 16 vCPU (8 physical cores) / 128 GiB nominal RAM; AMD EPYC 9V74 80-Core Processor |
| Client | Standard_F32als_v6; separate VM; CPUs 0–15 |
| Region | Japan East, zone 2 |
| OS | Ubuntu 24.04; Linux 6.17.0-1022-azure on both hosts |
| Storage | 6 × 1,919,850,381,312-byte dedicated local NVMe; SPDK/VFIO |
| Network | Kernel TCP; same VNet and availability zone; accelerated networking |
| Lavik | Official standard x86_64 v0.2.0 package; 2908f86f2770e9c6e8acc6585b3bedb4b44d1cd5 |

#### 测试方法

- key 为十进制 `1..N`，无前缀，value 为 1,024 字节，无 TTL。各产品分别独立装载 10M 条。
- memtier 2.5.1 固定在客户端 16 个 CPU 上，pipeline 1，全 key 范围均匀随机 GET 或覆盖 SET。保留的连接数为 160, 320, 640, 1280；每点测一次、30 秒，沿用原报告的默认客户端种子。
- 每个配置先预热 GET 10 秒，然后 GET、SET 各按连接数递增测量。所有产品在同一服务器串行运行，测量前已完成所有构建。
- Lavik 使用 16 个 worker 和独占 NVMe/SPDK。非默认设置为 `--tomb-raider-interval-ms=0`、`--spdk-max-completions-per-poll=16` 和 8 GiB EAL/大页内存。每个测量点前后检查 defrag 保持启用。
- Redis 8.10.2、Valkey 9.1.2 与旧报告版本一致，使用默认优化构建与 jemalloc，分别扫描 1, 2, 4, 8, 16 个 I/O 线程。`save ""` 禁用定时快照，AOF 保持关闭；初次装载后手动 SAVE，各线程配置从相同 RDB 基线恢复。正式写入只进入内存。
- THP 为 `never`，overcommit 为 1，不添加 cgroup 内存上限。每个配置前后核验精确 DBSIZE 和五个 value 长度，并检查装载请求数、零 GET 未命中、连接错误、版本和 CPU 亲和性。

### 16 vCPU：1B

| Item | Configuration |
| --- | --- |
| Server | Standard_L16aos_v4; Japan East zone 2; AMD EPYC 9V74; 8 physical cores / 16 vCPUs; 128 GiB nominal RAM |
| Client | Standard_F32als_v6; 32 physical CPUs; memtier 2.5.1; 32 threads pinned to CPUs 0–31 |
| OS | Ubuntu 24.04; Linux 6.17.0-1022-azure on both VMs |
| Storage | 6 × 1,919,850,381,312-byte dedicated NVMe; SPDK/VFIO; 8 GiB EAL/hugepage reservation |
| Network | Private VNet; kernel TCP; accelerated networking |
| Lavik | Official standard x86_64 v0.2.0; 2908f86f2770e9c6e8acc6585b3bedb4b44d1cd5 |

- key 为十进制 1..1,000,000,000，无前缀、无 TTL，value 为 1,024 字节。逻辑 value 净载荷为 953.67 GiB，不含 key、索引与存储开销。
- 从空盘顺序装载精确 10 亿次 SET，使用 32 线程、640 总连接。正式扫描使用独立客户端随机种子、全范围均匀随机 GET 或覆盖 SET、pipeline 1，每点 60 秒。先 GET 后 SET，连接数递增；不额外预热 GET。
- Lavik 使用 16 个服务端 worker、SPDK、停机 checkpoint、tomb-raider interval 0、SPDK completion cap 16，其余继承已核验默认值。每点前后检查 defrag 保持启用；扫描前后核验精确 DBSIZE 与五个 value 长度。
- 每格仅测一次，不给置信区间；吞吐和分位数来自完整窗口。这八个点会复用于存储引擎对比报告，不算作独立重复测量。
- 历史报告使用 16 个客户端线程并包含 80 连接；本次改为 32 线程且取消该档。区域也由 Japan West 改为 Japan East。

装载客户端调用耗时 **21.6 分钟**，memtier 装载吞吐 **940,654 ops/s**。正式点最大平均客户端 CPU 使用 **17.971 核**，最大单线程平均 **0.579 核**，包围测量窗口内服务端 CPU steal 最大 **0.0000%**。

## 证据与测量限制

[完整逐点数据](https://github.com/eloqdata/lavik-agent/blob/bb061b97e9836281e71c219f21570d742313fb8c/perf_reports/lavik-v0.2.0-vs-memory-databases-2026-10-21/metadata/results.csv) · [版本、硬件与校验元数据](https://github.com/eloqdata/lavik-agent/blob/bb061b97e9836281e71c219f21570d742313fb8c/perf_reports/lavik-v0.2.0-vs-memory-databases-2026-10-21/metadata/provenance.json)

每点仅一次测量，不提供置信区间。不同产品的成功响应不代表相同的同步持久化保证。新机器的物理放置、网络延迟和运行时间均可能与旧报告不同。[原始日志与测量凭据](https://github.com/eloqdata/lavik-agent/tree/bb061b97e9836281e71c219f21570d742313fb8c/perf_reports/lavik-v0.2.0-vs-memory-databases-2026-10-21)保存在 `eloqdata/lavik-agent`，并包含逐文件 SHA-256 清单。

原始证据按 `2vcpu`、`16vcpu-10m`、`16vcpu-1b` 分目录保存，归档目录包含逐文件 SHA-256 清单；CSV 中的 `suite` 和 `source` 可定位对应测量。

## 复现命令

### 2 vCPU

服务端使用上述 L2aos_v5，客户端使用同可用区的 F16as_v7；两端通过私网连接。准备 Lavik v0.2.0 标准 x86_64 二进制、Redis 8.10.2、Valkey 9.1.2 和 memtier 2.5.1。Redis / Valkey 使用默认优化构建、jemalloc 5.3.0，编译参数为 `make -j12 BUILD_TLS=yes`。以下命令使用 Bash。

#### 启动服务端

```bash
# Run on the server; replace the address and binary paths for your hosts.
SERVER_IP=10.96.0.4
LAVIK_BIN=/opt/bench020/downloads/lavik-v0.2.0-linux-x86_64/lavik
REDIS_BIN=/opt/bench020/bin/redis-server
VALKEY_BIN=/opt/bench020/bin/valkey-server
ulimit -n 65535
sudo sysctl -w vm.overcommit_memory=1
printf 'never\n' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

Lavik 使用空的专用 NVMe namespace。启动前按 [SPDK 存储指南](../../docs/operations/spdk-storage.md) 识别测试盘并绑定 VFIO，本测试设置 `HUGEMEM=2048`、`BYCORF_DPDK_MEMORY_MB=2048`。该 VM 未暴露 IOMMU，按指南使用了专用测试机的 no-IOMMU 配置。分别加载 100 万条和 1 亿条；两次之间正常退出 Lavik、恢复 NVMe 驱动，再清空已确认的测试 namespace 并重新绑定，独立加载数据。

```bash
# Use the verified PCI address and namespace ID of your empty test NVMe.
NVME_BDF=YOUR_NVME_PCI_ADDRESS
NVME_NSID=1
sudo prlimit --memlock=unlimited:unlimited --nofile=65535:65535 \
  env BYCORF_EAL_ARGS="-a ${NVME_BDF}" BYCORF_DPDK_MEMORY_MB=2048 \
  taskset -c 0-1 "$LAVIK_BIN" \
  --storage=spdk --bind="$SERVER_IP" \
  --tomb-raider-interval-ms=0 --spdk-max-completions-per-poll=16 \
  --log-dir="$PWD/lavik-logs" \
  --data-file="spdk://${NVME_BDF}/${NVME_NSID}"
```

以上省略 v0.2.0 的默认参数；`taskset -c 0-1` 下默认启动两个绑定 CPU 的 worker。`BYCORF_DPDK_MEMORY_MB=2048` 设置 SPDK 使用的 EAL 内存为 2 GiB，网络仍为默认的内核 TCP。保留的调优项将 tombstone 扫描间隔从默认一天改为关闭、SPDK completion cap 从默认 8 改为 16。

Redis 和 Valkey 分别测试 1、2 个 I/O 线程，每次只运行一个服务端：

```bash
# Run one product/configuration at a time. Repeat with IO_THREADS=2.
IO_THREADS=1
IO_ARGS=(); if ((IO_THREADS > 1)); then IO_ARGS=(--io-threads "$IO_THREADS"); fi
mkdir -p "$PWD/bench-data/redis" "$PWD/bench-data/valkey"

# Redis 8.10.2
taskset -c 0-1 "$REDIS_BIN" \
  --bind "$SERVER_IP" --protected-mode no \
  --dir "$PWD/bench-data/redis" \
  --save "" \
  "${IO_ARGS[@]}"

# Valkey 9.1.2 (after stopping Redis)
taskset -c 0-1 "$VALKEY_BIN" \
  --bind "$SERVER_IP" --protected-mode no \
  --dir "$PWD/bench-data/valkey" \
  --save "" \
  "${IO_ARGS[@]}"
```

#### 客户端灌数与测试

按 Lavik 100 万、Lavik 1 亿、Redis IO=1/2、Valkey IO=1/2 的顺序运行。切换配置时修改 `N` 和 `RUN`。每个产品首次灌数前确认 `DBSIZE=0`。Redis / Valkey 首次灌数后执行一次 `SAVE`，再用 `SHUTDOWN NOSAVE` 退出；每组 I/O 线程配置都从同一基线 RDB 重启，跳过再次灌数，重新检查数据后执行预热与扫描。Lavik 两组均从空的测试 namespace 独立灌数。

```bash
# Bash, on the separate client, CPUs 0–15; memtier_benchmark 2.5.1.
set -euo pipefail
SERVER_IP=10.96.0.4
N=1000000 # Use 100000000 for the large Lavik dataset.
RUN=lavik-1m # lavik-100m, redis-io1, redis-io2, valkey-io1, valkey-io2
mkdir -p "$RUN"
ulimit -n 65535
COMMON=(--server="$SERVER_IP" \
  --threads=16 --key-minimum=1 --key-maximum="$N" \
  --key-prefix= --data-size=1024 --hide-histogram \
  --print-percentiles=50,99,99.9,99.99 --show-config)

# Fill an empty dataset using disjoint sequential key streams: exactly N writes.
taskset -c 0-15 memtier_benchmark "${COMMON[@]}" --clients=10 \
  --requests="$((N / 160))" --ratio=1:0 --key-pattern=P:P \
  --json-out-file="$RUN/fill.json" > "$RUN/fill.txt" 2>&1

check_data() {
  test "$(redis-cli -h "$SERVER_IP" --raw DBSIZE)" = "$N"
  for key in 1 2 "$((N / 2))" "$((N - 1))" "$N"; do
    test "$(redis-cli -h "$SERVER_IP" --raw STRLEN "$key")" = 1024
  done
}
check_data

# For each memory peer only: create its baseline RDB once after the first fill.
# redis-cli -h "$SERVER_IP" SAVE

# Warmup, then GET and SET sweeps. Reuse this part after loading a baseline RDB.
taskset -c 0-15 memtier_benchmark "${COMMON[@]}" --clients=10 \
  --distinct-client-seed --test-time=10 \
  --command='GET __key__' --command-is-read \
  --json-out-file="$RUN/warmup.json" > "$RUN/warmup.txt" 2>&1

for kind in GET SET; do
  if [ "$kind" = GET ]; then
    COMMAND='GET __key__'; READ_FLAG=(--command-is-read)
  else
    COMMAND='SET __key__ __data__'; READ_FLAG=()
  fi
  for connections in 16 32 80 160 320 640 1280; do
    taskset -c 0-15 memtier_benchmark "${COMMON[@]}" \
      --clients="$((connections / 16))" --distinct-client-seed --test-time=30 \
      --command="$COMMAND" "${READ_FLAG[@]}" \
      --json-out-file="$RUN/${kind}-c${connections}.json" \
      > "$RUN/${kind}-c${connections}.txt" 2>&1
  done
done
check_data
```

每组结束后检查 GET 全部命中、连接错误为零，且 DBSIZE 保持不变。

### 16 vCPU / 10M

以下 Bash 命令对应实测参数；请按部署环境调整路径、地址和 PCI 身份。一次只运行一个数据库和一个压测客户端。每轮使用独立结果目录。

#### Lavik SPDK

按 [SPDK 存储指南](../../docs/operations/spdk-storage.md)核验六张专用临时 NVMe、准备 VFIO 和 8 GiB hugepages。10M 组从空盘独立灌入；不得覆盖系统盘或已有业务数据。完成后干净停止 Lavik、恢复内核驱动及 hugepage 预留，再测试内存数据库。以下为 10M 启动命令：

```bash
sudo prlimit --memlock=unlimited:unlimited --nofile=65535:65535 env 'BYCORF_EAL_ARGS=-a 7133:00:00.0 -a 10fc:00:00.0 -a 9c4a:00:00.0 -a ef09:00:00.0 -a fa44:00:00.0 -a 9816:00:00.0' BYCORF_DPDK_MEMORY_MB=8192 taskset \
  -c 0-15 /opt/bench020/downloads/lavik-v0.2.0-linux-x86_64/lavik \
  --storage=spdk \
  --bind=10.96.1.4 \
  --tomb-raider-interval-ms=0 \
  --spdk-max-completions-per-poll=16 \
  --log-dir=/opt/bench020/results/large/memory/lavik-10m/logs \
  --data-file=spdk://7133:00:00.0/1 \
  --data-file=spdk://10fc:00:00.0/1 \
  --data-file=spdk://9c4a:00:00.0/1 \
  --data-file=spdk://ef09:00:00.0/1 \
  --data-file=spdk://fa44:00:00.0/1 \
  --data-file=spdk://9816:00:00.0/1
```

启动后等待恢复及后台初始化完成，使用 `CONFIG GET defrag-paused` 核验为 `no`；各正式点前后再次核验。

#### Redis / Valkey

使用 jemalloc 5.3.0，构建命令为 `make -j12 BUILD_TLS=no`。Redis 8.10.2 对应提交 `498ecd0d6d007db11ddb3aea9428552598a78622`，Valkey 9.1.2 对应 `7f1dffedff6de73058b2c2a389422b6ecd56c8fb`。首次灌入使用 16 I/O 线程。

```bash
# On the server; start one product/configuration at a time.
PRODUCT=redis # redis or valkey
IO_THREADS=16 # tested: 1, 2, 4, 8, 16
IO_ARGS=(); if ((IO_THREADS > 1)); then IO_ARGS=(--io-threads "$IO_THREADS"); fi
DATA_DIR=/opt/bench020/results/large/data/$PRODUCT
mkdir -p "$DATA_DIR"
ulimit -n 65535
taskset -c 0-15 "/opt/bench020/bin/$PRODUCT-server" \
  --bind 10.96.1.4 --protected-mode no \
  --dir "$DATA_DIR" --save "" \
    "${IO_ARGS[@]}"
```

#### 灌入、预热和连接数扫描

```bash
# On the client: memtier_benchmark 2.5.1.
KEYS=10000000
SECONDS_PER_POINT=30
SEED=()
mkdir -p results
ulimit -n 65535
COMMON=(--server=10.96.1.4
  --threads=16 --key-minimum=1 --key-maximum="$KEYS"
  --key-prefix= --data-size=1024 --hide-histogram
  --print-percentiles=50,99,99.9,99.99 --show-config)

# Once per fresh dataset; each key is written once across 640 clients.
taskset -c 0-15 memtier_benchmark "${COMMON[@]}" --clients=40 \
  --json-out-file=results/fill.json --requests="$((KEYS / 640))" \
  --ratio=1:0 --key-pattern=P:P
redis-cli -h 10.96.1.4 --raw DBSIZE
redis-cli -h 10.96.1.4 STRLEN 1
```

对于 Redis/Valkey，首次灌入后执行下列基线命令；然后以 1、2、4、8、16 I/O 线程依次启动，每档等待 RDB 加载结束、DBSIZE=10000000，核验持久化配置，再执行下一段预热及测量。每档结束使用 `SHUTDOWN NOSAVE`，保持同一 `dump.rdb` 不变；不要重新灌入或在测量中执行 SAVE。Lavik 不执行这些 RDB 基线命令。

```bash
redis-cli -h 10.96.1.4 SAVE
redis-cli -h 10.96.1.4 SHUTDOWN NOSAVE
# On the server, record/check the unchanged RDB baseline hash:
sha256sum "$DATA_DIR/dump.rdb"
# After each restart, run these checks from the client:
redis-cli -h 10.96.1.4 CONFIG GET save
redis-cli -h 10.96.1.4 CONFIG GET appendonly
redis-cli -h 10.96.1.4 INFO persistence
```

```bash
# Run once for Lavik 10M and once per Redis/Valkey I/O-thread configuration.
if [ "$KEYS" -eq 10000000 ]; then
  taskset -c 0-15 memtier_benchmark "${COMMON[@]}" --clients=40 \
    --json-out-file=results/warmup.json --test-time=10 \
    --command='GET __key__' --command-is-read
fi
for OP in GET SET; do
  COMMAND=(--command='GET __key__' --command-is-read)
  if [ "$OP" = SET ]; then
    COMMAND=(--command='SET __key__ __data__' )
  fi
  for CONNECTIONS in 160 320 640 1280; do
    taskset -c 0-15 memtier_benchmark "${COMMON[@]}" \
      --clients="$((CONNECTIONS / 16))" \
      --json-out-file="results/$OP-c$CONNECTIONS.json" \
      --test-time="$SECONDS_PER_POINT" "${COMMAND[@]}" "${SEED[@]}" \
      > "results/$OP-c$CONNECTIONS.txt" 2>&1
  done
done
redis-cli -h 10.96.1.4 --raw DBSIZE
```

### 16 vCPU / 1B

按 [SPDK 操作指南](../../docs/operations/spdk-storage.md) 核验六块专用测试盘、准备 VFIO 与 8 GiB 大页。仅在空的测试介质上装载；启动后等待 PING，检查 defrag-paused 为 no。测量完成后干净停机，并按指南恢复内核驱动和大页预留。

```bash
sudo prlimit --memlock=unlimited:unlimited --nofile=65535:65535 env \
  BYCORF_EAL_ARGS='-a 7133:00:00.0 -a 10fc:00:00.0 -a 9c4a:00:00.0 -a ef09:00:00.0 -a fa44:00:00.0 -a 9816:00:00.0' \
  BYCORF_DPDK_MEMORY_MB=8192 \
  taskset -c 0-15 /opt/bench020/downloads/lavik-v0.2.0-linux-x86_64/lavik \
  --storage=spdk --bind=10.96.1.4 --shutdown-checkpoint \
  --tomb-raider-interval-ms=0 --spdk-max-completions-per-poll=16 \
  --log-dir=/opt/bench020/results/large/storage/lavik-1b/logs \
  --data-file=spdk://7133:00:00.0/1 \
  --data-file=spdk://10fc:00:00.0/1 \
  --data-file=spdk://9c4a:00:00.0/1 \
  --data-file=spdk://ef09:00:00.0/1 \
  --data-file=spdk://fa44:00:00.0/1 \
  --data-file=spdk://9816:00:00.0/1
```

在独立客户端运行：

```bash
SERVER=10.96.1.4
mkdir -p results
ulimit -n 65535
COMMON=(--server="$SERVER" --threads=32 --key-minimum=1
  --key-maximum=1000000000 --key-prefix= --data-size=1024
  --hide-histogram --print-percentiles=50,99,99.9,99.99 --show-config)
# 32 threads × 20 connections × 1,562,500 requests = 1,000,000,000 SETs.
taskset -c 0-31 memtier_benchmark "${COMMON[@]}" --clients=20 \
  --requests=1562500 --ratio=1:0 --key-pattern=P:P --json-out-file=results/fill.json
redis-cli -h "$SERVER" --raw DBSIZE
for key in 1 2 500000000 999999999 1000000000; do
  test "$(redis-cli -h "$SERVER" --raw STRLEN "$key")" = 1024
done
# No additional GET warmup for Lavik 1B.
for op in GET SET; do
  COMMAND=(--command='GET __key__' --command-is-read)
  if [[ "$op" == SET ]]; then COMMAND=(--command='SET __key__ __data__'); fi
  for connections in 160 320 640 1280; do
    redis-cli -h "$SERVER" CONFIG GET defrag-paused
    taskset -c 0-31 memtier_benchmark "${COMMON[@]}" \
      --clients="$((connections / 32))" --test-time=60 --distinct-client-seed \
      "${COMMAND[@]}" --json-out-file="results/$op-c$connections.json" \
      > "results/$op-c$connections.txt" 2>&1
    redis-cli -h "$SERVER" CONFIG GET defrag-paused
  done
done
redis-cli -h "$SERVER" --raw DBSIZE
```
