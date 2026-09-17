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

# Keylane SPDK/io_uring、Dragonfly、Garnet、Apache Kvrocks、Pika、Tendis 与 KeyDB On Flash 性能对比（2026-08-11）

[English](README.md) | **简体中文** | [报告目录](../README.md)

> 本报告记录项目更名为 Lavik 之前的 Keylane 测试。产品名、版本、命令和数据均对应当时的实验，
> 不代表当前 Lavik 版本的实测结果。
> 恢复来源： [2026-09-15 历史版本](https://github.com/eloqdata/lavik/tree/eedb3080d808769519d93e971195b53b38b09c1e/perf_reports)。

> 2026-08-12 更新：Keylane 三种后端、Dragonfly Tiered Storage、Microsoft Garnet Storage Tier、Apache Kvrocks、Pika、Tendis 和 KeyDB On Flash 已按统一的 5 分钟口径完成复测，表内均已替换为本轮结果。每种后端只灌数一次，随后依次执行纯读、1:1 读写混合和纯写。本次 Keylane 复测保持 defrag 开启，其他系统保持各自的后台回收或 auto compaction 开启。

> 2026-08-12 补充：增加 Azure Managed Redis 480 GB/16 vCPU 实例的独立容量与性能测试。该组使用约 400 GB 数据和 60 秒窗口，已用 `*` 标入主榜单，并在表下说明与本地双 NVMe 300 秒结果的口径差异。

## 测试结果

除带 `*` 的 Azure Managed Redis 独立结果外，本次测试使用双 NVMe、2 亿条 1–4 KB 数据、80 个客户端连接和不限速 workload。Keylane SPDK 的纯读和混合 QPS 最高，raw io_uring 的纯写 QPS 最高；三种 Keylane 后端在三类 workload 中均保持最高的一组吞吐。

| Workload | 系统 | QPS | p99 (ms) | p99.9 (ms) |
| --- | --- | ---: | ---: | ---: |
| 纯读 GET | Keylane SPDK | 310,387.22 | 0.455 | 0.895 |
| 纯读 GET | Keylane io_uring（双裸块设备） | 278,924.72 | 0.503 | 2.319 |
| 纯读 GET | Keylane io_uring（双 XFS 文件） | 272,726.70 | 0.519 | 2.319 |
| 纯读 GET | Microsoft Garnet Storage Tier | 205,297.49 | 2.303 | 4.223 |
| 纯读 GET | Dragonfly Tiered Storage | 187,652.86 | 2.911 | 16.383 |
| 纯读 GET | Pika | 86,669.28 | 1.775 | 2.383 |
| 纯读 GET | Apache Kvrocks | 70,671.82 | 2.479 | 3.407 |
| 纯读 GET | Tendis | 68,860.36 | 2.063 | 5.855 |
| 纯读 GET | Azure Managed Redis* | 44,480.15 | 5.855 | 11.839 |
| 纯读 GET | KeyDB On Flash | 6,146.18 | 18.047 | 25.471 |
| 纯写 SET | Keylane io_uring（双裸块设备） | 393,732.53 | 1.015 | 1.647 |
| 纯写 SET | Keylane SPDK | 392,283.87 | 0.999 | 1.607 |
| 纯写 SET | Keylane io_uring（双 XFS 文件） | 385,324.26 | 1.055 | 1.727 |
| 纯写 SET | Microsoft Garnet Storage Tier | 361,960.06 | 1.583 | 3.391 |
| 纯写 SET | Dragonfly Tiered Storage | 199,233.52 | 4.639 | 10.303 |
| 纯写 SET | Tendis | 160,641.92 | 1.335 | 2.127 |
| 纯写 SET | Apache Kvrocks | 110,166.99 | 1.759 | 2.671 |
| 纯写 SET | Azure Managed Redis* | 84,863.93 | 3.199 | 7.839 |
| 纯写 SET | Pika | 79,537.31 | 4.223 | 7.327 |
| 纯写 SET | KeyDB On Flash | 5,395.49 | 24.447 | 31.359 |
| 1:1 读写混合 | Keylane SPDK | 352,442.49 | 0.631 | 1.447 |
| 1:1 读写混合 | Keylane io_uring（双裸块设备） | 328,831.03 | 0.655 | 1.447 |
| 1:1 读写混合 | Keylane io_uring（双 XFS 文件） | 326,110.00 | 0.687 | 1.503 |
| 1:1 读写混合 | Microsoft Garnet Storage Tier | 209,366.38 | 2.511 | 4.895 |
| 1:1 读写混合 | Dragonfly Tiered Storage | 207,247.53 | 4.015 | 9.791 |
| 1:1 读写混合 | Tendis | 102,212.78 | 1.439 | 1.975 |
| 1:1 读写混合 | Apache Kvrocks | 88,084.47 | 2.207 | 4.927 |
| 1:1 读写混合 | Pika | 75,324.05 | 3.615 | 6.527 |
| 1:1 读写混合 | Azure Managed Redis* | 55,351.99 | 5.311 | 11.839 |
| 1:1 读写混合 | KeyDB On Flash | 5,197.16 | 29.311 | 39.167 |

\* Azure Managed Redis 使用 480 GB/16 vCPU 托管实例、154,088,000 条约 400 GB 数据和 60 秒窗口，通过 Private Endpoint 测试；其余系统使用本地双 NVMe、2 亿条数据和 300 秒窗口。因此该行可用于同一 workload 下的实测数量级对照，但不是完全同口径排名。容量、网络和复现细节见下方独立小节。

io_uring 双裸块设备相比双 XFS 文件的 QPS 分别高 2.27%（纯读）、2.18%（纯写）和 0.83%（1:1）；纯读 p99.9 相同，纯写和 1:1 的 p99.9 分别低 4.63% 和 3.73%。绕过 XFS 有稳定但不大的收益；双文件方案保留了大部分性能，同时更容易按普通 Linux 文件方式部署。

SPDK 相比 raw io_uring 的纯读和 1:1 QPS 分别高 11.28% 和 7.18%；raw io_uring 的纯写 QPS 高 0.37%。SPDK 纯读 p99.9 低 61.41%，1:1 p99.9 相同，纯写 p99.9 低 2.43%。本轮 SPDK 的主要吞吐收益集中在随机读和读写并发路径，纯写吞吐则与 raw io_uring 接近。

Dragonfly 本轮保持正常 tiered-storage 回收，灌数后执行 180 秒随机 GET 预热，三组正式测试之间不重启、不清库、不清 Linux page cache。正式结果采用完整 300 秒均值。

Garnet 本轮使用每 300 秒运行一次的 Lookup compaction，并启用 `compaction-force-delete` 直接删除完成回收的旧 segment。灌数完成后不做额外 GET 预热，也不等待后台回收，立即按纯读、1:1、纯写的顺序执行三组完整 300 秒测试。

Tendis 本轮始终开启 RocksDB auto compaction 和 Blob GC。2 亿条灌数完成后不做额外 GET 预热，也不等待后台整理，立即按纯读、1:1、纯写的顺序执行三组完整 300 秒测试，因此结果包含缓存冷启动和在线后台整理的影响。

KeyDB On Flash 本轮保留 RocksDB WAL 和 auto compaction，使用 64 GiB DRAM 热层。2 亿条灌数完成后不做额外 GET 预热，也不等待后台整理，立即按纯读、1:1、纯写的顺序执行三组完整 300 秒测试。公共 workload 是均匀随机访问，因此大部分请求落到 Flash；该结果不代表 KeyDB 官方建议的热点分布场景。

## Azure Managed Redis 独立容量测试

这组结果回答两个独立问题：480 GB/16 vCPU 托管实例在本 workload 下能灌入多少数据，以及在约 400 GB 数据集上、80 个连接且不限制 QPS 时的短窗口性能。结果已用 `*` 加入主榜单用于数量级对照；因为数据量、测试时长、网络路径和服务形态不同，不应把它视为完全同口径比较。

### 成本估算

以下月度价格由用户在 2026-08-12 提供，属于当前配置的估算值，不是固定报价：

| 部署 | 估计价格（USD/月） |
| --- | ---: |
| Azure Managed Redis Flex，480 GB/16 vCPU | $2,414.28 |
| Keylane server，`Standard_L16s_v3` | $1,152.67 |

Redis Flex 估计每月高 $1,261.61，约为 Keylane server VM 的 2.09 倍（高 109.45%）。两项都不计公共压测 client；实际账单还会随区域、计费方式、折扣、存储和网络费用变化。托管 Redis 与自管 VM 的服务边界也不同，因此这组数字是实例月费对照，不代表包含运维、可用性和支持成本的完整 TCO。

| Workload | QPS | p99 (ms) | p99.9 (ms) | 窗口 | 校验 |
| --- | ---: | ---: | ---: | ---: | --- |
| 纯读 GET | 44,480.15 | 5.855 | 11.839 | 60 s | 100% hit，0 error |
| 1:1 读写混合 | 55,351.99 | 5.311 | 11.839 | 60 s | GET 100% hit，0 error |
| 纯写 SET | 84,863.93 | 3.199 | 7.839 | 60 s | 0 error |

实例运行 Redis 7.4.3，`INFO server` 报告 `redis_mode:standalone`，使用 `noeviction`。Client 为 `Standard_L16s_v3`，通过同一 VNet 内的 Private Endpoint `10.0.0.6:10000` 直连；该实例关闭 TLS，因此命令未使用 `--tls`，endpoint 也不要求 `--cluster-mode`。Microsoft 文档说明 Azure Managed Redis 使用 10000 端口；只有 OSS cluster policy 必须使用 memtier `--cluster-mode`，Enterprise cluster policy 可按非集群 endpoint 使用。生产复现应优先把标准 hostname 解析到 Private Endpoint，而不是长期硬编码私网 IP。

容量使用 Redis `INFO memory` 的 `used_memory` 计数，而不是只累计 value 字节。最终正式数据集包含 154,088,000 个连续 key，测试前 `used_memory=400,002,049,295` bytes。随后按约 10 GB 一档追加，最后一档完整成功后的状态为 166,088,001 个 key、`used_memory=432,447,820,034` bytes、`evicted_keys=0`；再追加 320 万个新 key 时全部返回 `OOM command not allowed when used memory > 'maxmemory'`，`DBSIZE` 没有增加。因此本 workload 的最后完整成功点约为 432.45 GB `used_memory`，不是 480 GB 标称容量的精确可用数据量。

容量探测触顶后执行 `FLUSHDB SYNC`，确认 `DBSIZE=0` 且 `used_memory` 回到约 96 MB，再重新灌入正式的约 400 GB 数据。三组最终日志均无 OOM、认证或连接错误；纯读和混合的 GET 均全部命中。纯写完整窗口前另跑了 10 秒同参数安全探测，确认不会在 60 秒内触顶；该 10 秒结果未计入表格。三组结束后 `DBSIZE` 仍为 154,088,000、`evicted_keys=0`，`used_memory=426,201,007,973` bytes。覆盖写会提高热数据内存占用，因此接近容量上限的持续写入测试必须同时监控 OOM 和 `used_memory`，不能把 Redis error reply 计为成功吞吐。

复现时先配置 Azure Managed Redis Private Endpoint 和 Private DNS。Microsoft 建议客户端仍连接 `<cache>.<region>.redis.azure.net:10000`，并让 `privatelink.redis.azure.net` 私有 DNS zone 把标准 hostname 解析到私网地址。本次临时 DNS 尚未关联到 client VNet，因此非 TLS 实例直接使用已批准 Private Endpoint 的 `10.0.0.6` 做测试。

```bash
# 在 client 执行；不要把真实 access key 写入报告或仓库
export AMR_HOST=10.0.0.6
export AMR_PORT=10000
export AMR_ACCESS_KEY='<access-key>'

# 构造约 400 GB 的连续数据集
taskset -c 0-15 memtier_benchmark \
  -s "$AMR_HOST" -p "$AMR_PORT" -a "$AMR_ACCESS_KEY" \
  -t 16 -c 40 \
  -n allkeys \
  --distinct-client-seed \
  --ratio=1:0 \
  --key-pattern=P:P \
  --key-prefix="kv_" \
  --key-minimum=1 \
  --key-maximum=154088000 \
  --random-data \
  --data-size-range=1000-4000 \
  --data-size-pattern=R \
  --hide-histogram

# RATIO 依次替换为 0:1、1:1 和 1:0；每组结束后检查日志中没有 OOM/error
taskset -c 0-15 memtier_benchmark \
  -s "$AMR_HOST" -p "$AMR_PORT" -a "$AMR_ACCESS_KEY" \
  -t 8 -c 10 \
  --test-time 60 \
  --distinct-client-seed \
  --ratio=RATIO \
  --key-prefix="kv_" \
  --key-minimum=1 \
  --key-maximum=154088000 \
  --random-data \
  --data-size-range=1000-4000 \
  --data-size-pattern=R \
  --hide-histogram \
  --print-percentiles="99,99.9" \
  --randomize
```

参考：[Azure Managed Redis 性能测试建议](https://learn.microsoft.com/en-us/azure/redis/best-practices-performance)、[Azure Managed Redis Private Link](https://learn.microsoft.com/en-us/azure/redis/private-link)。

## 测试环境

| 角色 | Azure 机型 | 地址 |
| --- | --- | --- |
| Server | `Standard_L16s_v3` | `10.0.0.4:6379` |
| Client | `Standard_L16s_v3` | `10.0.0.5` |

公共 workload：8 个 memtier threads、每个 thread 10 个连接、1,000–4,000 byte 随机 value、key 范围 `kv_1`–`kv_200000000`、每组 300 秒、不限制 QPS。每个后端只灌入一次 2 亿条初始数据，随后依次执行纯读、1:1 读写混合和纯写，三组之间不重启、不清库。九组服务/后端在不同时段独占同一个端口运行，不并发运行。

Keylane 三个存储后端都使用 16 workers，并保持 defrag 开启。SPDK 直接访问两个 NVMe namespace；raw io_uring 通过 Linux NVMe 驱动直接访问两个块设备，direct-I/O alignment 为 512 bytes；file io_uring 让两块 NVMe 各自使用独立 XFS，并通过两个 1,600 GiB 预分配 regular files 执行 4 KiB 对齐的 O_DIRECT I/O。两种 io_uring 方案都不使用 RAID。Dragonfly 使用 v1.40.1、16 proactor threads、双 NVMe Linux RAID0、XFS，并关闭 experimental cooling。Garnet 使用 v2.1.3、.NET 10.0.302、同一个 RAID0/XFS、64 GiB hybrid-log memory、32 GiB read cache、4 GiB index 和 Linux Native libaio。Kvrocks 使用 v2.16.0、16 workers、同一个 RAID0/XFS、80 GiB block cache、BlobDB，并关闭压缩。Pika 使用 Git tag v4.0.3、16 network threads、32 request threads、3 个 RocksDB instances、共 24 GiB block cache 和 32 GiB RTC cache，并关闭压缩与 binlog。Tendis 使用 tag `2.8.4-rocksdb-v8.5.3`、16 executor threads、10 个 RocksDB stores、72 GiB shared block/blob cache、同一个 RAID0/XFS，并关闭 WAL、binlog 和压缩。KeyDB On Flash 使用 v6.3.4、4 个 server threads、64 GiB DRAM 热层、同一个 RAID0/XFS，并保留 RocksDB WAL。

## 结果边界与公平性说明

- Keylane 不使用 LSM-tree，没有 RocksDB compaction；本轮复测始终保持自身 defrag 开启。纯读不产生旧版本，不会主动触发 defrag；1:1 使用每设备最多 2 个活动任务、块间冷却 15 ms；纯写使用每设备最多 6 个活动任务且不设置块间冷却。两种写入负载的记录间冷却均为 0。
- Keylane io_uring 的 raw 和 regular-file 两组使用同一个二进制和服务参数。regular files 以 O_DIRECT 打开，不依赖 Linux page cache；raw 组绕过 XFS，但仍经过 Linux block layer 和 NVMe 内核驱动。两块盘均未组成 RAID，Keylane 自己把两个路径识别为独立设备并各分配 8 个 home workers。
- 当前代码把 `--registered-buffer-mb-per-worker=256` 解释为每个 worker 256 MiB；16 workers 合计约 4 GiB，而不是全进程 256 MiB。SPDK 和 io_uring 两组使用相同设置，因此后端对比一致，但部署容量规划必须按 per-worker 语义计算。
- 三种 Keylane 后端都在各自全量灌数后直接运行正式测试，没有预先老化数据或挑选短窗口。每种后端只灌数一次，正式顺序固定为纯读、1:1 读写混合、纯写。纯读和混合期间两块盘 I/O 量对称；raw 组绕过文件系统，file 组则保留更通用的普通 Linux 文件部署方式。
- raw io_uring 需要独占块设备，部署和运维约束接近 SPDK；regular-file io_uring 包含 XFS 成本，但更接近普通 Linux 文件部署。两者都保留 Linux NVMe 驱动、中断和内核块层成本。
- Dragonfly 的 `backing_file_direct=false` 使用 Linux buffered I/O。正常运行会保留 Linux page cache，因此本轮在灌数后执行 180 秒随机 GET 预热，随后依次执行纯读、1:1 和纯写；正式测试之间不清 page cache、不重启。
- Garnet 使用官方 v2.1.3 Release 源码直接发布二进制，不使用容器。只测试 raw string `GET`/`SET`，因此关闭 object store 和 pub/sub；4 GiB index 按官方每 key 约 16 bytes 的规则覆盖 2 亿 key，避免默认 128 MiB index 产生长 hash chain。
- Garnet storage tier 使用 Linux Native libaio、4 个 completion threads、每设备 512 个最大 in-flight I/O、8 KiB initial record read，并保留默认开启的 scatter-gather GET。64 GiB hybrid log 和 32 GiB read cache 加上 index 后，正式测试结束时进程 RSS 约 102 GiB；这是一组偏向最高性能的配置，不代表低内存部署。
- Garnet 不做额外读预热；全量灌数结束后直接开始纯读，三组之间不重启、不清库。正式测试结束时 64 GiB hybrid log 和 32 GiB read cache 均已分配。
- Garnet 关闭 AOF，不启用周期 checkpoint；每 300 秒执行一次 Lookup compaction，并使用 `compaction-force-delete` 直接删除已回收的旧 segment。该设置把在线旧版本回收成本计入正式窗口，并限制 hybrid log 的持续增长；代价是在 AOF 关闭时不能依赖旧 checkpoint 完成崩溃恢复，因此适用于本报告的 storage-tier cache-store 语义，而不是持久化数据库配置。
- Garnet v2.1.3 在 `--no-obj` 模式下执行 `DBSIZE` 会在全库扫描路径触发 `NullReferenceException` 并关闭该管理连接。它没有影响灌数和 GET/SET 会话；本次使用恰好 200,000,000 次成功 SET、`INFO store` 地址和后续随机 GET 全部命中交叉校验数据完整性。
- Kvrocks 全量灌数和三组正式测试期间均保持正常 flush 与 auto compaction 开启；灌数结束后不等待后台整理完成，直接依次执行纯读、1:1 和纯写。该口径包含在线 compaction 对业务请求的实际影响。
- Kvrocks 配置 80 GiB HCC block cache，灌数结束后不做额外读预热。
- Kvrocks 关闭 WAL、per-write sync、压缩和 Blob GC。WAL 关闭会改变故障恢复语义；正式测试结果只代表这组明确配置下的数据路径性能。
- Pika 使用官方 v4.0.3 tag（commit `d16db1eee9aadb1db42338269936deb7b584ddcc`）直接编译 Release 二进制，不使用容器；该 commit 的二进制版本字符串仍显示 4.0.2，因此同时记录 tag、commit 和自报版本，避免版本歧义。
- Pika 的 3 个 RocksDB instances 各配置 8 GiB shared block cache，合计 24 GiB；RTC cache 配置 32 GiB。本轮不额外等待 cache 预热或后台整理，灌数完成后直接开始正式纯读。
- Pika 关闭 RocksDB WAL/binlog 和压缩，但灌数和三组正式测试期间始终保持 auto compaction 开启，使结果包含在线 compaction 对业务请求的实际影响。
- Tendis 使用官方 tag `2.8.4-rocksdb-v8.5.3`（commit `6a5a4945f1b8dd9d248d8f25325c12881a3cbf5d`）直接编译 Release 二进制，不使用容器。默认的 10 个 RocksDB stores 各自维护 memtable 和后台任务；72 GiB block cache 由所有 stores 共享，Blob cache 计入同一个容量。
- Tendis 关闭 RocksDB WAL、Tendis binlog、per-transaction log flush 和压缩，故障恢复与复制语义不同于默认生产配置；auto compaction 和 Blob GC 在灌数及三组正式测试期间始终开启，覆盖写产生的旧 blob 可以被在线回收。
- Tendis 每个 store 使用 256 MiB write buffer、最多 4 个 memtables；全量灌数完成后不预热、不等待 compaction 清零，直接开始纯读。测试过程中没有发生 write stop 或后台错误。
- KeyDB On Flash 使用官方 v6.3.4（commit `7e7e5e57d25fe246a8201f0acf5e7363c0bf1e14`）源码启用 `ENABLE_FLASH=yes` 直接编译，不使用容器。官方将 On Flash 标注为 beta；结果只代表该实验特性在本 workload 和版本下的表现。
- KeyDB On Flash 使用 64 GiB `maxmemory` 和 `allkeys-lru`，未关闭内存 key cache；2 亿 key 的均匀随机访问与其面向热点数据的设计并不匹配，但与其他后端的测试 key 分布完全相同。正式纯读的所有 GET 都命中，说明低吞吐不是 missing key 导致。
- KeyDB On Flash 保留 RocksDB WAL、auto compaction 和默认无压缩数据列族，AOF/RDB 关闭以避免重复持久化。RocksDB `max_background_jobs=16`、`max_total_wal_size=8 GiB`；4、8、16 个 server threads 的短读测试分别约为 267.9k、202.6k、41.4k QPS，因此正式测试采用官方推荐上限 4 threads。
- KeyDB On Flash 全量灌数后不预热、不等待后台整理，立即开始正式纯读。三组结束后 `DBSIZE` 仍为 200,000,000，服务 active，无错误回复或拒绝连接；数据目录约占 552 GiB。

## 复现步骤

### 1. Keylane defrag 参数

Keylane 三种后端使用相同的动态参数。defrag 始终保持开启；参数在相应 workload 开始前设置。

1:1 读写混合使用较低的后台并行度，并在处理完每个 block 后冷却 15 ms，以降低在线长尾：

```bash
redis-cli -h 10.0.0.4 -p 6379 DEFRAG MAX-ACTIVE 2
redis-cli -h 10.0.0.4 -p 6379 DEFRAG BLOCK-SLEEP 15
redis-cli -h 10.0.0.4 -p 6379 DEFRAG RECORD-SLEEP 0
```

纯写使用更高的每设备并行度，使后台回收速度能够跟上持续覆盖写入：

```bash
redis-cli -h 10.0.0.4 -p 6379 DEFRAG MAX-ACTIVE 6
redis-cli -h 10.0.0.4 -p 6379 DEFRAG BLOCK-SLEEP 0
redis-cli -h 10.0.0.4 -p 6379 DEFRAG RECORD-SLEEP 0
```

`MAX-ACTIVE` 是每个存储设备的上限；本次使用两个设备，因此全进程最多分别有 4 个或 12 个活动 defrag 任务。`BLOCK-SLEEP` 的单位是毫秒，`RECORD-SLEEP` 的单位是微秒。纯读不产生覆盖写旧版本，沿用启动默认值即可。可用下面的命令确认当前配置、活动任务和排队任务：

```bash
redis-cli -h 10.0.0.4 -p 6379 DEFRAG STATUS
```

### 2. 启动 Keylane SPDK

```bash
sudo systemd-run \
  --unit=keylane-spdk.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitMEMLOCK=infinity \
  --property=LimitNOFILE=infinity \
  /path/to/bld-spdk/keylane \
  --bind=10.0.0.4 \
  --data-file=spdk://69f9:00:00.0/1 \
  --data-file=spdk://021d:00:00.0/1
```

### 3. 编译并启动 Keylane io_uring

普通 io_uring 构建显式关闭 SPDK。raw 和 regular-file 两种存储方式共用同一个二进制：

```bash
cmake -S . -B bld-iouring-files -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF \
  -DKEYLANE_ENABLE_OPT=ON \
  -DKEYLANE_WITH_SPDK=OFF
cmake --build bld-iouring-files -j 16
```

#### 双 XFS regular files

下面的两盘初始化命令会清除目标设备的现有文件系统和数据；必须先按实际机器确认设备名，且不能包含系统盘。

```bash
sudo wipefs -a /dev/nvme0n1
sudo wipefs -a /dev/nvme1n1
sudo mkfs.xfs -f -L keylane0 /dev/nvme0n1
sudo mkfs.xfs -f -L keylane1 /dev/nvme1n1

sudo mkdir -p /mnt/data0 /mnt/data1
sudo mount -o noatime /dev/nvme0n1 /mnt/data0
sudo mount -o noatime /dev/nvme1n1 /mnt/data1
sudo chown "$(id -un):$(id -gn)" /mnt/data0 /mnt/data1

fallocate -l 1600G /mnt/data0/keylane.data
fallocate -l 1600G /mnt/data1/keylane.data
```

`1,600 GiB` 是本机实验值，不是 Keylane 固定要求。部署时应按实际磁盘容量预留文件系统日志和运维空间；每个新文件必须是 8 MiB 的整数倍。Keylane 不会在启动时创建、扩展或 truncate 文件。

```bash
sudo systemd-run \
  --unit=keylane-iouring-files.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitMEMLOCK=infinity \
  --property=LimitNOFILE=infinity \
  /path/to/bld-iouring-files/keylane \
  --bind=10.0.0.4 \
  --data-file=/mnt/data0/keylane.data \
  --data-file=/mnt/data1/keylane.data
```

本次启动日志确认每个文件容量为 1,717,986,918,400 bytes、各有 204,799 个 data blocks，两个设备分别分配 8 个 home workers，direct-I/O alignment 为 4,096 bytes。

#### 双 raw block devices

raw 版本需要卸载文件系统并独占设备。以下操作会使原文件系统和 Keylane 文件数据不可访问；`wipefs` 加前 8 MiB zeroout 用于建立新的 Keylane metadata/bitmap，不是全盘安全擦除，旧数据块可能仍物理存在但不会进入新存储集。

```bash
sudo systemctl kill -s SIGINT keylane-iouring-files.service
sudo umount /mnt/data0
sudo umount /mnt/data1

sudo wipefs -a /dev/nvme0n1
sudo wipefs -a /dev/nvme1n1
sudo blkdiscard --zeroout --force \
  --offset 0 --length 8388608 /dev/nvme0n1
sudo blkdiscard --zeroout --force \
  --offset 0 --length 8388608 /dev/nvme1n1

sudo systemd-run \
  --unit=keylane-iouring-block.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitMEMLOCK=infinity \
  --property=LimitNOFILE=infinity \
  /path/to/bld-iouring-files/keylane \
  --bind=10.0.0.4 \
  --data-file=/dev/nvme0n1 \
  --data-file=/dev/nvme1n1
```

本次每个 NVMe 的原始容量为 1,920,383,410,176 bytes；Keylane 使用其中 1,920,378,863,616 bytes，忽略不足一个 8 MiB block 的尾部。每盘有 228,926 个 data blocks、分配 8 个 home workers，direct-I/O alignment 为 512 bytes。

### 4. 创建 RAID0 和 XFS

Dragonfly、Garnet、Kvrocks、Pika、Tendis 和 KeyDB On Flash 在不同时段复用这个文件系统。以下命令会清空 `/dev/nvme0n1` 和 `/dev/nvme1n1`；执行前必须按实际机器重新确认设备名，且不能包含系统盘。

```bash
sudo wipefs -a /dev/nvme0n1
sudo wipefs -a /dev/nvme1n1

sudo mdadm --create /dev/md/storage-raid0 \
  --level=0 \
  --raid-devices=2 \
  --chunk=512 \
  /dev/nvme0n1 /dev/nvme1n1

sudo mkfs.xfs -f -d su=512k,sw=2 /dev/md/storage-raid0
sudo mkdir -p /mnt/data
sudo mount -o noatime /dev/md/storage-raid0 /mnt/data
```

本次实际阵列为 RAID0、512 KiB chunk，总容量 3.49 TiB，挂载点为 `/mnt/data`。

### 5. 启动 Dragonfly Tiered Storage

测试版本：`dragonfly v1.40.1-434478e00c366c711985d0b3269023fc39db8ad1`。直接使用官方 GitHub Release 的 x86-64 二进制，二进制 SHA-256 为 `1d2b6654f4488ebc3f6cd5061199158880f6d957534705cd548a909108507b8b`，不使用容器运行时。版本检查保持 Dragonfly 默认开启。

```bash
curl -fL \
  https://github.com/dragonflydb/dragonfly/releases/download/v1.40.1/dragonfly-x86_64.tar.gz \
  -o /tmp/dragonfly-x86_64.tar.gz
tar -xzf /tmp/dragonfly-x86_64.tar.gz -C /tmp
mv /tmp/dragonfly-x86_64 /tmp/dragonfly-v1.40.1
echo "1d2b6654f4488ebc3f6cd5061199158880f6d957534705cd548a909108507b8b  /tmp/dragonfly-v1.40.1" \
  | sha256sum --check

sudo mkdir -p /mnt/data/dragonfly
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/dragonfly

sudo systemd-run \
  --unit=dragonfly-tiered.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitMEMLOCK=infinity \
  /tmp/dragonfly-v1.40.1 \
  --logtostderr \
  --bind=10.0.0.4 \
  --proactor_threads=16 \
  --maxmemory=64GB \
  --dir=/mnt/data/dragonfly \
  --dbfilename= \
  --tiered_prefix=/mnt/data/dragonfly/tier \
  --tiering_disk_storage_initial_size=40GB \
  --backing_file_direct=false \
  --tiered_offload_threshold=1.0 \
  --tiered_experimental_cooling=false \
  --tiered_max_pending_stash_bytes=16MB
```

Dragonfly v1.40.1 的显式参数均用于确定测试资源边界、容纳完整数据集或提高性能：

| 设置 | 目的与性能含义 |
| --- | --- |
| `--proactor_threads=16` | 明确使用 server 的 16 个可用 CPU，与其他被测系统的 CPU 范围一致。 |
| `--maxmemory=64GB` | 给 Dragonfly 内存层设定 64 GiB 上限；超过内存层容量的数据由 tiered storage 承载，Linux 仍可利用剩余内存作为 page cache。 |
| `--dbfilename=` | 正式窗口不生成 snapshot 文件，避免快照 I/O 干扰在线 GET/SET。 |
| `--tiering_disk_storage_initial_size=40GB` | 把初始 tier 文件从默认 256 MiB 提高到 40 GiB，减少灌数早期反复扩展文件的开销。 |
| `--backing_file_direct=false` | 默认值为 `true`；这里显式使用 buffered I/O，使正常运行中的 Linux page cache 能服务随机读。灌数后预热，正式测试之间不清 cache。 |
| `--tiered_offload_threshold=1.0` | 默认值为 `0.5`；可用内存比例一旦低于 100% 就尽早 offload，避免 2 亿条数据先逼近 64 GiB 内存上限后集中 backpressure。 |
| `--tiered_experimental_cooling=false` | 默认开启的 experimental cooling 在本 workload 下会降低灌数和前台吞吐；关闭中间 cooling 层，让可 offload 的 value 直接进入存储路径。 |
| `--tiered_max_pending_stash_bytes=16MB` | 从默认 256 KiB 提高到 16 MiB，允许更多写入在途和批处理，以更充分利用双 NVMe 带宽。 |

`proactor_affinity_mode=on` 和 `version_check=true` 都保持默认值，因此不在命令中重复。version check 仍然开启。

### 6. 编译并启动 Apache Kvrocks

测试版本：`kvrocks version 2.16.0 (commit 28440b5)`。下面是本次使用的完整配置；其中会影响性能或持久性语义的设置全部保留，避免只公布成绩而隐藏调优条件。

```bash
git clone --branch v2.16.0 --depth 1 https://github.com/apache/kvrocks.git
cd kvrocks
./x.py build --ninja -j16

sudo mkdir -p /mnt/data/kvrocks
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/kvrocks
```

`kvrocks-perf.conf`：

```text
bind 10.0.0.4
port 6379
workers 16
daemonize no
timeout 0
tcp-backlog 8192
maxclients 10000
db-name kvrocks-perf
dir /mnt/data/kvrocks
log-dir stdout:warning
log-level warning
log-retention-days -1
slowlog-log-slower-than -1
slowlog-max-len 0
persist-cluster-nodes-enabled no
max-io-mb 0

enable-blob-cache yes
rocksdb.block_cache_size 81920
rocksdb.block_cache_type hcc
rocksdb.max_open_files -1
rocksdb.write_buffer_size 512
rocksdb.target_file_size_base 512
rocksdb.max_write_buffer_number 8
rocksdb.min_write_buffer_number_to_merge 2
rocksdb.max_background_jobs 16
rocksdb.max_subcompactions 4
rocksdb.wal_compression no
rocksdb.max_total_wal_size 8192
rocksdb.wal_ttl_seconds 3600
rocksdb.wal_size_limit_mb 65536
rocksdb.block_size 16384
rocksdb.cache_index_and_filter_blocks yes
rocksdb.compression no
rocksdb.compression_start_level 0
rocksdb.compaction_readahead_size 2097152
rocksdb.enable_pipelined_write yes
rocksdb.level0_file_num_compaction_trigger 16
rocksdb.level0_slowdown_writes_trigger 128
rocksdb.level0_stop_writes_trigger 256
rocksdb.disable_auto_compactions no
rocksdb.enable_blob_files yes
rocksdb.min_blob_size 1000
rocksdb.blob_file_size 1073741824
rocksdb.enable_blob_garbage_collection no
rocksdb.level_compaction_dynamic_level_bytes no
rocksdb.max_bytes_for_level_base 68719476736
rocksdb.max_bytes_for_level_multiplier 10
rocksdb.read_options.async_io yes
rocksdb.write_options.sync no
rocksdb.write_options.disable_wal yes
rocksdb.write_options.no_slowdown no
rocksdb.rate_limiter_auto_tuned no
rocksdb.partition_filters yes
```

启动时把 16 个 server workers 限定在 server 的 CPU 0–15：

```bash
sudo systemd-run \
  --unit=kvrocks-perf.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitMEMLOCK=infinity \
  --property=LimitNOFILE=infinity \
  /path/to/kvrocks/build/kvrocks \
  -c /path/to/kvrocks-perf.conf
```

这些 Kvrocks 调优项的目的和代价如下：

| 设置 | 目的 | 代价或边界 |
| --- | --- | --- |
| 80 GiB HCC block cache、cache index/filter、16 KiB block | 提高读缓存命中率，减少块读取 | 占用 80 GiB server 内存 |
| BlobDB、1,000-byte threshold、1 GiB blob file、blob cache | 大 value 与 LSM metadata 分离，减少写放大 | 读取需要 metadata + blob 路径；空间回收依赖 Blob GC |
| Blob GC 关闭 | 避免测试期间 GC 抢占 I/O | 覆盖写产生的旧 blob 不回收，磁盘持续增长 |
| SST/WAL/blob 压缩关闭 | 降低 CPU 消耗 | 增加设备容量和写带宽需求 |
| 512 MiB write buffer、最多 8 个、最少 2 个合并 | 扩大写缓冲并减少 L0 flush 文件数 | 增加内存占用 |
| 灌数期间开启 auto compaction，L0 门槛 16/128/256 | 使用正常 flush/compaction 路径构造初始数据 | 灌数期间会占用后台 CPU/I/O |
| 16 background jobs、4 subcompactions、2 MiB compaction readahead | 为恢复常规 compaction 后保留并行能力 | 在线 compaction 会占用 CPU/I/O |
| L1 base 64 GiB、multiplier 10、关闭 dynamic level bytes | 控制常规 level compaction 的容量布局 | 需要按实际数据量调整 |
| pipelined write、async read I/O、I/O 不限速 | 提高并行度和吞吐 | 峰值时更容易打满设备 |
| WAL 和 per-write sync 均关闭 | 隔离数据写入路径并提高写吞吐 | 进程或机器故障可能丢失尚未 flush 的数据 |
| 16 workers、`max_open_files=-1` | 使用全部 server CPU 并避免反复打开文件 | 增加线程和文件描述符资源占用 |

### 7. 编译并启动 Pika

测试源码为官方 `v4.0.3` tag（commit `d16db1eee9aadb1db42338269936deb7b584ddcc`）；该 commit 编译出的 `pika -v` 显示 `pika_version: 4.0.2`。下面使用源码 Release 二进制直接运行，不使用容器：

```bash
git clone --branch v4.0.3 --depth 1 https://github.com/OpenAtomFoundation/pika.git
cd pika
cmake -S . -B output -DCMAKE_BUILD_TYPE=Release
cmake --build output -j 16

sudo mkdir -p /mnt/data/pika/{db,log,dump,dbsync}
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/pika
```

在发行版 `conf/pika.conf` 上设置以下参数；未列出的项目保持 v4.0.3 默认值：

```text
port : 6379
db-instance-num : 3
thread-num : 16
rtc-cache-read : yes
thread-pool-size : 32
log-path : /mnt/data/pika/log/
db-path : /mnt/data/pika/db/
dump-path : /mnt/data/pika/dump/
db-sync-path : /mnt/data/pika/dbsync/
pidfile : /mnt/data/pika/pika.pid

write-buffer-size : 256M
max-write-buffer-size : 8G
max-write-buffer-num : 2
min-write-buffer-number-to-merge : 1
max-subcompactions : 4
max-background-jobs : 12
max-background-flushes : 4
max-background-compactions : 8
compression : none
write-binlog : no

block-cache : 8G
num-shard-bits : 6
share-block-cache : yes
enable-partitioned-index-filters : yes
cache-index-and-filter-blocks : yes
pin_l0_filter_and_index_blocks_in_cache : yes
optimize-filters-for-hits : yes
level-compaction-dynamic-level-bytes : yes

cache-num : 16
cache-model : 1
cache-type : string, set, zset, list, hash, bit
cache-maxmemory : 34359738368
cache-maxmemory-policy : 1
disable_auto_compactions : false
```

`block-cache: 8G` 对每个 RocksDB instance 生效，3 个 instances 合计 24 GiB；`share-block-cache: yes` 是在单个 instance 内由 column families 共享。RTC cache 为 32 GiB。WAL/binlog 和压缩均关闭，属于偏向最高性能的配置，持久性与默认配置不同。

```bash
sudo systemd-run \
  --unit=pika-perf.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitMEMLOCK=infinity \
  --property=LimitNOFILE=infinity \
  /path/to/pika/output/pika \
  -c /path/to/pika/conf/pika.conf
```

正式测试保持 auto compaction 开启。灌数完成后不等待后台任务清零，开始纯读前确认配置仍为 `false`：

```bash
redis-cli -h 10.0.0.4 -p 6379 CONFIG GET disable_auto_compactions
```

### 8. 编译并启动 Microsoft Garnet

测试版本：[`Garnet 2.1.3`](https://github.com/microsoft/garnet/releases/tag/v2.1.3)，tag/commit 为 `v2.1.3` / `b4bf6275351dad3202467d88814a9aee793286c9`。直接发布并运行 Linux x64 二进制，不使用容器。内存和索引容量依据官方 [memory sizing](https://microsoft.github.io/garnet/docs/getting-started/memory) 说明，storage tier、Native I/O 和 read cache 参数见官方 [configuration reference](https://microsoft.github.io/garnet/docs/getting-started/configuration)。该版本要求 .NET SDK 10.0.302：

```bash
git clone --depth 1 --branch v2.1.3 \
  https://github.com/microsoft/garnet.git
cd garnet

curl -fsSL https://dot.net/v1/dotnet-install.sh \
  -o /tmp/garnet-dotnet-install.sh
bash /tmp/garnet-dotnet-install.sh \
  --version 10.0.302 \
  --install-dir /opt/dotnet-garnet

sudo apt-get install -y libaio-dev liburing2 patchelf

/opt/dotnet-garnet/dotnet publish \
  main/GarnetServer/GarnetServer.csproj \
  -c Release \
  -f net10.0 \
  -r linux-x64 \
  --self-contained false \
  -o /opt/garnet-2.1.3
```

Ubuntu 24.04 的 libaio runtime SONAME 是 `libaio.so.1t64`，而官方预编译 native-device library 引用 `libaio.so.1`。本次只改 ELF dependency 名称，不改 Garnet 代码；其他发行版如果 `ldd` 没有显示 `libaio.so.1 => not found`，不需要执行：

```bash
patchelf --replace-needed libaio.so.1 libaio.so.1t64 \
  /opt/garnet-2.1.3/runtimes/linux-x64/native/libnative_device.so
patchelf --replace-needed libaio.so.1 libaio.so.1t64 \
  /opt/garnet-2.1.3/runtimes/linux-x64/native/libnative_device_libaio.so
```

下面配置提供 storage-tier cache-store 语义，不启用 AOF 或 checkpoint recovery。hybrid log 会把内存中放不下的页写到 `hlog.*` segment，但这些文件本身不等于可在进程重启后恢复的数据副本。每 300 秒运行一次 Lookup compaction，并立即删除已经完成回收的旧 segment，使正式结果包含在线回收成本，同时避免覆盖写让磁盘用量无限增长。

```bash
sudo mkdir -p /mnt/data/garnet/log /mnt/data/garnet/checkpoints
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/garnet

sudo systemd-run \
  --unit=garnet-perf.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitNOFILE=1048576 \
  --setenv=DOTNET_ROOT=/opt/dotnet-garnet \
  --setenv=DOTNET_CLI_TELEMETRY_OPTOUT=1 \
  /opt/garnet-2.1.3/GarnetServer \
  --bind 10.0.0.4 \
  --protected-mode false \
  --memory 64g \
  --page 4m \
  --segment 1g \
  --index 4g \
  --index-max-size 4g \
  --storage-tier \
  --logdir /mnt/data/garnet/log \
  --checkpointdir /mnt/data/garnet/checkpoints \
  --readcache \
  --readcache-memory 32g \
  --readcache-page 4m \
  --no-obj \
  --no-pubsub \
  --device-type Native \
  --device-io-backend Libaio \
  --device-completion-threads 4 \
  --device-throttle-limit 512 \
  --initial-io-record-size 8k \
  --compaction-freq 300 \
  --compaction-type Lookup \
  --compaction-force-delete \
  --network-connection-limit 10000 \
  --minthreads 16 \
  --miniothreads 16 \
  --logger-level Warning
```

### 9. 编译并启动 Tendis

测试源码为官方 [`2.8.4-rocksdb-v8.5.3`](https://github.com/Tencent/Tendis/tree/2.8.4-rocksdb-v8.5.3) tag（commit `6a5a4945f1b8dd9d248d8f25325c12881a3cbf5d`）。直接编译并运行 Release 二进制，不使用容器：

```bash
git clone --recursive --branch 2.8.4-rocksdb-v8.5.3 \
  https://github.com/Tencent/Tendis.git
cd Tendis

cmake -S . -B build-perf -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_DISABLE_FIND_PACKAGE_Gflags=TRUE \
  -DCMAKE_DISABLE_FIND_PACKAGE_GTest=TRUE
cmake --build build-perf -j 16 --target tendisplus

sudo mkdir -p /mnt/data/tendis/{db,log,dump}
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/tendis
```

`tendis-perf.conf`：

```text
bind 10.0.0.4
port 6379
daemon off
logLevel warning
logdir /mnt/data/tendis/log
dumpdir /mnt/data/tendis/dump
dir /mnt/data/tendis/db
pidfile /mnt/data/tendis/tendisplus.pid
slowlog /mnt/data/tendis/log/slowlog
maxclients 10000
tcp-backlog 8192

netIoThreadNum 4
executorThreadNum 16
executorWorkPoolSize 4
binlog-enabled no
binlog-save-logs no
checkkeytypeforsetcmd no

rocks.blockcachemb 73728
rocks.blockcache_num_shard_bits 8
rocks.blobcache_in_blockcache yes
rocks.disable_wal yes
rocks.flush_log_at_trx_commit no
rocks.compress_type none
rocks.rate_limiter_rate_bytes_per_sec 0
rocks.write_buffer_size 268435456
rocks.max_write_buffer_number 4
rocks.min_write_buffer_number_to_merge 2
rocks.target_file_size_base 536870912
rocks.max_bytes_for_level_base 68719476736
rocks.level_compaction_dynamic_level_bytes 1
rocks.level0_file_num_compaction_trigger 16
rocks.level0_slowdown_writes_trigger 128
rocks.level0_stop_writes_trigger 256
rocks.max_background_jobs 16
rocks.max_subcompactions 4
rocks.compaction_readahead_size 2097152
rocks.enable_pipelined_write 1
rocks.max_open_files -1
rocks.cache_index_and_filter_blocks 1
rocks.pin_l0_filter_and_index_blocks_in_cache 1
rocks.partition_filters 1
rocks.block_size 16384
rocks.use_direct_reads 0
rocks.use_direct_io_for_flush_and_compaction 0
rocks.enable_blob_files 1
rocks.min_blob_size 1000
rocks.blob_file_size 1073741824
rocks.blob_compression_type none
rocks.enable_blob_garbage_collection 1
rocks.blob_garbage_collection_age_cutoff 0.25
rocks.blob_garbage_collection_force_threshold 0.5
```

72 GiB cache 在 10 个 stores 之间共享；256 MiB write buffer 和最多 4 个 memtables 则对每个 store 分别生效。使用 buffered I/O 让 Linux page cache 参与普通文件访问，但本轮不执行额外 GET 预热。关闭 WAL、Tendis binlog 和压缩以偏向最高数据路径吞吐；保持 auto compaction 和 Blob GC 开启，使实例在覆盖写下具备持续回收旧版本的能力。

```bash
sudo systemd-run \
  --unit=tendis-perf.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitMEMLOCK=infinity \
  --property=LimitNOFILE=infinity \
  /path/to/Tendis/build-perf/bin/tendisplus \
  /path/to/tendis-perf.conf
```

### 10. 编译并启动 KeyDB On Flash

测试版本为官方 [`KeyDB v6.3.4`](https://github.com/Snapchat/KeyDB/releases/tag/v6.3.4)，commit `7e7e5e57d25fe246a8201f0acf5e7363c0bf1e14`。官方 [On Flash 文档](https://docs.keydb.dev/docs/flash/) 将该功能标注为 beta，并要求在编译时显式启用 Flash：

```bash
git clone --recursive --branch v6.3.4 --depth 1 \
  https://github.com/Snapchat/KeyDB.git
cd KeyDB
make -j 16 ENABLE_FLASH=yes BUILD_TLS=no

./src/keydb-server --is-flash-enabled
```

Ubuntu 24.04 的 GCC 13 编译 KeyDB 锁定的旧 RocksDB commit `444b3f4845dd01b0d127c4b420fdd3b50ad56682` 时，两个头文件依赖旧编译器的间接 include。若报 `uint8_t/uint64_t does not name a type`，仅补标准头后重新构建，不改变运行逻辑：

```diff
--- a/deps/rocksdb/table/block_based/data_block_hash_index.h
+++ b/deps/rocksdb/table/block_based/data_block_hash_index.h
@@
+#include <cstdint>
 #include <string>

--- a/deps/rocksdb/util/string_util.h
+++ b/deps/rocksdb/util/string_util.h
@@
+#include <cstdint>
 #include <string>
```

本次 `keydb-flash.conf`：

```text
bind 10.0.0.4
protected-mode no
port 6379
daemonize no
loglevel warning
logfile ""
databases 1
save ""
appendonly no

server-threads 4
server-thread-affinity true
min-clients-per-thread 10
maxclients 10000
tcp-backlog 8192

maxmemory 64gb
maxmemory-policy allkeys-lru
maxmemory-samples 16

storage-provider flash /mnt/data/keydb-flash
storage-provider-options max_background_jobs=16;max_total_wal_size=8589934592
```

64 GiB 是官方 sizing 文档建议的约 50% 物理内存热层；`allkeys-lru` 保留热点数据，淘汰只移除 DRAM cache 中的对象，Flash 中的数据仍可读取。关闭 AOF 和 RDB 是为了避免在 Flash 自身 RocksDB 持久化之外再生成一套日志或快照；RocksDB WAL 保持开启。`max_background_jobs=16` 使用 server 可用 CPU 执行 flush/compaction，8 GiB WAL 上限减少频繁强制 flush，auto compaction 保持开启。

KeyDB 官方提示 server threads 过多会增加 spinlock 争用，并推荐不超过 4。本机用 200 万条数据、80 个连接和 `flash-disable-key-cache=yes` 隔离 Flash 读取后，4、8、16 threads 的 30 秒纯读分别约为 267.9k、202.6k、41.4k QPS，因此正式测试选择 4；`flash-disable-key-cache` 只用于线程选型，正式灌数和测试保持默认 `no`。

```bash
sudo mkdir -p /mnt/data/keydb-flash
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/keydb-flash

sudo systemd-run \
  --unit=keydb-flash.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitMEMLOCK=infinity \
  --property=LimitNOFILE=infinity \
  /path/to/KeyDB/src/keydb-server \
  /path/to/keydb-flash.conf

redis-cli -h 10.0.0.4 -p 6379 INFO memory \
  | grep '^storage_provider:flash'
```

### 11. 全量灌入 2 亿条数据

只在确认目标是允许清空的空白测试实例后执行一次 `FLUSHALL`，再从 client 使用 640 个连接完成全量 SET。`FLUSHALL` 会删除全库，已经灌完数据后不得再次执行：

```bash
redis-cli -h 10.0.0.4 -p 6379 FLUSHALL

taskset -c 0-15 memtier_benchmark \
  -t 16 -c 40 \
  -s 10.0.0.4 -p 6379 \
  -n allkeys \
  --distinct-client-seed \
  --ratio=1:0 \
  --key-pattern=P:P \
  --key-prefix="kv_" \
  --key-minimum=1 \
  --key-maximum=200000000 \
  --random-data \
  --data-size-range=1000-4000 \
  --data-size-pattern=R \
  --hide-histogram
```

灌数阶段只用于构造相同的 2 亿条初始数据，不记录耗时或吞吐，也不计入正式对比结果。

### 12. 预热 Dragonfly page cache

Dragonfly 全量灌数后先用随机 GET 预热 Linux page cache，随后三组正式测试之间不清 page cache、不重启。预热输出不计入正式结果。

```bash
taskset -c 0-15 memtier_benchmark \
  -t 16 -c 40 \
  -s 10.0.0.4 -p 6379 \
  --test-time 180 \
  --distinct-client-seed \
  --ratio=0:1 \
  --key-prefix="kv_" \
  --key-minimum=1 \
  --key-maximum=200000000 \
  --random-data \
  --data-size-range=1000-4000 \
  --data-size-pattern=R \
  --hide-histogram \
  --print-percentiles="99,99.9" \
  --randomize

```

Garnet、Kvrocks、Pika、Tendis 和 KeyDB On Flash 本轮均不执行额外随机 GET 预热；灌数完成后直接开始正式纯读，各自的后台回收或 auto compaction 保持开启。不要用 Garnet v2.1.3 的 `DBSIZE` 验证这组 `--no-obj` 数据；该组合存在前述管理命令异常。应确认全量灌数恰好完成 200,000,000 次 SET，并保存 `INFO store` 地址用于审计。KeyDB On Flash 可用 `DBSIZE` 验证恰好为 200,000,000，并用 `INFO memory` 确认 `storage_provider:flash`。

### 13. 确认 Kvrocks auto compaction

Kvrocks 在灌数和正式测试期间始终保持 auto compaction 开启。灌数完成后不等待正在执行或排队的 compaction，直接开始正式纯读，使测试覆盖真实在线后台整理成本。开始前确认配置没有被动态改为关闭：

```bash
redis-cli -h 10.0.0.4 -p 6379 CONFIG GET rocksdb.disable_auto_compactions
```

记录正式测试开始时的 level、pending compaction 和后台任务状态用于审计，但不以其清零作为开始条件：

```bash
redis-cli -h 10.0.0.4 -p 6379 INFO rocksdb \
  | grep -E 'num_files_at_level|estimate_pending_compaction_bytes|num_running_compactions|compaction_count'
```

### 14. 依次执行三组正式测试

`RATIO` 依次替换为纯读 `0:1`、1:1 混合 `1:1` 和纯写 `1:0`。每个后端只执行一次全量灌数，三组正式测试共用这份数据。每组结束后确认没有 background error；Keylane 还需用 `DEFRAG STATUS` 记录活动和排队任务。Kvrocks、Pika、Garnet、Tendis 和 KeyDB On Flash 的后台 compaction/回收保持开启，不等待任务清零。

所有系统在三组正式测试之间都不清理操作系统 page cache。只有 Dragonfly 在正式测试前执行额外的 Linux page-cache 预热；其余后端不做额外读预热，并保留灌数和前序正式 workload 自然形成的缓存状态。Keylane SPDK、raw io_uring、使用 O_DIRECT regular files 的 io_uring，以及使用 Native O_DIRECT storage tier 的 Garnet 不依赖该 page-cache 路径。

Garnet 三组顺序为纯读、1:1、纯写，全部在同一个进程和数据集上执行，不重启、不清库。正式测试结束时 `Log.BeginAddress=718970813904`、`Log.TailAddress=1533597570336`，log 目录约 702 GiB；服务保持正常，未发现后台错误。

```bash
# 在 Client 执行
taskset -c 0-15 memtier_benchmark \
  -t 8 -c 10 \
  -s 10.0.0.4 -p 6379 \
  --test-time 300 \
  --distinct-client-seed \
  --ratio=RATIO \
  --key-prefix="kv_" \
  --key-minimum=1 \
  --key-maximum=200000000 \
  --random-data \
  --data-size-range=1000-4000 \
  --data-size-pattern=R \
  --hide-histogram \
  --print-percentiles="99,99.9" \
  --randomize
```
