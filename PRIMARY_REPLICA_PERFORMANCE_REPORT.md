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

# Lavik 双机主从性能测试报告

测试日期：2026-08-18 UTC

## 1. 测试结论

本次测试在两台独立的 16 vCPU 服务器上部署 Lavik 主从，每台 Lavik
使用 12 个核心和两块 NVMe，另外 4 个核心用于网络 IRQ/softirq。主从均持有
400,000,000 条、value 大小随机分布在 1,000-4,000 字节的数据，总数据量约
1 TB。

正式测试分别将主节点和从节点客户端限制在 100,000 op/s 和 50,000 op/s。
主节点依次运行纯读、1:1 读写和纯写，从节点在每一轮中同时运行纯读。每轮
持续 300 秒。

主要结论如下：

- 12 个正式 memtier 进程全部正常退出，没有命令错误、miss、MOVED 或 ASK。
- 主从在两档负载下均能达到目标速率；50,000 档实际约为 49,920 op/s，这是
  memtier 单连接限速定时粒度造成的固定偏差。
- 主节点写入会增加从节点的复制 apply 成本。在从节点前台读速率固定为
  50,000 GET/s 时，从节点 CPU 从无复制写入时的 3.21 核，提高到接收
  25,000 SET/s 时的 3.92 核，以及接收 50,000 SET/s 时的 4.24 核。
- 100,000 档的相同三个场景中，从节点分别使用 5.58、6.30 和 7.03 核。
- 降至 50,000 档后，混合及纯写场景的 p99.9 长尾明显下降；纯读场景没有
  同样趋势，说明纯读的小幅尾延迟波动不是复制 apply 压力造成的。
- 全部正式测试期间复制保持 12 条数据 flow online，`lag=0`；测试结束后
  主从 `DBSIZE` 都是 400,000,000。

## 2. 版本与构建

- 被测代码：Lavik `main`，测试开始时为 `ec22e4f`。
- Release/SPDK 二进制：`/mnt/dev/lavik/bld-spdk/lavik`。
- 二进制 SHA-256：
  `5592677e5a40acb264ba0ccc3d5167bfaf9739cf6903c6eae4ecaf525662c31a`。
- 未使用 ASAN。
- READ latency trace 和 SET latency trace 均关闭。
- 测试结果首次写入仓库的提交：`f6d7d16`。

## 3. 测试拓扑

| 角色 | 地址 | Lavik CPU | 网络 CPU | 存储 |
|---|---|---:|---:|---|
| 主节点 | `10.0.0.4:6379` | 0-11，12 workers | 12-15 | `43bc:00:00.0/1`、`58bf:00:00.0/1` |
| 从节点 | `10.0.0.7:6379` | 0-11，12 workers | 12-15 | `3f5e:00:00.0/1`、`489e:00:00.0/1` |
| memtier 客户端 | `10.0.0.5` | 主流量 0-7；从流量 8-15 | 不适用 | 不适用 |

两台存储服务器均配置 4,096 个 2 MiB hugepage。两块盘都由 SPDK/VFIO 使用。
测试前只对每块目标盘开头 8 MiB 做零化。

主节点的 mlx5 IRQ 58-74 分布在 CPU 12-15。从节点没有可用的 accelerated
networking VF，只有 `hv_netvsc`；其 RX queue 的 RPS mask 设置为 `f000`，将
RX softirq 引导到 CPU 12-15。因此两台机器的标称 CPU/存储配置相同，但网络
路径并不完全相同。

## 4. Lavik 运行参数

主从公共关键参数：

```text
--threads=12
--replication-publish-queue-mb-per-worker=64
--registered-buffer-mb-per-worker=256
--busy-poll-us=20
--background-budget-us=10
--background-warrant-percent=1
--spdk-max-completions-per-poll=8
--spdk-foreground-pre-poll-us=5
--mimalloc-purge-delay-ms=60000
--flush-max-ms=1000
--flush-size-kb=128
--disable-read-crc
--tomb-raider-interval-ms=0
--defrag-paused
```

主节点运行时配置：

```text
CONFIG SET replication-snapshot-read-concurrency 16
```

## 5. 数据集准备

- Key 范围：`kv_1` 至 `kv_400000000`。
- Key 分布：随机访问。
- Value：随机内容，大小随机分布在 1,000-4,000 字节。
- 主节点最终 `DBSIZE`：400,000,000。
- 从节点最终 `DBSIZE`：400,000,000。

数据灌入期间曾在 156,847,206 条时暂停客户端，将 snapshot read concurrency
从 8 调至 16，完成从节点同步后继续灌入。最终主从都达到 400,000,000 条。
灌数过程包含暂停、全量同步和客户端 CPU affinity 调整，因此灌数日志不作为
正式吞吐结果。

## 6. 测试方法

memtier 版本为 2.5.1，使用 cluster mode。该版本不能按确定比例同时向同一
shard 的主从分配读请求：

- `--read-preference=primary` 只读主节点；
- `--read-preference=secondary` 只读从节点；
- `secondaryPreferred` 在从节点可用时仍只读从节点；
- `nearest` 根据延迟择优，不保证均匀分流；
- `--replica-clients` 已解析但尚未接入请求分发。

因此每轮并行运行两个 memtier 进程：一个强制访问主节点，另一个强制从节点
只读。每个进程使用 8 threads、每线程 10 clients、pipeline 1。100,000 档
的每连接限速为 1,250 op/s；50,000 档为 625 op/s。

共同数据参数：

```text
--cluster-mode
--key-pattern=R:R
--key-prefix=kv_
--key-minimum=1
--key-maximum=400000000
--random-data
--data-size-range=1000-4000
--data-size-pattern=R
--test-time=300
--print-percentiles=50,99,99.9,99.99
```

三种主节点 workload：

1. `--ratio=0:1`：100% GET。
2. `--ratio=1:1`：50% SET、50% GET。
3. `--ratio=1:0`：100% SET。

从节点进程在三轮中始终使用 `--ratio=0:1` 和
`--read-preference=secondary`。

## 7. 100,000 op/s × 2 测试结果

### 7.1 主节点结果

以下结果是在从节点同时承受约 100,000 GET/s 时测得：

| 主节点 workload | SET/s | GET/s | 总 op/s | 平均 | p50 | p99 | p99.9 | p99.99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 100% GET | 0 | 99,996.85 | 99,996.85 | 0.267 ms | 0.255 ms | 0.511 ms | 0.815 ms | 1.551 ms |
| 1:1 SET:GET | 49,996.65 | 49,996.45 | 99,993.10 | 0.259 ms | 0.231 ms | 0.791 ms | 2.607 ms | 4.095 ms |
| 100% SET | 99,999.77 | 0 | 99,999.77 | 0.229 ms | 0.199 ms | 0.831 ms | 1.671 ms | 2.431 ms |

### 7.2 从节点只读结果

| 同期主节点 workload | 从节点 GET/s | 平均 | p50 | p99 | p99.9 | p99.99 |
|---|---:|---:|---:|---:|---:|---:|
| 100% GET | 99,999.28 | 0.261 ms | 0.255 ms | 0.455 ms | 0.647 ms | 0.935 ms |
| 1:1 SET:GET | 99,986.19 | 0.269 ms | 0.255 ms | 0.543 ms | 1.319 ms | 2.303 ms |
| 100% SET | 99,999.76 | 0.289 ms | 0.271 ms | 0.663 ms | 1.671 ms | 3.503 ms |

### 7.3 Lavik CPU

100% 表示一个逻辑 CPU：

| 主节点 workload | 主节点 CPU | 从节点 CPU |
|---|---:|---:|
| 100% GET | 542.66% | 557.62% |
| 1:1 SET:GET | 486.52% | 630.30% |
| 100% SET | 457.44% | 703.37% |

## 8. 50,000 op/s × 2 测试结果

### 8.1 主节点结果

以下结果是在从节点同时承受约 50,000 GET/s 时测得：

| 主节点 workload | SET/s | GET/s | 总 op/s | 平均 | p50 | p99 | p99.9 | p99.99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 100% GET | 0 | 49,920.86 | 49,920.86 | 0.271 ms | 0.255 ms | 0.639 ms | 1.199 ms | 1.791 ms |
| 1:1 SET:GET | 24,958.41 | 24,958.19 | 49,916.61 | 0.255 ms | 0.239 ms | 0.703 ms | 1.279 ms | 3.199 ms |
| 100% SET | 49,920.27 | 0 | 49,920.27 | 0.234 ms | 0.207 ms | 0.791 ms | 1.375 ms | 2.127 ms |

### 8.2 从节点只读结果

| 同期主节点 workload | 从节点 GET/s | 平均 | p50 | p99 | p99.9 | p99.99 |
|---|---:|---:|---:|---:|---:|---:|
| 100% GET | 49,920.43 | 0.262 ms | 0.247 ms | 0.543 ms | 0.863 ms | 1.263 ms |
| 1:1 SET:GET | 49,919.15 | 0.262 ms | 0.247 ms | 0.527 ms | 0.863 ms | 1.711 ms |
| 100% SET | 49,920.41 | 0.263 ms | 0.247 ms | 0.575 ms | 1.167 ms | 2.415 ms |

### 8.3 Lavik CPU

| 主节点 workload | 主节点 CPU | 从节点 CPU |
|---|---:|---:|
| 100% GET | 304.79% | 320.84% |
| 1:1 SET:GET | 272.18% | 391.99% |
| 100% SET | 252.50% | 423.93% |

## 9. 两档负载对比

从节点始终承担固定读负载，同时还要 apply 主节点产生的 SET。CPU 数据能清楚
显示复制写入的增量成本：

| 每节点目标速率 | 主节点 SET/s | 从节点 GET/s | 从节点 CPU |
|---:|---:|---:|---:|
| 50,000 | 0 | 49,920 | 320.84% |
| 50,000 | 24,958 | 49,919 | 391.99% |
| 50,000 | 49,920 | 49,920 | 423.93% |
| 100,000 | 0 | 99,999 | 557.62% |
| 100,000 | 49,997 | 99,986 | 630.30% |
| 100,000 | 100,000 | 100,000 | 703.37% |

长尾变化：

- 主节点混合 workload 的 p99.9 从 2.607 ms 降到 1.279 ms。
- 同期从节点读 p99.9 从 1.319 ms 降到 0.863 ms。
- 主节点纯写 p99.9 从 1.671 ms 降到 1.375 ms。
- 同期从节点读 p99.9 从 1.671 ms 降到 1.167 ms。
- 纯读场景降低 QPS 后，主节点 p99.9 从 0.815 ms 变为 1.199 ms，从节点
  从 0.647 ms 变为 0.863 ms。这一差异方向与负载相反，更像运行抖动或低负载
  调度差异，不能归因于复制 apply。

## 10. 正确性与稳定性

- 两档负载共 12 个正式 memtier 进程，错误响应均为 0。
- GET miss 为 0。
- MOVED 和 ASK 均为 0。
- 测试期间没有新增 Lavik warning 或 error 日志。
- 复制始终为一个 control connection 加 12 个 data-flow connections。
- 最终 `INFO replication`：从节点 online，`lag=0`。
- 最终主从 `DBSIZE`：400,000,000 / 400,000,000。

这里的 SET 是异步复制测试，没有使用 `WAIT`；客户端收到主节点 SET 响应不代表
从节点已将该条写入持久化。`lag=0` 和最终 key 数相等用于确认测试期间复制没有
出现可见积压或断连，不等价于逐条同步确认。

## 11. 非正式极限读探测

正式限速测试前做过一次极限读探测，结果仅用于发现饱和点，不能作为无错误
性能结果：

- 默认 `--read-preference=primary` 的主节点读达到 586,276.92 GET/s，但出现
  少量 `SPDK I/O submission failed`。
- 强制 `--read-preference=secondary` 的从节点读平均 399,924.78 GET/s，但
  640 个活跃读连接耗尽 SPDK DMA overflow read buffer，产生 1,103,134 个
  `aligned heap read buffer allocation failed` 错误。
- 将正式测试降低到每个 memtier 进程 80 个活跃目标连接后，100,000 和
  50,000 两档均未复现这些错误。

因此极限探测数字只能看作错误条件下的上界，不能与正式结果直接比较。

## 12. 原始数据位置

memtier 日志位于客户端 `10.0.0.5`：

```text
/tmp/lavik-100k-{read,mixed,write}-{primary,secondary}-300s.memtier
/tmp/lavik-50k-{read,mixed,write}-{primary,secondary}-300s.memtier
```

主节点保存的每秒 pidstat 和测试前后 Prometheus 快照：

```text
/tmp/lavik-100k-{read,mixed,write}-{master,replica}.pidstat
/tmp/lavik-100k-{read,mixed,write}-{master,replica}-{before,after}.metrics
/tmp/lavik-50k-{read,mixed,write}-{master,replica}.pidstat
/tmp/lavik-50k-{read,mixed,write}-{master,replica}-{before,after}.metrics
```

服务日志：

```text
主节点：/tmp/lavik-full1t-master.log
从节点：10.0.0.7:/home/azureuser/lavik/logs/full1t-replica.log
```

## 13. 测试限制

- 主从网络路径不同：主节点有 mlx5 VF，从节点只有 `hv_netvsc`。
- 测试只覆盖一个主分片和一个从副本。
- 测试采用异步复制，没有测量 `WAIT` 或同步复制确认延迟。
- `DBSIZE` 一致不能替代逐 key/value 校验。
- 没有在本轮中重启进程验证 1 TB 数据的完整恢复时间。
- 极限读测试暴露的 DMA overflow buffer 高水位问题需要单独优化和回归。
