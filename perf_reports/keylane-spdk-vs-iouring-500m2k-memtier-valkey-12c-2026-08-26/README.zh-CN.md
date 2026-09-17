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

# Keylane SPDK 与裸设备 io_uring 对比：5 亿 key、固定 2 KiB、12 个 worker

[English](README.md) | **简体中文** | [报告目录](../README.md)

> 本报告记录项目更名为 Lavik 之前的 Keylane 测试。产品名、版本、命令和数据均对应当时的实验，
> 不代表当前 Lavik 版本的实测结果。
> 恢复来源： [2026-09-15 历史版本](https://github.com/eloqdata/lavik/tree/eedb3080d808769519d93e971195b53b38b09c1e/perf_reports)。
> 历史 `perf_runs/` 原始输入目录未包含在来源快照中；本次恢复报告及其中的结果表，不包含那些原始日志。

日期：2026-08-26 UTC

## 技术总结

在这台 16 个逻辑 CPU 的主机上，Keylane 使用固定在 CPU 0–11 的 12 个 worker，本地用户进程和 mlx5 IRQ 隔离到 CPU 12–15。两个后端均使用两个 1.92 TB NVMe namespace、Release 构建、pipeline=1、80 个客户端连接、八个客户端线程、固定 2048 字节 value、500,000,000 个已有 key、不限速闭环负载和 300 秒正式窗口。

两个客户端给出的后端对比方向一致：

- SPDK 的随机读和 1:1 读写更快。memtier 下，读吞吐领先裸 io_uring 5.69%，混合吞吐领先 6.72%；Valkey 下，读吞吐领先 8.24%。
- 纯写吞吐基本持平。裸 io_uring 在 memtier 下快 0.49%，在 Valkey 下快 0.19%，但深尾写延迟更高。
- SPDK 读吞吐几乎相同时，memtier 报告 p99.99 为 `1.039 ms`，Valkey 为 `(0.591, 0.623] ms`。裸 io_uring 也有相同现象：memtier 为 `1.079 ms`，Valkey 为 `(0.607, 0.639] ms`。额外读尾延迟因此主要来自客户端，而不是 Keylane 吞吐差异。
- 所有接受的测试均成功退出，保留恰好 500,000,000 个 key，内存拒绝为零，且未产生 Keylane error 或 latency-trace 日志。所有 memtier GET 负载均报告零 miss。

## 主要结果

### memtier_benchmark

memtier 对所要求的百分位给出点估计。

| 后端 | 负载 | QPS | SET QPS | GET QPS | 平均 ms | p99 ms | p99.9 ms | p99.99 ms | p99.999 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SPDK | 读 | 322,639.86 | 0 | 322,639.86 | 0.248 | 0.447 | 0.543 | 1.039 | 1.727 |
| raw io_uring | 读 | 305,282.28 | 0 | 305,282.28 | 0.262 | 0.455 | 0.559 | 1.079 | 1.703 |
| SPDK | 随机 1:1 | 370,683.28 | 185,341.70 | 185,341.58 | 0.216 | 0.551 | 1.351 | 1.639 | 4.959 |
| raw io_uring | 随机 1:1 | 347,351.57 | 173,675.85 | 173,675.72 | 0.230 | 0.679 | 1.543 | 2.255 | 5.919 |
| SPDK | 写 | 419,672.66 | 419,672.66 | 0 | 0.190 | 0.871 | 1.407 | 2.127 | 10.367 |
| raw io_uring | 写 | 421,737.32 | 421,737.32 | 0 | 0.189 | 0.903 | 1.495 | 2.623 | 13.439 |

混合负载行按独立 Redis 命令计数。memtier 对独立随机 key 生成长期相等的 SET:GET 比例；没有执行包含两条命令的事务，也没有强制读写使用相同 key。

### valkey-benchmark

本次刻意不使用 Valkey 测量 1:1，因为其 pipeline=1 下的自定义多命令序列路径无法随连接数扩展。下表中的 Valkey 百分位是保守的 HDR bucket 区间：真实百分位大于下界且不超过上界。

| 后端 | 负载 | QPS | 平均 ms | p99 ms | p99.9 ms | p99.99 ms | p99.999 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| SPDK | 读 | 322,096.47 | 0.230 | 0.423 | `(0.479, 0.511]` | `(0.591, 0.623]` | `(0.703, 1.271]` |
| raw io_uring | 读 | 297,588.91 | 0.251 | 0.439 | `(0.503, 0.527]` | `(0.607, 0.639]` | `(0.727, 3.055]` |
| SPDK | 写 | 434,038.25 | 0.163 | 0.839 | `(1.367, 1.479]` | `(1.879, 2.423]` | `(6.503, 7.599]` |
| raw io_uring | 写 | 434,877.59 | 0.163 | 0.847 | `(1.367, 1.479]` | `(1.951, 3.175]` | `(8.983, 10.367]` |

## 后端对比

| 客户端与负载 | SPDK QPS | raw io_uring QPS | 更快的后端 | 差异 |
|---|---:|---:|---|---:|
| memtier 读 | 322,639.86 | 305,282.28 | SPDK | 5.69% |
| memtier 随机 1:1 | 370,683.28 | 347,351.57 | SPDK | 6.72% |
| memtier 写 | 419,672.66 | 421,737.32 | raw io_uring | 0.49% |
| Valkey 读 | 322,096.47 | 297,588.91 | SPDK | 8.24% |
| Valkey 写 | 434,038.25 | 434,877.59 | raw io_uring | 0.19% |

SPDK 的优势集中在需要等待随机读的负载。它在用户态轮询 NVMe completion，避开裸设备后端的内核提交/完成路径与 managed NVMe IRQ。纯写具有足够的缓冲和批处理，使两个后端的前台吞吐接近。

虽然吞吐相同，裸设备后端的写尾延迟仍然更差。memtier 下，裸设备 p99.99 为 `2.623 ms`，SPDK 为 `2.127 ms`；p99.999 分别为 `13.439 ms` 和 `10.367 ms`。Valkey HDR 上界显示相同方向。

## 客户端对比

固定大小重测将此前的客户端差异隔离出来：

| 后端 | 客户端 | 读 QPS | 平均 ms | p99.9 ms | p99.99 ms |
|---|---|---:|---:|---:|---:|
| SPDK | memtier | 322,639.86 | 0.248 | 0.543 | 1.039 |
| SPDK | Valkey | 322,096.47 | 0.230 | `(0.479, 0.511]` | `(0.591, 0.623]` |
| raw io_uring | memtier | 305,282.28 | 0.262 | 0.559 | 1.079 |
| raw io_uring | Valkey | 297,588.91 | 0.251 | `(0.503, 0.527]` | `(0.607, 0.639]` |

SPDK 下两个客户端的读吞吐只差 0.17%，但 memtier 的 p99.99 至少比 Valkey 的保守上界高 66.8%。裸 io_uring 下 memtier 吞吐快 2.59%，但其 p99.99 至少比 Valkey 上界高 68.9%。这直接表明，即使 memtier 不限制服务端吞吐，也会在闭环负载下引入额外深尾延迟。

写入对比不够严格：memtier 的纯写窗口是在同一数据集的混合窗口后运行，而 Valkey 的纯写仅在读取之后运行。它仍具有运维参考价值，但不能单凭此结果将 3% 的写 QPS 差异归因于客户端实现。

## 异常的裸设备 Valkey 读取

首次 io_uring/Valkey 读取虽完成，但在第 277–282 秒出现持续 5.4 秒的扰动：

- 分段吞吐从约 290–300K QPS 降至约 51K QPS。
- 分段平均延迟升至约 1.54 ms。
- 完整窗口结果降至 290,002.56 QPS，p99.99 为 `(2.535, 2.735] ms`，最大值 `206.463 ms`。
- Keylane 维持约 1120% CPU。Keylane journal、kernel journal 和两块 NVMe 的 SMART 日志均无错误；两设备的 media error 和 critical warning 均为零。

紧接着进行的第二次 300 秒读取未复现该扰动，得到 297,588.91 QPS，p99.99 为 `(0.607, 0.639] ms`。主表采用稳定重测结果。首次测试在历史原始证据中的位置为 `iouring/valkey/read-run1-anomalous`；原因尚未查明，若复现应使用块层延迟跟踪调查。

## 数据集构造

由于原生随机 key 格式不兼容，每个客户端都使用单独重建的逻辑数据集：

- memtier key 为 `kv_1` 到 `kv_500000000`，使用并行顺序 `P:P` 生成和 `--requests=allkeys` 各创建一次。
- Valkey key 为 `kv_000000000000` 到 `kv_000499999999`，使用 `--sequential -n 500000000 -r 500000000` 各创建一次。

两个数据集都恰好有 500,000,000 个 key 和固定 2048 字节 value。每次重建前均执行 `FLUSHALL SYNC`，确认 `DBSIZE=0` 且 `DEFRAG STATUS` 无活动或待处理工作。Key 格式差异带来几字节长度差异，是直接客户端对比的限制，但避免了测量 GET miss 这一更严重的错误。

| 后端 | 客户端格式装载 | 装载 SET/s | 最终 DBSIZE | 拒绝数 |
|---|---|---:|---:|---:|
| SPDK | memtier | 663,908.40 | 500,000,000 | 0 |
| SPDK | Valkey | 654,454.56 | 500,000,000 | 0 |
| raw io_uring | memtier | 652,767.11 | 500,000,000 | 0 |
| raw io_uring | Valkey | 644,019.50 | 500,000,000 | 0 |

所有 memtier 读取和混合测试均报告零 miss。每个 Valkey 数据集的首、中、尾已知 key 均返回 `STRLEN=2048`，正式测试前的五秒随机 GET 探测也成功。

## 运行配置

### CPU 与 IRQ 布局

| 工作 | CPUs |
|---|---|
| Keylane 服务 cgroup 和 12 个固定 worker | 0-11 |
| 本地 user slice、tmux、Codex 和 SSH 启动器 | 12-15 |
| mlx5 异步 IRQ | 15 |
| mlx5 completion IRQ | 12-15 轮转分布 |
| 裸 io_uring NVMe managed completion queue | 每 CPU 一个队列；worker 队列仍在 0-11 |

最终生效状态为 `user-1000.slice AllowedCPUs=12-15`；mlx5 IRQ 58 位于 CPU 15，IRQ 59–74 在 CPU 12–15 上轮转分布。Linux 将裸 NVMe I/O vector 暴露为每 CPU 一个的 managed IRQ，因此无法全部迁出 worker CPU 集合。这部分内核工作有意计入裸 io_uring 的结果。

### Keylane 设置

- `--threads=12 --pin-workers`
- `--busy-poll-us=20`
- `--foreground-budget-us=1000`
- 仅 SPDK：`--spdk-max-completions-per-poll=8`
- SPDK 设备：`spdk://3cfa:00:00.0/1` 和 `spdk://fd75:00:00.0/1`
- 裸设备：稳定的 `/dev/disk/by-id/nvme-Microsoft_NVMe_Direct_Disk_*` 路径
- Release 构建，cross-core、read 和 SET latency tracing 均在编译时设为 `OFF`

裸设备构建在有意清空数据前，成功恢复了 SPDK 的五亿 key 数据集，展示了当时的存储格式兼容性。恢复约耗时 3 分 55 秒，每秒扫描约 2.7–2.8 百万条记录。

### Defrag 设置

所有装载和负载阶段都启用 defrag：

| 负载 | 每设备最大活动数 | Block 等待 | Record 等待 |
|---|---:|---:|---:|
| 读 | 8 | 0 ms | 0 us |
| 随机 1:1 | 2 | 15 ms | 0 us |
| 写 | 6 | 0 ms | 0 us |

纯写窗口观察到了活动 defrag job，结束时无排队或活动 job。因此写入结果包含在线回收，而非仅追加的短时突发。

## 客户端命令

memtier 正式命令模板，将 `RATIO` 设为 `0:1`、`1:1` 或 `1:0`：

```bash
taskset -c 0-15 /usr/bin/memtier_benchmark \
  -s 10.0.0.4 -p 6379 -P redis \
  -t 8 -c 10 --pipeline=1 --run-count=1 --test-time=300 \
  --ratio=RATIO \
  --key-minimum=1 --key-maximum=500000000 \
  --key-pattern=R:R --key-prefix=kv_ --distinct-client-seed \
  --data-size=2048 \
  --print-percentiles=50,95,99,99.9,99.99,99.999
```

Valkey 读取模板；写入测试将 GET 替换为 SET：

```bash
taskset -c 0-15 /tmp/keylane-valkey-offset/valkey-benchmark \
  -h 10.0.0.4 -p 6379 \
  -c 80 --threads 8 -P 1 \
  --warmup 5 --duration 300 --precision 3 --seed 20260826 \
  -r 500000000 -d 2048 -- GET kv___rand_int__
```

客户端版本和哈希：

- memtier `2.5.1`，SHA-256 `9b6ee614dae154c64b17a067a10236b3e6532f7ca52c43b547b87101556dd7d4`
- Valkey commit `382a1349`，SHA-256 `f4a6156b7b8f2200d07b5c8e7ba0a30411a1f9ee1e534ca9a3e69c34cccf46b5`

## 验证与限制

- Keylane commit：`c9f981732539fd32b6ec9000d2608f03e698691b`
- celer commit：`0a70d22086fb14435464253991ca6fc6a90f19d5`
- 每项主要测试只有一个 300 秒正式窗口；Valkey 额外预热五秒。
- 所有主要客户端退出码均为零，前后 `DBSIZE` 均为 500,000,000，`keylane_memory_rejected_commands_total` 一直为零。
- 所有主要服务 journal 窗口均没有 error、fatal、OOM 或 latency-trace 日志。
- 除异常裸设备 Valkey 读取的重测外，这是单主机、单次测试对比，没有测量跨独立进程重启的置信区间。
- 后端顺序使用相同物理设备，而非同时使用。温度低于警告阈值，SMART 未报告 media error。
- 各客户端原生 key 格式不同，无法构成完全相同的客户端 A/B；同一客户端内的后端对比比跨客户端对比更可靠。
- 写入顺序包含真实的数据集老化。memtier 顺序为读、混合、写，Valkey 为读、写。因此跨客户端写差异同时包含客户端行为与此前老化程度的影响。

## 建议的下一步

1. 在这台主机上优先使用 SPDK 处理读和混合负载；测试规模下优势为 5.7–8.2%。
2. 若运维简便性比该读优势更重要，可选择裸 io_uring；纯写吞吐基本相同。
3. 使用 Valkey 刻画亚毫秒深尾读延迟。memtier 仍适合吞吐和混合负载生成，但其 p99.99 读取值包含可测量的客户端延迟。
4. 若裸设备 Valkey 读取扰动复现，在同一时间段捕获 `block:block_rq_issue`、`block:block_rq_complete`、io_uring completion 延迟、每 worker 排队延迟及 `/proc/interrupts`。
5. 若发布门槛需要置信区间而非单次结果，应交替后端顺序，将整个矩阵重复三次。

## 证据

- 经复核的精简结果已包含在上述主要结果表中。
- 历史原始运行根目录：`perf_runs/keylane-500m2k-spdk-vs-iouring-memtier-valkey-12c-20260826/`
