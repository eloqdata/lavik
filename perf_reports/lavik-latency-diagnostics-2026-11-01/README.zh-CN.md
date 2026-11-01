# 在线延迟诊断：服务端性能开销

[English](README.md) | **简体中文**

测量日期为 2026-11-01。最终版本基于 main `9d745dcc`，代码提交为 `4ba8f0b3`。本报告分别评估常驻压力指标、运行时 GET/SET 内部阶段 tracing，以及独立文件日志。默认每 worker 每秒 20 条日志在五轮短测中没有呈现稳定的额外吞吐损失；全采样有可测成本，提高日志限额需要单独评估。

## 整体功能相对 main

每格为三轮测试中位数，轮换运行顺序。两个版本使用相同的优化 Release 构建选项和依赖。默认耗时阈值为 10ms，诊断默认关闭。所有采样模式均保留默认日志输出限额。

| 模式 | QPS | 相对 main | p99 ms | p99.9 ms | CPU µs/op | RSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| main 对照 | 200,189 | +0.00% | 0.623 | 2.975 | 9.180 | 854.53 |
| 诊断关闭，压力指标常驻 | 198,942 | -0.62% | 0.599 | 2.991 | 9.278 | 856.07 |
| 1/64 采样，默认阈值 | 197,268 | -1.46% | 0.631 | 2.991 | 9.345 | 855.89 |
| 全采样，默认阈值 | 191,282 | -4.45% | 0.607 | 2.975 | 9.721 | 855.84 |
| 全采样，阈值为零 | 188,290 | -5.94% | 0.599 | 2.975 | 9.870 | 856.04 |

表中 `always-on` 对应诊断关闭、压力指标常驻；`sample-64` / `sample-1` 对应 1/64 / 全采样；最后一行同时将阈值改为零。结果支持常驻压力计数，并以 1/64 采样进行临时诊断。全采样可在接受额外开销时使用。耗时阈值在样本完成后过滤，可降低保留和输出成本，但不会免去选中命令的计时成本。闭环客户端下吞吐降低不一定导致 p99 增大，应结合 CPU/op 和吞吐判断。

## 默认限额下写文件的增量开销

对照两侧使用同一个最终二进制，采样率、零阈值、直方图和近期记录相同，仅改变 `latency-diagnostics-log-enabled`。每格为五轮中位数，运行顺序轮换。两个 worker 各自拥有入队限额和队列。

| 采样 | QPS 关闭 → 开启 | 变化 | p99 ms 关闭 → 开启 | p99.9 ms 关闭 → 开启 | CPU µs/op 关闭 → 开启 |
|---|---:|---:|---:|---:|---:|
| 1/64 | 197,730 → 199,061 | +0.67% | 0.623 → 0.623 | 2.991 → 3.023 | 9.299 → 9.264 |
| 1/1 | 189,335 → 188,800 | -0.28% | 0.591 → 0.615 | 3.007 → 2.991 | 9.860 → 9.891 |

所有开启日志的 10 秒测量均写出 400 条，即两个 worker 合计约 40 条/秒；关闭时为零。超过限额的单条日志被省略，直方图和近期记录仍继续更新。吞吐变化有正有负。短测不能证明严格小于 1% 的开销，也不能作为尾延迟 SLO；逐轮对照见 [comparison.json](logging-final/comparison.json)。

## 调整日志限额

`CONFIG SET latency-diagnostics-log-max-per-second N` 设置每个 worker 的一秒入队限额，默认 20，0 表示取消速率限制。每 worker 64 条的 SPSC 队列仍有界，满时丢弃日志，避免阻塞命令。消费者从阻塞中恢复后可能批量写出，因此这不是严格的每秒磁盘写入上限。`latency-diagnostics-log-enabled no` 可独立停止新增输出。

以下每格仅有**一组配对测量**，每次 15 秒、阈值为零，属于探索性数据，不能当作稳定估计。关闭和开启使用相同二进制，以隔离文件输出成本。

| 每 worker 每秒限额 | 采样 | QPS 关闭 → 开启 | 变化 | 实际写出条/秒 | 实际写出 MB/秒 | CPU µs/op 关闭 → 开启 |
|---|---|---:|---:|---:|---:|---:|
| 200 | 1/64 | 197,433 → 196,100 | -0.68% | 400 | 0.296 | 9.343 → 9.378 |
| 200 | 1/1 | 189,078 → 187,073 | -1.06% | 400 | 0.296 | 9.882 → 9.921 |
| 0 | 1/64 | 197,855 → 197,319 | -0.27% | 3,067 | 2.274 | 9.319 → 9.389 |
| 0 | 1/1 | 187,678 → 189,286 | +0.86% | 6,246 | 4.639 | 9.878 → 9.946 |

限流、队列满和 sink 失败都会计入 `log_dropped`。不限速不保证每个符合条件的 trace 都落盘。原始结束时诊断计数包含预热，与测量区间的文件写入增量分别保留。

## 测试条件与适用范围

- Intel Xeon Platinum 8573C 虚拟机，8 个逻辑 CPU / 4 个物理核，约 62.8GiB 内存；Ubuntu Linux 6.17.0-1018-azure。服务端固定 CPU 0,2，memtier 固定在不同物理核的 4,6，日志线程不绑定。测量期间停止其他构建、Lavik 测试、Grafana、Prometheus、Alloy 和 Loki。
- memtier 2.5.1，loopback TCP，GET:SET=1:1，512B value，预填 10,000 个 key，2 个客户端线程、每线程 16 个连接。测量期间 GET 全部命中且无连接错误。每次新进程预热 3 秒，再进行上述测量。开启日志时首次创建文件的成本发生在预热期。
- 每次使用独立的 2GiB 数据文件，位于本地 NVMe RAID `/dev/md127`、ext4，使用正常 io_uring 存储与持久化设置，正常退出后删除。本次小数据覆盖写负载不代表大值落盘读取、长期清理或长时间运行的数据集。大值分页 GET 有独立正确性回归，本报告未测其性能。
- worker 复制定长数据到有界 SPSC 队列，独立线程序列化 JSON 并写入独立轮转 JSONL sink，每 20ms 轮询/flush，不做 fsync。磁盘阻塞、远程文件系统和外部采集器需按实际负载另测。最终测量各场景未发生日志轮转。
- 关闭输出仍保留日志线程和队列，因此开关对照衡量输出分支的增量成本。进程 CPU 来自 `/proc/PID/stat`，线程 CPU 来自 `/proc/PID/task/TID/schedstat`；内核回写和采集器 CPU 不包含在进程 CPU 内。RSS 含存储 buffer，是进程快照，不是模块内存分配明细。
- 延迟分位数来自 memtier 客户端。诊断直方图仅覆盖选中的慢命令，边界不同。三轮或五轮短测没有给出置信区间，也不是其他负载的性能上限。

最终构建布局检查显示，每个定长 trace 为 736B。每 worker 的 256 个上下文、128 条近期记录和 64 条日志队列合计为这些副本预留 322KiB，另有计数、直方图和空闲槽管理；诊断关闭时仍保留。默认 `CommandReply` 仍为 72B。选中的分页读取还会在现有 I/O 窗口内临时拥有子任务计时数据。

## 复现与原始数据

候选二进制 SHA-256 为 `7fe05f9f3aea772b1ecef9dad13b46a1f7ccb756c92bc21f5166244f397ec4d7`；main 对照为 `c5462bfd8233b09c79a300d202dfe4b54597a0d528ca5050c4164b588998b9ef`。使用 GCC 13、Ninja 和正常优化 Release 构建。Bycorf 固定为 `9f06006de978bd48bd51773c9d43d5122229aab1`，mimalloc 固定为 `acf2fdd329f9dc2a7ffe3f12a133fe7175e39378`。main 源码快照使用同样依赖和构建设置独立编译，得到下列对照路径。

```bash
cmake -S . -B build_perf -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build_perf

python3 microbench/online_latency_benchmark.py build_perf/lavik build_perf/lavik /mnt/local_nvme/lavik-396/logging-final --logging-only --workers 2 --server-cpus 0,2 --client-cpus 4,6 --single-client-cpus 4,6 --seconds 10

python3 microbench/online_latency_benchmark.py /mnt/local_nvme/lavik-396/baseline-build/lavik build_perf/lavik /mnt/local_nvme/lavik-396/feature-final --workers 2 --server-cpus 0,2 --client-cpus 4,6 --single-client-cpus 4,6 --seconds 10 --rounds 3

python3 microbench/online_latency_benchmark.py build_perf/lavik build_perf/lavik /mnt/local_nvme/lavik-396/logging-limit-200 --logging-only --log-limit 200 --workers 2 --server-cpus 0,2 --client-cpus 4,6 --single-client-cpus 4,6 --rounds 1

python3 microbench/online_latency_benchmark.py build_perf/lavik build_perf/lavik /mnt/local_nvme/lavik-396/logging-limit-0 --logging-only --log-limit 0 --workers 2 --server-cpus 0,2 --client-cpus 4,6 --single-client-cpus 4,6 --rounds 1
```

输出目录必须尚不存在。[整体运行数据](feature-final/runs.json)、[整体汇总](feature-final/summary.json)、[日志运行数据](logging-final/runs.json)、[日志汇总](logging-final/summary.json)、配置限额 [200](logging-limit-200/runs.json) / [0](logging-limit-0/runs.json) 的运行数据，以及运行元数据与源码、二进制标识均随报告提交。每次保留 `memtier.json`、`commands.json`、`server-args.json` 和诊断计数。自动生成的命令数组保留实际完整参数。完整服务端日志、scrape 和诊断 JSONL 留在测量机器 `/mnt/local_nvme/lavik-396/` 对应目录。

## 前一代码快照

[前一轮日志结果](logging/summary.json) 包含 1、2 worker，每模式五轮。其二进制为 `a08effe8ed5672c50ed874b0ddfae609181cf7535534bdcf7567f64c84121d7a`，源于 `b5726e4a` 加[保存的源码差异](logging/source.patch.gz)，早于可配置日志限额和分页读取计时补全。1 worker 在 1/64、全采样下开启日志的吞吐中位数变化分别为 −1.15%、−0.04%；2 worker 为 +0.78%、+1.41%。逐轮变化范围 −2.00% 至 +2.17%，有一组 p99.9 增加 0.448ms。这些是历史快照数据，不代表最终代码。

该快照的[额外 30 秒线程分析](logging-thread-profile/runs.json) 使用 2 worker 全采样。独立消费者关闭输出时用 CPU 15.754ms、开启时 15.605ms，均约占一个核的 0.052%，包含轮询唤醒；差异低于这组测量噪声。进程 QPS 变化 −0.45%、CPU/op 变化 +0.62%。保留此组供参考，最终代码结论以上方测量为准。
