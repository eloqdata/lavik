# Native FULL 普通 baseline record 窗口性能报告

2026-10-04 · [English](README.md) · [公共方法](../native-full-large-record-window-2026-10-04/METHODS.md) · [复现](../native-full-large-record-window-2026-10-04/REPRODUCE.md)

**2026-10-05补测：** [四flow、20ms、十对无抓包对照](followup-2026-10-05/README.zh-CN.md)使FULL中位耗时下降 **7.37%**，**8／10对更快**。[共享Meta CI修复](../native-full-large-record-window-2026-10-04/validation/CI-FIX.md)仅改测试。下文保留2026-10-04原始批次。

普通 baseline records pipeline 在本次单 source flow、密集分布负载下使 FULL 中位数在附加 RTT 5/20 ms 分别下降 **15.69% / 13.69%**。每个 partition+DB 只有一帧的稀疏均匀数据基本没有收益；持续 500 ops/s 写入时 FULL 仍接近十分钟，中位数仅改善 **2.88%**。4-flow 结果波动明显，完整重测组在 20 ms **回退 4.36%**。证据支持密集 records 场景的优化，不支持普遍 FULL 加速。

## 比较对象与条件

本报告独立测量 chunks `51d03f97fede341fb42c14bd46a9f1aaf416eeb1` → records `9da9c797eefaea45dfde3ccc521baa440de559a8` 的增量收益。候选叠在大对象窗口之上，只流水发送同一 partition/DB 的普通 baseline 帧；在 DB 边界、非空 live publish FIFO 服务前及相关生命周期边界保留 drain。普通 replacement/publish-record 和 command 调度没有重新设计。**普通 corpus 未测 main→records**，不能声称 main→完整 stack 的累计收益。

[来源、二进制哈希](evidence/cohort.json)和[构建证据](evidence/build/)固定实际测量版本，双方均为 GCC 13.3 Release native O3/LTO 及同一 Bycorf。8 CPU NVMe 主机上，1 source flow 配 1 target worker；4 source flows 配 2 target workers，CPU 集合互不重叠。Backlog 为每进程全局 64 MiB，本报告 publisher queue 为每 worker 16 MiB。专属 veth/netem 注入 RTT，[条件表](evidence/conditions.md)保存实测 idle PING。fresh seed 不等于冷缓存，也不是超内存数据证明。[公共方法](../native-full-large-record-window-2026-10-04/METHODS.md)、[功能验证](../native-full-large-record-window-2026-10-04/validation/VALIDATION.md)。

Dense seed 为 32,768 个普通 4 KiB values（128 MiB），每个 source flow 集中于一个热 partition。Uniform seed 为跨分区分散的 2,048 个 4 KiB values（8 MiB），缩小规模是为了限制逐分区停等耗时；只能比较组内前后，不能横比不同 corpus 的绝对吞吐。正式性能数据均为 DB 0。

这里有 **13 个完整比较、78 次可比较 accepted runs**，另保留原 4-flow 20 ms 组的 **2 次 partial accepted**。三次配对顺序 A/B、B/A、A/B；accepted 均通过单 FULL、完整 flow、同步后逐 flow fence 可见及完整 digest 相等检查。抓包组另要求 native stream 完整且内核抓包丢包为零。所有有效重复均保留。

## 抓包端到端 FULL

从 `LAVIK.REPLICAOF` 前计时到观察到 target ONLINE，包含 reset、传输、handoff、final cut、轮询；不含启动、seed、后续 digest 验证。耗时下降为正表示变快，保留 r1/r2/r3 原顺序。

| 场景 | 优化前 r1 / r2 / r3（秒） | 前中位数 | 优化后 r1 / r2 / r3（秒） | 后中位数 | 耗时下降 | 加速比 |
| --- | --- | --- | --- | --- | --- | --- |
| dense-live-128m-f1-rtt5 | 639.1325, 637.6717, 644.1651 | 639.1325 | 631.9098, 620.6961, 613.0959 | 620.6961 | 2.88% | 1.030× |
| dense-records-128m-f1-rtt0 | 1.0946, 1.1753, 1.6806 | 1.1753 | 1.0562, 0.9117, 0.8530 | 0.9117 | 22.43% | 1.289× |
| dense-records-128m-f1-rtt1 | 1.6223, 2.0047, 1.6020 | 1.6223 | 1.5166, 1.3880, 1.4304 | 1.4304 | 11.83% | 1.134× |
| dense-records-128m-f1-rtt20 | 8.4498, 8.1705, 8.2523 | 8.2523 | 7.1217, 7.1223, 7.1636 | 7.1223 | 13.69% | 1.159× |
| dense-records-128m-f1-rtt5 | 3.2288, 3.3125, 3.2324 | 3.2324 | 2.7250, 3.3794, 2.6487 | 2.7250 | 15.69% | 1.186× |
| dense-records-128m-f4-rtt20-capture-repeat | 2.6161, 2.8188, 2.7373 | 2.7373 | 2.4943, 2.8566, 3.1522 | 2.8566 | -4.36% | 0.958× |
| dense-records-128m-f4-rtt5 | 1.1112, 1.1117, 2.3956 | 1.1117 | 1.3131, 1.0857, 0.9842 | 1.0857 | 2.35% | 1.024× |
| uniform-records-8m-f1-rtt20 | 46.9394, 46.9540, 46.9488 | 46.9488 | 46.9173, 46.9522, 46.9491 | 46.9491 | -0.00% | 1.000× |
| uniform-records-8m-f1-rtt5 | 12.4175, 12.1926, 12.2356 | 12.2356 | 12.4384, 12.2483, 12.1805 | 12.2483 | -0.10% | 0.999× |
| uniform-records-8m-f4-rtt20 | 12.3455, 12.3812, 12.3048 | 12.3455 | 12.3058, 12.3818, 12.3405 | 12.3405 | 0.04% | 1.000× |
| uniform-records-8m-f4-rtt5 | 3.6508, 3.1978, 3.2735 | 3.2735 | 3.1982, 3.7023, 3.1978 | 3.1982 | 2.30% | 1.024× |

![抓包 FULL 随附加 RTT 变化，中位数及三次最小—最大值](full-vs-rtt.svg)

Dense 5 ms 并非每对都改善：r2 是 3.3125→3.3794 秒。4-flow 5 ms 的 before 有明显波动，4-flow 20 ms 完整重测组回退。Uniform 1-flow 20 ms 为 46.9488→46.9491 秒，基本不变；4-flow 20 ms 也仅差 0.04%。三次重复与最小—最大值不是统计置信区间。

## 同参数无抓包控制

后测控制组仅双方关闭抓包，保留所有 workload/server/workers/CPU 条件。它展示观测条件敏感性，不能精确扣除抓包成本。

| 场景 | 优化前 r1 / r2 / r3（秒） | 前中位数 | 优化后 r1 / r2 / r3（秒） | 后中位数 | 耗时下降 | 加速比 |
| --- | --- | --- | --- | --- | --- | --- |
| dense-records-128m-f1-rtt0-nocap | 0.8314, 0.8737, 0.8717 | 0.8717 | 0.7937, 0.7324, 0.7724 | 0.7724 | 11.40% | 1.129× |
| dense-records-128m-f1-rtt20-nocap | 8.0589, 8.2542, 8.1729 | 8.1729 | 7.5670, 7.0938, 7.1327 | 7.1327 | 12.73% | 1.146× |

Dense 单 flow 在零附加延迟及 20 ms 均保持正收益：11.40% / 12.73%。抓包零延迟的 22.43% 估计更大、波动也更明显，不能外推到稀疏或 4-flow 负载。

## 分布为何决定适用性

静态单 flow dense 实际为**同一个 partition+DB 的 66 个 record frames**；uniform 实际为**2,048 个 groups，每组恰好 1 帧**。候选可重叠 dense 组内的多帧，而保守 DB/partition 边界会在进入下一稀疏组前排空窗口。4-flow dense 有四个密集组。[帧分布](evidence/frame-distribution.csv)和[原始完整 partition+DB 映射](evidence/raw-results.jsonl)记录了实际流量，不只是 corpus 名字。

Dense 1-flow 5 ms 的 source record-byte 观察跨度中位数为 1.4500→0.9456 秒；跨度包括扫描、等待、调度和其他交错工作，并非独占 snapshot/apply 时间。[所有跨度](evidence/spans.md)与端到端时间分开报告，不做阶段相减或 reset/handoff 归因。

## 持续 live 写入

writer 经 5 秒 warmup，以 500/s 在 1,024 个独立 keys 上写 128-byte SET。六轮都无重试完成：before 为 639.1325/637.6717/644.1651 秒，after 为 631.9098/620.6961/613.0959 秒；中位数仅改善 2.88%。实际完成率 499.959–499.979 ops/s，每轮 FULL 内开始的成功延迟样本 306,529–322,069 个，p99 为 0.0709–0.0744 ms，观察最大值 18.55–25.93 ms。这是受控同步到达率的结果，不是饱和容量测试。[逐轮前台统计](evidence/live.md)。

首对各有 1,090 个 record frames，FULL command frames 却有 274,368→271,138。Record payload 字节仍更大，因此这里只比较帧数，不是带宽占比。六轮 FULL 内 sampled source FIFO peak 为 4,437–28,079 bytes，有效采样区间没有 publisher backpressure counter 增量；小队列不证明 command 服务便宜。源码支持的机制包括：仅批发 Peek 当时可见的命令前缀并等待 ACK、逐 partition 交错服务，以及 PR2 在非空 FIFO 前排空 baseline receipts；这些可能限制窗口占用，但没有量化独占耗时。[机制说明](LIVE-COMMAND-MECHANISM.md)明确保留首对作为示例，最终三对结果在这里给出；不会因 `override_catchup` 标签就归咎普通 replacement records。

## 资源与原组抓包失败

Dense 1-flow 5 ms 的“三次各自 FULL 内 sampled peak RSS 的中位数”是 source 198.1→198.2 MiB、target 169.3→169.2 MiB。有效采样区间平均 busy cores 的中位数是 source 0.198→0.252、target 0.089→0.121；区间覆盖 FULL 约 76–80%。Busy cores 增加本身不证明完整 FULL CPU 总量增加。[资源表](evidence/resources.md)、[逐轮 JSON](evidence/per-run.json)保留 RSS、retained memory、queue 和 CPU 有效区间，不将它们当精确 sender-credit 高水位或外推 CPU 总量。

原 `dense-records-128m-f4-rtt20/r2-records` 单次 FULL 已到 ONLINE 4/4，但 tcpdump 报告 **1 个内核抓包丢包**。Runner 在 digest 前拒绝该轮，所以**没有验证后的 digest**；这是观测工具无效样本，不是复制数据错误证据。原 r1 两个 accepted 在 JSON/CSV/资源表中保留为 partial，不混入新组。随后声明的同配置、同 16 MiB capture buffer 的完整六轮全部通过，表内 `capture-repeat` 即该组，保留其 4.36% 回退。[原失败证据](evidence/capture-failure/)。

所有 accepted captures 均通过正常完整性、内核丢包检查，可选时间跨度分析也成功。[逐轮 TCP overlap](evidence/tcp-overlap.jsonl)独立报告重复 source TCP payload；它与重传一致，但不是网络/qdisc 丢包率。冻结矩阵完成后没有增加新诊断负载。

## 审核与复现材料

- [逐轮 CSV](evidence/per-run.csv)、[采样与条件 JSON](evidence/per-run.json)、[比较及配对比例](evidence/comparisons.json)、[原始结果/acceptance/outcome](evidence/raw-results.jsonl)。
- [原始文件清单](evidence/retained-artifacts.json)、[帧分布](evidence/frame-distribution.csv)、[资源](evidence/resources.md)、[前台统计](evidence/live.md)、[实测 RTT 条件](evidence/conditions.md)。
- [公共复现文档](../native-full-large-record-window-2026-10-04/REPRODUCE.md)、[可移植工具](../native-full-large-record-window-2026-10-04/tools/)、[历史工具/recipes](../native-full-large-record-window-2026-10-04/evidence/historical-tools/)、[功能验收](../native-full-large-record-window-2026-10-04/validation/VALIDATION.md)。

完整 pcap/raw samples 保留于 NVMe。最终读回没有任务进程/netns，成功 data 已移除、无效轮次 data 保留。所列代码与二进制来源指实际测量 core commits，后续报告或等价测试 fixture 提交不改变这些测量版本。
