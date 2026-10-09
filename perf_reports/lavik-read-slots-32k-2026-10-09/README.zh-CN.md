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

# 32 KiB 注册读槽：SPDK / memtier 对照

测试日期：2026-10-09。基于 main `101a72544ef99cd17514b452e32e1138e15d290a`，将服务端和读池默认 payload 从 1 MiB 调整为 32 KiB，并修复缩小槽后暴露的 SPDK qpair 请求耗尽处理。没有包含之前的连接请求 buffer 优化。

## 三次中位数

| Command | 1 MiB QPS | 32 KiB QPS | Change | 1 MiB p99.9 ms | 32 KiB p99.9 ms |
| --- | --- | --- | --- | --- | --- |
| GET | 939,694 | 940,702 | +0.11% | 5.823 | 5.791 |
| SET | 725,675 | 710,871 | -2.04% | 10.623 | 11.391 |

| Command | Read payload KiB | QPS samples |
| --- | --- | --- |
| GET | 1024 | 939,694, 941,733, 939,025 |
| GET | 32 | 939,807, 940,702, 958,245 |
| SET | 1024 | 725,675, 724,790, 730,844 |
| SET | 32 | 709,828, 710,871, 716,681 |

GET 基本持平；在相同 256 MiB/worker 注册池预算下，SET 中位数下降 2.04%。这不是已证实的无回退结果。

## 方法与边界

- 对照组使用原 Bycorf、传 `--storage-read-buffer-kb=1024`；候选组包含下述 SPDK 排队修复，并省略参数验证 32 KiB 新默认值。双方 Lavik 源码和编译参数相同，按 1024→32 交替三轮。结果覆盖完整改动的开销，无法单独归因于槽大小；样本量不足以作统计显著性断言。
- GCC 13.3、Release/O3/LTO、x86-64-v2、SPDK 开启、静态 C++ runtime、测试故障注入关闭。源码、依赖版本、二进制及补丁校验和见 [build-manifest.json](build-manifest.json)。
- 16 worker，双方 CPU 0–15，独立 memtier 客户端，kernel TCP + 六块原生 SPDK NVMe，8 GiB EAL hugepages。保留上一轮重新灌入的十亿条 1 KiB 数据，qpair owner 计划不变，未清盘。
- 640 连接，memtier 2.5.1，16 线程、pipeline=1、均匀随机、distinct client seeds；每次先预热 GET 10 秒，再 GET/SET 各 60 秒。
- defrag 开启；registered buffer 256 MiB/worker，四个 8 MiB 写 buffer，flush 1000 ms，completion cap 16，pre-poll 5 us，其他参数和此前 SPDK 对照一致。
- 本次预算下，读槽从 222 个变为 5,734 个。产品默认 64 MiB 预算下，理论上从 31 个变为 819 个；这个较小预算未在本报告单独压测。
- 普通 compact 记录与 8 KiB group 通常可装入 32 KiB payload，余量用于记录头和直接 I/O 对齐。超过槽大小的记录和整值拼装继续走现有可复用 overflow 分配，不把 16 KiB 当成硬记录上限。

## 补测与结论边界

| Bycorf | Read KiB | Pool MiB/worker | SET QPS samples | Median QPS |
| --- | --- | --- | --- | --- |
| fixed | 1024 | 256 | 728,472, 709,804, 725,370 | 725,370 |
| fixed | 32 | 41 | 712,434, 726,706, 713,383 | 713,383 |

补测沿用相同二进制、数据和 memtier 参数，每个 session 预热 GET 10 秒后连续采样三次 SET，未像主对照一样每次都重启并先跑 60 秒 GET。因此它们用于辅助诊断，不能代替主对照或作严格的单变量因果结论。

固定 1 MiB 的修复版 Bycorf 中位数为 725,370 QPS，接近原基线；32 KiB 将注册池降至 41 MiB/worker（230 个读槽，接近旧配置的 222 个）后中位数为 713,383 QPS，未消除主对照观察到的差距。持续写入的单次波动也超过 2%。这些结果不足以将差距简单归因于新增重试路径或读槽数量，SET 差距的微观原因尚未定位。

## SPDK 请求容量修复

仅缩小读槽的初版在 GET 压测中出现 23 次 `SPDK I/O submission failed`。诊断重跑再次出现 16 次错误，返回 `-ENOMEM`、未完成 I/O 恰好 512；这与当前 SPDK 每 qpair 默认 512 个请求描述符一致。普通读取必须先取得固定读槽；旧配置至多保有 222 个普通读 lease，耗尽后等待在读池中；新配置有 5,734 个槽，允许更多读取继续提交。客户端并发仍是 640，不能从容量变化推断总排队量增加。当时日志没有逐 worker 的路由和调度轨迹；后续诊断捕获了相同并发下局部积压的形成过程，见下文。两轮失败结果均排除，保留在原始证据中。

修复在 worker 内为每个 qpair 保存等待队列，在轮询完成事件后重试，保持缓冲区与回调存活，等待项也计入 outstanding。它不增加共享锁，不扩大 qpair；没有可等待的 I/O 时仍返回错误，防止永久等待。应用侧原有 4,096 个请求对象上限保留。

新增只读硬件回归检查：两个 qpair 各连续提交 1,024 次 4 KiB 读取，提交之间不轮询，重复四轮。旧版每轮拒绝 1,024 次，修复版每轮 2,048 次全部完成，逐项校验结果内容和回调只执行一次。检查也覆盖未完成时拒绝 close 和请求对象复用。

## 验证

36 个单元测试与 3 个分组存储端到端测试通过。另验证了 8/16/24/32 KiB 邻近边界、最高 2 MiB String、超过 32 KiB 的长 key、128/256 KiB 集合元素，以及关闭重启后的磁盘读取和字节一致性。边界协议检查分别覆盖 io_uring 和 SPDK；SPDK 使用空的 DB 15，检查后逐键删除并验证该库为空，原 DB 0 十亿条数据计数不变。大值正确性验证不等于大值吞吐量测试；上表性能结果针对 1 KiB 工作负载。

12 个主对照点和 6 个 SET 补测点均无客户端错误，GET 无 miss；六个 session 前后均校验十亿条计数和样本长度，正常退出。结束后恢复六个内核 NVMe 驱动、hugepages=0、VFIO unsafe no-IOMMU=N。

[Lavik 补丁](read-slots-32k.patch)、[Bycorf 补丁](spdk-backpressure.patch)、[结果 CSV](results.csv)、[原始证据](evidence.tar.gz)、[校验和](SHA256SUMS)。本机原始目录：`/mnt/dev/peer-bench/read-slots-32k-2026-10-09/`。


## 后续调试：局部积压的来源

补充运行使用同一插桩版二进制，分别传 32 KiB / 1 MiB，其余仍为 SPDK、640 连接、pipeline=1、defrag 开启、原生 owner 数据。记录每个请求的生命周期、最近 64 次 poll、执行超过 1 ms 的任务，以及 Linux `sched_switch` / `sched_wakeup`，以 TSC 与 CLOCK_MONOTONIC 锚点对齐。它用于定位原因，不计入性能对照。

32 KiB 运行中，5 个 worker 的首次压力快照位于调度记录覆盖范围内；此前的 8 个长 poll 间隔有 95.7%–99.4% 的时间被调度记录证实处于 off-CPU 且仍可运行状态。示例：

| Worker | poll 间隔 ms | off-CPU ms | 换出时接替的线程 |
|---|---:|---:|---|
| 5 | 11.705 | 11.589 | sshd |
| 12 | 8.231 | 7.967 | sshd / kworker |
| 13 | 10.956 | 10.807 | sshd |
| 2 | 4.209 | 4.027 | tokio-rt-worker |
| 4 | 10.588 | 10.524 | sshd |

长任务日志对应普通命令派发，其超过 1 ms 的墙钟时间同样主要落在 off-CPU。这里没有发现命令自身执行长循环导致的停顿。worker 被换出时，其他 worker 仍完成请求；连接继续随机选 key，越来越多请求暂时聚集到落后的 owner。恢复运行后继续提交请求、每次最多回收 16 个完成，能够形成局部积压，客户端总并发无需上升。

修复版 32 KiB 的 outstanding 峰值 540，138 次首次提交遇到暂时性 `-ENOMEM`，最终全部完成；接受、回收、完成计数均为 66,031,238。相同修复版 1 MiB 的峰值 218，未发生描述符耗尽，计数均为 66,172,294。两组分别累计验证至少 6,400 万和 6,450 万次逻辑 owner → 物理 owner 路由均本地；本次没有发现 owner 错配、I/O 泄漏或重复完成的证据。

确定的正确性缺陷是旧 Bycorf 将可恢复的描述符耗尽作为 I/O 错误；旧读池较小会提前阻塞普通读取，从而掩盖这个触发条件。修复见 [Bycorf PR #9](https://github.com/eloqdata/bycorf/pull/9)。这解释了为何固定 640 连接仍能触发局部满队列，**不能据此将原来的 SET −2.04% 归因于排队或宣称性能无回退**。插桩打印和 perf 启动本身会扰动调度，因此这些峰值及错误次数不代表未插桩版本的自然频率，也不能回溯证明最初 16 个错误各自的触发过程。

[调试证据](debug-evidence.tar.gz)包含源码插桩差异、二进制哈希、原始日志、调度事件摘录、对齐脚本与分析结果。完整 perf 记录留在 `/mnt/dev/peer-bench/spdk-saturation-debug-2026-10-09/pressure/`。所有服务已正常 checkpoint 退出，测试盘恢复 NVMe 驱动，hugepages=0、unsafe no-IOMMU=N。
