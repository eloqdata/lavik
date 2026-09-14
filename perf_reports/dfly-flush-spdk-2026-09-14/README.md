# SPDK：100ms 与 1000ms flush 的 GET / SET 吞吐对比

日期：2026-09-14 UTC。18 轮全部完成，640 连接，每轮预热 30 秒、正式测试 300 秒，
每种 workload / flush 配置各三轮。正式阶段共 **4,684,361,101** 次成功请求，
其中 GET 2,616,193,062 次、SET 2,068,168,039 次；零请求错误、GET 全部命中。

**本次没有看到 100ms 带来稳定的 QPS 损失。** 相对 1000ms，纯 GET 汇总 +0.08%、
纯 SET +0.37%，两者逐对方向不一致；1:1 混合汇总 +2.31%，但三对分别为
+4.91%、+2.33%、+0.05%，最后一对基本持平，不能把 +2.31% 当成稳定收益或通用回归系数。
覆盖写的数据布局与 GC 活动随轮次变化，限制了严格的因果归因。

100ms 下，纯 SET 和混合的**总体 p99、p999 在三对中都更低**；p9999 则没有一致改善。
这份结果支持继续评估 100ms，不是“所有长尾已解决”的证明。本次只提交报告与复现脚本，
未修改生产代码或默认 flush 参数。

更新后的 [YCSB / Aerospike 100ms 对比报告](../ycsb-rerun-2026-09-13/README.md)
已先通过 [`2f02e6a`](https://github.com/thweetkomputer/keylane/commit/2f02e6a75e60e942946257b8a48135cd72c8d9ff)
推送到 main。本目录是独立的 String GET/SET、SPDK 实验，不与 Hash YCSB 数字混用。

## 结果

QPS 为三轮请求数之和 / 三轮时长之和；延迟是三轮各自百分位的中位数，
**不是合并请求后的百分位**。QPS 取整，读写子项取整后可能与总数相差 1。

| Workload | flush ms | 总 QPS | GET QPS | SET QPS | p99 中位数 ms | p999 中位数 ms |
|---|---:|---:|---:|---:|---:|---:|
| get | 1000 | 1,075,258 | 1,075,258 | 0 | 0.890 | 2.301 |
| get | 100 | 1,076,104 | 1,076,104 | 0 | 0.893 | 2.341 |
| set | 1000 | 769,825 | 0 | 769,825 | 7.827 | 11.960 |
| set | 100 | 772,645 | 0 | 772,645 | 6.128 | 9.535 |
| mixed | 1000 | 746,792 | 373,401 | 373,390 | 6.781 | 11.052 |
| mixed | 100 | 764,076 | 382,043 | 382,034 | 5.428 | 9.856 |

以下变化率均为 `100ms / 1000ms − 1`，不是相反方向：

| Workload | 汇总 QPS 变化 | 配对 1 | 配对 2 | 配对 3 |
|---|---:|---:|---:|---:|
| get | +0.08% | -0.16% | -0.07% | +0.46% |
| set | +0.37% | -2.02% | +0.23% | +3.03% |
| mixed | +2.31% | +4.91% | +2.33% | +0.05% |

### 混合时分别看读、写

| flush ms | 操作 | QPS | 平均延迟中位数 ms | p99 中位数 ms | p999 中位数 ms |
|---:|---|---:|---:|---:|---:|
| 1000 | Gets | 373,401 | 1.105 | 7.908 | 11.976 |
| 1000 | Sets | 373,390 | 0.629 | 4.593 | 8.058 |
| 100 | Gets | 382,043 | 1.064 | 6.599 | 11.388 |
| 100 | Sets | 382,034 | 0.630 | 3.564 | 6.130 |

读写平均延迟也分别取三轮中位数，不应再用它们相加重建总体分位数。
客户端没有分操作导出 p9999；下表只给**总体** p9999 的原始直方图半开桶区间。

### 全部 18 轮，按实际执行顺序

| Workload | 轮次 | flush ms | 总 QPS | GET QPS | SET QPS | p99 ms | p999 ms | 总体 p9999 桶 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| get | 1 | 1000 | 1,074,763 | 1,074,763 | 0 | 0.890 | 2.242 | [14, 16) |
| get | 1 | 100 | 1,073,045 | 1,073,045 | 0 | 0.895 | 2.341 | [16, 18) |
| get | 2 | 100 | 1,076,105 | 1,076,105 | 0 | 0.893 | 2.398 | [16, 18) |
| get | 2 | 1000 | 1,076,808 | 1,076,808 | 0 | 0.890 | 2.356 | [14, 16) |
| get | 3 | 1000 | 1,074,202 | 1,074,202 | 0 | 0.887 | 2.301 | [16, 18) |
| get | 3 | 100 | 1,079,163 | 1,079,163 | 0 | 0.893 | 2.283 | [14, 16) |
| set | 1 | 1000 | 792,034 | 0 | 792,034 | 8.753 | 13.274 | [18, 20) |
| set | 1 | 100 | 776,004 | 0 | 776,004 | 6.128 | 9.535 | [20, 25) |
| set | 2 | 100 | 767,090 | 0 | 767,090 | 6.442 | 9.867 | [14, 16) |
| set | 2 | 1000 | 765,366 | 0 | 765,366 | 6.917 | 10.875 | [16, 18) |
| set | 3 | 1000 | 752,074 | 0 | 752,074 | 7.827 | 11.960 | [16, 18) |
| set | 3 | 100 | 774,841 | 0 | 774,841 | 6.104 | 9.397 | [16, 18) |
| mixed | 1 | 1000 | 697,317 | 348,667 | 348,650 | 6.781 | 10.972 | [20, 25) |
| mixed | 1 | 100 | 731,553 | 365,787 | 365,765 | 5.805 | 9.910 | [18, 20) |
| mixed | 2 | 100 | 753,560 | 376,762 | 376,798 | 5.428 | 9.677 | [18, 20) |
| mixed | 2 | 1000 | 736,368 | 368,174 | 368,195 | 6.539 | 11.052 | [18, 20) |
| mixed | 3 | 1000 | 806,689 | 403,363 | 403,326 | 7.636 | 12.419 | [18, 20) |
| mixed | 3 | 100 | 807,117 | 403,579 | 403,538 | 5.309 | 9.856 | [20, 25) |

每行的原始数据可按标签找到，例如
[mixed-p1-1000ms.measured.client.json](mixed-p1-1000ms.measured.client.json)、
[同轮原始客户端日志](mixed-p1-1000ms.measured.client.log)；
所有未取整数据和汇总在 [results.json](results.json)，包括每轮读写延迟及最大值。

## I/O 与 GC：为什么小幅 QPS 变化不能全算到 timer 头上

以下是覆盖正式阶段的前后 metrics 快照差，包含边界处短暂的后台活动；
是服务端存储操作计数，**不是客户端延迟，也不是 SSD 固件内部 GC 统计**。
GiB = 2³⁰ 字节。SPDK 的 fdatasync 对应 NVMe FLUSH 命令，计数不是写请求数。

| Workload | 轮次 | flush ms | storage read GiB | storage write GiB | NVMe FLUSH 次数 | GC 成功次数 |
|---|---:|---:|---:|---:|---:|---:|
| set | 1 | 1000 | 0.48 | 246.42 | 73,038 | 62 |
| set | 1 | 100 | 253.23 | 358.08 | 280,018 | 32,562 |
| set | 2 | 100 | 233.38 | 354.94 | 272,147 | 29,873 |
| set | 2 | 1000 | 320.25 | 340.96 | 221,485 | 43,386 |
| set | 3 | 1000 | 218.73 | 340.14 | 180,373 | 28,000 |
| set | 3 | 100 | 263.72 | 364.18 | 285,898 | 33,758 |
| mixed | 1 | 1000 | 508.81 | 266.41 | 212,223 | 44,996 |
| mixed | 1 | 100 | 480.41 | 247.76 | 277,562 | 41,633 |
| mixed | 2 | 100 | 507.05 | 210.45 | 280,275 | 47,806 |
| mixed | 2 | 1000 | 461.84 | 222.26 | 179,908 | 38,145 |
| mixed | 3 | 1000 | 239.48 | 151.20 | 70,416 | 7,904 |
| mixed | 3 | 100 | 332.29 | 196.00 | 201,387 | 19,309 |

- 六轮纯 GET 共 1,936,271,239 次，storage read 次数与 GET 相同；
  storage write、fdatasync、Keylane GC 成功次数增量均为 **0**。
  即使没有这些后台写活动，总体 p9999 仍落在 14–18ms 范围内的桶，
  因此周期刷盘不能解释本次纯 GET 的全部残余长尾；本实验没有进一步定位它们的来源。
- 第一轮纯 SET 的 GC 成功次数只有 62，后续纯写轮次为数万；
  混合第三对 1000ms / 100ms 分别为 7,904 / 19,309 次。
  连续覆盖、复用 seed 和逐渐变化的旧版本分布使配对物理状态并不相等。
  两种周期的 flush 次数、写入字节量也不同，不能只按参数值推断持久化工作的成本。
- 所有轮次 GC error / resource_exhausted 计数均为零。未逐轮重灌，
  未在每种配置下建立独立的满盘 GC 稳态，因此表格是本次运行条件下的观测，
  不是“仅周期参数导致了精确百分比变化”的证明。

## 问题与预先固定的方法

仅比较 `--flush-max-ms=100` 和 `1000`。两者使用同一个 Release 二进制、
同一份 10 亿个数字 key、每个 value 1024 字节的数据库，全部六盘直连 SPDK。
数据体共 1,024,000,000,000 字节，不含 key、记录头、索引、对齐和 checkpoint。
`flush-max-ms` 控制部分写块的周期 flush；满块、defrag、shutdown 等路径仍可提前 flush，
不是强制等到 100/1000ms 才允许落盘。普通 SET 回复也不是同步持久化承诺。
SPDK 后端会把 `Fdatasync` 提交为 NVMe FLUSH，不能因绕过内核块层就假定 barrier 没有成本。

- `.5` 客户端：16 个线程 × 每线程 40 个连接 = **640 TCP 连接**；pipeline 1、
  `qps=0`、全范围 uniform、没有 key 前缀。该闭环测试不代表开环延迟 SLA。
- 工作负载：GET（0:1）、SET 覆盖已有 key（1:0）、SET:GET=1:1。
  这里不是 YCSB 的 Hash field UPDATE，也不是增加新 key 的 INSERT。
  dfly_bench 的混合 ratio 是概率选择，而非逐条交替；表里分别列实际完成的读写数/QPS。
- 正式每轮预热 30 秒、测 300 秒；每组顺序预先固定为
  `1000→100、100→1000、1000→100ms`，共 18 轮，不挑最好的一轮。
  配对内相同 seed，正式阶段为 43/44/45、预热为 101/102/103。
  配对复用随机序列不等于物理状态相同：第二次覆盖会重访一部分刚改过的 key，
  旧版本分布与可回收块数会变化；这是一份连续老化数据集上的配对实验，
  不是逐轮介质克隆后的完全等状态因果实验。
- 六轮纯 GET 先完成，然后六轮 SET，最后六轮混合；每轮正常停止、checkpoint 恢复。
  defrag 始终开启。覆盖写会使物理数据集逐渐老化，未逐轮重灌、未声称满盘 GC 稳态。
- [旧 dfly GET 报告](../keylane-spdk-dfly-bench-1b-value-size-limit-16worker-2026-08-31.md)
  使用每轮 6400 万次请求；本次改用固定 300 秒，保持全部连接持续发压，
  避免不同连接完成固定请求数的先后差异改变末段并发。
  旧报告关闭 defrag 并显式设置部分调度参数；本次 defrag 开启、调度参数保持当前默认值。
  因此历史 QPS 只作配方背景，不作本次单变量回归基线。

性能只来自客户端 dfly_bench。主 QPS 为 JSON 完成数 / 全程秒数；终端的
连接局部速率之和另行保存。分别核验 GET/SET 客户端计数与服务端 commandstats。
正式阶段启用 `--vmodule=dfly_bench=1 --logtostderr=true`，仅取每线程结束时的
精确 hit 计数，检查其和等于 GET 完成数；不依赖四舍五入的 `Hit rate: 100%`。
这不是逐请求日志，两种 flush 配置使用相同客户端参数。
客户端导出 p99、p999；p9999 仅报告总体直方图所在的半开桶区间，不伪造精确值，
不由服务端百分位替代，也不对两边百分位做减法。
汇总 QPS 使用三轮请求数之和 / 三轮时长之和；汇总延迟使用三轮各自百分位的中位数，
不是合并全部请求重新计算的百分位。每种配置只有三轮，不作“无差异”的显著性证明。

## 版本与机器

| 项目 | 固定配置 |
|---|---|
| Keylane main | `2f02e6a75e60e942946257b8a48135cd72c8d9ff`（生产代码来自 `73a348f`） |
| Celer | `f45d90c1b1a4f3954a2471992e9d404327887169` |
| Keylane 二进制 SHA-256 | `57986c5a42dc8b1b74ec1725d1ea8b4cb2f8151451caa16d4261264e2b4c494d` |
| 编译 | Clang 18、Release、native、IPO、静态 OpenSSL/C++ runtime、SPDK；fault/tracing 关闭 |
| dfly_bench SHA-256 | `68fbf912ddd469e621025e35b5b42cb658ed0daef476bf725a9d1e37cf0a538f`，与旧 dfly 报告相同 |
| 服务端 `.4` | Azure `Standard_L16aos_v4`，AMD EPYC 9V74，16 逻辑 CPU（8 核 × 2），约 125.8 GiB RAM |
| 客户端 `.5` | Azure `Standard_F16als_v7`，16 逻辑 CPU；[完整信息](client-machine.txt) |
| 区域 | 两端均为 `japaneast`、zone 3 |
| 存储 | 六块 Microsoft NVMe Direct Disk v2，各 1,919,850,381,312 字节；无 RAID、无文件系统 |
| 服务端 CPU | 16 workers，各自固定 CPU 0–15；独立 systemd benchmark slice，effective cpuset=0–15 |
| 网络与后台 | mlx5 16 个 completion IRQ 按队列号分散到 CPU 0–15；系统/用户 housekeeping 仍为 12–15 |
| 监控 | `.5` Prometheus/Grafana 保持运行；Keylane metrics 端口 19100；不运行 perf/tracing |

新编译未改生产源码。SPDK 的 ISA-L 构建自动重写了 manpage 的日期/版本行，
见 [自动生成差异](build-generated-manpage.diff)，不是运行时代码修改。
构建命令和日志见 [build.sh](build.sh)、[build.log](build.log)。

## 清盘与驱动

**原 `/dev/md0p1` 的 Keylane 数据和 `/dev/md0p2` 的 Aerospike 数据已删除，
无备份不能恢复。** `/dev/nvme0n1` 系统盘、`/dev/nvme0n2` 工作盘和仓库未清理。

[准备脚本](prepare_media.py) 核验六盘 serial、BDF、容量、无挂载、无 swap、
旧 RAID UUID 和数据库进程退出后，停止 md0，清除六盘 RAID superblock，整盘 discard。
六盘 `dlfeat` 均报告 discard 后读回零；随后对 Keylane 前 8 MiB 和每个 8 MiB 块的
8 KiB 块头显式清零并回读：每盘 228,863 个数据块头全部为零。
这是 benchmark 介质重置，不是经认证的安全擦除。

证据：[原设备清单](storage-before-lsblk.json)、[原 RAID](storage-before-raid.txt)、
[设备核验](media-reset.json)、[清理日志](media-reset.log)、[驱动状态](server-pci-after.txt)。

仅将以下六个 BDF 绑定到 `vfio-pci`：
`d95e:00:00.0`、`489e:00:00.0`、`9e72:00:00.0`、
`b78e:00:00.0`、`f70f:00:00.0`、`66d9:00:00.0`。
系统/工作盘控制器 `c05b:00:00.0` 明确 denylist。保留已配置的 8 GiB hugepages 和
`enable_unsafe_noiommu_mode=Y`；此模式无 IOMMU DMA 隔离，仅用于专用、受信任的测试机。

灌数初期发现旧 12-worker YCSB 的 IRQ 隔离布局仍把网络压在 CPU 12–15，
这些 CPU 的 softirq 占用约 70%–76%。在正式实验前，于
`2026-09-14T17:09:20Z` 恢复本次 16-worker 对应的均摊 IRQ 布局。
原始值和应用后的值见 [irq-before.json](irq-before.json)、[irq-apply.json](irq-apply.json)；
[irq_layout.py](irq_layout.py) 支持精确恢复原布局。RPS/XPS、housekeeping cpuset 未改变。
这次灌数的变速不能用于归因 flush 间隔；正式 18 轮固定相同 IRQ 布局。

## 启动与复现

下列命令须在已核验、已重置、已绑定的专用测试盘上执行。
不要把本目录的破坏性介质准备脚本用于其它主机或要保留的数据库。

```sh
# 通过 systemd benchmark slice 启动，不能从受限 user.slice 仅套 taskset。
# run_bench.py 保存实际每轮的完整 systemd 命令、cpuset、线程 affinity 和二进制 SHA。
keylane --bind 172.16.0.4 --port 6379 --metrics-port 19100 \
  --threads 16 --pin-workers --shutdown-checkpoint \
  --spdk-max-completions-per-poll 16 --flush-max-ms 1000 \
  --data-file spdk://d95e:00:00.0/1 \
  --data-file spdk://489e:00:00.0/1 \
  --data-file spdk://9e72:00:00.0/1 \
  --data-file spdk://b78e:00:00.0/1 \
  --data-file spdk://f70f:00:00.0/1 \
  --data-file spdk://66d9:00:00.0/1 --log-dir ROUND_LOG_DIR

# 在 .5；ratio 分别为 0:1、1:0、1:1，100ms 组只改服务端 flush 参数。
taskset -c 0-15 dfly_bench-x86_64 \
  --h=172.16.0.4 --p=6379 --proactor_threads=16 --c=40 \
  --test_time=300 --ratio=0:1 --pipeline=1 --qps=0 \
  --key_prefix= --key_minimum=0 --key_maximum=1000000000 \
  --key_dist=U --d=1024 --tcp_nodelay=true --seed=43 \
  --vmodule=dfly_bench=1 --logtostderr=true \
  --json_out_file=ROUND.json
```

自动步骤见 [run_bench.py](run_bench.py)：`load` → `smoke` → `matrix`。
load 使用 `key_dist=S`、每连接 1,562,500 次 SET，精确 10 亿次；之后检查
DBSIZE=10 亿、首/中/末等 6 个 key 的 STRLEN=1024、defrag 开启、读命中与错误数。
启动脚本首轮在发送 load 前因 root 进程 `/proc/PID/exe` 读取权限中断；
仅修正为 sudo readlink 后重新启动，未丢弃任何性能样本，原日志 [load-runner.log](load-runner.log) 保留。
短校验还修正了 stderr/stdout 直方图重复统计，以及把混合 ratio 误当成严格交替的
计数断言；这些是校验脚本问题，不是服务端请求失败。对应短测日志全部保留，
正式 300 秒矩阵在三种短校验通过后才启动。

灌数于 `2026-09-14T17:06:43Z` 开始，客户端用时 1,238.268 秒完成
1,000,000,000 次成功 SET；`17:27:56Z` 发布 generation 2 checkpoint，
包含 1,000,000,000 个索引条目，随后正常退出。
见 [灌数校验](load-1b.validation.json)、[灌数后快照](load-1b.after.snapshot.json)、
[checkpoint 日志](load-1b-r2.server.log)。这段灌数中调整过 IRQ，不参与 flush 性能比较。

脚本是本机实验记录，重跑需使用固定的源码 commit，并把 `RUNTIME`、客户端路径、
远程输出目录和报告输出位置改为本次新建的位置；不要覆盖已有原始证据。
`run_bench.py` 不清盘，遇到已有输出、错误响应或计数不一致会停止。
介质准备脚本另行保存，仅适用于它明确核验的六块测试盘；所需块头工具的构建方式是：

```sh
clang++-18 -O2 -pthread -std=c++20 clear_test_headers.cpp -o NEW_RUNTIME_DIR/clear_test_headers
```

完整矩阵结束后可运行以下只读复核；它从客户端原始 JSON/log 重新计算，
核验全程 640 连接、请求数、精确命中、零错误、DBSIZE、运行时配置、IRQ、正常关闭，
并核对 README 中全部逐轮结果。服务端 I/O/GC 计数只用于判断运行状态，
不是性能表的延迟来源。

```sh
python3 perf_reports/dfly-flush-spdk-2026-09-14/summarize.py --check-readme
```

## 完成状态

正式阶段首轮开始于 `2026-09-14T17:34:03Z`，末轮结束于 `19:23:24Z`；
18 轮客户端实际测量时长合计 5,400.155 秒，预热、恢复和正常关闭不计入 QPS。
见 [预先固定的顺序](matrix-plan.json)、[执行日志](matrix-runner.log)、
[完整完成标记](matrix-complete.json)。

末轮在 `19:23:40Z` 发布 generation 21 checkpoint，包含 **1,000,000,000** 个条目，
随后服务与矩阵 runner 均正常退出（exit 0）；
见 [最终停机日志](mixed-p3-100ms.server.log)、[退出状态](mixed-p3-100ms.stop.json)。
测试数据和 checkpoint 保留在六块 SPDK 盘上，没有再次清盘；六盘仍绑定 `vfio-pci`，
IRQ 仍保持本次 16-worker 布局。原 RAID / Aerospike 数据未重建。

`.5` 的 Prometheus 和 Grafana 仍运行。Keylane 压测实例已按计划停止，
所以此时 Keylane metrics target 会显示 down；启动上述 SPDK 实例后可恢复采集。
本次没有改数据库默认参数，也没有启动下一轮实验。
