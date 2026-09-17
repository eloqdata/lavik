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

# Keylane 与 Aerospike：YCSB 的 QPS 和长尾延迟

[English](README.md) | **简体中文** | [报告目录](../README.md)

> 本报告记录项目更名为 Lavik 之前的 Keylane 测试。产品名、版本、命令和数据均对应当时的实验，
> 不代表当前 Lavik 版本的实测结果。

测试日期：2026-09-16。两库重新各灌 **1 亿条记录，每条 10 × 128 B**，使用一台客户端、256 个 YCSB 工作线程，完整测量 A/B/C/D × 100K/不限速共 16 项，每项 300 秒。预热与正式阶段均零错误。

**Aerospike 使用默认性能参数与主机原有调度、IRQ 配置；Keylane 使用 12 个 worker，并单独启用 12+4 CPU/IRQ 隔离。** 这些是按要求选择的两种部署方式，CPU 配额及数据库参数不同。每阶段前后均记录并核对实际配置。

## 机型、网络和存储

| 项目 | 配置 |
|---|---|
| 服务端 | Azure `Standard_L16aos_v4`，AMD EPYC 9V74，16 逻辑 CPU / 8 物理核（SMT），约 125 GiB 内存 |
| 客户端 | Azure `Standard_F16als_v7`，AMD EPYC 9V45，16 逻辑 CPU / 16 物理核，约 32 GiB 内存；Java 17 |
| 操作系统 | Ubuntu 24.04，Linux `6.17.0-1022-azure`，x86_64 |
| 网络 | 一台客户端 `172.16.0.5` → 服务端 `172.16.0.4`；两库均使用内核网络及同一 mlx5 网卡 |
| 物理存储 | 六块 Microsoft NVMe Direct Disk v2，Linux RAID0 `/dev/md127`，chunk=512 KiB，总容量约 10.48 TiB |
| Aerospike 分区 | `/dev/md127p3`，1,099,494,850,560 B，约 1 TiB，裸块设备 |
| Keylane 分区 | `/dev/md127p4`，1,099,494,850,560 B，约 1 TiB，裸块设备 |
| 分区初始化 | 两个新分区均按 24 MiB 对齐并全段清零，无文件系统；现有 p1/p2 GET 数据保留 |
| 后台监控 | 客户端保留 Prometheus / Grafana；正式测量时没有同时运行另一数据库压测 |

[机器清单](evidence/inventory.json)包含机型元数据、CPU 拓扑、内核、路由及客户端 JAR SHA；[分区记录](evidence/storage-after.json)和[清零凭据](evidence/zeroing.json)保存设备身份与边界。两库串行使用同一 RAID0，没有同时争用其磁盘。

## 数据量与工作负载

| 项目 | Aerospike | Keylane |
|---|---:|---:|
| 正式矩阵开始记录数 | 100,000,000 | 100,000,000 |
| 每条字段值大小 | 10 × 128 B = 1280 B | 10 × 128 B = 1280 B |
| 起始字段净数据 | 128 GB（约 119.21 GiB） | 128 GB（约 119.21 GiB） |
| 随机读取范围 | 原始 100,000,000 个 key | 原始 100,000,000 个 key |
| 整个矩阵结束记录数 | 108,328,377 | 109,699,918 |

净数据只统计字段值，未计 key、字段名、索引及记录格式开销。两库使用相同的 hashed key 命名与固定字段大小，均从空库装载；成功 INSERT、服务端记录增量均核对为 1 亿。装载后分别干净停止并恢复，确认记录数以及 8 个分散 key 的 10 个字段长度和 SHA256 一致，再开始预热及正式测量。

| Workload | READ | UPDATE | INSERT | 读分布 |
|---|---:|---:|---:|---|
| A | 50% | 50% | 0 | Uniform |
| B | 95% | 5% | 0 | Uniform |
| C | 100% | 0 | 0 | Uniform |
| D-Uniform | 95% | 0 | 5% | 固定原始 1 亿 key 的 Uniform |

D 不读取本轮新增记录，因此属于 D-Uniform。两库起始规模相同；D 的实际成功插入数随吞吐而不同，所以矩阵结束条数不同。两库使用相同且互不重叠的阶段插入 ID 起点：100K 预热/正式为 20 亿/40 亿，不限速预热/正式为 60 亿/80 亿。D 中 `recordcount` 是新插入 ID 起点，`insertstart=0, insertcount=100000000` 固定读取域。

## 宿主配置：Aerospike 保持原样，Keylane 单独隔离

| 项目 | Aerospike | Keylane |
|---|---|---|
| 进程可用 CPU | 0–15，16 逻辑 CPU / 8 物理核；无额外 CPU 亲和性参数 | 0–11，12 逻辑 CPU / 6 物理核；12 个固定 worker |
| systemd 放置 | 默认 `system.slice`，不设置 AllowedCPUs/CPUAffinity | 独立 `keylane-bench.slice`，AllowedCPUs/CPUAffinity=0–11 |
| 系统任务 | `system.slice`、`user.slice`、`init.scope` 无额外 CPU 限制 | 上述任务限制到 CPU 12–15 |
| unbound workqueue | 原始 `ffff`，CPU 0–15 | `f000`，CPU 12–15 |
| 测试网卡 IRQ | 驱动原始分布：16 个 completion IRQ 对应 CPU 0–15 | 该网卡的 17 个 IRQ 轮转到 CPU 12–15 |
| irqbalance | 主机原本未安装，保持未运行 | 同样保持未运行 |
| RPS/XPS、另一张网卡 | 原始设置 | 保留原始设置 |

Aerospike 的所有线程亲和性、`auto-pin=none` 和原始 IRQ 配置在各阶段前后均通过检查。Keylane 调优只在 Aerospike 全部测完并停止后应用，结束后恢复原始状态。Keylane 各预热及正式阶段前后都检查配置值与实际 IRQ 落点，并通过中断计数差值确认该网卡 IRQ 未在 CPU 0–11 上运行。固定 per-CPU 内核线程与 managed NVMe IRQ 不属于可任意迁移的系统任务。

[原始宿主快照](evidence/host-original.json) · [Keylane 调优凭据](evidence/keylane-policy-applied.json) · [宿主恢复凭据](evidence/host-restored.json) · [新建辅助线程的亲和性恢复](evidence/inherited-affinity-restored.json)

## 两数据库参数

| 参数 | Aerospike | Keylane |
|---|---|---|
| 版本 | Community Edition 8.1.2.4-4 | `f666837b0038ab65564a17cb3a0bca8530f8e1be` |
| 网络与存储 | 内核网络，`storage-engine device`，裸 RAID0 分区 | 内核网络 + io_uring，裸 RAID0 分区；SPDK 关闭 |
| 并发 | 默认实际生效 `service-threads=80`、`auto-pin=none` | `--threads 12 --pin-workers` |
| 刷盘 | 默认 `flush-size=1 MiB`、`flush-max-ms=1000` | 写入分块 128 KiB，`--flush-max-ms 100` |
| 写缓存与读缓存 | 默认 `max-write-cache=64 MiB`、`post-write-cache=256 MiB`、`read-page-cache=false` | 沿用该版本默认存储参数 |
| 索引预算 | 默认 `indexes-memory-budget=0`，无单独预算上限 | 沿用该版本默认索引参数 |
| 数据与复制 | namespace `ycsb`，单节点 RF=1；TTL 默认 0 | 单实例 DB0，未配置副本 |
| GC/回收 | 使用服务器默认参数 | GC/defrag 开启，`max_active_per_device=8`，无额外 sleep |
| READ | 全部 10 个字段 | `HGETALL`，全部 10 个字段 |
| UPDATE | `REPLACE_ONLY`，全部 10 个字段 | `HREPLACE`，全部 10 个字段 |
| INSERT | `CREATE_ONLY` | `HMSET` 创建新 Hash，`redis.scanindex=none` |
| 客户端 | YCSB Aerospike binding，Java client 3.1.2 | YCSB Redis binding，Jedis 3.9.0 |

Aerospike 配置只指定必需的 cluster-name、监听/单节点通信、namespace、RF=1 与块设备，未覆盖线程数、auto-pin、缓存、刷盘、索引预算或回收参数。文件描述符上限使用安装包 systemd unit 的 100,000。完整[配置文件](evidence/aerospike.conf)、[实际启动命令](evidence/aerospike-measured-server-command.json)及[生效配置](evidence/aerospike-measured-ready.json)均保留；默认值以本次运行的查询结果为准。

Keylane 沿用原报告相同二进制，GCC 13.3 / Release / O3 / native / LTO，SHA256 `9162d45032c34a9ddffffd1ef8137495237256173092aa931b65890dc12b7266`。[构建凭据](evidence/keylane-build.json)和[启动命令](evidence/hreplace-measured-server-command.json)记录完整参数。100ms 为周期刷盘触发间隔，普通写成功不承诺逐条同步持久化；数据 fdatasync 后再提交块头 fdatasync。主动过期使用该版本默认预算。

## 压测与指标口径

- 两库各使用 256 个 YCSB 工作线程。Redis binding 每线程一个 Jedis 连接，Aerospike SDK 自行管理连接池；线程数相同不表示 TCP 连接总数相同。
- `fieldcount=10`、`fieldlength=128`、`fieldlengthdistribution=constant`、`readallfields=true`、`writeallfields=true`、`requestdistribution=uniform`、`insertorder=hashed`；关闭 SCAN 与 read-modify-write。客户端超时均为 10000 ms。
- 先测 Aerospike，后测 Keylane。每库顺序为 C/A/B/D，每种负载先 100K、再不限速；每项先由独立 JVM 预热 100 万次，再启动新的正式 JVM 测量 300 秒。未从正式窗口剔除开始阶段的 JIT/爬坡。各项间不清库、不重启。
- 100K 是全部线程、全部操作合计的 `-target 100000`。正式 `operationcount=2000000000` 为宽松上限，实际停止由 `maxexecutiontime=300` 控制。
- QPS = 正式窗口成功操作数 / YCSB 实际 RunTime。总 QPS 合计读写成功数，延迟按操作分别统计。
- 长尾来自客户端普通 operation HDR histogram，p999=p99.9，p9999=p99.99；使用完整 5 分钟窗口，不平均分段百分位。Intended 直方图单独保留于原始日志。
- 每项为一次 5 分钟测量，结果对应本次数据状态、执行顺序与部署参数，未估计跨多轮重复试验的波动范围。

| 数据库 | 正式窗口（UTC） | 项数 |
|---|---|---:|
| Aerospike | 2026-09-16T03:05:56Z – 2026-09-16T03:47:32Z | 8 |
| Keylane | 2026-09-16T03:54:38Z – 2026-09-16T04:35:31Z | 8 |

## QPS 与长尾延迟

每格为 **该操作 QPS；p99 / p999 / p9999（ms）**。各项独立四舍五入，显示的操作 QPS 之和可能与总 QPS 相差 1。

| Workload | 限速 | 操作 | Aerospike（默认参数、原始宿主配置） | Keylane（100ms、12+4 隔离） |
|---|---|---|---:|---:|
| A | 不限速 | 总 QPS | 489,819 | 511,169 |
| A | 不限速 | READ | 244,911；1.331 / 3.427 / 5.599 | 255,555；2.475 / 4.603 / 6.207 |
| A | 不限速 | UPDATE | 244,908；1.121 / 2.089 / 3.407 | 255,615；1.666 / 3.185 / 4.811 |
| A | 100K | 总 QPS | 99,905 | 99,963 |
| A | 100K | READ | 49,959；0.439 / 0.726 / 1.516 | 49,989；0.467 / 0.797 / 1.268 |
| A | 100K | UPDATE | 49,946；0.361 / 0.551 / 1.366 | 49,975；0.378 / 0.668 / 1.502 |
| B | 不限速 | 总 QPS | 445,515 | 540,102 |
| B | 不限速 | READ | 423,238；1.285 / 2.793 / 4.323 | 513,100；0.818 / 2.937 / 4.643 |
| B | 不限速 | UPDATE | 22,277；1.093 / 1.843 / 3.321 | 27,002；0.656 / 1.979 / 3.879 |
| B | 100K | 总 QPS | 99,874 | 99,973 |
| B | 100K | READ | 94,878；0.447 / 0.595 / 1.359 | 94,973；0.465 / 0.929 / 1.317 |
| B | 100K | UPDATE | 4,996；0.373 / 0.498 / 1.288 | 4,999；0.380 / 0.718 / 1.257 |
| C | 不限速 | READ | 445,698；1.201 / 2.623 / 4.735 | 550,587；0.740 / 2.719 / 4.411 |
| C | 100K | READ | 99,882；0.448 / 0.564 / 1.370 | 99,956；0.469 / 0.638 / 1.317 |
| D | 不限速 | 总 QPS | 448,832 | 540,275 |
| D | 不限速 | READ | 426,400；1.216 / 2.623 / 4.635 | 513,278；0.772 / 3.127 / 4.903 |
| D | 不限速 | INSERT | 22,432；1.037 / 1.690 / 2.271 | 26,997；0.639 / 2.311 / 4.291 |
| D | 100K | 总 QPS | 99,911 | 99,970 |
| D | 100K | READ | 94,918；0.434 / 0.555 / 1.371 | 94,974；0.425 / 0.559 / 1.275 |
| D | 100K | INSERT | 4,993；0.373 / 0.485 / 1.269 | 4,996；0.364 / 0.501 / 1.331 |

上述部署下，Keylane 相对 Aerospike 的不限速吞吐差异为 A **+4.36%**、B **+21.23%**、C **+23.53%**、D **+20.37%**。各操作长尾见表，100K 的吞吐接近目标本身不代表长尾更低。两库 CPU 配额、宿主策略、刷盘及缓存等设置不同，这些结果不能单独归因于 IRQ 调优或某一个参数。

吞吐与长尾需要分别比较：本轮不限速各操作的 p999 数值均是 **Aerospike 较低**；B/C/D 的 p99 为 **Keylane 较低**，A 的 READ/UPDATE 三档长尾均为 Aerospike 较低。Keylane A 的[区间吞吐](evidence/fresh100m-20260916-hreplace-a-c256-measured.log)前段约 53–54 万、后段约 47 万 QPS，[窗口结束快照](evidence/fresh100m-20260916-hreplace-a-c256-measured.host-after.json)记录 8 路 GC 活跃；表中平均值与百分位包含整个过程。

## 原始证据与离线复核

[结果 JSON](results.json) · [操作明细 CSV](summary.csv)（含平均、最大延迟与成功数） · [全部阶段](evidence/phases.json) · [初始测试脚本](evidence/runner.py) · [Keylane 继续执行脚本](evidence/resume_keylane.py) · [文件校验值](SHA256SUMS)

Aerospike 测完后，第一次切换因立即检查尚待下一次中断才生效的 IRQ affinity 而停止，宿主已自动恢复；当时尚未启动 Keylane。随后修正切换校验，在装载流量后确认实际 IRQ 落点，并保留了全部已完成的 Aerospike 结果。此故障发生在设置阶段，没有正式窗口被替换。原始脚本、[继续执行凭据](evidence/resume.json)和失败记录均归档。

离线校验重新解析原始日志，核对 2 次各 1 亿条装载、32 个预热/正式阶段、16 个正式窗口、28 个操作结果、服务端命令/记录增量，以及各阶段宿主与数据库生效配置。同时校验两种语言的结果表与归档文件哈希。只需 Python 3.11 或更新版本，不会启动数据库或修改系统：

```sh
python3 perf_reports/ycsb-rerun-2026-09-13/verify_report.py
```

完成时宿主已恢复原始策略，Keylane 干净停止，新 Aerospike 默认配置实例已恢复并核对数据；两个 YCSB 分区及原有 p1/p2 GET 数据均保留。见[完成凭据](evidence/complete.json)。归档中的压测脚本属于本批次执行记录；复测需使用新的批次及空数据介质，不能复用已消耗的 D 区间。
