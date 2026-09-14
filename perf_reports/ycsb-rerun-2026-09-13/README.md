# YCSB：Keylane f666837（100ms 刷盘）与 Aerospike，A/B/C/D × 256 线程

**Keylane 8/8 项已按 `--flush-max-ms 100` 重新测完，每项 5 分钟、零错误。** 新测窗口为 **2026-09-14 16:01:47–16:42:38 UTC**。主表全部 Keylane 数字来自本次完整重测，不混入此前短测或三组交替实验。

**Aerospike 本次未重跑**，保留 **2026-09-14 11:12:51–11:54:13 UTC** 的 8 项实测对照。两列各项均为 5 分钟，但来自不同时间窗口；两库 CPU 配额和物理数据老化状态也不同，不能把本表当成同轮交叉实验或单独归因于 100ms 的优化收益。

在下述部署配置中，Keylane 不限速 B/C/D 的吞吐分别高 **28.92% / 35.91% / 31.81%**；A 则低 **2.51%**。不限速各操作的 p99/p999/p9999 均低于 Aerospike 对照。**100K 下 A 的读写长尾仍高于 Aerospike**；B/C/D 的百分位整体接近，不将小差异解读为确定优劣。

## 完整结果

全部为 256 个 YCSB 工作线程。A = 50% READ / 50% UPDATE；B = 95% READ / 5% UPDATE；C = 纯读；D = 95% READ / 5% INSERT。

每格：**该操作 QPS；p99 / p999 / p9999（ms）**。总 QPS 不合并读写百分位；各项独立四舍五入，显示值之和可能相差 1。

| Workload | 限速 | 操作 | Aerospike CE（此前实测） | Keylane HREPLACE（100ms 新测） |
|---|---|---|---:|---:|
| A | 不限速 | 总 QPS | 485,671 | 473,477 |
| A | 不限速 | READ | 242,840；3.223 / 13.247 / 29.311 | 236,730；2.953 / 4.967 / 6.759 |
| A | 不限速 | UPDATE | 242,831；2.633 / 12.191 / 27.727 | 236,747；1.976 / 3.215 / 4.403 |
| A | 100K | 总 QPS | 99,894 | 99,965 |
| A | 100K | READ | 49,956；0.436 / 0.572 / 1.357 | 49,984；0.577 / 1.116 / 2.365 |
| A | 100K | UPDATE | 49,938；0.353 / 0.473 / 1.239 | 49,980；0.437 / 1.053 / 3.489 |
| B | 不限速 | 总 QPS | 415,142 | 535,185 |
| B | 不限速 | READ | 394,378；5.575 / 14.423 / 26.639 | 508,440；0.861 / 2.851 / 4.443 |
| B | 不限速 | UPDATE | 20,764；4.963 / 13.391 / 25.167 | 26,745；0.681 / 1.934 / 3.677 |
| B | 100K | 总 QPS | 99,846 | 99,975 |
| B | 100K | READ | 94,855；0.461 / 0.608 / 1.335 | 94,976；0.446 / 0.626 / 1.250 |
| B | 100K | UPDATE | 4,991；0.388 / 0.526 / 1.219 | 4,999；0.392 / 0.531 / 1.243 |
| C | 不限速 | READ | 404,598；5.019 / 10.911 / 18.911 | 549,885；0.738 / 2.587 / 4.023 |
| C | 100K | READ | 99,931；0.471 / 0.613 / 1.304 | 99,974；0.487 / 0.610 / 1.245 |
| D | 不限速 | 总 QPS | 412,465 | 543,677 |
| D | 不限速 | READ | 391,824；5.551 / 14.495 / 27.055 | 516,508；0.750 / 2.925 / 4.595 |
| D | 不限速 | INSERT | 20,641；4.943 / 13.551 / 26.127 | 27,169；0.616 / 2.010 / 3.925 |
| D | 100K | 总 QPS | 99,919 | 99,974 |
| D | 100K | READ | 94,918；0.453 / 0.593 / 1.357 | 94,976；0.454 / 0.636 / 1.241 |
| D | 100K | INSERT | 5,001；0.382 / 0.507 / 1.226 | 4,998；0.393 / 0.557 / 1.280 |

主表中的 Keylane 配置在 A/B 上使用 `HREPLACE`；C 没有写命令，D 的 INSERT 仍通过 binding 的 `HMSET` 创建新 Hash，不使用 HREPLACE。Aerospike UPDATE 使用 `REPLACE_ONLY`，INSERT 使用 `CREATE_ONLY`。双方读取、更新都包含全部 10 个字段，Redis scan index 关闭。

重点观察：

- 不限速 A：Keylane 473,477 vs Aerospike 485,671 QPS；Keylane READ / UPDATE p9999 为 6.759 / 4.403 ms，低于 Aerospike 的 29.311 / 27.727 ms。吞吐仍低约 2.51%，不能声称 A 已超过 Aerospike。
- 100K A：Keylane READ / UPDATE p999 为 1.116 / 1.053 ms，Aerospike 为 0.572 / 0.473 ms；Keylane p9999 为 2.365 / 3.489 ms，也高于对照的 1.357 / 1.239 ms。吞吐接近目标速率不代表长尾问题已经解决。
- 不限速 B/C/D：本轮 Keylane 的吞吐更高，表内各操作的 p99/p999/p9999 也更低。
- 100K B：Keylane READ / UPDATE p9999 为 1.250 / 1.243 ms，Aerospike 为 1.335 / 1.219 ms；p999 则为 0.626 / 0.531 vs 0.608 / 0.526 ms。B/C/D 的小差异不作确定的优化收益解释；各操作、各百分位仍应分别阅读。

## 指标口径与完整性

- 每项先运行独立 JVM 的 1M 次预热，再启动一个新的正式 JVM，设置 `maxexecutiontime=300`。预热不计入成绩，正式 JVM 仍包含自身启动/JIT 过程。
- 本次 Keylane 8 个正式窗口的 YCSB RunTime 为 **300,035–300,073 ms**，均有 YCSB 自身的计时停止标记；2B operationcount 只是非约束性上限，不是实际运行到 20 亿次。
- 本次正式成功操作数 **750,773,355**，另有 **8,000,000** 次预热，所有阶段零错误。已核对 8 项、16 个阶段的日志、退出码、参数、客户端 JAR/服务端二进制 SHA、成功计数、进程身份及记录增量。服务端 HGETALL/HREPLACE/HMSET 命令计数与 YCSB READ/UPDATE/INSERT 成功数逐阶段一致；GC 错误与资源耗尽计数增量均为零。
- Aerospike 的 8 项保留原测试凭据，原批次的日志和成功计数重新通过校验；这些不是本次新增的操作数。
- 延迟来自 **YCSB 普通 operation HDR histogram**，不是 Keylane metrics、Aerospike 服务端直方图，也不是模拟或服务端分段耗时。p999 = p99.9；p9999 = p99.99。
- 每操作 QPS = 该操作 `Return=OK × 1000 / RunTime(ms)`。总吞吐为成功操作数之和除以同一窗口耗时。主表是每次完整运行的百分位，不是十秒分段平均数，也不是多轮平均数。
- `measurement.interval=both` 另外保存的 Intended 直方图仅在原始 JSON/日志中独立保留，不与 operation 百分位混用。CSV 另存平均值、p50、p95、最大值和成功计数；极少数高尖峰不会因为未进入 p9999 就被删除。
- 两列合计 28 个操作结果的 QPS 与 p99/p999/p9999 均从原始日志复算；新 Keylane 日志还通过另一套 CSV 解析交叉核对。校验脚本检查主表全部数字与这些原始结果一致。

[Keylane 新测结果](verified-update-matrix/flush100-abcd-20260914-results.json) · [预热/正式全部阶段](verified-update-matrix/flush100-abcd-20260914-phases.json) · [两库校验后汇总](verified-update-matrix/flush100-abcd-20260914-summary.json) · [操作明细 CSV](verified-update-matrix/flush100-abcd-20260914-summary.csv) · [校验脚本](verified-update-matrix/flush100_summary.py) · [Aerospike 原始批次](verified-update-matrix/main-f666837-abcd-20260914-results.json)

## 数据与 D 的语义

沿用原有 **100M key 读取域、10 fields × 128 字节**，`requestdistribution=uniform`，本轮不清盘、不重新 load，也不改 RAID/分区。

| Workload | 读 | 更新 | 插入 | 读分布 |
|---|---:|---:|---:|---|
| A | 50% | 50% | 0 | 原始 100M 域 Uniform |
| B | 95% | 5% | 0 | 原始 100M 域 Uniform |
| C | 100% | 0 | 0 | 原始 100M 域 Uniform |
| D-Uniform | 95% | 0 | 5% | 原始 100M 域 Uniform，不读取本轮新增记录 |

**本轮 D 保留 D 的操作比例，但不是标准的 latest-D。** 按用户要求使用 Uniform；通过 `insertstart=0, insertcount=100000000` 固定读范围，`recordcount` 则作为 CoreWorkload 的事务插入计数器起点。D 中这个参数不是数据库物理记录数。

| 数据来源 | D 阶段 | 100K 插入 ID 起点 | 不限速插入 ID 起点 |
|---|---|---:|---:|
| Keylane 本次 100ms | 预热 | 100,000,000,000 | 104,000,000,000 |
| Keylane 本次 100ms | 正式 | 102,000,000,000 | 106,000,000,000 |
| Aerospike 此前实测 | 预热 | 10,000,000,000 | 14,000,000,000 |
| Aerospike 此前实测 | 正式 | 12,000,000,000 | 16,000,000,000 |

每段预留 2B 个 ID，避免预热/正式/不同限速重叠。Keylane 本次使用全新区间，不复用此前 D 已写入的 ID；两列的插入起点因此不同，但读取域、字段大小与操作比例相同。每阶段数据库记录增量与成功 INSERT 数一致，未把覆盖旧 key 伪装成插入。

Keylane 的 100K 预热/正式分别新增 **49,727 / 1,499,637** 条，不限速预热/正式分别新增 **49,790 / 8,152,211** 条，合计 **9,751,365** 条。

| 数据来源 | 对应批次开始记录数 | 对应批次结束记录数 |
|---|---:|---:|
| Keylane DB0，本次 100ms | 109,732,242 | 119,483,607 |
| Aerospike ycsb，此前实测 | 100,304,416 | 108,097,912 |

两库读取范围相同，但都保留历史记录，物理内容、老化和 GC 状态不完全一致。Keylane 在此前报告之后还经历过诊断与交替压测，本次没有清盘或重置物理状态；D 又使数据集继续增长。后续不能直接复用表中任何已运行的 D 区间。

## 机器、存储和资源放置

| 项目 | 配置 |
|---|---|
| 服务端 .4 | 172.16.0.4，AMD EPYC 9V74，16 逻辑 CPU / 8 物理核（SMT），约 125 GiB RAM |
| 客户端 .5 | 172.16.0.5，AMD EPYC 9V45，16 逻辑 / 16 物理核，约 32 GiB RAM，Java 17 |
| 存储 | 六块 NVMe，/dev/md0 RAID0，512 KiB chunk |
| Keylane | io_uring，/dev/md0p1（约 9.5 TiB），12 workers，CPU 0–11 |
| Aerospike | CE 8.1.2.4，/dev/md0p2（1 TiB），CPU 0–15 可用 |
| 系统与网卡 | housekeeping CPU 12–15，mlx5 IRQ 分布到 12–15，irqbalance 关闭；RPS/XPS 不变 |
| 监控 | .5 上 Prometheus / Grafana 保持运行；本轮没有启动 perf/BPF 或编译任务 |

CPU 0–11 是 **6 个完整物理核**，不是 12 个物理核。Keylane 位于独立 `keylane-bench.slice`，AllowedCPUs/CPUAffinity=0–11；`system.slice`、`user.slice`、`init.scope` 及 unbound workqueue 使用 CPU 12–15。固定 per-CPU 内核线程、managed NVMe IRQ 不保证全部迁移。

**Aerospike 同属独立 benchmark slice，但可用 CPU 0–15，并与 housekeeping/IRQ 共享后四 CPU。两库不是相同 CPU 配额或隔离条件。** 原对照批次中两库串行运行，Aerospike 的冷恢复在预热之前完成、不计入测量；本次仅运行 Keylane，Aerospike 保持停止。

256 表示 YCSB 工作线程数。Redis binding 每线程一个 Jedis 连接；Aerospike SDK 自行管理连接池，不能把同样线程数解释为相同 TCP 连接总数。

本次二进制/JAR 校验值、线程/IRQ 快照、监控状态与原服务参数见[100ms 环境凭据](verified-update-matrix/flush100-abcd-20260914-environment.json)。完整机型与 Aerospike 配置仍见[机器清单](verified-update-matrix/machine-inventory.json)及[Aerospike 原批次环境](verified-update-matrix/main-f666837-abcd-20260914-environment.json)。本次保持既有 CPU/IRQ 策略，没有重新配置主机。

## 源码、构建与启动

沿用此前发布的 [f666837](https://github.com/thweetkomputer/keylane/commit/f666837b0038ab65564a17cb3a0bca8530f8e1be) 二进制：GC 使用同步 predicate 找到物理身份匹配的候选，不再为此构造临时 vector；普通 `Find` 及异步读的候选快照路径不变。该版本包含此前的 Hash 读复制/结果构造优化及 `b0606c5` 的主动过期配置更新。**本次没有重新编译或修改生产代码，只将测试启动参数改为 `--flush-max-ms 100`**，不能用于单独估算 GC 优化的因果收益。

该二进制的原构建 checkout 是 `1b6c0d3`，其整个非报告源码树与 `f666837` 完全一致；构建使用 GCC 13.3、Release、O3/native/LTO、静态 C++/OpenSSL，SPDK 关闭、调试故障与读写 tracing 关闭。本次各阶段重新核对了二进制 SHA 和进程身份。

```text
Keylane commit: f666837b0038ab65564a17cb3a0bca8530f8e1be
Binary: /mnt/dev/keylane-main-f666837.siARSe/keylane
SHA256: 9162d45032c34a9ddffffd1ef8137495237256173092aa931b65890dc12b7266
```

原构建后通过 56 项索引/存储格式单测、4 项 Hash/String/extent GC 端到端验证，以及 TTL 端到端测试；这些验证不是本次重新运行的测试。[原构建凭据](verified-update-matrix/main-f666837-build.json)、[单测](verified-update-matrix/main-f666837-unit-tests.xml)、[Hash/GC](verified-update-matrix/main-f666837-hash-gc-tests.xml)、[TTL](verified-update-matrix/main-f666837-ttl-tests.xml)。

Keylane 等效命令：

```bash
taskset -c 0-11 /mnt/dev/keylane-main-f666837.siARSe/keylane \
  --bind 172.16.0.4 --port 16379 --metrics-port 19100 \
  --threads 12 --pin-workers --shutdown-checkpoint \
  --log-dir /mnt/dev/keylane/perf_reports/ycsb-rerun-2026-09-13/verified-update-matrix/flush100-abcd-20260914-server-logs \
  --data-file /dev/md0p1 --flush-max-ms 100
```

本次计时期间使用 `keylane-flush100-report.service`，独立 slice、AllowedCPUs/CPUAffinity=0–11、LimitNOFILE=20000；不能直接从被限制在 CPU 12–15 的 user.slice 启动并声称相同放置。[实际启动命令](verified-update-matrix/flush100-abcd-20260914-server-command.json)记录了完整 systemd 参数。

`flush-max-ms=100` 是周期刷盘触发间隔，不是每条写命令同步持久化的保证。**128 KiB 写入分块和“数据 fdatasync → 提交块头 fdatasync”的持久化顺序没有改变**。GC/defrag 全程开启：`paused=0 max_active_per_device=8 block_sleep_ms=0 record_sleep_us=0`。主动过期保持默认：interval=10 ms、map steps=256、deletes=64、index-maintenance steps=256，没有调低回收并发或主动过期预算。

Aerospike 原测量通过 `aerospike-ycsb-compare.service` 启动，本次未启动：

```bash
/usr/bin/asd --config-file /mnt/dev/aerospike-md0.conf --foreground
```

namespace 为 `ycsb`，RF=1、TTL=0、indexes-memory-budget=64G、flush-size=128K、max-write-cache=8G、stop-writes-sys-memory-pct=90；设备为 `/dev/md0p2`。完整配置保存在 Aerospike 原批次环境 JSON 中。

## 客户端参数与复核

两列实测均从 .5 发起请求。Keylane 使用 `/mnt/dev/YCSB-hreplace-dist`（Jedis 3.9.0）；Aerospike 原测量使用 `/mnt/dev/YCSB-aerospike-dist`（Aerospike Java 3.1.2）。本次重新核对了 Keylane 客户端 core、Redis binding、Jedis 三个 JAR 的 SHA，与[客户端清单](verified-update-matrix/client-artifacts.json)一致；该清单还记录了原先两份 core 的 75 个 class 字节一致的校验结果。没有修改或推送 YCSB 的 `redis-scanindex-none` PR 分支。

共同参数：

```properties
recordcount=100000000
fieldcount=10
fieldlength=128
fieldlengthdistribution=constant
readallfields=true
writeallfields=true
insertorder=hashed
requestdistribution=uniform
scanproportion=0
readmodifywriteproportion=0
measurementtype=hdrhistogram
measurement.interval=both
hdrhistogram.percentiles=50,95,99,99.9,99.99
operationcount=2000000000
maxexecutiontime=300
```

以上 `recordcount` 适用于 A/B/C；D 的覆盖参数见前表。操作比例按 workload 设置。使用 `-threads 256`；100K 档另加 `-target 100000`，该目标是所有线程、所有操作合计，不是每线程或每类操作各 100K。

Keylane binding 额外设置：

```properties
redis.host=172.16.0.4
redis.port=16379
redis.scanindex=none
redis.updatecommand=keylane.hreplace
redis.timeout=10000
redis.cluster=false
```

Aerospike 对应 `as.host=172.16.0.4, as.port=3000, as.namespace=ycsb, as.timeout=10000`。本次 Keylane 完整命令在同目录的 `flush100-abcd-20260914-*.command.json` 中，退出码、原始日志 SHA 和指标在对应 JSON 中；Aerospike 对照仍由 `main-f666837-abcd-20260914-aerospike-*.command.json` 标识。

[100ms 串行测试脚本](verified-update-matrix/flush100_abcd.py)保留实际执行顺序：C/A/B/D，每个 workload 先 100K、后不限速，每项独立 1M 次预热、再正式测量 300 秒。整个 Keylane 矩阵使用同一进程，没有在各项间清盘或重启。脚本使用固定批次名称并拒绝覆盖已有证据；再次压测必须先分配新的批次名与 D 区间，不可直接重复执行。原 Aerospike 的执行顺序见[原批次脚本](verified-update-matrix/main_abcd.py)。仅重算结果、检查 README 表格，不访问数据库：

```bash
python3 perf_reports/ycsb-rerun-2026-09-13/verified-update-matrix/flush100_summary.py --write --check-readme
```

测试结束后，Aerospike 保持停止；Keylane 恢复到原来的 `keylane-ycsb-cpu12.service` 启动参数，即**没有 `--flush-max-ms` 覆盖项的默认 1000ms**。上表成绩全部是在 100ms 下取得，不是恢复后的服务配置。DB0 为 **119,483,607 keys**、GC 开启，客户端没有残留 YCSB Java 进程；Prometheus/Grafana 保持运行。[完成凭据](verified-update-matrix/flush100-abcd-20260914-complete.json)、[最终服务状态](verified-update-matrix/flush100-abcd-20260914-final-server.json)、[恢复命令](verified-update-matrix/flush100-abcd-20260914-restore-command.json)。

生产二进制版本仍为 `f666837`；本次提交只包含报告、原始日志、参数和校验脚本，不包含生产代码或默认参数变更。
