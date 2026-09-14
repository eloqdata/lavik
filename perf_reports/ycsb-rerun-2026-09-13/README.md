# YCSB：Keylane main f666837 与 Aerospike，A/B/C/D × 256 线程

**16/16 项全部完成，零错误，每项正式测量 5 分钟。** 数据截至 **2026-09-14 11:54:13 UTC**。本报告只展示本轮新数据，不混入旧版本、短测或 GC 候选交叉实验。

在下述部署配置中，Keylane 不限速 B/C/D 的吞吐分别高 27.75% / 35.10% / 31.41%；A 则低 3.37%。**100K 下 A/B 的 p999、p9999 仍是 Keylane 更高**，尤其 A 的混合负载长尾没有解决。每项只测一遍，这不是优化收益的交叉实验或同 CPU 配额的比较。

## 完整结果

全部为 256 个 YCSB 工作线程。A = 50% READ / 50% UPDATE；B = 95% READ / 5% UPDATE；C = 纯读；D = 95% READ / 5% INSERT。

每格：**该操作 QPS；p99 / p999 / p9999（ms）**。总 QPS 不合并读写百分位；各项独立四舍五入，显示值之和可能相差 1。

| Workload | 限速 | 操作 | Aerospike CE | Keylane HREPLACE |
|---|---|---|---:|---:|
| A | 不限速 | 总 QPS | 485,671 | 469,325 |
| A | 不限速 | READ | 242,840；3.223 / 13.247 / 29.311 | 234,642；3.335 / 5.799 / 7.291 |
| A | 不限速 | UPDATE | 242,831；2.633 / 12.191 / 27.727 | 234,683；2.273 / 4.591 / 5.999 |
| A | 100K | 总 QPS | 99,894 | 99,973 |
| A | 100K | READ | 49,956；0.436 / 0.572 / 1.357 | 49,985；0.815 / 4.515 / 7.939 |
| A | 100K | UPDATE | 49,938；0.353 / 0.473 / 1.239 | 49,988；0.458 / 2.471 / 3.999 |
| B | 不限速 | 总 QPS | 415,142 | 530,361 |
| B | 不限速 | READ | 394,378；5.575 / 14.423 / 26.639 | 503,847；0.916 / 3.189 / 5.151 |
| B | 不限速 | UPDATE | 20,764；4.963 / 13.391 / 25.167 | 26,515；0.700 / 2.125 / 3.497 |
| B | 100K | 总 QPS | 99,846 | 99,971 |
| B | 100K | READ | 94,855；0.461 / 0.608 / 1.335 | 94,969；0.442 / 0.838 / 2.397 |
| B | 100K | UPDATE | 4,991；0.388 / 0.526 / 1.219 | 5,003；0.383 / 0.607 / 3.345 |
| C | 不限速 | READ | 404,598；5.019 / 10.911 / 18.911 | 546,596；0.759 / 2.545 / 3.957 |
| C | 100K | READ | 99,931；0.471 / 0.613 / 1.304 | 99,975；0.462 / 0.573 / 1.279 |
| D | 不限速 | 总 QPS | 412,465 | 542,007 |
| D | 不限速 | READ | 391,824；5.551 / 14.495 / 27.055 | 514,903；0.766 / 2.915 / 4.515 |
| D | 不限速 | INSERT | 20,641；4.943 / 13.551 / 26.127 | 27,104；0.632 / 1.945 / 3.845 |
| D | 100K | 总 QPS | 99,919 | 99,974 |
| D | 100K | READ | 94,918；0.453 / 0.593 / 1.357 | 94,976；0.450 / 0.571 / 1.292 |
| D | 100K | INSERT | 5,001；0.382 / 0.507 / 1.226 | 4,999；0.382 / 0.497 / 1.193 |

主表中的 Keylane 配置在 A/B 上使用 `HREPLACE`；C 没有写命令，D 的 INSERT 仍通过 binding 的 `HMSET` 创建新 Hash，不使用 HREPLACE。Aerospike UPDATE 使用 `REPLACE_ONLY`，INSERT 使用 `CREATE_ONLY`。双方读取、更新都包含全部 10 个字段，Redis scan index 关闭。

重点观察：

- 不限速 A：Keylane 469,325 vs Aerospike 485,671 QPS；Keylane READ / UPDATE p9999 为 7.291 / 5.999 ms，低于 Aerospike 的 29.311 / 27.727 ms。Keylane 的 READ p99 则略高，不能说所有延迟都更低。
- 100K A：Keylane READ / UPDATE p999 为 4.515 / 2.471 ms，Aerospike 为 0.572 / 0.473 ms；p9999 也明显较高。吞吐接近目标速率不代表长尾合格。
- 不限速 B/C/D：本轮 Keylane 的吞吐更高，表内各操作的 p99/p999/p9999 也更低。
- 100K B：Keylane p99 略低，但 READ / UPDATE p9999 为 2.397 / 3.345 ms，高于 Aerospike 的 1.335 / 1.219 ms。100K C/D 的三档百分位接近，不把这类小差异当成已证明的优化收益。

## 指标口径与完整性

- 每项先运行独立 JVM 的 1M 次预热，再启动一个新的正式 JVM，设置 `maxexecutiontime=300`。预热不计入成绩，正式 JVM 仍包含自身启动/JIT 过程。
- 16 个正式窗口的 YCSB RunTime 为 **300,030–300,059 ms**，均有 YCSB 自身的计时停止标记；2B operationcount 只是非约束性上限，不是实际运行到 20 亿次。
- 正式成功操作数 **1,381,874,165**，另有 **16,000,000** 次预热；所有阶段零错误。已验证完整的 16 项、32 个阶段日志、退出码、参数、SHA、成功计数、服务端进程身份及记录增量。
- 延迟来自 **YCSB 普通 operation HDR histogram**，不是 Keylane metrics、Aerospike 服务端直方图，也不是模拟或服务端分段耗时。p999 = p99.9；p9999 = p99.99。
- 每操作 QPS = 该操作 `Return=OK × 1000 / RunTime(ms)`。总吞吐为成功操作数之和除以同一窗口耗时。主表是每次完整运行的百分位，不是十秒分段平均数，也不是多轮平均数。
- `measurement.interval=both` 另外保存的 Intended 直方图仅在原始 JSON/日志中独立保留，不与 operation 百分位混用。CSV 另存平均值、p50、p95、最大值和成功计数；极少数高尖峰不会因为未进入 p9999 就被删除。
- 28 行操作 QPS 与 p99/p999/p9999 已通过另一套 CSV 读取方式从原始日志独立复算。

[完整原始结果](verified-update-matrix/main-f666837-abcd-20260914-results.json) · [校验后汇总](verified-update-matrix/main-f666837-abcd-20260914-summary.json) · [操作明细 CSV](verified-update-matrix/main-f666837-abcd-20260914-summary.csv) · [校验脚本](verified-update-matrix/main_abcd_summary.py)

## 数据与 D 的语义

沿用原有 **100M key 读取域、10 fields × 128 字节**，`requestdistribution=uniform`，本轮不清盘、不重新 load，也不改 RAID/分区。

| Workload | 读 | 更新 | 插入 | 读分布 |
|---|---:|---:|---:|---|
| A | 50% | 50% | 0 | 原始 100M 域 Uniform |
| B | 95% | 5% | 0 | 原始 100M 域 Uniform |
| C | 100% | 0 | 0 | 原始 100M 域 Uniform |
| D-Uniform | 95% | 0 | 5% | 原始 100M 域 Uniform，不读取本轮新增记录 |

**本轮 D 保留 D 的操作比例，但不是标准的 latest-D。** 按用户要求使用 Uniform；通过 `insertstart=0, insertcount=100000000` 固定读范围，`recordcount` 则作为 CoreWorkload 的事务插入计数器起点。D 中这个参数不是数据库物理记录数。

| D 阶段 | 100K 插入 ID 起点 | 不限速插入 ID 起点 |
|---|---:|---:|
| 预热 | 10,000,000,000 | 14,000,000,000 |
| 正式 | 12,000,000,000 | 16,000,000,000 |

每段预留 2B 个 ID，避免预热/正式/不同限速重叠，也不复用之前实验的插入区间；两库使用相同起点。每阶段数据库记录增量与成功 INSERT 数一致，未把覆盖旧 key 伪装成插入。不同吞吐造成两库实际插入数量不同。

| 数据库 | 本轮开始记录数 | 本轮结束记录数 |
|---|---:|---:|
| Keylane DB0 | 100,000,000 | 109,732,242 |
| Aerospike ycsb | 100,304,416 | 108,097,912 |

Aerospike 原有额外历史记录仍占用资源。两库读取范围相同，但物理内容、老化和 GC 状态不完全一致；D 也会使数据集增长。后续不能直接复用本轮 D 区间重新运行 CREATE_ONLY。

## 机器、存储和资源放置

| 项目 | 配置 |
|---|---|
| 服务端 .4 | 172.16.0.4，AMD EPYC 9V74，16 逻辑 CPU / 8 物理核（SMT），约 125 GiB RAM |
| 客户端 .5 | 172.16.0.5，AMD EPYC 9V45，16 逻辑 / 16 物理核，约 32 GiB RAM，Java 17 |
| 存储 | 六块 NVMe，/dev/md0 RAID0，512 KiB chunk |
| Keylane | io_uring，/dev/md0p1（约 9.5 TiB），12 workers，CPU 0–11 |
| Aerospike | CE 8.1.2.4，/dev/md0p2（1 TiB），CPU 0–15 可用 |
| 系统与网卡 | housekeeping CPU 12–15，mlx5 IRQ 分布到 12–15，irqbalance 关闭；RPS/XPS 不变 |
| 监控 | .5 上 Prometheus / Grafana 全程保持运行，未在计时期间执行 perf 或编译 |

CPU 0–11 是 **6 个完整物理核**，不是 12 个物理核。Keylane 位于独立 `keylane-bench.slice`，AllowedCPUs/CPUAffinity=0–11；`system.slice`、`user.slice`、`init.scope` 及 unbound workqueue 使用 CPU 12–15。固定 per-CPU 内核线程、managed NVMe IRQ 不保证全部迁移。

**Aerospike 同属独立 benchmark slice，但可用 CPU 0–15，并与 housekeeping/IRQ 共享后四 CPU。两库不是相同 CPU 配额或隔离条件。** 两库串行运行；Aerospike 的冷恢复在预热之前完成，不计入测量。

256 表示 YCSB 工作线程数。Redis binding 每线程一个 Jedis 连接；Aerospike SDK 自行管理连接池，不能把同样线程数解释为相同 TCP 连接总数。

完整机器、线程/IRQ 快照、服务参数、Aerospike 配置与版本校验值见[本轮环境凭据](verified-update-matrix/main-f666837-abcd-20260914-environment.json)及[机器清单](verified-update-matrix/machine-inventory.json)。本轮保持既有 CPU/IRQ 策略，没有重新配置主机。

## 源码、构建与启动

生产优化已推送到 [main f666837](https://github.com/thweetkomputer/keylane/commit/f666837b0038ab65564a17cb3a0bca8530f8e1be)：GC 使用同步 predicate 找到物理身份匹配的候选，不再为此构造临时 vector；普通 `Find` 及异步读的候选快照路径不变。main 同时包含之前的 Hash 读复制/结果构造优化及 `b0606c5` 的主动过期配置更新。本轮不能用于单独估算 GC 优化的因果收益。

构建 checkout 是 `1b6c0d3`，整个非报告源码树与 `f666837` 完全一致；构建使用 GCC 13.3、Release、O3/native/LTO、静态 C++/OpenSSL，SPDK 关闭、调试故障与读写 tracing 关闭。生产文件在计时期间没有变化。

```text
Keylane commit: f666837b0038ab65564a17cb3a0bca8530f8e1be
Binary: /mnt/dev/keylane-main-f666837.siARSe/keylane
SHA256: 9162d45032c34a9ddffffd1ef8137495237256173092aa931b65890dc12b7266
```

构建后通过 56 项索引/存储格式单测、4 项 Hash/String/extent GC 端到端验证，以及 TTL 端到端测试；不是声称重跑了全套测试。[构建凭据](verified-update-matrix/main-f666837-build.json)、[单测](verified-update-matrix/main-f666837-unit-tests.xml)、[Hash/GC](verified-update-matrix/main-f666837-hash-gc-tests.xml)、[TTL](verified-update-matrix/main-f666837-ttl-tests.xml)。

Keylane 等效命令：

```bash
taskset -c 0-11 /mnt/dev/keylane-main-f666837.siARSe/keylane \
  --bind 172.16.0.4 --port 16379 --metrics-port 19100 \
  --threads 12 --pin-workers --shutdown-checkpoint \
  --log-dir /mnt/dev/keylane-md0-keylane --data-file /dev/md0p1
```

实际通过 `keylane-ycsb-cpu12.service` 启动，独立 slice、AllowedCPUs/CPUAffinity=0–11、LimitNOFILE=20000；不能直接从被限制在 CPU 12–15 的 user.slice 启动并声称相同放置。GC/defrag 开启：`paused=0 max_active_per_device=8 block_sleep_ms=0 record_sleep_us=0`。主动过期保持默认：interval=10 ms、map steps=256、deletes=64、index-maintenance steps=256，没有通过调参减少后台工作。

Aerospike 通过 `aerospike-ycsb-compare.service` 启动：

```bash
/usr/bin/asd --config-file /mnt/dev/aerospike-md0.conf --foreground
```

namespace 为 `ycsb`，RF=1、TTL=0、indexes-memory-budget=64G、flush-size=128K、max-write-cache=8G、stop-writes-sys-memory-pct=90；设备为 `/dev/md0p2`。完整配置保存在本轮环境 JSON 中。

## 客户端参数与复核

两库都从 .5 发起请求。Keylane 使用 `/mnt/dev/YCSB-hreplace-dist`（Jedis 3.9.0）；Aerospike 使用 `/mnt/dev/YCSB-aerospike-dist`（Aerospike Java 3.1.2）。本轮重新核对了客户端 JAR 校验值，与[客户端清单](verified-update-matrix/client-artifacts.json)一致；两份 core 的 75 个 class 字节一致。没有修改或推送 YCSB 的 `redis-scanindex-none` PR 分支。

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

Aerospike 对应 `as.host=172.16.0.4, as.port=3000, as.namespace=ycsb, as.timeout=10000`。每次完整命令都在同目录的 `main-f666837-abcd-20260914-*.command.json` 中，退出码、原始日志 SHA 和指标在对应 JSON 中。

[串行测试脚本](verified-update-matrix/main_abcd.py)保留实际执行顺序、服务切换、插入区间和失败检查。它使用固定批次名称并拒绝覆盖已有证据；再次压测应先分配新的批次名与 D 区间，不可直接重复执行旧 D。仅重算结果、不访问数据库：

```bash
python3 perf_reports/ycsb-rerun-2026-09-13/verified-update-matrix/main_abcd_summary.py --write
```

测试结束后 Aerospike 已停止，最新 main 的 Keylane 已恢复、DB0 为 109,732,242 keys、GC 开启，客户端没有残留 YCSB Java 进程；Prometheus/Grafana 继续运行。[完成凭据](verified-update-matrix/main-f666837-abcd-20260914-complete.json)、[最终服务状态](verified-update-matrix/main-f666837-abcd-20260914-final-server.json)、[恢复命令](verified-update-matrix/main-f666837-restored-server-command.json)。

生产代码版本为 `f666837`；本报告、原始日志、参数和校验脚本作为独立文档提交，不包含额外生产代码变更。
