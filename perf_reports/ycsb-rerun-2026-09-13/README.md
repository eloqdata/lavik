# Keylane 与 Aerospike：YCSB 的 QPS 和长尾延迟

测试日期：2026-09-14。使用一台服务端、一台客户端、256 个 YCSB 工作线程，比较 A/B/C/D 四种负载的吞吐及 p99、p99.9、p99.99 延迟。每项先预热 100 万次，再正式测量 300 秒，分别测试总计 10 万 QPS 限速和不限速；所有正式测量均零错误。

**两库读取的范围同为 1 亿个 key，每条 10 × 128 字节；库内实际总记录数不同。** 两库保留历史写入数据，CPU 配额和测量时间也不同。下文完整列出这些差异，结果适用于这里记录的部署配置。

**本报告的 Aerospike 数据来自沿用 Keylane 宿主调优的环境，不能作为“系统保持原样”的 Aerospike 基线。** 切换到 Aerospike 时仅将其进程可用 CPU 放开为 0–15，housekeeping、网卡 IRQ 和 irqbalance 状态没有恢复。未调优宿主上的 Aerospike QPS 与长尾尚未测得，现有记录无法量化这项设置的性能影响。

## 机型与资源配置

| 项目 | 配置 |
|---|---|
| 服务端 | Azure VM；AMD EPYC 9V74，16 逻辑 CPU / 8 物理核（SMT），约 125 GiB 内存 |
| 客户端 | Azure VM；AMD EPYC 9V45，16 逻辑 CPU / 16 物理核，约 32 GiB 内存；Java 17 |
| 操作系统内核 | 两机均为 Linux `6.17.0-1022-azure`，x86_64 |
| 数据链路 | 客户端 `172.16.0.5` → 服务端 `172.16.0.4`；内核网络 |
| 物理存储 | 六块 NVMe 组成 Linux RAID0 `/dev/md0`；总容量约 10.48 TiB，chunk=512 KiB |
| Keylane CPU | CPU 0–11：12 个逻辑 CPU、6 个物理核；12 个固定 worker，独立 benchmark slice |
| Aerospike CPU | CPU 0–15：16 个逻辑 CPU、8 个物理核；自定义 benchmark slice，宿主保留 Keylane 调优 |
| 系统任务（两库测量期间） | `system.slice`、`user.slice`、`init.scope` 限制到 CPU 12–15；unbound workqueue 掩码 `f000` |
| 网卡 IRQ（两库测量期间） | 快照中的 17 个 mlx5 IRQ 有效亲和性均在 CPU 12–15；irqbalance 状态为 `inactive` |
| 后台监控 | 客户端保留 Prometheus / Grafana；测量时未运行 perf/BPF 或编译 |

CPU 12–15 同时在 Aerospike 的可用范围内，两库 CPU 配额和隔离条件不同。固定 per-CPU 内核线程与 managed NVMe IRQ 不保证全部迁移。两库串行测量，没有同时争用该 RAID0。硬件与放置记录见[机器清单](verified-update-matrix/machine-inventory.json)、[Keylane 环境](verified-update-matrix/flush100-abcd-20260914-environment.json)和[Aerospike 环境](verified-update-matrix/main-f666837-abcd-20260914-environment.json)。

Aerospike 原批次环境的 `affinity_before` 保存了上述宿主状态，8 个正式窗口的 `server_before` / `server_after` 均记录进程可用 CPU 0–15。[实际启动函数](verified-update-matrix/connection_matrix.py)设置 `AllowedCPUs=0-15`、`CPUAffinity=0-15`，没有恢复系统任务、workqueue 或 IRQ 策略。测量进程能使用全部 CPU，并不表示宿主配置保持原样。

未调优的 Aerospike 基线应保留主机原有的任务调度、IRQ 分布和 irqbalance 状态，不额外套用 Keylane 的隔离策略。需要在该环境中重新测量后，才能给出对应成绩。

## 数据量

| 项目 | Keylane | Aerospike |
|---|---:|---:|
| 每条记录的字段 | 10 × 128 B = 1280 B | 10 × 128 B = 1280 B |
| 均匀随机读取范围 | 原始 100,000,000 个 key | 原始 100,000,000 个 key |
| 读取范围对应的字段净数据 | 128 GB | 128 GB |
| 批次开始实际记录数 | 109,732,242 | 100,304,416 |
| 批次结束实际记录数 | 119,483,607 | 108,097,912 |
| 所用裸分区 | `/dev/md0p1` | `/dev/md0p2` |
| 分区容量 | 约 9.5 TiB | 1 TiB |

128 GB 按十进制计算，只包含字段值，不包含 key、字段名、索引及记录格式开销；分区容量是分配空间，不等于实际写入的数据量。两库均未在该批次前清盘重灌，历史记录、数据老化及 GC 状态不同；D 的插入继续增加总记录数。

D 使用均匀分布，固定读取原始 1 亿个 key，不读取本轮新增记录。YCSB 参数 `insertstart=0, insertcount=100000000` 固定读取域，D 的 `recordcount` 指定新插入 key 的编号起点；它不表示库内总记录数。

| 数据库 | D 阶段 | 100K 插入 ID 起点 | 不限速插入 ID 起点 |
|---|---|---:|---:|
| Keylane | 预热 | 100,000,000,000 | 104,000,000,000 |
| Keylane | 正式 | 102,000,000,000 | 106,000,000,000 |
| Aerospike | 预热 | 10,000,000,000 | 14,000,000,000 |
| Aerospike | 正式 | 12,000,000,000 | 16,000,000,000 |

每阶段预留 20 亿个 ID，避免与历史插入及其他阶段重叠。该负载记为 D-Uniform，区别于标准的 latest-D；每阶段记录增量已与成功 INSERT 数核对。

## 两数据库参数

| 参数 | Keylane | Aerospike |
|---|---|---|
| 版本 | `f666837b0038ab65564a17cb3a0bca8530f8e1be` | Community Edition 8.1.2.4 |
| 存储 | io_uring，裸分区 `/dev/md0p1`，SPDK 关闭 | `storage-engine device`，裸分区 `/dev/md0p2` |
| 监听 | `172.16.0.4:16379`；metrics `:19100` | 服务端口 `3000`，access-address `172.16.0.4` |
| 并发与放置 | `--threads 12 --pin-workers`，CPU 0–11 | CPU 0–15；完整配置见环境凭据 |
| 刷盘与写缓存 | `--flush-max-ms 100`，写入分块 128 KiB | `flush-size 128K`，`max-write-cache 8G`；配置未覆盖 `flush-max-ms` |
| 内存与复制 | 单实例，DB0 | namespace `ycsb`，RF=1，TTL=0，`indexes-memory-budget 64G` |
| 读取 | `HGETALL`，全部 10 个字段 | 读取全部 10 个字段 |
| 更新 | `HREPLACE`，全部 10 个字段 | `REPLACE_ONLY`，全部 10 个字段 |
| 插入 | `HMSET` 创建新 Hash；`redis.scanindex=none` | `CREATE_ONLY` 创建新记录 |
| 客户端 | YCSB Redis binding；Jedis 3.9.0 | YCSB Aerospike binding；Java client 3.1.2 |
| 客户端超时 | 10000 ms | 10000 ms |

Keylane 构建为 GCC 13.3 / Release / O3 / native / LTO，静态 C++ 和 OpenSSL；二进制 SHA256 为 `9162d45032c34a9ddffffd1ef8137495237256173092aa931b65890dc12b7266`。构建记录见[构建凭据](verified-update-matrix/main-f666837-build.json)。

Keylane 的 `flush-max-ms=100` 控制周期刷盘触发间隔，普通写成功不承诺逐条同步持久化；数据 fdatasync 后再提交块头 fdatasync。GC/defrag 开启，`max_active_per_device=8`，无额外 sleep。主动过期使用默认预算：interval=10 ms、map steps=256、deletes=64、index-maintenance steps=256。

Aerospike 停写阈值为 `stop-writes-sys-memory-pct=90`、`stop-writes-used-pct=90`、`stop-writes-avail-pct=5`。

完整 Keylane 命令和 systemd 放置在[启动凭据](verified-update-matrix/flush100-abcd-20260914-server-command.json)；Aerospike 的完整配置在[环境凭据的 `aerospike_config`](verified-update-matrix/main-f666837-abcd-20260914-environment.json)。参数描述均对应测量时配置。

## 压测参数与指标定义

| Workload | READ | UPDATE | INSERT | 读取分布 |
|---|---:|---:|---:|---|
| A | 50% | 50% | 0 | Uniform |
| B | 95% | 5% | 0 | Uniform |
| C | 100% | 0 | 0 | Uniform |
| D-Uniform | 95% | 0 | 5% | 固定原始 1 亿 key 的 Uniform |

- 256 个 YCSB 工作线程：Redis binding 每线程一个 Jedis 连接，Aerospike SDK 自行管理连接池；此数字不能等同为双方相同的 TCP 连接数。
- `readallfields=true`、`writeallfields=true`、`fieldlengthdistribution=constant`、`insertorder=hashed`；关闭 SCAN 和 read-modify-write。
- 每个 workload 先测 100K、再测不限速，顺序为 C/A/B/D。每项独立 JVM 预热 100 万次，再由新的 JVM 正式运行 `maxexecutiontime=300`；正式窗口包含自身启动/JIT。`operationcount=2000000000` 是宽松上限。
- 100K 是所有线程、所有操作合计的 `-target 100000`。QPS = 正式窗口成功操作数 / 实际 RunTime；总 QPS 为该窗口各操作成功数之和除以耗时。
- 延迟来自客户端 YCSB 普通 operation HDR histogram，单位为毫秒。p99、p999、p9999 分别为 p99、p99.9、p99.99，即 99%、99.9%、99.99% 的操作在该延迟内完成。
- 百分位按各项完整正式窗口、各操作分别报告，不平均分段百分位、不混合读写百分位。`measurement.interval=both` 的 Intended 直方图仅保留于原始证据。

Keylane 测量窗口为 **2026-09-14 16:01:47–16:42:38 UTC**；Aerospike 为 **2026-09-14 11:12:51–11:54:13 UTC**。每库 8 项，每项一个 5 分钟正式窗口。Keylane 实际 RunTime 为 300,035–300,073 ms，正式成功操作共 750,773,355 次。整理本报告没有新运行压测；Aerospike 数字来自上述较早窗口，不能用于单独推断 100ms 刷盘的因果收益。

## QPS 与长尾延迟

每格：**该操作 QPS；p99 / p999 / p9999（ms）**。各项独立四舍五入，显示的操作 QPS 之和可能与总 QPS 相差 1。

| Workload | 限速 | 操作 | Aerospike CE（宿主保留 Keylane 调优） | Keylane HREPLACE（100ms） |
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


在上述历史配置下，Keylane 的不限速 B/C/D 吞吐分别高 28.92% / 35.91% / 31.81%，A 吞吐低 2.51%；表内各操作长尾均更低。100K 下，A 的 Keylane 读写长尾更高，B/C/D 的小差异不作确定优劣判断。上述差异包含宿主调优、CPU 配额、历史数据状态和测量时间等因素，不能外推为相对未调优 Aerospike 基线的收益。

## 原始结果与离线复核

- [按操作汇总的 CSV](verified-update-matrix/flush100-abcd-20260914-summary.csv)保留 QPS、平均延迟、p50/p95/p99/p999/p9999、最大延迟和成功数。
- [汇总 JSON](verified-update-matrix/flush100-abcd-20260914-summary.json)、[Keylane 正式结果](verified-update-matrix/flush100-abcd-20260914-results.json)、[Keylane 预热及正式阶段](verified-update-matrix/flush100-abcd-20260914-phases.json)、[Aerospike 所在批次](verified-update-matrix/main-f666837-abcd-20260914-results.json)保存完整数值和来源；该批次文件中的其他实验结果不参与本报告。
- 同目录 `flush100-abcd-20260914-*.command.json` 和 `main-f666837-abcd-20260914-aerospike-*.command.json` 保存实际客户端命令，对应 `.log` 保存原始输出。
- [校验脚本](verified-update-matrix/flush100_summary.py)重新解析日志，核对 28 个操作结果、12 个混合负载总 QPS、退出码、记录增量、Keylane 服务端命令计数和报告表格；仅读取本地文件。

在仓库根目录执行：

```sh
python3 perf_reports/ycsb-rerun-2026-09-13/verified-update-matrix/flush100_summary.py --check-readme
```

原始压测脚本使用已消耗的 D 插入区间。若另行复测，需要分配新的批次名称和插入区间；离线复核无需启动数据库。
