# YCSB：HREPLACE 两组微优化未达到吞吐与长尾的共同目标

本轮按“不改算法、不动 mutex”尝试了两组 HREPLACE 代码优化，均未作为成功优化保留。生产源码和运行二进制已经恢复到原基线；此前已合并的 Hash 读取去复制、直接构造全字段读取结果、写参数预分配仍然保留。

新测的不限速 Workload A 中，现有 Keylane HREPLACE 基线为 **436,719 QPS**，Aerospike 为 **434,480 QPS**，中位数仅差 **0.52%**，不足以证明稳定吞吐领先。Keylane 的 READ/UPDATE p9999 为 **9.887 / 9.935 ms**，低于 Aerospike 的 **30.527 / 27.951 ms**；但 100K 限速时 Keylane 的混合长尾明显更高。不能把不限速下的结果概括为所有负载档位都优于 Aerospike，也不能把基线已有的表现算作本轮新代码的收益。

证据截至 **2026-09-13 16:46:51 UTC**。两组 Keylane 交替试验共 48 个计分单元，加上 Aerospike 12 个单元，共 **300M 正式操作＋60M 预热操作，全部成功**；另外的初测与 perf 采样不计入评分。

## 两库与两种 Hash 更新模式

256 个 YCSB 客户端线程，Uniform，查询原始 100M key 域，每条 10 个 field × 128 字节，全部字段读取/更新。A 为 50% READ / 50% UPDATE；C 为纯读；不含 INSERT。

单元格式：**操作 QPS；p99 / p999 / p9999（ms）**。所有延迟都来自 **YCSB operation histogram**，不是服务端内部计时，也不是 Intended histogram。各列取三轮独立指标的中位数，不是合并 HDR 的百分位，所以 READ/UPDATE QPS 的中位数之和不必等于总 QPS 中位数。

| Workload | 限速 | 操作 | Aerospike CE（本轮） | Keylane HMSET（前批参考） | Keylane HREPLACE（本轮基线） |
|---|---|---|---:|---:|---:|
| C | 不限速 | READ | 363,346；4.843 / 10.399 / 21.231 | 493,243；0.792 / 3.289 / 6.467 | 479,065；0.975 / 3.325 / 7.635 |
| A | 不限速 | 总 QPS | 434,480 | 396,825 | 436,719 |
| A | 不限速 | READ | 217,205；3.379 / 13.639 / 30.527 | 198,380；3.273 / 6.179 / 14.375 | 218,339；3.289 / 5.943 / 9.887 |
| A | 不限速 | UPDATE | 217,275；2.793 / 12.159 / 27.951 | 198,445；3.367 / 6.207 / 13.991 | 218,380；2.397 / 4.955 / 9.935 |
| C | 100K | READ | 99,453；0.474 / 0.635 / 2.335 | 99,820；0.472 / 0.700 / 1.599 | 99,804；0.471 / 0.678 / 1.608 |
| A | 100K | 总 QPS | 99,275 | 99,798 | 99,804 |
| A | 100K | READ | 49,601；0.442 / 0.586 / 1.561 | 49,906；0.525 / 3.405 / 6.375 | 49,885；0.934 / 5.811 / 9.135 |
| A | 100K | UPDATE | 49,639；0.359 / 0.487 / 1.419 | 49,925；0.558 / 3.431 / 6.503 | 49,891；0.587 / 2.439 / 4.131 |

100K 是全客户端读写合计的目标速率，不是每条连接或每类操作各 100K。HMSET 列保留此前 12＋4 配置的三轮结果，**本轮没有重新测试 HMSET**，不用于判定本轮代码收益。HREPLACE 列统一选取最后一组交替试验的三轮 baseline，不挑选较早或较快的一轮。Aerospike 不再采用旧的单轮延迟。

HREPLACE 基线三轮 A 不限速 QPS 为 440,917 / 403,779 / 436,719；Aerospike 为 428,192 / 434,480 / 442,517，运行间波动大于 0.52% 的中位数差。每次计分仅 5M 操作，不能把这组短测当作长期稳定容量上限。

## 本轮两组候选及处理

### 1. 内联排序索引、原位构造 HashEntry：不保留

保留同一套按字段排序、重复字段取最后值的 O(n log n) 算法；小请求将排序索引放入内联缓冲区，HashEntry 在已预留的数组位置构造。锁、持久化格式和 HSET/HMSET 语义均未改动。

三轮 A 不限速 QPS 中位数 444,879 → 444,484（−0.09%），UPDATE p9999 10.519 → 15.447 ms；C 不限速 QPS 中位数 486,760 → 476,872（−2.03%）。没有观察到满足目标的收益。

证据：[逐轮结果](verified-update-matrix/cpu12-hreplace-prep.results.json)、[指标 CSV](verified-update-matrix/cpu12-hreplace-prep.summary.csv)、[中位数](verified-update-matrix/cpu12-hreplace-prep.medians.json)、[环境与源码差异](verified-update-matrix/cpu12-hreplace-prep.environment.json)、[候选补丁](verified-update-matrix/cpu12-hreplace-prep.rejected.patch)。

### 2. 小 Hash 从字段视图直接编码：同样不保留

保留排序、去重、16 KiB 分组阈值和原编码算法。小 Hash 直接从字段视图生成拥有全部字节的最终 payload；视图在同步准备函数返回前销毁，不跨 await。大 Hash 仍构造拥有字段/值的 HashEntry，走原来的分组、extent、事务与恢复路径。内存准入和发布前校验不变。

对本例 10 个短字段名、每值 128 字节的小 Hash，准备路径省去排序索引数组、HashEntry 数组和 10 个 value 字符串的中间堆分配；这不是对整个请求总分配数的测量。编码仍为逐字段写入相同的长度和字节，没有使用上一批未通过的 resize-and-overwrite 编码改动。

| Workload | 限速 | 操作 | 基线 | 直接编码候选 |
|---|---|---|---:|---:|
| C | 不限速 | READ | 479,065；0.975 / 3.325 / 7.635 | 482,625；0.897 / 3.455 / 10.839 |
| A | 不限速 | 总 QPS | 436,719 | 440,762 |
| A | 不限速 | READ | 218,339；3.289 / 5.943 / 9.887 | 220,318；3.339 / 6.027 / 10.151 |
| A | 不限速 | UPDATE | 218,380；2.397 / 4.955 / 9.935 | 220,444；2.333 / 5.035 / 11.575 |
| C | 100K | READ | 99,804；0.471 / 0.678 / 1.608 | 99,796；0.460 / 0.662 / 1.459 |
| A | 100K | 总 QPS | 99,804 | 99,777 |
| A | 100K | READ | 49,885；0.934 / 5.811 / 9.135 | 49,881；0.824 / 4.831 / 9.207 |
| A | 100K | UPDATE | 49,891；0.587 / 2.439 / 4.131 | 49,879；0.477 / 2.505 / 4.451 |

A 不限速总 QPS 中位数提高 0.93%，但 READ/UPDATE p999、p9999 中位数均未改善，UPDATE p9999 增加约 16.51%。C 不限速吞吐中位数提高 0.74%，p9999 却从 7.635 升到 10.839 ms。100K 下也并非所有尾延迟都改善。按本次共同目标，不保留该候选；这不是断言视图方案在所有条件下必然更慢，而是本次证据不足以支持上线。

| 轮次 | A 基线 QPS | A 候选 QPS | 配对变化 | UPDATE p9999：基线 → 候选（ms） |
|---|---:|---:|---:|---:|
| 1 | 440,917 | 424,520 | -3.72% | 10.319 → 11.575 |
| 2 | 403,779 | 440,762 | 9.16% | 9.935 → 13.823 |
| 3 | 436,719 | 443,302 | 1.51% | 8.139 → 10.191 |

证据：[逐轮结果](verified-update-matrix/cpu12-hreplace-views.results.json)、[完整指标 CSV](verified-update-matrix/cpu12-hreplace-views.summary.csv)、[中位数](verified-update-matrix/cpu12-hreplace-views.medians.json)、[环境、源码差异与二进制 SHA](verified-update-matrix/cpu12-hreplace-views.environment.json)、[候选补丁](verified-update-matrix/cpu12-hreplace-views.rejected.patch)、[当时的测试补丁](verified-update-matrix/cpu12-hreplace-views.tests.patch)。

## perf 与后台活动：局部成本下降不等于长尾改善

独立运行 1M 预热＋20M A 操作，采用 `perf record -F 99 -e cpu-clock:u --call-graph dwarf,8192`，仅采样对应 Keylane PID。这台 VM 不支持 cycles/instructions 硬件 PMU。两份 profile 都没有丢失样本，均排除在上表评分之外；perf 正常以 SIGINT 停止，凭据中的退出码 −2 对应这个信号。

| on-CPU 指标 | HREPLACE 基线 | 直接编码候选 |
|---|---:|---:|
| 分配入口 self 占比 | 2.83% | 1.95% |
| 字符串构造调用链占比 | 3.00% | 1.42% |
| RecordIndex FindCandidates self 占比 | 2.98% | 5.02% |
| CRC self 占比 | 3.45% | 4.10% |
| memmove self 占比 | 3.29% | 3.94% |
| 同一采样窗口完成的 defrag 次数 | 1,856 | 2,841 |

占比来自各自采样窗口，调用链彼此包含，不能相加，也不能当作精确的单请求成本变化。采样看到了分配/构造成本降低，同时也有不同的 GC 工作量；它不包含 off-CPU 等待，不能证明 YCSB 尖峰的主因。

计分窗口中的后台工作量也不同。直接编码试验三轮 A 不限速，基线/候选完成的 defrag 次数分别为 **533/1,112、2,006/715、687/698**。第二轮基线仅 403,779 QPS，当时前后快照均有 8 个活跃 defrag；其 JVM GC 总时间为 101 ms，占约 0.82%。这些是观察到的背景差异，不是证明服务端 GC 或 Java GC 导致尖峰的分段耗时证据。

因此，不能把小幅 QPS 变化全部归因于代码。后续若继续优化，应先量化混合负载下后台整理、块分配与写缓冲等待的关系；本轮没有改 mutex、关闭 GC，或改变调度算法来换取分数。

证据：[基线 self](verified-update-matrix/cpu12-perf-hreplace-baseline-a.perf-self.txt)、[基线调用链](verified-update-matrix/cpu12-perf-hreplace-baseline-a.perf-children.txt)、[候选 self](verified-update-matrix/cpu12-perf-hreplace-views-a.perf-self.txt)、[候选调用链](verified-update-matrix/cpu12-perf-hreplace-views-a.perf-children.txt)、[逐轮背景计数](verified-update-matrix/cpu12-hreplace-views.background.csv)、[背景计数脚本](verified-update-matrix/cpu12_background_summary.py)。原始 perf 栈文件可能包含采样内存且体积较大，仅保留在本机，由 .gitignore 排除；可分享的符号摘要、命令、SHA 和 YCSB 日志均在同一证据目录。

## 机器、存储与 CPU 放置

| 项目 | 配置 |
|---|---|
| 服务端 | 172.16.0.4，AMD EPYC 9V74，16 逻辑 CPU / 8 物理核（SMT），约 125 GiB |
| 客户端 | 172.16.0.5，AMD EPYC 9V45，16 逻辑 CPU / 16 物理核，约 32 GiB，Java 17；Prometheus/Grafana 保持运行 |
| 磁盘 | 六块 NVMe 的 /dev/md0 RAID0，512 KiB chunk；本轮没有清盘或更改分区 |
| Keylane | io_uring，/dev/md0p1，原有设备标签容量保留；12 workers，CPU 0–11 |
| Aerospike | CE 8.1.2.4，/dev/md0p2，CPU 0–15 均可用；RF=1，indexes-memory-budget=64G，flush-size=128K，max-write-cache=8G |

CPU 0–11 是前 6 个完整物理核心，12–15 是后 2 个，不能称作 12 个物理核心。两库串行测量，不同时施压。

Keylane 位于独立 `keylane-bench.slice`，AllowedCPUs/CPUAffinity=0–11；`system.slice`、`user.slice`、`init.scope` 的运行时 AllowedCPUs=12–15，全局 unbound workqueue 使用后四 CPU。enP46392s1 的 mlx5 MSI IRQ 轮流分配到 CPU 12–15，RPS/XPS 保留原值，irqbalance 停用。固定 per-CPU 内核线程和 managed NVMe IRQ 不保证全部迁移。

**本轮 Aerospike 也在独立的 benchmark slice 中，但可使用 0–15，并未限制为 12 workers。** 当前相同的全局 IRQ/housekeeping 放置在两库测试时都保留；因此 Aero 可以使用、也会与 IRQ/housekeeping 共享后四 CPU。这不是两库相同的 CPU 资源隔离，也不同于更早 Aero 单轮的 IRQ 状态；不应把历史到本轮的 Aero 延迟变化归为数据库版本变化。

Keylane 保持 100,000,000 keys。Aerospike namespace 启动时为 100,304,416 records，额外历史记录未清理，仍占用索引和磁盘。客户端查询域、字段数与值大小相同，但两库物理数据状态不完全相同。Keylane 此前完成过清库重载；本轮两库均未重新 load。GC/数据老化和进程恢复状态无法视为相同。

## 启动与客户端配置

Keylane 等效进程参数：

```bash
taskset -c 0-11 /mnt/dev/keylane/build/keylane \
  --bind 172.16.0.4 --port 16379 --metrics-port 19100 \
  --threads 12 --pin-workers --shutdown-checkpoint \
  --log-dir /mnt/dev/keylane-md0-keylane --data-file /dev/md0p1
```

实际由 `keylane-ycsb-cpu12.service` 启动，LimitNOFILE=20000；不要直接在被限制到 CPU 12–15 的 user.slice 中启动。GC/defrag 状态为 `paused=0 max_active_per_device=8 block_sleep_ms=0 record_sleep_us=0`。启动/核验见 [cpu12_run.py](verified-update-matrix/cpu12_run.py)，CPU/IRQ 放置和原策略恢复见 [cpu12_affinity.py](verified-update-matrix/cpu12_affinity.py)。这些运行时隔离设置会在主机重启后清除。

Aerospike 等效命令为 `/usr/bin/asd --config-file /mnt/dev/aerospike-md0.conf --foreground`，本次通过 `aerospike-ycsb-compare.service`、独立 slice、AllowedCPUs/CPUAffinity=0–15、LimitNOFILE=20000 启动。完整 namespace/network 配置与服务命令保存在 [本轮 Aero 环境凭据](verified-update-matrix/cpu12-hreplace-aero.environment.json)，[复测脚本](verified-update-matrix/cpu12_aero_compare.py) 会等待恢复结束、核验 record 数及 stop-writes，再开始测试，结束后恢复 Keylane。

共同 YCSB 参数：

```properties
recordcount=100000000
fieldcount=10
fieldlength=128
fieldlengthdistribution=constant
readallfields=true
writeallfields=true
insertorder=hashed
requestdistribution=uniform
insertproportion=0
scanproportion=0
readmodifywriteproportion=0
measurementtype=hdrhistogram
measurement.interval=both
hdrhistogram.percentiles=50,95,99,99.9,99.99
```

A 设 readproportion=0.5、updateproportion=0.5；C 设 readproportion=1、updateproportion=0。使用 `-threads 256`，限速组另加 `-target 100000`。每个单元分别用独立 JVM 做 1M 预热和 5M 正式操作；每轮基线→候选，两版均重启，顺序 C→A、各自 100K→不限速。正式测量没有 perf、编译或正确性测试。Aero 恢复一次后按相同 C/A、100K/不限速顺序测三轮。

Keylane 客户端是 .5 的 `/mnt/dev/YCSB-hreplace-dist/bin/ycsb`（Jedis 3.9.0），设 `redis.scanindex=none`、`redis.updatecommand=keylane.hreplace`；HMSET 前批设为 `hmset`。Aerospike 用 `/mnt/dev/YCSB-aerospike-dist/bin/ycsb`，UPDATE 为 REPLACE_ONLY，并提交全部字段。HREPLACE 与 REPLACE_ONLY 都是只替换已存在记录；HMSET 仍为字段合并语义，需要读取旧 Hash。本轮未修改 YCSB binding 或其上游 PR 分支。

## 构建、正确性与复现证据

基线生产代码与 main `b3c55ca` 相同，包含 `63f868a` 的 Hash 优化及后续集合大小修复；保留二进制构建于 `a1b90e6`，其后的这两次 main 提交仅改报告/工具。构建使用 GCC 13、Release/O3、C++23、march=native、LTO 和静态 C++/OpenSSL，候选保持相同构建选项。

| 版本 | SHA256 | 处理 |
|---|---|---|
| 基线 | 8ded8812356d872eabb05bcd0ff828dcf8185771c50ef1d3b566d5d48403ab1d | 当前运行 |
| 内联准备候选 | e41c8c9259b6b036a69369dcc58f280d43128852eac1477639304cc00089bf65 | 不保留生产改动 |
| 直接编码候选 | 4365296b8206f9f3668b4c1e80fe1755e7b95df0c5a1d3118c1a82bed1e25bc5 | 不保留生产改动 |

直接编码候选通过 77 项正确性测试：67 项 Hash codec/旁表单测（含显式启用的 >1 GiB 编解码）、7 项 HREPLACE/全字段读取端到端、3 项 compact Hash/Set/String 事务或恢复。覆盖不同参数数目、重复/二进制/空字段、16 KiB 边界、TTL、WATCH/EXEC/Lua、复制、分组/extent 和冷恢复。2 项依赖注错的测试在普通 Release 下跳过，不能算通过。候选的逐字节视图编码测试随实现归档；通用重复字段和分组边界回归测试保留。没有测量普通 String SET/GET 的吞吐，本轮通过恢复原生产二进制避免交付未验证的性能变化。

- [候选单测](verified-update-matrix/cpu12-hreplace-views.unit-tests.json)、[HREPLACE/读取测试](verified-update-matrix/cpu12-hreplace-views.hash-tests.json)、[Hash/Set/String 测试](verified-update-matrix/cpu12-hreplace-views.collection-tests.json)。回退后再次通过 77 项，仍有 2 项 Release 注错测试跳过：[恢复版单测](verified-update-matrix/cpu12-hreplace-restored.unit-tests.json)、[恢复版 HREPLACE/读取](verified-update-matrix/cpu12-hreplace-restored.hash-tests.json)、[恢复版 Hash/Set/String](verified-update-matrix/cpu12-hreplace-restored.collection-tests.json)。
- [Aero 三轮原始结果](verified-update-matrix/cpu12-hreplace-aero.results.json)、[Aero 完整指标 CSV](verified-update-matrix/cpu12-hreplace-aero.summary.csv)、[两库汇总](verified-update-matrix/cpu12-hreplace-views.comparison.json)。
- [交替测试脚本](verified-update-matrix/cpu12_codec_ab.py)、[校验/汇总脚本](verified-update-matrix/cpu12_codec_summary.py)、[两库校验脚本](verified-update-matrix/cpu12_hreplace_compare_summary.py)、[采样脚本](verified-update-matrix/cpu12_perf.py)。交替脚本需指定 `--mode hreplace`，并使用新的 `--label`，不会覆盖已有试验。
- 原始同名 .log、.command.json、.before.txt、.after.txt 与汇总均在 `verified-update-matrix/`；日志记录实际客户端命令、成功/失败计数、普通/Intended 分位数、时间戳和 SHA。主表不混用两类直方图。
- [最终恢复凭据：原二进制 SHA、生产源码无差异、100M keys、defrag 开启](verified-update-matrix/cpu12-hreplace-restored.final.json)、[Aero 结束后的服务状态](verified-update-matrix/cpu12-hreplace-aero.restored.json)、[此前清库凭据](verified-update-matrix/cpu12-clear-db0.json)、[100M 加载结果](verified-update-matrix/cpu12-load.json)。
- HMSET 前批来自 [cpu12-codec.medians.json](verified-update-matrix/cpu12-codec.medians.json) 的 baseline；其 resize-and-overwrite 候选因纯读回退已撤回，结果与 [旧候选补丁](verified-update-matrix/cpu12-codec.rejected.patch) 仅留作审计。
- 本轮只维护这份报告；已有的 `md0-matrix/README.md`、旧 16-worker、旧短测及中间版本文件未改动，不进入本轮结论。不要用旧 `summarize.py` 重建覆盖本页。保留回归测试、报告及复测证据，两组候选的生产代码均已回退。
