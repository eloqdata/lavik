# Lavik v0.1.0 与 Aerospike：YCSB 吞吐及尾延迟

[English](README.md) | **简体中文**

两库独立新灌 **1 亿条记录，每条 10 × 128 B 字段**。完整 **A/B/C/D × 100K/不限速** 矩阵包含 16 个正式测量窗口，每个 300 秒；所有预热与正式阶段错误为零。**Lavik defrag 全程开启**。

Aerospike 使用默认性能配置及原始宿主策略；Lavik 使用 12 workers 加 4 核系统/IRQ 隔离，io_uring 接入 RAID0 裸分区。两者 CPU 分配与数据库配置不同。

不限速总吞吐相对 Aerospike 的实测差异：A **+4.36%**, B **+25.13%**, C **+28.76%**, D **+25.56%**.

图中柱形使用左轴总 QPS，折线使用右轴各操作 p99.9 延迟（ms）；两张图分别设置刻度。READ 与写操作分别计算，C 没有写操作。结果表的操作单元格为 **操作 QPS；p99 / p99.9（ms）**。

## 100K 限速

![100K 限速: throughput and p99.9 latency](ycsb-100k.png)

| Workload | 操作 | Aerospike | Lavik |
|---|---|---:|---:|
| A | 总 QPS | 99,883 | 99,966 |
| A | READ | 49,932; 0.342 / 0.437 | 49,987; 0.372 / 0.530 |
| A | UPDATE | 49,951; 0.292 / 0.385 | 49,979; 0.337 / 0.474 |
| B | 总 QPS | 99,827 | 99,967 |
| B | READ | 94,829; 0.374 / 0.498 | 94,968; 0.482 / 0.856 |
| B | UPDATE | 4,998; 0.321 / 0.426 | 4,999; 0.410 / 0.653 |
| C | 总 QPS | 99,902 | 99,970 |
| C | READ | 99,902; 0.379 / 0.481 | 99,970; 0.406 / 0.495 |
| D-Uniform | 总 QPS | 99,901 | 99,961 |
| D-Uniform | READ | 94,906; 0.351 / 0.442 | 94,962; 0.368 / 0.488 |
| D-Uniform | INSERT | 4,995; 0.311 / 0.404 | 4,999; 0.354 / 0.479 |

## 不限速

![不限速: throughput and p99.9 latency](ycsb-unlimited.png)

| Workload | 操作 | Aerospike | Lavik |
|---|---|---:|---:|
| A | 总 QPS | 507,401 | 529,501 |
| A | READ | 253,704; 1.407 / 2.953 | 264,758; 2.105 / 5.083 |
| A | UPDATE | 253,697; 1.196 / 2.255 | 264,743; 1.532 / 2.831 |
| B | 总 QPS | 461,404 | 577,373 |
| B | READ | 438,351; 1.274 / 2.313 | 548,492; 0.776 / 1.818 |
| B | UPDATE | 23,053; 1.087 / 1.853 | 28,881; 0.702 / 1.578 |
| C | 总 QPS | 461,251 | 593,911 |
| C | READ | 461,251; 1.253 / 1.982 | 593,911; 0.707 / 1.556 |
| D-Uniform | 总 QPS | 460,167 | 577,808 |
| D-Uniform | READ | 437,177; 1.246 / 2.055 | 548,915; 0.790 / 1.824 |
| D-Uniform | INSERT | 22,991; 1.077 / 1.754 | 28,893; 0.724 / 1.589 |

## 硬件与存储

- **服务端**：Azure Standard_L16aos_v4，Japan West / zone 2；AMD EPYC 9V74；16 逻辑核 / 8 物理核；Linux 内存 125.8 GiB
- **客户端**：Azure Standard_F32als_v6；AMD EPYC 9V74；32 物理核、62.8 GiB 内存；Java 17；YCSB 进程限制到 CPU 0–15
- **系统**：两端均为 Ubuntu 24.04、Linux 6.17.0-1022-azure；未重启
- **网络**：单客户端 10.84.0.5 → 服务端 10.84.1.4；kernel TCP、mlx5 加速路径
- **物理盘**：六张本地 NVMe，每张 1,919,850,381,312 B；Linux RAID0，chunk=512 KiB
- **分区**：两个独立裸分区，各 1,099,494,850,560 B；24 MiB 对齐；无文件系统；灌入前全量写零
- **监测**：每 phase 的有效配置、进程、IRQ、存储、客户端计数；开启 Lavik metrics

## 数据与工作负载

- **A**：READ: 50%；UPDATE: 50%；INSERT: 0；读取分布: 均匀随机
- **B**：READ: 95%；UPDATE: 5%；INSERT: 0；读取分布: 均匀随机
- **C**：READ: 100%；UPDATE: 0；INSERT: 0；读取分布: 均匀随机
- **D-Uniform**：READ: 95%；UPDATE: 0；INSERT: 5%；读取分布: 固定初始 1 亿条 key

两库均从 100,000,000 条记录开始；矩阵完成后 Aerospike 为 108,497,069 条，Lavik 为 110,271,711 条。

## 数据库与宿主配置

- **版本**：Aerospike: Community Edition 8.1.2.4-4；Lavik: v0.1.0 标准 x86_64 正式包
- **存储**：Aerospike: storage-engine device，RAID0 裸分区；Lavik: --storage=uring，RAID0 裸分区
- **CPU**：Aerospike: 原始宿主策略，CPU 0–15；默认 service-threads=80、auto-pin=none；Lavik: CPU 0–11 上 12 个固定 worker；系统任务、workqueue、NIC IRQ 使用 CPU 12–15
- **systemd**：Aerospike: 默认 system.slice，不额外限制 affinity；Lavik: lavik-bench.slice，AllowedCPUs/CPUAffinity=0–11
- **Flush**：Aerospike: 默认 1 MiB、1000 ms；Lavik: 128 KiB 写块，100 ms
- **缓存**：Aerospike: 默认 max-write-cache=64 MiB、post-write-cache=256 MiB、read-page-cache=false；Lavik: 正式包默认值
- **索引预算**：Aerospike: 默认 indexes-memory-budget=0；Lavik: 正式包默认值
- **GC / defrag**：Aerospike: 默认值；Lavik: 开启；每设备最多 8 任务；block/record sleep=0
- **READ**：Aerospike: 全部 10 个 bin；Lavik: HGETALL，全部 10 个字段
- **UPDATE**：Aerospike: REPLACE_ONLY，全部 10 个 bin；Lavik: LAVIK.HREPLACE，全部 10 个字段
- **INSERT**：Aerospike: CREATE_ONLY；Lavik: HMSET，redis.scanindex=none
- **客户端 binding**：Aerospike: YCSB Aerospike，Java client 3.1.2；Lavik: YCSB Redis，Jedis 3.9.0
- **复制 / TTL**：Aerospike: 单节点 RF=1、namespace ycsb；默认 TTL 0；Lavik: 单实例 DB0、无副本、无 TTL

Lavik 为未改动的标准正式包：`v0.1.0 / feb61224e414c5653fd09560e20c9d214db1e3ac`; SHA-256 `7bf8d204a137678cee0e1a621b02661181373fbbfe1366bc1c201942b9dcf420`. Aerospike SHA-256 `6e1b2bd6deebd5816f4f369ac1db4151217ed0be3b66578bdd6f15c24e6af8d0`.

## 测量方法与边界

- 两库独立新灌 1 亿条记录，按 C/A/B/D 顺序，每个 workload 先限速 100K 再不限速；先 Aerospike 后 Lavik。同一 RAID0 顺序使用，数据库不同时运行。正式矩阵各 case 之间不清库、不重启。
- 每个 case 使用 256 个 YCSB worker，单独 JVM 预热 100 万次操作，再用新的 JVM 正式测量 300 秒。`-target 100000` 是所有线程、所有操作合计限速；正式 `operationcount=2000000000` 仅作宽松上限，以计时器结束。JIT/启动爬坡没有从测量窗口剔除。
- 10 个字段，每字段固定 128 B；readallfields/writeallfields=true；hashed key、uniform 读取；SCAN 与 read-modify-write 关闭；客户端超时 10,000 ms。字段值原始载荷共 128 GB（119.21 GiB），不含 key、字段名、索引和记录格式开销。
- D 只读取最初的 1 亿条 key，因此标为 D-Uniform。100K 预热/测量 INSERT 起点为 2B/4B；不限速为 6B/8B，互不重叠。两库起始规模相同；最终记录数随 D 的成功插入吞吐而不同。
- 两库均核验恰好 1 亿次成功 INSERT 与服务端记录数一致；灌入后干净停止并恢复。对八个分布式样本核验全部十个字段长度及 SHA-256，比较恢复前后的一致性。每个 phase 核对客户端成功数、服务端命令计数和记录数增量，错误必须为零。
- Aerospike 仅配置必需的集群/监听/namespace/RF/设备选项；CPU 调度、IRQ、线程、缓存、flush、索引预算和回收保留默认值，每阶段前后记录有效配置。Aerospike 停止后才应用 Lavik 的 12+4 CPU/IRQ 隔离，结束后恢复宿主策略。核验 completion IRQ 的实际 CPU 及中断增量，确认它们在测试阶段不落到 Lavik worker 核。
- Lavik 在灌入、恢复、预热和测量全程开启 defrag，每设备最多 8 个任务，不加额外 sleep。每阶段记录 CONFIG GET defrag-paused 与 DEFRAG STATUS。本 YCSB 组的 Tomb Raider、active expiration 保留正式包默认值。100 ms 周期 flush 不代表逐写同步持久化承诺。
- CPU 编号为逻辑核：worker 0–11 占六个物理核，系统/IRQ 12–15 占另外两个物理核（SMT2）。源地址策略路由将 10.84.1.4 的回复经 eth1/table 101 发出，压测回复不走管理网卡。
- QPS=成功操作数/实际 YCSB RunTime；总 QPS 包含成功读写。各操作 p99/p99.9 取完整窗口 ordinary HDR histogram，不平均区间分位数；intended histogram 保存在原始日志中。相同 worker 数不意味着相同 TCP 连接数。
- 正式灌入前有三次环境准备失败：缺少 Aerospike 运行目录、缺少 python 启动别名、监听端口就绪早于分区归属就绪。原始日志保存在 evidence/setup-failure-*；补齐运行环境后，就绪检查等待全部 4096 个主分区。没有更换 Java binding 或修改数据库性能参数；失败尝试未插入记录，也不计入测量窗口。
- 自定义 YCSB 源码为 `eloqdata/YCSB@a47bf01c4f197d2ff2eecbbebd9743da5a1cae47`，叠加留档的 LAVIK.HREPLACE 命令名补丁；实际 binding 源码和 JAR 哈希均留档。
- 每个 cell 只有一次 5 分钟测量，未估计运行间波动或置信区间，产品顺序固定。两种指定部署的 CPU 分配、宿主策略、缓存、持久化保证不同，不是单变量实验。

- Aerospike: 2026-10-13T09:54:41Z – 2026-10-13T10:35:42Z (UTC), 8 个正式窗口.
- Lavik: 2026-10-13T10:39:09Z – 2026-10-13T11:20:04Z (UTC), 8 个正式窗口.

完成后已恢复原始 CPU/IRQ 策略，Lavik 干净停止，Aerospike 完成最终恢复与数据核验；记录数描述 YCSB 矩阵结束时的状态。完整证据归档并离线验证后，六张临时盘已按授权重建，用于后续最新版 Redis/Valkey 与 Lavik 1B 测试；YCSB 数据不再保留。

## 证据与复现

[结果 JSON](results.json) · [操作 CSV](summary.csv) · [二进制身份](binaries.json) · [服务端清单](server-inventory.json) · [客户端清单](client-inventory.json) · [网络路径](network-path.json) · [YCSB 源码](evidence/client-source/source.json) · [客户端 JAR 哈希](evidence/client-jars.sha256) · [全部阶段](evidence/phases.json) · [采集脚本](evidence/runner.py) · [宿主策略](evidence/host_policy.py) · [客户端命令](evidence/ycsb_client.py) · [Aerospike 配置](evidence/aerospike.conf) · [存储清单](evidence/storage-after.json) · [完成状态](evidence/complete.json) · [校验和](SHA256SUMS)

```sh
python3 perf_reports/lavik-v0.1.0-aerospike-ycsb-2026-10-13/verify_report.py
python3 perf_reports/lavik-v0.1.0-aerospike-ycsb-2026-10-13/build_assets.py
```

离线校验需要 Python 3.11+；绘图需要 Matplotlib 3.11.2。校验覆盖两次全新灌入、32 个阶段、16 个正式窗口、28 行操作结果、每阶段宿主/数据库策略、中英文表格和证据文件哈希；不会启动数据库或打开裸设备。在线重跑需要新的临时数据空间和未使用的 D 插入区间。不包含私钥、下载的可执行文件或生成的数据库。
