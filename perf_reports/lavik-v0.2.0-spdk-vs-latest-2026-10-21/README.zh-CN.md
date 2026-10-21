# Lavik v0.2.0：16 vCPU 上的 Redis / Valkey 与 1B 容量测试

[English](README.md) | **简体中文**

测试日期：2026-10-21（UTC）。全部 **120 个正式点**均在新建 Azure 测试环境重新测量。Lavik 的 1B 数据集包含 **953.67 GiB 逻辑 value 数据**，是 10M 数据量的 100 倍。

1B 数据集的 GET / SET 峰值为 **1,085,805 / 858,557 ops/s**，分别为小数据集峰值的 **93.9% / 90.8%**。这些峰值可能来自不同连接数，匹配连接数的结果见下表。 在 10M 基准中，GET 峰值最高的是 Lavik 0.2.0 / 10M（1,156,956 ops/s），SET 峰值最高的是 Lavik 0.2.0 / 10M（945,384 ops/s）。

## 吞吐与 p99.9 延迟

<img src="combined.svg" alt="GET and SET throughput and p99.9 latency" width="800">

| Command | Product / keys | Workers / I/O threads | Connections | Peak ops/s | p99.9 ms |
| --- | --- | --- | --- | --- | --- |
| GET | Lavik 0.2.0 / 10M | 16 | 1280 | 1,156,956 | 7.103 |
| GET | Lavik 0.2.0 / 1B | 16 | 1280 | 1,085,805 | 6.655 |
| GET | Redis 8.10.2 | 16 | 1280 | 976,087 | 7.039 |
| GET | Valkey 9.1.2 | 16 | 1280 | 913,756 | 7.743 |
| SET | Lavik 0.2.0 / 10M | 16 | 1280 | 945,384 | 10.175 |
| SET | Lavik 0.2.0 / 1B | 16 | 1280 | 858,557 | 7.455 |
| SET | Redis 8.10.2 | 16 | 1280 | 863,860 | 7.391 |
| SET | Valkey 9.1.2 | 8 | 1280 | 793,809 | 7.743 |

每种命令为每个对比产品选择峰值最高的**一个固定 I/O 线程配置**，整条吞吐和延迟曲线均使用该配置，不逐点拼接。展示全部五档连接数。Lavik 10 亿条结果同时用于存储型数据库对比报告。

## 相同连接数对比

每格为 **ops/s / p99.9 ms**。

| Command | Connections | Lavik 0.2.0 / 10M | Lavik 0.2.0 / 1B | Redis 8.10.2 | Valkey 9.1.2 |
| --- | --- | --- | --- | --- | --- |
| GET | 80 | 202,252 / 0.767 | 208,248 / 0.751 | 226,986 / 0.743 | 261,778 / 0.743 |
| GET | 160 | 377,857 / 0.855 | 383,469 / 0.823 | 408,159 / 0.871 | 459,449 / 0.895 |
| GET | 320 | 612,513 / 1.431 | 609,693 / 1.367 | 603,866 / 1.399 | 640,091 / 1.431 |
| GET | 640 | 876,217 / 3.983 | 855,417 / 3.935 | 800,217 / 3.711 | 791,161 / 3.887 |
| GET | 1280 | 1,156,956 / 7.103 | 1,085,805 / 6.655 | 976,087 / 7.039 | 913,756 / 7.743 |
| SET | 80 | 252,226 / 0.679 | 244,830 / 0.695 | 239,685 / 0.711 | 278,263 / 0.719 |
| SET | 160 | 431,054 / 1.255 | 410,033 / 0.903 | 384,939 / 0.903 | 453,171 / 0.847 |
| SET | 320 | 637,287 / 1.375 | 614,860 / 1.775 | 579,902 / 1.423 | 638,517 / 1.383 |
| SET | 640 | 834,508 / 3.871 | 788,279 / 3.775 | 732,342 / 3.423 | 749,664 / 3.759 |
| SET | 1280 | 945,384 / 10.175 | 858,557 / 7.455 | 863,860 / 7.391 | 793,809 / 7.743 |

## 完整 I/O 线程扫描

| Product | I/O threads | GET peak ops/s (connections) | SET peak ops/s (connections) |
| --- | --- | --- | --- |
| Redis 8.10.2 | 1 | 154,244 (320) | 142,910 (320) |
| Redis 8.10.2 | 2 | 158,908 (320) | 147,806 (320) |
| Redis 8.10.2 | 4 | 437,073 (1280) | 398,372 (1280) |
| Redis 8.10.2 | 8 | 838,330 (1280) | 822,820 (1280) |
| Redis 8.10.2 | 16 | 976,087 (1280) | 863,860 (1280) |
| Valkey 9.1.2 | 1 | 152,804 (320) | 143,996 (320) |
| Valkey 9.1.2 | 2 | 159,958 (640) | 148,287 (640) |
| Valkey 9.1.2 | 4 | 436,332 (640) | 395,389 (1280) |
| Valkey 9.1.2 | 8 | 846,926 (1280) | 793,809 (1280) |
| Valkey 9.1.2 | 16 | 913,756 (1280) | 773,060 (1280) |

## 内存与客户端诊断

| Product / keys | INFO used_memory (MiB) | Process RSS (MiB) |
| --- | --- | --- |
| Lavik 0.2.0 / 10M | 716.6–716.6 | 1,486.1–1,499.8 |
| Lavik 0.2.0 / 1B | 48,105.9–48,105.9 | 48,006.9–48,025.7 |
| Redis 8.10.2 | 12,677.9–12,681.3 | 12,797.9–12,877.8 |
| Valkey 9.1.2 | 12,654.7–12,655.8 | 12,773.2–13,065.9 |

范围来自正式点后的快照。INFO 内存使用各产品自己的统计口径；RSS 为操作系统进程驻留内存。Lavik 另有 **8,192 MiB 大页预留**。客户端最大平均 CPU 使用为 **15.066 核**，单个客户端线程的最大平均使用为 **0.948 核**；测量包围窗口内最大服务端 CPU steal 为 **0.0000%**。

部分点的客户端 CPU 使用已接近其 16 CPU 预算，可能受压测端限制；所示峰值不能直接作为服务器的容量上限。

## 版本与硬件

| 项目 | 配置 |
| --- | --- |
| Server | Standard_L16aos_v4; 16 vCPU (8 physical cores) / 128 GiB nominal RAM; AMD EPYC 9V74 80-Core Processor |
| Client | Standard_F32als_v6; separate VM; CPUs 0–15 |
| Region | Japan East, zone 2 |
| OS | Ubuntu 24.04; Linux 6.17.0-1022-azure on both hosts |
| Storage | 6 × 1,919,850,381,312-byte dedicated local NVMe; SPDK/VFIO |
| Network | Kernel TCP; same VNet and availability zone; accelerated networking |
| Lavik | Official standard x86_64 v0.2.0 package; 2908f86f2770e9c6e8acc6585b3bedb4b44d1cd5 |

## 测试方法

- key 为十进制 `1..N`，无前缀，value 为 1,024 字节，无 TTL。Lavik 10M 和 1B 分别独立装载；Redis、Valkey 各使用 10M。
- memtier 2.5.1 固定在客户端 16 个 CPU 上，pipeline 1，全 key 范围均匀随机 GET 或覆盖 SET。连接数为 80, 160, 320, 640, 1280；每点测一次，基准组 30 秒，10 亿条组 60 秒。1,000 万条组沿用原报告的默认客户端种子，10 亿条组使用独立种子；两组访问流策略不同，容量保留率仅作描述。
- 每个配置先预热 GET 10 秒；10 亿条组沿用原方法，不额外预热，然后 GET、SET 各按连接数递增测量。所有产品在同一服务器串行运行，测量前已完成所有构建。
- Lavik 使用 16 个 worker 和独占 NVMe/SPDK。非默认设置为 `--tomb-raider-interval-ms=0`、`--spdk-max-completions-per-poll=16` 和 8 GiB EAL/大页内存。每个测量点前后检查 defrag 保持启用。10 亿条实例启用 `--shutdown-checkpoint`。
- Redis 8.10.2、Valkey 9.1.2 与旧报告版本一致，使用默认优化构建与 jemalloc，分别扫描 1, 2, 4, 8, 16 个 I/O 线程。`save ""` 禁用定时快照，AOF 保持关闭；初次装载后手动 SAVE，各线程配置从相同 RDB 基线恢复。正式写入只进入内存。
- THP 为 `never`，overcommit 为 1，不添加 cgroup 内存上限。每个配置前后核验精确 DBSIZE 和五个 value 长度，并检查装载请求数、零 GET 未命中、连接错误、版本和 CPU 亲和性。

## 证据与测量限制

[完整逐点数据](results.csv) · [版本、硬件与校验元数据](provenance.json)

每点仅一次测量，不提供置信区间。不同产品的成功响应不代表相同的同步持久化保证。新机器的物理放置、网络延迟和运行时间均可能与旧报告不同，不能把历史差异全部归因于版本代码。[原始日志与测量凭据](raw-results.tar.gz)包含逐文件 SHA-256 清单，归档哈希记录在元数据中。

本次大规格测试位于 Japan East；历史 0.1.0 报告使用 Japan West。区域和网络放置变化尤其会影响低连接数结果。

## 运行命令

以下 Bash 命令对应实测参数；请按部署环境调整路径、地址和 PCI 身份。一次只运行一个数据库和一个压测客户端。每轮使用独立结果目录。

### Lavik SPDK

按 [SPDK 存储指南](../../docs/operations/spdk-storage.md)核验六张专用临时 NVMe、准备 VFIO 和 8 GiB hugepages。10M 和 1B 两组各从空盘独立灌入；不得覆盖系统盘或已有业务数据。完成后干净停止 Lavik、恢复内核驱动及 hugepage 预留，再测试内存数据库。以下为 10M 启动命令：

```bash
sudo prlimit --memlock=unlimited:unlimited --nofile=65535:65535 env 'BYCORF_EAL_ARGS=-a 7133:00:00.0 -a 10fc:00:00.0 -a 9c4a:00:00.0 -a ef09:00:00.0 -a fa44:00:00.0 -a 9816:00:00.0' BYCORF_DPDK_MEMORY_MB=8192 taskset \
  -c 0-15 /opt/bench020/downloads/lavik-v0.2.0-linux-x86_64/lavik \
  --storage=spdk \
  --bind=10.96.1.4 \
  --tomb-raider-interval-ms=0 \
  --spdk-max-completions-per-poll=16 \
  --log-dir=/opt/bench020/results/large/memory/lavik-10m/logs \
  --data-file=spdk://7133:00:00.0/1 \
  --data-file=spdk://10fc:00:00.0/1 \
  --data-file=spdk://9c4a:00:00.0/1 \
  --data-file=spdk://ef09:00:00.0/1 \
  --data-file=spdk://fa44:00:00.0/1 \
  --data-file=spdk://9816:00:00.0/1
```

1B 组使用相同参数，增加 `--shutdown-checkpoint`，并将日志目录改为 `results/large/storage/lavik-1b/logs`。启动后等待恢复及后台初始化完成，使用 `CONFIG GET defrag-paused` 核验为 `no`；各正式点前后再次核验。

### Redis / Valkey

使用 jemalloc 5.3.0，构建命令为 `make -j12 BUILD_TLS=no`。Redis 8.10.2 对应提交 `498ecd0d6d007db11ddb3aea9428552598a78622`，Valkey 9.1.2 对应 `7f1dffedff6de73058b2c2a389422b6ecd56c8fb`。首次灌入使用 16 I/O 线程。

```bash
# On the server; start one product/configuration at a time.
PRODUCT=redis # redis or valkey
IO_THREADS=16 # tested: 1, 2, 4, 8, 16
IO_ARGS=(); if ((IO_THREADS > 1)); then IO_ARGS=(--io-threads "$IO_THREADS"); fi
DATA_DIR=/opt/bench020/results/large/data/$PRODUCT
mkdir -p "$DATA_DIR"
ulimit -n 65535
taskset -c 0-15 "/opt/bench020/bin/$PRODUCT-server" \
  --bind 10.96.1.4 --protected-mode no \
  --dir "$DATA_DIR" --save "" \
    "${IO_ARGS[@]}"
```

### 灌入、预热和连接数扫描

```bash
# On the client: memtier_benchmark 2.5.1.
KEYS=10000000 # 1000000000 only for the Lavik 1B group
SECONDS_PER_POINT=30
SEED=()
if [ "$KEYS" -eq 1000000000 ]; then
  SECONDS_PER_POINT=60
  SEED=(--distinct-client-seed)
fi
mkdir -p results
ulimit -n 65535
COMMON=(--server=10.96.1.4
  --threads=16 --key-minimum=1 --key-maximum="$KEYS"
  --key-prefix= --data-size=1024 --hide-histogram
  --print-percentiles=50,99,99.9,99.99 --show-config)

# Once per fresh dataset; each key is written once across 640 clients.
taskset -c 0-15 memtier_benchmark "${COMMON[@]}" --clients=40 \
  --json-out-file=results/fill.json --requests="$((KEYS / 640))" \
  --ratio=1:0 --key-pattern=P:P
redis-cli -h 10.96.1.4 --raw DBSIZE
redis-cli -h 10.96.1.4 STRLEN 1
```

对于 Redis/Valkey，首次灌入后执行下列基线命令；然后以 1、2、4、8、16 I/O 线程依次启动，每档等待 RDB 加载结束、DBSIZE=10000000，核验持久化配置，再执行下一段预热及测量。每档结束使用 `SHUTDOWN NOSAVE`，保持同一 `dump.rdb` 不变；不要重新灌入或在测量中执行 SAVE。Lavik 不执行这些 RDB 基线命令。

```bash
redis-cli -h 10.96.1.4 SAVE
redis-cli -h 10.96.1.4 SHUTDOWN NOSAVE
# On the server, record/check the unchanged RDB baseline hash:
sha256sum "$DATA_DIR/dump.rdb"
# After each restart, run these checks from the client:
redis-cli -h 10.96.1.4 CONFIG GET save
redis-cli -h 10.96.1.4 CONFIG GET appendonly
redis-cli -h 10.96.1.4 INFO persistence
```

```bash
# Run once for Lavik 10M and once per Redis/Valkey I/O-thread configuration.
# The Lavik 1B group has no additional GET warmup.
if [ "$KEYS" -eq 10000000 ]; then
  taskset -c 0-15 memtier_benchmark "${COMMON[@]}" --clients=40 \
    --json-out-file=results/warmup.json --test-time=10 \
    --command='GET __key__' --command-is-read
fi
for OP in GET SET; do
  COMMAND=(--command='GET __key__' --command-is-read)
  if [ "$OP" = SET ]; then
    COMMAND=(--command='SET __key__ __data__' )
  fi
  for CONNECTIONS in 80 160 320 640 1280; do
    taskset -c 0-15 memtier_benchmark "${COMMON[@]}" \
      --clients="$((CONNECTIONS / 16))" \
      --json-out-file="results/$OP-c$CONNECTIONS.json" \
      --test-time="$SECONDS_PER_POINT" "${COMMAND[@]}" "${SEED[@]}" \
      > "results/$OP-c$CONNECTIONS.txt" 2>&1
  done
done
redis-cli -h 10.96.1.4 --raw DBSIZE
```
