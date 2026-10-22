# Lavik v0.2.0：16 vCPU、10M key 的 Redis / Valkey 对比

[English](README.md) | **简体中文**

保留 **10M 测试组的 88 个正式点**：Lavik、Redis、Valkey，160/320/640/1280 总连接；仅排除 80 连接档。每条 value 为 1 KiB，总逻辑 value 净载荷为 9.54 GiB。10M 使用 **16 个 memtier 线程、客户端 CPU 0–15**；32 线程的 1B 测试见[独立报告](../lavik-v0.2.0-spdk-1b-2026-10-21/README.zh-CN.md)。

UTC: 2026-10-21T13:01:41.211000+00:00 – 2026-10-21T14:41:58.874000+00:00.

本次恢复使用已保存的原始实测数据，没有重新压测。原实验扫描包含 80 连接；本报告的图表、表格、CSV 和正式测量复现循环均排除该档位。

## 吞吐与 p99.9 延迟

![GET / SET throughput and p99.9](combined.svg)

主图对每个命令、每种产品选择在保留档位中达到最高吞吐的固定 I/O 线程配置，并在整个连接数扫描中保持该配置。峰值与配置选择均已在排除 80 连接后重新计算。

| Command | Product | Workers / I/O threads | Connections | Peak ops/s | p99.9 ms |
| --- | --- | --- | --- | --- | --- |
| GET | Lavik 0.2.0 / 10M | 16 | 1280 | 1,156,956 | 7.103 |
| GET | Redis 8.10.2 | 16 | 1280 | 976,087 | 7.039 |
| GET | Valkey 9.1.2 | 16 | 1280 | 913,756 | 7.743 |
| SET | Lavik 0.2.0 / 10M | 16 | 1280 | 945,384 | 10.175 |
| SET | Redis 8.10.2 | 16 | 1280 | 863,860 | 7.391 |
| SET | Valkey 9.1.2 | 8 | 1280 | 793,809 | 7.743 |

相同连接数下的 **ops/s / p99.9 ms**：

| Command | Connections | Lavik 0.2.0 / 10M | Redis 8.10.2 | Valkey 9.1.2 |
| --- | --- | --- | --- | --- |
| GET | 160 | 377,857 / 0.855 | 408,159 / 0.871 | 459,449 / 0.895 |
| GET | 320 | 612,513 / 1.431 | 603,866 / 1.399 | 640,091 / 1.431 |
| GET | 640 | 876,217 / 3.983 | 800,217 / 3.711 | 791,161 / 3.887 |
| GET | 1280 | 1,156,956 / 7.103 | 976,087 / 7.039 | 913,756 / 7.743 |
| SET | 160 | 431,054 / 1.255 | 384,939 / 0.903 | 453,171 / 0.847 |
| SET | 320 | 637,287 / 1.375 | 579,902 / 1.423 | 638,517 / 1.383 |
| SET | 640 | 834,508 / 3.871 | 732,342 / 3.423 | 749,664 / 3.759 |
| SET | 1280 | 945,384 / 10.175 | 863,860 / 7.391 | 793,809 / 7.743 |

## 完整 I/O 线程扫描

![All tested configurations](all-configurations.svg)

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

- key 为十进制 `1..N`，无前缀，value 为 1,024 字节，无 TTL。各产品分别独立装载 10M 条。
- memtier 2.5.1 固定在客户端 16 个 CPU 上，pipeline 1，全 key 范围均匀随机 GET 或覆盖 SET。保留的连接数为 160, 320, 640, 1280；每点测一次、30 秒，沿用原报告的默认客户端种子。
- 每个配置先预热 GET 10 秒，然后 GET、SET 各按连接数递增测量。所有产品在同一服务器串行运行，测量前已完成所有构建。
- Lavik 使用 16 个 worker 和独占 NVMe/SPDK。非默认设置为 `--tomb-raider-interval-ms=0`、`--spdk-max-completions-per-poll=16` 和 8 GiB EAL/大页内存。每个测量点前后检查 defrag 保持启用。
- Redis 8.10.2、Valkey 9.1.2 与旧报告版本一致，使用默认优化构建与 jemalloc，分别扫描 1, 2, 4, 8, 16 个 I/O 线程。`save ""` 禁用定时快照，AOF 保持关闭；初次装载后手动 SAVE，各线程配置从相同 RDB 基线恢复。正式写入只进入内存。
- THP 为 `never`，overcommit 为 1，不添加 cgroup 内存上限。每个配置前后核验精确 DBSIZE 和五个 value 长度，并检查装载请求数、零 GET 未命中、连接错误、版本和 CPU 亲和性。

## 内存与客户端诊断

| Product | Max average client CPU / cores | Max thread average / cores | Max server steal / % | INFO used_memory / MiB | Process RSS / MiB |
| --- | --- | --- | --- | --- | --- |
| Lavik 0.2.0 / 10M | 15.066 | 0.948 | 0.0000 | 716.6–716.6 | 1,486.1–1,499.8 |
| Redis 8.10.2 | 13.187 | 0.840 | 0.0000 | 12,677.9–12,681.3 | 12,797.9–12,877.8 |
| Valkey 9.1.2 | 12.515 | 0.801 | 0.0000 | 12,654.7–12,655.8 | 12,773.3–13,065.9 |

部分测量点接近客户端 CPU 预算，观测到的峰值不能直接视为服务器容量上限。

## 证据与测量限制

[完整逐点数据](results.csv) · [版本、硬件与校验元数据](provenance.json)

每点仅一次测量，不提供置信区间。不同产品的成功响应不代表相同的同步持久化保证。新机器的物理放置、网络延迟和运行时间均可能与旧报告不同，不能把历史差异全部归因于版本代码。[原始日志与测量凭据](raw-results.tar.gz)包含逐文件 SHA-256 清单，归档哈希记录在元数据中。

本次大规格测试位于 Japan East；历史 0.1.0 报告使用 Japan West。区域和网络放置变化尤其会影响低连接数结果。

[Validation](validation.json)

## 运行命令

以下 Bash 命令对应实测参数；请按部署环境调整路径、地址和 PCI 身份。一次只运行一个数据库和一个压测客户端。每轮使用独立结果目录。

### Lavik SPDK

按 [SPDK 存储指南](../../docs/operations/spdk-storage.md)核验六张专用临时 NVMe、准备 VFIO 和 8 GiB hugepages。10M 组从空盘独立灌入；不得覆盖系统盘或已有业务数据。完成后干净停止 Lavik、恢复内核驱动及 hugepage 预留，再测试内存数据库。以下为 10M 启动命令：

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

启动后等待恢复及后台初始化完成，使用 `CONFIG GET defrag-paused` 核验为 `no`；各正式点前后再次核验。

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
KEYS=10000000
SECONDS_PER_POINT=30
SEED=()
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
  for CONNECTIONS in 160 320 640 1280; do
    taskset -c 0-15 memtier_benchmark "${COMMON[@]}" \
      --clients="$((CONNECTIONS / 16))" \
      --json-out-file="results/$OP-c$CONNECTIONS.json" \
      --test-time="$SECONDS_PER_POINT" "${COMMAND[@]}" "${SEED[@]}" \
      > "results/$OP-c$CONNECTIONS.txt" 2>&1
  done
done
redis-cli -h 10.96.1.4 --raw DBSIZE
```
