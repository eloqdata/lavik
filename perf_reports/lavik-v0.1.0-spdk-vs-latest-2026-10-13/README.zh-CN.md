# Lavik v0.1.0：最新版 Redis / Valkey 对比与 1TB 数据测试

[English](README.md) | **简体中文**

测试日期：2026-10-13。使用下载的 **Lavik v0.1.0 标准 x86_64 正式包**，重新测量 Redis 8.10.2、Valkey 9.1.2 对照。本报告的 **120 个正式测量点**包含 110 个 1,000 万 key 对比点，以及 10 个 Lavik 单独运行的 10 亿 key 点；每条 value 均为 1,024 B。Lavik 在灌入、预热、测量期间始终开启 defrag；没有复用历史对照数据。

10M 组的 Lavik GET 峰值为 **1145.8k QPS**（1280 连接，p99.9 **7.135 ms**）；SET 峰值为 **972.0k QPS**（1280 连接，p99.9 **7.199 ms**）。这些是单次测量结果。图中展示吞吐和 p99.9 延迟随连接数的变化。

## 吞吐对比：Lavik 10M / 1B 与 Redis / Valkey

![Lavik 10M / 1B, Redis and Valkey throughput](combined-qps.svg)

四组均展示 80–1280 连接。10M 组每点 30 秒，使用默认客户端随机种子；1B 组每点 60 秒，使用独立客户端随机种子。数据规模、时长及随机种子设置存在差异。

## p99.9 延迟对比

![Lavik 10M / 1B, Redis and Valkey p99.9 latency](combined-p999.svg)

延迟为客户端完整测量窗口的 p99.9，单位为毫秒，越低越好。使用与吞吐图相同的 I/O 线程配置；横轴为 log₂ 连接数刻度。

1B GET 峰值为 1,069,126 QPS（640 连接），SET 峰值为 817,113 QPS（1280 连接）。这些仅描述本次单轮扫描。

整批正式测量采集时间（含每点准备/快照开销）：2026-10-13 11:33:05 UTC – 2026-10-13 13:12:17 UTC



## 硬件与部署

| 项目 | 配置 |
|---|---|
| 服务端 | Azure Standard_L16aos_v4，Japan West / zone 2；AMD EPYC 9V74；16 逻辑核 / 8 物理核；Linux 内存 125.8 GiB |
| 客户端 | Azure Standard_F32als_v6；AMD EPYC 9V74；32 物理核；Linux 内存 62.8 GiB；压测进程限制到 CPU 0–15 |
| 系统 | 两端均为 Ubuntu 24.04、Linux 6.17.0-1022-azure；本批未重启 |
| 网络 | 客户端 10.84.0.5 → 服务端 10.84.1.4:6379；mlx5 加速路径；kernel TCP |
| 服务端 CPU | 全产品使用 CPU 0–15；Lavik 16 workers；Redis/Valkey 1/2/4/8/16 I/O 线程 |
| Lavik 存储 | 六张专用本地 NVMe，每张 1,919,850,381,312 B；SPDK/VFIO；8 GiB hugepages |
| Redis/Valkey 存储 | 数据驻 DRAM；关闭 AOF、自动快照；校验后的 RDB 仅用于各线程配置间重置基线 |

## 方法与边界

- 方法沿用 [beta.1 对比报告](../lavik-v0.1.0-beta.1-spdk-vs-peers-2026-09-18/README.zh-CN.md)中的 Redis/Valkey 10M 组，以及 Lavik 的 1B 存储组；本批不包含 Dragonfly 或 Garnet。按 2026-10-13 官方稳定版选择 [Redis 8.10.2](https://github.com/redis/redis/releases/tag/8.10.2)、[Valkey 9.1.2](https://valkey.io/download/releases/v9-1-2/)。
- 顺序为 Lavik SPDK 10M → Lavik SPDK 1B → Redis → Valkey。任一时刻只运行一个被测数据库和一个压测客户端；本组不改变服务端 CPU/IRQ 策略。编译在正式测量前完成。
- 使用 `lavik-v0.1.0-linux-x86_64.tar.gz`；源码 `feb61224e414c5653fd09560e20c9d214db1e3ac`；可执行文件 SHA-256 `7bf8d204a137678cee0e1a621b02661181373fbbfe1366bc1c201942b9dcf420`；GCC 13 Release / x86-64-v2。未重新编译 Lavik，也未添加源码补丁。压缩包校验与发布的 SHA-256 一致。
- Lavik 使用 `spdk-max-completions-per-poll=16`（v0.1.0 默认 8）。Defrag 保持开启，每个正式点前后均核验。
- 清盘前按序列号、PCI、namespace、容量核验六张临时盘，排除系统盘和开发盘。分别独立灌入 10M 与 1B Lavik 数据集，两次灌入之间重新清理六张临时盘；Lavik 干净停止后，盘交还内核并释放 hugepage 预留。
- Redis/Valkey 使用发布的 8.10.2 / 9.1.2 固定 commit 编译，分配器为 jemalloc 5.3.0。各自新灌数据并生成 SHA 校验的 RDB；每档 I/O 线程从该基线启动。AOF、自动快照关闭，其他配置保留产品默认值。
- 10M 组：memtier 2.5.1，16 线程、pipeline=1、不限速；十进制 key 1..10,000,000、无前缀；均匀随机 GET 或覆盖 SET；value=1,024 B。连接数 80/160/320/640/1280，每点 30 秒，每档服务端配置先 GET 预热 10 秒。按原 10M 方法保留默认相关客户端随机种子。
- 1B 组使用十进制 key 1..1,000,000,000、均匀随机 GET 或覆盖 SET；80/160/320/640/1280 连接，每点 60 秒，各客户端使用 distinct seed，不额外 GET 预热；沿用原报告存储组方法，并启用 shutdown checkpoint。1.024 TB 为 value 原始载荷，不包含 key、索引与存储格式开销。
- 每档服务端配置前后核验精确 DBSIZE 和五个样本 value 长度。全部正式 GET miss=0，全部正式点连接错误=0。延迟为完整测量窗口的客户端分位数；
- 每点仅测一次，未估计置信区间或运行间波动；产品按顺序而非交错执行。线程职责不同，Lavik 持久化与纯内存对照的保证也不同。此次客户端型号、CPU 以及 Lavik 构建与历史报告存在差异，不能将跨报告的变化全部归因于 Lavik 代码。

## Redis / Valkey 持久化配置

周期性 RDB 和 AOF 在灌入、预热及正式测量期间均关闭。配置文件：[redis-valkey-persistence.conf](redis-valkey-persistence.conf)。

```conf
save ""
appendonly no
```

每个产品首次灌入 10M 数据后，仅在正式窗口之外手动执行一次 `SAVE`，供各 I/O 线程配置重启恢复同一基线。启动时读取该 RDB 不会开启周期性快照。正式测量期间写入仅在内存中完成。

## 运行命令

以下 Bash 命令对应实测参数；请按部署环境调整路径、地址和 PCI 身份。一次只运行一个数据库和一个压测客户端。每轮使用独立结果目录。

### Lavik SPDK

按 [SPDK 存储指南](../../docs/operations/spdk-storage.md)核验六张专用临时 NVMe、准备 VFIO 和 8 GiB hugepages。10M 和 1B 两组各从空盘独立灌入；不得覆盖系统盘或已有业务数据。完成后干净停止 Lavik、恢复内核驱动及 hugepage 预留，再测试内存数据库。以下为 10M 启动命令：

```bash
sudo env 'BYCORF_EAL_ARGS=-a 2401:00:00.0 -a 221c:00:00.0 -a 7f10:00:00.0 -a 399c:00:00.0 -a c0ef:00:00.0 -a 6319:00:00.0' BYCORF_DPDK_MEMORY_MB=8192 taskset \
  -c 0-15 /mnt/dev/benchmark/release010/downloads/lavik-v0.1.0-linux-x86_64/lavik \
  --storage=spdk \
  --bind=10.84.1.4 \
  --threads=16 \
  --tomb-raider-interval-ms=0 \
  --spdk-max-completions-per-poll=16 \
  --log-dir=/mnt/dev/benchmark/release010-latest/suite/memory/lavik-spdk/logs \
  --data-file=spdk://2401:00:00.0/1 \
  --data-file=spdk://221c:00:00.0/1 \
  --data-file=spdk://7f10:00:00.0/1 \
  --data-file=spdk://399c:00:00.0/1 \
  --data-file=spdk://c0ef:00:00.0/1 \
  --data-file=spdk://6319:00:00.0/1
```

1B 组使用相同参数，增加 `--shutdown-checkpoint`，并将日志目录改为 `suite/storage/lavik-spdk/logs`。启动后等待恢复及后台初始化完成，使用 `CONFIG GET defrag-paused` 核验为 `no`；各正式点前后再次核验。

### Redis / Valkey

使用 jemalloc 5.3.0，构建命令为 `make -j16 BUILD_TLS=no`。Redis 8.10.2 对应提交 `498ecd0d6d007db11ddb3aea9428552598a78622`，Valkey 9.1.2 对应 `7f1dffedff6de73058b2c2a389422b6ecd56c8fb`。首次灌入使用 16 I/O 线程。

```bash
# On the server; start one product/configuration at a time.
PRODUCT=redis                    # redis or valkey
IO_THREADS=16                    # tested: 1, 2, 4, 8, 16
DATA_DIR=/mnt/dev/benchmark/release010-latest/suite/data/$PRODUCT
mkdir -p "$DATA_DIR"
ulimit -n 65535
taskset -c 0-15 "/mnt/dev/benchmark/release010-latest/downloads/$PRODUCT/src/$PRODUCT-server" \
  --bind 10.84.1.4 --protected-mode no --port 6379 --daemonize no \
  --dir "$DATA_DIR" --dbfilename dump.rdb --save "" --appendonly no \
  --maxmemory 0 --maxclients 10000 --io-threads "$IO_THREADS"
```

图中 Redis GET/SET 使用 16 I/O 线程，Valkey GET 使用 16、SET 使用 8。它们来自完整的 1/2/4/8/16 线程扫描，每个命令选择单个峰值最高的配置。

### 灌入、预热和连接数扫描

```bash
# On the client: memtier_benchmark 2.5.1.
KEYS=10000000                    # 1000000000 only for the Lavik 1B group
SECONDS_PER_POINT=30
SEED=()
if [ "$KEYS" -eq 1000000000 ]; then
  SECONDS_PER_POINT=60
  SEED=(--distinct-client-seed)
fi
mkdir -p results
ulimit -n 65535
COMMON=(--server=10.84.1.4 --port=6379 --protocol=redis
  --threads=16 --pipeline=1 --key-minimum=1 --key-maximum="$KEYS"
  --key-prefix= --data-size=1024 --hide-histogram
  --print-percentiles=50,99,99.9,99.99 --show-config)

# Once per fresh dataset; each key is written once across 640 clients.
taskset -c 0-15 memtier_benchmark "${COMMON[@]}" --clients=40 \
  --json-out-file=results/fill.json --requests="$((KEYS / 640))" \
  --ratio=1:0 --key-pattern=P:P
redis-cli -h 10.84.1.4 -p 6379 --raw DBSIZE
redis-cli -h 10.84.1.4 -p 6379 STRLEN 1
```

对于 Redis/Valkey，首次灌入后执行下列基线命令；然后以 1、2、4、8、16 I/O 线程依次启动，每档等待 RDB 加载结束、DBSIZE=10000000，核验持久化配置，再执行下一段预热及测量。每档结束使用 `SHUTDOWN NOSAVE`，保持同一 `dump.rdb` 不变；不要重新灌入或在测量中执行 SAVE。Lavik 不执行这些 RDB 基线命令。

```bash
redis-cli -h 10.84.1.4 -p 6379 SAVE
redis-cli -h 10.84.1.4 -p 6379 SHUTDOWN NOSAVE
# On the server, record/check the unchanged RDB baseline hash:
sha256sum "$DATA_DIR/dump.rdb"
# After each restart, run these checks from the client:
redis-cli -h 10.84.1.4 -p 6379 CONFIG GET save
redis-cli -h 10.84.1.4 -p 6379 CONFIG GET appendonly
redis-cli -h 10.84.1.4 -p 6379 INFO persistence
```

```bash
# Run once for Lavik 10M and once per Redis/Valkey I/O-thread configuration.
# The Lavik 1B group has no additional GET warmup.
if [ "$KEYS" -eq 10000000 ]; then
  taskset -c 0-15 memtier_benchmark "${COMMON[@]}" --clients=40 \
    --json-out-file=results/warmup.json --test-time=10 \
    --command='GET __key__' --command-key-pattern=R --command-is-read
fi
for OP in GET SET; do
  COMMAND=(--command='GET __key__' --command-key-pattern=R --command-is-read)
  if [ "$OP" = SET ]; then
    COMMAND=(--command='SET __key__ __data__' --command-key-pattern=R)
  fi
  for CONNECTIONS in 80 160 320 640 1280; do
    taskset -c 0-15 memtier_benchmark "${COMMON[@]}" \
      --clients="$((CONNECTIONS / 16))" \
      --json-out-file="results/$OP-c$CONNECTIONS.json" \
      --test-time="$SECONDS_PER_POINT" "${COMMAND[@]}" "${SEED[@]}" \
      > "results/$OP-c$CONNECTIONS.txt" 2>&1
  done
done
redis-cli -h 10.84.1.4 -p 6379 --raw DBSIZE
```
