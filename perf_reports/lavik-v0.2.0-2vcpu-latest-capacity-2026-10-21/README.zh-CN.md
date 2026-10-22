# Lavik v0.2.0：2 vCPU 上的 Redis / Valkey 与 100M 容量测试

[English](README.md) | **简体中文**

测试日期：2026-10-21（UTC）。全部 **84 个正式点**均在新建 Azure 测试环境重新测量。Lavik 的 100M 数据集包含 **95.37 GiB 逻辑 value 数据**，是 1M 数据量的 100 倍。

100M 数据集的 GET / SET 峰值为 **215,644 / 231,361 ops/s**，分别为小数据集峰值的 **100.0% / 96.6%**。这些峰值可能来自不同连接数，匹配连接数的结果见下表。 在 1M 基准中，GET 峰值最高的是 Valkey 9.1.2（245,955 ops/s），SET 峰值最高的是 Lavik 0.2.0 / 1M（239,405 ops/s）。

## 吞吐与 p99.9 延迟

<img src="combined.svg" alt="GET and SET throughput and p99.9 latency" width="800">

| Command | Product / keys | Workers / I/O threads | Connections | Peak ops/s | p99.9 ms |
| --- | --- | --- | --- | --- | --- |
| GET | Lavik 0.2.0 / 1M | 2 | 80 | 215,640 | 0.687 |
| GET | Lavik 0.2.0 / 100M | 2 | 640 | 215,644 | 5.535 |
| GET | Redis 8.10.2 | 2 | 160 | 209,958 | 1.311 |
| GET | Valkey 9.1.2 | 2 | 32 | 245,955 | 0.223 |
| SET | Lavik 0.2.0 / 1M | 2 | 320 | 239,405 | 2.527 |
| SET | Lavik 0.2.0 / 100M | 2 | 320 | 231,361 | 2.943 |
| SET | Redis 8.10.2 | 2 | 80 | 188,371 | 0.679 |
| SET | Valkey 9.1.2 | 2 | 80 | 214,117 | 0.511 |

每种命令为每个对比产品选择峰值最高的**一个固定 I/O 线程配置**，整条吞吐和延迟曲线均使用该配置，不逐点拼接。主图展示 16–640 连接，表格和完整配置图保留 1280 连接。

## 相同连接数对比

每格为 **ops/s / p99.9 ms**。

| Command | Connections | Lavik 0.2.0 / 1M | Lavik 0.2.0 / 100M | Redis 8.10.2 | Valkey 9.1.2 |
| --- | --- | --- | --- | --- | --- |
| GET | 16 | 105,536 / 0.343 | 105,464 / 0.223 | 124,422 / 0.167 | 172,900 / 0.167 |
| GET | 32 | 198,832 / 0.335 | 197,851 / 0.271 | 184,549 / 0.271 | 245,955 / 0.223 |
| GET | 80 | 215,640 / 0.687 | 215,643 / 0.639 | 205,073 / 0.567 | 225,845 / 0.543 |
| GET | 160 | 214,971 / 3.583 | 215,639 / 1.111 | 209,958 / 1.311 | 240,260 / 1.007 |
| GET | 320 | 215,632 / 2.303 | 215,644 / 2.159 | 208,124 / 2.831 | 230,637 / 6.367 |
| GET | 640 | 215,452 / 7.583 | 215,644 / 5.535 | 206,284 / 6.559 | 225,932 / 6.399 |
| GET | 1280 | 215,620 / 12.479 | 215,439 / 12.927 | 204,466 / 14.207 | 227,679 / 13.503 |
| SET | 16 | 148,361 / 0.271 | 157,101 / 0.183 | 139,237 / 0.247 | 169,978 / 0.167 |
| SET | 32 | 204,416 / 0.303 | 208,295 / 0.303 | 182,105 / 0.271 | 199,425 / 0.247 |
| SET | 80 | 229,009 / 0.639 | 220,759 / 0.655 | 188,371 / 0.679 | 214,117 / 0.511 |
| SET | 160 | 237,336 / 1.543 | 225,243 / 2.255 | 184,826 / 1.791 | 202,535 / 2.607 |
| SET | 320 | 239,405 / 2.527 | 231,361 / 2.943 | 178,853 / 3.887 | 194,044 / 2.959 |
| SET | 640 | 234,839 / 8.255 | 227,927 / 5.567 | 178,734 / 7.935 | 193,103 / 7.103 |
| SET | 1280 | 231,271 / 13.503 | 220,561 / 13.439 | 175,050 / 17.023 | 193,829 / 13.951 |

## 完整 I/O 线程扫描

| Product | I/O threads | GET peak ops/s (connections) | SET peak ops/s (connections) |
| --- | --- | --- | --- |
| Redis 8.10.2 | 1 | 189,749 (160) | 180,645 (320) |
| Redis 8.10.2 | 2 | 209,958 (160) | 188,371 (80) |
| Valkey 9.1.2 | 1 | 183,408 (32) | 180,624 (160) |
| Valkey 9.1.2 | 2 | 245,955 (32) | 214,117 (80) |

![All measured configurations](all-configurations.svg)

## 内存与客户端诊断

| Product / keys | INFO used_memory (MiB) | Process RSS (MiB) |
| --- | --- | --- |
| Lavik 0.2.0 / 1M | 96.0–96.0 | 739.2–748.6 |
| Lavik 0.2.0 / 100M | 5,069.4–5,069.4 | 5,513.4–5,521.7 |
| Redis 8.10.2 | 1,264.7–1,265.0 | 1,287.9–1,978.4 |
| Valkey 9.1.2 | 1,268.1–1,268.6 | 1,290.6–2,223.3 |

范围来自正式点后的快照。INFO 内存使用各产品自己的统计口径；RSS 为操作系统进程驻留内存。Lavik 另有 **2,048 MiB 大页预留**。客户端最大平均 CPU 使用为 **3.718 核**，单个客户端线程的最大平均使用为 **0.249 核**；测量包围窗口内最大服务端 CPU steal 为 **0.0000%**。

## 版本与硬件

| 项目 | 配置 |
| --- | --- |
| Server | Standard_L2aos_v5; 2 vCPU (1 physical core) / 16 GiB nominal RAM; AMD EPYC 9V45 96-Core Processor |
| Client | Standard_F16as_v7; separate VM; CPUs 0–15 |
| Region | East US, zone 3 |
| OS | Ubuntu 24.04; Linux 6.17.0-1022-azure on both hosts |
| Storage | 1 × 1,439,887,785,984-byte dedicated local NVMe; SPDK/VFIO |
| Network | Kernel TCP; same VNet and availability zone; accelerated networking |
| Lavik | Official standard x86_64 v0.2.0 package; 2908f86f2770e9c6e8acc6585b3bedb4b44d1cd5 |

## 测试方法

- key 为十进制 `1..N`，无前缀，value 为 1,024 字节，无 TTL。Lavik 1M 和 100M 分别独立装载；Redis、Valkey 各使用 1M。
- memtier 2.5.1 固定在客户端 16 个 CPU 上，pipeline 1，全 key 范围均匀随机 GET 或覆盖 SET。连接数为 16, 32, 80, 160, 320, 640, 1280；每点测一次，基准与 1 亿条组 30 秒。全部定时测量均使用独立客户端随机种子。
- 每个配置先预热 GET 10 秒，然后 GET、SET 各按连接数递增测量。所有产品在同一服务器串行运行，测量前已完成所有构建。
- Lavik 使用 2 个 worker 和独占 NVMe/SPDK。非默认设置为 `--tomb-raider-interval-ms=0`、`--spdk-max-completions-per-poll=16` 和 2 GiB EAL/大页内存。每个测量点前后检查 defrag 保持启用。
- Redis 8.10.2、Valkey 9.1.2 与旧报告版本一致，使用默认优化构建与 jemalloc，分别扫描 1, 2 个 I/O 线程。`save ""` 禁用定时快照，AOF 保持关闭；初次装载后手动 SAVE，各线程配置从相同 RDB 基线恢复。正式写入只进入内存。
- THP 为 `never`，overcommit 为 1，不添加 cgroup 内存上限。每个配置前后核验精确 DBSIZE 和五个 value 长度，并检查装载请求数、零 GET 未命中、连接错误、版本和 CPU 亲和性。

## 证据与测量限制

[完整逐点数据](results.csv) · [版本、硬件与校验元数据](provenance.json)

每点仅一次测量，不提供置信区间。不同产品的成功响应不代表相同的同步持久化保证。新机器的物理放置、网络延迟和运行时间均可能与旧报告不同。[原始日志与测量凭据](raw-results.tar.gz)包含逐文件 SHA-256 清单，归档哈希记录在元数据中。

## 复现命令

服务端使用上述 L2aos_v5，客户端使用同可用区的 F16as_v7；两端通过私网连接。准备 Lavik v0.2.0 标准 x86_64 二进制、Redis 8.10.2、Valkey 9.1.2 和 memtier 2.5.1。Redis / Valkey 使用默认优化构建、jemalloc 5.3.0，编译参数为 `make -j12 BUILD_TLS=yes`。以下命令使用 Bash。

### 启动服务端

```bash
# Run on the server; replace the address and binary paths for your hosts.
SERVER_IP=10.96.0.4
LAVIK_BIN=/opt/bench020/downloads/lavik-v0.2.0-linux-x86_64/lavik
REDIS_BIN=/opt/bench020/bin/redis-server
VALKEY_BIN=/opt/bench020/bin/valkey-server
ulimit -n 65535
sudo sysctl -w vm.overcommit_memory=1
printf 'never\n' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

Lavik 使用空的专用 NVMe namespace。启动前按 [SPDK 存储指南](../../docs/operations/spdk-storage.md) 识别测试盘并绑定 VFIO，本测试设置 `HUGEMEM=2048`、`BYCORF_DPDK_MEMORY_MB=2048`。该 VM 未暴露 IOMMU，按指南使用了专用测试机的 no-IOMMU 配置。分别加载 100 万条和 1 亿条；两次之间正常退出 Lavik、恢复 NVMe 驱动，再清空已确认的测试 namespace 并重新绑定，独立加载数据。

```bash
# Use the verified PCI address and namespace ID of your empty test NVMe.
NVME_BDF=YOUR_NVME_PCI_ADDRESS
NVME_NSID=1
sudo prlimit --memlock=unlimited:unlimited --nofile=65535:65535 \
  env BYCORF_EAL_ARGS="-a ${NVME_BDF}" BYCORF_DPDK_MEMORY_MB=2048 \
  taskset -c 0-1 "$LAVIK_BIN" \
  --storage=spdk --bind="$SERVER_IP" \
  --tomb-raider-interval-ms=0 --spdk-max-completions-per-poll=16 \
  --log-dir="$PWD/lavik-logs" \
  --data-file="spdk://${NVME_BDF}/${NVME_NSID}"
```

以上省略 v0.2.0 的默认参数；`taskset -c 0-1` 下默认启动两个绑定 CPU 的 worker。`BYCORF_DPDK_MEMORY_MB=2048` 设置 SPDK 使用的 EAL 内存为 2 GiB，网络仍为默认的内核 TCP。保留的调优项将 tombstone 扫描间隔从默认一天改为关闭、SPDK completion cap 从默认 8 改为 16。

Redis 和 Valkey 分别测试 1、2 个 I/O 线程，每次只运行一个服务端：

```bash
# Run one product/configuration at a time. Repeat with IO_THREADS=2.
IO_THREADS=1
IO_ARGS=(); if ((IO_THREADS > 1)); then IO_ARGS=(--io-threads "$IO_THREADS"); fi
mkdir -p "$PWD/bench-data/redis" "$PWD/bench-data/valkey"

# Redis 8.10.2
taskset -c 0-1 "$REDIS_BIN" \
  --bind "$SERVER_IP" --protected-mode no \
  --dir "$PWD/bench-data/redis" \
  --save "" \
  "${IO_ARGS[@]}"

# Valkey 9.1.2 (after stopping Redis)
taskset -c 0-1 "$VALKEY_BIN" \
  --bind "$SERVER_IP" --protected-mode no \
  --dir "$PWD/bench-data/valkey" \
  --save "" \
  "${IO_ARGS[@]}"
```

### 客户端灌数与测试

按 Lavik 100 万、Lavik 1 亿、Redis IO=1/2、Valkey IO=1/2 的顺序运行。切换配置时修改 `N` 和 `RUN`。每个产品首次灌数前确认 `DBSIZE=0`。Redis / Valkey 首次灌数后执行一次 `SAVE`，再用 `SHUTDOWN NOSAVE` 退出；每组 I/O 线程配置都从同一基线 RDB 重启，跳过再次灌数，重新检查数据后执行预热与扫描。Lavik 两组均从空的测试 namespace 独立灌数。

```bash
# Bash, on the separate client, CPUs 0–15; memtier_benchmark 2.5.1.
set -euo pipefail
SERVER_IP=10.96.0.4
N=1000000 # Use 100000000 for the large Lavik dataset.
RUN=lavik-1m # lavik-100m, redis-io1, redis-io2, valkey-io1, valkey-io2
mkdir -p "$RUN"
ulimit -n 65535
COMMON=(--server="$SERVER_IP" \
  --threads=16 --key-minimum=1 --key-maximum="$N" \
  --key-prefix= --data-size=1024 --hide-histogram \
  --print-percentiles=50,99,99.9,99.99 --show-config)

# Fill an empty dataset using disjoint sequential key streams: exactly N writes.
taskset -c 0-15 memtier_benchmark "${COMMON[@]}" --clients=10 \
  --requests="$((N / 160))" --ratio=1:0 --key-pattern=P:P \
  --json-out-file="$RUN/fill.json" > "$RUN/fill.txt" 2>&1

check_data() {
  test "$(redis-cli -h "$SERVER_IP" --raw DBSIZE)" = "$N"
  for key in 1 2 "$((N / 2))" "$((N - 1))" "$N"; do
    test "$(redis-cli -h "$SERVER_IP" --raw STRLEN "$key")" = 1024
  done
}
check_data

# For each memory peer only: create its baseline RDB once after the first fill.
# redis-cli -h "$SERVER_IP" SAVE

# Warmup, then GET and SET sweeps. Reuse this part after loading a baseline RDB.
taskset -c 0-15 memtier_benchmark "${COMMON[@]}" --clients=10 \
  --distinct-client-seed --test-time=10 \
  --command='GET __key__' --command-is-read \
  --json-out-file="$RUN/warmup.json" > "$RUN/warmup.txt" 2>&1

for kind in GET SET; do
  if [ "$kind" = GET ]; then
    COMMAND='GET __key__'; READ_FLAG=(--command-is-read)
  else
    COMMAND='SET __key__ __data__'; READ_FLAG=()
  fi
  for connections in 16 32 80 160 320 640 1280; do
    taskset -c 0-15 memtier_benchmark "${COMMON[@]}" \
      --clients="$((connections / 16))" --distinct-client-seed --test-time=30 \
      --command="$COMMAND" "${READ_FLAG[@]}" \
      --json-out-file="$RUN/${kind}-c${connections}.json" \
      > "$RUN/${kind}-c${connections}.txt" 2>&1
  done
done
check_data
```

每组结束后检查 GET 全部命中、连接错误为零，且 DBSIZE 保持不变。
