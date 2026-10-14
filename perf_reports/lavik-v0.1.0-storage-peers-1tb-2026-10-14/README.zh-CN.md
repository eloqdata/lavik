# Lavik v0.1.0：1TB 数据与 Redis 兼容存储系统对比

[English](README.md) | **简体中文**

测试日期：2026 年 10 月 13–14 日。所有系统在 Azure Standard_L16aos_v4 上存储 **10 亿条 key，每条 value 为 1 KiB（1TB）**。使用独立客户端，分别测量纯 GET 和覆盖 SET，连接数为 **80、160、320、640、1280**。

## 吞吐与 p99.9 延迟

<img src="combined.svg" alt="五档连接数下 GET、SET 的吞吐与 p99.9 延迟" width="800">

横轴为总连接数，采用 log₂ 刻度；上排为 QPS，下排为客户端观测的 p99.9 延迟，单位毫秒。80 连接时，Lavik 的 GET 为 385,514 QPS、p99.9 为 0.439 ms，SET 为 425,907 QPS、p99.9 为 2.655 ms。完整扫描中，Lavik 的 GET 峰值最高（1,069,126 QPS），Garnet 的 SET 峰值最高（900,794 QPS）。

## 环境与方法

| 项目 | 配置 |
|---|---|
| 服务端 | Azure Standard_L16aos_v4，日本西部 zone 2；16 vCPU，标称 128 GiB 内存 |
| 客户端 | 独立 Azure Standard_F32als_v6；memtier 限定在 CPU 0–15 |
| 系统 | 两台均为 Ubuntu 24.04、Linux 6.17.0-1022-azure |
| 网络 | 私有 VNet，客户端 → 10.84.1.4:6379，kernel TCP、加速网卡 |
| 存储 | 六块约 1.92 TB 的本地 NVMe；Lavik 使用 SPDK，其余系统使用 RAID0/XFS、noatime |
| 客户端负载 | memtier 2.5.1，16 线程；每连接一个未完成请求，均匀随机访问，独立客户端随机种子 |
| 数据集 | 十进制 key 1…1,000,000,000，无前缀，value 为 1,024 B；key 和存储格式开销另计 |
| 测量 | 每点 60 秒，先 GET 后 SET，各组内按连接数递增 |

每个系统独立灌入数据。Dragonfly 与 Garnet 在灌入后，用八个客户端线程、80 连接 GET 预热 180 秒；其余系统灌入并抽查样本后开始扫描。各产品在同一台服务端依次运行，限定在 CPU 0–15。每个点测量一次。

Lavik defrag 保持开启；Garnet 使用 Lookup compaction；Kvrocks、Pika、Tendis 的 RocksDB 自动 compaction 保持开启。各产品内存预算不同：Dragonfly 限制为 96 GiB；Garnet 为 64 GiB log、32 GiB read cache、16 GiB index；Kvrocks 为 80 GiB block cache；Pika 为 24 GiB block cache 和 32 GiB 应用缓存；Tendis 使用共享 72 GiB 缓存。这些为配置预算，不是实测总常驻内存。

各产品的持久化语义不同：Lavik 使用 SPDK 存储引擎；Dragonfly 使用 tiered 文件且无周期性快照；Garnet 使用存储日志且未启用 AOF；Kvrocks、Tendis 关闭 WAL，Pika 关闭 binlog。成功响应不代表相同的同步持久化保证。所有正式 GET 均无未命中，全部测试点连接错误为零；各产品灌入阶段均完成十亿次 SET，并在扫描前后抽查 value 长度。

## 版本

| 系统 | 测试版本 |
|---|---|
| Lavik | [v0.1.0](https://github.com/eloqdata/lavik/releases/tag/v0.1.0)，标准 x86_64 包 |
| Dragonfly | [v2.0.2](https://github.com/dragonflydb/dragonfly/releases/tag/v2.0.2) |
| Garnet | [v2.2.1](https://github.com/microsoft/garnet/releases/tag/v2.2.1) |
| Apache Kvrocks | [v2.17.0](https://github.com/apache/kvrocks/releases/tag/v2.17.0) |
| Pika / PikiwiDB | [v3.5.7](https://github.com/OpenAtomFoundation/pikiwidb/releases/tag/v3.5.7) |
| Tendis | [2.8.4-rocksdb-v8.5.3](https://github.com/Tencent/Tendis/releases/tag/2.8.4-rocksdb-v8.5.3) |

Garnet 使用 .NET 10.0.12；Kvrocks 使用 GCC 13 从发布 tag 编译。Pika 使用官方 v3.5.7 Ubuntu 包；包内版本字符串为 3.5.6，revision 为 `62a57f85534198e0d1033c32f1215c958bef8a5e`。

## 运行命令

以下使用 Bash，按实际环境替换地址和安装路径。安装上述发布包及 memtier 2.5.1。每次只运行一个数据库，启动命令保持前台运行，干净停止后再启动下一个。文件系统步骤只使用经过核验的六块空临时盘。Lavik 独立灌入前，按 [SPDK 存储指南](../../docs/operations/spdk-storage.md)准备专用设备和 8 GiB hugepages。

配置块仅包含相对测试版本配置模板的覆盖项。分别保存为注释指定的 `.overrides` 文件，再执行覆盖脚本；各系统使用同一套客户端负载。

### Storage and paths

```bash
# Filesystem peers: six verified, empty local NVMe devices, kernel driver bound.
# These names are placeholders: replace each with a verified scratch device.
DEVICES=(/dev/TEST_NVME1 /dev/TEST_NVME2 /dev/TEST_NVME3
         /dev/TEST_NVME4 /dev/TEST_NVME5 /dev/TEST_NVME6)
sudo mdadm --create /dev/md/bench --level=0 --raid-devices=6 "${DEVICES[@]}"
sudo mkfs.xfs /dev/md/bench
sudo mkdir -p /mnt/bench
sudo mount -o noatime /dev/md/bench /mnt/bench
sudo mkdir -p /mnt/bench/{dragonfly,garnet,kvrocks,pika/{db,log,dump,dbsync},tendis/{db,log,dump}}
sudo chown -R "$(id -u):$(id -g)" /mnt/bench
ulimit -n 65535
```

### Lavik SPDK

```bash
# Prepare the six verified SPDK namespaces using the SPDK storage guide.
# Use the actual PCI addresses after binding the empty scratch devices to VFIO.
BDFS=(2401:00:00.0 221c:00:00.0 7f10:00:00.0 399c:00:00.0 c0ef:00:00.0 6319:00:00.0)
FILES=(); EAL=''
for bdf in "${BDFS[@]}"; do
  FILES+=(--data-file="spdk://${bdf}/1")
  EAL+=" -a ${bdf}"
done
sudo env BYCORF_EAL_ARGS="$EAL" BYCORF_DPDK_MEMORY_MB=8192 \
  taskset -c 0-15 /opt/lavik-v0.1.0-linux-x86_64/lavik \
  --storage=spdk --bind=10.84.1.4 --shutdown-checkpoint \
  --spdk-max-completions-per-poll=16 --log-dir=/mnt/dev/bench-lavik-logs \
  "${FILES[@]}"
```

### Dragonfly 2.0.2

```bash
taskset -c 0-15 /opt/dragonfly/dragonfly-x86_64 \
  --logtostderr --bind=10.84.1.4 --proactor_threads=16 --maxmemory=96GB \
  --dir=/mnt/bench/dragonfly --dbfilename= \
  --tiered_prefix=/mnt/bench/dragonfly/tier \
  --tiering_disk_storage_initial_size=64GB --tiered_max_file_size=2TB \
  --backing_file_direct=false --tiered_offload_threshold=1.0 \
  --tiered_experimental_cooling=false --tiered_max_pending_stash_bytes=16MB
```

### Garnet 2.2.1

```bash
cd /opt/garnet/net10.0
export DOTNET_ROOT=/opt/dotnet
export LD_LIBRARY_PATH="$PWD${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
# The release archive places its native device library at the package root.
mkdir -p runtimes/linux-x64/native
ln -sf ../../../libnative_device.so runtimes/linux-x64/native/libnative_device.so
ln -sf /lib/x86_64-linux-gnu/libaio.so.1t64 libaio.so.1

taskset -c 0-15 ./GarnetServer \
  --bind 10.84.1.4 --protected-mode false --memory 64g --page 4m \
  --index 16g --index-max-size 16g --storage-tier \
  --logdir /mnt/bench/garnet/log --checkpointdir /mnt/bench/garnet/checkpoints \
  --readcache --readcache-memory 32g --no-obj --no-pubsub \
  --device-type Native --device-io-backend Libaio --device-throttle-limit 512 \
  --initial-io-record-size 8k --compaction-freq 300 --compaction-type Lookup \
  --compaction-force-delete --minthreads 16 --miniothreads 16
```

### Kvrocks

```conf
# kvrocks.overrides
bind 10.84.1.4
port 6379
workers 16
tcp-backlog 8192
db-name kvrocks-perf
dir /mnt/bench/kvrocks
log-dir stdout:warning
log-level warning
slowlog-log-slower-than -1
slowlog-max-len 0
persist-cluster-nodes-enabled no
enable-blob-cache yes
rocksdb.block_cache_size 81920
rocksdb.max_open_files -1
rocksdb.write_buffer_size 512
rocksdb.target_file_size_base 512
rocksdb.max_write_buffer_number 8
rocksdb.min_write_buffer_number_to_merge 2
rocksdb.max_background_jobs 16
rocksdb.max_subcompactions 4
rocksdb.max_total_wal_size 8192
rocksdb.wal_ttl_seconds 3600
rocksdb.wal_size_limit_mb 65536
rocksdb.compression no
rocksdb.compression_start_level 0
rocksdb.enable_pipelined_write yes
rocksdb.level0_file_num_compaction_trigger 16
rocksdb.level0_slowdown_writes_trigger 128
rocksdb.level0_stop_writes_trigger 256
rocksdb.enable_blob_files yes
rocksdb.min_blob_size 1000
rocksdb.blob_file_size 1073741824
rocksdb.level_compaction_dynamic_level_bytes no
rocksdb.max_bytes_for_level_base 68719476736
rocksdb.write_options.disable_wal yes
rocksdb.rate_limiter_auto_tuned no
```

### Pika

```conf
# pika.overrides
port : 6379
thread-num : 16
thread-pool-size : 32
log-path : /mnt/bench/pika/log/
db-path : /mnt/bench/pika/db/
dump-path : /mnt/bench/pika/dump/
pidfile : /mnt/bench/pika/pika.pid
db-sync-path : /mnt/bench/pika/dbsync/
max-subcompactions : 4
write-binlog : no
max-write-buffer-size : 8G
compression : none
max-background-jobs : 12
max-background-flushes : 4
max-background-compactions : 8
block-cache : 24G
num-shard-bits : 6
share-block-cache : yes
cache-index-and-filter-blocks : yes
pin_l0_filter_and_index_blocks_in_cache : yes
optimize-filters-for-hits : yes
cache-maxmemory : 34359738368
```

### Tendis

```conf
# tendis.overrides
bind 10.84.1.4
port 6379
daemon off
logLevel warning
logdir /mnt/bench/tendis/log
dumpdir /mnt/bench/tendis/dump
dir /mnt/bench/tendis/db
pidfile /mnt/bench/tendis/tendisplus.pid
slowlog /mnt/bench/tendis/log/slowlog
tcp-backlog 8192
netIoThreadNum 4
executorThreadNum 16
executorWorkPoolSize 4
binlog-enabled no
binlog-save-logs no
rocks.blockcachemb 73728
rocks.blockcache_num_shard_bits 8
rocks.blobcache_in_blockcache yes
rocks.disable_wal yes
rocks.compress_type none
rocks.write_buffer_size 268435456
rocks.target_file_size_base 536870912
rocks.max_bytes_for_level_base 68719476736
rocks.level0_file_num_compaction_trigger 16
rocks.level0_slowdown_writes_trigger 128
rocks.level0_stop_writes_trigger 256
rocks.max_background_jobs 16
rocks.max_subcompactions 4
rocks.compaction_readahead_size 2097152
rocks.enable_pipelined_write 1
rocks.cache_index_and_filter_blocks 1
rocks.pin_l0_filter_and_index_blocks_in_cache 1
rocks.partition_filters 1
rocks.enable_blob_files 1
rocks.min_blob_size 1000
rocks.blob_file_size 1073741824
rocks.enable_blob_garbage_collection 1
rocks.blob_garbage_collection_force_threshold 0.5
```

```bash
# Overlay the changes above on the tested release's configuration template.
# Paths: /opt/kvrocks/kvrocks.conf, /opt/pika/conf/pika.conf, /opt/tendis/tendisplus.conf.
python3 - <<'CONFIG'
from pathlib import Path
import re
for product, template in [('kvrocks', '/opt/kvrocks/kvrocks.conf'),
                          ('pika', '/opt/pika/conf/pika.conf'),
                          ('tendis', '/opt/tendis/tendisplus.conf')]:
    separator = r'\s*:\s*' if product == 'pika' else r'\s+'
    changes = {}
    for line in Path(product + '.overrides').read_text().splitlines():
        if line.strip() and not line.lstrip().startswith('#'):
            key = re.split(separator, line.strip(), maxsplit=1)[0].lower()
            changes[key] = line
    lines = []
    for line in Path(template).read_text().splitlines():
        stripped = line.strip()
        key = re.split(separator, stripped, maxsplit=1)[0].lower() if stripped else ''
        if stripped.startswith('#') or key not in changes:
            lines.append(line)
    Path(product + '.conf').write_text('\n'.join(lines + list(changes.values())) + '\n')
CONFIG

# Start only the selected product, with an empty data directory.
taskset -c 0-15 /opt/kvrocks/build/kvrocks -c kvrocks.conf
taskset -c 0-15 /opt/pika/bin/pika -c pika.conf
taskset -c 0-15 /opt/tendis/bin/tendisplus tendis.conf
```

### Client: load, warm up and measure

```bash
# Run on the separate client. Use a new OUT directory for each server.
SERVER=10.84.1.4
PRODUCT=dragonfly  # lavik, dragonfly, garnet, kvrocks, pika, tendis
OUT="results/$PRODUCT"
mkdir -p "$OUT"
ulimit -n 65535
COMMON=(--server="$SERVER" --threads=16 --key-minimum=1
        --key-maximum=1000000000 --key-prefix= --data-size=1024)

taskset -c 0-15 memtier_benchmark "${COMMON[@]}" --clients=40 \
  --requests=1562500 --ratio=1:0 --key-pattern=P:P \
  --json-out-file="$OUT/fill.json"

# Check sampled value lengths; do not scan the full keyspace.
for key in 1 2 500000000 999999999 1000000000; do
  test "$(redis-cli -h "$SERVER" --raw STRLEN "$key")" = 1024
done

if [[ "$PRODUCT" == dragonfly || "$PRODUCT" == garnet ]]; then
  taskset -c 0-15 memtier_benchmark "${COMMON[@]}" --threads=8 --clients=10 \
    --test-time=180 --distinct-client-seed --command='GET __key__' \
    --command-is-read --json-out-file="$OUT/warmup.json"
fi

for op in GET SET; do
  if [[ "$op" == GET ]]; then
    COMMAND=(--command='GET __key__' --command-is-read)
  else
    COMMAND=(--command='SET __key__ __data__')
  fi
  for connections in 80 160 320 640 1280; do
    taskset -c 0-15 memtier_benchmark "${COMMON[@]}" \
      --clients="$((connections / 16))" --test-time=60 --distinct-client-seed \
      "${COMMAND[@]}" --json-out-file="$OUT/$op-c$connections.json"
  done
done
```
