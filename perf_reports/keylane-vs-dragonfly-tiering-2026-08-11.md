# Keylane SPDK/io_uring、Dragonfly Tiered Storage 与 Apache Kvrocks 性能对比（2026-08-11）

## 测试结果

本次测试使用双 NVMe、2 亿条 1–4 KB 数据、80 个客户端连接和不限速 workload。Keylane SPDK 在三种 workload 下的 QPS 均最高；不需要 SPDK、RAID 或裸块设备的 Keylane io_uring 双文件方案仍明显高于 Dragonfly 和 Kvrocks。

| Workload | 系统 | QPS | p99 (ms) | p99.9 (ms) |
| --- | --- | ---: | ---: | ---: |
| 纯读 GET | Keylane SPDK | 310,459.04 | 0.463 | 1.295 |
| 纯读 GET | Keylane io_uring（双 XFS 文件） | 275,869.18 | 0.511 | 2.511 |
| 纯读 GET | Dragonfly Tiered Storage | 237,334.07 | 1.511 | 8.031 |
| 纯读 GET | Apache Kvrocks | 105,865.14 | 1.479 | 1.655 |
| 纯写 SET | Keylane SPDK | 410,003.11 | 1.023 | 1.823 |
| 纯写 SET | Keylane io_uring（双 XFS 文件） | 404,836.60 | 1.015 | 1.679 |
| 纯写 SET | Dragonfly Tiered Storage | 220,881.37 | 4.191 | 9.471 |
| 纯写 SET | Apache Kvrocks | 166,106.17 | 1.359 | 3.775 |
| 1:1 读写混合 | Keylane SPDK | 349,069.27 | 0.655 | 1.655 |
| 1:1 读写混合 | Keylane io_uring（双 XFS 文件） | 320,834.33 | 0.831 | 1.975 |
| 1:1 读写混合 | Dragonfly Tiered Storage | 217,717.09 | 3.599 | 9.279 |
| 1:1 读写混合 | Apache Kvrocks | 52,569.09 | 3.711 | 5.439 |

SPDK 相比 io_uring 双文件方案的 QPS 分别高 12.54%（纯读）、1.28%（纯写）和 8.80%（1:1）。纯写时 io_uring 的 p99.9 反而低 7.90%；纯读和混合时 SPDK 的 p99.9 分别低 48.43% 和 16.20%。这说明 SPDK 的主要收益集中在随机读和读写并发路径，而不是顺序批量写入。

## 测试环境

| 角色 | Azure 机型 | 地址 |
| --- | --- | --- |
| Server | `Standard_L16s_v3` | `10.0.0.4:6379` |
| Client | `Standard_L16s_v3` | `10.0.0.5` |

公共 workload：8 个 memtier threads、每个 thread 10 个连接、1,000–4,000 byte 随机 value、key 范围 `kv_1`–`kv_200000000`、每组 300 秒、不限制 QPS。四个服务/后端在不同时段独占同一个端口运行，不并发运行。

Keylane 两个后端都使用 16 workers、暂停 defrag、关闭 tomb-raider。SPDK 直接访问两个 NVMe namespace；io_uring 让两块 NVMe 各自使用独立 XFS，并通过两个 1,600 GiB 预分配 regular files 执行 4 KiB 对齐的 O_DIRECT I/O，不使用 RAID。Dragonfly 使用 v1.40.1、16 proactor threads、双 NVMe Linux RAID0、XFS，并关闭 experimental cooling。Kvrocks 使用 v2.16.0、16 workers、同一个 RAID0/XFS、80 GiB block cache、BlobDB，关闭压缩，并在全量灌数后的 compaction 完成且 block cache 预热满以后计时。

## 结果边界与公平性说明

- Keylane 不使用 LSM-tree，没有 RocksDB compaction；本组 Keylane 测试暂停了自身 defrag。
- Keylane io_uring 使用 regular files，但存储文件以 O_DIRECT 打开，不依赖 Linux page cache。两块盘没有组成 RAID；Keylane 自己把两个文件识别为独立设备并各分配 8 个 home workers。
- 当前代码把 `--registered-buffer-mb=256` 解释为每个 worker 256 MiB；16 workers 合计约 4 GiB，而不是全进程 256 MiB。SPDK 和 io_uring 两组使用相同设置，因此后端对比一致，但部署容量规划必须按 per-worker 语义计算。
- io_uring 全量灌数后直接运行正式测试，没有挑选短窗口。纯读和混合期间两块盘都约 100% util，且 I/O 量对称；结果包含 XFS、Linux block layer、NVMe 内核驱动和中断路径的成本，因此比 SPDK 更接近普通 Linux 文件部署。
- Dragonfly 的 `backing_file_direct=false` 使用 Linux buffered I/O，每组测试前清理 Linux page cache，因此结果不代表其 O_DIRECT 模式或 warm page-cache 模式。
- Kvrocks 的正式测试是刻意隔离 compaction 的 best-case：先等待灌数触发的 compaction 完成，再动态关闭 auto compaction。三组正式测试期间 `num_running_compactions=0`。
- Kvrocks 的 80 GiB HCC block cache 在正式计时前已预热到 `85,899,049,296` bytes。数据目录当时约 552 GiB，因此 cache 已满不代表整个数据集都在内存中；随机读仍会发生真实块设备读取。
- Kvrocks 纯写结束时 metadata L0 文件数为 268；混合测试结束时为 310，`estimate_pending_compaction_bytes[metadata]` 约 7.04 GB。也就是说，报告中的 Kvrocks 写入成绩推迟了不可避免的在线 compaction 成本，不代表可以长期维持的稳态吞吐。
- Kvrocks 保留 WAL，但设置 `rocksdb.write_options.sync=no`；进程崩溃可依赖 WAL 恢复，但机器掉电可能丢失尚未同步的最近写入。Blob GC 关闭后，覆盖写产生的旧 blob 不会在测试期间回收，数据目录在三组测试后增长到约 687 GiB。

## 复现步骤

### 1. 启动 Keylane SPDK

```bash
sudo ./bld-spdk/keylane \
  --bind=10.0.0.4 \
  --port=6379 \
  --metrics-port=9100 \
  --threads=16 \
  --recv-buffers=1024 \
  --registered-buffer-mb=256 \
  --busy-poll-us=20 \
  --background-budget-us=10 \
  --background-warrant-percent=1 \
  --spdk-max-completions-per-poll=8 \
  --spdk-foreground-pre-poll-us=5 \
  --mimalloc-purge-delay-ms=60000 \
  --data-file=spdk://69f9:00:00.0/1 \
  --data-file=spdk://021d:00:00.0/1 \
  --flush-max-ms=1000 \
  --flush-size-kb=128 \
  --disable-read-crc \
  --tomb-raider-interval-ms=0 \
  --defrag-paused \
  --defrag-max-active-per-device=1 \
  --defrag-sleep-ms=100
```

### 2. 编译并启动 Keylane io_uring 双文件版本

普通 io_uring 构建显式关闭 SPDK。下面的两盘初始化命令会清除目标设备的现有文件系统和数据；必须先按实际机器确认设备名，且不能包含系统盘。

```bash
cmake -S . -B bld-iouring-files -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF \
  -DKEYLANE_ENABLE_OPT=ON \
  -DKEYLANE_WITH_SPDK=OFF
cmake --build bld-iouring-files -j 16

sudo wipefs -a /dev/nvme0n1
sudo wipefs -a /dev/nvme1n1
sudo mkfs.xfs -f -L keylane0 /dev/nvme0n1
sudo mkfs.xfs -f -L keylane1 /dev/nvme1n1

sudo mkdir -p /mnt/data0 /mnt/data1
sudo mount -o noatime /dev/nvme0n1 /mnt/data0
sudo mount -o noatime /dev/nvme1n1 /mnt/data1
sudo chown "$(id -un):$(id -gn)" /mnt/data0 /mnt/data1

fallocate -l 1600G /mnt/data0/keylane.data
fallocate -l 1600G /mnt/data1/keylane.data
```

`1,600 GiB` 是本机实验值，不是 Keylane 固定要求。部署时应按实际磁盘容量预留文件系统日志和运维空间；每个新文件必须是 8 MiB 的整数倍。Keylane 不会在启动时创建、扩展或 truncate 文件。

```bash
sudo systemd-run \
  --unit=keylane-iouring-files.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitNOFILE=1048576 \
  /path/to/bld-iouring-files/keylane \
  --bind=10.0.0.4 \
  --port=6379 \
  --metrics-port=9100 \
  --threads=16 \
  --pin-workers \
  --recv-buffers=1024 \
  --registered-buffer-mb=256 \
  --busy-poll-us=20 \
  --background-budget-us=10 \
  --background-warrant-percent=1 \
  --mimalloc-purge-delay-ms=60000 \
  --data-file=/mnt/data0/keylane.data \
  --data-file=/mnt/data1/keylane.data \
  --flush-max-ms=1000 \
  --flush-size-kb=128 \
  --disable-read-crc \
  --tomb-raider-interval-ms=0 \
  --tomb-raider-sleep-ms=10 \
  --defrag-paused \
  --defrag-max-active-per-device=1 \
  --defrag-sleep-ms=100
```

本次启动日志确认每个文件容量为 1,717,986,918,400 bytes、各有 204,799 个 data blocks，两个设备分别分配 8 个 home workers，direct-I/O alignment 为 4,096 bytes。

### 3. 创建 RAID0 和 XFS

Dragonfly 和 Kvrocks 在不同时段复用这个文件系统。以下命令会清空 `/dev/nvme0n1` 和 `/dev/nvme1n1`；执行前必须按实际机器重新确认设备名，且不能包含系统盘。

```bash
sudo wipefs -a /dev/nvme0n1
sudo wipefs -a /dev/nvme1n1

sudo mdadm --create /dev/md/storage-raid0 \
  --level=0 \
  --raid-devices=2 \
  --chunk=512 \
  /dev/nvme0n1 /dev/nvme1n1

sudo mkfs.xfs -f -d su=512k,sw=2 /dev/md/storage-raid0
sudo mkdir -p /mnt/data
sudo mount -o noatime /dev/md/storage-raid0 /mnt/data
```

本次实际阵列为 RAID0、512 KiB chunk，总容量 3.49 TiB，挂载点为 `/mnt/data`。

### 4. 启动 Dragonfly Tiered Storage

测试版本：`dragonfly v1.40.1-434478e00c366c711985d0b3269023fc39db8ad1`。直接使用官方 GitHub Release 的 x86-64 二进制，二进制 SHA-256 为 `1d2b6654f4488ebc3f6cd5061199158880f6d957534705cd548a909108507b8b`，不使用容器运行时。

```bash
curl -fL \
  https://github.com/dragonflydb/dragonfly/releases/download/v1.40.1/dragonfly-x86_64.tar.gz \
  -o /tmp/dragonfly-x86_64.tar.gz
tar -xzf /tmp/dragonfly-x86_64.tar.gz -C /tmp
mv /tmp/dragonfly-x86_64 /tmp/dragonfly-v1.40.1
echo "1d2b6654f4488ebc3f6cd5061199158880f6d957534705cd548a909108507b8b  /tmp/dragonfly-v1.40.1" \
  | sha256sum --check

sudo mkdir -p /mnt/data/dragonfly
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/dragonfly

sudo systemd-run \
  --unit=dragonfly-tiered.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitMEMLOCK=infinity \
  /tmp/dragonfly-v1.40.1 \
  --logtostderr \
  --bind=10.0.0.4 \
  --port=6379 \
  --proactor_threads=16 \
  --proactor_affinity_mode=on \
  --maxmemory=64GB \
  --dir=/mnt/data/dragonfly \
  --dbfilename= \
  --tiered_prefix=/mnt/data/dragonfly/tier \
  --tiering_disk_storage_initial_size=40GB \
  --backing_file_direct=false \
  --tiered_offload_threshold=1.0 \
  --tiered_experimental_cooling=false \
  --tiered_max_pending_stash_bytes=16MB \
  --primary_port_http_enabled=false \
  --version_check=false
```

### 5. 编译并启动 Apache Kvrocks

测试版本：`kvrocks version 2.16.0 (commit 28440b5)`。下面是本次使用的完整配置；其中会影响性能或持久性语义的设置全部保留，避免只公布成绩而隐藏调优条件。

```bash
git clone --branch v2.16.0 --depth 1 https://github.com/apache/kvrocks.git
cd kvrocks
./x.py build --ninja -j16

sudo mkdir -p /mnt/data/kvrocks
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/kvrocks
```

`kvrocks-perf.conf`：

```text
bind 10.0.0.4
port 6379
workers 16
daemonize no
timeout 0
tcp-backlog 8192
maxclients 10000
db-name kvrocks-perf
dir /mnt/data/kvrocks
log-dir stdout:warning
log-level warning
log-retention-days -1
slowlog-log-slower-than -1
slowlog-max-len 0
persist-cluster-nodes-enabled no
max-io-mb 0

enable-blob-cache yes
rocksdb.block_cache_size 81920
rocksdb.block_cache_type hcc
rocksdb.max_open_files -1
rocksdb.write_buffer_size 256
rocksdb.target_file_size_base 512
rocksdb.max_write_buffer_number 8
rocksdb.min_write_buffer_number_to_merge 2
rocksdb.max_background_jobs 16
rocksdb.max_subcompactions 4
rocksdb.wal_compression no
rocksdb.max_total_wal_size 8192
rocksdb.wal_ttl_seconds 3600
rocksdb.wal_size_limit_mb 65536
rocksdb.block_size 16384
rocksdb.cache_index_and_filter_blocks yes
rocksdb.compression no
rocksdb.compression_start_level 0
rocksdb.compaction_readahead_size 2097152
rocksdb.enable_pipelined_write yes
rocksdb.level0_file_num_compaction_trigger 16
rocksdb.level0_slowdown_writes_trigger 128
rocksdb.level0_stop_writes_trigger 256
rocksdb.disable_auto_compactions no
rocksdb.enable_blob_files yes
rocksdb.min_blob_size 1000
rocksdb.blob_file_size 1073741824
rocksdb.enable_blob_garbage_collection no
rocksdb.level_compaction_dynamic_level_bytes no
rocksdb.max_bytes_for_level_base 68719476736
rocksdb.max_bytes_for_level_multiplier 10
rocksdb.read_options.async_io yes
rocksdb.write_options.sync no
rocksdb.write_options.disable_wal no
rocksdb.write_options.no_slowdown no
rocksdb.rate_limiter_auto_tuned no
rocksdb.partition_filters yes
```

启动时把 16 个 server workers 限定在 server 的 CPU 0–15：

```bash
sudo systemd-run \
  --unit=kvrocks-perf.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitNOFILE=1048576 \
  /path/to/kvrocks/build/kvrocks \
  -c /path/to/kvrocks-perf.conf
```

这些 Kvrocks 调优项的目的和代价如下：

| 设置 | 目的 | 代价或边界 |
| --- | --- | --- |
| 80 GiB HCC block cache、cache index/filter、16 KiB block | 提高读缓存命中率，减少块读取 | 占用 80 GiB server 内存 |
| BlobDB、1,000-byte threshold、1 GiB blob file、blob cache | 大 value 与 LSM metadata 分离，减少写放大 | 读取需要 metadata + blob 路径；空间回收依赖 Blob GC |
| Blob GC 关闭 | 避免测试期间 GC 抢占 I/O | 覆盖写产生的旧 blob 不回收，磁盘持续增长 |
| SST/WAL/blob 压缩关闭 | 降低 CPU 消耗 | 增加设备容量和写带宽需求 |
| 256 MiB write buffer、最多 8 个、最少 2 个合并 | 扩大写缓冲并减少小 flush | 增加内存占用和故障恢复工作量 |
| 16 background jobs、4 subcompactions、2 MiB compaction readahead | 加快灌数后的 compaction | 在线 compaction 时会更积极争用 CPU/I/O |
| L1 base 64 GiB、multiplier 10、关闭 dynamic level bytes | 推迟大规模 level compaction | 增加 L0/L1 和后续 compaction debt |
| pipelined write、async read I/O、I/O 不限速 | 提高并行度和吞吐 | 峰值时更容易打满设备 |
| WAL 开启、per-write sync 关闭 | 保留进程崩溃恢复并减少 fsync 延迟 | 机器掉电可能丢失最近写入 |
| 16 workers、`max_open_files=-1` | 使用全部 server CPU 并避免反复打开文件 | 增加线程和文件描述符资源占用 |

### 6. 全量灌入 2 亿条数据

清空对应服务后，从 client 使用 640 个连接完成全量 SET：

```bash
redis-cli -h 10.0.0.4 -p 6379 FLUSHALL

taskset -c 0-15 memtier_benchmark \
  -t 16 -c 40 \
  -s 10.0.0.4 -p 6379 \
  -n allkeys \
  --distinct-client-seed \
  --ratio=1:0 \
  --key-pattern=P:P \
  --key-prefix="kv_" \
  --key-minimum=1 \
  --key-maximum=200000000 \
  --random-data \
  --data-size-range=1000-4000 \
  --data-size-pattern=R \
  --hide-histogram
```

Keylane io_uring 双文件版本本次灌数完成 200,000,000 次 SET，用时 354.674 秒，平均 584,416.51 QPS；Kvrocks 用时 1,847.693 秒，平均 113,040.98 QPS。灌数吞吐只用于确认复现过程，不计入上面的正式对比表。

### 7. 等待 Kvrocks compaction 完成并预热 block cache

灌数完成后保持 auto compaction 开启，持续检查 RocksDB 状态。只有 `num_running_compactions=0`、所有 `estimate_pending_compaction_bytes=0`，并确认不会立刻调度下一轮任务后才继续。本次 metadata 文件从 L0/L1=`92/5` 收敛到 `0/12`，`compaction_count=1`。

```bash
redis-cli -h 10.0.0.4 -p 6379 INFO rocksdb \
  | grep -E 'num_files_at_level|estimate_pending_compaction_bytes|num_running_compactions|compaction_count'
```

随后关闭 auto compaction，并提高 L0 写入门槛，防止 300 秒纯写和混合测试因人为积累的 L0 文件停写。Kvrocks v2.16.0 对这两个门槛允许的最大值均为 1024：

```bash
redis-cli -h 10.0.0.4 -p 6379 CONFIG SET rocksdb.disable_auto_compactions yes
redis-cli -h 10.0.0.4 -p 6379 CONFIG SET rocksdb.level0_slowdown_writes_trigger 1024
redis-cli -h 10.0.0.4 -p 6379 CONFIG SET rocksdb.level0_stop_writes_trigger 1024
```

在 client 运行随机 GET 预热。预热输出不计入成绩；本次在 `block_cache_usage` 达到 85,899,049,296 bytes 且保持稳定后，用 `Ctrl+C` 停止预热，再静置并确认 compaction 仍为 0：

```bash
taskset -c 0-15 memtier_benchmark \
  -t 16 -c 40 \
  -s 10.0.0.4 -p 6379 \
  --test-time 3600 \
  --distinct-client-seed \
  --ratio=0:1 \
  --key-prefix="kv_" \
  --key-minimum=1 \
  --key-maximum=200000000 \
  --random-data \
  --data-size-range=1000-4000 \
  --data-size-pattern=R \
  --hide-histogram \
  --print-percentiles="99,99.9" \
  --randomize

redis-cli -h 10.0.0.4 -p 6379 INFO rocksdb \
  | grep -E 'block_cache_usage|num_running_compactions|num_background_errors'
```

### 8. 依次执行三组正式测试

`RATIO` 依次替换为纯读 `0:1`、纯写 `1:0` 和 1:1 混合 `1:1`。每组结束后确认 block cache 仍为满容量、`num_running_compactions=0` 且没有 background error。

Dragonfly 每组测试前在 server 执行 `sync` 并清理 Linux page cache；不清空数据库。Keylane SPDK、使用 O_DIRECT regular files 的 Keylane io_uring，以及已经预热的 Kvrocks 不执行 `drop_caches`。

```bash
# 仅 Dragonfly：每组测试前在 Server 执行
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches

# 在 Client 执行
taskset -c 0-15 memtier_benchmark \
  -t 8 -c 10 \
  -s 10.0.0.4 -p 6379 \
  --test-time 300 \
  --distinct-client-seed \
  --ratio=RATIO \
  --key-prefix="kv_" \
  --key-minimum=1 \
  --key-maximum=200000000 \
  --random-data \
  --data-size-range=1000-4000 \
  --data-size-pattern=R \
  --hide-histogram \
  --print-percentiles="99,99.9" \
  --randomize
```

Kvrocks 测试结束后应恢复正常门槛并重新开启 auto compaction，让测试期间积累的 L0 文件和 blob 空间进入后台整理：

```bash
redis-cli -h 10.0.0.4 -p 6379 CONFIG SET rocksdb.level0_slowdown_writes_trigger 128
redis-cli -h 10.0.0.4 -p 6379 CONFIG SET rocksdb.level0_stop_writes_trigger 256
redis-cli -h 10.0.0.4 -p 6379 CONFIG SET rocksdb.disable_auto_compactions no
```
