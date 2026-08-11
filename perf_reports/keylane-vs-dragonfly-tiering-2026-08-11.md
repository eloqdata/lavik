# Keylane SPDK/io_uring、Dragonfly、Garnet、Apache Kvrocks 与 Pika 性能对比（2026-08-11）

## 测试结果

本次测试使用双 NVMe、2 亿条 1–4 KB 数据、80 个客户端连接和不限速 workload。Keylane SPDK 的纯读和混合 QPS 最高，io_uring 双裸块设备的纯写 QPS 最高；Microsoft Garnet 是三个非 Keylane 系统中纯写和混合 QPS 最高的一组，纯读 QPS 略低于 Dragonfly，但 p99.9 明显更低。

| Workload | 系统 | QPS | p99 (ms) | p99.9 (ms) |
| --- | --- | ---: | ---: | ---: |
| 纯读 GET | Keylane SPDK | 310,459.04 | 0.463 | 1.295 |
| 纯读 GET | Keylane io_uring（双裸块设备） | 278,223.05 | 0.511 | 2.399 |
| 纯读 GET | Keylane io_uring（双 XFS 文件） | 275,869.18 | 0.511 | 2.511 |
| 纯读 GET | Dragonfly Tiered Storage（待 warm-cache 重测） | 237,334.07 | 1.511 | 8.031 |
| 纯读 GET | Microsoft Garnet Storage Tier | 229,277.75 | 2.143 | 2.623 |
| 纯读 GET | Apache Kvrocks | 105,865.14 | 1.479 | 1.655 |
| 纯读 GET | Pika | 96,994.53 | 1.647 | 3.791 |
| 纯写 SET | Keylane io_uring（双裸块设备） | 418,140.27 | 0.991 | 1.647 |
| 纯写 SET | Keylane SPDK | 410,003.11 | 1.023 | 1.823 |
| 纯写 SET | Keylane io_uring（双 XFS 文件） | 404,836.60 | 1.015 | 1.679 |
| 纯写 SET | Microsoft Garnet Storage Tier | 381,280.31 | 1.167 | 1.463 |
| 纯写 SET | Dragonfly Tiered Storage（待 warm-cache 重测） | 220,881.37 | 4.191 | 9.471 |
| 纯写 SET | Apache Kvrocks | 166,106.17 | 1.359 | 3.775 |
| 纯写 SET | Pika | 156,673.29 | 1.455 | 2.239 |
| 1:1 读写混合 | Keylane SPDK | 349,069.27 | 0.655 | 1.655 |
| 1:1 读写混合 | Keylane io_uring（双裸块设备） | 330,866.27 | 0.799 | 1.855 |
| 1:1 读写混合 | Keylane io_uring（双 XFS 文件） | 320,834.33 | 0.831 | 1.975 |
| 1:1 读写混合 | Microsoft Garnet Storage Tier | 286,026.66 | 1.759 | 2.575 |
| 1:1 读写混合 | Dragonfly Tiered Storage（待 warm-cache 重测） | 217,717.09 | 3.599 | 9.279 |
| 1:1 读写混合 | Apache Kvrocks | 52,569.09 | 3.711 | 5.439 |
| 1:1 读写混合 | Pika | 51,035.84 | 3.839 | 5.247 |

io_uring 双裸块设备相比双 XFS 文件的 QPS 分别高 0.85%（纯读）、3.29%（纯写）和 3.13%（1:1），p99.9 分别低 4.46%、1.91% 和 6.08%。绕过 XFS 有稳定但不大的收益；双文件方案保留了大部分性能，同时更容易按普通 Linux 文件方式部署。

SPDK 相比 io_uring 双裸块设备的纯读和混合 QPS 分别高 11.59% 和 5.50%，但 raw io_uring 的纯写 QPS 高 1.98%、纯写 p99.9 低 9.65%。SPDK 的主要收益仍集中在随机读和读写并发路径，而不是顺序批量写入。

Garnet 的纯读 QPS 比 Dragonfly 低 3.39%，但纯读 p99.9 低 67.34%；纯写和混合 QPS 分别比 Dragonfly 高 72.62% 和 31.38%，p99.9 分别低 84.55% 和 72.25%。与最快的 Keylane 后端相比，Garnet 的纯读、纯写和混合 QPS 分别低 26.15%、8.82% 和 18.06%；Garnet 的纯写 p99.9 为 1.463 ms，是本表所有系统中最低值，但其纯读和混合 p99.9 仍高于三个 Keylane 后端。

## 测试环境

| 角色 | Azure 机型 | 地址 |
| --- | --- | --- |
| Server | `Standard_L16s_v3` | `10.0.0.4:6379` |
| Client | `Standard_L16s_v3` | `10.0.0.5` |

公共 workload：8 个 memtier threads、每个 thread 10 个连接、1,000–4,000 byte 随机 value、key 范围 `kv_1`–`kv_200000000`、每组 300 秒、不限制 QPS。七组服务/后端在不同时段独占同一个端口运行，不并发运行。

Keylane 三个存储后端都使用 16 workers、暂停 defrag、关闭 tomb-raider。SPDK 直接访问两个 NVMe namespace；raw io_uring 通过 Linux NVMe 驱动直接访问两个块设备，direct-I/O alignment 为 512 bytes；file io_uring 让两块 NVMe 各自使用独立 XFS，并通过两个 1,600 GiB 预分配 regular files 执行 4 KiB 对齐的 O_DIRECT I/O。两种 io_uring 方案都不使用 RAID。Dragonfly 使用 v1.40.1、16 proactor threads、双 NVMe Linux RAID0、XFS，并关闭 experimental cooling。Garnet 使用 v2.1.3、.NET 10.0.302、同一个 RAID0/XFS、64 GiB hybrid-log memory、32 GiB read cache、4 GiB index 和 Linux Native libaio。Kvrocks 使用 v2.16.0、16 workers、同一个 RAID0/XFS、80 GiB block cache、BlobDB，并关闭压缩。Pika 使用 Git tag v4.0.3、16 network threads、32 request threads、3 个 RocksDB instances、共 24 GiB block cache 和 32 GiB RTC cache，并关闭压缩与 binlog。

## 结果边界与公平性说明

- Keylane 不使用 LSM-tree，没有 RocksDB compaction；本组 Keylane 测试暂停了自身 defrag。
- Keylane io_uring 的 raw 和 regular-file 两组使用同一个二进制和服务参数。regular files 以 O_DIRECT 打开，不依赖 Linux page cache；raw 组绕过 XFS，但仍经过 Linux block layer 和 NVMe 内核驱动。两块盘均未组成 RAID，Keylane 自己把两个路径识别为独立设备并各分配 8 个 home workers。
- 当前代码把 `--registered-buffer-mb=256` 解释为每个 worker 256 MiB；16 workers 合计约 4 GiB，而不是全进程 256 MiB。SPDK 和 io_uring 两组使用相同设置，因此后端对比一致，但部署容量规划必须按 per-worker 语义计算。
- 两组 io_uring 都在各自全量灌数后直接运行正式测试，没有挑选短窗口。纯读和混合期间两块盘都约 100% util，且 I/O 量对称。raw 组将平均读请求从文件版约 6.6 KiB 降至约 3.1 KiB，但总随机读仍受两盘合计约 28.2 万 IOPS 限制，因此纯读 QPS 只提高 0.85%。
- raw io_uring 需要独占块设备，部署和运维约束接近 SPDK；regular-file io_uring 包含 XFS 成本，但更接近普通 Linux 文件部署。两者都保留 Linux NVMe 驱动、中断和内核块层成本。
- Dragonfly 的 `backing_file_direct=false` 使用 Linux buffered I/O。正常运行会保留 Linux page cache，因此新复现口径在灌数后先预热、正式测试之间不清 page cache。表内当前 Dragonfly 数字来自此前 cold-cache 流程，已明确标记为待重测，不能当作 warm-cache 结果。
- Garnet 使用官方 v2.1.3 Release 源码直接发布二进制，不使用容器。只测试 raw string `GET`/`SET`，因此关闭 object store 和 pub/sub；4 GiB index 按官方每 key 约 16 bytes 的规则覆盖 2 亿 key，避免默认 128 MiB index 产生长 hash chain。
- Garnet storage tier 使用 Linux Native libaio、4 个 completion threads、每设备 512 个最大 in-flight I/O、8 KiB initial record read，并保留默认开启的 scatter-gather GET。64 GiB hybrid log 和 32 GiB read cache 加上 index 后，正式测试时进程 RSS 约 101 GiB；这是一组偏向最高性能的配置，不代表低内存部署。
- Garnet 正式测试前用 640 个连接做了 180 秒随机 GET 预热；read cache 达到完整 32 GiB，48,617,210 次预热 GET 全部命中，预热成绩不计入表格。正式纯读的 68,783,514 次 GET，以及混合测试中的全部 GET 也都是 0 miss。
- Garnet 关闭 AOF、checkpoint 和 compaction，因此表中不包含同步持久化或旧版本回收成本。storage-tier hybrid log 本身不是可重启恢复的数据副本；测试期间纯写和混合覆盖产生的旧版本没有回收，log 目录从灌数后的约 414 GiB 增长到约 767 GiB。该配置适合隔离在线数据路径的上限，不代表可以无限期维持的磁盘稳态。
- Garnet v2.1.3 在 `--no-obj` 模式下执行 `DBSIZE` 会在全库扫描路径触发 `NullReferenceException` 并关闭该管理连接。它没有影响灌数和 GET/SET 会话；本次改用恰好 200,000,000 次成功 SET、`INFO store` 的 506,254,709,784-byte log tail，以及后续随机 GET 全部 0 miss 交叉校验数据完整性。
- Kvrocks 从全量灌数开始关闭自动 compaction，但保留正常 memtable flush；L0 compaction、slowdown 和 stop 阈值都设为该版本上限 1024。该配置用于隔离前台请求路径，生产环境通常需要在线 compaction，其资源开销可能影响吞吐和延迟。
- Kvrocks 的 80 GiB HCC block cache 在正式计时前预热到稳定容量；cache 已满不表示整个数据集都在内存中，随机读仍会访问块设备。
- Kvrocks 关闭 WAL、per-write sync、压缩和 Blob GC。WAL 关闭会改变故障恢复语义；正式测试结果只代表这组明确配置下的数据路径性能。
- Pika 使用官方 v4.0.3 tag（commit `d16db1eee9aadb1db42338269936deb7b584ddcc`）直接编译 Release 二进制，不使用容器；该 commit 的二进制版本字符串仍显示 4.0.2，因此同时记录 tag、commit 和自报版本，避免版本歧义。
- Pika 的 3 个 RocksDB instances 各配置 8 GiB shared block cache，合计 24 GiB；RTC cache 配置 32 GiB。正式计时前执行随机 GET 预热，256.023 秒完成约 2550 万次 GET，平均 99,633.56 QPS 且 0 miss，预热成绩不计入表格。
- Pika 关闭 RocksDB WAL/binlog 和压缩。灌数与后台整理完成后关闭自动 compaction，再进行正式测试；该配置用于隔离前台请求路径。生产环境通常需要在线 compaction，其资源开销可能影响吞吐和延迟。

## 复现步骤

### 1. 启动 Keylane SPDK

```bash
sudo systemd-run \
  --unit=keylane-spdk.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitMEMLOCK=infinity \
  --property=LimitNOFILE=infinity \
  /path/to/bld-spdk/keylane \
  --bind=10.0.0.4 \
  --data-file=spdk://69f9:00:00.0/1 \
  --data-file=spdk://021d:00:00.0/1
```

### 2. 编译并启动 Keylane io_uring

普通 io_uring 构建显式关闭 SPDK。raw 和 regular-file 两种存储方式共用同一个二进制：

```bash
cmake -S . -B bld-iouring-files -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF \
  -DKEYLANE_ENABLE_OPT=ON \
  -DKEYLANE_WITH_SPDK=OFF
cmake --build bld-iouring-files -j 16
```

#### 双 XFS regular files

下面的两盘初始化命令会清除目标设备的现有文件系统和数据；必须先按实际机器确认设备名，且不能包含系统盘。

```bash
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
  --property=LimitMEMLOCK=infinity \
  --property=LimitNOFILE=infinity \
  /path/to/bld-iouring-files/keylane \
  --bind=10.0.0.4 \
  --data-file=/mnt/data0/keylane.data \
  --data-file=/mnt/data1/keylane.data
```

本次启动日志确认每个文件容量为 1,717,986,918,400 bytes、各有 204,799 个 data blocks，两个设备分别分配 8 个 home workers，direct-I/O alignment 为 4,096 bytes。

#### 双 raw block devices

raw 版本需要卸载文件系统并独占设备。以下操作会使原文件系统和 Keylane 文件数据不可访问；`wipefs` 加前 8 MiB zeroout 用于建立新的 Keylane metadata/bitmap，不是全盘安全擦除，旧数据块可能仍物理存在但不会进入新存储集。

```bash
sudo systemctl kill -s SIGINT keylane-iouring-files.service
sudo umount /mnt/data0
sudo umount /mnt/data1

sudo wipefs -a /dev/nvme0n1
sudo wipefs -a /dev/nvme1n1
sudo blkdiscard --zeroout --force \
  --offset 0 --length 8388608 /dev/nvme0n1
sudo blkdiscard --zeroout --force \
  --offset 0 --length 8388608 /dev/nvme1n1

sudo systemd-run \
  --unit=keylane-iouring-block.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitMEMLOCK=infinity \
  --property=LimitNOFILE=infinity \
  /path/to/bld-iouring-files/keylane \
  --bind=10.0.0.4 \
  --data-file=/dev/nvme0n1 \
  --data-file=/dev/nvme1n1
```

本次每个 NVMe 的原始容量为 1,920,383,410,176 bytes；Keylane 使用其中 1,920,378,863,616 bytes，忽略不足一个 8 MiB block 的尾部。每盘有 228,926 个 data blocks、分配 8 个 home workers，direct-I/O alignment 为 512 bytes。

### 3. 创建 RAID0 和 XFS

Dragonfly、Garnet 和 Kvrocks 在不同时段复用这个文件系统。以下命令会清空 `/dev/nvme0n1` 和 `/dev/nvme1n1`；执行前必须按实际机器重新确认设备名，且不能包含系统盘。

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

测试版本：`dragonfly v1.40.1-434478e00c366c711985d0b3269023fc39db8ad1`。直接使用官方 GitHub Release 的 x86-64 二进制，二进制 SHA-256 为 `1d2b6654f4488ebc3f6cd5061199158880f6d957534705cd548a909108507b8b`，不使用容器运行时。版本检查保持 Dragonfly 默认开启。

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
  --primary_port_http_enabled=false
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
rocksdb.write_buffer_size 512
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
rocksdb.level0_file_num_compaction_trigger 1024
rocksdb.level0_slowdown_writes_trigger 1024
rocksdb.level0_stop_writes_trigger 1024
rocksdb.disable_auto_compactions yes
rocksdb.enable_blob_files yes
rocksdb.min_blob_size 1000
rocksdb.blob_file_size 1073741824
rocksdb.enable_blob_garbage_collection no
rocksdb.level_compaction_dynamic_level_bytes no
rocksdb.max_bytes_for_level_base 68719476736
rocksdb.max_bytes_for_level_multiplier 10
rocksdb.read_options.async_io yes
rocksdb.write_options.sync no
rocksdb.write_options.disable_wal yes
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
  --property=LimitMEMLOCK=infinity \
  --property=LimitNOFILE=infinity \
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
| 512 MiB write buffer、最多 8 个、最少 2 个合并 | 扩大写缓冲并减少 L0 flush 文件数 | 增加内存占用 |
| auto compaction 关闭、三个 L0 阈值设为上限 1024 | 灌数、预热和正式窗口只允许 flush，不调度自动 compaction | 长期运行需要重新开启后台整理 |
| 16 background jobs、4 subcompactions、2 MiB compaction readahead | 为恢复常规 compaction 后保留并行能力 | 在线 compaction 会占用 CPU/I/O |
| L1 base 64 GiB、multiplier 10、关闭 dynamic level bytes | 控制常规 level compaction 的容量布局 | 需要按实际数据量调整 |
| pipelined write、async read I/O、I/O 不限速 | 提高并行度和吞吐 | 峰值时更容易打满设备 |
| WAL 和 per-write sync 均关闭 | 隔离数据写入路径并提高写吞吐 | 进程或机器故障可能丢失尚未 flush 的数据 |
| 16 workers、`max_open_files=-1` | 使用全部 server CPU 并避免反复打开文件 | 增加线程和文件描述符资源占用 |

### 6. 编译并启动 Pika

测试源码为官方 `v4.0.3` tag（commit `d16db1eee9aadb1db42338269936deb7b584ddcc`）；该 commit 编译出的 `pika -v` 显示 `pika_version: 4.0.2`。下面使用源码 Release 二进制直接运行，不使用容器：

```bash
git clone --branch v4.0.3 --depth 1 https://github.com/OpenAtomFoundation/pika.git
cd pika
cmake -S . -B output -DCMAKE_BUILD_TYPE=Release
cmake --build output -j 16

sudo mkdir -p /mnt/data/pika/{db,log,dump,dbsync}
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/pika
```

在发行版 `conf/pika.conf` 上设置以下参数；未列出的项目保持 v4.0.3 默认值：

```text
port : 6379
db-instance-num : 3
thread-num : 16
rtc-cache-read : yes
thread-pool-size : 32
log-path : /mnt/data/pika/log/
db-path : /mnt/data/pika/db/
dump-path : /mnt/data/pika/dump/
db-sync-path : /mnt/data/pika/dbsync/
pidfile : /mnt/data/pika/pika.pid

write-buffer-size : 256M
max-write-buffer-size : 8G
max-write-buffer-num : 2
min-write-buffer-number-to-merge : 1
max-subcompactions : 4
max-background-jobs : 12
max-background-flushes : 4
max-background-compactions : 8
compression : none
write-binlog : no

block-cache : 8G
num-shard-bits : 6
share-block-cache : yes
enable-partitioned-index-filters : yes
cache-index-and-filter-blocks : yes
pin_l0_filter_and_index_blocks_in_cache : yes
optimize-filters-for-hits : yes
level-compaction-dynamic-level-bytes : yes

cache-num : 16
cache-model : 1
cache-type : string, set, zset, list, hash, bit
cache-maxmemory : 34359738368
cache-maxmemory-policy : 1
disable_auto_compactions : false
```

`block-cache: 8G` 对每个 RocksDB instance 生效，3 个 instances 合计 24 GiB；`share-block-cache: yes` 是在单个 instance 内由 column families 共享。RTC cache 为 32 GiB。WAL/binlog 和压缩均关闭，属于偏向最高性能的配置，持久性与默认配置不同。

```bash
sudo systemd-run \
  --unit=pika-perf.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitMEMLOCK=infinity \
  --property=LimitNOFILE=infinity \
  /path/to/pika/output/pika \
  -c /path/to/pika/conf/pika.conf
```

全量灌数和后台整理结束后，在正式预热前动态关闭自动 compaction：

```bash
redis-cli -h 10.0.0.4 -p 6379 CONFIG SET disable_auto_compactions true
```

### 7. 编译并启动 Microsoft Garnet

测试版本：[`Garnet 2.1.3`](https://github.com/microsoft/garnet/releases/tag/v2.1.3)，tag/commit 为 `v2.1.3` / `b4bf6275351dad3202467d88814a9aee793286c9`。直接发布并运行 Linux x64 二进制，不使用容器。内存和索引容量依据官方 [memory sizing](https://microsoft.github.io/garnet/docs/getting-started/memory) 说明，storage tier、Native I/O 和 read cache 参数见官方 [configuration reference](https://microsoft.github.io/garnet/docs/getting-started/configuration)。该版本要求 .NET SDK 10.0.302：

```bash
git clone --depth 1 --branch v2.1.3 \
  https://github.com/microsoft/garnet.git
cd garnet

curl -fsSL https://dot.net/v1/dotnet-install.sh \
  -o /tmp/garnet-dotnet-install.sh
bash /tmp/garnet-dotnet-install.sh \
  --version 10.0.302 \
  --install-dir /opt/dotnet-garnet

sudo apt-get install -y libaio-dev liburing2 patchelf

/opt/dotnet-garnet/dotnet publish \
  main/GarnetServer/GarnetServer.csproj \
  -c Release \
  -f net10.0 \
  -r linux-x64 \
  --self-contained false \
  -o /opt/garnet-2.1.3
```

Ubuntu 24.04 的 libaio runtime SONAME 是 `libaio.so.1t64`，而官方预编译 native-device library 引用 `libaio.so.1`。本次只改 ELF dependency 名称，不改 Garnet 代码；其他发行版如果 `ldd` 没有显示 `libaio.so.1 => not found`，不需要执行：

```bash
patchelf --replace-needed libaio.so.1 libaio.so.1t64 \
  /opt/garnet-2.1.3/runtimes/linux-x64/native/libnative_device.so
patchelf --replace-needed libaio.so.1 libaio.so.1t64 \
  /opt/garnet-2.1.3/runtimes/linux-x64/native/libnative_device_libaio.so
```

下面配置只提供 storage-tier cache-store 语义，不启用 AOF 或 checkpoint recovery。hybrid log 会把内存中放不下的页写到 `hlog.*` segment，但这些文件本身不等于可在进程重启后恢复的数据副本。测试期间关闭 compaction，避免后台回收干扰 300 秒窗口；长期运行必须重新评估 compaction、AOF/checkpoint、磁盘容量和延迟之间的取舍。

```bash
sudo mkdir -p /mnt/data/garnet/log /mnt/data/garnet/checkpoints
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/garnet

sudo systemd-run \
  --unit=garnet-perf.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitNOFILE=1048576 \
  --setenv=DOTNET_ROOT=/opt/dotnet-garnet \
  --setenv=DOTNET_CLI_TELEMETRY_OPTOUT=1 \
  /opt/garnet-2.1.3/GarnetServer \
  --bind 10.0.0.4 \
  --protected-mode false \
  --memory 64g \
  --page 4m \
  --segment 1g \
  --index 4g \
  --index-max-size 4g \
  --storage-tier \
  --logdir /mnt/data/garnet/log \
  --checkpointdir /mnt/data/garnet/checkpoints \
  --readcache \
  --readcache-memory 32g \
  --readcache-page 4m \
  --no-obj \
  --no-pubsub \
  --device-type Native \
  --device-io-backend Libaio \
  --device-completion-threads 4 \
  --device-throttle-limit 512 \
  --initial-io-record-size 8k \
  --compaction-freq 0 \
  --compaction-type None \
  --network-connection-limit 10000 \
  --minthreads 16 \
  --miniothreads 16 \
  --logger-level Warning
```

### 8. 全量灌入 2 亿条数据

只在确认目标是允许清空的空白测试实例后执行一次 `FLUSHALL`，再从 client 使用 640 个连接完成全量 SET。`FLUSHALL` 会删除全库，已经灌完数据后不得再次执行：

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

灌数阶段只用于构造相同的 2 亿条初始数据，不记录耗时或吞吐，也不计入正式对比结果。

### 9. 预热 Dragonfly、Garnet 与 Pika cache

Dragonfly 全量灌数后先用随机 GET 预热 Linux page cache，随后三组正式测试之间不清 page cache、不重启。Garnet 用同一命令预热到 `ReadCache.AllocatedPageCount=8192`、`ReadCache.CurrentMemorySizeBytes=34359738368`。预热输出不计入正式结果；本次 Garnet 预热完成 48,617,210 次 GET，全部命中。

```bash
taskset -c 0-15 memtier_benchmark \
  -t 16 -c 40 \
  -s 10.0.0.4 -p 6379 \
  --test-time 180 \
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

redis-cli -h 10.0.0.4 -p 6379 INFO store \
  | grep -E 'Log\.(Head|Flushed|Tail)|ReadCache\.(Allocated|Current)'
```

不要用 Garnet v2.1.3 的 `DBSIZE` 验证这组 `--no-obj` 数据；该组合存在前述管理命令异常。应同时确认全量灌数恰好完成 200,000,000 次 SET、预热 GET 为 0 miss，并保存 `INFO store` 地址用于审计。

Pika 使用相同的随机 GET 预热命令。本次预热运行 256.023 秒，完成约 2550 万次 GET，平均 99,633.56 QPS，全部命中；预热结果不计入正式成绩。

### 10. 验证 Kvrocks no-compaction 状态并预热 block cache

Kvrocks 从空库启动前已经在配置文件中关闭 auto compaction，并把 L0 compaction、slowdown 和 stop 阈值都设为 1024。flush 正常生成 L0 SST，但灌数、预热和三组正式测试期间不调度自动 compaction。灌数过程中持续确认 `num_running_compactions=0`、没有 background error，且 L0 文件数没有达到 1024：

```bash
redis-cli -h 10.0.0.4 -p 6379 INFO rocksdb \
  | grep -E 'num_files_at_level|estimate_pending_compaction_bytes|num_running_compactions|compaction_count'
```

在 client 运行随机 GET 预热。预热输出不计入成绩；在 `block_cache_usage` 达到稳定容量后用 `Ctrl+C` 停止预热，再确认 compaction 仍为 0：

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

### 11. 依次执行三组正式测试

`RATIO` 依次替换为纯读 `0:1`、纯写 `1:0` 和 1:1 混合 `1:1`。每组结束后确认 block cache 仍为满容量、`num_running_compactions=0` 且没有 background error。

所有系统在三组正式测试之间都不清理操作系统 page cache。Dragonfly 保留预热后的 Linux page cache；Keylane SPDK、raw io_uring、使用 O_DIRECT regular files 的 io_uring、使用 Native O_DIRECT storage tier 的 Garnet，以及已经预热的 Kvrocks 本身不依赖该 page-cache 路径。

Garnet 三组顺序为 GET、SET、1:1，全部在同一个进程和数据集上执行，不重启、不清库。纯写和混合会继续追加 hybrid-log 旧版本；本次三组完成后 `Log.TailAddress=884989925872`，log 目录约 767 GiB。Garnet 正式测试中的 GET 全部为 hit。

```bash
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

如果测试实例随后转为长期运行，应按生产 workload 重新选择 compaction 策略。下面仅示范恢复本次调优前的 L0 门槛并开启 auto compaction：

```bash
redis-cli -h 10.0.0.4 -p 6379 CONFIG SET rocksdb.level0_file_num_compaction_trigger 16
redis-cli -h 10.0.0.4 -p 6379 CONFIG SET rocksdb.level0_slowdown_writes_trigger 128
redis-cli -h 10.0.0.4 -p 6379 CONFIG SET rocksdb.level0_stop_writes_trigger 256
redis-cli -h 10.0.0.4 -p 6379 CONFIG SET rocksdb.disable_auto_compactions no
```
