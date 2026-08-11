# Keylane SPDK 与 Dragonfly Tiered Storage 性能对比（2026-08-11）

## 测试结果

在本次双 NVMe、2 亿条 1–4 KB 数据、80 个客户端连接和不限速测试中，Keylane 的 QPS 和尾延迟在三种 workload 下均优于 Dragonfly Tiered Storage。

| Workload | 系统 | QPS | p99 (ms) | p99.9 (ms) |
| --- | --- | ---: | ---: | ---: |
| 纯读 GET | Keylane SPDK | 310,459.04 | 0.463 | 1.295 |
| 纯读 GET | Dragonfly Tiered Storage | 237,334.07 | 1.511 | 8.031 |
| 纯写 SET | Keylane SPDK | 410,003.11 | 1.023 | 1.823 |
| 纯写 SET | Dragonfly Tiered Storage | 220,881.37 | 4.191 | 9.471 |
| 1:1 读写混合 | Keylane SPDK | 349,069.27 | 0.655 | 1.655 |
| 1:1 读写混合 | Dragonfly Tiered Storage | 217,717.09 | 3.599 | 9.279 |

## 测试环境

| 角色 | Azure 机型 | 地址 |
| --- | --- | --- |
| Server | `Standard_L16s_v3` | `10.0.0.4:6379` |
| Client | `Standard_L16s_v3` | `10.0.0.5` |

公共 workload：8 个 memtier threads、每个 thread 10 个连接、1,000–4,000 byte 随机 value、key 范围 `kv_1`–`kv_200000000`、每组 300 秒、不限制 QPS。

Keylane 使用 16 workers、双 NVMe SPDK、暂停 defrag、关闭 tomb-raider。Dragonfly 使用 v1.40.1、16 proactor threads、双 NVMe Linux RAID0、XFS，并关闭 experimental cooling。Dragonfly 的 `backing_file_direct=false` 使用 Linux buffered I/O；因此这组结果不代表其 O_DIRECT 模式。

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

### 2. 创建 Dragonfly RAID0 和 XFS

以下命令会清空 `/dev/nvme0n1` 和 `/dev/nvme1n1`；执行前必须按实际机器重新确认设备名，且不能包含系统盘。

```bash
sudo wipefs -a /dev/nvme0n1
sudo wipefs -a /dev/nvme1n1

sudo mdadm --create /dev/md/dragonfly-raid0 \
  --level=0 \
  --raid-devices=2 \
  --chunk=512 \
  /dev/nvme0n1 /dev/nvme1n1

sudo mkfs.xfs -f -d su=512k,sw=2 /dev/md/dragonfly-raid0
sudo mkdir -p /mnt/data
sudo mount -o noatime /dev/md/dragonfly-raid0 /mnt/data
sudo mkdir -p /mnt/data/dragonfly
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/dragonfly
```

本次实际阵列为 RAID0、512 KiB chunk，总容量 3.49 TiB，挂载点为 `/mnt/data`。

### 3. 启动 Dragonfly Tiered Storage

测试版本：`dragonfly v1.40.1-434478e00c366c711985d0b3269023fc39db8ad1`。直接使用官方 GitHub Release 的 x86-64 二进制，二进制 SHA-256 为 `1d2b6654f4488ebc3f6cd5061199158880f6d957534705cd548a909108507b8b`，不使用容器运行时。

```bash
curl -fL \
  https://github.com/dragonflydb/dragonfly/releases/download/v1.40.1/dragonfly-x86_64.tar.gz \
  -o /tmp/dragonfly-x86_64.tar.gz
tar -xzf /tmp/dragonfly-x86_64.tar.gz -C /tmp
mv /tmp/dragonfly-x86_64 /tmp/dragonfly-v1.40.1
echo "1d2b6654f4488ebc3f6cd5061199158880f6d957534705cd548a909108507b8b  /tmp/dragonfly-v1.40.1" \
  | sha256sum --check

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

### 4. 全量灌入 2 亿条数据

先清空数据库，再从客户端 `10.0.0.5` 使用 640 个连接完成全量 SET：

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

redis-cli -h 10.0.0.4 -p 6379 DBSIZE
```

只有 `DBSIZE` 返回 `200000000` 后才开始正式测试。

### 5. 依次执行三组正式测试

Dragonfly 每组测试前在服务端执行 `sync` 并清理 Linux page cache；不清空数据库。`RATIO` 依次替换为纯读 `0:1`、纯写 `1:0` 和 1:1 混合 `1:1`。

```bash
# Dragonfly 每组测试前在 Server 执行
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
