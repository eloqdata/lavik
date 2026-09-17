<!--
Copyright (C) 2026 EloqData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Keylane SPDK/io_uring vs. Dragonfly, Garnet, Apache Kvrocks, Pika, Tendis, and KeyDB On Flash (August 11, 2026)

**English** | [简体中文](README.zh-CN.md) | [All reports](../README.md)

> Historical benchmark of Keylane, the project now named Lavik. Product names,
> versions, commands, and measurements describe the original test; this is not a
> measurement of the current Lavik release.
> Restored from the [2026-09-15 archive](https://github.com/eloqdata/lavik/tree/eedb3080d808769519d93e971195b53b38b09c1e/perf_reports).

> August 12, 2026 update: all three Keylane backends, Dragonfly Tiered Storage,
> Microsoft Garnet Storage Tier, Apache Kvrocks, Pika, Tendis, and KeyDB On
> Flash were retested with a uniform five-minute protocol. The table now uses
> only these results. Each backend was loaded once and then ran read-only, 1:1
> mixed read/write, and write-only workloads in that order. Keylane kept
> defragmentation enabled; the other systems kept their configured reclamation
> or automatic compaction enabled.

> August 12, 2026 addendum: an independent capacity and performance test of an
> Azure Managed Redis 480 GB / 16 vCPU instance was added. It used about 400 GB
> of data and 60-second windows. Its rows carry `*`, and the differences from
> the local dual-NVMe, 300-second protocol are documented below the table.

## Results

Except for the independent Azure Managed Redis rows marked `*`, this test used
two NVMe devices, 200 million keys with uniformly random 1–4 KB values, 80
client connections, and unlimited-rate workloads. Keylane SPDK had the highest
read-only and mixed QPS; raw io_uring had the highest write-only QPS. All three
Keylane backends formed the highest-throughput group in every workload.

| Workload | System | QPS | p99 (ms) | p99.9 (ms) |
| --- | --- | ---: | ---: | ---: |
| Read-only GET | Keylane SPDK | 310,387.22 | 0.455 | 0.895 |
| Read-only GET | Keylane io_uring (two raw block devices) | 278,924.72 | 0.503 | 2.319 |
| Read-only GET | Keylane io_uring (two XFS files) | 272,726.70 | 0.519 | 2.319 |
| Read-only GET | Microsoft Garnet Storage Tier | 205,297.49 | 2.303 | 4.223 |
| Read-only GET | Dragonfly Tiered Storage | 187,652.86 | 2.911 | 16.383 |
| Read-only GET | Pika | 86,669.28 | 1.775 | 2.383 |
| Read-only GET | Apache Kvrocks | 70,671.82 | 2.479 | 3.407 |
| Read-only GET | Tendis | 68,860.36 | 2.063 | 5.855 |
| Read-only GET | Azure Managed Redis* | 44,480.15 | 5.855 | 11.839 |
| Read-only GET | KeyDB On Flash | 6,146.18 | 18.047 | 25.471 |
| Write-only SET | Keylane io_uring (two raw block devices) | 393,732.53 | 1.015 | 1.647 |
| Write-only SET | Keylane SPDK | 392,283.87 | 0.999 | 1.607 |
| Write-only SET | Keylane io_uring (two XFS files) | 385,324.26 | 1.055 | 1.727 |
| Write-only SET | Microsoft Garnet Storage Tier | 361,960.06 | 1.583 | 3.391 |
| Write-only SET | Dragonfly Tiered Storage | 199,233.52 | 4.639 | 10.303 |
| Write-only SET | Tendis | 160,641.92 | 1.335 | 2.127 |
| Write-only SET | Apache Kvrocks | 110,166.99 | 1.759 | 2.671 |
| Write-only SET | Azure Managed Redis* | 84,863.93 | 3.199 | 7.839 |
| Write-only SET | Pika | 79,537.31 | 4.223 | 7.327 |
| Write-only SET | KeyDB On Flash | 5,395.49 | 24.447 | 31.359 |
| 1:1 read/write | Keylane SPDK | 352,442.49 | 0.631 | 1.447 |
| 1:1 read/write | Keylane io_uring (two raw block devices) | 328,831.03 | 0.655 | 1.447 |
| 1:1 read/write | Keylane io_uring (two XFS files) | 326,110.00 | 0.687 | 1.503 |
| 1:1 read/write | Microsoft Garnet Storage Tier | 209,366.38 | 2.511 | 4.895 |
| 1:1 read/write | Dragonfly Tiered Storage | 207,247.53 | 4.015 | 9.791 |
| 1:1 read/write | Tendis | 102,212.78 | 1.439 | 1.975 |
| 1:1 read/write | Apache Kvrocks | 88,084.47 | 2.207 | 4.927 |
| 1:1 read/write | Pika | 75,324.05 | 3.615 | 6.527 |
| 1:1 read/write | Azure Managed Redis* | 55,351.99 | 5.311 | 11.839 |
| 1:1 read/write | KeyDB On Flash | 5,197.16 | 29.311 | 39.167 |

\* Azure Managed Redis used a 480 GB / 16 vCPU managed instance, 154,088,000
keys containing about 400 GB of data, and 60-second windows through a Private
Endpoint. Every other row used local dual NVMe, 200 million keys, and 300-second
windows. The Azure result provides an observed order-of-magnitude comparison,
not a fully controlled ranking. Capacity, networking, and reproduction details
appear in the dedicated section below.

Raw-device io_uring exceeded the two-XFS-file configuration by 2.27% on GET,
2.18% on SET, and 0.83% on the 1:1 workload. GET p99.9 was identical; raw-device
p99.9 was 4.63% lower on SET and 3.73% lower on the mixed workload. Bypassing
XFS provided a small but consistent gain, while regular files retained most of
the performance and remained easier to deploy as ordinary Linux files.

SPDK exceeded raw io_uring by 11.28% on GET and 7.18% on the 1:1 workload; raw
io_uring led SPDK by 0.37% on SET. SPDK reduced GET p99.9 by 61.41%, matched the
mixed p99.9, and reduced SET p99.9 by 2.43%. SPDK's throughput advantage was
concentrated in random reads and concurrent read/write traffic, while pure
write throughput remained close to raw io_uring.

Dragonfly kept normal tiered-storage reclamation enabled. After loading, it ran
a 180-second random-GET warmup. It was not restarted, cleared, or stripped of
Linux page cache between the three formal 300-second tests.

Garnet ran Lookup compaction every 300 seconds and used
`compaction-force-delete` to delete reclaimed segments immediately. It received
no extra GET warmup and did not wait for background reclamation; formal GET,
1:1, and SET tests began immediately after loading.

Tendis kept RocksDB automatic compaction and Blob GC enabled throughout. It
received no extra warmup and did not wait for background organization after
loading, so the result includes cold-cache and online-compaction effects.

KeyDB On Flash retained the RocksDB WAL and automatic compaction with a 64 GiB
DRAM hot tier. It received no extra warmup and did not wait for background
organization. Uniform random access sent most requests to Flash; this is not
the hot-key distribution recommended for KeyDB On Flash.

## Independent Azure Managed Redis capacity test

This test answers two separate questions: how much data a 480 GB / 16 vCPU
managed instance could accept under this workload, and its short-window
performance with about 400 GB loaded and 80 unlimited-rate connections. The
main table includes these rows with `*` for magnitude context. Differences in
dataset size, duration, network path, and service model prevent a controlled
ranking against the local results.

### Cost estimate

The user supplied the following monthly estimates on August 12, 2026. They
describe this configuration and are not fixed quotations.

| Deployment | Estimated price (USD/month) |
| --- | ---: |
| Azure Managed Redis Flex, 480 GB / 16 vCPU | $2,414.28 |
| Keylane server, `Standard_L16s_v3` | $1,152.67 |

Redis Flex was estimated to cost $1,261.61 more per month, or 2.09× the Keylane
server VM price (109.45% higher). Neither figure includes the benchmark client.
Actual bills vary with region, purchasing model, discounts, storage, and
networking. Managed Redis and a self-managed VM also have different service
boundaries, so this is an instance-price comparison rather than a full TCO.

| Workload | QPS | p99 (ms) | p99.9 (ms) | Window | Validation |
| --- | ---: | ---: | ---: | ---: | --- |
| Read-only GET | 44,480.15 | 5.855 | 11.839 | 60 s | 100% hits, 0 errors |
| 1:1 read/write | 55,351.99 | 5.311 | 11.839 | 60 s | 100% GET hits, 0 errors |
| Write-only SET | 84,863.93 | 3.199 | 7.839 | 60 s | 0 errors |

The instance ran Redis 7.4.3, reported `redis_mode:standalone`, and used
`noeviction`. The client was a `Standard_L16s_v3` and connected directly to
`10.0.0.6:10000` through a Private Endpoint in the same VNet. TLS was disabled,
so the command did not use `--tls`; the endpoint also did not require
`--cluster-mode`. Microsoft documents port 10000 for Azure Managed Redis. Only
the OSS cluster policy requires memtier `--cluster-mode`; an Enterprise cluster
policy may expose a non-cluster endpoint. Production reproductions should
resolve the standard hostname through Private DNS rather than permanently
hard-coding a private IP.

Capacity used Redis `INFO memory` and not merely accumulated value bytes. The
formal dataset contained 154,088,000 sequential keys and reported
`used_memory=400,002,049,295` bytes before testing. Additional roughly 10 GB
steps reached a final fully successful point of 166,088,001 keys and
`used_memory=432,447,820,034` bytes with `evicted_keys=0`. A subsequent attempt
to add 3.2 million keys returned `OOM command not allowed when used memory >
'maxmemory'` for every operation, and `DBSIZE` did not increase. The last fully
successful point was therefore approximately 432.45 GB of `used_memory`, not
the exact usable capacity implied by the 480 GB product label.

After the capacity probe, `FLUSHDB SYNC` confirmed `DBSIZE=0` and approximately
96 MB of `used_memory`. The formal 400 GB dataset was then loaded from scratch.
All three final logs were free of OOM, authentication, and connection errors;
GETs in the read and mixed tests all hit. A separate ten-second safety run with
the SET parameters confirmed that the 60-second test would not reach the limit;
that run is excluded from the table. After all tests, `DBSIZE` remained
154,088,000, `evicted_keys=0`, and `used_memory=426,201,007,973` bytes. Overwrite
traffic raised hot-data memory use, so sustained writes near the capacity limit
must monitor both OOM replies and `used_memory`; Redis error replies must not be
counted as successful throughput.

To reproduce, first configure the Azure Managed Redis Private Endpoint and
Private DNS. Microsoft recommends connecting to
`<cache>.<region>.redis.azure.net:10000` and resolving the standard hostname to
the private address through a `privatelink.redis.azure.net` private DNS zone.
The temporary DNS zone was not yet linked to the client VNet during this test,
so the approved Private Endpoint address `10.0.0.6` was used directly for the
non-TLS instance.

```bash
# Run on the client; never put the real access key in a report or repository.
export AMR_HOST=10.0.0.6
export AMR_PORT=10000
export AMR_ACCESS_KEY='<access-key>'

# Build the sequential, approximately 400 GB dataset.
taskset -c 0-15 memtier_benchmark \
  -s "$AMR_HOST" -p "$AMR_PORT" -a "$AMR_ACCESS_KEY" \
  -t 16 -c 40 \
  -n allkeys \
  --distinct-client-seed \
  --ratio=1:0 \
  --key-pattern=P:P \
  --key-prefix="kv_" \
  --key-minimum=1 \
  --key-maximum=154088000 \
  --random-data \
  --data-size-range=1000-4000 \
  --data-size-pattern=R \
  --hide-histogram

# Substitute 0:1, 1:1, and 1:0 for RATIO. Check each log for OOM/errors.
taskset -c 0-15 memtier_benchmark \
  -s "$AMR_HOST" -p "$AMR_PORT" -a "$AMR_ACCESS_KEY" \
  -t 8 -c 10 \
  --test-time 60 \
  --distinct-client-seed \
  --ratio=RATIO \
  --key-prefix="kv_" \
  --key-minimum=1 \
  --key-maximum=154088000 \
  --random-data \
  --data-size-range=1000-4000 \
  --data-size-pattern=R \
  --hide-histogram \
  --print-percentiles="99,99.9" \
  --randomize
```

References: [Azure Managed Redis performance guidance](https://learn.microsoft.com/en-us/azure/redis/best-practices-performance),
[Azure Managed Redis Private Link](https://learn.microsoft.com/en-us/azure/redis/private-link).

## Test environment

| Role | Azure VM size | Address |
| --- | --- | --- |
| Server | `Standard_L16s_v3` | `10.0.0.4:6379` |
| Client | `Standard_L16s_v3` | `10.0.0.5` |

The common workload used eight memtier threads with ten connections each,
uniformly random 1,000–4,000-byte values, keys `kv_1` through `kv_200000000`,
300-second windows, and no QPS limit. Each backend loaded the 200 million keys
once and then ran read-only, 1:1 mixed, and write-only tests without restarting
or clearing the database. The nine services/backends used the same port at
different times and never ran concurrently.

All three Keylane backends used 16 workers with defragmentation enabled. SPDK
directly accessed two NVMe namespaces. Raw io_uring directly accessed two block
devices through the Linux NVMe driver with 512-byte direct-I/O alignment. File
io_uring used one XFS filesystem and one fully preallocated 1,600 GiB regular
file per drive with 4 KiB-aligned O_DIRECT I/O. Neither io_uring configuration
used RAID. Dragonfly v1.40.1 used 16 proactor threads and RAID0/XFS with
experimental cooling disabled. Garnet v2.1.3 used .NET 10.0.302, RAID0/XFS, 64
GiB of hybrid-log memory, a 32 GiB read cache, a 4 GiB index, and Linux Native
libaio. Kvrocks v2.16.0 used 16 workers, RAID0/XFS, an 80 GiB block cache,
BlobDB, and no compression. Pika v4.0.3 used 16 network threads, 32 request
threads, three RocksDB instances, 24 GiB total block cache, 32 GiB RTC cache,
and no compression or binlog. Tendis tag `2.8.4-rocksdb-v8.5.3` used 16
executor threads, ten RocksDB stores, a shared 72 GiB block/blob cache,
RAID0/XFS, and no WAL, binlog, or compression. KeyDB On Flash v6.3.4 used four
server threads, a 64 GiB DRAM hot tier, RAID0/XFS, and retained its RocksDB WAL.

## Result boundaries and fairness notes

- Keylane does not use an LSM tree or RocksDB compaction. Defragmentation stayed
  enabled. Read-only traffic creates no obsolete versions and does not actively
  trigger defrag. The 1:1 workload allowed at most two active tasks per device
  and cooled for 15 ms between blocks. The write-only workload allowed six
  active tasks per device with no inter-block cooling. Both write workloads used
  zero inter-record cooling.
- Keylane raw and file io_uring used the same binary and service parameters.
  Regular files were opened with O_DIRECT and did not rely on the Linux page
  cache. Raw devices bypassed XFS but retained the Linux block layer and NVMe
  kernel driver. Keylane treated both paths as independent devices and assigned
  eight home workers to each.
- `--registered-buffer-mb-per-worker=256` means 256 MiB per worker in this code,
  or roughly 4 GiB across 16 workers—not 256 MiB for the process. SPDK and
  io_uring used the same setting, so their comparison remains consistent, but
  deployment capacity planning must use the per-worker interpretation.
- Each Keylane backend began formal testing immediately after its own full
  load, without artificial aging or a hand-picked short window. Each backend
  loaded once and then ran read-only, mixed, and write-only workloads in that
  fixed order. Both drives had symmetric I/O during read and mixed tests. The
  raw path bypassed the filesystem; the file path preserved an ordinary Linux
  file deployment model.
- Raw io_uring requires exclusive block-device ownership and has deployment and
  operational constraints similar to SPDK. Regular-file io_uring includes XFS
  overhead but is closer to a conventional Linux deployment. Both retain the
  Linux NVMe driver, interrupt handling, and kernel block-layer costs.
- Dragonfly used Linux buffered I/O because `backing_file_direct=false`.
  Normal operation retained page cache, so it received a 180-second random GET
  warmup. The process and page cache remained intact between the formal GET,
  mixed, and SET tests.
- Garnet used the official v2.1.3 Release binary rather than a container. Only
  raw-string GET and SET were tested, so object store and pub/sub were disabled.
  The 4 GiB index covered 200 million keys using Garnet's documented estimate
  of about 16 bytes per key, avoiding long hash chains from the 128 MiB default.
- Garnet Storage Tier used Linux Native libaio, four completion threads, at most
  512 in-flight I/Os per device, an 8 KiB initial record read, and the default
  scatter-gather GET. With 64 GiB hybrid log, 32 GiB read cache, and the index,
  RSS was about 102 GiB after testing. This was a maximum-performance setup,
  not a low-memory deployment.
- Garnet received no extra read warmup. Read-only testing began immediately
  after the full load; the process and database were retained between tests.
  Both the 64 GiB hybrid log and 32 GiB read cache were allocated by the end.
- Garnet disabled AOF and periodic checkpoints. Lookup compaction ran every 300
  seconds with `compaction-force-delete`, so formal results include online
  reclamation cost and bound hybrid-log growth. Without AOF, deleted old
  segments cannot be treated as a restart-recoverable checkpoint. This is the
  storage-tier cache-store configuration tested here, not a persistent database
  configuration.
- Garnet v2.1.3 throws `NullReferenceException` and closes the management
  connection when `DBSIZE` is used with `--no-obj`. Data sessions were
  unaffected. Exactly 200,000,000 successful SETs, `INFO store` addresses, and
  subsequent all-hit random GETs jointly validated the dataset.
- Kvrocks kept normal flush and automatic compaction enabled during loading and
  formal tests. Read-only testing began without waiting for background
  compaction to finish, so the result includes realistic online compaction cost.
- Kvrocks used an 80 GiB HCC block cache and received no additional read warmup.
- Kvrocks disabled WAL, per-write sync, compression, and Blob GC. Disabling WAL
  changes recovery semantics; the result represents only the specified data
  path configuration.
- Pika was built from official tag v4.0.3 at commit
  `d16db1eee9aadb1db42338269936deb7b584ddcc`, without a container. Its binary
  still reports 4.0.2, so the tag, commit, and self-reported version are all
  recorded to avoid ambiguity.
- Each of Pika's three RocksDB instances had an 8 GiB shared block cache, for 24
  GiB total. RTC cache was 32 GiB. It received no additional read warmup or wait
  for background organization after loading.
- Pika disabled RocksDB WAL/binlog and compression but retained automatic
  compaction, so formal results include online compaction cost.
- Tendis was built from official tag `2.8.4-rocksdb-v8.5.3` at commit
  `6a5a4945f1b8dd9d248d8f25325c12881a3cbf5d`, without a container. Its ten
  default RocksDB stores each maintain memtables and background tasks; all
  stores share the 72 GiB block/blob cache.
- Tendis disabled RocksDB WAL, Tendis binlog, per-transaction log flush, and
  compression, so recovery and replication semantics differ from defaults.
  Automatic compaction and Blob GC stayed enabled to reclaim obsolete blobs.
- Each Tendis store used a 256 MiB write buffer and at most four memtables. It
  received no warmup and did not wait for compaction after loading. No write
  stop or background error occurred during testing.
- KeyDB On Flash was built from official v6.3.4 commit
  `7e7e5e57d25fe246a8201f0acf5e7363c0bf1e14` with `ENABLE_FLASH=yes`, without
  a container. The vendor labels On Flash beta; results apply only to this
  experimental feature, workload, and version.
- KeyDB On Flash used 64 GiB `maxmemory`, `allkeys-lru`, and retained its
  in-memory key cache. Uniform random access to 200 million keys does not match
  its hot-data design, but matches every other backend. All formal GETs hit, so
  its low throughput was not caused by missing keys.
- KeyDB On Flash retained the RocksDB WAL and automatic compaction with the
  default uncompressed data column family. AOF and RDB were disabled to avoid a
  second persistence layer. It used `max_background_jobs=16` and an 8 GiB total
  WAL limit. Short GET tests with 4, 8, and 16 server threads produced about
  267.9k, 202.6k, and 41.4k QPS, so the formal test used the vendor-recommended
  maximum of four threads.
- KeyDB On Flash received no warmup or wait for background organization.
  `DBSIZE` remained 200,000,000 after all tests, the service stayed active, no
  errors or connection refusals occurred, and the data directory used about
  552 GiB.

## Reproduction

### 1. Keylane defragmentation parameters

All three Keylane backends used the same dynamic parameters. Defragmentation
remained enabled, with parameters set before each applicable workload.

The 1:1 workload used lower background concurrency and a 15 ms pause after each
block to reduce online tail latency:

```bash
redis-cli -h 10.0.0.4 -p 6379 DEFRAG MAX-ACTIVE 2
redis-cli -h 10.0.0.4 -p 6379 DEFRAG BLOCK-SLEEP 15
redis-cli -h 10.0.0.4 -p 6379 DEFRAG RECORD-SLEEP 0
```

The write-only workload increased concurrency per device so reclamation could
keep up with sustained overwrites:

```bash
redis-cli -h 10.0.0.4 -p 6379 DEFRAG MAX-ACTIVE 6
redis-cli -h 10.0.0.4 -p 6379 DEFRAG BLOCK-SLEEP 0
redis-cli -h 10.0.0.4 -p 6379 DEFRAG RECORD-SLEEP 0
```

`MAX-ACTIVE` is per storage device. With two devices, these settings allow at
most four or twelve active defrag tasks process-wide. `BLOCK-SLEEP` is in
milliseconds and `RECORD-SLEEP` is in microseconds. Read-only traffic creates
no obsolete versions and can retain the startup defaults. Use the following
command to confirm configuration, active tasks, and queued tasks:

```bash
redis-cli -h 10.0.0.4 -p 6379 DEFRAG STATUS
```

### 2. Start Keylane with SPDK

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

### 3. Build and start Keylane with io_uring

The ordinary io_uring build explicitly disables SPDK. Raw block devices and
regular files use the same binary:

```bash
cmake -S . -B bld-iouring-files -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF \
  -DKEYLANE_ENABLE_OPT=ON \
  -DKEYLANE_WITH_SPDK=OFF
cmake --build bld-iouring-files -j 16
```

#### Two XFS regular files

The following initialization destroys the existing filesystems and data on the
target devices. Verify the actual device names first and never include the
system disk.

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

`1,600 GiB` was specific to this machine, not a fixed Keylane requirement.
Production provisioning must leave filesystem journal and operational space.
Each new file must be a multiple of 8 MiB. Keylane does not create, expand, or
truncate these files at startup.

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

The logs confirmed 1,717,986,918,400 bytes and 204,799 data blocks per file,
eight home workers per device, and 4,096-byte direct-I/O alignment.

#### Two raw block devices

The raw configuration requires unmounted, exclusively owned devices. The
following operations make the original filesystem and Keylane file data
inaccessible. `wipefs` plus zeroing the first 8 MiB establishes new Keylane
metadata and bitmap state; it is not a secure full-device erase, so old data
blocks may remain physically present but are excluded from the new storage set.

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

Each NVMe device had a raw capacity of 1,920,383,410,176 bytes. Keylane used
1,920,378,863,616 bytes and ignored the tail shorter than one 8 MiB block. Each
device provided 228,926 data blocks, eight home workers, and 512-byte
direct-I/O alignment.

### 4. Create RAID0 and XFS

Dragonfly, Garnet, Kvrocks, Pika, Tendis, and KeyDB On Flash reused this
filesystem at different times. These commands erase `/dev/nvme0n1` and
`/dev/nvme1n1`; revalidate device identity on the target machine and never
include the system disk.

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

The resulting array was RAID0 with a 512 KiB chunk, 3.49 TiB total capacity,
and `/mnt/data` as its mount point.

### 5. Start Dragonfly Tiered Storage

Tested version: `dragonfly v1.40.1-434478e00c366c711985d0b3269023fc39db8ad1`.
The official GitHub Release x86-64 binary was used directly, without a
container. Its SHA-256 was
`1d2b6654f4488ebc3f6cd5061199158880f6d957534705cd548a909108507b8b`.
Dragonfly's version check remained enabled.

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
  --maxmemory=64GB \
  --dir=/mnt/data/dragonfly \
  --dbfilename= \
  --tiered_prefix=/mnt/data/dragonfly/tier \
  --tiering_disk_storage_initial_size=40GB \
  --backing_file_direct=false \
  --tiered_offload_threshold=1.0 \
  --tiered_experimental_cooling=false \
  --tiered_max_pending_stash_bytes=16MB
```

Every explicit Dragonfly v1.40.1 setting either fixes the test resource
boundary, permits the full dataset, or improves performance:

| Setting | Purpose and performance implication |
| --- | --- |
| `--proactor_threads=16` | Explicitly use all 16 available server CPUs, matching the CPU range of the other systems. |
| `--maxmemory=64GB` | Limit Dragonfly's memory tier to 64 GiB. Tiered Storage carries the rest, while Linux may use remaining memory as page cache. |
| `--dbfilename=` | Suppress snapshot creation during formal windows, avoiding snapshot I/O interference. |
| `--tiering_disk_storage_initial_size=40GB` | Raise the initial tier file from 256 MiB to 40 GiB, reducing repeated file growth early in loading. |
| `--backing_file_direct=false` | Explicitly use buffered I/O instead of the `true` default, allowing Linux page cache to serve random reads. Cache is warmed after loading and retained between tests. |
| `--tiered_offload_threshold=1.0` | Begin offloading as soon as the available-memory ratio falls below 100%, avoiding concentrated backpressure after approaching the 64 GiB limit. The default is `0.5`. |
| `--tiered_experimental_cooling=false` | Disable the intermediate experimental cooling layer, which reduced load and foreground throughput under this workload, and send eligible values directly to storage. |
| `--tiered_max_pending_stash_bytes=16MB` | Raise the limit from 256 KiB to 16 MiB, allowing more in-flight writes and batching to utilize both NVMe devices. |

`proactor_affinity_mode=on` and `version_check=true` retained their defaults and
are not repeated in the command.

### 6. Build and start Apache Kvrocks

Tested version: `kvrocks version 2.16.0 (commit 28440b5)`. The complete tested
configuration follows; settings that affect performance or durability are
included so the score is not separated from its tuning conditions.

```bash
git clone --branch v2.16.0 --depth 1 https://github.com/apache/kvrocks.git
cd kvrocks
./x.py build --ninja -j16

sudo mkdir -p /mnt/data/kvrocks
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/kvrocks
```

`kvrocks-perf.conf`:

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
rocksdb.write_options.disable_wal yes
rocksdb.write_options.no_slowdown no
rocksdb.rate_limiter_auto_tuned no
rocksdb.partition_filters yes
```

Pin the 16 server workers to CPUs 0–15:

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

The purposes and costs of these Kvrocks settings are:

| Setting | Purpose | Cost or boundary |
| --- | --- | --- |
| 80 GiB HCC block cache, cached indexes/filters, 16 KiB blocks | Improve read-cache hit rate and reduce block reads | Uses 80 GiB of server memory |
| BlobDB, 1,000-byte threshold, 1 GiB blob files, blob cache | Separate large values from LSM metadata to reduce write amplification | Reads use metadata plus blob paths; space reclamation depends on Blob GC |
| Blob GC disabled | Prevent GC from competing for I/O during the test | Obsolete blobs from overwrites are not reclaimed and disk use grows |
| SST/WAL/blob compression disabled | Reduce CPU use | Raises capacity and write-bandwidth requirements |
| 512 MiB write buffers, at most eight, merge at least two | Increase write buffering and reduce L0 flush-file count | Raises memory use |
| Automatic compaction enabled while loading, L0 thresholds 16/128/256 | Build the initial dataset through normal flush/compaction paths | Consumes background CPU and I/O during loading |
| 16 background jobs, four subcompactions, 2 MiB compaction readahead | Preserve parallelism for ordinary compaction | Online compaction consumes CPU and I/O |
| 64 GiB L1 base, multiplier 10, dynamic level bytes disabled | Control capacity layout for level compaction | Must be adjusted for the actual dataset size |
| Pipelined writes, async read I/O, unlimited I/O rate | Increase concurrency and throughput | Can saturate the devices more readily |
| WAL and per-write sync disabled | Isolate the data-write path and raise throughput | A process or machine failure may lose data not yet flushed |
| 16 workers, `max_open_files=-1` | Use every server CPU and avoid repeated file opens | Raises thread and file-descriptor use |

### 7. Build and start Pika

The tested source was official tag `v4.0.3` at commit
`d16db1eee9aadb1db42338269936deb7b584ddcc`. The binary built from that commit
reports `pika_version: 4.0.2`. A source-built Release binary ran directly,
without a container:

```bash
git clone --branch v4.0.3 --depth 1 https://github.com/OpenAtomFoundation/pika.git
cd pika
cmake -S . -B output -DCMAKE_BUILD_TYPE=Release
cmake --build output -j 16

sudo mkdir -p /mnt/data/pika/{db,log,dump,dbsync}
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/pika
```

The following parameters were set on top of the release `conf/pika.conf`; all
unlisted values retained v4.0.3 defaults:

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

`block-cache: 8G` applies to each RocksDB instance, for 24 GiB across three
instances. `share-block-cache: yes` shares the cache among column families
inside each instance. RTC cache was 32 GiB. WAL/binlog and compression were
disabled, favoring maximum performance over default durability semantics.

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

Automatic compaction remained enabled during formal testing. After loading,
the test did not wait for background tasks to finish; it confirmed that the
setting remained `false` before read-only testing:

```bash
redis-cli -h 10.0.0.4 -p 6379 CONFIG GET disable_auto_compactions
```

### 8. Build and start Microsoft Garnet

Tested version: [`Garnet 2.1.3`](https://github.com/microsoft/garnet/releases/tag/v2.1.3),
tag/commit `v2.1.3` / `b4bf6275351dad3202467d88814a9aee793286c9`.
A Linux x64 binary was published and run directly, without a container. Memory
and index sizing followed the official
[memory sizing guide](https://microsoft.github.io/garnet/docs/getting-started/memory);
Storage Tier, Native I/O, and read-cache parameters followed the official
[configuration reference](https://microsoft.github.io/garnet/docs/getting-started/configuration).
This version required .NET SDK 10.0.302.

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

Ubuntu 24.04 names the libaio runtime `libaio.so.1t64`, while the prebuilt
native-device library references `libaio.so.1`. This test changed only the ELF
dependency name, not Garnet code. Skip this step on distributions where `ldd`
does not report `libaio.so.1 => not found`:

```bash
patchelf --replace-needed libaio.so.1 libaio.so.1t64 \
  /opt/garnet-2.1.3/runtimes/linux-x64/native/libnative_device.so
patchelf --replace-needed libaio.so.1 libaio.so.1t64 \
  /opt/garnet-2.1.3/runtimes/linux-x64/native/libnative_device_libaio.so
```

The following configuration provides storage-tier cache-store semantics and
does not enable AOF or checkpoint recovery. Hybrid-log pages that no longer fit
in memory are written to `hlog.*` segments, but those files alone are not a
restart-recoverable copy. Lookup compaction ran every 300 seconds and deleted
reclaimed segments immediately, including online reclamation cost in the
formal results while bounding growth from overwrite traffic.

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
  --compaction-freq 300 \
  --compaction-type Lookup \
  --compaction-force-delete \
  --network-connection-limit 10000 \
  --minthreads 16 \
  --miniothreads 16 \
  --logger-level Warning
```

### 9. Build and start Tendis

The tested source was official tag
[`2.8.4-rocksdb-v8.5.3`](https://github.com/Tencent/Tendis/tree/2.8.4-rocksdb-v8.5.3)
at commit `6a5a4945f1b8dd9d248d8f25325c12881a3cbf5d`. A Release binary
was built and run directly, without a container:

```bash
git clone --recursive --branch 2.8.4-rocksdb-v8.5.3 \
  https://github.com/Tencent/Tendis.git
cd Tendis

cmake -S . -B build-perf -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_DISABLE_FIND_PACKAGE_Gflags=TRUE \
  -DCMAKE_DISABLE_FIND_PACKAGE_GTest=TRUE
cmake --build build-perf -j 16 --target tendisplus

sudo mkdir -p /mnt/data/tendis/{db,log,dump}
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/tendis
```

`tendis-perf.conf`:

```text
bind 10.0.0.4
port 6379
daemon off
logLevel warning
logdir /mnt/data/tendis/log
dumpdir /mnt/data/tendis/dump
dir /mnt/data/tendis/db
pidfile /mnt/data/tendis/tendisplus.pid
slowlog /mnt/data/tendis/log/slowlog
maxclients 10000
tcp-backlog 8192

netIoThreadNum 4
executorThreadNum 16
executorWorkPoolSize 4
binlog-enabled no
binlog-save-logs no
checkkeytypeforsetcmd no

rocks.blockcachemb 73728
rocks.blockcache_num_shard_bits 8
rocks.blobcache_in_blockcache yes
rocks.disable_wal yes
rocks.flush_log_at_trx_commit no
rocks.compress_type none
rocks.rate_limiter_rate_bytes_per_sec 0
rocks.write_buffer_size 268435456
rocks.max_write_buffer_number 4
rocks.min_write_buffer_number_to_merge 2
rocks.target_file_size_base 536870912
rocks.max_bytes_for_level_base 68719476736
rocks.level_compaction_dynamic_level_bytes 1
rocks.level0_file_num_compaction_trigger 16
rocks.level0_slowdown_writes_trigger 128
rocks.level0_stop_writes_trigger 256
rocks.max_background_jobs 16
rocks.max_subcompactions 4
rocks.compaction_readahead_size 2097152
rocks.enable_pipelined_write 1
rocks.max_open_files -1
rocks.cache_index_and_filter_blocks 1
rocks.pin_l0_filter_and_index_blocks_in_cache 1
rocks.partition_filters 1
rocks.block_size 16384
rocks.use_direct_reads 0
rocks.use_direct_io_for_flush_and_compaction 0
rocks.enable_blob_files 1
rocks.min_blob_size 1000
rocks.blob_file_size 1073741824
rocks.blob_compression_type none
rocks.enable_blob_garbage_collection 1
rocks.blob_garbage_collection_age_cutoff 0.25
rocks.blob_garbage_collection_force_threshold 0.5
```

The 72 GiB cache was shared by all ten stores. Each store independently used
256 MiB write buffers and at most four memtables. Buffered I/O allowed Linux
page cache to participate in regular-file access, but this run added no GET
warmup. WAL, Tendis binlog, and compression were disabled for maximum data-path
throughput. Automatic compaction and Blob GC remained enabled so obsolete
versions from overwrites could be reclaimed continuously.

```bash
sudo systemd-run \
  --unit=tendis-perf.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitMEMLOCK=infinity \
  --property=LimitNOFILE=infinity \
  /path/to/Tendis/build-perf/bin/tendisplus \
  /path/to/tendis-perf.conf
```

### 10. Build and start KeyDB On Flash

The tested version was official
[`KeyDB v6.3.4`](https://github.com/Snapchat/KeyDB/releases/tag/v6.3.4) at
commit `7e7e5e57d25fe246a8201f0acf5e7363c0bf1e14`. The vendor's
[On Flash documentation](https://docs.keydb.dev/docs/flash/) labels the feature
beta and requires explicitly enabling Flash at build time:

```bash
git clone --recursive --branch v6.3.4 --depth 1 \
  https://github.com/Snapchat/KeyDB.git
cd KeyDB
make -j 16 ENABLE_FLASH=yes BUILD_TLS=no

./src/keydb-server --is-flash-enabled
```

When Ubuntu 24.04's GCC 13 builds KeyDB's pinned RocksDB commit
`444b3f4845dd01b0d127c4b420fdd3b50ad56682`, two headers rely on indirect
includes from older compilers. If the build reports
`uint8_t/uint64_t does not name a type`, add only the standard header and
rebuild; this does not change runtime logic:

```diff
--- a/deps/rocksdb/table/block_based/data_block_hash_index.h
+++ b/deps/rocksdb/table/block_based/data_block_hash_index.h
@@
+#include <cstdint>
 #include <string>

--- a/deps/rocksdb/util/string_util.h
+++ b/deps/rocksdb/util/string_util.h
@@
+#include <cstdint>
 #include <string>
```

Tested `keydb-flash.conf`:

```text
bind 10.0.0.4
protected-mode no
port 6379
daemonize no
loglevel warning
logfile ""
databases 1
save ""
appendonly no

server-threads 4
server-thread-affinity true
min-clients-per-thread 10
maxclients 10000
tcp-backlog 8192

maxmemory 64gb
maxmemory-policy allkeys-lru
maxmemory-samples 16

storage-provider flash /mnt/data/keydb-flash
storage-provider-options max_background_jobs=16;max_total_wal_size=8589934592
```

The 64 GiB hot tier follows the vendor sizing recommendation of approximately
50% of physical memory. `allkeys-lru` retains hot data; eviction removes objects
only from the DRAM cache, and values remain readable from Flash. AOF and RDB
were disabled to avoid a second log or snapshot beside the RocksDB persistence
layer; the RocksDB WAL stayed enabled. `max_background_jobs=16` used the
available server CPUs for flush and compaction, while the 8 GiB WAL limit
reduced frequent forced flushes. Automatic compaction remained enabled.

KeyDB warns that too many server threads increase spinlock contention and
recommends at most four. On this machine, an isolated 2M-key, 80-connection,
30-second Flash-read test with `flash-disable-key-cache=yes` produced about
267.9k, 202.6k, and 41.4k QPS with 4, 8, and 16 threads. The formal test
therefore used four. `flash-disable-key-cache` was used only for thread
selection; the formal load and tests retained its default value of `no`.

```bash
sudo mkdir -p /mnt/data/keydb-flash
sudo chown -R "$(id -un):$(id -gn)" /mnt/data/keydb-flash

sudo systemd-run \
  --unit=keydb-flash.service \
  --collect \
  --property=AllowedCPUs=0-15 \
  --property=LimitMEMLOCK=infinity \
  --property=LimitNOFILE=infinity \
  /path/to/KeyDB/src/keydb-server \
  /path/to/keydb-flash.conf

redis-cli -h 10.0.0.4 -p 6379 INFO memory \
  | grep '^storage_provider:flash'
```

### 11. Load all 200 million keys

Run `FLUSHALL` once, and only after confirming that the target is a disposable,
empty benchmark instance. `FLUSHALL` deletes the entire database and must not be
run again after loading. Use 640 client connections for the complete SET load:

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

Loading only constructs an identical 200-million-key starting dataset. Its
duration and throughput are not part of the formal comparison.

### 12. Warm Dragonfly's page cache

After loading Dragonfly, warm the Linux page cache with random GETs. Do not
clear page cache or restart between the subsequent formal tests. Warmup output
is excluded from the formal results.

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
```

Garnet, Kvrocks, Pika, Tendis, and KeyDB On Flash received no extra random-GET
warmup. Read-only testing began immediately after loading with reclamation or
automatic compaction still enabled. Do not use `DBSIZE` to validate Garnet
v2.1.3 data in the `--no-obj` configuration because of the documented
management-command failure. Instead, confirm exactly 200,000,000 successful
SETs and save `INFO store` addresses for audit. KeyDB On Flash can use `DBSIZE`
to verify 200,000,000 keys and `INFO memory` to verify
`storage_provider:flash`.

### 13. Confirm Kvrocks automatic compaction

Kvrocks kept automatic compaction enabled throughout loading and testing. The
test did not wait for active or queued compaction to reach zero before starting
read-only traffic, intentionally including online organization cost. Confirm
that the setting was not disabled dynamically:

```bash
redis-cli -h 10.0.0.4 -p 6379 CONFIG GET rocksdb.disable_auto_compactions
```

Record level state, pending compaction, and background tasks at the start for
audit, but do not require them to become idle:

```bash
redis-cli -h 10.0.0.4 -p 6379 INFO rocksdb \
  | grep -E 'num_files_at_level|estimate_pending_compaction_bytes|num_running_compactions|compaction_count'
```

### 14. Run the three formal workloads in order

Substitute read-only `0:1`, 1:1 mixed `1:1`, and write-only `1:0` for `RATIO` in
that order. Each backend performs one full load, and all three tests reuse that
dataset. After every test, verify that no background error occurred. For
Keylane, also record active and queued tasks with `DEFRAG STATUS`. Kvrocks,
Pika, Garnet, Tendis, and KeyDB On Flash keep background compaction or
reclamation active; do not wait for it to become idle.

No system clears OS page cache between the three tests. Only Dragonfly receives
an extra Linux page-cache warmup before formal testing. Other backends retain
the cache state naturally created by loading and earlier workloads. Keylane
SPDK, raw io_uring, O_DIRECT regular-file io_uring, and Garnet's Native O_DIRECT
Storage Tier do not depend on this page-cache path.

Garnet runs read-only, 1:1, and write-only workloads in one process and dataset,
without restart or clearing. At the end, `Log.BeginAddress=718970813904`,
`Log.TailAddress=1533597570336`, and the log directory occupied about 702 GiB.
The service remained healthy with no background errors.

```bash
# Run on the client.
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
