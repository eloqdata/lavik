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

# Lavik v0.1.0-beta.1: SPDK versus peers

**English** | [简体中文](README.zh-CN.md)

Measured the downloaded standard beta release package on fresh SPDK datasets. This report contains 22 new Lavik points and 124 reused peer-control points from an earlier sweep on the same hosts. io_uring is excluded from this report.

- 10M GET: SPDK peak **1012.2k QPS** at 640 connections; p99 **3.599 ms**, p99.9 **5.631 ms** at that point.
- 10M SET: SPDK peak **930.5k QPS** at 1280 connections; p99 **4.799 ms**, p99.9 **8.095 ms** at that point.
- 1B GET: SPDK peak **952.6k QPS** at 640 connections; p99 **3.599 ms**, p99.9 **5.535 ms** at that point.
- 1B SET: SPDK peak **764.9k QPS** at 1280 connections; p99 **10.879 ms**, p99.9 **16.639 ms** at that point.

## 10M keys × 1 KiB: in-memory controls

![10M throughput](memory-qps.png)

| Command | System | Threads¹ | Connections | QPS | Avg ms | p50 ms | p99 ms | p99.9 ms | p99.99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| GET | Lavik SPDK | 16 | 640 | 1,012,180 | 0.632 | 0.543 | 3.599 | 5.631 | 10.239 |
| GET | Redis 8.8.0 | 16 | 1280 | 976,801 | 1.310 | 1.119 | 4.671 | 8.031 | 17.407 |
| GET | Valkey 9.1.0 | 16 | 1280 | 965,697 | 1.325 | 1.111 | 4.543 | 8.159 | 16.895 |
| SET | Lavik SPDK | 16 | 1280 | 930,465 | 1.375 | 1.143 | 4.799 | 8.095 | 17.535 |
| SET | Redis 8.8.0 | 16 | 1280 | 910,426 | 1.405 | 1.167 | 4.831 | 8.127 | 16.895 |
| SET | Valkey 9.1.0 | 8 | 1280 | 802,519 | 1.594 | 1.423 | 4.383 | 8.895 | 16.895 |

![10M p99.9](memory-p999.png)

Redis and Valkey bars select one measured I/O-thread configuration per command: Redis GET/SET 16 threads, Valkey GET 16 / SET 8. Each configuration is shown across the complete connection sweep; the latency chart uses the same selections.

## 1B keys × 1 KiB: storage-tier controls

![1B throughput](storage-qps.png)

| Command | System | Threads¹ | Connections | QPS | Avg ms | p50 ms | p99 ms | p99.9 ms | p99.99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| GET | Lavik SPDK | 16 | 640 | 952,560 | 0.671 | 0.543 | 3.599 | 5.535 | 9.791 |
| GET | Dragonfly 1.40.2 | 16 | 1280 | 466,039 | 2.746 | 1.455 | 38.911 | 100.863 | 217.087 |
| GET | Garnet 2.1.5 | 16 | 320 | 446,366 | 0.716 | 0.423 | 4.223 | 9.151 | 20.607 |
| SET | Lavik SPDK | 16 | 1280 | 764,939 | 1.672 | 1.087 | 10.879 | 16.639 | 22.143 |
| SET | Dragonfly 1.40.2 | 16 | 640 | 539,616 | 1.185 | 0.783 | 14.591 | 32.127 | 48.639 |
| SET | Garnet 2.1.5 | 16 | 1280 | 751,057 | 1.703 | 1.383 | 5.535 | 12.735 | 20.863 |

![1B p99.9](storage-p999.png)

## Redis and Valkey I/O-thread sweep

![I/O threads](iothread-scaling.png)

Each panel shows all five I/O-thread configurations across connections. The table below summarizes each curve's maximum; it does not replace the full curves.

| System | I/O threads | Peak GET QPS @ connections | Peak SET QPS @ connections |
| --- | --- | --- | --- |
| Redis 8.8.0 | 1 | 157,964 @ 160 | 148,280 @ 320 |
| Redis 8.8.0 | 2 | 159,791 @ 80 | 148,384 @ 320 |
| Redis 8.8.0 | 4 | 436,230 @ 640 | 401,369 @ 640 |
| Redis 8.8.0 | 8 | 835,794 @ 1280 | 814,410 @ 1280 |
| Redis 8.8.0 | 16 | 976,801 @ 1280 | 910,426 @ 1280 |
| Valkey 9.1.0 | 1 | 155,408 @ 320 | 145,804 @ 160 |
| Valkey 9.1.0 | 2 | 162,097 @ 80 | 151,504 @ 160 |
| Valkey 9.1.0 | 4 | 446,155 @ 640 | 409,846 @ 1280 |
| Valkey 9.1.0 | 8 | 857,049 @ 1280 | 802,519 @ 1280 |
| Valkey 9.1.0 | 16 | 965,697 @ 1280 | 741,469 @ 1280 |

## SPDK configuration and reproduction

Follow the [SPDK storage guide](../../docs/operations/spdk-storage.md) for
release-package selection, obtaining the pinned setup helper without compiling
Lavik, mapping serial numbers to PCI addresses, VFIO/hugepage preparation,
and restoring kernel drivers after shutdown. The standard package includes
SPDK; the minimal package does not. SPDK storage here uses kernel TCP.

The six benchmark controllers were:

| Serial | PCI address | Namespace | Capacity (bytes) |
|---|---|---|---:|
| 40291d45b421bc880001 | `f698:00:00.0` | 1 | 1919850381312 |
| 40291d45b421bc880002 | `d2b4:00:00.0` | 1 | 1919850381312 |
| 40291d45b421bc880003 | `9038:00:00.0` | 1 | 1919850381312 |
| 40291d45b421bc880004 | `3da6:00:00.0` | 1 | 1919850381312 |
| 40291d45b421bc880005 | `674c:00:00.0` | 1 | 1919850381312 |
| 40291d45b421bc880006 | `d408:00:00.0` | 1 | 1919850381312 |

These addresses and serials belong to this host. Replace them with your own
verified dedicated controllers; binding affects all namespaces on a
controller. `LAVIK_SPDK_SETUP` and `LAVIK_BINARY` below are the helper and
extracted package paths set in the guide. The launch example omits fixed
`v0.1.0-beta.1` defaults and adapts the binary/log paths. The default worker
count follows the 16 CPUs selected by `taskset`.
The exact acquisition command is preserved in `server-command.json`.

The benchmark saved the previous hugepage/VFIO settings, cleared each
serial-allowlisted scratch device with `blkdiscard` while it was owned by the
kernel, and enabled VFIO no-IOMMU mode on this dedicated VM before binding.
The guide explains the VM-specific isolation constraint; ordinary IOMMU
hosts do not need that unsafe mode. Do not erase an existing database to
start or recover it.

```bash
sudo env PCI_ALLOWED='f698:00:00.0 d2b4:00:00.0 9038:00:00.0 3da6:00:00.0 674c:00:00.0 d408:00:00.0' \
  DRIVER_OVERRIDE=vfio-pci HUGEMEM=8192 \
  "$LAVIK_SPDK_SETUP" config
```

Start the 10M-key configuration:

```bash
sudo prlimit --memlock=unlimited:unlimited --nofile=65535:65535 \
  env BYCORF_DPDK_MEMORY_MB=8192 \
  BYCORF_EAL_ARGS='-a f698:00:00.0 -a d2b4:00:00.0 -a 9038:00:00.0 -a 3da6:00:00.0 -a 674c:00:00.0 -a d408:00:00.0' \
  taskset -c 0-15 "$LAVIK_BINARY" \
  --storage=spdk \
  --bind=172.16.0.4 \
  --tomb-raider-interval-ms=0 \
  --spdk-max-completions-per-poll=16 \
  --log-dir=/var/log/lavik/benchmark \
  --data-file=spdk://f698:00:00.0/1 \
  --data-file=spdk://d2b4:00:00.0/1 \
  --data-file=spdk://9038:00:00.0/1 \
  --data-file=spdk://3da6:00:00.0/1 \
  --data-file=spdk://674c:00:00.0/1 \
  --data-file=spdk://d408:00:00.0/1
```

For the independent 1B-key load, use the same settings and add
`--shutdown-checkpoint`. Both groups begin with empty media and load data
from scratch. No checkpoint from another backend is reused. The 8 GiB EAL
reservation is separate from Lavik's other memory use.
`--spdk-max-completions-per-poll=16` overrides the default of `8`, and
`--tomb-raider-interval-ms=0` disables the default daily tombstone sweep.
`CONFIG GET spdk-max-completions-per-poll` returns `16`. The foreground
pre-poll keeps its default of 5 µs.

The archive contains the exact `server-command.json`, `eal-environment.json`,
`devices-before.json`, runtime checks, driver setup/reset logs, and before/after
host settings under `memory/lavik-spdk/` and `storage/lavik-spdk/`.
Client arguments for every point are recorded in `*.command.json` alongside
its memtier JSON and text output. Use those commands with your own host
addresses to reproduce the connection sweep. The scripts never infer that an
arbitrary NVMe drive is disposable.

## Method and scope

- Downloaded the standard x86_64 [v0.1.0-beta.1 release](https://github.com/eloqdata/lavik/releases/tag/v0.1.0-beta.1). Asset: `lavik-v0.1.0-beta.1-linux-x86_64.tar.gz`. The archive checksum matches its published SHA-256 file. Source revision: `3955b98d43b312324aa8d52775df52cfb111c0d0`; executable SHA-256: `b14da83ef4f3146848e8b27f4c556d01696e8f28398cf38f3a97bd68d68b1a31`. The executable reports `lavik 0.1.0-beta.1`. The annotated tag resolves to the archive revision (tag.json). No Lavik rebuild or source patch. The interrupted nightly attempt is retained under `aborted-nightly-attempt/` for provenance and contributes no reported measurements.
- Server: 172.16.0.4, AMD EPYC 9V74, 8 physical cores / 16 logical CPUs, approximately 126 GiB RAM. Client: 172.16.0.5, AMD EPYC 9V45, 16 cores, approximately 31 GiB RAM. All products and the client are pinned to CPUs 0–15. One product runs at a time; CPU/IRQ placement is not tuned during the experiment.
- Lavik uses 16 workers, kernel TCP, six serial-verified raw NVMe disks, busy-poll 20 µs, foreground/background budgets 1000/10 µs, background warrant 1%, and Tomb Raider disabled. The metrics listener is disabled; enclosing-window OS/INFO snapshots are collected. SPDK starts with cleared disks and independently loads both dataset sizes. No io_uring dataset or checkpoint is reused. The user excluded io_uring from the final scope; its interrupted acquisition remains under excluded-io-uring-attempt/ and contributes no reported measurements.
- SPDK uses VFIO, eight GiB of hugepages, completion cap 16 and foreground pre-poll 5 µs. The VM requires VFIO no-IOMMU mode. Only the six benchmark controllers are rebound. Their kernel driver ownership, hugepage count and VFIO mode are restored after each session. OS and workspace disks are excluded.
- Redis 8.8.0 and Valkey 9.1.0 test 1/2/4/8/16 I/O threads with AOF and automatic snapshots disabled. Each configuration starts from its product's freshly generated, hash-verified 10M-key RDB, verifies the configured thread count and receives a 10-second GET warmup.
- Dragonfly 1.40.2 uses 16 proactors, 96 GiB maxmemory and Tiered Storage. Garnet 2.1.5 uses a 64 GiB hybrid log, 32 GiB read cache, 16 GiB index and Libaio Storage Tier. Both use files on RAID0/XFS built from the same six benchmark NVMe disks, then receive a 180-second GET warmup (eight client threads, 80 connections). The benchmark RAID is unmounted and stopped before SPDK.
- Garnet's pre-test exact DBSIZE scan passed with future compaction paused, log boundaries stable for 30 seconds, and unchanged boundaries across the scan. Lookup compaction was restored and verified before every formal point. The post-test full scan was stopped at the user's request after all 12 formal points had completed; the post-test key count and value-length check are therefore unverified. The waiver, interrupted-client traceback and actual SIGTERM server exit status are retained in the evidence. Pausing compaction avoids counting moved records twice during a concurrent scan; the pre-test scan itself affects cache and storage history.
- All formal clients use memtier 2.5.1, 16 threads, pipeline=1, no rate limit, decimal keys 1..N without prefixes, 1,024-byte values, uniform random GET or overwriting SET. 10M points run for 30 seconds at 80/160/320/640/1280 connections; 1B points run for 60 seconds and add 2560 connections. Lavik receives a 10-second GET warmup for 10M and no additional 1B warmup, matching the prior acquisition.
- The 10M group retains the historical scripts' default correlated client seeds. The 1B group uses `--distinct-client-seed` for every product to avoid artificial locality from cloned random streams. The September 6 storage acquisition's seed behavior could not be established from the report; this is a disclosed methodological difference, not a code-only performance comparison.
- Every product is measured once per command, connection count, and server-thread configuration: 30 seconds for 10M keys and 60 seconds for 1B keys. Dataset counts and sampled value lengths are validated before every configuration and after every configuration except Garnet, whose post-test scan was waived by the user. All measured GETs have zero misses, and all 146 formal points have zero connection errors. These client checks do not substitute for Garnet's omitted post-test exact count. QPS and client latency describe the memtier measurement window; server CPU snapshots enclose additional setup and INFO work.
- The 124 peer-control points are reused from an earlier sweep and are compared with SPDK; only the 22 Lavik SPDK points are newly measured with the beta release package. Source paths, timestamps and control-provenance.json identify the reused data. Products run sequentially, not interleaved; temporal variation, CPU baseline, memory reservations, disk topology, cache policies and persistence guarantees limit causal interpretations. The old report used a Clang/native Lavik build; this report uses the downloaded GCC/x86-64-v2 package. Do not compare absolute rankings across the 10M and 1B groups or claim equal crash durability.

¹ Thread roles differ: Lavik workers, Redis/Valkey I/O threads, Dragonfly proactors, and Garnet minimum thread-pool threads. Garnet's actual thread count is not fixed at 16, but all threads share the 16-CPU affinity limit.

## Evidence and reproduction

[All 146 points](results.csv) · [10M results](memory-results.csv) · [1B results](storage-results.csv) · [Binary identity](binary.json) · [Peer hashes](binaries.json) · [Release metadata](release.json) · [Raw manifest](raw-SHA256SUMS) · [Raw evidence](evidence.tar.gz)

`runner.py` provides the shared acquisition routines, `spdk_fresh.py` acquires fresh SPDK data, `orchestrate.py` serializes the stages, and `summarize.py` validates every formal point against its recorded command and client JSON. These are host-specific scripts with an explicit six-device allowlist and destructive scratch-storage preparation. `generate_report.py` requires the complete validated matrix before rendering. The evidence archive excludes executable downloads and generated datasets; each raw file has a recorded SHA-256.

## Regenerate the figures

The committed [build_assets.py](build_assets.py) reads only [results.csv](results.csv)
and validates the full 146-point matrix before writing the five SVG/PNG figures.
The charts follow the previous report's grouped bars, GET/SET panels, thread-sweep
small multiples, and blue/orange/magenta palette. QPS axes begin at zero. Latency
charts use the same selected configurations as the throughput comparisons.
The acquisition scripts in the evidence archive retain their original export;
use this committed script for the figures displayed here.

```bash
python3 -m venv /tmp/lavik-chart-venv
/tmp/lavik-chart-venv/bin/pip install matplotlib==3.11.2
/tmp/lavik-chart-venv/bin/python perf_reports/lavik-v0.1.0-beta.1-spdk-vs-peers-2026-09-18/build_assets.py
```

Run the last command from the repository root. It reads committed CSV data and
writes image files only; it does not start servers or access benchmark disks.
