# Lavik v0.1.0: latest Redis / Valkey and a 1TB dataset

**English** | [简体中文](README.zh-CN.md)

Test date: 2026-10-13. The downloaded **Lavik v0.1.0 standard x86_64 release** was tested against freshly measured Redis 8.10.2 and Valkey 9.1.2 controls. This report presents **120 formal points**: 110 with 10 million keys and 10 Lavik-only points with one billion keys; values are 1,024 bytes. Lavik defrag stayed enabled throughout loading, warmup and measurement. No historical control results are reused.

In the 10M group, Lavik's peak GET was **1145.8k QPS** at 1280 connections (p99.9 **7.135 ms**); peak SET was **972.0k QPS** at 1280 connections (p99.9 **7.199 ms**). These are single-run observations. The charts show throughput and p99.9 latency across connection counts.

## Throughput: Lavik 10M / 1B versus Redis / Valkey

![Lavik 10M / 1B, Redis and Valkey throughput](combined-qps.png)

All four series cover 80–1280 connections. The 10M group uses 30 seconds per point and default client seeds; the 1B group uses 60 seconds and distinct client seeds. Dataset size, duration and seed settings differ.

## p99.9 latency comparison

![Lavik 10M / 1B, Redis and Valkey p99.9 latency](combined-p999.png)

Latency is client-observed p99.9 over each full measurement window, in milliseconds; lower is better. The I/O-thread configurations match the throughput chart. The connection axis uses a log₂ scale.

The 1B GET peak is 1,069,126 QPS at 640 connections; SET peaks at 817,113 QPS with 1280 connections. These are observations from this single sweep.

Full-batch acquisition span (including per-point setup/snapshots): 2026-10-13 11:33:05 UTC – 2026-10-13 13:12:17 UTC



## Hardware and deployment

| Item | Configuration |
|---|---|
| Server | Azure Standard_L16aos_v4, Japan West zone 2; AMD EPYC 9V74; 16 logical CPUs / 8 physical cores; 125.8 GiB Linux RAM |
| Client | Azure Standard_F32als_v6; AMD EPYC 9V74; 32 physical cores; 62.8 GiB Linux RAM; benchmark process restricted to CPUs 0–15 |
| OS | Ubuntu 24.04; Linux 6.17.0-1022-azure on both hosts; no reboot during this batch |
| Network | Client 10.84.0.5 → server 10.84.1.4:6379, accelerated mlx5 path, kernel TCP |
| Server CPUs | 0–15 for all products; 16 Lavik workers; Redis/Valkey 1/2/4/8/16 I/O threads |
| Lavik storage | Six dedicated local NVMe, 1,919,850,381,312 B each; SPDK/VFIO; 8 GiB hugepages |
| Redis/Valkey storage | Dataset in DRAM; AOF and automatic snapshots disabled; validated RDB used only for configuration resets |

## Method and limits

- Method follows the [beta.1 peer report](../lavik-v0.1.0-beta.1-spdk-vs-peers-2026-09-18/README.md), covering its Redis/Valkey 10M group and Lavik 1B storage group. Dragonfly and Garnet are outside this batch. Latest stable peer versions were selected on 2026-10-13: [Redis 8.10.2](https://github.com/redis/redis/releases/tag/8.10.2), [Valkey 9.1.2](https://valkey.io/download/releases/v9-1-2/).
- Order: Lavik SPDK 10M, Lavik SPDK 1B, Redis, Valkey. One database and one load generator run at a time. Server CPU/IRQ policy is not changed during this group. Builds finish before measurement.
- Standard package `lavik-v0.1.0-linux-x86_64.tar.gz`; source `feb61224e414c5653fd09560e20c9d214db1e3ac`; executable SHA-256 `7bf8d204a137678cee0e1a621b02661181373fbbfe1366bc1c201942b9dcf420`; GCC 13 release / x86-64-v2. No Lavik rebuild or source patch. Archive checksum matches the published checksum.
- Lavik: 16 pinned workers; busy-poll 20 µs; foreground/background budgets 1000/10 µs; background warrant 1%; SPDK completion cap 16 and foreground pre-poll 5 µs; metrics listener disabled. `defrag-paused=no`, `max_active_per_device=8`, block/record sleeps zero. Tomb Raider interval remains 0 as in this GET/SET reference: that switch controls tombstone sweeping, not defrag. Defrag state is checked before every formal point and in both enclosing snapshots.
- The six disks are matched by serial, PCI address, namespace and capacity before clearing. OS and workspace disks are excluded. Independent 10M and 1B Lavik datasets are loaded, clearing the six scratch devices between them. SPDK controllers are returned to the kernel after the clean Lavik stop; the hugepage reservation is released before subsequent tests.
- Redis/Valkey are built from their pinned 8.10.2 / 9.1.2 release commits with jemalloc 5.3.0. Each product loads a fresh 10M dataset and saves a hash-verified RDB. Each I/O-thread configuration starts from that baseline, with AOF and scheduled snapshots disabled. Other settings retain product defaults.
- 10M group: memtier 2.5.1, 16 client threads, pipeline 1, no rate limit; decimal keys 1..10,000,000 without prefixes; uniform random GET or overwriting SET, 1,024 B values. Connections 80/160/320/640/1280; 30 seconds per point; 10-second GET warmup per server configuration. The original 10M method's default correlated client seeds are retained.
- The 1B group uses decimal keys 1..1,000,000,000, uniformly random GET or overwriting SET, and 80/160/320/640/1280 connections, 60 seconds per point, distinct client seeds, and no extra GET warmup, following the original storage-group method. Shutdown checkpoint is enabled. The 1.024 TB raw value payload excludes keys and storage/index overhead.
- Exact DBSIZE and five sampled value lengths are checked before and after each server configuration. All measured GETs have zero misses; all formal points have zero connection errors. The latency percentiles are whole-window client measurements. Server CPU columns enclose setup/INFO overhead and are diagnostics, not exact-window CPU utilization.
- Each point is measured once; confidence intervals and run-to-run variability are not estimated. Products run sequentially, not interleaved. Thread roles differ, and Lavik persistence differs from the memory-only peer configurations. The new client CPU/model and release build differ from historical reports, so historical performance changes cannot be attributed solely to Lavik code.

## Evidence and offline reproduction

[All 120 points](results.csv) · [Validation](validation.json) · [Release](release.json) · [Peer build identity](evidence/builds.json) · [Version selection](latest-release-selection.json) · [Server inventory](server-inventory.json) · [Client inventory](client-inventory.json) · [Network path](network-path.json) · [Raw evidence manifest](SHA256SUMS)

The `evidence/` directory contains acquisition scripts, per-point command arrays, memtier JSON/text, enclosing-window host/INFO snapshots, before/after exact counts and sampled value lengths, driver changes and clean-stop status. Private keys, executable downloads and generated databases are excluded.

```sh
python3 perf_reports/lavik-v0.1.0-spdk-vs-latest-2026-10-13/verify_results.py
python3 perf_reports/lavik-v0.1.0-spdk-vs-latest-2026-10-13/build_assets.py
```

The verifier uses Python 3.11+ and runs offline. Figures require Matplotlib 3.11.2 and are exported as SVG and PNG. Acquisition scripts are host-specific and can erase only the six explicitly allowlisted scratch devices; do not run them on arbitrary disks.

## Redis / Valkey persistence configuration

Periodic RDB snapshots and AOF are both disabled throughout load, warmup and measurement. The explicit settings are committed in [redis-valkey-persistence.conf](redis-valkey-persistence.conf):

```conf
save ""
appendonly no
```

The acquisition commands apply the equivalent `--save "" --appendonly no`; full commands are archived under `evidence/memory/{redis,valkey}/io*/server-command.json`. One manual `SAVE` runs after each product’s initial 10M fill, outside every formal window, to create the validated RDB baseline used when restarting with another I/O-thread count. Reading that baseline at startup does not enable periodic snapshots. Before and after each I/O-thread sweep, CONFIG GET confirms save is empty and appendonly is no; INFO persistence confirms no background save and no AOF. The effective values are archived in persistence-before.json and persistence-after.json. Measured writes are memory-only.
