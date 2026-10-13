# Lavik v0.1.0 on 2 vCPUs: GET/SET versus Redis and Valkey

> Follow-up: [latest Redis / Valkey and a fourth Lavik 100M-key curve](../lavik-v0.1.0-2vcpu-latest-capacity-2026-10-13/README.md). All curves are freshly measured with independent client seeds; this historical report is preserved.

**English** | [简体中文](README.zh-CN.md)

Test date: 2026-10-13. All **70 formal points** were freshly measured on one
Azure **L2aos_v5, 2 vCPU / 16 GiB** instance. Each product uses the same two
vCPUs; these are **two SMT threads of one physical core**. The dataset contains
**1 million keys × 1 KiB values**, with no artificial memory restriction.

Lavik's observed peak GET is **215.5k QPS** at 640 connections
(p99.9 **7.167 ms**); peak SET is **232.7k QPS** at
640 connections (p99.9 **8.191 ms**).
Against each peer's best observed configuration, the peak-throughput differences
are **+10.1% / -1.3% for GET** and
**+22.9% / +15.6% for SET**
(Redis / Valkey). These are single-run observations, not confidence-bounded estimates.

Lavik GET is already near its plateau at 160 connections: 215,230 QPS with
p99.9 of 1.791 ms. At 16–80 GET connections, both peers are faster. Lavik’s SET
throughput lead appears at 160 connections and above; its p99.9 is not
consistently the lowest.

## Throughput and tail latency

![Throughput](throughput.png)

| Command | System | Threads¹ | Connections | QPS | Avg ms | p50 ms | p99 ms | p99.9 ms | p99.99 ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| GET | Lavik 0.1.0 SPDK | 2 | 640 | 215,479 | 2.969 | 2.975 | 3.759 | 7.167 | 10.047 |
| GET | Redis 8.8.0 | 1 | 640 | 195,703 | 3.269 | 3.103 | 5.791 | 7.103 | 10.623 |
| GET | Valkey 9.1.0 | 2 | 160 | 218,285 | 0.733 | 0.735 | 0.831 | 1.367 | 2.783 |
| SET | Lavik 0.1.0 SPDK | 2 | 640 | 232,675 | 2.750 | 2.735 | 4.127 | 8.191 | 12.927 |
| SET | Redis 8.8.0 | 1 | 1280 | 189,382 | 6.757 | 6.495 | 12.671 | 15.999 | 22.271 |
| SET | Valkey 9.1.0 | 2 | 640 | 201,209 | 3.180 | 3.167 | 3.759 | 6.911 | 9.599 |

![p99.9 latency](latency.png)

For each command and peer, select the **one I/O-thread setting with the highest
measured peak**, then retain that setting for its entire QPS and latency curve.
The table shows each product's own peak point; concurrency can differ.
¹ Lavik counts data workers; Redis/Valkey count I/O threads. All are confined to
the same two server vCPUs.

## Comparison at matched concurrency

The same fixed configuration selection is used below. Cells show **k QPS / p99.9 ms**.

| Command | Connections | Lavik | Redis | Valkey |
|---|---:|---:|---:|---:|
| GET | 16 | 27.8 / 0.927 | 30.4 / 0.927 | 30.7 / 0.975 |
| GET | 32 | 54.7 / 0.999 | 60.1 / 0.919 | 60.8 / 0.959 |
| GET | 80 | 136.3 / 2.007 | 148.6 / 0.991 | 149.9 / 1.439 |
| GET | 160 | 215.2 / 1.791 | 192.7 / 1.599 | 218.3 / 1.367 |
| GET | 320 | 215.4 / 3.423 | 189.3 / 3.135 | 211.3 / 6.335 |
| GET | 640 | 215.5 / 7.167 | 195.7 / 7.103 | 217.5 / 6.719 |
| GET | 1280 | 213.9 / 20.735 | 195.1 / 15.359 | 211.7 / 14.079 |
| SET | 16 | 30.9 / 0.943 | 29.8 / 0.831 | 30.1 / 1.535 |
| SET | 32 | 60.4 / 1.151 | 60.2 / 0.919 | 60.8 / 0.855 |
| SET | 80 | 142.4 / 1.879 | 147.3 / 0.871 | 149.7 / 1.215 |
| SET | 160 | 213.4 / 2.207 | 183.9 / 1.471 | 199.7 / 4.607 |
| SET | 320 | 231.6 / 3.071 | 183.1 / 3.199 | 199.3 / 2.543 |
| SET | 640 | 232.7 / 8.191 | 187.5 / 7.487 | 201.2 / 6.911 |
| SET | 1280 | 228.2 / 17.791 | 189.4 / 15.999 | 198.8 / 17.919 |

## All I/O-thread settings

![All thread settings](all-configurations.png)

| System | I/O threads | Peak GET QPS @ connections | Peak SET QPS @ connections |
|---|---:|---:|---:|
| Redis 8.8.0 | 1 | 195,703 @ 640 | 189,382 @ 1280 |
| Redis 8.8.0 | 2 | 192,909 @ 640 | 184,093 @ 320 |
| Valkey 9.1.0 | 1 | 192,523 @ 1280 | 186,607 @ 1280 |
| Valkey 9.1.0 | 2 | 218,285 @ 160 | 201,209 @ 640 |

## Hardware and configuration

| Item | Configuration |
|---|---|
| Server | One Azure Standard_L2aos_v5, East US zone 3; nominal 2 vCPU / 16 GiB; Linux reports 15.62 GiB RAM |
| CPU topology | AMD EPYC 9V45; **2 SMT threads of one physical core**, both available to every product |
| Client | Separate Standard_F16as_v7 in the same zone; AMD EPYC 9V45; 16 physical cores; memtier pinned to CPUs 0–15 |
| OS | Ubuntu 24.04; Linux 6.17.0-1022-azure on both hosts |
| Network | Private addresses, same VNet and zone; accelerated MANA VF; kernel TCP |
| Lavik storage | One dedicated local NVMe namespace, 1,439,887,785,984 B; SPDK/VFIO; temporary 2 GiB hugepage reservation |
| Peer storage | Data in DRAM; no AOF or automatic RDB snapshots; baseline RDB loading only between configurations |
| Server placement | All product processes and threads limited to CPUs 0–1, sharing those CPUs with NIC interrupts; no memory cgroup limit |
| Host policy | THP `never`, `vm.overcommit_memory=1`, no swap; same policy for all products |

The initial inventories precede host-policy tuning; the effective policy is
recorded in `evidence/config.json` and `evidence/post-run.json`.

All products run sequentially on **the same L2aos_v5 instance**, so CPU, RAM,
network and host placement do not differ between them. Redis and Valkey do not
use its local NVMe for timed operations. This isolates the 2-vCPU comparison;
it is not a comparison of the cheapest VM or equal-cost deployment for each
product. No memory pressure, eviction or artificially low memory cap is used.

## Method

The scenario follows the [16-vCPU v0.1.0 Redis/Valkey report](https://github.com/thweetkomputer/lavik/blob/34eb4259bbdc954dd5c0b0f41c7e7f3f17caf9d7/perf_reports/lavik-v0.1.0-redis-valkey-2026-10-13/README.md).
The dataset is reduced from 10 million to **1 million keys**, and the server
CPU budget from 16 to **2 vCPUs**. Connections 16 and 32 are added to the original
80/160/320/640/1280 sweep. The SPDK reservation is reduced from 8 GiB to 2 GiB;
Redis/Valkey I/O-thread settings are 1 and 2. All comparison results are newly
measured; no historical Redis/Valkey numbers are reused.

- Decimal keys `1..1000000`, no key prefix, 1,024-byte values, no TTL. Total value payload is 0.954 GiB, before keys and metadata. Each product loads its own fresh dataset.
- memtier 2.5.1, 16 client threads, pipeline 1, no rate limit. Separate uniform-random GET and overwriting SET workloads; 30 seconds per formal point. A 10-second GET warmup precedes each server configuration. Default correlated client seeds are retained to match the earlier memory-peer scenario.
- Order: Lavik, Redis IO=1, Redis IO=2, Valkey IO=1, Valkey IO=2. Within a configuration, GET then SET, with increasing concurrency. Builds finish before any measurement. Each formal point is measured once.
- Official standard x86_64 Lavik v0.1.0 release, source `feb61224e414c5653fd09560e20c9d214db1e3ac`, with no rebuild or patch. Archive and binary hashes are recorded in [binaries.json](binaries.json).
- Lavik has 2 pinned data workers; kernel TCP and SPDK storage; busy-poll 20 µs; foreground/background budgets 1000/10 µs; background warrant 1%; SPDK completion cap 16 and foreground pre-poll 5 µs. The metrics listener is disabled.
- Defrag remains enabled during load, warmup and all measurements: `defrag-paused=no`, max active per device 8, block/record sleeps zero. Tomb Raider interval 0 follows the reference and disables tombstone sweeping, not defrag. State is checked around each measured point.
- The empty local NVMe is matched by serial, PCI address, namespace, capacity and lack of mounted partitions before discard. Lavik stops cleanly, the NVMe returns to the kernel, and hugepage/VFIO settings are restored before the peer tests.
- Redis 8.8.0 and Valkey 9.1.0 use their published sources, default release builds and jemalloc 5.3.0. Both use `save ""`, `appendonly no`, `maxmemory 0`. One manual `SAVE`, outside every formal window, creates a baseline that is loaded for each I/O-thread configuration. Measured peer writes are memory-only. Other product settings retain defaults.
- Exact `DBSIZE` and five sampled `STRLEN` values are checked before and after each configuration. The verifier checks all formal client outputs for zero GET misses and connection errors, reconciles throughput against counts and durations, verifies percentiles, checks CPU placement, and confirms clean server exits.

Readiness probes can receive connection refusals or `LOADING` while a server
starts and restores its baseline RDB, before warmup. No command rejection/failure
counter increases during any formal enclosing window. The highest per-window mean memtier CPU
usage is 3.564 core-equivalents out of 16; measured server steal is 0%.

## Interpretation limits

This is a **small-instance, single-node GET/SET benchmark**, not a low-memory
stress test. It does not measure persistence-equivalent peers, replication,
failover, TTL, mixed commands, large data types, pipeline scaling, or long-term
SSD/defrag steady state. Local NVMe is temporary storage; this run does not
establish survival through VM deallocation or host loss.

Each point is a single 30-second observation. There are no repetitions,
confidence intervals or order randomization; small differences cannot establish
statistical significance. Default correlated seeds can change cache locality
and repeated-key contention versus independent client streams. The saturation
curves and p99.9 at unequal peak loads are not fixed-rate SLA comparisons.

Thread counts describe different roles: Lavik data workers versus Redis/Valkey
I/O threads. The **total available server CPU budget remains two vCPUs**. Client
CPU and enclosing server CPU/steal/RSS diagnostics are in [results.csv](results.csv).
Server snapshots include INFO/SSH overhead and are not exact-window utilization.
RSS alone does not account for the full SPDK hugepage reservation and should
not be used to claim total host-memory savings.

The earlier 16-vCPU report used another VM generation, CPU model, network path,
storage layout and dataset size. These runs are therefore not a controlled
2-versus-16-vCPU scaling experiment.

## Evidence and reproduction

[All 70 points](results.csv) · [Validation](validation.json) · [Binary identity](binaries.json) · [Server inventory](server-inventory.json) · [Client inventory](client-inventory.json) · [Reproduction guide](REPRODUCE.md) · [SHA-256 manifest](SHA256SUMS)

`evidence/` contains the acquisition scripts, effective config, raw memtier
JSON/text, commands, server logs, client CPU records, enclosing host/INFO
snapshots, dataset checks, clean exits and SPDK restoration records. Binaries,
generated databases, SSH private keys and cloud credentials are excluded.

```sh
python3 verify_results.py
python3 build_assets.py  # Matplotlib 3.11.2
shasum -a 256 -c SHA256SUMS
```

The offline verifier needs only Python 3.11+. Figures are exported as SVG and
PNG. The acquisition scripts retain the original private IPs and the explicit
scratch-device allowlist; read [REPRODUCE.md](REPRODUCE.md) before adapting them.
