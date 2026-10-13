# Lavik v0.1.0 and Aerospike: YCSB throughput and tail latency

**English** | [简体中文](README.zh-CN.md)

Both databases were freshly loaded with **100 million records, each containing 10 × 128 B fields**. The complete **A/B/C/D × 100K/unlimited** matrix contains 16 measured windows of 300 seconds each; all warmup and measured phases have zero errors. **Lavik defrag stays enabled throughout.**

Aerospike uses default performance settings and original host policy. Lavik uses 12 workers with 4 CPUs for system/IRQ isolation and io_uring on a raw RAID0 partition. CPU allocation and database settings differ between these deployments.

Measured unlimited total-throughput differences relative to Aerospike: A **+4.36%**, B **+25.13%**, C **+28.76%**, D **+25.56%**.

Bars use the left total-QPS axis; lines use the right per-operation p99.9-latency axis (ms). Each figure uses its own axis ranges. Reads and writes are measured separately; C has no writes. Operation cells in the tables show **operation QPS; p99 / p99.9 (ms)**.

## 100K target

![100K target: throughput and p99.9 latency](ycsb-100k.png)

| Workload | Operation | Aerospike | Lavik |
|---|---|---:|---:|
| A | Total QPS | 99,883 | 99,966 |
| A | READ | 49,932; 0.342 / 0.437 | 49,987; 0.372 / 0.530 |
| A | UPDATE | 49,951; 0.292 / 0.385 | 49,979; 0.337 / 0.474 |
| B | Total QPS | 99,827 | 99,967 |
| B | READ | 94,829; 0.374 / 0.498 | 94,968; 0.482 / 0.856 |
| B | UPDATE | 4,998; 0.321 / 0.426 | 4,999; 0.410 / 0.653 |
| C | Total QPS | 99,902 | 99,970 |
| C | READ | 99,902; 0.379 / 0.481 | 99,970; 0.406 / 0.495 |
| D-Uniform | Total QPS | 99,901 | 99,961 |
| D-Uniform | READ | 94,906; 0.351 / 0.442 | 94,962; 0.368 / 0.488 |
| D-Uniform | INSERT | 4,995; 0.311 / 0.404 | 4,999; 0.354 / 0.479 |

## Unlimited

![Unlimited: throughput and p99.9 latency](ycsb-unlimited.png)

| Workload | Operation | Aerospike | Lavik |
|---|---|---:|---:|
| A | Total QPS | 507,401 | 529,501 |
| A | READ | 253,704; 1.407 / 2.953 | 264,758; 2.105 / 5.083 |
| A | UPDATE | 253,697; 1.196 / 2.255 | 264,743; 1.532 / 2.831 |
| B | Total QPS | 461,404 | 577,373 |
| B | READ | 438,351; 1.274 / 2.313 | 548,492; 0.776 / 1.818 |
| B | UPDATE | 23,053; 1.087 / 1.853 | 28,881; 0.702 / 1.578 |
| C | Total QPS | 461,251 | 593,911 |
| C | READ | 461,251; 1.253 / 1.982 | 593,911; 0.707 / 1.556 |
| D-Uniform | Total QPS | 460,167 | 577,808 |
| D-Uniform | READ | 437,177; 1.246 / 2.055 | 548,915; 0.790 / 1.824 |
| D-Uniform | INSERT | 22,991; 1.077 / 1.754 | 28,893; 0.724 / 1.589 |

## Hardware and storage

- **Server**：Azure Standard_L16aos_v4, Japan West zone 2; AMD EPYC 9V74; 16 logical CPUs / 8 physical cores; 125.8 GiB Linux RAM
- **Client**：Azure Standard_F32als_v6; AMD EPYC 9V74; 32 physical cores, 62.8 GiB RAM; Java 17; YCSB process restricted to CPUs 0–15
- **OS**：Ubuntu 24.04; Linux 6.17.0-1022-azure on both machines; no reboot
- **Network**：One client 10.84.0.5 → server 10.84.1.4; kernel TCP over accelerated mlx5
- **Disks**：Six local NVMe, each 1,919,850,381,312 B; Linux RAID0, 512 KiB chunk
- **Partitions**：Two distinct raw partitions; each 1,099,494,850,560 B; 24 MiB alignment; no filesystem; fully zeroed before load
- **Monitoring**：Per-phase effective configuration, process, IRQ, storage and client counters; Lavik metrics enabled

## Data and workloads

- **A**：READ: 50%；UPDATE: 50%；INSERT: 0；Read distribution: Uniform
- **B**：READ: 95%；UPDATE: 5%；INSERT: 0；Read distribution: Uniform
- **C**：READ: 100%；UPDATE: 0；INSERT: 0；Read distribution: Uniform
- **D-Uniform**：READ: 95%；UPDATE: 0；INSERT: 5%；Read distribution: Fixed original 100M keys

Both databases start with 100,000,000 records. After the matrix, Aerospike has 108,497,069 records and Lavik has 110,271,711 records.

## Database and host configuration

- **Version**：Aerospike: Community Edition 8.1.2.4-4；Lavik: v0.1.0 standard x86_64 release
- **Storage**：Aerospike: `storage-engine device`, raw RAID0 partition；Lavik: `--storage=uring`, raw RAID0 partition
- **CPU**：Aerospike: Original host policy, CPUs 0–15; defaults `service-threads=80`, `auto-pin=none`；Lavik: 12 pinned workers on CPUs 0–11; system tasks, workqueue and NIC IRQs on CPUs 12–15
- **systemd**：Aerospike: Default system.slice, no affinity override；Lavik: lavik-bench.slice, AllowedCPUs/CPUAffinity=0–11
- **Flush**：Aerospike: Defaults: 1 MiB, 1000 ms；Lavik: 128 KiB write chunks, 100 ms
- **Cache**：Aerospike: Defaults: max-write-cache=64 MiB, post-write-cache=256 MiB, read-page-cache=false；Lavik: Release defaults
- **Index**：Aerospike: Default indexes-memory-budget=0；Lavik: Release defaults
- **GC / defrag**：Aerospike: Defaults；Lavik: Enabled; 8 active jobs/device; block and record sleep=0
- **READ**：Aerospike: All 10 bins；Lavik: HGETALL, all 10 fields
- **UPDATE**：Aerospike: REPLACE_ONLY, all 10 bins；Lavik: LAVIK.HREPLACE, all 10 fields
- **INSERT**：Aerospike: CREATE_ONLY；Lavik: HMSET, redis.scanindex=none
- **Binding**：Aerospike: YCSB Aerospike, Java client 3.1.2；Lavik: YCSB Redis, Jedis 3.9.0
- **Replication / TTL**：Aerospike: One node, RF=1, namespace ycsb; default TTL 0；Lavik: One instance DB0, no replica, no TTL

Lavik is the unmodified standard release package: `v0.1.0 / feb61224e414c5653fd09560e20c9d214db1e3ac`; SHA-256 `7bf8d204a137678cee0e1a621b02661181373fbbfe1366bc1c201942b9dcf420`. Aerospike SHA-256 `6e1b2bd6deebd5816f4f369ac1db4151217ed0be3b66578bdd6f15c24e6af8d0`.

## Measurement method and limits

- Each database starts with 100M freshly loaded records. Workloads run in C/A/B/D order, 100K then unlimited for each workload; Aerospike first, then Lavik. Databases share the RAID0 sequentially, never concurrently. There are no clears or restarts between measured cases.
- Each case uses 256 YCSB workers, a separate one-million-operation warmup JVM and a fresh 300-second measurement JVM. `-target 100000` is the total rate across threads and operations. The measurement operation cap is 2 billion, so the timer controls termination. Initial JIT/ramp-up stays in the measured window.
- `fieldcount=10`, `fieldlength=128`, constant field lengths; `readallfields=true`, `writeallfields=true`; hashed key names; uniform reads; no SCAN or read-modify-write; 10,000 ms client timeouts. Raw field payload is 128 GB (119.21 GiB), excluding keys, field names, indexes and record metadata.
- D reads only the original 100M keys. Warmup/measured inserts use disjoint starting IDs: 2B/4B at 100K, 6B/8B unlimited. Equal starting sizes do not imply equal final sizes because D insert counts depend on throughput.
- Every load reconciles exactly 100M successful INSERTs with the server count. Each database is stopped cleanly and recovered after loading. Eight distributed records, all ten field lengths and their SHA-256 values are compared before/after recovery. Every phase reconciles client successes with database command counters and record-count deltas; errors must remain zero.
- Aerospike configuration changes only required cluster/listener/namespace/RF/device settings. CPU scheduling, IRQs, threads, caches, flushing, index budget and reclamation retain defaults. Effective values are recorded before/after every phase. Lavik's separate 12+4 CPU/IRQ policy is applied only after Aerospike stops, and restored afterward. Completion IRQ destinations and per-CPU interrupt deltas verify that benchmark NIC completion IRQs do not execute on Lavik worker CPUs during phases.
- Lavik defrag is enabled throughout load, recovery, warmup and measurement, with 8 active jobs/device and no additional sleep. `CONFIG GET defrag-paused` and `DEFRAG STATUS` are captured per phase. Tomb Raider and active expiration use release defaults in this YCSB group. Periodic 100 ms flushing is not a per-write synchronous durability guarantee.
- CPU numbers are logical CPUs: workers 0–11 occupy six physical cores and system/IRQ CPUs 12–15 occupy two separate physical cores (SMT2). Source-based routing sends replies from 10.84.1.4 through eth1/table 101; the management interface is not the benchmark reply path.
- QPS = successful operations / actual YCSB RunTime. Total QPS sums successful reads and writes. Each operation's p99/p99.9 comes from its complete-window ordinary HDR histogram, not an average of interval percentiles; intended histograms remain in raw logs. Equal YCSB worker counts do not imply equal TCP connection counts.
- Three setup attempts failed before any records were inserted: missing Aerospike runtime directories, a missing `python` launcher alias, then listener readiness preceding partition ownership. Their logs are retained under `evidence/setup-failure-*`. Runtime prerequisites were completed and readiness now waits for all 4096 master partitions. The Java bindings and database performance settings were unchanged. None of these setup attempts is a measured window.
- The custom YCSB source is `eloqdata/YCSB@a47bf01c4f197d2ff2eecbbebd9743da5a1cae47`, with the recorded LAVIK.HREPLACE command-name patch. The exact binding source and JAR hashes are archived.
- One five-minute measurement per cell; run-to-run variability and confidence intervals are not estimated. Product order is fixed. CPU allocation, host policies, caches and persistence differ between the two requested deployments, so the result is not a single-variable comparison.

- Aerospike: 2026-10-13T09:54:41Z – 2026-10-13T10:35:42Z (UTC), 8 measured windows.
- Lavik: 2026-10-13T10:39:09Z – 2026-10-13T11:20:04Z (UTC), 8 measured windows.

At completion the original CPU/IRQ policy is restored, Lavik has stopped cleanly, and Aerospike has recovered with its final data verified. These counts describe the end of the YCSB matrix. After complete evidence archival and offline verification, the six scratch devices were rebuilt with authorization for the subsequent latest-peer and Lavik 1B batch; the YCSB datasets are no longer retained.

## Evidence and reproduction

[Results JSON](results.json) · [Operation CSV](summary.csv) · [Binary identity](binaries.json) · [Server inventory](server-inventory.json) · [Client inventory](client-inventory.json) · [Network path](network-path.json) · [YCSB source](evidence/client-source/source.json) · [Client JAR hashes](evidence/client-jars.sha256) · [All phases](evidence/phases.json) · [Runner](evidence/runner.py) · [Host policy](evidence/host_policy.py) · [Client commands](evidence/ycsb_client.py) · [Effective Aerospike configuration](evidence/aerospike.conf) · [Storage](evidence/storage-after.json) · [Completion](evidence/complete.json) · [Checksums](SHA256SUMS)

```sh
python3 perf_reports/lavik-v0.1.0-aerospike-ycsb-2026-10-13/verify_report.py
python3 perf_reports/lavik-v0.1.0-aerospike-ycsb-2026-10-13/build_assets.py
```

Offline verification needs Python 3.11+; figure generation needs Matplotlib 3.11.2. The verifier checks both fresh loads, all 32 phases, 16 measured windows, 28 operation rows, per-phase host/database policy, both language tables and archived file hashes. It never starts databases or opens raw devices. A live rerun needs new scratch media and unused D insert ranges. Private keys, executable downloads and generated databases are not included.
