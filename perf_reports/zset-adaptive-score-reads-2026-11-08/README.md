# Adaptive bounded ZMSCORE leaf reads

**English** | [简体中文](README.zh-CN.md)

Single-connection throughput is **4.29–4.36×** baseline; 32/320-connection paired changes remain within **±0.03%**. Smaller ZSCORE/ZRANK differences are mixed across pairs and are not claimed as stable gains.

Low concurrency can overlap leaf reads; high concurrency already overlaps commands and therefore reduces optional within-command fanout. The separate read-only device probe reached approximately 1.467 GB/s for 8/16 KiB reads, matching the high-concurrency ZMSCORE plateau. This is evidence for a device/backend throughput limit in this setup, not proof of an SPDK defect.

Baseline: `a9756dca5f99dc21604d72b615d590f9693552ce`; Bycorf: `ad14f2508c9a98bc09de117368bc483ea387eadf`. Release builds, test faults disabled during measurement. Server: Azure L16aos_v4 (16 vCPUs, 128 GiB); separate L32aos_v4 load host. The same SPDK controller holds this single key. SPDK poll completion limit 16; 8 GiB DPDK memory; defrag paused and tomb raider disabled for this focused diagnostic. These are **not** the normal-maintenance Gaming workload measurements.

One ZSET with 1,000,000 distinct 32-byte members and `score = member_id * 1024`. Command completion timing is not a synchronous-durability guarantee. Engine read counters are diagnostic counters, not independent hardware telemetry. System maintenance timers were temporarily held with rollback and restored after the phase. No continuous monitor or perf sampling ran in formal windows.

[Window receipts, source/binary identities and functional test names](results.json) · [Exact benchmark client source](client.cpp) · [Sorted initialization loader](loader.cpp).

## Measurement

Initialize once on main using the included loader: sorted 8,192-member ZADD batches. Six recovered-server visits in main/candidate/candidate/main/main/candidate order then use this unchanged read-only dataset. Every visit uses 3 s warmup + 30 s timing per case, with full member/score verification before and after; 13 full verifications including initialization. ZMSCORE has 32 operands. Up to 32 load threads, one outstanding command per connection; visits 1→2, 4→3 and 5→6 are paired.

| Command | Connections | Main median ops/s | Candidate median ops/s | Paired changes |
| --- | ---: | ---: | ---: | --- |
| ZMSCORE (32 members) | 1 | 423.4 | 1815.5 | +329.99% / +336.37% / +328.72% |
| ZMSCORE (32 members) | 32 | 6972.7 | 6972.4 | -0.01% / +0.01% / -0.00% |
| ZMSCORE (32 members) | 320 | 7228.6 | 7228.2 | +0.01% / -0.00% / -0.03% |
| ZRANK | 320 | 84805.6 | 85835.2 | +2.77% / +0.69% / -0.12% |
| ZSCORE | 32 | 128690.5 | 135837.1 | +6.03% / -1.88% / +6.22% |


## Validation and reproduction

12 fault-enabled checks and 8 Release checks passed, with zero skips. The exact test names and candidate patch/source/binary SHA-256 identities are in `results.json`. Candidate sources were checked against those identities again before publishing.

The Linux C++20 client is self-contained. Required arguments are explicit; optional arguments equal to client defaults are omitted. Use dedicated test data and the server configuration above. The short client commands below do not automate server lifecycle, counter collection or full experimental sequencing.

```bash
g++ -O3 -std=c++20 -pthread client.cpp -o client
g++ -O3 -DNDEBUG -std=c++20 -pthread loader.cpp -o loader
# Initialize once on main using sorted 8192-member batches.
# Checkpoint, stop, and restart before every measured visit.
./loader --host "$DB_HOST" --key zbench:single --op score --phase load --members 1000000 --connections 1 --threads 1 --seconds 1 --seed 101 --out load.json
# After recovery, run warmup and measurement separately. Repeat with C=1/32/320.
./client --host "$DB_HOST" --key zbench:single --op mscore32 --phase run --members 1000000 --connections "$C" --threads 32 --seconds 3 --seed 1107 --out warm.json
./client --host "$DB_HOST" --key zbench:single --op mscore32 --phase run --members 1000000 --connections "$C" --threads 32 --seconds 30 --seed 1107 --out result.json
```
