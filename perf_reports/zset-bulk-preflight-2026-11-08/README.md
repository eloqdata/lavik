# Skip impossible fixed-topology ZADD preflights

**English** | [简体中文](README.zh-CN.md)

Paired member-throughput gains: **+27.90% / +30.72% / +29.69%**. This applies to sorted bulk ingestion beyond an ordered endpoint; it is not a measured point-update or Gaming mixed-workload gain.

Baseline: `a9756dca5f99dc21604d72b615d590f9693552ce`; Bycorf: `ad14f2508c9a98bc09de117368bc483ea387eadf`. Release builds, test faults disabled during measurement. Server: Azure L16aos_v4 (16 vCPUs, 128 GiB); separate L32aos_v4 load host. The same SPDK controller holds this single key. SPDK poll completion limit 16; 8 GiB DPDK memory; defrag paused and tomb raider disabled for this focused diagnostic. These are **not** the normal-maintenance Gaming workload measurements.

One ZSET with 1,000,000 distinct 32-byte members and `score = member_id * 1024`. Command completion timing is not a synchronous-durability guarantee. Engine read counters are diagnostic counters, not independent hardware telemetry. System maintenance timers were temporarily held with rollback and restored after the phase. No continuous monitor or perf sampling ran in formal windows.

[Window receipts, source/binary identities and functional test names](results.json) · [Exact benchmark client source](client.cpp).

## Measurement

Six fresh empty-database loads in main/candidate/candidate/main/main/candidate order; one connection/thread, 8,192 members per ZADD (last batch partial), 123 commands per load. Every load was followed by full member/score verification. Pairings are visits 1→2, 4→3 and 5→6. No warmup or cross-visit cache pooling.

| Visit | Binary | Elapsed s | Members/s |
| --- | --- | ---: | ---: |
| bulk-1-main-load | main | 65.156270 | 15347.7 |
| bulk-2-bulk-load | bulk | 50.944261 | 19629.3 |
| bulk-3-bulk-load | bulk | 50.509178 | 19798.4 |
| bulk-4-main-load | main | 66.025405 | 15145.7 |
| bulk-5-main-load | main | 65.936463 | 15166.1 |
| bulk-6-bulk-load | bulk | 50.840372 | 19669.4 |


## Validation and reproduction

14 fault-enabled checks and 9 Release checks passed, with zero skips. The exact test names and candidate patch/source/binary SHA-256 identities are in `results.json`. Candidate sources were checked against those identities again before publishing.

The Linux C++20 client is self-contained. Required arguments are explicit; optional arguments equal to client defaults are omitted. Use dedicated test data and the server configuration above. The short client commands below do not automate server lifecycle, counter collection or full experimental sequencing.

```bash
g++ -O3 -DNDEBUG -std=c++20 -pthread client.cpp -o client
# Start the chosen binary on an empty, dedicated test database.
./client --host "$DB_HOST" --key zbench:single --op score --phase load --members 1000000 --connections 1 --threads 1 --seconds 1 --seed 101 --out load.json
./client --host "$DB_HOST" --key zbench:single --op score --phase verify --members 1000000 --connections 1 --threads 1 --seconds 1 --seed 101 --out verify.json
```
