# Lavik v0.2.0: 1B keys with 32 client threads

**English** | [简体中文](README.zh-CN.md)

A fresh **1B-key × 1 KiB-value** dataset was loaded on a newly provisioned Azure 16-vCPU server. **32 memtier threads on client CPUs 0–31** measure **160/320/640/1280 total connections**. All **8 formal points** have zero connection errors and zero GET misses. This report covers only the 1B dataset, with no 10M baseline or capacity-retention metric.

Measured GET / SET peaks are **1,129,597 / 910,356 ops/s**, at **1280 / 1280 total connections**, respectively.

UTC: 2026-10-21T16:55:31.378000+00:00 – 2026-10-21T17:03:39.576000+00:00.

![Throughput and p99.9](combined.svg)

| Command | Connections | Client threads | Ops/s | p50 ms | p99 ms | p99.9 ms | p99.99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GET | 160 | 32 | 375,966 | 0.423 | 0.743 | 0.823 | 0.927 |
| GET | 320 | 32 | 652,881 | 0.495 | 0.815 | 0.943 | 1.471 |
| GET | 640 | 32 | 985,019 | 0.631 | 1.247 | 1.751 | 3.743 |
| GET | 1280 | 32 | 1,129,597 | 1.095 | 2.159 | 4.015 | 8.575 |
| SET | 160 | 32 | 409,886 | 0.391 | 0.687 | 0.783 | 1.055 |
| SET | 320 | 32 | 701,241 | 0.447 | 0.815 | 0.959 | 1.519 |
| SET | 640 | 32 | 861,911 | 0.735 | 1.271 | 1.727 | 3.839 |
| SET | 1280 | 32 | 910,356 | 1.391 | 2.239 | 4.319 | 8.575 |

## Environment and method

| Item | Configuration |
| --- | --- |
| Server | Standard_L16aos_v4; Japan East zone 2; AMD EPYC 9V74; 8 physical cores / 16 vCPUs; 128 GiB nominal RAM |
| Client | Standard_F32als_v6; 32 physical CPUs; memtier 2.5.1; 32 threads pinned to CPUs 0–31 |
| OS | Ubuntu 24.04; Linux 6.17.0-1022-azure on both VMs |
| Storage | 6 × 1,919,850,381,312-byte dedicated NVMe; SPDK/VFIO; 8 GiB EAL/hugepage reservation |
| Network | Private VNet; kernel TCP; accelerated networking |
| Lavik | Official standard x86_64 v0.2.0; 2908f86f2770e9c6e8acc6585b3bedb4b44d1cd5 |

- Decimal keys 1..1,000,000,000, no prefix or TTL, 1,024-byte values: 953.67 GiB logical value payload, excluding keys, index and storage overhead.
- Fresh sequential fill: exactly 1B SETs, 32 threads and 640 total connections. Formal sweeps use independent client seeds, uniform random GET or overwriting SET, pipeline 1, and 60 seconds per point. GET precedes SET; concurrency increases within each sweep. No additional GET warmup.
- Lavik uses 16 server workers, SPDK, shutdown checkpointing, tomb-raider interval 0 and SPDK completion cap 16. Other tested defaults are inherited. Defrag stays enabled and is checked around every measured point. Exact DBSIZE and five value lengths are verified before and after the sweep.
- Each cell has one run; no confidence intervals. Throughput and percentiles come from the complete measurement window. These eight points are reused in the storage-peer report, not counted as independent repeats.
- The historical report used 16 client threads and included 80 connections. This run uses 32 threads and omits that point. Region also changed from Japan West to Japan East.

Load invocation wall time: **21.6 minutes**; reported fill throughput: **940,654 ops/s**. Across formal points, maximum average client CPU use is **17.971 cores**, maximum individual thread average is **0.579 cores**, and maximum server steal in enclosing sampled windows is **0.0000%**.

[Point data](results.csv) · [Provenance](provenance.json) · [Validation](validation.json) · [Raw logs and receipts](raw-results.tar.gz)

## Reproduction commands

Use the [SPDK operations guide](../../docs/operations/spdk-storage.md) to verify six dedicated test devices and prepare VFIO with 8 GiB hugepages. Load only empty test media; wait for PING and check defrag-paused is no. Stop cleanly after measurement, then restore kernel drivers and hugepage reservations using the guide.

```bash
sudo prlimit --memlock=unlimited:unlimited --nofile=65535:65535 env \
  BYCORF_EAL_ARGS='-a 7133:00:00.0 -a 10fc:00:00.0 -a 9c4a:00:00.0 -a ef09:00:00.0 -a fa44:00:00.0 -a 9816:00:00.0' \
  BYCORF_DPDK_MEMORY_MB=8192 \
  taskset -c 0-15 /opt/bench020/downloads/lavik-v0.2.0-linux-x86_64/lavik \
  --storage=spdk --bind=10.96.1.4 --shutdown-checkpoint \
  --tomb-raider-interval-ms=0 --spdk-max-completions-per-poll=16 \
  --log-dir=/opt/bench020/results/large/storage/lavik-1b/logs \
  --data-file=spdk://7133:00:00.0/1 \
  --data-file=spdk://10fc:00:00.0/1 \
  --data-file=spdk://9c4a:00:00.0/1 \
  --data-file=spdk://ef09:00:00.0/1 \
  --data-file=spdk://fa44:00:00.0/1 \
  --data-file=spdk://9816:00:00.0/1
```

Run on the separate client:

```bash
SERVER=10.96.1.4
mkdir -p results
ulimit -n 65535
COMMON=(--server="$SERVER" --threads=32 --key-minimum=1
  --key-maximum=1000000000 --key-prefix= --data-size=1024
  --hide-histogram --print-percentiles=50,99,99.9,99.99 --show-config)
# 32 threads × 20 connections × 1,562,500 requests = 1,000,000,000 SETs.
taskset -c 0-31 memtier_benchmark "${COMMON[@]}" --clients=20 \
  --requests=1562500 --ratio=1:0 --key-pattern=P:P --json-out-file=results/fill.json
redis-cli -h "$SERVER" --raw DBSIZE
for key in 1 2 500000000 999999999 1000000000; do
  test "$(redis-cli -h "$SERVER" --raw STRLEN "$key")" = 1024
done
# No additional GET warmup for Lavik 1B.
for op in GET SET; do
  COMMAND=(--command='GET __key__' --command-is-read)
  if [[ "$op" == SET ]]; then COMMAND=(--command='SET __key__ __data__'); fi
  for connections in 160 320 640 1280; do
    redis-cli -h "$SERVER" CONFIG GET defrag-paused
    taskset -c 0-31 memtier_benchmark "${COMMON[@]}" \
      --clients="$((connections / 32))" --test-time=60 --distinct-client-seed \
      "${COMMAND[@]}" --json-out-file="results/$op-c$connections.json" \
      > "results/$op-c$connections.txt" 2>&1
    redis-cli -h "$SERVER" CONFIG GET defrag-paused
  done
done
redis-cli -h "$SERVER" --raw DBSIZE
```
