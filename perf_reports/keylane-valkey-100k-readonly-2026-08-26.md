# Keylane 100K QPS read test with valkey-benchmark

Date: 2026-08-26 UTC

## Setup

- Keylane commit: `5ad8c1ff2fd0`
- Keylane endpoint: `10.0.0.4:6379`
- Keylane process: 15 workers pinned to CPUs 0-14, SPDK data files on two NVMe namespaces
- Dataset already in Keylane: 200,000,000 keys named `kv_1` through `kv_200000000`; values are 1-4 KB and total about 500 GB
- valkey-benchmark commit: `382a134959b7`
- Client: 80 connections, 8 client threads, pipeline depth 1
- Rate: 100,000 requests/s globally
- Timing: 5 seconds warmup followed by 30 seconds measured

The benchmark dataset contains 4,000,000 unique keys generated with:

```text
key(i) = kv_(((i * 1000003) mod 200000000) + 1)
```

Because 1,000,003 and 200,000,000 are coprime, this is a permutation over the full key range. It avoids the zero-padding behavior of `__rand_int__` and avoids repeated keys during the 3.5 million warmup plus measured requests.

## Command

```bash
/mnt/dev/keylane/valkey-src/src/valkey-benchmark \
  -h 10.0.0.4 -p 6379 -c 80 --threads 8 -P 1 \
  --warmup 5 --duration 30 --rps 100000 --precision 3 \
  --dataset /mnt/dev/keylane/perf_runs/valkey-keylane-100k-20260826/keys.csv \
  GET '__field:key__'
```

## Result

| Metric | Result |
|---|---:|
| Completed requests | 3,000,001 |
| Throughput | 99,893.48 requests/s |
| Average latency | 0.174 ms |
| p50 | 0.159 ms |
| p95 | 0.287 ms |
| p99 | 0.495 ms |
| p99.9 | about 0.807 ms |
| p99.99 | 1.207-1.303 ms |
| Maximum | 7.431 ms |
| Keylane process CPU | 1,070.11% average (10.701 cores) |
| Client CPU | 177% |

The p99.99 interval is bounded from the cumulative histogram: 2,999,639 observations were at or below 1.207 ms, while 2,999,722 were at or below 1.303 ms. Keylane remained active after the test, responded to `PING`, and retained `DBSIZE=200000000`.

Raw artifacts are under `perf_runs/valkey-keylane-100k-20260826/`.
