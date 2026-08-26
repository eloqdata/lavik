# Keylane SPDK 500G readonly benchmark (15 cores, 2026-08-25)

## Result

Keylane served a 200,000,000-key dataset from two raw SPDK NVMe namespaces with a 100% hit rate and no connection errors in either 300-second read run.

| Client load | GET/s | Avg latency | p50 | p90 | p99 | p99.9 | p99.99 | Server CPU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Unlimited | 309,215.97 | 0.25836 ms | 0.255 ms | 0.343 ms | 0.447 ms | 0.575 ms | 1.639 ms | 1,418.51% |
| 100,000 QPS limit | 99,994.82 | 0.25756 ms | 0.247 ms | 0.351 ms | 0.575 ms | 1.015 ms | 2.191 ms | 544.53% |

The rate-limited run reached 99.995% of its target. The unlimited run used about 14.19 CPU cores on average; the 100K run used about 5.45 cores. The latter is not an efficiency measurement at identical throughput because Keylane keeps a 20 us busy-poll window and the client-side rate limiter changes request arrival timing.

## Server and storage

- Server: `10.0.0.4:6379`; metrics: `10.0.0.4:9100`.
- Keylane revision: `5ad8c1ff2fd04b97cc653409120b20e34b746ef1`.
- Celer revision: `fb8b2d9f0f71a9ddc1819db1f9ffbae9e0bec3b1`.
- Release build with `KEYLANE_WITH_SPDK=ON` and `KEYLANE_ENABLE_OPT=ON`.
- 15 Keylane workers pinned to CPUs 0-14. CPU 15 was excluded from the Keylane service for scheduler/housekeeping work. Network IRQs retained the documented distributed layout instead of being concentrated on one CPU.
- 4,096 2 MiB hugepages (8 GiB total).
- `/dev/nvme0n1`, BDF `fe8d:00:00.0`, and `/dev/nvme1n1`, BDF `9913:00:00.0`, were wiped and bound to `vfio-pci`.
- The Azure VM exposes no IOMMU groups, so SPDK required VFIO unsafe no-IOMMU mode. This is suitable for the dedicated benchmark VM but is not a production deployment recommendation.
- Usable capacity was 1,920,370,475,008 bytes per device. SPDK assigned controller `9913` to workers `[0,2,4,6,8,10,12,14]` and controller `fe8d` to `[1,3,5,7,9,11,13]`, with 256 I/O qpairs per controller.

Server command:

```bash
taskset -c 0-14 /mnt/dev/keylane/bld-spdk-rebase-release/keylane \
  --bind=10.0.0.4 --port=6379 --metrics-port=9100 \
  --threads=15 --pin-workers \
  --data-file=spdk://fe8d:00:00.0/1 \
  --data-file=spdk://9913:00:00.0/1 \
  --logtostderr
```

## Dataset fill

The client on `10.0.0.5` issued exactly 200,000,000 SETs using 16 threads and 40 clients per thread. Keys were `kv_1` through `kv_200000000`; values were uniformly sized from 1,000 to 4,000 random bytes. Expected value payload was 500,000,000,000 bytes. Keylane consumed 528,700,407,808 bytes including keys, record headers, alignment, and storage metadata.

Fill result: 588,630.94 SET/s, 1.08139 ms average latency, 0.887 ms p50, 5.631 ms p99, and 9.535 ms p99.9. `DBSIZE` was exactly 200,000,000 after the fill.

Reproduction command (run on `10.0.0.5`):

```bash
memtier_benchmark --server=10.0.0.4 --port=6379 --protocol=redis \
  --threads=16 --clients=40 --requests=312500 --ratio=1:0 \
  --key-pattern=P:P --key-prefix=kv_ \
  --key-minimum=1 --key-maximum=200000000 \
  --data-size-range=1000-4000 --random-data --hide-histogram
```

## Read commands

Both runs used 8 client threads, 10 connections per thread, random keys over the full 200M-key space, and a 300-second duration. No explicit warmup or server restart occurred between the fill and reads.

Unlimited:

```bash
memtier_benchmark --server=10.0.0.4 --port=6379 --protocol=redis \
  --threads=8 --clients=10 --test-time=300 --ratio=0:1 \
  --key-pattern=R:R --key-prefix=kv_ \
  --key-minimum=1 --key-maximum=200000000 --hide-histogram
```

100K aggregate limit (`1,250` requests/s for each of 80 connections):

```bash
memtier_benchmark --server=10.0.0.4 --port=6379 --protocol=redis \
  --threads=8 --clients=10 --test-time=300 --ratio=0:1 \
  --key-pattern=R:R --key-prefix=kv_ \
  --key-minimum=1 --key-maximum=200000000 \
  --rate-limiting=1250 --hide-histogram
```

## Why the 100K limiter has a worse tail

The higher rate-limited tail is not explained by having fewer observations. The 300-second limited run still contains 29,998,789 GETs: approximately 3,000 observations lie beyond p99.99, 30,000 beyond p99.9, and 300,000 beyond p99. The regression also starts at p99 and is visible in per-second percentile series. The formal pooled p99.99 is 2.191 ms with the limiter versus 1.639 ms without it.

The primary identified cause is the pacing implementation in memtier_benchmark 2.5.1. For each connection it computes:

```text
requests_per_interval = ceil(request_rate / 50)
events_per_second = request_rate / requests_per_interval
request_interval_us = 1,000,000 / events_per_second
```

At 1,250 requests/s per connection this becomes 25 requests of quota every 20 ms, not one request every 800 us. Each connection's timer replenishes that quota and calls `fill_pipeline()`. With 80 connections, timer phase alignment can expose Keylane to bursts of up to 2,000 newly eligible requests every 20 ms. Pipeline depth 1 serializes requests within a connection, but it does not turn the 20 ms quota replenishment into a smooth open-loop arrival process.

Current-period, same-dataset 60-second tests isolate the two leading hypotheses:

| Load shape / server setting | GET/s | Avg | p50 | p90 | p99 | p99.9 | p99.99 | Server CPU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| memtier 100K limiter, SPDK max completions 8 | 99,984.19 | 0.380 ms | 0.239 ms | 0.703 ms | 2.623 ms | 4.447 ms | 5.983 ms | 682.65% |
| memtier 100K limiter, SPDK max completions 1 | 99,986.33 | 0.384 ms | 0.239 ms | 0.703 ms | 2.623 ms | 4.447 ms | 5.919 ms | 685.55% |
| Closed loop, 20 connections | 99,851.03 | 0.200 ms | 0.199 ms | 0.255 ms | 0.327 ms | 0.479 ms | **0.983 ms** | 1,093.69% |
| Closed loop, 22 connections | 108,211.32 | 0.203 ms | 0.199 ms | 0.263 ms | 0.327 ms | 0.471 ms | 0.999 ms | 1,092.67% |
| Closed loop, 26 connections | 123,762.48 | 0.210 ms | 0.207 ms | 0.271 ms | 0.351 ms | 0.479 ms | 1.015 ms | 1,148.52% |

Changing `spdk-max-completions-per-poll` from 8 to 1 leaves p50, p90, p99, and p99.9 identical; p99.99 changes by only 0.064 ms and CPU by 0.03 core. An additional value of 0 (unlimited completions per poll) also did not improve the result. At 100K QPS the completion queue normally does not accumulate enough ready operations for this cap to bind, so it is not an effective low-rate latency control.

The 20-connection run is not a replacement for a controlled open-loop benchmark, but it is a strong causal check: the same server, dataset, and client sustained 99,851 GET/s with a 100% hit rate, 0.200 ms mean, and **0.983 ms p99.99** when the coarse rate limiter was absent. Keylane therefore already meets a sub-1-ms p99.99 at approximately 100K QPS under this closed-loop load shape. The cost is much higher server CPU because the workers stay hot.

Keylane's 20 us busy-poll window remains a possible secondary amplifier. At 100K aggregate QPS over 15 workers, the nominal average spacing is about 150 us per worker. A burst followed by a gap can repeatedly move workers through park/wake paths, while an unlimited closed-loop workload keeps them active. This mechanism is consistent with the CPU and latency results but has not yet been isolated with a startup-parameter A/B.

Reducing the runtime `foreground-budget-us` from 1,000 to 50 did not provide convincing relief. In paired 30-second limiter runs, 50 us produced 0.396 ms mean / 2.799 ms p99 / 6.143 ms p99.99, while the restored 1,000 us setting produced 0.393 ms / 2.751 ms / 6.367 ms. Although the single pooled p99.99 was 0.224 ms lower at 50 us, every percentile from p90 through p99.9 and the mean were slightly worse, and the server histogram had a slightly larger fraction above 1 ms. This is consistent with limiter phase noise, not a demonstrated scheduler improvement. The live configuration was restored to 1,000 us.

## GET critical-path audit

The code and existing request-phase traces point to scheduling and cross-core wakeup at the extreme tail, not index lookup, buffer allocation, decoding, or the SPDK completion cap.

1. A single-key GET is routed to `ShardForKey(key)` and executes through `SubmitTaskTo()` when the connection worker is not the key owner (`src/redis/command.cpp`). With uniformly random keys, about `(workers - 1) / workers` of requests are remote: historical 12-worker traces report 91.6-91.8%, and 15 workers imply about 93.3%. A remote GET therefore normally pays an origin-to-owner handoff and an owner-to-origin reply handoff around the disk read.
2. The owner computes the digest, takes a shared key lock, looks up the in-memory index, acquires a registered read buffer, submits one aligned SPDK read, validates the record and CRC, and frames the response (`src/storage/engine/read.cpp`). Historical online traces report about 1.8-2.0 us for lookup, 0.1 us for buffer acquisition, 1.4-1.5 us for decode, and 105-128 us for storage I/O. Heap read-buffer fallback was 0%.
3. In those online traces, sporadic 1.5-3 ms total p99.99 samples occurred while storage I/O p99.99 remained at or below 0.5 ms; the excess was commonly in route-out and occasionally route-back. During full sync, route-out rose into the millisecond range as well. This is evidence that the last tail is mostly off-CPU/cross-core scheduling delay rather than media latency.
4. Celer batches wakeups in thread-local state at `MarkWakeWorker()` and calls `FlushWakes()` only at the end of the scheduler round. That is efficient for active targets, but a target that is already parked can wait for the source worker to finish the current foreground round before receiving `MSG_RING`. The existing 1,000-to-50-us whole-budget A/B did not prove that this delay dominates, so an immediate parked-target wake should be instrumented and A/B tested rather than merged on theory alone.
5. The SPDK poller was empty about 99.5% of polls in the relevant low-rate trace and observed completion batches no larger than 8. Together with the max-completions 8/1/0 A/B, this rules out completion batch size as the first optimization at 100K QPS.

The highest-value server experiment is a hybrid wake policy: preserve per-round deduplication for active targets, but on the first post to a target in a round, detect `wake_seq == parked` and issue the wake immediately. Add counters for immediate wakes, deferred wakes, wake-to-drain time, and route-out/route-back latency before changing the policy. The publication-before-wake and park `wake_seq` CAS handshake must remain intact. If this does not move p99.99 under a genuinely smooth open-loop load, the next experiment should be adaptive busy polling rather than a permanently larger fixed spin window.

The following are lower priority for this workload:

- The SPSC cached-head and empty-bitmap fast paths improved prior throughput by only about 0.7% and did not consistently improve tail latency.
- Read-buffer allocation, CRC/decoding, and index lookup are only a few microseconds in the phase trace.
- In-flight read coalescing can help skewed/hot-key workloads, but not uniform random reads over 200M keys.
- Eliminating the two cross-core hops entirely would require key-owner-aware client routing or a materially different connection/socket ownership model; it is not a transparent GET-path micro-optimization.

## Aerospike comparison boundary

The observed statement that Aerospike is below 1 ms does not by itself establish a server-code gap. Under the same Keylane dataset, server, and client host, Keylane is already at 0.983 ms p99.99 near 100K QPS when requests are closed-loop rather than released from memtier's 20 ms quota buckets. An apples-to-apples Aerospike comparison still needs the exact asbench mode (sync versus async), connection/thread count, throughput pacing, object size, storage/cache mode, replication setting, client topology, and percentile aggregation. Until those are fixed, the defensible conclusion is that Keylane has a client-pacing problem in the failing run and a smaller, still-optimizable cross-core scheduling tail—not that its SPDK read itself is slower than Aerospike by several milliseconds.

Recommended order of work:

1. Fix or replace client pacing before tuning the server. Use a microsecond token pacer (about one token every 800 us per connection at 1,250 QPS), randomized connection phases, or a load generator with true open-loop/Poisson pacing. Keep the 20-connection closed-loop result only as a causal control, not as the final open-loop SLA test.
2. Keep `spdk-max-completions-per-poll=8`. Values 1 and 0 provided no benefit at 100K.
3. Instrument and A/B a hybrid immediate wake for already-parked cross-core targets while retaining batching for active targets. Measure route-out, route-back, wake-to-drain, CPU, and p99.99. Do not infer success from aggregate latency alone.
4. If parked-target wake remains material, make `busy-poll-us` runtime-configurable and interleave 20/50/100/200-us runs. Prefer an adaptive policy based on recent work/inter-arrival time; a longer fixed window trades idle CPU for latency.
5. Keep `foreground-budget-us=1000`; 50 us did not improve the distribution consistently. Only after smooth pacing and wake instrumentation, test `spdk-foreground-pre-poll-us=0` versus 5.

The formal 300-second average latency was not worse under the limiter (0.25756 ms versus 0.25836 ms); its tail was worse. The later 60-second diagnostic runs showed both mean and tail inflation because memtier timer phases and environmental state differ across runs. This run-to-run variation is another reason to fix pacing and interleave repeated variants before attributing sub-0.1 ms differences to Keylane.

## Celer SPSC observations

The baseline Celer code had two real optimization opportunities; both are present in the current local, uncommitted Celer worktree used for follow-up evaluation:

1. `SpscRing::try_enqueue()` reloads the consumer-owned `head_` with acquire ordering for every enqueue attempt. A producer-owned cached head avoids that cross-core read until the cached value says the ring may be full. This is a standard SPSC optimization and should help most under dense or bursty cross-core traffic.
2. `TakeActiveSenders()` unconditionally exchanged every active-sender bitmap word with zero, and `DrainCrossCore()` unconditionally exchanged `overflow_pending_` with false. During the 20 us busy-poll loop these were atomic RMW operations even when no cross-core work existed. The local change adds a relaxed-load fast reject so the empty path is read-only, while a nonempty path still uses an acquire exchange.

The safe bitmap form is:

```cpp
auto& bits = active_senders_[index].bits_;
if (bits.load(std::memory_order_relaxed) == 0) {
  return 0;
}
return bits.exchange(0, std::memory_order_acquire);
```

A stale zero can delay a newly published bit until the next drain iteration but does not clear or lose it. The wake-sequence snapshot/recheck handshake before parking prevents a lost wakeup. The producer publishes with release ordering, so an exchange that returns a nonzero value must retain acquire semantics. Clearing the bitmap does not publish consumer data back to producers, so release semantics on the exchange are unnecessary.

This optimization must not be copied to the per-lane `active_.exchange(false, acq_rel)` close handshake. That RMW participates in the lane activation correctness protocol and cannot be replaced with a load/store fast path.

On x86 the unconditional exchange is a locked atomic RMW. The relaxed empty check becomes an ordinary load. It is too strong to claim that this always leaves the line in MESI Shared state: an ordinary load does not downgrade a line that the receiver already owns Modified. The defensible benefits are avoiding unnecessary modification-order operations and avoiding an ownership request when the line is not already locally owned.

### Existing preliminary A/B evidence

An earlier 2026-08-22, 16-core, approximately 400 GB read experiment in `perf_reports/data/` provides a useful magnitude check:

| Variant | GET/s | Change from baseline | Avg latency | p99 |
|---|---:|---:|---:|---:|
| Baseline | 275,061.53 | - | 0.29040 ms | 0.903 ms |
| Cached head, fixed-data rerun | 276,548.94 | +0.54% | 0.28885 ms | 0.911 ms |
| Cached head + bitmap zero check | 276,994.90 | +0.70% | 0.28845 ms | 0.943 ms |

The first cached-head run was 276,660.52 GET/s (+0.58%), which is consistent with the fixed-data rerun. The bitmap zero check added only about 0.16% over cached head in the available run. Tail latency did not consistently improve. These are single formal samples per final variant, so the correct conclusion is **useful but small under this read workload**, not a statistically established 0.16% win. A repeated, interleaved baseline/cached-head/zero-check A/B test is still required before merging on performance grounds.

`perf c2c` cannot validate the HITM mechanism on this Azure VM: `perf c2c record` reports `failed: no PMU supports the memory events`, and `perf_event_paranoid` is 4. HITM measurement needs bare metal or a VM exposing the required memory-event PMU/PEBS support.

## Artifacts

All raw logs, JSON results, CPU samples, service logs, metrics, device bindings, revisions, and the failed `perf c2c` capability probe are under [`perf_runs/spdk-500g-readonly-20260825`](../perf_runs/spdk-500g-readonly-20260825/).
