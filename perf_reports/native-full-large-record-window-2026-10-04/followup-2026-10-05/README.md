# Native FULL large-value window: no-capture live-write follow-up

2026-10-05 · [简体中文](README.zh-CN.md) · [Original report](../README.md) · [CI gate repair](../validation/CI-FIX.md)

The no-capture live-write comparisons confirm lower median FULL time at 5/20 ms added RTT, but do not establish an all-positive foreground-latency result. All 40 runs passed correctness and cleanup checks. This report compares main to the large-value window only.

| Added RTT | Main FULL median | Chunks FULL median | Time reduction | Faster pairs |
| --- | --- | --- | --- | --- |
| 5 ms | 5.4924 s | 4.9901 s | 9.14% | 9/10 |
| 20 ms | 13.6226 s | 10.4858 s | 23.03% | 10/10 |

At 5 ms one candidate run remains slower than its matched baseline. [All ten pairs](full-table.md) retain the complete distributions and paired reductions, rather than selecting only favorable samples.

## Foreground writes

| Added RTT | FULL-started samples, main/chunks | Median of per-run maxima, main/chunks | Largest observation, main/chunks | Pairs with higher candidate maximum |
| --- | --- | --- | --- | --- |
| 5 ms | 50 / 45 | 125.24 / 129.63 ms | 177.07 / 225.22 ms | 6/10 |
| 20 ms | 134 / 105 | 166.76 / 212.63 ms | 311.10 / 335.42 ms | 7/10 |

The 20 ms median of per-run maxima increases about 27.51%; at 5 ms it increases about 3.50%. These are observed maxima from small, unequal-duration latency populations, not estimates of a stable p99. Lower FULL time also shortens the writer's observation interval and changes the amount of concurrent replication work. [Per-run latency counts, medians, maxima and completed QPS](latency-table.md) and raw samples allow these qualifications to be checked.

Disabling packet capture does not remove the observed foreground tradeoff. The previous captured maxima were larger, but separate cohorts cannot prove capture caused that difference. The evidence supports faster FULL under these two live workloads while retaining a foreground-latency concern, especially at 20 ms. It does not support saying that the optimization improves every relevant metric.

The 32 MiB-per-worker queue is the predeclared matched setting for both variants. This cohort neither reruns nor fixes the original 16 MiB baseline admission/gate stall; that diagnosis and failed attempt remain in the original report.

## Method and provenance

This is a separately declared no-packet-capture cohort, not a replacement for the original three-pair cohorts. Each case uses ten paired repetitions with alternating A/B and B/A order. All declared results are retained. The three-case campaign contains 60 runs; the large-value report contains 40 and the ordinary-baseline report 20. No local builds, tests or heavy offline analysis ran concurrently with measurement.

The original immutable Release binaries were reused: main `25e159418603a95c1b2cca68173b329067e3ef48`, chunks `51d03f97fede341fb42c14bd46a9f1aaf416eeb1`, and records `9da9c797eefaea45dfde3ccc521baa440de559a8`, with Bycorf `62509c93d40c2480f5046b71454db6cf95801b04`. SHA-256 values remain `a056e41003b0c833157fe89737448b6a7715d2c5ba63aa32ad1675b4d643fabc`, `c9dae66f6d752823acc72f7d399016d2a14201c6b848ea1aa610e910aaada751`, and `ae24694323ff25448658bf56797f0fb36694fea310ce37224b93ab00d1f57e6a`, respectively. The subsequent CI repair changes only test orchestration; the production source matches these measured cores byte for byte. These are fixed-base component comparisons, not a fresh latest-main rebuild or a main-to-combined benchmark.

Large-value cases use eight existing 16 MiB strings, one source/target worker, 5/20 ms added RTT, 32 MiB publisher queue per worker, one synchronous `MULTI; SET; EXEC` transaction/s, and 5 s warmup. Source CPU 0, target CPU 1 and client CPUs 6–7 are disjoint. The ordinary baseline case uses 32,768 × 4 KiB values, four source/two target workers, 20 ms RTT, 16 MiB publisher queues per worker and no concurrent writes. Source CPUs 0–3, target 4–5 and client 6–7 are disjoint. Both use 2 GiB data-file and memory limits per process, 64 MiB process-global backlog, 64 MiB registered buffers per worker, 100 ms flush interval and 900 s timeout. The new recipes preserve the matched original inputs and disable tcpdump for both variants. Network delay is applied as half the requested RTT in each direction; actual PING samples remain in raw results. Sources are freshly seeded, not a disk-bound or cold-cache scale test.

FULL time starts immediately before `LAVIK.REPLICAOF` and ends at observed target ONLINE, including reset, transfer, handoff, final cut and polling. It excludes process startup, seeding and final digest verification. Every run must pass one FULL attempt per expected flow, all flows ONLINE, final equal source/target digests, per-flow sentinel visibility, successful writer attempts and clean teardown. No failed run or slow valid run is silently replaced.

Write latency covers operations begun during FULL, including operations completing after ONLINE; completed QPS instead counts completions inside FULL. Different FULL durations create different numbers of foreground writes and potentially different replication work. Without capture, no native frame-byte or per-origin speedup is inferred. Sample counts and per-run maxima are descriptive; these short runs do not establish robust p99/p99.9 or rare-stall behavior. RSS and CPU summaries in `per-run.json` describe sampled peaks/intervals, not exact memory peaks or total CPU cost.

A later no-capture cohort cannot isolate packet-capture overhead from temporal variation. The original captured regressions remain valid observations. Results apply to these workloads, topology, queue settings and host; they do not prove a universal improvement.

## Evidence and reproduction

- [Frozen recipe](recipe.json), [paired comparisons](comparisons.json), [per-run summaries](per-run.json), and [all individual FULL times](full-table.md).
- [Original result, acceptance and invocation records](raw-results.jsonl), including exact binary hashes, timing bounds, digests and cleanup results.
- [Raw writer samples](writer-samples.jsonl), including warmup and operations crossing the FULL boundary; empty for the static ordinary-baseline case.
- [Retained artifact paths and hashes](retained-artifacts.json). Full resource samples and process logs remain on the original NVMe host under `/mnt/local_nvme/i131/measurements-followup`; results and writer samples are included here.

Use the shared portable tools from the large-value report, a new output directory, and a variants JSON mapping `main`, `chunks` and `records` to local binaries and the exact source revisions above. The recipe is restricted to this report's cases, while the original declaration records the full 60-run campaign. The original host command was:

```bash
source /mnt/local_nvme/i131/env.sh
flock -n /mnt/local_nvme/i131/benchmark.lane.lock \
  python3 /mnt/local_nvme/i131/run-benchmark-matrix.py \
  --variants /mnt/local_nvme/i131/benchmark-variants.json \
  --recipe /mnt/local_nvme/i131/benchmark-nocap-followup-recipe.json \
  --output /mnt/local_nvme/i131/measurements-followup --execute
```

For another checkout, pass explicit `--runner`, `--recipe`, `--variants` and `--output` paths to `tools/run-benchmark-matrix.py`; it defaults to dry-run and refuses existing run directories. Its acceptance guard rejects incomplete FULL, mismatched digests, writer errors and cleanup failures. Use `tools/aggregate-benchmarks.py --root ... --recipe ... --output ...` after all runs finish. Existing build provenance and complete setup remain in the shared large-value report's methods/reproduction documents.
