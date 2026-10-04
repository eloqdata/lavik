# Native FULL large-value record window: measured results

2026-10-04 · [中文](README.zh-CN.md) · [Methods](METHODS.md) · [Reproduce](REPRODUCE.md)

The large-value record window reduced end-to-end FULL time in the measured single-source-flow cases: 128 MiB strings improved by **12.76% at 5 ms added RTT** and **13.57% at 20 ms**; large hashes improved by **13.69% and 18.54%**. This is a scoped benefit, with important negative and variable results: **four-source-flow strings at 5 ms regressed 13.95% with capture**, while the later matched no-capture cohort improved 21.30%. At zero added RTT, the no-capture median regressed 1.23%. These results do not establish a universal gain or a precise causal cost of packet capture.

## Comparison and conditions

This report compares main `25e159418603a95c1b2cca68173b329067e3ef48` with chunks `51d03f97fede341fb42c14bd46a9f1aaf416eeb1`. The candidate pipelines large-value record frames, bounded by eight frames / 16 MiB per source flow, and waits for the value's receipts before releasing its lifetime protections. Functional tests cover baseline, replacement and publish-FIFO origins separately; the live performance corpus below is a mixture and does not isolate those origins. See [validation](validation/VALIDATION.md).

The measured core revisions above remain the binary provenance even when later publication heads add documentation or an equivalent test-fixture update. Both sides use GCC 13.3 Release native O3/LTO and the same Bycorf revision. Binaries and exact source/configuration hashes are in [cohort provenance](evidence/cohort.json) and [build evidence](evidence/build/). Data is freshly seeded on an eight-CPU NVMe host; it is not an evicted-cache or larger-than-memory benchmark. One source flow uses one target worker; four source flows use two target workers, with disjoint source/target/client CPU sets. Added RTT is true netem delay on task-owned veth endpoints; 0 means no injected delay. [Conditions](evidence/conditions.md) retain each run's observed idle application PING RTT.

Backlog is **64 MiB globally per process**, divided across data-worker flows. Publisher queue is **16 MiB per worker**, except the separately named `queue32` live cohorts at **32 MiB per worker**. These quotas differ from sender-window credit. All formal corpora use DB 0. Corpus shapes, host details, CPU sets, capture configuration, and timer boundaries are in [Methods](METHODS.md).

There are **14 complete comparisons / 84 accepted runs** here. Each uses three paired repetitions in A/B, B/A, A/B order. Across both reports there are 162 comparable accepted runs, two additional accepted partial observations, and two rejected attempts. Every accepted run has one FULL attempt, all expected flows, equal final content digests and visible per-flow fence writes. Captured runs additionally passed stream completeness and zero kernel capture-drop checks. No slow valid run was removed.

## Captured end-to-end FULL results

All times are seconds from immediately before `LAVIK.REPLICAOF` to observed target ONLINE, including reset, transfer, handoffs, final cut and polling. Startup, seeding and later digest verification are outside the timer. Positive time reduction means faster; negative means slower. Values are shown in repetition order, not sorted.

| Case | Before r1 / r2 / r3 (s) | Before median | After r1 / r2 / r3 (s) | After median | Time reduction | Speedup |
| --- | --- | --- | --- | --- | --- | --- |
| large-hash-128m-f1-rtt20 | 20.7283, 20.5630, 20.1671 | 20.5630 | 16.7511, 16.6005, 17.1841 | 16.7511 | 18.54% | 1.228× |
| large-hash-128m-f1-rtt5 | 15.1408, 14.2867, 13.7164 | 14.2867 | 12.3303, 13.2779, 12.2749 | 12.3303 | 13.69% | 1.159× |
| large-live-128m-f1-rtt20-queue32 | 13.7024, 13.8993, 13.4126 | 13.7024 | 10.5407, 10.3854, 10.5759 | 10.5407 | 23.07% | 1.300× |
| large-live-128m-f1-rtt5 | 5.3479, 5.3089, 5.2680 | 5.3089 | 4.7452, 5.2757, 4.7880 | 4.7880 | 9.81% | 1.109× |
| large-live-128m-f1-rtt5-queue32 | 4.8219, 5.6541, 5.7339 | 5.6541 | 4.8444, 4.8904, 5.4021 | 4.8904 | 13.51% | 1.156× |
| large-string-128m-f1-rtt0 | 2.5744, 2.4457, 2.4231 | 2.4457 | 2.3300, 2.3504, 2.2237 | 2.3300 | 4.73% | 1.050× |
| large-string-128m-f1-rtt1 | 3.4783, 3.0659, 3.2179 | 3.2179 | 2.8366, 3.0259, 2.7850 | 2.8366 | 11.85% | 1.134× |
| large-string-128m-f1-rtt20 | 10.7465, 10.1033, 10.4320 | 10.4320 | 9.5011, 8.7026, 9.0166 | 9.0166 | 13.57% | 1.157× |
| large-string-128m-f1-rtt5 | 4.4691, 4.7640, 4.8089 | 4.7640 | 4.1560, 4.2270, 3.9605 | 4.1560 | 12.76% | 1.146× |
| large-string-128m-f4-rtt20 | 3.4593, 3.6217, 3.1836 | 3.4593 | 3.3008, 3.2289, 3.5839 | 3.3008 | 4.58% | 1.048× |
| large-string-128m-f4-rtt5 | 2.5770, 1.6670, 1.9945 | 1.9945 | 2.2727, 2.2953, 2.2451 | 2.2727 | -13.95% | 0.878× |

![Captured FULL time by added RTT; median and min–max of three runs](full-vs-rtt.svg)

The four-flow 5 ms regression remains part of the result. Its before values span 1.6670–2.5770 s, while candidate values span 2.2451–2.2953 s. At four flows / 20 ms the median improves only 4.58%, and the third paired candidate run is slower. Three repetitions and min–max bars are descriptive, not confidence intervals.

## Matched no-capture controls

These controls ran later with the same workload, worker counts, CPU sets and server settings, with capture disabled for both variants. The four-flow control was declared after observing the captured regression and before any control run; it supplements that result rather than replacing it. A later cohort cannot isolate capture overhead from other temporal variation.

| Case | Before r1 / r2 / r3 (s) | Before median | After r1 / r2 / r3 (s) | After median | Time reduction | Speedup |
| --- | --- | --- | --- | --- | --- | --- |
| large-string-128m-f1-rtt0-nocap | 2.5841, 2.3736, 2.2787 | 2.3736 | 2.3973, 2.4028, 2.7017 | 2.4028 | -1.23% | 0.988× |
| large-string-128m-f1-rtt20-nocap | 9.7902, 9.9831, 9.9386 | 9.9386 | 9.1836, 8.5406, 9.0324 | 9.0324 | 9.12% | 1.100× |
| large-string-128m-f4-rtt5-nocap | 2.0179, 1.8232, 2.0684 | 2.0179 | 1.5881, 1.5629, 1.6420 | 1.5881 | 21.30% | 1.271× |

The 20 ms single-flow improvement remains positive without capture (9.12%). The four-flow 5 ms direction reverses, and the zero-delay gain disappears. The appropriate conclusion is sensitivity to workload and observation conditions, not that one cohort invalidates the other.

## Mixed live writes and the baseline stall

The large writer rotates existing 16 MiB strings using one synchronous transaction/s after five seconds of warmup. The original 16 MiB publisher-queue / 20 ms **main** run stopped making native progress and was explicitly interrupted after more than five minutes. Its writer timed out at 30.052 s; this is not a 900 s FULL timeout sample. No final digest was obtained, and the candidate plus remaining repetitions under that original condition were not run. [The retained investigation](BASELINE-STALL.md) describes a source-supported gate/admission/fence cycle consistent with the observations, without coroutine-stack proof. This PR does not claim to fix that issue.

A new matched cohort gives both variants 32 MiB publisher queues per worker, preserving the workload/rate/backlog. All twelve runs at 5/20 ms passed; their median reductions are 13.51% / 23.07%. Keep them separate from the original 16 MiB live 5 ms cohort (9.81%). At 20 ms / queue32, main completed 13/14/13 writes during FULL versus 10/10/10 for the candidate; captured native frame bytes were about 242.58/258.58/242.58 MiB versus 210.58 MiB each. This is an end-to-end live-load result with different total work, not fixed-byte throughput.

Repeated large-value begins were observed for five candidate keys and six baseline keys in each queue32 / 20 ms run, demonstrating repeated large-record transfer. Native frames do not tag the baseline/replacement/FIFO origin, so this does not establish separate speedups for each origin. Source tests supply that correctness coverage.

The live latency sample sizes are small: only 4–14 operations begun per FULL. Report counts and maxima, not robust p99/p99.9 conclusions. The largest observed queue32 / 5 ms candidate latency was **695.54 ms**, versus **156.90 ms** among baseline samples; at 20 ms they were **515.40 vs 208.69 ms**. Two candidate 5 ms operations finished after ONLINE and remain included in the latency population. Faster FULL is not evidence that foreground latency is unchanged. [All live counts, QPS and latency statistics](evidence/live.md) preserve these observations.

## Resource and frame evidence

At single-flow strings / 5 ms, median per-run sampled peak source RSS was 249.5→249.3 MiB and target RSS 233.3→235.0 MiB; source mean busy cores over the valid sampled interval were 0.249→0.277, target 0.097→0.116. These intervals cover only about 78–87% of FULL. [Resource tables](evidence/resources.md) and [per-run JSON](evidence/per-run.json) retain per-run sampled peaks, CPU summaries and valid intervals; full raw sample rows remain on NVMe. The gauges are not exact sender-credit high-water measurements; no total FULL CPU or exact allocation peak is inferred.

Static single-flow strings emit 80 record frames across eight partition+DB groups (ten each); hashes emit 76 frames across four groups (19 each). The source record-byte observation span for strings / 5 ms has median 3.9357→3.5135 s. It includes intervening waits and other work; it is not an exclusive phase and cannot be subtracted from FULL to estimate reset or handoff cost. [Spans](evidence/spans.md), [frame distribution](evidence/frame-distribution.csv) and full maps in [raw results](evidence/raw-results.jsonl) preserve the measured evidence.

All 134 accepted captures across the two reports produced unambiguous optional spans. Sixteen contain repeated source TCP payload observations; these are consistent with retransmission, not measured qdisc loss. In captured four-flow strings / 5 ms, the repeated byte counts were main 170/0/65,160 and chunks 1,205/4,492/3,267. They do not establish the cause of the timing regression. See [per-run overlap analysis](evidence/tcp-overlap.jsonl); zero tcpdump drops must not be read as zero network loss.

## Review and reproduction artifacts

- [Every run, selected fields (CSV)](evidence/per-run.csv), [detailed sampled summaries (JSON)](evidence/per-run.json), [all comparisons and paired ratios](evidence/comparisons.json).
- [Compact original result / acceptance / driver outcome records](evidence/raw-results.jsonl), including exact digests, commands, seed distribution and all observed frame maps.
- [Retained raw-artifact paths and hashes](evidence/retained-artifacts.json); full pcap and raw samples stay on the original NVMe task directory. [Final process/namespace cleanup readback](evidence/final-cleanup.json) found none remaining. Successful data files were deleted; failure data remains.
- [Reproduction instructions](REPRODUCE.md), [portable tools](tools/), [exact historical tools and recipes](evidence/historical-tools/), and [baseline-failure evidence](evidence/diagnostic-failure/).

The ordinary-baseline-record report measures its own parent-to-candidate comparison. Ratios from the two reports must not be multiplied into a claimed main-to-combined speedup.
