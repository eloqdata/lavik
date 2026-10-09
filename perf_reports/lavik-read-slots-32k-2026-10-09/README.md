<!--
Copyright (C) 2026 EloqData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# 32 KiB registered read slots: SPDK / memtier comparison

2026-10-09, main `101a72544ef99cd17514b452e32e1138e15d290a`. The change lowers server and pool default read payloads from 1 MiB to 32 KiB and fixes SPDK request-descriptor exhaustion exposed by the larger slot count. The earlier per-connection request-buffer patch is excluded.

Three-sample medians:

| Command | 1 MiB QPS | 32 KiB QPS | Change | 1 MiB p99.9 ms | 32 KiB p99.9 ms |
| --- | --- | --- | --- | --- | --- |
| GET | 939,694 | 940,702 | +0.11% | 5.823 | 5.791 |
| SET | 725,675 | 710,871 | -2.04% | 10.623 | 11.391 |

| Command | Read payload KiB | QPS samples |
| --- | --- | --- |
| GET | 1024 | 939,694, 941,733, 939,025 |
| GET | 32 | 939,807, 940,702, 958,245 |
| SET | 1024 | 725,675, 724,790, 730,844 |
| SET | 32 | 709,828, 710,871, 716,681 |

The control uses the original Bycorf runtime with explicit `--storage-read-buffer-kb=1024`; the candidate includes the SPDK backpressure fix and uses the new 32 KiB default. Both use identical Lavik sources and build flags. Results include the full change, rather than isolating buffer size. Three alternating rounds use native SPDK, kernel TCP, 16 workers, 640 connections, memtier 2.5.1, pipeline 1, distinct client seeds, one billion existing 1 KiB values, 10-second GET warmups and 60-second GET/SET points. Defrag is enabled. Registered memory remains 256 MiB/worker with four 8 MiB write buffers; flush 1000 ms, SPDK completion cap 16, pre-poll 5 us. No media discard or owner remap occurs.

Build: GCC 13.3, Release/O3/LTO, x86-64-v2, SPDK enabled, static C++ runtime, test faults disabled. See [build-manifest.json](build-manifest.json).

At the tested budget, read slots increase from 222 to 5,734. At the product's default 64 MiB budget they would increase from 31 to 819; that lower budget was not benchmarked here. Oversized records and whole-value assembly retain reusable overflow allocations. The grouping threshold is not a hard record-size limit.

The equal-budget main comparison leaves GET effectively unchanged, but SET has a 2.04% lower median. This is not evidence of a regression-free change.

Supplemental SET runs:

| Bycorf | Read KiB | Pool MiB/worker | SET QPS samples | Median QPS |
| --- | --- | --- | --- | --- |
| fixed | 1024 | 256 | 728,472, 709,804, 725,370 | 725,370 |
| fixed | 32 | 41 | 712,434, 726,706, 713,383 | 713,383 |

Each supplemental session takes three consecutive 60-second SET samples after a 10-second GET warmup. Unlike the main comparison, these samples do not each restart and run a preceding 60-second GET, so they are diagnostic observations rather than a strict causal control. The fixed runtime at 1 MiB has a 725,370 QPS median, close to the original baseline. Reducing the 32 KiB pool to 41 MiB/worker (230 read slots) leaves a 713,383 QPS median. Individual sustained-write samples also vary by over 2%. These probes do not establish either retry-path overhead or slot count as the cause; the microscopic SET performance cause remains unresolved.

The initial slot-only candidate produced 23 server errors; a diagnostic rerun reproduced 16 with `rc=-12` and exactly 512 outstanding I/Os, matching the qpair descriptor capacity. Ordinary reads acquire a slot before submission and wait in the pool when slots are exhausted. Changing capacity can move waiting downstream, but client concurrency remains 640: the original failure logs do not establish increased total queueing or reconstruct the per-worker peak of 512. The later diagnostic traces below capture how local backlog forms at unchanged concurrency. Both failed runs are excluded and archived. The fix retains rejected commands in per-qpair worker-local FIFOs and retries after polling, preserving buffers, callbacks and outstanding-I/O accounting. It adds no shared lock or qpair expansion. Requests that cannot progress through a completion still fail; the existing 4,096-request application pool remains bounded.

A deterministic read-only hardware check submits 1,024 reads per qpair to two qpairs without polling, repeated four times. The old backend rejects 1,024 of 2,048 reads each round; the fixed backend completes all requests with matching bytes and exactly one callback each. Pending close rejection and pool reuse are also exercised.

36 unit tests and three grouped-storage end-to-end tests pass. Additional protocol checks cover boundaries around 8/16/24/32 KiB, Strings up to 2 MiB, keys over 32 KiB, 128/256 KiB collection elements and byte-for-byte reads after restart on both io_uring and SPDK. The SPDK probe uses an initially empty DB 15, deletes its keys afterward and confirms DB 0 retains one billion keys. Large-value correctness checks do not establish large-value throughput; the measured workload uses 1 KiB values.

All 12 primary and six supplemental points pass client error checks, and GET points have no misses. All six sessions validate counts/lengths before and after and exit cleanly. NVMe drivers, hugepages (0) and unsafe no-IOMMU (N) are restored. See [Lavik patch](read-slots-32k.patch), [Bycorf patch](spdk-backpressure.patch), [CSV](results.csv), [raw evidence](evidence.tar.gz) and [SHA256SUMS](SHA256SUMS). Full local evidence: `/mnt/dev/peer-bench/read-slots-32k-2026-10-09/`.


## Follow-up: where local backlog comes from

Two diagnostic sessions use the same instrumented fixed-runtime binary with 32 KiB and 1 MiB slots, native owner placement, SPDK, 640 connections, pipeline 1 and defrag enabled. Request-lifetime counters, a 64-poll history, task durations above 1 ms, and Linux scheduler events are aligned using TSC/CLOCK_MONOTONIC anchors. These sessions are excluded from performance results.

Five workers' first pressure snapshots fall within scheduler-trace coverage. Eight preceding long poll gaps spend 95.7–99.4% of their duration off CPU while runnable. Examples include worker 5 (11.705 ms poll gap / 11.589 ms off CPU), worker 13 (10.956 / 10.807 ms), and worker 2 (4.209 / 4.027 ms). Switch-out events show sshd and local control processes taking the CPU. Long task samples resolve to ordinary command dispatch; their wall-clock overruns are also mostly off CPU, rather than evidence of a long command execution loop.

While one owner is delayed, other workers complete requests and the closed-loop clients choose new random keys. More requests can consequently accumulate at that owner despite unchanged global concurrency. Upon resumption, it submits queued work while polling at most 16 completions per pass. The old read pool limits ordinary reads before SPDK, moving the waiting point upstream.

The 32 KiB run reaches 540 outstanding operations and recovers all 138 initially resource-constrained submissions; accepted/released/completed counters all finish at 66,031,238. The same binary with 1 MiB slots peaks at 218, has no descriptor exhaustion, and finishes with all three counters at 66,172,294. At least 64 million / 64.5 million recorded GET routes, respectively, match logical and physical owners. These traces show no owner mismatch, leaked I/O or duplicate completion.

The confirmed correctness bug is the old backend treating recoverable descriptor pressure as a permanent I/O failure, fixed in [Bycorf PR #9](https://github.com/eloqdata/bycorf/pull/9). This explains the mechanism for local queue saturation at 640 connections. It **does not establish the cause of SET −2.04% or certify no performance regression**. Logging and perf startup perturb scheduling; diagnostic peaks and exhaustion counts are not estimates of uninstrumented frequency, nor retrospective traces of the original 16 errors.

[Diagnostic evidence](debug-evidence.tar.gz) includes instrumentation patches, binary hash, raw logs, scheduler excerpts, alignment script and analysis. Full perf recordings remain at `/mnt/dev/peer-bench/spdk-saturation-debug-2026-10-09/pressure/`. Both servers checkpointed and exited; NVMe drivers, hugepages (0) and unsafe no-IOMMU (N) were restored.
