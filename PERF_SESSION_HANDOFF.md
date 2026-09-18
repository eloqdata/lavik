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

# Lavik Performance Session Handoff

Updated: 2026-08-21 UTC

## 2026-08-21 two-host 1-TiB ONLINE and rolling FULLSYNC retest

This Markdown handoff is the retained technical record. Generated HTML,
artifact JSON, and reviewed CSV outputs were removed from the repository; the
reviewed headline results and snapshot-reader A/B addendum remain below.

### 2026-08-21 snapshot reader A/B addendum

The proposed fixed reader pool with a continuously replenished I/O window was
rejected after three valid 300-second A/B runs from client `10.0.0.5`. All
runs used memtier cluster mode, 80 active connections, 100,000 QPS,
SET:GET=1:19, 1,000-4,000 byte values, and remained in FULLSYNC for the entire
measurement window:

    Implementation                 Read concurrency  QPS        p99      p99.9    p99.99  Primary CPU  Replica CPU
    legacy grouped barrier         16                99,996.91  2.863ms  4.511ms  5.663ms  1,096.6%     279.3%
    sliding reader pool            16                99,959.88  3.279ms  5.023ms  6.079ms  1,101.5%     357.1%
    sliding reader pool             8                99,997.92  2.943ms  4.671ms  5.791ms  1,093.2%     303.3%
    grouped barrier + digest reuse 16                99,983.16  2.895ms  4.479ms  5.663ms  1,113.8%     244.7%

Relative to the sliding pool at concurrency 16, the final grouped-barrier
build improved p99, p99.9, and p99.99 by 11.7%, 10.8%, and 6.8%. It is within
1.1%, -0.7%, and 0% of the original grouped-barrier baseline. The sliding
pool saved per-record coroutine creation but immediately replaced every
completed snapshot read, keeping NVMe queue depth continuously occupied and
removing the natural foreground I/O gaps between waves. Lowering its dynamic
concurrency to eight reduced the damage but did not beat the barrier.

The apparently unused CPU is I/O wait, not a lost scheduling opportunity.
With the client stopped and only FULLSYNC running, source worker CPUs reported
about 25% iowait and replica worker CPUs 69-76% iowait while the replication
link carried about 1.15 GB/s (only about 9.4% of the 100-Gb link). During the
formal workload the source process still used 10.9-11.1 of its 12 assigned
worker CPUs; CPUs 12-15 are intentionally reserved for IRQ work. Increasing
snapshot in-flight I/O therefore raises foreground tail latency without
increasing the storage completion rate.

The final code keeps snapshot work on the foreground queue, restores bounded
read waves with a completion barrier, retains dynamic
`replication-snapshot-batch-size` and
`replication-snapshot-read-concurrency`, and reuses the already-computed key
digest during snapshot acknowledgement without changing the wire format. The
deployed binary is
`bin/lavik-perf-20260821-snapshot-barrier-final`, SHA-256
`c93767271430c00ce625292c366fa31ade880b50fb2aba14fa381692a5ed15ac`.
The current primary PID is 171886 and replica PID is 35937; the replica was
cleared by zeroing only the first 8 MiB of each replica device and is currently
continuing its fresh FULLSYNC.

A subsequent valid 50,000-QPS sensitivity run used the same barrier build,
80 connections, SET:GET=1:19, and 300-second all-FULLSYNC window. Its total
p99/p99.9/p99.99 was 3.743/5.471/7.231 ms, worse than the matching 100,000-QPS
run's 2.895/4.479/5.663 ms by 29.3%/22.1%/27.7%. SET tails were essentially
unchanged at 2.591/4.127/5.343 ms; GET tails rose to
3.791/5.503/7.263 ms. Lower foreground demand therefore does not automatically
improve latency: snapshot work consumes the freed scheduling and I/O capacity,
so read-tail isolation requires explicit admission or budgeting.

The Release/SPDK binary was `bin/lavik-perf-20260821`, SHA-256
`e340769db8549f5d26140ccbcff699443dd0ae30ca454d6ad318e7d4be1bd09d`.
The primary on `10.0.0.4` and replica on `10.0.0.7` each used two NVMe devices
and 12 workers on CPUs 0-11, with CPUs 12-15 reserved for network IRQ or
softirq work. The dataset contained 400,000,000 keys with 1,000-4,000 byte
values. The fill averaged 226,152.79 SET/s. All formal workload rounds used
80 active business connections, memtier cluster mode, a 100,000 QPS target,
and a 300-second duration.

ONLINE steady-state results with the replica caught up:

    Workload       QPS          p50 ms   p99 ms   p99.9 ms   p99.99 ms
    pure read      100,000.31   0.231    0.511    0.799      1.415
    pure write      99,999.35   0.215    0.767    1.407      1.887
    SET:GET 1:1     99,999.58   0.231    0.679    1.191      1.567
    SET:GET 1:19   100,000.60   0.239    0.591    0.951      1.263

After clearing the first 8 MiB of both replica devices, the empty replica
completed full sync in approximately 19 minutes 22 seconds. Three workloads
ran entirely during FULLSYNC and all held the 100K QPS target:

    Workload       QPS         p99 ms   p99.99 ms   source CPU   target CPU
    pure write     99,980.79   2.943    5.823        1,073.2%      289.0%
    SET:GET 1:1    99,974.31   2.799    5.567        1,077.9%      301.1%
    SET:GET 1:19   99,996.91   2.863    5.663        1,096.6%      279.3%

There was no replication backlog failure: credit waits remained zero, the
largest full-sync queue observation was 505,293 bytes, and the largest normal
publish queue observation was 4,742 bytes. The source and replica finished at
400,000,000 keys, ONLINE, with lag zero. FULLSYNC nevertheless increased p99
by 3.84-4.84x and p99.99 by 3.09-4.48x versus the matching ONLINE workloads.
Source CPU increased by 2.29-2.39x, consuming about 10.7-11.0 of the 12 worker
cores. Perf attributes the added work mainly to storage polling, cross-core
drain, CRC, NVMe completions, and snapshot digest. The remaining rolling
upgrade issue is therefore latency isolation, not sync progress or queue
capacity.

The current primary PID is 126030 and the current replica PID is 22738. Both
are ONLINE at 400,000,000 keys and lag zero. Formal memtier logs remain on the
client at `/tmp/lavik-{fullsync,online}-*.memtier`; local and replica CPU and
metrics samples remain under matching `/tmp/lavik-*.pidstat` and
`/tmp/lavik-*.metrics.csv` names.

## 2026-08-20 local 20-GB continuous-write full-sync validation

The final verification reran the Release binary from 20:13:08 to 20:33:08
UTC against the same 8,000,000-key, roughly 20-GB source while 16 threads and
160 memtier connections continuously replaced random 1,000-4,000-byte values.
One full-sync session reached ONLINE in exactly 20 minutes without a history
gap, reconnect, OOM rejection, or backlog invalidation. At ONLINE the source
reported offset 8,950,064 and `lag=0`; the 1,275.7-second workload completed
13.32 million successful SETs at 10,444.54 SET/s and 25,977.99 KiB/s, with
15.29-ms mean, 173.055-ms p99.9, and 438.271-ms p99.99 latency.

The run exposed one final admission issue at the cut itself: while the source
drained the command gate, some attempted SETs received `TRYAGAIN database
flush is in progress`. The cut now closes and drains transaction admission
before the DB gates, and ordinary DB admission suspends the coroutine instead
of returning a transient error. The complete 66-test Debug matrix, including
the replication E2Es, passed after that correction. A post-ONLINE 24-MiB
exclusive command then had exact `STRLEN=25,165,824` on both nodes; source and
replica both reported `DBSIZE=8,000,001` and `lag=0`.

### Earlier capacity-resize and reconnect run

The final in-memory-backlog Release build was exercised with one source worker
and one replica worker against 64-GiB file-backed devices. The source began
with 8,000,000 `kv_` keys whose random values were 1,000-4,000 bytes (roughly
20 GB of live values). During full sync, memtier continuously replaced random
keys using 16 threads, 10 clients per thread, the same value-size range, and a
nominal per-client rate limit of 625.

Full sync started at 18:55:10 UTC and reached ONLINE at 19:11:23, or about 16
minutes 13 seconds. It stayed on one replication session through runtime
changes of `repl-backlog-size` from 1 GiB to 512 MiB and back, and
`replication-publish-queue-mb-per-worker` from 16 MiB to 8 MiB and back. Source
and replica ended with 8,000,001 keys; the extra key was a 24-MiB value written
after its partition entered TAILING. That command exceeded the 16-MiB queue
waterline, obtained an exclusive FIFO admission, completed in 2.25 seconds,
and had an exact source length of 25,165,824 bytes. The large-value E2E covers
byte-exact transfer; this performance run additionally verified equal final
source/replica key counts. Source RSS was about 2.16 GiB, full-sync coverage
reservation was about 248 KiB, and no command was rejected for OOM.

After ONLINE, the replica ACK cursor advanced from 9,400,968 to 9,862,934 in
30 seconds with `lag=0`. The interrupted 1,298.7-second memtier run completed
about 15.95 million SETs at 12,287 SET/s and 30,530 KiB/s overall, with 13.01
ms average, 81.41 ms p99, and 172.03 ms p99.9. Early in the scan it sustained
roughly 30-50k SET/s; as more partitions entered TAILING, target apply
throughput became the bound and source admission correctly applied
backpressure instead of dropping replication history.

The first forced disconnect exposed an eager low-water trim: releasing the
last ACK pin cut an otherwise valid reconnect window from 100% to 75%, making
the requested cursor 19,035 LSNs older than the new floor. The final patch
limits low-water hysteresis to connected pinned consumers; an unpinned circular
log now evicts only the chunks required by new writes. A dedicated chunk-log
test covers that boundary. A post-fix Release integration run then requested
cursor 32,504 with floor 12,557 and tail 35,290, explicitly selected
`CONTINUE`, and returned ONLINE in about one second. The final regression set
passed the replication-log E2E, all 55 unit tests, nine replication E2Es
(multi-replica, reconnect, TLS, transactions, FLUSHDB/FLUSHALL, backpressure,
and destructive full sync), the metrics E2E, and both file and SPDK Release
builds.

## 2026-08-18 two-host 1-TB primary/replica controlled-load results

The current `main` build at `ec22e4f` was tested with a primary on `10.0.0.4`
and a replica on `10.0.0.7`. Each Lavik process used 12 workers on CPUs
0-11 and both local NVMe devices through SPDK; CPUs 12-15 were reserved for
network IRQ/softirq work. The primary and replica each contained exactly
400,000,000 `kv_` keys with random 1,000-4,000 byte values. Both nodes remained
at 400,000,000 keys after the test.

Memtier 2.5.1 cluster mode does not currently provide a deterministic weighted
split across the primary and replica: `primary` sends all reads to the primary,
`secondary` sends all reads to replicas, and `--replica-clients` is parsed but
not wired. The test therefore ran two memtier processes concurrently on
`10.0.0.5`. Each used eight threads, ten clients per thread, one request in
flight per connection, and `--rate-limiting=1250`, producing a 100,000-op/s
target per process. The primary process was pinned to CPUs 0-7 with
`--read-preference=primary`; the replica read process was pinned to CPUs 8-15
with `--read-preference=secondary`. Each workload ran for 300 seconds.

Primary workload results while the replica simultaneously served 100,000
GET/s:

    Primary load  SET/s       GET/s       Total/s      Avg       p50       p99       p99.9     p99.99
    100% GET       0           99,996.85   99,996.85    0.267 ms  0.255 ms  0.511 ms  0.815 ms  1.551 ms
    1:1 SET:GET    49,996.65   49,996.45   99,993.10    0.259 ms  0.231 ms  0.791 ms  2.607 ms  4.095 ms
    100% SET       99,999.77   0           99,999.77    0.229 ms  0.199 ms  0.831 ms  1.671 ms  2.431 ms

Concurrent replica-only GET results:

    Primary load  Replica GET/s  Avg       p50       p99       p99.9     p99.99
    100% GET       99,999.28      0.261 ms  0.255 ms  0.455 ms  0.647 ms  0.935 ms
    1:1 SET:GET    99,986.19      0.269 ms  0.255 ms  0.543 ms  1.319 ms  2.303 ms
    100% SET       99,999.76      0.289 ms  0.271 ms  0.663 ms  1.671 ms  3.503 ms

Average Lavik process CPU usage, where 100% is one logical CPU:

    Primary load  Primary CPU  Replica CPU
    100% GET       542.66%      557.62%
    1:1 SET:GET    486.52%      630.30%
    100% SET       457.44%      703.37%

All six memtier processes exited successfully with zero error responses,
warnings, misses, MOVED, or ASK replies. Replication remained online with 12
data flows and `lag=0`; neither Lavik log gained a new warning or error. The
replica CPU increase from 5.58 cores in the read/read workload to 7.03 cores
while the primary wrote at 100,000 SET/s measures the extra replication-apply
cost while it continued serving the same 100,000 GET/s.

Raw memtier logs are on `10.0.0.5` as
`/tmp/lavik-100k-{read,mixed,write}-{primary,secondary}-300s.memtier`.
Per-second Lavik CPU samples and before/after Prometheus snapshots are on the
primary as `/tmp/lavik-100k-{read,mixed,write}-{master,replica}.pidstat` and
`/tmp/lavik-100k-{read,mixed,write}-{master,replica}-{before,after}.metrics`.

The identical six tests were repeated at a 50,000-op/s target per process by
changing only the per-connection rate limit from 1,250 to 625. Memtier's
achieved rate was about 49,920 op/s because of its rate-timer granularity.

Primary workload results while the replica simultaneously served 50,000
GET/s:

    Primary load  SET/s       GET/s       Total/s      Avg       p50       p99       p99.9     p99.99
    100% GET       0           49,920.86   49,920.86    0.271 ms  0.255 ms  0.639 ms  1.199 ms  1.791 ms
    1:1 SET:GET    24,958.41   24,958.19   49,916.61    0.255 ms  0.239 ms  0.703 ms  1.279 ms  3.199 ms
    100% SET       49,920.27   0           49,920.27    0.234 ms  0.207 ms  0.791 ms  1.375 ms  2.127 ms

Concurrent replica-only GET results:

    Primary load  Replica GET/s  Avg       p50       p99       p99.9     p99.99
    100% GET       49,920.43      0.262 ms  0.247 ms  0.543 ms  0.863 ms  1.263 ms
    1:1 SET:GET    49,919.15      0.262 ms  0.247 ms  0.527 ms  0.863 ms  1.711 ms
    100% SET       49,920.41      0.263 ms  0.247 ms  0.575 ms  1.167 ms  2.415 ms

Average Lavik process CPU usage at the 50,000-op/s target:

    Primary load  Primary CPU  Replica CPU
    100% GET       304.79%      320.84%
    1:1 SET:GET    272.18%      391.99%
    100% SET       252.50%      423.93%

The replica's cost increases with primary writes even though its foreground
read rate is fixed: it used 3.21 cores with no incoming replication writes,
3.92 cores while applying about 25,000 SET/s, and 4.24 cores while applying
about 50,000 SET/s. Halving both client targets reduced the replica's 100%-SET
case from 7.03 to 4.24 cores. The mixed and write-heavy tails also improved:
relative to the 100,000-op/s tests, replica p99.9 fell from 1.319 to 0.863 ms
for the mixed case and from 1.671 to 1.167 ms for the write case. The pure
read p99/p99.9 did not improve at the lower rate, so its small tail difference
is not caused by replication apply pressure.

All six 50,000-op/s clients also completed with zero errors, warnings, misses,
MOVED, or ASK replies. Replication remained online with 12 flows and `lag=0`,
and both nodes still reported 400,000,000 keys. The raw logs use the matching
`/tmp/lavik-50k-{read,mixed,write}-{primary,secondary}-300s.memtier` names on
`10.0.0.5`; CPU and metrics files on the primary use the same `lavik-50k-`
prefix.

## 2026-08-18 replication throughput and IRQ session

This session ran from uncommitted changes on Lavik `main` at
`fcb06253ef54`. The source and replica use separate Microsoft NVMe Direct
Disks through SPDK: the source is `spdk://43bc:00:00.0/1` on port 6379 and the
replica is `spdk://58bf:00:00.0/1` on port 6380. Only the first 8 MiB of each
device was zeroed between clean runs.

The original live-replication path allowed only one unacknowledged command per
flow. At about 120k 2-KiB SET/s it fell below the legacy 64-MiB history floor
after roughly eight seconds, disconnected all flows, and required another full
sync. The optimized path now:

- pipelines up to 128 frames or 2 MiB per source flow before reading ordered
  per-command ACKs;
- sends each batch with io_uring `sendmsg` and iovecs, referencing replication
  frame payloads directly instead of copying them into a combined string;
- stores all 18-byte wire headers in one contiguous allocation per batch;
- combines the header and payload of non-backlog data frames into one send;
- retains the existing per-command ACK wire protocol and acknowledged cursor,
  including batches that end in the middle of a fragmented command.

The celer submodule has an uncommitted vectored `TcpStream::WriteAllV` path and
the earlier multishot-recv pause/drain handoff fix. The replication publisher
staging queue is configurable through
`--replication-publish-queue-mb-per-worker`; these tests used 64 MiB per worker. The
default at that time was 8 MiB (the current default is 16 MiB). Increasing that queue fixed publisher staging
invalidation but did not by itself prevent a sustained consumer from falling
below that legacy history floor.

With 8 source cores and 8 replica cores, the optimized path sustained a warm
10-second unlimited run at 222,064 SET/s (about 434 MiB/s), with 0.360 ms
average, 2.655 ms p99, and 4.991 ms p99.9. Source and replica were immediately
equal and all flows remained online; neither log contained backlog, overflow,
or disconnect warnings. A 30-second pre-iovec coalesced-send run also sustained
219,547 SET/s without lag, proving that the 64-MiB queue was not hiding a slow
consumer.

The final runtime layout is 6+6+4 on the 16-vCPU server:

- source Lavik: CPUs 0-5, `--threads=6`;
- replica Lavik: CPUs 6-11, `--threads=6`;
- mlx5 IRQs 58-74: CPUs 12-15, with completion IRQs round-robin;
- memtier remains remote on 10.0.0.5, pinned to its CPUs 8-15.

At a controlled 120k SET/s, a warm same-process IRQ A/B gave:

    IRQ placement       SET/s       Average     p99        p99.9
    mixed CPU 0-15      119,938     0.368 ms    2.007 ms   4.223 ms
    isolated CPU 12-15  119,983     0.313 ms    1.567 ms   3.759 ms

IRQ isolation therefore improved average latency by about 15%, p99 by 22%,
and p99.9 by 11% in the warm A/B. The four IRQ CPUs were only about 3-7% busy.
An unlimited 6+6+4 run sustained 219,931 SET/s with 0.363 ms average, 2.271 ms
p99, and 5.151 ms p99.9, about 1% below the 8+8 peak. The source used nearly
all six cores while the replica used about 2.8 cores.

### SET latency trace

An independent `LAVIK_ENABLE_SET_LATENCY_TRACE` CMake option now instruments
the SET path. It is off by default. The trace reports per-worker 10-second
distributions for cross-worker routing, key and store locks, lookup, append,
new-block wait/allocation, encoding, index update, replication publication,
route-back, and response send. Percentiles are histogram bucket upper bounds,
and phase percentiles are independent rather than additive.

With tracing enabled, the 120k SET/s run produced 119,966 SET/s, 0.322 ms
average, 1.639 ms p99, and 3.823 ms p99.9. Across all six workers the internal
SET p99 was <=1.5 ms. The dominant p99 phase was the origin-to-owner worker
handoff (`route-out`) at <=0.75-1.0 ms; `route-back` and response send were each
<=0.3 ms. The actual owner storage path was <=30 us, append <=15 us, and
replication publication <=1 us. Thus the 120k p99 is scheduler/handoff latency,
not NVMe append or replication-queue latency.

At unlimited load, the trace build sustained 212,578 SET/s with 0.376 ms
average, 2.383 ms p99, and 4.031 ms p99.9. Its trace instrumentation overhead
means this throughput must not be compared directly with the 219,931 SET/s
non-trace result. Internal p99 was <=2-3 ms: `route-out` rose to <=2 ms while
the owner path remained <=30-75 us, append <=8-15 us, replication publication
<=1 us, route-back <=0.3-0.5 ms, and send <=0.5 ms. The same cross-worker
handoff remains the p99 bottleneck near saturation.

Disconnecting the replica while keeping the same trace-enabled six-worker
source showed that replication did not qualitatively change the tail shape:

    Load       Replica  p50       p99/p50  p99.9/p50
    120k/s     yes      0.263 ms     6.23      14.54
    120k/s     no       0.239 ms     6.46      12.51
    unlimited  yes      0.295 ms     8.08      13.66
    unlimited  no       0.263 ms     7.30      13.75

The p99 ratios differ by 3.6% at 120k/s and 10.7% at unlimited load; the p99.9
ratios differ by 16.2% and 0.6%. Connected replication therefore shifts
latency and lowers throughput but does not create a new order-of-magnitude
scheduler tail. The historical fresh-fill result used eight source workers,
no trace, and sequential append. It is not directly comparable to the current
six-worker replicated path.

### Replica session cleanup fix

Promoting the replica with `REPLICAOF NO ONE` and attaching it again exposed a
detached-flow lifetime bug. On a failed full sync the coordinator cancelled
sockets but waited for flow exit only after an already-online session, and it
tracked only flows past handshake. Old flow coroutines could still be inside
storage apply when the replacement session started. Repeated failures left 126
flow connections and eventually exhausted storage write buffers.

`ReplicaSession` now tracks every detached flow from spawn through connect,
handshake, storage apply, and teardown. Every failed or closed session shuts
down its sockets and waits without a retry timeout for `active_flows` to reach
zero before the coordinator can create another session. The same-process
promote/reattach path was added to the e2e regression.

After zeroing only the replica device's first 8 MiB, the patched replica
completed a clean full sync of 27,237,851 keys in about 7.5 minutes. The live
flow metric stayed exactly six throughout, source and replica ended with the
same key count, and the patched replica log had no buffer, flow, or connection
warning. Full-sync throughput was about 60.5k keys/s. Profiling showed the
source at 575% CPU and the replica at only 44%: snapshot batches contain 16
keys and load their values serially, leaving only one random NVMe read in
flight per source worker. The full-sync bottleneck is therefore source-side
snapshot random-read queue depth, not replica apply or network throughput.

### Configurable full-sync snapshot read concurrency

The source snapshot path now overlaps value reads within each 16-key batch.
The maximum in-flight reads per source flow is runtime configurable from 1 to
16 and is sampled again for every batch, so it can be changed during a full
sync without reconnecting either node:

    CONFIG GET replication-snapshot-read-concurrency
    CONFIG SET replication-snapshot-read-concurrency 8

The default remains 1. Snapshot read children run in celer's background task
class, so ordinary client work retains scheduler priority. Their shared key
locks are explicitly released before the parent snapshot coroutine is woken;
completed background frames therefore cannot hold up foreground writes while
waiting for a later cleanup slice. The file-backend replication regression ran
with concurrency 8 over 32 same-partition snapshot keys, including a
promote/reattach full sync, and verified every copied value.

The SPDK validation used the same 6+6+4 CPU/IRQ layout, preserved the source
disk, zeroed only the replica disk's first 8 MiB, and set the source concurrency
to 8 before attaching the empty replica. Full sync copied 27,991,053 keys in
157.275 seconds, averaging 177,976 keys/s. This is 2.94x the previous roughly
60.5k keys/s and reduced the full-sync duration from about 7.5 minutes to 2
minutes 37 seconds. Source CPU averaged about 543% and replica CPU about 117%
during the middle of the snapshot. Both nodes ended at 27,991,053 keys with
six flows online, and neither log contained warnings, errors, overflow, or
disconnects. Logs are `/tmp/lavik-snapshot-q8-{master,replica}.log`.

One independent recovery anomaly preceded the test: immediately before the
source restart its live DBSIZE was 27,237,851, while the new process recovered
27,991,053 keys from the unchanged source disk, an increase of 753,202. This
happened before the replica was started or any parallel snapshot read ran, so
it is not caused by the concurrency change, but it requires separate recovery
correctness investigation.

The same runtime CONFIG table also exposes defrag and tomb-raider controls.
`CONFIG GET` applies its glob pattern once across the static descriptor table
and reads only matched values. Supported names are `defrag-paused`,
`defrag-max-active-per-device`, `defrag-sleep-ms`,
`defrag-record-sleep-us`, `tomb-raider-mode`,
`tomb-raider-interval-ms`, `tomb-raider-sleep-ms`, and
`tomb-raider-daily-time`. For example:

    CONFIG GET defrag-*
    CONFIG SET defrag-paused yes
    CONFIG GET tomb-raider-*
    CONFIG SET tomb-raider-interval-ms 60000

The trace-run logs are
`/tmp/lavik-fcb0625-settrace-6c-{master,replica}.log`; the clean patched
replica log is `/tmp/lavik-fcb0625-sessionfix-replica.log`. The final live
state is 27,237,851 keys on both nodes,
`lavik_replication_state:online`, and six connected flows. `irqbalance` is
not installed or active, but the manual IRQ affinity is runtime state and must
be reapplied after a reboot or NIC driver rebind.

## Current source state

- Lavik branch: main; this handoff update contains the SPDK tail-latency
  tuning and default worker-pinning integration.
- Celer submodule: 2f93c69 perf: reduce SPDK worker tail latency
- mimalloc submodule: acf2fdd (v3.4.5)
- io_uring build directory: ./bld
- SPDK build directory: ./bld-spdk
- Two processes are live on CPUs 0-7 for read-tail A/B. The SPDK build uses
  `spdk://69f9:00:00.0/1` on Redis/metrics ports 6379/9100 and has 200,000,000
  keys in the benchmark range and currently reports 206,811,216 total logical
  keys after a newer refill. The io_uring build uses `/dev/nvme1n1` on ports
  6380/9101 and has 250,931,993 logical keys. Both use 256 MiB registered
  storage buffers per worker, a 60,000 ms mimalloc purge delay, tomb raider
  disabled, and defrag paused. At idle each process consumes about 4% CPU
  because busy-poll is 20us.
- Prometheus scrapes both metrics ports. Grafana histogram quantiles retain the
  `instance` label instead of incorrectly merging buckets across servers.
- aerospike-bench.conf, bld/, bld-libc/, bld-spdk/, celer-raft/,
  perf_reports/, and the local block-device helper are untracked and
  intentionally not committed.

Build:

    cmake -S . -B bld \
      -DCMAKE_BUILD_TYPE=Release \
      -DLAVIK_ENABLE_READ_LATENCY_TRACE=OFF \
      -DLAVIK_WITH_SPDK=OFF
    cmake --build bld -j 8

Mimalloc is now mandatory: `LAVIK_USE_MIMALLOC` no longer exists. Every
Lavik build links mimalloc 3.4.5, compiles with `MI_NO_THP=ON` and
`MI_DEFAULT_ARENA_EAGER_COMMIT=1`. Recovery keeps mimalloc's native 1,000 ms
purge delay; after every worker has finished recovery and forced a local heap
collection, Lavik switches to the configured online delay, which defaults to
60,000 ms. The startup log is the source of truth and should report
`recovery_purge_delay=1000 online_purge_delay=60000 arena_eager_commit=1
allow_thp=0`, followed by the online-switch log after recovery.

SPDK build:

    git submodule update --init --recursive
    sudo apt-get install -y \
      build-essential cmake pkg-config ninja-build meson python3 \
      python3-tabulate python3-pyelftools libnuma-dev uuid-dev libaio-dev \
      libssl-dev
    cmake -S . -B bld-spdk \
      -DCMAKE_BUILD_TYPE=Release \
      -DLAVIK_ENABLE_READ_LATENCY_TRACE=OFF \
      -DLAVIK_WITH_SPDK=ON
    cmake --build bld-spdk -j 8

`LAVIK_WITH_SPDK=ON` changes only the storage backend. Networking remains on
celer/io_uring. SPDK v26.05 is a nested celer submodule pinned at
`d519b163cbc0e2f28c35d9bc86d610da368b032c`; SPDK and DPDK are linked
statically, so `bld-spdk/lavik` has no runtime `libspdk` or `librte`
dependency.

## Machine and storage

- Lavik uses CPUs 0-7 with 8 workers.
- memtier uses CPUs 8-15 with 8 threads and 10 connections per thread.
- Historical io_uring device: `/dev/nvme1n1`, PCI `021d:00:00.0`,
  1,920,383,410,176 bytes total. It still contains the previous dataset.
- Current SPDK test device: first NVMe, PCI `69f9:00:00.0`, namespace 1,
  exposed to Lavik as `spdk://69f9:00:00.0/1` with the same capacity.
- Current Lavik automatically uses the complete raw-device capacity. It
  exposed 228,926 8 MiB data blocks, including eight defrag-reserve blocks,
  and 1,920,370,475,008 usable data bytes in this session.
- The first NVMe was verified unmounted and without a filesystem signature,
  then destructively initialized for this SPDK test. While bound to vfio-pci,
  `/dev/nvme0n1` intentionally does not exist.
- Logical sector size: 512 bytes.
- Secure Boot lockdown rejects direct BAR mapping through `uio_pci_generic` on
  this Azure host. VFIO works only with temporary no-IOMMU mode because the VM
  exposes no IOMMU group. This permits DMA without IOMMU isolation; do not use
  it with an untrusted process.
- Warning: both NVMe devices now contain benchmark datasets. Do not discard or
  format either unless a fresh fill is intended.

## Current dataset

- The SPDK device contains all 200 million benchmark keys. After the latest
  user refill it reports 206,811,216 total logical keys; the earlier exactly
  200-million-key fill described below remains the historical comparison.
- Prefix: kv_
- Range: 1 through 200000000.
- Values are fixed at 2000 bytes.
- Mixed tests overwrite existing keys.
- SPDK fill rate: 475,698 SET/s. The comparable io_uring historical fill was
  489,176 SET/s, so SPDK was 2.8% slower in this run.
- Records are packed inside 8 MiB storage blocks.
- Average physical GET read size is 2.56 KiB after 512-byte alignment.
- The clean post-fill SPDK restart scanned exactly 200 million records in
  116.3 seconds at 1.72 million records/s; all workers were ready 150.5 seconds
  after launch. `DBSIZE` returned exactly 200 million, and keys 1, 100000000,
  and 200000000 each had a 2000-byte value.
- The formal 1:10 and 1:1 tests subsequently added overwrite records but did
  not change the logical key count.
- The final post-test restart scanned 215,545,468 physical record versions.
  After recovery, `DBSIZE` was still exactly 200 million; keys 1, 100000000,
  and 200000000 were each 2000 bytes, and all defrag result counters remained
  zero.

Initial fill:

    taskset -c 8-15 memtier_benchmark \
      -t 8 -c 10 -s 10.0.0.4 -p 6379 \
      -n allkeys \
      --distinct-client-seed \
      --ratio=1:0 \
      --key-prefix="kv_" \
      --key-minimum=1 \
      --key-maximum=200000000 \
      --random-data \
      --data-size-range=2000-2000 \
      --data-size-pattern=R \
      --hide-histogram \
      --key-pattern=P:P

Ordinary blkdiscard did not guarantee zero reads on this Azure NVMe. To erase
and refill an io_uring device, stop Lavik and use the zeroing discard:

    sudo blkdiscard -z -f --length 644245094400 /dev/nvme1n1

This command is destructive.

## Lavik launch

The current SPDK server uses:

    sudo taskset -c 0-7 ./bld-spdk/lavik \
      --bind=10.0.0.4 \
      --port=6379 \
      --metrics-port=9100 \
      --recv-buffers-per-worker=1024 \
      --registered-buffer-mb-per-worker=256 \
      --busy-poll-us=20 \
      --background-budget-us=10 \
      --background-warrant-percent=1 \
      --spdk-max-completions-per-poll=8 \
      --spdk-foreground-pre-poll-us=5 \
      --mimalloc-purge-delay-ms=60000 \
      --data-file=spdk://69f9:00:00.0/1 \
      --threads=8 \
      --flush-max-ms=1000 \
      --flush-size-kb=128 \
      --tomb-raider-interval-ms=0 \
      --tomb-raider-sleep-ms=10 \
      --defrag-paused \
      --defrag-max-active-per-device=1 \
      --defrag-sleep-ms=100

Worker pinning is enabled by default. The runtime enumerates the inherited
CPU-affinity mask in ascending order and pins worker `i` to allowed CPU `i`.
For the command above the eight workers are therefore pinned one-to-one to
CPUs 0-7. `--no-pin-workers` restores the old behavior in which every worker
inherits the whole 0-7 mask and may migrate between those CPUs.

Before starting it, reserve hugepages and bind only the first controller:

    sudo modprobe vfio-pci
    sudo sh -c \
      'echo 1 > /sys/module/vfio/parameters/enable_unsafe_noiommu_mode'
    cd celer/third_party/spdk
    sudo env PCI_ALLOWED='69f9:00:00.0' DRIVER_OVERRIDE=vfio-pci \
      HUGEMEM=4096 ./scripts/setup.sh
    cd ../../..

To return only that controller to the kernel NVMe driver after Lavik stops:

    cd celer/third_party/spdk
    sudo env PCI_ALLOWED='69f9:00:00.0' ./scripts/setup.sh reset
    cd ../../..

The comparable io_uring launch is:

    sudo taskset -c 0-7 ./bld/lavik \
      --bind=10.0.0.4 \
      --port=6380 \
      --metrics-port=9101 \
      --recv-buffers-per-worker 1024 \
      --registered-buffer-mb-per-worker=256 \
      --busy-poll-us=20 \
      --background-budget-us=10 \
      --background-warrant-percent=1 \
      --mimalloc-purge-delay-ms=60000 \
      --data-file=/dev/nvme1n1 \
      --threads=8 \
      --flush-max-ms=1000 \
      --flush-size-kb=128
      --tomb-raider-interval-ms=0 \
      --tomb-raider-sleep-ms=10 \
      --defrag-paused \
      --defrag-max-active-per-device=1 \
      --defrag-sleep-ms=100

### Worker-affinity and SPDK polling retest

The 2026-08-10 tail investigation isolated the two servers by pausing the
inactive process with `SIGSTOP`; Prometheus scraping was also disabled for one
control. Removing Prometheus made no measurable difference: SPDK remained at
0.22182 ms average, 1.111 ms p99.9, and 2.479 ms p99.99. Grafana queries
Prometheus rather than Lavik directly, and scrape duration was about 1 ms,
so monitoring was ruled out as the SPDK tail source.

Celer previously inherited the process-level `taskset -c 0-7` mask for every
worker but did not pin individual worker threads. SPDK workers remain runnable
while their qpairs have outstanding I/O, making scheduler migration and
preemption much more visible than for io_uring workers that block awaiting
CQEs. Dynamically pinning worker 0 through worker 7 to CPUs 0 through 7 changed
the isolated 40-second pure-read result as follows:

    Backend     Worker affinity    GET/s       Average       p99.9       p99.99
    SPDK        shared 0-7 mask    99,997.27   0.22182 ms    1.111 ms    2.479 ms
    SPDK        one CPU/worker     99,994.01   0.22188 ms    0.959 ms    1.775 ms
    io_uring    shared 0-7 mask    99,994       0.24220 ms    1.151 ms    2.127 ms
    io_uring    one CPU/worker     99,994.01   0.24313 ms    1.039 ms    2.079 ms

Pinning improved SPDK p99.99 by 28.4% in this A/B and improved io_uring by
only 2.3%. The runtime now performs that one-to-one mapping by default, using
the inherited affinity list rather than assuming CPU numbers start at zero.

The SPDK hot path was also changed to reuse a preallocated per-worker request
pool instead of allocating one callback object per I/O. Completion processing
can be capped with `--spdk-max-completions-per-poll`; multiple namespaces are
polled round-robin under the shared cap. A small
`--spdk-foreground-pre-poll-us` slice prevents storage polling from repeatedly
winning over fresh network and cross-worker work. Trace runs showed that about
98.4% of SPDK poll calls were empty; normal nonempty batches were small, so the
cap is primarily a burst guard rather than the main average-latency change.

The deployed release settings are a completion cap of 8 and a 5 us foreground
pre-poll slice. Before rebasing the newly arrived persistent-list commit, a
warm isolated run against the earlier exactly 200,000,000-key state produced:

    GET/s:       99,995.39
    average:     0.22078 ms
    p99.9:       0.967 ms
    p99.99:      1.719 ms

An immediately preceding 40-second run contained a one-second throughput dip
and reported 2.719 ms p99.99. Similar isolated runs alternated between clean
1.7-2.5 ms tails and occasional scheduler/allocator/host stalls. The clean
result establishes that SPDK itself is no longer slower than io_uring here,
but a longer production-window percentile must retain those system-level
stalls rather than selecting only the best interval.

After rebasing Lavik `e552276` and deploying final main `231da6b` with Celer
`2f93c69`, the user-refilled dataset reported 206,811,216 total keys. Keys
`kv_1`, `kv_100000000`, and `kv_200000000` were each 2000 bytes, and the full
random benchmark range was all hits. The first post-recovery window contained
two short throughput stalls and reported 3.215 ms p99.99. The immediately
following clean 40-second hot-state window reported:

    GET/s:       99,996.83
    average:     0.22262 ms
    p99.9:       0.959 ms
    p99.99:      1.855 ms

This final-main clean p99.99 is 10.8% below the 2.079 ms worker-pinned io_uring
comparison and 51.9% below the user's original 3.855 ms SPDK observation.

### 60-second mimalloc purge-delay restart

The SPDK process was restarted twice against the same 200,000,000-key device
with every option unchanged except mimalloc purge delay. Recovery throughput
was effectively unchanged: both runs scanned near 1.71-1.72M records/s and
became ready in about 206 seconds.

With purge disabled, ready-state allocator live bytes were 27.45 GB while RSS
was 74.83 GB; allocator active was 93.23 GB. With a 60,000 ms delay, RSS was
52.31 GB immediately after recovery and 27.53 GB after one purge cycle, while
live bytes remained 27.45 GB. SPDK additionally mapped 3.72 GiB of hugetlb
memory from the separately configured 4 GiB hugepage pool.

This demonstrates recovery-time retained pages rather than a larger live
index. Mimalloc can reuse compatible pages, but per-thread heaps, size classes,
hash-table growth, and abandoned segments prevent arbitrary immediate reuse.
On allocation failure mimalloc forces a collect/purge and retries once, but it
does not proactively monitor Linux `MemAvailable` or cgroup pressure. Linux may
invoke the OOM killer before that retry, and `purge=-1` disables OS purging even
for the forced collect path. Keep the 60-second delay for the dual-instance A/B
unless a controlled latency test explicitly changes it.

`--data-file-size-mb` no longer exists. Raw block devices use their
persisted or detected full capacity, so passing the old option aborts startup.
The memory-accounting fix makes the default automatic limit usable on
this dataset. Do not restore the historical `--max-memory=1tb` benchmark
workaround. The current tomb-raider defaults are a 24-hour interval
(`--tomb-raider-interval-ms=86400000`) and a 10 ms per-block sleep.

Historical tcmalloc launch used before mimalloc became mandatory (not the
current process and not supported by current CMake):

    sudo env LD_PRELOAD=/lib/x86_64-linux-gnu/libtcmalloc.so.4 \
      taskset -c 0-7 ./bld-libc/lavik \
      --bind=10.0.0.4 \
      --port=6379 \
      --metrics-port=9100 \
      --recv-buffers-per-worker=1024 \
      --registered-buffer-mb-per-worker=64 \
      --busy-poll-us=20 \
      --data-file=/dev/nvme1n1 \
      --threads=8 \
      --flush-max-ms=1000 \
      --flush-size-kb=128 \

That command requires the historical `bld-libc` binary. It cannot be recreated
from current main because the allocator switch was removed.

Graceful stop:

    pid=$(pgrep -n -x lavik)
    kill -INT "$pid"

Wait for shutdown so partial write buffers are flushed before restarting.

## Storage changes in f71dc45

- Raw block devices use BLKSSZGET to discover direct-I/O alignment.
- /dev/nvme1n1 therefore uses 512-byte read offset and length alignment.
- Regular files continue to use 4096-byte alignment.
- --flush-size-kb controls the maximum storage write submission.
- Default --flush-size-kb is 128, matching the tested 128 KiB submission size.
- The 8 MiB write buffer, on-disk block, index, and recovery format are
  unchanged.
- Registered slices use WriteFixed; heap fallback buffers use ordinary async
  write.
- Flush size must be a power of two between direct-I/O alignment and 8 MiB.
- Detailed GET phase latency logging is disabled in both current builds. The
  older 99e1d01 trace path had a compile issue; that historical limitation is
  not evidence about current main.

Aerospike reference:

- flush-size controls device submission size, not write-block size.
- Aerospike allows values starting at 4 KiB.
- Its default is 1 MiB.
- The sample SSD configuration uses 128 KiB.

## Standard benchmark commands

With 8 memtier threads, 10 connections each, and rate-limiting 1250, total
requested rate is 100,000 operations/s.

Pure random read, 60 seconds:

    taskset -c 8-15 memtier_benchmark \
      -t 8 -c 10 -s 10.0.0.4 -p 6379 \
      --test-time 60 \
      --distinct-client-seed \
      --ratio=0:1 \
      --key-prefix="kv_" \
      --key-minimum=1 \
      --key-maximum=200000000 \
      --random-data \
      --data-size-range=2000-2000 \
      --data-size-pattern=R \
      --hide-histogram \
      --print-percentiles="99.9,99.99" \
      --rate-limiting=1250 \
      --randomize

SET:GET = 1:10, 60 seconds:

    taskset -c 8-15 memtier_benchmark \
      -t 8 -c 10 -s 10.0.0.4 -p 6379 \
      --test-time 60 \
      --distinct-client-seed \
      --ratio=1:10 \
      --key-prefix="kv_" \
      --key-minimum=1 \
      --key-maximum=200000000 \
      --random-data \
      --data-size-range=2000-2000 \
      --data-size-pattern=R \
      --hide-histogram \
      --print-percentiles="99.9,99.99" \
      --rate-limiting=1250 \
      --randomize

SET:GET = 1:1, 300 seconds:

    taskset -c 8-15 memtier_benchmark \
      -t 8 -c 10 -s 10.0.0.4 -p 6379 \
      --test-time 300 \
      --distinct-client-seed \
      --ratio=1:1 \
      --key-prefix="kv_" \
      --key-minimum=1 \
      --key-maximum=200000000 \
      --random-data \
      --data-size-range=2000-2000 \
      --data-size-pattern=R \
      --hide-histogram \
      --print-percentiles="99.9,99.99" \
      --rate-limiting=1250 \
      --randomize

Collect iostat without the misleading since-boot first report:

    iostat -y -t -xmd 1 305 > /tmp/lavik-test.iostat

This works only while the controller is owned by the kernel NVMe driver. SPDK
owns the first controller through VFIO, so its operations are intentionally
absent from `iostat` and the kernel block layer.

## Results

### SPDK tomb-raider long-run baseline

Before the two-hour online-defrag observation on 2026-08-10, the SPDK server
was cleanly restarted with the normal 600,000 ms tomb-raider interval and 10 ms
per-block sleep. Tomb raider was then temporarily switched off at runtime so
the three standard baseline windows contained no background scan or defrag.
All windows requested 100,000 operations/s over the complete 200-million-key
`kv_` range. `DBSIZE` remained exactly 200 million, every GET was a hit, OOM
rejections stayed zero, and all defrag counters stayed zero.

    Workload       Ops/s       Average       p99.9       p99.99      Server CPU
    Pure GET       99,971.82   0.21274 ms    1.271 ms    5.983 ms     271.52%
    1:10 total     99,989.60   0.21748 ms    1.503 ms    5.311 ms     253.27%
    1:1 total      99,999.46   0.18219 ms    1.231 ms    3.007 ms     222.76%

The 1:10 split was 9,090.65 SET/s at 0.08924 ms average and 90,898.94
GET/s at 0.23031 ms average. The five-minute 1:1 split was 49,999.78 SET/s at
0.11645 ms average and 49,999.67 GET/s at 0.24793 ms average. Its SET/GET
p99.99 values were 2.463/3.455 ms. These are the no-background-work reference
numbers for the following two-hour 1:1 run.

Artifacts use this prefix:

    /tmp/lavik-spdk-defrag-basic-{read,1to10,1to1}.{memtier,pidstat}
    /tmp/lavik-spdk-defrag-basic-{read,1to10,1to1}.{before,after}.metrics

The two-hour 1:1 run started at `2026-08-10T08:36:31Z` and is scheduled to end
at `2026-08-10T10:36:31Z`. It uses the same 100,000 ops/s command as the
five-minute baseline. Tomb raider was reset to `INTERVAL 600000` immediately
before the workload, so the first background round is expected approximately
ten minutes into the window. Prometheus and Grafana were both healthy when the
run started. Full-window artifacts are:

    /tmp/lavik-spdk-defrag-2h-1to1.memtier
    /tmp/lavik-spdk-defrag-2h-1to1.pidstat
    /tmp/lavik-spdk-defrag-2h-1to1.timeline
    /tmp/lavik-spdk-defrag-2h-1to1.{before,after}.metrics
    /tmp/lavik-spdk-defrag-2h-1to1.{before,after}.info

### SPDK v26.05 backend on the first NVMe

This 2026-08-10 test used the local `LAVIK_WITH_SPDK=ON` implementation,
SPDK v26.05, statically linked DPDK, mandatory mimalloc, eight Lavik workers
on CPUs 0-7, and the first NVMe at `69f9:00:00.0`. Networking remained
io_uring. The storage URI was `spdk://69f9:00:00.0/1`, flush submissions were
128 KiB, purge and tomb raider were disabled, THP was compile-time disabled,
and eager arena commit was compile-time enabled.

The first multi-worker attempt exposed a critical DPDK integration detail:
`spdk_env_init(core_mask=0x1)` changed the calling thread's affinity to CPU 0,
so all subsequently created Lavik workers inherited CPU 0. Fill throughput
was only 52,547 SET/s. The backend now saves and restores the caller's affinity
around SPDK initialization. After the fix, all workers inherited CPUs 0-7 and
the device was zeroed before the formal refill.

Fresh 200-million-key fill:

    SET/s:       475,698.23
    average:     0.16778 ms
    p99:         0.527 ms
    p99.9:       1.535 ms
    p99.99:      2.207 ms
    records:     200,000,000

This is 2.8% below the comparable io_uring fill rate of 489,175.53 SET/s. A
graceful restart then recovered exactly 200 million physical and logical
records. The data-block scan took approximately 116.3 seconds at 1.72 million
records/s; all eight workers were ready about 150.5 seconds after launch. These
times are effectively the same as the historical io_uring recovery.

Two clean 60-second pure-read windows at 100,000 requested GET/s gave:

    Window    GET/s       Average       p99.9       p99.99
    A         99,976.36   0.22238 ms    1.479 ms    6.079 ms
    B         99,951.61   0.21323 ms    1.367 ms    6.623 ms
    Mean      99,963.99   0.21781 ms    1.423 ms    6.351 ms

All GETs were hits. Server CPU during the loaded portion was approximately
255%. Against the 3a247ca io_uring mean, SPDK improved average latency by 7.9%
and CPU by roughly 6%, but p99.9 increased 14.4% and p99.99 increased 49.8%.
The extreme-tail regression repeated in both full windows; it is not a single
sample anomaly.

SET:GET = 1:10, clean 60-second window:

    Type       Ops/s       Average       p99.9       p99.99
    SET         9,091.20   0.08311 ms    0.839 ms    3.151 ms
    GET        90,903.68   0.22064 ms    1.143 ms    4.639 ms
    Total      99,994.88   0.20813 ms    1.127 ms    4.479 ms

Server CPU was approximately 261%. Compared with the same tomb-raider-off
io_uring window, average latency improved 10.5%, p99.9 improved 1.4%, CPU was
effectively unchanged, and p99.99 regressed 40.7%.

SET:GET = 1:1, full 300-second window:

    Type       Ops/s       Average       p99.9       p99.99
    SET        49,999.60   0.11647 ms    1.199 ms    2.831 ms
    GET        49,999.35   0.24791 ms    1.423 ms    3.631 ms
    Total      99,998.95   0.18219 ms    1.319 ms    3.279 ms

Against the historical io_uring five-minute result, SPDK improved average
latency by 6.1%, while p99.9 regressed 7.9% and p99.99 regressed 29.8%. The
logical count remained exactly 200 million after every test. There were no OOM
rejections, qpair/completion errors, or formal-window defrag runs or errors.

Conclusion: this first implementation is functionally sound and removes some
average-path overhead, but it is not yet a tail-latency win. The next useful
work is correlated per-request SPDK submit/completion tracing and qpair polling
delay measurement. Kernel `iostat` cannot observe the VFIO-owned controller,
so do not compare the SPDK run using missing block-layer statistics.

One pre-refill smoke test repeatedly overwrote the same redis-benchmark key
about 10,000 times. After restart, the existing Lavik defrag path reported
`block live-byte accounting underflow`, and a later FLUSHALL reclamation
reported that the storage writer had stopped. The fresh sequential refill and
all formal SPDK windows did not reproduce it: defrag error stayed zero. Keep
this separate overwrite/defrag issue visible rather than treating the smoke
failure as an NVMe completion error.

Artifacts:

    /tmp/lavik-spdk-fill.perf.data
    /tmp/lavik-spdk-fill.perf.record

### Latest 8c171a9 kernel perf attribution before SPDK

The user-run 1:1 memtier workload on the io_uring build used 16 threads, ten
connections per thread, a total requested rate of 100,000 ops/s, the
`kv_1..kv_200000000` range, random 1000-4000-byte values, and tomb raider
disabled. A 30-second profile saved as
`/tmp/lavik-8c171a9-live-memtier.perf.data` captured 48,134 samples with zero
lost samples.

The raw perf DSO split was 57.31% kernel, 39.42% Lavik, and 2.89% libc, but
perf callchain collection inflated kernel cost. A less intrusive pidstat window
measured 120.6% user and 108.8% system CPU, so kernel work was approximately
47.4% of server CPU. The device sustained about 50,000 reads/s and 132 MiB/s,
about 1,000 writes/s and 126 MiB/s, 0.12-0.14 ms await, and roughly 33% device
utilization.

`nvme_submit_cmds` accounted for 16.04% of all perf samples; the remaining
block/storage kernel path was roughly another 1%. Therefore about 17% of total
on-CPU samples were work that SPDK could plausibly bypass. The 7.50%
`_raw_spin_unlock_irqrestore` entry was traced through loopback TCP receive and
send paths, not filesystem or NVMe work, so SPDK storage cannot remove it.

### Mimalloc purge, eager-commit, and THP environment A/B

This 2026-08-10 UTC A/B used the unchanged 3a247ca Release binary with built-in
mimalloc 3.4.5; no code was changed or rebuilt. Each configuration was applied
at process startup, one variable at a time except for the explicitly requested
purge-off plus THP-off combination. Every restart recovered the same
237,312,012 physical records and 220,716,329 logical keys. After a 15-second
warmup, each reported window read random keys from `kv_1..kv_200000000` for 60
seconds at a requested 100,000 GET/s. All GETs were hits.

The table reports clean-window means. Control includes two opening windows and
one closing confirmation; the other rows include two windows each.

    Configuration                   GET/s       Average     p99.9      p99.99     vs control   CPU       minflt/s   RSS
    control (no override)           99,996.93   0.23562 ms  1.130 ms   3.567 ms      --        267.41%    17.89     31.78 GB
    PURGE_DELAY=-1                  99,996.67   0.23693 ms  1.151 ms   2.087 ms    -41.5%      263.95%     0.00     55.12 GB
    ARENA_EAGER_COMMIT=1            99,996.51   0.23430 ms  1.063 ms   3.375 ms     -5.4%      269.05%    14.16     31.80 GB
    ALLOW_THP=0                     99,997.45   0.23820 ms  1.175 ms   2.183 ms    -38.8%      269.52%    23.44     30.76 GB
    PURGE_DELAY=-1 + ALLOW_THP=0    99,997.65   0.23346 ms  0.991 ms   1.903 ms    -46.6%      273.18%     0.36     53.88 GB

Disabling purge is the strongest isolated result. Its p99.99 values were 2.175
and 1.999 ms versus the control's 3.695, 3.519, and closing 3.487 ms. It removed
measured minor faults from the read windows, strongly linking the old extreme
tail to purged pages being faulted back in. The tradeoff is substantial: RSS
rose about 73% because unused pages were no longer returned to the OS.

Setting `MIMALLOC_ARENA_EAGER_COMMIT=1` did not produce a material independent
change. Mimalloc's Linux default is `2`, which already enables eager arena
commit on an overcommit OS, so this result is expected and does not support
first-touch commit as the main tail source.

Disabling THP alone also reduced p99.99, but average latency increased 1.1%,
p99.9 increased 4.0%, minor faults increased, and startup-to-ready recovery was
approximately 190.0 seconds versus about 177-179 seconds for control. RSS fell
about 3.2%. It is useful for extreme tail but has broader performance costs.

The combined purge-off plus THP-off setting was best overall in these windows:
p99.99 fell 46.6% versus control and 8.8% versus purge-off alone, while p99.9
fell to 0.991 ms. Its RSS remained high because purge was disabled. Combination
recovery took approximately 184.2 seconds, slower than control but faster than
THP-off alone.

All clean windows sustained 100,000 device reads/s, about 253.3 MiB/s, and
0.13-0.14 ms average read await. One first combination window hit the recurring
NVMe slow plateau (96,656 GET/s, 0.293 ms average latency, and 324.6% server
CPU); it is preserved but excluded from every mean above. The closing control
returned to 3.487 ms p99.99 with normal device metrics, ruling out test-order
drift as the explanation for the improvements. No configuration recorded an
OOM rejection.

The server was finally left running in the no-override mimalloc control
configuration.

With purge and THP both enabled (the no-override mimalloc defaults), a clean
SET:GET=1:10, 60-second mixed window gave:

    Type       Ops/s       Average       p99.9        p99.99
    SET         9,091.80   0.10847 ms    0.943 ms     1.927 ms
    GET        90,907.94   0.30037 ms    2.847 ms     3.503 ms
    Total      99,999.74   0.28292 ms    2.831 ms     3.455 ms

Server CPU averaged 295.05%, with 45.98 minor faults/s. The device averaged
91,889.6 reads/s, 722.14 MiB/s reads, 0.190 ms read await, 163.3 writes/s,
18.56 MiB/s writes, and 0.271 ms write await. A later source and timing audit
showed that the unexpectedly high read bandwidth was the periodic tomb-raider
sweep, not SET read amplification or defrag. The raider wakes every 600 seconds,
reads each allocated 8 MiB records block, and sleeps 10 ms per block; the device
splits those large reads into approximately 512 KiB operations. This matches the
observed extra approximately 470 MiB/s and 1,000 reads/s. Plain SET only looks
up the old metadata in memory and appends a new record; it loads the old value
only for the SET GET option. Current allocator bytes changed only from
30,794,996,584 to 30,795,828,072; RSS remained about 31.85 GB and OOM rejections
stayed zero.

An immediate confirmation window, still overlapping the tomb-raider sweep,
reproduced the approximately 692 MiB/s read bandwidth but entered the recurring
NVMe slow plateau and delivered only 96,553 ops/s; it is excluded from the clean
result above. Defrag run, active, and pending metrics were all zero.

To isolate the mixed workload, Lavik was then gracefully restarted with
`--tomb-raider-interval-ms=0`. Before the test, five consecutive idle samples
reported zero device reads and writes, and `INFO` reported zero tomb-raider
rounds. The clean SET:GET=1:10 window gave:

    Type       Ops/s       Average       p99.9        p99.99
    SET         9,091.71   0.10016 ms    0.927 ms     4.351 ms
    GET        90,906.48   0.24571 ms    1.159 ms     3.103 ms
    Total      99,998.19   0.23248 ms    1.143 ms     3.183 ms

Server CPU averaged 259.10%. The device averaged 90,906.1 reads/s, 234.67
MiB/s reads, 0.140 ms read await, 164.5 writes/s, 18.70 MiB/s writes, and 0.439
ms write await. Tomb-raider rounds remained zero throughout, memory-limit
rejections remained zero, and current allocator bytes changed only from
31,647,278,616 to 31,651,356,176.

Compared with the tomb-raider-overlapped full-throughput window, disabling the
raider for this test reduced read bandwidth by 67.5%, server CPU by 12.2%,
average latency by 17.8%, and p99.9 by 59.6%. This confirms that plain SET does
not cause the observed read amplification; it came from the background sweep.

Artifacts:

    /tmp/lavik-3a247ca-miab-control-*
    /tmp/lavik-3a247ca-miab-purge-off-*
    /tmp/lavik-3a247ca-miab-eager-commit-*
    /tmp/lavik-3a247ca-miab-thp-off-*
    /tmp/lavik-3a247ca-miab-purge-thp-off-*
    /tmp/lavik-3a247ca-miab-control-confirm-*
    /tmp/lavik-3a247ca-miab-control-ratio1-10*
    /tmp/lavik-3a247ca-mimalloc-tomb-off-*

### Latest main 3a247ca mimalloc memory-accounting retest

This 2026-08-10 UTC retest fast-forwarded main from 99e1d01 to 3a247ca and
rebuilt Release with `LAVIK_USE_MIMALLOC=ON`, mimalloc 3.4.5, LTO, and
detailed GET tracing disabled. Celer remained at b3d78fe. The existing raw-disk
dataset was preserved; no refill was required.

Lavik started without an explicit `--max-memory` override. It selected the
automatic 108,010,510,746-byte (100.59 GiB) limit. Recovery scanned 215,543,309
physical records in 133.12 seconds and all workers were ready after 154.22
seconds. `DBSIZE` returned exactly 200,000,000, and `kv_1`, `kv_100000000`, and
`kv_200000000` all had 2,000-byte values.

Three clean 60-second pure-read windows at a requested 100,000 operations/s
gave:

    Window    GET/s        Average       p99.9        p99.99       Server CPU
    A         99,999.52    0.23995 ms    1.319 ms     4.607 ms     266.22%
    B         99,997.01    0.22998 ms    1.119 ms     4.031 ms     279.77%
    C         99,996.14    0.23946 ms    1.295 ms     4.079 ms     265.88%
    Mean      99,997.56    0.23646 ms    1.244 ms     4.239 ms     270.62%

All 18,000,240 GETs were hits. The SSD sustained 100,000 reads/s and about
253.30 MiB/s in every window. One-second average read await was 0.13-0.14 ms;
there were no writes in the pure-read windows. Saved before/after Prometheus
command counters exactly match each memtier operation count, confirming that a
later user-run workload did not overlap these formal windows.

Compared with the two clean 99e1d01 mimalloc windows, average latency increased
1.1%, p99.9 improved 0.5%, and p99.99 increased from 3.903 ms to 4.239 ms
(8.6%). The extreme-tail increase repeated in all three windows, but the rest
of the distribution and the SSD average await did not regress. The old theory
that a 100-ms `mi_stats_get()` sample alone explained the tail is incomplete:
3a247ca now samples only the lightweight sharded live-byte counters every
100 ms and moves mimalloc diagnostics to explicit metrics/INFO refreshes. It
also adds `mi_usable_size()` plus a worker-local accounting update to each C++
allocation and free. That is the main new hot-path candidate, but attributing
the 8.6% p99.99 difference requires a same-commit build that disables only the
allocation hooks; the current measurements do not prove causality.

SET:GET = 1:10, clean 60-second window with the default automatic limit:

    Type       Ops/s       Average       p99.9        p99.99
    SET         9,091.70   0.10306 ms    1.007 ms     2.591 ms
    GET        90,906.36   0.24613 ms    1.191 ms     3.743 ms
    Total      99,998.07   0.23312 ms    1.175 ms     3.599 ms

This mixed result is effectively unchanged at p99.99 from the 99e1d01 valid
workaround run (3.631 ms). Memory accounting remained below the automatic
limit throughout: before and after the mixed window, current used bytes were
27,909,105,296 and 27,874,545,744, while RSS was approximately 28.35 GB
(26.40 GiB). There were zero memory-limit rejections and no OOM responses.
After all test windows, `INFO memory` reported 25.66 GiB current, 26.40 GiB
RSS, and 26.08 GiB
peak current. This confirms the false-OOM bug is fixed and also reflects the
smaller 3a247ca index footprint; the previous process RSS was approximately
38 GB on the same logical dataset.

Artifacts:

    /tmp/lavik-3a247ca-w8-mimalloc-fixed-server.log
    /tmp/lavik-3a247ca-w8-mimalloc-fixed-read80-{a,b,c}.{memtier,pidstat,iostat}
    /tmp/lavik-3a247ca-w8-mimalloc-fixed-read80-{a,b,c}.{before,after}.metrics
    /tmp/lavik-3a247ca-w8-mimalloc-fixed-ratio1-10.{memtier,pidstat,iostat}
    /tmp/lavik-3a247ca-w8-mimalloc-fixed-ratio1-10.{before,after}.metrics

### Latest main 99e1d01 mimalloc retest and allocator tail A/B

This 2026-08-10 UTC retest started from a discarded `/dev/nvme1n1` and
refilled exactly 200,000,000 keys named `kv_1` through `kv_200000000`, each
with a fixed 2,000-byte value. Lavik used CPUs 0-7 with eight workers;
memtier used CPUs 8-15 with eight threads and ten connections per thread. The
build was Release with native optimization, LTO, mimalloc 3.4.5, and detailed
GET tracing disabled.

Fresh fill:

    SET/s:       489,175.53
    average:     0.16337 ms
    p99:         0.615 ms
    p99.9:       1.639 ms
    p99.99:      2.447 ms
    records:     200,000,000
    server CPU:  780.45%
    writes:      8,115.94/s, 996.96 MiB/s

After a graceful shutdown, recovery found exactly 200,000,000 physical and
logical records. The data-block scan took 115.66 seconds at approximately
1.73 million records/s; all eight workers were ready 144.41 seconds after
launch. DB 0 reported 200,000,000 keys, and the first, middle, and last values
were all 2,000 bytes.

The first formal pure-read window used the default automatic memory limit and
preceded any rejected write traffic:

    GET/s:       99,996.20
    average:     0.23091 ms
    p99:         0.559 ms
    p99.9:       1.295 ms
    p99.99:      3.919 ms
    hits:        6,000,080
    misses:      0

The first 1:10 attempt was invalid. After recovery, the allocator gauge was
93,053,255,680 bytes while RSS was approximately 38.0 GB. During overwriting
SETs the gauge rose above the automatically selected 108,010,510,746-byte
limit, even though RSS remained approximately 38.0 GB. Lavik rejected
350,000 commands with `OOM command not allowed when used memory >
'maxmemory'`. The same failure reproduced in the atomicity stress test. The
server was restarted with `--max-memory=1tb`; the invalid window is excluded
from all performance results below.

Pure-read confirmation after the 1 TiB workaround:

    GET/s:       99,998.50
    average:     0.23685 ms
    p99:         0.615 ms
    p99.9:       1.207 ms
    p99.99:      3.887 ms
    hits:        6,000,080
    misses:      0

SET:GET = 1:10, clean 60-second window with the workaround:

    Type       Ops/s       Average       p99          p99.9        p99.99
    SET         9,091.55   0.09475 ms    0.359 ms     0.895 ms     5.375 ms
    GET        90,904.84   0.23774 ms    0.583 ms     1.199 ms     3.583 ms
    Total      99,996.39   0.22474 ms    0.575 ms     1.175 ms     3.631 ms

SET:GET = 1:1, 300 seconds with the workaround:

    Type       Ops/s       Average       p99          p99.9        p99.99
    SET        49,992.23   0.11826 ms    0.567 ms     1.031 ms     1.943 ms
    GET        49,992.11   0.26971 ms    0.847 ms     1.367 ms     2.879 ms
    Total      99,984.34   0.19399 ms    0.727 ms     1.223 ms     2.527 ms

Every valid formal GET was a hit. Full-window server and device averages for
the workaround runs were:

    Workload           CPU       r/s         rMiB/s    rAwait     w/s      wMiB/s   wAwait
    Pure read confirm  263.90%   100,000.0   253.29    0.140 ms     0.0      0.00   0.000 ms
    1:10               266.20%    90,906.0   230.27    0.132 ms   163.4     18.57   0.477 ms
    1:1, 300 seconds   222.14%    49,989.2   126.63    0.121 ms   849.9    102.72   0.172 ms

At the end of the five-minute mixed test, the mimalloc gauge was approximately
110.45 GB while process RSS remained approximately 38.0 GB and the explicit
1 TiB run had zero memory-limit rejections. This confirms that the automatic
limit failure is a gauge/enforcement problem rather than physical memory
exhaustion in this workload.

The new pure-read p99.99 was reproducibly near 3.9 ms even though average,
p99, CPU, device throughput, and average read await did not regress. A
same-commit allocator A/B used `bld-libc`, built with
`LAVIK_USE_MIMALLOC=OFF`; all other build, server, dataset, CPU, and memtier
parameters were unchanged. Two clean 60-second windows gave:

    Allocator      Average       p99          p99.9        p99.99
    mimalloc       0.23388 ms    0.587 ms     1.251 ms     3.903 ms
    libc           0.23908 ms    0.583 ms     1.259 ms     2.791 ms

Disabling mimalloc reduced clean-window p99.99 by 28.5% while the other
latency levels stayed essentially unchanged. The strongest mechanism is the
new memory sampler: worker 0 calls the global `mi_stats_get()` every 100 ms.
That is approximately 600 samples per 60-second run, the same order as the
600 slowest requests that define p99.99 among six million operations. This is
high-confidence evidence that the combined mimalloc/statistics path causes a
large part of the new extreme tail; isolating allocator behavior from
`mi_stats_get()` itself still requires a mimalloc build with statistics
sampling disabled. One libc window hit the recurring NVMe slowdown and is
excluded from the clean A/B average.

The logged TSC conversion frequency is not responsible. Lavik reported
2,793.437-2,793.439 MHz, matching the kernel's 2,793.437 MHz detection. The
machine uses TSC as its clocksource and advertises `constant_tsc`,
`nonstop_tsc`, `tsc_reliable`, and `tsc_known_freq`.

Artifacts:

    /tmp/lavik-99e1d01-w8-refill-server.log
    /tmp/lavik-99e1d01-w8-refill.{memtier,iostat,pidstat}
    /tmp/lavik-99e1d01-w8-recovery-server.log
    /tmp/lavik-99e1d01-w8-read80.{memtier,iostat,pidstat}
    /tmp/lavik-99e1d01-w8-max1tb-server.log
    /tmp/lavik-99e1d01-w8-max1tb-mixed1to10.{memtier,iostat,pidstat}
    /tmp/lavik-99e1d01-w8-max1tb-mixed1to1-300s.{memtier,iostat,pidstat}
    /tmp/lavik-99e1d01-w8-max1tb-read80-confirm.{memtier,iostat,pidstat}
    /tmp/lavik-99e1d01-w8-libc-server.log
    /tmp/lavik-99e1d01-w8-libc-read80-{a,b,c}.{memtier,iostat,pidstat}

### Latest main 390197c fresh refill and standard retest

This 2026-08-09/10 UTC retest started from an empty `/dev/nvme1n1`, used the
launch command above, and refilled exactly 200,000,000 fixed-size 2,000-byte
values. Lavik used CPUs 0-7 with eight workers. memtier used CPUs 8-15 with
eight threads, ten connections per thread, and the full 200-million-key range.
The build was Release with native optimization, LTO, and detailed GET latency
tracing enabled.

Fresh fill:

    SET/s:       468,358.46
    average:     0.17057 ms
    p99:         0.567 ms
    p99.9:       1.583 ms
    p99.99:      2.223 ms
    records:     200,000,000

After a graceful shutdown, recovery found exactly 200,000,000 records. The
data-block scan took 116.8 seconds at approximately 1.71 million records/s;
all eight workers were initialized 157.5 seconds after launch. DB 0 reported
200,000,000 keys, DB 1 reported zero, and keys 1, 100,000,000, and 200,000,000
all had 2,000-byte values.

The first formal pure-read run hit the recurring device slowdown during
seconds 53-57. It fell to approximately 74K GET/s at 1.08 ms average latency
in that interval, so it is retained as an affected sample:

    GET/s:       97,613.95
    average:     0.30396 ms
    p99:         1.407 ms
    p99.9:       1.767 ms
    p99.99:      2.239 ms

The immediate pure-read confirmation was a clean 60-second window:

    GET/s:       99,996.41
    average:     0.24468 ms
    p99:         0.639 ms
    p99.9:       1.167 ms
    p99.99:      2.111 ms
    hits:        6,000,080
    misses:      0

SET:GET = 1:10, clean 60-second window:

    Type       Ops/s       Average       p99          p99.9        p99.99
    SET         9,091.73   0.10296 ms    0.479 ms     0.903 ms     1.775 ms
    GET        90,908.56   0.24265 ms    0.599 ms     1.047 ms     2.079 ms
    Total     100,000.29   0.22995 ms    0.591 ms     1.039 ms     2.063 ms

SET:GET = 1:1, 300 seconds:

    Type       Ops/s       Average       p99          p99.9        p99.99
    SET        49,995.71   0.12056 ms    0.551 ms     1.007 ms     1.823 ms
    GET        49,995.67   0.27119 ms    0.815 ms     1.335 ms     2.527 ms
    Total      99,991.38   0.19588 ms    0.711 ms     1.199 ms     2.319 ms

Every formal GET was a hit. Full-window server and raw-device averages were:

    Workload             CPU       r/s         rMiB/s    rAwait     w/s      wMiB/s   wAwait
    Pure read affected   318.55%   97,615.4    247.27    0.150 ms     0.0      0.00   0.000 ms
    Pure read clean      270.38%   99,999.7    253.29    0.131 ms     0.0      0.00   0.000 ms
    1:10                 270.55%   90,906.1    230.25    0.130 ms   163.1     18.50   0.286 ms
    1:1, 300 seconds     234.05%   49,993.1    128.36    0.120 ms   852.5    103.05   0.163 ms

The clean random-read request size averaged 2.59 KiB. The affected pure-read
run reached 100% device utilization and 0.340 ms one-second read await during
its slow plateau; the clean confirmation peaked at 42.2% utilization and
0.140 ms read await.

The default 600-second tomb-raider interval remained enabled. Near the end of
the five-minute 1:1 run, approximately ten minutes after worker initialization,
a 512 KiB sequential sweep became visible. Its I/O overlapped roughly the last
two measured seconds and continued after foreground traffic stopped. This
raised the full-window 1:1 read bandwidth slightly above the foreground-only
rate, but aggregate throughput stayed at the 100K/s target. The final
ten-second server histograms also showed wider storage-I/O p99.99 buckets, so
the 1:1 tail above includes the current default periodic-maintenance behavior.

After all workloads, DB 0 still contained exactly 200,000,000 keys, DB 1 was
empty, and the three sampled values remained 2,000 bytes. The server log had
no fatal, assertion, corruption, checksum, storage, or request errors. Lavik
was stopped cleanly and all storage buffers were durably flushed.

Artifacts:

    /tmp/lavik-390197c-w8-refill-server.log
    /tmp/lavik-390197c-w8-refill.{memtier,iostat,pidstat}
    /tmp/lavik-390197c-w8-recovery-bench-server.log
    /tmp/lavik-390197c-w8-read80-warmup.memtier
    /tmp/lavik-390197c-w8-read80.{memtier,iostat,pidstat}
    /tmp/lavik-390197c-w8-read80-confirm.{memtier,iostat,pidstat}
    /tmp/lavik-390197c-w8-mixed1to10.{memtier,iostat,pidstat}
    /tmp/lavik-390197c-w8-mixed1to1-300s.{memtier,iostat,pidstat}

### Pure read clean windows

Eight independent clean 10-second windows:

- Throughput: approximately 100K GET/s.
- Average latency: 0.232 to 0.247 ms.
- p99.9: 0.927 to 1.207 ms.
- p99.99: 1.815 to 3.135 ms.
- NVMe read await: 0.13 to 0.14 ms.
- Physical read size: 2.56 KiB.

Best clean window:

    GET/s:       99,997
    average:     0.23155 ms
    p99.9:       0.927 ms
    p99.99:      1.815 ms
    NVMe await:  0.130 ms

### Latest main refill, 8 workers, 80 connections

Source was Lavik c3488d9 with celer a5cd07d. The raw device was zeroed and
refilled from scratch with exactly 200,000,000 fixed-size 2000-byte values.
Lavik was pinned to CPUs 0-7; memtier used 8 threads and 10 connections per
thread on CPUs 8-15.

Fresh fill:

    SET/s:       460,364.10
    average:     0.17661 ms
    p50:         0.159 ms
    p99:         0.487 ms
    p99.9:       1.215 ms
    records:     200,000,000

After a graceful shutdown, recovery found exactly 200,000,000 records. The
full device scan took approximately 130 seconds at 1.54M records/s, and all
workers were ready in approximately 151 seconds. DB 0 reported 200,000,000
keys, DB 1 reported zero, and both the first and last keys had 2000-byte
values.

Pure random GET, 60 seconds, rate-limited to 100K operations/s:

    GET/s:       99,996.57
    average:     0.25289 ms
    p50:         0.231 ms
    p90:         0.359 ms
    p99:         0.679 ms
    p99.9:       1.183 ms
    p99.99:      2.039 ms
    hits:        6,000,080
    misses:      0

Lavik used 278.31% CPU on average (157.64% user and 120.67% system). NVMe
averaged 100,013 reads/s and 250.29 MiB/s with 0.130 ms read await, 2.56 KiB
requests, queue depth 13.06, and 45.6% utilization.

A separate perf profile showed ScanHashMap at 2.54% self CPU: RehashStep was
1.41% and FindWithoutStep was 1.13%. The remaining Abseil flat_hash_map lookup
for block state was 0.26%. The perf-attached latency run is not the headline
result because perf attachment disturbed its first five seconds.

### Pure-read tail-latency attribution

The c3488d9 build had `LAVIK_ENABLE_READ_LATENCY_TRACE=ON`. Across the 48
worker/10-second reports covering approximately 6.0 million GETs, the
request-weighted average server-side phases were:

    Phase              Average
    total              210.41 us
    storage I/O        158.16 us
    send                28.54 us
    route out            9.95 us
    route back           9.22 us
    index lookup         2.40 us
    decode               0.50 us
    buffer acquire       0.10 us

Storage I/O was therefore approximately 75% of average server-side time. The
per-worker/window percentile bucket upper bounds were:

    Phase              p99.9 range       p99.99 range
    total              0.75-1.00 ms      1.00-3.00 ms
    storage I/O        0.75 ms           0.75-1.50 ms
    route out          0.15-0.30 ms      0.50-1.50 ms
    route back         0.075-0.20 ms     0.15-1.50 ms
    send               0.15-0.50 ms      0.20-0.50 ms
    index lookup       0.015-0.020 ms    0.030-0.075 ms

Individual phase percentiles are not additive because their slow requests are
not necessarily the same requests. The trace begins after request parsing, so
memtier's end-to-end latency is also expected to be somewhat higher.

The corresponding on-CPU perf profile was dominated by NVMe submission and
cross-core/network work:

    nvme_submit_cmds                         23.79%
    celer::Worker::DrainCrossCore             9.07%
    _raw_spin_unlock_irqrestore               5.64%
    ScanHashMap::RehashStep                    1.41%
    ScanHashMap::FindWithoutStep               1.13%

The latency tracing itself accounted for roughly 4% of sampled CPU when the
clock reads and histogram updates are combined. Perf is useful for CPU cost but
does not directly measure off-CPU NVMe latency; the phase trace and block-layer
latency data are the stronger evidence for the tail.

Defrag was not responsible for this pure-read tail. Every one-second
`/dev/nvme1n1` sample during the headline pure-read run reported zero write
IOPS, and perf contained no `DefragOne`, `CleanBlockLocked`, or
`RelocateIfCurrent` samples. Defrag only becomes eligible when a completed
block's live ratio falls to 50% or below. The fresh 200-million-key dataset had
only about 4.1 million random overwrites across the new 1:1 and 1:10 tests, so
blocks remained far above that threshold.

The origin-worker load was also uneven despite 80 connections. Worker 3
handled 23.1% of traced requests while the least-loaded worker handled 9.0%.
Worker 3 averaged 240.6 us total, 55.1 us send, and 19.8 us route-back; typical
workers averaged about 198-203 us total, 17-22 us send, and 5-6 us route-back.
This points to `SO_REUSEPORT` connection placement plus cross-core mailbox
scheduling as the main software contribution to the tail.

`ScanHashMap::Find()` performs one incremental `RehashStep()` on every mutable
lookup while a table is expanding. It is measurable and should be removed from
the latency-sensitive GET path or completed outside foreground reads, but its
30-75 us p99.99 lookup bucket is too small to explain the observed 2 ms class
tail by itself.

### Accept-time connection-balancing retest

This retest used the same Lavik c3488d9 data and 8-worker/80-connection
100K GET/s workload, with the local Celer accept-time round-robin placement
described above. Recovery reported 200,000,000 live keys; DB 1 was empty; the
first, middle, and last sampled values were all 2000 bytes. All benchmark GETs
were hits.

The first clean 60-second run compared with the pre-change headline run was:

    Metric              Before       Balanced      Change
    GET/s               99,996.57    99,999.29      +0.00%
    average latency      0.25289 ms   0.23416 ms     -7.41%
    p99.9                1.183 ms     1.023 ms      -13.52%
    p99.99               2.039 ms     1.903 ms       -6.67%
    server CPU           278.31%      274.67%        -1.31%

Request placement changed from a 9.0%-23.1% per-worker range to 12.500% on
each worker. Across 48 worker/windows and 6,000,176 requests, the server-side
phase averages changed as follows:

    Phase              Before       Balanced      Change
    total              210.41 us    194.98 us      -7.33%
    storage I/O        158.16 us    155.41 us      -1.74%
    route out            9.95 us      8.16 us     -17.99%
    route back           9.22 us      6.05 us     -34.38%
    send                28.54 us     20.80 us     -27.12%
    index lookup         2.40 us      2.40 us       0.00%

The mean p99.99 bucket upper bound across those windows improved by 11.8% for
total latency, 19.0% for route-out, 30.0% for route-back, and 17.1% for send.
NVMe behavior was essentially unchanged at approximately 100K reads/s,
250 MiB/s, and 0.130 ms read await, with zero writes. This supports improved
origin-worker queue balance, rather than storage or defrag, as the cause of the
clean-run gain.

The new perf run sampled `celer::Worker::DrainCrossCore` at 12.20% versus
9.07% before. Its share did not fall because connections still execute storage
work on key-owner workers; accept-time placement balances the origin queues but
does not eliminate cross-core messages. `ScanHashMap::RehashStep` and
`FindWithoutStep` were 2.36% and 1.18% respectively in this sample.

A second clean-command confirmation encountered the known device slowdown for
its first five seconds: throughput was approximately 75K GET/s and latency was
approximately 1.06 ms before returning to 100K GET/s and 0.23 ms. Including
that interval, the 60-second result was 97,952.54 GET/s, 0.28529 ms average,
1.735 ms p99.9, and 2.207 ms p99.99. Consequently, the balanced clean-window
improvement is clear, but a small single-run p99.99 delta should not be treated
as statistically conclusive without repeated runs that classify the NVMe slow
intervals separately.

Artifacts:

    /tmp/lavik-c3488d9-w8-accept-balance-retest-server.log
    /tmp/lavik-c3488d9-w8-accept-balance-read80.memtier
    /tmp/lavik-c3488d9-w8-accept-balance-read80.{iostat,pidstat}
    /tmp/lavik-c3488d9-w8-accept-balance-read80.perf.data
    /tmp/lavik-c3488d9-w8-accept-balance-read80-perf.memtier
    /tmp/lavik-c3488d9-w8-accept-balance-read80-confirm.memtier

### Direct-from-read-buffer String GET retest

This retest kept the recovered 200-million-key dataset and the same
8-worker/80-connection 100K GET/s workload. It added the specialized String
GET reply path: values through 1 MiB - 8 KiB are framed in their disk-read
buffer and sent without first materializing the value at the start of the
buffer. Larger values retain the copy fallback. The 2000-byte benchmark values
therefore exercised the direct path. Recovery and sampled first, middle, and
last values were correct, and all benchmark GETs were hits.

The clean 60-second result compared with the accept-balanced result immediately
above was:

    Metric              Balanced      Direct frame   Change
    GET/s               99,999.29      99,996.84      -0.00%
    average latency      0.23416 ms     0.24371 ms     +4.08%
    p99.9                1.023 ms       1.143 ms      +11.73%
    p99.99               1.903 ms       1.959 ms       +2.94%
    server CPU           274.67%        263.20%         -4.18%
    server user CPU      154.94%        143.38%         -7.46%
    server system CPU    119.74%        119.82%         +0.07%

The CPU reduction is the clearest observed benefit: user CPU fell by 11.56
percentage points while system CPU was unchanged. Throughput remained at the
rate limit. Latency did not improve in this single clean window because the
storage portion was slower. Across 40 worker/windows and 5,000,141 requests,
the server-side phase averages were:

    Phase              Balanced      Direct frame   Change
    total              194.98 us      202.03 us       +3.62%
    storage I/O        155.41 us      160.29 us       +3.14%
    route out            8.16 us        8.88 us       +8.82%
    route back           6.05 us        6.43 us       +6.28%
    send                20.80 us       21.92 us       +5.38%
    index lookup         2.40 us        2.50 us       +4.17%
    decode/copy          0.50 us        0.40 us      -20.00%

The phase logger has only 0.1 us resolution for these averages, so the
decode/copy row is directional rather than a precise measure of all saved CPU.
Worker placement remained exactly balanced at 12.500% each. The direct-frame
run averaged 100,000 NVMe reads/s, 250.27 MiB/s, 0.140 ms read await, queue
depth 13.58, and 44.12% utilization, with zero data-device writes. The prior
balanced clean run was approximately 0.130 ms read await, consistent with the
extra storage time masking a software latency gain.

The mean p99.99 phase-bucket upper bounds were 1.713 ms total, 0.863 ms
storage I/O, 0.898 ms route-out, 0.516 ms route-back, and 0.345 ms send. These
are coarse per-worker/window histogram bounds and are not additive.

The first formal attempt contained the recurring device slowdown at seconds
45-51. During that interval throughput fell to approximately 75K GET/s and
latency rose to approximately 1.06 ms. Its full-run result was 97,717.73 GET/s,
0.30175 ms average, 1.759 ms p99.9, and 2.207 ms p99.99. It is retained as an
affected sample rather than used for the code comparison.

Artifacts:

    /tmp/lavik-c3488d9-w8-direct-frame-retest-server.log
    /tmp/lavik-c3488d9-w8-direct-frame-read80-warmup.memtier
    /tmp/lavik-c3488d9-w8-direct-frame-read80.{memtier,iostat,pidstat}
    /tmp/lavik-c3488d9-w8-direct-frame-read80-confirm.{memtier,iostat,pidstat}

### Five-minute p99/p99.9/p99.99 retest, three workloads

The direct-frame build was also run for 300 seconds per workload with p99,
p99.9, and p99.99 enabled. The setup remained 8 Lavik workers on CPUs 0-7,
8 memtier threads with 10 connections each on CPUs 8-15, a 200-million-key
random range, fixed 2000-byte values, and a requested 100K aggregate operation
rate. The modes ran in order: pure read, SET:GET=1:10, then SET:GET=1:1. The
server was not restarted or refilled between modes.

Pure random read:

    Type       Ops/s       Average       p99          p99.9        p99.99
    GET        99,537.06   0.25421 ms    1.015 ms     1.567 ms     2.175 ms

The pure-read run included the recurring device/system slow interval at
seconds 31-36. During it, throughput was approximately 75K/s and latency was
approximately 1.07 ms; the five-minute aggregate intentionally retains this
interval. All 29,861,406 GETs were hits.

SET:GET=1:10:

    Type       Ops/s       Average       p99          p99.9        p99.99
    SET         9,090.84   0.09943 ms    0.583 ms     1.831 ms     2.655 ms
    GET        90,908.36   0.24006 ms    0.663 ms     1.207 ms     2.207 ms
    Total      99,999.19   0.22727 ms    0.655 ms     1.271 ms     2.303 ms

This was a clean five-minute window and remained at the requested rate
throughout. All GETs were hits.

SET:GET=1:1:

    Type       Ops/s       Average       p99          p99.9        p99.99
    SET        49,979.06   0.12998 ms    0.751 ms     1.599 ms     2.591 ms
    GET        49,978.83   0.29402 ms    1.127 ms     1.711 ms     2.591 ms
    Total      99,957.90   0.21200 ms    0.991 ms     1.679 ms     2.591 ms

The first five seconds ran at approximately 97.5K/s and 0.79 ms before the
workload stabilized at 100K/s and approximately 0.20 ms. The aggregate retains
that startup/slow interval. All GETs were hits.

Full-window server and data-device averages were:

    Workload     CPU       r/s         rMiB/s    rAwait     w/s      wMiB/s   wAwait
    Pure read    272.18%   99,531.4    249.09    0.143 ms     0.0      0.00   0.000 ms
    1:10         260.37%   90,901.2    227.49    0.130 ms   152.7     18.60   0.438 ms
    1:1          229.50%   49,972.4    125.09    0.124 ms   821.2    102.16   0.343 ms

After all three modes, DB 0 still contained exactly 200,000,000 live keys, DB
1 was empty, and the first, middle, and last sampled values were all 2000
bytes. No server errors or checksum failures were logged.

Artifacts:

    /tmp/lavik-c3488d9-w8-direct-frame-5m-server.log
    /tmp/lavik-c3488d9-w8-direct-frame-read80-5m.{memtier,iostat,pidstat}
    /tmp/lavik-c3488d9-w8-direct-frame-setget10-5m.{memtier,iostat,pidstat}
    /tmp/lavik-c3488d9-w8-direct-frame-setget1-5m.{memtier,iostat,pidstat}

### 50 GiB regular-file online defrag impact

This test used Lavik 767bb11 with Celer 0afbc77 and an isolated 50 GiB
preallocated regular file at `/mnt/data0/lavik-defrag-50g.data` on the
`/dev/nvme0n1` ext4 filesystem. It did not touch the 200-million-key raw-device
dataset or Dragonfly's files. Lavik exposed 6,399 8 MiB blocks with 4 KiB
direct-I/O alignment. The file was filled with 8,000,000 fixed 2000-byte values
at 454,823.41 SET/s; the smaller live set deliberately left enough free space
for relocation.

The controlled foreground workload was split into two independent clients:
50K random GET/s as the measured online business and 50K random SET/s as the
fragmentation source. Both used 8 threads and 10 connections per thread on
CPUs 8-15, while the 8 Lavik workers remained on CPUs 0-7. With 8 million
keys, random replacement is expected to reduce the original-record live ratio
to 50% after approximately `-ln(0.5) * 8M / 50K = 111` seconds. The observed
SET slowdown began at seconds 107-110, matching the code's 50% threshold.

The first 60-second window was before that threshold. The first active window
started as the threshold was crossed. A later attempt to obtain an A/B/A clean
window instead produced a second active reproduction: although defrag drained
when SET traffic stopped, resuming SET immediately discovered more already-low
live-ratio blocks and queued another burst.

    Window               GET/s       Average       p99          p99.9        p99.99
    Before defrag        49,923.70    0.25714 ms    0.807 ms     1.895 ms      3.087 ms
    Defrag active #1     49,888.92    0.29965 ms    2.351 ms     7.167 ms     13.311 ms
    Defrag active #2     47,556.15    0.70051 ms   11.391 ms    17.023 ms     23.423 ms

Relative to the threshold-before baseline, the first active window retained
the GET rate but increased average, p99, p99.9, and p99.99 by 16.5%, 191.3%,
278.2%, and 331.2% respectively. The second burst was stronger: the GET client
missed its 50K/s target by 4.9%, average latency was 2.72x baseline, and p99
was 14.1x baseline. During the second active window, the concurrent SET client
delivered 49,621.31 SET/s at 0.25655 ms average, 2.127 ms p99, 14.271 ms p99.9,
and 20.223 ms p99.99.

First-60-second server and device averages were:

    Window               CPU       r/s         rMiB/s    rAwait     w/s       wMiB/s   wAwait
    Before defrag        253.88%   49,833.3    296.06    0.144 ms     846.5    102.15   0.321 ms
    Defrag active #1     285.50%   49,994.0    396.43    0.151 ms   1,252.7    152.44   0.076 ms
    Defrag active #2     369.00%   48,008.8    558.67    0.239 ms   1,869.5    228.28   0.096 ms

The first active window raised server CPU by 12.5%, read bandwidth by 33.9%,
and write bandwidth by 49.2%. It did not exhaust either global resource:
Lavik peaked at 303% of the 800% available CPU, while NVMe utilization
averaged 33.1% and peaked at 37.9%. The server phase trace isolates the first
window's added delay:

    Phase              Before       Active #1     Change
    total              213.75 us     244.94 us     +31.19 us
    storage I/O        170.00 us     170.11 us      +0.11 us
    route out           10.02 us      42.57 us     +32.55 us
    route back           7.23 us       7.32 us      +0.09 us
    send                22.28 us      20.75 us      -1.53 us

Thus the clean first reproduction was dominated by worker/cross-core queueing,
not storage latency or total CPU capacity. Defrag relocation shares the worker,
key-lock, store-state-mutex, and cross-core mailbox paths with foreground requests.

The second reproduction raised CPU and I/O much further, but its most extreme
interval also overlapped the known device/system slow plateau. NVMe utilization
peaked at 91.7%, read await at 0.670 ms, process CPU at 689%, and system CPU at
532%. Its 23.423 ms p99.99 must therefore be treated as defrag plus device
saturation, not a defrag-only result. Even without that confounder, the first
active window proves a material defrag tail effect.

Baseline perf contained no `CleanBlockLocked` or
`RelocateIfCurrent`; both active profiles sampled those functions. Their
on-CPU shares were modest because block reads and writes spend significant time
off CPU, but their presence together with the large extra I/O confirms that
defrag was active rather than this being ordinary foreground SET cost.

The result is that current defrag materially harms online tail latency in
short bursts. It reads complete 8 MiB blocks, relocates live records through
the normal write and key-lock path, and can run once per worker. Rate limiting,
lower defrag concurrency, I/O budgeting, and yielding between relocations are
appropriate follow-up experiments.

After the experiment, DB 0 still contained exactly 8,000,000 live keys and the
first, middle, and last sampled values were all 2000 bytes. All measured GETs
were hits, no defrag/checksum errors were logged, and the defrag I/O drained to
zero after foreground writes stopped. The 50 GiB test file was retained for
follow-up tests.

Artifacts:

    /tmp/lavik-c3488d9-w8-defrag50g-server.log
    /tmp/lavik-c3488d9-w8-defrag50g-fill.memtier
    /tmp/lavik-c3488d9-w8-defrag50g-churn-set50k.memtier
    /tmp/lavik-c3488d9-w8-defrag50g-nodefrag-read50k.memtier
    /tmp/lavik-c3488d9-w8-defrag50g-nodefrag.{iostat,pidstat,perf.data}
    /tmp/lavik-c3488d9-w8-defrag50g-active-read50k.memtier
    /tmp/lavik-c3488d9-w8-defrag50g-active.{iostat,pidstat,perf.data}
    /tmp/lavik-c3488d9-w8-defrag50g-post-{set50k,read50k}.memtier
    /tmp/lavik-c3488d9-w8-defrag50g-post.{iostat,pidstat,perf.data}

### Round-budget defrag correctness smoke test

The local round-budget implementation was checked on an isolated 1 GiB ext4
regular file after moving Abseil under Celer. Celer always used Abseil
`CycleClock`; no package lookup, compile-time switch, or clock fallback was
involved. The scheduler used a 1,000 us foreground budget, a 50 us background
budget, and a 10% background warrant. Foreground coroutine and cross-core work
were queued separately from background defrag work. Defrag and every nested
task, I/O completion, and cross-core continuation inherited the background
classification.

The test first loaded 100,000 unique 2,000-byte values, then ran independent
50K random SET/s and 50K random GET/s clients concurrently for 30 seconds on
CPUs 8-15. Lavik used eight workers on CPUs 0-7. Repeated replacement
triggered defrag quickly in the small file; scheduler logs reported thousands
of background resumes on every worker throughout the measured interval.

    Operation    Ops/s        Average       p99          p99.9        p99.99
    SET           49,998.49    0.12326 ms    0.615 ms     1.591 ms      2.751 ms
    GET           49,996.56    0.19125 ms    0.575 ms     1.367 ms      2.975 ms

All 1,500,019 measured GETs were hits. `DBSIZE` remained exactly 100,000 and
the first, arbitrary, middle, and last sampled values were all 2,000 bytes. No
defrag, checksum, corruption, or storage errors were logged. The temporary
1 GiB file was deleted after Lavik stopped; the retained 50 GiB defrag file
and the raw-device dataset were not modified.

In steady 10-second scheduler windows, average rounds were approximately
5.5-5.6 us and maximum foreground slices were 1.07-1.45 ms. Background work
was present on every worker. Although its configured slice is 50 us, maximum
observed background slices were 0.79-0.94 ms because the budget is cooperative:
a continuation can only stop when it reaches the next `Yield` or suspension.
The measured background share was 26-28%, above the 10% warrant because a
single defrag continuation commonly overran the small nominal slice. The
online p99.99 nevertheless remained below 3 ms in this smoke workload. This is
a functional and scheduling check, not a replacement for the retained 50 GiB
before/active comparison.

Artifacts:

    /tmp/lavik-round-budget-smoke-v2-{fill,write,read}.txt
    /tmp/lavik-round-budget-smoke-v2-server.log

### Unlimited fixed-key overwrite with round-budget defrag

The retained 50 GiB file was recovered with exactly 8,000,000 live
`defragkey_` keys and 2,000-byte values. The measured workload never expanded
that logical key range: four memtier threads with ten connections each randomly
overwrote those keys without rate limiting for 180 seconds. A separate,
identically configured GET client on the other four client CPUs attempted
50K GET/s throughout. Lavik remained on CPUs 0-7. A matching read-only
baseline and a 60-second read-only run after writes stopped used the exact same
GET client configuration.

    Phase                 GET/s       Average       p99          p99.9        p99.99
    Read-only baseline     49,996.37    0.19478 ms    0.647 ms     1.063 ms      1.839 ms
    Unlimited overwrite    47,559.80    0.66034 ms    3.199 ms     4.991 ms      6.719 ms
    Writes stopped         49,998.60    0.19472 ms    0.655 ms     1.071 ms      1.919 ms

During unlimited overwrite, GET missed its target by 4.9%, average latency was
3.39x baseline, p99 was 4.94x, p99.9 was 4.70x, and p99.99 was 3.65x. All GETs
still hit. The SET client delivered 232,429.17 SET/s at 0.17197 ms average,
0.711 ms p99, 1.247 ms p99.9, and 1.767 ms p99.99. Its first partial-defrag
ten-second window averaged about 268K SET/s, fell to 209K SET/s in seconds
30-59 during stronger cleaning, and stabilized around 236K SET/s in the final
60 seconds.

    Phase                 CPU       r/s       rMiB/s    rAwait     w/s      wMiB/s   wAwait    NVMe util
    Read-only baseline    193.8%    50,001      296.9    0.110 ms       0        0.0   0.000 ms     40.7%
    Unlimited overwrite   789.0%    49,364    1,266.1    0.437 ms   7,733      949.6   0.146 ms     97.0%
    Writes stopped        193.9%    50,001      297.1    0.110 ms       0        0.0   0.000 ms     40.8%

Unlimited overwrite therefore makes the defrag effect unmistakable, but it is
an intentional saturation test rather than an isolated defrag comparison:
Lavik used essentially all eight server CPUs and NVMe utilization averaged
97%, with a queue depth of 22.4. Defrag increased physical reads to 1.27 GiB/s
because it scans whole blocks while foreground GETs continue.

The scheduler trace revealed a more important policy problem. Once defrag was
active, measured background share was normally 42-46% and sometimes 51-54%,
despite the configured 10% warrant. Maximum background continuations were
usually about 0.94-1.3 ms. The cooperative 50 us budget explains individual
slice overruns, but not the whole policy failure: `ShouldRunBackground()` runs
background unconditionally whenever the foreground ready queues are empty.
With asynchronous foreground I/O those queues commonly drain every round even
under full load, so this idle shortcut bypasses the rolling 10% share check.
The warrant currently constrains background only when foreground work remains
queued after its slice. A follow-up should base the idle exception on actual
recent foreground activity or pending/in-flight online work, not merely an
empty ready queue; defrag work should also reach a checkpoint at a finer unit
than one relocation continuation.

After writes stopped, background resumes returned to zero and the 60-second GET
result, CPU, and device metrics returned almost exactly to the read-only
baseline. `DBSIZE` remained 8,000,000, four sampled values were all 2,000 bytes,
and no storage, defrag, checksum, space-exhaustion, or request errors were
logged. Lavik was stopped cleanly and the 50 GiB file was retained.

Artifacts:

    /tmp/lavik-roundbudget-unlimited-server.log
    /tmp/lavik-roundbudget-unlimited-baseline-match-{read.txt,read.realtime,iostat,pidstat}
    /tmp/lavik-roundbudget-unlimited-active-{write.txt,write.realtime,read.txt,read.realtime,iostat,pidstat}
    /tmp/lavik-roundbudget-unlimited-post-{read.txt,read.realtime,iostat,pidstat}

### Current Lavik versus Dragonfly comparison, 8 workers, 80 connections

These results use the same 200-million-key range, fixed 2000-byte values,
CPU split, memtier concurrency, and 100K operation/s limit. Lavik is 767bb11
with Celer 0afbc77, including accept balancing and direct String GET framing.
Dragonfly is v1.40.0, build e4ebd, and was launched as:

    taskset -c 0-7 /mnt/dev/dragonfly-x86_64 \
      --bind=0.0.0.0 \
      --port=6379 \
      --proactor_threads=8 \
      --maxmemory=40GB \
      --dir=/mnt/data/dfly \
      --dbfilename=dump-{timestamp} \
      --tiered_prefix=/mnt/data0/dfly/tiered/dragonfly \
      --backing_file_direct=true

Dragonfly was freshly filled on the new ext4 tiered device. Refill comparison:

    System       SET/s         Average       p99          p99.9       p99.99
    Lavik      460,364.10    0.17661 ms    0.487 ms     1.215 ms    not captured
    Dragonfly    305,953.70    0.26120 ms    0.863 ms     1.463 ms    2.255 ms

Dragonfly's refill throughput was 33.5% below Lavik's; equivalently,
Lavik was 50.5% faster. After refill, Dragonfly reported exactly 200,000,000
keys. Its first, middle, and last values were all 2000 bytes.

Pure random read, 60 seconds:

    System       GET/s         Average       p99          p99.9       p99.99
    Lavik       99,996.84    0.24371 ms    not captured 1.143 ms    1.959 ms
    Dragonfly     99,998.17    0.23004 ms    0.543 ms     0.983 ms    1.775 ms

Both systems reached the 100K/s rate limit with zero misses. Dragonfly's
average, p99.9, and p99.99 were respectively 5.6%, 14.0%, and 9.4% lower.
Lavik averaged 263.20% server CPU and 250.27 MiB/s of NVMe reads; Dragonfly
averaged 241.46% CPU and 388.28 MiB/s. The difference in physical bandwidth is
mainly 2.56 KiB aligned reads for Lavik versus 4 KiB filesystem reads for
Dragonfly.

SET:GET = 1:1, 60 seconds:

    System       Type       Ops/s       Average       p99.9       p99.99
    Lavik      SET       50,000.29    0.12316 ms    1.287 ms    2.495 ms
    Lavik      GET       49,999.18    0.27528 ms    1.295 ms    2.223 ms
    Lavik      Total     99,999.47    0.19922 ms    1.287 ms    2.367 ms
    Dragonfly    SET       49,999.73    0.18575 ms    1.127 ms    1.759 ms
    Dragonfly    GET       49,998.41    0.35186 ms    1.511 ms    1.927 ms
    Dragonfly    Total     99,998.13    0.26880 ms    1.399 ms    1.879 ms

At the same capped throughput, Lavik's total average latency was 25.9%
lower. Its SET and GET averages were 33.7% and 21.8% lower. Dragonfly had the
better p99.99 tails: SET, GET, and total were 29.5%, 13.3%, and 20.6% lower.
Server CPU averaged 250.36% for Lavik and 278.85% for Dragonfly.

SET:GET = 1:10, 60-second clean confirmation:

    System       Type       Ops/s       Average       p99.9       p99.99
    Lavik      SET        9,091.74    0.09749 ms    1.567 ms    2.431 ms
    Lavik      GET       90,907.04    0.24167 ms    1.231 ms    2.191 ms
    Lavik      Total     99,998.78    0.22856 ms    1.255 ms    2.239 ms
    Dragonfly    SET        9,091.74    0.14056 ms    0.879 ms    1.503 ms
    Dragonfly    GET       90,906.79    0.25534 ms    0.967 ms    1.663 ms
    Dragonfly    Total     99,998.54    0.24490 ms    0.967 ms    1.663 ms

Lavik's total average latency was 6.7% lower, with SET and GET averages 30.2%
and 5.4% lower. Dragonfly's SET and GET p99.99 were 38.2% and 24.1% lower;
its total p99.99 was 25.7% lower. Server CPU averaged 269.75% for Lavik and
262.92% for Dragonfly. All GETs hit on both systems.

The first Dragonfly 1:10 attempt encountered the recurring six-second NVMe
slowdown and finished at 97,230.91 operations/s. Its SET and GET p99.99 were
1.759 ms and 2.367 ms. The clean confirmation above is the comparison headline,
matching the treatment of the corresponding Lavik run. Both raw runs remain
available in /tmp.

After all Dragonfly mixed tests, DBSIZE remained exactly 200,000,000; sampled
first, middle, and last values remained 2000 bytes. Every formal benchmark GET
hit, and no fatal, error, assertion, or corruption message appeared in the
Dragonfly log. Dragonfly was left running as PID 7829 at the end of that
historical comparison, but it was no longer running during the 390197c retest.

### Pure read, one connection, 10 workers

Lavik used 10 workers pinned to CPUs 0-9. memtier used one thread and one
connection pinned to CPUs 10-15. The test ran for 60 seconds against the full
200-million-key range with fixed 2000-byte values and no rate limit.

    GET/s:       5,174.49
    average:     0.19328 ms
    p50:         0.199 ms
    p90:         0.247 ms
    p99:         0.263 ms
    p99.9:       0.279 ms
    p99.99:      0.311 ms
    hits:        310,470
    misses:      0

Lavik phase logging reported 167.2 to 169.6 us average total latency and
119.0 to 119.6 us storage-I/O latency. Device read await averaged 0.112 ms.
The single serial connection limits throughput to approximately the reciprocal
of the average request latency; this is not a server throughput limit.

The mixed 8-worker/10-worker on-disk layout also recovered successfully before
this run. DB 0 reported exactly 200,000,000 keys and all benchmark reads hit.

### SET:GET = 1:10, 60 seconds, 128 KiB flush

    Type      Ops/s       Average       p99.9       p99.99
    SET       9,091.61    0.10490 ms    1.199 ms    3.791 ms
    GET      90,905.45    0.24861 ms    1.415 ms    2.575 ms
    Total    99,997.06    0.23554 ms    1.399 ms    2.671 ms

Device:

    read await:       0.130 ms
    write await:      0.460 ms
    max write await:  0.560 ms
    read request:     2.56 KiB
    write request:    124.6 KiB
    max utilization:  52.3%

### SET:GET = 1:1, 60 seconds, 128 KiB flush

    Type      Ops/s       Average       p99.9       p99.99
    SET      49,999.01    0.12606 ms    1.543 ms    2.639 ms
    GET      49,997.67    0.28175 ms    1.663 ms    2.927 ms
    Total    99,996.68    0.20390 ms    1.599 ms    2.783 ms

Device:

    read:             49,996 IOPS, 125.1 MiB/s
    write:            810 IOPS, 100.7 MiB/s
    read await:       0.120 ms
    write await:      0.339 ms
    max write await:  0.490 ms
    read request:     2.56 KiB
    write request:    127.4 KiB
    max utilization:  46.6%

### SET:GET = 1:1, 300 seconds, 128 KiB flush

Approximately 30 million total operations:

    Type      Ops/s       Average       p99.9       p99.99
    SET      49,852.61    0.12007 ms    1.255 ms    1.919 ms
    GET      49,852.53    0.27944 ms    1.927 ms    2.479 ms
    Total    99,705.14    0.19975 ms    1.775 ms    2.351 ms

Device averages:

    read:             49,847 IOPS, 124.7 MiB/s
    write:            809 IOPS, 100.7 MiB/s
    read await:       0.122 ms
    write await:      0.358 ms
    max write await:  0.500 ms
    read request:     2.56 KiB
    write request:    127.4 KiB

The first five seconds showed the recurring device slowdown:

    read IOPS:        40.7K to 41.4K
    read await:       0.390 ms
    utilization:      98.6% to 100%
    queue depth:      approximately 16

The remaining 295 seconds returned to approximately 50K read IOPS with
0.120 ms read await. The slowdown occupied 1.67% of the test, but overall GET
p99.99 remained 2.479 ms.

## Result files in /tmp

These disappear after reboot:

    /tmp/lavik-raw512-fill-iostat.log
    /tmp/lavik-raw512-read-iostat.log
    /tmp/lavik-raw512-read-memtier.log
    /tmp/lavik-raw512-flush128-mixed-server.log
    /tmp/lavik-raw512-flush128-mixed.iostat
    /tmp/lavik-raw512-flush128-mixed.memtier
    /tmp/lavik-raw512-flush128-mixed-1to1.iostat
    /tmp/lavik-raw512-flush128-mixed-1to1.memtier
    /tmp/lavik-raw512-flush128-mixed-1to1-300s.iostat
    /tmp/lavik-raw512-flush128-mixed-1to1-300s.memtier
    /tmp/lavik-main411-w10-oneconn-read.memtier
    /tmp/lavik-main411-w10-oneconn-read.pidstat
    /tmp/lavik-main411-w10-oneconn-read.iostat
    /tmp/lavik-main411-w10-oneconn-server.log
    /tmp/lavik-c3488d9-w8-refill-server.log
    /tmp/lavik-c3488d9-w8-refill.memtier
    /tmp/lavik-c3488d9-w8-refill.iostat
    /tmp/lavik-c3488d9-w8-refill.pidstat
    /tmp/lavik-c3488d9-w8-recovery-server.log
    /tmp/lavik-c3488d9-w8-read-warmup.memtier
    /tmp/lavik-c3488d9-w8-read80.memtier
    /tmp/lavik-c3488d9-w8-read80.iostat
    /tmp/lavik-c3488d9-w8-read80.pidstat
    /tmp/lavik-c3488d9-w8-read80.perf.data
    /tmp/lavik-c3488d9-w8-read80-perf.memtier
    /tmp/lavik-c3488d9-w8-mixed1to1.{memtier,iostat,pidstat}
    /tmp/lavik-c3488d9-w8-mixed1to10.{memtier,iostat,pidstat}
    /tmp/lavik-c3488d9-w8-mixed1to10-confirm.{memtier,iostat,pidstat}
    /tmp/dragonfly-v1.40-w8-tiered-server.log
    /tmp/dragonfly-v1.40-w8-refill.{memtier,iostat,pidstat}
    /tmp/dragonfly-v1.40-w8-read80.{memtier,iostat,pidstat}
    /tmp/dragonfly-v1.40-w8-mixed1to1.{memtier,iostat,pidstat}
    /tmp/dragonfly-v1.40-w8-mixed1to10.{memtier,iostat,pidstat}
    /tmp/dragonfly-v1.40-w8-mixed1to10-confirm.{memtier,iostat,pidstat}

## Known observations and next steps

1. The recurring 5-to-6-second plateau correlates with the raw NVMe becoming
   saturated at a lower IOPS rate. Read await rises from 0.12-0.14 ms to
   0.39-0.42 ms and utilization reaches 100%. It is not filesystem overhead.

2. Clean device windows still have software or individual-I/O tail latency.
   Clean pure-read p99.9 is about 1 ms and p99.99 is about 2-3 ms.

3. Detailed Lavik phase histograms show storage IO and cross-core routing both
   contribute to p99.99. One-second iostat cannot reveal individual IO tails.

4. A useful next test is simultaneous block-layer eBPF latency tracing and
   Lavik phase histograms during a clean 100K QPS window.

5. For a fair flush-size comparison, restart with --flush-size-kb=8192 and run
   the same five-minute 1:1 workload. Compare GET p99.9/p99.99, wareq-sz, and
   per-second read await against the 128 KiB results.

6. No Lavik or Dragonfly process was left running after the 390197c retest.
   The final Lavik shutdown drained requests and durably flushed all storage
   buffers.
