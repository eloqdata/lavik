# Keylane Performance Session Handoff

Updated: 2026-08-07 UTC

## Current source state

- Latest Keylane commit: c3488d9 Persist recovery metadata through device owners
- Celer submodule: a5cd07d Narrow worker IDs to 16 bits
- Celer has local accept-time connection-balancing changes: every worker keeps
  its `SO_REUSEPORT` listener, then accepted sockets are assigned round-robin
  before recv is armed. Live request-boundary migration is intentionally left
  as a TODO.
- Build directory: ./bld
- aerospike-bench.conf and bld/ are untracked and intentionally not committed.

Build:

    cmake --build bld -j 8

## Machine and storage

- Keylane uses CPUs 0-7 with 8 workers.
- memtier uses CPUs 8-15 with 8 threads and 10 connections per thread.
- Keylane raw device: /dev/nvme1n1
- Dragonfly tiered device: /dev/nvme0n1, ext4, mounted at /mnt/data0 with
  noatime. Files use the prefix /mnt/data0/dfly/tiered/dragonfly.
- /dev/nvme0n1 ext4 UUID: 1fff0614-431d-49f9-ab95-0402163791d7.
- Logical sector size: 512 bytes.
- Warning: /dev/nvme1n1 contains the current benchmark dataset. Do not discard
  or format it unless a fresh fill is intended.

## Current dataset

- Exactly 200 million keys, freshly refilled with c3488d9 using 8 workers.
- Prefix: kvkeyprefix_
- Range: 1 through 200000000.
- Values are fixed at 2000 bytes.
- Mixed tests overwrite existing keys.
- Latest memtier-reported fill rate: 460,364 SET/s.
- Records are packed inside 8 MiB storage blocks.
- Average physical GET read size is 2.56 KiB after 512-byte alignment.
- Latest full-scan recovery rate is approximately 1.54 million records/s.
- Full recovery and worker initialization take roughly 2.5 minutes.

Initial fill:

    taskset -c 8-15 memtier_benchmark \
      -t 8 -c 10 -s 127.0.0.1 -p 6379 \
      -n allkeys \
      --distinct-client-seed \
      --ratio=1:0 \
      --key-prefix="kvkeyprefix_" \
      --key-minimum=1 \
      --key-maximum=200000000 \
      --random-data \
      --data-size-range=2000-2000 \
      --data-size-pattern=R \
      --hide-histogram \
      --key-pattern=P:P

Ordinary blkdiscard did not guarantee zero reads on this Azure NVMe. To erase
and refill, stop Keylane and use the zeroing discard:

    sudo blkdiscard -z -f --length 644245094400 /dev/nvme1n1

This command is destructive.

## Keylane launch

The latest server was tested with 128 KiB flush submissions:

    sudo env LD_PRELOAD=/lib/x86_64-linux-gnu/libtcmalloc_minimal.so.4 \
      taskset -c 0-7 ./bld/keylane \
      --recv-buffers 1024 \
      --registered-buffer-mb=64 \
      --busy-poll-us=20 \
      --data-file=/dev/nvme1n1 \
      --data-file-size-mb=614400 \
      --threads=8 \
      --flush-max-ms=1000 \
      --flush-size-kb=128 \
      --disable-read-crc

Graceful stop:

    pid=$(pgrep -n -x keylane)
    kill -INT "$pid"

Wait for shutdown so partial write buffers are flushed before restarting.

## Storage changes in f71dc45

- Raw block devices use BLKSSZGET to discover direct-I/O alignment.
- /dev/nvme1n1 therefore uses 512-byte read offset and length alignment.
- Regular files continue to use 4096-byte alignment.
- --flush-size-kb controls the maximum storage write submission.
- Default --flush-size-kb is 8192, preserving the old 8 MiB behavior.
- The tested value is 128.
- The 8 MiB write buffer, on-disk block, index, and recovery format are
  unchanged.
- Registered slices use WriteFixed; heap fallback buffers use ordinary async
  write.
- Flush size must be a power of two between direct-I/O alignment and 8 MiB.
- Detailed GET phase latency logging is enabled in this build.

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
      -t 8 -c 10 -s 127.0.0.1 -p 6379 \
      --test-time 60 \
      --distinct-client-seed \
      --ratio=0:1 \
      --key-prefix="kvkeyprefix_" \
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
      -t 8 -c 10 -s 127.0.0.1 -p 6379 \
      --test-time 60 \
      --distinct-client-seed \
      --ratio=1:10 \
      --key-prefix="kvkeyprefix_" \
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
      -t 8 -c 10 -s 127.0.0.1 -p 6379 \
      --test-time 300 \
      --distinct-client-seed \
      --ratio=1:1 \
      --key-prefix="kvkeyprefix_" \
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

    iostat -y -t -xmd 1 305 > /tmp/keylane-test.iostat

## Results

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

Source was Keylane c3488d9 with celer a5cd07d. The raw device was zeroed and
refilled from scratch with exactly 200,000,000 fixed-size 2000-byte values.
Keylane was pinned to CPUs 0-7; memtier used 8 threads and 10 connections per
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

Keylane used 278.31% CPU on average (157.64% user and 120.67% system). NVMe
averaged 100,013 reads/s and 250.29 MiB/s with 0.130 ms read await, 2.56 KiB
requests, queue depth 13.06, and 45.6% utilization.

A separate perf profile showed ScanHashMap at 2.54% self CPU: RehashStep was
1.41% and FindWithoutStep was 1.13%. The remaining Abseil flat_hash_map lookup
for block state was 0.26%. The perf-attached latency run is not the headline
result because perf attachment disturbed its first five seconds.

### Pure-read tail-latency attribution

The c3488d9 build had `KEYLANE_ENABLE_READ_LATENCY_TRACE=ON`. Across the 48
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

This retest used the same Keylane c3488d9 data and 8-worker/80-connection
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

    /tmp/keylane-c3488d9-w8-accept-balance-retest-server.log
    /tmp/keylane-c3488d9-w8-accept-balance-read80.memtier
    /tmp/keylane-c3488d9-w8-accept-balance-read80.{iostat,pidstat}
    /tmp/keylane-c3488d9-w8-accept-balance-read80.perf.data
    /tmp/keylane-c3488d9-w8-accept-balance-read80-perf.memtier
    /tmp/keylane-c3488d9-w8-accept-balance-read80-confirm.memtier

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

    /tmp/keylane-c3488d9-w8-direct-frame-retest-server.log
    /tmp/keylane-c3488d9-w8-direct-frame-read80-warmup.memtier
    /tmp/keylane-c3488d9-w8-direct-frame-read80.{memtier,iostat,pidstat}
    /tmp/keylane-c3488d9-w8-direct-frame-read80-confirm.{memtier,iostat,pidstat}

### Five-minute p99/p99.9/p99.99 retest, three workloads

The direct-frame build was also run for 300 seconds per workload with p99,
p99.9, and p99.99 enabled. The setup remained 8 Keylane workers on CPUs 0-7,
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

    /tmp/keylane-c3488d9-w8-direct-frame-5m-server.log
    /tmp/keylane-c3488d9-w8-direct-frame-read80-5m.{memtier,iostat,pidstat}
    /tmp/keylane-c3488d9-w8-direct-frame-setget10-5m.{memtier,iostat,pidstat}
    /tmp/keylane-c3488d9-w8-direct-frame-setget1-5m.{memtier,iostat,pidstat}

### Current Keylane versus Dragonfly comparison, 8 workers, 80 connections

These results use the same 200-million-key range, fixed 2000-byte values,
CPU split, memtier concurrency, and 100K operation/s limit. Keylane is c3488d9
with celer a5cd07d plus the local accept-balancing and direct String GET
changes. Dragonfly is v1.40.0, build e4ebd, and was launched as:

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
    Keylane      460,364.10    0.17661 ms    0.487 ms     1.215 ms    not captured
    Dragonfly    305,953.70    0.26120 ms    0.863 ms     1.463 ms    2.255 ms

Dragonfly's refill throughput was 33.5% below Keylane's; equivalently,
Keylane was 50.5% faster. After refill, Dragonfly reported exactly 200,000,000
keys. Its first, middle, and last values were all 2000 bytes.

Pure random read, 60 seconds:

    System       GET/s         Average       p99          p99.9       p99.99
    Keylane       99,996.84    0.24371 ms    not captured 1.143 ms    1.959 ms
    Dragonfly     99,998.17    0.23004 ms    0.543 ms     0.983 ms    1.775 ms

Both systems reached the 100K/s rate limit with zero misses. Dragonfly's
average, p99.9, and p99.99 were respectively 5.6%, 14.0%, and 9.4% lower.
Keylane averaged 263.20% server CPU and 250.27 MiB/s of NVMe reads; Dragonfly
averaged 241.46% CPU and 388.28 MiB/s. The difference in physical bandwidth is
mainly 2.56 KiB aligned reads for Keylane versus 4 KiB filesystem reads for
Dragonfly.

SET:GET = 1:1, 60 seconds:

    System       Type       Ops/s       Average       p99.9       p99.99
    Keylane      SET       50,000.29    0.12316 ms    1.287 ms    2.495 ms
    Keylane      GET       49,999.18    0.27528 ms    1.295 ms    2.223 ms
    Keylane      Total     99,999.47    0.19922 ms    1.287 ms    2.367 ms
    Dragonfly    SET       49,999.73    0.18575 ms    1.127 ms    1.759 ms
    Dragonfly    GET       49,998.41    0.35186 ms    1.511 ms    1.927 ms
    Dragonfly    Total     99,998.13    0.26880 ms    1.399 ms    1.879 ms

At the same capped throughput, Keylane's total average latency was 25.9%
lower. Its SET and GET averages were 33.7% and 21.8% lower. Dragonfly had the
better p99.99 tails: SET, GET, and total were 29.5%, 13.3%, and 20.6% lower.
Server CPU averaged 250.36% for Keylane and 278.85% for Dragonfly.

SET:GET = 1:10, 60-second clean confirmation:

    System       Type       Ops/s       Average       p99.9       p99.99
    Keylane      SET        9,091.74    0.09749 ms    1.567 ms    2.431 ms
    Keylane      GET       90,907.04    0.24167 ms    1.231 ms    2.191 ms
    Keylane      Total     99,998.78    0.22856 ms    1.255 ms    2.239 ms
    Dragonfly    SET        9,091.74    0.14056 ms    0.879 ms    1.503 ms
    Dragonfly    GET       90,906.79    0.25534 ms    0.967 ms    1.663 ms
    Dragonfly    Total     99,998.54    0.24490 ms    0.967 ms    1.663 ms

Keylane's total average latency was 6.7% lower, with SET and GET averages 30.2%
and 5.4% lower. Dragonfly's SET and GET p99.99 were 38.2% and 24.1% lower;
its total p99.99 was 25.7% lower. Server CPU averaged 269.75% for Keylane and
262.92% for Dragonfly. All GETs hit on both systems.

The first Dragonfly 1:10 attempt encountered the recurring six-second NVMe
slowdown and finished at 97,230.91 operations/s. Its SET and GET p99.99 were
1.759 ms and 2.367 ms. The clean confirmation above is the comparison headline,
matching the treatment of the corresponding Keylane run. Both raw runs remain
available in /tmp.

After all Dragonfly mixed tests, DBSIZE remained exactly 200,000,000; sampled
first, middle, and last values remained 2000 bytes. Every formal benchmark GET
hit, and no fatal, error, assertion, or corruption message appeared in the
Dragonfly log. Dragonfly remains running as PID 7829 on port 6379 and must not
be restarted casually because tiered recovery is slow.

### Pure read, one connection, 10 workers

Keylane used 10 workers pinned to CPUs 0-9. memtier used one thread and one
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

Keylane phase logging reported 167.2 to 169.6 us average total latency and
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

    /tmp/keylane-raw512-fill-iostat.log
    /tmp/keylane-raw512-read-iostat.log
    /tmp/keylane-raw512-read-memtier.log
    /tmp/keylane-raw512-flush128-mixed-server.log
    /tmp/keylane-raw512-flush128-mixed.iostat
    /tmp/keylane-raw512-flush128-mixed.memtier
    /tmp/keylane-raw512-flush128-mixed-1to1.iostat
    /tmp/keylane-raw512-flush128-mixed-1to1.memtier
    /tmp/keylane-raw512-flush128-mixed-1to1-300s.iostat
    /tmp/keylane-raw512-flush128-mixed-1to1-300s.memtier
    /tmp/keylane-main411-w10-oneconn-read.memtier
    /tmp/keylane-main411-w10-oneconn-read.pidstat
    /tmp/keylane-main411-w10-oneconn-read.iostat
    /tmp/keylane-main411-w10-oneconn-server.log
    /tmp/keylane-c3488d9-w8-refill-server.log
    /tmp/keylane-c3488d9-w8-refill.memtier
    /tmp/keylane-c3488d9-w8-refill.iostat
    /tmp/keylane-c3488d9-w8-refill.pidstat
    /tmp/keylane-c3488d9-w8-recovery-server.log
    /tmp/keylane-c3488d9-w8-read-warmup.memtier
    /tmp/keylane-c3488d9-w8-read80.memtier
    /tmp/keylane-c3488d9-w8-read80.iostat
    /tmp/keylane-c3488d9-w8-read80.pidstat
    /tmp/keylane-c3488d9-w8-read80.perf.data
    /tmp/keylane-c3488d9-w8-read80-perf.memtier
    /tmp/keylane-c3488d9-w8-mixed1to1.{memtier,iostat,pidstat}
    /tmp/keylane-c3488d9-w8-mixed1to10.{memtier,iostat,pidstat}
    /tmp/keylane-c3488d9-w8-mixed1to10-confirm.{memtier,iostat,pidstat}
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

3. Detailed Keylane phase histograms show storage IO and cross-core routing both
   contribute to p99.99. One-second iostat cannot reveal individual IO tails.

4. A useful next test is simultaneous block-layer eBPF latency tracing and
   Keylane phase histograms during a clean 100K QPS window.

5. For a fair flush-size comparison, restart with --flush-size-kb=8192 and run
   the same five-minute 1:1 workload. Compare GET p99.9/p99.99, wareq-sz, and
   per-second read await against the 128 KiB results.

6. No Keylane process was left running. The final shutdown drained requests and
   durably flushed all storage buffers. Dragonfly PID 7829 was intentionally
   left running on port 6379; do not restart it unless required.
