# Native FULL live benchmark: baseline stall investigation

Investigation date: 2026-10-04. Scope: read-only inspection of captured process evidence and source at main [`25e159418603a95c1b2cca68173b329067e3ef48`](https://github.com/eloqdata/lavik/tree/25e159418603a95c1b2cca68173b329067e3ef48). No production code, running-server configuration, or remote tracker was changed by this investigation. No additional server, build, test, or pcap analysis was run.

## Conclusion and confidence

An unoptimized baseline run stopped making progress during native FULL. The source contains a possible circular wait between an EXEC publisher reservation, the closed snapshot transaction gate, and the final FULL publisher fence. The observed accounting matches this explanation closely. We did **not** obtain a coroutine stack or inspect the gate's in-memory state, so the exact suspended call sites in this run remain unconfirmed.

This is an observed baseline failure and a source-supported diagnosis of an existing limitation. It is not a measured performance result. The optimized candidate was **not run with the original 16 MiB queue at 20 ms RTT**; there is no evidence that either window optimization fixes or avoids this stall. A production lifecycle fix is outside the two window PRs' scope.

## Observed run and retained evidence

Original run: `/mnt/local_nvme/i131/measurements/large-live-128m-f1-rtt20/r1-main`.

Configuration: eight 16 MiB String keys; one source worker and one target worker; 20 ms added RTT; one foreground client rotating one `MULTI; SET key value; EXEC` transaction per second; five-second writer warmup; **the benchmark's configured 16 MiB publisher queue per worker**; 64 MiB global backlog per process; 2 GiB data file and 2 GiB max memory. Each transaction updates one key, not all eight keys.

Evidence files:

- [Source log](evidence/diagnostic-failure/source-server.log): the flow selected FULL at 13:01:00.885 UTC. Repeated progress reports from 13:01:30 onward retained `phase=override_catchup partition=16383 sequence=0 cursor=1:0`.
- [Periodic metrics excerpt with original line numbers and trace hash](evidence/diagnostic-failure/samples-excerpt.json): stable FULL admitted bytes of **16,777,933**, FULL queued bytes of **0**, ordinary publisher queued bytes of **0**, backlog floor/tail **11/12**, backlog bytes **50,331,648**, backlog pinned cursors **0**, and backlog backpressure/wait counters **0**. Target CPU ticks were unchanged in successive stall samples.
- [Network snapshot, 13:03:24 UTC](evidence/diagnostic-failure/stall-snapshot.json): native data socket send/receive queues were both zero; bytes sent equalled bytes ACKed; the last source send was approximately 131 seconds earlier. Both dedicated netem qdiscs reported zero drops.
- [Process snapshot](evidence/diagnostic-failure/stall-processes.json): servers remained alive; worker wait-channel observations did not expose coroutine state.
- [Client snapshot, 13:05:57 UTC](evidence/diagnostic-failure/stall-clients.json): the original loopback client remained visible. All `CLIENT LIST` entries reported `cmd=client`, so that field cannot identify the blocked command here. `blocked_clients=0` does not prove absence of internal coroutine waits.

Final evidence update: the saved writer samples contain `TimeoutError('timed out')` after **30.052146 seconds**. The owner explicitly interrupted the task runner with SIGINT at **2026-10-04T13:07:28.704184+00:00**, after more than five minutes without native data progress. The result records `KeyboardInterrupt()`, `verified=false`, and `cleanup_errors=[]`; all task server/runner processes and the namespace were confirmed gone. The source/target data files and all failed-run artifacts remain on NVMe. This is `diagnostic-aborted-invalid`, **not the configured 900-second FULL timeout and not a performance sample**. A client-side timeout does not establish that the server had cancelled and released its command reservation.

The last `override_catchup` label is not a stack trace. The sender sets that progress label during the scan, then crosses several final barriers and the publisher fence before updating later protocol progress. Consequently, partition 16383 does not prove that the coroutine is blocked inside `ReadPartitionFullSyncOverrides`.

## Source-supported circular wait

1. **EXEC reserves publication capacity before entering the snapshot transaction gate.** `ExecuteExec` sums command staging and replacement metadata, obtains `AcquireReplicationPublisherAdmission`, calls `ExecuteExecBody`, and ordinarily releases the reservation after that body returns. The body later calls `BeginSnapshotTransaction`. That helper sleeps while the gate is closed; it does not release and reacquire publisher admission around the wait. [EXEC admission and release](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/redis/command.cpp#L10869-L10946), [body enters the transaction gate](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/redis/command.cpp#L9587-L9597), [closed-gate wait](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/redis/command.cpp#L2790-L2798).

2. **The reservation is charged to the active log and FULL session.** Admission increments `log.publisher_admitted_bytes_` and each participating FULL session's admitted bytes. An oversized item is allowed when occupancy is zero, even if its size exceeds the configured queue waterline. Thus observing admitted bytes greater than 16 MiB does not itself indicate corrupted accounting. [Admission accounting](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/storage/engine/replication_log.cpp#L524-L531), [oversized-item capacity rule](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/storage/engine/replication_log.cpp#L136-L145).

3. **FULL closes the transaction gate before inserting its final log fence.** The close helper waits for already-active snapshot transactions, but an EXEC waiting to enter the closed gate has not incremented that active count. After the final override/publish drains, FULL calls `FenceReplicationLog`; gate reopening occurs only after that fence, retention, and capture-stop barriers complete. [Close and active-transaction drain](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/replication/replication.cpp#L10618-L10638), [final-cut ordering](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/replication/replication.cpp#L10949-L11058).

4. **The fence requires 256 bytes of publisher headroom, including admitted bytes in occupancy.** `FenceReplicationLog` waits until `PublisherHasCapacity(256, capacity, log.queue_bytes, log.admitted_bytes)` succeeds. For the captured reservation and 16 MiB capacity, that test cannot succeed even with an empty queue. [Fence capacity wait](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/storage/engine/replication_log.cpp#L956-L982).

The possible cycle is therefore:

```text
EXEC holds publisher admission -> waits for snapshot transaction gate to reopen
FULL holds gate closed         -> waits for publisher fence capacity
publisher fence capacity      -> requires EXEC admission to be released
EXEC releases admission       -> only after its body returns
```

It can arise when an EXEC reaches the closed gate with its reservation held, including admission granted while the cut is already closed. It does not require a key-lock cycle or a transaction containing multiple SETs. Read/materialization does acquire a shared key lock, and EXEC holds its key lock during mutation, but the available evidence does not identify either as the actual wait here. [Override materialization lock](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/storage/engine/replication.cpp#L343-L392), [single-shard EXEC lock lifetime](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/redis/command.cpp#L10217-L10291).

## Exact reservation arithmetic

For this build and the benchmark key `seed:000000000` (14 bytes), one SET's outer EXEC admission is:

```text
16,777,216  value bytes
       256  publisher item metadata
        96  three std::string elements, 32 bytes each
         3  "SET"
        14  key
       320  FULL replacement metadata
        28  two copies of the 14-byte key identity
-----------
16,777,933  bytes, exactly the observed FULL admitted gauge
```

The command staging formula counts item metadata, string elements, and argument lengths; EXEC adds replacement identity credit. [Command staging formula](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/storage/engine/replication_log.cpp#L65-L83), [replacement admission formula](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/redis/command.cpp#L2553-L2570).

This is **not** evidence that the FULL queue must hold all eight values, or even a 16 MiB after-image payload. The transaction's captured after-image initially has an empty value string, and the FULL record FIFO charges only `320 + 2 * key.size()` bytes: 348 bytes for this key. Its value is materialized/streamed later. The large observed reservation comes from the outer command admission. [Captured transaction effect](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/storage/engine/write.cpp#L2093-L2126), [record FIFO accounting](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/storage/engine/write.cpp#L2435-L2475).

## Why backlog retention is not the leading explanation

The captured backlog has no retained cursor, no capacity-backpressured flag, and no recorded capacity wait. Its physical occupancy is below the 64 MiB quota. The log's capacity path waits on replica ACK progress when a retained cursor prevents eviction; without such a cursor it can evict older complete events. Moreover, FULL installs its stable retention cursor only **after** the final fence. These observations weigh against a pinned-backlog/transaction-commit cycle in this run. They do not prove that every other storage wait is impossible. [Retention-dependent waits and eviction](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/storage/engine/replication_log.cpp#L1428-L1492), [FULL retention after fence](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/replication/replication.cpp#L11027-L11040).

The empty socket queues, fully ACKed TCP bytes, zero netem drops, and stable target CPU likewise argue against the flow merely awaiting transmission or consuming a large record backlog. TCP evidence alone cannot establish application ACK completion or locate a suspended coroutine.

## Benchmark disposition and replacement cohort

- Preserve the original run as a failed baseline with its logs, metrics, and snapshots. Do not turn its timeout/stall into a numeric slow-baseline sample, speedup denominator, zero, or infinity. Do not silently replace its evidence.
- Run a new, explicitly identified **32 MiB publisher-queue cohort** for both main and the chunks candidate, at both 5 ms and 20 ms RTT, with all required repetitions. Keep the dataset, one-transaction-per-second writer, warmup, worker layout, storage, backlog, and all other conditions matched. Existing 16 MiB results may remain descriptive evidence, but must not be pooled with 32 MiB results in a before/after comparison.
- For one pending reservation and an otherwise empty publisher queue, the precise minimum that lets the 256-byte fence coexist is `16,777,933 + 256 = 16,778,189` bytes. Since the benchmark configures integer MiB, **17 MiB is the mathematical minimum for this specific occupancy**. Choosing 32 MiB provides a conventional margin for staging metadata and transient queue occupancy; it does not reduce the requested write rate or shrink the workload to obtain a favorable result.
- With one foreground writer, a 32 MiB queue can accommodate this reservation and a fence; two such reservations would exceed it. This is a targeted test-configuration workaround for the source-supported capacity cycle, not a general production deadlock fix or a proof covering arbitrary concurrent clients.
- Successful new runs must still pass the existing acceptance checks: one FULL attempt per source flow, complete capture when enabled, no writer error, all-flow ONLINE, post-write fences, and complete source/target data digest agreement. They can support only the measured recovery comparison under their declared **32 MiB queue** conditions.
- The original 20 ms candidate with a 16 MiB queue was not executed. Neither optimization may be credited with preventing this failure. Whether the exact hypothesized cycle caused this run, and how to repair the production admission/gate contract, require separate investigation or a focused reproduction with stronger introspection.

中文简述：已观察到未优化 main 在配置 16 MiB queue 的 live FULL benchmark 中停滞。源码存在“EXEC 持 reservation 等 gate；FULL 持 gate 等 fence 容量”的循环，现场字节数精确吻合，但没有 coroutine 栈，尚不能确认具体等待位置。新 32 MiB cohort 是双方一致的测试配置调整；原失败证据保留，不能作为耗时样本，也不能声称窗口优化解决了该问题。

Final outcome, guard and writer evidence: [diagnostic classification](evidence/diagnostic-failure/diagnostic-outcome.json), [result](evidence/diagnostic-failure/result.json), [acceptance rejection](evidence/diagnostic-failure/acceptance.json), [driver outcome](evidence/diagnostic-failure/driver-outcome.json), [raw writer attempts](evidence/diagnostic-failure/writer-samples.jsonl). Full periodic samples and pcap remain at their original NVMe paths.
