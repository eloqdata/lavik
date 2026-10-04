# Native FULL command batching and sustained live-write interleaving

Date: 2026-10-04. Read-only source analysis for issue #131. This note preserves the first accepted pair observed while the matrix was running; the completed repeated-run comparison belongs in the main report. No build, process test, benchmark, or pcap analysis was run for this note. The existing runner-produced JSON and logs were read directly.

The common FULL command path really batches frames, but its batching is bounded by the currently available FIFO prefix and waits for a batch's completed-command ACKs before collecting the next prefix. A single scan interleave can repeat this sequence for newly arriving commands until it has completed 64 items. Consequently, a small, healthy queue can coexist with substantial time spent servicing live writes before the scanner advances. This is a mechanism to profile, not a measured allocation of this run's total time.

## Observed first pair

Case: `dense-live-128m-f1-rtt5`: 32,768 seed Strings of 4 KiB concentrated in partition 13907; one source and one target worker; 5 ms added RTT; one client issuing ordinary SETs at a maximum 500/s to a rotating set of 1,024 separate keys; 16 MiB publisher queue per worker. Five seconds of warmup populated the writer keys before FULL. This is a mixed dense/sparse, continuously written workload, not merely a static 128 MiB dense snapshot.

| Existing result | chunks | ordinary baseline records |
|---|---:|---:|
| Lavik revision | `51d03f97fede341fb42c14bd46a9f1aaf416eeb1` | `9da9c797eefaea45dfde3ccc521baa440de559a8` |
| FULL seconds | 639.132501 | 631.909821 |
| Completed foreground writes during FULL | 319,546 | 315,929 |
| Completed foreground QPS | 499.9683 | 499.9590 |
| `kRecords` frames | 1,090 | 1,090 |
| `kFullSyncCommand` frames | 274,368 | 271,138 |
| Reset frames / partition handoffs | 256 / 16,384 | 256 / 16,384 |
| Runner's FULL FIFO gauge peak over entire collection window | 24,157 | 28,079 |
| FULL publisher backpressure waits | 0 | 0 |
| Acceptance / final source-target digest | Passed / equal within run | Passed / equal within run |

Evidence: [compact original results and acceptance records](evidence/raw-results.jsonl), selecting `case=dense-live-128m-f1-rtt5`, `repeat=1`, and labels `chunks` / `records`. Full logs and samples remain at the original NVMe paths listed in the artifact manifest.

These are one accepted pair, not the completed repeated-run comparison. Their source digests legitimately differ between runs because the number and final versions of live writes differ; each run's source and target digest match. Reported typical queue samples around 1–2 KiB must not be presented as the global peak: the existing runner summaries above record larger sampled maxima, still far below the configured queue waterline. Those runner maxima include its collection interval, not an independently reconstructed exclusive FULL interval.

The existing record grouping can account for the 1,090 record frames as 66 frames at the dense seed partition plus 1,024 single-frame writer-key partition groups. Record frames alone do not tag their source as baseline versus replacement; this count is not proof that every writer-key record came from one particular origin. It does show that the hundreds of thousands of frames are command traffic, not hundreds of thousands of ordinary replacement-record frames.

## What command batching actually guarantees

The following mechanism is present in main `25e159418603a95c1b2cca68173b329067e3ef48` and unchanged by the large-value chunks PR. PR2 retains the command batching and ACK contract, adding the snapshot-receipt barrier described below.

1. `drain_fullsync_publish_queue(max_items)` peeks at most `min(remaining_items, 128)` **currently queued** items. An empty peek returns immediately. It collects only the consecutive command prefix; a publish-record item stops that prefix and is handled by its separate path. There is no deliberate wait to fill a larger command batch. [Peek and prefix selection](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/replication/replication.cpp#L10186-L10251), [storage peek returns existing queue entries](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/storage/engine/replication.cpp#L1318-L1342).

2. Those commands are encoded into wire batches capped at **128 frames and 2 MiB**, then submitted through `WriteAllV`. The ordinary scan interleave supplies a **64-item** budget, so for these small, single-frame SET commands that invocation can send no more than 64 logical commands in total; a particular `WriteAllV` may contain only one or a few. The 128-frame constant is a ceiling, not the observed batch size. [Constants](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/replication/replication_internal.h#L228-L255), [wire batching](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/replication/replication.cpp#L10258-L10349).

3. After `WriteAllV`, the sender waits for each completed command's exact final-fragment ACK in that wire batch before advancing to the next batch. Commands already in the same batch overlap in flight: this is **not** one network RTT per command when a batch contains several commands. However, new commands arriving while those ACKs are awaited do not join the already-built batch. The next peek happens only after the selected commands have been processed. Large commands can span batches; only their final fragment requests an ACK, so intermediate fragments do not impose a per-fragment stop/wait. [ACK barrier and queue release](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/replication/replication.cpp#L10317-L10362).

4. The target processes each command's final fragment by decoding and applying the logical command, finishing its partition context, and then sending its ACK. Multiple command frames in a source write do not imply concurrent target command application. Partition handoff ordering may also be awaited. [Target command apply and ACK](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/replication/replication.cpp#L9144-L9270).

Neither the frame totals nor periodically sampled queue bytes reveal the actual distribution of commands per `WriteAllV`, the number of empty peeks, or the number of 64-item budget exits. Those quantities remain unmeasured.

## Why a low queue does not imply a cheap interleave

The 64-item budget applies to the entire drain call, **including commands that arrive after its first peek**. A possible sequence is: peek two commands, write them, wait for their ACKs, observe several newly committed commands, write those, and repeat until the total reaches 64. Returning early depends on encountering an empty FIFO at a peek boundary. A queue containing only a few commands after every ACK can therefore keep one drain invocation active for many ACK batches.

Illustratively, 500 captured commands/s would produce about 2.5 commands during a 5 ms interval. If a small batch's turnaround allowed similar arrivals and each next peek remained nonempty, servicing 64 commands could consume roughly 0.128 s before returning to scanning. This is an arithmetic example, **not an estimate of this run's actual batch latency**: only writes to covered/tailing keys enter the command FIFO, command application and scheduling add time, and arrivals and empty checks are not synchronized to a fixed RTT.

The high/low-watermark policy is additional to this behavior. Each interleave first performs the 64-item drain, then checks whether queued plus admitted bytes exceed capacity/8. With a 16 MiB queue this threshold is 2 MiB, and the low threshold is 1 MiB. Exceeding the high threshold causes further 128-item drain passes until queued bytes fall below the low threshold. Thus the ordinary repeated-small-batch behavior requires neither queue saturation nor this priority mode. The observed samples and zero admission-backpressure count do not support attributing the long run to publisher-capacity blocking; unsampled brief occupancy spikes are not excluded. [Interleave and watermarks](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/replication/replication.cpp#L10465-L10495).

## Scan work and where these waits occur

The sender scans one partition/DB at a time. Snapshot reads use a default 64-key scheduling quantum and retain ordinary records across reads until they form a transfer-sized frame or reach the DB boundary. After **every snapshot-read return**, it awaits the live publisher interleave and then the current partition's replacements before requesting the next read. Storage scan buckets and byte limits can make the actual returned batch differ from exactly 64 records. [Snapshot aggregation and interleave](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/replication/replication.cpp#L10726-L10826), [storage selection and read waves](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/storage/engine/replication.cpp#L823-L937).

There is also a publisher interleave **after every included partition completes**, before its handoff. This applies even when all of that partition's DBs used the empty-DB fast path. In this standalone full-population run, all 16,384 physical partitions are included; the work is not limited to the one dense seed partition or the partitions that emitted record frames. Reset batches and handoffs remain part of that traversal, and empty scans periodically yield for scheduling. [Partition completion interleave](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/replication/replication.cpp#L10849-L10883), [empty DB transition](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/storage/engine/replication.cpp#L729-L758).

Ordinary writes to an unstarted DB need no FULL command yet; later baseline scanning observes their state. Once a key has baseline coverage, or its DB is tailing, subsequent command effects enter the FULL command FIFO. Consequently, as the scan passes more writer-key partitions, a larger fraction of ongoing writes can require command replay before the cut. The gate that makes the final prefix finite closes only after the scan/handoff readiness barriers. More wall time before that cut allows more live commands to arrive, which can require more interleaved service before further scan progress. [Capture routing](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/storage/engine/write.cpp#L2300-L2332), [readiness and final cut](https://github.com/eloqdata/lavik/blob/25e159418603a95c1b2cca68173b329067e3ef48/src/replication/replication.cpp#L10887-L10958).

The existing source logs show continued partition progress and repeated `snapshot` labels for dense partition 13907, followed by further partition traversal. A `snapshot` interval includes interleaved command service; an `override_catchup` label can remain visible across the publisher drain and handoff. These labels must not be treated as exclusive scan or replacement timings. The source-veth record/command spans likewise include intervening work and waits.

## Applicability of the two record-window PRs

PR1 changes streamed large-value record frames. This workload's seed values are 4 KiB and its separate writer values are 128 bytes, so that optimization is not a mechanism for accelerating its ordinary command FIFO.

PR2 pipelines ordinary **baseline record frames within a DB**, but preserves completion before servicing a nonempty publish FIFO. It reaps completed receipts on an empty peek; a nonempty peek first calls `drain_snapshot_acks()`. Under sustained live traffic, a freshly flushed baseline frame may therefore be joined at the next interleave before later baseline frames fill the eight-frame window. Also, the 1,024 single-frame writer-key groups have no within-DB multi-frame overlap opportunity. These are explicit limits of the scoped optimization. [PR2 nonempty-FIFO barrier, revision 9da9c797](https://github.com/eloqdata/lavik/blob/9da9c797eefaea45dfde3ccc521baa440de559a8/src/replication/replication.cpp#L10285), [PR2 scan interleave and DB drain](https://github.com/eloqdata/lavik/blob/9da9c797eefaea45dfde3ccc521baa440de559a8/src/replication/replication.cpp#L10980).

The small difference in this first pair does not establish how much time any of these mechanisms consumed. It does establish that the accepted live workload produced far more command frames than record frames; record payload bytes were still larger, so this is a frame-count comparison rather than a bandwidth comparison. A large static-dense-record gain must not be generalized to this condition. Neither PR implements a new ordinary replacement/publish-record pipeline or a command-window redesign. There is no basis here for blaming the long run on an ordinary replacement-record path merely because the last progress label says `override_catchup`.

## Follow-up profiling that would distinguish the hypotheses

Collect these only in a separately scheduled profiling run, after the current benchmark lane is free:

- Per drain invocation: call site (snapshot quantum, partition completion, handoff-ready wait, final cut), commands present at entry, total commands completed, number of peeks and `WriteAllV` calls, and exit reason (empty FIFO, item budget, error). This distinguishes continuously replenished 64-item passes from a single large batch.
- Per command wire batch: frame count, logical-command count, bytes, source encode/write time, and elapsed wait for the batch's last required ACK. Instrument sender ownership directly; frame totals or packet counts cannot identify these coroutine batch boundaries reliably.
- Target: handoff dependency wait, decode/apply time, owner-worker scheduling, and ACK write/completion time. Preserve the distinction between a source-side ACK wait and target CPU work.
- Scanner: actual `SnapshotPartition` calls, keys/records and cursor progress per call, time reading data, time inside the command/replacement drains, partition completion count, and reset/handoff waits. Record PR2 snapshot-window occupancy and the reason for each drain.
- High/low-watermark entries and queue occupancy at the same points, with foreground arrival/completion timestamps. This determines whether admission or the scan/publisher scheduling policy limits progress without inferring it from occasional queue samples.

No particular command scheduling change is proposed by this note. The first step is to measure the above boundaries; changing a frame window, batch-fill policy, or scan quantum without them could trade source write latency against scan progress in an unmeasured way.

中文简述：FULL command 的确支持批发，但批大小取决于 Peek 当时已有的连续命令；发完本批并等 ACK 后，当前 interleave 还可继续处理新到命令，直到累计64项或某次 Peek 为空。这个入口既在每次小批扫描后，也在每个 included partition（含空分区）完成时运行。因此小队列、无背压也可能伴随许多小批 ACK 等待和持续写入放大。PR2 还会在非空 FIFO 前排空 baseline receipts，live 场景未必能填满记录窗口。当前证据不能把数百秒拆成独占阶段，也不能将其归咎普通 replacement。
