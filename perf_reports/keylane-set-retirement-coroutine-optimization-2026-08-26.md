# Keylane SET retirement coroutine optimization

Date: 2026-08-26 UTC

## Result

Keylane no longer creates a child `Task` coroutine for every superseded record
after a block flush. It retains one foreground settlement coroutine per flush,
settles records owned by the current worker synchronously, and uses the
`SubmitTo` awaiter only for exceptional remote-owner records. Record order is
preserved; there is no sort.

At 100K QPS, 1K overwrite SET, pipeline one, the client p99.99 histogram moved
from `1.759-2.095 ms` to `1.271-1.535 ms`. The two bounds improved by 27.7%
and 26.7%, respectively. Throughput remained rate-limiter-bound and unchanged:
99,860.96 QPS before versus 99,868.21 QPS after. Average latency moved from
0.085 ms to 0.087 ms and p99 from 0.215 ms to 0.223 ms, both within run noise.

| Metric | Original | Optimized |
|---|---:|---:|
| Requests | 5,500,042 | 5,500,042 |
| Throughput | 99,860.96 QPS | 99,868.21 QPS |
| Average | 0.085 ms | 0.087 ms |
| p99 | 0.215 ms | 0.223 ms |
| p99.99 histogram bracket | `1.759-2.095 ms` | `1.271-1.535 ms` |
| Maximum | 7.279 ms | 7.303 ms |

The client reports adjacent HDR histogram percentiles at 99.988% and 99.994%,
so p99.99 is correctly stated as a bracket rather than an invented exact
number.

## Root cause and implementation

The flush completion path batches superseded `RetiredRecord` objects and
spawns one `MarkRetiredRecordsDead` task. The previous loop then called
`co_await MarkRecordDead(record)` for every element. A normal overwrite almost
always belongs to the same worker, so that child task did not suspend, but its
coroutine frame was still allocated, entered, completed, and destroyed once per
retired record. Under 100K overwrite SET this is approximately 100K unnecessary
child coroutine lifecycles per second.

The optimized loop checks `block_owner_` inside the already-existing settlement
task. Same-worker records call `MarkRecordDeadLocal` directly. Cross-worker
records retain the original order and await `SubmitTo` directly. The settlement
task remains foreground work because live-byte accounting and defrag eligibility
are correctness work, not optional background maintenance.

An earlier experiment settled same-worker records inline in the flush completion
critical section. It produced similar latency but was rejected: the old block
could still have `flush_in_progress_` set when `MaybeQueueDefrag` ran, so the
candidate was skipped with no later retry. Keeping one post-flush settlement
task preserves the required scheduling boundary without returning to one child
task per record.

## Validation

- Release build: `-O3 -DNDEBUG`, SET latency trace enabled only for the A/B.
- Unit tests: 99/99 passed.
- Relevant E2E tests: multikey, MULTI/EXEC, atomicity stress, TTL, and extent
  recovery all passed (5/5).
- `keylane_flushdb_reclaim_e2e` passed in a true Debug build in 28.99 seconds.
- The same test is not a valid Release regression check because its deterministic
  flush-snapshot pause hook is compiled only under `#ifndef NDEBUG`. Both the
  original and optimized Release binaries timed out in its armed defrag-crash
  scenario.

The performance run used 12 pinned Keylane workers on CPUs 0-11, housekeeping
and local tools on CPUs 12-15, SPDK completion batch 8, busy poll 20 us, and
foreground budget 1000 us. The client on 10.0.0.5 used 80 connections, eight
threads, pipeline one, 100K target QPS, 55 seconds, a 33,003,587-key overwrite
range, and 1000-byte generated values.

Raw evidence:

- Original: `perf_runs/keylane-empty-set-tail-ab-20260826/crosscore-overwrite-1k-100k-55s/`
- Optimized metrics and trace: `perf_runs/keylane-empty-set-tail-ab-20260826/one-flush-coroutine-no-child-task-overwrite-1k-100k-55s/`

## Remaining tail

The optimized server-side SET trace normally placed each worker's p99.99 at or
below 1 ms, with append/block wait dominating that bucket. The client p99.99
still lies above 1.27 ms, so removing coroutine-frame churn is a material but
not complete solution. The next independent target is the block rollover and
writer-buffer readiness path; it should be profiled separately rather than
folded into this change.
