<!-- Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 -->

# Purpose-Specific Meta Views

[简体中文](README.zh-CN.md)

Acceptance measurements for [#256](https://github.com/eloqdata/lavik/issues/256),
the final batch of [#144](https://github.com/eloqdata/lavik/issues/144).
This compares the pre-migration source with the completed sequence of consumer
migrations. Removing the now-unused aggregate APIs in this final batch is not,
by itself, a new runtime optimization of ordinary reads.

## Revisions and Environment

- Baseline: `25e159418603a95c1b2cca68173b329067e3ef48`, the batch-1 baseline.
  Its original temporary measurement package was removed during the cleanup
  documented in [#257](https://github.com/eloqdata/lavik/pull/257). These are
  fresh paired measurements of that exact source, not recovered historical
  samples or a comparison between different machines.
- Candidate: `920f879b05636d066ef0485f1ee1121813ef79ac` plus this change's
  aggregate-API removal. The retained metadata records the production diff,
  executable SHA-256 values, instrumented-source hashes, and compiler commands.
  Other intervening commits are not reverted: component instrumentation directly
  supports the copy-volume claim, while process timings compare complete Meta
  revisions and do not isolate every latency change to this migration.
- One x86-64 VM, Intel Xeon Platinum 8573C, 8 logical CPUs / 4 cores, one NUMA
  node; inherited CPU affinity `0-7` for both variants. No dedicated-host,
  fixed-frequency, or production-capacity claim is made.
- GCC 13.3.0, C++23, Release `-O3 -DNDEBUG`, native CPU tuning and LTO;
  statically linked C++ runtime. Meta uses the system C++ allocator backed by
  glibc 2.39, **not mimalloc**. The embedded Raft bridge uses Go 1.26.8.
- Bycorf `62509c93d40c2480f5046b71454db6cf95801b04` and mimalloc
  `acf2fdd329f9dc2a7ffe3f12a133fe7175e39378` are identical for both builds.
  The process comparison uses the **same candidate Debug Data and CLI binaries**
  for both Meta variants. It isolates a Meta change, not two complete Release
  server stacks. Data retains its ordinary mimalloc allocator.
- Builds, compiler/Go/Python caches, sockets, temporary files, WALs, snapshots,
  and Data files are under `/mnt/local_nvme/issue256`; the short test-data alias
  `/mnt/local_nvme/i256d` points into that tree.
- Each case has three independent alternating pairs: **AB, BA, AB**. Builds
  and functional tests completed before the measurement sequence. Normal VM
  services remain running; process names and CPU usage are recorded.

## Measurement Boundaries

### Owned Captures

`bench/component.cpp` installs the same valid serialized fixture into each
state machine, warms each reader for 25 ms plus three captures, then measures
100 capture/destruction pairs. Equal-duration warmup reduces cold-start bias
from the much longer aggregate-copy path's unequal preparation time. The
persistent-snapshot serialization control uses ten pairs.
Capture timing includes transferring the returned value into the benchmark's
optional owner; destruction resets that owner. Inline return/move costs are
therefore included, separate from the instrumented state-lock interval.

Candidate rows exercise proposal, observation facts, creation/membership/
failover discovery, automatic detection, Data publication, Admin Group,
operation status, committed cursor, and the two domain exports. The legacy
rows use `StoresSnapshot()` as an **aggregate-copy reference**. Repeating that
reference under each row name does not mean every historical endpoint used
that primitive: for example, operation status already had a point-read path.
These rows are not historical endpoint latency or complete planner timing.
The discovery fixtures contain maintenance operations, not matching active
creation/membership/failover work. Their empty-worklist captures test filtering
and unrelated payload exclusion; zero allocations do not describe active plans.

Global C++ allocation wrappers retain `malloc/free` and count requested bytes,
allocation calls, delete calls, and allocator-usable bytes freed. Requested and
usable bytes are different quantities. Direct C/Go allocations are not counted.
These are dynamic allocation bytes, not every byte read or copied: inline
storage such as the aggregate's slot array is outside the allocation counters.
Counters are thread-local; logging and fixture preparation are outside the
timed interval. Task-only generated state-machine translation units instrument
the existing mutex guards, without changing production locks or adding hooks.
Lock hold/wait columns are means; latency columns are sample quantiles.

The matrix separates:

- Current topology: 8/2, 128/32, and 4,096/512 Data nodes/Groups, including
  active transition facts; current directives grow from 3 to 96.
- Fixed selected records: 1/16/128 unrelated 256 KiB operations, 1/128 archived
  64 KiB results, 32 versions of each relevant Policy, and 128 unreferenced
  manifests with 128 entries each.
- Selected operation body: 1 KiB, 64 KiB, and 256 KiB. Copying that selected
  complete record is intentional for paths that own it.
- Exact retained audit sizes: 0, 1,024, and 65,536, plus combined fixtures.

The unrelated-growth check compares both requested bytes and allocation counts
for ten ordinary capture entry points in every trial. Exports are deliberately
excluded: archive export must retain its domain, and audit export retains the
existing shared-page handles. Necessary global filtering/scanning and current
topology costs remain; this is not an O(1) claim for all reads or all work.

### Apply Under Read Pressure

Separate two-second runs apply successful idempotent `RegisterNode` commands
while a reader requests publication at 100 Hz. Both variants use the same
offered read schedule. Actual sample counts are retained because contention
can delay a reader. Every apply result is decoded and checked for acceptance.
The contended publication timer spans capture **and destruction** of the view;
it is distinct from the isolated capture-only rows.
Throughput includes the benchmark's result checks and sample bookkeeping; it
is not Raft throughput or a production maximum. The writer grows/rotates the
audit window, so these are runs from specified initial states, not frozen
audit-size experiments.

### Real Concurrent Processes

Each trial creates three real Meta processes, two managed-Single Data nodes,
and a readable/writable owner. One sequential stream re-proposes the same
maintenance operation with a **20 Hz target and one outstanding request**,
avoiding live-operation growth.
Admin status, Sentinel
MASTER, and Redis SET have 100 Hz offered schedules. Fifty changing lease
Policy revisions force real publication to both Data sessions, with a 600 ms
pause after each completed publication. This gives the 1,100/1,200 ms policy's
causal lease renewal time to complete; faster policy churn is a different
authority-starvation workload. Each trial's publication p99 is its maximum
among 50 samples, so these are local observed tails, not a high-confidence
production p99 estimate.

Publication requires exactly one `full_states_applied_total` increment on each
Data node. Session connectivity, reconnect/protocol-error counters, and Meta
leadership/term must remain stable. The reported latency is an **observed
completion bound**, including HTTP metric polling and detection overhead, not
a pure transport or wire latency. Both submission-to-observation and
reply-to-observation samples are retained.

Before timing, Sentinel must converge to the expected published owner within ten
seconds, with elapsed setup wait recorded. The owner must become writable;
only transient
`MASTERDOWN`/`LOADING` replies are retried, for at most ten seconds, and the
retry count is recorded. Measured Redis commands never retry and must return
`OK`. Sentinel MASTER must identify the expected
owner; `master,disconnected` responses during Policy/ACK freshness gaps are
counted separately from `master`. They are not silently converted into healthy
availability samples. Sentinel counts cover the complete measured phase.

Latency summaries and proposal throughput use only the publication-overlap
interval. The proposal stream can continue afterward to reach 500 requests;
those tail samples are retained but excluded from concurrent summary columns.
This is a paced sequential workload, not saturation testing. Timers begin just
before sending, so latency excludes scheduling delay. Slow responses delay
target ticks and can cause catch-up bursts. Throughput is completed requests per
overlap second, not capacity. Snapshot distance is 1,000,000, and an unchanged snapshot
index is required throughout the timed interval.

Audit targets are **initial/evolving windows**. Filling reserves the exact rows
needed by subsequent operation setup, and the final count must equal 1,024 or
65,536. The filler identity exists in every case. Target 0 means the minimum
bootstrap/fixture audit, not an empty live log. Actual before/after counts are
recorded. Accepted writes append
audit rows and bounded rotation remains enabled. Only the static component
matrix establishes exact 0/1,024/65,536 retained sizes.

### Serialization, Raft I/O, Snapshots, and Strict Admission

- Capture/destruction rows exclude command encoding, Raft, network, disk I/O,
  and durable snapshot generation.
- `command_encode` is a minimal `RegisterNode` serialization control.
- `snapshot` measures the existing full-state `Capture(index)` serialization
  path, not a purpose-specific read. Whole durable-state work is preserved.
- Real proposal latency includes admission, serialization, Raft/quorum/WAL,
  dispatch and Admin transport. It is not labeled raw Raft I/O, and no
  subtraction of unrelated component medians is used to manufacture a Raft
  number. The earlier scoped raw-Raft experiment remains available in
  [the batch-2 evidence](https://gist.github.com/liunyl/a24279afc090f89ecd6ad13aafc58a32).
- Manual snapshot timing is recorded separately after each process phase and
  includes Raft coordination, serialization and persistence. Final audit counts
  differ with completed load; these are contextual costs, not identical-image
  serialization pairs.
- A separate one-Meta strict-export experiment measures 256 successful commands
  approaching capacity and then 256 exact `ERR resource-exhausted` replies at
  65,536 records. A stabilized committed cursor must not advance during the
  rejected phase. Rejection and successful-write distributions are never pooled.

## Results

The raw trials and the generated per-case median tables accompany this report.
Summary values are medians of independent trial statistics, not pooled
percentiles. All three rounds, including small fixtures and write-path results,
must be considered; no best-run selection is used.

### Copy Volume and Capture Costs

All **420** fixed-selected-record allocation checks pass: three trials, ten
ordinary readers, seven unrelated-growth shapes, and bytes/counts separately.
The combined 65,536-audit fixture has 128 Data nodes, 32 Groups, 128 unrelated
64 KiB operations, 32 archived 64 KiB results, Policy history, unreferenced
manifests, and twelve current directives. Medians below compare owned candidate
captures with the legacy **aggregate-copy reference**, not old endpoint latency.

| Capture | Requested bytes before / after | Allocations before / after | Capture p50 us before / after | Destroy p50 us before / after | Lock hold us before / after |
|---|---:|---:|---:|---:|---:|
| Proposal audit gate | ~10,861,500 / 8,233 | 2,844 / 2 | 842.673 / 3.552 | 69.116 / 2.940 | 793.339 / 3.475 |
| Observation facts | ~10,861,500 / 37,408 | 2,844 / 322 | 842.619 / 9.917 | 69.177 / 3.820 | 792.892 / 9.833 |
| Data publication | ~10,861,500 / 76,391 | 2,844 / 797 | 842.614 / 39.271 | 69.343 / 9.222 | 792.927 / 38.832 |
| Admin Group | ~10,861,500 / 470 | 2,844 / 7 | 843.054 / 0.233 | 69.977 / 0.069 | 793.464 / 0.147 |

This removes unrelated deep copies, not necessary current-state work:

- At 128/32 nodes/Groups, facts allocate 37,408 bytes and take 10.037 us p50;
  at 4,096/512 they allocate 930,304 bytes and take 255.187 us. Including 512
  active transitions increases this to 980,480 bytes and 335.349 us.
- Growing current directives from 3 to 96 at 128/32 nodes/Groups raises
  publication allocation from 69,947 to 136,535 bytes and capture p50 from
  37.177 to 50.155 us.
- Growing the **selected** operation body from 1 to 64 to 256 KiB raises
  publication allocation from 6,535 to 71,047 to 267,655 bytes. The point
  operation-status view grows from 1,741 to 66,253 to 262,861 bytes. Facts stay
  at 2,338 bytes. This is the allowed full-selected-record cost.
- The 65,536-entry audit export still allocates 8,192 bytes for one page-handle
  vector. Archive export retains ~2.10 MB of its requested domain. Neither is
  claimed constant-size or included in ordinary-view invariance.

Full persistent snapshot capture remains ~160.95 MB of cumulative allocation
and 84 allocation calls on both sides of the combined fixture, with p50
13.436 / 13.512 ms. Minimal command encoding remains 375 bytes / six calls,
0.168 / 0.166 us p50. These controls show no material intended optimization.
CSV values use six significant digits; large byte values are approximate.

### Successful Apply Under Equal Offered Reads

| Initial shape | Accepted applies/s before / after | Apply p99 us before / after | Publication capture + destruction p99 us before / after |
|---|---:|---:|---:|
| Small | 543,749 / 571,569 | 3.698 / 2.090 | 172.777 / 123.786 |
| Combined, audit 0 | 151,309 / 164,373 | 10.967 / 7.580 | 1,481.720 / 710.563 |
| Combined, audit 1,024 | 150,667 / 165,436 | 10.975 / 7.724 | 1,531.440 / 942.787 |
| Combined, audit 65,536 | 150,588 / 166,361 | 11.439 / 8.150 | 1,440.250 / 595.982 |

Combined writer-lock wait falls from about 0.58 to 0.067 us per measured
acquisition. This is direct state-machine apply under a 100 Hz offered reader,
not a claim about Raft or deployed write capacity.

### Concurrent Process Latency

All 36 trials completed with successful Redis writes, stable leadership and
Data sessions, exactly one application per Policy publication, and no snapshot
index change during measurement. Achieved proposal rates were 19.996-20.031/s,
matching the offered target rather than measuring capacity.

| Initial audit target | Unrelated operations | Proposal p50/p95/p99 ms before -> after | Publication p99 ms before / after |
|---:|---:|---|---:|
| 0 | 0 | 2.073/2.654/5.673 -> 1.233/1.473/1.676 | 20.238 / 12.087 |
| 0 | 128 | 4.098/6.522/12.143 -> 1.258/1.454/1.669 | 35.885 / 11.935 |
| 1,024 | 0 | 2.137/2.759/5.378 -> 1.238/1.425/1.673 | 21.116 / 12.160 |
| 1,024 | 128 | 4.156/6.725/11.101 -> 1.250/1.467/1.689 | 36.680 / 11.967 |
| 65,536 | 0 | 2.241/3.356/6.933 -> 1.282/1.471/1.672 | 21.795 / 12.456 |
| 65,536 | 128 | 4.278/8.184/12.841 -> 1.298/1.485/1.692 | 50.353 / 12.269 |

Target 0 actually starts with 36 audit records without history, or 164 with
128 unrelated operations. In the largest case, Admin p99 is 8.289 / 0.205 ms,
Sentinel MASTER p99 is 6.933 / 0.481 ms, and Redis SET p99 is 0.375 / 0.275 ms.
Sentinel still reports `master,disconnected` in 304/9,433 baseline and
122/9,198 candidate samples across the three complete phases. Successful
request latency therefore does not establish continuous healthy Sentinel
availability. All cases and flag counts are in [the generated summary](summary.md).

Manual snapshot medians for that largest case are 90.107 / 90.690 ms,
measured after load and not included in the proposal/publication intervals.
Together with the component controls, the final small/write-path pairs show
no repeatable material regression in this workload; this is not a uniform
speedup claim across all retained full-state operations or stress protocols.

### Strict Audit Admission

Each variant has three trials, each with 256 near-full successful commands
followed by 256 exact capacity rejections. All six trials pass the acceptance
and unchanged-committed-cursor checks. Median trial quantiles are separate:

| Path | Variant | p50 us | p95 us | p99 us |
|---|---|---:|---:|---:|
| Near-full successful write | Before | 481.534 | 698.676 | 809.463 |
| Near-full successful write | After | 215.236 | 287.241 | 336.663 |
| Full-window rejection | Before | 255.382 | 284.197 | 320.875 |
| Full-window rejection | After | 45.634 | 57.212 | 74.657 |

These are one-Meta admission measurements. Fast rejection is not successful
write throughput and is not pooled with accepted-command latency.

### Diagnostics and Excluded Attempts

All interrupted attempts are retained separately, not mixed into final pairs:

- The first component campaign had only three count-based warmups. Small
  snapshot p50 appeared to regress from 18.904 to 21.345 us although its
  Capture/Serialize implementation was unchanged. Repeating **all six**
  component trials with equal 25 ms warmup gave 21.338 / 20.772 us. This is
  preconditioning-sensitive evidence, not proof of a particular CPU/cache
  cause. The initial campaign's combined-65,536 publication tail also varied
  across overlapping ranges; the final equal-warmup pairs are reported above.
- Initial process setup hit transient `MASTERDOWN` and `s_down,master` after
  audit filling. Bounded untimed readiness checks were added; timed requests
  retain strict error checks. Filling audit before adding large blobs avoids
  paying unrelated deep copies during preparation and reserves exact audit rows.
- A 10 ms-between-publications campaign completed fifteen trials, then its
  baseline audit-1,024/no-history trial lost the Redis connection. The owner
  logged lease expiration while Meta logged pending/stale causal lease anchors.
  Policy changes invalidate pending grants, so this is consistent with lease
  starvation under concurrent commits/churn, not a process crash. It is an observed
  baseline workload failure, not successful latency evidence or proof that
  the candidate cannot fail under such churn.
- Increasing publication spacing to 600 ms while retaining unpaced proposals
  still failed the first baseline trial after about 9.4 seconds, 1,929 proposals,
  and fifteen publications. Thus policy pacing alone did not resolve it. The
  baseline heartbeat requires the publisher's validated high-water to catch
  the committed cursor before granting authority. Continuous proposal load can
  keep that proof stale. The final latency experiment therefore also uses an
  identical 20 Hz target-paced proposal load on both sides; it does **not** claim
  successful saturation or reuse successful prefixes of the failed runs.

Component and final process campaigns have separate metadata because only the
process setup/pacing harness changed between them. Executable hashes match.

## Retained Evidence

- [`summary.md`](summary.md): generated medians, all capture/destruction
  counters, lock costs, apply results, process validity context, and strict
  admission distributions. Generation rejects incomplete trial matrices.
- [`evidence.tar.gz`](evidence.tar.gz),
  [`archive checksum`](evidence.tar.gz.sha256), and
  [`per-file checksums`](raw-SHA256SUMS): raw CSV/JSON samples, process logs and
  configurations, compiler/binary/source provenance, task environment, test
  XML/logs, and prerequisite CI evidence. Binaries, WALs and Data images are
  deliberately excluded from the committed archive and remain task-local.
- `measurements/component-*` contains the six final warmed component trials;
  `measurements/combined/results.json` and `measurements/strict/results.json`
  are the final process datasets. Original component metadata and final
  `process-metadata.json` retain their separate execution-time snapshots.
- `measurements-initial`, `combined-aggressive`, `combined-unpaced`, the saved
  aggressive/unpaced harnesses, and the process/strict smoke and preflight
  directories are diagnostic evidence, **not additional final trials**.

To verify the bundle, run `sha256sum -c evidence.tar.gz.sha256`, extract it under
an isolated `/mnt/local_nvme` directory, then run
`sha256sum -c /path/to/raw-SHA256SUMS` from that extracted root.
`bench/summarize.py <extracted-root>/measurements` regenerates the summary.

## Reproduction

The source harness is in [`bench/`](bench). Start from an isolated NVMe root;
the commands below assume the candidate checkout is `$ROOT/src`, the exact
baseline checkout is `$ROOT/baseline`, and matching dependencies are initialized.

```bash
export ROOT=/mnt/local_nvme/issue256
export TMPDIR=$ROOT/tmp
export XDG_CACHE_HOME=$ROOT/cache
export GOCACHE=$ROOT/cache/go
export GOMODCACHE=$ROOT/cache/gomod
export GOTMPDIR=$ROOT/tmp
export PYTHONPYCACHEPREFIX=$ROOT/cache/pycache
export LAVIK_TEST_DATA_DIR=/mnt/local_nvme/i256d
mkdir -p "$TMPDIR" "$GOCACHE" "$GOMODCACHE" "$ROOT/data"
ln -s "$ROOT/data" "$LAVIK_TEST_DATA_DIR"

cmake -S "$ROOT/baseline" -B "$ROOT/release-before" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF \
  -DLAVIK_ENABLE_OPT=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -DLAVIK_BYCORF_SOURCE_DIR="$ROOT/src/bycorf"
cmake -S "$ROOT/src" -B "$ROOT/release-after" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF \
  -DLAVIK_ENABLE_OPT=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -DLAVIK_BYCORF_SOURCE_DIR="$ROOT/src/bycorf"
cmake --build "$ROOT/release-before" --target lavik-meta -j 2
cmake --build "$ROOT/release-after" --target lavik-meta -j 2

cmake -S "$ROOT/src" -B "$ROOT/build" -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON \
  -DLAVIK_ENABLE_OPT=OFF -DLAVIK_ENABLE_TEST_FAULTS=ON \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build "$ROOT/build" --target lavik lavik-ctl -j 2

export BENCH=$ROOT/src/perf_reports/meta-purpose-views-2026-10-07/bench
python3 "$BENCH/build.py" --source "$ROOT/baseline" \
  --build "$ROOT/release-before" --output "$ROOT/bench-before" --legacy
python3 "$BENCH/build.py" --source "$ROOT/src" \
  --build "$ROOT/release-after" --output "$ROOT/bench-after"
# Run only after all builds/tests have ended. A new measurements directory is required.
taskset -c 0-7 python3 "$BENCH/run.py" --root "$ROOT" --rounds 3
python3 "$BENCH/summarize.py" "$ROOT/measurements"
```

For this acceptance run, the final process invocation used `--process-only`
after preserving interrupted attempts in separate directories. That mode
requires the original executable hashes and leaves the completed component
trials/metadata intact. A fresh default invocation runs all phases with the
final protocol; no benchmark process is resumed from a partial trial.

The original run used the same read-only dependency checkout at
`/home/ubuntu/workspace/keylane/bycorf` for both builds; its generated artifacts
were in the NVMe build directories. Reproduction can use the initialized,
revision-matched dependency inside the isolated checkout instead.

Functional acceptance commands/results and the complete production call-site
allowlist belong in the issue/PR, not in architecture documentation. The current
architectural model is in
[`08-meta-control-plane.md`](../../docs/architecture/08-meta-control-plane.md).
