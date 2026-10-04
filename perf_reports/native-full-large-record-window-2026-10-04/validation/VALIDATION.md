# Native FULL bounded-window correctness validation

This compact bundle records local Debug correctness validation on 2026-10-04. It is independent of the performance measurements. The tested implementation sources were subsequently committed as chunks `51d03f97fede341fb42c14bd46a9f1aaf416eeb1` and ordinary snapshots `9da9c797`, stacked on chunks. The separate existing-fixture correction is `7f2e4098`. Final process runs were sequential, without a concurrent heavy build.

## Final results

| Scope | Result | Time | Evidence |
| --- | --- | --- | --- |
| Chunks focused mode | 6 process scenarios passed | approximately 171 s | [excerpt](focused-chunks.txt) |
| Ordinary snapshots focused CTest | 1/1 CTest; 3 process scenarios passed | 102.62 s | [excerpt](focused-snapshots.txt) |
| Chunks final regression | 53/53 CTests passed | 750.36 s | [summary](regression-chunks-fixed-final.log) |
| Snapshots final regression | 54/54 CTests passed | 752.45 s | [summary](regression-snapshots-final.log) |

Both final regressions include 38 cluster-model cases, record-window/handoff unit cases (9 chunks / 10 snapshots), the benchmark evidence-validator test, native replication, managed Single, population fail-closed, replication-manager integration, and replication-log E2E. Native gate durations were 494.72 s / 492.95 s; managed Single durations were 210.20 s / 214.01 s. `benchmark.native_full_evidence` validates evidence tooling, not replication speed.

The focused chunks mode was invoked directly, not through a separately recorded CTest run. The focused snapshots test was invoked through its registered 300-second CTest. Focused cases remain separate from the original default native gate and its 600-second timeout.

## Commands actually run

Task environment was initialized with:

```bash
source /mnt/local_nvme/i131/env.sh
```

This places temporary data, caches, and logs under `/mnt/local_nvme/i131`; `LAVIK_TEST_DATA_DIR=/mnt/local_nvme/i131/data`. Binaries were Debug builds, with `/usr/bin/redis-cli` as the client.

Chunks focused mode:

```bash
python3 /mnt/local_nvme/i131/chunks/tests/meta_integration/gate_native_replication.py \
  /mnt/local_nvme/i131/build-chunks-debug/lavik-meta \
  /mnt/local_nvme/i131/build-chunks-debug/lavik \
  /mnt/local_nvme/i131/build-chunks-debug/lavik-ctl \
  /usr/bin/redis-cli record_window
```

Snapshots focused CTest:

```bash
ctest --test-dir /mnt/local_nvme/i131/build-snapshots-debug \
  -R '^meta_integration.gate_full_snapshot_window$' --output-on-failure
```

Final chunks regression:

```bash
ctest --test-dir /mnt/local_nvme/i131/build-chunks-debug \
  -R 'FullSync(RecordWindow|HandoffProgress)Test|cluster_model|lavik_replication_log_e2e|cluster_integration\.(population_fail_closed|replication_manager_control_api)|benchmark\.native_full_evidence|meta_integration\.(gate_native_replication|gate_managed_single)$' \
  --output-on-failure --stop-on-failure
```

Final snapshots regression:

```bash
ctest --test-dir /mnt/local_nvme/i131/build-snapshots-debug \
  -R 'FullSync(RecordWindow|HandoffProgress)Test|cluster_model|lavik_replication_log_e2e|cluster_integration\.(population_fail_closed|replication_manager_control_api)|benchmark\.native_full_evidence|meta_integration\.(gate_native_replication|gate_managed_single)$' \
  --output-on-failure --stop-on-failure
```

## What the focused scenarios establish

**Large values:** Actual Meta-authorized source/target processes transfer a value larger than the 16 MiB window. The frame-aware proxy holds selected record ACKs while continuing to forward data. Separate success cases cover baseline, captured replacement, and publish-record FIFO origins. Failure cases disconnect with frames outstanding, inject a validly encoded ACK with the wrong partition, or duplicate a correct ACK. Assertions observe eight frames before ACK, bound bytes/frames, release one chunk ACK to admit exactly one additional frame, and show that releasing a tiny begin ACK does not bypass byte credit. Success verifies the full value and ONLINE tail replication; failure verifies LOADING and teardown.

**Ordinary snapshots:** Each case seeds 20,481 one-KiB values in one partition in DB0 and DB15. Normal operation observes eight frames and exact one-ACK progress, while DB15 sends no watched frame before DB0 drains. Same-key updates, deletion, exact TTL, complete batched reads/counts, and ONLINE tail replication verify source coverage lifetime. Disconnect keeps both DBs unavailable. The Debug `LAVIK_FULL_SNAPSHOT_RECEIPT_OOM` fault rejects per-frame receipt metadata admission and proves synchronous fallback: one held frame at a time, followed by successful completion. The fixed receipt-array admission is not bypassed by this fault.

The ordinary window spans consecutive scan batches within a partition/DB. It drains at each DB completion, inside the partition loop. Sparse populations with only one frame per partition+DB cannot gain consecutive-batch overlap from this change.

## Earlier failures and investigation

The [investigation excerpts](failure-investigation.txt) preserve key result lines. Original logs are identified by exact paths and hashes in [ORIGINAL_LOGS.tsv](ORIGINAL_LOGS.tsv).

1. **Manager deadline case:** The initial targeted run passed 11/12 CTests; the manager executable passed 55/56 internal cases. `CandidateRecoveryDeadlineFreezesActualCompleteApplied` reached its existing 300 ms deadline with complete cut `1,1` rather than expected `2,1`. This was investigated as timing sensitivity, not counted as a pass. Neither the production deadline nor test expectation changed. The exact isolated command below passed all 10 repeats, then a full manager CTest passed in 38.27 s (38.29 s total), followed by complete manager passes in both final regressions. This does not prove the timing sensitivity cannot recur.

```bash
/mnt/local_nvme/i131/build-chunks-debug/lavik_cluster_replication_manager_integration_test \
  --gtest_filter=ReplicationManagerIntegrationTest.CandidateRecoveryDeadlineFreezesActualCompleteApplied \
  --gtest_repeat=10 \
  > /mnt/local_nvme/i131/logs/test-recovery-deadline-repeat.log 2>&1
```

2. **Existing native lease fixture:** The initial consolidated run stopped after 48 passing CTests at `explicit_full_limit`. Its empty-source leader-replacement case aborted with `cluster source admission is suspended until lease renewal`; record transmission was not exercised. Fresh isolation passed once, then reproduced the failure. Elections configured at 700–1400 ms plus a 1400 ms election-derived lease quarantine could exceed the separate three-second admission retry budget. Independent test-only commit `7f2e4098` uses 300–600 ms elections and waits for seven busy attempts, spanning at least 3.5 seconds. Production retry policy is unchanged. Three fresh `explicit_full_limit` invocations, each executing leader replacement and cancellation, passed in 18.471, 17.413, and 17.281 s; both complete native gates subsequently passed. The isolation retained private directories `explicit-isolated-fixed-{1,2,3}` under the task data directory.

3. **Development focused runs:** The first chunks focused run exposed a skipped FULL sequence with conditional `co_await`; the sender was changed to explicit branches. A subsequent run reached the override case but exhausted the fixture's small data file. The focused fixture now provisions an additional 512 MiB source data file. The successful six-case run followed both corrections. These initial failures remain indexed as `new-chunk-windows-1.log` and `new-chunk-windows-2.log`; only the final successful run supports the focused pass claim.

## Scope and checks not claimed

- No complete repository-wide CTest run, sanitizer run, mixed-version peer process test, or remote CI result is claimed by this bundle.
- The six-case large-record focused mode was not separately rerun against the snapshots binary. Snapshots ran its own three-case focused gate and all 54 consolidated tests, including existing native/Single large-value coverage.
- Wire tests establish bounded overlap and correctness, not throughput, RTT scaling, or a whole-process memory cap. Release benchmarks have a separate evidence owner and report; no performance result is included here.
- Source identity reflects the unchanged implementation source later committed at the cited revisions. These logs do not contain binary SHA256 fingerprints or establish a new executable build at commit time.

## Archive and integrity

The two passing CTest summaries are copied byte-for-byte. Focused/investigation excerpts include original line numbers; explanatory text is labeled separately. Full process-detail logs are retained outside this compact bundle rather than copied into the repository.

[ORIGINAL_LOGS.tsv](ORIGINAL_LOGS.tsv) lists the original absolute path, byte size, and SHA256 of every cited source log. [SHA256SUMS](SHA256SUMS) hashes the files in this bundle, excluding itself. Original task paths are provenance locations on the validation host, not public download links. The shared correctness bundle belongs at `perf_reports/native-full-large-record-window-2026-10-04/validation/`; both PRs reference this single copy. Performance measurements live in the separate large-record and snapshot-window reports.
