# Reproducing the native FULL measurements

The submitted numbers come from the historical artifacts listed in `evidence/retained-artifacts.json`. This document describes a new run; it does not promise identical timings on another machine. Use an isolated eight-CPU Linux host with local NVMe, Python 3.11+ (`hashlib.file_digest`), `ip`, `tc`, `taskset`, `tcpdump`, `flock`, `setpriv`, and passwordless sudo for task-owned network namespaces/veth devices. The runner does not change a host-wide qdisc.

## Build and binary provenance

Check out the exact source revisions in `evidence/cohort.json` and the records report. Build each with the corresponding `evidence/build/*/CMakeCache.txt`: GCC 13.3.0, Release, `LAVIK_ENABLE_OPT=ON`, `LAVIK_MARCH=native`, io_uring (`LAVIK_KERNEL_BYPASS=OFF`), and `LAVIK_ENABLE_TEST_FAULTS=OFF`; Bycorf is pinned at `62509c93d40c2480f5046b71454db6cf95801b04`. Preserve the resulting source SHA, binary SHA 256, cache, configure output and build log. The original build provenance connects each immutable copied binary to its source revision; original machine-specific binary hashes need not match a rebuild on a different compiler/host.

Create a local copy of `tools/benchmark-variants.json` whose `binary` fields point to those immutable executables, retaining their exact `revision` fields. The three labels are `main`, `chunks`, and `records`. The second comparison is chunks→records, the marginal effect of the stacked second PR. It is not an isolated records-only branch or a measured main→records composite.

## Run one workload at a time

The portable scripts in `tools/` add Apache headers and change only default/local path resolution from the preserved `evidence/historical-tools/` versions. They were not used for the original measurements. Always pass the current repository's runner explicitly. Replace all placeholder paths below with absolute paths. Never run builds, tests, another case, or offline pcap analysis concurrently with a measurement.

```sh
TASK=/absolute/path/to/new-task
REPO=/absolute/path/to/lavik
TOOLS=/absolute/path/to/this-report/tools
mkdir -p "$TASK"
flock -n "$TASK/benchmark.lane.lock" python3 "$TOOLS/run-benchmark-matrix.py"   --variants "$TASK/variants.json" --recipe "$TOOLS/benchmark-accepted-primary-recipe.json"   --runner "$REPO/scripts/bench_native_full_sync.py"   --output "$TASK/measurements" --execute
flock -n "$TASK/benchmark.lane.lock" python3 "$TOOLS/run-benchmark-matrix.py" \
  --variants "$TASK/variants.json" --recipe "$TOOLS/benchmark-capture-repeat-recipe.json" \
  --runner "$REPO/scripts/bench_native_full_sync.py" --output "$TASK/measurements" --execute
flock -n "$TASK/benchmark.lane.lock" python3 "$TOOLS/run-benchmark-matrix.py" \
  --variants "$TASK/variants.json" --recipe "$TOOLS/benchmark-live-headroom-recipe.json" \
  --runner "$REPO/scripts/bench_native_full_sync.py" --output "$TASK/measurements" --execute
flock -n "$TASK/benchmark.lane.lock" python3 "$TOOLS/run-benchmark-matrix.py" \
  --variants "$TASK/variants.json" --recipe "$TOOLS/benchmark-controls-recipe.json"   --runner "$REPO/scripts/bench_native_full_sync.py"   --output "$TASK/measurements" --execute
```

Without `--execute`, the driver prints commands only. Each case uses three paired repetitions with order A/B, B/A, A/B. Existing run directories are rejected rather than overwritten. A runner failure or acceptance rejection stops the batch; retain the failed run, diagnose it, and document any separate replacement cohort. The original measurements ran the four focused 5 ms cases first, then the remaining primary cases. A baseline live 20 ms stall and a later capture-drop failure stopped their batches. The separate capture-repeat and 32 MiB live-headroom cohorts were declared after those diagnoses; controls ran last. Exact execution timestamps and argv are in `evidence/raw-results.jsonl`.

The accepted-primary recipe has 19 cases (114 runs). Run the six-run `benchmark-capture-repeat-recipe.json` and twelve-run `benchmark-live-headroom-recipe.json` separately with the same driver/runner/output arguments, followed by the five-case control recipe (30 runs). Together these define 27 complete comparisons and 162 comparable accepted runs. The two originally accepted partial runs and two rejected attempts remain evidence, making 164 accepted runs out of 166 attempts. The original 21-case primary recipe is preserved only as historical scope; it is not the default accepted-run recipe. The fifth control (four-flow large-string at 5 ms) was declared before any controls ran, after the original captured cohort showed a regression. It preserves rather than replaces those results. The recipes include sparse/uniform ordinary values and dense values, large strings and hashes, one and four source flows, added RTT 0/1/5/20 ms where listed, and controlled live writers. Four-source-flow cases use two target workers to keep source `0-3`, target `4-5`, client `6-7` disjoint. Single-flow cases use source `0`, target `1`, client `6-7`. Preserve the specified worker counts when reproducing; four flows do not mean four target workers.

## Analyze after all measurement processes exit

For each accepted captured run, extract byte-observation spans with `python3 "$TOOLS/analyze-full-pcap.py" RUN/frames.pcap --output RUN/frame-spans.json`. The analyzer rejects incomplete or ambiguous captures. Then run:

```sh
python3 "$TOOLS/aggregate-benchmarks.py" --root "$TASK/measurements"   --recipe "$TOOLS/benchmark-combined-recipe.json" --output "$TASK/aggregates"
```

`benchmark-combined-recipe.json` is analysis-only: it joins original primary, replacement, headroom, and uncaptured cases and includes their expected capture settings. Do not execute it as a workload recipe. The public tables use all three accepted values and ratios of medians; paired ratios are also retained in JSON. Keep primary and uncaptured cohorts separate, as they ran at different times.

## Acceptance and retained evidence

Every included run must have one selected=FULL source session, every expected source flow exactly once, final ONLINE with all flows, matching complete source/target digests, visible post-FULL fence writes for every source flow, no writer error, and no cleanup error. Captured runs additionally require all expected flows, one CUT each, no TCP gap/unparsed tail, and zero kernel drops. Retry-then-success is rejected for performance comparisons.

The runner deletes only its successful task-owned source/target `data` files after all processes have exited and verification has passed. Source/target logs, result/acceptance/outcome, samples, pcap and capture logs remain. Failure data is retained. Full pcap and raw sampling files from this measurement stay under the original `/mnt/local_nvme/i131/measurements` paths; reviewable result/acceptance/outcome and derived per-run metrics are bundled here. The original exact tools expect `/mnt/local_nvme/i131`, a `bench-dev` checkout, immutable `bin/{main,chunks,records}` binaries, and those task-local recipe paths.

## Deliberately excluded original cases

The original `large-live-128m-f1-rtt20/r1-main` stopped making native progress with a 16 MiB publisher queue per worker and was explicitly interrupted after more than five minutes. This was not the configured 900 s FULL timeout. Its writer had a 30.052 s socket timeout, and no final digest was obtained. The original candidate and remaining repetitions were not executed. See [the source-supported gate/admission diagnosis and evidence](BASELINE-STALL.md). The new live-headroom cohort uses 32 MiB publisher queues per worker for both variants at both 5/20 ms; do not pool it with the original 16 MiB live 5 results, and do not claim the window optimization fixes the baseline stall.

The original `dense-records-128m-f4-rtt20` cohort had two accepted r 1 runs, then one packet dropped by the kernel capture buffer in r 2-records. Native transport was ONLINE with four flows, but capture validation ran before the digest, so that run has no verified dataset result. Its complete replacement cohort is `dense-records-128m-f4-rtt20-capture-repeat`, with the same 16 MiB tcpdump buffer and all other settings unchanged. The original two accepted values are retained as partial observations and are not mixed into the replacement median.

For an intentional reproduction of the baseline stall, select only the original case with the historical primary recipe:

```sh
python3 "$TOOLS/run-benchmark-matrix.py" --variants "$TASK/variants.json" \
  --recipe "$TOOLS/benchmark-recipe.json" --only '^large-live-128m-f1-rtt20$' \
  --runner "$REPO/scripts/bench_native_full_sync.py" --output "$TASK/stall-diagnostic" --execute
```

This diagnostic may stop making progress and wait for the 900-second FULL timeout; an explicit intervention must be labeled as an abort with its actual timestamp, not as a measured FULL duration. Do not run it alongside another measurement. Preserve the resulting failed data, pcap and logs. The accepted-run commands above deliberately omit it.

## Optional plotting

The SVG figures were generated after every benchmark had finished, with `/mnt/local_nvme/i131/plot-env/bin/python` in a task-local virtual environment. Matplotlib and its exact installed dependencies are listed in `evidence/plot-dependencies.txt`. `evidence/historical-tools/plot-benchmark-results.py` reads the final aggregate summary and draws the median and min–max of all three accepted repetitions; its original absolute task path is preserved. For a new location, adjust only its `ROOT` path to the directory containing `aggregates-final` and the two report directories. No plotting installation or rendering ran during workload measurement.
