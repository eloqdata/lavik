#!/usr/bin/env bash
# Copyright (C) 2026 EloqData Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

# Run the complete software suite, or one shard on an isolated CI runner.
# Build every target first; fixtures start child servers from this build tree.
cd "$(dirname "${BASH_SOURCE[0]}")/.."
build_dir=build_ci
if [[ $# -gt 0 && "$1" != --* ]]; then
  build_dir=$1
  shift
fi
shard_index=0
shard_count=1
plan_only=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --shard-index|--shard-count)
      if [[ $# -lt 2 || ! "$2" =~ ^[0-9]+$ ]]; then
        echo "$1 requires a nonnegative integer" >&2
        exit 2
      fi
      if [[ "$1" == --shard-index ]]; then
        shard_index=$((10#$2))
      else
        shard_count=$((10#$2))
      fi
      shift 2
      ;;
    --plan-only)
      plan_only=1
      shift
      ;;
    *)
      echo "Usage: $0 [build-dir] [--shard-index N --shard-count N] [--plan-only]" >&2
      exit 2
      ;;
  esac
done
if (( shard_count < 1 || shard_index >= shard_count )); then
  echo "Shard index must be in [0, shard count), and shard count must be positive" >&2
  exit 2
fi
build_dir=$(realpath -- "$build_dir")
results_dir="$build_dir/test-results"
if (( shard_count > 1 )); then
  results_dir="$results_dir/shard-$shard_index-of-$shard_count"
fi
mkdir -p "$results_dir"

python3 scripts/ci_test_plan.py --build-dir "$build_dir" \
  --shard-index "$shard_index" --shard-count "$shard_count" --output-dir "$results_dir"
if (( plan_only )); then
  exit 0
fi

export LAVIK_TEST_DATA_DIR=${LAVIK_TEST_DATA_DIR:-/mnt/dev}
if [[ ! -d "$LAVIK_TEST_DATA_DIR" || ! -w "$LAVIK_TEST_DATA_DIR" || ! -x "$LAVIK_TEST_DATA_DIR" ]]; then
  echo "Tests require a writable and searchable LAVIK_TEST_DATA_DIR for private scratch files." >&2
  exit 1
fi

# The Sentinel discovery gate (meta_integration.sentinel_discovery) is only
# meaningful with the pinned redis-py module. CI bundles it hash-pinned under
# <build>/test_tools/redis_py and exposes it through PYTHONPATH. Require it so a
# provisioning regression cannot turn acceptance coverage into a silent skip. Outside
# CI the gate self-probes <build>/test_tools/redis_py and skips when redis-py
# is absent.
if [[ -n "${RUNNER_TEMP:-}" ]]; then
  export LAVIK_REQUIRE_REDIS_PY=1
fi

status=0
run_suite() {
  local name=$1
  shift
  # Preserve failures while still exercising later suites. pipefail also makes
  # a failed test or log write fail the job instead of inheriting tee's success.
  if "$@" 2>&1 | tee "$results_dir/$name.log"; then
    echo "PASS: $name"
  else
    echo "FAIL: $name" >&2
    status=1
  fi
}

# Shards belong on different runners: fixed ports and private disk images still
# require serial scheduling inside each shard. The index file selects exact
# discovered tests, including names containing regular-expression characters.
if [[ -s "$results_dir/ctest-indices.txt" ]]; then
  run_suite ctest ctest --test-dir "$build_dir" --parallel 1 \
    --tests-information "$results_dir/ctest-indices.txt" \
    --timeout 300 --no-tests=error --output-on-failure \
    --output-junit "$results_dir/ctest.xml"
fi

# Escalate after 30 seconds if a timed-out process cannot finish shutdown.
while IFS= read -r suite; do
  case "$suite" in
    large-native-list)
      run_suite "$suite" timeout --kill-after=30s 45m \
        "$build_dir/lavik_replica_abort_reclaim_e2e_test" --large-list
      ;;
    large-native-hash)
      run_suite "$suite" timeout --kill-after=30s 45m \
        "$build_dir/lavik_replica_abort_reclaim_e2e_test" --large-hash
      ;;
    large-rdb)
      run_suite "$suite" timeout --kill-after=30s 45m env LAVIK_RUN_LARGE_RDB=1 \
        "$build_dir/lavik_grouped_ordered_write_e2e_test" "$build_dir/lavik" \
        --gtest_filter=GroupedRdbStreamE2e.LargeListOverOneGiBImportsAndExportsWithoutAggregate \
        --gtest_output="xml:$results_dir/large-rdb.xml"
      ;;
    valkey-tcl)
      # These compatibility suites are not registered with CTest.
      run_suite "$suite" timeout --kill-after=30s 45m env LAVIK_BIN="$build_dir/lavik" \
        tests/valkey/run-lavik
      ;;
    *)
      echo "Unknown external suite in CI plan: $suite" >&2
      status=1
      ;;
  esac
done < "$results_dir/external-suites.txt"

exit "$status"
