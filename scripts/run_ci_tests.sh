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

# Run the same complete software suite locally and on each CI architecture.
# Build every target first; fixtures start child servers from this build tree.
cd "$(dirname "${BASH_SOURCE[0]}")/.."
build_dir=$(realpath -- "${1:-build_ci}")
results_dir="$build_dir/test-results"
mkdir -p "$results_dir"

export LAVIK_TEST_DATA_DIR=${LAVIK_TEST_DATA_DIR:-/mnt/dev}
if [[ ! -d "$LAVIK_TEST_DATA_DIR" || ! -w "$LAVIK_TEST_DATA_DIR" || ! -x "$LAVIK_TEST_DATA_DIR" ]]; then
  echo "Tests require a writable and searchable LAVIK_TEST_DATA_DIR for private scratch files." >&2
  exit 1
fi

# The Sentinel discovery gate (meta_integration.sentinel_discovery) is only
# meaningful with the pinned redis-py module. CI installs it hash-pinned into
# the test venv (ci.yml "Install build and test dependencies"), whose python
# then runs the gates; make the gate mandatory there so a provisioning
# regression cannot turn the acceptance coverage into a silent skip. Outside
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

# Process fixtures use fixed ports and large private disk images. Serial CTest
# scheduling avoids contention; the tested servers still use multiple workers.
run_suite ctest ctest --test-dir "$build_dir" --parallel 1 \
  --timeout 300 --no-tests=error --output-on-failure \
  --output-junit "$results_dir/ctest.xml"

# Escalate after 30 seconds if a timed-out process cannot finish shutdown.
run_suite large-native-list timeout --kill-after=30s 45m \
  "$build_dir/lavik_replica_abort_reclaim_e2e_test" --large-list
run_suite large-native-hash timeout --kill-after=30s 45m \
  "$build_dir/lavik_replica_abort_reclaim_e2e_test" --large-hash
run_suite large-rdb timeout --kill-after=30s 45m env LAVIK_RUN_LARGE_RDB=1 \
  "$build_dir/lavik_grouped_ordered_write_e2e_test" "$build_dir/lavik" \
  --gtest_filter=GroupedRdbStreamE2e.LargeListOverOneGiBImportsAndExportsWithoutAggregate \
  --gtest_output="xml:$results_dir/large-rdb.xml"

# The vendored compatibility suites are a separate CMake target, not CTest cases.
run_suite valkey-tcl timeout --kill-after=30s 45m env LAVIK_BIN="$build_dir/lavik" \
  tests/valkey/run-lavik

exit "$status"
