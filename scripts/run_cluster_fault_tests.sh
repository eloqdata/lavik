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

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
build_dir=${LAVIK_CLUSTER_BUILD_DIR:-$repo_root/build_cluster_fault}
tier=model
duration=60
require_hardware=0

usage() {
  echo "usage: $0 [--tier model|integration|soak|hardware] [--build-dir DIR]" \
       "[--duration SECONDS] [--require-hardware]"
}

while (($#)); do
  case "$1" in
    --tier)
      (($# >= 2)) || { usage >&2; exit 2; }
      tier=$2
      shift 2
      ;;
    --build-dir)
      (($# >= 2)) || { usage >&2; exit 2; }
      build_dir=$2
      shift 2
      ;;
    --duration)
      (($# >= 2)) || { usage >&2; exit 2; }
      duration=$2
      shift 2
      ;;
    --require-hardware)
      require_hardware=1
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
done

case "$tier" in
  model|integration|soak|hardware) ;;
  *)
    echo "unknown cluster fault tier: $tier" >&2
    exit 2
    ;;
esac
if [[ ! $duration =~ ^[1-9][0-9]*$ ]]; then
  echo "--duration must be a positive integer" >&2
  exit 2
fi
if [[ ! -f $build_dir/CMakeCache.txt ]]; then
  echo "missing configured build tree: $build_dir" >&2
  echo "configure with BUILD_TESTING=ON and LAVIK_ENABLE_TEST_FAULTS=ON" >&2
  exit 2
fi
if ! grep -qx 'BUILD_TESTING:BOOL=ON' "$build_dir/CMakeCache.txt" ||
   ! grep -qx 'LAVIK_ENABLE_TEST_FAULTS:BOOL=ON' \
      "$build_dir/CMakeCache.txt"; then
  echo "cluster fault tiers require BUILD_TESTING=ON and" \
       "LAVIK_ENABLE_TEST_FAULTS=ON" >&2
  exit 2
fi

jobs=${LAVIK_CLUSTER_BUILD_JOBS:-$(nproc)}
case "$tier" in
  model)
    cmake --build "$build_dir" --target lavik_cluster_model_tests \
      lavik_cluster_fault -j"$jobs"
    label=cluster-model
    timeout_seconds=60
    ;;
  integration)
    targets=(lavik lavik_process_support_tests)
    if ctest --test-dir "$build_dir" -N | grep -q lavik_sentinel_e2e; then
      targets+=(lavik_sentinel_e2e_test)
    fi
    cmake --build "$build_dir" --target "${targets[@]}" -j"$jobs"
    label=cluster-integration
    timeout_seconds=900
    ;;
  soak)
    cmake --build "$build_dir" --target lavik_cluster_fault -j"$jobs"
    exec timeout "$((duration + 30))" \
      "$build_dir/lavik_cluster_fault" --soak-seconds "$duration" \
      --seed "${LAVIK_CLUSTER_SEED:-1}" --trace-out \
      "$build_dir/cluster-fault-artifacts"
    ;;
  hardware)
    cmake --build "$build_dir" --target lavik -j"$jobs"
    label=cluster-hardware
    timeout_seconds=120
    ;;
esac

test_count=$(ctest --test-dir "$build_dir" -N -L "$label" |
  sed -n 's/^Total Tests: //p')
if [[ -z $test_count || $test_count == 0 ]]; then
  echo "no CTest tests registered for label $label" >&2
  exit 1
fi

if [[ $tier == hardware && $require_hardware == 1 ]]; then
  output=$(mktemp)
  trap 'rm -f "$output"' EXIT
  set +e
  timeout "$timeout_seconds" ctest --test-dir "$build_dir" -L "$label" \
    --output-on-failure --no-tests=error 2>&1 | tee "$output"
  status=${PIPESTATUS[0]}
  set -e
  if ((status != 0)); then
    exit "$status"
  fi
  if grep -q '\*\*\*Skipped' "$output"; then
    echo "hardware tests were required but skipped" >&2
    exit 1
  fi
  exit 0
fi

exec timeout "$timeout_seconds" ctest --test-dir "$build_dir" -L "$label" \
  --output-on-failure --no-tests=error
