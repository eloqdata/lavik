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

cd "$(dirname "$0")/.."

build_dir=${LAVIK_ASAN_BUILD_DIR:-build_asan}
clang_c=${LAVIK_ASAN_CC:-clang-18}
clang_cxx=${LAVIK_ASAN_CXX:-clang++-18}

command -v "${clang_c}" >/dev/null || {
  echo "ASan C compiler not found: ${clang_c}" >&2
  exit 1
}
command -v "${clang_cxx}" >/dev/null || {
  echo "ASan C++ compiler not found: ${clang_cxx}" >&2
  exit 1
}

cmake -S . -B "${build_dir}" \
  -DCMAKE_C_COMPILER="${clang_c}" \
  -DCMAKE_CXX_COMPILER="${clang_cxx}" \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_CXX_FLAGS="-fsanitize=address -fno-omit-frame-pointer" \
  -DCMAKE_CXX_FLAGS_RELWITHDEBINFO="-O1 -g" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address" \
  -DLAVIK_ENABLE_OPT=OFF \
  -DLAVIK_ENABLE_TEST_FAULTS=ON \
  -DBUILD_TESTING=ON
cmake --build "${build_dir}" -j"$(nproc)"

echo "Clang ASan build complete: ${build_dir}/lavik, ${build_dir}/lavik-meta, ${build_dir}/lavik-ctl"
