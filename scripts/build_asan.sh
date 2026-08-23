#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

build_dir=${KEYLANE_ASAN_BUILD_DIR:-build_asan}
clang_c=${KEYLANE_ASAN_CC:-clang-18}
clang_cxx=${KEYLANE_ASAN_CXX:-clang++-18}

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
  -DKEYLANE_ENABLE_OPT=OFF \
  -DKEYLANE_STATIC_OPENSSL=ON \
  -DBUILD_TESTING=ON
cmake --build "${build_dir}" -j"$(nproc)"

echo "Clang ASan build complete: ${build_dir}/keylane"
