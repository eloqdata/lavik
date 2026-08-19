#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cmake -B build_debug -DCMAKE_BUILD_TYPE=Debug -DKEYLANE_ENABLE_OPT=OFF \
  -DKEYLANE_STATIC_OPENSSL=ON \
  -DBUILD_TESTING=ON
cmake --build build_debug -j"$(nproc)"
echo "Debug build complete: build_debug/keylane"
