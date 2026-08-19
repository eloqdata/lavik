#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cmake -B build -DCMAKE_BUILD_TYPE=Release -DKEYLANE_ENABLE_OPT=ON \
  -DKEYLANE_MARCH=native -DKEYLANE_STATIC_OPENSSL=ON \
  -DBUILD_TESTING=OFF
cmake --build build --target keylane -j"$(nproc)"
echo "Release build complete: build/keylane"
