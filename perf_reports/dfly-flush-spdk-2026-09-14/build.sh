#!/usr/bin/env bash
set -euo pipefail
# Run in the pinned source checkout; benchmark only after this process exits.
test "$#" -eq 1
git rev-parse HEAD
git submodule status --recursive
cmake -S . -B "$1" -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18 \
  -DKEYLANE_ENABLE_OPT=ON -DKEYLANE_MARCH=native \
  -DKEYLANE_STATIC_OPENSSL=ON -DKEYLANE_STATIC_CXX_RUNTIME=ON \
  -DKEYLANE_WITH_SPDK=ON -DKEYLANE_BUILD_FAULT_SERVER=OFF \
  -DBUILD_TESTING=OFF
cmake --build "$1" --target keylane -j12
sha256sum "$1/keylane"
