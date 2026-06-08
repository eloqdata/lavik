#!/bin/bash
set -e
cd "$(dirname "$0")/.."
cmake -B build_debug -DCMAKE_BUILD_TYPE=Debug -DKEYLANE_ENABLE_OPT=OFF
cmake --build build_debug -j$(nproc)
echo "Debug build complete: build_debug/keylane"
