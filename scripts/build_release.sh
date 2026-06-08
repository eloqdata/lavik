#!/bin/bash
set -e
cd "$(dirname "$0")/.."
cmake -B build -DCMAKE_BUILD_TYPE=Release -DKEYLANE_ENABLE_OPT=ON
cmake --build build -j$(nproc)
echo "Release build complete: build/keylane"
