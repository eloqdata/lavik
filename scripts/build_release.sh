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
cmake -B build -DCMAKE_BUILD_TYPE=Release -DLAVIK_ENABLE_OPT=ON \
  -DLAVIK_MARCH=native -DLAVIK_STATIC_OPENSSL=ON \
  -DLAVIK_BUILD_META=ON \
  -DBUILD_TESTING=OFF "$@"
cmake --build build --target lavik lavik-meta lavik-ctl -j"$(nproc)"
echo "Release build complete: build/lavik, build/lavik-meta, build/lavik-ctl"
