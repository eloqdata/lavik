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
cmake -S . -B build_debug -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DLAVIK_ENABLE_OPT=OFF \
  -DLAVIK_KERNEL_BYPASS=OFF \
  -DBUILD_TESTING=ON \
  "$@"
echo "Debug build configured in build_debug"
