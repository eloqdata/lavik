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

# Install the pinned standard redis-py acceptance client used by the Sentinel
# and Cluster client compatibility gates. The wheel hash in
# requirements-redis-py.txt is enforced by pip itself.
set -euo pipefail
if [[ $# != 1 ]]; then
  echo "Usage: $0 destination-directory" >&2
  exit 2
fi
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
mkdir -p "$1"
python3 -m pip install \
  --disable-pip-version-check --no-input --only-binary :all: \
  --require-hashes \
  -r "$script_dir/../tests/meta_integration/requirements-redis-py.txt" \
  --target "$1"
