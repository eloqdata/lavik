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

# Install the pinned integration-test client; this is not a runtime dependency.
set -euo pipefail
if [[ $# != 1 ]]; then
  echo "Usage: $0 destination-directory" >&2
  exit 2
fi
case "$(uname -m)" in
  x86_64)
    arch=amd64
    checksum=c255565a60c15f9d1b4d38ef46d30056161266ffa75e11065c7b1901e236bff7
    ;;
  aarch64|arm64)
    arch=arm64
    checksum=5fc2b7a393f159f8981c1e8d712b63c0866628b99ad030ed3e46c9869c22ff04
    ;;
  *) echo "Unsupported RedisShake test architecture" >&2; exit 1 ;;
esac
archive=$(mktemp)
trap 'rm -f "$archive"' EXIT
curl --fail --location --retry 3 \
  "https://github.com/tair-opensource/RedisShake/releases/download/v4.6.2/redis-shake-v4.6.2-linux-${arch}.tar.gz" \
  --output "$archive"
printf '%s  %s\n' "$checksum" "$archive" | sha256sum --check
mkdir -p "$1"
tar -xzf "$archive" -C "$1" ./redis-shake
