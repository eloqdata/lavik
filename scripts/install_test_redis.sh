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

# ScanReader forwards Lavik DUMP payloads (RDB 11), requiring Redis >= 7.2.
# Pin the real integration peer consistently on both CI architectures.
set -euo pipefail
if [[ $# != 1 ]]; then
  echo "Usage: $0 destination-directory" >&2
  exit 2
fi
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
curl --fail --location --retry 3 \
  https://download.redis.io/releases/redis-7.2.14.tar.gz \
  --output "$work/redis.tar.gz"
# Published by redis/redis-hashes for the upstream release archive.
printf '%s  %s\n' \
  21326da3f66c0aead4c8204c0ac52ff905337a77cadd169f75ac22835ea30025 \
  "$work/redis.tar.gz" | sha256sum --check
tar -xzf "$work/redis.tar.gz" -C "$work"
make -C "$work/redis-7.2.14" -j"${LAVIK_TEST_BUILD_JOBS:-2}" \
  MALLOC=libc BUILD_TLS=yes REDIS_CFLAGS= REDIS_LDFLAGS= redis-server redis-cli
mkdir -p "$1"
install -m 755 "$work/redis-7.2.14/src/redis-server" \
  "$work/redis-7.2.14/src/redis-cli" "$1/"
