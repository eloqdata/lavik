#!/usr/bin/env bash
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

case "${1:?target architecture is required}" in
  amd64) arch=x86_64 ;;
  arm64) arch=aarch64 ;;
  *) echo "Unsupported architecture: $1" >&2; exit 1 ;;
esac
cd /tmp/release
archive="lavik-v0.1.0-beta.1-linux-${arch}-minimal.tar.gz"
curl --fail --location --retry 3 --connect-timeout 30 --max-time 300 \
  "https://github.com/eloqdata/lavik/releases/download/v0.1.0-beta.1/$archive" \
  --output "$archive"
awk -v file="$archive" '$2 == file' release.sha256 | sha256sum --check -
mkdir unpacked
tar -xzf "$archive" --strip-components=1 -C unpacked
test "$(cat unpacked/VERSION)" = v0.1.0-beta.1
test "$(cat unpacked/REVISION)" = 3955b98d43b312324aa8d52775df52cfb111c0d0
mkdir -p /out/bin /out/docs
mv unpacked/lavik unpacked/lavik-meta unpacked/lavik-ctl /out/bin/
# Retain every non-executable release file, including licenses and notices.
cp -a unpacked/. /out/docs/
