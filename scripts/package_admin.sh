#!/usr/bin/env bash
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
# Stage only the Admin runtime inputs, never local workspace state or SSH keys.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
stage=${1:?usage: package_admin.sh PACKAGE_DIRECTORY}
case "${LAVIK_PACKAGE_ARCH:-$(uname -m)}" in
  x86_64|amd64) node_arch=x64; checksum=472655581fb851559730c48763e0c9d3bc25975c59d518003fc0849d3e4ba0f6 ;;
  aarch64|arm64) node_arch=arm64; checksum=f3d5a797b5d210ce8e2cb265544c8e482eaedcb8aa409a8b46da7e8595d0dda0 ;;
  *) echo 'Unsupported Admin runtime architecture' >&2; exit 1 ;;
esac
mkdir -p "$stage/admin/public" "$stage/runtime/bin"
for module in askpass cli deploy fleet hosts meta releases resp server ssh store; do
  install -m 0644 "$repo/admin/$module.mjs" "$stage/admin/"
done
install -m 0644 "$repo/admin/remote.py" "$stage/admin/"
install -m 0644 "$repo"/admin/public/* "$stage/admin/public/"
install -m 0755 "$repo/admin/lavik-admin" "$stage/lavik-admin"
install -m 0644 "$repo/admin/RELEASE-README.md" "$stage/LAVIK-ADMIN.md"
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT
archive="node-v24.15.0-linux-$node_arch.tar.xz"
curl --fail --location --retry 3 --connect-timeout 30 --max-time 300 \
  "https://nodejs.org/dist/v24.15.0/$archive" -o "$temporary/$archive"
(cd "$temporary" && printf '%s  %s\n' "$checksum" "$archive" | sha256sum --check -)
tar -xJf "$temporary/$archive" -C "$temporary"
install -m 0755 "$temporary/node-v24.15.0-linux-$node_arch/bin/node" "$stage/runtime/bin/node"
# Node's upstream LICENSE includes notices for its bundled third-party code.
install -m 0644 "$temporary/node-v24.15.0-linux-$node_arch/LICENSE" "$stage/runtime/LICENSE"
printf '\n\nNode.js v24.15.0 and bundled dependencies\n\n' >> "$stage/THIRD_PARTY_NOTICES"
cat "$stage/runtime/LICENSE" >> "$stage/THIRD_PARTY_NOTICES"
"$stage/runtime/bin/node" --version
"$stage/lavik-admin" --help >/dev/null
