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

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
HOST_ARCH=$(uname -m)

case "$HOST_ARCH" in
  x86_64|amd64)
    PACKAGE_ARCH=x86_64
    BYPASS_MARCH=x86-64-v2
    ;;
  aarch64|arm64)
    PACKAGE_ARCH=aarch64
    BYPASS_MARCH=armv8-a+crc
    ;;
  *)
    echo "Unsupported packaging architecture: $HOST_ARCH" >&2
    exit 1
    ;;
esac

KERNEL_BYPASS=${LAVIK_PACKAGE_KERNEL_BYPASS:-OFF}
case "$KERNEL_BYPASS" in
  ON)
    PACKAGE_VARIANT=
    DEFAULT_MARCH=$BYPASS_MARCH
    ;;
  OFF)
    PACKAGE_VARIANT=-minimal
    DEFAULT_MARCH=
    ;;
  *)
    echo "LAVIK_PACKAGE_KERNEL_BYPASS must be ON or OFF" >&2
    exit 1
    ;;
esac
# Minimal passes an explicit empty cache value instead of inheriting CMake's
# native default or a previous build directory's CPU target. Bypass also
# needs the instruction set required by DPDK's inline headers.
PACKAGE_MARCH=${LAVIK_PACKAGE_MARCH-$DEFAULT_MARCH}
BUILD_DIR=${LAVIK_PACKAGE_BUILD_DIR:-$REPO_ROOT/build/package-$PACKAGE_ARCH$PACKAGE_VARIANT}
OUTPUT_DIR=${LAVIK_PACKAGE_OUTPUT_DIR:-$REPO_ROOT/dist}
BUILD_JOBS=${LAVIK_PACKAGE_JOBS:-$(nproc)}
VERSION=$(git -C "$REPO_ROOT" describe --tags --always --dirty --exclude=nightly)
VERSION=${VERSION//\//-}
REVISION=$(git -C "$REPO_ROOT" rev-parse HEAD)
VERSION_SUFFIX=-dev
RELEASE_TAG=${LAVIK_PACKAGE_TAG:-}
if [[ -n "$RELEASE_TAG" ]]; then
  # Tagged packages must identify the release in both filenames and binaries;
  # changing only the archive name would leave --version reporting -dev.
  tag_pattern='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-([0-9A-Za-z-]+\.)*[0-9A-Za-z-]+)?$'
  if [[ ! "$RELEASE_TAG" =~ $tag_pattern ]]; then
    echo "LAVIK_PACKAGE_TAG must be vX.Y.Z or vX.Y.Z-prerelease" >&2
    exit 1
  fi
  TAG_VERSION=${RELEASE_TAG#v}
  BASE_VERSION=${TAG_VERSION%%-*}
  VERSION_SUFFIX=${TAG_VERSION#"$BASE_VERSION"}
  IFS=. read -ra identifiers <<<"${VERSION_SUFFIX#-}"
  for identifier in "${identifiers[@]}"; do
    if [[ "$identifier" =~ ^0[0-9]+$ ]]; then
      echo "Numeric prerelease identifiers must not have leading zeros" >&2
      exit 1
    fi
  done
  PROJECT_VERSION=$(sed -nE 's/^project\(lavik VERSION ([0-9]+\.[0-9]+\.[0-9]+) .*/\1/p' "$REPO_ROOT/CMakeLists.txt")
  if [[ "$BASE_VERSION" != "$PROJECT_VERSION" ]]; then
    echo "Tag version $BASE_VERSION does not match CMake project version $PROJECT_VERSION" >&2
    exit 1
  fi
  if [[ "$(git -C "$REPO_ROOT" rev-parse "refs/tags/$RELEASE_TAG^{commit}")" != "$REVISION" ]]; then
    echo "Release tag must point to the checked-out commit" >&2
    exit 1
  fi
  VERSION=$RELEASE_TAG
fi
# CI keeps stable nightly asset names while VERSION/REVISION identify the
# actual source build inside the archive.
PACKAGE_VERSION=${LAVIK_PACKAGE_VERSION:-$VERSION}
if [[ -n "$RELEASE_TAG" ]]; then
  PACKAGE_VERSION=$RELEASE_TAG
fi
if [[ ! "$PACKAGE_VERSION" =~ ^[A-Za-z0-9._+-]+$ ]]; then
  echo "LAVIK_PACKAGE_VERSION must be a filename-safe version or channel" >&2
  exit 1
fi
PACKAGE_NAME=lavik-$PACKAGE_VERSION-linux-$PACKAGE_ARCH$PACKAGE_VARIANT
STAGE_DIR=$BUILD_DIR/$PACKAGE_NAME
ARCHIVE=$OUTPUT_DIR/$PACKAGE_NAME.tar.gz
APPS=(lavik lavik-meta lavik-ctl)

if [[ "$KERNEL_BYPASS" == ON ]]; then
  # SPDK's makefile otherwise defaults to -march=native independently of
  # Lavik's CMake flags. Its supported override also reaches recursive makes.
  export TARGET_ARCHITECTURE=${PACKAGE_MARCH:-$BYPASS_MARCH}
  SPDK_SOURCE=$REPO_ROOT/bycorf/third_party/spdk
  # SPDK builds in its source tree and does not track changes to CPU flags.
  # Discard earlier objects so a local native build cannot enter this package.
  if [[ -f "$SPDK_SOURCE/mk/config.mk" ]]; then
    make -C "$SPDK_SOURCE" clean
  fi
fi

cmake -S "$REPO_ROOT" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF \
  -DLAVIK_BUILD_FAULT_SERVER=OFF \
  -DLAVIK_BUILD_META=ON \
  -DLAVIK_KERNEL_BYPASS="$KERNEL_BYPASS" \
  -DLAVIK_ENABLE_OPT=ON \
  -DLAVIK_MARCH="$PACKAGE_MARCH" \
  -DLAVIK_STATIC_OPENSSL=ON \
  -DLAVIK_STATIC_CXX_RUNTIME=ON \
  -DLAVIK_VERSION_SUFFIX="$VERSION_SUFFIX"
cmake --build "$BUILD_DIR" --target "${APPS[@]}" -j"$BUILD_JOBS"

cmake -E remove_directory "$STAGE_DIR"
cmake -E make_directory "$STAGE_DIR"
for app in "${APPS[@]}"; do
  BINARY=$BUILD_DIR/$app
  if [[ ! -x "$BINARY" ]]; then
    echo "Release build did not produce $BINARY" >&2
    exit 1
  fi
  DYNAMIC_SECTION=$(readelf -d "$BINARY")
  if grep -Eq 'Shared library: \[(libssl|libcrypto|libstdc\+\+|libgcc_s)\.so' \
      <<<"$DYNAMIC_SECTION"; then
    echo "Packaging refused: $app still links OpenSSL or the C++ runtime dynamically" >&2
    exit 1
  fi
  install -m 0755 "$BINARY" "$STAGE_DIR/$app"
  if command -v strip >/dev/null 2>&1; then
    strip --strip-unneeded "$STAGE_DIR/$app"
  fi
  "$STAGE_DIR/$app" --help >/dev/null 2>&1
done
install -m 0644 "$REPO_ROOT/LICENSE" "$REPO_ROOT/NOTICE" "$STAGE_DIR/"
install -m 0644 "$REPO_ROOT/docs/design-docs/tls-and-auth.md" \
  "$STAGE_DIR/tls-and-auth.md"
OPENSSL_LICENSE=${LAVIK_OPENSSL_LICENSE:-/usr/share/common-licenses/Apache-2.0}
if [[ ! -f "$OPENSSL_LICENSE" ]]; then
  echo "OpenSSL license text not found at $OPENSSL_LICENSE" >&2
  echo "Set LAVIK_OPENSSL_LICENSE to the Apache-2.0 license file." >&2
  exit 1
fi
install -m 0644 "$OPENSSL_LICENSE" "$STAGE_DIR/OPENSSL-LICENSE.txt"
printf '%s\n' "$VERSION" >"$STAGE_DIR/VERSION"
printf '%s\n' "$REVISION" >"$STAGE_DIR/REVISION"

cmake -E make_directory "$OUTPUT_DIR"
tar -C "$BUILD_DIR" -czf "$ARCHIVE" "$PACKAGE_NAME"
# Relative archive names make downloaded checksum files usable as-is.
(cd "$OUTPUT_DIR" && sha256sum "$PACKAGE_NAME.tar.gz" >"$PACKAGE_NAME.tar.gz.sha256")

echo "Release package: $ARCHIVE"
if [[ -n "$PACKAGE_MARCH" ]]; then
  echo "CPU baseline: -march=$PACKAGE_MARCH"
else
  echo "CPU baseline: compiler default (no explicit -march)"
fi
echo "Kernel bypass: $KERNEL_BYPASS"
if [[ "$KERNEL_BYPASS" == ON ]]; then
  echo "SPDK CPU target: $TARGET_ARCHITECTURE; DPDK uses its generic baseline"
fi
echo "Dynamic dependencies:"
for app in "${APPS[@]}"; do
  echo "$app:"
  ldd "$STAGE_DIR/$app"
done
cat "$ARCHIVE.sha256"
