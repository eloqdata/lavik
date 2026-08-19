#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
HOST_ARCH=$(uname -m)

case "$HOST_ARCH" in
  x86_64|amd64)
    PACKAGE_ARCH=x86_64
    DEFAULT_MARCH=x86-64-v2
    ;;
  aarch64|arm64)
    PACKAGE_ARCH=aarch64
    DEFAULT_MARCH=armv8-a
    ;;
  *)
    echo "Unsupported packaging architecture: $HOST_ARCH" >&2
    exit 1
    ;;
esac

PACKAGE_MARCH=${KEYLANE_PACKAGE_MARCH:-$DEFAULT_MARCH}
BUILD_DIR=${KEYLANE_PACKAGE_BUILD_DIR:-$REPO_ROOT/build/package-$PACKAGE_ARCH}
OUTPUT_DIR=${KEYLANE_PACKAGE_OUTPUT_DIR:-$REPO_ROOT/dist}
BUILD_JOBS=${KEYLANE_PACKAGE_JOBS:-$(nproc)}
VERSION=$(git -C "$REPO_ROOT" describe --tags --always --dirty)
VERSION=${VERSION//\//-}
PACKAGE_NAME=keylane-$VERSION-linux-$PACKAGE_ARCH
STAGE_DIR=$BUILD_DIR/$PACKAGE_NAME
ARCHIVE=$OUTPUT_DIR/$PACKAGE_NAME.tar.gz

cmake -S "$REPO_ROOT" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF \
  -DKEYLANE_ENABLE_OPT=ON \
  -DKEYLANE_MARCH="$PACKAGE_MARCH" \
  -DKEYLANE_STATIC_OPENSSL=ON \
  -DKEYLANE_STATIC_CXX_RUNTIME=ON
cmake --build "$BUILD_DIR" --target keylane -j"$BUILD_JOBS"

BINARY=$BUILD_DIR/keylane
if [[ ! -x "$BINARY" ]]; then
  echo "Release build did not produce $BINARY" >&2
  exit 1
fi

DYNAMIC_SECTION=$(readelf -d "$BINARY")
if grep -Eq 'Shared library: \[(libssl|libcrypto)\.so' <<<"$DYNAMIC_SECTION"; then
  echo "Packaging refused: OpenSSL is still dynamically linked" >&2
  exit 1
fi
if grep -Eq 'Shared library: \[(libstdc\+\+|libgcc_s)\.so' \
    <<<"$DYNAMIC_SECTION"; then
  echo "Packaging refused: the C++ runtime is still dynamically linked" >&2
  exit 1
fi

cmake -E remove_directory "$STAGE_DIR"
cmake -E make_directory "$STAGE_DIR"
install -m 0755 "$BINARY" "$STAGE_DIR/keylane"
if command -v strip >/dev/null 2>&1; then
  strip --strip-unneeded "$STAGE_DIR/keylane"
fi
install -m 0644 "$REPO_ROOT/docs/tls-and-auth.md" \
  "$STAGE_DIR/tls-and-auth.md"
OPENSSL_LICENSE=${KEYLANE_OPENSSL_LICENSE:-/usr/share/common-licenses/Apache-2.0}
if [[ ! -f "$OPENSSL_LICENSE" ]]; then
  echo "OpenSSL license text not found at $OPENSSL_LICENSE" >&2
  echo "Set KEYLANE_OPENSSL_LICENSE to the Apache-2.0 license file." >&2
  exit 1
fi
install -m 0644 "$OPENSSL_LICENSE" "$STAGE_DIR/OPENSSL-LICENSE.txt"
printf '%s\n' "$VERSION" >"$STAGE_DIR/VERSION"

"$STAGE_DIR/keylane" --help >/dev/null

cmake -E make_directory "$OUTPUT_DIR"
tar -C "$BUILD_DIR" -czf "$ARCHIVE" "$PACKAGE_NAME"

echo "Release package: $ARCHIVE"
echo "CPU baseline: -march=$PACKAGE_MARCH"
echo "Dynamic dependencies:"
ldd "$STAGE_DIR/keylane" || true
sha256sum "$ARCHIVE"
