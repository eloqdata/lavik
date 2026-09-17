#!/usr/bin/env bash
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/install_build_deps.sh [--with-bypass] [--dry-run]

Install Ubuntu 24.04 build packages for Lavik, lavik-meta, and lavik-ctl.
  --with-bypass  Include DPDK/SPDK and the private FreeBSD stack's build tools.
  --dry-run      Print the apt commands without changing the system.
  -h, --help    Show this help.

Run as your normal user; only apt uses sudo when needed. This script installs
system packages, not Git submodules. See README.md for the complete build steps.
EOF
}

with_bypass=false
dry_run=false
for arg in "$@"; do
  case "$arg" in
    --with-bypass) with_bypass=true ;;
    --dry-run) dry_run=true ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $arg" >&2; usage >&2; exit 2 ;;
  esac
done

# Package names and compiler versions match the supported release build image.
# Fail before apt on other distributions rather than installing a partial set.
if [[ ! -r /etc/os-release ]]; then
  echo "This installer requires Ubuntu 24.04 (/etc/os-release is missing)." >&2
  exit 1
fi
# shellcheck disable=SC1091
source /etc/os-release
if [[ "${ID:-}" != ubuntu || "${VERSION_ID:-}" != 24.04 ]]; then
  echo "This installer supports Ubuntu 24.04; found ${PRETTY_NAME:-unknown OS}." >&2
  echo "See docs/operations/building-and-packaging.md for build requirements." >&2
  exit 1
fi

packages=(
  build-essential gcc-13 g++-13 cmake ninja-build git
  pkg-config python3 libssl-dev
)
if "$with_bypass"; then
  packages+=(
    clang-18 meson python3-pyelftools python3-dev
    # genrpc.py imports both modules even when only C headers are generated.
    python3-jinja2 python3-tabulate
    autoconf automake libtool nasm help2man patch
    libnuma-dev uuid-dev libaio-dev libjson-c-dev libncurses-dev libkeyutils-dev
  )
fi

privilege=()
if (( EUID != 0 )); then
  if ! "$dry_run" && ! command -v sudo >/dev/null 2>&1; then
    echo "Installing system packages requires root or sudo." >&2
    exit 1
  fi
  privilege=(sudo)
fi

run() {
  if "$dry_run"; then
    printf '%q ' "$@"
    printf '\n'
  else
    "$@"
  fi
}

run "${privilege[@]}" apt-get update
run "${privilege[@]}" apt-get install -y --no-install-recommends "${packages[@]}"
