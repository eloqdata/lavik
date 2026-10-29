#!/usr/bin/env bash
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
# Install the same versions as the Compose stack, checking upstream SHA-256 lists.
set -euo pipefail
install_root=${1:?usage: install_monitoring_tools.sh DIRECTORY}
mkdir -p "$install_root"
install_root=$(cd "$install_root" && pwd)
case "$(uname -m)" in x86_64) arch=amd64;; aarch64|arm64) arch=arm64;; *) exit 1;; esac
for spec in prometheus:3.11.3 alertmanager:0.34.1; do
  project=${spec%:*}
  version=${spec#*:}
  archive="$project-$version.linux-$arch.tar.gz"
  url="https://github.com/prometheus/$project/releases/download/v$version"
  temporary=$(mktemp -d)
  trap 'rm -rf "$temporary"' EXIT
  curl -fsSL --retry 3 "$url/$archive" -o "$temporary/$archive"
  curl -fsSL --retry 3 "$url/sha256sums.txt" -o "$temporary/sha256sums.txt"
  (cd "$temporary" && awk -v archive="$archive" '$2 == archive {print}' sha256sums.txt > selected.sha256 && test -s selected.sha256 && sha256sum -c selected.sha256)
  tar -xzf "$temporary/$archive" -C "$install_root"
  if [ "$project" = prometheus ]; then programs='prometheus promtool'; else programs='alertmanager amtool'; fi
  for program in $programs; do
    ln -sfn "$project-$version.linux-$arch/$program" "$install_root/$program"
  done
  rm -rf "$temporary"
  trap - EXIT
done
