#!/bin/sh
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
# Emit complete discovery files atomically; a failed update preserves the old set.
set -eu
set -f
export LC_ALL=C
output=${TARGET_DIR:-/targets}
mode=${LAVIK_MODE:-standalone}
cluster=${LAVIK_CLUSTER:-default}
case "$mode" in standalone|meta-managed) ;; *) echo 'Invalid LAVIK_MODE' >&2; exit 1;; esac
case "$cluster" in ''|*[!a-zA-Z0-9_.-]*) echo 'Invalid LAVIK_CLUSTER' >&2; exit 1;; esac

# The accepted host:port alphabet cannot inject JSON, shell syntax or credentials.
endpoints() {
  list=$1
  case "$list" in ''|,*|*,|*,,*) echo 'Empty target' >&2; return 1;; esac
  saved_ifs=$IFS
  IFS=,
  set -- $list
  IFS=$saved_ifs
  result=
  seen=,
  for target do
    case "$target" in *'
'*) echo 'Newline in target' >&2; return 1;; esac
    if ! printf '%s\n' "$target" | grep -Eq '^(\[[0-9A-Fa-f:]+\]|[a-zA-Z0-9_.-]+):[0-9]{1,5}$'; then
      echo 'Target must be host:port' >&2; return 1
    fi
    port=${target##*:}
    if [ "$port" -lt 1 ] || [ "$port" -gt 65535 ]; then
      echo 'Target port out of range' >&2; return 1
    fi
    case "$seen" in *",$target,"*) echo 'Duplicate target' >&2; return 1;; esac
    seen="$seen$target,"
    result="${result}${result:+,}\"$target\""
  done
  printf '%s' "$result"
}
mkdir -p "$output"
trap 'rm -f "$output/lavik.json.tmp" "$output/alertmanagers.json.tmp"' EXIT HUP INT TERM
if [ -n "${LAVIK_TARGETS_FILE:-}" ]; then
  # Advanced file-SD is user-owned Prometheus configuration, validated by Prometheus.
  cp "${TARGET_INPUT:-/input/lavik.json}" "$output/lavik.json.tmp"
else
  targets=$(endpoints "${LAVIK_TARGETS:-}")
  printf '[{"targets":[%s],"labels":{"service":"lavik","cluster":"%s","lavik_mode":"%s"}}]\n' \
    "$targets" "$cluster" "$mode" > "$output/lavik.json.tmp"
fi
if [ -n "${ALERTMANAGER_TARGETS:-}" ]; then
  managers=$(endpoints "$ALERTMANAGER_TARGETS")
  printf '[{"targets":[%s]}]\n' "$managers" > "$output/alertmanagers.json.tmp"
else
  printf '[]\n' > "$output/alertmanagers.json.tmp"
fi
mv "$output/lavik.json.tmp" "$output/lavik.json"
mv "$output/alertmanagers.json.tmp" "$output/alertmanagers.json"
