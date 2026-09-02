#!/usr/bin/env bash
set -euo pipefail

if [[ ${KEYLANE_CLUSTER_HARDWARE_OPT_IN:-0} != 1 ]]; then
  echo "cluster hardware tests skipped: set KEYLANE_CLUSTER_HARDWARE_OPT_IN=1"
  exit 77
fi

device=${KEYLANE_CLUSTER_SCRATCH_DEVICE:-}
if [[ -z $device ]]; then
  echo "KEYLANE_CLUSTER_SCRATCH_DEVICE is required after hardware opt-in" >&2
  exit 1
fi
if [[ $device != /dev/* || ! -b $device ]]; then
  echo "scratch device must be an existing absolute block device: $device" >&2
  exit 1
fi
if findmnt --noheadings --source "$device" >/dev/null 2>&1; then
  echo "refusing mounted scratch device: $device" >&2
  exit 1
fi

root_source=$(findmnt --noheadings --output SOURCE / | head -n 1)
if [[ $root_source == "$device" || $root_source == "$device"* ]]; then
  echo "refusing root filesystem device: $device" >&2
  exit 1
fi

echo "cluster hardware safety gate accepted $device"
