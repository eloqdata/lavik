#!/bin/sh
# Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
set -eu
umask 077

role=${1:?Expected meta or data}
node_number=${2:?Expected node number 1, 2, or 3}
case "$node_number" in 1|2|3) ;; *) echo 'Node number must be 1, 2, or 3' >&2; exit 2 ;; esac

case "$role" in
  meta)
    mkdir -p /data/meta
    set -- /build/lavik-meta --id "$node_number" \
      --addr "172.29.91.1${node_number}:7100" \
      --ctl-addr "172.29.91.1${node_number}:7200" \
      --data-control-addr "172.29.91.1${node_number}:7300" \
      --data-dir /data/meta
    # The manifest is a first-start bootstrap input, not a restart argument.
    # Existing state must recover its committed membership without bootstrap.
    if [ -z "$(ls -A /data/meta)" ]; then
      set -- "$@" --initial-cluster-manifest /quickstart/cluster.toml
    fi
    ;;
  data)
    # Never truncate a population when Compose recreates a container.
    if [ ! -e /data/lavik.data ]; then
      fallocate -l 1G /data/lavik.data
    fi
    node_id=$(printf '%040d' "$node_number")
    set -- /build/lavik --bind 0.0.0.0 --port 6379 \
      --threads 1 --no-pin-workers --client-mode cluster --meta-managed yes \
      --node-id "$node_id" --announce-ip "172.29.91.2${node_number}" \
      --data-file /data/lavik.data --log-dir /data/logs \
      --meta-seed 172.29.91.11:7300 \
      --meta-seed 172.29.91.12:7300 \
      --meta-seed 172.29.91.13:7300
    ;;
  *) echo 'Role must be meta or data' >&2; exit 2 ;;
esac

if [ ! -x "$1" ]; then
  echo 'Build the binaries first: docker compose -f admin/quickstart/compose.yaml run --rm build' >&2
  exit 1
fi
exec "$@"
