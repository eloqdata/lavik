#!/bin/sh
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
# Run on the Docker host; Admin never receives access to the host Docker socket.
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repo"
project=${COMPOSE_PROJECT_NAME:-lavik-admin-quickstart}
case "$project" in
  ''|*[!a-z0-9_-]*|-*|_*) echo 'Invalid Compose project name.' >&2; exit 1 ;;
esac
case "${1:-}" in
  --yes) [ "$#" -eq 1 ] || exit 1 ;;
  '')
    printf 'Permanently remove Docker demo project %s?\n' "$project"
    printf 'This deletes its containers, Meta/Data volumes, release cache, Admin workspace and token.\n'
    printf 'Type %s to confirm: ' "$project"
    read -r confirmation
    [ "$confirmation" = "$project" ] || { echo 'Cancelled; no changes made.'; exit 1; }
    ;;
  *) echo 'Usage: ./admin/quickstart/remove.sh [--yes]' >&2; exit 1 ;;
esac
command -v docker >/dev/null || { echo 'Docker is required.' >&2; exit 1; }
docker info >/dev/null
docker compose -p "$project" -f admin/quickstart/compose.yaml --profile tools down --volumes --remove-orphans
# Older source-built demos used this extra named volume. Delete it only when
# Compose labels prove it belongs to this project; never use a global prune.
legacy_volume="${project}_build"
if docker volume inspect "$legacy_volume" >/dev/null 2>&1; then
  owner=$(docker volume inspect --format '{{index .Labels "com.docker.compose.project"}}' "$legacy_volume")
  if [ "$owner" = "$project" ]; then
    docker volume rm "$legacy_volume"
  else
    printf 'Retained unowned volume: %s\n' "$legacy_volume" >&2
  fi
fi
printf 'Removed demo project %s. Run ./admin/quickstart/setup.sh to start fresh.\n' "$project"
