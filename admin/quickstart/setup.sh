#!/bin/sh
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
# One entry point; rerunning retains volumes and observes existing creation jobs.
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repo"
command -v docker >/dev/null || { echo 'Install Docker Desktop or Docker Engine with Compose v2 first.' >&2; exit 1; }
docker info >/dev/null
docker compose version >/dev/null
git submodule update --init bycorf third_party/mimalloc
git -C bycorf submodule update --init third_party/liburing third_party/abseil
docker build -f admin/Dockerfile.toolchain -t lavik-admin-toolchain:local .
docker compose -f admin/quickstart/compose.yaml run --rm build
docker compose -f admin/quickstart/compose.yaml up -d
docker compose -f admin/quickstart/compose.yaml exec -T admin node quickstart/bootstrap.mjs
printf '\nOpen http://localhost:%s and sign in with this token:\n' "${LAVIK_QUICKSTART_PORT:-4173}"
docker compose -f admin/quickstart/compose.yaml exec -T admin cat /data/lavik-admin/token
printf '\n'
