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

# Runs after the dedicated six-device RAID has been dismantled and all six
# members have been discarded. The script intentionally refuses non-block
# targets so a typo cannot redirect the destructive benchmark to a regular
# workspace file.

readonly CLIENT_HOST=172.16.0.5
readonly SERVER_HOST=172.16.0.4
readonly BINARY=/mnt/dev/keylane/bld-iouring-peer-1t/keylane
readonly REDIS_CLI=/mnt/dev/peer-bench/redis/v8.8.0/src/src/redis-cli
readonly RESULT_ROOT=/mnt/dev/peer-bench/results-2026-09-06/redis-valkey-iothreads-10g-1k/keylane
readonly CLIENT_THREADS=16
readonly KEY_MIN=1
readonly KEY_MAX=10000000
readonly KEY_COUNT=10000000
readonly REQUESTS_PER_FILL_CLIENT=15625
readonly TEST_SECONDS=30
readonly WARMUP_SECONDS=10
readonly -a CONNECTIONS=(80 160 320 640 1280)
readonly -a DEVICES=(
  /dev/nvme1n1
  /dev/nvme2n1
  /dev/nvme3n1
  /dev/nvme4n1
  /dev/nvme5n1
  /dev/nvme6n1
)

mkdir -p "${RESULT_ROOT}/logs"
rm -f "${RESULT_ROOT}/SHA256SUMS"

for device in "${DEVICES[@]}"; do
  [[ -b "${device}" ]] || { echo "not a block device: ${device}" >&2; exit 1; }
  findmnt -rn -S "${device}" | grep -q . && {
    echo "refusing mounted device: ${device}" >&2
    exit 1
  }
done

run_memtier() {
  local output=$1
  local workload=$2
  local connections=$3
  local clients_per_thread=$((connections / CLIENT_THREADS))
  local command command_flags

  if [[ "${workload}" == GET ]]; then
    command="GET __key__"
    command_flags="--command-is-read"
  else
    command="SET __key__ __data__"
    command_flags=""
  fi

  ssh -o BatchMode=yes "${CLIENT_HOST}" \
    "ulimit -n 65535; exec taskset -c 0-15 memtier_benchmark \
      --server=${SERVER_HOST} --port=6379 --protocol=redis \
      --threads=${CLIENT_THREADS} --clients=${clients_per_thread} \
      --test-time=${TEST_SECONDS} --pipeline=1 \
      --key-minimum=${KEY_MIN} --key-maximum=${KEY_MAX} --key-prefix='' \
      --data-size=1024 --command='${command}' --command-key-pattern=R \
      ${command_flags} --hide-histogram \
      --print-percentiles=50,99,99.9,99.99 --show-config" \
    >"${output}" 2>&1
}

{
  date -u --iso-8601=seconds
  uname -a
  lscpu
  free -h
  git -C /mnt/dev/keylane show -s --format='%H %ci %s' 29dc8e6
  "${BINARY}" --version
  sha256sum "${BINARY}"
  printf 'client=%s\nkeys=%s\nvalue_bytes=1024\nclient_threads=%s\n' \
    "${CLIENT_HOST}" "${KEY_COUNT}" "${CLIENT_THREADS}"
  printf 'connections=%s\ntest_seconds=%s\nwarmup_seconds=%s\n' \
    "${CONNECTIONS[*]}" "${TEST_SECONDS}" "${WARMUP_SECONDS}"
  printf 'devices=%s\n' "${DEVICES[*]}"
} >"${RESULT_ROOT}/INFO.txt"

echo "[$(date -u +%T)] keylane: starting fresh six-device raw io_uring store"
sudo -n taskset -c 0-15 "${BINARY}" \
  --bind="${SERVER_HOST}" --port=6379 --metrics-port=9100 \
  --threads=16 --pin-workers --maxclients=10000 \
  --busy-poll-us=20 --foreground-budget-us=1000 \
  --background-budget-us=10 --background-warrant-percent=1 \
  --defrag-paused --tomb-raider-interval-ms=0 \
  --data-file=/dev/nvme1n1 --data-file=/dev/nvme2n1 \
  --data-file=/dev/nvme3n1 --data-file=/dev/nvme4n1 \
  --data-file=/dev/nvme5n1 --data-file=/dev/nvme6n1 \
  --log-dir="${RESULT_ROOT}/logs" \
  >"${RESULT_ROOT}/server-console.log" 2>&1 &
server_pid=$!
echo "${server_pid}" >"${RESULT_ROOT}/server.pid"

cleanup() {
  if kill -0 "${server_pid}" 2>/dev/null; then
    sudo -n kill -TERM "${server_pid}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

for _ in $(seq 1 600); do
  if "${REDIS_CLI}" -h "${SERVER_HOST}" -p 6379 ping 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 0.1
done
"${REDIS_CLI}" -h "${SERVER_HOST}" -p 6379 ping | grep -q PONG

echo "[$(date -u +%T)] keylane: creating 10M-key baseline"
ssh -o BatchMode=yes "${CLIENT_HOST}" \
  "ulimit -n 65535; exec taskset -c 0-15 memtier_benchmark \
    --server=${SERVER_HOST} --port=6379 --protocol=redis \
    --threads=${CLIENT_THREADS} --clients=40 \
    --requests=${REQUESTS_PER_FILL_CLIENT} --pipeline=1 \
    --ratio=1:0 --key-pattern=P:P \
    --key-minimum=${KEY_MIN} --key-maximum=${KEY_MAX} --key-prefix='' \
    --data-size=1024 --hide-histogram \
    --print-percentiles=50,99,99.9,99.99 --show-config" \
  >"${RESULT_ROOT}/fill.txt" 2>&1

dbsize=$("${REDIS_CLI}" -h "${SERVER_HOST}" -p 6379 dbsize)
[[ "${dbsize}" == "${KEY_COUNT}" ]] || {
  echo "unexpected Keylane DB size: ${dbsize}" >&2
  exit 1
}
ssh -o BatchMode=yes "${CLIENT_HOST}" \
  "ulimit -n 65535; exec taskset -c 0-15 memtier_benchmark \
    --server=${SERVER_HOST} --port=6379 --protocol=redis \
    --threads=${CLIENT_THREADS} --clients=40 \
    --test-time=${WARMUP_SECONDS} --pipeline=1 \
    --key-minimum=${KEY_MIN} --key-maximum=${KEY_MAX} --key-prefix='' \
    --command='GET __key__' --command-key-pattern=R --command-is-read \
    --hide-histogram" >/dev/null 2>&1

for workload in GET SET; do
  for connection_count in "${CONNECTIONS[@]}"; do
    echo "[$(date -u +%T)] keylane: ${workload} c=${connection_count}"
    run_memtier \
      "${RESULT_ROOT}/${workload,,}-c${connection_count}.txt" \
      "${workload}" "${connection_count}"
  done
done

(cd "${RESULT_ROOT}" && sha256sum -- INFO.txt fill.txt get-c*.txt set-c*.txt \
  server-console.log >SHA256SUMS)

# Stop explicitly so the runner's controlling terminal cannot deliver SIGHUP
# at shell exit. The raw dataset remains available for a later recovery test.
sudo -n kill -TERM "${server_pid}" 2>/dev/null || true
for _ in $(seq 1 100); do
  kill -0 "${server_pid}" 2>/dev/null || break
  sleep 0.1
done
trap - EXIT
echo "[$(date -u +%T)] keylane: sweep complete; service stopped"
