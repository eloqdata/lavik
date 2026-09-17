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

# Runs on the 172.16.0.4 server and drives memtier on 172.16.0.5. Redis and
# Valkey are restarted from an immutable RDB before every I/O-thread variant,
# so preceding overwrite tests cannot influence the next variant's dataset.

readonly CLIENT_HOST=172.16.0.5
readonly SERVER_HOST=172.16.0.4
readonly REDIS_ROOT=/mnt/dev/peer-bench/redis/v8.8.0/src
readonly VALKEY_ROOT=/mnt/dev/peer-bench/valkey/v9.1.0/src
readonly RESULT_ROOT=/mnt/dev/peer-bench/results-2026-09-06/redis-valkey-iothreads-10g-1k
readonly DATA_ROOT=/mnt/dev/peer-bench/redis-valkey-iothreads-10g-1k
readonly CLIENT_THREADS=16
readonly KEY_MIN=1
readonly KEY_MAX=10000000
readonly KEY_COUNT=10000000
readonly REQUESTS_PER_FILL_CLIENT=15625
readonly TEST_SECONDS=30
readonly WARMUP_SECONDS=10
readonly -a IO_THREADS=(1 2 4 8 16)
readonly -a CONNECTIONS=(80 160 320 640 1280)

mkdir -p "${RESULT_ROOT}" "${DATA_ROOT}"

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

warm_dataset() {
  ssh -o BatchMode=yes "${CLIENT_HOST}" \
    "ulimit -n 65535; exec taskset -c 0-15 memtier_benchmark \
      --server=${SERVER_HOST} --port=6379 --protocol=redis \
      --threads=${CLIENT_THREADS} --clients=40 \
      --test-time=${WARMUP_SECONDS} --pipeline=1 \
      --key-minimum=${KEY_MIN} --key-maximum=${KEY_MAX} --key-prefix='' \
      --command='GET __key__' --command-key-pattern=R --command-is-read \
      --hide-histogram" >/dev/null 2>&1
}

fill_dataset() {
  local output=$1
  ssh -o BatchMode=yes "${CLIENT_HOST}" \
    "ulimit -n 65535; exec taskset -c 0-15 memtier_benchmark \
      --server=${SERVER_HOST} --port=6379 --protocol=redis \
      --threads=${CLIENT_THREADS} --clients=40 \
      --requests=${REQUESTS_PER_FILL_CLIENT} --pipeline=1 \
      --ratio=1:0 --key-pattern=P:P \
      --key-minimum=${KEY_MIN} --key-maximum=${KEY_MAX} --key-prefix='' \
      --data-size=1024 --hide-histogram \
      --print-percentiles=50,99,99.9,99.99 --show-config" \
    >"${output}" 2>&1
}

stop_server() {
  local cli=$1
  local pidfile=$2
  if [[ -s "${pidfile}" ]]; then
    "${cli}" -h 127.0.0.1 -p 6379 shutdown nosave >/dev/null 2>&1 || true
    for _ in $(seq 1 100); do
      [[ ! -e "${pidfile}" ]] && return 0
      sleep 0.1
    done
    local pid
    pid=$(<"${pidfile}")
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
}

start_server() {
  local binary=$1
  local cli=$2
  local data_dir=$3
  local pidfile=$4
  local logfile=$5
  local thread_count=$6

  ulimit -n 65535
  taskset -c 0-15 "${binary}" \
    --bind 0.0.0.0 --protected-mode no --port 6379 \
    --daemonize yes --pidfile "${pidfile}" --logfile "${logfile}" \
    --dir "${data_dir}" --dbfilename dump.rdb \
    --save "" --appendonly no --maxmemory 0 --maxclients 10000 \
    --io-threads "${thread_count}"

  for _ in $(seq 1 600); do
    if "${cli}" -h 127.0.0.1 -p 6379 ping 2>/dev/null | grep -q PONG; then
      return 0
    fi
    sleep 0.1
  done
  echo "server did not become ready: ${binary}, io-threads=${thread_count}" >&2
  return 1
}

run_product() {
  local product=$1
  local binary=$2
  local cli=$3
  local product_dir="${RESULT_ROOT}/${product}"
  local data_dir="${DATA_ROOT}/${product}"
  local pidfile="${data_dir}/server.pid"

  mkdir -p "${product_dir}" "${data_dir}"
  rm -f "${product_dir}/SHA256SUMS"
  # These directories are dedicated to this benchmark; clearing only their
  # top-level contents prevents stale RDBs from changing the fill population.
  find "${data_dir}" -mindepth 1 -maxdepth 1 -delete

  {
    date -u --iso-8601=seconds
    uname -a
    lscpu
    free -h
    "${binary}" --version
    "${cli}" --version
    sha256sum "${binary}" "${cli}"
    printf 'client=%s\nkeys=%s\nvalue_bytes=1024\nclient_threads=%s\n' \
      "${CLIENT_HOST}" "${KEY_COUNT}" "${CLIENT_THREADS}"
    printf 'connections=%s\nio_threads=%s\ntest_seconds=%s\nwarmup_seconds=%s\n' \
      "${CONNECTIONS[*]}" "${IO_THREADS[*]}" "${TEST_SECONDS}" "${WARMUP_SECONDS}"
  } >"${product_dir}/INFO.txt"

  echo "[$(date -u +%T)] ${product}: creating 10M-key baseline"
  start_server "${binary}" "${cli}" "${data_dir}" "${pidfile}" \
    "${product_dir}/fill-server.log" 16
  fill_dataset "${product_dir}/fill.txt"
  local dbsize
  dbsize=$("${cli}" -h 127.0.0.1 -p 6379 dbsize)
  if [[ "${dbsize}" != "${KEY_COUNT}" ]]; then
    echo "unexpected DB size for ${product}: ${dbsize}" >&2
    return 1
  fi
  "${cli}" -h 127.0.0.1 -p 6379 info memory >"${product_dir}/fill-memory.txt"
  /usr/bin/time -f 'save_wall_seconds=%e' -o "${product_dir}/save-time.txt" \
    "${cli}" -h 127.0.0.1 -p 6379 save >/dev/null
  sha256sum "${data_dir}/dump.rdb" >"${product_dir}/dump-rdb.sha256"
  stop_server "${cli}" "${pidfile}"

  for thread_count in "${IO_THREADS[@]}"; do
    echo "[$(date -u +%T)] ${product}: io-threads=${thread_count} loading baseline"
    start_server "${binary}" "${cli}" "${data_dir}" "${pidfile}" \
      "${product_dir}/io${thread_count}-server.log" "${thread_count}"
    dbsize=$("${cli}" -h 127.0.0.1 -p 6379 dbsize)
    [[ "${dbsize}" == "${KEY_COUNT}" ]]
    "${cli}" -h 127.0.0.1 -p 6379 config get io-threads \
      >"${product_dir}/io${thread_count}-config.txt"
    "${cli}" -h 127.0.0.1 -p 6379 info memory \
      >"${product_dir}/io${thread_count}-memory-before.txt"
    warm_dataset

    for workload in GET SET; do
      for connection_count in "${CONNECTIONS[@]}"; do
        echo "[$(date -u +%T)] ${product}: io=${thread_count} ${workload} c=${connection_count}"
        run_memtier \
          "${product_dir}/io${thread_count}-${workload,,}-c${connection_count}.txt" \
          "${workload}" "${connection_count}"
      done
    done

    "${cli}" -h 127.0.0.1 -p 6379 info all \
      >"${product_dir}/io${thread_count}-info-after.txt"
    stop_server "${cli}" "${pidfile}"
  done

  (cd "${product_dir}" && sha256sum -- * >SHA256SUMS)
}

case "${1:-all}" in
  redis)
    run_product redis "${REDIS_ROOT}/src/redis-server" "${REDIS_ROOT}/src/redis-cli"
    ;;
  valkey)
    run_product valkey "${VALKEY_ROOT}/src/valkey-server" "${VALKEY_ROOT}/src/valkey-cli"
    ;;
  all)
    run_product redis "${REDIS_ROOT}/src/redis-server" "${REDIS_ROOT}/src/redis-cli"
    run_product valkey "${VALKEY_ROOT}/src/valkey-server" "${VALKEY_ROOT}/src/valkey-cli"
    ;;
  *)
    echo "usage: $0 [redis|valkey|all]" >&2
    exit 2
    ;;
esac
