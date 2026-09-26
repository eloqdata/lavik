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

lavik_bin=$1
redis_cli=$2
case_dir=$(mktemp -d "${LAVIK_TEST_DATA_DIR:-/tmp}/lavik-native-replicaof-XXXXXX")
pids=()
cleanup() {
  status=$?
  if ((status != 0)); then
    for log in "${case_dir}"/*.log; do
      [[ -f ${log} ]] && tail -80 "${log}" >&2
    done
  fi
  for pid in "${pids[@]}"; do kill "${pid}" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do wait "${pid}" 2>/dev/null || true; done
  rm -rf -- "${case_dir}"
}
trap cleanup EXIT

readarray -t ports < <(python3 - <<'PY'
import socket
sockets = []
for _ in range(3):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sockets.append(sock)
for sock in sockets:
    print(sock.getsockname()[1])
PY
)
source_port=${ports[0]}
replica_port=${ports[1]}
third_port=${ports[2]}

start_lavik() {
  local name=$1 port=$2
  local config_args=()
  if [[ ${name} == replica ]]; then
    printf '# Native replication runtime test\n' >"${case_dir}/replica.conf"
    config_args=("${case_dir}/replica.conf")
  fi
  fallocate -l 256M "${case_dir}/${name}.data"
  "${lavik_bin}" "${config_args[@]}" --logtostderr --port "${port}" --threads 2 --no-pin-workers \
    --recv-buffers-per-worker 0 --max-memory 1073741824 \
    --data-file "${case_dir}/${name}.data" >"${case_dir}/${name}.log" 2>&1 &
  pids+=("$!")
  for _ in {1..600}; do
    if [[ $("${redis_cli}" -p "${port}" ping 2>/dev/null || true) == PONG ]]; then
      return 0
    fi
    sleep 0.05
  done
  return 1
}

start_lavik source "${source_port}"
start_lavik replica "${replica_port}"
start_lavik third "${third_port}"

replica_get() {
  local port=$1 key=$2 db=${3:-0}
  printf 'READONLY\nGET %s\n' "${key}" |
    "${redis_cli}" --raw -p "${port}" -n "${db}" | tail -n 1
}

[[ $("${redis_cli}" -p "${source_port}" set baseline before) == OK ]]
[[ $("${redis_cli}" -p "${source_port}" -n 1 set other-db initial) == OK ]]
[[ $("${redis_cli}" -p "${replica_port}" LAVIK.REPLICAOF 127.0.0.1 "${source_port}") == OK ]]

for _ in {1..600}; do
  [[ $(replica_get "${replica_port}" baseline 2>/dev/null || true) == before ]] &&
    [[ $(replica_get "${replica_port}" other-db 1 2>/dev/null || true) == initial ]] && break
  sleep 0.05
done
[[ $(replica_get "${replica_port}" baseline) == before ]]
[[ $(replica_get "${replica_port}" other-db 1) == initial ]]
rewrite_reply=$("${redis_cli}" -p "${replica_port}" CONFIG REWRITE 2>&1)
[[ ${rewrite_reply} == *'cannot persist a LAVIK.REPLICAOF upstream'* ]]
[[ $(<"${case_dir}/replica.conf") == '# Native replication runtime test' ]]

[[ $("${redis_cli}" -p "${source_port}" set baseline online) == OK ]]
for _ in {1..600}; do
  [[ $(replica_get "${replica_port}" baseline 2>/dev/null || true) == online ]] && break
  sleep 0.05
done
[[ $(replica_get "${replica_port}" baseline) == online ]]

# A replica is not a native source. Probe rejection must leave the third node
# writable and must not replace the active source/replica relationship.
cascade_reply=$("${redis_cli}" -p "${third_port}" LAVIK.REPLICAOF 127.0.0.1 "${replica_port}" 2>&1)
[[ ${cascade_reply} == *'native cascading replication is not supported'* ]]
[[ $("${redis_cli}" -p "${third_port}" set stayed-master yes) == OK ]]
[[ $("${redis_cli}" -p "${third_port}" get stayed-master) == yes ]]
[[ $("${redis_cli}" -p "${source_port}" set after-rejection intact) == OK ]]
for _ in {1..600}; do
  [[ $(replica_get "${replica_port}" after-rejection 2>/dev/null || true) == intact ]] && break
  sleep 0.05
done
[[ $(replica_get "${replica_port}" after-rejection) == intact ]]

# Rejection also preserves a live subscription on the requesting node.
[[ $("${redis_cli}" -p "${third_port}" LAVIK.REPLICAOF 127.0.0.1 "${source_port}") == OK ]]
for _ in {1..600}; do
  [[ $(replica_get "${third_port}" baseline 2>/dev/null || true) == online ]] && break
  sleep 0.05
done
[[ $(replica_get "${third_port}" baseline) == online ]]
cascade_reply=$("${redis_cli}" -p "${third_port}" LAVIK.REPLICAOF 127.0.0.1 "${replica_port}" 2>&1)
[[ ${cascade_reply} == *'native cascading replication is not supported'* ]]
[[ $("${redis_cli}" -p "${source_port}" set kept-subscription yes) == OK ]]
for _ in {1..600}; do
  [[ $(replica_get "${third_port}" kept-subscription 2>/dev/null || true) == yes ]] && break
  sleep 0.05
done
[[ $(replica_get "${third_port}" kept-subscription) == yes ]]

[[ $("${redis_cli}" -p "${replica_port}" LAVIK.REPLICAOF NO ONE) == OK ]]
[[ $("${redis_cli}" -p "${replica_port}" set promoted writable) == OK ]]
[[ $("${redis_cli}" -p "${replica_port}" get promoted) == writable ]]
