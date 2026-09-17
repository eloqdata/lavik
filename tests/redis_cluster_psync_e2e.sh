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
redis_server=$2
redis_cli=$3
case_template=${LAVIK_TEST_DATA_DIR:-/tmp}/lavik-redis-cluster-e2e-XXXXXX
case_dir=$(mktemp -d "${case_template}")
redis_pids=()
lavik_pid=

cleanup() {
  status=$?
  if ((status != 0)); then
    echo "Redis Cluster PSYNC test failed; Lavik log follows:" >&2
    tail -100 "$case_dir/lavik.log" >&2 2>/dev/null || true
    for redis_log in "$case_dir"/*/redis.log; do
      [[ -e $redis_log ]] || continue
      echo "Redis log $redis_log follows:" >&2
      tail -100 "$redis_log" >&2 2>/dev/null || true
    done
  fi
  # Stop all peers before joining any of them: leaving Redis live while
  # joining its replica can strand cleanup in an upstream socket read.
  # This fixture checks PSYNC data, not graceful-shutdown durability.
  local pids=("${redis_pids[@]}")
  if [[ -n $lavik_pid ]]; then pids+=("$lavik_pid"); fi
  if ((${#pids[@]})); then
    kill "${pids[@]}" 2>/dev/null || true
    for _ in {1..100}; do
      local alive=false
      for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then alive=true; fi
      done
      if ! "$alive"; then break; fi
      sleep 0.1
    done
    kill -KILL "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
  fi
  if [[ $case_dir == "${case_template%XXXXXX}"* ]]; then
    rm -rf -- "$case_dir"
  fi
}
trap cleanup EXIT

readarray -t ports < <(python3 - <<'PY'
import random
import socket

sockets = []
ports = []
while len(ports) < 4:
    port = random.randrange(20000, 45000)
    candidates = [port]
    if len(ports) < 3:
        candidates.append(port + 10000)  # Redis Cluster bus port.
    opened = []
    try:
        for candidate in candidates:
            sock = socket.socket()
            sock.bind(("127.0.0.1", candidate))
            opened.append(sock)
    except OSError:
        for sock in opened:
            sock.close()
        continue
    sockets.extend(opened)
    ports.append(port)
print(*ports, sep="\n")
PY
)
master_ports=("${ports[0]}" "${ports[1]}" "${ports[2]}")
lavik_port=${ports[3]}

for port in "${master_ports[@]}"; do
  node_dir=$case_dir/$port
  mkdir "$node_dir"
  "$redis_server" --port "$port" --cluster-enabled yes \
    --cluster-config-file nodes.conf --cluster-node-timeout 3000 \
    --appendonly no --save "" --dir "$node_dir" --daemonize no \
    >"$node_dir/redis.log" 2>&1 &
  redis_pids+=("$!")
done

for port in "${master_ports[@]}"; do
  for _ in {1..200}; do
    "$redis_cli" -p "$port" ping >/dev/null 2>&1 && break
    sleep 0.05
  done
  [[ $("$redis_cli" -p "$port" ping) == PONG ]]
done

printf 'yes\n' | "$redis_cli" --cluster create \
  "127.0.0.1:${master_ports[0]}" "127.0.0.1:${master_ports[1]}" \
  "127.0.0.1:${master_ports[2]}" --cluster-replicas 0 >/dev/null
for port in "${master_ports[@]}"; do
  for _ in {1..200}; do
    "$redis_cli" -p "$port" cluster info | tr -d '\r' | \
      grep -q '^cluster_state:ok$' && break
    sleep 0.05
  done
  "$redis_cli" -p "$port" cluster info | tr -d '\r' | \
    grep -q '^cluster_state:ok$'
done

[[ $("$redis_cli" -c -p "${master_ports[0]}" set '{a}baseline' one) == OK ]]
[[ $("$redis_cli" -c -p "${master_ports[0]}" set '{b}baseline' two) == OK ]]
[[ $("$redis_cli" -c -p "${master_ports[0]}" set '{c}baseline' three) == OK ]]
fullsync_function=$'#!lua name=redis_import_fullsync\nredis.register_function{function_name="redis_import_fullsync_value", callback=function(keys, args) return args[1] end, flags={"no-writes"}}'
for port in "${master_ports[@]}"; do
  [[ $("$redis_cli" -p "$port" function load replace "$fullsync_function") == redis_import_fullsync ]]
done

fallocate -l 128M "$case_dir/lavik.data"
# Keep three workers for cross-worker replication on two-CPU hosted runners.
"$lavik_bin" --logtostderr --port "$lavik_port" --threads 3 --no-pin-workers \
  --recv-buffers-per-worker 0 --max-memory 8589934592 --flush-max-ms 20 \
  --data-file "$case_dir/lavik.data" >"$case_dir/lavik.log" 2>&1 &
lavik_pid=$!
for _ in {1..1200}; do
  "$redis_cli" -p "$lavik_port" ping >/dev/null 2>&1 && break
  if ! kill -0 "$lavik_pid" 2>/dev/null; then
    wait "$lavik_pid" || lavik_status=$?
    lavik_pid=
    echo "Lavik exited before accepting connections (status ${lavik_status:-0})" >&2
    exit 1
  fi
  sleep 0.05
done
[[ $("$redis_cli" -p "$lavik_port" ping) == PONG ]]

"$redis_cli" -p "$lavik_port" replicaof 127.0.0.1 \
  "${master_ports[0]}" >/dev/null
overlap=$("$redis_cli" -p "$lavik_port" addreplicaof 127.0.0.1 \
  "${master_ports[0]}" 2>&1)
grep -q 'slot overlap' <<<"$overlap"
"$redis_cli" -p "$lavik_port" addreplicaof 127.0.0.1 \
  "${master_ports[1]}" >/dev/null
"$redis_cli" -p "$lavik_port" addreplicaof 127.0.0.1 \
  "${master_ports[2]}" >/dev/null

for _ in {1..400}; do
  "$redis_cli" -p "$lavik_port" info replication 2>/dev/null | \
    grep -q 'master_link_status:up' && break
  sleep 0.05
done
info=$("$redis_cli" -p "$lavik_port" info replication | tr -d '\r')
grep -q '^lavik_redis_sources:3$' <<<"$info"
grep -q '^master_link_status:up$' <<<"$info"

[[ $("$redis_cli" -p "$lavik_port" get '{a}baseline') == one ]]
[[ $("$redis_cli" -p "$lavik_port" get '{b}baseline') == two ]]
[[ $("$redis_cli" -p "$lavik_port" get '{c}baseline') == three ]]
[[ $("$redis_cli" -p "$lavik_port" fcall_ro redis_import_fullsync_value 0 baseline) == baseline ]]

for port in "${master_ports[@]}"; do
  master_info=$("$redis_cli" -p "$port" info replication | tr -d '\r')
  grep -q '^connected_slaves:1$' <<<"$master_info"
  grep -q "slave0:.*port=$lavik_port" <<<"$master_info"
done

"$redis_cli" -c -p "${master_ports[0]}" set '{a}online' A >/dev/null
"$redis_cli" -c -p "${master_ports[0]}" set '{b}online' B >/dev/null
"$redis_cli" -c -p "${master_ports[0]}" set '{c}online' C >/dev/null
incremental_function=$'#!lua name=redis_import_incremental\nredis.register_function{function_name="redis_import_incremental_value", callback=function(keys, args) return args[1] end, flags={"no-writes"}}'
for port in "${master_ports[@]}"; do
  [[ $("$redis_cli" -p "$port" function load replace "$incremental_function") == redis_import_incremental ]]
done
for pair in '{a}online A' '{b}online B' '{c}online C'; do
  read -r key value <<<"$pair"
  for _ in {1..200}; do
    [[ $("$redis_cli" -p "$lavik_port" get "$key" 2>/dev/null || true) == \
       "$value" ]] && break
    sleep 0.05
  done
  [[ $("$redis_cli" -p "$lavik_port" get "$key") == "$value" ]]
done
for _ in {1..200}; do
  [[ $("$redis_cli" -p "$lavik_port" fcall_ro redis_import_incremental_value 0 incremental 2>/dev/null || true) == incremental ]] && break
  sleep 0.05
done
[[ $("$redis_cli" -p "$lavik_port" fcall_ro redis_import_incremental_value 0 incremental) == incremental ]]

for port in "${master_ports[@]}"; do
  "$redis_cli" -p "$port" client kill type slave >/dev/null
done
"$redis_cli" -c -p "${master_ports[0]}" set '{a}resumed' RA >/dev/null
"$redis_cli" -c -p "${master_ports[0]}" set '{b}resumed' RB >/dev/null
"$redis_cli" -c -p "${master_ports[0]}" set '{c}resumed' RC >/dev/null
for pair in '{a}resumed RA' '{b}resumed RB' '{c}resumed RC'; do
  read -r key value <<<"$pair"
  for _ in {1..400}; do
    [[ $("$redis_cli" -p "$lavik_port" get "$key" 2>/dev/null || true) == \
       "$value" ]] && break
    sleep 0.05
  done
  [[ $("$redis_cli" -p "$lavik_port" get "$key") == "$value" ]]
done
for _ in {1..200}; do
  partial_count=$(grep -c 'Redis partial resynchronization continued' \
    "$case_dir/lavik.log" || true)
  ((partial_count >= 3)) && break
  sleep 0.05
done
((partial_count >= 3))
