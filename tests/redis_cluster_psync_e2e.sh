#!/usr/bin/env bash
set -euo pipefail

keylane_bin=$1
redis_server=$2
redis_cli=$3
case_dir=$(mktemp -d /tmp/keylane-redis-cluster-e2e-XXXXXX)
redis_pids=()
keylane_pid=

cleanup() {
  status=$?
  if ((status != 0)); then
    echo "Redis Cluster PSYNC test failed; Keylane log follows:" >&2
    tail -100 "$case_dir/keylane.log" >&2 2>/dev/null || true
    for redis_log in "$case_dir"/*/redis.log; do
      [[ -e $redis_log ]] || continue
      echo "Redis log $redis_log follows:" >&2
      tail -100 "$redis_log" >&2 2>/dev/null || true
    done
  fi
  if [[ -n $keylane_pid ]]; then
    kill "$keylane_pid" 2>/dev/null || true
    wait "$keylane_pid" 2>/dev/null || true
  fi
  if ((${#redis_pids[@]})); then
    kill "${redis_pids[@]}" 2>/dev/null || true
    wait "${redis_pids[@]}" 2>/dev/null || true
  fi
  if [[ $case_dir == /tmp/keylane-redis-cluster-e2e-* ]]; then
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
keylane_port=${ports[3]}

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
for _ in {1..200}; do
  "$redis_cli" -p "${master_ports[0]}" cluster info | tr -d '\r' | \
    grep -q '^cluster_state:ok$' && break
  sleep 0.05
done

"$redis_cli" -c -p "${master_ports[0]}" set '{a}baseline' one >/dev/null
"$redis_cli" -c -p "${master_ports[0]}" set '{b}baseline' two >/dev/null
"$redis_cli" -c -p "${master_ports[0]}" set '{c}baseline' three >/dev/null

fallocate -l 128M "$case_dir/keylane.data"
"$keylane_bin" --port "$keylane_port" --threads 3 \
  --recv-buffers-per-worker 0 --max-memory 8589934592 --flush-max-ms 20 \
  --data-file "$case_dir/keylane.data" >"$case_dir/keylane.log" 2>&1 &
keylane_pid=$!
for _ in {1..1200}; do
  "$redis_cli" -p "$keylane_port" ping >/dev/null 2>&1 && break
  if ! kill -0 "$keylane_pid" 2>/dev/null; then
    wait "$keylane_pid" || keylane_status=$?
    keylane_pid=
    echo "Keylane exited before accepting connections (status ${keylane_status:-0})" >&2
    exit 1
  fi
  sleep 0.05
done
[[ $("$redis_cli" -p "$keylane_port" ping) == PONG ]]

"$redis_cli" -p "$keylane_port" replicaof 127.0.0.1 \
  "${master_ports[0]}" >/dev/null
overlap=$("$redis_cli" -p "$keylane_port" addreplicaof 127.0.0.1 \
  "${master_ports[0]}" 2>&1)
grep -q 'slot overlap' <<<"$overlap"
"$redis_cli" -p "$keylane_port" addreplicaof 127.0.0.1 \
  "${master_ports[1]}" >/dev/null
"$redis_cli" -p "$keylane_port" addreplicaof 127.0.0.1 \
  "${master_ports[2]}" >/dev/null

for _ in {1..400}; do
  "$redis_cli" -p "$keylane_port" info replication 2>/dev/null | \
    grep -q 'master_link_status:up' && break
  sleep 0.05
done
info=$("$redis_cli" -p "$keylane_port" info replication | tr -d '\r')
grep -q '^keylane_redis_sources:3$' <<<"$info"
grep -q '^master_link_status:up$' <<<"$info"

[[ $("$redis_cli" -p "$keylane_port" get '{a}baseline') == one ]]
[[ $("$redis_cli" -p "$keylane_port" get '{b}baseline') == two ]]
[[ $("$redis_cli" -p "$keylane_port" get '{c}baseline') == three ]]

for port in "${master_ports[@]}"; do
  master_info=$("$redis_cli" -p "$port" info replication | tr -d '\r')
  grep -q '^connected_slaves:1$' <<<"$master_info"
  grep -q "slave0:.*port=$keylane_port" <<<"$master_info"
done

"$redis_cli" -c -p "${master_ports[0]}" set '{a}online' A >/dev/null
"$redis_cli" -c -p "${master_ports[0]}" set '{b}online' B >/dev/null
"$redis_cli" -c -p "${master_ports[0]}" set '{c}online' C >/dev/null
for pair in '{a}online A' '{b}online B' '{c}online C'; do
  read -r key value <<<"$pair"
  for _ in {1..200}; do
    [[ $("$redis_cli" -p "$keylane_port" get "$key" 2>/dev/null || true) == \
       "$value" ]] && break
    sleep 0.05
  done
  [[ $("$redis_cli" -p "$keylane_port" get "$key") == "$value" ]]
done

for port in "${master_ports[@]}"; do
  "$redis_cli" -p "$port" client kill type slave >/dev/null
done
"$redis_cli" -c -p "${master_ports[0]}" set '{a}resumed' RA >/dev/null
"$redis_cli" -c -p "${master_ports[0]}" set '{b}resumed' RB >/dev/null
"$redis_cli" -c -p "${master_ports[0]}" set '{c}resumed' RC >/dev/null
for pair in '{a}resumed RA' '{b}resumed RB' '{c}resumed RC'; do
  read -r key value <<<"$pair"
  for _ in {1..400}; do
    [[ $("$redis_cli" -p "$keylane_port" get "$key" 2>/dev/null || true) == \
       "$value" ]] && break
    sleep 0.05
  done
  [[ $("$redis_cli" -p "$keylane_port" get "$key") == "$value" ]]
done
for _ in {1..200}; do
  partial_count=$(grep -c 'Redis partial resynchronization continued' \
    "$case_dir/keylane.log" || true)
  ((partial_count >= 3)) && break
  sleep 0.05
done
((partial_count >= 3))
