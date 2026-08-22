#!/usr/bin/env bash
set -euo pipefail

keylane_bin=$1
redis_server=$2
redis_cli=$3
extra_args=()
case ${4:-} in
  '') ;;
  backpressure) extra_args+=(--redis-export-backpressure) ;;
  *) echo "unknown test mode: $4" >&2; exit 2 ;;
esac
case_dir=$(mktemp -d /tmp/keylane-redis-export-e2e-XXXXXX)
keylane_pid=
redis_pid=

cleanup() {
  status=$?
  if ((status != 0)); then
    tail -100 "${case_dir}/keylane.log" >&2 2>/dev/null || true
    tail -100 "${case_dir}/redis.log" >&2 2>/dev/null || true
  fi
  [[ -z ${redis_pid} ]] || kill "${redis_pid}" 2>/dev/null || true
  [[ -z ${keylane_pid} ]] || kill "${keylane_pid}" 2>/dev/null || true
  [[ -z ${redis_pid} ]] || wait "${redis_pid}" 2>/dev/null || true
  [[ -z ${keylane_pid} ]] || wait "${keylane_pid}" 2>/dev/null || true
  if [[ ${case_dir} == /tmp/keylane-redis-export-e2e-* ]]; then
    rm -rf -- "${case_dir}"
  fi
}
trap cleanup EXIT

readarray -t ports < <(python3 - <<'PY'
import socket
sockets = []
for _ in range(2):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sockets.append(sock)
for sock in sockets:
    print(sock.getsockname()[1])
PY
)
keylane_port=${ports[0]}
redis_port=${ports[1]}

fallocate -l 256M "${case_dir}/keylane.data"
"${keylane_bin}" --port "${keylane_port}" --threads 4 --no-pin-workers \
  --recv-buffers-per-worker 0 --max-memory 4294967296 \
  --data-file "${case_dir}/keylane.data" \
  "${extra_args[@]}" \
  >"${case_dir}/keylane.log" 2>&1 &
keylane_pid=$!
"${redis_server}" --port "${redis_port}" --save '' --appendonly no \
  --dir "${case_dir}" --daemonize no >"${case_dir}/redis.log" 2>&1 &
redis_pid=$!

for _ in {1..600}; do
  "${redis_cli}" -p "${keylane_port}" ping >/dev/null 2>&1 && \
    "${redis_cli}" -p "${redis_port}" ping >/dev/null 2>&1 && break
  sleep 0.05
done
[[ $("${redis_cli}" -p "${keylane_port}" ping) == PONG ]]
[[ $("${redis_cli}" -p "${redis_port}" ping) == PONG ]]

"${redis_cli}" -p "${keylane_port}" set baseline one >/dev/null
"${redis_cli}" -p "${keylane_port}" hset hash field value >/dev/null
"${redis_cli}" -p "${keylane_port}" rpush list a b c >/dev/null
"${redis_cli}" -p "${keylane_port}" sadd set x y >/dev/null
"${redis_cli}" -p "${keylane_port}" zadd zset 1 one 2 two >/dev/null
"${redis_cli}" -p "${keylane_port}" -n 1 set db-one before >/dev/null
"${redis_cli}" -p "${redis_port}" replicaof 127.0.0.1 \
  "${keylane_port}" >/dev/null

for _ in {1..600}; do
  "${redis_cli}" -p "${redis_port}" info replication 2>/dev/null | \
    tr -d '\r' | grep -q '^master_link_status:up$' && break
  sleep 0.05
done
"${redis_cli}" -p "${redis_port}" info replication | tr -d '\r' | \
  grep -q '^master_link_status:up$'
[[ $("${redis_cli}" -p "${redis_port}" get baseline) == one ]]
[[ $("${redis_cli}" -p "${redis_port}" hget hash field) == value ]]
[[ $("${redis_cli}" -p "${redis_port}" llen list) == 3 ]]
[[ $("${redis_cli}" -p "${redis_port}" scard set) == 2 ]]
[[ $("${redis_cli}" -p "${redis_port}" zcard zset) == 2 ]]
[[ $("${redis_cli}" -p "${redis_port}" -n 1 get db-one) == before ]]

"${redis_cli}" -p "${keylane_port}" set online two >/dev/null
"${redis_cli}" -p "${keylane_port}" mset tx-a A tx-b B >/dev/null
for _ in {1..200}; do
  [[ $("${redis_cli}" -p "${redis_port}" get online 2>/dev/null || true) == \
     two ]] && \
    [[ $("${redis_cli}" -p "${redis_port}" mget tx-a tx-b 2>/dev/null | \
      tr '\n' ' ') == 'A B ' ]] && break
  sleep 0.05
done
[[ $("${redis_cli}" -p "${redis_port}" get online) == two ]]
[[ $("${redis_cli}" -p "${redis_port}" mget tx-a tx-b | tr '\n' ' ') == \
   'A B ' ]]

printf 'MULTI\nSET {a}exec first\nSET {b}exec second\nEXEC\n' | \
  "${redis_cli}" -p "${keylane_port}" >/dev/null
for _ in {1..200}; do
  [[ $("${redis_cli}" -p "${redis_port}" mget '{a}exec' '{b}exec' | \
    tr '\n' ' ') == 'first second ' ]] && break
  sleep 0.05
done
[[ $("${redis_cli}" -p "${redis_port}" mget '{a}exec' '{b}exec' | \
  tr '\n' ' ') == 'first second ' ]]

# Keep the link online long enough for Redis to send periodic REPLCONF ACKs.
sleep 3
"${redis_cli}" -p "${redis_port}" info replication | tr -d '\r' | \
  grep -q '^master_link_status:up$'
"${redis_cli}" -p "${keylane_port}" flushdb >/dev/null
for _ in {1..200}; do
  [[ $("${redis_cli}" -p "${redis_port}" dbsize) == 0 ]] && break
  sleep 0.05
done
[[ $("${redis_cli}" -p "${redis_port}" dbsize) == 0 ]]
"${redis_cli}" -p "${redis_port}" replicaof no one >/dev/null
"${redis_cli}" -p "${redis_port}" set detached writable >/dev/null
[[ $("${redis_cli}" -p "${redis_port}" get detached) == writable ]]
