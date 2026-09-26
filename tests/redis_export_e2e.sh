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
test_mode=${4:-normal}
case ${test_mode} in
  normal) crash_point=; disk_one_block=; scan_pause_ms=800 ;;
  crash) crash_point=redis_export_backlog_stopped; disk_one_block=; scan_pause_ms=800 ;;
  full) crash_point=; disk_one_block=1; scan_pause_ms=5000 ;;
  *) echo "unknown test mode: ${test_mode}" >&2; exit 2 ;;
esac
case_template=${LAVIK_TEST_DATA_DIR:-/tmp}/lavik-redis-export-e2e-XXXXXX
case_dir=$(mktemp -d "${case_template}")
lavik_pid=
redis_pid=

cleanup() {
  status=$?
  if ((status != 0)); then
    tail -100 "${case_dir}/lavik.log" >&2 2>/dev/null || true
    tail -100 "${case_dir}/redis.log" >&2 2>/dev/null || true
  fi
  [[ -z ${redis_pid} ]] || kill "${redis_pid}" 2>/dev/null || true
  [[ -z ${lavik_pid} ]] || kill "${lavik_pid}" 2>/dev/null || true
  [[ -z ${redis_pid} ]] || wait "${redis_pid}" 2>/dev/null || true
  [[ -z ${lavik_pid} ]] || wait "${lavik_pid}" 2>/dev/null || true
  if [[ ${case_dir} == "${case_template%XXXXXX}"* ]]; then
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
lavik_port=${ports[0]}
redis_port=${ports[1]}

fallocate -l 256M "${case_dir}/lavik.data"
LAVIK_RDB_SCAN_PAUSE_MS="${scan_pause_ms}" \
LAVIK_CRASH_POINT="${crash_point}" \
LAVIK_TEST_REDIS_EXPORT_ONE_BLOCK="${disk_one_block}" \
"${lavik_bin}" --logtostderr --port "${lavik_port}" --threads 4 --no-pin-workers \
  --recv-buffers-per-worker 0 --max-memory 4294967296 \
  --data-file "${case_dir}/lavik.data" \
  >"${case_dir}/lavik.log" 2>&1 &
lavik_pid=$!
"${redis_server}" --port "${redis_port}" --save '' --appendonly no \
  --dir "${case_dir}" --daemonize no >"${case_dir}/redis.log" 2>&1 &
redis_pid=$!

for _ in {1..600}; do
  "${redis_cli}" -p "${lavik_port}" ping >/dev/null 2>&1 && \
    "${redis_cli}" -p "${redis_port}" ping >/dev/null 2>&1 && break
  sleep 0.05
done
[[ $("${redis_cli}" -p "${lavik_port}" ping) == PONG ]]
[[ $("${redis_cli}" -p "${redis_port}" ping) == PONG ]]

"${redis_cli}" -p "${lavik_port}" set baseline one >/dev/null
"${redis_cli}" -p "${lavik_port}" hset hash field value >/dev/null
"${redis_cli}" -p "${lavik_port}" rpush list a b c >/dev/null
"${redis_cli}" -p "${lavik_port}" sadd set x y >/dev/null
"${redis_cli}" -p "${lavik_port}" zadd zset 1 one 2 two >/dev/null
"${redis_cli}" -p "${lavik_port}" -n 1 set db-one before >/dev/null
# Force the snapshot scan through its asynchronous out-of-index-key path.
external_key=$(printf '%05000d' 0)
"${redis_cli}" -p "${lavik_port}" set "${external_key}" external >/dev/null
fullsync_function=$'#!lua name=redis_export_fullsync\nredis.register_function{function_name="redis_export_fullsync_value", callback=function(keys, args) return args[1] end, flags={"no-writes"}}'
[[ $("${redis_cli}" -p "${lavik_port}" function load "${fullsync_function}") == redis_export_fullsync ]]
"${redis_cli}" -p "${redis_port}" replicaof 127.0.0.1 \
  "${lavik_port}" >/dev/null

# Wait for the snapshot cut, then overwrite a key before the paused RDB scan
# can read it. The disk stream must replay this command after the old image.
for _ in {1..600}; do
  grep -q 'began RDB and disk capture' "${case_dir}/lavik.log" && break
  sleep 0.01
done
grep -q 'began RDB and disk capture' "${case_dir}/lavik.log"
"${redis_cli}" -p "${lavik_port}" set baseline during-rdb >/dev/null
"${redis_cli}" -p "${lavik_port}" mset overlap-a A overlap-b B >/dev/null
if [[ ${test_mode} == normal ]]; then
  # Two large writes cross an 8 MiB export-block boundary before RDB ends.
  python3 -c 'import sys; sys.stdout.write("x" * 5000000)' | \
    "${redis_cli}" -p "${lavik_port}" -x set disk-spanning-a >/dev/null
  python3 -c 'import sys; sys.stdout.write("y" * 5000000)' | \
    "${redis_cli}" -p "${lavik_port}" -x set disk-spanning-b >/dev/null
fi

if [[ ${test_mode} == full ]]; then
  python3 -c 'import sys; sys.stdout.write("x" * 5000000)' | \
    "${redis_cli}" -p "${lavik_port}" -x set disk-full-a >/dev/null
  python3 -c 'import sys; sys.stdout.write("y" * 5000000)' | \
    "${redis_cli}" -p "${lavik_port}" -x set disk-full-b >/dev/null
  for _ in {1..600}; do
    grep -q 'Redis export disk backlog capacity exhausted' \
      "${case_dir}/lavik.log" && break
    sleep 0.01
  done
  grep -q 'Redis export disk backlog capacity exhausted' \
    "${case_dir}/lavik.log"
  [[ $(timeout 5 "${redis_cli}" -p "${lavik_port}" set after-disk-full okay) == OK ]]
  [[ $("${redis_cli}" -p "${lavik_port}" get after-disk-full) == okay ]]
  exit 0
fi

if [[ ${test_mode} == crash ]]; then
  for _ in {1..600}; do
    process_state=$(ps -o stat= -p "${lavik_pid}" 2>/dev/null || true)
    [[ -z ${process_state} || ${process_state} == Z* ]] && break
    sleep 0.05
  done
  process_state=$(ps -o stat= -p "${lavik_pid}" 2>/dev/null || true)
  [[ -z ${process_state} || ${process_state} == Z* ]]
  crash_status=0
  wait "${lavik_pid}" || crash_status=$?
  lavik_pid=
  [[ ${crash_status} == 86 ]]
  kill "${redis_pid}" 2>/dev/null || true
  wait "${redis_pid}" 2>/dev/null || true
  redis_pid=
  "${lavik_bin}" --logtostderr --port "${lavik_port}" --threads 4 \
    --no-pin-workers --recv-buffers-per-worker 0 --max-memory 4294967296 \
    --data-file "${case_dir}/lavik.data" \
    >"${case_dir}/restarted.log" 2>&1 &
  lavik_pid=$!
  for _ in {1..600}; do
    "${redis_cli}" -p "${lavik_port}" ping >/dev/null 2>&1 && break
    sleep 0.05
  done
  [[ $("${redis_cli}" -p "${lavik_port}" ping) == PONG ]]
  [[ $("${redis_cli}" -p "${lavik_port}" set after-restart okay) == OK ]]
  [[ $("${redis_cli}" -p "${lavik_port}" get after-restart) == okay ]]
  exit 0
fi

for _ in {1..600}; do
  "${redis_cli}" -p "${redis_port}" info replication 2>/dev/null | \
    tr -d '\r' | grep -q '^master_link_status:up$' && break
  sleep 0.05
done
"${redis_cli}" -p "${redis_port}" info replication | tr -d '\r' | \
  grep -q '^master_link_status:up$'
[[ $("${redis_cli}" -p "${redis_port}" get baseline) == during-rdb ]]
[[ $("${redis_cli}" -p "${redis_port}" mget overlap-a overlap-b | tr '\n' ' ') == 'A B ' ]]
[[ $("${redis_cli}" -p "${redis_port}" strlen disk-spanning-a) == 5000000 ]]
[[ $("${redis_cli}" -p "${redis_port}" strlen disk-spanning-b) == 5000000 ]]
[[ $("${redis_cli}" -p "${redis_port}" hget hash field) == value ]]
[[ $("${redis_cli}" -p "${redis_port}" llen list) == 3 ]]
[[ $("${redis_cli}" -p "${redis_port}" scard set) == 2 ]]
[[ $("${redis_cli}" -p "${redis_port}" zcard zset) == 2 ]]
[[ $("${redis_cli}" -p "${redis_port}" -n 1 get db-one) == before ]]
[[ $("${redis_cli}" -p "${redis_port}" get "${external_key}") == external ]]
[[ $("${redis_cli}" -p "${redis_port}" fcall_ro redis_export_fullsync_value 0 baseline) == baseline ]]
grep -Eq 'Redis PSYNC disk backlog session=.*blocks=[2-9]' "${case_dir}/lavik.log"

"${redis_cli}" -p "${lavik_port}" set online two >/dev/null
"${redis_cli}" -p "${lavik_port}" mset tx-a A tx-b B >/dev/null
incremental_function=$'#!lua name=redis_export_incremental\nredis.register_function{function_name="redis_export_incremental_value", callback=function(keys, args) return args[1] end, flags={"no-writes"}}'
[[ $("${redis_cli}" -p "${lavik_port}" function load "${incremental_function}") == redis_export_incremental ]]
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
for _ in {1..200}; do
  [[ $("${redis_cli}" -p "${redis_port}" fcall_ro redis_export_incremental_value 0 incremental 2>/dev/null || true) == incremental ]] && break
  sleep 0.05
done
[[ $("${redis_cli}" -p "${redis_port}" fcall_ro redis_export_incremental_value 0 incremental) == incremental ]]

printf 'MULTI\nSET {a}exec first\nSET {b}exec second\nEXEC\n' | \
  "${redis_cli}" -p "${lavik_port}" >/dev/null
for _ in {1..200}; do
  [[ $("${redis_cli}" -p "${redis_port}" mget '{a}exec' '{b}exec' | \
    tr '\n' ' ') == 'first second ' ]] && break
  sleep 0.05
done
[[ $("${redis_cli}" -p "${redis_port}" mget '{a}exec' '{b}exec' | \
  tr '\n' ' ') == 'first second ' ]]

# A Lavik-only replacement must export standard Redis commands atomically,
# including source absolute TTL. Redis itself must never see the extension.
"${redis_cli}" -p "${lavik_port}" hset replace-hash old value retained old >/dev/null
"${redis_cli}" -p "${lavik_port}" expire replace-hash 3600 >/dev/null
replace_expiry=$("${redis_cli}" -p "${lavik_port}" pexpiretime replace-hash)
[[ $("${redis_cli}" -p "${lavik_port}" LAVIK.HREPLACE replace-hash only new) == OK ]]
for _ in {1..200}; do
  [[ $("${redis_cli}" -p "${redis_port}" hget replace-hash only) == new ]] && break
  sleep 0.05
done
[[ $("${redis_cli}" -p "${redis_port}" hget replace-hash only) == new ]]
[[ $("${redis_cli}" -p "${redis_port}" hlen replace-hash) == 1 ]]
[[ $("${redis_cli}" -p "${redis_port}" pexpiretime replace-hash) == "$replace_expiry" ]]
printf 'MULTI\nLAVIK.HREPLACE replace-hash final image\nHMSET replace-hash tail value\nEXEC\n' | \
  "${redis_cli}" -p "${lavik_port}" >/dev/null
for _ in {1..200}; do
  [[ $("${redis_cli}" -p "${redis_port}" hget replace-hash tail) == value ]] && break
  sleep 0.05
done
[[ $("${redis_cli}" -p "${redis_port}" hget replace-hash final) == image ]]
[[ $("${redis_cli}" -p "${redis_port}" hget replace-hash tail) == value ]]
[[ $("${redis_cli}" -p "${redis_port}" hlen replace-hash) == 2 ]]
[[ $("${redis_cli}" -p "${redis_port}" pexpiretime replace-hash) == "$replace_expiry" ]]

# Keep the link online long enough for Redis to send periodic REPLCONF ACKs.
sleep 3
"${redis_cli}" -p "${redis_port}" info replication | tr -d '\r' | \
  grep -q '^master_link_status:up$'
"${redis_cli}" -p "${lavik_port}" flushdb >/dev/null
for _ in {1..200}; do
  [[ $("${redis_cli}" -p "${redis_port}" dbsize) == 0 ]] && break
  sleep 0.05
done
[[ $("${redis_cli}" -p "${redis_port}" dbsize) == 0 ]]
"${redis_cli}" -p "${redis_port}" replicaof no one >/dev/null
"${redis_cli}" -p "${redis_port}" set detached writable >/dev/null
[[ $("${redis_cli}" -p "${redis_port}" get detached) == writable ]]
# Ordinary RDB streaming, online replication, ACKs, and an intentional detach
# must never be reported as an online-write backlog overrun.
! grep -q 'Redis replica fell behind online writes' "${case_dir}/lavik.log"
