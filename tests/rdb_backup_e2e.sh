#!/usr/bin/env bash
set -euo pipefail

keylane_bin=$1
redis_cli=$2
case_dir=$(mktemp -d /tmp/keylane-rdb-backup-e2e.XXXXXX)
source_pid=
import_pid=

cleanup() {
  if [[ -n ${source_pid} ]]; then kill -KILL "${source_pid}" 2>/dev/null || true; fi
  if [[ -n ${import_pid} ]]; then kill -KILL "${import_pid}" 2>/dev/null || true; fi
  wait 2>/dev/null || true
  rm -rf "${case_dir}"
}
trap cleanup EXIT

wait_ready() {
  local port=$1
  for _ in $(seq 1 1000); do
    if "${redis_cli}" -p "${port}" ping 2>/dev/null | grep -qx PONG; then
      return 0
    fi
    sleep 0.01
  done
  return 1
}

stop_server() {
  local pid=$1
  # The source data file is deliberately disposable; waiting for its ordinary
  # shutdown flush would test storage shutdown latency rather than RDB backup.
  kill -KILL "${pid}"
  wait "${pid}" 2>/dev/null || true
}

emit_sets() {
  local prefix=$1
  local first=$2
  local last=$3
  local value=$4
  local i key
  for i in $(seq "${first}" "${last}"); do
    key="${prefix}${i}"
    printf '*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' \
      "${#key}" "${key}" "${#value}" "${value}"
  done
}

emit_named_sets() {
  local value=$1
  shift
  local key
  for key in "$@"; do
    printf '*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' \
      "${#key}" "${key}" "${#value}" "${value}"
  done
}

source_port=16479
import_port=16480
truncate -s 1G "${case_dir}/source.data"
KEYLANE_RDB_CAPTURE_PAUSE_MS=500 \
"${keylane_bin}" --port "${source_port}" --threads 4 --no-pin-workers \
  --recv-buffers-per-worker 0 --data-file "${case_dir}/source.data" \
  --rdb-dir "${case_dir}" --dbfilename dump.rdb \
  >"${case_dir}/source.log" 2>&1 &
source_pid=$!
wait_ready "${source_port}"

emit_sets key: 1 5000 OLD | "${redis_cli}" -p "${source_port}" --pipe >/dev/null
head -c 10485760 /dev/zero | tr '\0' A |
  "${redis_cli}" -p "${source_port}" -x set external-big >/dev/null
"${redis_cli}" -p "${source_port}" mset tx-left OLD tx-right OLD >/dev/null
"${redis_cli}" -p "${source_port}" set race-trigger OLD >/dev/null
race_value=$(head -c 1048576 /dev/zero | tr '\0' R)
race_keys=(race-fill:2 race-fill:6 race-fill:10 race-fill:14 race-fill:18
  race-fill:23 race-fill:27 race-fill:32 race-fill:36 race-fill:41
  race-fill:45 race-fill:49)
# These keys and race-trigger have Redis slots congruent modulo four, so all
# writes share one worker's append stream without concentrating SCAN in a
# single logical partition.
emit_named_sets "${race_value}" "${race_keys[@]}" |
  "${redis_cli}" -p "${source_port}" --pipe >/dev/null

previous=$("${redis_cli}" -p "${source_port}" lastsave)
[[ $("${redis_cli}" -p "${source_port}" bgsave) == "Background saving started" ]]

# BGSAVE replies only after the global cut. Every operation below is therefore
# post-cut: overwrites/deletes must preserve old records, inserts must be absent,
# and the two-key transaction must appear entirely on one side of the cut.
"${redis_cli}" -p "${source_port}" set race-trigger NEW >/dev/null &
race_trigger_pid=$!
sleep 0.05
emit_named_sets "${race_value}" "${race_keys[@]}" |
  "${redis_cli}" -p "${source_port}" --pipe >/dev/null
wait "${race_trigger_pid}"
emit_sets key: 1 5000 NEW | "${redis_cli}" -p "${source_port}" --pipe >/dev/null
emit_sets after: 1 1000 AFTER | "${redis_cli}" -p "${source_port}" --pipe >/dev/null
"${redis_cli}" -p "${source_port}" del key:1 key:2 key:3 >/dev/null
"${redis_cli}" -p "${source_port}" set external-big NEW >/dev/null
printf 'MULTI\nSET tx-left NEW\nSET tx-right NEW\nEXEC\n' |
  "${redis_cli}" -p "${source_port}" >/dev/null

for _ in $(seq 1 3000); do
  current=$("${redis_cli}" -p "${source_port}" lastsave)
  if [[ ${current} != "${previous}" ]]; then break; fi
  sleep 0.01
done
[[ ${current} != "${previous}" ]]
[[ -s "${case_dir}/dump.rdb" ]]

stop_server "${source_pid}"
source_pid=

truncate -s 1G "${case_dir}/import.data"
"${keylane_bin}" --port "${import_port}" --threads 4 --no-pin-workers \
  --recv-buffers-per-worker 0 --data-file "${case_dir}/import.data" \
  --load-rdb "${case_dir}/dump.rdb" --rdb-dir "${case_dir}" \
  --dbfilename imported.rdb >"${case_dir}/import.log" 2>&1 &
import_pid=$!
wait_ready "${import_port}"

[[ $("${redis_cli}" -p "${import_port}" dbsize) == 5016 ]]
[[ $("${redis_cli}" -p "${import_port}" --scan --pattern 'after:*' | wc -l) == 0 ]]
values=$("${redis_cli}" -p "${import_port}" --scan --pattern 'key:*' |
  xargs -r -n 200 "${redis_cli}" -p "${import_port}" mget | sort | uniq -c)
[[ ${values} =~ 5000[[:space:]]+OLD ]]
[[ $("${redis_cli}" -p "${import_port}" strlen external-big) == 10485760 ]]
[[ $("${redis_cli}" -p "${import_port}" getrange external-big 0 0) == A ]]
[[ $("${redis_cli}" -p "${import_port}" getrange external-big -1 -1) == A ]]
[[ $("${redis_cli}" -p "${import_port}" get tx-left) == OLD ]]
[[ $("${redis_cli}" -p "${import_port}" get tx-right) == OLD ]]
[[ $("${redis_cli}" -p "${import_port}" get race-trigger) == OLD ]]
[[ $("${redis_cli}" -p "${import_port}" --scan --pattern 'race-fill:*' |
  wc -l) == 12 ]]

stop_server "${import_pid}"
import_pid=
