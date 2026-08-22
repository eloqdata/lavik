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

source_port=16479
import_port=16480
truncate -s 1G "${case_dir}/source.data"
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

previous=$("${redis_cli}" -p "${source_port}" lastsave)
[[ $("${redis_cli}" -p "${source_port}" bgsave) == "Background saving started" ]]

# BGSAVE replies only after the global cut. Every operation below is therefore
# post-cut: overwrites/deletes must preserve old records, inserts must be absent,
# and the two-key transaction must appear entirely on one side of the cut.
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

[[ $("${redis_cli}" -p "${import_port}" dbsize) == 5003 ]]
[[ $("${redis_cli}" -p "${import_port}" --scan --pattern 'after:*' | wc -l) == 0 ]]
values=$("${redis_cli}" -p "${import_port}" --scan --pattern 'key:*' |
  xargs -r -n 200 "${redis_cli}" -p "${import_port}" mget | sort | uniq -c)
[[ ${values} =~ 5000[[:space:]]+OLD ]]
[[ $("${redis_cli}" -p "${import_port}" strlen external-big) == 10485760 ]]
[[ $("${redis_cli}" -p "${import_port}" getrange external-big 0 0) == A ]]
[[ $("${redis_cli}" -p "${import_port}" getrange external-big -1 -1) == A ]]
[[ $("${redis_cli}" -p "${import_port}" get tx-left) == OLD ]]
[[ $("${redis_cli}" -p "${import_port}" get tx-right) == OLD ]]

stop_server "${import_pid}"
import_pid=
