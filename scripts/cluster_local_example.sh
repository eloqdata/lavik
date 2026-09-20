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

# Create and run a disposable local three-Meta, two-Group cluster. The manifest
# and process arguments are generated together so node IDs, ports, seeds, and
# storage paths cannot drift apart in the example.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/cluster_local_example.sh <init|up|create|status|down|bootstrap> \
  --root PATH [--bin-dir PATH] [--data-size SIZE]

init       Create a fresh manifest and four fresh Data files. Refuses an existing root.
up         Start Meta and Data processes from an initialized root.
create     Submit the one-time destructive cluster-create request and wait for READY.
status     Print cluster status.
down       Gracefully stop processes started from this root.
bootstrap  Run init, up, and create.
EOF
}

[[ $# -gt 0 ]] || { usage >&2; exit 2; }
action=$1
shift
root=''
bin_dir="$PWD/build"
data_size=1G
while (($#)); do
  case "$1" in
    --root) root=${2:?--root needs a path}; shift 2 ;;
    --bin-dir) bin_dir=${2:?--bin-dir needs a path}; shift 2 ;;
    --data-size) data_size=${2:?--data-size needs a size}; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done
case "$action" in init|up|create|status|down|bootstrap) ;; *) usage >&2; exit 2 ;; esac
[[ -n $root ]] || { printf '%s\n' '--root is required' >&2; exit 2; }

lavik_bin="$bin_dir/lavik"
meta_bin="$bin_dir/lavik-meta"
ctl_bin="$bin_dir/lavik-ctl"
node_ids=(
  1111111111111111111111111111111111111111
  2222222222222222222222222222222222222222
  3333333333333333333333333333333333333333
  4444444444444444444444444444444444444444
)

require_binaries() {
  for binary in "$lavik_bin" "$meta_bin" "$ctl_bin"; do
    [[ -x $binary ]] || { printf 'Missing executable: %s\n' "$binary" >&2; exit 1; }
  done
}

owns_pid() {
  local pid=$1
  [[ $pid =~ ^[0-9]+$ && -r /proc/$pid/cmdline ]] || return 1
  tr '\0' ' ' <"/proc/$pid/cmdline" | grep -Fq -- "$root"
}

write_manifest() {
  cat >"$root/cluster.toml" <<'EOF'
schema_version = 1
client_mode = "cluster"
slot_strategy = "contiguous-even"

[[meta_members]]
id = 1
raft_endpoint = "tcp://127.0.0.1:7101"
data_control_endpoint = "tcp://127.0.0.1:7301"
ctl_endpoint = "tcp://127.0.0.1:7201"

[[meta_members]]
id = 2
raft_endpoint = "tcp://127.0.0.1:7102"
data_control_endpoint = "tcp://127.0.0.1:7302"
ctl_endpoint = "tcp://127.0.0.1:7202"

[[meta_members]]
id = 3
raft_endpoint = "tcp://127.0.0.1:7103"
data_control_endpoint = "tcp://127.0.0.1:7303"
ctl_endpoint = "tcp://127.0.0.1:7203"

[[data_nodes]]
id = "1111111111111111111111111111111111111111"
client_endpoint = "tcp://127.0.0.1:6371"

[[data_nodes]]
id = "2222222222222222222222222222222222222222"
client_endpoint = "tcp://127.0.0.1:6372"

[[data_nodes]]
id = "3333333333333333333333333333333333333333"
client_endpoint = "tcp://127.0.0.1:6373"

[[data_nodes]]
id = "4444444444444444444444444444444444444444"
client_endpoint = "tcp://127.0.0.1:6374"

[[groups]]
id = "group-1"
primary = "1111111111111111111111111111111111111111"
replicas = ["2222222222222222222222222222222222222222"]

[[groups]]
id = "group-2"
primary = "3333333333333333333333333333333333333333"
replicas = ["4444444444444444444444444444444444444444"]
EOF
}

init() {
  [[ ! -e $root ]] || { printf 'Refusing to initialize existing path: %s\n' "$root" >&2; exit 1; }
  install -d -m 0700 "$root"
  for i in 1 2 3; do install -d -m 0700 "$root/meta-$i"; done
  for i in 1 2 3 4; do
    install -d -m 0700 "$root/data-$i"
    fallocate -l "$data_size" "$root/data-$i/lavik.data"
  done
  write_manifest
  printf 'Initialized %s\n' "$root"
}

write_pid() { printf '%s\n' "$2" >"$root/$1.pid"; }
start_meta() {
  local i=$1 manifest_arg=()
  [[ -e $root/meta-$i/cluster_config.dat ]] || manifest_arg=(--initial-cluster-manifest "$root/cluster.toml")
  "$meta_bin" --id "$i" --addr "127.0.0.1:$((7100 + i))" \
    --ctl-addr "127.0.0.1:$((7200 + i))" \
    --data-control-addr "127.0.0.1:$((7300 + i))" \
    --data-dir "$root/meta-$i" "${manifest_arg[@]}" \
    >"$root/meta-$i.log" 2>&1 &
  write_pid "meta-$i" "$!"
}

start_data() {
  local i=$1
  "$lavik_bin" --bind 127.0.0.1 --port "$((6370 + i))" \
    --threads 1 --no-pin-workers \
    --node-id "${node_ids[i-1]}" \
    --announce-ip 127.0.0.1 \
    --meta-seed 127.0.0.1:7301 \
    --meta-seed 127.0.0.1:7302 \
    --meta-seed 127.0.0.1:7303 \
    --data-file "$root/data-$i/lavik.data" --log-dir "$root/data-$i/logs" \
    >"$root/data-$i.log" 2>&1 &
  write_pid "data-$i" "$!"
}

wait_for_meta_leader() {
  local deadline=$((SECONDS + 30)) i output
  while ((SECONDS < deadline)); do
    for i in 1 2 3; do
      set +e
      output=$("$ctl_bin" --socket "$root/meta-$i/meta-admin.sock" status 2>&1)
      set -e
      [[ $output == *'leader=1'* ]] && return 0
    done
    sleep 1
  done
  printf '%s\n' 'No Meta leader became ready; inspect the logs under the root directory' >&2
  return 1
}

up() {
  [[ -f $root/cluster.toml ]] || { printf 'Run init first: %s\n' "$root" >&2; exit 1; }
  require_binaries
  local name pid
  for name in meta-1 meta-2 meta-3 data-1 data-2 data-3 data-4; do
    [[ -f $root/$name.pid ]] || continue
    pid=$(<"$root/$name.pid")
    owns_pid "$pid" && { printf 'Already running: %s (pid %s)\n' "$name" "$pid" >&2; exit 1; }
  done
  for i in 1 2 3; do start_meta "$i"; done
  wait_for_meta_leader
  for i in 1 2 3 4; do start_data "$i"; done
  printf 'Started processes. Check with: %s status --root %q\n' "$0" "$root"
}

wait_ready() {
  local deadline=$((SECONDS + 120)) output status_code
  while ((SECONDS < deadline)); do
    set +e
    output=$("$ctl_bin" cluster-status --socket "$root/meta-1/meta-admin.sock" 2>&1)
    status_code=$?
    set -e
    printf '%s\n' "$output"
    if ((status_code == 0)); then return 0; fi
    if ((status_code != 2 && status_code != 3)); then return "$status_code"; fi
    sleep 1
  done
  printf '%s\n' 'Timed out waiting for READY; inspect the logs under the root directory' >&2
  return 1
}

create() {
  require_binaries
  "$ctl_bin" cluster-create --manifest "$root/cluster.toml" \
    --socket "$root/meta-1/meta-admin.sock" --yes
  wait_ready
}

status() {
  require_binaries
  "$ctl_bin" cluster-status --socket "$root/meta-1/meta-admin.sock" --json
}

down() {
  local name pid deadline running
  for name in data-1 data-2 data-3 data-4 meta-1 meta-2 meta-3; do
    [[ -f $root/$name.pid ]] || continue
    pid=$(<"$root/$name.pid")
    owns_pid "$pid" && kill -TERM "$pid"
  done
  deadline=$((SECONDS + 60))
  while ((SECONDS < deadline)); do
    running=0
    for name in data-1 data-2 data-3 data-4 meta-1 meta-2 meta-3; do
      [[ -f $root/$name.pid ]] || continue
      pid=$(<"$root/$name.pid")
      owns_pid "$pid" && running=1
    done
    ((running == 0)) && { printf 'Stopped processes under %s\n' "$root"; return; }
    sleep 1
  done
  printf 'Timed out waiting for processes under %s to stop\n' "$root" >&2
  return 1
}

case "$action" in
  init) init ;;
  up) up ;;
  create) create ;;
  status) status ;;
  down) down ;;
  bootstrap) init; up; create ;;
esac
