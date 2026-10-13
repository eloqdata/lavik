#!/bin/bash
set -euo pipefail
root=/mnt/dev/benchmark/release010
while ! test -f "$root/memory/COMPLETE"; do
 state=$(systemctl show release010-memory -p ActiveState --value)
 test "$state" = active || { echo "GET/SET did not complete: $state"; exit 1; }
 sleep 15
done
export PATH="$root/ycsb:$PATH"
if ! python3 -m venv "$root/venv"; then
 export DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=l
 apt-get update
 apt-get install -y python3-venv
 python3 -m venv "$root/venv"
fi
"$root/venv/bin/pip" install aerospike > "$root/ycsb/python-dependencies.log" 2>&1
sudo -u azureuser ssh -i "$root/client-control" -o BatchMode=yes azureuser@10.84.0.5 'sudo mkdir -p /opt/benchmark/release010/ycsb-aerospike-dist; sudo tar -xzf /opt/benchmark/release010/ycsb-source/aerospike/target/ycsb-aerospike-binding-0.18.0-SNAPSHOT.tar.gz --strip-components=1 -C /opt/benchmark/release010/ycsb-aerospike-dist; sha256sum /opt/benchmark/release010/ycsb-aerospike-dist/lib/*.jar /opt/benchmark/ycsb-pr318-dist/lib/*.jar' > "$root/ycsb/client-jars.sha256"
"$root/venv/bin/pip" freeze > "$root/ycsb/python-dependencies.txt"
python3 "$root/ycsb/provision.py"
cd "$root/ycsb"
python3 -c 'import host_policy; host_policy.initialize()'
"$root/venv/bin/python" "$root/ycsb/runner.py"
