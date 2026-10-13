#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y build-essential pkg-config autoconf automake libtool libevent-dev libssl-dev zlib1g-dev libpcre2-dev libnuma1 libuuid1 nvme-cli pciutils fio sysstat ethtool redis-tools curl git python3 jq
systemctl disable --now redis-server valkey 2>/dev/null || true
install -d -o azureuser -g azureuser /opt/bench2c /opt/bench2c/downloads /opt/bench2c/results
printf 'ready\n' > /opt/bench2c/BASE_READY
