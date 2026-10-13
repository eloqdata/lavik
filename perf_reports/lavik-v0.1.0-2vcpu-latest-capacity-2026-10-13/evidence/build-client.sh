#!/usr/bin/env bash
set -euo pipefail
cd /opt/bench2c/downloads
fetch() { curl -fL --retry 3 --connect-timeout 20 "$1" -o "$2"; }
fetch https://github.com/redis/redis/archive/498ecd0d6d007db11ddb3aea9428552598a78622.tar.gz redis-source.tar.gz
mkdir redis
tar -xzf redis-source.tar.gz --strip-components=1 -C redis
make -C redis -j12 BUILD_TLS=yes
fetch https://github.com/valkey-io/valkey/archive/7f1dffedff6de73058b2c2a389422b6ecd56c8fb.tar.gz valkey-source.tar.gz
mkdir valkey
tar -xzf valkey-source.tar.gz --strip-components=1 -C valkey
make -C valkey -j12 BUILD_TLS=yes
fetch https://github.com/RedisLabs/memtier_benchmark/archive/refs/tags/2.5.1.tar.gz memtier-source.tar.gz
mkdir memtier
tar -xzf memtier-source.tar.gz --strip-components=1 -C memtier
cd memtier
autoreconf -ivf
./configure
make -j12
sudo make install
cd ..
sha256sum redis-source.tar.gz valkey-source.tar.gz memtier-source.tar.gz redis/src/redis-server valkey/src/valkey-server /usr/local/bin/memtier_benchmark > build-sha256.txt
redis/src/redis-server --version
valkey/src/valkey-server --version
memtier_benchmark --version
touch /opt/bench2c/BUILD_READY
