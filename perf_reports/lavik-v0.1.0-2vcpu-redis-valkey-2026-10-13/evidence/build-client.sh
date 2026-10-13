#!/usr/bin/env bash
set -euo pipefail
cd /opt/bench2c/downloads
fetch() { curl -fL --retry 3 --connect-timeout 20 "$1" -o "$2"; }
fetch https://github.com/redis/redis/archive/5a693aaed0842c4eef21c706b79fa49938ca9f6c.tar.gz redis-source.tar.gz
mkdir redis
tar -xzf redis-source.tar.gz --strip-components=1 -C redis
make -C redis -j12 BUILD_TLS=yes
fetch https://github.com/valkey-io/valkey/archive/c9e8005e9d0ec817e26c7db318861cb821409249.tar.gz valkey-source.tar.gz
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
