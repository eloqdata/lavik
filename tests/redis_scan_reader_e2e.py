#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Export Single and Meta Cluster keyspaces using real RedisShake ScanReader."""

from contextlib import ExitStack
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import redis_follower_smoke as S
import gate_cluster_create as C
import harness as H
from gate_data_control import DataProcess

PASSWORD = "scan-source-secret"


def seed(client, tag):
    prefix = "{" + tag + "}"
    assert client.call("SET", prefix + "string", "value") == "OK"
    assert client.call("RPUSH", prefix + "list", "first", "second") == 2
    assert client.call("HSET", prefix + "hash", "field", "value") == 1
    assert client.call("SADD", prefix + "set", "a", "b") == 2
    assert client.call("ZADD", prefix + "zset", 1, "a", 2, "b") == 2
    assert client.call("XADD", prefix + "stream", "1-0", "field", "value") == "1-0"
    assert client.call("SET", prefix + "ttl", "expires", "PX", 60000) == "OK"
    return prefix


def verify(client, prefix):
    assert client.call("GET", prefix + "string") == "value"
    assert client.call("PTTL", prefix + "string") == -1
    assert client.call("LRANGE", prefix + "list", 0, -1) == ["first", "second"]
    assert client.call("HGET", prefix + "hash", "field") == "value"
    assert sorted(client.call("SMEMBERS", prefix + "set")) == ["a", "b"]
    assert client.call("ZRANGE", prefix + "zset", 0, -1, "WITHSCORES") == [
        "a",
        "1",
        "b",
        "2",
    ]
    assert client.call("XRANGE", prefix + "stream", "-", "+") == [
        ["1-0", ["field", "value"]]
    ]
    assert client.call("GET", prefix + "ttl") == "expires"
    assert 0 < client.call("PTTL", prefix + "ttl") <= 60000


def scan(shake, directory, source, target, cluster=False, password=PASSWORD):
    """Use the supported one-shot scan path, without replication or KSN."""
    directory.mkdir()
    config = directory / "shake.toml"
    config.write_text(f'''[scan_reader]
cluster = {str(cluster).lower()}
address = "127.0.0.1:{source}"
password = "{password}"
scan = true
ksn = false
count = 16

[redis_writer]
address = "127.0.0.1:{target}"

[advanced]
dir = "{directory / "data"}"
log_file = "{directory / "shake.log"}"
rdb_restore_command_behavior = "rewrite"
''')
    result = subprocess.run(
        [shake, str(config)], cwd=directory, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stdout + result.stderr


def standalone(lavik, redis, shake, root):
    with (
        S.process(
            lavik,
            root / "single",
            "source",
            extra=("--requirepass", PASSWORD),
            password=PASSWORD,
        ) as (source, source_port, _),
        S.process(redis, root / "single-target", "target", redis=True) as (
            target,
            target_port,
            _,
        ),
    ):
        source.call("SELECT", 0)
        db0 = seed(source, "db0")
        source.call("SELECT", 15)
        db15 = seed(source, "db15")
        S.reject(source, ("PSYNC", "?", "-1"), "unknown command")
        scan(shake, root / "single-scan", source_port, target_port)
        verify(target, db0)
        target.call("SELECT", 15)
        verify(target, db15)
        assert target.call("DBSIZE") == 7


def managed_cluster(lavik, redis, shake, meta_binary, ctl, root):
    C.CTL = ctl
    directory = root / "cluster"
    directory.mkdir()
    meta = H.Node(meta_binary, str(directory), 1, args=C.creation_raft_args())
    nodes = [
        DataProcess(
            lavik,
            str(directory / f"source-{i}"),
            str(i + 1) * 40,
            meta.data_control_endpoint,
            workers=2,
        )
        for i in range(2)
    ]
    lines = [
        "schema_version = 1",
        'client_mode = "cluster"',
        'slot_strategy = "contiguous-even"',
    ]
    lines += C.meta_manifest_lines(meta)
    for i, node in enumerate(nodes):
        lines += [
            "[[data_nodes]]",
            f'id = "{node.node_id}"',
            f'client_endpoint = "{node.advertised_endpoint}"',
        ]
        lines += ["[[groups]]", f'id = "scan-{i}"', f'primary = "{node.node_id}"']
    manifest = directory / "cluster.toml"
    manifest.write_text("\n".join(lines) + "\n")
    try:
        meta.start(initial_cluster_manifest=str(manifest))
        meta.wait_leader()
        for node in nodes:
            node.start()
        C.command(
            os.environ.copy(),
            [
                ctl,
                "cluster-create",
                "--manifest",
                str(manifest),
                "--socket",
                meta.ctl_path,
                "--yes",
            ],
        )
        C.wait_cluster_ready(meta, "scan source cluster ready", 30)
        with ExitStack() as cleanup:
            prefixes = []
            for i, node in enumerate(nodes):
                client = S.Client(node.redis_port)
                cleanup.callback(client.close)
                tag = C.key_in_range(f"scan-{i}", i * 8192, (i + 1) * 8192 - 1)
                prefixes.append(seed(client, tag))
                S.reject(client, ("PSYNC", "?", "-1"), "unknown command")
            with S.process(redis, root / "cluster-target", "target", redis=True) as (
                target,
                port,
                _,
            ):
                scan(
                    shake,
                    root / "cluster-scan",
                    nodes[0].redis_port,
                    port,
                    cluster=True,
                    password="",
                )
                for prefix in prefixes:
                    verify(target, prefix)
                assert target.call("DBSIZE") == 14
    finally:
        for node in nodes:
            node.force_kill()
        meta.kill9()


if __name__ == "__main__":
    lavik, redis, shake, meta, ctl = map(lambda p: str(Path(p).resolve()), sys.argv[1:])
    with tempfile.TemporaryDirectory(
        prefix="lavik-scan-reader-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        root = Path(directory)
        standalone(lavik, redis, shake, root)
        managed_cluster(lavik, redis, shake, meta, ctl, root)
    print("RedisShake ScanReader checks passed")
