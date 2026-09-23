#!/usr/bin/env python3
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

"""Known multi-DB population shared by Single FULL, restart and HA gates."""

import time

from gate_native_replication import Client

DATABASES = (0, 1, 15)
TYPES = ("string", "hash", "set", "list", "zset", "stream")
PREFIX = "multidb:"
COPY_DATABASES = ((0, 15), (15, 1), (1, 0))


def seed(client):
    """Seed independently checkable contents, including grouped values and PEL."""
    deadline = int(time.time() * 1000) + 600_000
    for db in range(16):
        assert client.call("SELECT", db) == "OK"
        assert client.call("SET", PREFIX + "identity", f"db-{db}") == "OK"
    for db in DATABASES:
        assert client.call("SELECT", db) == "OK"
        assert client.call("SET", PREFIX + "string", f"value-{db}") == "OK"
        fields = [
            part for i in range(128) for part in (f"f{i}", f"db-{db}-" + "h" * 256)
        ]
        assert client.call("HSET", PREFIX + "hash", *fields) == 128
        assert client.call("SADD", PREFIX + "set", "a", "b") == 2
        assert client.call("RPUSH", PREFIX + "list", "a", "b") == 2
        assert client.call("ZADD", PREFIX + "zset", 1, "a", 2, "b") == 2
        for i in range(1, 65):
            assert (
                client.call("XADD", PREFIX + "stream", f"{i}-0", "f", "s" * 512)
                == f"{i}-0"
            )
        assert client.call("XGROUP", "CREATE", PREFIX + "stream", "g", "0") == "OK"
        # FORCE creates real pending state through an already supported command;
        # XREADGROUP remains outside this issue's managed Single admission.
        assert client.call(
            "XCLAIM", PREFIX + "stream", "g", "consumer", 0, "1-0", "FORCE", "JUSTID"
        ) == ["1-0"]
        for kind in TYPES:
            assert client.call("PEXPIREAT", PREFIX + kind, deadline) == 1
    for source, destination in COPY_DATABASES:
        client.call("SELECT", source)
        for kind in TYPES:
            assert (
                client.call(
                    "COPY", PREFIX + kind, PREFIX + "copy:" + kind, "DB", destination
                )
                == 1
            )
    assert client.call("SELECT", 0) == "OK"
    return deadline


def require_value(client, kind, key, source_db, deadline):
    assert client.call("TYPE", key) == kind, (source_db, key)
    if kind == "string":
        assert client.call("GET", key) == f"value-{source_db}"
    elif kind == "hash":
        assert client.call("HLEN", key) == 128
        assert client.call("HGET", key, "f127") == f"db-{source_db}-" + "h" * 256
    elif kind == "set":
        assert sorted(client.call("SMEMBERS", key)) == ["a", "b"]
    elif kind == "list":
        assert client.call("LRANGE", key, 0, -1) == ["a", "b"]
    elif kind == "zset":
        assert client.call("ZRANGE", key, 0, -1, "WITHSCORES") == ["a", "1", "b", "2"]
    else:
        assert client.call("XRANGE", key, "-", "+") == [
            [f"{i}-0", ["f", "s" * 512]] for i in range(1, 65)
        ]
        assert client.call("XPENDING", key, "g") == [1, "1-0", "1-0", [["consumer", 1]]]
        groups = client.call("XINFO", "GROUPS", key)
        assert len(groups) == 1
        group = dict(zip(groups[0][::2], groups[0][1::2]))
        assert (
            group["name"] == "g" and group["consumers"] == 1 and group["pending"] == 1
        )
    # Absolute expiry catches COPY or FULL restarting the TTL. Sample before and
    # after the request so elapsed network time does not make this flaky.
    before = int(time.time() * 1000)
    ttl = client.call("PTTL", key)
    after = int(time.time() * 1000)
    assert ttl > 0 and before + ttl <= deadline + 5 and after + ttl >= deadline - 5


def require(client, deadline):
    for db in range(16):
        assert client.call("SELECT", db) == "OK"
        assert client.call("GET", PREFIX + "identity") == f"db-{db}", db
    for db in DATABASES:
        assert client.call("SELECT", db) == "OK"
        for kind in TYPES:
            require_value(client, kind, PREFIX + kind, db, deadline)
    for source, destination in COPY_DATABASES:
        client.call("SELECT", destination)
        for kind in TYPES:
            require_value(client, kind, PREFIX + "copy:" + kind, source, deadline)
    assert client.call("SELECT", 0) == "OK"
    return True


def seed_node(node):
    client = Client(node)
    try:
        return seed(client)
    finally:
        client.close()


def node_matches(node, deadline):
    client = Client(node)
    try:
        return require(client, deadline)
    except AssertionError:
        return False
    finally:
        client.close()
