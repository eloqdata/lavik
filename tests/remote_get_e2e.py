#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exercise connection-worker compact reads, owner fallback, and detach."""

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import sys
import tempfile
import time

import defrag_payload_e2e as support


def stats(client):
    return dict(
        line.split(b":", 1)
        for line in client.batch([[b"INFO", b"stats"]])[0].splitlines()
        if b":" in line
    )


def drain(client):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        values = stats(client)
        if all(
            values[key] == b"0"
            for key in (
                b"storage_dirty_staging_bytes",
                b"storage_flushes_pending",
                b"storage_tx_commits_pending",
            )
        ):
            return
        time.sleep(0.02)
    raise AssertionError("durability did not drain")


class RedisError(bytes):
    pass


class CollectionClient(support.Client):
    """Decode RESP2/3, retaining command errors for parity assertions."""

    def response(self):
        head = self.stream.readline()
        assert head.endswith(b"\r\n"), head
        kind, value = head[:1], head[1:-2]
        if kind in (b"*", b"~"):
            return (
                None if value == b"-1" else [self.response() for _ in range(int(value))]
            )
        if kind == b"%":
            return {self.response(): self.response() for _ in range(int(value))}
        if kind == b"$":
            if value == b"-1":
                return None
            result = self.stream.read(int(value))
            assert self.stream.read(2) == b"\r\n"
            return result
        if kind == b"_":
            return None
        if kind == b"-":
            return RedisError(value)
        if kind == b":":
            return int(value)
        if kind == b",":
            return float(value)
        assert kind == b"+", head
        return value


def collection_reads(keys):
    h, s, l, z, g, x = keys
    return [
        [b"HGET", h, b"a"],
        [b"HMGET", h, b"b", b"absent", b"a", b"b"],
        [b"HLEN", h],
        [b"HEXISTS", h, b"a"],
        [b"HSTRLEN", h, b"b"],
        [b"HGETALL", h],
        [b"HKEYS", h],
        [b"HVALS", h],
        [b"HSCAN", h, b"0", b"COUNT", b"100"],
        [b"HSCAN", h, b"0", b"COUNT", b"0"],
        [b"SCARD", s],
        [b"SISMEMBER", s, b"a"],
        [b"SMISMEMBER", s, b"b", b"absent", b"a", b"b"],
        [b"SMEMBERS", s],
        [b"SSCAN", s, b"0", b"MATCH", b"a*"],
        [b"LLEN", l],
        [b"LINDEX", l, b"-1"],
        [b"LINDEX", l, b"1000"],
        [b"LRANGE", l, b"-3", b"-1"],
        [b"LRANGE", l, b"4", b"1"],
        [b"LPOS", l, b"a", b"RANK", b"-1", b"COUNT", b"0"],
        [b"LPOS", l, b"a", b"RANK", b"0"],
        [b"ZCARD", z],
        [b"ZSCORE", z, b"a"],
        [b"ZMSCORE", z, b"b", b"absent", b"a", b"b"],
        [b"ZCOUNT", z, b"(1", b"+inf"],
        [b"ZLEXCOUNT", z, b"[a", b"[b"],
        [b"ZRANK", z, b"b", b"WITHSCORE"],
        [b"ZREVRANK", z, b"a"],
        [b"ZRANGE", z, b"0", b"-1", b"WITHSCORES"],
        [b"ZRANGE", z, b"0", b"10", b"BYSCORE", b"LIMIT", b"1", b"2"],
        [b"ZREVRANGE", z, b"0", b"-1", b"WITHSCORES"],
        [b"ZRANGEBYSCORE", z, b"-inf", b"+inf"],
        [b"ZREVRANGEBYSCORE", z, b"+inf", b"-inf"],
        [b"ZRANGEBYLEX", z, b"-", b"+"],
        [b"ZREVRANGEBYLEX", z, b"+", b"-"],
        [b"ZSCAN", z, b"0", b"COUNT", b"100"],
        [b"ZRANGE", z, b"bad", b"-1"],
        [b"GEODIST", g, b"a", b"b"],
        [b"GEOHASH", g, b"a", b"absent"],
        [b"GEOPOS", g, b"a", b"b"],
        [b"GEORADIUS_RO", g, b"13", b"38", b"1000", b"km", b"ASC"],
        [b"GEORADIUSBYMEMBER_RO", g, b"a", b"1000", b"km", b"WITHDIST", b"ASC"],
        [b"GEOSEARCH", g, b"FROMMEMBER", b"a", b"BYRADIUS", b"1000", b"km", b"ASC"],
        [b"XLEN", x],
        [b"XRANGE", x, b"-", b"+"],
        [b"XREVRANGE", x, b"+", b"-", b"COUNT", b"1"],
        [b"XRANGE", x, b"(1-0", b"+"],
        [b"XRANGE", x, b"bad", b"+"],
        [b"XINFO", b"STREAM", x],
        [b"XINFO", b"STREAM", x, b"FULL", b"COUNT", b"1"],
        [b"XINFO", b"GROUPS", x],
        [b"XPENDING", x, b"group"],
        [b"HGET", l, b"a"],
        [b"LRANGE", z, b"0", b"-1"],
        [b"SMEMBERS", h],
        [b"ZSCORE", s, b"a"],
        [b"XLEN", h],
    ]


def assert_owner_parity(client, commands):
    # EXEC always holds owner-side intents and bypasses remote admission.
    # Compare that independent route with standalone execution on the same
    # connection, including syntax errors and wrong-type/missing-key replies.
    expected = client.batch([[b"MULTI"], *commands, [b"EXEC"]])[-1]
    actual = client.batch(commands)
    for command, got, want in zip(commands, actual, expected, strict=True):
        assert got == want, (command, got, want)


def check_collections(port):
    client = CollectionClient(port)
    families = [
        [
            kind + b"-remote-" + str(i).encode()
            for kind in (b"h", b"s", b"l", b"z", b"g", b"x")
        ]
        for i in range(32)
    ]
    try:
        for keys in families:
            h, s, l, z, g, x = keys
            replies = client.batch(
                [
                    [b"HSET", h, b"a", b"one", b"b", b"two"],
                    [b"SADD", s, b"a", b"b"],
                    [b"RPUSH", l, b"a", b"b", b"a"],
                    [b"ZADD", z, b"1", b"a", b"2", b"b"],
                    [b"GEOADD", g, b"13.3", b"38.1", b"a", b"13.4", b"38.2", b"b"],
                    [b"XADD", x, b"1-0", b"f", b"one"],
                    [b"XADD", x, b"2-0", b"f", b"two"],
                    [b"XGROUP", b"CREATE", x, b"group", b"0"],
                ]
            )
            assert not any(isinstance(value, RedisError) for value in replies), replies
        drain(client)
        for version in (b"2", b"3"):
            client.batch([[b"HELLO", version]])
            for keys in families:
                assert_owner_parity(client, collection_reads(keys))
                # Random reads share the same input snapshot but need not draw
                # identical members on the two executions.
                random = client.batch(
                    [
                        [b"HRANDFIELD", keys[0]],
                        [b"SRANDMEMBER", keys[1]],
                        [b"ZRANDMEMBER", keys[3]],
                    ]
                )
                assert all(value in (b"a", b"b") for value in random), random
            assert_owner_parity(
                client, collection_reads([b"absent-" + key for key in families[0]])
            )
        client.batch([[b"HELLO", b"2"]])
        # Promotion exercises the ordinary owner fallback for every family.
        large = b"p" * 20000
        keys = families[0]
        client.batch(
            [
                [b"HSET", keys[0], b"large", large],
                [b"SADD", keys[1], large],
                [b"RPUSH", keys[2], large],
                [b"ZADD", keys[3], b"3", large],
                [b"XADD", keys[5], b"3-0", b"f", large],
            ]
        )
        drain(client)
        assert_owner_parity(client, collection_reads(keys))

        # A compact HMGET must see one complete record during atomic updates.
        key = families[1][0]
        client.batch([[b"HSET", key, b"a", b"old", b"b", b"old"]])
        drain(client)

        def writer():
            peer = CollectionClient(port)
            try:
                for i in range(300):
                    value = b"new" if i % 2 else b"old"
                    assert peer.batch(
                        [
                            [b"MULTI"],
                            [b"HSET", key, b"a", value, b"b", value],
                            [b"EXEC"],
                        ]
                    )[-1] == [0]
            finally:
                peer.close()

        with ThreadPoolExecutor(max_workers=1) as pool:
            task = pool.submit(writer)
            for _ in range(600):
                got = client.batch([[b"HMGET", key, b"a", b"b"]])[0]
                assert got in ([b"old", b"old"], [b"new", b"new"]), got
            task.result()
    finally:
        client.close()


def check():
    keys = [b"remote-" + str(i).encode() for i in range(6000)]
    before = b"a" * 4096
    after = b"b" * 4096
    with support.server(2) as (admin, _):
        port = admin.socket.getpeername()[1]
        for batch in support.chunks(keys):
            assert admin.batch([[b"SET", key, before] for key in batch]) == [
                b"OK"
            ] * len(batch)
        drain(admin)
        for batch in support.chunks(keys):
            assert admin.batch([[b"GET", key] for key in batch]) == [before] * len(
                batch
            )
        # Hash tags select populated indexes while the complete keys differ.
        missing = [b"{" + key + b"}:missing" for key in keys[:100]]
        assert admin.batch([[b"GET", key] for key in missing]) == [None] * len(missing)

        def read(count, allowed):
            client = support.Client(port)
            try:
                for i in range(count):
                    assert client.batch([[b"GET", keys[i % 1000]]])[0] in allowed
            finally:
                client.close()

        def write():
            client = support.Client(port)
            try:
                # Retire enough of the first sealed blocks to overlap reads
                # with the ordinary defrag admission and last-unpin retry.
                for i in range(4500):
                    key = keys[i]
                    value = after if i % 2 else before
                    assert client.batch(
                        [[b"MULTI"], [b"SET", key, value], [b"GET", key], [b"EXEC"]]
                    ) == [b"OK", b"QUEUED", b"QUEUED", [b"OK", value]]
            finally:
                client.close()

        with ThreadPoolExecutor(max_workers=5) as pool:
            tasks = [pool.submit(read, 6000, (before, after)) for _ in range(4)]
            tasks.append(pool.submit(write))
            for task in tasks:
                task.result()

        check_collections(port)

        # Clear while requests are arriving. Either generation is valid for
        # an overlapping GET, but stale bytes must never reappear after FLUSHDB.
        with ThreadPoolExecutor(max_workers=2) as pool:
            task = pool.submit(read, 1000, (None, before, after))
            assert admin.batch([[b"FLUSHDB"]]) == [b"OK"]
            task.result()
        for batch in support.chunks(keys[:1000]):
            assert admin.batch([[b"GET", key] for key in batch]) == [None] * len(batch)
        assert admin.batch([[b"SET", b"survivor", before]]) == [b"OK"]
        drain(admin)
    with support.server(2) as (admin, _):
        assert admin.batch([[b"GET", b"survivor"]]) == [before]
        for batch in support.chunks(keys[:1000]):
            assert admin.batch([[b"GET", key] for key in batch]) == [None] * len(batch)


def main():
    support.BINARY = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(
        prefix="lavik-remote-get-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        support.WORKDIR = Path(directory)
        with (support.WORKDIR / "data").open("wb") as file:
            file.truncate(512 * 1024 * 1024)
        check()
    print(
        "remote compact reads, RESP2/3, fallback, transactions, FLUSHDB and recovery passed"
    )


if __name__ == "__main__":
    main()
