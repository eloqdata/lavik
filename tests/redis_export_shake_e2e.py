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

"""Lavik Primary -> unmodified RedisShake 4.6.2 -> Redis 7.2.14."""

from contextlib import contextmanager
import os
from pathlib import Path
import socket
import sys
import tempfile
import time

import redis
import redis_sync_reader_e2e as I
import redis_follower_smoke as S
import gate_native_replication as N


def client(port, db=0):
    return redis.Redis(port=port, db=db, password=I.PASSWORD or None, socket_timeout=15)


@contextmanager
def source(root, mode):
    if mode == "standalone":
        with S.process(
            I.LAVIK,
            root / "source",
            "lavik",
            extra=("--requirepass", I.PASSWORD),
            password=I.PASSWORD,
            max_memory="256M",
        ) as (_, port, _):
            yield port
    else:
        with N.pair(root, "managed", client_mode=mode) as (meta, primary, _, _):
            N.ready(meta)
            yield primary.redis_port


class Shake(I.Shake):
    def __init__(self, root, source_port, target_port):
        # The common harness owns logs, process cleanup and offset evidence.
        super().__init__(
            root, source_port, target_port, False, "single", try_diskless=True
        )


def positions(info, field):
    value = info[field]
    return [
        int(n)
        for n in (value if isinstance(value, list) else str(value).split(","))
        if n
    ]


def drain(src, shake, source_port):
    I.H.wait_until(
        "export admitted",
        30,
        lambda: src.info("replication")["redis_export_session_id"] != 0,
    )
    cut = src.info("replication")
    identity = cut["redis_export_session_id"]
    assert identity
    source_cut = positions(cut, "redis_export_source_next_lsns")

    def covered():
        info = src.info("replication")
        assert info["redis_export_session_id"] == identity, info
        sent = positions(info, "redis_export_sent_next_lsns")
        return (
            info["redis_export_phase"] == "online"
            and len(sent) == len(source_cut)
            and all(a >= b for a, b in zip(sent, source_cut))
        )

    I.H.wait_until("all source flow positions sent", 45, covered)
    offset = src.info("replication")["redis_export_offset"]
    I.H.wait_until(
        "RedisShake received/sent offset and writer replies",
        45,
        lambda: I.drained(shake, {f"127.0.0.1:{source_port}": offset}),
    )


def compare(src, dst):
    expected, actual = I.snapshot(src), I.snapshot(dst)
    assert expected.keys() == actual.keys(), (
        expected.keys() - actual.keys(),
        actual.keys() - expected.keys(),
    )
    for key, (kind, value, expiry) in expected.items():
        other_kind, other_value, other_expiry = actual[key]
        assert (kind, value) == (other_kind, other_value), (key, kind)
        # The accepted RedisShake contract preserves expiry classification;
        # relative TTL conversion is not subject to a correction threshold.
        assert (expiry < 0) == (other_expiry < 0), (key, expiry, other_expiry)
    assert I.catalog(src) == I.catalog(dst)


def run(root, mode):
    with (
        source(root, mode) as port,
        S.process(
            I.REDIS,
            root / "redis",
            "redis",
            redis=True,
            extra=(("--requirepass", I.PASSWORD) if I.PASSWORD else ()),
            password=I.PASSWORD or None,
        ) as (_, dest, _),
    ):
        src, dst = client(port), client(dest)
        assert dst.info("server")["redis_version"] == "7.2.14"
        assert src.config_get("redis-export-disk-backlog-size") == {
            "redis-export-disk-backlog-size": "1073741824"
        }
        assert src.config_set("redis-export-disk-backlog-size", "25mb")
        assert src.config_get("redis-export-disk-backlog-size") == {
            "redis-export-disk-backlog-size": "25165824"
        }
        assert src.config_get("replication-backlog-backpressure") == {
            "replication-backlog-backpressure": "yes"
        }
        databases = range(16) if mode != "cluster" else (0,)
        for db in databases:
            c = client(port, db)
            c.set(f"db-{db}", f"value-{db}")
            c.close()
        I.seed(src, b"{export}:rdb:")
        src.function_load(I.library("snapshot"))
        shake = Shake(root / "shake", port, dest)
        try:
            drain(src, shake, port)
            for db in databases:
                compare(client(port, db), client(dest, db))
            I.seed(src, b"{export}:online:")
            for db in databases:
                c = client(port, db)
                c.set(f"db-{db}", f"online-{db}")
                c.close()
            if mode != "cluster":
                assert src.copy("db-0", "copied-from-db0", destination_db=15)
            src.delete(b"{export}:rdb:delete")
            src.rename(b"{export}:rdb:rename", b"{export}:renamed")
            src.function_load(I.library("online"), replace=True)
            with src.pipeline(transaction=True) as tx:
                tx.set("{export}:tx-a", "A")
                tx.set("{export}:tx-b", "B")
                tx.delete("{export}:rdb:binary\x00\xff")
                tx.execute()
            drain(src, shake, port)
            for db in databases:
                compare(client(port, db), client(dest, db))
            src.flushdb()
            src.set("{export}:after-flush", "retained")
            drain(src, shake, port)
            for db in databases:
                compare(client(port, db), client(dest, db))
            src.flushall()
            drain(src, shake, port)
            for db in databases:
                compare(client(port, db), client(dest, db))
        except BaseException:
            print((shake.root / "process.log").read_text(), file=sys.stderr)
            raise
        finally:
            shake.close()
            I.H.wait_until(
                "tool exit releases export",
                15,
                lambda: src.info("replication")["redis_export_active"] == 0,
            )
            assert src.set("{export}:after-tool-exit", "writable")
            src.close()
            dst.close()


if __name__ == "__main__":
    I.LAVIK, I.REDIS, I.SHAKE, I.META, I.CTL, mode = sys.argv[1:]
    N.C.DATA, N.C.META, N.C.CTL = I.LAVIK, I.META, I.CTL
    I.PASSWORD = "export-test-secret" if mode == "standalone" else ""
    with tempfile.TemporaryDirectory(
        prefix="lavik-export-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as temporary:
        run(Path(temporary), mode)
    print("PASS", mode)
