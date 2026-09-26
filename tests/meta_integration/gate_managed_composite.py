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

"""Composite commands share one managed Group execution and publication cut."""

import concurrent.futures
import os
from pathlib import Path
import sys
import tempfile

from gate_native_replication import C, H, Client, pair, ready, rejects


def transactions(root, mode):
    with pair(root, f"transactions-{mode}", client_mode=mode) as (
        meta,
        source,
        target,
        writer,
    ):
        ready(meta)
        reader = Client(target, readonly=mode == "cluster")
        try:
            first = "xa" if mode == "single" else "{tx}a"
            second = "{b}k" if mode == "single" else "{tx}b"
            if mode == "single":
                assert C.redis_slot(first) % 2 != C.redis_slot(second) % 2
            assert writer.call("MULTI") == "OK"
            assert writer.call("SET", first, "one") == "QUEUED"
            if mode == "single":
                assert writer.call("SELECT", 15) == "QUEUED"
            assert writer.call("SET", second, "two") == "QUEUED"
            assert writer.call("GET", second) == "QUEUED"
            expected = (
                ["OK", "OK", "OK", "two"] if mode == "single" else ["OK", "OK", "two"]
            )
            assert writer.call("EXEC") == expected
            if mode == "single":
                assert reader.call("SELECT", 15) == "OK"
            H.wait_until(
                "EXEC data replay", 20, lambda: reader.call("GET", second) == "two"
            )

            # WATCH uses its own command key set; watched-only slots are not
            # part of EXEC's queued-key union, matching Redis Cluster.
            peer = Client(source)
            try:
                if mode == "single":
                    assert peer.call("SELECT", 15) == "OK"
                assert writer.call("WATCH", second) == "OK"
                assert peer.call("SET", second, "changed") == "OK"
                assert writer.call("MULTI") == "OK"
                assert writer.call("GET", second) == "QUEUED"
                assert writer.call("EXEC") == []
                assert writer.call("WATCH", "{watch}unused") == "OK"
                assert writer.call("WATCH", second) == "OK"
                assert writer.call("MULTI") == "OK"
                assert writer.call("SET", second, "watched") == "QUEUED"
                assert writer.call("EXEC") == ["OK"]
                assert writer.call("WATCH", second) == "OK"
                assert writer.call("UNWATCH") == "OK"
                assert writer.call("MULTI") == "OK"
                assert writer.call("SET", second, "discarded") == "QUEUED"
                assert writer.call("DISCARD") == "OK"
                assert writer.call("GET", second) == "watched"
            finally:
                peer.close()
            script = "return redis.call('SET',KEYS[1],ARGV[1])"
            sha = writer.call("SCRIPT", "LOAD", script)
            assert writer.call("SCRIPT", "EXISTS", sha) == [1]
            assert writer.call("EVALSHA", sha, 1, second, "script") == "OK"
            assert (
                writer.call("EVAL_RO", "return redis.call('GET',KEYS[1])", 1, second)
                == "script"
            )
            rejects(writer, ("EVAL_RO", script, 1, second, "bad"), "read-only")
            rejects(writer, ("EVAL", "return redis.call('GET','undeclared')", 0), "key")
            assert writer.call("SCRIPT", "FLUSH") == "OK"
            rejects(writer, ("EVALSHA", sha, 1, second, "bad"), "NOSCRIPT")
            if mode == "cluster":
                rejects(writer, ("SELECT", 1), "SELECT")
                rejects(writer, ("WATCH", first, "{other}k"), "CROSSSLOT")
                assert writer.call("MULTI") == "OK"
                assert writer.call("SET", first, "must-not-commit") == "QUEUED"
                assert writer.call("SET", "{other}k", "bad") == "QUEUED"
                rejects(writer, ("EXEC",), "CROSSSLOT")
                assert writer.call("GET", first) == "one"
                rejects(writer, ("EVAL", "return 1", 2, first, "{other}k"), "CROSSSLOT")
            else:
                assert (
                    writer.call(
                        "EVAL",
                        "redis.call('SET',KEYS[1],ARGV[1]); return redis.call('SET',KEYS[2],ARGV[1])",
                        2,
                        first,
                        second,
                        "wide",
                    )
                    == "OK"
                )

            code = (
                "#!lua name=transaction_library\n"
                "redis.register_function('transaction_set',function(keys,args) "
                "return redis.call('SET',keys[1],args[1]) end)\n"
                "redis.register_function{function_name='transaction_get', "
                "callback=function(keys,args) return redis.call('GET',keys[1]) end, flags={'no-writes'}}"
            )
            assert writer.call("MULTI") == "OK"
            assert writer.call("FUNCTION", "LOAD", code) == "QUEUED"
            assert (
                writer.call("FCALL", "transaction_set", 1, second, "function")
                == "QUEUED"
            )
            assert writer.call("EXEC") == ["transaction_library", "OK"]
            H.wait_until(
                "mixed catalog EXEC replay",
                20,
                lambda: reader.call("GET", second) == "function",
            )
            H.wait_until(
                "catalog replay", 20, lambda: reader.call("FUNCTION", "LIST") != []
            )
            assert reader.call("FCALL_RO", "transaction_get", 1, second) == "function"
            assert (
                reader.call("EVAL_RO", "return redis.call('GET',KEYS[1])", 1, second)
                == "function"
            )
            dump = writer.call("FUNCTION", "DUMP", decode=False)
            assert writer.call("MULTI") == "OK"
            assert writer.call("FUNCTION", "FLUSH") == "QUEUED"
            assert writer.call("FUNCTION", "RESTORE", dump, "FLUSH") == "QUEUED"
            assert writer.call("EXEC") == ["OK", "OK"]
            assert writer.call("MULTI") == "OK"
            assert writer.call("FUNCTION", "DELETE", "transaction_library") == "QUEUED"
            assert writer.call("EXEC") == ["OK"]
            H.wait_until(
                "keyless catalog EXEC replay",
                20,
                lambda: reader.call("FUNCTION", "LIST") == [],
            )
        finally:
            reader.close()


def streams(root, mode):
    with pair(root, f"streams-{mode}", client_mode=mode) as (
        meta,
        source,
        target,
        writer,
    ):
        ready(meta)
        reader = Client(target, readonly=mode == "cluster")
        waiters = [Client(source), Client(source)]
        key = "{stream}one"
        second = "{stream}two" if mode == "cluster" else "other-stream"
        try:
            if mode == "single":
                for client in (writer, reader, *waiters):
                    assert client.call("SELECT", 1) == "OK"
            assert writer.call("XGROUP", "CREATE", key, "g", 0, "MKSTREAM") == "OK"
            assert (
                writer.call("XREADGROUP", "GROUP", "g", "empty", "STREAMS", key, ">")
                == []
            )
            assert writer.call("XADD", key, "1-0", "v", "one") == "1-0"
            assert writer.call(
                "XREADGROUP", "GROUP", "g", "c", "NOACK", "STREAMS", key, ">"
            ) == [[key, [["1-0", ["v", "one"]]]]]
            assert writer.call("XPENDING", key, "g")[0] == 0
            assert writer.call("XADD", key, "2-0", "v", "two") == "2-0"
            assert writer.call(
                "XREADGROUP", "GROUP", "g", "c", "STREAMS", key, ">"
            ) == [[key, [["2-0", ["v", "two"]]]]]
            assert writer.call("XREADGROUP", "GROUP", "g", "c", "STREAMS", key, 0) == [
                [key, [["2-0", ["v", "two"]]]]
            ]
            pending = writer.call("XPENDING", key, "g", "-", "+", 10)
            assert pending[0][3] == 2

            def replayed():
                rows = reader.call("XPENDING", key, "g", "-", "+", 10)
                return (
                    len(rows) == 1 and rows[0][0:2] == ["2-0", "c"] and rows[0][3] == 2
                )

            H.wait_until("Stream delivery count replay", 20, replayed)
            consumers = reader.call("XINFO", "CONSUMERS", key, "g")
            assert {dict(zip(row[::2], row[1::2]))["name"] for row in consumers} == {
                "empty",
                "c",
            }
            assert writer.call("XADD", second, "1-0", "v", "other") == "1-0"
            assert len(writer.call("XREAD", "STREAMS", key, second, 0, 0)) == 2
            assert writer.call("MULTI") == "OK"
            assert writer.call("XREAD", "BLOCK", 0, "STREAMS", key, "$") == "QUEUED"
            assert (
                writer.call(
                    "XREADGROUP", "GROUP", "g", "exec", "BLOCK", 0, "STREAMS", key, ">"
                )
                == "QUEUED"
            )
            assert writer.call("EXEC") == [[], []]
            script = "return redis.call('XREADGROUP','GROUP','g','lua','STREAMS',KEYS[1],'>')"
            assert writer.call("EVAL", script, 1, key) is None
            rejects(
                writer,
                (
                    "EVAL",
                    "return redis.call('XREAD','BLOCK',1,'STREAMS',KEYS[1],'$')",
                    1,
                    key,
                ),
                "BLOCK",
            )
            if mode == "cluster":
                rejects(
                    writer,
                    ("XREAD", "STREAMS", key, "{different}stream", 0, 0),
                    "CROSSSLOT",
                )
                rejects(
                    writer,
                    (
                        "XREADGROUP",
                        "GROUP",
                        "g",
                        "bad",
                        "STREAMS",
                        key,
                        "{different}stream",
                        ">",
                        ">",
                    ),
                    "CROSSSLOT",
                )
            assert waiters[0].call("XREAD", "BLOCK", 20, "STREAMS", key, "$") == []
            ids = [client.call("CLIENT", "ID") for client in waiters]

            def blocked(count):
                return (
                    sum(
                        "flags=b" in line
                        for line in writer.call("CLIENT", "LIST").splitlines()
                    )
                    >= count
                )

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                futures = [
                    pool.submit(client.call, "XREAD", "BLOCK", 0, "STREAMS", key, "$")
                    for client in waiters
                ]
                H.wait_until("broadcast readers sleeping", 10, lambda: blocked(2))
                assert writer.call("XADD", key, "3-0", "v", "wake") == "3-0"
                expected = [[key, [["3-0", ["v", "wake"]]]]]
                assert [f.result(timeout=10) for f in futures] == [expected, expected]
                future = pool.submit(
                    waiters[0].call, "XREAD", "BLOCK", 0, "STREAMS", key, "$"
                )
                H.wait_until("unblock reader sleeping", 10, lambda: blocked(1))
                assert writer.call("CLIENT", "UNBLOCK", ids[0], "TIMEOUT") == 1
                assert future.result(timeout=10) == []
                fifo = "{fifo}stream"
                assert writer.call("XGROUP", "CREATE", fifo, "g", 0, "MKSTREAM") == "OK"
                futures = []
                for index, client in enumerate(waiters):
                    futures.append(
                        pool.submit(
                            client.call,
                            "XREADGROUP",
                            "GROUP",
                            "g",
                            f"fifo-{index}",
                            "COUNT",
                            1,
                            "BLOCK",
                            0,
                            "STREAMS",
                            fifo,
                            ">",
                        )
                    )
                    H.wait_until(
                        "FIFO consumer registered", 10, lambda: blocked(index + 1)
                    )
                assert writer.call("XADD", fifo, "1-0", "v", "first") == "1-0"
                assert futures[0].result(timeout=10) == [
                    [fifo, [["1-0", ["v", "first"]]]]
                ]
                assert not futures[1].done()
                assert writer.call("XADD", fifo, "2-0", "v", "second") == "2-0"
                assert futures[1].result(timeout=10) == [
                    [fifo, [["2-0", ["v", "second"]]]]
                ]
                read = pool.submit(
                    waiters[0].call, "XREAD", "BLOCK", 0, "STREAMS", fifo, "$"
                )
                group = pool.submit(
                    waiters[1].call,
                    "XREADGROUP",
                    "GROUP",
                    "g",
                    "flushed",
                    "BLOCK",
                    0,
                    "STREAMS",
                    fifo,
                    ">",
                )
                H.wait_until("Stream waiters before FLUSH", 10, lambda: blocked(2))
                assert writer.call("FLUSHDB") == "OK"
                try:
                    group.result(timeout=10)
                except H.Failure as error:
                    assert "NOGROUP" in str(error), error
                else:
                    raise AssertionError("flushed consumer group still served")
                assert not read.done()
                assert writer.call("XADD", fifo, "3-0", "v", "after-flush") == "3-0"
                assert read.result(timeout=10) == [
                    [fifo, [["3-0", ["v", "after-flush"]]]]
                ]

        finally:
            for client in (*waiters, reader):
                client.close()


def grouped_recovery(root, mode):
    key = "{grouped}stream"
    value = "v" * 65536
    code = "#!lua name=stream_functions\nredis.register_function('consume',function(keys,args) return redis.call('XREADGROUP','GROUP','g',args[1],'STREAMS',keys[1],'>') end)"
    expected = [[key, [["1-0", ["f", value]]]]]

    def seed(writer):
        if mode == "single":
            assert writer.call("SELECT", 15) == "OK"
        assert writer.call("FUNCTION", "LOAD", code) == "stream_functions"
        assert writer.call("XADD", key, "1-0", "f", value) == "1-0"
        assert writer.call("XGROUP", "CREATE", key, "g", 0) == "OK"
        assert writer.call("FCALL", "consume", 1, key, "before-full") == expected

    def intact(node):
        client = Client(node, readonly=mode == "cluster")
        try:
            if mode == "single":
                assert client.call("SELECT", 15) == "OK"
            return (
                client.call("XREAD", "STREAMS", key, 0) == expected
                and client.call("XPENDING", key, "g")[0] == 1
                and bool(client.call("FUNCTION", "LIST"))
            )
        finally:
            client.close()

    with pair(
        root,
        f"grouped-{mode}",
        client_mode=mode,
        seed=seed,
        require_seed_before_full=True,
    ) as (meta, source, target, writer):
        ready(meta)
        H.wait_until(
            "grouped Stream and Function FULL state", 20, lambda: intact(target)
        )
        target.terminate()
        target.start()
        H.wait_until("grouped Stream replica restart", 30, lambda: intact(target))
        source.terminate()
        source.start()
        H.wait_until("grouped Stream source recovery", 30, lambda: intact(source))
        H.wait_until("grouped Stream follower rejoin", 30, lambda: intact(target))
        current = None

        def writable_owner():
            nonlocal current
            for node in (source, target):
                candidate = Client(node)
                try:
                    if mode == "single":
                        assert candidate.call("SELECT", 15) == "OK"
                    if candidate.call("SET", "{grouped}probe", "ready") == "OK":
                        current = candidate
                        return True
                except H.Failure:
                    pass
                candidate.close()
            return False

        H.wait_until("writable owner after recovery", 30, writable_owner)
        try:
            assert (
                current.call(
                    "EVAL",
                    "return redis.call('XREADGROUP','GROUP','g','before-full','STREAMS',KEYS[1],0)",
                    1,
                    key,
                )
                == expected
            )
            assert current.call("MULTI") == "OK"
            assert current.call("XREAD", "STREAMS", key, 0) == "QUEUED"
            assert current.call("DEL", key) == "QUEUED"
            # The reply pins the command-position graph across later deletion.
            assert current.call("EXEC") == [expected, 1]
        finally:
            current.close()


def main():
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    H.set_tag("managed-composite")
    with tempfile.TemporaryDirectory(
        prefix="composite-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        for mode in ("single", "cluster"):
            transactions(Path(directory), mode)
            streams(Path(directory), mode)
            grouped_recovery(Path(directory), mode)
    H.log("PASS")


if __name__ == "__main__":
    main()
