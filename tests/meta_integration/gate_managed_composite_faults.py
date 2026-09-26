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

"""Managed composite authority cuts preserve already committed effects."""

import concurrent.futures
import os
from pathlib import Path
import sys
import tempfile
import time

import gate_failover as F
from gate_native_replication import C, H, Client, pair, ready


def expire(meta, source, mode):
    meta.pause()
    refusal = "MASTERDOWN" if mode == "single" else "CLUSTERDOWN"
    H.wait_until(
        "lease expired at execution boundary",
        8,
        lambda: refusal in F.redis_error(source, ["GET", "lease-probe"]),
    )
    return refusal


def reached(source, variable):
    H.wait_until(
        "execution reached held boundary",
        10,
        lambda: f"fault pause reached: {variable}" in Path(source.log_path).read_text(),
    )


def stream_revoke(root, mode, partial, empty=False, noack=False):
    name = f"stream-{mode}-{partial}-{empty}-{noack}"
    hold = root / f"{name}.hold"
    variable = (
        "LAVIK_STREAM_AFTER_FIRST_KEY_HOLD_FILE"
        if partial
        else "LAVIK_STREAM_AFTER_AUTHORITY_HOLD_FILE"
    )
    with pair(root, name, client_mode=mode, source_faults={variable: str(hold)}) as (
        meta,
        source,
        target,
        writer,
    ):
        ready(meta)
        first = "xa" if mode == "single" else "{stream}a"
        second = "{b}k" if mode == "single" else "{stream}b"
        for key in (first, second):
            assert writer.call("XGROUP", "CREATE", key, "g", 0, "MKSTREAM") == "OK"
            if not empty:
                assert writer.call("XADD", key, "1-0", "v", "one") == "1-0"
        reader = Client(target, readonly=mode == "cluster")
        try:
            hold.touch()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                args = ["XREADGROUP", "GROUP", "g", "revoked"]
                if noack:
                    args.append("NOACK")
                pending = pool.submit(
                    writer.call, *args, "STREAMS", first, second, ">", ">"
                )
                try:
                    reached(source, variable)
                    refusal = expire(meta, source, mode)
                    hold.unlink()
                    try:
                        result = pending.result(timeout=15)
                    except H.Failure as error:
                        # Lease loss retires the old connection, so a request
                        # that has not yet committed may see either refusal
                        # or connection close.
                        expected = ("closed",) if partial else ("closed", refusal)
                        assert any(token in str(error) for token in expected), error
                    else:
                        raise AssertionError(
                            f"revoked Stream attempt succeeded: {result!r}"
                        )
                finally:
                    hold.unlink(missing_ok=True)
                    meta.resume()
            current = Client(source)
            try:
                H.wait_until(
                    "authority renewed",
                    20,
                    lambda: current.call("GET", "lease-probe") is None,
                )
                for key, mutated in ((first, partial), (second, False)):

                    def matches(client):
                        rows = client.call("XINFO", "CONSUMERS", key, "g")
                        names = [dict(zip(row[::2], row[1::2]))["name"] for row in rows]
                        groups = client.call("XINFO", "GROUPS", key)
                        group = dict(zip(groups[0][::2], groups[0][1::2]))
                        return (
                            names == (["revoked"] if mutated else [])
                            and group["last-delivered-id"]
                            == ("1-0" if mutated and not empty else "0-0")
                            and client.call("XPENDING", key, "g")[0]
                            == int(mutated and not empty and not noack)
                        )

                    assert matches(current)
                    H.wait_until("attempt effects replay", 20, lambda: matches(reader))
            finally:
                current.close()
        finally:
            reader.close()


def catalog_revoke(root, mode, mixed, boundary):
    name = f"catalog-{mode}-{mixed}-{boundary}"
    hold = root / f"{name}.hold"
    variable = f"LAVIK_FUNCTION_CATALOG_{boundary}_HOLD_FILE"
    with pair(root, name, client_mode=mode, source_faults={variable: str(hold)}) as (
        meta,
        source,
        target,
        writer,
    ):
        ready(meta)
        first = "xa" if mode == "single" else "{catalog}a"
        second = "{b}k" if mode == "single" else "{catalog}b"
        code = "#!lua name=committed\nredis.register_function('committed_value',function(keys,args) return 'ok' end)"
        assert writer.call("MULTI") == "OK"
        if mixed:
            assert writer.call("SET", first, "before") == "QUEUED"
        assert writer.call("FUNCTION", "LOAD", code) == "QUEUED"
        if mixed:
            assert writer.call("SET", second, "after") == "QUEUED"
        hold.touch()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(writer.call, "EXEC")
            try:
                reached(source, variable)
                refusal = expire(meta, source, mode)
                hold.unlink()
                if not mixed and boundary == "AFTER_ROOT_WRITE":
                    try:
                        assert pending.result(timeout=15) == ["committed"]
                    except H.Failure as error:
                        # The durable catalog result may lose its reply when
                        # the lease-expiry sweep retires this connection.
                        assert "closed" in str(error), error
                else:
                    try:
                        result = pending.result(timeout=15)
                    except H.Failure as error:
                        expected = ("closed",) if mixed else ("closed", refusal)
                        assert any(token in str(error) for token in expected), error
                    else:
                        raise AssertionError(f"revoked EXEC succeeded: {result!r}")
            finally:
                hold.unlink(missing_ok=True)
                meta.resume()
        current = Client(source)
        reader = Client(target, readonly=mode == "cluster")
        try:

            def matches(client):
                catalog = client.call("FUNCTION", "LIST")
                return (
                    bool(catalog) == (boundary == "AFTER_ROOT_WRITE")
                    and client.call("GET", first) == ("before" if mixed else None)
                    and client.call("GET", second) is None
                )

            H.wait_until("source committed prefix", 20, lambda: matches(current))
            H.wait_until("replica committed prefix", 20, lambda: matches(reader))
        finally:
            current.close()
            reader.close()


def wait_registration_race(root):
    hold = root / "registration.hold"
    variable = "LAVIK_STREAM_BEFORE_WAIT_REGISTRATION_HOLD_FILE"
    with pair(
        root, "registration", client_mode="single", source_faults={variable: str(hold)}
    ) as (meta, source, _, writer):
        ready(meta)
        blocked = Client(source)
        try:
            assert writer.call("XADD", "race", "1-0", "v", "old") == "1-0"
            hold.touch()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(
                    blocked.call, "XREAD", "BLOCK", 10000, "STREAMS", "race", "$"
                )
                try:
                    reached(source, variable)
                    assert writer.call("XADD", "race", "2-0", "v", "new") == "2-0"
                finally:
                    hold.unlink(missing_ok=True)
                assert pending.result(timeout=15) == [["race", [["2-0", ["v", "new"]]]]]
        finally:
            blocked.close()


def full_with_idle_consumer(root):
    # Start the waiter while FULL has an active session but has not scanned
    # its partition. Retaining the request's publisher token while asleep
    # prevents that partition's UNSTARTED -> CAPTURING transition.
    low = next(
        f"full-low-{i}" for i in range(1000000) if C.redis_slot(f"full-low-{i}") == 0
    )
    key = next(
        f"full-high-{i}"
        for i in range(1000000)
        if C.redis_slot(f"full-high-{i}") == 16383
    )

    def seed(client):
        assert client.call("SET", low, "low") == "OK"
        assert client.call("XGROUP", "CREATE", key, "g", 0, "MKSTREAM") == "OK"

    with pair(
        root,
        "idle-full",
        client_mode="single",
        source_workers=1,
        seed=seed,
        require_seed_before_full=True,
        source_faults={"LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "5000"},
    ) as (meta, source, target, writer):
        H.wait_until(
            "FULL paused before waiter partition",
            15,
            lambda: "paused full sync after acknowledged handoff partition 0"
            in Path(source.log_path).read_text(),
        )
        blocked = Client(source)
        reader = Client(target)
        try:
            client_id = blocked.call("CLIENT", "ID")
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(
                    blocked.call,
                    "XREADGROUP",
                    "GROUP",
                    "g",
                    "idle",
                    "BLOCK",
                    0,
                    "STREAMS",
                    key,
                    ">",
                )
                try:
                    H.wait_until(
                        "idle group waiter registered",
                        10,
                        lambda: " flags=b "
                        in writer.call("CLIENT", "LIST", "ID", client_id),
                    )
                    H.wait_until(
                        "FULL finishes despite idle consumer",
                        20,
                        lambda: len(reader.call("XINFO", "CONSUMERS", key, "g")) == 1,
                    )
                    assert not pending.done()
                    assert writer.call("XADD", key, "1-0", "v", "wake") == "1-0"
                    assert pending.result(timeout=10) == [
                        [key, [["1-0", ["v", "wake"]]]]
                    ]
                    H.wait_until(
                        "post-FULL PEL replay",
                        20,
                        lambda: reader.call("XPENDING", key, "g")[0] == 1,
                    )
                finally:
                    if not pending.done():
                        writer.call("CLIENT", "UNBLOCK", client_id, "TIMEOUT")
        finally:
            blocked.close()
            reader.close()


def full_with_db_gate_waiters(root, mode):
    # Use an already captured partition so publisher reservations are visible
    # as FULL queue credit. Waiting for the DB gate must release that credit;
    # the final FULL cut legitimately waits for KEYS to reopen the gate.
    key = next(f"gate-{i}" for i in range(1000000) if C.redis_slot(f"gate-{i}") == 0)
    hold = root / f"db-gate-{mode}.hold"
    variable = "LAVIK_KEYS_AFTER_DB_CLOSE_HOLD_FILE"

    def seed(client):
        assert client.call("XGROUP", "CREATE", key, "g", 0, "MKSTREAM") == "OK"

    with pair(
        root,
        f"db-gate-{mode}",
        client_mode=mode,
        source_workers=1,
        seed=seed,
        require_seed_before_full=True,
        source_faults={
            "LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "5000",
            variable: str(hold),
        },
    ) as (_, source, target, writer):
        H.wait_until(
            "FULL paused after capturing waiter partition",
            15,
            lambda: "paused full sync after acknowledged handoff partition 0"
            in Path(source.log_path).read_text(),
        )
        scanner, timed = Client(source), Client(source)
        waiters = [Client(source), Client(source)]
        reader = Client(target, readonly=mode == "cluster")
        try:
            blocked_id = waiters[1].call("CLIENT", "ID")
            hold.touch()
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                scanning = pool.submit(scanner.call, "KEYS", "*")
                try:
                    reached(source, variable)
                    for command in ("XREAD", "XREADGROUP"):
                        group = (
                            ("GROUP", "g", "timed") if command == "XREADGROUP" else ()
                        )
                        cursor = ">" if group else "$"
                        assert (
                            timed.call(
                                command, *group, "BLOCK", 20, "STREAMS", key, cursor
                            )
                            == []
                        )
                    pending = [
                        pool.submit(
                            client.call,
                            "XREADGROUP",
                            "GROUP",
                            "g",
                            f"wait-{index}",
                            *(("BLOCK", 0) if index else ()),
                            "STREAMS",
                            key,
                            ">",
                        )
                        for index, client in enumerate(waiters)
                    ]
                    # Allow both requests to reach the closed gate while FULL
                    # is paused; their calls must stay pending without credit.
                    time.sleep(0.1)
                    assert all(not future.done() for future in pending)
                    H.wait_until(
                        "publisher reservations drain during DB gate wait",
                        5,
                        lambda: 'lavik_fullsync_publish_queue_admitted_bytes{worker="0"} 0\n'
                        in source.metrics(),
                    )
                    assert not scanning.done()
                    assert all(not future.done() for future in pending)
                finally:
                    hold.unlink(missing_ok=True)
                assert key in scanning.result(timeout=10)
                assert pending[0].result(timeout=10) == []
                H.wait_until(
                    "consumer enters normal Stream wait after DB gate opens",
                    10,
                    lambda: " flags=b "
                    in writer.call("CLIENT", "LIST", "ID", blocked_id),
                )
                assert writer.call("CLIENT", "UNBLOCK", blocked_id, "ERROR") == 1
                try:
                    pending[1].result(timeout=10)
                except H.Failure as error:
                    assert "UNBLOCKED" in str(error), error
                else:
                    raise AssertionError("consumer ignored CLIENT UNBLOCK ERROR")
                H.wait_until(
                    "FULL and consumer metadata replay after DB gate opens",
                    15,
                    lambda: len(reader.call("XINFO", "CONSUMERS", key, "g")) == 2,
                )

        finally:
            hold.unlink(missing_ok=True)
            for client in (scanner, timed, reader, *waiters):
                client.close()


def stream_cutover(root):
    fixture = F.FailoverFixture(
        C.META,
        C.DATA,
        C.CTL,
        str(root / "cutover"),
        False,
        pause_after_begin_ms=8000,
        data_workers=2,
        client_mode="single",
    )
    clients = []
    try:
        fixture.start_created(add_follower=True)
        source = fixture.by_id[F.OWNER]
        writer = Client(source)
        clients.append(writer)
        assert writer.call("XGROUP", "CREATE", "cutover", "g", 0, "MKSTREAM") == "OK"
        sleepers = [Client(source), Client(source)]
        clients.extend(sleepers)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            pending = [
                pool.submit(
                    sleepers[0].call, "XREAD", "BLOCK", 0, "STREAMS", "cutover", "$"
                ),
                pool.submit(
                    sleepers[1].call,
                    "XREADGROUP",
                    "GROUP",
                    "g",
                    "old",
                    "BLOCK",
                    0,
                    "STREAMS",
                    "cutover",
                    ">",
                ),
            ]
            H.wait_until(
                "both Stream waiters dormant",
                20,
                lambda: writer.call("CLIENT", "LIST").count(" flags=b ") >= 2,
            )
            operation = fixture.submit_failover()
            if fixture.wait_post_begin_pause():

                def pause_applied():
                    try:
                        writer.call(
                            "XREADGROUP", "GROUP", "g", "new", "STREAMS", "cutover", ">"
                        )
                    except H.Failure as error:
                        return "TRYAGAIN" in str(error)
                    return False

                H.wait_until(
                    "Data applies Controlled Pause admission barrier", 10, pause_applied
                )
            begin = {}

            def begun():
                nonlocal begin
                begin = F.require_unique_failover_event(
                    fixture.metas, "begin", "controlled", loss="none"
                )
                return True

            H.wait_until("controlled Begin", 30, begun)
            F.wait_owner(fixture, begin["candidate"], fixture.data_nodes)
            F.wait_operation(
                fixture,
                operation,
                "OK completed failover-completed",
                "idle Stream waiters do not block drain",
            )
            for future in pending:
                try:
                    result = future.result(timeout=30)
                except H.Failure as error:
                    assert any(
                        token in str(error)
                        for token in (
                            "TRYAGAIN",
                            "MASTERDOWN",
                            "READONLY",
                            "LOADING",
                            "closed",
                        )
                    ), error
                else:
                    raise AssertionError(f"old population waiter returned {result!r}")
            current = Client(fixture.by_id[begin["candidate"]])
            clients.append(current)
            assert current.call("XADD", "cutover", "1-0", "v", "new") == "1-0"
            assert current.call(
                "XREADGROUP", "GROUP", "g", "successor", "STREAMS", "cutover", ">"
            ) == [["cutover", [["1-0", ["v", "new"]]]]]
        for client in clients:
            client.close()
        clients.clear()
        fixture.require_expected_processes_alive()
        fixture.clean_shutdown()
    except BaseException:
        fixture.dump_logs()
        raise
    finally:
        for client in clients:
            client.close()
        fixture.force_kill()


def lua_partial_stream(root, mode):
    with pair(root, f"lua-partial-{mode}", client_mode=mode) as (
        meta,
        source,
        target,
        writer,
    ):
        ready(meta)
        first = "xa" if mode == "single" else "{partial}a"
        second = "{b}k" if mode == "single" else "{partial}b"
        assert writer.call("XADD", first, "1-0", "v", "one") == "1-0"
        assert writer.call("XGROUP", "CREATE", first, "g", 0) == "OK"
        script = "redis.pcall('XREADGROUP','GROUP','g','partial','STREAMS',KEYS[1],KEYS[2],'>','>'); return redis.call('SET',KEYS[3],'caught')"
        probe = "{xa}probe" if mode == "single" else "{partial}probe"
        assert writer.call("EVAL", script, 3, first, second, probe) == "OK"
        assert writer.call("XPENDING", first, "g")[0] == 1
        reader = Client(target, readonly=mode == "cluster")
        try:
            H.wait_until(
                "partial Lua transaction replayed",
                20,
                lambda: reader.call("GET", probe) == "caught",
            )
            assert reader.call("XPENDING", first, "g")[0] == 1, (
                "Lua dropped the successful Stream delta"
            )
        finally:
            reader.close()


def delete_partial_run(root):
    hold = root / "delete.hold"
    variable = "LAVIK_EXEC_DELETE_HOLD_FILE"
    first, second = "xa", "{b}k"
    with pair(
        root,
        "delete-partial",
        client_mode="single",
        source_faults={
            variable: str(hold),
            "LAVIK_EXEC_DELETE_HOLD_KEY": second,
            "LAVIK_EXEC_DELETE_DONE_KEY": first,
        },
    ) as (meta, source, target, writer):
        ready(meta)
        assert writer.call("MSET", first, "first", second, "second") == "OK"
        reader = Client(target)
        try:
            H.wait_until(
                "predecessor keys replayed",
                20,
                lambda: reader.call("MGET", first, second) == ["first", "second"],
            )
            assert writer.call("MULTI") == "OK"
            assert writer.call("DEL", first, second) == "QUEUED"
            hold.touch()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(writer.call, "EXEC")
                try:
                    reached(source, variable)
                    H.wait_until(
                        "first deletion committed",
                        10,
                        lambda: "EXEC deletion committed before held peer"
                        in Path(source.log_path).read_text(),
                    )
                    expire(meta, source, "single")
                    hold.unlink()
                    try:
                        pending.result(timeout=15)
                    except H.Failure as error:
                        assert "closed" in str(error), error
                    else:
                        raise AssertionError(
                            "partial deletion returned a certain outcome"
                        )
                finally:
                    hold.unlink(missing_ok=True)
                    meta.resume()
            current = Client(source)
            try:
                H.wait_until(
                    "source authority restored",
                    20,
                    lambda: current.call("SET", "{xa}probe", "after") == "OK",
                )
                assert current.call("MGET", first, second) == [None, "second"]
                H.wait_until(
                    "post-delete marker replayed",
                    20,
                    lambda: reader.call("GET", "{xa}probe") == "after",
                )
                assert reader.call("MGET", first, second) == [None, "second"], (
                    "partial DEL lost its successful key effect"
                )
            finally:
                current.close()
        finally:
            reader.close()


def main():
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    scenario = sys.argv[5]
    H.set_tag(f"composite-{scenario}")
    with tempfile.TemporaryDirectory(
        prefix="fault-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        root = Path(directory)
        if scenario == "partial":
            for mode in ("single", "cluster"):
                lua_partial_stream(root, mode)
            delete_partial_run(root)
            H.log("PASS")
            return
        if scenario == "db-gate":
            for mode in ("single", "cluster"):
                full_with_db_gate_waiters(root, mode)
            H.log("PASS")
            return
        if scenario == "lifecycle":
            wait_registration_race(root)
            full_with_idle_consumer(root)
            for mode in ("single", "cluster"):
                full_with_db_gate_waiters(root, mode)
            stream_cutover(root)
            H.log("PASS")
            return
        for mode in ("single", "cluster"):
            if scenario == "stream":
                stream_revoke(root, mode, False)
                stream_revoke(root, mode, True)
                stream_revoke(root, mode, True, empty=True)
                stream_revoke(root, mode, True, noack=True)
            else:
                for boundary in ("BEFORE_ROOT", "AFTER_ROOT_WRITE"):
                    for mixed in (False, True):
                        catalog_revoke(root, mode, mixed, boundary)
    H.log("PASS")


if __name__ == "__main__":
    main()
