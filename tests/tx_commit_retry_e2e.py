#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

"""Recover acknowledged transactions after foreground commit space returns."""

import argparse
import contextlib
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import time
import urllib.request

from defrag_payload_e2e import Client, free_ports


def command(client, *args):
    return client.batch(
        [[str(a).encode() if not isinstance(a, bytes) else a for a in args]]
    )[0]


def stats(client):
    return dict(
        line.split(b":", 1)
        for line in command(client, "INFO", "STATS").splitlines()
        if b":" in line
    )


def wait_for(predicate, description, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(description)


def drain(client):
    def complete():
        values = stats(client)
        assert values.get(b"storage_durability_failed", b"0") == b"0", values
        return values[b"storage_durability_pending"] == b"0"

    wait_for(complete, "durability did not complete")


@contextlib.contextmanager
def server(binary, path, *, checkpoint, workers=1, environment=None):
    port, metrics = free_ports()
    args = [
        binary,
        "--bind=127.0.0.1",
        f"--port={port}",
        f"--metrics-port={metrics}",
        f"--threads={workers}",
        "--no-pin-workers",
        "--max-memory=2G",
        f"--data-file={path / 'data'}",
        "--defrag-paused",
        "--logtostderr",
        f"--log-dir={path / 'logs'}",
    ]
    if not checkpoint:
        args.append("--no-shutdown-checkpoint")
    with (path / "server.log").open("a") as log:
        process = subprocess.Popen(
            args,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=os.environ | (environment or {}),
        )
    client = None
    try:

        def connect():
            nonlocal client
            assert process.poll() is None, (path / "server.log").read_text()
            try:
                client = Client(port)
                return command(client, "PING") == b"PONG"
            except OSError:
                return False

        wait_for(connect, "server startup", 60)
        yield client, process
    finally:
        if client:
            client.close()
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise


def natural(binary, path, kind, checkpoint, shutdown=False):
    path.mkdir()
    with (path / "data").open("wb") as disk:
        disk.truncate(96 * 1024 * 1024)
    # One ordinary block plus two exactly full transaction blocks leaves no
    # foreground successor for the decision. The protected reserve stays free.
    keys = [f"k{i:07d}".encode() for i in range(4092)]
    value = b"x" * 4008
    with server(binary, path, checkpoint=checkpoint) as (client, process):
        assert command(client, "SELECT", 1) == b"OK"
        assert command(client, "SET", "filler", "ordinary") == b"OK"
        drain(client)
        assert command(client, "SELECT", 0) == b"OK"
        if kind == "mset":
            assert (
                command(
                    client, "MSET", *(part for key in keys for part in (key, value))
                )
                == b"OK"
            )
        elif kind == "exec":
            assert command(client, "MULTI") == b"OK"
            assert client.batch([[b"SET", key, value] for key in keys]) == [
                b"QUEUED"
            ] * len(keys)
            assert command(client, "EXEC") == [b"OK"] * len(keys)
        else:
            assert (
                command(
                    client,
                    "EVAL",
                    "for i=1,#KEYS do redis.call('SET',KEYS[i],ARGV[1]) end; return 'OK'",
                    len(keys),
                    *keys,
                    value,
                )
                == b"OK"
            )
        wait_for(
            lambda: "commit append failed: out of disk space"
            in (path / "server.log").read_text(),
            "commit never exhausted space",
        )
        assert stats(client)[b"storage_tx_commits_pending"] == b"1", (
            "failed commit was discarded"
        )
        assert command(client, "GET", keys[0]) == value
        if shutdown:
            start = time.monotonic()
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=4) != 0, (
                "shutdown certified an abandoned commit"
            )
            assert time.monotonic() - start < 4, "shutdown added a retry drain window"
        else:
            assert command(client, "SELECT", 1) == b"OK"
            assert command(client, "FLUSHDB") == b"OK"
            assert command(client, "DEFRAG", "RESUME") == b"OK"
            assert command(client, "SELECT", 0) == b"OK"
            drain(client)
            values = stats(client)
            assert int(values[b"tx_commit_retries"]) > 0
            assert values[b"storage_durability_failed"] == b"0"
    assert process.returncode == (1 if shutdown else 0), (
        path / "server.log"
    ).read_text()
    with server(binary, path, checkpoint=checkpoint) as (client, process):
        assert command(client, "DBSIZE") == (b"0" if shutdown else b"4092")
        assert command(client, "GET", keys[0]) == (None if shutdown else value)
        assert command(client, "GET", keys[-1]) == (None if shutdown else value)
    assert process.returncode == 0


def injected(binary, path, checkpoint, failure=None):
    path.mkdir()
    with (path / "data").open("wb") as disk:
        disk.truncate(256 * 1024 * 1024)
    base = path / "commit"
    pause = Path(str(base) + ".pause")
    fault = Path(
        str(base) + (".full" if failure in (None, "shutdown") else "." + failure)
    )
    pause.touch()
    fault.touch()
    environment = {
        "LAVIK_TX_COMMIT_TEST_GATE": str(base),
        "LAVIK_TX_COMMIT_TEST_TXID": "2",
    }
    with server(binary, path, checkpoint=checkpoint, environment=environment) as (
        client,
        process,
    ):
        for i in range(1, 4):
            assert command(client, "MSET", f"a{i}", "a", f"b{i}", "b") == b"OK"
        assert stats(client)[b"storage_tx_commits_pending"] == b"3"
        if failure == "shutdown":
            # Admission closes before the drain's first attempt. A succeeds,
            # B first encounters exhaustion during shutdown, and C is dropped.
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=4) != 0
            log = (path / "server.log").read_text()
            assert log.count("tx commit test attempt txid=2") == 1
            assert "tx commit test attempt txid=3" not in log
            assert "all storage buffers durably flushed" not in log
            return
        pause.unlink()
        if failure is not None:
            wait_for(
                lambda: "runtime storage failure fenced request serving"
                in (path / "server.log").read_text(),
                "terminal commit error did not fence serving",
            )
            values = stats(client)
            assert values[b"storage_durability_failed"] == b"1"
            assert values[b"storage_tx_commits_pending"] == b"0"
            port = next(
                a.split("=", 1)[1]
                for a in process.args
                if a.startswith("--metrics-port=")
            )
            exported = urllib.request.urlopen(
                f"http://127.0.0.1:{port}/metrics", timeout=5
            ).read()
            assert b"lavik_storage_durability_failed 1\n" in exported
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=4) != 0
            log = (path / "server.log").read_text()
            assert "tx commit test attempt txid=2" in log
            assert "all storage buffers durably flushed" not in log
            assert log.count("tx commit test attempt txid=2") == 1
            return
        wait_for(
            lambda: "tx commit test durable txid=1"
            in (path / "server.log").read_text(),
            "appended prefix did not flush before retry",
        )
        # These arrivals must join behind the failed cursor and its batch tail.
        for i in range(4, 6):
            assert command(client, "MSET", f"a{i}", "a", f"b{i}", "b") == b"OK"
        wait_for(
            lambda: int(stats(client)[b"tx_commit_retries"]) >= 3,
            "capacity error was not retried",
        )
        values = stats(client)
        assert values[b"storage_tx_commits_pending"] == b"4"
        assert values[b"tx_commit_queue_depth"] == b"2"
        assert values[b"tx_commit_batch_transactions"] == b"3"
        log = (path / "server.log").read_text()
        assert re.findall(r"tx commit test appended txid=(\d+)", log) == ["1"]
        ticks = [
            int(x)
            for x in re.findall(r"tx commit test attempt txid=2 tick_ns=(\d+)", log)
        ]
        assert all(b - a >= 90_000_000 for a, b in zip(ticks, ticks[1:])), ticks
        fault.unlink()
        drain(client)
        log = (path / "server.log").read_text()
        assert re.findall(r"tx commit test appended txid=(\d+)", log) == [
            "1",
            "2",
            "3",
            "4",
            "5",
        ]
        assert stats(client)[b"tx_commit_batch_transactions"] == b"5"
    assert process.returncode == 0
    with server(binary, path, checkpoint=checkpoint) as (client, process):
        assert command(client, "DBSIZE") == b"10"
    assert process.returncode == 0


def matrix_cases():
    large = b"x" * 32768
    members = [f"{i:04d}-".encode() + b"m" * 251 for i in range(128)]
    pairs = [p for i in range(128) for p in (f"f{i}", b"v" * 256)]
    scored = [p for i, member in enumerate(members) for p in (i, member)]
    sets = [("SADD", "s1", "a"), ("SADD", "s2", "b")]
    zsets = [("ZADD", "z1", 1, "a"), ("ZADD", "z2", 2, "b")]
    return {
        "string": ([], ("SET", "target", large), ("GET", "target"), large),
        "hash": ([], ("HSET", "target", *pairs), ("HLEN", "target"), b"128"),
        "set": ([], ("SADD", "target", *members), ("SCARD", "target"), b"128"),
        "list": ([], ("RPUSH", "target", *members), ("LLEN", "target"), b"128"),
        "zset": ([], ("ZADD", "target", *scored), ("ZCARD", "target"), b"128"),
        "stream": ([], ("XADD", "target", "1-0", "f", large), ("XLEN", "target"), b"1"),
        "ttl": (
            [("SET", "target", large)],
            ("EXPIRE", "target", 3600),
            ("STRLEN", "target"),
            b"32768",
        ),
        "smove": (
            sets,
            ("SMOVE", "s1", "target", "a"),
            ("SISMEMBER", "target", "a"),
            b"1",
        ),
        "sunionstore": (
            sets,
            ("SUNIONSTORE", "target", "s1", "s2"),
            ("SCARD", "target"),
            b"2",
        ),
        "sinterstore": (
            sets,
            ("SINTERSTORE", "s1", "s1", "s2"),
            ("EXISTS", "s1"),
            b"0",
        ),
        "sdiffstore": (sets, ("SDIFFSTORE", "s1", "s1", "s2"), ("SCARD", "s1"), b"1"),
        "zunionstore": (
            zsets,
            ("ZUNIONSTORE", "target", 2, "z1", "z2"),
            ("ZCARD", "target"),
            b"2",
        ),
        "zinterstore": (
            zsets,
            ("ZINTERSTORE", "z1", 2, "z1", "z2"),
            ("EXISTS", "z1"),
            b"0",
        ),
        "zdiffstore": (
            zsets,
            ("ZDIFFSTORE", "z1", 2, "z1", "z2"),
            ("ZCARD", "z1"),
            b"1",
        ),
        "zrangestore": (
            zsets,
            ("ZRANGESTORE", "z1", "z1", 0, -1),
            ("ZCARD", "z1"),
            b"1",
        ),
        "geo": (
            [("GEOADD", "geo", 13.3, 38.1, "place")],
            (
                "GEOSEARCHSTORE",
                "target",
                "geo",
                "FROMMEMBER",
                "place",
                "BYRADIUS",
                10,
                "km",
            ),
            ("ZCARD", "target"),
            b"1",
        ),
        "msetnx": (
            [],
            ("MSETNX", "target", "new", "other", "new"),
            ("GET", "target"),
            b"new",
        ),
        "copy": (
            [("SET", "source", "old")],
            ("COPY", "source", "target"),
            ("GET", "target"),
            b"old",
        ),
        "rename": (
            [("SET", "source", "old")],
            ("RENAME", "source", "target"),
            ("GET", "target"),
            b"old",
        ),
    }


def matrix(binary, path, workers, name, case):
    path.mkdir()
    with (path / "data").open("wb") as disk:
        disk.truncate(512 * 1024 * 1024)
    seed, mutation, query, expected = case
    base = path / "commit"
    fault = Path(str(base) + ".full")
    with server(
        binary,
        path,
        checkpoint=False,
        workers=workers,
        environment={"LAVIK_TX_COMMIT_TEST_GATE": str(base)},
    ) as (client, process):
        for args in seed:
            command(client, *args)
        drain(client)
        fault.touch()
        command(client, *mutation)
        wait_for(
            lambda: int(stats(client)[b"tx_commit_retries"]) >= 2,
            f"{name} bypassed the retry queue",
        )
        assert stats(client)[b"storage_durability_failed"] == b"0"
        fault.unlink()
        drain(client)
        assert command(client, *query) == expected
        if name == "ttl":
            assert int(command(client, "TTL", "target")) > 3500
    assert process.returncode == 0
    with server(binary, path, checkpoint=False, workers=workers) as (client, process):
        assert command(client, *query) == expected
        if name == "ttl":
            assert int(command(client, "TTL", "target")) > 3500
    assert process.returncode == 0


def superseded(binary, path, operation, checkpoint):
    path.mkdir()
    with (path / "data").open("wb") as disk:
        disk.truncate(256 * 1024 * 1024)
    base = path / "commit"
    fault = Path(str(base) + ".full")
    with server(
        binary,
        path,
        checkpoint=checkpoint,
        environment={"LAVIK_TX_COMMIT_TEST_GATE": str(base)},
    ) as (client, process):
        assert command(client, "SET", "target", "old") == b"OK"
        drain(client)
        fault.touch()
        assert command(client, "MSET", "target", "tentative", "other", "value") == b"OK"
        wait_for(lambda: int(stats(client)[b"tx_commit_retries"]) >= 1, "no retry")
        mutations = {
            "overwrite": ("SET", "target", "latest"),
            "delete": ("DEL", "target"),
            "expire": ("PEXPIRE", "target", 1),
            "flushdb": ("FLUSHDB",),
            "flushall": ("FLUSHALL",),
        }
        command(client, *mutations[operation])
        expected = b"latest" if operation == "overwrite" else None
        wait_for(
            lambda: command(client, "GET", "target") == expected,
            "superseding mutation invisible",
        )
        fault.unlink()
        drain(client)
        assert command(client, "GET", "target") == expected
    assert process.returncode == 0
    with server(binary, path, checkpoint=checkpoint) as (client, process):
        assert command(client, "GET", "target") == expected
        assert command(client, "GET", "other") == (
            None if operation.startswith("flush") else b"value"
        )
    assert process.returncode == 0


def borrowed_and_direct(binary, path, workers):
    path.mkdir()
    with (path / "data").open("wb") as disk:
        disk.truncate(512 * 1024 * 1024)
    base = path / "commit"
    fault = Path(str(base) + ".full")
    with server(
        binary,
        path,
        checkpoint=False,
        workers=workers,
        environment={"LAVIK_TX_COMMIT_TEST_GATE": str(base)},
    ) as (client, process):
        command(client, "SADD", "set-source", "member")
        command(client, "ZADD", "zset-source", 1, "member")
        command(client, "RPUSH", "list-source", 2, 1)
        drain(client)
        for lua in (False, True):
            before = int(stats(client)[b"tx_commit_batch_transactions"])
            fault.touch()
            if lua:
                command(
                    client,
                    "EVAL",
                    "redis.call('SUNIONSTORE',KEYS[1],KEYS[2]); return redis.call('ZRANGESTORE',KEYS[3],KEYS[4],0,-1)",
                    4,
                    "set-target-lua",
                    "set-source",
                    "zset-target-lua",
                    "zset-source",
                )
            else:
                command(client, "MULTI")
                command(client, "SUNIONSTORE", "set-target-exec", "set-source")
                command(client, "ZUNIONSTORE", "zset-target-exec", 1, "zset-source")
                assert command(client, "EXEC") == [b"1", b"1"]
            wait_for(
                lambda: int(stats(client)[b"storage_tx_commits_pending"]) == 1,
                "outer commit missing",
            )
            wait_for(
                lambda: "commit append failed" in (path / "server.log").read_text(),
                "outer commit did not reach capacity failure",
            )
            fault.unlink()
            drain(client)
            assert int(stats(client)[b"tx_commit_batch_transactions"]) == before + 1
        # These APIs await their commit, so failure still belongs to the
        # command's rollback path rather than the background retry queue.
        fault.touch()
        before = int(stats(client)[b"tx_commit_retries"])
        for args in (
            ("LMOVE", "list-source", "list-target", "LEFT", "RIGHT"),
            ("SORT", "list-source", "STORE", "sort-target"),
        ):
            try:
                command(client, *args)
            except AssertionError as error:
                assert "out of disk space" in str(error), error
            else:
                raise AssertionError(f"direct commit did not fail: {args}")
        fault.unlink()
        drain(client)
        assert int(stats(client)[b"tx_commit_retries"]) == before
        assert command(client, "LRANGE", "list-source", 0, -1) == [b"2", b"1"]
        assert command(client, "EXISTS", "list-target", "sort-target") == b"0"
    assert process.returncode == 0


def backpressure_shutdown(binary, path):
    path.mkdir()
    with (path / "data").open("wb") as disk:
        disk.truncate(256 * 1024 * 1024)
    base = path / "commit"
    Path(str(base) + ".full").touch()
    with server(
        binary,
        path,
        checkpoint=False,
        environment={"LAVIK_TX_COMMIT_TEST_GATE": str(base)},
    ) as (client, process):
        command(client, "MSET", "a", "a", "b", "b")
        wait_for(
            lambda: int(stats(client)[b"tx_commit_retries"]) >= 1,
            "head did not enter backoff",
        )
        # Leave replies unread: the request that crosses 4096 must be parked
        # in capacity waiting when SIGTERM begins draining accepted requests.
        parts = (b"MSET", b"a", b"a", b"b", b"b")
        wire = b"*5\r\n" + b"".join(b"$%d\r\n" % len(p) + p + b"\r\n" for p in parts)
        client.socket.sendall(wire * 4100)
        observer = Client(client.socket.getpeername()[1])
        try:
            wait_for(
                lambda: int(stats(observer)[b"tx_commit_backpressure_waits"]) > 0,
                "watermark never reached",
            )
            assert int(stats(observer)[b"tx_commit_queue_depth"]) >= 4096
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=4) != 0
        finally:
            observer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument(
        "--case",
        choices=("all", "natural", "shutdown", "injected", "matrix", "superseded"),
        default="all",
    )
    args = parser.parse_args()
    root = Path(
        tempfile.mkdtemp(
            prefix="lavik-tx-commit-retry-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
        )
    )
    print(f"artifacts: {root}", flush=True)
    for checkpoint in (False, True):
        if args.case in ("all", "natural"):
            for kind in ("mset", "exec", "lua"):
                natural(args.binary, root / f"{kind}-{checkpoint}", kind, checkpoint)
                print(f"PASS {kind} checkpoint={checkpoint}", flush=True)
        if args.case in ("all", "shutdown"):
            natural(
                args.binary,
                root / f"shutdown-{checkpoint}",
                "mset",
                checkpoint,
                shutdown=True,
            )
            print(f"PASS shutdown checkpoint={checkpoint}", flush=True)
        if (
            args.case in ("all", "injected")
            and os.environ.get("LAVIK_TEST_FAULTS_AVAILABLE", "1") == "1"
        ):
            for failure in (None, "terminal", "flush-fail", "shutdown"):
                injected(
                    args.binary,
                    root / f"injected-{failure}-{checkpoint}",
                    checkpoint,
                    failure,
                )
                print(f"PASS injected={failure} checkpoint={checkpoint}", flush=True)
        if (
            args.case in ("all", "superseded")
            and os.environ.get("LAVIK_TEST_FAULTS_AVAILABLE", "1") == "1"
        ):
            for operation in (
                "overwrite",
                "delete",
                "expire",
                "flushdb",
                "flushall",
            ):
                superseded(
                    args.binary,
                    root / f"{operation}-{checkpoint}",
                    operation,
                    checkpoint,
                )
                print(f"PASS {operation} checkpoint={checkpoint}", flush=True)
    if (
        args.case in ("all", "matrix")
        and os.environ.get("LAVIK_TEST_FAULTS_AVAILABLE", "1") == "1"
    ):
        for workers in (1, 3):
            for name, case in matrix_cases().items():
                matrix(args.binary, root / f"{name}-{workers}", workers, name, case)
                print(f"PASS {name} workers={workers}", flush=True)
            borrowed_and_direct(args.binary, root / f"borrowed-{workers}", workers)
            print(f"PASS borrowed and direct workers={workers}", flush=True)
        backpressure_shutdown(args.binary, root / "backpressure-shutdown")
        print("PASS backpressure shutdown", flush=True)
    # Keep logs for CI diagnostics without retaining successful test devices.
    for disk in root.glob("*/data"):
        disk.unlink()


if __name__ == "__main__":
    main()
