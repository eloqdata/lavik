#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""MGET sharing, mixed fallback, worker metrics, and transaction replies."""

import argparse
import asyncio
import json
from pathlib import Path
import re
import tempfile
import urllib.request

import defrag_payload_e2e as support
from read_flight_e2e import connect, wait_retired
from remote_get_e2e import drain, stats

VALUE = bytes(range(256)) * 16
LARGE = VALUE * 32
KEY = b"{mget-flight}:disk"
GROUP = b"{mget-flight}:group"
LIST = b"{mget-flight}:list"
STAGED = b"{mget-flight}:fresh"
MISSING = b"{mget-flight}:missing"
KEYS = [KEY] * 40 + [GROUP, LIST, MISSING, STAGED, KEY]
EXPECTED = [VALUE] * 40 + [LARGE, None, None, b"fresh", VALUE]


def wire(args):
    return b"*%d\r\n" % len(args) + b"".join(
        b"$%d\r\n" % len(v) + v + b"\r\n" for v in args
    )


async def response(reader):
    head = await reader.readline()
    assert head.endswith(b"\r\n"), head
    if head[:1] == b"*":
        return [await response(reader) for _ in range(int(head[1:-2]))]
    if head == b"$-1\r\n":
        return None
    assert head[:1] == b"$", head
    value = await reader.readexactly(int(head[1:-2]))
    assert await reader.readexactly(2) == b"\r\n"
    return value


async def traffic(port):
    clients = await asyncio.gather(*(connect(port) for _ in range(16)))
    request = wire([b"MGET", *KEYS])
    for _, writer in clients:
        writer.write(request * 16)

    async def receive(reader, writer):
        try:
            for _ in range(16):
                assert await response(reader) == EXPECTED
        finally:
            writer.close()
            await writer.wait_closed()

    await asyncio.wait_for(asyncio.gather(*(receive(*c) for c in clients)), 45)


def check_metrics(port, workers):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    text = opener.open(f"http://127.0.0.1:{port}/metrics").read().decode()
    for suffix in (
        "read_flight_leaders_total",
        "read_flight_joins_total",
        "read_flight_cancellations_total",
        "read_flight_rejections_total",
        "read_flight_table_full_total",
        "read_flight_waiter_full_total",
        "read_flight_shared_budget_total",
        "read_flights_pending",
        "shared_read_bytes",
    ):
        aggregate = re.findall(rf"^lavik_storage_{suffix} (\d+)$", text, re.M)
        owners = re.findall(
            rf'^lavik_storage_worker_{suffix}\{{worker="(\d+)"\}} (\d+)$', text, re.M
        )
        assert len(aggregate) == 1 and len(owners) == workers, (suffix, owners)
        assert {int(w) for w, _ in owners} == set(range(workers))
        assert int(aggregate[0]) == sum(int(v) for _, v in owners), suffix


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binary")
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    support.BINARY = str(Path(args.binary).resolve())
    with tempfile.TemporaryDirectory(prefix="lavik-mget-flight-") as directory:
        support.WORKDIR = Path(directory)
        with (support.WORKDIR / "data").open("wb") as f:
            f.truncate(512 * 1024 * 1024)
        with support.server(args.workers) as (admin, _):
            assert admin.batch([[b"SET", KEY, VALUE], [b"SET", GROUP, LARGE]]) == [
                b"OK",
                b"OK",
            ]
            assert admin.batch([[b"LPUSH", LIST, b"entry"]]) == [b"1"]
            drain(admin)
        with support.server(args.workers) as (admin, metrics):
            assert admin.batch([[b"SET", STAGED, b"fresh"]]) == [b"OK"]
            for setting, value in (
                (b"log-enabled", b"no"),
                (b"sample-every", b"1"),
                (b"slow-threshold-us", b"0"),
                (b"enabled", b"yes"),
            ):
                assert admin.batch(
                    [[b"CONFIG", b"SET", b"latency-diagnostics-" + setting, value]]
                ) == [b"OK"]
            assert admin.batch([[b"MGET", KEY, KEY, KEY, KEY]]) == [[VALUE] * 4]
            records = [
                json.loads(v)
                for v in admin.batch([[b"LATENCY", b"TRACE", b"GET", b"16"]])[0]
            ]
            trace = next(t for t in records if t["command"] == "MGET")
            stages = {s["name"]: s for s in trace["stages"]}
            assert stages["read_io"].get("observations", 1) == 1, trace
            assert stages["read_flight_wait"]["observations"] == 3, trace
            assert admin.batch(
                [[b"CONFIG", b"SET", b"latency-diagnostics-enabled", b"no"]]
            ) == [b"OK"]
            before = stats(admin)
            assert admin.batch([[b"MGET", *KEYS]]) == [EXPECTED]
            # The same MGET in MULTI must keep its wave barrier and nil semantics.
            assert admin.batch([[b"MULTI"], [b"MGET", *KEYS], [b"EXEC"]]) == [
                b"OK",
                b"QUEUED",
                [EXPECTED],
            ]
            asyncio.run(traffic(admin.socket.getpeername()[1]))
            after = asyncio.run(wait_retired(admin))
            assert int(after[b"storage_read_flight_joins"]) > int(
                before[b"storage_read_flight_joins"]
            ), after
            check_metrics(metrics, args.workers)
            assert admin.batch([[b"GET", KEY], [b"GET", GROUP]]) == [VALUE, LARGE]
        with support.server(args.workers) as (admin, _):
            assert admin.batch([[b"MGET", *KEYS]]) == [EXPECTED]
    print("MGET mixed waves/transactions/worker metrics/recovery passed")


if __name__ == "__main__":
    main()
