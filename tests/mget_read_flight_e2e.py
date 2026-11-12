#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Verify MGET read sharing through Redis traces and worker metrics."""

import argparse
import asyncio
import json
from pathlib import Path
import re
import tempfile
import urllib.request

import defrag_payload_e2e as support
from read_flight_e2e import wait_retired

VALUE = bytes(range(256)) * 16
KEY = b"{mget-flight}:disk"


def check_metrics(port, workers):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    text = opener.open(f"http://127.0.0.1:{port}/metrics").read().decode()
    for suffix in (
        "read_flight_eligible_attempts_total",
        "read_flight_metadata_bypass_total",
        "read_flight_leaders_total",
        "read_flight_joins_total",
        "read_flight_cancellations_total",
        "read_flight_rejections_total",
        "read_flight_table_full_total",
        "read_flight_waiter_full_total",
        "read_flight_shared_budget_total",
        "read_flights_pending",
        "read_flight_waiters_pending",
        "shared_read_bytes",
        "shared_read_peak_bytes",
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
            assert admin.batch([[b"SET", KEY, VALUE]]) == [b"OK"]
        # Clean shutdown flushes KEY; pause defrag at startup so recovery's disk
        # location cannot be replaced by a staged relocation before MGET.
        with support.server(args.workers, paused=True) as (admin, metrics):
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
            assert "read_io" in stages and "read_flight_wait" in stages, trace
            assert stages["read_io"].get("observations", 1) == 1, trace
            assert stages["read_flight_wait"]["observations"] == 3, trace
            asyncio.run(wait_retired(admin))
            check_metrics(metrics, args.workers)
    print("MGET disk sharing/trace/worker metrics/retirement passed")


if __name__ == "__main__":
    main()
