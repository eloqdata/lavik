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

"""Measure large ZSET RESTORE and unrelated PING latency on one Lavik worker.

Redis supplies valid DUMP payloads; only Lavik's RESTORE interval is timed.
Example: python3 tests/restore_single_worker_bench.py --lavik build_debug/lavik
"""

import argparse
import json
import math
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class RedisClient:
    def __init__(self, port, timeout=240):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.input = self.sock.makefile("rb")

    def close(self):
        self.input.close()
        self.sock.close()

    def command(self, *arguments):
        parts = [
            arg if isinstance(arg, bytes) else str(arg).encode() for arg in arguments
        ]
        request = [f"*{len(parts)}\r\n".encode()]
        for part in parts:
            request.extend((f"${len(part)}\r\n".encode(), part, b"\r\n"))
        self.sock.sendall(b"".join(request))
        return self.reply()

    def reply(self):
        prefix = self.input.read(1)
        if not prefix:
            raise ConnectionError("connection closed before RESP reply")
        if prefix in (b"+", b"-", b":"):
            body = self.input.readline().rstrip(b"\r\n")
            if prefix == b"-":
                raise RuntimeError(body.decode(errors="replace"))
            return int(body) if prefix == b":" else body
        if prefix == b"$":
            length = int(self.input.readline())
            if length == -1:
                return None
            payload = self.input.read(length)
            if len(payload) != length or self.input.read(2) != b"\r\n":
                raise ConnectionError("incomplete RESP bulk reply")
            return payload
        raise ValueError(f"unexpected RESP prefix: {prefix!r}")


def wait_ready(port, process):
    until = time.monotonic() + 20
    while time.monotonic() < until:
        if process.poll() is not None:
            raise RuntimeError(f"server exited with status {process.returncode}")
        try:
            client = RedisClient(port, timeout=1)
            if client.command("PING") == b"PONG":
                # Readiness probes are short; benchmark commands can run longer.
                client.sock.settimeout(240)
                return client
            client.close()
        except (OSError, ConnectionError):
            time.sleep(0.05)
    raise TimeoutError(f"server on port {port} did not become ready")


def percentile(samples, quantile):
    if not samples:
        return None
    ordered = sorted(samples)
    return round(ordered[math.ceil(quantile * len(ordered)) - 1] * 1000, 3)


def ping_during(port, stop, samples, errors):
    try:
        client = RedisClient(port, timeout=240)
        try:
            while not stop.is_set():
                start = time.monotonic()
                if client.command("PING") != b"PONG":
                    raise RuntimeError("PING reply was not PONG")
                samples.append(time.monotonic() - start)
                stop.wait(0.01)
        finally:
            client.close()
    except Exception as exc:  # The error must be reported with the RESTORE result.
        errors.append(str(exc))


def run_case(redis, lavik, lavik_port, count):
    source = f"source:{count}"
    target = f"restore:{count}"
    # Use small ZADD batches so payload construction does not need a huge array.
    for begin in range(0, count, 1000):
        args = ["ZADD", source]
        for index in range(begin, min(begin + 1000, count)):
            args.extend((index, f"member-{index:09d}"))
        redis.command(*args)
    payload = redis.command("DUMP", source)
    if payload is None:
        raise RuntimeError("Redis did not produce a DUMP payload")

    stop = threading.Event()
    pings = []
    errors = []
    probe = threading.Thread(
        target=ping_during, args=(lavik_port, stop, pings, errors), daemon=True
    )
    probe.start()
    start = time.monotonic()
    try:
        response = lavik.command("RESTORE", target, 0, payload, "REPLACE", "ABSTTL")
        elapsed = time.monotonic() - start
    except Exception:
        print(
            json.dumps(
                {
                    "members": count,
                    "dump_bytes": len(payload),
                    "elapsed_before_error_seconds": round(time.monotonic() - start, 3),
                    "ping_samples": len(pings),
                    "ping_max_ms": percentile(pings, 1),
                }
            ),
            flush=True,
        )
        raise
    finally:
        stop.set()
        probe.join(timeout=5)
    if errors:
        raise RuntimeError(f"concurrent PING failed: {errors}")
    if response != b"OK":
        raise RuntimeError(f"RESTORE returned {response!r}")
    if lavik.command("ZCARD", target) != count:
        raise RuntimeError("restored ZSET cardinality differs from source")
    for index in (0, count - 1):
        if float(lavik.command("ZSCORE", target, f"member-{index:09d}")) != index:
            raise RuntimeError(f"restored ZSET score differs at member {index}")
    return {
        "members": count,
        "dump_bytes": len(payload),
        "restore_seconds": round(elapsed, 3),
        "ping_samples": len(pings),
        "ping_p50_ms": percentile(pings, 0.5),
        "ping_p99_ms": percentile(pings, 0.99),
        "ping_max_ms": percentile(pings, 1),
    }


def run_zadd_case(lavik, count):
    key = f"zadd:{count}"
    arguments = ["ZADD", key]
    for index in range(count):
        arguments.extend((index, f"member-{index:09d}"))
    start = time.monotonic()
    added = lavik.command(*arguments)
    elapsed = time.monotonic() - start
    if added != count or lavik.command("ZCARD", key) != count:
        raise RuntimeError("large ZADD cardinality differs from input")
    for index in (0, count - 1):
        if float(lavik.command("ZSCORE", key, f"member-{index:09d}")) != index:
            raise RuntimeError(f"large ZADD score differs at member {index}")
    return {"zadd_members": count, "zadd_seconds": round(elapsed, 3)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lavik", type=Path, required=True)
    parser.add_argument("--redis-server", default="redis-server")
    parser.add_argument("--counts", type=int, nargs="+", default=[10_000, 100_000])
    parser.add_argument("--zadd-count", type=int)
    parser.add_argument("--max-memory", default="10G")
    parser.add_argument("--data-file-gib", type=int, default=8)
    args = parser.parse_args()
    if any(count <= 0 for count in args.counts):
        parser.error("all counts must be positive")
    if args.zadd_count is not None and args.zadd_count <= 0:
        parser.error("--zadd-count must be positive")
    if args.data_file_gib <= 0:
        parser.error("--data-file-gib must be positive")
    lavik_binary = args.lavik.resolve(strict=True)
    redis_port, lavik_port = free_port(), free_port()
    with tempfile.TemporaryDirectory(prefix="lavik-restore-bench-") as root:
        work = Path(root)
        data_file = work / "lavik.data"
        with data_file.open("wb") as data:
            data.truncate(args.data_file_gib * 1024**3)
        with (
            (work / "redis.log").open("wb") as redis_log,
            (work / "lavik.log").open("wb") as lavik_log,
        ):
            redis_process = subprocess.Popen(
                [
                    args.redis_server,
                    "--bind",
                    "127.0.0.1",
                    "--port",
                    str(redis_port),
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                    "--dir",
                    str(work),
                ],
                stdout=redis_log,
                stderr=subprocess.STDOUT,
            )
            lavik_process = subprocess.Popen(
                [
                    str(lavik_binary),
                    "--bind",
                    "127.0.0.1",
                    "--port",
                    str(lavik_port),
                    "--threads",
                    "1",
                    "--no-pin-workers",
                    "--recv-buffers-per-worker",
                    "0",
                    "--max-memory",
                    args.max_memory,
                    "--data-file",
                    str(data_file),
                    "--logtostderr",
                ],
                stdout=lavik_log,
                stderr=subprocess.STDOUT,
            )
            try:
                redis = wait_ready(redis_port, redis_process)
                lavik = wait_ready(lavik_port, lavik_process)
                try:
                    for count in args.counts:
                        print(
                            json.dumps(run_case(redis, lavik, lavik_port, count)),
                            flush=True,
                        )
                    if args.zadd_count is not None:
                        print(
                            json.dumps(run_zadd_case(lavik, args.zadd_count)),
                            flush=True,
                        )
                finally:
                    redis.close()
                    lavik.close()
            except Exception:
                print("Lavik log tail:", flush=True)
                print((work / "lavik.log").read_text(errors="replace")[-8000:])
                raise
            finally:
                for process in (lavik_process, redis_process):
                    process.terminate()
                for process in (lavik_process, redis_process):
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()


if __name__ == "__main__":
    main()
