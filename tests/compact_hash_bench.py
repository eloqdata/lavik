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

"""Run alternating baseline/candidate compact Hash RESP benchmarks with memtier.

Owns a fresh sparse data file and server for each case; never uses an existing
server or dataset. Raw memtier JSON, logs and actual commands stay in --output.
Use --data-directory /dev/shm for a CPU-focused tmpfs run, or a real filesystem
for storage-inclusive measurements. Neither is a durable-device benchmark.
"""

import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time


class Client:
    def __init__(self, port):
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=15)
        self.stream = self.socket.makefile("rb")

    def close(self):
        self.stream.close()
        self.socket.close()

    def command(self, *args):
        args = [arg if isinstance(arg, bytes) else str(arg).encode() for arg in args]
        wire = b"*%d\r\n" % len(args)
        wire += b"".join(b"$%d\r\n" % len(arg) + arg + b"\r\n" for arg in args)
        self.socket.sendall(wire)
        return self.read()

    def read(self):
        line = self.stream.readline()
        if not line:
            raise RuntimeError("server disconnected")
        kind, body = line[:1], line[1:-2]
        if kind == b"-":
            raise RuntimeError(body.decode())
        if kind == b":":
            return int(body)
        if kind == b"$":
            size = int(body)
            if size == -1:
                return None
            data = self.stream.read(size)
            assert self.stream.read(2) == b"\r\n"
            return data
        if kind == b"*":
            return [self.read() for _ in range(int(body))]
        return body


def field(index):
    return f"field-{index}".ljust(32, "f")


def run_case(args, binary, label, count, operation, repeat):
    stem = f"{count}-{operation}-{repeat}-{label}"
    with tempfile.TemporaryDirectory(
        prefix="compact-hash-", dir=args.data_directory
    ) as data:
        disk = Path(data) / "data"
        with disk.open("wb") as stream:
            stream.truncate(4 * 1024**3)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        server_cmd = [
            str(binary),
            "--port",
            str(port),
            "--threads",
            "1",
            "--data-file",
            str(disk),
            "--max-memory",
            "1G",
            "--metrics-port",
            "0",
            "--logtostderr",
        ]
        if args.server_cpu is not None:
            server_cmd += ["--cpus", str(args.server_cpu)]
        commands = {"server": server_cmd}
        with (args.output / f"{stem}-server.log").open("w") as log:
            server = subprocess.Popen(server_cmd, stdout=log, stderr=subprocess.STDOUT)
            client = None
            try:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if server.poll() is not None:
                        raise RuntimeError(f"server exited: {stem}")
                    try:
                        client = Client(port)
                        assert client.command("PING") == b"PONG"
                        break
                    except OSError:
                        time.sleep(0.05)
                if client is None:
                    raise RuntimeError("server startup timeout")
                seed = []
                for index in range(count):
                    seed += [field(index), b"a" * 64]
                for key in range(args.keys):
                    assert client.command("HSET", f"compact:{key}", *seed) == count
                selected = [
                    field(count - 1 - i)
                    for i in range(8 if operation.endswith("8") else 1)
                ]
                if operation.startswith("hset"):
                    command = "HSET __key__ " + " ".join(
                        f"{name} __data__" for name in selected
                    )
                else:
                    command = (
                        ("HMGET" if operation == "hmget8" else "HGET")
                        + " __key__ "
                        + " ".join(selected)
                    )
                bench = [
                    "memtier_benchmark",
                    "--server",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--threads",
                    "2",
                    "--clients",
                    "4",
                    "--pipeline",
                    "8",
                    "--key-prefix",
                    "compact:",
                    "--key-maximum",
                    str(args.keys - 1),
                    "--command",
                    command,
                    "--hide-histogram",
                ]
                if operation.startswith("hset"):
                    bench += ["--data-size", "64", "--random-data"]
                if args.client_cpus:
                    bench = ["taskset", "-c", args.client_cpus, *bench]
                warmup = [*bench, "--test-time", "1"]
                measured = [
                    *bench,
                    "--test-time",
                    str(args.seconds),
                    "--json-out-file",
                    str(args.output / f"{stem}.json"),
                ]
                commands.update(warmup=warmup, measured=measured)
                (args.output / f"{stem}-commands.json").write_text(
                    json.dumps(commands, indent=2) + "\n"
                )
                for phase, cmd in [("warmup", warmup), ("measured", measured)]:
                    with (args.output / f"{stem}-{phase}.log").open("w") as bench_log:
                        subprocess.run(
                            cmd,
                            stdout=bench_log,
                            stderr=subprocess.STDOUT,
                            check=True,
                            timeout=args.seconds + 60,
                        )
                measured_log = (args.output / f"{stem}-measured.log").read_text()
                if "error" in measured_log.lower() or "fail" in measured_log.lower():
                    raise RuntimeError(f"benchmark errors: {stem}")
                measured_json = json.loads((args.output / f"{stem}.json").read_text())
                assert measured_json["ALL STATS"]["Totals"]["Ops/sec"] > 0
                assert client.command("HLEN", "compact:0") == count
                observed = client.command("HMGET", "compact:0", *selected)
                assert all(value is not None and len(value) == 64 for value in observed)
                if operation.startswith("hset"):
                    assert observed[0] != b"a" * 64, "benchmark must change values"
                info = client.command("INFO").decode()
                (args.output / f"{stem}-info.txt").write_text(info)
                print(stem, flush=True)
            finally:
                if client:
                    client.close()
                if server.poll() is None:
                    server.send_signal(signal.SIGTERM)
                    try:
                        server.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        server.kill()
                        server.wait()
                if server.returncode != 0:
                    raise RuntimeError(
                        f"server failed: {stem} exit={server.returncode}"
                    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-directory", default="/tmp")
    parser.add_argument("--seconds", type=int, default=5)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--keys", type=int, default=256)
    parser.add_argument("--fields", type=int, nargs="+", default=[16, 128])
    parser.add_argument(
        "--operations",
        nargs="+",
        default=["hset1", "hset8", "hget", "hmget8"],
        choices=["hset1", "hset8", "hget", "hmget8"],
    )
    parser.add_argument("--server-cpu", type=int)
    parser.add_argument("--client-cpus")
    args = parser.parse_args()
    if args.keys < 2 or min(args.fields) < 8 or args.seconds < 1 or args.repeats < 1:
        parser.error("keys >= 2, fields >= 8, seconds/repeats >= 1 required")
    args.baseline, args.candidate = args.baseline.resolve(), args.candidate.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "environment.json").write_text(
        json.dumps(
            {
                "uname": list(os.uname()),
                "arguments": vars(args),
                "memtier_version": subprocess.check_output(
                    ["memtier_benchmark", "--version"], text=True
                ),
            },
            default=str,
            indent=2,
        )
        + "\n"
    )
    for count in args.fields:
        for operation in args.operations:
            for repeat in range(args.repeats):
                variants = [("baseline", args.baseline), ("candidate", args.candidate)]
                if repeat % 2:
                    variants.reverse()
                for label, binary in variants:
                    run_case(args, binary, label, count, operation, repeat)


if __name__ == "__main__":
    main()
