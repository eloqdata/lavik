#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Exercise bounded trace ownership and a real slow-client send over RESP."""

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.file = self.sock.makefile("rb")

    @staticmethod
    def encode(*args):
        args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
        return b"*%d\r\n" % len(args) + b"".join(
            b"$%d\r\n" % len(a) + a + b"\r\n" for a in args
        )

    def read(self):
        line = self.file.readline()
        if not line:
            raise RuntimeError("server closed RESP connection")
        kind, value = line[:1], line[1:-2]
        if kind == b"*":
            return [self.read() for _ in range(int(value))]
        if kind == b"$":
            n = int(value)
            if n < 0:
                return None
            data = self.file.read(n)
            assert self.file.read(2) == b"\r\n"
            return data
        if kind == b"-":
            raise RuntimeError(value.decode())
        return value.decode()

    def command(self, *args):
        self.sock.sendall(self.encode(*args))
        return self.read()

    def close(self):
        self.file.close()
        self.sock.close()


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(predicate, seconds=15):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition did not become true")


def run(binary):
    with tempfile.TemporaryDirectory(prefix="lavik-latency-pressure-") as tmp:
        root = Path(tmp)
        data = root / "data"
        with data.open("wb") as f:
            os.posix_fallocate(f.fileno(), 0, 1024**3)
        port, metrics_port = free_port(), free_port()
        while port == metrics_port:
            metrics_port = free_port()
        with (root / "server.log").open("wb") as log:
            server = subprocess.Popen(
                [
                    binary,
                    "--threads",
                    "1",
                    "--port",
                    str(port),
                    "--metrics-port",
                    str(metrics_port),
                    "--data-file",
                    str(data),
                    "--logtostderr",
                    "--recv-buffers-per-worker",
                    "0",
                ],
                stdout=log,
                stderr=log,
            )
            slow = []
            client = None
            try:

                def connect():
                    nonlocal client
                    try:
                        client = Client(port)
                        return client.command("PING") == "PONG"
                    except OSError:
                        return False

                wait_for(connect)
                assert (
                    client.command("SET", "pressure-private", b"v" * (512 * 1024))
                    == "OK"
                )
                for key, value in [
                    ("sample-every", 1),
                    ("slow-threshold-us", 0),
                    ("enabled", "yes"),
                ]:
                    assert (
                        client.command(
                            "CONFIG", "SET", "latency-diagnostics-" + key, value
                        )
                        == "OK"
                    )

                def metric(name):
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{metrics_port}/metrics", timeout=5
                    ) as response:
                        body = response.read().decode()
                    return float(
                        next(
                            line[len(name) + 1 :]
                            for line in body.splitlines()
                            if line.startswith(name + " ")
                        )
                    )

                # Keep entire ready batches alive behind real socket backpressure.
                for _ in range(6):
                    peer = Client(port)
                    peer.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
                    peer.sock.sendall(Client.encode("GET", "pressure-private") * 128)
                    slow.append(peer)
                wait_for(lambda: metric("lavik_response_writes_inflight") > 0)
                wait_for(
                    lambda: metric(
                        'lavik_latency_diagnostics_samples_total{result="pool_dropped"}'
                    )
                    > 0
                )
                assert metric("lavik_latency_diagnostics_inflight") <= 256
                assert metric("lavik_request_queue_length") > 0
                # A slow sink or full trace pool must not stop unrelated requests.
                assert client.command("PING") == "PONG"
                time.sleep(0.1)
                for peer in slow:
                    peer.close()
                slow.clear()
                wait_for(lambda: metric("lavik_latency_diagnostics_inflight") == 0)
                wait_for(lambda: metric("lavik_request_queue_length") == 0)
                wait_for(lambda: metric("lavik_response_writes_inflight") == 0)
                assert (
                    client.command("CONFIG", "SET", "latency-diagnostics-enabled", "no")
                    == "OK"
                )
                traces = [
                    json.loads(value)
                    for value in client.command("LATENCY", "TRACE", "GET", 128)
                ]
                assert traces
                sends = [
                    stage["duration_us"]
                    for trace in traces
                    for stage in trace["stages"]
                    if stage["name"] == "socket_send"
                ]
                assert max(sends) > 50_000, sends
                trace_id = traces[0]["trace_id"]
                shown = client.command("LATENCY", "TRACE", "SHOW", trace_id)
                assert len(shown) == 1 and json.loads(shown[0])["trace_id"] == trace_id
                assert b"pressure-private" not in b"".join(shown)
                for trace in traces:
                    assert all(
                        stage["start_us"] >= 0 and stage["duration_us"] >= 0
                        for stage in trace["stages"]
                    )
                print(
                    "PASS: slow send attribution, bounded pool/drop accounting, cancellation cleanup and TRACE SHOW"
                )
            finally:
                for peer in slow:
                    peer.close()
                if client:
                    client.close()
                server.terminate()
                try:
                    server.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()
                if server.returncode:
                    print((root / "server.log").read_text()[-6000:])
                assert server.returncode == 0


def lock_case(binary):
    with tempfile.TemporaryDirectory(prefix="lavik-latency-lock-") as tmp:
        root = Path(tmp)
        data, pause = root / "data", root / "pause"
        pause.touch()
        with data.open("wb") as f:
            os.posix_fallocate(f.fileno(), 0, 512 * 1024**2)
        port, metrics_port = free_port(), free_port()
        while port == metrics_port:
            metrics_port = free_port()
        env = dict(
            os.environ,
            LAVIK_RECORD_WRITE_PAUSE_KEY="lock-private",
            LAVIK_RECORD_WRITE_PAUSE_FILE=str(pause),
        )
        with (root / "server.log").open("wb") as log:
            server = subprocess.Popen(
                [
                    binary,
                    "--threads",
                    "2",
                    "--port",
                    str(port),
                    "--metrics-port",
                    str(metrics_port),
                    "--data-file",
                    str(data),
                    "--logtostderr",
                    "--recv-buffers-per-worker",
                    "0",
                ],
                env=env,
                stdout=log,
                stderr=log,
            )
            first = second = None
            try:

                def ready():
                    nonlocal first
                    try:
                        first = Client(port)
                        return first.command("PING") == "PONG"
                    except OSError:
                        return False

                wait_for(ready)
                for key, value in [
                    ("sample-every", 1),
                    ("slow-threshold-us", 0),
                    ("enabled", "yes"),
                ]:
                    first.command("CONFIG", "SET", "latency-diagnostics-" + key, value)
                first.sock.sendall(
                    Client.encode("SET", "lock-private", "value")
                    + Client.encode("GET", "missing")
                )
                # The pause is inside append after key acquisition, and yields
                # the worker. A second connection must wait on that key.
                wait_for(
                    lambda: "record write publication pause armed"
                    in (root / "server.log").read_text()
                )
                second = Client(port)
                second.sock.sendall(Client.encode("GET", "lock-private"))

                def waiting():
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{metrics_port}/metrics", timeout=5
                    ) as response:
                        return (
                            'lavik_waiters{resource="key_lock"} 1'
                            in response.read().decode()
                        )

                wait_for(waiting)
                time.sleep(0.08)
                pause.unlink()
                assert first.read() == "OK"
                assert first.read() is None
                assert second.read() == b"value"
                first.command("CONFIG", "SET", "latency-diagnostics-enabled", "no")
                traces = [
                    json.loads(t) for t in first.command("LATENCY", "TRACE", "GET", 20)
                ]
                stages = [stage for trace in traces for stage in trace["stages"]]
                for stage in ["key_lock", "request_queue", "append"]:
                    assert (
                        max(s["duration_us"] for s in stages if s["name"] == stage)
                        >= 50_000
                    ), traces
                assert b"lock-private" not in json.dumps(traces).encode()
                print(
                    "PASS: injected append stall, key-lock wait and pipelined ready-queue attribution across two workers"
                )
            finally:
                pause.unlink(missing_ok=True)
                for client in [first, second]:
                    if client:
                        client.close()
                server.terminate()
                try:
                    server.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()
                assert server.returncode == 0, (root / "server.log").read_text()[-6000:]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument(
        "--faults",
        action="store_true",
        help="also exercise compiled-in deterministic storage faults",
    )
    args = parser.parse_args()
    binary = str(Path(args.binary).resolve())
    run(binary)
    if args.faults:
        lock_case(binary)
