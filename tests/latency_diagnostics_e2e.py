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
                    "--log-dir",
                    str(root / "logs"),
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
                    "--log-dir",
                    str(root / "logs"),
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


def read_buffer_case(binary, workers=1):
    """Keep a real arena lease busy, then separate queued buffer time from I/O."""
    with tempfile.TemporaryDirectory(prefix="lavik-latency-buffer-") as tmp:
        root = Path(tmp)
        data, pause = root / "data", root / "pause"
        with data.open("wb") as f:
            os.posix_fallocate(f.fileno(), 0, 512 * 1024**2)
        port, metrics_port = free_port(), free_port()
        while port == metrics_port:
            metrics_port = free_port()
        env = dict(
            os.environ,
            LAVIK_FAIL_READ_BUFFER_ALLOCATION="1",
            LAVIK_FAIL_EXTENT_BUFFER_ALLOCATION="1",
            LAVIK_LATENCY_PAUSE_READ_COMPLETION="1",
            LAVIK_LATENCY_PAUSE_READ_COMPLETION_FILE=str(pause),
        )
        with (root / "server.log").open("wb") as log:
            # Two 8 MiB write buffers leave a single 1 MiB read arena.
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
                    "--log-dir",
                    str(root / "logs"),
                    "--recv-buffers-per-worker",
                    "0",
                    "--registered-buffer-mb-per-worker",
                    "17",
                    "--storage-write-buffers-per-worker",
                    "2",
                    "--storage-read-buffer-kb",
                    "1024",
                    "--flush-max-ms",
                    "1",
                ],
                env=env,
                stdout=log,
                stderr=log,
            )
            client = first = second = None
            try:

                def ready():
                    nonlocal client
                    try:
                        client = Client(port)
                        return client.command("PING") == "PONG"
                    except OSError:
                        return False

                wait_for(ready)
                payload = b"v" * (512 * 1024)
                external = b"e" * (9 * 1024**2)
                assert client.command("SET", "private-buffer-key", payload) == "OK"
                assert client.command("SET", "private-large-key", external) == "OK"
                wait_for(
                    lambda: b"storage_durability_pending:0\r\n"
                    in client.command("INFO", "STATS")
                )
                # A durable active block can still serve from its write buffer.
                # Restart to ensure both values must use the physical read path.
                client.close()
                client = None
                server.terminate()
                server.wait(timeout=20)
                assert server.returncode == 0
                restart_args = list(server.args)
                restart_args[restart_args.index("--threads") + 1] = str(workers)
                server = subprocess.Popen(restart_args, env=env, stdout=log, stderr=log)
                wait_for(ready)

                def recovered():
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{metrics_port}/metrics", timeout=5
                    ) as response:
                        return "lavik_server_ready 1" in response.read().decode()

                wait_for(recovered)
                for key, value in [
                    ("sample-every", 1),
                    ("slow-threshold-us", 0),
                    ("enabled", "yes"),
                ]:
                    client.command("CONFIG", "SET", "latency-diagnostics-" + key, value)
                pause.touch()
                first = Client(port)
                first.sock.sendall(Client.encode("GET", "private-buffer-key"))
                wait_for(
                    lambda: "fault pause reached: LAVIK_LATENCY_PAUSE_READ_COMPLETION_FILE"
                    in (root / "server.log").read_text()
                )
                second = Client(port)
                second.sock.sendall(Client.encode("GET", "private-buffer-key"))

                def metrics():
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{metrics_port}/metrics", timeout=5
                    ) as response:
                        return response.read().decode()

                def buffer_waiters():
                    return float(
                        next(
                            line.rsplit(" ", 1)[1]
                            for line in metrics().splitlines()
                            if line.startswith('lavik_waiters{resource="read_buffer"} ')
                        )
                    )

                wait_for(lambda: buffer_waiters() > 0)
                time.sleep(0.08)
                pause.unlink()
                assert first.read() == payload
                assert second.read() == payload
                wait_for(lambda: 'lavik_waiters{resource="read_buffer"} 0' in metrics())
                traces = [
                    json.loads(t) for t in client.command("LATENCY", "TRACE", "GET", 20)
                ]
                stages = [stage for trace in traces for stage in trace["stages"]]
                for name in ["read_buffer", "read_io"]:
                    assert (
                        max(s["duration_us"] for s in stages if s["name"] == name)
                        >= 50_000
                    ), traces
                queued = max(
                    (s for s in stages if s["name"] == "read_buffer"),
                    key=lambda s: s["duration_us"],
                )
                assert queued["observations"] >= 2 and len(queued["intervals"]) >= 2
                # A larger paged String merges concurrent child observations.
                # Keep the complete resource-time sum after eight detail slots.
                assert client.command("GET", "private-large-key") == external
                client.command("CONFIG", "SET", "latency-diagnostics-enabled", "no")
                traces = [
                    json.loads(t) for t in client.command("LATENCY", "TRACE", "GET", 1)
                ]
                io = next(
                    stage for stage in traces[0]["stages"] if stage["name"] == "read_io"
                )
                assert io["observations"] > 8, traces
                assert (
                    len(io["intervals"]) == 8
                    and io["intervals_dropped"] == io["observations"] - 8
                )
                assert io["duration_us"] >= sum(
                    i["duration_us"] for i in io["intervals"]
                )
                assert "private-large-key" not in json.dumps(traces)
                print(
                    "PASS: real read-buffer contention, separate I/O and bounded repeated page intervals; workers="
                    + str(workers)
                )
            except BaseException:
                print((root / "server.log").read_text()[-6000:])
                raise
            finally:
                pause.unlink(missing_ok=True)
                for peer in [client, first, second]:
                    if peer:
                        peer.close()
                server.terminate()
                try:
                    server.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()
                assert server.returncode == 0, (root / "server.log").read_text()[-6000:]


def logging_case(binary, unavailable=False):
    """The file-output switch and sink failures must preserve history/metrics."""
    with tempfile.TemporaryDirectory(prefix="lavik-latency-logging-") as tmp:
        root = Path(tmp)
        data = root / "data"
        with data.open("wb") as f:
            os.posix_fallocate(f.fileno(), 0, 256 * 1024**2)
        diagnostic = root / "logs" / "latency-diagnostics.log"
        if unavailable:
            # A directory at the file path deterministically makes the sink
            # unavailable without relying on root-sensitive file permissions.
            diagnostic.mkdir(parents=True)
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
                    "--log-dir",
                    str(root / "logs"),
                ],
                stdout=log,
                stderr=log,
            )
            client = None
            try:

                def ready():
                    nonlocal client
                    try:
                        client = Client(port)
                        return client.command("PING") == "PONG"
                    except OSError:
                        return False

                wait_for(ready)
                assert (
                    client.command(
                        "CONFIG", "GET", "latency-diagnostics-log-max-per-second"
                    )[1]
                    == b"20"
                )
                assert (
                    client.command(
                        "CONFIG", "SET", "latency-diagnostics-log-max-per-second", 1
                    )
                    == "OK"
                )
                for key, value in [
                    ("sample-every", 1),
                    ("slow-threshold-us", 0),
                    ("log-enabled", "no"),
                    ("enabled", "yes"),
                ]:
                    assert (
                        client.command(
                            "CONFIG", "SET", "latency-diagnostics-" + key, value
                        )
                        == "OK"
                    )
                for _ in range(10):
                    assert client.command("GET", "private-log-key") is None
                time.sleep(0.05)
                assert not diagnostic.is_file()
                assert len(client.command("LATENCY", "TRACE", "GET", 128)) == 10
                assert (
                    client.command(
                        "CONFIG", "SET", "latency-diagnostics-log-enabled", "yes"
                    )
                    == "OK"
                )
                client.sock.sendall(Client.encode("GET", "private-log-key") * 5)
                for _ in range(5):
                    assert client.read() is None

                def metrics():
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{metrics_port}/metrics", timeout=5
                    ) as response:
                        return response.read().decode()

                if unavailable:
                    wait_for(
                        lambda: 'lavik_latency_diagnostics_samples_total{result="log_dropped"} 5'
                        in metrics()
                    )
                else:
                    wait_for(
                        lambda: diagnostic.is_file() and diagnostic.stat().st_size > 0
                    )
                    entries = [
                        json.loads(line) for line in diagnostic.read_text().splitlines()
                    ]
                    assert len(entries) == 1 and all(
                        e["event"] == "command_latency_trace" for e in entries
                    )
                    assert "private-log-key" not in diagnostic.read_text()
                assert (
                    client.command(
                        "CONFIG", "SET", "latency-diagnostics-log-enabled", "no"
                    )
                    == "OK"
                )
                # Let already queued output finish before taking a baseline.
                time.sleep(0.06)
                before = diagnostic.stat().st_size if diagnostic.is_file() else 0
                for _ in range(10):
                    assert client.command("GET", "private-log-key") is None
                time.sleep(0.06)
                after = diagnostic.stat().st_size if diagnostic.is_file() else 0
                assert after == before
                assert len(client.command("LATENCY", "TRACE", "GET", 128)) == 25
                body = metrics()
                assert (
                    'lavik_latency_diagnostics_samples_total{result="retained"} 25'
                    in body
                )
                assert (
                    'lavik_latency_diagnostics_stage_seconds_count{command="GET",stage="server_total"} 25'
                    in body
                )
                assert "lavik_latency_diagnostics_log_enabled 0" in body
                assert client.command("PING") == "PONG"
                assert (
                    client.command(
                        "CONFIG", "SET", "latency-diagnostics-log-max-per-second", 0
                    )
                    == "OK"
                )
                assert (
                    client.command(
                        "CONFIG", "SET", "latency-diagnostics-log-enabled", "yes"
                    )
                    == "OK"
                )
                client.sock.sendall(Client.encode("GET", "private-log-key") * 32)
                for _ in range(32):
                    assert client.read() is None
                if unavailable:
                    wait_for(
                        lambda: 'lavik_latency_diagnostics_samples_total{result="log_dropped"} 37'
                        in metrics()
                    )
                else:
                    wait_for(lambda: len(diagnostic.read_text().splitlines()) == 33)
                assert len(client.command("LATENCY", "TRACE", "GET", 128)) == 57
                assert "lavik_latency_diagnostics_log_max_per_second 0" in metrics()
                print(
                    "PASS: log switch preserves history/histograms; sink unavailable="
                    + str(unavailable)
                )
            finally:
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
    logging_case(binary)
    logging_case(binary, unavailable=True)
    if args.faults:
        lock_case(binary)
        read_buffer_case(binary)
        read_buffer_case(binary, workers=2)
