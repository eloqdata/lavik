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
            n = int(value)
            return None if n < 0 else [self.read() for _ in range(n)]
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
            first = second = third = fourth = None
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
                # a/b route to distinct workers; the shared key conflicts
                # with the paused writer on one participant.
                third = Client(port)
                third.sock.sendall(Client.encode("MGET", "lock-private", "a", "b"))
                fourth = Client(port)
                fourth.command("MULTI")
                fourth.command("GET", "lock-private")
                fourth.sock.sendall(Client.encode("EXEC"))
                time.sleep(0.08)
                pause.unlink()
                assert first.read() == "OK"
                assert first.read() is None
                assert second.read() == b"value"
                assert third.read() == [b"value", None, None]
                assert fourth.read() == [b"value"]
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
                multi = next(t for t in traces if t["command"] == "MGET")
                assert (
                    max(
                        stage["duration_us"]
                        for stage in multi["stages"]
                        if stage["name"] == "key_lock"
                    )
                    >= 50_000
                ), multi
                transaction = next(t for t in traces if t["command"] == "EXEC")
                assert (
                    max(
                        stage["duration_us"]
                        for stage in transaction["stages"]
                        if stage["name"] == "key_lock"
                    )
                    >= 50_000
                ), transaction
                assert b"lock-private" not in json.dumps(traces).encode()
                print(
                    "PASS: injected append stall, key-lock wait and pipelined ready-queue attribution across two workers"
                )
            finally:
                pause.unlink(missing_ok=True)
                for client in [first, second, third, fourth]:
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
            # Recovered grouped records must remain cold: startup cleaning can
            # otherwise relocate them into resident write buffers before GET.
            LAVIK_TX_CLEANER_START_PAUSED="1",
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
                decode = next(
                    stage for stage in traces[0]["stages"] if stage["name"] == "decode"
                )
                # Chunk copies and checksums occur before await_resume; each
                # physical chunk contributes actual decode work, not one
                # return-wrapper interval per value.
                assert decode["observations"] >= io["observations"], traces
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
                    'lavik_latency_diagnostics_stage_seconds_count{command_family="get",stage="server_total"} 25'
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
                # The explicit PING above is also one completed selected command.
                assert len(client.command("LATENCY", "TRACE", "GET", 128)) == 58
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


def storage_path_coverage(client):
    """Small strings, grouped random access and specialized command paths."""
    client.command("CONFIG", "SET", "latency-diagnostics-enabled", "yes")
    client.command("SET", "coverage:string", b"v" * 1024)
    pairs = [arg for i in range(256) for arg in (str(i), b"v" * 128)]
    client.command("HSET", "coverage:hash", *pairs)
    client.command("SADD", "coverage:set", *[str(i) + "v" * 64 for i in range(512)])

    def trace(*command):
        client.command("LATENCY", "TRACE", "RESET")
        client.command(*command)
        records = [json.loads(v) for v in client.command("LATENCY", "TRACE", "GET", 32)]
        found = next(t for t in records if t["command"] == command[0])
        return {s["name"] for s in found["stages"]}

    for command in [
        ("GETRANGE", "coverage:string", 0, 20),
        ("GETBIT", "coverage:string", 0),
    ]:
        assert {"read_buffer", "decode"} <= trace(*command), command
    for command in [
        ("APPEND", "coverage:string", "more"),
        ("SETRANGE", "coverage:string", 0, "A"),
        ("SETBIT", "coverage:string", 0, 1),
    ]:
        assert {"read_buffer", "decode", "value_process", "append"} <= trace(
            *command
        ), command
    for command in [
        ("HRANDFIELD", "coverage:hash", 2),
        ("SRANDMEMBER", "coverage:set", 2),
    ]:
        assert {"read_buffer", "decode", "value_process"} <= trace(*command), command
    assert {"read_buffer", "decode", "append"} <= trace("SPOP", "coverage:set", 2)
    assert "append" in trace("SPOP", "coverage:set", 1000)
    for command in [
        ("COPY", "coverage:string", "coverage:copy"),
        ("RENAME", "coverage:copy", "coverage:renamed"),
    ]:
        assert {"read_buffer", "decode", "append"} <= trace(*command), command
    for command in [
        ("EXPIRE", "coverage:string", 3600),
        ("PERSIST", "coverage:string"),
    ]:
        assert {
            "key_lock",
            "store_lock",
            "lookup",
            "read_buffer",
            "decode",
            "append",
        } <= trace(*command), command
    assert {"key_lock", "lookup"} <= trace("STRLEN", "coverage:string")
    assert {"read_buffer", "append"} <= trace(
        "BITOP", "NOT", "coverage:bits", "coverage:string"
    )
    client.command("MULTI")
    client.command("GET", "coverage:string")
    client.command("EXEC")
    records = [json.loads(v) for v in client.command("LATENCY", "TRACE", "GET", 32)]
    transaction = next(t for t in records if t["command"] == "EXEC")
    assert "key_lock" in {s["name"] for s in transaction["stages"]}, transaction
    # Grouped mutations inside an outer transaction await auxiliary durability.
    client.command("MULTI")
    client.command("HSET", "coverage:hash", "0", b"z" * 128)
    client.command("EXEC")
    records = [json.loads(v) for v in client.command("LATENCY", "TRACE", "GET", 32)]
    transaction = next(t for t in records if t["command"] == "EXEC")
    assert "durability_wait" in {s["name"] for s in transaction["stages"]}, transaction
    for command in [
        ("COPY", "coverage:string", "coverage:exec-copy"),
        ("RENAME", "coverage:exec-copy", "coverage:exec-renamed"),
    ]:
        client.command("LATENCY", "TRACE", "RESET")
        client.command("MULTI")
        client.command(*command)
        client.command("EXEC")
        records = [json.loads(v) for v in client.command("LATENCY", "TRACE", "GET", 32)]
        transaction = next(t for t in records if t["command"] == "EXEC")
        assert {"read_buffer", "decode", "append"} <= {
            stage["name"] for stage in transaction["stages"]
        }, transaction


def generic_commands_case(binary):
    """Trace the actual command identity and common lifecycle across families."""
    with tempfile.TemporaryDirectory(prefix="lavik-latency-families-") as tmp:
        root = Path(tmp)
        data = root / "data"
        with data.open("wb") as file:
            os.posix_fallocate(file.fileno(), 0, 1024**3)
        port = free_port()
        with (root / "server.log").open("wb") as log:
            server = subprocess.Popen(
                [
                    binary,
                    "--threads",
                    "2",
                    "--port",
                    str(port),
                    "--data-file",
                    str(data),
                    "--log-dir",
                    str(root / "logs"),
                    "--rdb-dir",
                    str(root),
                    "--logtostderr",
                ],
                env=dict(os.environ, LAVIK_TX_CLEANER_START_PAUSED="1"),
                stdout=log,
                stderr=log,
            )
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
                # Recovery leaves the large Hash value on disk. This must
                # exercise a real typed/grouped read, not a cached reply or
                # a synthetic timing assignment.
                large_value = b"h" * (512 * 1024)
                assert (
                    client.command("HSET", "private-family-disk", "field", large_value)
                    == "1"
                )
                disk_value = b"d" * 8192
                assert (
                    client.command(
                        "MSET",
                        "private-family-disk-a",
                        disk_value,
                        "private-family-disk-b",
                        disk_value,
                    )
                    == "OK"
                )
                list_values = [str(i).encode() + b"l" * 65536 for i in range(8)]
                assert (
                    client.command("RPUSH", "private-family-disk-list", *list_values)
                    == "8"
                )
                zset_members = [str(i).encode() + b"z" * 32768 for i in range(16)]
                zset_args = [
                    arg for i, member in enumerate(zset_members) for arg in (i, member)
                ]
                assert (
                    client.command("ZADD", "private-family-disk-zset", *zset_args)
                    == "16"
                )
                assert (
                    client.command("SET", "private-family-disk-string", large_value)
                    == "OK"
                )
                stream_value = b"x" * 65536
                stream_expected = []
                for i in range(16):
                    entry_id = f"{i + 1}-0"
                    assert (
                        client.command(
                            "XADD",
                            "private-family-stream",
                            entry_id,
                            "field",
                            stream_value,
                        )
                        == entry_id.encode()
                    )
                    stream_expected.append(
                        [entry_id.encode(), [b"field", stream_value]]
                    )
                client.close()
                client = None
                server.terminate()
                assert server.wait(timeout=20) == 0
                server = subprocess.Popen(
                    server.args,
                    env=dict(os.environ, LAVIK_TX_CLEANER_START_PAUSED="1"),
                    stdout=log,
                    stderr=log,
                )
                wait_for(connect)
                for name, value in [
                    ("sample-every", 1),
                    ("slow-threshold-us", 0),
                    ("log-enabled", "no"),
                    ("enabled", "yes"),
                ]:
                    assert (
                        client.command(
                            "CONFIG", "SET", "latency-diagnostics-" + name, value
                        )
                        == "OK"
                    )
                assert (
                    client.command("XRANGE", "private-family-stream", "-", "+")
                    == stream_expected
                )
                # Read before any new append can attach the recovered tail
                # to a write buffer and legitimately make these pages hot.
                assert (
                    client.command("GETRANGE", "private-family-disk-string", 0, -1)
                    == large_value
                )
                assert client.command(
                    "MGET", "private-family-disk-a", "private-family-disk-b"
                ) == [disk_value, disk_value]
                assert (
                    client.command("LRANGE", "private-family-disk-list", 0, -1)
                    == list_values
                )
                assert (
                    client.command("ZRANGE", "private-family-disk-zset", 0, -1)
                    == zset_members
                )
                assert (
                    client.command("HGET", "private-family-disk", "field")
                    == large_value
                )
                assert client.command("HGETALL", "private-family-disk") == [
                    b"field",
                    large_value,
                ]
                assert (
                    client.command(
                        "HSET", "private-family-disk", "field", b"g" * len(large_value)
                    )
                    == "0"
                )
                assert (
                    client.command(
                        "LPUSH", "private-family-disk-list", b"new-list-entry"
                    )
                    == "9"
                )
                assert (
                    client.command(
                        "ZADD", "private-family-disk-zset", 100, b"new-zset-member"
                    )
                    == "1"
                )
                assert client.command("ZPOPMAX", "private-family-disk-zset") == [
                    b"new-zset-member",
                    b"100",
                ]
                assert (
                    client.command("ZREMRANGEBYRANK", "private-family-disk-zset", 0, 0)
                    == "1"
                )
                assert (
                    client.command(
                        "SET", "private-family-disk-string", b"s" * len(large_value)
                    )
                    == "OK"
                )
                assert (
                    client.command(
                        "XADD",
                        "private-family-stream",
                        "17-0",
                        "field",
                        b"new-stream-entry",
                    )
                    == b"17-0"
                )
                assert client.command("PING") == "PONG"
                assert (
                    client.command("ECHO", "private-family-value")
                    == b"private-family-value"
                )
                assert client.command("INCR", "private-family-counter") == "1"
                assert (
                    client.command("HSET", "private-family-hash", "field", "value")
                    == "1"
                )
                assert (
                    client.command("HGET", "private-family-hash", "field") == b"value"
                )
                assert (
                    client.command(
                        "MSET", "private-family-a", "a", "private-family-b", "b"
                    )
                    == "OK"
                )
                assert client.command(
                    "MGET", "private-family-a", "private-family-b"
                ) == [b"a", b"b"]
                assert client.command("LPUSH", "private-family-list", "a", "b") == "2"
                assert client.command("LRANGE", "private-family-list", 0, -1) == [
                    b"b",
                    b"a",
                ]
                assert client.command(
                    "HGET", "private-family-disk", "field"
                ) == b"g" * len(large_value)
                assert client.command("MULTI") == "OK"
                assert client.command("INCR", "private-family-counter") == "QUEUED"
                assert (
                    client.command("HGET", "private-family-hash", "field") == "QUEUED"
                )
                assert client.command("GET", "private-family-a") == "QUEUED"
                assert client.command("EXEC") == ["2", b"value", b"a"]
                assert client.command(
                    "EVAL",
                    "return {redis.call('GET',KEYS[1]),redis.call('GET',KEYS[2])}",
                    2,
                    "private-family-a",
                    "private-family-b",
                ) == [b"a", b"b"]
                assert (
                    client.command(
                        "EVAL_RO",
                        "for i=1,24 do redis.call('GET',KEYS[1]) end return 24",
                        1,
                        "private-family-a",
                    )
                    == "24"
                )
                assert client.command("BLPOP", "private-family-blocked", "0.01") is None
                blocked_client = Client(port)
                try:
                    blocked_client.sock.sendall(
                        Client.encode("BLPOP", "private-family-wake", 1)
                    )
                    wait_for(
                        lambda: b"blocked_clients:1\r\n"
                        in client.command("INFO", "clients")
                    )
                    time.sleep(0.01)
                    assert (
                        client.command("RPUSH", "private-family-wake", "wake-value")
                        == "1"
                    )
                    assert blocked_client.read() == [
                        b"private-family-wake",
                        b"wake-value",
                    ]
                finally:
                    blocked_client.close()
                dump = client.command("DUMP", "private-family-disk")
                assert isinstance(dump, bytes) and dump
                assert client.command("SAVE") == "OK"
                assert client.command("SADD", "private-family-sa", "a", "b") == "2"
                assert client.command("SADD", "private-family-sb", "b", "c") == "2"
                assert sorted(
                    client.command("SUNION", "private-family-sa", "private-family-sb")
                ) == [b"a", b"b", b"c"]
                assert client.command("SORT", "private-family-sa", "ALPHA") == [
                    b"a",
                    b"b",
                ]
                assert client.command("ZADD", "private-family-za", 1, "a") == "1"
                assert client.command("ZADD", "private-family-zb", 2, "b") == "1"
                assert client.command(
                    "ZUNION", 2, "private-family-za", "private-family-zb"
                ) == [b"a", b"b"]
                assert client.command("ZPOPMIN", "private-family-za") == [b"a", b"1"]
                assert client.command("SETNX", "private-family-nx", "first") == "1"
                assert (
                    client.command("GETSET", "private-family-nx", "second") == b"first"
                )
                assert client.command("TYPE", "private-family-nx") == "string"
                assert client.command("PTTL", "private-family-nx") == "-1"
                assert client.command("EXISTS", "private-family-nx") == "1"
                assert client.command("DEL", "private-family-nx") == "1"
                assert (
                    client.command("CONFIG", "SET", "latency-diagnostics-enabled", "no")
                    == "OK"
                )
                raw = client.command("LATENCY", "TRACE", "GET", 128)
                traces = {}
                for value in raw:  # TRACE GET returns newest records first.
                    trace = json.loads(value)
                    if not trace.get("queued", False):
                        traces.setdefault(trace["command"], trace)
                expected = {
                    "PING",
                    "ECHO",
                    "INCR",
                    "HSET",
                    "HGET",
                    "MSET",
                    "MGET",
                    "LPUSH",
                    "LRANGE",
                }
                assert expected <= traces.keys(), sorted(traces)
                for command in expected:
                    trace = traces[command]
                    stages = {stage["name"]: stage for stage in trace["stages"]}
                    assert {
                        "request_queue",
                        "dispatch",
                        "socket_send",
                        "server_total",
                    } <= stages.keys(), trace
                    assert stages["dispatch"]["kind"] == "summary", trace
                    assert stages["server_total"]["kind"] == "summary", trace
                    assert trace["write_ok"] is True, trace
                for command in ("INCR", "HSET", "LPUSH"):
                    assert {
                        "key_lock",
                        "store_lock",
                        "lookup",
                        "value_process",
                        "append",
                    } <= {stage["name"] for stage in traces[command]["stages"]}, traces[
                        command
                    ]
                for command in ("MSET", "MGET"):
                    assert {"tx_schedule", "tx_round"} <= {
                        stage["name"] for stage in traces[command]["stages"]
                    }, traces[command]
                assert "append" in {
                    stage["name"] for stage in traces["MSET"]["stages"]
                }, traces["MSET"]
                assert any(
                    trace["command"] == "HGET"
                    and any(stage["name"] == "read_io" for stage in trace["stages"])
                    for trace in map(json.loads, raw)
                ), raw
                for command in ("MGET", "LRANGE", "ZRANGE", "GETRANGE", "HGETALL"):
                    candidates = [
                        json.loads(value)
                        for value in raw
                        if json.loads(value)["command"] == command
                    ]
                    cold = [
                        trace
                        for trace in candidates
                        if any(stage["name"] == "read_io" for stage in trace["stages"])
                    ]
                    assert cold, candidates
                    stages = {stage["name"]: stage for stage in cold[0]["stages"]}
                    assert {"read_io", "decode"} <= stages.keys(), cold[0]
                    if command != "HGETALL":
                        assert stages["read_io"].get("observations", 1) >= 2, cold[0]
                for command in ("HSET", "LPUSH", "ZADD", "SET"):
                    appends = [
                        stage
                        for value in raw
                        for trace in [json.loads(value)]
                        if trace["command"] == command
                        for stage in trace["stages"]
                        if stage["name"] == "append"
                    ]
                    assert (
                        appends
                        and max(stage.get("observations", 1) for stage in appends) >= 2
                    ), (command, appends)
                    assert all(stage["kind"] == "summary" for stage in appends), appends
                for command in ("ZPOPMAX", "ZREMRANGEBYRANK"):
                    stages = {
                        stage["name"]: stage for stage in traces[command]["stages"]
                    }
                    assert {"value_process", "append"} <= stages.keys(), traces[command]
                    # Selection reads the score page; nested removal also reads
                    # the member index. Both physical reads belong to this command.
                    assert stages["read_buffer"].get("observations", 1) >= 2, traces[
                        command
                    ]
                queued = [
                    json.loads(value)
                    for value in raw
                    if json.loads(value).get("queued", False)
                ]
                assert len(queued) == 3, queued
                for trace in queued:
                    names = {stage["name"] for stage in trace["stages"]}
                    assert {"dispatch", "server_total"} <= names, trace
                    assert not ({"read_io", "value_process", "append"} & names), trace
                for command, children in [
                    ("EXEC", ["INCR", "HGET", "GET"]),
                    ("EVAL", ["GET", "GET"]),
                ]:
                    trace = traces[command]
                    assert [
                        child["command"] for child in trace.get("children", [])
                    ] == children, trace
                    assert trace["children_omitted"] == 0, trace
                    assert "child_execute" in {
                        stage["name"] for stage in trace["stages"]
                    }, trace
                assert {"script_prepare", "script_run"} <= {
                    stage["name"] for stage in traces["EVAL"]["stages"]
                }, traces["EVAL"]
                bounded = traces["EVAL_RO"]
                assert (
                    len(bounded["children"]) == 16 and bounded["children_omitted"] == 8
                ), bounded
                child_stage = next(
                    stage
                    for stage in bounded["stages"]
                    if stage["name"] == "child_execute"
                )
                assert child_stage["observations"] == 24, bounded
                blocked = traces["BLPOP"]
                block_stage = next(
                    (
                        stage
                        for stage in blocked["stages"]
                        if stage["name"] == "blocking_wait"
                    ),
                    None,
                )
                assert block_stage and block_stage["duration_us"] >= 5000, blocked
                wake_stages = {stage["name"] for stage in blocked["stages"]}
                assert {
                    "blocking_wait",
                    "wake_resume",
                    "key_lock",
                    "store_lock",
                    "lookup",
                    "value_process",
                    "append",
                } <= wake_stages, blocked
                assert any(
                    {"route_to_owner", "route_back"}
                    <= {stage["name"] for stage in traces[command]["stages"]}
                    for command in ("INCR", "HSET", "LPUSH")
                ), {command: traces[command] for command in ("INCR", "HSET", "LPUSH")}
                for command in ("PING", "ECHO", "INCR", "HSET", "LPUSH", "MGET"):
                    assert "reply_build" in {
                        stage["name"] for stage in traces[command]["stages"]
                    }, traces[command]
                dump_stages = {
                    stage["name"]: stage for stage in traces["DUMP"]["stages"]
                }
                assert dump_stages["socket_send"].get("observations", 1) >= 2, traces[
                    "DUMP"
                ]
                assert "reply_build" in dump_stages, traces["DUMP"]
                stream_stages = {
                    stage["name"]: stage for stage in traces["XRANGE"]["stages"]
                }
                assert {"read_io", "decode", "reply_build"} <= stream_stages.keys(), (
                    traces["XRANGE"]
                )
                assert stream_stages["read_io"].get("observations", 1) >= 2, traces[
                    "XRANGE"
                ]
                assert "append" in {
                    stage["name"] for stage in traces["XADD"]["stages"]
                }, traces["XADD"]
                assert "snapshot_prepare" in {
                    stage["name"] for stage in traces["SAVE"]["stages"]
                }, traces["SAVE"]
                for command in ("SUNION", "SORT", "ZUNION", "ZPOPMIN"):
                    assert {"lookup", "decode"} <= {
                        stage["name"] for stage in traces[command]["stages"]
                    }, traces[command]
                for command in ("SETNX", "GETSET", "DEL"):
                    assert {"lookup", "store_lock", "append"} <= {
                        stage["name"] for stage in traces[command]["stages"]
                    }, traces[command]
                for command in ("TYPE", "PTTL", "EXISTS"):
                    assert "lookup" in {
                        stage["name"] for stage in traces[command]["stages"]
                    }, traces[command]
                assert "private-family" not in "".join(value.decode() for value in raw)
                storage_path_coverage(client)
                print(
                    "PASS: canonical identities and common lifecycle across command families"
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
    generic_commands_case(binary)
    run(binary)
    logging_case(binary)
    logging_case(binary, unavailable=True)
    if args.faults:
        lock_case(binary)
        read_buffer_case(binary)
        read_buffer_case(binary, workers=2)
