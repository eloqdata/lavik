#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Raw diskless PSYNC boundaries and live shared backpressure policy."""

import concurrent.futures
import os
from pathlib import Path
import socket
import sys
import tempfile
import time

import redis
import redis_follower_smoke as S


def wire(*args):
    args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
    return b"*%d\r\n" % len(args) + b"".join(
        b"$%d\r\n" % len(a) + a + b"\r\n" for a in args
    )


class Export:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), 5)
        self.sock.settimeout(10)
        self.file = self.sock.makefile("rb", buffering=0)
        self.sock.sendall(wire("REPLCONF", "capa", "eof"))
        assert self.file.readline() == b"+OK\r\n"
        self.sock.sendall(wire("PSYNC", "?", "-1"))
        header = self.file.readline().split()
        assert header[0] == b"+FULLRESYNC" and int(header[2]) == 1, header
        self.identity = header[1]
        eof = self.file.readline().strip()
        assert eof.startswith(b"$EOF:") and len(eof) == 45, eof
        self.token = eof[5:]

    def rdb(self):
        data = bytearray()
        # Deliberately split every EOF token byte across a read boundary.
        while not data.endswith(self.token):
            part = self.file.read(1)
            assert part, "export ended before EOF"
            data += part
        assert data.startswith(b"REDIS0010")
        return bytes(data[:-40])

    def ack(self):
        self.sock.sendall(wire("REPLCONF", "ACK", 1))

    def close(self):
        self.file.close()
        self.sock.close()


def wait_export_idle(src):
    S.H.wait_until(
        "export joined",
        15,
        lambda: src.info("replication")["redis_export_active"] == 0,
    )


def check_handshake_state(port, src):
    def exchange(sock, file, *args):
        sock.sendall(wire(*args))
        return file.readline()

    with socket.create_connection(("127.0.0.1", port), 5) as sock:
        with sock.makefile("rb", buffering=0) as file:
            assert exchange(sock, file, "MULTI") == b"+OK\r\n"
            assert (
                exchange(sock, file, "SET", "not-executed", "value") == b"+QUEUED\r\n"
            )
            for args in (("REPLCONF", "capa", "eof"), ("PSYNC", "?", "-1")):
                assert b"not allowed inside MULTI" in exchange(sock, file, *args)
            assert exchange(sock, file, "EXEC").startswith(b"-EXECABORT")
            assert exchange(sock, file, "PING") == b"+PONG\r\n"
            assert b"requires REPLCONF capa eof" in exchange(
                sock, file, "PSYNC", "?", "-1"
            )
    assert src.get("not-executed") is None
    wait_export_idle(src)

    # Every successful RESET path clears capabilities, including Pub/Sub and
    # an open transaction; malformed RESET must preserve the negotiated state.
    for mode in ("ordinary", "multi", "pubsub", "invalid"):
        with socket.create_connection(("127.0.0.1", port), 5) as sock:
            with sock.makefile("rb", buffering=0) as file:
                assert exchange(sock, file, "REPLCONF", "capa", "eof") == b"+OK\r\n"
                if mode == "multi":
                    assert exchange(sock, file, "MULTI") == b"+OK\r\n"
                elif mode == "pubsub":
                    sock.sendall(wire("SUBSCRIBE", "events"))
                    expected = b"*3\r\n$9\r\nsubscribe\r\n$6\r\nevents\r\n:1\r\n"
                    assert b"".join(file.readline() for _ in range(6)) == expected
                if mode == "invalid":
                    assert exchange(sock, file, "RESET", "extra").startswith(b"-ERR")
                    assert exchange(sock, file, "PSYNC", "?", "-1").startswith(
                        b"+FULLRESYNC"
                    )
                else:
                    assert exchange(sock, file, "RESET") == b"+RESET\r\n"
                    assert b"requires REPLCONF capa eof" in exchange(
                        sock, file, "PSYNC", "?", "-1"
                    )
        wait_export_idle(src)


def check_invalid_acks(port, src):
    for args in (
        ("PING", "ACK", 1, "FACK", 0),
        ("REPLCONF", "ACK", 1, "FACK", "bad"),
        ("REPLCONF", "ACK", 1, "FACK", -1),
        ("REPLCONF", "ACK", 1, "FACK"),
        ("REPLCONF", "ACK", 1, "OTHER", 0),
        ("REPLCONF", "ACK", "bad", "FACK", 0),
    ):
        exported = Export(port)
        try:
            exported.rdb()
            exported.sock.sendall(wire(*args))
            assert exported.sock.recv(1) == b"", args
        finally:
            exported.close()
        wait_export_idle(src)


def run(root, binary):
    os.environ["LAVIK_REDIS_EXPORT_FINAL_QUEUE_RACE"] = "1"
    with S.process(
        binary,
        root / "source",
        "lavik",
        workers=1,
        extra=(
            "--repl-backlog-size",
            "8mb",
            "--replication-publish-queue-mb-per-worker",
            "1",
        ),
    ) as (_, port, _):
        src = redis.Redis(port=port, socket_timeout=5)
        check_handshake_state(port, src)
        check_invalid_acks(port, src)
        src.set("last-fragment", "present")
        src.config_set("redis-export-disk-backlog-size", "8mb")
        exported = Export(port)
        try:
            src.config_set("redis-export-disk-backlog-size", "16mb")
            assert (
                src.info("replication")["redis_export_disk_capacity"] == 8 * 1024 * 1024
            )
            data = exported.rdb()
            assert b"last-fragment" in data and b"present" in data
            src.set("first-increment", "after-eof")
            # FACK is durability metadata; it must not open the initial ACK gate.
            exported.sock.sendall(wire("REPLCONF", "ACK", 0, "FACK", 1))
            exported.sock.settimeout(0.15)
            try:
                got = exported.sock.recv(1)
            except socket.timeout:
                pass
            else:
                raise AssertionError(("increment sent before initial ACK", got))
            exported.sock.sendall(wire("replconf", "ack", 1, "fack", 0))
            exported.sock.settimeout(5)
            data = b""
            while b"after-eof" not in data:
                data += exported.sock.recv(65536)
            assert b"first-increment" in data
            exported.ack()  # The original three-argument form remains valid.

            # Changing policy with intact coverage preserves this connection.
            assert src.config_set("replication-backlog-backpressure", "no")
            assert src.config_set("replication-backlog-backpressure", "yes")

            def writes(marker):
                writer = redis.Redis(port=port, socket_timeout=30)
                try:
                    for i in range(40):
                        writer.set("overwrite", bytes([i]) * (1024 * 1024))
                    writer.set("writes-finished", marker)
                finally:
                    writer.close()

            with concurrent.futures.ThreadPoolExecutor() as pool:
                # Default retention must also recover through ordinary reader
                # progress, without changing policy or dropping the session.
                future = pool.submit(writes, "resumed-with-retention")
                time.sleep(2)
                assert not future.done(), "slow exporter did not backpressure writes"
                data = b""
                while b"resumed-with-retention" not in data:
                    part = exported.sock.recv(1024 * 1024)
                    assert part, "retained export disconnected during recovery"
                    data = data[-128:] + part
                future.result(timeout=20)

                # Fill the socket and log again. A policy flip must wake the
                # publisher without any consumer cooperation.
                future = pool.submit(writes, "released-by-policy")
                time.sleep(2)
                assert not future.done(), "slow exporter did not backpressure writes"
                assert src.config_set("replication-backlog-backpressure", "no")
                future.result(timeout=20)
                # Resume consumption: the lost source coverage must close this
                # session, and the switch back to yes must not resurrect it.
                assert src.config_set("replication-backlog-backpressure", "yes")
                exported.sock.settimeout(15)
                while exported.sock.recv(1024 * 1024):
                    pass
        finally:
            exported.close()
        S.H.wait_until(
            "export joined",
            15,
            lambda: src.info("replication")["redis_export_active"] == 0,
        )
        assert src.set("after-cancel", "writable")
        again = Export(port)
        try:
            assert len(again.identity) == 40
            assert (
                src.info("replication")["redis_export_disk_capacity"]
                == 16 * 1024 * 1024
            )
        finally:
            again.close()
        src.close()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="lavik-export-wire-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as temp:
        run(Path(temp), sys.argv[1])
    print("PASS protocol and live policy")
