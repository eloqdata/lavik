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

"""Composite reply ownership, bounded writes, and pipeline order over TCP/TLS."""

from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time

import redis_follower_smoke as S


def bulk(value):
    return b"$%d\r\n" % len(value) + value + b"\r\n"


def array(values):
    return b"*%d\r\n" % len(values) + b"".join(values)


def wire(*args):
    return array([bulk(a if isinstance(a, bytes) else str(a).encode()) for a in args])


@contextmanager
def connection(port, tls=False):
    sock = socket.create_connection(("127.0.0.1", port), timeout=20)
    if tls:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        sock = context.wrap_socket(sock, server_hostname="localhost")
    with sock, sock.makefile("rb") as reader:
        yield sock, reader


def response(reader):
    line = reader.readline()
    assert line.endswith(b"\r\n"), line
    tag, body = line[:1], line[1:-2]
    if tag in (b"+", b":"):
        return body
    if tag == b"$":
        length = int(body)
        if length == -1:
            return None
        value = reader.read(length)
        assert reader.read(2) == b"\r\n"
        return value
    if tag in (b"*", b">", b"%"):
        return [response(reader) for _ in range(int(body) * (2 if tag == b"%" else 1))]
    if tag == b"_":
        return None
    raise AssertionError(line)


def expect(reader, expected):
    # Small reads also cover replies whose fragments cross application read
    # boundaries; the sender must retain buffers until all writes complete.
    for offset in range(0, len(expected), 8192):
        part = expected[offset : offset + 8192]
        actual = reader.read(len(part))
        assert actual == part, (offset, actual[:80], part[:80])


def check(port, tls):
    values = [b"", b"\x00\xff\r\n"] + [b"value-%d" % i for i in range(130)]
    # A single fragment exceeds the 256 KiB write budget. Repeated references
    # later fill the socket while its reader is deliberately paused.
    large = bytes(range(256)) * 2300
    values.append(large)
    keys = [f"fragment:{i}" for i in range(len(values))]
    with connection(port, tls) as (sock, reader):
        sock.sendall(b"".join(wire("SET", k, v) for k, v in zip(keys, values)))
        expect(reader, b"+OK\r\n" * len(keys))
        sock.sendall(wire("HSET", "fragment:wrongtype", "f", "v"))
        expect(reader, b":1\r\n")
        for version in (2, 3):
            sock.sendall(wire("HELLO", version))
            response(reader)
            null = b"_\r\n" if version == 3 else b"$-1\r\n"
            selected = keys + ["fragment:missing", "fragment:wrongtype", keys[0]]
            frames = [bulk(v) for v in values] + [null, null, bulk(values[0])]
            sock.sendall(
                wire("PING")
                + wire("MGET", *selected)
                + wire("PING")
                + wire("MGET", *reversed(selected))
            )
            expect(
                reader,
                b"+PONG\r\n" + array(frames) + b"+PONG\r\n" + array(frames[::-1]),
            )

            children = [b"exec-%d\x00\xff" % i for i in range(150)]
            children[70] = large
            sock.sendall(
                wire("MULTI")
                + b"".join(wire("ECHO", v) for v in children)
                + wire("EXEC")
                + wire("PING")
            )
            expect(
                reader,
                b"+OK\r\n"
                + b"+QUEUED\r\n" * len(children)
                + array([bulk(v) for v in children])
                + b"+PONG\r\n",
            )

        # RESP3 subscribed clients use a separate queueing path. It must retain
        # the complete composite response, including nulls and binary payloads.
        sock.sendall(wire("SUBSCRIBE", "fragment-events"))
        assert response(reader) == [b"subscribe", b"fragment-events", b"1"]
        sock.sendall(wire("MGET", keys[1], "fragment:missing"))
        expect(reader, array([bulk(values[1]), b"_\r\n"]))
        sock.sendall(wire("UNSUBSCRIBE", "fragment-events"))
        assert response(reader) == [b"unsubscribe", b"fragment-events", b"0"]
        sock.sendall(wire("HELLO", 2))
        response(reader)
        sock.sendall(wire("GET", keys[-1]) + wire("PING"))
        expect(reader, bulk(large) + b"+PONG\r\n")

        # Lazy replies still follow the aggregate header and finish before the
        # next pipelined reply; EXEC mixes a lazy Stream reply with a scalar.
        sock.sendall(wire("XADD", "fragment-stream", "1-0", "f", "v"))
        expect(reader, bulk(b"1-0"))
        stream_reply = array([array([bulk(b"1-0"), array([bulk(b"f"), bulk(b"v")])])])
        sock.sendall(
            wire("MULTI")
            + wire("XRANGE", "fragment-stream", "-", "+")
            + wire("ECHO", b"after-stream")
            + wire("EXEC")
            + wire("KEYS", "fragment-stream")
            + wire("PING")
        )
        expect(
            reader,
            b"+OK\r\n+QUEUED\r\n+QUEUED\r\n"
            + array([stream_reply, bulk(b"after-stream")])
            + array([bulk(b"fragment-stream")])
            + b"+PONG\r\n",
        )

        sock.sendall(wire("MGET", *([keys[-1]] * 16)) + wire("PING"))
        time.sleep(0.1)
        expect(reader, array([bulk(large)] * 16) + b"+PONG\r\n")

    # Disconnect with a large write outstanding. Its owners must be released,
    # and the worker must remain usable for subsequent commands.
    with connection(port, tls) as (sock, reader):
        sock.sendall(wire("MGET", *([keys[-1]] * 32)))
        assert reader.read(1) == b"*"
    with connection(port, tls) as (sock, reader):
        sock.sendall(wire("PING"))
        expect(reader, b"+PONG\r\n")


def run(root, binary):
    tls_port = S.H.free_port()
    openssl = shutil.which("openssl")
    extra = []
    if openssl:
        cert, key = root / "cert.pem", root / "key.pem"
        subprocess.run(
            [
                openssl,
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-keyout",
                str(key),
                "-out",
                str(cert),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        extra = [
            "--tls-port",
            str(tls_port),
            "--tls-cert-file",
            str(cert),
            "--tls-key-file",
            str(key),
            "--tls-auth-clients",
            "no",
        ]
    with S.process(binary, root / "server", "lavik", workers=2, extra=extra) as (
        _,
        port,
        _,
    ):
        check(port, False)
        if openssl:
            # Use fresh data for the same exact expected replies on TLS.
            client = S.Client(port)
            try:
                assert client.call("FLUSHALL") == "OK"
            finally:
                client.close()
            check(tls_port, True)
        else:
            print("SKIP TLS: openssl executable unavailable")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="lavik-fragments-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as tmp:
        run(Path(tmp), str(Path(sys.argv[1]).resolve()))
    print("PASS composite reply writes")
