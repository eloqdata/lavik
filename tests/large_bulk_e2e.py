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

"""Large request ownership, incremental admission, and cancellation over TCP/TLS."""

from contextlib import contextmanager
import os
from pathlib import Path
import socket
import ssl
import sys
import tempfile
import time

import redis_follower_smoke as S


MIB = 1024 * 1024


def bulk(value):
    return b"$%d\r\n" % len(value) + value + b"\r\n"


def wire(*args):
    return b"*%d\r\n" % len(args) + b"".join(
        bulk(value if isinstance(value, bytes) else str(value).encode())
        for value in args
    )


def set_prefix(key, size):
    return b"*3\r\n" + bulk(b"SET") + bulk(key.encode()) + b"$%d\r\n" % size


def payload(size):
    # Embedded NUL, CRLF, non-UTF8 bytes, and RESP punctuation must survive
    # direct reads just as they survive the ordinary incremental parser.
    return (bytes(range(256)) * ((size + 255) // 256))[:size]


class ErrorReply(Exception):
    pass


def response(reader):
    line = reader.readline()
    assert line.endswith(b"\r\n"), ("incomplete RESP response", line)
    tag, body = line[:1], line[1:-2]
    if tag == b"+":
        return body
    if tag == b"-":
        raise ErrorReply(body)
    if tag == b":":
        return int(body)
    if tag == b"$":
        length = int(body)
        if length == -1:
            return None
        value = reader.read(length)
        assert len(value) == length, ("short bulk reply", len(value), length)
        assert reader.read(2) == b"\r\n"
        return value
    if tag == b"*":
        return [response(reader) for _ in range(int(body))]
    raise AssertionError(("unexpected RESP response", line))


class Client:
    def __init__(self, sock, reader):
        self.sock = sock
        self.reader = reader

    def call(self, *args):
        self.sock.sendall(wire(*args))
        return response(self.reader)


@contextmanager
def connection(port, tls=False):
    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    if tls:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        sock = context.wrap_socket(sock, server_hostname="localhost")
    with sock, sock.makefile("rb") as reader:
        yield Client(sock, reader)


def buffered_bytes(control):
    info = control.call("INFO", "memory")
    for line in info.split(b"\r\n"):
        if line.startswith(b"client_buffered_request_bytes:"):
            return int(line.split(b":", 1)[1])
    raise AssertionError("INFO memory omitted client_buffered_request_bytes")


def wait_buffered(control, predicate, description):
    deadline = time.monotonic() + 5
    while True:
        actual = buffered_bytes(control)
        if predicate(actual):
            return actual
        if time.monotonic() >= deadline:
            raise AssertionError((description, "last buffered bytes", actual))
        time.sleep(0.01)


def wait_released(control, baseline):
    # INFO's own request is charged while its snapshot is generated. Reusing
    # exactly the same request gives a stable nonzero baseline, including when
    # the tested connection still has a prepared but unfilled body window.
    wait_buffered(control, lambda actual: actual == baseline, "request release")


def expect_error_and_close(client, message):
    try:
        result = response(client.reader)
    except ErrorReply as error:
        assert message in error.args[0], error.args[0]
    else:
        raise AssertionError(("expected error", message, result))
    try:
        assert client.reader.read(1) == b"", "failed request connection stayed open"
    except (ConnectionResetError, ssl.SSLEOFError):
        pass


def check_roundtrips(port, tls, control, baseline, prefix):
    with connection(port, tls) as client:
        for index, size in enumerate((MIB, 2 * MIB + 137)):
            key = f"{prefix}:fragmented:{index}"
            value = payload(size)
            header = set_prefix(key, size)
            sent = 0
            # Observe each partial header before sending the next fragment so
            # TCP coalescing cannot hide the split length/CRLF parser states.
            for end in (3, 4, len(header) - 1, len(header)):
                client.sock.sendall(header[sent:end])
                sent = end
                wait_buffered(
                    control,
                    lambda actual: actual == baseline + sent,
                    "fragmented header",
                )
            client.sock.sendall(value[:1024])
            wait_buffered(
                control,
                lambda actual: actual >= baseline + len(header) + min(size, MIB),
                "large body admission",
            )
            for offset in range(1024, size, 65537):
                client.sock.sendall(value[offset : offset + 65537])
            client.sock.sendall(b"\r")
            wait_buffered(
                control,
                lambda actual: actual == baseline + len(header) + size + 1,
                "fragmented final CRLF",
            )
            small_key = f"{prefix}:small:{index}"
            client.sock.sendall(
                b"\n"
                + wire("SET", small_key, b"small\x00value")
                + wire("GET", small_key)
                + wire("GET", key)
                + wire("PING")
            )
            for expected in (b"OK", b"OK", b"small\x00value", value, b"PONG"):
                assert response(client.reader) == expected
            wait_released(control, baseline)

        first, second = payload(MIB + 17), payload(MIB + 8193)
        keys = [f"{prefix}:mset:{index}" for index in range(2)]
        client.sock.sendall(
            wire("MSET", keys[0], first, keys[1], second)
            + wire("GET", keys[1])
            + wire("SET", f"{prefix}:after-mset", "small")
            + wire("GET", keys[0])
        )
        for expected in (b"OK", second, b"OK", first):
            assert response(client.reader) == expected
    wait_released(control, baseline)


def check_transactions(port, tls, control, baseline, prefix):
    value = payload(MIB + 19)
    for ending in ("DISCARD", "EXEC", "disconnect"):
        keys = [f"{prefix}:transaction:{ending}:{index}" for index in range(2)]
        frames = [wire("SET", key, value) for key in keys]
        with connection(port, tls) as client:
            assert client.call("MULTI") == b"OK"
            client.sock.sendall(b"".join(frames))
            assert response(client.reader) == b"QUEUED"
            assert response(client.reader) == b"QUEUED"
            wait_buffered(
                control,
                lambda actual: actual == baseline + sum(map(len, frames)),
                "MULTI retains exactly the received command bytes",
            )
            assert control.call("GET", keys[0]) is None
            if ending == "EXEC":
                assert client.call(ending) == [b"OK", b"OK"]
            elif ending == "DISCARD":
                assert client.call(ending) == b"OK"
        wait_released(control, baseline)
        for key in keys:
            assert control.call("GET", key) == (value if ending == "EXEC" else None)


def check_incomplete_and_invalid(port, tls, control, baseline, prefix):
    key = f"{prefix}:incomplete"
    header = set_prefix(key, 64 * MIB)
    with connection(port, tls) as client:
        client.sock.sendall(header)
        # A declared length alone must not allocate/admit the entire body or
        # even the first direct-read window. Progress is observed, not timed.
        wait_buffered(
            control,
            lambda actual: actual == baseline + len(header),
            "header-only request retains only header bytes",
        )
        client.sock.sendall(b"x" * 1024)
        admitted = wait_buffered(
            control,
            lambda actual: actual >= baseline + MIB,
            "partial body prepays one bounded window",
        )
        assert admitted < baseline + 2 * MIB, admitted
        # Close with an outstanding read into the parser's reserved string.
    wait_released(control, baseline)
    assert control.call("GET", key) is None

    key = f"{prefix}:malformed"
    with connection(port, tls) as client:
        client.sock.sendall(wire("PING") + set_prefix(key, MIB) + payload(MIB) + b"\rX")
        assert response(client.reader) == b"PONG"
        expect_error_and_close(client, b"terminator")
    wait_released(control, baseline)
    assert control.call("GET", key) is None
    assert control.call("PING") == b"PONG"


def check_query_limit_shrink(port, tls, control, baseline, prefix):
    config = "client-query-buffer-limit"
    previous = control.call("CONFIG", "GET", config)[1]
    key = f"{prefix}:query-limit"
    try:
        with connection(port, tls) as client:
            client.sock.sendall(set_prefix(key, 3 * MIB) + payload(MIB + 65536))
            # A second prepaid window proves the first 1 MiB window was
            # consumed. The smallest valid runtime limit is 1 MiB, so the
            # retained command now exceeds it before the suspended read ends.
            wait_buffered(
                control,
                lambda actual: actual > baseline + 2 * MIB,
                "second receive window before runtime limit shrink",
            )
            assert control.call("CONFIG", "SET", config, "1mb") == b"OK"
            client.sock.sendall(b"x")
            expect_error_and_close(client, b"query buffer limit")
        wait_released(control, baseline)
        assert control.call("GET", key) is None
    finally:
        assert control.call("CONFIG", "SET", config, previous) == b"OK"


def check(port, tls, control, baseline):
    prefix = "large-bulk:tls" if tls else "large-bulk:tcp"
    check_roundtrips(port, tls, control, baseline, prefix)
    check_transactions(port, tls, control, baseline, prefix)
    check_incomplete_and_invalid(port, tls, control, baseline, prefix)
    check_query_limit_shrink(port, tls, control, baseline, prefix)
    wait_released(control, baseline)


def check_quota(port, tls, control, baseline):
    prefix = "large-bulk:quota:tls" if tls else "large-bulk:quota:tcp"
    queued_key, partial_key = f"{prefix}:queued", f"{prefix}:partial"
    value = payload(MIB)
    with connection(port, tls) as client:
        assert client.call("MULTI") == b"OK"
        assert client.call("SET", queued_key, value) == b"QUEUED"
        wait_buffered(
            control,
            lambda actual: actual == baseline + len(wire("SET", queued_key, value)),
            "queued request before quota exhaustion",
        )
        # The full declared body is never sent. Its next bounded window alone
        # exceeds the remaining quota while MULTI owns the earlier command.
        client.sock.sendall(set_prefix(partial_key, 2 * MIB) + b"x" * 1024)
        expect_error_and_close(client, b"memory limit")
    wait_released(control, baseline)
    assert control.call("GET", queued_key) is None
    assert control.call("GET", partial_key) is None
    with connection(port, tls) as healthy:
        assert healthy.call("PING") == b"PONG"
        assert healthy.call("ECHO", value) == value
    wait_released(control, baseline)


def tls_options(port):
    certificates = Path(__file__).resolve().parent / "tls"
    return [
        "--tls-port",
        str(port),
        "--tls-cert-file",
        str(certificates / "server.crt"),
        "--tls-key-file",
        str(certificates / "server.key"),
        "--tls-auth-clients",
        "no",
    ]


def run(root, binary):
    tls_port = S.H.free_port()
    with S.process(
        binary,
        root / "roundtrips",
        "large-bulk",
        workers=1,
        extra=tls_options(tls_port),
    ) as (_, port, _):
        with connection(port) as control:
            baseline = buffered_bytes(control)
            check(port, False, control, baseline)
            check(tls_port, True, control, baseline)

    tls_port = S.H.free_port()
    with S.process(
        binary,
        root / "quota",
        "large-bulk-quota",
        workers=1,
        extra=tls_options(tls_port) + ["--maxmemory-clients", "2mb"],
    ) as (_, port, _):
        with connection(port) as control:
            baseline = buffered_bytes(control)
            check_quota(port, False, control, baseline)
            check_quota(tls_port, True, control, baseline)


if __name__ == "__main__":
    assert len(sys.argv) == 2, "usage: large_bulk_e2e.py /path/to/lavik"
    binary = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(
        prefix="lavik-large-bulk-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        run(Path(directory), binary)
    print("large bulk receive and admission checks passed over TCP and TLS")
