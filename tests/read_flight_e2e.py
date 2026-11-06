#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Shared remote GETs under socket backpressure, disconnect, and recovery."""

import asyncio
import json
from pathlib import Path
import socket
import struct
import sys
import tempfile

import defrag_payload_e2e as support
from remote_get_e2e import drain, stats

KEY = b"shared-remote-value"
VALUE = bytes(range(256)) * 16
GET = b"*2\r\n$3\r\nGET\r\n$" + str(len(KEY)).encode() + b"\r\n" + KEY + b"\r\n"


async def connect(port, slow=False):
    raw = socket.socket()
    raw.setblocking(False)
    if slow:
        raw.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
    await asyncio.get_running_loop().sock_connect(raw, ("127.0.0.1", port))
    return await asyncio.open_connection(sock=raw, limit=1024 if slow else 65536)


async def traffic(port, admin):
    # All sockets are established before requests start, so several connections
    # on each worker can overlap their physical reads.
    slow = await asyncio.gather(*(connect(port, True) for _ in range(32)))
    fast = await asyncio.gather(*(connect(port) for _ in range(32)))
    for _, writer in slow:
        # Queue 16 MiB of replies per connection, exceeding ordinary TCP send
        # buffers. The earlier small pipeline could fit entirely in the kernel
        # and finish without retaining a result across socket backpressure.
        writer.write(GET * 4096)
    for _, writer in fast:
        writer.write(GET * 128)

    async def receive(reader, writer):
        for _ in range(128):
            assert await reader.readline() == b"$4096\r\n"
            assert await reader.readexactly(len(VALUE)) == VALUE
            assert await reader.readexactly(2) == b"\r\n"
        writer.close()
        await writer.wait_closed()

    async def disconnect():
        # Wait for the remote read stream to stop while a shared result remains
        # owned. This proves retention across backpressure without depending on
        # a fixed startup delay or the speed of the machine running the test.
        previous = None
        for _ in range(100):
            await asyncio.sleep(0.05)
            blocked = stats(admin)
            observed = tuple(
                blocked[key]
                for key in (
                    b"storage_read_flight_leaders",
                    b"storage_read_flight_joins",
                    b"storage_shared_read_bytes",
                )
            )
            if (
                blocked[b"storage_read_flights_pending"] == b"0"
                and int(observed[-1]) > 0
                and observed == previous
            ):
                break
            previous = observed
        else:
            raise AssertionError(
                ("no stable shared result under backpressure", blocked)
            )
        print(
            "shared bytes retained under socket backpressure:",
            blocked[b"storage_shared_read_bytes"].decode(),
        )
        for _, writer in slow:
            # Abort while the server can own a partially sent shared response.
            sock = writer.get_extra_info("socket")
            sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
            )
            writer.transport.abort()

    await asyncio.wait_for(
        asyncio.gather(disconnect(), *(receive(*client) for client in fast)), 45
    )


async def wait_retired(client):
    for _ in range(500):
        values = stats(client)
        if (
            values[b"storage_read_flights_pending"] == b"0"
            and values[b"storage_shared_read_bytes"] == b"0"
        ):
            return values
        await asyncio.sleep(0.01)
    raise AssertionError("producer or shared result remained after disconnect")


def main():
    support.BINARY = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="lavik-read-flight-e2e-") as directory:
        support.WORKDIR = Path(directory)
        with (support.WORKDIR / "data").open("wb") as f:
            f.truncate(512 * 1024 * 1024)
        with support.server(2) as (admin, _):
            assert admin.batch([[b"SET", KEY, VALUE]]) == [b"OK"]
            drain(admin)
        with support.server(2) as (admin, _):
            asyncio.run(traffic(admin.socket.getpeername()[1], admin))
            result = asyncio.run(wait_retired(admin))
            assert int(result[b"storage_read_flight_joins"]) > 0, result
            assert result[b"storage_read_flight_rejections"] == b"0", result
            assert admin.batch([[b"GET", KEY]]) == [VALUE]
            print(
                json.dumps(
                    {
                        k.decode(): int(v)
                        for k, v in result.items()
                        if k.startswith(
                            (b"storage_read_flight", b"storage_shared_read")
                        )
                    }
                )
            )
        with support.server(2) as (admin, _):
            assert admin.batch([[b"GET", KEY]]) == [VALUE]
    print("read-flight backpressure/disconnect/recovery passed")


if __name__ == "__main__":
    main()
