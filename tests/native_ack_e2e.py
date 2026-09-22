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

"""Exercise source ACK validation with a wire peer after a real FULL export.

The peer deliberately makes false/malformed progress claims. Dataset replay is
covered by the native replication gate; this gate checks the trust boundary
before source retention and WAIT advance.
"""

from contextlib import ExitStack, contextmanager
import os
from pathlib import Path
import socket
import struct
import sys
import tempfile
import uuid

from redis_follower_smoke import process, F


def crc32c(payload):
    crc = 0xFFFFFFFF
    for byte in payload:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def frame(kind, payload):
    return (
        struct.pack("<IBBHII", 0x3146564C, 1, kind, 16, len(payload), crc32c(payload))
        + payload
    )


def read_frame(reader):
    header = reader.read(16)
    assert len(header) == 16, f"truncated frame: {header!r}"
    magic, version, kind, size, length, checksum = struct.unpack("<IBBHII", header)
    assert (magic, version, size) == (0x3146564C, 1, 16)
    assert length <= 64 * 1024 * 1024
    payload = reader.read(length)
    assert len(payload) == length and crc32c(payload) == checksum
    return kind, payload


@contextmanager
def online_peer(port, ranges):
    with ExitStack() as stack:

        def connection():
            sock = stack.enter_context(
                socket.create_connection(("127.0.0.1", port), 10)
            )
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return sock, stack.enter_context(sock.makefile("rb"))

        control, control_reader = connection()
        identity = uuid.uuid4().hex + "12345678"
        control.sendall(
            F.encode_resp(
                [
                    "LVPSYNC",
                    "1",
                    "?" + identity + ":12345",
                    "?",
                    "?",
                    identity,
                    identity,
                    "?",
                ]
            )
        )
        words = control_reader.readline().decode().strip().split()
        assert len(words) == 8 and words[0] == "+LVFULLRESYNC", words
        assert words[6] == "1", words
        flow, reader = connection()
        flow.sendall(
            F.encode_resp(
                ["LVFLOW", "1", words[1], "0", "1", "0", words[7]]
                + (["ACKRANGE"] if ranges else [])
            )
        )
        assert reader.readline().decode().strip() == (
            f"+LVFLOW {words[1]} 0 FULL" + (" ACKRANGE" if ranges else "")
        )
        while True:
            kind, payload = read_frame(reader)
            if kind == 1:  # RESET has no session-local sequence.
                partition, sequence = 65535, 0
            elif kind in (2, 6):  # RECORDS and partition handoff.
                sequence, partition = struct.unpack_from("<QH", payload)
            elif kind == 8:  # The empty Function catalog is still transferred.
                if not payload[30] & 2:  # Only the final fragment is ACKed.
                    continue
                sequence, partition = struct.unpack_from("<QH", payload)
            elif kind == 7:  # FULL cut.
                partition, sequence = 65535, struct.unpack_from("<Q", payload)[0]
            elif kind == 5:  # ONLINE cursor uses the original ACK format.
                partition, sequence = 0, struct.unpack_from("<Q", payload)[0]
            else:
                raise AssertionError(f"unexpected idle FULL frame {kind}")
            flow.sendall(frame(3, struct.pack("<HQ", partition, sequence)))
            if kind == 5:
                break
        assert control_reader.readline() == b"+LVONLINE\r\n"
        yield flow, reader


def publish(client, flow_reader, count=16):
    client.socket.sendall(
        b"".join(F.encode_resp(["SET", f"ack:{i}", "value"]) for i in range(count))
    )
    for _ in range(count):
        assert F.read_resp(client.reader) == "OK"
    lsns = []
    for _ in range(count):
        kind, payload = read_frame(flow_reader)
        assert kind == 4, kind
        lsns.append(struct.unpack_from("<Q", payload)[0])
    assert lsns == list(range(lsns[0], lsns[0] + count)), lsns
    return lsns


def main():
    cases = (
        "valid",
        "legacy",
        "unnegotiated",
        "reversed",
        "wrong-start",
        "unsent",
        "oversized",
        "overflow",
        "trailing",
        "duplicate",
    )
    with tempfile.TemporaryDirectory(
        prefix="lavik-native-ack-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        for case in cases:
            # Independent sources keep a rejected ACK from affecting another
            # case's source history, session or reconnect-retention lease.
            with process(sys.argv[1], Path(directory) / case, case, workers=1) as (
                client,
                port,
                _,
            ):
                with online_peer(port, case not in ("legacy", "unnegotiated")) as (
                    flow,
                    reader,
                ):
                    lsns = publish(client, reader, 128 if case == "valid" else 16)
                    first, last = lsns[0], lsns[-1]
                    if case == "legacy":
                        flow.sendall(
                            b"".join(
                                frame(3, struct.pack("<HQ", 0, lsn)) for lsn in lsns
                            )
                        )
                        assert client.call("WAIT", 1, 2000) == 1
                        print(f"PASS: {case}", flush=True)
                        continue
                    if case == "reversed":
                        first, last = last, first
                    elif case == "wrong-start":
                        first += 1
                    elif case == "unsent":
                        last += 1
                    elif case == "oversized":
                        last = first + 128
                    elif case == "overflow":
                        first, last = 2**64 - 2, 2**64 - 1
                    payload = struct.pack("<QQ", first, last)
                    if case == "trailing":
                        payload += b"x"
                    flow.sendall(frame(9, payload))
                    if case in ("valid", "duplicate"):
                        assert client.call("WAIT", 1, 2000) == 1
                        next_lsns = publish(client, reader)
                        if case == "valid":
                            # A singleton remains legal after a range.
                            flow.sendall(
                                b"".join(
                                    frame(3, struct.pack("<HQ", 0, lsn))
                                    for lsn in next_lsns
                                )
                            )
                            assert client.call("WAIT", 1, 2000) == 1
                            print(f"PASS: {case}", flush=True)
                            continue
                        flow.sendall(frame(9, payload))
                    assert reader.read(1) == b"", f"{case}: malformed ACK accepted"
                    assert client.call("WAIT", 1, 20) == 0
            print(f"PASS: {case}", flush=True)


if __name__ == "__main__":
    main()
