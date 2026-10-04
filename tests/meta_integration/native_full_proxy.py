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

"""Frame-aware observer for native FULL process tests.

The proxy preserves production source/target admission and application. Holding
ACKs, rather than delaying data reads, exposes the sender's application window
independently of TCP socket buffering. Tests can release one completion without
releasing other ACKs or the flow itself.
"""

from collections import defaultdict
import socket
import struct
import threading

import harness as H


def _exact(sock, size):
    result = bytearray()
    while len(result) < size:
        part = sock.recv(size - len(result))
        if not part:
            raise EOFError("native proxy connection closed")
        result.extend(part)
    return bytes(result)


def _line(sock):
    result = bytearray()
    while not result.endswith(b"\r\n"):
        result.extend(_exact(sock, 1))
        if len(result) > 65536:
            raise AssertionError("native proxy handshake line is too long")
    return bytes(result)


def _frame(sock):
    header = _exact(sock, 16)
    magic, version, kind, header_bytes, size, _crc = struct.unpack("<IBBHII", header)
    assert (magic, version, header_bytes) == (0x3146564C, 1, 16)
    assert size <= 12 * 1024 * 1024, size
    payload = _exact(sock, size)
    return kind, payload, header + payload


def _records(payload):
    sequence, partition, count = struct.unpack_from("<QHI", payload)
    offset = 14
    records = []
    for _ in range(count):
        kind = payload[offset]
        key_bytes, value_bytes = struct.unpack_from("<II", payload, offset + 43)
        key = payload[offset + 51 : offset + 51 + key_bytes]
        offset += 51 + key_bytes + value_bytes
        records.append((key, kind))
    assert offset == len(payload), (offset, len(payload))
    return sequence, partition, records


def _ack(partition, sequence):
    payload = struct.pack("<HQ", partition, sequence)
    crc = 0xFFFFFFFF
    for byte in payload:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return struct.pack("<IBBHII", 0x3146564C, 1, 3, 16, 10, crc ^ 0xFFFFFFFF) + payload


class FullRecordProxy(H.Proxy):
    """Hold record ACKs selected by key, or the partition-zero handoff ACK."""

    def __init__(self, keys=(), hold_handoff=False):
        super().__init__("full-record-window", 0)
        self._state = threading.RLock()
        self._hold = set(key.encode() for key in keys)
        self._watched = set(self._hold)
        self._hold_handoff = hold_handoff
        self._pending = []
        self._stats = defaultdict(lambda: {"frames": [], "forwarded": 0})
        self._flow_pairs = []
        self.handoff_seen = threading.Event()
        self.errors = []

    def _on_accept(self, conn):
        threading.Thread(target=self._route, args=(conn,), daemon=True).start()

    def _route(self, conn):
        pair = None
        upstream = None
        try:
            conn.settimeout(10)
            line = _line(conn)
            assert line.startswith(b"*"), line
            raw = bytearray(line)
            args = []
            for _ in range(int(line[1:-2])):
                length = _line(conn)
                assert length.startswith(b"$"), length
                value = _exact(conn, int(length[1:-2]) + 2)
                assert value.endswith(b"\r\n")
                raw.extend(length + value)
                args.append(value[:-2])
            upstream = socket.create_connection(self.target, timeout=10)
            upstream.sendall(raw)
            conn.settimeout(None)
            upstream.settimeout(None)
            pair = (conn, upstream)
            with self._lock:
                self._pairs.add(pair)
            if args[0] != b"LVFLOW":
                for src, dst in ((conn, upstream), (upstream, conn)):
                    threading.Thread(
                        target=super()._pump, args=(src, dst, pair), daemon=True
                    ).start()
                return
            reply = _line(upstream)
            conn.sendall(reply)
            if not reply.startswith(b"+LVFLOW "):
                self._cut_pair(pair)
                return
            flow = {"pair": pair, "send": threading.Lock(), "records": {}}
            with self._state:
                self._flow_pairs.append(pair)
            threading.Thread(target=self._data, args=(flow,), daemon=True).start()
            threading.Thread(target=self._acks, args=(flow,), daemon=True).start()
        except (OSError, EOFError):
            if pair is not None:
                self._cut_pair(pair)
            else:
                self._close_sock(conn)
                if upstream is not None:
                    self._close_sock(upstream)
        except BaseException as error:
            with self._state:
                self.errors.append(repr(error))
            self._close_sock(conn)
            if upstream is not None:
                self._close_sock(upstream)

    def _data(self, flow):
        conn, upstream = flow["pair"]
        try:
            while self._running:
                kind, payload, frame = _frame(upstream)
                with self._state:
                    if kind == 2:
                        sequence, partition, records = _records(payload)
                        selected = next(
                            (
                                (key, record_kind)
                                for key, record_kind in records
                                if key in self._watched
                            ),
                            None,
                        )
                        if selected:
                            key, record_kind = selected
                            event = (sequence, len(frame), record_kind, partition)
                            self._stats[key]["frames"].append(event)
                            flow["records"][sequence] = (key, event)
                    elif kind == 6:
                        sequence, partition = struct.unpack_from("<QH", payload)
                        if partition == 0 and self._hold_handoff:
                            flow["records"][sequence] = (None, None)
                conn.sendall(frame)
        except (OSError, EOFError):
            pass
        except BaseException as error:
            with self._state:
                self.errors.append(repr(error))
        finally:
            self._cut_pair(flow["pair"])

    def _acks(self, flow):
        conn, _upstream = flow["pair"]
        try:
            while self._running:
                kind, payload, frame = _frame(conn)
                with self._state:
                    selected = None
                    if kind == 3:
                        partition, sequence = struct.unpack("<HQ", payload)
                        selected = flow["records"].pop(sequence, None)
                    if selected is not None:
                        key, event = selected
                        if key is None:
                            self.handoff_seen.set()
                        if key in self._hold or (key is None and self._hold_handoff):
                            self._pending.append((flow, frame, key, event))
                            continue
                    self._send_ack(flow, frame, selected[0] if selected else None)
        except (OSError, EOFError):
            pass
        except BaseException as error:
            with self._state:
                self.errors.append(repr(error))
        finally:
            self._cut_pair(flow["pair"])

    def _send_ack(self, flow, frame, key):
        with flow["send"]:
            flow["pair"][1].sendall(frame)
        if key is not None:
            self._stats[key]["forwarded"] += 1

    def snapshot(self, key):
        """Return immutable wire observations, including ACKs still held."""
        key = key.encode()
        with self._state:
            assert not self.errors, self.errors
            stats = self._stats[key]
            return {
                "frames": list(stats["frames"]),
                "forwarded": stats["forwarded"],
                "held": sum(item[2] == key for item in self._pending),
            }

    def release(self, key=None, record_kind=None, one=False):
        """Release selected ACKs, optionally just one frame of a given kind."""
        key = key.encode() if key is not None else None
        one = one or record_kind is not None
        with self._state:
            if not one:
                if key is None:
                    self._hold_handoff = False
                else:
                    self._hold.discard(key)
            released = 0
            retained = []
            for flow, frame, selected_key, event in self._pending:
                matches = selected_key == key and (
                    not one
                    or (
                        released == 0
                        and (record_kind is None or event[2] == record_kind)
                    )
                )
                if matches:
                    self._send_ack(flow, frame, key)
                    released += 1
                else:
                    retained.append((flow, frame, selected_key, event))
            self._pending = retained
            return released

    def corrupt_ack(self, key, duplicate=False):
        """Send a valid-CRC ACK with the wrong identity, or repeat an exact ACK."""
        key = key.encode()
        with self._state:
            flow, original, _, event = next(
                item for item in self._pending if item[2] == key
            )
            sequence, _, _, partition = event
            if duplicate:
                self._send_ack(flow, original + original, key)
            else:
                self._send_ack(flow, _ack((partition + 1) % 16384, sequence), key)

    def cut_flows(self):
        with self._state:
            pairs = list(self._flow_pairs)
        for pair in pairs:
            self._cut_pair(pair)
