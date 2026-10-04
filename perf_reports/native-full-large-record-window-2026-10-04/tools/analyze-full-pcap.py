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

"""Offline source-veth native-frame spans; no server or network changes.

Only in-order captures are accepted. Retransmitted bytes are ignored. Spans
include gaps and other work between the first and last selected frame; they
are not exclusive scan/apply/ACK phases and do not measure target completion.
"""
import argparse
from collections import deque
import json
from pathlib import Path
import socket
import struct

class Stream:
    def __init__(self):
        self.next = None
        self.buffer = bytearray()
        self.base = 0
        self.end = 0
        self.pieces = deque()
        self.found = False
        self.events = []

    def consume(self, size):
        del self.buffer[:size]
        self.base += size
        while self.pieces and self.pieces[0][1] <= self.base:
            self.pieces.popleft()

    def feed(self, sequence, data, when):
        if self.next is None:
            self.next = sequence
        delta = (sequence - self.next + (1 << 31)) % (1 << 32) - (1 << 31)
        if delta > 0:
            raise ValueError('capture has TCP gaps/reordering; offline span rejected')
        if -delta >= len(data):
            return
        data = data[-delta:]
        self.next = (self.next + len(data)) % (1 << 32)
        self.pieces.append((self.end, self.end + len(data), when))
        self.end += len(data)
        self.buffer.extend(data)
        if not self.found:
            at = self.buffer.find(b'LVF1')
            if at < 0:
                self.consume(max(0, len(self.buffer) - 3))
                return
            self.consume(at)
            self.found = True
        while len(self.buffer) >= 16:
            magic, version, kind, header, size, _ = struct.unpack_from('<IBBHII', self.buffer)
            if magic != 0x3146564c or version != 1 or header != 16 or size > 12 * 1024 * 1024:
                raise ValueError('malformed native frame')
            if len(self.buffer) < 16 + size:
                return
            end = self.base + 16 + size
            times = [when for start, stop, when in self.pieces if start < end and stop > self.base]
            self.events.append(dict(kind=kind, wire_bytes=16 + size,
                                    first_byte_capture=min(times), complete_capture=max(times)))
            self.consume(16 + size)

def summarize(events):
    result = {}
    for kind, name in ((1, 'reset'), (2, 'records'), (6, 'handoff'),
                       (7, 'cut'), (8, 'full_command'), (5, 'cursor')):
        selected = [event for event in events if event['kind'] == kind]
        if selected:
            first = min(event['first_byte_capture'] for event in selected)
            last = max(event['complete_capture'] for event in selected)
            result[name] = dict(frames=len(selected), wire_bytes=sum(e['wire_bytes'] for e in selected),
                                first_byte_capture_unix=first, final_byte_capture_unix=last,
                                capture_span_seconds=last - first)
    return result

def analyze(path):
    streams = {}
    with path.open('rb') as source:
        header = source.read(24)
        if header[:4] not in (b'\xd4\xc3\xb2\xa1', b'\x4d\x3c\xb2\xa1'):
            raise ValueError('little-endian pcap required')
        scale = 1e9 if header[:4] == b'\x4d\x3c\xb2\xa1' else 1e6
        if struct.unpack_from('<I', header, 20)[0] != 1:
            raise ValueError('Ethernet capture required')
        while raw := source.read(16):
            seconds, fraction, size, _ = struct.unpack('<IIII', raw)
            packet = source.read(size)
            if len(packet) != size:
                raise ValueError('truncated packet')
            if len(packet) < 54 or packet[12:14] != b'\x08\x00' or packet[23] != 6:
                continue
            ip_header = (packet[14] & 15) * 4
            ip_size = struct.unpack_from('!H', packet, 16)[0]
            tcp = 14 + ip_header
            tcp_header = (packet[tcp + 12] >> 4) * 4
            sport, dport, sequence = struct.unpack_from('!HHI', packet, tcp)
            identity = (socket.inet_ntoa(packet[26:30]), sport,
                        socket.inet_ntoa(packet[30:34]), dport)
            stream = streams.setdefault(str(identity), Stream())
            if packet[tcp + 13] & 2:
                sequence = (sequence + 1) % (1 << 32)
                stream.next = sequence
            payload = packet[tcp + tcp_header:14 + ip_size]
            if payload:
                stream.feed(sequence, payload, seconds + fraction / scale)
    active = {key: stream for key, stream in streams.items() if stream.found}
    for stream in active.values():
        if stream.buffer or sum(e['kind'] == 7 for e in stream.events) != 1:
            raise ValueError('incomplete native FULL flow capture')
    return {'capture': str(path),
            'scope': 'source-veth frame-byte observation spans; includes intervening gaps/other frame kinds',
            'flows': {key: summarize(stream.events) for key, stream in active.items()},
            'all_flows': summarize([event for stream in active.values() for event in stream.events])}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('pcap', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    output = json.dumps(analyze(args.pcap), indent=2) + '\n'
    if args.output:
        args.output.write_text(output)
    else:
        print(output, end='')
