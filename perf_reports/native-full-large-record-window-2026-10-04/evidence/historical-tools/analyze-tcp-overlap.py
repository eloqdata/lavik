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
"""Count repeated TCP byte observations in existing source-only Ethernet pcaps.

Repeated payload is consistent with retransmission, not a measured loss rate.
Unique out-of-order segments are not counted as repeated bytes. This cannot
recover qdisc drop statistics or receiver-to-source ACK retransmission.
"""
import argparse
import bisect
import json
from pathlib import Path
import socket
import struct


def insert_range(ranges, start, stop):
    """Union one half-open range and return exactly the already-covered bytes."""
    index = bisect.bisect_left(ranges, (start,))
    if index and ranges[index - 1][1] >= start:
        index -= 1
    end = index
    overlap = 0
    merged_start, merged_stop = start, stop
    while end < len(ranges) and ranges[end][0] <= stop:
        lo, hi = ranges[end]
        overlap += max(0, min(stop, hi) - max(start, lo))
        merged_start, merged_stop = min(merged_start, lo), max(merged_stop, hi)
        end += 1
    ranges[index:end] = [(merged_start, merged_stop)]
    return overlap


def analyze(path):
    streams = {}
    with path.open('rb') as source:
        header = source.read(24)
        if header[:4] not in (b'\xd4\xc3\xb2\xa1', b'\x4d\x3c\xb2\xa1'):
            raise ValueError('little-endian pcap required')
        if struct.unpack_from('<I', header, 20)[0] != 1:
            raise ValueError('Ethernet capture required')
        while raw := source.read(16):
            if len(raw) != 16:
                raise ValueError('truncated packet header')
            _, _, size, _ = struct.unpack('<IIII', raw)
            packet = source.read(size)
            if len(packet) != size:
                raise ValueError('truncated packet')
            if len(packet) < 54 or packet[12:14] != b'\x08\x00' or packet[23] != 6:
                continue
            tcp = 14 + (packet[14] & 15) * 4
            ip_size = struct.unpack_from('!H', packet, 16)[0]
            tcp_header = (packet[tcp + 12] >> 4) * 4
            sport, dport, sequence = struct.unpack_from('!HHI', packet, tcp)
            payload = packet[tcp + tcp_header:14 + ip_size]
            if not payload:
                continue
            identity = str((socket.inet_ntoa(packet[26:30]), sport,
                            socket.inet_ntoa(packet[30:34]), dport))
            flow = streams.setdefault(identity, {'initial_sequence': sequence, 'ranges': [],
                'tcp_payload_observations': 0, 'observed_payload_bytes': 0,
                'repeated_payload_observations': 0, 'repeated_payload_bytes': 0})
            start = (sequence - flow['initial_sequence']) % (1 << 32)
            if start >= 1 << 31:
                raise ValueError('capture starts after an earlier sequence or spans >=2GiB; ambiguous')
            overlap = insert_range(flow['ranges'], start, start + len(payload))
            flow['tcp_payload_observations'] += 1
            flow['observed_payload_bytes'] += len(payload)
            flow['repeated_payload_observations'] += int(overlap > 0)
            flow['repeated_payload_bytes'] += overlap
    for flow in streams.values():
        flow['unique_payload_bytes'] = sum(hi - lo for lo, hi in flow.pop('ranges'))
    return {'capture': str(path), 'scope': __doc__, 'flows': streams,
            'total_repeated_payload_bytes': sum(f['repeated_payload_bytes'] for f in streams.values()),
            'total_repeated_payload_observations': sum(f['repeated_payload_observations'] for f in streams.values())}


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
