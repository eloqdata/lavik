#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Pure in-process checks; no servers, network namespaces, or benchmark loads."""

import importlib.util
import json
from pathlib import Path
import struct
import tempfile
import unittest


spec = importlib.util.spec_from_file_location(
    "bench_native_full_sync",
    Path(__file__).resolve().parents[1] / "scripts" / "bench_native_full_sync.py",
)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class BenchmarkEvidenceTest(unittest.TestCase):
    def test_interval_latency_includes_late_completion_and_qps_is_independent(self):
        writer = bench.Writer.__new__(bench.Writer)
        writer.rate, writer.mode, writer.errors = 10, "separate-keys", []
        writer.samples = [
            {
                "begin_monotonic": begin,
                "end_monotonic": end,
                "latency_ms": (end - begin) * 1000,
                "success": success,
            }
            for begin, end, success in (
                (9, 11, True),  # warmup operation completes inside FULL
                (12, 13, True),  # entirely inside FULL
                (19, 21, True),  # slow operation completes after FULL
                (20, 20.5, True),  # starts outside the half-open FULL interval
                (14, 15, False),  # an error is preserved, not a successful write
                (18, 20, True),  # completion at the excluded right endpoint
            )
        ]
        summary = writer.summary(10, 20)
        self.assertEqual(summary["count"], 3)
        self.assertEqual(summary["begun_count"], 4)
        self.assertEqual(summary["failed_begun_count"], 1)
        self.assertEqual(summary["crossed_interval_end_count"], 2)
        self.assertEqual(summary["p99_ms"], 2000)
        self.assertEqual(summary["completed_count"], 2)
        self.assertEqual(summary["completed_qps"], 0.2)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "writer-samples.jsonl"
            writer.save_samples(path)
            self.assertEqual(
                [json.loads(line) for line in path.read_text().splitlines()],
                writer.samples,
            )

    def test_frame_parser_reassembles_and_ignores_tcp_retransmission(self):
        payload = struct.pack("<QHI", 1, 12, 1)
        payload += struct.pack("<BBBQQQQIIII", 3, 15, 0, 1, 2, 0, 3, 0, 1, 1, 3)
        payload += b"kvvv"
        frame = struct.pack("<IBBHII", 0x3146564C, 1, 2, 16, len(payload), 0) + payload
        stream = bench.FrameStream()
        stream.next_sequence = 100
        stream.append(110, frame[10:])
        self.assertFalse(stream.counts)
        stream.append(100, frame[:10])
        stream.append(100, frame)  # duplicate transport bytes are not new frames
        self.assertEqual(stream.counts[2], 1)
        self.assertEqual(stream.partition_db["12:15"]["records"], 1)
        self.assertEqual(stream.large_value_begins["15:6b"], 1)
        self.assertFalse(stream.pending)
        self.assertFalse(stream.buffer)


if __name__ == "__main__":
    unittest.main()
