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

"""Verify the complete report offline, including every archived file hash."""
import csv
import hashlib
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'evidence'))
import verify_results

actual = verify_results.verify()
assert actual == json.loads((ROOT / 'results.json').read_text())
with (ROOT / 'summary.csv').open(newline='') as stream:
    csv_rows = list(csv.DictReader(stream))
assert csv_rows == [{name: str(value) for name, value in row.items()} for row in actual['rows']]
from presentation import result_table
for name, zh in [('README.md', False), ('README.zh-CN.md', True)]:
    report = (ROOT / name).read_text()
    for target in [100000, 0]:
        assert result_table(actual, target, zh) in report, (name, target)
    assert report.count('|---|---|---:|---:|') == 2, name
    assert report.count('![') == 2, name
    for target in re.findall(r'\[[^]]+\]\(([^)]+)\)', report):
        if not target.startswith(('https://', 'http://', '#')):
            assert (ROOT / target.split('#', 1)[0]).exists(), (name, target)
for line in (ROOT / 'SHA256SUMS').read_text().splitlines():
    digest, name = line.split('  ', 1)
    assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest, name
print('PASS: 2 fresh 100M loads, 32 phases, 16 full measurements, 28 operation rows, host policies, English/Chinese reports and file hashes.')
