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
# Preserve the archived Chinese table renderer. Translate only its labels
# so both published reports must contain the same verified measurements.
chinese_table = verify_results.markdown(actual)
english_table = chinese_table
for source, translated in (
    ('不限速', 'Unlimited'),
    ('限速', 'Rate limit'),
    ('操作', 'Operation'),
    ('默认参数、原始宿主配置', 'defaults, original host policy'),
    ('100ms、12+4 隔离', '100ms, 12+4 isolation'),
    ('总 QPS', 'Total QPS'),
    ('（', ' ('),
    ('）', ')'),
    ('；', '; '),
):
    english_table = english_table.replace(source, translated)
for name, expected_table in (
    ('README.md', english_table),
    ('README.zh-CN.md', chinese_table),
):
    report = (ROOT / name).read_text()
    assert expected_table in report, name
    for target in re.findall(r'\[[^]]+\]\(([^)]+)\)', report):
        if not target.startswith(('https://', 'http://', '#')):
            assert (ROOT / target.split('#', 1)[0]).exists(), (name, target)
for line in (ROOT / 'SHA256SUMS').read_text().splitlines():
    digest, name = line.split('  ', 1)
    assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest, name
print('PASS: 2 fresh 100M loads, 32 phases, 16 full measurements, 28 operation rows, host policies, English/Chinese reports and file hashes.')
