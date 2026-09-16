#!/usr/bin/env python3
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
report = (ROOT / 'README.md').read_text()
assert verify_results.markdown(actual) in report
for target in re.findall(r'\[[^]]+\]\(([^)]+)\)', report):
    if not target.startswith(('https://', 'http://', '#')):
        assert (ROOT / target.split('#', 1)[0]).exists(), target
for line in (ROOT / 'SHA256SUMS').read_text().splitlines():
    digest, name = line.split('  ', 1)
    assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest, name
print('PASS: 2 fresh 100M loads, 32 phases, 16 full measurements, 28 operation rows, host policies, README and file hashes.')
