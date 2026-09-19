#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Verify published measurements, source hashes, launch flags, and both tables."""
import csv
import hashlib
import json
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
rows = list(csv.DictReader((ROOT / 'results.csv').open()))
sources = json.loads((ROOT / 'sources.json').read_text())
index = {(x['system'], x['workload']): x for x in sources}
assert len(rows) == len(index) == 27
archive = ROOT / 'evidence.tar.gz'
assert hashlib.sha256(archive.read_bytes()).hexdigest() == (ROOT / 'evidence.tar.gz.sha256').read_text().split()[0]
with tarfile.open(archive) as evidence:
    for line in (ROOT / 'raw-SHA256SUMS').read_text().splitlines():
        digest, name = line.split('  ', 1)
        assert hashlib.sha256(evidence.extractfile(name).read()).hexdigest() == digest, name
    for row in rows:
        source = index[row['system'], row['workload']]
        raw = evidence.extractfile(source['archive_path']).read()
        assert hashlib.sha256(raw).hexdigest() == source['sha256']
        stats = json.loads(raw)['ALL STATS']
        totals = stats['Totals']
        assert float(row['qps']) == totals['Ops/sec']
        assert float(row['p99_ms']) == totals['Percentile Latencies']['p99.00']
        assert float(row['p999_ms']) == totals['Percentile Latencies']['p99.90']
        assert int(row['count']) == totals['Count']
        assert totals['Connection Errors'] == 0
        assert stats.get('Gets', {}).get('Misses/sec', 0) == 0
        if row['system'].startswith('lavik-'):
            launch = Path(source['archive_path']).parent / 'server-command.json'
            assert '--flush-max-ms=100' in json.load(evidence.extractfile(str(launch)))
        rendered = f"| {float(row['qps']):,.2f} | {float(row['p99_ms']):.3f} | {float(row['p999_ms']):.3f} |"
        for name in ['README.md', 'README.zh-CN.md']:
            assert rendered in (ROOT / name).read_text(), (name, row['system'], row['workload'])
for line in (ROOT / 'chart-SHA256SUMS').read_text().splitlines():
    digest, name = line.split('  ', 1)
    assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest, name
print('PASS: 27 raw measurements, provenance, 100 ms launch flags, both tables, and chart hashes')
