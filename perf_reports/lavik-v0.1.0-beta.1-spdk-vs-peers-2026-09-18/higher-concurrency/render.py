"""Verify the archived samples and render the common 16-thread control grid."""
from pathlib import Path
import hashlib
import json
import math
import statistics
import tarfile

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
rows = json.loads((ROOT / 'results.json').read_text())
peaks = json.loads((ROOT / 'peaks.json').read_text())
styles = {
    'lavik-spdk': ('Lavik SPDK', '#1764ab', 'o', '-'),
    'redis-io16': ('Redis · I/O=16', '#c5433c', 's', '--'),
    'valkey-io16': ('Valkey · I/O=16', '#25855c', '^', '-.'),
}
points = list(range(320, 3841, 320))
expected = {(p, op, c) for p in styles for op in ['GET', 'SET'] for c in points}
assert len(rows) == 72
assert {(r['product'], r['op'], r['connections']) for r in rows} == expected
archive = ROOT / 'evidence.tar.gz'
assert hashlib.sha256(archive.read_bytes()).hexdigest() == (ROOT / 'evidence.tar.gz.sha256').read_text().split()[0]
with tarfile.open(archive) as evidence:
    for row in rows:
        samples = [json.load(evidence.extractfile(p)) for p in row['sources']]
        for source in row['sources']:
            raw = json.load(evidence.extractfile(source.removesuffix('.result.json') + '.json'))['ALL STATS']
            assert str(raw['Runtime']['Interrupted']).lower() == 'false'
            stats = raw['Gets' if row['op'] == 'GET' else 'Sets']
            assert stats['Connection Errors'] == 0
            if row['op'] == 'GET':
                assert stats['Misses/sec'] == 0
        assert len(samples) == row['n']
        assert len(set(row['sources'])) == row['n']
        for sample in samples:
            assert sample['connections'] == row['connections']
            assert sample['kind'] == row['op']
            assert sample['keys'] == 10000000
        for field in ['qps', 'p99_ms', 'p999_ms']:
            assert math.isclose(statistics.mean(s[field] for s in samples), row[field], rel_tol=1e-12)
        assert min(s['qps'] for s in samples) == row['qps_min']
        assert max(s['qps'] for s in samples) == row['qps_max']
assert len(peaks) == 6
for peak in peaks:
    group = [r for r in rows if (r['product'], r['op']) == (peak['product'], peak['op'])]
    assert peak['qps'] == max(r['qps'] for r in group)
    assert peak['n'] == 3 and peak['decline']
    assert peak['decline']['connections'] > peak['connections']
    assert peak['decline']['qps'] <= .98 * peak['qps']

plt.rcParams.update({'font.size': 11, 'axes.spines.top': False,
                     'axes.spines.right': False, 'svg.fonttype': 'none'})
fig, axes = plt.subplots(2, 1, figsize=(11, 9), sharex=True, sharey=True)
for ax, op in zip(axes, ['GET', 'SET']):
    for product, (label, color, marker, line) in styles.items():
        data = sorted((r for r in rows if r['product'] == product and r['op'] == op), key=lambda r: r['connections'])
        xs = [r['connections'] for r in data]
        ys = [r['qps'] / 1e6 for r in data]
        ax.plot(xs, ys, color=color, marker=marker, linestyle=line, linewidth=2, markersize=5, label=label)
        for x, y, row in zip(xs, ys, data):
            if row['n'] > 1:
                ax.errorbar(x, y, yerr=[[(row['qps'] - row['qps_min']) / 1e6], [(row['qps_max'] - row['qps']) / 1e6]], color=color, capsize=3, linewidth=1)
        peak = max(data, key=lambda r: r['qps'])
        ax.plot(peak['connections'], peak['qps'] / 1e6, marker='o', markersize=11, markerfacecolor='none', markeredgecolor=color, markeredgewidth=1.4)
    ax.set_title(op, loc='left', fontweight='bold')
    ax.set_ylabel('Throughput (million ops/s)')
    ax.set_ylim(0, 1.12)
    ax.set_yticks([0, .2, .4, .6, .8, 1.0])
    ax.grid(axis='y', alpha=.2)
    ax.set_xlim(240, 3920)
axes[-1].set_xticks(points)
axes[-1].set_xlabel('Total connections (step = 320)')
fig.suptitle('10 million keys × 1 KiB · GET / SET concurrency sweep', x=.09, ha='left', fontsize=16, fontweight='bold')
fig.text(.09, .935, 'Lavik 8b7a11d (SPDK + kernel TCP) · Redis 8.8.0 · Valkey 9.1.0', fontsize=11)
handles, labels = axes[0].get_legend_handles_labels()
fig.legend(handles, labels, loc='upper left', bbox_to_anchor=(.08, .922), ncol=3, frameon=False)
fig.text(.09, .023, 'Rings mark grid peaks (3-run means). Whiskers show observed min–max for repeated points.\n30 seconds/run · 16 client threads · pipeline 1 · identical connection grid for all systems.', fontsize=10, color='#444444')
fig.subplots_adjust(left=.09, right=.98, top=.855, bottom=.12, hspace=.18)
fig.savefig(ROOT / 'throughput.png', dpi=170)
fig.savefig(ROOT / 'throughput.svg')
plt.close(fig)
print('Verified 72 aggregates and 6 repeated peaks; rendered PNG and SVG.')
