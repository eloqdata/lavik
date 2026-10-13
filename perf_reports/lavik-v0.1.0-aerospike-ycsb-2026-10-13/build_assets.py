#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Render two rate-specific QPS/p99.9 figures from the validated YCSB results."""
import hashlib
import json
import math
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter, MultipleLocator

ROOT = Path(__file__).resolve().parent
ORDER = ['aerospike', 'hreplace']
COLORS = {'hreplace': '#1473E6', 'aerospike': '#E97827'}
NAMES = {'hreplace': 'Lavik 0.1.0', 'aerospike': 'Aerospike 8.1.2.4'}


def comparison(totals, rows, target):
    fig = plt.figure(figsize=(16, 9.2), facecolor='white')
    label = '100K target' if target else 'Unlimited'
    fig.text(.06, .955, f'Lavik and Aerospike · {label}', fontsize=24, weight='bold', va='top')
    fig.text(.06, .895, 'YCSB A/B/C/D-Uniform · 256 client workers · 300 s/run · one window per case', fontsize=14, color='#566573')
    handles = []
    for mode in ORDER:
        color = COLORS[mode]
        handles.extend([
            Patch(facecolor=color, alpha=.3, label=NAMES[mode] + ' · total QPS'),
            Line2D([], [], color=color, marker='o', linewidth=2.7, label=NAMES[mode] + ' · READ p99.9'),
            Line2D([], [], color=color, marker='s', linestyle='--', linewidth=2.7, label=NAMES[mode] + ' · write p99.9'),
        ])
    fig.legend(handles=[handles[i] for i in [0, 3, 1, 4, 2, 5]], loc='upper center', bbox_to_anchor=(.5, .845), ncol=3,
               frameon=False, fontsize=11.5, handlelength=2.8, columnspacing=2)
    ax = fig.add_axes([.085, .23, .82, .49])
    latency = ax.twinx()
    ax.set_axisbelow(True)
    ax.grid(axis='y', color='#DDE3E8', linewidth=.8)
    for axis in [ax, latency]:
        axis.spines['top'].set_visible(False)
        axis.tick_params(axis='both', length=0, pad=10, labelsize=12, colors='#566573')
        for spine in axis.spines.values():
            spine.set_color('#7B8794')
    ax.set_xticks(range(4), ['A · 50% reads', 'B · 95% reads', 'C · 100% reads', 'D-Uniform · 95% reads'])
    ax.set_xlim(-.6, 3.6)
    ax.set_ylabel('Total successful operations/s · bars ↑', fontsize=12, labelpad=13)
    latency.set_ylabel('p99.9 latency (ms) · lines ↓', fontsize=12, labelpad=13)
    qps_step = 20000 if target else 100000
    peak_qps = max(r['qps'] for r in totals.values() if r['target'] == target)
    ax.set_ylim(0, math.ceil(peak_qps / qps_step) * qps_step + qps_step)
    ax.yaxis.set_major_locator(MultipleLocator(qps_step))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda n, _: f'{n/1000:g}k'))
    peak_latency = max(r['p999_ms'] for r in rows.values() if r['target'] == target)
    latency_step = .2 if target else 1
    latency.set_ylim(0, math.ceil(peak_latency / latency_step) * latency_step)
    latency.yaxis.set_major_locator(MultipleLocator(latency_step))
    for i, mode in enumerate(ORDER):
        values = [totals[mode, w, target]['qps'] for w in 'ABCD']
        bars = ax.bar([x + (i-.5)*.32 for x in range(4)], values, width=.29,
                      color=COLORS[mode], alpha=.3, edgecolor=COLORS[mode], linewidth=.8)
        for bar, value in zip(bars, values):
            latency.annotate(f'{value/1000:.1f}k',
                             xy=(bar.get_x()+bar.get_width()/2, value), xycoords=ax.transData,
                             xytext=(0, 7), textcoords='offset points', ha='center', fontsize=11,
                             zorder=6, bbox=dict(facecolor='white', edgecolor='none', pad=1.5))
        reads = [rows[mode, w, target, 'READ']['p999_ms'] for w in 'ABCD']
        # C has no write operation: retain a gap instead of inventing a zero.
        writes = [rows[mode, w, target, 'UPDATE' if w in 'AB' else 'INSERT']['p999_ms']
                  if w != 'C' else float('nan') for w in 'ABCD']
        for values, marker, style in [(reads, 'o', '-'), (writes, 's', '--')]:
            latency.plot(range(4), values, color=COLORS[mode], marker=marker, linestyle=style,
                         linewidth=2.7, markersize=8, markeredgecolor='white', markeredgewidth=.7)
    fig.text(.06, .15, 'Writes: UPDATE in A/B; INSERT in D. C has no writes. Lines connect workload categories, not a continuous scale.', fontsize=11, color='#566573')
    fig.text(.06, .105, 'Left and right axes are independent; scales differ between rate settings. Latency is measured separately for reads and writes.', fontsize=11, color='#566573')
    fig.text(.06, .055, '100M fresh records/database · 10 × 128 B fields · six-NVMe RAID0 · Lavik defrag enabled', fontsize=11.5, color='#566573')
    stem = 'ycsb-100k' if target else 'ycsb-unlimited'
    for ext in ['svg', 'png']:
        p = ROOT / f'{stem}.{ext}'
        fig.savefig(p, dpi=150, metadata={'Date': None} if ext == 'svg' else {})
        if ext == 'svg':
            p.write_text('\n'.join(line.rstrip() for line in p.read_text().splitlines())+'\n')
    plt.close(fig)


def main():
    plt.rcParams.update({'font.family':'DejaVu Sans','text.color':'#17202A','axes.labelcolor':'#17202A',
                         'svg.fonttype':'none','svg.hashsalt':'lavik010-ycsb20261013'})
    data = json.loads((ROOT/'results.json').read_text())
    assert data['complete']
    totals = {(r['mode'], r['workload'], r['target']): r for r in data['total_qps']}
    rows = {(r['mode'], r['workload'], r['target'], r['operation']): r for r in data['rows']}
    assert len(totals) == 16 and len(rows) == 28
    for target in [100000, 0]:
        comparison(totals, rows, target)
    (ROOT/'chart-SHA256SUMS').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+p.name+'\n'
        for p in sorted(ROOT.iterdir()) if p.suffix in ['.svg', '.png']))
    print('Validated 16 totals / 28 operation rows; rendered two combined figures as SVG and PNG.')


if __name__ == '__main__':
    main()
