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
"""Plot already-accepted static FULL comparisons after measurement has ended."""
import json
import os
from pathlib import Path

ROOT = Path('/mnt/local_nvme/i131')
os.environ.setdefault('MPLCONFIGDIR', str(ROOT / 'cache/matplotlib'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 9,
                     'svg.hashsalt': 'native-full-2026-10-04'})
report = json.loads((ROOT / 'aggregates-final/summary.json').read_text())
comparisons = {c['case']: c for c in report['comparisons'] if c['status'] == 'ready'}

for name, labels, panels in (
    ('chunks', ('main', 'chunks'), [
        ('128 MiB strings · 1 source / 1 target worker', 'large-string-128m-f1-rtt', [0, 1, 5, 20]),
        ('128 MiB strings · 4 source / 2 target workers', 'large-string-128m-f4-rtt', [5, 20]),
    ]),
    ('records', ('chunks', 'records'), [
        ('128 MiB dense · 1 source / 1 target worker', 'dense-records-128m-f1-rtt', [0, 1, 5, 20]),
        ('8 MiB uniform · 1 source / 1 target worker', 'uniform-records-8m-f1-rtt', [5, 20]),
    ]),
):
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.3))
    for axis, (title, prefix, rtts) in zip(axes, panels):
        for label, color, marker in zip(labels, ('#0072B2', '#D55E00'), ('o', 's')):
            medians, low, high = [], [], []
            for rtt in rtts:
                variant = comparisons[prefix + str(rtt)]['variants'][label]
                values = variant['full_seconds']
                assert len(values) == 3
                mid = variant['statistics']['median_seconds']
                medians.append(mid)
                low.append(mid - min(values))
                high.append(max(values) - mid)
            axis.errorbar(rtts, medians, yerr=[low, high], color=color, marker=marker,
                          markersize=5, linewidth=1.4, capsize=3, label=label)
        axis.set_title(title, fontsize=9)
        axis.set_xlabel('Added RTT (ms; 0 = no injected delay)')
        axis.set_ylabel('End-to-end FULL time (s)')
        axis.set_xticks(rtts)
        axis.set_ylim(bottom=0)
        axis.grid(axis='y', alpha=0.25)
        axis.spines[['top', 'right']].set_visible(False)
        axis.legend(frameon=False)
    fig.suptitle('Captured static workloads · median and min–max of 3 runs', fontsize=11)
    fig.tight_layout()
    out = ROOT / ('report-' + name) / 'full-vs-rtt.svg'
    fig.savefig(out, metadata={'Date': '2026-10-04', 'Creator': 'matplotlib; plot-benchmark-results.py'})
    plt.close(fig)
    print(out)
