# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Format operation results separately for each measured rate setting."""

def result_table(data, target, zh=False):
    totals = {(r['mode'], r['workload'], r['target']): r for r in data['total_qps']}
    rows = {(r['mode'], r['workload'], r['target'], r['operation']): r for r in data['rows']}
    operation = '操作' if zh else 'Operation'
    lines = [f'| Workload | {operation} | Aerospike | Lavik |', '|---|---|---:|---:|']
    for workload in 'ABCD':
        label = 'D-Uniform' if workload == 'D' else workload
        total_label = '总 QPS' if zh else 'Total QPS'
        values = [f"{totals[mode, workload, target]['qps']:,.0f}" for mode in ['aerospike', 'hreplace']]
        lines.append(f'| {label} | {total_label} | ' + ' | '.join(values) + ' |')
        operations = ['READ'] + (['UPDATE'] if workload in 'AB' else ['INSERT'] if workload == 'D' else [])
        for operation in operations:
            values = []
            for mode in ['aerospike', 'hreplace']:
                r = rows[mode, workload, target, operation]
                values.append(f"{r['qps']:,.0f}; {r['p99_ms']:.3f} / {r['p999_ms']:.3f}")
            lines.append(f'| {label} | {operation} | ' + ' | '.join(values) + ' |')
    return '\n'.join(lines)
