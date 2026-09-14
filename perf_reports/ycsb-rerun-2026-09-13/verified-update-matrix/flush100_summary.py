#!/usr/bin/env python3
"""Validate the 100ms refresh and join only the retained Aerospike reference.

The two columns have different execution windows, recorded on every row.
Never relabel the old Keylane column as a new result or average percentiles.
The existing report's independent validation remains read-only and supplies
the explicitly historical Aerospike comparison.
"""
import argparse
import csv
import io
import json
import math
from pathlib import Path
import re

import flush100_abcd as experiment
import main_abcd_summary as historical
import run as bench

ROOT = Path(__file__).resolve().parent
PREFIX = experiment.PREFIX
PS = ('99thPercentileLatency(us)', '99.9PercentileLatency(us)', '99.99PercentileLatency(us)')


def raw_csv(path):
    result = {}
    for row in csv.reader(path.read_text().splitlines(), skipinitialspace=True):
        if len(row) == 3 and re.fullmatch(r'\[[^]]+\]', row[0]):
            result.setdefault(row[0][1:-1], {})[row[1]] = float(row[2])
    return result


def collect():
    env = json.loads((ROOT / f'{PREFIX}-environment.json').read_text())
    cells = json.loads((ROOT / f'{PREFIX}-results.json').read_text())
    phases = json.loads((ROOT / f'{PREFIX}-phases.json').read_text())
    complete = json.loads((ROOT / f'{PREFIX}-complete.json').read_text())
    final = json.loads((ROOT / f'{PREFIX}-final-server.json').read_text())
    assert len(cells) == complete['count'] == 8 and len(phases) == 16
    assert env['flush_max_ms'] == 100 and not env['aerospike_rerun']
    assert env['binary_sha256'] == experiment.SHA
    expected = {(w, r, p) for w in 'ABCD' for r in (0, 100000) for p in ('warmup', 'measured')}
    seen = set()
    rows = []
    count = env['original']['dbsize']
    for cell in phases:
        w, rate, phase = cell['workload'], cell['target_ops_sec'], cell['phase']
        assert (w, rate, phase) in expected and (w, rate, phase) not in seen
        seen.add((w, rate, phase))
        suffix = f'-target{rate}' if rate else ''
        stem = f'{PREFIX}-hreplace-{w.lower()}-c256{suffix}-{phase}'
        receipt = json.loads((ROOT / f'{stem}.json').read_text())
        command = json.loads((ROOT / f'{stem}.command.json').read_text())
        duration = 300 if phase == 'measured' else 0
        parsed = bench.parse(ROOT / f'{stem}.log', None if duration else 1000000, duration_seconds=duration)
        assert all(receipt[k] == v for k, v in parsed.items())
        assert all(receipt[k] == v for k, v in command.items())
        assert all(cell[k] == v for k, v in receipt.items())
        assert receipt['metrics'] == raw_csv(ROOT / f'{stem}.log')
        assert cell['failed'] == cell['exit_code'] == 0
        args = receipt['command']
        assert args[args.index('-threads') + 1] == '256'
        assert (int(args[args.index('-target') + 1]) if '-target' in args else 0) == rate
        props = dict(v.split('=', 1) for v in args if '=' in v)
        for k, v in {'fieldcount': '10', 'fieldlength': '128', 'readallfields': 'true',
                     'writeallfields': 'true', 'fieldlengthdistribution': 'constant',
                     'requestdistribution': 'uniform', 'measurement.interval': 'both',
                     'measurementtype': 'hdrhistogram', 'redis.scanindex': 'none',
                     'redis.updatecommand': 'keylane.hreplace', 'scanproportion': '0',
                     'readmodifywriteproportion': '0'}.items():
            assert props[k] == v
        assert int(props.get('maxexecutiontime', 0)) == duration
        assert int(props['operationcount']) == (2000000000 if duration else 1000000)
        if duration:
            assert 300000 <= receipt['runtime_ms'] < 303000
        for key, value in zip(('readproportion', 'updateproportion', 'insertproportion'), bench.WORKLOADS[w]):
            assert float(props[key]) == value
        if w == 'D':
            assert int(props['recordcount']) == experiment.D_STARTS[(rate, phase)]
            assert props['insertstart'] == '0' and props['insertcount'] == '100000000'
        else:
            assert props['recordcount'] == '100000000'
        before, after = cell['server_before'], cell['server_after']
        assert before['dbsize'] == count
        count += cell['metrics'].get('INSERT', {}).get('Return=OK', 0)
        assert after['dbsize'] == count
        assert before['sha256'] == after['sha256'] == experiment.SHA
        assert before['pid'] == after['pid']
        assert before['argv'] == after['argv'] and before['argv'][-2:] == ['--flush-max-ms', '100']
        assert before['workers'] == after['workers'] and sorted(before['workers'].values()) == list(range(12))
        assert before['defrag']['paused'] == after['defrag']['paused'] == 0
        m1 = experiment.parse_metrics((ROOT / f'{stem}.before.txt').read_text())
        m2 = experiment.parse_metrics((ROOT / f'{stem}.after.txt').read_text())
        operations = {'READ'} | ({'UPDATE'} if w in 'AB' else {'INSERT'} if w == 'D' else set())
        assert set(cell['metrics']) & {'READ', 'UPDATE', 'INSERT'} == operations
        for op, cmd in [('READ', 'hgetall'), ('UPDATE', 'keylane.hreplace'), ('INSERT', 'hmset')]:
            key = f'keylane_command_calls_total{{command="{cmd}"}}'
            assert m2.get(key, 0) - m1.get(key, 0) == cell['metrics'].get(op, {}).get('Return=OK', 0)
        for result in ('error', 'resource_exhausted'):
            key = f'keylane_storage_defrag_runs_total{{result="{result}"}}'
            assert m2[key] - m1[key] == 0
        if phase == 'measured':
            assert cell in cells
            for op in sorted(operations):
                v = cell['metrics'][op]
                assert v['Operations'] == v['Return=OK']
                for histogram in (op, 'Intended-' + op):
                    h = cell['metrics'][histogram]
                    assert h['Operations'] == v['Return=OK']
                    assert 0 <= h[PS[0]] <= h[PS[1]] <= h[PS[2]] <= h['MaxLatency(us)']
                rows.append({'mode': 'hreplace', 'workload': w, 'target': rate, 'operation': op,
                             'qps': v['Return=OK'] * 1000 / cell['runtime_ms'],
                             'p99_ms': v[PS[0]] / 1000, 'p999_ms': v[PS[1]] / 1000, 'p9999_ms': v[PS[2]] / 1000,
                             'average_us': v['AverageLatency(us)'], 'p50_us': v['50thPercentileLatency(us)'],
                             'p95_us': v['95thPercentileLatency(us)'], 'max_us': v['MaxLatency(us)'],
                             'successful': v['Return=OK'], 'raw_log': stem + '.log',
                             'start_utc': cell['start_utc'], 'end_utc': cell['end_utc'],
                             'batch': PREFIX, 'flush_max_ms': 100})
            assert math.isclose(sum(v['Return=OK'] for op, v in cell['metrics'].items() if op in operations)
                                * 1000 / cell['runtime_ms'], cell['success_qps'], rel_tol=1e-12)
    assert seen == expected
    assert final['dbsize'] == complete['keylane_count'] == count
    assert complete['original_restored'] and final['argv'] == env['original']['argv']
    assert final['defrag']['paused'] == 0
    old = historical.collect()
    old_cells = json.loads((ROOT / f'{historical.PREFIX}-results.json').read_text())
    for row in old['rows']:
        if row['mode'] != 'aerospike':
            continue
        cell = next(c for c in old_cells if c['mode'] == 'aerospike' and c['workload'] == row['workload']
                    and c['target_ops_sec'] == row['target'])
        rows.append({**row, 'start_utc': cell['start_utc'], 'end_utc': cell['end_utc'],
                     'batch': historical.PREFIX, 'flush_max_ms': None})
    totals = [{'mode': 'hreplace', 'workload': c['workload'], 'target': c['target_ops_sec'],
               'qps': c['success_qps']} for c in cells]
    totals += [r for r in old['total_qps'] if r['mode'] == 'aerospike']
    assert len(rows) == 28 and len(totals) == 16
    return {'complete': True, 'new_keylane_cells': 8, 'retained_aerospike_cells': 8,
            'rows': rows, 'total_qps': totals,
            'new_successes': sum(c['succeeded'] for c in cells),
            'new_warmup_operations': 8000000,
            'new_runtime_ms_range': [min(c['runtime_ms'] for c in cells), max(c['runtime_ms'] for c in cells)],
            'new_start_utc': min(c['start_utc'] for c in cells), 'new_end_utc': max(c['end_utc'] for c in cells),
            'aerospike_start_utc': min(r['start_utc'] for r in rows if r['mode'] == 'aerospike'),
            'aerospike_end_utc': max(r['end_utc'] for r in rows if r['mode'] == 'aerospike'),
            'keylane_start_records': env['original']['dbsize'], 'keylane_end_records': final['dbsize'],
            'D_phase_counts': [{'target': c['target_ops_sec'], 'phase': c['phase'], 'start': c['d_insert_start'],
                                'inserts': c['metrics']['INSERT']['Return=OK']} for c in phases if c['workload'] == 'D'],
            'old_keylane_1000ms_reference': [r for r in old['rows'] if r['mode'] == 'hreplace'],
            'comparison_caveat': 'Keylane refreshed at 100ms; Aerospike retained from an earlier run. Different CPU allocations and aged physical datasets; no causal speedup inferred.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--check-readme', action='store_true')
    args = parser.parse_args()
    data = collect()
    table = historical.markdown(data).replace('Keylane main HREPLACE', 'Keylane HREPLACE（100ms）')
    if args.write:
        experiment.save('summary', data)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=list(data['rows'][0]))
        writer.writeheader()
        writer.writerows(data['rows'])
        (ROOT / f'{PREFIX}-summary.csv').write_text(output.getvalue())
    if args.check_readme:
        report = (ROOT.parent / 'README.md').read_text()
        for line in table.splitlines():
            if re.match(r'^\| [ABCD] \|', line):
                assert line in report, line
        for target in re.findall(r'\[[^]]+\]\(([^)]+)\)', report):
            if not target.startswith(('https://', 'http://', '#')):
                assert (ROOT.parent / target.split('#', 1)[0]).exists(), target
        assert report.count('```') % 2 == 0
        width = None
        for line in report.splitlines():
            if line.startswith('|'):
                columns = len(line.split('|')) - 2
                if width is None:
                    width = columns
                assert columns == width, line
            else:
                width = None
        totals = {(r['mode'], r['workload'], r['target']): r['qps'] for r in data['total_qps']}
        for workload in 'ABCD':
            change = 100 * (totals['hreplace', workload, 0] / totals['aerospike', workload, 0] - 1)
            assert f'{abs(change):.2f}%' in report
        print('All displayed comparison rows match independently re-parsed logs (28 operation results and 12 total-QPS values).')
    print(table)
    print(json.dumps({k: v for k, v in data.items() if k not in ('rows', 'total_qps', 'old_keylane_1000ms_reference')}, indent=2))


if __name__ == '__main__':
    main()
