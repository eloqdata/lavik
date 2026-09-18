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

"""Offline reconciliation of YCSB logs, server counts, and per-phase policies.

This module never starts a database or changes host policy. It works in the
measurement directory or in the report's evidence directory after publication.
"""
import csv
import hashlib
import json
from pathlib import Path
import re
import sys

import runner
import ycsb_client as bench

ROOT = Path(__file__).resolve().parent
PERCENTILES = {'p99_ms': '99thPercentileLatency(us)',
               'p999_ms': '99.9PercentileLatency(us)',
               'p9999_ms': '99.99PercentileLatency(us)'}


# Some archived script hashes predate this exact publication license notice.
# Name normalization updates the affected script hashes along with the scripts;
# only this prefix may be removed when checking a pre-notice hash.
_ARCHIVE_LICENSE_HEADER = b"""# Copyright (C) 2026 EloqData Inc.
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

"""


def check_script_digest(name, expected):
    """Verify the archived script, allowing only the publication notice."""
    body = (ROOT / name).read_bytes()
    if hashlib.sha256(body).hexdigest() == expected:
        return
    shebang = b'#!/usr/bin/env python3\n'
    assert body.startswith(shebang + _ARCHIVE_LICENSE_HEADER), name
    measured_body = shebang + body[len(shebang + _ARCHIVE_LICENSE_HEADER):]
    assert hashlib.sha256(measured_body).hexdigest() == expected, name


def data(name):
    return json.loads((ROOT / name).read_text())


def check_host(mode, current, original, require_effective=False):
    assert current['boot_id'] == original['boot_id']
    assert current['irqbalance'] == original['irqbalance'] == 'inactive'
    assert current['irqbalance_enabled'] == original['irqbalance_enabled'] == 'not-found'
    assert current['queue_masks'] == original['queue_masks']
    assert current['irqs'].keys() == original['irqs'].keys()
    if mode == 'aerospike':
        assert current['units'] == original['units'] == {
            'system.slice': '', 'user.slice': '', 'init.scope': ''}
        assert current['workqueue'] == original['workqueue'] == 'ffff'
    else:
        assert current['units'] == {k: '12-15' for k in original['units']}
        assert current['workqueue'] == 'f000'
    for irq, value in current['irqs'].items():
        assert value['name'] == original['irqs'][irq]['name']
        if mode == 'hreplace' and runner.host.PCI in value['name']:
            assert value['configured'] in ('12', '13', '14', '15')
            if require_effective:
                assert value['effective'] == value['configured']
        else:
            assert value['configured'] == original['irqs'][irq]['configured']


def rows_from_csv(path):
    result = {}
    for row in csv.reader(path.read_text().splitlines(), skipinitialspace=True):
        if len(row) == 3 and re.fullmatch(r'\[[^]]+\]', row[0]):
            result.setdefault(row[0][1:-1], {})[row[1]] = float(row[2])
    return result


def check_database(mode, side):
    """Reconcile deployment claims with effective settings, not just argv."""
    process = side['process']
    assert process['affinity'] == list(range(16 if mode == 'aerospike' else 12))
    assert process['sha256'] == (runner.AERO_SHA if mode == 'aerospike' else runner.LAVIK_SHA)
    if mode == 'aerospike':
        service = runner.kv(side['service_config'])
        assert service['auto-pin'] == 'none' and service['service-threads'] == '80'
        assert all(t['affinity'] == list(range(16)) for t in process['threads'])
        assert '/system.slice/' in process['cgroup']
        namespace = runner.kv(side['namespace'])
        defaults = {'indexes-memory-budget': '0',
                    'storage-engine.flush-max-ms': '1000',
                    'storage-engine.flush-size': '1048576',
                    'storage-engine.max-write-cache': '67108864',
                    'storage-engine.post-write-cache': '268435456',
                    'storage-engine.read-page-cache': 'false'}
        for name, expected in defaults.items():
            assert namespace[name] == expected, (name, namespace[name])
    else:
        assert '/lavik-bench.slice/' in process['cgroup']
        argv = process['argv']
        for name, expected in (('--threads', '12'), ('--flush-max-ms', '100'),
                               ('--data-file', '/dev/md127p4')):
            assert argv[argv.index(name) + 1] == expected
        assert '--pin-workers' in argv
        pinned = {tuple(t['affinity']) for t in process['threads'] if t['tid'] != process['pid']}
        assert {(cpu,) for cpu in range(12)} <= pinned
        assert all(set(t['affinity']) <= set(range(12)) for t in process['threads'])
        defrag = dict(item.split('=', 1) for item in side['defrag'].split())
        for name, expected in {'paused': '0', 'max_active_per_device': '8',
                               'block_sleep_ms': '0', 'record_sleep_us': '0'}.items():
            assert defrag[name] == expected


def check_irq_deltas(before, after):
    """Validate actual interrupt execution, independently of affinity masks."""
    counts = []
    for state in (before, after):
        current = {}
        for line in state['interrupts'].splitlines():
            if runner.host.PCI in line:
                parts = line.split()
                current[parts[0]] = list(map(int, parts[1:17]))
        assert len(current) == 17
        counts.append(current)
    assert counts[0].keys() == counts[1].keys()
    for irq, start in counts[0].items():
        delta = [z - a for a, z in zip(start, counts[1][irq])]
        assert min(delta) >= 0 and sum(delta[:12]) == 0, (irq, delta)


def verify(allow_partial=False):
    experiment = data('experiment.json')
    original = data('host-original.json')
    phases = data('phases.json')
    measured = data('results.json')
    if not allow_partial:
        assert len(phases) == 32 and len(measured) == 16
        assert data('complete.json')['complete'] == 16
        check_host('aerospike', data('host-restored.json')['after'], original)
        inherited = data('inherited-affinity-restored.json')
        check_script_digest('restore_inherited_affinity.py', inherited['script_sha256'])
        check_host('aerospike', inherited['after'], original)
        assert all(row['reason'] == 'exited' for row in inherited['skipped'])
        for row in inherited['restored']:
            assert row['thread']['affinity'] == [12, 13, 14, 15]
            assert row['ancestor']['affinity'] == row['after'] == list(range(16))
    assert experiment['fresh_records_each'] == 100_000_000
    assert experiment['fieldcount'] == 10 and experiment['fieldlength'] == 128
    assert experiment['threads'] == 256 and experiment['measurement_seconds'] == 300
    assert experiment['server'] == '172.16.0.4' and experiment['client'] == '172.16.0.5'
    assert (ROOT / 'aerospike.conf').read_text() == experiment['aerospike_configuration']
    jars = data('inventory.json')['client']['jars']
    assert all(jars[name] == digest for name, digest in data('client-builds.json')['sha256'].items())
    for name, digest in experiment['script_sha256'].items():
        check_script_digest(name, digest)
    if (ROOT / 'resume.json').exists():
        continuation = data('resume.json')
        assert continuation['retained_formal_windows'] == 8 and continuation['retained_phases'] == 16
        for name, digest in continuation['script_sha256'].items():
            check_script_digest(name, digest)
        failure = data(continuation['failed_transition'] + '/failure.json')
        assert failure['completed'] == 8 and failure['mode'] is None
    allowed_aero = {'cluster-name', 'address', 'port', 'mode', 'replication-factor', 'device'}
    for line in experiment['aerospike_configuration'].splitlines():
        tokens = line.strip().split()
        if tokens and not line.lstrip().startswith('#') and not line.rstrip().endswith(('{', '}')):
            assert tokens[0] in allowed_aero | {'context'}, tokens
    counts = {}
    for mode in ('aerospike', 'hreplace'):
        if not (ROOT / f'{mode}-load-result.json').exists():
            assert allow_partial
            continue
        saved = data(f'{mode}-load-result.json')
        parsed = bench.parse(ROOT / f'{mode}-load.log', 100_000_000)
        assert saved['exit_code'] == parsed['failed'] == 0
        assert all(saved[k] == v for k, v in parsed.items())
        assert parsed['metrics'] == rows_from_csv(ROOT / f'{mode}-load.log')
        command = data(f'{mode}-load-command.json')
        assert all(saved[k] == v for k, v in command.items())
        argv = command['command']
        assert argv[1] == 'load' and argv[argv.index('-threads') + 1] == '256'
        props = dict(value.split('=', 1) for value in argv if '=' in value)
        for name, expected in {'recordcount': '100000000', 'insertstart': '0',
                               'insertcount': '100000000', 'fieldcount': '10',
                               'fieldlength': '128', 'fieldlengthdistribution': 'constant',
                               'insertorder': 'hashed'}.items():
            assert props[name] == expected
        a, z = data(f'{mode}-load-before.json'), data(f'{mode}-load-after.json')
        assert a['dbsize'] == 0 and z['dbsize'] == 100_000_000
        runner.validate_counts(mode, a, z, parsed)
        for side in (a, z):
            check_host(mode, side['host'], original, require_effective=(side is z))
            check_database(mode, side)
        if not allow_partial or (ROOT / f'{mode}-recovered-samples.json').exists():
            assert data(f'{mode}-loaded-samples.json') == data(f'{mode}-recovered-samples.json')
            recovered = data(f'{mode}-measured-ready.json')
            assert recovered['dbsize'] == 100_000_000
            check_database(mode, recovered)
        counts[mode] = 100_000_000
    seen, rows, totals = set(), [], []
    for phase in phases:
        mode, workload, target, name = phase['mode'], phase['workload'], phase['target_ops_sec'], phase['phase']
        key = (mode, workload, target, name)
        assert key not in seen
        seen.add(key)
        assert mode in ('aerospike', 'hreplace') and workload in 'ABCD' and target in (0, 100000)
        assert name in ('warmup', 'measured')
        stem = phase['host_before'].removesuffix('.host-before.json')
        receipt, command = data(stem + '.json'), data(stem + '.command.json')
        assert receipt == phase
        assert all(receipt[k] == v for k, v in command.items())
        parsed = bench.parse(ROOT / (stem + '.log'), 1000000 if name == 'warmup' else None,
                             duration_seconds=300 if name == 'measured' else 0)
        assert phase['failed'] == phase['exit_code'] == 0
        assert all(phase[k] == v for k, v in parsed.items())
        assert phase['metrics'] == rows_from_csv(ROOT / (stem + '.log'))
        argv = phase['command']
        assert argv[argv.index('-threads') + 1] == '256'
        assert (int(argv[argv.index('-target') + 1]) if '-target' in argv else 0) == target
        props = dict(value.split('=', 1) for value in argv if '=' in value)
        for k, v in {'fieldcount': '10', 'fieldlength': '128', 'fieldlengthdistribution': 'constant',
                     'readallfields': 'true', 'writeallfields': 'true', 'requestdistribution': 'uniform',
                     'measurementtype': 'hdrhistogram', 'measurement.interval': 'both',
                     'scanproportion': '0', 'readmodifywriteproportion': '0'}.items():
            assert props[k] == v
        assert int(props['operationcount']) == (2000000000 if name == 'measured' else 1000000)
        assert int(props.get('maxexecutiontime', 0)) == (300 if name == 'measured' else 0)
        for k, v in zip(('readproportion', 'updateproportion', 'insertproportion'), bench.WORKLOADS[workload]):
            assert float(props[k]) == v
        if workload == 'D':
            assert int(props['recordcount']) == experiment['D_insert_starts'][f'{target}-{name}']
            assert props['insertstart'] == '0' and props['insertcount'] == '100000000'
        else:
            assert props['recordcount'] == '100000000'
        if mode == 'hreplace':
            assert props['redis.updatecommand'] == 'lavik.hreplace' and props['redis.scanindex'] == 'none'
        a, z = data(phase['host_before']), data(phase['host_after'])
        for side in (a, z):
            check_host(mode, side['host'], original, require_effective=True)
            check_database(mode, side)
        if mode == 'hreplace':
            check_irq_deltas(a, z)
        runner.validate_counts(mode, a, z, phase)
        assert a['dbsize'] == counts[mode]
        counts[mode] = z['dbsize']
        assert phase['before_records'] == a['dbsize'] and phase['after_records'] == z['dbsize']
        expected_ops = {'READ'} | ({'UPDATE'} if workload in 'AB' else {'INSERT'} if workload == 'D' else set())
        assert set(phase['metrics']) & {'READ', 'UPDATE', 'INSERT'} == expected_ops
        if name == 'measured':
            assert phase in measured and 300000 <= phase['runtime_ms'] < 303000
            totals.append({'mode': mode, 'workload': workload, 'target': target, 'qps': phase['success_qps'],
                'runtime_ms': phase['runtime_ms'], 'successes': phase['succeeded'],
                'start_utc': phase['start_utc'], 'end_utc': phase['end_utc']})
            for operation in sorted(expected_ops):
                values = phase['metrics'][operation]
                row = {'mode': mode, 'workload': workload, 'target': target, 'operation': operation,
                    'qps': values['Return=OK'] * 1000 / phase['runtime_ms'],
                    'mean_us': values['AverageLatency(us)'], 'max_us': values['MaxLatency(us)'],
                    'successes': values['Return=OK'], 'raw_log': stem + '.log'}
                row.update({column: values[metric] / 1000 for column, metric in PERCENTILES.items()})
                assert 0 <= row['p99_ms'] <= row['p999_ms'] <= row['p9999_ms'] <= row['max_us'] / 1000
                rows.append(row)
    if not allow_partial:
        assert seen == {(m, w, t, p) for m in ('aerospike', 'hreplace') for w in 'ABCD'
                        for t in (0, 100000) for p in ('warmup', 'measured')}
        assert len(rows) == 28 and len(totals) == 16
        for mode in counts:
            assert data(mode + '-complete.json')['records'] == counts[mode]
        assert data('complete.json')['aerospike_final_records'] == counts['aerospike']
        assert data('complete.json')['lavik_final_records'] == counts['hreplace']
        final = data('aerospike-final-ready.json')
        assert final['dbsize'] == counts['aerospike']
        check_host('aerospike', final['host'], original)
        check_database('aerospike', final)
        assert data('aerospike-final-samples.json') == data('aerospike-completed-samples.json')
    return {'complete': len(totals) == 16, 'rows': rows, 'total_qps': totals, 'final_records': counts,
            'recordcount_at_start_each': 100_000_000,
            'comparison': 'Aerospike server defaults and original host policy; Lavik 12 workers with 12+4 host isolation. Fresh equal starting datasets; different CPU allocations and database defaults.'}


def markdown(summary):
    rows = {(r['mode'], r['workload'], r['target'], r['operation']): r for r in summary['rows']}
    totals = {(r['mode'], r['workload'], r['target']): r for r in summary['total_qps']}
    lines = ['| Workload | 限速 | 操作 | Aerospike（默认参数、原始宿主配置） | Lavik（100ms、12+4 隔离） |',
             '|---|---|---|---:|---:|']
    for w in 'ABCD':
        for t in (0, 100000):
            rate = '100K' if t else '不限速'
            if w != 'C':
                q = [totals[m, w, t]['qps'] for m in ('aerospike', 'hreplace')]
                lines.append(f'| {w} | {rate} | 总 QPS | {q[0]:,.0f} | {q[1]:,.0f} |')
            for op in ('READ', 'UPDATE') if w in 'AB' else ('READ', 'INSERT') if w == 'D' else ('READ',):
                cells = []
                for m in ('aerospike', 'hreplace'):
                    r = rows[m, w, t, op]
                    cells.append(f"{r['qps']:,.0f}；{r['p99_ms']:.3f} / {r['p999_ms']:.3f} / {r['p9999_ms']:.3f}")
                lines.append(f'| {w} | {rate} | {op} | ' + ' | '.join(cells) + ' |')
    return '\n'.join(lines)


if __name__ == '__main__':
    partial = sys.argv[1:] == ['--partial']
    assert not sys.argv[1:] or partial
    result = verify(allow_partial=partial)
    print(f"PASS: {len(result['total_qps'])} formal windows, {len(result['rows'])} operation rows; logs, database counts and host policies reconciled.")
    if result['complete']:
        print(markdown(result))
