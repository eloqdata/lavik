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

"""Continue after the setup-only IRQ validation failure; retain all Aero results.

The original scripts and completed receipts are immutable. The first controller
stopped before opening Lavik's fresh partition and restored host settings.
This continuation permits deferred IRQ migration during setup, requires data
queue convergence after load, and checks actual CPU IRQ deltas for every phase.
"""
import fcntl
import json
import os
from pathlib import Path
import shutil
import traceback

import runner as r
import host_policy_v2 as host

ROOT = Path(__file__).resolve().parent
r.host = host


def converged(state):
    queues = [v for v in state['host']['irqs'].values()
              if host.PCI in v['name'] and v['name'].startswith('mlx5_comp')]
    assert len(queues) == 16
    assert all(v['configured'] == v['effective'] for v in queues), queues


def interrupt_counts(state):
    values = {}
    for line in state['interrupts'].splitlines():
        if host.PCI in line:
            pieces = line.split()
            values[pieces[0].removesuffix(':')] = list(map(int, pieces[1:17]))
    return values


def validate_irq_deltas(before, after):
    a, z = interrupt_counts(before), interrupt_counts(after)
    assert a.keys() == z.keys()
    for irq in a:
        delta = [end - start for start, end in zip(a[irq], z[irq])]
        assert min(delta) >= 0
        # No completion IRQ may execute on a worker CPU during a phase. The
        # rarely used async vector is recorded separately; it is not a queue
        # carrying benchmark packets and can remain pending while idle.
        if before['host']['irqs'][irq]['name'].startswith('mlx5_comp'):
            assert sum(delta[:12]) == 0, (irq, delta)


def main():
    assert os.geteuid() == 0
    lock = (ROOT / 'runner.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    assert not (ROOT / 'resume.json').exists(), 'One-shot continuation'
    failure = json.loads((ROOT / 'failure.json').read_text())
    assert failure['completed'] == 8 and failure['mode'] is None
    assert 'host.apply_lavik()' in failure['traceback']
    phases = json.loads((ROOT / 'phases.json').read_text())
    results = json.loads((ROOT / 'results.json').read_text())
    assert len(phases) == 16 and len(results) == 8
    assert all(p['mode'] == 'aerospike' for p in phases)
    assert not list(ROOT.glob('hreplace-*-server-command.json'))
    r.no_client()
    assert not r.run(['pgrep', '-x', 'asd'], check=False)
    assert not r.run(['pgrep', '-x', 'lavik'], check=False)
    host.assert_default()
    assert r.sha(r.LAVIK) == r.LAVIK_SHA
    experiment = json.loads((ROOT / 'experiment.json').read_text())
    for name, digest in experiment['script_sha256'].items():
        assert r.sha(ROOT / name) == digest
    archive = ROOT / 'setup-attempts' / '2-deferred-irq-migration'
    archive.mkdir()
    for name in ('failure.json', 'progress.json', 'lavik-policy-before.json',
                 'lavik-policy-applied.json', 'host-restored.json'):
        shutil.move(ROOT / name, archive / name)
    r.save('resume.json', {'utc': r.now(), 'retained_formal_windows': 8,
        'retained_phases': 16, 'failed_transition': str(archive.relative_to(ROOT)),
        'reason': 'effective IRQ affinity is deferred until the next interrupt; validate completion queues under traffic',
        'script_sha256': {name: r.sha(ROOT / name) for name in ('resume_lavik.py', 'host_policy_v2.py')}})
    current_mode = None
    try:
        host.apply_lavik()
        current_mode = 'hreplace'
        r.progress('starting-load', mode=current_mode, complete=8, total=16)
        r.start(current_mode, 'load')
        r.progress('loading', mode=current_mode, complete=8, total=16)
        r.load(current_mode)
        converged(r.state(current_mode))
        loaded = r.samples(current_mode, 'loaded')
        r.stop(current_mode)
        r.progress('recovering', mode=current_mode, complete=8, total=16)
        recovered = r.start(current_mode, 'measured')
        assert recovered['dbsize'] == r.COUNT
        assert r.samples(current_mode, 'recovered') == loaded
        expected_count = r.COUNT
        for workload in experiment['workload_order']:
            for rate in experiment['rates']:
                for phase in ('warmup', 'measured'):
                    r.no_client()
                    r.progress(phase, mode=current_mode, workload=workload, target=rate,
                               complete=len(results), total=16)
                    before = r.state(current_mode)
                    converged(before)
                    assert before['dbsize'] == expected_count
                    stem = f'{r.LABEL}-hreplace-{workload.lower()}-c256' + (f'-target{rate}' if rate else '') + f'-{phase}'
                    r.save(stem + '.host-before.json', before)
                    result = r.bench.run(current_mode, workload, 256, phase, target=rate,
                        measurement_interval='both', label=r.LABEL,
                        duration_seconds=300 if phase == 'measured' else 0,
                        d_insert_start=r.D_STARTS[rate, phase] if workload == 'D' else None)
                    after = r.state(current_mode)
                    r.save(stem + '.host-after.json', after)
                    converged(after)
                    validate_irq_deltas(before, after)
                    assert result['failed'] == 0
                    r.validate_counts(current_mode, before, after, result)
                    expected_count = after['dbsize']
                    result.update({'before_records': before['dbsize'], 'after_records': after['dbsize'],
                                   'host_before': stem + '.host-before.json', 'host_after': stem + '.host-after.json'})
                    r.save(stem + '.json', result)
                    phases.append(result)
                    r.save('phases.json', phases)
                    if phase == 'measured':
                        results.append(result)
                        r.save('results.json', results)
        r.samples(current_mode, 'completed')
        r.save('hreplace-complete.json', {'utc': r.now(), 'records': expected_count, 'state': r.state(current_mode)})
        r.stop(current_mode)
        current_mode = None
        host.restore()
        assert len(phases) == 32 and len(results) == 16
        r.progress('final-recovery', mode='aerospike', complete=16, total=16)
        final = r.start('aerospike', 'final')
        aero_count = next(v for v in reversed(results) if v['mode'] == 'aerospike')['after_records']
        assert final['dbsize'] == aero_count
        assert r.samples('aerospike', 'final') == json.loads((ROOT / 'aerospike-completed-samples.json').read_text())
        r.save('complete.json', {'utc': r.now(), 'complete': 16, 'phases': 32, 'host_restored': True,
            'aerospike_final_records': aero_count, 'lavik_final_records': expected_count,
            'aerospike_default_service_running': True, 'lavik_stopped': True})
        r.progress('complete', complete=16, total=16)
    except BaseException:
        r.save('failure.json', {'utc': r.now(), 'traceback': traceback.format_exc(), 'mode': current_mode,
                               'completed': len(results)})
        if current_mode:
            try: r.stop(current_mode)
            except BaseException: r.save('stop-failure.json', {'traceback': traceback.format_exc()})
        try: host.restore()
        except BaseException: r.save('restore-failure.json', {'traceback': traceback.format_exc()})
        r.progress('failed', complete=len(results), total=16)
        raise


if __name__ == '__main__':
    main()
