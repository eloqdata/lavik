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

"""Record host policy and apply Lavik isolation with deferred IRQ migration.

IRQ identities belong to one boot and PCI function. Restoration checks both;
task restoration also checks start time, so recycled TIDs cannot be changed.
Aerospike never calls the tuning path.
"""
import json
import os
from pathlib import Path
import subprocess as sp
import time

ROOT = Path(__file__).resolve().parent
UNITS = ('system.slice', 'user.slice', 'init.scope')
NICS = ('eth0', 'enP3258s1', 'eth1', 'enP5697s2')
PCI = '0cba:00:02.0'
WORKQUEUE = Path('/sys/devices/virtual/workqueue/cpumask')


def run(args, check=True):
    return sp.run(args, check=check, text=True, capture_output=True).stdout.strip()


def save(name, value):
    p = ROOT / name
    temporary = p.with_suffix(p.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(p)


def thread_snapshot():
    result = []
    for path in Path('/proc').glob('[0-9]*/task/[0-9]*'):
        try:
            raw = (path / 'stat').read_text()
            rest = raw[raw.rfind(')') + 2:].split()
            result.append({'pid': int(path.parent.parent.name), 'tid': int(path.name),
                'comm': (path / 'comm').read_text().strip(), 'flags': int(rest[6]),
                'starttime': int(rest[19]), 'affinity': sorted(os.sched_getaffinity(int(path.name)))})
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
    return result


def snapshot(include_threads=False):
    irqs = {}
    for line in Path('/proc/interrupts').read_text().splitlines():
        if 'mlx5' not in line:
            continue
        irq = line.split(':', 1)[0].strip()
        irqs[irq] = {'name': line.split()[-1],
            'configured': Path('/proc/irq', irq, 'smp_affinity_list').read_text().strip(),
            'effective': Path('/proc/irq', irq, 'effective_affinity_list').read_text().strip()}
    result = {'utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
        'units': {u: run(['systemctl', 'show', u, '-p', 'AllowedCPUs', '--value']) for u in UNITS},
        'workqueue': WORKQUEUE.read_text().strip(),
        'irqbalance': run(['systemctl', 'is-active', 'irqbalance'], check=False),
        'irqbalance_enabled': run(['systemctl', 'is-enabled', 'irqbalance'], check=False),
        'irqs': irqs, 'queue_masks': {str(p): p.read_text().strip() for nic in NICS
            for pattern in ('rx-*/rps_cpus', 'tx-*/xps_cpus')
            for p in Path('/sys/class/net', nic, 'queues').glob(pattern)}}
    if include_threads:
        result['threads'] = thread_snapshot()
    return result


def assert_default(current=None):
    original = json.loads((ROOT / 'host-original.json').read_text())
    current = current or snapshot()
    for k in ('boot_id', 'units', 'workqueue', 'irqbalance', 'irqbalance_enabled', 'queue_masks'):
        assert current[k] == original[k], (k, current[k], original[k])
    assert current['irqs'].keys() == original['irqs'].keys()
    for irq, value in current['irqs'].items():
        assert value['name'] == original['irqs'][irq]['name']
        assert value['configured'] == original['irqs'][irq]['configured'], (irq, value)
    return current


def initialize():
    assert not (ROOT / 'host-original.json').exists()
    state = snapshot(True)
    assert all(value == '' for value in state['units'].values())
    assert state['workqueue'] == 'ffff'
    # The driver-created completion queues span all CPUs on this boot. There
    # is no irqbalance service installed; do not install one to invent a default.
    assert state['irqbalance'] == 'inactive' and state['irqbalance_enabled'] == 'not-found'
    selected = [v for v in state['irqs'].values() if 'mlx5_comp' in v['name'] and PCI in v['name']]
    assert len(selected) == 16
    assert {v['configured'] for v in selected} == {str(i) for i in range(16)}
    save('host-original.json', state)


def apply_lavik():
    assert os.geteuid() == 0
    assert_default()
    assert not run(['pgrep', '-x', 'asd'], check=False)
    assert not run(['pgrep', '-x', 'lavik'], check=False)
    assert not (ROOT / 'lavik-policy-before.json').exists()
    before = snapshot(True)
    save('lavik-policy-before.json', before)
    for unit in UNITS:
        run(['systemctl', 'set-property', '--runtime', unit, 'AllowedCPUs=12-15'])
    WORKQUEUE.write_text('f000\n')
    selected = [irq for irq, v in before['irqs'].items() if PCI in v['name']]
    assert len(selected) == 17
    for index, irq in enumerate(selected):
        Path('/proc/irq', irq, 'smp_affinity_list').write_text(str(12 + index % 4) + '\n')
    moved, skipped = [], []
    for thread in before['threads']:
        if thread['flags'] & 0x04000000 or thread['comm'].startswith('kworker/'):
            continue
        try:
            os.sched_setaffinity(thread['tid'], {12, 13, 14, 15})
            moved.append(thread)
        except (OSError, ProcessLookupError) as error:
            skipped.append({'tid': thread['tid'], 'error': str(error)})
    save('lavik-policy-applied.json', {'moved': moved, 'skipped': skipped, 'after': snapshot(True)})
    assert_lavik()


def assert_lavik(current=None):
    current = current or snapshot()
    original = json.loads((ROOT / 'host-original.json').read_text())
    assert current['boot_id'] == original['boot_id']
    assert all(v == '12-15' for v in current['units'].values())
    assert current['workqueue'] == 'f000' and current['irqbalance'] == 'inactive'
    assert current['queue_masks'] == original['queue_masks']
    for irq, value in current['irqs'].items():
        if PCI in value['name']:
            # An idle MSI-X vector can retain its old effective destination until
            # its next interrupt. The resumed controller separately requires
            # completion-vector convergence after load and before each phase,
            # then checks per-CPU interrupt deltas during every measured window.
            assert value['configured'] in ('12', '13', '14', '15')
        else:
            assert value['configured'] == original['irqs'][irq]['configured']
    return current


def restore():
    p = ROOT / 'lavik-policy-before.json'
    if not p.exists():
        return assert_default()
    before = json.loads(p.read_text())
    assert before['boot_id'] == Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    assert not run(['pgrep', '-x', 'lavik'], check=False)
    for unit, value in before['units'].items():
        run(['systemctl', 'set-property', '--runtime', unit, 'AllowedCPUs=' + value])
    WORKQUEUE.write_text(before['workqueue'] + '\n')
    now = snapshot()
    for irq, value in before['irqs'].items():
        assert now['irqs'][irq]['name'] == value['name']
        if now['irqs'][irq]['configured'] != value['configured']:
            Path('/proc/irq', irq, 'smp_affinity_list').write_text(value['configured'] + '\n')
    live = {t['tid']: t for t in thread_snapshot()}
    restored, skipped = [], []
    for thread in before['threads']:
        t = live.get(thread['tid'])
        if t is None or (t['pid'], t['starttime']) != (thread['pid'], thread['starttime']):
            continue
        if t['flags'] & 0x04000000 or t['comm'].startswith('kworker/'):
            continue
        try:
            os.sched_setaffinity(t['tid'], set(thread['affinity']))
            restored.append(t['tid'])
        except (OSError, ProcessLookupError) as error:
            skipped.append({'tid': t['tid'], 'error': str(error)})
    after = assert_default(snapshot(True))
    save('host-restored.json', {'restored_tids': restored, 'skipped': skipped, 'after': after})
    return after
