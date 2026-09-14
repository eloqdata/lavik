#!/usr/bin/env python3
"""Apply/restore the benchmark's 16-queue layout, with exact NIC guards.

Only mlx5 completion IRQs belonging to this benchmark NIC are changed. Keep
RPS/XPS, housekeeping cpusets, and all storage/controller IRQ policies intact.
"""
import datetime
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parent
PATTERN = r'^\s*(\d+):.* mlx5_comp(\d+)@pci:b538:00:02\.0$'


def snapshot():
    queues = {}
    for irq, queue in re.findall(PATTERN, Path('/proc/interrupts').read_text(), re.M):
        queues[int(queue)] = {'irq': int(irq),
            'configured': Path(f'/proc/irq/{irq}/smp_affinity_list').read_text().strip(),
            'effective': Path(f'/proc/irq/{irq}/effective_affinity_list').read_text().strip()}
    assert set(queues) == set(range(16)), queues
    return {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'queues': queues,
            'softnet_stat': Path('/proc/net/softnet_stat').read_text(),
            'rps_xps': {str(p): p.read_text().strip() for interface in ('eth0', 'enP46392s1')
                        for p in Path(f'/sys/class/net/{interface}/queues').glob('*/*ps_cpus')}}


def main():
    assert sys.argv[1:] in (['apply'], ['restore'])
    before = snapshot()
    if sys.argv[1] == 'apply':
        with (ROOT / 'irq-before.json').open('x') as stream:
            json.dump(before, stream, indent=2)
        targets = {q: str(q) for q in before['queues']}
    else:
        original = json.loads((ROOT / 'irq-before.json').read_text())
        targets = {q: original['queues'][str(q)]['configured'] for q in before['queues']}
    for queue, row in before['queues'].items():
        Path(f"/proc/irq/{row['irq']}/smp_affinity_list").write_text(targets[queue] + '\n')
    after = snapshot()
    for queue, row in after['queues'].items():
        assert row['configured'] == targets[queue]
    with (ROOT / ('irq-' + sys.argv[1] + '.json')).open('x') as stream:
        json.dump(after, stream, indent=2)
    print(json.dumps(after, indent=2))


if __name__ == '__main__':
    main()
