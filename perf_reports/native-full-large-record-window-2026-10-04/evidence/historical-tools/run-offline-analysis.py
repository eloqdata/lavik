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
"""Analyze accepted captures after all workload processes have exited."""
import concurrent.futures
import json
from pathlib import Path
import subprocess

ROOT = Path('/mnt/local_nvme/i131')


def one(acceptance):
    run = acceptance.parent
    checked = json.loads(acceptance.read_text())
    pcap = run / 'frames.pcap'
    if not checked['accepted'] or not pcap.exists():
        return None
    result = {'run_dir': str(run)}
    for tool, name in (('analyze-full-pcap.py', 'frame-spans'),
                       ('analyze-tcp-overlap.py', 'tcp-overlap')):
        output = run / (name + '.json')
        if output.exists():
            result[name] = 'existing'
            continue
        command = ['python3', str(ROOT / tool), str(pcap), '--output', str(output)]
        completed = subprocess.run(command, capture_output=True, text=True)
        result[name] = 'ok' if completed.returncode == 0 else 'unavailable'
        if completed.returncode:
            (run / (name + '-error.json')).write_text(json.dumps({
                'command': command, 'returncode': completed.returncode,
                'stdout': completed.stdout, 'stderr': completed.stderr}, indent=2) + '\n')
    return result


if __name__ == '__main__':
    paths = sorted((ROOT / 'measurements').glob('*/r*/acceptance.json'))
    outcomes = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        for result in pool.map(one, paths):
            if result:
                outcomes.append(result)
                if len(outcomes) % 10 == 0:
                    print(json.dumps({'captured_runs_processed': len(outcomes), 'last': result}), flush=True)
    (ROOT / 'offline-analysis-outcomes.json').write_text(json.dumps(outcomes, indent=2) + '\n')
    print(json.dumps({'captured_runs': len(outcomes),
        'span_unavailable': sum(r['frame-spans'] == 'unavailable' for r in outcomes),
        'tcp_overlap_unavailable': sum(r['tcp-overlap'] == 'unavailable' for r in outcomes)}))
