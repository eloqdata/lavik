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

"""Accept/reject a finished task-local FULL benchmark; no servers are launched."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sys


def audit(directory):
    checks, notes = [], []
    outcome = {'schema_version': 1, 'run_dir': str(directory),
               'checked_at_utc': datetime.now(timezone.utc).isoformat(),
               'accepted': False, 'checks': checks, 'notes': notes}
    def check(name, passed, detail):
        checks.append({'name': name, 'passed': bool(passed), 'detail': detail})
    try:
        result = json.loads((directory / 'result.json').read_text())
        args = result['arguments']
        workers = int(args['source_workers'])
        outcome['variant'] = args['label']
        outcome['revision'] = args['revision']
        outcome['binary_sha256'] = result['binary_sha256']
        check('dataset_verified', result.get('verified') is True and
              result.get('source_digest') == result.get('target_digest') and
              re.fullmatch('[0-9a-f]{64}', result.get('source_digest', '')),
              {'source_digest': result.get('source_digest'), 'target_digest': result.get('target_digest')})
        check('no_run_error', 'error' not in result, result.get('error'))
        check('cleanup', result.get('cleanup_errors') == [], result.get('cleanup_errors'))
        check('declared_revisions', bool(re.fullmatch('[0-9a-f]{40}', args.get('revision', ''))) and
              bool(re.fullmatch('[0-9a-f]{40}', args.get('runtime_revision', ''))),
              {'source': args.get('revision'), 'runtime': args.get('runtime_revision')})
        binary = Path(args['binary'])
        with binary.open('rb') as source:
            actual_hash = hashlib.file_digest(source, 'sha256').hexdigest()
        check('binary_unchanged', actual_hash == result['binary_sha256'],
              {'path': str(binary), 'recorded': result['binary_sha256'], 'now': actual_hash})
        seconds = result.get('full', {}).get('seconds')
        check('finite_full_time', isinstance(seconds, (int, float)) and math.isfinite(seconds) and seconds > 0,
              seconds)
        info = result.get('full', {}).get('final_replication_info', {})
        check('all_flows_online', info.get('lavik_replication_state') == 'online' and
              info.get('lavik_source_workers') == str(workers) and
              info.get('lavik_connected_flows') == str(workers),
              {'state': info.get('lavik_replication_state'), 'source_workers': info.get('lavik_source_workers'),
               'connected_flows': info.get('lavik_connected_flows')})
        log = (directory / 'source' / 'server.log').read_text()
        selections = re.findall(r'\breplication session (\S+) flow (\d+) [^\n]*? selected=(FULL|CONTINUE)\b', log)
        sessions = {session for session, _, _ in selections}
        flows = Counter(int(flow) for _, flow, mode in selections if mode == 'FULL')
        exactly_once = (len(sessions) == 1 and flows == Counter(range(workers))
                        and len(selections) == workers and all(mode == 'FULL' for _, _, mode in selections))
        check('single_full_attempt', exactly_once,
              {'expected_workers': workers, 'selections': selections,
               'sessions': sorted(sessions), 'full_flow_occurrences': dict(flows)})
        if args.get('capture'):
            stats = (directory / 'tcpdump.log').read_text()
            captured_flows = list(result.get('frames', {}).values())
            check('capture_complete', result.get('capture_complete') is True and
                  len(captured_flows) == workers and
                  all(flow.get('frame_counts', {}).get('7') == 1 and
                      not flow.get('tcp_gap_segments') and not flow.get('unparsed_bytes')
                      for flow in captured_flows) and '\n0 packets dropped by kernel' in stats,
                  {'runner_complete': result.get('capture_complete'),
                   'captured_flows': len(captured_flows), 'tcpdump_statistics': stats})
        if args.get('write_rate', 0):
            sample_path = directory / result.get('writer_samples_file', 'writer-samples.jsonl')
            samples = [json.loads(line) for line in sample_path.read_text().splitlines()]
            check('raw_writer_samples', bool(samples) and all('success' in s for s in samples),
                  {'path': str(sample_path), 'samples': len(samples)})
            check('no_writer_error', all(sample['success'] for sample in samples),
                  {'failed_samples': sum(not sample['success'] for sample in samples)})
            count = result.get('writer', {}).get('during_full', {}).get('count', 0)
            if count < 100:
                notes.append(f'Only {count} successful write latency samples began during FULL; do not claim robust p99/p999.')
        if result.get('metric_sample_errors'):
            notes.append({'metric_sample_errors': result['metric_sample_errors'],
                          'limit': 'Metrics are incomplete; E2E timing does not validate missing CPU/RSS data.'})
        notes.append('Source/runtime revision fields are declarations; retain build logs/cache beside the copied binary to prove provenance.')
    except Exception as error:
        check('audit_inputs_readable', False, repr(error))
    outcome['accepted'] = bool(checks) and all(item['passed'] for item in checks)
    return outcome


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_dir', type=Path)
    args = parser.parse_args()
    outcome = audit(args.run_dir)
    args.run_dir.mkdir(parents=True, exist_ok=True)
    (args.run_dir / 'acceptance.json').write_text(json.dumps(outcome, indent=2) + '\n')
    print(json.dumps({'run_dir': str(args.run_dir), 'accepted': outcome['accepted'],
                      'failures': [c['name'] for c in outcome['checks'] if not c['passed']]}))
    sys.exit(0 if outcome['accepted'] else 1)
