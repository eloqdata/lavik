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

"""Task-local orchestration for benchmark-recipe.json; dry-run is the default."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--variants', required=True)
p.add_argument('--recipe', default=str(Path(__file__).parent / 'benchmark-accepted-primary-recipe.json'))
p.add_argument('--runner', required=True)
p.add_argument('--output', default='measurements')
p.add_argument('--only', default='.*', help='regular expression selecting named cases')
p.add_argument('--execute', action='store_true')
a = p.parse_args()
recipe = json.loads(Path(a.recipe).read_text())
variants = json.loads(Path(a.variants).read_text())
for case in recipe['cases']:
    if not re.search(a.only, case['name']):
        continue
    labels = recipe['comparisons'][case['comparison']]
    for repeat in range(recipe['repeats']):
        # Alternate the order of paired runs to reduce monotonic drift.
        for label in labels if repeat % 2 == 0 else list(reversed(labels)):
            variant = variants[label]
            if a.execute and not re.fullmatch('[0-9a-f]{40}', variant['revision']):
                raise SystemExit(f'exact committed revision required for {label}')
            run_dir = Path(a.output) / case['name'] / f'r{repeat + 1}-{label}'
            args = {**recipe['common'], **case['arguments'], **variant,
                    'label': label, 'run_dir': str(run_dir)}
            argv = [sys.executable, a.runner]
            for key, value in args.items():
                argv.extend(['--' + key.replace('_', '-'), str(value)])
            if recipe['capture']:
                argv.append('--capture')
            print(shlex.join(argv), flush=True)
            if a.execute:
                if run_dir.exists():
                    raise SystemExit(f'refusing to replace existing run: {run_dir}')
                run_dir.parent.mkdir(parents=True, exist_ok=True)
                log = run_dir.parent / f'r{repeat + 1}-{label}.driver.log'
                started = datetime.now(timezone.utc).isoformat()
                with log.open('w') as output:
                    completed = subprocess.run(argv, check=False, stdout=output, stderr=subprocess.STDOUT)
                outcome = {'command': argv, 'started_at_utc': started,
                           'finished_at_utc': datetime.now(timezone.utc).isoformat(),
                           'runner_exit_code': completed.returncode, 'driver_log': str(log)}
                run_dir.mkdir(parents=True, exist_ok=True)
                acceptance = subprocess.run([sys.executable, str(Path(__file__).parent / 'check-benchmark-run.py'),
                                             str(run_dir)], capture_output=True, text=True, check=False)
                outcome.update({'acceptance_exit_code': acceptance.returncode,
                                'acceptance_stdout': acceptance.stdout,
                                'acceptance_stderr': acceptance.stderr})
                (run_dir / 'driver-outcome.json').write_text(json.dumps(outcome, indent=2) + '\n')
                print(acceptance.stdout.strip(), flush=True)
                if completed.returncode or acceptance.returncode:
                    raise SystemExit(f'benchmark run rejected; inspect {run_dir}')
