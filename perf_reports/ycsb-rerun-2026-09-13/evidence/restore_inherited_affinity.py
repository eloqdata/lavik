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

"""Restore helper threads created while their parents had temporary affinity.

The main controller restores pre-existing TIDs. New helper threads can inherit
that temporary mask, so handle only recognizable helpers with a live ancestor
whose identity and original unrestricted affinity were recorded before tuning.
PID/TID start times are checked again immediately before changing affinity.
"""
import json
import os
from pathlib import Path

import host_policy_v2 as host
import runner

ROOT = Path(__file__).resolve().parent


def identity(pid):
    raw = Path(f'/proc/{pid}/stat').read_text()
    fields = raw[raw.rfind(')') + 2:].split()
    return {'ppid': int(fields[1]), 'starttime': int(fields[19])}


def main():
    assert os.geteuid() == 0
    assert not (ROOT / 'inherited-affinity-restored.json').exists()
    host.assert_default()
    baseline = json.loads((ROOT / 'lavik-policy-before.json').read_text())
    old = {t['pid']: t for t in baseline['threads'] if t['tid'] == t['pid']}
    recognized = {'codex', 'codex-code-mode', 'sshd', '(udev-worker)', 'psimon'}
    before = host.snapshot(True)
    restored, skipped = [], []
    for thread in before['threads']:
        if thread['affinity'] != [12, 13, 14, 15]:
            continue
        if thread['flags'] & 0x04000000 or thread['comm'].startswith('kworker/'):
            continue
        try:
            cursor, ancestors = thread['pid'], []
            while cursor > 0 and len(ancestors) < 32:
                current = identity(cursor)
                ancestors.append(cursor)
                if cursor in old and current['starttime'] == old[cursor]['starttime']:
                    ancestor = old[cursor]
                    break
                cursor = current['ppid']
            else:
                skipped.append({'thread': thread, 'reason': 'no recorded live ancestor'})
                continue
            owner = Path(f"/proc/{thread['pid']}/comm").read_text().strip()
            if owner not in recognized or ancestor['affinity'] != list(range(16)):
                skipped.append({'thread': thread, 'reason': 'not a recognized unrestricted helper'})
                continue
            current = identity(thread['tid'])
            assert current['starttime'] == thread['starttime']
            assert sorted(os.sched_getaffinity(thread['tid'])) == [12, 13, 14, 15]
            os.sched_setaffinity(thread['tid'], set(ancestor['affinity']))
            restored.append({'thread': thread, 'owner': owner, 'ancestor': ancestor,
                             'ancestry': ancestors, 'after': sorted(os.sched_getaffinity(thread['tid']))})
        except (FileNotFoundError, ProcessLookupError):
            skipped.append({'thread': thread, 'reason': 'exited'})
    after = host.assert_default(host.snapshot(True))
    host.save('inherited-affinity-restored.json', {'utc': runner.now(),
        'script_sha256': runner.sha(Path(__file__)), 'before': before,
        'restored': restored, 'skipped': skipped, 'after': after})
    print('Restored inherited helper affinities:', len(restored))
    print('Skipped:', [(x['thread']['tid'], x['reason']) for x in skipped])


if __name__ == '__main__':
    main()
