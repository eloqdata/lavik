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

"""Provision fresh YCSB partitions only in verified free RAID0 space.

The two existing GET datasets remain owned by p1/p2. This is deliberately a
one-shot operation: an interrupted attempt must be inspected before resuming.
"""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess as sp
import time

ROOT = Path(__file__).resolve().parent
MD = '/dev/md127'


def save(name, value):
    (ROOT / name).write_text(json.dumps(value, indent=2) + '\n')


def run(args):
    p = sp.run(args, text=True, capture_output=True)
    with (ROOT / 'provision-commands.jsonl').open('a') as f:
        f.write(json.dumps({'utc': time.time(), 'argv': args, 'code': p.returncode,
                           'stdout': p.stdout, 'stderr': p.stderr}) + '\n')
    assert p.returncode == 0, (args, p.stdout, p.stderr)
    return p.stdout


def boundaries(part):
    size = part['size'] * 512
    with open(part['node'], 'rb', buffering=0) as f:
        result = {}
        for offset in (0, size // 2, size - 2**20):
            f.seek(offset)
            result[str(offset)] = hashlib.sha256(f.read(2**20)).hexdigest()
        return result


def main():
    assert os.geteuid() == 0
    assert not (ROOT / 'storage-before.json').exists()
    assert not Path('/dev/md127p3').exists() and not Path('/dev/md127p4').exists()
    detail = run(['mdadm', '--detail', '--test', MD])
    assert 'ea9bc790:01bcea07:35f053d1:cb4fa74e' in detail
    assert Path('/sys/class/block/md127/md/level').read_text().strip() == 'raid0'
    members = []
    for path in sorted(Path('/sys/class/block/md127/slaves').iterdir()):
        serial = (path / 'device/serial').read_text().strip()
        assert path.name != 'nvme0n1' and serial != 'SN00000'
        members.append({'name': path.name, 'serial': serial, 'sysfs': str(path.resolve())})
    assert len(members) == 6
    old = json.loads(run(['sfdisk', '--json', MD]))['partitiontable']
    assert len(old['partitions']) == 2
    assert [p['uuid'].lower() for p in old['partitions']] == [
        '33f27d15-2ead-4a86-94b0-f858d315334f',
        '6ae73491-1ed8-4633-8cc6-03063e710483']
    assert sp.run(['pgrep', '-x', 'lavik'], capture_output=True).returncode == 1
    previous = json.loads((ROOT / 'preserved-aerospike.json').read_text())
    pid = previous['pid']
    assert Path(f'/proc/{pid}/exe').resolve() == Path('/usr/bin/asd')
    assert Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0')[:-1] == [s.encode() for s in previous['argv']]
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + 180
    while Path(f'/proc/{pid}/exe').exists():
        assert time.monotonic() < deadline, 'Previous Aerospike failed to stop'
        time.sleep(.2)
    assert sp.run(['fuser', MD, '/dev/md127p1', '/dev/md127p2'], capture_output=True).returncode == 1
    old_hashes = {p['node']: boundaries(p) for p in old['partitions']}
    save('storage-before.json', {'table': old, 'members': members, 'detail': detail,
                                'preserved_boundaries': old_hashes, 'paused_pid': pid})
    run(['sgdisk', '--backup=' + str(ROOT / 'gpt-before.bin'), MD])
    (ROOT / 'gpt-before.txt').write_text(run(['sfdisk', '--dump', MD]))
    # Align both starts and sizes to the LCM of a full 3 MiB RAID stripe and
    # an 8 MiB Lavik block. Both products receive the same fresh capacity.
    alignment = 24 * 2**20 // 512
    size = (2**40 // 512 // alignment) * alignment
    end = max(p['start'] + p['size'] for p in old['partitions'])
    start = (end + alignment - 1) // alignment * alignment
    assert start + 2 * size - 1 <= old['lastlba']
    run(['sgdisk', '--set-alignment=1',
         f'--new=3:{start}:{start + size - 1}', '--typecode=3:8300',
         '--change-name=3:aerospike-ycsb-default-20260916',
         f'--new=4:{start + size}:{start + 2 * size - 1}', '--typecode=4:8300',
         '--change-name=4:lavik-ycsb-tuned-20260916', MD])
    run(['partprobe', MD])
    run(['udevadm', 'settle'])
    new = json.loads(run(['sfdisk', '--json', MD]))['partitiontable']
    assert new['partitions'][:2] == old['partitions']
    assert {p['node']: boundaries(p) for p in old['partitions']} == old_hashes
    save('storage-after.json', {'table': new, 'preserved_boundaries': old_hashes})
    receipts = []
    for part in new['partitions'][2:]:
        assert part['size'] == size and part['start'] % alignment == 0
        assert sp.run(['findmnt', '-S', part['node']], capture_output=True).returncode == 1
        assert sp.run(['fuser', part['node']], capture_output=True).returncode == 1
        byte_count = size * 512
        assert byte_count % (16 * 2**20) == 0
        command = ['dd', 'if=/dev/zero', 'of=' + part['node'], 'bs=16M',
                   'count=' + str(byte_count // (16 * 2**20)), 'oflag=direct',
                   'conv=fdatasync', 'status=progress']
        began = time.time()
        print('ZEROING', part['node'], byte_count, flush=True)
        with (ROOT / (Path(part['node']).name + '-zero.log')).open('w') as output:
            p = sp.run(command, stdout=output, stderr=output)
        assert p.returncode == 0
        with open(part['node'], 'rb', buffering=0) as f:
            for offset in (0, byte_count // 2, byte_count - 2**20):
                f.seek(offset)
                assert f.read(2**20) == bytes(2**20)
        receipts.append({'device': part['node'], 'bytes': byte_count, 'argv': command,
                         'start_epoch': began, 'end_epoch': time.time(), 'exit_code': p.returncode})
        save('zeroing.json', receipts)
    assert {p['node']: boundaries(p) for p in old['partitions']} == old_hashes
    save('provision-complete.json', {'utc': time.time(), 'new_devices': new['partitions'][2:],
                                   'preserved_boundaries_verified': True})
    print('PROVISION COMPLETE', flush=True)


if __name__ == '__main__':
    main()
