#!/usr/bin/env python3
"""One-shot destructive reset of the six explicitly identified benchmark disks.

Never generalize these selectors or use this as a production provisioning tool.
The original md0 datasets cannot be recovered after discard without a backup.
The operating-system/workspace NVMe controller is deliberately excluded.
"""
import concurrent.futures
import datetime
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
DISKS = {
    'nvme1n1': ('f6cef3461085189d0005', 'd95e:00:00.0'),
    'nvme2n1': ('f6cef3461085189d0006', '489e:00:00.0'),
    'nvme3n1': ('f6cef3461085189d0003', '9e72:00:00.0'),
    'nvme4n1': ('f6cef3461085189d0004', 'b78e:00:00.0'),
    'nvme5n1': ('f6cef3461085189d0002', 'f70f:00:00.0'),
    'nvme6n1': ('f6cef3461085189d0001', '66d9:00:00.0'),
}
CAPACITY = 1919850381312


def run(*argv):
    return subprocess.check_output(argv, text=True, stderr=subprocess.STDOUT)


def main():
    if sys.argv[1:] != ['--erase-six-benchmark-disks']:
        raise SystemExit('explicit destructive selector required')
    # Reserve the audit file first: an accidental rerun must not erase new data.
    with (ROOT / 'media-reset.json').open('x') as audit:
        evidence = {'start_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    'devices': DISKS, 'checks': {}}
        for service in ['keylane-ycsb-cpu12.service', 'aerospike-ycsb-compare.service']:
            assert run('systemctl', 'show', service, '-p', 'MainPID', '--value').strip() == '0'
        assert len(Path('/proc/swaps').read_text().splitlines()) == 1
        md = run('sudo', 'mdadm', '--detail', '/dev/md0')
        assert 'fcfca005:d5332897:07fc491b:c23b602e' in md
        for name, (serial, bdf) in DISKS.items():
            controller = name.split('n1')[0]
            assert Path(f'/sys/class/nvme/{controller}/serial').read_text().strip() == serial
            assert Path(f'/sys/class/nvme/{controller}/device').resolve().name == bdf
            assert int(Path(f'/sys/class/block/{name}/size').read_text()) * 512 == CAPACITY
            assert {p.name for p in Path(f'/sys/class/block/{name}/holders').iterdir()} == {'md0'}
            mounts = run('lsblk', '-n', '-o', 'MOUNTPOINTS', f'/dev/{name}')
            assert not mounts.strip(), (name, mounts)
            identity = json.loads(run('sudo', 'nvme', 'id-ns', f'/dev/{name}', '-o', 'json'))
            assert identity['dlfeat'] & 7 == 1, 'discard must read back zero'
            evidence['checks'][name] = identity
        json.dump(evidence, audit, indent=2)
        audit.flush()
    # Stopping md0 is also an exclusive-ownership gate for its child partitions.
    print(run('sudo', 'mdadm', '--stop', '/dev/md0'), flush=True)
    for name in DISKS:
        assert not list(Path(f'/sys/class/block/{name}/holders').iterdir())
        print(run('sudo', 'mdadm', '--zero-superblock', f'/dev/{name}'), flush=True)

    def discard(name):
        result = run('sudo', 'blkdiscard', '--verbose', f'/dev/{name}')
        print(result, flush=True)
        return result

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(discard, DISKS))
    # Also explicitly zero and read-verify all potential native Keylane headers.
    print(run('sudo', '/mnt/dev/keylane-dfly-flush-spdk-20260914.sNjwQC/clear_test_headers',
              '--erase-six-benchmark-disks'), flush=True)
    print(run('lsblk', '-o', 'NAME,SIZE,FSTYPE,MOUNTPOINTS'), flush=True)
    print('RESET COMPLETE', datetime.datetime.now(datetime.timezone.utc).isoformat(), flush=True)


if __name__ == '__main__':
    main()
