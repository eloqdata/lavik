#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
"""Verify the native Admin protocol against the initialized disposable gamma fixture."""
import json
import os
import socket
import subprocess
import time

binary = os.environ.get('LAVIK_CTL', '/build/lavik-ctl')
workspace = os.environ.get('LAVIK_ADMIN_TEST_DATA', '/data/admin-workspace')
seed = '127.0.0.1:9000'
owner, spare = f'{6600:040x}', f'{6601:040x}'


def native(*args, success=True):
    result = subprocess.run([binary, '--addr', seed, *args], capture_output=True, text=True, timeout=15)
    if success:
        assert result.returncode == 0 and result.stdout.startswith('OK'), result.stdout + result.stderr
    else:
        assert result.returncode != 0 and result.stdout.startswith('ERR'), result.stdout + result.stderr
    return result.stdout.strip()


def fleet(*args):
    result = subprocess.run([binary, '--socket', workspace + '/admin.sock', *args], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0 and result.stdout.startswith('OK '), result.stdout + result.stderr
    return json.loads(result.stdout[3:])


def completed(job):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        current = next(j for j in fleet('fleet-operations', 'gamma')['jobs'] if j['id'] == job['id'])
        if current['state'] == 'completed':
            return
        assert current['state'] not in ('failed', 'uncertain'), current
        time.sleep(.5)
    raise AssertionError('Replica operation did not complete')


fields = dict(part.split('=', 1) for part in native('getgroup', 'group-1').split()[1:])
assert fields['owner'] == owner
assert 'owner-or-transition' in native('unassignnode', 'group-1', owner, fields['revision'], success=False)
assert 'stale-revision' in native('unassignnode', 'group-1', spare, str(int(fields['revision']) + 1), success=False)
assert 'not-a-member' in native('unassignnode', 'group-1', spare, fields['revision'], success=False)
assert 'endpoints=tcp://127.0.0.1:6600' in native('getnode', owner)
first = native('listops', '0', '1').split()
assert first[1] == 'listops-v1' and len(first) == 3
sequence = first[2].split(':')[1]
for row in native('listops', sequence, '100').split()[2:]:
    assert int(row.split(':')[1]) > int(sequence)
assert 'bad-request' in native('listops', '0', '101', success=False)

completed(fleet('fleet-replica-add', 'gamma', 'group-1', spare, 'tcp://127.0.0.1:6601'))
completed(fleet('fleet-replica-remove', 'gamma', 'group-1', spare))
assert not next(n for n in fleet('fleet-status', 'gamma')['nodes'] if n['node_id'] == spare)['group_id']

# A rejected isolated native handshake must wake the peer to retry promptly.
# Without source-side shutdown this socket could remain open indefinitely.
with socket.create_connection(('127.0.0.1', 6600), timeout=2) as peer:
    peer.sendall(b'*1\r\n$7\r\nLVPSYNC\r\n')
    assert peer.recv(1024) == b''
result = subprocess.run(['redis-cli', '-p', '6600', 'GET', 'created:in:admin'], capture_output=True, text=True, check=True)
assert result.stdout.strip() == 'works'
print('Native protocol passed: pagination, endpoints, primary/CAS guards, add/remove, handshake close, data preservation')
