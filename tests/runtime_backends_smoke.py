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

"""Check runtime routing, one ring per worker, and recovery on disposable files.

Kernel mode needs ordinary io_uring permissions. --dpdk also requires root and
an unused bycorfdp0 TAP; it never binds physical devices. The binary must include
both capabilities to verify that compiled SPDK/DPDK remain inactive; the
kernel checks also run on the default io_uring-only build. --expect-no-bypass
also checks that a minimal package rejects both unavailable backends.
"""
import argparse
import os
from pathlib import Path
import signal
import socket
import subprocess as sp
import tempfile
import time


def rpc(sock, *args):
    parts = [a if isinstance(a, bytes) else str(a).encode() for a in args]
    sock.sendall(b'*%d\r\n' % len(parts) + b''.join(b'$%d\r\n' % len(a) + a + b'\r\n' for a in parts))
    f = sock.makefile('rb')
    line = f.readline(); kind, body = line[:1], line[1:-2]
    if kind == b'+': return body
    if kind == b':': return int(body)
    if kind == b'$':
        n = int(body)
        if n == -1: return None
        value = f.read(n); assert f.read(2) == b'\r\n'; return value
    raise AssertionError(line)


def run(binary, network, data, directory, iteration, populate):
    host = '198.18.0.2' if network == 'dpdk' else '127.0.0.1'
    port = 16401
    env = {k: v for k, v in os.environ.items() if not k.startswith('BYCORF_')}
    if network == 'dpdk':
        assert not Path('/sys/class/net/bycorfdp0').exists()
        env.update(BYCORF_DPDK_QUEUES='1', BYCORF_DPDK_RX_STEERING='hash', BYCORF_DPDK_MODE='adaptive')
    else:
        # An inactive backend must not even parse these invalid settings.
        env.update(BYCORF_EAL_ARGS='--invalid-disabled-eal-option', BYCORF_DPDK_MODE='invalid')
    path = directory / f'{iteration}-{network}.log'
    args = [str(binary), '--network', network, '--storage', 'uring', '--bind', host,
            '--port', str(port), '--metrics-port', '0', '--threads', '2',
            '--registered-buffer-mb-per-worker', '64', '--shutdown-checkpoint',
            '--data-file', str(data), '--log-dir', str(directory / f'logs-{iteration}')]
    with path.open('w') as log:
        p = sp.Popen(args, stdout=log, stderr=sp.STDOUT, env=env)
        sock = None
        try:
            ready = time.monotonic() + 45
            tap_ready = False
            while time.monotonic() < ready:
                assert p.poll() is None, path.read_text()[-5000:]
                if network == 'dpdk' and not tap_ready:
                    if path.read_text().count('bycorf0: Ethernet address:') < 3:
                        time.sleep(.05); continue
                    sp.run(['ip', 'link', 'set', 'bycorfdp0', 'address', '02:00:00:00:00:01'], check=True)
                    sp.run(['ip', 'address', 'add', '198.18.0.1/24', 'dev', 'bycorfdp0'], check=True)
                    tap_ready = True
                try:
                    sock = socket.create_connection((host, port), .5); sock.settimeout(15)
                    assert rpc(sock, 'PING') == b'PONG'; break
                except (OSError, AssertionError):
                    if sock: sock.close(); sock = None
                    time.sleep(.1)
            assert sock is not None, path.read_text()[-5000:]
            rings = []
            for fd in Path(f'/proc/{p.pid}/fd').iterdir():
                try: rings.append(os.readlink(fd))
                except FileNotFoundError: pass  # unrelated short-lived descriptors

            assert sum('io_uring' in f for f in rings) == 3, rings
            if network == 'kernel': assert 'EAL:' not in path.read_text()
            values = {f'backend-{i}': bytes([65+i % 26]) * (1024 if i < 31 else 262144) for i in range(32)}
            if populate:
                assert rpc(sock, 'DBSIZE') == 0
                for k, v in values.items(): assert rpc(sock, 'SET', k, v) == b'OK'
                assert rpc(sock, 'HSET', 'backend-hash', 'field', 'value') == 1
                assert rpc(sock, 'RPUSH', 'backend-list', 'one', 'two') == 2
            for k, v in values.items(): assert rpc(sock, 'GET', k) == v
            assert rpc(sock, 'HGET', 'backend-hash', 'field') == b'value'
            assert rpc(sock, 'LINDEX', 'backend-list', 1) == b'two'
            assert rpc(sock, 'DBSIZE') == 34
            # Leave a live connection to cover the selected backend's close path.
            p.send_signal(signal.SIGTERM)
            assert p.wait(timeout=45) == 0, path.read_text()[-5000:]
            print(network, iteration, 'PASS: data, recovery, 2 shards + control / 3 rings, clean shutdown', flush=True)
        finally:
            if sock: sock.close()
            if p.poll() is None:
                p.terminate()
                try: p.wait(timeout=30)
                except sp.TimeoutExpired: p.kill(); p.wait()
    if network == 'dpdk': assert not Path('/sys/class/net/bycorfdp0').exists()


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('binary', type=Path)
    ap.add_argument('--dpdk', action='store_true'); ap.add_argument('--output', type=Path)
    ap.add_argument('--expect-no-bypass', action='store_true')
    a = ap.parse_args(); binary = a.binary.resolve()
    if a.dpdk and a.expect_no_bypass:
        ap.error('--dpdk and --expect-no-bypass are mutually exclusive')
    with tempfile.TemporaryDirectory(prefix='lavik-runtime-backends-') as tmp:
        directory = a.output or Path(tmp); directory.mkdir(parents=True, exist_ok=True)
        data = Path(tmp) / 'data'; data.touch(); data.open('r+b').truncate(512 * 1024 * 1024)
        if a.expect_no_bypass:
            for args in [['--network=dpdk', '--data-file', str(data)],
                         ['--storage=spdk', '--data-file', 'spdk://0000:00:00.0/1']]:
                p = sp.run([str(binary), *args], capture_output=True, text=True, timeout=10)
                assert p.returncode == 2 and 'requires a build with LAVIK_KERNEL_BYPASS=ON' in p.stderr, (
                    args, p.returncode, p.stdout, p.stderr)
            print('minimal PASS: DPDK and SPDK rejected before backend initialization', flush=True)
        for args in [ ['--network=invalid'], ['--storage=invalid'],
                      ['--storage=spdk', '--data-file', str(data)],
                      ['--storage=uring', '--data-file', 'spdk://0000:00:00.0/1'] ]:
            p = sp.run([str(binary), *args], capture_output=True, text=True, timeout=10)
            assert p.returncode != 0 and 'EAL:' not in p.stdout + p.stderr
        modes = ['kernel', 'kernel'] + (['dpdk', 'dpdk', 'kernel'] if a.dpdk else [])
        for index, network in enumerate(modes): run(binary, network, data, directory, index, index == 0)


if __name__ == '__main__': main()
