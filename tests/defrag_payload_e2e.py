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

"""Check cross-worker defrag payload ownership and live/obsolete corruption."""

import contextlib
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

WORKDIR = None
BINARY = None


def free_ports():
    # Keep both reservations alive while choosing the pair. Closing the first
    # socket before choosing the second can return the same ephemeral port.
    with socket.socket() as redis, socket.socket() as metrics:
        redis.bind(('127.0.0.1', 0))
        metrics.bind(('127.0.0.1', 0))
        return redis.getsockname()[1], metrics.getsockname()[1]


class Client:
    def __init__(self, p):
        self.socket = socket.create_connection(('127.0.0.1', p), timeout=30)
        self.stream = self.socket.makefile('rb')

    def response(self):
        head = self.stream.readline()
        assert head.endswith(b'\r\n'), head
        if head[:1] == b'$':
            size = int(head[1:-2])
            if size == -1:
                return None
            value = self.stream.read(size)
            assert self.stream.read(2) == b'\r\n'
            return value
        assert head[:1] != b'-', head
        return head[1:-2]

    def batch(self, commands):
        wire = b''.join(b'*' + str(len(args)).encode() + b'\r\n' + b''.join(
            b'$' + str(len(arg)).encode() + b'\r\n' + arg + b'\r\n' for arg in args)
            for args in commands)
        self.socket.sendall(wire)
        return [self.response() for _ in commands]

    def close(self):
        self.stream.close()
        self.socket.close()


@contextlib.contextmanager
def server(workers, *, paused=False):
    p, m = free_ports()
    args = [BINARY, '--bind=127.0.0.1', '--port=' + str(p), '--metrics-port=' + str(m),
            '--threads=' + str(workers), '--data-file=' + str(WORKDIR / 'data'),
            '--no-pin-workers', '--recv-buffers-per-worker=0',
            '--registered-buffer-mb-per-worker=64', '--max-memory=1G',
            '--no-shutdown-checkpoint',
            '--flush-max-ms=10', '--log-dir=' + str(WORKDIR / ('logs-' + str(workers)))]
    if paused:
        args.append('--defrag-paused')
    with (WORKDIR / 'server.log').open('a') as log:
        log.write('Starting ' + repr(args) + '\n')
        log.flush()
        process = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
    client = None
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            assert process.poll() is None, 'server exited during recovery'
            try:
                client = Client(p)
                assert client.batch([[b'PING']]) == [b'PONG']
                break
            except (OSError, AssertionError):
                if client:
                    client.close()
                    client = None
                time.sleep(.05)
        assert client is not None
        yield client, m
    except BaseException:
        print((WORKDIR / 'server.log').read_text(errors='replace')[-12000:],
              file=sys.stderr)
        raise
    finally:
        if client:
            client.close()
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=120)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise
        assert process.returncode == 0, process.returncode


def chunks(items, size=64):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def check_borrowed_payloads():
    WORKDIR.mkdir(exist_ok=False)
    with (WORKDIR / 'data').open('wb') as file:
        file.truncate(256 * 1024 * 1024)
    keys = [(b'view-' + str(i).encode() + (b'k' * 5000 if i % 127 == 0 else b'')) for i in range(16000)]
    old = bytes(range(256)) * 8
    new = bytes(reversed(range(256))) * 8
    giant_key = b'giant-key-' + b'q' * 5000
    giant = bytes(range(256)) * (9 * 1024 * 1024 // 256)
    with server(2) as (client, _):
        assert client.batch([[b'SET', giant_key, giant]]) == [b'OK']
        assert client.batch([[b'SET', b'empty-value', b'']]) == [b'OK']
        for group in chunks(keys):
            assert client.batch([[b'SET', key, old] for key in group]) == [b'OK'] * len(group)
    # Recovery remaps keys and physical blocks independently across three
    # owners. Defrag must keep borrowed views alive across remote submissions.
    with server(3) as (client, metrics):
        changed = [key for i, key in enumerate(keys) if i % 5 != 0]
        for _ in range(2):
            for group in chunks(changed):
                assert client.batch([[b'SET', key, new] for key in group]) == [b'OK'] * len(group)
        deadline = time.monotonic() + 60
        completed = 0
        while time.monotonic() < deadline:
            text = urllib.request.urlopen(f'http://127.0.0.1:{metrics}/metrics', timeout=10).read().decode()
            completed = int(re.search(r'lavik_storage_defrag_runs_total\{result="success"\} (\d+)', text).group(1))
            if completed >= 2:
                break
            time.sleep(.1)
        assert completed >= 2, 'test did not complete actual relocation'
        (WORKDIR / 'metrics.txt').write_text(text)
        print('completed defrag attempts:', completed, flush=True)
    with server(2) as (client, _):
        assert client.batch([[b'GET', giant_key]]) == [giant]
        assert client.batch([[b'GET', b'empty-value']]) == [b'']
        for offset in range(0, len(keys), 64):
            group = keys[offset:offset + 64]
            expected = [old if i % 5 == 0 else new for i in range(offset, offset + len(group))]
            assert client.batch([[b'GET', key] for key in group]) == expected
    (WORKDIR / 'data').unlink()
    print('PASS: borrowed defrag buffers survive relocation and recovery', flush=True)


def check_payload_crc(obsolete):
    WORKDIR.mkdir(exist_ok=False)
    with (WORKDIR / 'data').open('wb') as file:
        file.truncate(256 * 1024 * 1024)
    sentinel = b'checksum-sentinel'
    old = b'\x92' * 2048
    new = b'\x73' * 2048
    keys = [b'crc-' + str(i).encode() for i in range(16000)]
    with server(2, paused=True) as (client, _):
        assert client.batch([[b'SET', sentinel, old]]) == [b'OK']
        for group in chunks(keys):
            assert client.batch([[b'SET', key, new] for key in group]) == [b'OK'] * len(group)
        changed = [key for i, key in enumerate(keys) if i % 5 != 0]
        for group in chunks(changed):
            assert client.batch([[b'SET', key, new] for key in group]) == [b'OK'] * len(group)
        if obsolete:
            assert client.batch([[b'SET', sentinel, new]]) == [b'OK']
    # Clean shutdown drains every flush; recovery checks the original payload
    # before we damage it. Starting paused prevents reclamation from racing the
    # injection, and no writer can subsequently overwrite our changed byte.
    with server(2, paused=True) as (client, metrics):
        fd = os.open(WORKDIR / 'data', os.O_RDWR)
        try:
            data = os.pread(fd, os.fstat(fd).st_size, 0)
            at = data.find(old)
            assert at >= 0 and data.find(old, at + 1) < 0, 'sentinel not uniquely located'
            assert os.pwrite(fd, b'\x91', at + 101) == 1
            os.fsync(fd)
        finally:
            os.close(fd)
        assert client.batch([[b'DEFRAG', b'RESUME']]) == [b'OK']
        deadline = time.monotonic() + 60
        counts = {}
        while time.monotonic() < deadline:
            text = urllib.request.urlopen(f'http://127.0.0.1:{metrics}/metrics', timeout=10).read().decode()
            counts = {result: int(count) for result, count in re.findall(
                r'lavik_storage_defrag_runs_total\{result="([^"]+)"\} (\d+)', text)}
            if (counts.get('success', 0) >= 2 if obsolete else counts.get('error', 0) > 0):
                break
            time.sleep(.1)
        (WORKDIR / 'metrics.txt').write_text(text)
        if obsolete:
            assert counts.get('success', 0) >= 2, counts
            assert counts.get('error', 0) == 0, counts
            assert client.batch([[b'GET', sentinel]]) == [new]
        else:
            assert counts.get('error', 0) > 0, counts
            try:
                client.batch([[b'GET', sentinel]])
            except AssertionError as exc:
                assert b'checksum' in exc.args[0].lower(), exc
            else:
                raise AssertionError('corrupted live payload was accepted')
    if obsolete:
        # Cold recovery would encounter the damaged payload if the old block
        # remained allocated. Also verify the surviving version after restart.
        with server(2) as (client, _):
            assert client.batch([[b'GET', sentinel]]) == [new]
    (WORKDIR / 'data').unlink()
    print('PASS:', 'obsolete record skipped' if obsolete else 'live corruption rejected', counts, flush=True)


def main():
    global WORKDIR, BINARY
    BINARY = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(
            prefix='lavik-defrag-payload-',
            dir=os.environ.get('LAVIK_TEST_DATA_DIR')) as directory:
        root = Path(directory)
        WORKDIR = root / 'borrowed'
        check_borrowed_payloads()
        for obsolete in (False, True):
            WORKDIR = root / ('obsolete' if obsolete else 'live')
            check_payload_crc(obsolete)
    print('defrag payload lifetime and corruption checks passed')


if __name__ == '__main__':
    main()
