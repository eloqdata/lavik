import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=5)
        self.file = self.sock.makefile('rb')

    def send(self, *args):
        data = [x if isinstance(x, bytes) else str(x).encode() for x in args]
        self.sock.sendall(b'*%d\r\n' % len(data) + b''.join(
            b'$%d\r\n' % len(x) + x + b'\r\n' for x in data))

    def read(self):
        line = self.file.readline()
        if not line:
            raise EOFError()
        if line[:1] == b'$':
            n = int(line[1:])
            return self.file.read(n + 2)[:-2] if n >= 0 else None
        return line.strip()

    def cmd(self, *args):
        self.send(*args)
        return self.read()


def stats(client):
    reply = client.cmd('INFO', 'STATS').decode()
    return {k: v for line in reply.splitlines() if ':' in line
            for k, v in [line.split(':', 1)]
            if any(x in k for x in ('storage_', 'defrag', 'tx_', 'total_commands'))}


def run(args, iteration):
    root = Path(tempfile.mkdtemp(prefix='lavik-gc-probe-'))
    data = root / 'data'
    with open(data, 'wb') as f:
        os.posix_fallocate(f.fileno(), 0, 128 * 1024 * 1024)
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]
    base = [args.binary, '--logtostderr', '--port', str(port), '--threads', '2',
            '--no-pin-workers', '--recv-buffers-per-worker', '0',
            '--max-memory', '8589934592', '--flush-max-ms', '20',
            '--data-file', str(data)]
    proc = None
    def start(crash=False):
        nonlocal proc
        env = os.environ.copy()
        if crash:
            env['LAVIK_CRASH_POINT'] = 'defrag-source-retired'
        log = open(root / ('armed.log' if crash else 'setup.log'), 'wb')
        proc = subprocess.Popen(base, stdout=log, stderr=log, env=env)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                c = Client(port)
                if c.cmd('PING') == b'+PONG':
                    return c
            except OSError:
                pass
            if proc.poll() is not None:
                raise RuntimeError(('startup exit', proc.returncode, root))
            time.sleep(.01)
        raise RuntimeError(('startup timeout', root))
    key = '{large-hash-gc}:hash'
    fillers = [f'{{large-hash-gc}}:filler-{i}' for i in range(400)]
    try:
        c = start()
        hset = ['HSET', key]
        for i in range(9):
            hset.extend([f'field-{i}', b'g' * (1024 * 1024)])
        hset.extend(['version', 'g'])
        assert c.cmd(*hset) == b':10'
        for filler in fillers:
            assert c.cmd('SET', filler, b'f' * (16 * 1024 - 1)) == b'+OK'
        deadline = time.monotonic() + 30
        while stats(c).get('storage_durability_pending') != '0':
            assert time.monotonic() < deadline
            time.sleep(.01)
        setup_stats = stats(c)
        if args.setup_delay:
            time.sleep(args.setup_delay)
        proc.kill()
        proc.wait()
        c = start(crash=True)
        if args.cleaner_ms is not None:
            assert c.cmd('CONFIG', 'SET', 'tx-cleaner-cooldown-ms', args.cleaner_ms) == b'+OK'
        before = stats(c)
        delete_client = Client(port)
        delete_client.send('DEL', *fillers)
        started = time.monotonic()
        snapshots = []
        while proc.poll() is None and time.monotonic() - started < 30:
            try:
                snapshots.append((round(time.monotonic() - started, 3), stats(c)))
            except (EOFError, OSError):
                break
            time.sleep(.1)
        if proc.poll() is None:
            try:
                proc.wait(timeout=.1)
            except subprocess.TimeoutExpired:
                pass
        result = dict(iteration=iteration, root=str(root), rc=proc.poll(),
                      elapsed=round(time.monotonic() - started, 3),
                      setup=setup_stats, before=before, snapshots=snapshots)
        (root / 'stats.json').write_text(json.dumps(result, indent=2))
        print(json.dumps({k: result[k] for k in ('iteration', 'root', 'rc', 'elapsed')}), flush=True)
        if proc.poll() is None:
            print('DEL reply:', delete_client.read(), flush=True)
            print('DBSIZE:', c.cmd('DBSIZE'), 'last stats:', snapshots[-1], flush=True)
            if args.hold_failure:
                print('Holding PID', proc.pid, flush=True)
                time.sleep(args.hold_failure)
            return False
        return proc.returncode == 86
    finally:
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait()


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('binary')
    p.add_argument('--repeat', type=int, default=1)
    p.add_argument('--setup-delay', type=float, default=0)
    p.add_argument('--cleaner-ms', type=int)
    p.add_argument('--hold-failure', type=int, default=0)
    args = p.parse_args()
    for i in range(args.repeat):
        if not run(args, i):
            raise SystemExit(1)
