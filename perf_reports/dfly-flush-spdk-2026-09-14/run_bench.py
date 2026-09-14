#!/usr/bin/env python3
"""Pinned SPDK flush-interval experiment; never erases media or edits source.

Performance measurements belong to dfly_bench on .5. INFO/metrics snapshots
only validate correctness and explain server activity. All artifacts are
exclusive-created, and failed phases stop the matrix rather than being dropped.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
from pathlib import Path
import re
import shlex
import socket
import subprocess
import time
import urllib.request

from irq_layout import snapshot as irq_snapshot

ROOT = Path(__file__).resolve().parent
RUNTIME = Path('/mnt/dev/keylane-dfly-flush-spdk-20260914.sNjwQC')
BINARY = RUNTIME / 'build/keylane'
SOURCE = '2f02e6a75e60e942946257b8a48135cd72c8d9ff'
CLIENT = '/mnt/dev/dfly-get-issue20-20260909/dfly_bench-x86_64'
CLIENT_SHA = '68fbf912ddd469e621025e35b5b42cb658ed0daef476bf725a9d1e37cf0a538f'
REMOTE = '/mnt/dev/dfly-flush-spdk-20260914'
HOST = '172.16.0.4'
BDFS = ['d95e:00:00.0', '489e:00:00.0', '9e72:00:00.0',
        'b78e:00:00.0', 'f70f:00:00.0', '66d9:00:00.0']
RECORDS = 1_000_000_000
RATIOS = {'get': '0:1', 'set': '1:0', 'mixed': '1:1'}


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def save(name, value):
    with (ROOT / name).open('x') as out:
        if isinstance(value, str):
            out.write(value)
        else:
            json.dump(value, out, indent=2)
            out.write('\n')


def output(*argv):
    return subprocess.check_output(argv, text=True, stderr=subprocess.STDOUT)


def command(*args):
    with socket.create_connection((HOST, 6379), timeout=30) as sock:
        values = [str(a).encode() for a in args]
        sock.sendall(b'*%d\r\n' % len(values) + b''.join(
            b'$%d\r\n' % len(a) + a + b'\r\n' for a in values))
        stream = sock.makefile('rb')

        def read():
            line = stream.readline()
            if not line:
                raise RuntimeError('unexpected RESP EOF')
            kind, value = line[:1], line[1:-2]
            if kind == b'-':
                raise RuntimeError(value.decode())
            if kind == b':':
                return int(value)
            if kind == b'+':
                return value.decode()
            if kind == b'$':
                size = int(value)
                if size == -1:
                    return None
                data = stream.read(size)
                assert len(data) == size and stream.read(2) == b'\r\n'
                return data.decode()
            if kind == b'*':
                return [read() for _ in range(int(value))]
            raise RuntimeError(line)

        return read()


def snapshot(label, expected):
    info = command('INFO')
    layout = irq_snapshot()
    if not label.startswith('load-'):
        assert all(row['effective'] == str(queue) for queue, row in layout['queues'].items())
    data = {'utc': now(), 'dbsize': command('DBSIZE'), 'info': info, 'irq': layout,
            'defrag': command('DEFRAG', 'STATUS'),
            'lengths': {str(k): command('STRLEN', k)
                        for k in (0, 1, 12345678, 500000000, 999999998, 999999999)}}
    save(label + '.snapshot.json', data)
    with urllib.request.urlopen(f'http://{HOST}:19100/metrics', timeout=30) as response:
        save(label + '.metrics.txt', response.read().decode())
    assert data['dbsize'] == expected, data['dbsize']
    assert all(v == (1024 if expected else 0) for v in data['lengths'].values())
    assert 'defrag_paused:0' in info
    return data


def properties(unit):
    text = output('systemctl', 'show', unit, '-p', 'MainPID', '-p', 'ActiveState',
                  '-p', 'ExecMainStatus', '-p', 'ControlGroup')
    return dict(line.split('=', 1) for line in text.splitlines())


def start(label, interval, expected):
    assert BINARY.is_file()
    assert output('git', '-C', str(ROOT.parents[1]), 'rev-parse', 'HEAD').strip() == SOURCE
    assert output('systemctl', 'show', 'keylane-spdk-flush-build.service',
                  '-p', 'ActiveState', '--value').strip() != 'active', 'build still running'
    for bdf in BDFS:
        assert Path(f'/sys/bus/pci/devices/{bdf}/driver').resolve().name == 'vfio-pci'
    try:
        command('PING')
    except (OSError, RuntimeError):
        pass
    else:
        raise RuntimeError('RESP port is occupied')
    unit = 'keylane-dfly-' + label + '.service'
    logs = RUNTIME / (label + '-logs')
    logs.mkdir()
    argv = [str(BINARY), '--bind', HOST, '--port', '6379', '--metrics-port', '19100',
            '--threads', '16', '--pin-workers', '--shutdown-checkpoint',
            '--spdk-max-completions-per-poll', '16', '--flush-max-ms', str(interval),
            '--log-dir', str(logs)]
    for bdf in BDFS:
        argv += ['--data-file', 'spdk://' + bdf + '/1']
    launch = ['sudo', '-n', 'systemd-run', '--unit=' + unit,
              '--slice=keylane-bench.slice', '-p', 'AllowedCPUs=0-15',
              '-p', 'CPUAffinity=0-15', '-p', 'LimitNOFILE=65536',
              '-p', 'LimitMEMLOCK=infinity', '-p', 'TimeoutStopSec=900',
              '-p', 'SendSIGKILL=no', '-p', 'Restart=no',
              '-p', 'StandardOutput=append:' + str(logs / 'stdout.log'),
              '-p', 'StandardError=inherit', '--'] + argv
    save(label + '.server-command.json', {'utc': now(), 'source': SOURCE,
         'binary_sha256': hashlib.sha256(BINARY.read_bytes()).hexdigest(),
         'flush_max_ms': interval, 'argv': argv, 'systemd_argv': launch})
    subprocess.run(launch, check=True)
    began = time.monotonic()
    while time.monotonic() - began < 900:
        status = properties(unit)
        assert status['ActiveState'] not in ('failed', 'inactive'), status
        try:
            if command('PING') == 'PONG':
                break
        except (OSError, RuntimeError):
            time.sleep(1)
    else:
        raise TimeoutError('server startup exceeded 900s')
    data = snapshot(label + '.ready', expected)
    pid = int(re.search(r'process_id:(\d+)', data['info'])[1])
    assert Path(output('sudo', '-n', 'readlink', '-f', f'/proc/{pid}/exe').strip()) == BINARY
    cgroup = properties(unit)['ControlGroup']
    assert Path('/sys/fs/cgroup' + cgroup + '/cpuset.cpus.effective').read_text().strip() == '0-15'
    affinity = output('ps', '-L', '-p', str(pid), '-o', 'pid,tid,psr,comm')
    save(label + '.process.json', {'utc': now(), 'pid': pid, 'unit': unit,
         'ready_seconds': time.monotonic() - began, 'threads': affinity,
         'task_affinities': {p.name: re.search(r'Cpus_allowed_list:\s*(.*)',
              (p / 'status').read_text())[1] for p in Path(f'/proc/{pid}/task').iterdir()}})
    print(now(), label, 'READY', pid, flush=True)
    return unit, pid, logs


def stop(server, label):
    unit, pid, logs = server
    assert Path(output('sudo', '-n', 'readlink', '-f', f'/proc/{pid}/exe').strip()) == BINARY
    began = time.monotonic()
    subprocess.run(['sudo', '-n', 'systemctl', 'kill', '--kill-who=main',
                    '--signal=SIGTERM', unit], check=True)
    while time.monotonic() - began < 900:
        status = properties(unit)
        if status['MainPID'] == '0':
            break
        time.sleep(1)
    else:
        raise TimeoutError('graceful shutdown exceeded 900s; process left intact')
    save(label + '.stop.json', {'utc': now(), 'seconds': time.monotonic() - began, **status})
    assert status['ExecMainStatus'] == '0' and status['ActiveState'] == 'inactive', status
    save(label + '.server.log', output('sudo', '-n', 'cat', str(logs / 'keylane.log')))
    save(label + '.stdout.log', output('sudo', '-n', 'cat', str(logs / 'stdout.log')))
    print(now(), label, 'STOPPED CLEANLY', flush=True)


def calls(info, name):
    match = re.search('cmdstat_' + name + r':calls=(\d+)', info)
    return int(match[1]) if match else 0


def phase(label, workload, *, seconds=0, requests=10000, sequential=False, seed=42):
    remote_file = REMOTE + '/' + label + '.json'
    args = ['taskset', '-c', '0-15', CLIENT, '--h=' + HOST, '--p=6379',
            '--proactor_threads=16', '--c=40', '--n=' + str(requests),
            '--test_time=' + str(seconds), '--ratio=' + RATIOS[workload],
            '--pipeline=1', '--qps=0', '--key_prefix=', '--key_minimum=0',
            '--key_maximum=' + str(RECORDS), '--key_dist=' + ('S' if sequential else 'U'),
            '--d=1024', '--tcp_nodelay=true', '--seed=' + str(seed),
            '--json_out_file=' + remote_file]
    if not sequential:
        # dfly_bench's displayed hit percentage is rounded. Its end-of-thread
        # counters let us verify zero misses without instrumenting requests.
        args += ['--vmodule=dfly_bench=1', '--logtostderr=true']
    cmd = 'test ! -e ' + shlex.quote(remote_file) + ' && ulimit -n 8192 && exec ' + shlex.join(args)
    # Explicitly fail before launch if the remote immutable output exists.
    subprocess.run(['ssh', '172.16.0.5', 'test ! -e ' + shlex.quote(remote_file)], check=True)
    save(label + '.client-command.json', {'utc': now(), 'client_sha256': CLIENT_SHA,
         'ssh_host': '172.16.0.5', 'command': cmd, 'seconds': seconds,
         'expected_requests': None if seconds else requests * 640})
    before = command('INFO', 'commandstats')
    began = now()
    print(began, label, 'CLIENT START', flush=True)
    with (ROOT / (label + '.client.log')).open('x') as log:
        subprocess.run(['ssh', '172.16.0.5', cmd], stdout=log,
                       stderr=subprocess.STDOUT, check=True, timeout=7200)
    ended = now()
    after = command('INFO', 'commandstats')
    save(label + '.commandstats.json', {'before': before, 'after': after})
    local_file = ROOT / (label + '.client.json')
    assert not local_file.exists()
    subprocess.run(['scp', '172.16.0.5:' + remote_file, str(local_file)],
                   check=True, stdout=subprocess.DEVNULL)
    raw = (ROOT / (label + '.client.log')).read_text()
    final = re.search(r'Overall number of requests: (\d+), QPS: ([\d.e+]+), P99 lat: ([\d.e+]+)us', raw)
    assert final, raw[-1500:]
    assert not any(int(n) for n in re.findall(r'errs: (\d+)', raw)), raw[-1500:]
    assert not re.search(r'Got \d+ error responses!', raw), raw[-1500:]
    stats = json.loads(local_file.read_text())['ALL STATS']
    total = stats['Totals']
    count = total['Count']
    assert count == int(final[1])
    if not seconds:
        assert count == requests * 640, (count, requests * 640)
    duration_ms = stats['Runtime']['Total duration']
    if seconds:
        assert seconds * 1000 <= duration_ms < (seconds + 15) * 1000, duration_ms
    qps = count * 1000 / duration_ms
    assert abs(qps - total['Ops/sec']) < 0.001
    gets, sets = calls(after, 'get') - calls(before, 'get'), calls(after, 'set') - calls(before, 'set')
    assert gets == stats['Gets']['Count'] and sets == stats['Sets']['Count']
    assert gets + sets == count
    if workload == 'get':
        assert sets == 0
    if workload == 'set':
        assert gets == 0
    if workload == 'mixed':
        # Built-in ratio chooses operations probabilistically, not alternately.
        assert abs(gets - sets) <= 6 * count ** 0.5 + 640, (gets, sets)
    if gets:
        assert re.search(r'Hit rate: 100%', raw), raw[-1500:]
    exact_hits = None
    if not sequential:
        thread_hits = [int(n) for n in re.findall(r'Total hits: (\d+)', raw)]
        assert len(thread_hits) == 16, thread_hits
        exact_hits = sum(thread_hits)
        assert exact_hits == gets, (exact_hits, gets)
    # CONSOLE_INFO is mirrored to stderr with logtostderr. Validate identical
    # copies and use one histogram; counting both would double observations.
    histograms = [[(int(lo), int(hi), int(n)) for lo, hi, n in
                   re.findall(r'^\[\s*(\d+),\s*(\d+)\s*\)\s+(\d+)', section, re.M)]
                  for section in raw.split('Latency summary, all times are in usec:')[1:]]
    assert histograms and all(h == histograms[0] for h in histograms)
    buckets = histograms[0]
    assert sum(n for _, _, n in buckets) == count
    cumulative = 0
    p9999 = None
    for low, high, n in buckets:
        cumulative += n
        if cumulative * 10000 >= count * 9999:
            p9999 = [low / 1000, high / 1000]
            break
    data = {'start_utc': began, 'end_utc': ended, 'requests': count,
            'gets': gets, 'sets': sets, 'errors': 0, 'hit_rate': 1 if gets else None,
            'connections': 640, 'exact_hits': exact_hits,
            'duration_ms': duration_ms, 'wall_qps': qps,
            'console_qps': float(final[2]), 'p9999_total_bucket_ms': p9999,
            'operations': {key: {k: v for k, v in stats[key].items() if k != 'Time-Serie'}
                           for key in ('Totals', 'Gets', 'Sets')},
            'json_sha256': hashlib.sha256(local_file.read_bytes()).hexdigest(),
            'log_sha256': hashlib.sha256((ROOT / (label + '.client.log')).read_bytes()).hexdigest()}
    save(label + '.validation.json', data)
    print(now(), label, 'VALIDATED', count, 'requests;', round(qps), 'QPS', flush=True)
    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['load', 'matrix', 'smoke'])
    args = parser.parse_args()
    with (ROOT / '.runner.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert output('ssh', '172.16.0.5', 'sha256sum ' + CLIENT).split()[0] == CLIENT_SHA
        subprocess.run(['ssh', '172.16.0.5', 'mkdir -p ' + REMOTE], check=True)
        if args.action == 'load':
            server = start('load-1b-r2', 1000, 0)
            phase('load-1b', 'set', requests=1562500, sequential=True)
            snapshot('load-1b.after', RECORDS)
            stop(server, 'load-1b-r2')
        elif args.action == 'smoke':
            server = start('smoke', 1000, RECORDS)
            for workload in RATIOS:
                phase('smoke-' + workload, workload, seconds=3)
            snapshot('smoke.after', RECORDS)
            stop(server, 'smoke')
        else:
            schedule = [{'workload': workload, 'pair': pair, 'flush_ms': interval}
                        for workload in ('get', 'set', 'mixed')
                        for pair in (1, 2, 3)
                        for interval in ((1000, 100) if pair != 2 else (100, 1000))]
            save('matrix-plan.json', {'utc': now(), 'measured_seconds': 300,
                 'warmup_seconds': 30, 'source': SOURCE, 'schedule': schedule})
            for cell in schedule:
                workload, pair, interval = cell['workload'], cell['pair'], cell['flush_ms']
                label = f'{workload}-p{pair}-{interval}ms'
                server = start(label, interval, RECORDS)
                phase(label + '.warmup', workload, seconds=30, seed=100 + pair)
                snapshot(label + '.before', RECORDS)
                phase(label + '.measured', workload, seconds=300, seed=42 + pair)
                snapshot(label + '.after', RECORDS)
                stop(server, label)
            save('matrix-complete.json', {'utc': now(), 'cells': len(schedule)})


if __name__ == '__main__':
    main()
