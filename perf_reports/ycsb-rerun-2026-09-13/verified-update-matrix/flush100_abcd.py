#!/usr/bin/env python3
"""Refresh Keylane's eight report cells at 100ms; retain the Aerospike evidence.

This is a one-shot batch, not a general provisioning tool. Never reuse its D
reservations or overwrite receipts. Storage is retained and all server exits
are graceful. Finally restore the original invocation, not a changed default.
"""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import urllib.request

import cpu12_affinity as affinity
import run as bench

ROOT = Path(__file__).resolve().parent
PREFIX = 'flush100-abcd-20260914'
ORIGINAL = 'keylane-ycsb-cpu12.service'
UNIT = 'keylane-flush100-report.service'
BINARY = '/mnt/dev/keylane-main-f666837.siARSe/keylane'
SHA = '9162d45032c34a9ddffffd1ef8137495237256173092aa931b65890dc12b7266'
COMMIT = 'f666837b0038ab65564a17cb3a0bca8530f8e1be'
BASE = ['--bind', '172.16.0.4', '--port', '16379', '--metrics-port', '19100',
        '--threads', '12', '--pin-workers', '--shutdown-checkpoint',
        '--log-dir', '/mnt/dev/keylane-md0-keylane', '--data-file', '/dev/md0p1']
WORKLOADS = ('C', 'A', 'B', 'D')
RATES = (100000, 0)
# Historical report reservations end below 18B. Use a separated, recorded
# region; each phase has at most 2B operations and therefore at most 2B inserts.
D_STARTS = {(100000, 'warmup'): 100_000_000_000,
            (100000, 'measured'): 102_000_000_000,
            (0, 'warmup'): 104_000_000_000,
            (0, 'measured'): 106_000_000_000}


def cmd(args, **kwargs):
    return subprocess.run(args, check=True, capture_output=True, text=True, **kwargs).stdout.strip()


def now():
    return time.strftime('%FT%TZ', time.gmtime())


def save(name, data):
    path = ROOT / f'{PREFIX}-{name}.json'
    temporary = path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(data, indent=2) + '\n')
    temporary.replace(path)


def redis(*args):
    return cmd(['redis-cli', '--raw', '-h', '172.16.0.4', '-p', '16379', *args], timeout=15)


def no_client():
    found = subprocess.run(['ssh', '-o', 'BatchMode=yes', bench.CLIENT, 'pgrep -a java'],
                           capture_output=True, text=True, timeout=15)
    assert found.returncode == 1 and not found.stdout.strip(), found


def pid_of(unit):
    result = subprocess.run(['systemctl', 'show', unit, '-p', 'MainPID', '--value'],
                            capture_output=True, text=True)
    return int(result.stdout.strip() or '0')


def status():
    return {k: int(v) for k, v in re.findall(r'(\w+)=(\d+)', redis('DEFRAG', 'STATUS'))}


def identity(unit):
    pid = pid_of(unit)
    assert pid > 0
    exe = cmd(['sudo', '-n', 'readlink', f'/proc/{pid}/exe'])
    workers = {}
    for task in Path(f'/proc/{pid}/task').iterdir():
        cpus = sorted(os.sched_getaffinity(int(task.name)))
        if len(cpus) == 1 and (task / 'comm').read_text().strip() == 'keylane':
            workers[task.name] = cpus[0]
    assert sorted(workers.values()) == list(range(12)), workers
    assert exe == BINARY and hashlib.sha256(Path(exe).read_bytes()).hexdigest() == SHA
    state = status()
    assert all(state[k] == v for k, v in {'paused': 0, 'max_active_per_device': 8,
                                         'block_sleep_ms': 0, 'record_sleep_us': 0}.items())
    return {'pid': pid, 'exe': exe, 'sha256': SHA, 'workers': workers,
            'argv': Path(f'/proc/{pid}/cmdline').read_bytes().decode().rstrip('\0').split('\0'),
            'defrag': state, 'dbsize': int(redis('DBSIZE')),
            'active_expiration': redis('CONFIG', 'GET', 'active-expiration-*')}


def stop(unit):
    pid = pid_of(unit)
    if not pid:
        return
    print('GRACEFUL STOP', unit, pid, flush=True)
    cmd(['sudo', '-n', 'systemctl', 'kill', '--kill-whom=main', '--signal=SIGTERM', unit])
    while Path(f'/proc/{pid}').exists():
        time.sleep(2)
    while pid_of(unit):
        time.sleep(.2)


def launch(unit, original=False):
    assert not pid_of(ORIGINAL) and not pid_of(UNIT)
    owner = subprocess.run(['sudo', '-n', 'fuser', '/dev/md0p1'], capture_output=True, text=True)
    assert owner.returncode == 1 and not owner.stdout.strip(), owner
    args = list(BASE)
    output = ROOT / 'cpu12-server.stdout'
    if not original:
        args += ['--flush-max-ms', '100']
        args[args.index('--log-dir') + 1] = str(ROOT / f'{PREFIX}-server-logs')
        output = ROOT / f'{PREFIX}-server.stdout'
    command = ['sudo', '-n', 'systemd-run', '--collect', '--unit=' + unit,
               '--slice=keylane-bench.slice', '-p', 'AllowedCPUs=0-11',
               '-p', 'CPUAffinity=0 1 2 3 4 5 6 7 8 9 10 11', '-p', 'LimitNOFILE=20000',
               '-p', 'TimeoutStopSec=3min', '-p', 'KillSignal=TERM',
               '-p', 'StandardOutput=append:' + str(output), '-p', 'StandardError=inherit',
               '/usr/bin/taskset', '-c', '0-11', BINARY, *args]
    save('restore-command' if original else 'server-command', {'utc': now(), 'command': command})
    cmd(command, timeout=30)
    deadline = time.monotonic() + 900
    while True:
        assert pid_of(unit), 'Server exited during recovery'
        try:
            if redis('PING') == 'PONG':
                return
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            pass
        assert time.monotonic() < deadline, 'Recovery exceeded time limit'
        time.sleep(2)


def parse_metrics(text):
    return {name: float(value) for line in text.splitlines() if line and not line.startswith('#')
            for name, value in [line.rsplit(' ', 1)]}


def main():
    signal.signal(signal.SIGTERM, lambda n, f: (_ for _ in ()).throw(KeyboardInterrupt()))
    lock = (ROOT / f'{PREFIX}.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    assert not (ROOT / f'{PREFIX}-environment.json').exists(), 'Do not rerun this batch or its D IDs'
    no_client()
    for unit in (UNIT, 'aerospike-ycsb-compare.service', 'keylane-flush-qps-20260914.service'):
        assert not pid_of(unit), unit
    for name in ('perf', 'bpftrace'):
        assert subprocess.run(['pgrep', '-x', name], capture_output=True).returncode == 1
    original = identity(ORIGINAL)
    assert original['argv'] == [BINARY, *BASE]
    assert redis('EXISTS', '_indices') == '0'
    assert original['dbsize'] == 109_732_242
    inventory = json.loads((ROOT / 'client-artifacts.json').read_text())
    paths = [p for p in inventory['sha256'] if '/YCSB-hreplace-dist/' in p]
    actual = {p: sha for line in bench.remote(['sha256sum', *paths]).splitlines()
              for sha, p in [line.split(maxsplit=1)]}
    assert actual == {p: inventory['sha256'][p] for p in paths}
    environment = {'start_utc': now(), 'main_commit': COMMIT, 'binary_sha256': SHA,
                   'original': original, 'flush_max_ms': 100, 'expected_cells': 8,
                   'threads': 256, 'measurement_seconds': 300, 'warmup_operations': 1000000,
                   'workloads_in_execution_order': WORKLOADS, 'targets_ops_sec': RATES,
                   'D_insert_reservations': {f'{rate}-{phase}': start for (rate, phase), start in D_STARTS.items()},
                   'aerospike_source_batch': 'main-f666837-abcd-20260914',
                   'aerospike_rerun': False, 'client_sha256': actual,
                   'affinity_before': affinity.snapshot(),
                   'monitoring': bench.remote(['systemctl', 'is-active', 'prometheus', 'grafana-server']),
                   'note': 'No reload, profiling, production-code changes or default change; D adds fresh records.'}
    save('environment', environment)
    results = []
    phases = []
    count = original['dbsize']
    success = False
    try:
        stop(ORIGINAL)
        launch(UNIT)
        started = identity(UNIT)
        assert started['dbsize'] == count
        save('server', started)
        for workload in WORKLOADS:
            for rate in RATES:
                for phase in ('warmup', 'measured'):
                    no_client()
                    before = identity(UNIT)
                    assert before['dbsize'] == count
                    assert before['argv'][-2:] == ['--flush-max-ms', '100']
                    save('progress', {'utc': now(), 'completed': len(results), 'expected': 8,
                                      'workload': workload, 'target': rate, 'phase': phase})
                    cell = bench.run('hreplace', workload, 256, phase, target=rate,
                                     measurement_interval='both', label=PREFIX,
                                     duration_seconds=300 if phase == 'measured' else 0,
                                     d_insert_start=D_STARTS[(rate, phase)] if workload == 'D' else None)
                    assert cell['exit_code'] == cell['failed'] == 0
                    if phase == 'measured':
                        assert 300000 <= cell['runtime_ms'] < 303000
                    count += cell['metrics'].get('INSERT', {}).get('Return=OK', 0)
                    after = identity(UNIT)
                    assert after['dbsize'] == count
                    assert all(before[k] == after[k] for k in ('pid', 'sha256', 'workers', 'argv', 'active_expiration'))
                    suffix = f'-target{rate}' if rate else ''
                    stem = f'{PREFIX}-hreplace-{workload.lower()}-c256{suffix}-{phase}'
                    m1 = parse_metrics((ROOT / f'{stem}.before.txt').read_text())
                    m2 = parse_metrics((ROOT / f'{stem}.after.txt').read_text())
                    delta = {k: m2.get(k, 0) - m1.get(k, 0) for k in m1.keys() | m2.keys()
                             if k.startswith(('keylane_command_calls_total', 'keylane_storage_defrag_runs_total',
                                              'keylane_storage_io_'))}
                    for op, command in [('READ', 'hgetall'), ('UPDATE', 'keylane.hreplace'), ('INSERT', 'hmset')]:
                        expected = cell['metrics'].get(op, {}).get('Return=OK', 0)
                        assert expected == delta.get(f'keylane_command_calls_total{{command="{command}"}}', 0)
                    for result in ('error', 'resource_exhausted'):
                        assert delta[f'keylane_storage_defrag_runs_total{{result="{result}"}}'] == 0
                    available = [v for k, v in m2.items() if k.startswith('keylane_storage_available_bytes{')]
                    assert len(available) == 1 and available[0] > 500 * 1024**3
                    checked = {**cell, 'server_before': before, 'server_after': after, 'counter_delta': delta}
                    phases.append(checked)
                    save('phases', phases)
                    if phase == 'measured':
                        results.append(checked)
                        save('results', results)
                        print(f'RESULT {len(results)}/8 {workload} target={rate}: {cell["success_qps"]:.0f} QPS', flush=True)
        success = True
        save('measured-complete', {'count': len(results), 'end_utc': now()})
    except BaseException as error:
        save('failure', {'utc': now(), 'type': type(error).__name__, 'message': str(error), 'completed': len(results)})
        raise
    finally:
        no_client()
        # On a failed D verification, retain the actual count for recovery;
        # never delete records to make it match the planned successful count.
        if pid_of(UNIT):
            actual_count = int(redis('DBSIZE'))
            stop(UNIT)
        else:
            actual_count = count
        if not pid_of(ORIGINAL):
            launch(ORIGINAL, original=True)
        final = identity(ORIGINAL)
        assert final['argv'] == original['argv'] and final['dbsize'] == actual_count
        save('final-server', final)
        save('progress', {'utc': now(), 'completed': len(results), 'expected': 8,
                          'phase': 'complete' if success else 'failed', 'original_restored': True})
        if success:
            assert actual_count == count
            save('complete', {'count': len(results), 'end_utc': now(), 'keylane_count': count,
                              'original_restored': True, 'aerospike_rerun': False})
        print('RESTORED original 1000ms invocation; dataset retained; GC on', flush=True)


if __name__ == '__main__':
    main()
