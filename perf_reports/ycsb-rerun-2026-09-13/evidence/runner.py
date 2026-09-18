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

"""Fresh 100M-record YCSB comparison with product-specific host policies.

Each phase owns one client JVM and one database process. Every phase records
the effective host policy; receipts are written only after log, record-count,
and server command-counter reconciliation. Existing GET partitions are never
opened by these database instances. A failed run stops its server and restores
the captured host policy without overwriting completed evidence.
"""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess as sp
import sys
import time
import traceback
import urllib.request

import host_policy as host
import ycsb_client as bench

ROOT = Path(__file__).resolve().parent
LAVIK = '/mnt/dev/lavik-main-f666837.siARSe/lavik'
LAVIK_SHA = '9162d45032c34a9ddffffd1ef8137495237256173092aa931b65890dc12b7266'
AERO_SHA = '6e1b2bd6deebd5816f4f369ac1db4151217ed0be3b66578bdd6f15c24e6af8d0'
UNITS = {'aerospike': 'aerospike-ycsb-default-20260916.service',
         'hreplace': 'lavik-ycsb-tuned-20260916.service'}
COUNT = 100_000_000
LABEL = 'fresh100m-20260916'
D_STARTS = {(100000, 'warmup'): 2_000_000_000,
            (100000, 'measured'): 4_000_000_000,
            (0, 'warmup'): 6_000_000_000,
            (0, 'measured'): 8_000_000_000}


def now():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def save(name, value):
    host.save(name, value)


def sha(path):
    with open(path, 'rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def run(args, check=True):
    p = sp.run(args, text=True, capture_output=True)
    if check:
        assert p.returncode == 0, (args, p.stdout, p.stderr)
    return p.stdout.strip()


def progress(state, **kwargs):
    value = {'utc': now(), 'state': state, **kwargs}
    save('progress.json', value)
    print(json.dumps(value), flush=True)


def redis(*args):
    with socket.create_connection((bench.SERVER, 16379), timeout=20) as connection:
        pieces = [str(a).encode() if not isinstance(a, bytes) else a for a in args]
        connection.sendall(b'*' + str(len(pieces)).encode() + b'\r\n' +
            b''.join(b'$' + str(len(p)).encode() + b'\r\n' + p + b'\r\n' for p in pieces))
        f = connection.makefile('rb')
        def read():
            line = f.readline()
            assert line.endswith(b'\r\n'), line
            kind, value = line[:1], line[1:-2]
            if kind == b'+': return value
            if kind == b'-': raise RuntimeError(value.decode(errors='replace'))
            if kind == b':': return int(value)
            if kind == b'$':
                length = int(value)
                if length == -1: return None
                result = f.read(length)
                assert f.read(2) == b'\r\n'
                return result
            if kind == b'*': return [read() for _ in range(int(value))]
            raise RuntimeError(line)
        return read()


def asinfo(command):
    return run(['asinfo', '-h', bench.SERVER, '-p', '3000', '-v', command])


def kv(raw):
    return dict(item.split('=', 1) for item in raw.split(';') if '=' in item)


def no_client():
    result = sp.run(bench.ssh_argv() + ['pgrep -a java'], capture_output=True, text=True)
    assert result.returncode == 1, result.stdout + result.stderr


def process(mode):
    pid = int(run(['systemctl', 'show', UNITS[mode], '-p', 'MainPID', '--value']))
    assert pid > 1
    executable = LAVIK if mode == 'hreplace' else '/usr/bin/asd'
    assert Path(f'/proc/{pid}/exe').resolve() == Path(executable)
    assert bench.matching_pids(executable) == [pid]
    affinity = sorted(os.sched_getaffinity(pid))
    assert affinity == list(range(12 if mode == 'hreplace' else 16)), affinity
    threads = []
    for path in Path(f'/proc/{pid}/task').iterdir():
        try:
            threads.append({'tid': int(path.name), 'comm': (path / 'comm').read_text().strip(),
                            'affinity': sorted(os.sched_getaffinity(int(path.name)))})
        except (FileNotFoundError, ProcessLookupError):
            pass
    return {'pid': pid, 'argv': [v.decode() for v in Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0') if v],
        'affinity': affinity, 'threads': threads,
        'cgroup': Path(f'/proc/{pid}/cgroup').read_text(), 'sha256': sha(executable),
        'stat': Path(f'/proc/{pid}/stat').read_text(),
        'limits': Path(f'/proc/{pid}/limits').read_text()}


def metrics():
    return urllib.request.urlopen('http://' + bench.SERVER + ':19100/metrics', timeout=15).read().decode()


def metric_values(raw):
    result = {}
    for line in raw.splitlines():
        if line and not line.startswith('#'):
            name, value = line.rsplit(' ', 1)
            result[name] = float(value)
    return result


def state(mode):
    policy = host.assert_default() if mode == 'aerospike' else host.assert_lavik()
    result = {'utc': now(), 'monotonic': time.monotonic(), 'host': policy, 'process': process(mode),
              'diskstats': Path('/proc/diskstats').read_text(),
              'interrupts': Path('/proc/interrupts').read_text(),
              'meminfo': Path('/proc/meminfo').read_text()}
    if mode == 'aerospike':
        result['namespace'] = asinfo('namespace/ycsb')
        ns = kv(result['namespace'])
        assert ns['stop_writes'] == 'false' and ns['unavailable_partitions'] == '0'
        result['dbsize'] = int(ns['objects'])
        result['service_config'] = asinfo('get-config:context=service')
        service = kv(result['service_config'])
        assert service['auto-pin'] == 'none'
        assert all(t['affinity'] == list(range(16)) for t in result['process']['threads'])
    else:
        result['dbsize'] = redis('DBSIZE')
        result['metrics'] = metrics()
        result['defrag'] = redis('DEFRAG', 'STATUS').decode()
        assert 'paused=0' in result['defrag']
        result['info'] = redis('INFO', 'commandstats').decode()
        result['active_expiration'] = [x.decode() for x in redis('CONFIG', 'GET', 'active-expiration-*')]
        workers = [t for t in result['process']['threads'] if t['comm'].startswith('celer-')]
        result['worker_candidates'] = workers
    return result


def start(mode, stage):
    no_client()
    assert not run(['pgrep', '-x', 'asd'], check=False)
    assert not run(['pgrep', '-x', 'lavik'], check=False)
    command = ['systemd-run', '--unit', UNITS[mode], '--collect', '-p', 'TimeoutStopSec=300',
        '-p', 'StandardOutput=append:' + str(ROOT / (mode + '-server.stdout')),
        '-p', 'StandardError=inherit']
    if mode == 'aerospike':
        host.assert_default()
        # Match the packaged Aerospike unit's descriptor limit, with its
        # normal system.slice placement and no CPU-affinity override.
        command += ['-p', 'LimitNOFILE=100000', '/usr/bin/asd', '--config-file', str(ROOT / 'aerospike.conf'), '--foreground']
    else:
        host.assert_lavik()
        command += ['--slice=lavik-bench.slice', '-p', 'AllowedCPUs=0-11', '-p', 'CPUAffinity=0-11',
            '-p', 'LimitNOFILE=20000', LAVIK, '--bind', bench.SERVER, '--port', '16379',
            '--metrics-port', '19100', '--threads', '12', '--pin-workers', '--shutdown-checkpoint',
            '--data-file', '/dev/md127p4', '--flush-max-ms', '100',
            '--log-dir', str(ROOT / 'lavik-server-logs')]
    save(f'{mode}-{stage}-server-command.json', {'utc': now(), 'argv': command,
        'binary_sha256': sha(LAVIK if mode == 'hreplace' else '/usr/bin/asd')})
    run(command)
    deadline = time.monotonic() + 1500
    while True:
        unit_state = run(['systemctl', 'show', UNITS[mode], '-p', 'ActiveState', '--value'])
        assert unit_state in ('active', 'activating'), (mode, unit_state)
        try:
            if mode == 'aerospike':
                ns = kv(asinfo('namespace/ycsb'))
                ready = ns.get('stop_writes') == 'false' and ns.get('unavailable_partitions') == '0'
            else:
                ready = redis('PING') == b'PONG'
            if ready: break
        except (AssertionError, OSError, RuntimeError):
            pass
        assert time.monotonic() < deadline, f'{mode} readiness timeout'
        time.sleep(2)
    value = state(mode)
    save(f'{mode}-{stage}-ready.json', value)
    return value


def stop(mode):
    no_client()
    result = sp.run(['systemctl', 'stop', UNITS[mode]], text=True, capture_output=True, timeout=330)
    assert result.returncode == 0 or 'not loaded' in result.stderr, result.stderr
    executable = LAVIK if mode == 'hreplace' else '/usr/bin/asd'
    deadline = time.monotonic() + 30
    while bench.matching_pids(executable):
        assert time.monotonic() < deadline, f'{mode} did not drain'
        time.sleep(.2)


def key_name(index):
    # Matches the loaded YCSB CoreWorkload.buildKeyName / Utils.fnvhash64:
    # little-endian input bytes, signed Java long, absolute value, zeropadding=1.
    value = 0xCBF29CE484222325
    for shift in range(0, 64, 8):
        value = ((value ^ ((index >> shift) & 255)) * 1099511628211) & ((1 << 64) - 1)
    if value >= 1 << 63:
        value -= 1 << 64
    return 'user' + str(abs(value))


def samples(mode, name):
    client = None
    if mode == 'aerospike':
        import aerospike
        client = aerospike.client({'hosts': [(bench.SERVER, 3000)]}).connect()
    rows = []
    try:
        for index in (0, 1, 17, 999, 99999, 1234567, 50000000, 99999999):
            key = key_name(index)
            if client is not None:
                _, _, values = client.get(('ycsb', 'usertable', key))
            else:
                raw = redis('HGETALL', key)
                values = {raw[i].decode(): raw[i + 1] for i in range(0, len(raw), 2)}
            assert set(values) == {f'field{i}' for i in range(10)}, (key, list(values))
            record = {}
            for field, value in values.items():
                value = value.encode() if isinstance(value, str) else bytes(value)
                assert len(value) == 128, (key, field, len(value))
                record[field] = hashlib.sha256(value).hexdigest()
            rows.append({'index': index, 'key': key, 'field_sha256': record})
    finally:
        if client is not None: client.close()
    save(f'{mode}-{name}-samples.json', rows)
    return rows


def load(mode):
    dist = '/mnt/dev/YCSB-aerospike-dist' if mode == 'aerospike' else '/mnt/dev/YCSB-hreplace-dist'
    binding = 'aerospike' if mode == 'aerospike' else 'redis'
    props = {'recordcount': COUNT, 'insertstart': 0, 'insertcount': COUNT,
        'fieldcount': 10, 'fieldlength': 128, 'fieldlengthdistribution': 'constant',
        'readallfields': 'true', 'writeallfields': 'true', 'insertorder': 'hashed',
        'measurementtype': 'hdrhistogram', 'measurement.interval': 'both',
        'hdrhistogram.percentiles': '50,95,99,99.9,99.99', 'status.interval': 10}
    if mode == 'aerospike':
        props.update({'as.host': bench.SERVER, 'as.port': 3000, 'as.namespace': 'ycsb', 'as.timeout': 10000})
    else:
        props.update({'redis.host': bench.SERVER, 'redis.port': 16379, 'redis.scanindex': 'none',
                      'redis.timeout': 10000, 'redis.cluster': 'false'})
    command = [dist + '/bin/ycsb', 'load', binding, '-s', '-threads', '256', '-P', dist + '/workloads/workloada']
    for k, v in props.items(): command += ['-p', f'{k}={v}']
    before = state(mode)
    assert before['dbsize'] == 0
    save(f'{mode}-load-before.json', before)
    receipt = {'mode': mode, 'stage': 'load', 'start_utc': now(), 'command': command}
    save(f'{mode}-load-command.json', receipt)
    with (ROOT / f'{mode}-load.log').open('w') as output:
        p = sp.run(bench.ssh_argv() + [shlex.join(command)], stdout=output, stderr=sp.STDOUT, timeout=7200)
    parsed = bench.parse(ROOT / f'{mode}-load.log', COUNT)
    assert p.returncode == parsed['failed'] == 0
    after = state(mode)
    assert after['dbsize'] == COUNT
    save(f'{mode}-load-after.json', after)
    validate_counts(mode, before, after, parsed)
    save(f'{mode}-load-result.json', {**receipt, **parsed, 'exit_code': p.returncode, 'end_utc': now()})


def validate_counts(mode, before, after, parsed):
    operations = {op: parsed['metrics'].get(op, {}).get('Return=OK', 0) for op in ('READ', 'UPDATE', 'INSERT')}
    assert after['dbsize'] - before['dbsize'] == operations['INSERT']
    assert before['process']['pid'] == after['process']['pid']
    assert before['process']['sha256'] == after['process']['sha256']
    if mode == 'aerospike':
        a, z = kv(before['namespace']), kv(after['namespace'])
        assert int(z['client_read_success']) - int(a['client_read_success']) == operations['READ']
        assert int(z['client_write_success']) - int(a['client_write_success']) == operations['UPDATE'] + operations['INSERT']
        for k in ('client_read_error', 'client_read_timeout', 'client_read_not_found',
                  'client_write_error', 'client_write_timeout', 'expired_objects', 'evicted_objects'):
            assert z[k] == a[k], (k, a[k], z[k])
    else:
        a, z = metric_values(before['metrics']), metric_values(after['metrics'])
        for op, command in (('READ', 'hgetall'), ('UPDATE', 'lavik.hreplace'), ('INSERT', 'hmset')):
            key = 'lavik_command_calls_total{command="' + command + '"}'
            assert z.get(key, 0) - a.get(key, 0) == operations[op], (key, operations[op])
        for error in ('error', 'resource_exhausted'):
            key = 'lavik_storage_defrag_runs_total{result="' + error + '"}'
            assert z.get(key, 0) == a.get(key, 0), key


def main():
    assert os.geteuid() == 0
    lock = (ROOT / 'runner.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    assert (ROOT / 'provision-complete.json').exists()
    assert not (ROOT / 'experiment.json').exists(), 'One-shot batch; inspect partial results before resuming'
    assert sha(LAVIK) == LAVIK_SHA and sha('/usr/bin/asd') == AERO_SHA
    no_client()
    environment = {'start_utc': now(), 'lavik_commit': 'f666837b0038ab65564a17cb3a0bca8530f8e1be',
        'lavik_sha256': LAVIK_SHA, 'aerospike_sha256': AERO_SHA,
        'order': ['aerospike', 'hreplace'], 'workload_order': ['C', 'A', 'B', 'D'],
        'rates': [100000, 0], 'threads': 256, 'measurement_seconds': 300,
        'warmup_operations': 1000000, 'fresh_records_each': COUNT,
        'fieldcount': 10, 'fieldlength': 128, 'server': bench.SERVER, 'client': bench.CLIENT,
        'D_insert_starts': {f'{rate}-{phase}': value for (rate, phase), value in D_STARTS.items()},
        'aerospike_configuration': (ROOT / 'aerospike.conf').read_text(),
        'script_sha256': {name: sha(ROOT / name) for name in ('runner.py', 'host_policy.py', 'ycsb_client.py')}}
    save('experiment.json', environment)
    phases, results = [], []
    save('phases.json', phases)
    save('results.json', results)
    current_mode = None
    try:
        for mode in environment['order']:
            if mode == 'hreplace': host.apply_lavik()
            else: host.assert_default()
            current_mode = mode
            progress('starting-load', mode=mode, complete=len(results), total=16)
            start(mode, 'load')
            progress('loading', mode=mode, complete=len(results), total=16)
            load(mode)
            loaded = samples(mode, 'loaded')
            stop(mode)
            progress('recovering', mode=mode, complete=len(results), total=16)
            recovered = start(mode, 'measured')
            assert recovered['dbsize'] == COUNT
            assert samples(mode, 'recovered') == loaded
            expected_count = COUNT
            for workload in environment['workload_order']:
                for rate in environment['rates']:
                    for phase in ('warmup', 'measured'):
                        no_client()
                        progress(phase, mode=mode, workload=workload, target=rate,
                                 complete=len(results), total=16)
                        before = state(mode)
                        assert before['dbsize'] == expected_count
                        stem = f'{LABEL}-{mode}-{workload.lower()}-c256' + (f'-target{rate}' if rate else '') + f'-{phase}'
                        save(stem + '.host-before.json', before)
                        result = bench.run(mode, workload, 256, phase, target=rate,
                            measurement_interval='both', label=LABEL,
                            duration_seconds=300 if phase == 'measured' else 0,
                            d_insert_start=D_STARTS[rate, phase] if workload == 'D' else None)
                        after = state(mode)
                        save(stem + '.host-after.json', after)
                        assert result['failed'] == 0
                        validate_counts(mode, before, after, result)
                        expected_count = after['dbsize']
                        result.update({'before_records': before['dbsize'], 'after_records': after['dbsize'],
                                       'host_before': stem + '.host-before.json', 'host_after': stem + '.host-after.json'})
                        save(stem + '.json', result)
                        phases.append(result)
                        save('phases.json', phases)
                        if phase == 'measured':
                            results.append(result)
                            save('results.json', results)
            samples(mode, 'completed')
            save(mode + '-complete.json', {'utc': now(), 'records': expected_count, 'state': state(mode)})
            stop(mode)
            current_mode = None
        host.restore()
        assert len(phases) == 32 and len(results) == 16
        progress('final-recovery', mode='aerospike', complete=16, total=16)
        final = start('aerospike', 'final')
        aero_count = next(r for r in reversed(results) if r['mode'] == 'aerospike')['after_records']
        assert final['dbsize'] == aero_count
        assert samples('aerospike', 'final') == json.loads((ROOT / 'aerospike-completed-samples.json').read_text())
        save('complete.json', {'utc': now(), 'complete': 16, 'phases': 32, 'host_restored': True,
            'aerospike_final_records': aero_count, 'lavik_final_records': results[-1]['after_records'],
            'aerospike_default_service_running': True, 'lavik_stopped': True})
        progress('complete', complete=16, total=16)
    except BaseException:
        save('failure.json', {'utc': now(), 'traceback': traceback.format_exc(), 'mode': current_mode,
                              'completed': len(results)})
        if current_mode:
            try: stop(current_mode)
            except BaseException: save('stop-failure.json', {'traceback': traceback.format_exc()})
        try: host.restore()
        except BaseException: save('restore-failure.json', {'traceback': traceback.format_exc()})
        progress('failed', complete=len(results), total=16)
        raise


if __name__ == '__main__':
    main()
