#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
"""Start isolated, disposable real Lavik clusters for Admin integration tests."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import signal

if len(sys.argv) > 2 and sys.argv[1] == '--stop':
    fixture_root = Path(sys.argv[2]).resolve()
    stopped = []
    for pid_file in fixture_root.glob('*.pid'):
        pid = int(pid_file.read_text())
        command_line = Path(f'/proc/{pid}/cmdline')
        if command_line.exists() and str(fixture_root).encode() in command_line.read_bytes():
            os.kill(pid, signal.SIGTERM)
            stopped.append(pid)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and any(
            Path(f'/proc/{pid}/cmdline').exists() and
            str(fixture_root).encode() in Path(f'/proc/{pid}/cmdline').read_bytes()
            for pid in stopped):
        time.sleep(.1)
    # These are this script's disposable fixtures, never external clusters.
    for pid in stopped:
        command_line = Path(f'/proc/{pid}/cmdline')
        if command_line.exists() and str(fixture_root).encode() in command_line.read_bytes():
            os.kill(pid, signal.SIGKILL)
    sys.exit(0)

root = Path(sys.argv[1] if len(sys.argv) > 1 else '/data/admin-fixture')
binary = Path(os.environ.get('LAVIK_BIN_DIR', '/build'))
root.mkdir(mode=0o700)  # Never reuse or initialize an existing population.
(root / 'bootstrap.pid').write_text(str(os.getpid()))
processes = []


def launch(name, args):
    """Retain process IDs and logs for diagnosis and scoped cleanup."""
    with (root / f'{name}.log').open('wb') as output:
        process = subprocess.Popen([str(arg) for arg in args], stdout=output, stderr=subprocess.STDOUT)
    processes.append(process)
    (root / f'{name}.pid').write_text(str(process.pid))


def ctl(*args):
    return subprocess.run([str(binary / 'lavik-ctl'), *map(str, args)], text=True, capture_output=True, timeout=15)


def setup(name, meta_base, data_base, meta_count, groups, replicas, initialize=True):
    """Build a manifest and matching running nodes; leave one spare unassigned."""
    directory = root / name
    directory.mkdir(mode=0o700)
    count = groups * (replicas + 1)
    identities = [f'{data_base + i:040x}' for i in range(count + 1)]
    manifest = ['schema_version = 1', 'slot_strategy = "contiguous-even"']
    for i in range(meta_count):
        manifest += ['[[meta_members]]', f'id = {i + 1}',
                     f'raft_endpoint = "tcp://127.0.0.1:{meta_base + i}"',
                     f'ctl_endpoint = "tcp://127.0.0.1:{meta_base + 100 + i}"',
                     f'data_control_endpoint = "tcp://127.0.0.1:{meta_base + 200 + i}"']
    for i in range(count):
        manifest += ['[[data_nodes]]', f'id = "{identities[i]}"',
                     f'client_endpoint = "tcp://127.0.0.1:{data_base + i}"']
    for i in range(groups):
        group_ids = identities[i * (replicas + 1):(i + 1) * (replicas + 1)]
        manifest += ['[[groups]]', f'id = "group-{i + 1}"', f'primary = "{group_ids[0]}"',
                     f'replicas = {json.dumps(group_ids[1:])}']
    manifest_path = directory / 'cluster.toml'
    manifest_path.write_text('\n'.join(manifest) + '\n')
    for i in range(meta_count):
        data_dir = directory / f'meta-{i + 1}'
        data_dir.mkdir(mode=0o700)
        launch(f'{name}-meta-{i + 1}', [binary / 'lavik-meta', '--id', i + 1,
               '--addr', f'127.0.0.1:{meta_base + i}', '--ctl-addr', f'127.0.0.1:{meta_base + 100 + i}',
               '--data-control-addr', f'127.0.0.1:{meta_base + 200 + i}', '--data-dir', data_dir,
               '--initial-cluster-manifest', manifest_path])
    for i in range(count + 1):
        data_dir = directory / f'data-{i + 1}'
        data_dir.mkdir(mode=0o700)
        data_file = data_dir / 'lavik.data'
        subprocess.run(['fallocate', '-l', '1G', str(data_file)], check=True)
        args = [binary / 'lavik', '--bind', '127.0.0.1', '--port', data_base + i,
                '--threads', 1, '--no-pin-workers', '--client-mode', 'cluster', '--meta-managed', 'yes',
                '--node-id', identities[i], '--announce-ip', '127.0.0.1',
                '--data-file', data_file, '--log-dir', data_dir / 'logs']
        for m in range(meta_count):
            args += ['--meta-seed', f'127.0.0.1:{meta_base + 200 + m}']
        launch(f'{name}-data-{i + 1}', args)
    seed = f'127.0.0.1:{meta_base + 100}'
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if any(process.poll() is not None for process in processes):
            raise RuntimeError('A fixture process exited; inspect its retained log')
        result = ctl('cluster-status', '--addr', seed, '--allow-plaintext-admin', '--json')
        if result.stdout:
            status = json.loads(result.stdout)
            if status.get('cluster_state') == 'uninitialized':
                break
        time.sleep(.5)
    else:
        raise RuntimeError(f'{name}: Meta did not become available: {result.stderr}')
    if not initialize:
        return dict(id=name, seed=seed, manifest=str(manifest_path))
    result = ctl('cluster-create', '--addr', seed, '--allow-plaintext-admin', '--manifest', manifest_path, '--yes')
    if result.returncode:
        raise RuntimeError(f'{name}: create failed: {result.stdout} {result.stderr}')
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        result = ctl('cluster-status', '--addr', seed, '--allow-plaintext-admin', '--json')
        if result.returncode == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError(f'{name}: did not become ready: {result.stdout} {result.stderr}')
    print(f'{name}: ready ({groups} groups, {count} data nodes, {meta_count} Meta members)', flush=True)
    return dict(id=name, seed=seed, primary_port=data_base, spare=identities[-1],
                spare_endpoint=f'tcp://127.0.0.1:{data_base + count}', manifest=str(manifest_path))


try:
    fixtures = ([setup('gamma', 8900, 6600, 1, 1, 0, initialize=False)]
                if os.environ.get('LAVIK_ADMIN_FIXTURE_EMPTY') == '1' else
                [setup('alpha', 8100, 6400, 3, 2, 1), setup('beta', 8500, 6500, 1, 1, 0)])
    (root / 'fixtures.json').write_text(json.dumps(fixtures, indent=2))
    print(json.dumps(fixtures), flush=True)
except BaseException:
    for process in processes:
        process.terminate()
    raise
