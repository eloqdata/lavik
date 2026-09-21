#!/usr/bin/env python3
"""DPDK-only control connect and service placement over an owned temporary TAP."""
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time

tap = 'bycorfdp0'


def skip(reason):
    print(f'SKIP: {reason}', flush=True)
    sys.exit(77)


# A bypass build can run under an unprivileged CTest runner. Check prerequisites
# before starting EAL or creating the TAP; real failures after startup must fail.
if sys.platform != 'linux':
    skip('DPDK control smoke requires Linux')
if shutil.which('ip') is None:
    skip('DPDK control smoke requires iproute2')
if not os.access('/dev/net/tun', os.R_OK | os.W_OK):
    skip('DPDK control smoke requires access to /dev/net/tun')
caps = next(line.split()[1] for line in Path('/proc/self/status').read_text().splitlines()
            if line.startswith('CapEff:'))
required_caps = (1 << 12) | (1 << 13)  # CAP_NET_ADMIN and CAP_NET_RAW.
if int(caps, 16) & required_caps != required_caps:
    skip('DPDK control smoke requires CAP_NET_ADMIN and CAP_NET_RAW')
if Path('/sys/class/net', tap).exists():
    skip(f'refusing to alter existing TAP {tap}')

env = {k: v for k, v in os.environ.items() if not k.startswith('BYCORF_')}
env.update(BYCORF_DPDK_MODE='adaptive', BYCORF_DPDK_QUEUES='1',
           BYCORF_DPDK_RX_STEERING='hash', BYCORF_DPDK_IP='198.18.0.2',
           BYCORF_DPDK_NETMASK='255.255.255.0')
with tempfile.TemporaryDirectory(prefix='lavik-dpdk-control-') as directory:
    path = Path(directory, 'server.log')
    with path.open('w') as log:
        child = subprocess.Popen([sys.argv[1]], stdout=log, stderr=subprocess.STDOUT, env=env)
        try:
            deadline = time.monotonic() + 30
            while path.read_text().count('bycorf0: Ethernet address:') < 3:
                assert child.poll() is None, path.read_text()
                assert time.monotonic() < deadline, path.read_text()
                time.sleep(.05)
            subprocess.run(['ip', 'link', 'set', tap, 'address', '02:00:00:00:00:01'], check=True)
            subprocess.run(['ip', 'addr', 'add', '198.18.0.1/24', 'dev', tap], check=True)
            with socket.socket() as listener:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(('198.18.0.1', 16405))
                listener.listen()
                listener.settimeout(20)
                control, peer = listener.accept()
                with control:
                    control.settimeout(15)
                    owners = set()
                    for _ in range(64):
                        with socket.create_connection(('198.18.0.2', 16404), 3) as client:
                            owner = client.recv(1)
                            assert owner in (b'\0', b'\1'), owner
                            owners.add(owner)
                    assert owners == {b'\0', b'\1'}, owners
                    payload = b''
                    while len(payload) < 4096:
                        piece = control.recv(4096 - len(payload))
                        assert piece
                        payload += piece
                    control.sendall(payload)
            assert child.wait(timeout=15) == 0, path.read_text()
            assert 'PASS DPDK control' in path.read_text(), path.read_text()
            print('PASS: Redis listeners only on workers 0/1; outbound DPDK control on worker 2')
        finally:
            if child.poll() is None:
                child.terminate()
                try: child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            print(path.read_text())
