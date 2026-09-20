#!/usr/bin/env python3
"""DPDK-only control connect and service placement over an owned temporary TAP."""
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

tap = 'bycorfdp0'
assert not Path('/sys/class/net', tap).exists(), 'refusing to alter an existing TAP'
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
