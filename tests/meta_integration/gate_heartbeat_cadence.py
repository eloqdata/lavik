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

"""An Ack's transit time must count toward the heartbeat interval."""

import os
from pathlib import Path
import select
import socket
import struct
import sys
import tempfile
import threading
import time

import gate_cluster_create as C
import harness as H
from gate_data_control import DataProcess
from gate_native_replication import Client


class AckDelay(H.Proxy):
    """Delay only Acks, preserving every byte and frame order."""

    def __init__(self, port):
        super().__init__("ack-delay", port)
        self.delay = 0.0
        self.delayed = threading.Event()
        self.errors = []

    def _pump(self, src, dst, pair):
        src.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        dst.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if src is pair[0]:
            return super()._pump(src, dst, pair)

        def exact(size):
            data = bytearray()
            while len(data) < size:
                chunk = src.recv(size - len(data))
                if not chunk:
                    raise OSError("control stream ended")
                data.extend(chunk)
            return data

        try:
            while self._running:
                header = exact(28)
                magic, version, kind = struct.unpack_from(">IHH", header)
                size = struct.unpack_from(">I", header, 12)[0]
                assert magic == 0x4C564350 and version == 1 and size <= 16384
                frame = header + exact(size)
                if kind == 9 and self.delay:
                    self.delayed.set()
                    time.sleep(self.delay)
                dst.sendall(frame)
        except OSError:
            pass
        except BaseException as error:
            self.errors.append(repr(error))
        finally:
            self._cut_pair(pair)


def run(root):
    (root / "meta").mkdir()
    meta = H.Node(
        C.META,
        str(root / "meta"),
        1,
        args=H.raft_args(
            snapshot_distance=100000, election_ms_low=2000, election_ms_high=4000
        ),
    )
    proxy = AckDelay(meta.data_control_port)
    meta.advertised_data_control_endpoint = proxy.endpoint
    data = DataProcess(C.DATA, str(root / "data"), C.DATA_NODE, proxy.endpoint)
    manifest = root / "cluster.toml"
    C.write_manifest(manifest, data.advertised_endpoint, meta)
    client = None
    expires = "lavik_cluster_control_lease_expirations_total"
    grants = 'lavik_cluster_control_lease_decisions_total{decision="granted"}'
    try:
        proxy.start()
        meta.start(initial_cluster_manifest=str(manifest))
        meta.wait_leader()
        data.start()
        C.command(
            os.environ.copy(),
            [
                C.CTL,
                "cluster-create",
                "--manifest",
                str(manifest),
                "--socket",
                meta.ctl_path,
                "--yes",
            ],
        )
        C.wait_cluster_ready(meta, "initial owner", 60)
        assert meta.put_authority_lease_policy(2, 500).startswith("OK")
        C.wait_cluster_ready(meta, "500 ms policy installed", 15)
        before = data.metric(grants)
        data.wait_metric(grants, lambda n: n >= before + 4, "short grants")
        baseline = data.metric(expires)
        before = data.metric(grants)
        client = Client(data)
        # 200 + 166 + 200 exceeds 500 ms if sleep starts at Ack receipt;
        # send-anchored cadence leaves time for the next response instead.
        proxy.delay = 0.2
        assert proxy.delayed.wait(3)
        deadline = time.monotonic() + 5
        writes = 0
        while time.monotonic() < deadline:
            assert client.call("SET", "{cadence}key", writes, "PX", 10000) == "OK"
            assert client.call("GET", "{cadence}key") == str(writes)
            writes += 1
            time.sleep(0.01)
        assert data.metric(expires) == baseline, "Ack latency exhausted renewal budget"
        assert data.metric(grants) >= before + 10, "renewal stalled"
        assert not proxy.errors, proxy.errors
        H.log(f"PASS: {writes} write/read pairs with 200 ms Ack delay and 500 ms lease")
        # Scheduling earlier must not change the finite challenge-based lease.
        proxy.delay = 0.7
        data.wait_metric(
            expires,
            lambda n: n > baseline,
            "late Acks must expire authority",
            timeout=3,
        )
        # The lease boundary retires the old socket. A new diagnostic socket
        # must still receive the ordinary admission error while fenced.
        assert select.select([client.socket], [], [], 5)[0]
        assert client.reader.read(1) == b""
        client.close()
        client = Client(data)
        try:
            client.call("SET", "{cadence}must-fence", "invalid")
        except H.Failure as error:
            assert "CLUSTERDOWN" in str(error), error
        else:
            raise AssertionError("an Ack received after expiry prolonged authority")
        H.log("PASS: Ack delay beyond the lease still fences writes")
        proxy.delay = 0
        client.close()
        client = None
        data.terminate()
        meta.terminate()
    except BaseException:
        H.dump_node_logs([meta])
        print(data.log_tail(lines=80), file=sys.stderr)
        raise
    finally:
        if client:
            client.close()
        proxy.delay = 0
        data.force_kill()
        meta.force_kill()
        proxy.close()


if __name__ == "__main__":
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    with tempfile.TemporaryDirectory(
        prefix="lavik-cadence-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        run(Path(directory))
