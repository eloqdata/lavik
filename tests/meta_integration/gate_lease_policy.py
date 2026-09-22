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

"""Lease policy publication must not interrupt unchanged owner authority.

Delay a complete control update for four old lease windows while forwarding
heartbeats and their Acks. Only transport sequences/CRCs are regenerated; all
business identities, decisions, payloads and per-object ordering are retained.
This separates slow policy publication from an actual disconnected leader.
Then exercise two primary/replica pairs sharing one Meta worker to cover
publication CPU cost across concurrent sessions without a forwarding proxy.
"""

import os
from pathlib import Path
import select
import struct
import sys
import tempfile
import threading
import time

import gate_cluster_create as C
import harness as H
from gate_data_control import DataProcess
from gate_native_replication import Client


def crc_table():
    result = []
    for value in range(256):
        crc = value
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
        result.append(crc)
    return result


CRC_TABLE = crc_table()


def crc32c(data):
    crc = 0xFFFFFFFF
    for value in data:
        crc = CRC_TABLE[(crc ^ value) & 255] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


class PolicyBarrier(H.Proxy):
    def __init__(self, port):
        super().__init__("lease-policy", port)
        self.armed = threading.Event()
        self.held = threading.Event()
        self.release = threading.Event()
        self.errors = []
        self.trace = []

    def _pump(self, src, dst, pair):
        if src is pair[0]:
            return super()._pump(src, dst, pair)
        sequence = 0
        pending = []

        def exact(size):
            data = b""
            while len(data) < size:
                chunk = src.recv(size - len(data))
                if not chunk:
                    raise OSError("control stream ended")
                data += chunk
            return data

        def send(frame):
            nonlocal sequence
            sequence += 1
            frame = bytearray(frame)
            struct.pack_into(">Q", frame, 16, sequence)
            struct.pack_into(">I", frame, 24, 0)
            struct.pack_into(">I", frame, 24, crc32c(frame))
            dst.sendall(frame)
            kind = struct.unpack_from(">H", frame, 6)[0]
            detail = ""
            if kind == 9:
                payload = frame[28:]
                offset = 29 + struct.unpack_from(">I", payload, 25)[0]
                decision = payload[offset]
                detail = f"decision={decision} " + (
                    f"duration={struct.unpack_from('>I', payload, len(payload) - 4)[0]}"
                    if decision == 1
                    else ""
                )
            self.trace.append((time.time(), kind, detail))

        try:
            while self._running:
                if pending and self.release.is_set():
                    for frame in pending:
                        send(frame)
                    pending.clear()
                if not select.select([src], [], [], 0.01)[0]:
                    continue
                header = exact(28)
                magic, version, kind = struct.unpack_from(">IHH", header)
                size = struct.unpack_from(">I", header, 12)[0]
                assert magic == 0x4C564350 and version == 1 and size <= 16384
                frame = header + exact(size)
                if (
                    self.armed.is_set()
                    and not self.release.is_set()
                    and kind in (3, 4, 5, 19)
                ):
                    pending.append(frame)
                    self.held.set()
                else:
                    send(frame)
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
    proxy = PolicyBarrier(meta.data_control_port)
    meta.advertised_data_control_endpoint = proxy.endpoint
    data = DataProcess(
        C.DATA, str(root / "data"), C.DATA_NODE, proxy.endpoint, workers=2
    )
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
        C.wait_cluster_ready(meta, "initial owner authority", 60)
        client = Client(data)
        assert (
            client.call("SET", "{policy}large", b"x" * (2 * 1024 * 1024), "PX", 120000)
            == "OK"
        )
        assert meta.put_authority_lease_policy(2, 300).startswith("OK")
        C.wait_cluster_ready(meta, "short lease installed", 15)
        # Require sustained short grants, not merely one transient READY poll.
        first = data.metric(grants)
        data.wait_metric(grants, lambda n: n >= first + 5, "short lease renewals")
        baseline = data.metric(expires)
        before = data.metric(grants)
        proxy.armed.set()
        assert meta.put_authority_lease_policy(3, 2000).startswith("OK")
        assert proxy.held.wait(5), "policy update did not reach the barrier"
        end = time.monotonic() + 1.2
        writes = 0
        while time.monotonic() < end:
            assert client.call("SET", "{policy}sentinel", writes, "PX", 10000) == "OK"
            assert client.call("GET", "{policy}sentinel") == str(writes)
            writes += 1
            time.sleep(0.01)
        assert data.metric(grants) >= before + 4, (
            "old projection stopped renewing during publication"
        )
        assert data.metric(expires) == baseline, (
            "lease expired while only policy publication was delayed"
        )
        proxy.release.set()
        C.wait_cluster_ready(meta, "long policy applied", 15)
        before = data.metric(grants)
        data.wait_metric(grants, lambda n: n >= before + 3, "long lease renewals")
        # Exercise both directions and waking the prior long heartbeat sleep.
        for version in range(4, 12):
            duration = 300 if version % 2 == 0 else 2000
            H.log(f"policy version={version} duration={duration} at {time.time()}")
            assert meta.put_authority_lease_policy(version, duration).startswith("OK")
            C.wait_cluster_ready(meta, "policy round trip", 15)
            before = data.metric(grants)
            data.wait_metric(
                grants, lambda n: n >= before + 2, "renewals after policy change"
            )
            assert client.call("STRLEN", "{policy}large") == 2 * 1024 * 1024
            assert data.metric(expires) == baseline, (
                "policy round trip expired authority"
            )
        assert not proxy.errors, proxy.errors
        H.log(
            f"PASS: {writes} writes across delayed publication; 8 policy changes; no expiry"
        )

        # A later incompatible commit must invalidate the bridge even while
        # the publisher is waiting for the previous update's Applied receipt.
        assert meta.put_authority_lease_policy(12, 300).startswith("OK")
        C.wait_cluster_ready(meta, "short lease before bridge invalidation", 15)
        before = data.metric(grants)
        data.wait_metric(
            grants, lambda n: n >= before + 3, "short grants before invalidation"
        )
        assert data.metric(expires) == baseline
        proxy.held.clear()
        proxy.release.clear()
        assert meta.put_authority_lease_policy(13, 2000).startswith("OK")
        assert proxy.held.wait(5)
        before = data.metric(grants)
        data.wait_metric(grants, lambda n: n >= before + 4, "compatible bridge renewed")
        assert meta.put_authority_lease_policy(14, 150).startswith("OK")
        data.wait_metric(
            expires,
            lambda n: n == baseline + 1,
            "shorter committed policy invalidates old renewal proof",
            timeout=5,
        )
        try:
            client.call("SET", "{policy}must-fence", "invalid")
        except H.Failure as error:
            assert "CLUSTERDOWN" in str(error), error
        else:
            raise AssertionError("old authority survived a shorter committed policy")
        proxy.release.set()
        C.wait_cluster_ready(
            meta, "latest policy recovers after delayed publication", 15
        )
        assert client.call("SET", "{policy}recovered", "valid") == "OK"
        assert not proxy.errors, proxy.errors
        H.log(
            "PASS: a newer incompatible commit revokes the bridge and recovery succeeds"
        )
        client.close()
        client = None
        data.terminate()
        meta.terminate()
    except BaseException:
        print("Control frame trace:", proxy.trace[-100:], file=sys.stderr)
        H.dump_node_logs([meta])
        print(data.log_tail(lines=120), file=sys.stderr)
        raise
    finally:
        proxy.release.set()
        if client:
            client.close()
        data.force_kill()
        meta.force_kill()
        proxy.close()


def run_multi(root):
    """Four publishers share one Meta worker; no proxy masks their CPU cost."""
    root.mkdir()
    (root / "meta").mkdir()
    meta = H.Node(C.META, str(root / "meta"), 1, args=C.creation_raft_args())
    nodes = [
        DataProcess(C.DATA, str(root / name), node_id, meta.data_control_endpoint)
        for name, node_id in (
            ("primary1", C.PRIMARY_1),
            ("replica1", C.REPLICA_1),
            ("primary2", C.PRIMARY_2),
            ("replica2", C.REPLICA_2),
        )
    ]
    manifest = root / "cluster.toml"
    C.write_multi_manifest(manifest, nodes, True, meta)
    clients = []
    expires = "lavik_cluster_control_lease_expirations_total"
    try:
        meta.start(initial_cluster_manifest=str(manifest))
        meta.wait_leader()
        for node in nodes:
            node.start()
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
        C.wait_cluster_ready(meta, "four-node cluster ready", 90)
        clients = [Client(nodes[0]), Client(nodes[2])]
        keys = [
            C.key_in_range("policy-multi-1", 0, 8191),
            C.key_in_range("policy-multi-2", 8192, 16383),
        ]
        baseline = [node.metric(expires) for node in nodes]
        for version in range(2, 10):
            duration = 300 if version % 2 == 0 else 2000
            assert meta.put_authority_lease_policy(version, duration).startswith("OK")
            # Keep checking actual write authority throughout publication,
            # including the intervals hidden by a final READY-only assertion.
            deadline = time.monotonic() + 1.0
            count = 0
            while time.monotonic() < deadline:
                for client, key in zip(clients, keys):
                    value = f"{version}:{count}"
                    assert client.call("SET", key, value, "PX", 10000) == "OK"
                    assert client.call("GET", key) == value
                count += 1
                time.sleep(0.01)
            C.wait_cluster_ready(meta, "four-node policy applied", 15)
            assert [node.metric(expires) for node in nodes] == baseline
            H.log(
                f"PASS: four-node policy version={version} duration={duration}, "
                f"{count} writes per primary"
            )
        for client in clients:
            client.close()
        clients.clear()
        for node in reversed(nodes):
            node.terminate()
        meta.terminate()
    except BaseException:
        H.dump_node_logs([meta])
        for node in nodes:
            print(node.log_tail(lines=100), file=sys.stderr)
        raise
    finally:
        for client in clients:
            client.close()
        for node in nodes:
            node.force_kill()
        meta.force_kill()


if __name__ == "__main__":
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    with tempfile.TemporaryDirectory(
        prefix="lavik-policy-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        run(Path(directory))
        run_multi(Path(directory) / "multi")
