#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A suspended directive submission must not block the control reader."""

import os
import json
from pathlib import Path
import socket
import signal
import struct
import sys
import tempfile
import threading
import time

import gate_cluster_create as C
import harness as H
from gate_data_control import DataProcess
from gate_lease_policy import crc32c

HOLD = "LAVIK_TEST_META_DIRECTIVE_RESULT_HOLD_FILE"


class ResultProxy(H.Proxy):
    def __init__(self, port, copies=1, conflict=False):
        super().__init__("directive-results", port)
        self.copies = copies
        self.conflict = conflict
        self.events = []
        self.errors = []
        self.result_pair = None
        self.result_seen = threading.Event()
        self.release_copies = threading.Event()
        self.closed = threading.Event()

    def snapshot(self):
        with self._lock:
            return list(self.events)

    def _pump(self, src, dst, pair):
        upstream = src is pair[0]
        src.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        dst.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sequence = 0

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
                copies = 1
                if upstream and kind == 14 and not self.result_seen.is_set():
                    self.result_pair = pair
                    copies = self.copies
                    self.result_seen.set()
                for copy in range(copies):
                    if copy == 1:
                        # First prove that the front item is in flight. Without
                        # this barrier a fast reader could fill the queue before
                        # its consumer starts, missing the intended fault cut.
                        assert self.release_copies.wait(
                            10
                        ), "duplicate barrier timed out"
                    if copy == 1 and self.conflict:
                        # session(16), boot(40), assignment(16), identity(56).
                        frame[28 + 128] = 2  # Failed instead of Succeeded
                    # Duplication must preserve the transport's exact sequence.
                    sequence += 1
                    struct.pack_into(">Q", frame, 16, sequence)
                    struct.pack_into(">I", frame, 24, 0)
                    struct.pack_into(">I", frame, 24, crc32c(frame))
                    dst.sendall(frame)
                    with self._lock:
                        self.events.append(
                            (pair, upstream, kind, time.monotonic(), bytes(frame[28:]))
                        )
        except OSError:
            pass
        except BaseException as error:
            self.errors.append(repr(error))
        finally:
            if pair is self.result_pair:
                self.closed.set()
            self._cut_pair(pair)


def run(root, scenario="progress"):
    root.mkdir(parents=True, exist_ok=True)
    (root / "meta").mkdir()
    meta = H.Node(C.META, str(root / "meta"), 1, args=C.creation_raft_args())
    proxy = ResultProxy(
        meta.data_control_port,
        copies=(
            17  # Includes the held front item in the 16-result session limit.
            if scenario == "overflow"
            else 2 if scenario in ("progress", "conflict") else 1
        ),
        conflict=scenario == "conflict",
    )
    meta.advertised_data_control_endpoint = proxy.endpoint
    nodes = [meta]
    proxies = {meta.id: proxy}
    if scenario == "demotion":
        for node_id in (2, 3):
            node = H.Node(
                C.META, str(root / "meta"), node_id, args=C.creation_raft_args()
            )
            node_proxy = ResultProxy(node.data_control_port)
            node.advertised_data_control_endpoint = node_proxy.endpoint
            nodes.append(node)
            proxies[node_id] = node_proxy
    data = DataProcess(C.DATA, str(root / "data"), C.DATA_NODE, proxy.endpoint)
    manifest = root / "cluster.toml"
    C.write_manifest(manifest, data.advertised_endpoint, nodes)
    hold = root / "hold"
    hold.touch()
    previous = os.environ.get(HOLD)
    try:
        for node_proxy in proxies.values():
            node_proxy.start()
        os.environ[HOLD] = str(hold)
        for node in nodes:
            node.start(initial_cluster_manifest=str(manifest))
        if previous is None:
            os.environ.pop(HOLD)
        else:
            os.environ[HOLD] = previous
        meta = H.find_leader(nodes, timeout=15)
        proxy = proxies[meta.id]
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
        H.wait_until(
            "result submission paused",
            20,
            lambda: f"fault pause reached: {HOLD}" in Path(meta.log_path).read_text(),
        )
        assert proxy.result_seen.is_set()
        proxy.release_copies.set()
        baseline = time.monotonic()
        if scenario == "progress":

            def received(kind, upstream=False):
                return [
                    e
                    for e in proxy.snapshot()
                    if e[0] is proxy.result_pair
                    and e[1] == upstream
                    and e[2] == kind
                    and e[3] > baseline
                ]

            H.wait_until(
                "heartbeats while result submission is held",
                3,
                lambda: len(received(9)) >= 2,
            )
            # An unrelated commit publishes an FDS; its Applied must be read
            # and adopted before subsequent heartbeats can continue.
            assert meta.put_authority_lease_policy(2, 250).startswith("OK")
            H.wait_until("Applied during held submission", 3, lambda: received(7, True))
            applied = received(7, True)[-1][3]
            H.wait_until(
                "heartbeat after Applied",
                3,
                lambda: any(e[3] > applied for e in received(9)),
            )
            assert not proxy.closed.is_set(), "original session was replaced"
            assert not any(
                e[2] == 15 for e in proxy.snapshot()
            ), "ack before submission"
            H.log(
                "PASS: same-session heartbeat and Applied progress with submission held; no early ack"
            )
            assert meta.put_authority_lease_policy(3, 500).startswith("OK")
        elif scenario == "overflow":
            assert proxy.closed.wait(3), "queue overflow did not close the session"
            assert not any(
                e[2] == 15 for e in proxy.snapshot()
            ), "overflow acknowledged uncommitted result"
            H.log("PASS: full result queue closes without waiting for proposal")
        elif scenario in ("disconnect", "shutdown"):
            if scenario == "disconnect":
                proxy._cut_pair(proxy.result_pair)
            else:
                meta.proc.send_signal(signal.SIGTERM)
            assert proxy.closed.wait(
                3
            ), "pending submission prevented transport retirement"
            assert not any(
                e[2] == 15 for e in proxy.snapshot()
            ), "closed session acknowledged pending result"
        if scenario == "demotion":
            followers = [node for node in nodes if node is not meta]
            for node in followers:
                node.pause()
            # Release into the real Raft proposal seam without a quorum.
            # The old term must retire transport and join that proposal.
            hold.unlink()
            assert proxy.closed.wait(
                6
            ), "demotion did not retire pending result session"
            H.wait_until("old result owner demoted", 6, lambda: not meta.is_leader())
            assert not any(
                e[2] == 15 for e in proxy.snapshot()
            ), "minority acknowledged result"
            for node in followers:
                node.resume()
            meta = H.find_leader(nodes, timeout=15)
            H.log("PASS: lost quorum drains pending result; old session sends no ack")
        else:
            hold.unlink()
        if scenario == "shutdown":
            assert (
                meta.proc.wait(timeout=10) == 0
            ), "shutdown did not join pending result task"
            meta.start()
            meta.wait_leader()
        if scenario == "conflict":
            assert proxy.closed.wait(5), "conflicting result did not close session"
            assert (
                len([e for e in proxy.snapshot() if e[2] == 15]) == 1
            ), "conflicting result was acknowledged"
            H.log(
                "PASS: conflicting queued duplicate closes session after original receipt commits"
            )
        C.wait_cluster_ready(meta, "result replay completed", 30)
        if scenario in ("progress", "overflow", "conflict"):
            H.wait_until(
                "committed result ack",
                5,
                lambda: any(e[2] == 15 for p in proxies.values() for e in p.snapshot()),
            )
        else:
            # Disconnect can race an already accepted append. Reconciliation
            # may remove that directive before Data reconnects, so READY is
            # the durable completion check; an Ack on the old socket is not
            # required (and must never be fabricated for the new session).
            assert any(
                e[0] is not proxy.result_pair and e[1] and e[2] == 7
                for p in proxies.values()
                for e in p.snapshot()
            )
            H.log("PASS: replacement session adopts committed state and reaches READY")
        if scenario == "progress":
            H.wait_until(
                "duplicate result ack",
                5,
                lambda: len([e for e in proxy.snapshot() if e[2] == 15]) >= 2,
            )
            acks = [e[4] for e in proxy.snapshot() if e[2] == 15]
            assert (
                acks[0] == acks[1]
            ), "duplicate must return exact original receipt/index"
            assert struct.unpack_from(">Q", acks[0], len(acks[0]) - 8)[0] > 0
            H.log("PASS: duplicate returns identical durable receipt and commit index")
        for node_proxy in proxies.values():
            assert not node_proxy.errors, node_proxy.errors
        data.terminate()
        for node in nodes:
            node.terminate()
    except BaseException:
        H.dump_node_logs(nodes)
        print(data.log_tail(lines=50), file=sys.stderr)
        raise
    finally:
        hold.unlink(missing_ok=True)
        if previous is None:
            os.environ.pop(HOLD, None)
        else:
            os.environ[HOLD] = previous
        data.force_kill()
        for node in nodes:
            node.force_kill()
        for node_proxy in proxies.values():
            node_proxy.release_copies.set()
            node_proxy.close()
        pairs = {}
        trace = []
        for pair, upstream, kind, timestamp, payload in sorted(
            (e for p in proxies.values() for e in p.snapshot()), key=lambda e: e[3]
        ):
            connection = pairs.setdefault(id(pair), len(pairs))
            trace.append(
                dict(
                    connection=connection,
                    upstream=upstream,
                    kind=kind,
                    time=timestamp,
                    ack=payload.hex() if kind == 15 else None,
                )
            )
        (root / "trace.json").write_text(json.dumps(trace, indent=2))


if __name__ == "__main__":
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    if not C.has_fault(C.META, HOLD.encode()):
        H.log("SKIP: directive-result fault hook is disabled")
        sys.exit(77)
    if len(sys.argv) > 5:
        run(Path(sys.argv[5]), sys.argv[6] if len(sys.argv) > 6 else "progress")
    else:
        with tempfile.TemporaryDirectory(
            prefix="lavik-results-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
        ) as directory:
            for scenario in (
                "progress",
                "overflow",
                "conflict",
                "disconnect",
                "shutdown",
                "demotion",
            ):
                run(Path(directory) / scenario, scenario)
