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

"""Bootstrap waits, transport interruption, registration and no early Redis."""

import os
from pathlib import Path
import signal
import socket
import sys
import tempfile
import threading
import time

import gate_cluster_create as C
import harness as H
from gate_data_control import DataProcess, make_ca, make_leaf


def no_redis(data):
    try:
        with socket.create_connection(("127.0.0.1", data.redis_port), 0.1):
            raise AssertionError("Redis listened before committed mode")
    except OSError:
        pass
    assert "storage initialized" not in data.log_tail(200)


def stop_bootstrap(data):
    no_redis(data)
    started = time.monotonic()
    data.proc.send_signal(signal.SIGINT)
    assert data.proc.wait(timeout=3) == 0, data.log_tail()
    assert time.monotonic() - started < 3


def late_meta(root):
    scenario = root / "late"
    scenario.mkdir()
    meta = H.Node(C.META, str(scenario), 1, args=C.creation_raft_args())
    data = DataProcess(
        C.DATA, str(scenario / "data"), "2" * 40, meta.data_control_endpoint
    )
    try:
        data.start()
        no_redis(data)
        meta.start(bootstrap=True)
        meta.wait_leader()
        H.wait_until(
            "bootstrap sees uninitialized Meta",
            10,
            lambda: "waiting for leader, cluster creation or Data registration"
            in data.log_tail(),
        )
        no_redis(data)
        # Real committed creation supplies mode while the unrelated primary is
        # still offline. This Data identity remains deliberately unregistered.
        reply = meta.ctl(
            C.create_request(
                meta, "1" * 40, f"tcp://127.0.0.1:{H.free_port()}", "group"
            )
        )
        assert reply.startswith("OK clustercreate"), reply
        time.sleep(0.3)
        no_redis(data)
        reply = meta.registernode(
            data.node_id,
            "lavik://node/" + data.node_id,
            "replica",
            endpoints=(data.advertised_endpoint,),
        )
        assert reply.startswith("OK "), reply
        H.wait_until("registered Data starts before Created", 15, data._metrics_ready)
        # Metrics and Redis listeners start independently after registration.
        H.wait_until(
            "registered Data Redis listener starts before Created",
            15,
            lambda: data.command_head(["PING"]) == "+PONG",
        )
        assert data.command_head(["GET", "key"]).startswith("-LOADING")
        assert C.cluster_status(meta)["cluster_state"] == "creating"
        data.terminate()
        meta.terminate()
    finally:
        data.force_kill()
        meta.force_kill()


def cancel_transport(root, tls=False, partial=False):
    name = "tls" if tls else "partial" if partial else "read"
    scenario = root / name
    scenario.mkdir()
    accepted = threading.Event()
    stopping = threading.Event()
    peers = []
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(0.1)

        def stall():
            while not stopping.is_set():
                try:
                    peer, _ = listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                peers.append(peer)
                accepted.set()
                if partial:
                    peer.sendall(b"LVC")
                    peer.close()

        thread = threading.Thread(target=stall, daemon=True)
        thread.start()
        credentials = None
        if tls:
            ca, key = make_ca(str(scenario))
            cert, private = make_leaf(
                str(scenario), ca, key, "data", "lavik://node/" + "2" * 40
            )
            credentials = (ca, cert, private)
        data = DataProcess(
            C.DATA,
            str(scenario / "data"),
            "2" * 40,
            f"127.0.0.1:{listener.getsockname()[1]}",
            tls=credentials,
        )
        try:
            data.start(wait_ready=False)
            assert accepted.wait(5), data.log_tail()
            if partial:
                H.wait_until("partial reply reconnects", 5, lambda: len(peers) >= 2)
            stop_bootstrap(data)
        finally:
            data.force_kill()
            stopping.set()
            listener.close()
            for peer in peers:
                peer.close()
            thread.join(timeout=2)


def cancel_unavailable(root):
    data = DataProcess(
        C.DATA, str(root / "unavailable"), "2" * 40, f"127.0.0.1:{H.free_port()}"
    )
    try:
        data.start()
        stop_bootstrap(data)
    finally:
        data.force_kill()


def main():
    C.META, C.DATA, C.CTL = map(os.path.abspath, sys.argv[1:4])
    H.set_tag("bootstrap")
    with tempfile.TemporaryDirectory(
        prefix="lavik-bootstrap-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        root = Path(directory)
        late_meta(root)
        cancel_unavailable(root)
        cancel_transport(root)
        cancel_transport(root, tls=True)
        cancel_transport(root, partial=True)
    H.log("PASS")


if __name__ == "__main__":
    main()
