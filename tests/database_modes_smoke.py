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

"""Verify DB0-only cluster recovery and standalone DB15 across mode changes."""

from contextlib import contextmanager, ExitStack
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import struct
import time

sys.path.insert(0, str(Path(__file__).parent / "meta_integration"))
import gate_failover as F
import harness as H
import gate_cluster_create as C


class BootstrapOnly(H.Proxy):
    """Use real committed bootstrap without authorizing population replacement."""

    def _pump(self, src, dst, pair):
        if src is pair[0]:
            prefix = bytearray()
            try:
                while len(prefix) < 28:
                    part = src.recv(28 - len(prefix))
                    if not part:
                        return
                    prefix.extend(part)
                _, _, kind = struct.unpack_from(">IHH", prefix)
                if kind != 20:
                    self._cut_pair(pair)
                    return
                dst.sendall(prefix)
            except OSError:
                self._cut_pair(pair)
                return
        super()._pump(src, dst, pair)


@contextmanager
def mode_source(directory, label, endpoint):
    (directory / (label + "-meta")).mkdir()
    meta = H.Node(
        C.META, str(directory / (label + "-meta")), 1, args=C.creation_raft_args()
    )
    proxy = BootstrapOnly("mode-bootstrap", meta.data_control_port)
    meta.advertised_data_control_endpoint = proxy.endpoint
    try:
        proxy.start()
        meta.start(bootstrap=True)
        meta.wait_leader()
        # Election can finish before the bootstrap Meta identity binding.
        # Cluster creation shares admission with that initial reconciliation.
        H.wait_until(
            "bootstrap Meta identity committed",
            5,
            lambda: C.cluster_status(meta)["meta_membership_stable"],
        )
        request = C.create_request(meta, "1" * 40, endpoint, "group")
        reply = meta.ctl(request)
        assert reply.startswith("OK clustercreate"), reply
        yield proxy.endpoint
    finally:
        proxy.close()
        meta.force_kill()


@contextmanager
def server(
    binary, directory, label, *, cluster=False, checkpoint=False, rejected=False
):
    port = H.free_port()
    log_path = directory / f"{label}.log"
    args = [
        binary,
        "--bind",
        "127.0.0.1",
        "--port",
        str(port),
        "--metrics-port",
        "0",
        "--threads",
        "2",
        "--no-pin-workers",
        "--recv-buffers-per-worker",
        "0",
        "--registered-buffer-mb-per-worker",
        "64",
        "--max-memory",
        "1G",
        "--data-file",
        str(directory / "lavik.data"),
        "--rdb-dir",
        str(directory),
        "--logtostderr",
    ]
    if checkpoint:
        args.append("--shutdown-checkpoint")
    with ExitStack() as stack:
        if cluster:
            seed = stack.enter_context(
                mode_source(directory, label, f"tcp://127.0.0.1:{port}")
            )
            args += [
                "--node-id",
                "1" * 40,
                "--meta-seed",
                seed,
                "--announce-ip",
                "127.0.0.1",
            ]
        log = stack.enter_context(log_path.open("w"))
        process = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
        connection = None
        try:
            if rejected:
                assert process.wait(timeout=20) != 0, log_path.read_text()
                assert "disabled database" in log_path.read_text(), log_path.read_text()
                yield None
                return
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                assert process.poll() is None, log_path.read_text()
                try:
                    connection = socket.create_connection(("127.0.0.1", port), 0.2)
                    break
                except OSError:
                    time.sleep(0.02)
            assert connection is not None, log_path.read_text()
            connection.settimeout(10)
            with connection.makefile("rb") as reader:

                def rpc(*args):
                    connection.sendall(F.encode_resp(list(args)))
                    return F.read_resp(reader)

                assert rpc("PING") == "PONG"
                yield rpc
            process.send_signal(signal.SIGINT)
            assert process.wait(timeout=20) == 0, log_path.read_text()
        finally:
            if connection is not None:
                connection.close()
            if process.poll() is None:
                process.kill()
                process.wait()


def exercise(binary, directory, checkpoint):
    directory.mkdir()
    with (directory / "lavik.data").open("wb") as data:
        os.posix_fallocate(data.fileno(), 0, 256 * 1024 * 1024)
    with server(binary, directory, "populate", checkpoint=checkpoint) as rpc:
        assert rpc("SET", "retained", "db0") == "OK"
        assert rpc("SELECT", "15") == "OK"
        assert rpc("SET", "retained", "db15") == "OK"
    # Both checkpoint and cold recovery must fail closed. The failed attempt
    # must leave the original standalone data recoverable.
    with server(
        binary, directory, "reject", cluster=True, checkpoint=checkpoint, rejected=True
    ):
        pass
    if checkpoint:
        assert "cannot preallocate indexes" in (directory / "reject.log").read_text()
    with server(binary, directory, "clear-disabled", checkpoint=checkpoint) as rpc:
        assert rpc("GET", "retained") == "db0"
        assert rpc("SELECT", "15") == "OK"
        assert rpc("GET", "retained") == "db15"
        assert rpc("FLUSHDB", "SYNC") == "OK"
    with server(
        binary, directory, "cluster", cluster=True, checkpoint=checkpoint
    ) as rpc:
        # No Meta authority is granted here. Keyspace introspection still
        # proves storage recovery finished and only DB0 is populated.
        info = rpc("INFO", "keyspace")
        assert "db0:keys=1" in info.splitlines(), info
        assert "db15:" not in info, info
        try:
            rpc("SELECT", "15")
        except H.Failure as error:
            assert "cluster mode" in str(error), str(error)
        else:
            raise AssertionError("cluster SELECT 15 was accepted")
    with server(binary, directory, "standalone-again", checkpoint=checkpoint) as rpc:
        assert rpc("GET", "retained") == "db0"
        assert rpc("SELECT", "15") == "OK"
        assert rpc("GET", "retained") is None
        assert rpc("SET", "new", "db15") == "OK"
        assert rpc("FLUSHALL", "SYNC") == "OK"
        assert rpc("DBSIZE") == 0
    if checkpoint:
        for label in ("cluster", "standalone-again"):
            log = (directory / f"{label}.log").read_text()
            assert "loaded shutdown checkpoint" in log, log


def main():
    C.META, C.CTL = map(os.path.abspath, sys.argv[2:4])
    with tempfile.TemporaryDirectory(
        prefix="lavik-database-modes-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as workdir:
        for checkpoint in (False, True):
            exercise(sys.argv[1], Path(workdir) / str(checkpoint), checkpoint)
    print("database mode recovery checks passed")


if __name__ == "__main__":
    main()
