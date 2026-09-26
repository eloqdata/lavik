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

"""WAIT observes the local Group through ordinary managed Redis connections."""

import os
import concurrent.futures
import socket
from pathlib import Path
import sys
import tempfile

from gate_native_replication import C, H, Client, pair, ready, rejects
import gate_failover as F


def acknowledged(root, mode):
    with pair(root, mode, client_mode=mode) as (meta, source, target, writer):
        ready(meta)
        reader = Client(target, readonly=mode == "cluster")
        try:
            # Writes on both publisher workers and multiple logical databases
            # must be covered by one connection's WAIT, even after SELECT.
            keys = [C.key_in_range(f"wait-{i}", i, i) for i in range(2)]
            databases = (0, 15) if mode == "single" else (0,)
            for db in databases:
                assert writer.call("SELECT", db) == "OK"
                for key in keys:
                    assert writer.call("SET", key, f"db-{db}") == "OK"
            assert writer.call("SELECT", 0) == "OK"
            assert writer.call("WAIT", 1, 5000) == 1
            identifier = writer.call("CLIENT", "ID")
            for unblocking in ("TIMEOUT", "ERROR"):
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    pending = pool.submit(writer.call, "WAIT", 2, 0)

                    def unblock():
                        return (
                            F.redis_call(
                                source,
                                ["CLIENT", "UNBLOCK", str(identifier), unblocking],
                            )
                            == 1
                        )

                    H.wait_until("WAIT registered for CLIENT UNBLOCK", 5, unblock)
                    if unblocking == "TIMEOUT":
                        assert pending.result(timeout=5) == 1
                    else:
                        try:
                            pending.result(timeout=5)
                            raise AssertionError("UNBLOCK ERROR returned success")
                        except H.Failure as failure:
                            assert "UNBLOCKED" in str(failure), failure
            for db in databases:
                assert reader.call("SELECT", db) == "OK"
                for key in keys:
                    assert reader.call("GET", key) == f"db-{db}"
            rejects(reader, ("WAIT", 1, 1), "WAIT cannot be used with replica")
            # Redis checks replica role before parsing the two integer arguments.
            rejects(reader, ("WAIT", "invalid", 1), "WAIT cannot be used with replica")
            rejects(reader, ("WAIT", 1, -1), "WAIT cannot be used with replica")
            assert writer.call("WAIT", -1, 1) == 1
            rejects(writer, ("WAIT", 1, -1), "ERR timeout is negative")
            rejects(
                writer,
                ("WAIT", 1, "invalid"),
                "ERR timeout is not an integer or out of range",
            )
            rejects(writer, ("WAIT", "01", 1), "ERR value is not an integer")
            rejects(writer, ("WAIT", 1, "01"), "ERR timeout is not an integer")
            rejects(
                writer,
                ("WAIT", 0, 9223372036854775807),
                "ERR timeout is out of range",
            )
            assert writer.call("WAIT", 2, 20) == 1
            # A peer write-half-close cancels WAIT, but complete commands
            # already buffered after it must still receive their replies.
            pipelined = Client(source)
            try:
                pipelined.socket.sendall(
                    C.encode_resp(["WAIT", "2", "0"])
                    + C.encode_resp(["PING"])
                )
                pipelined.socket.shutdown(socket.SHUT_WR)
                try:
                    C.read_resp(pipelined.reader)
                    raise AssertionError("disconnected WAIT succeeded")
                except H.Failure as failure:
                    assert "WAIT interrupted" in str(failure), failure
                assert C.read_resp(pipelined.reader) == "PONG"
            finally:
                pipelined.close()
            target.pause()
            try:
                assert writer.call("SET", keys[0], "not-acknowledged") == "OK"
                assert writer.call("WAIT", 1, 30) == 0
            finally:
                target.resume()
            assert writer.call("WAIT", 1, 5000) == 1
        finally:
            reader.close()


def retirement_before_registration(root, mode, boundary="before"):
    hold = root / f"{mode}-{boundary}.hold"
    variable = "LAVIK_WAIT_BEFORE_REGISTER_HOLD_FILE"
    with pair(
        root,
        f"retire-{mode}-{boundary}",
        client_mode=mode,
        source_faults={variable: str(hold)},
    ) as (meta, source, target, writer):
        ready(meta)
        assert writer.call("SET", "wait-cut", "before") == "OK"
        assert writer.call("WAIT", 1, 5000) == 1
        if boundary != "blocked":
            hold.touch()
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(writer.call, "WAIT", 2, 0)
                if boundary == "blocked":
                    H.wait_until(
                        "WAIT blocked",
                        5,
                        lambda: "blocked_clients:1"
                        in F.redis_call(source, ["INFO", "clients"]).splitlines(),
                    )
                else:
                    H.wait_until(
                        "WAIT held before registration",
                        10,
                        lambda: variable in Path(source.log_path).read_text(),
                    )
                if boundary == "disconnect":
                    writer.socket.shutdown(socket.SHUT_RDWR)
                else:
                    meta.pause()
                    error = "MASTERDOWN" if mode == "single" else "CLUSTERDOWN"
                    H.wait_until(
                        "Owner lease expired",
                        10,
                        lambda: error in F.redis_error(source, ["SET", "probe", "x"]),
                    )
                try:
                    pending.result(timeout=10)
                    raise AssertionError("retired WAIT returned a reply")
                except H.Failure as failure:
                    assert "closed its Redis connection" in str(failure), failure
                hold.unlink(missing_ok=True)
                # EOF alone does not prove the server coroutine has stopped.
                # INFO uses a fresh connection; it must be the only client.
                H.wait_until(
                    "retired WAIT coroutine drained",
                    5,
                    lambda: "connected_clients:1"
                    in F.redis_call(source, ["INFO", "clients"]).splitlines(),
                )
        finally:
            hold.unlink(missing_ok=True)
            meta.resume()


if __name__ == "__main__":
    C.META, C.DATA, C.CTL, C.REDIS_CLI = sys.argv[1:5]
    with tempfile.TemporaryDirectory(
        prefix="wait-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        for mode in ("single", "cluster"):
            if "--retirement" not in sys.argv:
                acknowledged(Path(directory), mode)
            boundaries = (
                ("before", "blocked", "disconnect")
                if os.environ.get("LAVIK_TEST_FAULTS_AVAILABLE", "1") == "1"
                else ("blocked",)
            )
            for boundary in boundaries:
                retirement_before_registration(Path(directory), mode, boundary)
    print("managed WAIT checks passed")
