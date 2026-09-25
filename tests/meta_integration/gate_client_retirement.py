#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Data connection lifetime across authority and population transitions."""

import os
from pathlib import Path
import select
import signal
import socket
import sys
import tempfile

import gate_failover as F
import time

from gate_native_replication import C, H, Client, pair, ready, rejects


def require_closed(client, label, timeout=20):
    readable, _, _ = select.select([client.socket], [], [], timeout)
    assert readable, f"{label}: old connection remained open"
    try:
        remaining = client.socket.recv(4096)
    except ConnectionResetError:
        remaining = b""
    assert remaining == b"", f"{label}: expected EOF, received {remaining!r}"


def expiry(root):
    with pair(root, "expiry", client_mode="single") as (meta, owner, replica, writer):
        ready(meta)
        clients = [Client(owner) for _ in range(8)]
        follower = Client(replica)
        fresh = replacement_sub = None
        try:
            for client in clients:
                assert client.call("PING") == "PONG"
            assert clients[-1].call("SUBSCRIBE", "retirement") == [
                "subscribe",
                "retirement",
                1,
            ]
            H.wait_until(
                "replica readable",
                20,
                lambda: follower.call("GET", "{native}seed") == "baseline",
            )
            clients[-2].socket.sendall(
                b"*3\r\n$5\r\nBLPOP\r\n$5\r\nempty\r\n$1\r\n0\r\n"
            )
            H.wait_until(
                "blocked connection registered",
                10,
                lambda: "blocked_clients:1\r\n" in writer.call("INFO", "clients"),
            )
            meta.proc.send_signal(signal.SIGSTOP)
            try:
                for index, client in enumerate(clients):
                    require_closed(client, f"idle/subscriber {index}")
                # Losing Owner authority is not losing a legitimate replica's
                # complete population, and it must not close this old socket.
                assert follower.call("GET", "{native}seed") == "baseline"
                fresh = Client(owner)
                assert fresh.call("ROLE")[0] == "master"
                info = fresh.call("INFO", "replication")
                assert "role:master\r\n" in info
                assert "lavik_owner_authority:0\r\n" in info
                assert "lavik_data_readable:0\r\n" in info
                rejects(fresh, ("GET", "{native}seed"), "MASTERDOWN")
                assert fresh.call("PING") == "PONG"
                replacement_sub = Client(owner)
                assert replacement_sub.call("SUBSCRIBE", "retirement") == [
                    "subscribe",
                    "retirement",
                    1,
                ]
            finally:
                meta.proc.send_signal(signal.SIGCONT)

            def recovered():
                try:
                    return fresh.call("GET", "{native}seed") == "baseline"
                except H.Failure:
                    return False

            H.wait_until(
                "same-boot authority restores on new connection", 30, recovered
            )
            info = fresh.call("INFO", "replication")
            assert "lavik_owner_authority:1\r\n" in info
            assert "lavik_data_readable:1\r\n" in info
            assert replacement_sub.call("PING") == ["pong", ""]
        finally:
            for client in (fresh, replacement_sub):
                if client is not None:
                    client.close()
            follower.close()
            for client in clients:
                client.close()


def controlled(root):
    fixture = F.FailoverFixture(
        C.META,
        C.DATA,
        C.CTL,
        str(root / "controlled"),
        False,
        data_workers=2,
        client_mode="single",
    )
    clients = []
    try:
        fixture.start_created(add_follower=True)
        owner = fixture.by_id[F.OWNER]
        candidate = fixture.by_id[F.CANDIDATE]
        clients = [Client(node) for node in (candidate, owner)]
        for client in clients:
            assert client.call("PING") == "PONG"
        operation = fixture.submit_failover()
        F.wait_owner(fixture, F.CANDIDATE, fixture.data_nodes)
        F.wait_operation(
            fixture,
            operation,
            "OK completed failover-completed",
            "controlled cutover complete",
        )
        require_closed(clients[0], "promoted replica connection", timeout=5)
        require_closed(clients[1], "former owner connection", timeout=5)
        fixture.require_expected_processes_alive()
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        for client in clients:
            client.close()
        fixture.force_kill()


def main():
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    H.set_tag("client-retirement")
    with tempfile.TemporaryDirectory(
        prefix="lavik-client-retirement-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as d:
        (controlled if len(sys.argv) > 5 and sys.argv[5] == "controlled" else expiry)(
            Path(d)
        )
    H.log("PASS")


if __name__ == "__main__":
    main()
