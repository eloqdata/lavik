#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
"""Real three-member exporter, bounded HTTP, failover and recovery contract."""

import concurrent.futures
import http.client
import os
import socket
import subprocess
import sys
import time

import harness as H


def scrape(node):
    connection = http.client.HTTPConnection("127.0.0.1", node.metrics_port, timeout=4)
    try:
        connection.request("GET", "/metrics")
        response = connection.getresponse()
        return response.status, response.read().decode()
    finally:
        connection.close()


def metrics(node):
    code, body = scrape(node)
    if code != 200:
        raise H.Failure(f"metrics HTTP {code}: {body}")
    return body


def value(body, name, **labels):
    for line in body.splitlines():
        if line.startswith(name + " ") or line.startswith(name + "{"):
            if all(f'{key}="{val}"' in line for key, val in labels.items()):
                return float(line.rsplit(" ", 1)[1])
    return None


def main():
    workdir, keep = H.make_workdir(sys.argv, "meta_metrics_")
    nodes = H.make_nodes(os.path.abspath(sys.argv[1]), workdir, 3)
    for node in nodes:
        node.metrics_port = H.free_port()
        node.args += ["--metrics-port", str(node.metrics_port)]
    mesh = H.Mesh()
    slow = None
    partial = []
    try:
        leader = H.bootstrap_meshed_cluster(nodes, mesh)
        _, result = leader.propose("metrics")
        assert result.startswith("OK "), result
        H.wait_cluster_committed(nodes, int(result[3:]))
        body = metrics(leader)
        assert value(body, "lavik_meta_raft_leader_authority") == 1
        assert value(body, "lavik_meta_source_available", source="raft") == 1
        assert value(body, "lavik_meta_proposals_total", result="applied_success") >= 2
        assert value(body, "lavik_meta_raft_applied_index") >= int(result[3:])
        assert "# TYPE etcd_disk_wal_write_bytes_total gauge\n" in body
        assert "etcd_snap_db_" not in body and "etcd_server_" not in body
        for node in nodes:
            if node != leader:
                assert value(metrics(node), "lavik_meta_raft_peer_match_index") is None
        sample = os.path.join(workdir, "metrics.prom")
        with open(sample, "w") as stream:
            stream.write(body)
        if H.shutil.which("promtool"):
            checked = subprocess.run(
                ["promtool", "check", "metrics"],
                input=body,
                text=True,
                capture_output=True,
            )
            # The upstream Gauge intentionally violates the _total convention.
            unexpected = [
                line
                for line in (checked.stdout + checked.stderr).splitlines()
                if line
                != 'etcd_disk_wal_write_bytes_total non-counter metrics should not have "_total" suffix'
            ]
            assert not unexpected, checked.stdout + checked.stderr

        # Incomplete headers never occupy the application worker.
        slow = socket.create_connection(("127.0.0.1", leader.metrics_port))
        slow.settimeout(5)
        slow.sendall(b"GET /metrics HTTP/1.1\r\n")
        began = time.monotonic()
        assert leader.propose("while-slow-scrape")[1].startswith("OK ")
        assert slow.recv(1) == b""
        assert time.monotonic() - began < 4.5
        slow.close()
        slow = None
        with socket.create_connection(("127.0.0.1", leader.metrics_port)) as conn:
            conn.settimeout(4)
            conn.sendall(b"GET /metrics HTTP/1.1\r\nX: " + b"x" * 16384)
            assert b"431" in conn.recv(256)

        def pressure(_):
            try:
                return scrape(leader)[0]
            except (OSError, http.client.HTTPException):
                return 0  # bounded admission or total connection deadline

        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
            outcomes = list(pool.map(pressure, range(128)))
        assert 200 in outcomes and set(outcomes) <= {0, 200, 503}, outcomes
        assert leader.propose("after-concurrent-scrape")[1].startswith("OK ")
        H.log(
            f"metrics: concurrent HTTP outcomes {dict((v, outcomes.count(v)) for v in set(outcomes))}"
        )

        H.manual_snapshot(leader)
        body = metrics(leader)
        for stage in ["capture", "prepare", "publish"]:
            assert (
                value(
                    body,
                    "lavik_meta_raft_operations_total",
                    stage=stage,
                    result="success",
                )
                >= 1
            )

        victim = next(n for n in nodes if n != leader)
        mesh.proxy(victim.id).set_drop()

        def stale_evidence():
            observed = metrics(leader)
            return (
                value(observed, "lavik_meta_raft_peer_fresh", peer_id=str(victim.id))
                == 0
                and value(
                    observed,
                    "lavik_meta_raft_peer_applied_index",
                    peer_id=str(victim.id),
                )
                is None
            )

        H.wait_until(
            "isolated follower has no valid applied metric", 10, stale_evidence
        )
        assert leader.propose("surviving-quorum")[1].startswith("OK ")
        mesh.proxy(victim.id).heal()
        H.wait_until(
            "peer fresh after reconnect",
            15,
            lambda: value(
                metrics(leader), "lavik_meta_raft_peer_fresh", peer_id=str(victim.id)
            )
            == 1,
        )

        old = leader
        old.terminate()
        leader = H.find_leader(nodes, exclude=[old.id])
        assert leader.propose("replacement-leader")[1].startswith("OK ")
        old.start()
        H.wait_until(
            "recovered runtime available",
            15,
            lambda: value(metrics(old), "lavik_meta_source_available", source="raft")
            == 1,
        )
        assert (
            value(
                metrics(old),
                "lavik_meta_raft_operations_total",
                stage="install",
                result="success",
            )
            >= 1
        )

        # Losing both remote voters removes serving authority, not merely the hint.
        other = [n for n in nodes if n != leader]
        for node in other:
            node.terminate()
        H.wait_until(
            "authority withdrawn after quorum loss",
            10,
            lambda: value(metrics(leader), "lavik_meta_raft_leader_authority") == 0,
        )
        partial = [
            socket.create_connection(("127.0.0.1", leader.metrics_port))
            for _ in range(32)
        ]
        slow = partial[0]
        for client in partial:
            client.sendall(b"GET /metrics")
        started = time.monotonic()
        leader.terminate()
        assert time.monotonic() - started < 5
        slow.settimeout(1)
        try:
            assert slow.recv(1) == b""
        except ConnectionResetError:
            pass  # Closing a not-yet-read partial header may reset the socket.
        for client in partial:
            client.close()
        H.log(
            "metrics: three-member progress, snapshot, reconnect, failover, recovery, quorum loss and shutdown passed"
        )
    finally:
        for client in partial:
            client.close()
        if slow is not None:
            slow.close()
        for node in nodes:
            node.force_kill()
        mesh.close()
        if not keep:
            H.shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
