#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Alternating real-Raft measurements with current process-test fixtures."""

import argparse
import concurrent.futures
import json
from pathlib import Path
import socket
import statistics
import sys
import threading
import time


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        raise RuntimeError("empty measurement")
    return {
        name: ordered[min(len(ordered) - 1, int(q * len(ordered)))]
        for name, q in (("p50_us", 0.5), ("p95_us", 0.95), ("p99_us", 0.99))
    } | {"count": len(ordered), "mean_us": statistics.mean(ordered)}


def checked(reply):
    if not reply.startswith("OK "):
        raise RuntimeError(reply)
    return reply


def redis_ok(reply):
    if isinstance(reply, D.RespError) or reply != b"OK":
        raise RuntimeError(f"unexpected Redis reply: {reply!r}")


def master_ok(reply, port, counts=None):
    if not isinstance(reply, list) or len(reply) % 2:
        raise RuntimeError(f"unexpected Sentinel reply: {reply!r}")
    fields = dict(zip(reply[::2], reply[1::2]))
    if (
        fields.get(b"ip") != b"127.0.0.1"
        or fields.get(b"port") != str(port).encode()
        or fields.get(b"flags") not in (b"master", b"master,disconnected")
    ):
        raise RuntimeError(f"unhealthy or unexpected master: {fields!r}")
    if counts is not None:
        flag = fields[b"flags"].decode()
        counts[flag] = counts.get(flag, 0) + 1
    return True


def control_health(data):
    values = {
        name: data.metric("lavik_cluster_control_" + name)
        for name in ("connected", "reconnects_total", "protocol_errors_total")
    }
    if values["connected"] != 1:
        raise RuntimeError(f"Data session disconnected: {values}")
    return values


def wait_writable(client):
    # Audit setup can briefly outpace lease refresh. This is an untimed setup
    # precondition only: the measured SET stream never retries errors.
    deadline = time.monotonic() + 10
    retries = 0
    while True:
        reply = client.command("SET", "bench", "ready")
        if not isinstance(reply, D.RespError) and reply == b"OK":
            return retries
        if (
            not isinstance(reply, D.RespError)
            or not reply.startswith((b"MASTERDOWN ", b"LOADING "))
            or time.monotonic() >= deadline
        ):
            raise RuntimeError(f"owner did not become writable after setup: {reply!r}")
        retries += 1
        time.sleep(0.05)


def fill_audit(node, target):
    command = "registernode fffffffffffffffffffffffffffffffffffffff0 lavik://node/fffffffffffffffffffffffffffffffffffffff0 primary tcp://127.0.0.1:19999\n"
    # Keep current identities identical even in the minimum-audit case.
    checked(node.ctl(command.strip()))
    count = int(node.status()["audit_size"])
    remaining = max(0, target - count)
    # Bounded outstanding sessions keep setup faster without changing the
    # offered load of the measured phase. All filler commands are committed.

    def worker(n):
        with socket.socket(socket.AF_UNIX) as connection:
            connection.settimeout(60)
            connection.connect(node.ctl_path)
            stream = connection.makefile("rb")
            for _ in range(n):
                connection.sendall(command.encode())
                checked(stream.readline().decode().strip())

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [
            pool.submit(worker, remaining // 8 + (i < remaining % 8)) for i in range(8)
        ]
        for future in futures:
            future.result()
    return int(node.status()["audit_size"])


def combined(args, label, round_id, audit, operations):
    directory = args.output / f"combined-{label}-{round_id}-{audit}-{operations}"
    fixture = D.DiscoveryFixture(
        str(getattr(args, label)), str(args.data), str(args.ctl), directory
    )
    for meta in fixture.metas:
        # Keep periodic snapshots out of the timed overlap; measure an
        # explicit snapshot afterward under both implementations instead.
        meta.args[meta.args.index("--snapshot-distance") + 1] = "1000000"
    stop = threading.Event()
    publication_done = threading.Event()
    samples = {
        name: []
        for name in (
            "proposal",
            "admin",
            "sentinel",
            "redis",
            "publication_after_reply",
            "publication_from_submit",
        )
    }
    failures = []
    threads = []
    sentinel_flags = {}
    try:
        fixture.start_created()
        node = fixture.leader
        # Reserve the exact audit rows needed by subsequent operation setup.
        # Filling before large blobs changes only untimed preparation cost.
        fill_audit(node, max(0, audit - operations - 1))
        for _ in range(operations):
            checked(node.submitop(node.new_op_id(), "maintenance", "x" * 65536))
        fixed_id = node.new_op_id()
        checked(node.submitop(fixed_id, "maintenance", "bench"))
        before = node.status()
        seeded = int(before["audit_size"])
        if audit and seeded != audit:
            raise RuntimeError(f"unexpected initial audit count: {seeded} != {audit}")
        # Warm up actual discovery and serving before measuring.
        sentinel = D.Client(fixture.sentinel_ports[node.id])
        redis_ok(sentinel.command("AUTH", D.SENTINEL_PASSWORD))
        setup_master_start = time.perf_counter_ns()
        D.poll_call(
            "expected published Sentinel owner after audit setup",
            10,
            lambda: master_ok(
                sentinel.command("SENTINEL", "MASTER", D.GROUP),
                fixture.data_nodes[0].redis_port,
            ),
        )
        setup_master_wait_us = (time.perf_counter_ns() - setup_master_start) / 1000
        redis = D.Client(fixture.data_nodes[0].redis_port)
        setup_write_retries = wait_writable(redis)
        # Drain creation-time publication before attributing one counter
        # increment per policy revision. Reconnects invalidate the whole run.
        time.sleep(0.5)
        session_health = [control_health(data) for data in fixture.data_nodes]

        def paced(name, hz, action):
            try:
                next_time = time.monotonic()
                while not stop.is_set():
                    started = time.perf_counter_ns()
                    action()
                    samples[name].append((time.perf_counter_ns() - started) / 1000)
                    next_time += 1 / hz
                    stop.wait(max(0, next_time - time.monotonic()))
            except Exception as error:
                failures.append(f"{name}: {error}")
                stop.set()

        for name, action in (
            ("admin", lambda: checked(node.ctl("status"))),
            (
                "sentinel",
                lambda: master_ok(
                    sentinel.command("SENTINEL", "MASTER", D.GROUP),
                    fixture.data_nodes[0].redis_port,
                    sentinel_flags,
                ),
            ),
            ("redis", lambda: redis_ok(redis.command("SET", "bench", "value"))),
        ):
            thread = threading.Thread(target=paced, args=(name, 100, action))
            thread.start()
            threads.append(thread)

        def proposals():
            try:
                next_time = time.monotonic()
                while not stop.is_set() and (
                    not publication_done.is_set()
                    or len(samples["proposal"]) < args.requests
                ):
                    started = time.perf_counter_ns()
                    # Idempotent re-proposals still traverse admission, Raft,
                    # audit and effect verification without growing live state.
                    checked(node.submitop(fixed_id, "maintenance", "bench"))
                    samples["proposal"].append(
                        (time.perf_counter_ns() - started) / 1000
                    )
                    next_time += 1 / args.proposal_hz
                    stop.wait(max(0, next_time - time.monotonic()))
            except Exception as error:
                failures.append(f"proposal: {error}")

        proposal_thread = threading.Thread(target=proposals)
        begin = time.perf_counter()
        proposal_thread.start()
        threads.append(proposal_thread)
        metric = "lavik_cluster_control_full_states_applied_total"
        # Each policy revision changes the projected duration below the
        # election clamp. No other measured request changes desired state.
        for index in range(args.publications):
            if failures:
                raise RuntimeError(failures)
            previous = [data.metric(metric) for data in fixture.data_nodes]
            started = time.perf_counter_ns()
            checked(node.put_authority_lease_policy(index + 2, 1100 + index % 2 * 100))
            replied = time.perf_counter_ns()
            deadline = time.monotonic() + 15
            while True:
                current = [data.metric(metric) for data in fixture.data_nodes]
                if any(value > old + 1 for value, old in zip(current, previous)):
                    raise RuntimeError("unattributed extra publication")
                if all(value == old + 1 for value, old in zip(current, previous)):
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError("publication did not reach both Data sessions")
                time.sleep(0.001)
            ended = time.perf_counter_ns()
            samples["publication_after_reply"].append((ended - replied) / 1000)
            samples["publication_from_submit"].append((ended - started) / 1000)
            if [control_health(data) for data in fixture.data_nodes] != session_health:
                raise RuntimeError(
                    "Data session restarted or protocol error during publication"
                )
            # A policy change invalidates the previous projection's pending
            # lease challenge. Leave both implementations the same renewal
            # interval; a rapid stream instead measures authority starvation.
            time.sleep(args.publication_settle_ms / 1000)
        overlap_elapsed = time.perf_counter() - begin
        overlap_counts = {name: len(values) for name, values in samples.items()}
        publication_done.set()
        proposal_thread.join()
        elapsed = time.perf_counter() - begin
        stop.set()
        for thread in threads:
            thread.join()
        sentinel.close()
        redis.close()
        if failures:
            raise RuntimeError(failures)
        after = node.status()
        if (
            after["term"] != before["term"]
            or after["leader"] != before["leader"]
            or after["snapshot_idx"] != before["snapshot_idx"]
            or [control_health(data) for data in fixture.data_nodes] != session_health
        ):
            raise RuntimeError(
                "leadership, snapshot, or Data session changed during measurement"
            )
        snapshot_start = time.perf_counter_ns()
        snapshot = H.manual_snapshot(node, timeout=60)
        snapshot_us = (time.perf_counter_ns() - snapshot_start) / 1000
        return {
            "kind": "combined",
            "label": label,
            "round": round_id,
            "audit_target": audit,
            "seeded_audit": seeded,
            "operations": operations,
            "publication_settle_ms": args.publication_settle_ms,
            "proposal_hz": args.proposal_hz,
            "setup_write_retries": setup_write_retries,
            "setup_master_wait_us": setup_master_wait_us,
            "before": before,
            "after": after,
            "elapsed_s": elapsed,
            "overlap_elapsed_s": overlap_elapsed,
            "overlap_counts": overlap_counts,
            "completed_proposals_per_overlap_second": overlap_counts["proposal"]
            / overlap_elapsed,
            "sentinel_flags": sentinel_flags,
            "session_health": session_health,
            "manual_snapshot_index": snapshot,
            "manual_snapshot_us": snapshot_us,
            "distributions": {
                name: distribution(values[: overlap_counts[name]])
                for name, values in samples.items()
            },
            "samples_us": samples,
        }
    except Exception as error:
        stop.set()
        for thread in threads:
            thread.join(timeout=10)
        (directory / "failure.json").write_text(
            json.dumps({"error": str(error), "samples_us": samples}, indent=2) + "\n"
        )
        raise
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=10)
        fixture.force_kill()


def strict(args, label, round_id):
    directory = args.output / f"strict-{label}-{round_id}"
    directory.mkdir()
    node = H.Node(
        str(getattr(args, label)),
        str(directory),
        1,
        args=H.raft_args(snapshot_distance=100000),
    )
    try:
        node.start(bootstrap=True)
        node.wait_leader()
        checked(node.ctl("setauditpolicy strict-export benchmark"))
        fill_audit(node, 65536 - 256)
        success = []
        for _ in range(256):
            started = time.perf_counter_ns()
            checked(
                node.registernode(
                    "f" * 39 + "0",
                    "lavik://node/" + "f" * 39 + "0",
                    endpoints=("tcp://127.0.0.1:19999",),
                )
            )
            success.append((time.perf_counter_ns() - started) / 1000)
        # The Admin audit count is an applied-state read; Raft's atomic
        # committed cursor can catch up just after the proposal reply.
        time.sleep(0.2)
        full = node.status()
        if int(full["audit_size"]) != 65536:
            raise RuntimeError(full)
        rejected = []
        for _ in range(256):
            started = time.perf_counter_ns()
            reply = node.submitop(node.new_op_id(), "maintenance", "rejected")
            rejected.append((time.perf_counter_ns() - started) / 1000)
            if reply != "ERR resource-exhausted":
                raise RuntimeError(f"unexpected strict rejection: {reply}")
        after = node.status()
        if full["committed"] != after["committed"]:
            raise RuntimeError(
                f"strict rejection changed Raft cursor: {full} -> {after}"
            )
        return {
            "kind": "strict",
            "label": label,
            "round": round_id,
            "near_full_success": distribution(success),
            "full_rejected": distribution(rejected),
            "samples_us": {"near_full_success": success, "full_rejected": rejected},
            "full": full,
            "after": after,
        }
    finally:
        node.force_kill()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("source", "before", "after", "data", "ctl", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--requests", type=int, default=500)
    parser.add_argument("--proposal-hz", type=int, default=20)
    parser.add_argument("--publications", type=int, default=50)
    parser.add_argument("--publication-settle-ms", type=int, default=600)
    parser.add_argument("--audits", type=int, nargs="+", default=[0, 1024, 65536])
    parser.add_argument("--operations", type=int, nargs="+", default=[0, 128])
    parser.add_argument("--mode", choices=["combined", "strict"], default="combined")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(args.source / "tests/meta_integration"))
    import harness as H
    import gate_sentinel_discovery as D

    D.C.CTL = str(args.ctl)
    results = []
    for audit, operations in (
        [(a, o) for a in args.audits for o in args.operations]
        if args.mode == "combined"
        else [(0, 0)]
    ):
        for round_id in range(args.rounds):
            for label in (
                ["before", "after"] if round_id % 2 == 0 else ["after", "before"]
            ):
                result = (
                    combined(args, label, round_id, audit, operations)
                    if args.mode == "combined"
                    else strict(args, label, round_id)
                )
                results.append(result)
                (args.output / "results.json").write_text(
                    json.dumps(results, indent=2) + "\n"
                )
                print(
                    json.dumps({k: v for k, v in result.items() if k != "samples_us"}),
                    flush=True,
                )
