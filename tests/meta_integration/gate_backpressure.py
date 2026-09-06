#!/usr/bin/env python3
"""Integration gate: slow-peer backpressure for keylane_meta.

Two scenarios on 3-node proxy-meshed clusters:

1. slow-link: follower B's inbound link is delayed 2s per chunk (20x the
   100ms heartbeat) for a ~10s window under continuous propose load.
   Asserts:
   (a) the leader + follower A quorum keeps committing the whole window
       (OK'd propose count and the committed index both advance);
   (b) the leader's memory stays bounded: VmRSS delta across the window
       must stay under RSS_BUDGET_KB. The adapter caps client egress at
       256 messages / 8MB per peer and the test payloads are a few bytes,
       so anything remotely near the budget would mean queueing leaks.
   (c) after heal(), B converges (log the path: install_snapshot vs log
       append) with the full history intact;
   (d) every proxy carried bytes (the mesh really carries raft traffic).
2. slow-recovery: same delay, but the leader takes a manual snapshot and
   compacts while B is delayed, so B's post-heal catch-up must go through
   install_snapshot (its snapshot_idx advances from 0) and still ends
   byte-identical.

Cluster runs with reserved_log_items=500 so a healed slow follower can
close the post-snapshot gap via appends; see gate_transport.py for the
snapshot-chase rationale.

Usage: gate_backpressure.py /path/to/keylane_meta [workdir]
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402

SLOW_DELAY_S = 2.0
SLOW_WINDOW_S = 10.0
RSS_BUDGET_KB = 64 * 1024  # 64MB


def scenario_slow_link(binary, workdir):
    H.log("scenario slow-link: 2s inbound delay on a follower under load")
    args = H.raft_args(reserved_log_items=500)
    subdir = os.path.join(workdir, "slow_link")
    os.makedirs(subdir, exist_ok=True)
    nodes = H.make_nodes(binary, subdir, 3, args=args)
    mesh = H.Mesh()
    load = None
    try:
        leader = H.bootstrap_meshed_cluster(nodes, mesh)
        history = H.CommittedHistory()
        op_id, reply = leader.propose("warmup")
        if not reply.startswith("OK "):
            raise H.Failure(f"warmup propose: {reply}")
        history.record(op_id, "warmup")
        H.wait_cluster_committed(nodes, int(reply[3:]))

        load = H.LoadThread(nodes, history, prefix="bp")
        load.start()
        time.sleep(1.5)  # warm up allocators before the RSS baseline

        leader = H.find_leader(nodes)
        slow = next(n for n in nodes if n.id != leader.id)
        proxy = mesh.proxy(slow.id)
        rss0 = H.read_rss_kb(leader.pid)
        ok0 = load.ok_count
        c0 = leader.committed()
        snap0 = slow.snapshot_idx()

        proxy.set_delay(SLOW_DELAY_S)
        H.log(f"delaying node {slow.id} inbound {SLOW_DELAY_S}s/chunk for "
              f"{SLOW_WINDOW_S}s (leader=node {leader.id}, "
              f"rss0={rss0}kB, committed={c0})")
        time.sleep(SLOW_WINDOW_S)

        # (a) quorum kept committing through the window.
        ok1 = load.ok_count
        c1 = H.max_committed([n for n in nodes if n.id != slow.id])
        if ok1 <= ok0:
            raise H.Failure(f"propose made no progress during slow-link "
                            f"({ok0} -> {ok1})")
        if c1 < c0 + 10:
            raise H.Failure(f"committed stalled during slow-link "
                            f"({c0} -> {c1})")
        rate = (ok1 - ok0) / SLOW_WINDOW_S
        H.log(f"window done: ok {ok0} -> {ok1} ({rate:.0f}/s), "
              f"quorum committed {c0} -> {c1}, node {slow.id} at "
              f"{slow.committed()}")

        # (b) leader memory bounded.
        rss1 = H.read_rss_kb(leader.pid)
        growth = rss1 - rss0
        H.log(f"leader RSS {rss0}kB -> {rss1}kB (delta {growth}kB, "
              f"budget {RSS_BUDGET_KB}kB)")
        if growth > RSS_BUDGET_KB:
            raise H.Failure(
                f"leader RSS grew {growth}kB > {RSS_BUDGET_KB}kB behind a "
                f"slow peer; egress/queue bound suspect")

        # Freeze the successful-history target before healing. If load keeps
        # running while the lagging member installs a snapshot, aggressive
        # test compaction can move the target faster than recovery and turn
        # this into a snapshot-chase benchmark instead of a backpressure gate.
        load.stop()
        load.join()
        load = None
        c1 = H.max_committed([n for n in nodes if n.id != slow.id])

        # (c) heal and converge; record which catch-up path was taken.
        proxy.heal()
        slow.wait_committed(c1, timeout=30)
        path = ("install_snapshot" if slow.snapshot_idx() > snap0
                else "log-append")
        H.log(f"node {slow.id} caught up via {path} "
              f"(snapshot_idx {snap0} -> {slow.snapshot_idx()})")
        history.check(nodes, timeout=30, desc="slow-link history")

        # (d) the mesh really carried the traffic.
        for node in nodes:
            forwarded = mesh.proxy(node.id).bytes_forwarded
            if forwarded == 0:
                raise H.Failure(f"proxy n{node.id} forwarded 0 bytes")
            H.log(f"proxy n{node.id}: {forwarded} bytes forwarded")

        history.check(nodes, timeout=30, desc="slow-link final")
        H.assert_intact(nodes, "slow-link")
        for node in nodes:
            node.terminate()
    finally:
        if load is not None:
            load.stop()
            load.join()
        for node in nodes:
            node.force_kill()
        mesh.close()


def scenario_slow_recovery(binary, workdir):
    H.log("scenario slow-recovery: delayed follower must install_snapshot")
    # reserved=0: the manual snapshot compacts the whole prefix, so the
    # delayed follower's only catch-up path is install_snapshot. No load
    # runs during the catch-up itself, so the snapshot-chase window from
    # gate_transport.py does not apply here.
    args = H.raft_args(snapshot_distance=20, reserved_log_items=0)
    subdir = os.path.join(workdir, "slow_recovery")
    os.makedirs(subdir, exist_ok=True)
    nodes = H.make_nodes(binary, subdir, 3, args=args)
    mesh = H.Mesh()
    try:
        leader = H.bootstrap_meshed_cluster(nodes, mesh)
        history = H.CommittedHistory()
        slow = nodes[2] if leader.id != 3 else nodes[1]
        proxy = mesh.proxy(slow.id)

        proxy.set_delay(SLOW_DELAY_S)
        last = H.propose_ops(leader, 0, 50, prefix="sr", history=history)
        snap_idx = H.manual_snapshot(leader)
        H.log(f"node {slow.id} delayed; 50 operations committed (idx {last}) "
              f"and leader snapshotted at {snap_idx}")
        proxy.heal()

        # The only possible catch-up path: the leader compacted past the
        # follower's last index, so install_snapshot must fire.
        H.wait_until(f"node {slow.id} install_snapshot >= {snap_idx}", 30,
                     lambda: slow.alive()
                     and slow.snapshot_idx() >= snap_idx)
        slow.wait_committed(last, timeout=30)
        history.check(nodes, timeout=30, desc="slow-recovery history")
        H.log(f"node {slow.id} recovered via install_snapshot "
              f"(snapshot_idx={slow.snapshot_idx()})")
        H.assert_intact(nodes, "slow-recovery")
        for node in nodes:
            node.terminate()
    finally:
        for node in nodes:
            node.force_kill()
        mesh.close()


def main():
    workdir, keep = H.make_workdir(sys.argv, "meta_integration_backpressure_")
    started = time.monotonic()
    try:
        scenario_slow_link(BINARY, workdir)
        scenario_slow_recovery(BINARY, workdir)
        H.log(f"PASS (2/2 scenarios) in {time.monotonic() - started:.1f}s")
        return 0
    except Exception as exc:  # noqa: BLE001 - dump everything on failure
        print(f"[gate-backpressure] FAIL: {exc}", file=sys.stderr)
        for root, _, files in os.walk(workdir):
            for name in sorted(files):
                if not name.endswith(".log"):
                    continue
                path = os.path.join(root, name)
                print(f"--- {path} tail ---", file=sys.stderr)
                try:
                    with open(path, "r", errors="replace") as handle:
                        print("".join(handle.readlines()[-40:]),
                              file=sys.stderr)
                except OSError as log_exc:
                    print(f"<unreadable: {log_exc}>", file=sys.stderr)
        return 1
    finally:
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    BINARY = sys.argv[1]
    H.set_tag("gate-backpressure")
    sys.exit(main())
