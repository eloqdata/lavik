#!/usr/bin/env python3
"""Integration gate: WAL / snapshot recovery matrix for keylane_meta (issue #19).

Four serial scenarios, each on a fresh 3-node cluster with plain direct
connections (faults are process-level: SIGKILL / SIGSTOP / SIGTERM):

A. crash-roles: kill -9 a follower mid-write, restart it, converge; then
   kill -9 the leader mid-write, fail over, restart it, converge. After
   every restart the node's committed index must reach its pre-kill value
   (no durable regression) and the committed history must stay intact.
B. snapshot-recovery: --snapshot-distance 20 forces automatic
   snapshot+compaction during the writes; kill -9 and restart each role
   in turn — state must come back complete via snapshot + log replay, and
   snapshot_idx must not regress.
C. install-snapshot: SIGSTOP a follower, write 100 keys, take a manual
   snapshot on the leader (reserved-log-items 0 purges the log prefix),
   SIGCONT — the follower can only catch up via install_snapshot, so its
   snapshot_idx must advance to the leader's snapshot and every key must
   arrive.
D. clean-restart: SIGTERM-restart every node in turn; committed never
   regresses and data stays complete.

Usage: gate_recovery.py /path/to/keylane_meta [workdir]
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402


def fresh_cluster(binary, workdir, name, args=None):
    subdir = os.path.join(workdir, name)
    os.makedirs(subdir, exist_ok=True)
    nodes = H.make_nodes(binary, subdir, 3, args=args)
    H.bootstrap_cluster(nodes)
    return nodes


def stop_load(load):
    if load is not None:
        load.stop()
        load.join()


def scenario_crash_roles(binary, workdir):
    H.log("scenario A: kill -9 follower, then leader, mid-write")
    nodes = fresh_cluster(binary, workdir, "crash_roles")
    load = None
    try:
        history = H.CommittedHistory()
        leader = H.find_leader(nodes)
        H.propose_ops(leader, 0, 20, prefix="a", history=history)
        load = H.LoadThread(nodes, history, prefix="al")
        load.start()
        time.sleep(1.0)

        # Follower crash + restart under load.
        leader = H.find_leader(nodes)
        follower = next(n for n in nodes if n.id != leader.id)
        pre = follower.committed()
        follower.kill9()
        time.sleep(1.0)  # the remaining quorum keeps serving the load
        follower.start(bootstrap=False)
        H.wait_no_regress(follower, pre, timeout=20)
        history.check([follower], timeout=30,
                      desc="follower post-crash catch-up")
        H.log(f"node {follower.id} recovered from kill -9 (follower)")

        # Leader crash + failover + restart under load.
        leader = H.find_leader(nodes)
        pre = leader.committed()
        leader.kill9()
        survivors = [n for n in nodes if n.id != leader.id]
        new_leader = H.find_leader(survivors, timeout=15)
        time.sleep(1.0)
        leader.start(bootstrap=False)
        H.wait_no_regress(leader, pre, timeout=20)
        history.check(nodes, timeout=30, desc="leader post-crash catch-up")
        H.log(f"node {leader.id} recovered from kill -9 (was leader, "
              f"now node {new_leader.id} leads)")

        stop_load(load)
        H.log(load.stats())
        load = None
        history.check(nodes, timeout=30, desc="scenario A final")
        for node in nodes:
            node.terminate()
    finally:
        stop_load(load)
        for node in nodes:
            node.force_kill()


def scenario_snapshot_recovery(binary, workdir):
    H.log("scenario B: auto snapshot+compact, kill -9 and restart roles")
    args = H.raft_args(snapshot_distance=20)
    nodes = fresh_cluster(binary, workdir, "snapshot_recovery", args=args)
    try:
        history = H.CommittedHistory()
        leader = H.find_leader(nodes)
        last = H.propose_ops(leader, 0, 60, prefix="b", history=history)
        H.wait_cluster_committed(nodes, last, timeout=20)
        for node in nodes:
            H.wait_until(f"node {node.id} automatic snapshot", 20,
                         lambda node=node: node.snapshot_idx() > 0)
        H.log("automatic snapshots observed on all nodes")

        # Restart a follower: recovery must start from its snapshot.
        follower = next(n for n in nodes if n.id != leader.id)
        pre_c, pre_s = follower.committed(), follower.snapshot_idx()
        follower.kill9()
        follower.start(bootstrap=False)
        H.wait_no_regress(follower, pre_c, timeout=20)
        if follower.snapshot_idx() < pre_s:
            raise H.Failure(
                f"node {follower.id} snapshot_idx regressed: "
                f"{follower.snapshot_idx()} < {pre_s}")
        history.check([follower], timeout=30,
                      desc="follower snapshot recovery")
        H.log(f"node {follower.id} recovered via snapshot (follower)")

        # Restart the leader: failover first, then snapshot recovery.
        leader = H.find_leader(nodes)
        pre_c, pre_s = leader.committed(), leader.snapshot_idx()
        leader.kill9()
        H.find_leader([n for n in nodes if n.id != leader.id], timeout=15)
        leader.start(bootstrap=False)
        H.wait_no_regress(leader, pre_c, timeout=20)
        if leader.snapshot_idx() < pre_s:
            raise H.Failure(
                f"node {leader.id} snapshot_idx regressed: "
                f"{leader.snapshot_idx()} < {pre_s}")
        history.check(nodes, timeout=30, desc="leader snapshot recovery")
        H.log(f"node {leader.id} recovered via snapshot (was leader)")

        for node in nodes:
            node.terminate()
    finally:
        for node in nodes:
            node.force_kill()


def scenario_install_snapshot(binary, workdir):
    H.log("scenario C: paused follower catches up via install_snapshot")
    # Large distance: no automatic snapshot may fire before the manual one.
    args = H.raft_args(snapshot_distance=100000)
    nodes = fresh_cluster(binary, workdir, "install_snapshot", args=args)
    try:
        history = H.CommittedHistory()
        leader = H.find_leader(nodes)
        follower = nodes[2] if leader.id != 3 else nodes[1]

        follower.pause()
        last = H.propose_ops(leader, 0, 100, prefix="c", history=history)
        snap_idx = H.manual_snapshot(leader)
        if snap_idx < last:
            raise H.Failure(
                f"manual snapshot idx {snap_idx} < last write idx {last}")
        H.log(f"node {follower.id} paused; 100 operations + manual snapshot "
              f"at idx {snap_idx} on leader")

        follower.resume()
        # The log prefix is compacted away, so install_snapshot is the only
        # possible catch-up path: snapshot_idx must advance from 0.
        H.wait_until(f"node {follower.id} install_snapshot >= {snap_idx}",
                     30, lambda: follower.alive()
                     and follower.snapshot_idx() >= snap_idx)
        follower.wait_committed(last, timeout=30)
        history.check([follower], timeout=30,
                      desc="install-snapshot catch-up")
        H.log(f"node {follower.id} caught up via install_snapshot")

        for node in nodes:
            node.terminate()
    finally:
        for node in nodes:
            node.force_kill()


def scenario_clean_restart(binary, workdir):
    H.log("scenario D: SIGTERM restarts never regress committed")
    nodes = fresh_cluster(binary, workdir, "clean_restart")
    try:
        history = H.CommittedHistory()
        leader = H.find_leader(nodes)
        last = H.propose_ops(leader, 0, 25, prefix="d", history=history)
        H.wait_cluster_committed(nodes, last, timeout=20)

        for node in nodes:
            pre = node.committed()
            node.terminate()
            node.start(bootstrap=False)
            H.wait_no_regress(node, pre, timeout=20)
            H.log(f"node {node.id} clean restart: committed {pre} -> "
                  f"{node.committed()}")
        history.check(nodes, timeout=30, desc="clean restarts")

        for node in nodes:
            node.terminate()
    finally:
        for node in nodes:
            node.force_kill()


def main():
    workdir, keep = H.make_workdir(sys.argv, "meta_integration_recovery_")
    started = time.monotonic()
    scenarios = [
        scenario_crash_roles,
        scenario_snapshot_recovery,
        scenario_install_snapshot,
        scenario_clean_restart,
    ]
    done = 0
    try:
        for scenario in scenarios:
            scenario(BINARY, workdir)
            done += 1
            H.log(f"{scenario.__name__}: OK "
                  f"({time.monotonic() - started:.1f}s elapsed)")
        H.log(f"PASS ({done}/{len(scenarios)} scenarios) in "
              f"{time.monotonic() - started:.1f}s")
        return 0
    except Exception as exc:  # noqa: BLE001 - dump everything on failure
        print(f"[gate-recovery] FAIL after {done}/{len(scenarios)} "
              f"scenarios: {exc}", file=sys.stderr)
        # Node objects are scoped to their scenarios; find their logs.
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
    H.set_tag("gate-recovery")
    sys.exit(main())
