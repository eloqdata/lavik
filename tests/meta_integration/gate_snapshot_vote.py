#!/usr/bin/env python3
"""Gate: an offline three-voter member cannot win after a fourth member joins.

The surviving current member must reject stale RequestVote messages with a
retained WAL, an entirely compacted WAL, and after reopening that snapshot.
Inspect internal election decisions as well as public status: an invalid
leader can stall before it reaches the public leader callback.

Usage: gate_snapshot_vote.py /path/to/keylane-meta [workdir]
"""

import os
import re
import sys
import time

import harness as H


def read_since(node, offset):
    with open(node.log_path, "rb") as source:
        source.seek(offset)
        return source.read().decode(errors="replace")


def run_case(binary, workdir, mode):
    scenario = os.path.join(workdir, mode)
    os.makedirs(scenario)
    nodes = H.make_nodes(binary, scenario, 4,
                         args=H.raft_args(snapshot_distance=100_000))
    late = nodes[2]
    manifest = os.path.join(scenario, "initial.toml")
    H.write_initial_cluster_manifest(manifest, nodes[:3])
    history = H.CommittedHistory()
    try:
        for node in nodes[:3]:
            node.start(initial_cluster_manifest=manifest)
        leader = H.find_leader(nodes[:3])
        H.wait_until("all initial members complete genesis", 10, lambda: all(
            os.path.exists(os.path.join(node.data_dir,
                                        "initial_bindings_complete.dat"))
            for node in nodes[:3]))
        index = H.propose_ops(leader, 0, 1, prefix="before-offline",
                              history=history)
        H.wait_cluster_committed(nodes[:3], index)
        late.terminate()

        # Node 3 never sees the new config, log entries, or snapshot. Its
        # older local three-voter config is legitimate crash-recovery input.
        leader = H.find_leader(nodes[:2])
        nodes[3].start()
        H.join_and_verify(leader, nodes[3])
        active = [node for node in nodes if node.alive()]
        leader = H.find_leader(active)
        index = H.propose_ops(leader, 1, 8, prefix="after-add", history=history)
        H.wait_cluster_committed(active, index)
        survivor = next(node for node in nodes[:2] if node is not leader)
        if mode != "wal":
            snapshot_index = H.manual_snapshot(survivor)
            H.wait_until("snapshot durable and WAL entirely compacted", 10,
                         lambda: survivor.snapshot_idx() == snapshot_index
                         and H.wal_segment_first_indexes(survivor)
                         == [snapshot_index + 1]
                         # WAL v1's remaining segment contains only its
                         # 20-byte header, hence last_entry() has term zero.
                         and os.path.getsize(os.path.join(
                             survivor.data_dir,
                             f"log-{snapshot_index + 1}.seg")) == 20)

        removed = [node for node in active if node is not survivor]
        for node in removed:
            node.kill9()
        if mode == "snapshot_restart":
            survivor.terminate()
            survivor.start()
            if survivor.snapshot_idx() != snapshot_index:
                raise H.Failure("restart did not recover the compacted snapshot")

        minority = [survivor, late]
        offsets = {node.id: os.path.getsize(node.log_path) for node in minority}
        late.start()
        # A timeout looking for a public leader used to pass even though the
        # old core granted the stale vote, entered become_leader, then stalled
        # while its peer attempted to roll back an already committed prefix.
        decisions = []
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            for node in minority:
                log = read_since(node, offsets[node.id])
                if "BECOME LEADER" in log or "rollback logs" in log:
                    raise H.Failure(f"{mode}: node {node.id} elected or rolled back")
            decisions = re.findall(
                rf"\[VOTE REQ\] my role \w+, from peer {late.id},"
                r".*?decision: ([OX])", read_since(survivor, offsets[survivor.id]),
                flags=re.DOTALL)
            if "O" in decisions:
                raise H.Failure(f"{mode}: compacted member granted a stale vote")
            time.sleep(0.05)
        if not decisions:
            raise H.Failure(f"{mode}: no stale RequestVote decision observed")
        H.assert_intact(minority, mode)
        if survivor.committed() < index:
            raise H.Failure(f"{mode}: committed state regressed")
        for node in minority:
            if node.is_leader():
                raise H.Failure(f"{mode}: two of four elected a leader")
            reply = node.put_automatic_uncontrolled_failover_policy(
                1, timeout=2)
            if reply.startswith("OK "):
                raise H.Failure(f"{mode}: two of four committed a write")

        # A real majority must still elect, catch up the stale member, retain
        # all acknowledged history, and commit new work after the partition.
        leader.start()
        majority = [node for node in nodes if node.alive()]
        leader = H.find_leader(majority)
        H.propose_ops(leader, 100, 1, prefix="restored", history=history)
        history.check(majority, desc=f"{mode}: restored majority")
        H.log(f"{mode}: stale votes denied, 2/4 fenced, 3/4 recovered — OK")
    except Exception:
        H.dump_node_logs(nodes, lines=80)
        raise
    finally:
        for node in nodes:
            node.force_kill()


def main():
    binary = sys.argv[1]
    workdir, keep = H.make_workdir(sys.argv, "meta_snapshot_vote_")
    try:
        for mode in ("wal", "snapshot", "snapshot_restart"):
            run_case(binary, workdir, mode)
        return 0
    except Exception as error:  # noqa: BLE001
        print(f"[snapshot-vote] FAIL: {error}", file=sys.stderr)
        return 1
    finally:
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    H.set_tag("snapshot-vote")
    sys.exit(main())
