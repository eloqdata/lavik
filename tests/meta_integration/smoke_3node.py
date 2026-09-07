#!/usr/bin/env python3
"""3-node smoke test for the keylane-meta Raft driver.

Usage: smoke_3node.py /path/to/keylane-meta [workdir]

Scenario: bootstrap node1, add node2/node3 through the ctl surface,
replicate committed writes (real SubmitOperation/CompleteOperation
commands), kill -9 node3 mid-run, keep writing, restart node3 on the same
data directory and wait for catch-up (log replay and/or snapshot install),
then verify automatic + manual snapshots and a clean SIGTERM shutdown of
all nodes. Stdlib only; the whole run is budgeted well under 60 seconds.

All process/ctl plumbing (Node, wait_until, find_leader, join_and_verify,
propose_ops, workdir handling) lives in harness.py; this file is only the
smoke scenario's own orchestration plus its smoke-specific checks (the
full committed-history sweep and the WAL v2 segment compaction check).
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402

TOTAL_OPS_1 = 50
TOTAL_OPS_2 = 20


def main():
    workdir, keep = H.make_workdir(sys.argv, "meta_integration_")
    nodes = H.make_nodes(BINARY, workdir, 3)
    started = time.monotonic()
    try:
        # 1. Single-node bootstrap; wait for self-election.
        nodes[0].start(bootstrap=True)
        leader = H.find_leader([nodes[0]])

        # 2. Empty nodes join through the ctl surface, one at a time; each
        #    join is verified by an actual replicated probe operation.
        nodes[1].start(bootstrap=False)
        nodes[2].start(bootstrap=False)
        for node in nodes[1:]:
            H.join_and_verify(leader, node)
        H.log("node2/node3 added")

        # Membership is proven; do one more end-to-end warmup write.
        history = H.CommittedHistory()
        op_id, reply = leader.propose("warmup1")
        if not reply.startswith("OK "):
            raise H.Failure(f"warmup propose: {reply}")
        history.record(op_id, "warmup1")
        warmup_idx = int(reply[3:])
        H.wait_cluster_committed(nodes, warmup_idx)
        for node in nodes:
            if node.getop(op_id) != "OK completed warmup1":
                raise H.Failure(
                    f"node {node.id} did not replicate the warmup op")
        H.log("3-node cluster converged")

        # 3. Commit 50 operations; verify byte-identical reads everywhere.
        last_idx = H.propose_ops(leader, 0, TOTAL_OPS_1, history=history)
        H.wait_cluster_committed(nodes, last_idx)
        history.check(nodes, timeout=30, desc="initial replication")
        H.log(f"{TOTAL_OPS_1} operations replicated to all nodes "
              f"(idx {last_idx})")

        # 4. Crash node3 mid-run; the remaining quorum keeps serving.
        nodes[2].kill9()
        time.sleep(0.2)
        leader = H.find_leader(nodes[:2])
        last_idx = H.propose_ops(leader, TOTAL_OPS_1, TOTAL_OPS_2,
                                 history=history)
        H.wait_cluster_committed(nodes[:2], last_idx)
        history.check(nodes[:2], timeout=30, desc="quorum after crash")
        H.log(f"{TOTAL_OPS_2} more operations with node3 down "
              f"(idx {last_idx})")

        # 5. Restart node3 on the same data dir; it must catch up via log
        #    replay and/or a snapshot install (distance 30 makes the leader
        #    compact while node3 is down). start() blocks until the ctl
        #    surface answers.
        nodes[2].start(bootstrap=False)
        nodes[2].wait_committed(last_idx, timeout=30)
        history.check([nodes[2]], timeout=30, desc="node3 catch-up")
        H.log("node 3 caught up after restart")

        # 6. Automatic snapshots fired on every node (and compaction with
        #    reserved_log_items=0 leaves the log starting past index 1).
        for node in nodes:
            H.wait_until(f"node {node.id} automatic snapshot", 20,
                         lambda node=node: node.snapshot_idx() > 0)
        H.log("automatic snapshots observed on all nodes")

        # 7. Manual snapshot on the current leader: the ctl drives
        #    create_snapshot with serialize_commit (exact cut point), the
        #    durable write + log compaction then complete asynchronously —
        #    poll for both the snapshot index and the WAL v2 segment
        #    compaction (min log-*.seg first index advances past 1).
        leader = H.find_leader(nodes)
        snap_idx = H.manual_snapshot(leader)
        leader.wait_committed(snap_idx)
        H.wait_until(f"leader snapshot_idx >= {snap_idx}", 20,
                     lambda: leader.snapshot_idx() >= snap_idx)
        H.wait_until("leader WAL compaction", 20,
                     lambda: H.wal_segment_first_indexes(leader)
                     and H.wal_segment_first_indexes(leader)[0] > 1)
        first_indexes = H.wal_segment_first_indexes(leader)
        H.log(f"manual snapshot at idx {snap_idx}; leader WAL segments "
              f"start at {first_indexes[0]} ({len(first_indexes)} segment"
              f" file(s))")

        # 8. Clean shutdown of all three via SIGTERM.
        for node in nodes:
            node.terminate()

        elapsed = time.monotonic() - started
        H.log(f"PASS in {elapsed:.1f}s")
        return 0
    except Exception as exc:  # noqa: BLE001 - dump everything on failure
        print(f"[smoke] FAIL: {exc}", file=sys.stderr)
        H.dump_node_logs(nodes)
        return 1
    finally:
        for node in nodes:
            node.force_kill()
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    BINARY = sys.argv[1]
    H.set_tag("smoke")
    sys.exit(main())
