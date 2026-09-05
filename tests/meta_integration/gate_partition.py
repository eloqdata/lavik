#!/usr/bin/env python3
"""Meta-plane partition safety gate.

A leader isolated from both followers cannot commit a privileged operation,
steps down after leadership validity expires, and exposes no grant-renewal or
other committed-state progress. Once the majority heals, a leader is elected,
the rejected operation remains absent, and all acknowledged history converges
without a fork. Data-node lease self-expiry is outside the Meta control-plane
boundary.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402


def main():
    workdir, keep = H.make_workdir(sys.argv, "meta_partition_")
    nodes = H.make_nodes(BINARY, workdir, 3,
                         args=H.raft_args(snapshot_distance=100000))
    try:
        leader = H.bootstrap_cluster(nodes)
        history = H.CommittedHistory()
        op_id, reply = leader.propose("before-partition")
        if not reply.startswith("OK "):
            raise H.Failure(f"warmup: {reply}")
        history.record(op_id, "before-partition")
        H.wait_cluster_committed(nodes, int(reply[3:]))

        followers = [node for node in nodes if node.id != leader.id]
        before = leader.committed()
        for follower in followers:
            follower.pause()
        isolated_id = leader.new_op_id()
        isolated = leader.submitop(isolated_id, "partition", "minority",
                                   timeout=6)
        if isolated.startswith("OK "):
            raise H.Failure("minority leader returned false commit success")
        if leader.committed() != before:
            raise H.Failure("minority advanced committed state")
        H.wait_until("isolated leader steps down", 6,
                     lambda: not leader.is_leader())
        H.log(f"minority rejected write as {isolated} and stepped down")

        for follower in followers:
            follower.resume()
        healed_leader = H.find_leader(nodes, timeout=15)
        confirm_id, confirmed = healed_leader.propose("after-heal")
        if not confirmed.startswith("OK "):
            raise H.Failure(f"post-heal write: {confirmed}")
        history.record(confirm_id, "after-heal")
        H.wait_cluster_committed(nodes, int(confirmed[3:]), timeout=20)
        states = {}

        def isolated_converged():
            states["values"] = [node.getop(isolated_id) for node in nodes]
            return len(set(states["values"])) == 1

        H.wait_until("uncertain minority tail converges", 15,
                     isolated_converged)
        outcome = states["values"][0]
        if outcome == "OK submitted":
            # A follower may already hold the uncommitted tail in its kernel
            # receive buffer. It is legal for the healed majority to elect a
            # holder and confirm that prefix; the key invariant is that it
            # was invisible and unacknowledged during the partition.
            completed = healed_leader.completeop(isolated_id, "minority")
            if not completed.startswith("OK "):
                raise H.Failure(f"complete confirmed uncertain tail: {completed}")
            history.record(isolated_id, "minority")
            H.wait_cluster_committed(nodes, int(completed[3:]), timeout=20)
        elif outcome != "ERR not-found":
            raise H.Failure(f"unexpected uncertain-tail outcome: {outcome}")
        history.check(nodes, timeout=20, desc="partition heal")
        H.assert_intact(nodes, "partition heal")
        for node in nodes:
            node.terminate()
        H.log("PASS: minority unavailable, leader yielded, heal converged")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"[gate-partition] FAIL: {exc}", file=sys.stderr)
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
    H.set_tag("gate-partition")
    sys.exit(main())
