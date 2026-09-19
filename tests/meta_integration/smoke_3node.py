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

"""3-node smoke test for the lavik-meta Raft driver.

Usage: smoke_3node.py /path/to/lavik-meta /path/to/lavik-ctl [workdir]

Scenario: bootstrap node1, add node2/node3 through the ctl surface,
replicate committed writes (real SubmitOperation/CompleteOperation
commands), kill -9 node3 mid-run, keep writing, restart node3 on the same
data directory and wait for catch-up (log replay and/or snapshot install),
then verify automatic + manual snapshots and a clean SIGTERM shutdown of
all nodes. Stdlib only; the whole run is budgeted well under 60 seconds.

All process/ctl plumbing (Node, wait_until, find_leader, join_and_verify,
propose_ops, workdir handling) lives in harness.py; this file is only the
smoke scenario's own orchestration plus its smoke-specific checks (the
full committed-history sweep and the WAL v1 segment compaction check).
"""

import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402

TOTAL_OPS_1 = 50
TOTAL_OPS_2 = 20


def main():
    harness_argv = [sys.argv[0], BINARY] + sys.argv[3:]
    workdir, keep = H.make_workdir(harness_argv, "meta_integration_")
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

        # Status discovery starts at a real follower and uses only the
        # committed ctl directory to reach the leader. The cluster is not
        # serving-ready yet because this repository has no first ReadyToken
        # producer, so a stable result is exit 2 rather than exit 0.
        follower = next(node for node in nodes if node.id != leader.id)
        cluster_status = subprocess.run(
            [CTL, "cluster-status", "--addr", follower.ctl_endpoint,
             "--allow-plaintext-admin", "--json"],
            capture_output=True, text=True, timeout=10)
        if (cluster_status.returncode != 2 or
                '"result":"not_ready"' not in cluster_status.stdout):
            raise H.Failure(
                "follower-seeded cluster status failed: "
                f"exit={cluster_status.returncode} "
                f"stdout={cluster_status.stdout!r} "
                f"stderr={cluster_status.stderr!r}")
        H.log("follower seed discovered leader for cluster status")

        # 3. Commit 50 operations; verify byte-identical reads everywhere.
        last_idx = H.propose_ops(leader, 0, TOTAL_OPS_1, history=history)
        H.wait_cluster_committed(nodes, last_idx)
        history.check(nodes, timeout=30, desc="initial replication")
        H.log(f"{TOTAL_OPS_1} operations replicated to all nodes "
              f"(idx {last_idx})")

        # 4. Launch discovery against a survivor, then crash the current
        # leader while those real clients are in flight. Every outcome must
        # remain a stable cut (2) or a typed retry (3), never fatal wire
        # corruption (1). The remaining quorum keeps serving.
        victim = H.find_leader(nodes)
        survivors = [node for node in nodes if node.id != victim.id]
        transition_seed = survivors[0]
        transition_probes = [
            subprocess.Popen(
                [CTL, "cluster-status", "--addr", transition_seed.ctl_endpoint,
                 "--allow-plaintext-admin", "--timeout-ms", "1000", "--json"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            for _ in range(8)
        ]
        victim.kill9()
        for probe in transition_probes:
            stdout, stderr = probe.communicate(timeout=5)
            if probe.returncode not in (2, 3):
                raise H.Failure(
                    "leader-change status probe became fatal: "
                    f"exit={probe.returncode} stdout={stdout!r} "
                    f"stderr={stderr!r}")
            expected_result = ("not_ready" if probe.returncode == 2
                               else "retryable")
            if f'"result":"{expected_result}"' not in stdout:
                raise H.Failure(
                    "leader-change status result disagreed with exit code: "
                    f"exit={probe.returncode} stdout={stdout!r}")
        leader = H.find_leader(survivors)
        # There are no Data sessions to refresh runtime eligibility, and no
        # further Meta command is issued before this status read. Reconciliation
        # must follow Raft's leader hint on its own after election.
        cluster_status = subprocess.run(
            [CTL, "cluster-status", "--addr", leader.ctl_endpoint,
             "--allow-plaintext-admin", "--json"],
            capture_output=True, text=True, timeout=10)
        if (cluster_status.returncode != 2 or
                '"result":"not_ready"' not in cluster_status.stdout):
            raise H.Failure(
                "status probed or required the unavailable follower: "
                f"exit={cluster_status.returncode} "
                f"stdout={cluster_status.stdout!r} "
                f"stderr={cluster_status.stderr!r}")
        last_idx = H.propose_ops(leader, TOTAL_OPS_1, TOTAL_OPS_2,
                                 history=history)
        H.wait_cluster_committed(survivors, last_idx)
        history.check(survivors, timeout=30, desc="quorum after crash")
        H.log(f"{TOTAL_OPS_2} more operations with node {victim.id} down "
              f"(idx {last_idx})")

        # 5. Restart the former leader on the same data dir; it must catch up
        #    via log replay and/or a snapshot install (distance 30 makes the leader
        #    compact while it is down). start() blocks until the ctl
        #    surface answers.
        victim.start(bootstrap=False)
        victim.wait_committed(last_idx, timeout=30)
        history.check([victim], timeout=30, desc="former leader catch-up")
        H.log(f"node {victim.id} caught up after restart")

        # 6. Automatic snapshots fired on every node (and compaction with
        #    reserved_log_items=0 leaves the log starting past index 1).
        for node in nodes:
            H.wait_until(f"node {node.id} automatic snapshot", 20,
                         lambda node=node: node.snapshot_idx() > 0)
        H.log("automatic snapshots observed on all nodes")

        # 7. Manual snapshot returns only after durable publication. Check the
        # retained logical suffix; whole-file GC is exercised by the Go tests
        # with forced WAL rotation (small smoke logs share one etcd segment).
        leader = H.find_leader(nodes)
        snap_idx = H.manual_snapshot(leader)
        leader.wait_committed(snap_idx)
        H.wait_until(f"leader snapshot_idx >= {snap_idx}", 20,
                     lambda: leader.snapshot_idx() >= snap_idx)
        H.wait_until("leader log compaction", 20,
                     lambda: int(leader.status()["first_log_idx"]) > 1)
        H.log(f"manual snapshot at idx {snap_idx}; first retained log "
              f"index {leader.status()['first_log_idx']}")

        # 8. Remove quorum and prove the same external command reports a
        # retryable control-plane outage (exit 3), rather than claiming the
        # previously stable NOT READY cut remains current.
        leader = H.find_leader(nodes)
        for node in nodes:
            if node.id != leader.id:
                node.kill9()
        time.sleep(0.8)
        no_quorum = subprocess.run(
            [CTL, "cluster-status", "--addr", leader.ctl_endpoint,
             "--allow-plaintext-admin", "--timeout-ms", "1000", "--json"],
            capture_output=True, text=True, timeout=5)
        if (no_quorum.returncode != 3 or
                '"result":"retryable"' not in no_quorum.stdout):
            raise H.Failure(
                "no-quorum cluster status was not retryable: "
                f"exit={no_quorum.returncode} stdout={no_quorum.stdout!r} "
                f"stderr={no_quorum.stderr!r}")
        H.log("no-quorum cluster status returned RETRYABLE/3")

        # 9. Clean shutdown of the remaining process via SIGTERM.
        leader.terminate()

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
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(2)
    BINARY = sys.argv[1]
    CTL = sys.argv[2]
    H.set_tag("smoke")
    sys.exit(main())
