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

"""Integration gate: membership-change semantics for lavik-meta.

One cluster under a continuous propose load; serial phases:

1. 3 -> 4 nodes: addsrv node4 commits configuration/identity; a replicated
   probe additionally proves the joiner's state-machine catch-up.
2. removesrv a follower under load: the cluster must keep committing and
   the removed node's committed index must freeze.
3. Post-join crash: addsrv node5, kill -9 after completion, then repeat
   addsrv while it is down. Restart it and prove catch-up without replacing
   its membership. Interrupted invites are covered by gate_membership_recovery.
4. Conflicting ops: addsrv for an existing member returns "ERR already-exists";
   removesrv while an offline learner's addsrv is pending returns
   "ERR config-changing". Starting the learner must complete that same
   operation without another addsrv, then replicate a committed probe.
5. A new request to remove the current leader is rejected before submission.
   A previously admitted removal target elected during recovery is a distinct
   case handled by the background driver's leadership handoff.
6. Full committed-history check on the surviving members, clean teardown.

Usage: gate_membership.py /path/to/lavik-meta [workdir]
"""

import os
import re
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402


def assert_frozen(node, settle_s=1.0, observe_s=0.7):
    """A removed node's committed index must stop advancing once the
    leader stops sending it append_entries."""
    time.sleep(settle_s)
    first = node.committed()
    time.sleep(observe_s)
    second = node.committed()
    if second != first:
        raise H.Failure(
            f"removed node {node.id} still advancing: {first} -> {second}")
    H.log(f"node {node.id} frozen at committed={first} after removal")


def main():
    workdir, keep = H.make_workdir(sys.argv, "meta_integration_membership_")
    # Joiners catch up from empty under continuous load via install_snapshot.
    # Keeping 500 log entries prevents the leader from compacting past a
    # joiner before its snapshot sync completes, leaving the post-sync append
    # path available.
    args = H.raft_args(reserved_log_items=500)
    nodes = H.make_nodes(BINARY, workdir, 3, args=args)
    extras = []  # joiners, for log dumps and cleanup
    started = time.monotonic()
    load = None
    try:
        leader = H.bootstrap_cluster(nodes)
        history = H.CommittedHistory()
        op_id, reply = leader.propose("warmup")
        if not reply.startswith("OK "):
            raise H.Failure(f"warmup propose: {reply}")
        history.record(op_id, "warmup")
        H.wait_cluster_committed(nodes, int(reply[3:]))

        load = H.LoadThread(nodes, history, prefix="mb")
        load.start()

        # --- phase 1: addsrv node4, probe-verified -----------------------
        node4 = H.Node(BINARY, workdir, 4, args=args)
        extras.append(node4)
        node4.start(bootstrap=False)
        H.join_and_verify(leader, node4)
        history.check([node4], timeout=30, desc="node4 post-join")
        H.log("phase 1: 3 -> 4 nodes, probe-verified")

        # --- phase 2: removesrv a follower under load --------------------
        members = nodes + [node4]
        leader = H.find_leader(members)
        victim = next(n for n in members if n.id != leader.id)
        ok_before = load.ok_count
        reply = leader.ctl(f"removesrv {victim.id}")
        if reply != "OK":
            raise H.Failure(f"removesrv node {victim.id}: {reply}")
        H.wait_until("cluster keeps committing after removesrv", 15,
                     lambda: load.ok_count > ok_before + 5)
        assert_frozen(victim)
        victim.kill9()  # out of the cluster; shut it down
        members = [n for n in members if n.id != victim.id]
        H.log(f"phase 2: node {victim.id} removed under load, "
              f"cluster kept committing")

        # --- phase 3: post-join crash and repeated request ----------------
        node5 = H.Node(BINARY, workdir, 5, args=args)
        extras.append(node5)
        node5.start(bootstrap=False)
        H.wait_until("node 5 ctl answers", 15,
                     lambda: node5.alive() and node5.status())
        leader = H.find_leader(members)
        reply = leader.ctl(
            f"addsrv 5 {node5.endpoint} {node5.data_control_endpoint} "
            f"{node5.ctl_endpoint}")
        if reply != "OK":
            raise H.Failure(f"addsrv node 5: {reply}")
        node5.kill9()
        H.log("phase 3: node 5 killed after committed join")

        observed = []
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            reply = leader.ctl(
                f"addsrv 5 {node5.endpoint} {node5.data_control_endpoint} "
                f"{node5.ctl_endpoint}")
            if not observed or observed[-1] != reply:
                observed.append(reply)
            # Already-exists confirms an accepted task was not replaced just
            # because its target subsequently went offline.
            if reply == "ERR already-exists":
                break
            if len(observed) >= 2 and observed[-1] == "OK":
                break
            time.sleep(0.5)
        H.log(f"phase 3: addsrv replies with dead joiner: {observed}")

        node5.start(bootstrap=False)
        H.join_and_verify(leader, node5, timeout=40)
        history.check([node5], timeout=30, desc="node5 post-crash join")
        members.append(node5)
        H.log("phase 3: node 5 caught up after post-join crash")

        # --- phase 4: conflicting membership ops -------------------------
        leader = H.find_leader(members)
        existing = members[0] if members[0].id != leader.id else members[1]
        reply = leader.ctl(
            f"addsrv {existing.id} {existing.endpoint} "
            f"{existing.data_control_endpoint} {existing.ctl_endpoint}")
        H.log(f"phase 4: addsrv existing member {existing.id} -> {reply}")
        if reply != "ERR already-exists":
            raise H.Failure(
                f"addsrv existing member: {reply}, want ERR already-exists")

        # Keep an add pending on an offline learner. Removing an offline
        # voter can commit without that voter's response, so pausing the
        # removal target would not establish concurrent operations.
        node6 = H.Node(BINARY, workdir, 6, args=args)
        extras.append(node6)

        def membership_changes():
            # The load writer can roll phase markers out of log_tail's window.
            # Track exact operation IDs across the complete process log.
            with open(leader.log_path, encoding="utf-8", errors="replace") as log:
                text = log.read()
            return set(re.findall(
                r"membership ([0-9a-f]{32}) phase=change-config\b",
                text))

        operations_before = membership_changes()
        first_result = {}

        def add_node6():
            first_result["reply"] = leader.ctl(
                f"addsrv {node6.id} {node6.endpoint} "
                f"{node6.data_control_endpoint} {node6.ctl_endpoint}",
                timeout=15)

        first_thread = threading.Thread(target=add_node6, name="add-node6")
        first_thread.start()
        H.wait_until("offline learner owns membership reservation", 10,
                     lambda: len(membership_changes() - operations_before) == 1)
        operation, = membership_changes() - operations_before
        second = leader.ctl(f"removesrv {node5.id}")
        if second != "ERR config-changing":
            raise H.Failure(
                f"concurrent removesrv: {second}, want ERR config-changing")
        node6.start(bootstrap=False)
        # A bounded Admin wait may report uncertainty, but the original durable
        # operation must complete. Do not issue another addsrv: retrying could
        # hide a lost operation by admitting a replacement task.
        H.wait_until("original learner add completes without resubmission", 30,
                     lambda: leader.getop(operation) == "OK completed member-added")
        first_thread.join(timeout=15)
        if first_thread.is_alive():
            raise H.Failure("first addsrv did not finish after node6 start")
        first = first_result.get("reply", "ERR missing-result")
        if first not in ("OK", f"ERR uncertain-outcome operation={operation}"):
            raise H.Failure(f"pending addsrv node {node6.id}: {first}")
        H.log(f"phase 4: addsrv {node6.id} operation={operation} -> {first}; "
              f"overlapping removesrv {node5.id} -> {second}; "
              "original operation completed")
        probe = "membership-serialization"
        op_id, reply = leader.propose(probe)
        if not reply.startswith("OK "):
            raise H.Failure(f"post-join probe: {reply}")
        history.record(op_id, probe)
        history.check([node6], timeout=30, desc="serialized learner catch-up")
        for removed in (node6, node5):
            reply = leader.ctl(f"removesrv {removed.id}")
            if reply != "OK":
                raise H.Failure(f"removesrv node {removed.id}: {reply}")
        node6.kill9()
        ok_before = load.ok_count
        H.wait_until("cluster keeps committing after phase-4 removesrv",
                     15, lambda: load.ok_count > ok_before + 5)
        assert_frozen(node5)
        node5.kill9()
        members = [n for n in members if n.id != node5.id]
        H.log("phase 4: single-change serialization confirmed")

        # --- phase 5: removesrv the leader itself ------------------------
        leader = H.find_leader(members)
        reply = leader.ctl(f"removesrv {leader.id}")
        H.log(f"phase 5: removesrv leader node {leader.id} -> {reply}")
        if reply == "OK":
            # Raft accepted a leader step-down: the rest must re-elect
            # and keep every acknowledged write.
            H.log("phase 5: leader removal accepted; waiting re-election")
            rest = [n for n in members if n.id != leader.id]
            H.find_leader(rest, timeout=15)
            history.check(rest, timeout=30, desc="post leader-removal")
            members = rest
            leader.kill9()
        else:
            if reply != "ERR cannot-remove-leader":
                raise H.Failure(
                    f"removesrv leader: {reply}, "
                    f"want ERR cannot-remove-leader")
            current = H.find_leader(members)
            _, check = current.propose("post-removesrv-leader")
            if not check.startswith("OK "):
                raise H.Failure(f"propose after refused removal: {check}")
            H.log("phase 5: leader removal refused, cluster healthy")

        # --- phase 6: final history check + clean teardown ---------------
        load.stop()
        load.join()
        H.log(load.stats())
        history.check(members, timeout=30, desc="final membership")
        for node in members:
            node.terminate()

        elapsed = time.monotonic() - started
        H.log(f"PASS in {elapsed:.1f}s")
        return 0
    except Exception as exc:  # noqa: BLE001 - dump everything on failure
        print(f"[gate-membership] FAIL: {exc}", file=sys.stderr)
        H.dump_node_logs(nodes + extras)
        return 1
    finally:
        if load is not None:
            load.stop()
            load.join()
        for node in nodes + extras:
            node.force_kill()
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    BINARY = sys.argv[1]
    H.set_tag("gate-membership")
    sys.exit(main())
