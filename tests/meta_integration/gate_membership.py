#!/usr/bin/env python3
"""Integration gate: membership-change semantics for keylane-meta.

One cluster under a continuous propose load; serial phases:

1. 3 -> 4 nodes: addsrv node4, proven by a real replicated probe operation
   (addsrv's OK only means the invite was accepted, never that the config
   committed — the probe is the proof).
2. removesrv a follower under load: the cluster must keep committing and
   the removed node's committed index must freeze.
3. Invite-crash retry: addsrv node5, kill -9 it mid-invite, then poll
   addsrv with the joiner down and record NuRaft's actual replies (join
   serialization and/or activity-timeout reset). Restart node5 and drive
   addsrv until the probe proves it joined. The contract being gated is
   "single config change at a time, retryable" — the exact reply sequence
   is observed and logged.
4. Conflicting ops: addsrv for an existing member (NuRaft
   SERVER_ALREADY_EXISTS -> "ERR already-exists") and a second removesrv
   while one is in flight (SERVER_IS_LEAVING -> "ERR leaving"). The first
   target is paused and the commands use separate ctl sessions so the overlap
   is deterministic.
5. removesrv the leader itself: record NuRaft's actual behavior. In this
   build (third_party/nuraft @ 0b01b18) handle_rm_srv_req refuses it with
   CANNOT_REMOVE_LEADER; if that ever changes to an accepted step-down,
   the gate follows the new behavior and still requires no data loss.
6. Full committed-history check on the surviving members, clean teardown.

Usage: gate_membership.py /path/to/keylane-meta [workdir]
"""

import os
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

        # --- phase 3: invite-crash retry ---------------------------------
        node5 = H.Node(BINARY, workdir, 5, args=args)
        extras.append(node5)
        node5.start(bootstrap=False)
        H.wait_until("node 5 ctl answers", 15,
                     lambda: node5.alive() and node5.status())
        leader = H.find_leader(members)
        reply = leader.ctl(
            f"addsrv 5 {node5.endpoint} {node5.data_control_endpoint}")
        if reply != "OK":
            raise H.Failure(f"addsrv node 5: {reply}")
        node5.kill9()
        H.log("phase 3: node 5 killed mid-invite")

        observed = []
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            reply = leader.ctl(
                f"addsrv 5 {node5.endpoint} {node5.data_control_endpoint}")
            if not observed or observed[-1] != reply:
                observed.append(reply)
            # Once a retry gets OK again the leader reset the dead join;
            # anything further would just pile on more invites.
            if len(observed) >= 2 and observed[-1] == "OK":
                break
            time.sleep(0.5)
        H.log(f"phase 3: addsrv replies with dead joiner: {observed}")

        node5.start(bootstrap=False)
        H.join_and_verify(leader, node5, timeout=40)
        history.check([node5], timeout=30, desc="node5 post-crash join")
        members.append(node5)
        H.log("phase 3: node 5 rejoined after invite-crash")

        # --- phase 4: conflicting membership ops -------------------------
        leader = H.find_leader(members)
        existing = members[0] if members[0].id != leader.id else members[1]
        reply = leader.ctl(
            f"addsrv {existing.id} {existing.endpoint} "
            f"{existing.data_control_endpoint}")
        H.log(f"phase 4: addsrv existing member {existing.id} -> {reply}")
        if reply != "ERR already-exists":
            raise H.Failure(
                f"addsrv existing member: {reply}, want ERR already-exists")

        # A ctl reply follows both the NuRaft result and the committed identity
        # retirement, so two calls made serially are not concurrent. Pause
        # node5 and issue the first removal on another ctl session; NuRaft
        # retains its single-change gate while awaiting the leave response.
        node5.pause()
        first_result = {}

        def remove_node5():
            first_result["reply"] = leader.ctl(
                f"removesrv {node5.id}", timeout=15)

        first_thread = threading.Thread(target=remove_node5,
                                        name="remove-node5")
        first_thread.start()
        time.sleep(0.1)
        try:
            second = leader.ctl(f"removesrv {nodes[2].id}")
        finally:
            node5.resume()
        first_thread.join(timeout=15)
        if first_thread.is_alive():
            raise H.Failure("first removesrv did not finish after node5 resume")
        first = first_result.get("reply", "ERR missing-result")
        H.log(f"phase 4: removesrv {node5.id} -> {first}; "
              f"overlapping removesrv {nodes[2].id} -> {second}")
        if first != "OK":
            raise H.Failure(f"removesrv node {node5.id}: {first}")
        if second not in ("ERR leaving", "ERR config-changing"):
            raise H.Failure(
                f"concurrent removesrv: {second}, want ERR leaving")
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
            # NuRaft accepted a leader step-down: the rest must re-elect
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
