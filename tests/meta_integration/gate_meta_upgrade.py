#!/usr/bin/env python3
"""Production metadata upgrade and uncommitted-tail gate (issue #19).

The first scenario attests an N-1 member as schema [1,1], proves the committed
v2 write-format switch is rejected while it is present, removes it, commits
the switch, then rolling-restarts every member and verifies v2 writes and
snapshot recovery. The frozen v1 byte fixture is loaded by
MetaModelCommands.LoadsFrozenNMinusOneFixture.

The remaining scenarios force an acknowledged-as-uncertain local tail with a
short client timeout and no quorum. If a node without the tail wins, the
operation is never visible; if the tail holder retains leadership and regains
quorum, it becomes visible only after quorum confirmation.
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402


def join_with_schema(leader, node, min_schema, max_schema, timeout=30):
    deadline = time.monotonic() + timeout
    invited = False
    while time.monotonic() < deadline:
        reply = leader.ctl(
            f"addsrv {node.id} {node.endpoint} keylane://meta/{node.id} "
            f"{min_schema} {max_schema}")
        if reply not in ("OK", "ERR joining", "ERR config-changing",
                         "ERR already-exists"):
            raise H.Failure(f"schema join node {node.id}: {reply}")
        invited = invited or reply in ("OK", "ERR already-exists")
        if invited:
            op_id, probe = leader.propose("upgrade-join")
            if probe.startswith("OK "):
                try:
                    H.wait_until(
                        f"node {node.id} receives schema-join probe", 4,
                        lambda: node.getop(op_id) ==
                        "OK completed upgrade-join")
                    return
                except H.Failure:
                    pass
        time.sleep(0.2)
    raise H.Failure(f"node {node.id} did not join with schema attestation")


def scenario_rolling_upgrade(binary, workdir):
    H.log("scenario A: schema attestation gate and rolling restart")
    root = os.path.join(workdir, "rolling")
    os.makedirs(root, exist_ok=True)
    args = H.raft_args(snapshot_distance=20, reserved_log_items=200)
    nodes = H.make_nodes(binary, root, 3, args=args)
    old = H.Node(V1_BINARY, root, 4, args=args)
    all_nodes = nodes + [old]
    try:
        leader = H.bootstrap_cluster(nodes)
        if any(node.schema() != 1 for node in nodes):
            raise H.Failure("new binaries must continue writing schema v1 "
                            "before the committed switch")

        old.start(bootstrap=False)
        join_with_schema(leader, old, 1, 1)
        rejected = leader.setschema(2, "mixed-window")
        if not rejected.startswith("ERR "):
            raise H.Failure(f"schema v2 accepted with N-1 member: {rejected}")
        if any(node.schema() != 1 for node in nodes):
            raise H.Failure("rejected schema switch changed committed state")
        H.log(f"N-1 [1,1] member blocks v2 switch: {rejected}")

        removed = leader.ctl(f"removesrv {old.id}")
        if removed != "OK":
            raise H.Failure(f"remove N-1 member: {removed}")
        old.kill9()
        switched = leader.setschema(2, "all-members-v2")
        if not switched.startswith("OK "):
            raise H.Failure(f"schema v2 switch: {switched}")
        H.wait_until("all members observe schema v2", 15,
                     lambda: all(node.schema() == 2 for node in nodes))

        # A fresh v1-only executable cannot re-enter after the committed
        # switch. Claim [1,2] in addsrv to prove the operator-supplied aux is
        # not trusted: its transport hello still advertises the compiled
        # [1,1], and the leader rejects the mismatch.
        reentry = H.Node(V1_BINARY, root, 5, args=args)
        all_nodes.append(reentry)
        reentry.start(bootstrap=False)
        reply = leader.ctl(
            f"addsrv {reentry.id} {reentry.endpoint} "
            f"keylane://meta/{reentry.id} 1 2")
        if reply not in ("OK", "ERR joining", "ERR config-changing"):
            raise H.Failure(f"attempt old-binary re-entry: {reply}")
        op_id, committed = leader.propose("reject-old-reentry")
        if not committed.startswith("OK "):
            raise H.Failure(f"cluster stalled by old-binary re-entry: "
                            f"{committed}")
        try:
            time.sleep(4)
            if not reentry.alive():
                raise H.Failure(
                    "v1-only binary received incompatible schema bytes "
                    "instead of being rejected by the transport handshake")
            try:
                old_view = reentry.getop(op_id)
            except OSError:
                old_view = "ERR disconnected"
            if old_view == "OK completed reject-old-reentry":
                raise H.Failure("v1-only binary re-entered a schema v2 group")
        finally:
            leader.ctl(f"removesrv {reentry.id}")
            reentry.force_kill()

        history = H.CommittedHistory()
        for step, node in enumerate(nodes):
            leader = H.find_leader(nodes)
            value = f"v2-before-restart-{step}"
            op_id, reply = leader.propose(value)
            if not reply.startswith("OK "):
                raise H.Failure(f"v2 write before restart {step}: {reply}")
            history.record(op_id, value)
            pre = node.committed()
            node.terminate()
            node.start(bootstrap=False)
            H.wait_no_regress(node, pre)
            H.wait_until(f"node {node.id} restores schema v2", 15,
                         lambda: node.schema() == 2)
            history.check([node], timeout=20,
                          desc=f"rolling restart node {node.id}")

        leader = H.find_leader(nodes)
        snap = H.manual_snapshot(leader)
        victim = next(node for node in nodes if node.id != leader.id)
        pre = victim.committed()
        victim.kill9()
        victim.start(bootstrap=False)
        H.wait_no_regress(victim, pre)
        H.wait_until("v2 snapshot state survives restart", 15,
                     lambda: victim.schema() == 2)
        history.check(nodes, timeout=20, desc="v2 final history")
        H.log(f"schema v2 survived rolling restart and snapshot {snap}")
        for node in nodes:
            node.terminate()
    finally:
        for node in all_nodes:
            node.force_kill()


def tail_cluster(binary, workdir, name):
    root = os.path.join(workdir, name)
    os.makedirs(root, exist_ok=True)
    args = H.raft_args(snapshot_distance=100000, election_ms_low=1500,
                       election_ms_high=2000) + [
        "--client-req-timeout-ms", "200"]
    nodes = H.make_nodes(binary, root, 3, args=args)
    mesh = H.Mesh()
    H.bootstrap_meshed_cluster(nodes, mesh)
    return nodes, mesh


def uncertain_submit(leader, followers, mesh, label):
    for follower in followers:
        mesh.proxy(follower.id).set_drop()
    op_id = leader.new_op_id()
    before = leader.committed()
    # NuRaft's async return mode completes an uncommitted request when the
    # leader resigns, not at client_req_timeout_. Waiting for that resignation
    # made the "tail holder regains quorum" branch race its 2-second leadership
    # expiry. Model the real client contract instead: the caller times out and
    # disconnects while the proposal remains an unresolved local tail.
    try:
        reply = leader.submitop(op_id, "tail", label, timeout=0.25)
    except OSError:
        reply = "ERR client-timeout"
    if reply.startswith("OK "):
        raise H.Failure(f"{label}: no-quorum submit returned false success")
    if leader.committed() != before:
        raise H.Failure(f"{label}: no-quorum committed index advanced")
    H.log(f"{label}: client outcome uncertain as {reply}")
    return op_id


def scenario_tail_truncated(binary, workdir):
    H.log("scenario B: tail holder loses election, tail is truncated")
    nodes, mesh = tail_cluster(binary, workdir, "tail_truncated")
    try:
        leader = H.find_leader(nodes)
        followers = [node for node in nodes if node.id != leader.id]
        op_id = uncertain_submit(leader, followers, mesh, "truncate-tail")
        leader.kill9()
        for follower in followers:
            mesh.proxy(follower.id).heal()
        new_leader = H.find_leader(followers)
        _, committed = new_leader.propose("truncate-confirm")
        if not committed.startswith("OK "):
            raise H.Failure(f"truncate branch confirmation: {committed}")
        leader.start(bootstrap=False)
        H.wait_cluster_committed(nodes, int(committed[3:]), timeout=20)
        H.wait_until(
            "truncated tail disappears on every node", 15,
            lambda: all(node.getop(op_id) == "ERR not-found"
                        for node in nodes))
        H.log("uncommitted tail absent after non-holder election")
        for node in nodes:
            node.terminate()
    finally:
        for node in nodes:
            node.force_kill()
        mesh.close()


def scenario_tail_committed(binary, workdir):
    H.log("scenario C: tail holder regains quorum, prefix commits legally")
    nodes, mesh = tail_cluster(binary, workdir, "tail_committed")
    try:
        leader = H.find_leader(nodes)
        followers = [node for node in nodes if node.id != leader.id]
        op_id = uncertain_submit(leader, followers, mesh, "commit-tail")
        if leader.getop(op_id) != "ERR not-found":
            raise H.Failure("tail became visible before quorum confirmation")
        mesh.proxy(followers[0].id).heal()
        H.wait_until("tail commits after quorum returns", 15,
                     lambda: leader.getop(op_id) == "OK submitted")
        completed = leader.completeop(op_id, "commit-tail")
        if not completed.startswith("OK "):
            raise H.Failure(f"complete confirmed tail: {completed}")
        mesh.proxy(followers[1].id).heal()
        H.wait_cluster_committed(nodes, int(completed[3:]), timeout=20)
        for node in nodes:
            if node.getop(op_id) != "OK completed commit-tail":
                raise H.Failure(
                    f"node {node.id} lost legally committed tail operation")
        H.log("tail remained invisible until quorum, then committed as prefix")
        for node in nodes:
            node.terminate()
    finally:
        for node in nodes:
            node.force_kill()
        mesh.close()


def main():
    # argv[2] is the independently compiled v1-only executable; argv[3]
    # remains the optional kept workdir used for debugging.
    workdir_argv = [sys.argv[0], sys.argv[1], *sys.argv[3:]]
    workdir, keep = H.make_workdir(workdir_argv, "meta_upgrade_")
    scenarios = (scenario_rolling_upgrade, scenario_tail_truncated,
                 scenario_tail_committed)
    try:
        for scenario in scenarios:
            scenario(BINARY, workdir)
        H.log(f"PASS ({len(scenarios)}/{len(scenarios)} scenarios)")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"[gate-meta-upgrade] FAIL: {exc}", file=sys.stderr)
        return 1
    finally:
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(2)
    BINARY = sys.argv[1]
    V1_BINARY = sys.argv[2]
    H.set_tag("gate-meta-upgrade")
    sys.exit(main())
