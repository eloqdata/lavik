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

"""Transport connect/cancel/timeout behavior at cluster level.

Single 3-node cluster on the proxy mesh, continuous propose load
throughout:

1. Half-open peer, 3 rounds: the follower's inbound proxy accepts but
   blackholes every byte. The Go transport contract under test: the
   leader's sends to that peer must fail inside bounded RPC timeouts, never
   hang forever. Observable evidence: (a) the quorum keeps committing
   while the follower's committed index stays frozen, (b) the leader's
   transport-failure counter increases after a bounded handshake or write
   deadline. Each round ends with heal()
   and a bounded (<15s) catch-up.
2. Refused peer, 1 round: refuse mode (ECONNREFUSED) exercises the fast
   connect-failure path with the same assertions.
3. Cancel/race stress: ~20 rapid drop/heal flaps at 200-400ms cadence on
   the follower's link while the load keeps proposing. Afterwards the
   cluster must fully converge in bounded time with the committed history
   intact — proving exactly-once handler drains and late-response
   filtering never wedge a peer's busy flag — and no node may have
   crashed or hung its ctl surface.

Usage: gate_transport.py /path/to/lavik-meta [workdir]
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402

DROP_WINDOW_S = 4.0     # Longer than the configured Raft request timeout.
REFUSE_WINDOW_S = 2.0
CATCHUP_ASSERT_S = 15.0
FLAP_CYCLES = 20


def rpc_failure_count(nodes):
    """Count across members so an election cannot move the evidence stream."""
    return sum(int(node.status()["rpc_failures"])
               for node in nodes if node.alive())


def timed_catchup(follower, target_idx, desc):
    started = time.monotonic()
    follower.wait_committed(target_idx, timeout=CATCHUP_ASSERT_S)
    elapsed = time.monotonic() - started
    H.log(f"{desc}: node {follower.id} caught up to {target_idx} in "
          f"{elapsed:.2f}s (< {CATCHUP_ASSERT_S}s)")
    return elapsed


def fault_round(nodes, mesh, history, mode, window_s, round_name):
    """One inbound-fault round on a follower: cluster must keep committing,
    the follower must stall, the failure must be observable on the leader,
    and heal() must restore the follower within CATCHUP_ASSERT_S."""
    leader = H.find_leader(nodes)
    follower = next(n for n in nodes if n.id != leader.id and n.alive())
    proxy = mesh.proxy(follower.id)
    evidence0 = rpc_failure_count(nodes)
    c0 = H.max_committed([n for n in nodes if n.id != follower.id])
    f0 = follower.committed()

    if mode == "drop":
        proxy.set_drop()
        detail = "half-open blackhole"
    else:
        proxy.set_refuse()
        detail = "connection refused"
    H.log(f"{round_name}: {mode} ({detail}) on node {follower.id} "
          f"inbound for {window_s}s")
    time.sleep(window_s)

    quorum = [n for n in nodes if n.id != follower.id]
    c1 = H.max_committed(quorum)
    if c1 <= c0:
        raise H.Failure(f"{round_name}: quorum stalled during {mode} "
                        f"(committed {c0} -> {c1})")
    # Status and proposal are separate requests. The injected asymmetric
    # partition can elect another leader between them; require a real commit
    # within a fixed deadline rather than treating the status as a lease.
    def commits_during_fault():
        for candidate in quorum:
            if not candidate.is_leader():
                continue
            _, reply = candidate.propose(f"{round_name}-live")
            if reply.startswith("OK "):
                return True
        return False

    H.wait_until(f"{round_name}: quorum commits during {mode}", 5,
                 commits_during_fault)
    f1 = follower.committed()
    if f1 > f0 + 10:
        raise H.Failure(f"{round_name}: isolated node {follower.id} kept "
                        f"advancing ({f0} -> {f1}); fault not effective")
    # Network completion and status publication are asynchronous with respect
    # to the control socket. Keep the fault active for a short bounded grace
    # period and sample all members: comparing two instantaneous counts from
    # one leader made this gate flaky when an election moved the log stream or
    # the timeout callback landed just after the ctl proposal completed.
    deadline = time.monotonic() + 2.0
    evidence1 = rpc_failure_count(nodes)
    while evidence1 <= evidence0 and time.monotonic() < deadline:
        time.sleep(0.05)
        evidence1 = rpc_failure_count(nodes)
    proxy.heal()

    if evidence1 <= evidence0:
        raise H.Failure(
            f"{round_name}: no peer RPC failure evidence in transport counters; "
            f"Raft transport may be hanging instead of timing out")
    H.log(f"{round_name}: quorum committed {c0} -> {c1}, node "
          f"{follower.id} frozen at {f1}; cluster RPC failure/reconnect "
          f"signals {evidence0} -> {evidence1}")

    timed_catchup(follower, c1, f"{round_name} post-heal")
    history.check(nodes, timeout=30, desc=f"{round_name} history")
    H.assert_intact(nodes, round_name)
    return follower.id


def flap_stress(nodes, mesh, history, follower_id):
    """Rapid drop/heal flapping: every flap cuts established connections
    and forces client recreation, hammering the connection owner's connect/cancel
    and exactly-once drain paths."""
    proxy = mesh.proxy(follower_id)
    H.log(f"flap: {FLAP_CYCLES} drop/heal cycles on node {follower_id} "
          f"inbound at 200-400ms cadence")
    for cycle in range(FLAP_CYCLES):
        proxy.set_drop()
        time.sleep(0.20 if cycle % 2 == 0 else 0.35)
        proxy.heal()
        time.sleep(0.35 if cycle % 2 == 0 else 0.20)
    proxy.heal()
    H.log("flap: done; waiting for full convergence")

    leader = H.find_leader(nodes, timeout=20)
    _, reply = leader.propose("post-flap")
    if not reply.startswith("OK "):
        raise H.Failure(f"post-flap propose: {reply}")
    H.wait_until("post-flap committed convergence", 30,
                 lambda: all(n.committed() >= int(reply[3:])
                             for n in nodes if n.alive()))
    history.check(nodes, timeout=30, desc="post-flap history")
    H.assert_intact(nodes, "post-flap")
    for node in nodes:
        node.wait_status(lambda s: True, f"node {node.id} ctl alive", 5)
    H.log("flap: cluster converged, history intact, all nodes responsive")


def main():
    workdir, keep = H.make_workdir(sys.argv, "meta_integration_transport_")
    # reserved_log_items=500: after a completed install_snapshot the
    # follower still needs the entries between its snapshot and the
    # leader's live log. With the smoke's reserved=0 and distance=30 under
    # sustained load, the leader compacts those away during the sync and
    # the follower chases one stale snapshot after another; a 500-entry
    # reserve window keeps the post-sync append path open. Compaction still fires every 30 entries.
    args = H.raft_args(reserved_log_items=500)
    nodes = H.make_nodes(BINARY, workdir, 3, args=args)
    mesh = H.Mesh()
    started = time.monotonic()
    load = None
    try:
        leader = H.bootstrap_meshed_cluster(nodes, mesh)

        history = H.CommittedHistory()
        op_id, reply = leader.propose("warmup")
        if not reply.startswith("OK "):
            raise H.Failure(f"warmup propose: {reply}")
        history.record(op_id, "warmup")
        H.wait_cluster_committed(nodes, int(reply[3:]))

        load = H.LoadThread(nodes, history, prefix="tp")
        load.start()
        time.sleep(1.0)
        if load.ok_count < 5:
            raise H.Failure(f"load thread made no progress: {load.stats()}")

        # Phase 1: half-open blackhole, 3 rounds.
        dropped_on = None
        for round_no in (1, 2, 3):
            dropped_on = fault_round(nodes, mesh, history, "drop",
                                     DROP_WINDOW_S, f"drop-round{round_no}")

        # Phase 2: refused connections, 1 round (fast connect-fail path).
        fault_round(nodes, mesh, history, "refuse", REFUSE_WINDOW_S,
                    "refuse-round")

        # Phase 3: rapid link flapping on the same victim.
        flap_stress(nodes, mesh, history, dropped_on)

        load.stop()
        load.join()
        H.log(load.stats())
        history.check(nodes, timeout=30, desc="final")
        H.assert_intact(nodes, "final")
        for node in nodes:
            node.terminate()

        elapsed = time.monotonic() - started
        H.log(f"PASS in {elapsed:.1f}s")
        return 0
    except Exception as exc:  # noqa: BLE001 - dump everything on failure
        print(f"[gate-transport] FAIL: {exc}", file=sys.stderr)
        H.dump_node_logs(nodes)
        return 1
    finally:
        if load is not None:
            load.stop()
            load.join()
        for node in nodes:
            node.force_kill()
        mesh.close()
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    BINARY = sys.argv[1]
    H.set_tag("gate-transport")
    sys.exit(main())
