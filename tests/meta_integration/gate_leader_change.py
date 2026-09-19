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

"""Integration gate: leader-change safety for lavik-meta.

Single 3-node cluster with ALL raft traffic flowing through the proxy
mesh (bootstrap_meshed_cluster), continuous propose load throughout:

1. Round 1: kill -9 the leader. Assert a survivor is elected well inside
   ELECTION_ASSERT_S; assert the new leader's committed index reaches the
   pre-kill cluster maximum almost immediately (the externally observable
   effect of wait_for_sm_catchup_on_becoming_leader_=true) and a fresh
   propose succeeds. Restart the old leader on the same data dir; it must
   catch up and serve the full history.
2. Round 2: kill -9 the NEW leader and repeat. Every write that ever
   returned OK must survive both failovers.
   Both rounds additionally assert the raft_callback_ trail directly (the
   [raft-cb] lines emitted by lavik-meta): the new leader logs
   BecomeLeader at a term above the victim's, timestamped at the moment
   its committed index observably caught up; the surviving follower logs
   BecomeFollower at the new term; the victim's pre-kill log shows its own
   BecomeLeader; and after rejoining, the victim logs BecomeFollower at the
   post-failover term. (A kill -9'd leader cannot log its own transition
   out — it is dead — so "old leader becomes follower" is asserted on the
   surviving follower pre-restart and on the victim post-restart.)
3. Link-fault sanity on the mesh: per-chunk delay, then half-open drop,
   then refuse (RST) on one follower's inbound path, healing between
   rounds. These are asymmetric faults (the follower's outbound traffic
   still flows), so term churn and extra elections are legitimate; the
   assertions are safety (no recorded write lost) plus bounded
   reconvergence, never tight timing.
4. Stop the load, run the full committed-history check, and shut all
   nodes down cleanly.

Usage: gate_leader_change.py /path/to/lavik-meta [workdir]
"""

from datetime import datetime
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402

ELECTION_ASSERT_S = 5.0
# Slack for comparing a [raft-cb] log timestamp with the wall-clock moment
# the gate first observes its externally visible effect. The ctl status poll
# runs on a 50ms cadence plus a ctl round trip, so 2s is far outside poll
# jitter while still catching a callback that fired wildly out of order.
EVENT_TS_SLACK_S = 2.0

CB_LINE = re.compile(
    r"^\[n\d+\] (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}) \[info\]"
    r" \[raft-cb\] event=(\w+)")
CB_TERM = re.compile(r"\bterm=(\d+)")


def line_count(node):
    """Current number of lines in the node's log; used as a marker so a
    post-restart scan only sees lines written after the restart (the log
    file appends across restarts)."""
    try:
        with open(node.log_path, "r", errors="replace") as handle:
            return sum(1 for _ in handle)
    except OSError:
        return 0


def raft_cb_events(node, event, since_line=0):
    """(wall_ts, term) for every `[raft-cb] event=<event>` line at or past
    `since_line`; term is None when the event carries no term= field."""
    events = []
    try:
        with open(node.log_path, "r", errors="replace") as handle:
            for lineno, line in enumerate(handle):
                if lineno < since_line:
                    continue
                match = CB_LINE.match(line)
                if match is None or match.group(2) != event:
                    continue
                wall = datetime.strptime(
                    match.group(1), "%Y-%m-%dT%H:%M:%S.%f").timestamp()
                term = CB_TERM.search(line)
                events.append(
                    (wall, int(term.group(1)) if term is not None else None))
    except OSError:
        pass
    return events


def assert_election_events(round_name, victim, new_leader, survivors,
                           victim_term, kill_wall, observed_wall):
    """raft_callback_ assertions for one kill round; returns the new term.

    The role relay exposes BecomeLeader only after actual application of
    the current-term fence and fresh quorum liveness. This includes the
    commit advancement the gate observes as committed >= pre — so the callback's log timestamp must sit
    within EVENT_TS_SLACK_S of that observation point (and after the kill).
    """
    status_term = new_leader.term()
    wins = [(ts, term) for ts, term in
            raft_cb_events(new_leader, "BecomeLeader")
            if term is not None and term > victim_term]
    if not wins:
        raise H.Failure(f"{round_name}: node {new_leader.id} log has no "
                        f"BecomeLeader event above victim term {victim_term}")
    become_ts, new_term = wins[-1]
    if new_term != status_term:
        raise H.Failure(f"{round_name}: node {new_leader.id} BecomeLeader "
                        f"term {new_term} != reported term {status_term}")
    if not kill_wall - 1.0 <= become_ts <= observed_wall + EVENT_TS_SLACK_S:
        raise H.Failure(
            f"{round_name}: node {new_leader.id} BecomeLeader timestamp "
            f"{become_ts:.3f} outside [{kill_wall - 1.0:.3f}, "
            f"{observed_wall + EVENT_TS_SLACK_S:.3f}]")
    if become_ts < observed_wall - EVENT_TS_SLACK_S:
        raise H.Failure(
            f"{round_name}: node {new_leader.id} BecomeLeader timestamp "
            f"predates the observable committed catch-up by "
            f"{observed_wall - become_ts:.2f}s (> {EVENT_TS_SLACK_S}s)")
    # The surviving follower sees the winner's higher term (vote request or
    # append entries) and steps into it: BecomeFollower at the new term.
    follower = next(n for n in survivors if n.id != new_leader.id)
    if not any(term is not None and term >= new_term
               for _, term in raft_cb_events(follower, "BecomeFollower")):
        raise H.Failure(f"{round_name}: surviving follower node "
                        f"{follower.id} log has no BecomeFollower at term "
                        f">= {new_term}")
    # The victim's pre-kill log must show its own election (the process is
    # dead and its log handle closed at this point, so the file holds only
    # pre-kill content).
    if not any(term == victim_term
               for _, term in raft_cb_events(victim, "BecomeLeader")):
        raise H.Failure(f"{round_name}: victim node {victim.id} pre-kill log "
                        f"has no BecomeLeader at its term {victim_term}")
    H.log(f"{round_name}: [raft-cb] trail verified (node {new_leader.id} "
          f"BecomeLeader term={new_term}, node {follower.id} "
          f"BecomeFollower, victim node {victim.id} BecomeLeader "
          f"term={victim_term})")
    return new_term


def leader_kill_round(nodes, history, round_name):
    """kill -9 the current leader; a survivor must be elected fast, start
    with the pre-kill committed state, and accept proposals."""
    leader = H.find_leader(nodes)
    pre = H.max_committed(nodes)
    victim_term = leader.term()
    H.log(f"{round_name}: killing leader node {leader.id} "
          f"(cluster committed={pre})")
    started = time.monotonic()
    kill_wall = time.time()
    leader.kill9()
    survivors = [n for n in nodes if n.id != leader.id]
    new_leader = H.find_leader(survivors, timeout=15)
    elapsed = time.monotonic() - started
    if new_leader.id == leader.id:
        raise H.Failure(f"{round_name}: dead node {leader.id} re-elected?!")
    if elapsed > ELECTION_ASSERT_S:
        raise H.Failure(
            f"{round_name}: election took {elapsed:.1f}s, "
            f"want < {ELECTION_ASSERT_S}s")
    H.log(f"{round_name}: node {new_leader.id} elected in "
          f"{elapsed:.2f}s")
    # sm-catchup gate: once leader=1 is observable, the committed index
    # must already cover everything committed before the kill.
    H.wait_until(f"{round_name}: new leader committed >= {pre}", 8,
                 lambda: new_leader.committed() >= pre)
    observed_wall = time.time()
    _, reply = new_leader.propose(f"{round_name}-resume")
    if not reply.startswith("OK "):
        raise H.Failure(f"{round_name}: propose after election: {reply}")
    H.log(f"{round_name}: propose resumed (idx {reply[3:]})")
    new_term = assert_election_events(round_name, leader, new_leader,
                                      survivors, victim_term, kill_wall,
                                      observed_wall)
    return leader, new_leader, new_term


def restart_and_catchup(node, nodes, history, min_term):
    # Line marker before the restart: the post-restart BecomeFollower scan
    # only trusts lines appended from this boot onward.
    boot_mark = line_count(node)
    node.start(bootstrap=False)
    H.wait_until(f"node {node.id} ctl answers", 15,
                 lambda: node.alive() and node.status())
    history.check(nodes, timeout=30, desc=f"node {node.id} restart catch-up")
    # The rejoined node's stored term predates the failover, so the first
    # heartbeat bumps it: BecomeFollower at >= the post-failover term.
    if not any(term is not None and term >= min_term
               for _, term in raft_cb_events(node, "BecomeFollower",
                                             since_line=boot_mark)):
        raise H.Failure(f"node {node.id}: post-restart log has no "
                        f"BecomeFollower at term >= {min_term}")
    H.log(f"node {node.id} restarted and caught up (post-restart "
          f"BecomeFollower term >= {min_term} verified)")


def link_fault_round(nodes, mesh, history, leader, follower, mode, hold_s):
    """Inject one inbound link fault on `follower`, heal, then assert the
    cluster reconverges and no recorded write was lost."""
    proxy = mesh.proxy(follower.id)
    if mode == "delay":
        proxy.set_delay(0.25)
        detail = "250ms/chunk"
    elif mode == "drop":
        proxy.set_drop()
        detail = "half-open blackhole"
    else:
        proxy.set_refuse()
        detail = "connection refused"
    pre = H.max_committed(nodes)
    H.log(f"link fault: {mode} ({detail}) on node {follower.id} inbound "
          f"for {hold_s}s (committed={pre})")
    time.sleep(hold_s)
    proxy.heal()
    H.wait_until(f"post-{mode}: a leader exists", 20,
                 lambda: any(n.alive() and n.is_leader() for n in nodes))
    H.wait_until(f"post-{mode}: committed >= {pre}", 20,
                 lambda: H.max_committed(nodes) >= pre)
    current = H.find_leader(nodes)
    _, reply = current.propose(f"post-{mode}")
    if not reply.startswith("OK "):
        raise H.Failure(f"post-{mode}: propose: {reply}")
    history.check(nodes, timeout=30, desc=f"post-{mode}")
    H.log(f"link fault {mode}: cluster recovered, history intact")


def main():
    workdir, keep = H.make_workdir(sys.argv, "meta_integration_leader_")
    # A snapshot carries the accumulated audit window, so install_snapshot
    # takes measurable time. Keeping 500 log entries prevents the leader from
    # compacting past a healing follower before its stream ends and forcing it
    # to chase successive snapshots, while compaction still fires every 30
    # entries.
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
        history.check(nodes, timeout=15, desc="post-setup")

        load = H.LoadThread(nodes, history, prefix="lc")
        load.start()
        time.sleep(2.0)  # accumulate acknowledged writes
        if load.ok_count < 10:
            raise H.Failure(f"load thread made no progress: {load.stats()}")
        H.log(f"load running ({load.ok_count} writes OK)")

        victim1, _, term1 = leader_kill_round(nodes, history, "round1")
        restart_and_catchup(victim1, nodes, history, term1)
        victim2, _, term2 = leader_kill_round(nodes, history, "round2")
        restart_and_catchup(victim2, nodes, history, term2)
        history.check(nodes, timeout=30, desc="after two failovers")

        # Link-fault sanity on the proxy mesh (also proves the mesh modes
        # work against real raft traffic).
        leader = H.find_leader(nodes)
        follower = next(n for n in nodes
                        if n.id != leader.id and n.alive())
        link_fault_round(nodes, mesh, history, leader, follower,
                         "delay", 2.0)
        link_fault_round(nodes, mesh, history, leader, follower,
                         "drop", 1.5)
        link_fault_round(nodes, mesh, history, leader, follower,
                         "refuse", 1.0)
        for node in nodes:
            proxied = mesh.proxy(node.id).bytes_forwarded
            if proxied == 0:
                raise H.Failure(
                    f"node {node.id}: proxy forwarded 0 bytes; "
                    f"mesh is not carrying raft traffic")
            H.log(f"proxy n{node.id}: {proxied} bytes forwarded")

        load.stop()
        load.join()
        H.log(load.stats())
        history.check(nodes, timeout=30, desc="final")
        for node in nodes:
            node.terminate()

        elapsed = time.monotonic() - started
        H.log(f"PASS in {elapsed:.1f}s")
        return 0
    except Exception as exc:  # noqa: BLE001 - dump everything on failure
        print(f"[gate-leader-change] FAIL: {exc}", file=sys.stderr)
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
    H.set_tag("gate-leader-change")
    sys.exit(main())
