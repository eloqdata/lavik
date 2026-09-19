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

"""Gate: an offline three-voter member cannot win after a fourth member joins.

The surviving current member must reject stale RequestVote messages with a
retained WAL, an entirely compacted WAL, and after reopening that snapshot.
Inspect internal election decisions as well as public status: an invalid
leader can stall before it reaches the public leader callback.

Usage: gate_snapshot_vote.py /path/to/lavik-meta [workdir]
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
            node.status()["initial_bindings_pending"] == "0"
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
            H.wait_until("snapshot durable and logical log compacted", 10,
                         lambda: survivor.snapshot_idx() == snapshot_index
                         and int(survivor.status()["first_log_idx"]) == snapshot_index + 1)


        removed = [node for node in active if node is not survivor]
        for node in removed:
            node.kill9()
        if mode == "snapshot_restart":
            survivor.terminate()
            survivor.start()
            if survivor.snapshot_idx() != snapshot_index:
                raise H.Failure("restart did not recover the compacted snapshot")

        minority = [survivor, late]
        before_votes = survivor.status()
        late.start()
        # Require actual denied Vote/PreVote replies, rather than inferring
        # log freshness solely from the absence of a public leader.
        H.wait_until("stale candidate vote rejected", 6,
                     lambda: int(survivor.status()["vote_rejections"]) >
                     int(before_votes["vote_rejections"]))
        time.sleep(1)
        if int(survivor.status()["vote_grants"]) != int(before_votes["vote_grants"]):
            raise H.Failure(f"{mode}: compacted member granted a stale vote")
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
