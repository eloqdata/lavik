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

"""Stale or incarnation-forged observations never reach committed state.

One 3-node cluster; serial phases:

1. Register a data node and build the committed anchors observation
   freshness checks match against: group g1 promoted to term 1
   (creategroup + begingroupterm).
2. Trusted session gen=1 adopted through the ctl session adapter;
   a candidate observation carrying the correct committed term is ACCEPTED
   and visible through the facts-filtered query path.
3. Forgery matrix, every entry rejected AND written to the obs audit ring:
   stale session generation (after the generation advanced), future group
   term, stale partition replication epoch, old boot incarnation under the
   current generation, and an unregistered node. The generation bump itself
   purged the earlier candidate (superseded-by-generation audit event).
4. Committed term advancement invalidates the previously accepted candidate;
   the old term is rejected and commit-driven RevalidateAll records the purge.
   Reporting at the new term restores the candidate without changing durable
   operation state.
5. Leader-local semantics: kill -9 the leader; the NEW leader's store is
   empty (candidate query returns zero) until the node re-reports through a
   fresh session on the new leader. Committed history is verified
   unaffected by all observation traffic.

Usage: gate_observation_lifecycle.py /path/to/lavik-meta [workdir]
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402

# Data-plane node id (40 lowercase hex chars, the topology convention) and
# Boot incarnations (40 hex chars = 20 bytes, opaque, never ordered).
DATA_NODE = "dd" * 20
GHOST_NODE = "ee" * 20
BOOT_A = "a1" * 20
BOOT_B = "b2" * 20
GROUP = "g1"


def expect_ok(reply, what):
    if not reply.startswith("OK"):
        raise H.Failure(f"{what}: {reply}, want OK")


def expect_err(reply, what, needle):
    if not (reply.startswith("ERR") and needle in reply):
        raise H.Failure(f"{what}: {reply}, want ERR containing {needle!r}")


def candidate_count(node, group):
    reply = node.observations(group)
    if not reply.startswith("OK candidates="):
        raise H.Failure(f"observations {group}: {reply}")
    return int(reply.split()[1].split("=")[1])


def main():
    workdir, keep = H.make_workdir(sys.argv, "meta_observations_")
    nodes = H.make_nodes(BINARY, workdir, 3)
    started = H.time.monotonic()
    try:
        leader = H.bootstrap_cluster(nodes)
        history = H.CommittedHistory()

        # --- phase 1: committed anchors ----------------------------------
        expect_ok(leader.put_authority_lease_policy(1),
                  "put Authority Lease Policy")
        expect_ok(leader.registernode(DATA_NODE, f"lavik://node/{DATA_NODE}",
                                      "primary",
                                      endpoints=("tcp://127.0.0.1:6379",)),
                  "registernode")
        expect_ok(leader.creategroup(GROUP), "creategroup")
        expect_ok(leader.assignnode(GROUP, DATA_NODE), "assignnode")
        expect_ok(leader.begingroupterm(GROUP, 0, 1), "begingroupterm 0->1")
        H.log("phase 1: node registered and assigned, group g1 at term 1")

        # --- phase 2: valid candidate accepted ---------------------------
        expect_ok(leader.adoptsession(DATA_NODE, BOOT_A, 1), "adoptsession 1")
        expect_ok(
            leader.obs_candidate(DATA_NODE, BOOT_A, 1, GROUP,
                                 term=1, manifest=0, history=7),
            "candidate with correct term")
        if candidate_count(leader, GROUP) != 1:
            raise H.Failure("accepted candidate not visible in query")
        H.log("phase 2: correct-term candidate accepted and queryable")

        # --- phase 3: forgery matrix, all rejected + audited -------------
        # Bump the session generation: the gen-1 candidate is atomically
        # purged, and gen-1 reports are stale from now on.
        expect_ok(leader.adoptsession(DATA_NODE, BOOT_A, 2), "adoptsession 2")
        if candidate_count(leader, GROUP) != 0:
            raise H.Failure("generation bump did not purge old observations")
        expect_ok(
            leader.obs_candidate(DATA_NODE, BOOT_A, 2, GROUP,
                                 term=1, manifest=0, history=7),
            "re-report at gen 2")

        expect_err(
            leader.obs_candidate(DATA_NODE, BOOT_A, 1, GROUP,
                                 term=1, manifest=0, history=7),
            "stale-generation candidate", "stale-generation")
        expect_err(
            leader.obs_candidate(DATA_NODE, BOOT_A, 2, GROUP,
                                 term=2, manifest=0, history=7),
            "future-term candidate", "term-mismatch")
        expect_err(
            leader.obs_candidate(DATA_NODE, BOOT_A, 2, GROUP,
                                 term=1, manifest=0, history=7,
                                 partition_epoch=1),
            "stale-population-epoch candidate", "partition-epoch-mismatch")
        expect_err(
            leader.obs_candidate(DATA_NODE, BOOT_B, 2, GROUP,
                                 term=1, manifest=0, history=7),
            "old-boot candidate", "boot-mismatch")
        expect_err(leader.obs_boot(GHOST_NODE, BOOT_A, 1),
                   "unregistered-node boot", "node-not-active")

        audit = leader.obsaudit()
        for needle in ("detail=superseded-by-generation:2",
                       "detail=stale-generation",
                       "detail=term-mismatch",
                       "detail=partition-epoch-mismatch",
                       "detail=boot-mismatch",
                       "detail=node-not-active"):
            if needle not in audit:
                raise H.Failure(f"obsaudit missing {needle!r}: {audit}")
        H.log("phase 3: forged generation/term/epoch/boot/node all rejected "
              "and audited")

        # --- phase 4: commit-driven observation invalidation -------------
        expect_ok(leader.begingroupterm(GROUP, 1, 2), "begingroupterm 1->2")
        if candidate_count(leader, GROUP) != 0:
            raise H.Failure("term advancement did not invalidate candidate")
        expect_err(
            leader.obs_candidate(DATA_NODE, BOOT_A, 2, GROUP,
                                 term=1, manifest=0, history=7),
            "candidate from previous term", "term-mismatch")
        # Queries and admission reject the old term against committed facts
        # immediately. The dispatch thread separately purges the retained
        # candidate, so the commit reply need not include its audit record yet.
        audit = ""

        def stale_candidate_purged():
            nonlocal audit
            audit = leader.obsaudit()
            return "detail=commit-stale:term-mismatch" in audit

        try:
            H.wait_until("stale candidate purged and audited", 10,
                         stale_candidate_purged)
        except H.Failure as error:
            raise H.Failure(f"{error}; obsaudit={audit}") from error
        expect_ok(
            leader.obs_candidate(DATA_NODE, BOOT_A, 2, GROUP,
                                 term=2, manifest=0, history=7),
            "candidate from current term")
        if candidate_count(leader, GROUP) != 1:
            raise H.Failure("current-term candidate not visible")
        H.log("phase 4: committed term advancement purged stale candidate; "
              "current-term report restored it")

        # --- phase 5: leader-local obs, committed history unaffected ------
        for value in ("pre-kill-1", "pre-kill-2"):
            op_id2, reply = leader.propose(value)
            if not reply.startswith("OK "):
                raise H.Failure(f"propose {value}: {reply}")
            history.record(op_id2, value)
        H.wait_cluster_committed(nodes, leader.committed())

        leader.kill9()
        survivors = [n for n in nodes if n.id != leader.id]
        new_leader = H.find_leader(survivors)

        # Leader-local: the surviving processes never received the reports;
        # until a fresh re-report the candidate set is empty even though the
        # committed group/term anchors exist on every node.
        if candidate_count(new_leader, GROUP) != 0:
            raise H.Failure("new leader serves observations it never "
                            "received (leader-local violation)")
        expect_ok(new_leader.adoptsession(DATA_NODE, BOOT_A, 1),
                  "re-adopt on new leader")
        expect_ok(
            new_leader.obs_candidate(DATA_NODE, BOOT_A, 1, GROUP,
                                     term=2, manifest=0, history=7),
            "re-report on new leader")
        if candidate_count(new_leader, GROUP) != 1:
            raise H.Failure("re-reported candidate not visible")
        H.log("phase 5: new leader obs empty until re-report, then rebuilt")

        # Committed history ignores observation traffic entirely: every
        # recorded operation converges, and the cluster keeps committing.
        for value in ("post-obs-1", "post-obs-2"):
            op_id3, reply = new_leader.propose(value)
            if not reply.startswith("OK "):
                raise H.Failure(f"propose {value}: {reply}")
            history.record(op_id3, value)
        history.check(survivors, timeout=30, desc="committed vs obs traffic")
        H.assert_intact(survivors, "end of gate")
        for node in survivors:
            node.terminate()

        elapsed = H.time.monotonic() - started
        H.log(f"PASS in {elapsed:.1f}s")
        return 0
    except Exception as exc:  # noqa: BLE001 - dump everything on failure
        print(f"[gate-observation-lifecycle] FAIL: {exc}", file=sys.stderr)
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
    H.set_tag("gate-observation-lifecycle")
    sys.exit(main())
