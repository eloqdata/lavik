#!/usr/bin/env python3
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
4. Operation evidence lifecycle: a history binding committed via
   transitionop lets evidence be ACCEPTED while the operation is live;
   forged evidence (unbound history) is rejected and the operation never
   advances on it; after completeop retires the operation the same
   well-formed evidence is rejected (operation-unknown-or-terminal) and the
   commit-driven RevalidateAll drop appears as commit-stale in the ring.
5. Leader-local semantics: kill -9 the leader; the NEW leader's store is
   empty (candidate query returns zero) until the node re-reports through a
   fresh session on the new leader. Committed history is verified
   unaffected by all observation traffic.

Usage: gate_stale_evidence.py /path/to/keylane-meta [workdir]
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
    workdir, keep = H.make_workdir(sys.argv, "meta_integration_stale_evidence_")
    nodes = H.make_nodes(BINARY, workdir, 3)
    started = H.time.monotonic()
    try:
        leader = H.bootstrap_cluster(nodes)
        history = H.CommittedHistory()

        # --- phase 1: committed anchors ----------------------------------
        expect_ok(leader.put_authority_lease_policy(1),
                  "put Authority Lease Policy")
        expect_ok(leader.registernode(DATA_NODE, f"keylane://node/{DATA_NODE}",
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

        # --- phase 4: operation evidence lifecycle ------------------------
        op_id = leader.new_op_id()
        expect_ok(leader.submitop(op_id, "migration", "ev1", history=55),
                  "submitop")
        # Forged evidence (history never bound to the operation) is rejected
        # and the operation must not advance on it.
        expect_err(
            leader.obs_evidence(DATA_NODE, BOOT_A, 2, op_id, "phase1",
                                "forged", GROUP, term=1, manifest=0,
                                history=99),
            "unbound-history evidence", "history-not-bound")
        if leader.getop(op_id) != "OK submitted":
            raise H.Failure(f"operation advanced on forged evidence: "
                            f"{leader.getop(op_id)}")

        # The submit committed history 55; well-formed evidence is accepted
        # after the operation enters its running phase.
        expect_ok(leader.transitionop(op_id, "phase1", 55), "transitionop")
        expect_err(
            leader.obs_evidence(DATA_NODE, BOOT_A, 2, op_id, "phase1",
                                "old-generation", GROUP, term=1, manifest=0,
                                history=55, partition_epoch=1),
            "wrong-population-epoch evidence", "partition-epoch-mismatch")
        expect_ok(
            leader.obs_evidence(DATA_NODE, BOOT_A, 2, op_id, "phase1",
                                "proof", GROUP, term=1, manifest=0,
                                history=55),
            "bound-history evidence while live")
        expect_err(
            leader.obs_evidence(DATA_NODE, BOOT_A, 2, op_id, "phase1",
                                "forged2", GROUP, term=1, manifest=0,
                                history=54),
            "still-unbound history", "history-not-bound")
        if leader.getop(op_id) != "OK running":
            raise H.Failure(f"operation advanced on stale evidence: "
                            f"{leader.getop(op_id)}")

        # Retire the operation: the ctl's post-commit RevalidateAll purges
        # the now-stale evidence, and re-ingestion rejects it.
        expect_ok(leader.completeop(op_id, "done"), "completeop")
        expect_err(
            leader.obs_evidence(DATA_NODE, BOOT_A, 2, op_id, "phase1",
                                "proof", GROUP, term=1, manifest=0,
                                history=55),
            "evidence after terminal", "operation-unknown-or-terminal")
        audit = leader.obsaudit()
        for needle in ("detail=history-not-bound",
                       "detail=partition-epoch-mismatch",
                       "detail=commit-stale:operation-unknown-or-terminal",
                       "detail=operation-unknown-or-terminal"):
            if needle not in audit:
                raise H.Failure(f"obsaudit missing {needle!r}: {audit}")
        H.log("phase 4: evidence accepted while live, purged and rejected "
              "after terminal")

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
                                     term=1, manifest=0, history=7),
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
        print(f"[gate-stale-evidence] FAIL: {exc}", file=sys.stderr)
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
    H.set_tag("gate-stale-evidence")
    sys.exit(main())
