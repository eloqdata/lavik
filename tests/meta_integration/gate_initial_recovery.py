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

"""Late genesis catch-up and snapshot crash recovery preserve Raft quorum."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402


def bindings_complete(node):
    return node.status()["initial_bindings_pending"] == "0"


def prove_write(nodes, leader, value):
    operation_id, reply = leader.propose(value)
    if not reply.startswith("OK "):
        raise H.Failure(f"proposal failed: {reply}")
    H.wait_cluster_committed(nodes, int(reply[3:]), timeout=20)
    for node in nodes:
        if node.getop(operation_id) != f"OK completed {value}":
            raise H.Failure(f"node {node.id} did not apply {value}")


def start_with_crash(node, point):
    previous = os.environ.get("LAVIK_CRASH_POINT")
    try:
        os.environ["LAVIK_CRASH_POINT"] = point
        node.start(wait_ready=False)
    finally:
        if previous is None:
            os.environ.pop("LAVIK_CRASH_POINT", None)
        else:
            os.environ["LAVIK_CRASH_POINT"] = previous
    H.wait_until(f"node {node.id} reaches {point}", 20,
                 lambda: not node.alive())
    if node.proc.returncode != 86:
        raise H.Failure(f"expected injected exit 86, got {node.proc.returncode}")


def run_case(binary, workdir, snapshot, crash_point=None):
    name = crash_point or ("snapshot" if snapshot else "wal")
    scenario = os.path.join(workdir, name)
    os.makedirs(scenario, mode=0o700)
    nodes = H.make_nodes(binary, scenario, 4,
                         args=H.raft_args(
                             snapshot_distance=10 if snapshot else 100_000))
    late = nodes[2]
    manifest = os.path.join(scenario, "initial.toml")
    H.write_initial_cluster_manifest(manifest, nodes[:3])
    try:
        # Node 3 retains the initial three-voter marker while the majority
        # finishes genesis and commits an ordinary fourth-member addition.
        late.start(initial_cluster_manifest=manifest)
        late.terminate()
        for node in nodes[:2]:
            node.start(initial_cluster_manifest=manifest)
        leader = H.find_leader(nodes[:2])
        H.wait_until("majority completes genesis", 10,
                     lambda: all(bindings_complete(n) for n in nodes[:2]))
        nodes[3].start()
        H.join_and_verify(leader, nodes[3])
        active = [nodes[0], nodes[1], nodes[3]]
        leader = H.find_leader(active)
        for sequence in range(15):
            prove_write(active, leader, f"before-catchup-{sequence}")
        if snapshot:
            snapshot_index = H.manual_snapshot(leader)
            H.wait_until("leader publishes membership snapshot", 10,
                         lambda: leader.snapshot_idx() >= snapshot_index)

        if crash_point:
            start_with_crash(late, crash_point)
        late.start()
        H.wait_cluster_committed(nodes, leader.committed(), timeout=20)
        H.wait_until("late member completes genesis", 10,
                     lambda: bindings_complete(late))
        if snapshot:
            H.wait_until("late member installs snapshot", 10,
                         lambda: late.snapshot_idx() >= snapshot_index)
        elif late.snapshot_idx() != 0:
            raise H.Failure("WAL-only case unexpectedly used a snapshot")

        # Restart again to check the durable result, not merely the live
        # reconfiguration. The old bug resurrected three voters on this boot.
        late.terminate()
        late.start()
        leader = H.find_leader(nodes)
        prove_write(nodes, leader, "after-second-restart")

        # Remove the leader and one other member, retaining node 3. Two of
        # four must neither elect a fresh leader nor acknowledge a write.
        removed = [leader, nodes[3] if leader is not nodes[3] else nodes[0]]
        for node in removed:
            node.kill9()
        minority = [node for node in nodes if node.alive()]
        if len(minority) != 2 or late not in minority:
            raise H.Failure("minority setup did not retain the late member")
        try:
            H.find_leader(minority, timeout=6)
        except H.Failure as error:
            if "timeout" not in str(error):
                raise
        else:
            raise H.Failure("two of four members elected a leader")
        for node in minority:
            try:
                reply = node.put_automatic_uncontrolled_failover_policy(
                    1, timeout=2)
            except (OSError, H.Failure):
                continue
            if reply.startswith("OK "):
                raise H.Failure(f"two of four committed a write: {reply}")

        removed[0].start()
        majority = [node for node in nodes if node.alive()]
        leader = H.find_leader(majority)
        prove_write(majority, leader, "majority-restored")
        H.log(f"{name}: catch-up, restart, 2/4 rejection, 3/4 recovery — OK")
    except Exception:
        H.dump_node_logs(nodes, lines=100)
        raise
    finally:
        for node in nodes:
            node.force_kill()


def has_crash_hooks(binary):
    needle = b"meta-snapshot-before-membership"
    tail = b""
    with open(binary, "rb") as source:
        while chunk := source.read(1 << 20):
            if needle in tail + chunk:
                return True
            tail = chunk[-len(needle):]
    return False


def main():
    binary = sys.argv[1]
    workdir, keep = H.make_workdir(sys.argv, "meta_initial_recovery_")
    try:
        run_case(binary, workdir, snapshot=False)
        run_case(binary, workdir, snapshot=True)
        if has_crash_hooks(binary):
            for point in ("before-membership", "after-file", "after-marker"):
                run_case(binary, workdir, snapshot=True,
                         crash_point=f"meta-snapshot-{point}")
        else:
            H.log("SKIP snapshot crash cuts: binary has no fault hooks")
        return 0
    except Exception as error:  # noqa: BLE001
        print(f"[initial-recovery] FAIL: {error}", file=sys.stderr)
        return 1
    finally:
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    H.set_tag("initial-recovery")
    sys.exit(main())
