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

"""Real-process gates for manifest-bootstrapped one-, three-, and five-Meta
genesis.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402


class InitialBindingFaultNode(H.Node):
    pause_after = None

    def start(self, *args, **kwargs):
        variable = "LAVIK_TEST_PAUSE_INITIAL_BINDINGS_AFTER"
        previous = os.environ.get(variable)
        try:
            if self.pause_after is None:
                os.environ.pop(variable, None)
            else:
                os.environ[variable] = str(self.pause_after)
            return super().start(*args, **kwargs)
        finally:
            if previous is None:
                os.environ.pop(variable, None)
            else:
                os.environ[variable] = previous


def has_initial_binding_fault():
    needle = b"LAVIK_TEST_PAUSE_INITIAL_BINDINGS_AFTER"
    tail = b""
    with open(META, "rb") as source:
        while chunk := source.read(1 << 20):
            if needle in tail + chunk:
                return True
            tail = chunk[-len(needle):]
    return False


def prove_replicated(nodes, leader, value):
    operation_id, reply = leader.propose(value)
    if not reply.startswith("OK "):
        raise H.Failure(f"initial membership proposal failed: {reply}")
    index = int(reply[3:])
    H.wait_cluster_committed(nodes, index, timeout=20)
    for node in nodes:
        if node.getop(operation_id) != f"OK completed {value}":
            raise H.Failure(
                f"node {node.id} did not apply initial membership proposal")


def run_count(workdir, count):
    scenario = os.path.join(workdir, f"meta-{count}")
    os.makedirs(scenario, mode=0o700)
    nodes = [
        InitialBindingFaultNode(
            META, scenario, node_id,
            args=H.raft_args(snapshot_distance=100_000))
        for node_id in range(1, count + 1)
    ]
    manifest = os.path.join(scenario, "initial-cluster.toml")
    H.write_initial_cluster_manifest(manifest, nodes)
    try:
        if count == 3:
            # A manifest-bootstrapped three-voter config cannot elect with one
            # process. The same pristine node becomes viable as soon as a
            # majority arrives; nobody is added through the membership API.
            faults_enabled = has_initial_binding_fault()
            if faults_enabled:
                for node in nodes:
                    node.pause_after = 1
            nodes[0].start(initial_cluster_manifest=manifest)
            try:
                H.find_leader(nodes[:1], timeout=2)
                raise H.Failure("one of three initial voters elected alone")
            except H.Failure as error:
                if "timeout" not in str(error):
                    raise
            nodes[1].start(initial_cluster_manifest=manifest)
            leader = H.find_leader(nodes[:2], timeout=15)

            # Stop after exactly one committed descriptor binding, kill the
            # leader, and restart from the marker/WAL prefix without replaying
            # the genesis input. The never-started third member still consumes
            # the same manifest on its own first boot.
            if faults_enabled:
                H.wait_until(
                    "initial identity reconciler pauses after one binding", 10,
                    lambda: any(
                        "initial Meta identity reconciliation paused after 1 "
                        "bindings" in node.log_tail(lines=300)
                        for node in nodes[:2]))
                leader = H.find_leader(nodes[:2], timeout=5)
                leader.kill9()
                for node in nodes[:2]:
                    if node.alive():
                        node.terminate()
                    node.pause_after = None
                    node.start()
                nodes[2].pause_after = None
            else:
                H.log("SKIP mid-binding restart: ordinary Release erases "
                      "the pause hook")
            nodes[2].start(initial_cluster_manifest=manifest)
            leader = H.find_leader(nodes, timeout=20)
        else:
            for node in nodes:
                node.start(initial_cluster_manifest=manifest,
                           explicit_ctl_socket=count != 1)
            leader = H.find_leader(nodes, timeout=20)

        prove_replicated(nodes, leader, f"manifest-{count}")
        H.wait_until(
            f"all {count} members close initial binding grace", 10,
            lambda: all(
                node.status()["initial_bindings_pending"] == "0"
                for node in nodes))
        before_restart = max(node.committed() for node in nodes)

        for node in nodes:
            node.terminate()
        for node in nodes:
            # The genesis file remains on disk but is intentionally not an
            # input to ordinary startup.
            node.start(explicit_ctl_socket=count != 1)
        leader = H.find_leader(nodes, timeout=20)
        for node in nodes:
            H.wait_no_regress(node, before_restart)
        prove_replicated(nodes, leader, f"restart-{count}")
        H.log(
            f"manifest-bootstrapped {count}-Meta genesis and "
            "manifest-free restart — OK")
    except Exception:
        H.dump_node_logs(nodes, lines=120)
        raise
    finally:
        for node in nodes:
            node.force_kill()


def main():
    harness_argv = [sys.argv[0], META] + sys.argv[2:]
    workdir, keep = H.make_workdir(harness_argv, "meta_initial_")
    try:
        for count in (1, 3, 5):
            run_count(workdir, count)
        return 0
    except Exception as error:  # noqa: BLE001
        print(f"[initial-meta] FAIL: {error}", file=sys.stderr)
        return 1
    finally:
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    META = sys.argv[1]
    H.set_tag("initial-meta")
    sys.exit(main())
