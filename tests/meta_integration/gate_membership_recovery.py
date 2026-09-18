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

"""Real-process durable membership recovery in temporary Meta directories."""
import os
import re
import socket
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402


class FaultNode(H.Node):
    phase = None
    action = "add"

    def start(self, *args, **kwargs):
        values = {"LAVIK_TEST_PAUSE_MEMBERSHIP_PHASE": self.phase,
                  "LAVIK_TEST_PAUSE_MEMBERSHIP_TARGET": "4",
                  "LAVIK_TEST_PAUSE_MEMBERSHIP_ACTION": self.action}
        previous = {key: os.environ.get(key) for key in values}
        try:
            for key, value in values.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            return super().start(*args, **kwargs)
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


def add_request(node):
    return (f"addsrv {node.id} {node.endpoint} "
            f"{node.data_control_endpoint} {node.ctl_endpoint}")


def find_operation(node, phase, exclude=()):
    matches = re.findall(r"membership ([0-9a-f]{32}) phase=" + re.escape(phase),
                         node.log_tail(lines=1000))
    matches = [operation for operation in matches if operation not in exclude]
    return matches[-1] if matches else None


def completed(node, operation, add):
    return node.getop(operation) == "OK completed " + ("member-added" if add else "member-removed")


def pending_invite(workdir):
    scenario = os.path.join(workdir, "offline-invite")
    os.makedirs(scenario)
    meta = H.Node(BINARY, scenario, 1, args=H.raft_args(snapshot_distance=100000))
    joiner = H.Node(BINARY, scenario, 2)
    try:
        meta.start(bootstrap=True)
        meta.wait_leader()
        # Equivalent numeric-port spellings must bind canonically and attach
        # to the same retained task when retried with canonical spelling.
        request = add_request(joiner).replace("127.0.0.1:", "127.0.0.1:0")
        reply = meta.ctl(request, timeout=8)
        bootstrap_deadline = time.monotonic() + 5
        while reply == "ERR config-changing" and time.monotonic() < bootstrap_deadline:
            time.sleep(0.025)
            reply = meta.ctl(request, timeout=8)
        match = re.fullmatch(r"ERR uncertain-outcome operation=([0-9a-f]{32})", reply)
        if not match:
            raise H.Failure(f"offline invite did not retain an operation: {reply}")
        operation = match.group(1)
        if meta.ctl(add_request(joiner), timeout=8) != reply:
            raise H.Failure("identical retry replaced the membership task")
        if meta.ctl("removesrv 1") != "ERR config-changing":
            raise H.Failure("uncertain invite released membership admission")
        if meta.ctl(f"abortop {operation}") != "ERR workflow-owned":
            raise H.Failure("generic abort abandoned an accepted invite")
        H.manual_snapshot(meta)
        started = time.monotonic()
        meta.terminate()
        elapsed = time.monotonic() - started
        if elapsed > 2:
            raise H.Failure(f"offline invite blocked shutdown for {elapsed}s")
        meta.start(bootstrap=True)
        meta.wait_leader()
        joiner.start(bootstrap=False)
        H.wait_until("offline invite resumes without another addsrv", 30,
                     lambda: completed(meta, operation, True))
        probe, reply = meta.propose("membership-recovered")
        if not reply.startswith("OK "):
            raise H.Failure(reply)
        H.wait_until("joiner receives committed probe", 10,
                     lambda: joiner.getop(probe) == "OK completed membership-recovered")
        H.log(f"offline-invite: snapshot recovery, stable operation, SIGTERM {elapsed:.3f}s")
    except Exception:
        H.dump_node_logs([meta, joiner])
        raise
    finally:
        joiner.force_kill()
        meta.force_kill()


def recovery_cut(workdir, add, phase, snapshot=False, failover=False):
    name = f"{'add' if add else 'remove'}-{phase}-{'failover' if failover else 'restart'}"
    scenario = os.path.join(workdir, name)
    os.makedirs(scenario)
    args = H.raft_args(snapshot_distance=100000, reserved_log_items=500)
    nodes = [FaultNode(BINARY, scenario, 1, args=args)] + H.make_nodes(BINARY, scenario, 2, args=args, first_id=2)
    target = H.Node(BINARY, scenario, 4, args=args)
    nodes[0].phase = phase
    nodes[0].action = "add" if add else "remove"
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(8)
    try:
        leader = H.bootstrap_cluster(nodes)
        if leader.id != 1:
            raise H.Failure("fault owner was not bootstrap leader")
        target.start(bootstrap=False)
        if not add:
            H.join_and_verify(leader, target)
        old_operations = set(re.findall(r"membership ([0-9a-f]{32}) phase=",
                                        leader.log_tail(lines=1000)))
        connection.connect(leader.ctl_path)
        connection.sendall((add_request(target) if add else "removesrv 4").encode() + b"\n")
        H.wait_until(f"{name}: durable cut", 10,
                     lambda: find_operation(leader, phase, old_operations))
        operation = find_operation(leader, phase, old_operations)
        if snapshot:
            H.manual_snapshot(leader)
        # Never reuse the original request or submit a replacement operation.
        if failover:
            leader.kill9()
            restored = H.find_leader(nodes[1:] + ([target] if add else []), timeout=15)
        else:
            for node in nodes[1:]:
                node.kill9()
            started = time.monotonic()
            if snapshot:
                leader.kill9()
            else:
                leader.terminate()
            if time.monotonic() - started > 2:
                raise H.Failure(f"{name}: shutdown waited for remote membership")
            nodes[0].phase = None
            for node in nodes:
                node.start(bootstrap=node.id == 1)
            restored = H.find_leader(nodes + ([target] if add else []), timeout=15)
        H.wait_until(f"{name}: original task completes", 30,
                     lambda: completed(restored, operation, add))
        if add:
            probe, reply = restored.propose("after-membership-recovery")
            if not reply.startswith("OK "):
                raise H.Failure(reply)
            H.wait_until("target catches up", 15,
                         lambda: target.getop(probe) == "OK completed after-membership-recovery")
        else:
            replies = []

            def terminally_retired():
                replies[:] = [restored.ctl("removesrv 4"),
                              restored.ctl(add_request(target))]
                return replies == ["OK", "ERR rejected"]

            # Completion and retirement are already committed. Allow the
            # recovered workflow to release its membership reservation,
            # then observe the stable admission result.
            try:
                H.wait_until("removed identity is terminally retired", 5,
                             terminally_retired)
            except H.Failure as error:
                raise H.Failure(
                    f"removed identity was not terminally retired: {replies}") from error
        H.log(f"{name}: original operation completes without resubmission ({'snapshot' if snapshot else 'WAL'})")
    except Exception:
        H.dump_node_logs(nodes + [target])
        raise
    finally:
        connection.close()
        for node in nodes + [target]:
            node.force_kill()


def has_faults():
    needle = b"LAVIK_TEST_PAUSE_MEMBERSHIP_PHASE"
    tail = b""
    with open(BINARY, "rb") as source:
        while chunk := source.read(1 << 20):
            if needle in tail + chunk:
                return True
            tail = chunk[-len(needle):]
    return False


def main():
    workdir, keep = H.make_workdir(sys.argv, "meta_membership_recovery_")
    try:
        pending_invite(workdir)
        if has_faults():
            for add, phase, snapshot in (
                    (True, "submitted", False), (True, "change-config", False),
                    (True, "config-committed", True), (False, "change-config", False),
                    (False, "config-committed", True), (False, "identity-complete", False)):
                recovery_cut(workdir, add, phase, snapshot)
            recovery_cut(workdir, True, "change-config", failover=True)
            recovery_cut(workdir, False, "config-committed", failover=True)
        else:
            H.log("SKIP deterministic cuts: ordinary Release erases hooks")
        H.log("PASS")
        return 0
    except Exception as error:
        H.log(f"FAIL: {error}")
        keep = True
        return 1
    finally:
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    BINARY = os.path.abspath(sys.argv[1])
    H.set_tag("membership-recovery")
    sys.exit(main())
