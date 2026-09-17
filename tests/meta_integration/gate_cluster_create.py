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

"""Real-process manifest-bootstrapped Meta cluster-create and recovery gate.

Usage: gate_cluster_create.py META DATA CTL REDIS_CLI [workdir]
"""

import json
import os
import re
import socket
import ssl
import struct
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402
from gate_data_control import DataProcess, make_ca, make_leaf  # noqa: E402


DATA_NODE = "0123456789abcdef0123456789abcdef01234567"
PRIMARY_1 = "1111111111111111111111111111111111111111"
REPLICA_1 = "2222222222222222222222222222222222222222"
PRIMARY_2 = "3333333333333333333333333333333333333333"
REPLICA_2 = "4444444444444444444444444444444444444444"
GROUPS = {
    "group-1": (PRIMARY_1, REPLICA_1, 0, 8191),
    "group-2": (PRIMARY_2, REPLICA_2, 8192, 16383),
}
TRANSIENT_SELF_FENCE = "CLUSTERDOWN Hash slot not served"


def meta_manifest_lines(*metas):
    lines = []
    for meta in sorted(metas, key=lambda node: node.id):
        data_endpoint = getattr(meta, "advertised_data_control_endpoint",
                                meta.data_control_endpoint)
        lines.extend([
            "[[meta_members]]",
            f"id = {meta.id}",
            f'raft_endpoint = "tcp://{meta.endpoint}"',
            f'data_control_endpoint = "tcp://{data_endpoint}"',
            f'ctl_endpoint = "tcp://{meta.ctl_endpoint}"',
            "",
        ])
    return lines


def write_manifest(path, endpoint, metas):
    if not isinstance(metas, (list, tuple)):
        metas = [metas]
    with open(path, "w", encoding="utf-8") as output:
        output.write(
            "schema_version = 1\n\n" +
            "\n".join(meta_manifest_lines(*metas)) +
            "[[data_nodes]]\n"
            f'id = "{DATA_NODE}"\n'
            f'client_endpoint = "{endpoint}"\n\n'
            "[[groups]]\nid = \"group-1\"\n"
            f'primary = "{DATA_NODE}"\n\n'
            "[[slot_ranges]]\nfirst = 0\nlast = 16383\n"
            'group = "group-1"\n')


def command(environment, arguments, input_text=None, timeout=90, expected=0):
    result = subprocess.run(
        arguments, input=input_text, capture_output=True, text=True,
        timeout=timeout, env=environment)
    if result.returncode != expected:
        raise H.Failure(
            f"command failed ({result.returncode}, want {expected}): "
            f"{' '.join(arguments)} "
            f"stdout={result.stdout!r} stderr={result.stderr!r}")
    return result.stdout


def cluster_status(meta, admin=None):
    result = subprocess.run(
        [CTL, "cluster-status", "--json"] +
        (admin if admin is not None else ["--socket", meta.ctl_path]),
        capture_output=True, text=True, timeout=5)
    if result.returncode not in (0, 2):
        raise H.Failure(f"cluster-status failed: {result}")
    return json.loads(result.stdout)


def wait_cluster_ready(meta, description, timeout, admin=None):
    """Observe completed creation and readiness in the same status cut."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = cluster_status(meta, admin=admin)
        if (last.get("result") == "ready" and
                last.get("cluster_state") == "created"):
            return last
        if last.get("cluster_state") == "provisioning-failed":
            root = last.get("root_operation_id")
            detail = meta.getop(root) if root else "root operation unavailable"
            raise H.Failure(
                f"{description} failed: "
                f"{last.get('provisioning_failure_summary')}; {detail}")
        time.sleep(0.25)
    raise H.Failure(
        f"timeout ({timeout}s) waiting for: {description}; status={last}")


def create_request(meta, node_id, endpoint, group_id, meta_id=None,
                   operation_id=None):
    """Send a normalized public v1 envelope so CLI cannot hide races."""
    metas = meta if isinstance(meta, (list, tuple)) else [meta]
    operation_id = operation_id or os.urandom(16)
    # Direct admission/recovery cases construct the same current v1 durable
    # intent as the CLI. Older persisted layouts are deliberately unsupported.
    payload = struct.pack(">H", 1) + operation_id
    payload += struct.pack(">II", 5000, 5000)
    payload += struct.pack(">I", len(metas))
    for member in sorted(metas, key=lambda item: item.id):
        member_id = member.id if meta_id is None else meta_id
        payload += struct.pack(">I", member_id)
        for value in (f"tcp://{member.endpoint}",
                      "tcp://" + getattr(
                          member, "advertised_data_control_endpoint",
                          member.data_control_endpoint),
                      "tcp://" + getattr(
                          member, "advertised_ctl_endpoint",
                          member.ctl_endpoint)):
            encoded = value.encode()
            payload += struct.pack(">I", len(encoded)) + encoded
    payload += struct.pack(">HI", 0, 1)
    for value in (node_id, endpoint, ""):
        encoded = value.encode()
        payload += struct.pack(">I", len(encoded)) + encoded
    payload += struct.pack(">I", 1)
    for value in (group_id, node_id):
        encoded = value.encode()
        payload += struct.pack(">I", len(encoded)) + encoded
    payload += struct.pack(">II", 0, 1)
    payload += struct.pack(">HH", 0, 16383)
    encoded_group = group_id.encode()
    payload += struct.pack(">I", len(encoded_group)) + encoded_group
    return "clustercreate 1 " + payload.hex()


def read_reply(connection):
    reply = b""
    while not reply.endswith(b"\n"):
        chunk = connection.recv(4096)
        if not chunk:
            raise H.Failure("Admin connection closed before its reply")
        reply += chunk
    return reply.decode().strip()


class InitialProjectionBarrier(H.Proxy):
    """Hold the accepted ServerHello after Meta has built its initial FDS."""

    def __init__(self, target_port):
        super().__init__("initial-projection", target_port)
        self.blocked = threading.Event()
        self.release = threading.Event()
        self.error = None

    def _pump(self, src, dst, pair):
        if src is pair[1] and not self.blocked.is_set():
            try:
                # Control v1 has a 28-byte header followed by ServerHello's
                # one-byte disposition. Check acceptance so a redirect cannot
                # accidentally move the commit before initial FDS generation.
                prefix = b""
                while len(prefix) < 29:
                    chunk = src.recv(29 - len(prefix))
                    if not chunk:
                        raise H.Failure("Meta closed before ServerHello")
                    prefix += chunk
                if (struct.unpack_from(">IHH", prefix) != (0x4c564350, 1, 2) or
                        prefix[28] != 1):
                    raise H.Failure("expected an accepted v1 ServerHello")
                self.blocked.set()
                if not self.release.wait(8):
                    raise H.Failure("initial projection barrier timed out")
                dst.sendall(prefix)
            except (OSError, H.Failure) as error:
                self.error = str(error)
                self.blocked.set()
                self._cut_pair(pair)
                return
        super()._pump(src, dst, pair)

    def close(self):
        self.release.set()
        super().close()


def run_unrelated_commit_case(workdir):
    scenario = os.path.join(workdir, "unrelated-commit")
    os.makedirs(scenario, mode=0o700)
    meta = H.Node(META, scenario, 1,
                  args=H.raft_args(snapshot_distance=100_000))
    proxy = InitialProjectionBarrier(meta.data_control_port)
    data = DataProcess(DATA, os.path.join(scenario, "data"), DATA_NODE,
                       proxy.endpoint)
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(15)

    try:
        meta.start(bootstrap=True)
        meta.wait_leader()
        H.wait_until("bootstrap Meta identity committed", 5,
                     lambda: cluster_status(meta)["meta_membership_stable"])
        connection.connect(meta.ctl_path)
        request = create_request(meta, DATA_NODE, data.advertised_endpoint,
                                 "group-1")
        connection.sendall(request.encode() + b"\n")
        accepted = read_reply(connection)
        if not accepted.startswith("OK clustercreate 1 "):
            raise H.Failure(
                f"unrelated commit did not admit Genesis: {accepted}")
        operation_id = accepted.split()[-1]
        # Complete all topology/authority commits before starting Data. The
        # only later metadata change is a new version of the automatic
        # failover Policy, which is intentionally absent from Data's FDS.
        H.wait_until("creation committed its authority", 5,
                     lambda: any(group.get("owner_node_id") == DATA_NODE
                                 for group in cluster_status(meta)["groups"]))
        proxy.start()
        data.start()
        if not proxy.blocked.wait(5) or proxy.error is not None:
            raise H.Failure(f"initial FDS was not held: {proxy.error}")

        before = meta.committed()
        content = H.automatic_uncontrolled_failover_policy()
        reply = meta.put_automatic_uncontrolled_failover_policy(2)
        match = re.fullmatch(r"OK (\d+)", reply)
        if match is None or int(match.group(1)) <= before:
            raise H.Failure(
                f"non-projected policy did not advance Meta: {reply}")
        current = meta.getpolicy(H.AUTOMATIC_UNCONTROLLED_FAILOVER_POLICY_ID)
        expected = f"OK version=2 content={content}"
        if current != expected:
            raise H.Failure(
                f"getpolicy did not return current raw Policy: {current!r}, "
                f"want {expected!r}")
        held = cluster_status(meta)
        if any(node["current_session"] or node["projection_current"]
               for node in held["data_nodes"]):
            raise H.Failure("creation advanced before Data acknowledged FDS")

        # The initial object's source index predates this commit, but its
        # semantic content is still current. No further commits are needed to
        # unblock creation: the publisher must validate the installed object.
        proxy.release.set()
        if proxy.error is not None:
            raise H.Failure(f"initial FDS barrier failed: {proxy.error}")
        H.wait_until("creation with an unrelated commit reaches READY", 15,
                     lambda: cluster_status(meta)["result"] == "ready")
        if meta.getop(operation_id) != "OK completed cluster-created":
            raise H.Failure("creation did not durably complete its operation")
        H.log("unrelated-commit: acknowledged older FDS reaches READY after "
              "Meta validates the unchanged projection")
        data.terminate()
        meta.terminate()
    except Exception:
        H.dump_node_logs([meta])
        print(data.log_tail(lines=250), file=sys.stderr)
        raise
    finally:
        proxy.close()
        connection.close()
        data.force_kill()
        meta.force_kill()


def run_concurrent_case(workdir, transports):
    name = "concurrent-" + "-".join(transports)
    scenario = os.path.join(workdir, name)
    os.makedirs(scenario, mode=0o700)
    meta = H.Node(META, scenario, 1,
                  args=H.raft_args(snapshot_distance=100_000))
    connections = []
    node_ids = [f"{index + 1:040x}" for index in range(2)]
    endpoints = [f"tcp://127.0.0.1:{H.free_port()}" for _ in node_ids]
    groups = [f"group-{index + 1}" for index in range(2)]
    try:
        meta.start(bootstrap=True)
        meta.wait_leader()
        # Election precedes the bootstrap identity commit. Wait for that
        # independent write before attributing index changes to our request.
        H.wait_until(f"{name}: bootstrap Meta identity committed", 5,
                     lambda: cluster_status(meta)["meta_membership_stable"])
        # A rejected preflight must release admission before any proposal.
        before = meta.committed()
        rejected = meta.ctl(create_request(
            meta, node_ids[0], endpoints[0], groups[0], meta_id=2))
        if (not rejected.startswith(
                "ERR clustercreate 1 preflight bad-request ") or
                meta.committed() != before):
            raise H.Failure(f"{name}: invalid preflight mutated Meta: {rejected}")

        for transport in transports:
            if transport == "unix":
                connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                address = meta.ctl_path
            else:
                connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                address = ("127.0.0.1", meta.ctl_port)
            connections.append(connection)
            connection.settimeout(10)
            connection.connect(address)
            # Admit both sessions before queuing requests, including when
            # they belong to distinct Unix/TCP listener instances.
            connection.sendall(b"status\n")
            if not read_reply(connection).startswith("OK "):
                raise H.Failure(f"{name}: Admin session did not become ready")

        meta.pause()
        for index, connection in enumerate(connections):
            request = create_request(meta, node_ids[index], endpoints[index],
                                     groups[index])
            connection.sendall(request.encode() + b"\n")
        meta.resume()

        # Genesis acceptance returns before Data readiness. Exactly one
        # proposal owns the singleton lifecycle; the competing manifest is
        # rejected without comparison or partial topology mutation.
        replies = [read_reply(connection) for connection in connections]
        winners = [index for index, reply in enumerate(replies)
                   if reply.startswith("OK clustercreate 1 ")]
        losers = [index for index, reply in enumerate(replies)
                  if (" already-created " in reply or
                      " pre-commit-failed " in reply)]
        if len(winners) != 1 or len(losers) != 1:
            raise H.Failure(
                f"{name}: expected one accepted Genesis: {replies}")
        winner = winners[0]
        loser = losers[0]
        H.wait_until(f"{name}: admitted identity committed", 2,
                     lambda: meta.getnode(node_ids[winner]).startswith("OK "))
        if meta.getnode(node_ids[loser]) != "ERR not-found":
            raise H.Failure(f"{name}: rejected create left a durable identity")
        if meta.ctl("removesrv 1") != "ERR config-changing":
            raise H.Failure(f"{name}: membership bypassed creation admission")

        # The durable background task retains singleton creation ownership
        # after the accepted Admin request returns.
        if meta.ctl("removesrv 1") != "ERR config-changing":
            raise H.Failure(f"{name}: acceptance abandoned durable admission")
        retry = meta.ctl(create_request(meta, node_ids[winner], endpoints[winner],
                                       groups[winner]))
        if not retry.startswith(
                "ERR clustercreate 1 preflight already-created "):
            raise H.Failure(f"{name}: partial create was admitted again: {retry}")

        meta.terminate()
        meta.start(bootstrap=True)
        meta.wait_leader()
        status = subprocess.run(
            [CTL, "cluster-status", "--socket", meta.ctl_path, "--json"],
            capture_output=True, text=True, timeout=10)
        if status.returncode != 2:
            raise H.Failure(f"{name}: unexpected status after restart: {status}")
        restored = json.loads(status.stdout)
        if ([node["node_id"] for node in restored["data_nodes"]] !=
                [node_ids[winner]] or
                [group["group_id"] for group in restored["groups"]] !=
                [groups[winner]] or
                restored["slot_ranges"] != [
                    {"first": "0", "last": "16383",
                     "group_id": groups[winner]}]):
            raise H.Failure(
                f"{name}: concurrent creates polluted topology: {restored}")
        meta.terminate()
        H.log(f"{name}: one creator admitted; "
              "rejected topology absent after restart")
    except Exception:
        H.dump_node_logs([meta])
        raise
    finally:
        for connection in connections:
            connection.close()
        meta.force_kill()


def run_single_meta_client_loss_case(workdir):
    """A disconnected creator cannot roll back or duplicate one-node Genesis."""
    scenario = os.path.join(workdir, "single-meta-client-loss")
    os.makedirs(scenario, mode=0o700)
    meta = H.Node(META, scenario, 1,
                  args=H.raft_args(snapshot_distance=100_000))
    operation_id = os.urandom(16)
    node_id = "0123456789abcdef0123456789abcdef01234567"
    try:
        meta.start(bootstrap=True)
        meta.wait_leader()
        H.wait_until("single Meta identity committed", 5,
                     lambda: cluster_status(meta)["meta_membership_stable"])
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(5)
        connection.connect(meta.ctl_path)
        request = create_request(
            meta, node_id, f"tcp://127.0.0.1:{H.free_port()}", "group-1",
            operation_id=operation_id)
        connection.sendall(request.encode() + b"\n")
        connection.close()  # Deliberately lose the proposal response.

        expected_root = operation_id.hex()
        H.wait_until(
            "one-node Genesis survives client loss", 5,
            lambda: cluster_status(meta).get("root_operation_id") ==
            expected_root)
        retry = meta.ctl(create_request(
            meta, node_id, f"tcp://127.0.0.1:{H.free_port()}", "group-1"))
        if " already-created " not in retry:
            raise H.Failure(
                f"single Meta admitted another create after client loss: {retry}")

        meta.terminate()
        meta.start(bootstrap=True)
        meta.wait_leader()
        restored = cluster_status(meta)
        if (restored.get("cluster_state") != "creating" or
                restored.get("root_operation_id") != expected_root):
            raise H.Failure(
                f"single Meta restart lost Genesis binding: {restored}")
        H.log("single Meta client/response loss and restart — OK")
        meta.terminate()
    except Exception:
        H.dump_node_logs([meta])
        raise
    finally:
        meta.force_kill()


def run_case(workdir, interactive):
    name = "interactive" if interactive else "yes"
    scenario = os.path.join(workdir, name)
    os.makedirs(scenario, mode=0o700, exist_ok=True)
    meta_workdir = os.path.join(scenario, "meta")
    os.makedirs(meta_workdir, mode=0o700)
    meta = H.Node(META, meta_workdir, 1,
                  args=H.raft_args(snapshot_distance=100_000))
    data = DataProcess(DATA, os.path.join(scenario, "data"), DATA_NODE,
                       meta.data_control_endpoint)
    environment = os.environ.copy()
    environment["PATH"] = (os.path.dirname(REDIS_CLI) + os.pathsep +
                           environment.get("PATH", ""))
    manifest = os.path.join(scenario, "cluster.toml")
    write_manifest(manifest, data.advertised_endpoint, meta)
    try:
        meta.start(bootstrap=True)
        meta.wait_leader()
        for malformed in ("clustercreate", "clustercreate 1 00",
                          "clustercreate 2 00"):
            reply = meta.ctl(malformed)
            if not reply.startswith(
                    "ERR clustercreate 1 decode bad-request "):
                raise H.Failure(
                    f"cluster-create error envelope is not versioned: "
                    f"command={malformed!r} reply={reply!r}")
        # Data is deliberately started before its identity exists in Meta. It
        # remains fenced while reconnecting; cluster-create must register it,
        # publish the first complete FDS, and drive initialization in-place.
        data.start()
        arguments = [
            CTL, "cluster-create", "--manifest", manifest,
            "--socket", meta.ctl_path, "--timeout-ms", "60000",
        ]
        input_text = "yes\n" if interactive else None
        if not interactive:
            arguments.append("--yes")
        created = command(environment, arguments, input_text=input_text)
        if ("WARNING: existing data on all Data nodes will be erased" not in
                created or "Cluster create accepted: genesis committed=" not in
                created or "Run cluster-status" not in created):
            raise H.Failure(
                f"{name} cluster-create omitted plan or success: {created!r}")
        operation_match = re.search(r"operation=([0-9a-f]{32})", created)
        if operation_match is None:
            raise H.Failure(
                f"{name} cluster-create omitted its operation id: {created!r}")

        status = wait_cluster_ready(
            meta, f"{name} cluster reaches READY", 20)
        status_text = json.dumps(status, sort_keys=True)
        expected_group = {
            "group_id": "group-1",
            "term": "1",
            "owner_node_id": DATA_NODE,
            "serving_ready": True,
            "topology_converged": True,
        }
        groups = status.get("groups", [])
        nodes = status.get("data_nodes", [])
        group = groups[0] if len(groups) == 1 else {}
        node = nodes[0] if len(nodes) == 1 else {}
        ranges = status.get("slot_ranges", [])
        if (status.get("result") != "ready" or len(groups) != 1 or
                len(nodes) != 1 or
                any(group.get(key) != value
                    for key, value in expected_group.items()) or
                node.get("node_id") != DATA_NODE or
                node.get("role") != "primary" or
                not node.get("current_session") or
                not node.get("projection_current") or
                not node.get("population_current") or
                node.get("lease") != "recently_granted" or
                ranges != [{"first": "0", "last": "16383",
                            "group_id": "group-1"}]):
            raise H.Failure(
                f"{name} cluster-status did not expose the exact v1 state: "
                f"{status_text}")

        node_record = command(
            environment, [CTL, "--socket", meta.ctl_path, "getnode", DATA_NODE])
        operation = command(
            environment,
            [CTL, "--socket", meta.ctl_path, "getop",
             operation_match.group(1)])
        if (f"principal=lavik://node/{DATA_NODE}" not in node_record or
                "role=primary" not in node_record or
                operation.strip() != "OK completed cluster-created"):
            raise H.Failure(
                f"{name} durable identity/operation state is wrong: "
                f"node={node_record!r} operation={operation!r}")
        if (meta.completeop(operation_match.group(1), "cluster-created") !=
                "ERR workflow-owned" or
                meta.ctl(f"abortop {operation_match.group(1)} retry") !=
                "ERR workflow-owned"):
            raise H.Failure(
                f"{name} generic Admin terminalization bypassed workflow ownership")
        genesis_index = int(status["genesis_commit_index"])
        if not meta.archiveoperations(genesis_index).startswith("OK "):
            raise H.Failure(f"{name} could not archive the creation root")
        if (meta.completeop(operation_match.group(1), "cluster-created") !=
                "ERR workflow-owned" or
                meta.ctl(f"abortop {operation_match.group(1)} retry") !=
                "ERR workflow-owned"):
            raise H.Failure(
                f"{name} archived root lost workflow ownership")
        if not meta.ctl(f"pruneoperations {genesis_index}").startswith("OK "):
            raise H.Failure(f"{name} could not prune the creation root")
        if (meta.completeop(operation_match.group(1), "cluster-created") !=
                "ERR workflow-owned" or
                meta.ctl(f"abortop {operation_match.group(1)} retry") !=
                "ERR workflow-owned"):
            raise H.Failure(
                f"{name} pruned root lost permanent workflow ownership")
        if meta.ctl("removesrv 1") != "ERR cannot-remove-leader":
            raise H.Failure(f"{name}: successful create leaked admission")

        endpoint = data.advertised_endpoint.removeprefix("tcp://")
        host, port = endpoint.rsplit(":", 1)
        redis = [REDIS_CLI, "-c", "--raw", "-h", host, "-p", port]
        info = command(environment, redis + ["CLUSTER", "INFO"])
        slots = command(environment, redis + ["CLUSTER", "SLOTS"])
        keyslot = command(
            environment, redis + ["CLUSTER", "KEYSLOT", "{create}probe"])
        if ("cluster_state:ok" not in info or
                slots.splitlines()[:2] != ["0", "16383"] or
                not keyslot.strip().isdigit()):
            raise H.Failure(
                f"{name} Redis cluster verification failed: "
                f"info={info!r} slots={slots!r} keyslot={keyslot!r}")
        H.log(f"{name}: cluster-create reached READY and served Redis Cluster")
        data.terminate()
        meta.terminate()
    except Exception:
        H.dump_node_logs([meta])
        print(f"--- Data log tail ({data.log_path}) ---", file=sys.stderr)
        print(data.log_tail(lines=250), file=sys.stderr)
        raise
    finally:
        data.force_kill()
        meta.force_kill()


def run_manifest_bootstrapped_multi_meta_case(workdir, count, late_voter):
    """Every manifest vector creates through the same fixed Meta barrier."""
    # Meta Admin uses Unix-domain sockets, so keep this scenario component
    # short enough for sockaddr_un even when the test-data root is long.
    scenario = os.path.join(workdir, f"manifest-{count}")
    meta_workdir = os.path.join(scenario, "meta")
    os.makedirs(meta_workdir, mode=0o700)
    metas = H.make_nodes(
        META, meta_workdir, count,
        args=H.raft_args(snapshot_distance=100_000))
    data = DataProcess(DATA, os.path.join(scenario, "data"), DATA_NODE,
                       metas[0].data_control_endpoint)
    manifest = os.path.join(scenario, "cluster.toml")
    write_manifest(manifest, data.advertised_endpoint, metas)
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(120)
    post_create_joiner = None
    try:
        # Data may start before any Meta process and remains fail closed while
        # its unregistered session retries the configured Meta seed.
        data.start()
        running = metas[:-1] if late_voter else metas
        for meta in running:
            meta.start(initial_cluster_manifest=manifest)
        leader = H.find_leader(running, timeout=20)
        H.wait_until(
            "initial Meta identities converge before Cluster Create", 10,
            lambda: cluster_status(leader)["meta_membership_stable"])

        connection.connect(leader.ctl_path)
        connection.sendall(
            (create_request(metas, DATA_NODE, data.advertised_endpoint,
                            "group-1") + "\n").encode())

        def waiting_at_barrier():
            status = cluster_status(leader)
            codes = {blocker["code"] for blocker in status["blockers"]}
            return ("meta_catching_up" in codes and
                    ("data_unregistered_retrying" in codes or
                     "data_unregistered" in codes))

        if late_voter:
            H.wait_until(
                "late Meta and pre-registration Data blockers", 15,
                waiting_at_barrier)
            if leader.ctl(f"removesrv {count}") != "ERR config-changing":
                raise H.Failure(
                    "Cluster Create did not retain membership admission")
            old_leader = leader
            old_leader.terminate()
            try:
                H.find_leader(
                    [meta for meta in metas[:-1] if meta is not old_leader],
                    timeout=2)
                raise H.Failure(
                    "submitted Cluster Create elected without a majority")
            except H.Failure as error:
                if "timeout" not in str(error):
                    raise
            metas[-1].start(initial_cluster_manifest=manifest)
            leader = H.find_leader(
                [meta for meta in metas if meta is not old_leader],
                timeout=20)
            # The accepted response may be lost with its leader. Restart that
            # member without the genesis manifest and let the new leader
            # resume the durable root without another create request.
            connection.close()
            connection = None
            old_leader.start()
        else:
            reply = read_reply(connection)
            if not reply.startswith("OK clustercreate 1 "):
                raise H.Failure(
                    "initial Meta barrier did not release Cluster Create: "
                    f"{reply}")
        status = wait_cluster_ready(
            leader, f"{count}-Meta cluster reaches serving readiness", 20)
        if (status["result"] != "ready" or
                not status["meta_membership_stable"] or
                len(status["meta_members"]) != count):
            raise H.Failure(
                f"{count}-Meta Cluster Create is not READY: {status}")
        if count == 3:
            post_create_joiner = H.Node(
                META, meta_workdir, count + 1,
                args=H.raft_args(snapshot_distance=100_000))
            post_create_joiner.start()
            leader = H.find_leader(metas)
            H.join_and_verify(leader, post_create_joiner, timeout=30)
            retire_replies = []

            def post_create_member_retires():
                retire_replies[:] = [
                    leader.ctl(f"removesrv {post_create_joiner.id}")]
                return retire_replies == ["OK"]

            try:
                H.wait_until("post-create membership remains reusable", 10,
                             post_create_member_retires)
            except H.Failure as error:
                raise H.Failure(
                    "post-create membership workflow did not retire member: "
                    f"{retire_replies}") from error
            post_create_joiner.terminate()
            minority = next(meta for meta in metas if meta is not leader)
            minority.terminate()
            before = leader.committed()
            reply = leader.put_automatic_uncontrolled_failover_policy(2)
            match = re.fullmatch(r"OK (\d+)", reply)
            if match is None or int(match.group(1)) <= before:
                raise H.Failure(
                    "post-create writes retained all-peer completion: "
                    f"{reply}")
        H.log(f"manifest-bootstrapped {count}-Meta barrier and "
              "Data-first create — OK")
        data.terminate()
        for meta in metas:
            meta.terminate()
    except Exception:
        H.dump_node_logs(metas)
        print(f"--- Data log tail ({data.log_path}) ---", file=sys.stderr)
        print(data.log_tail(lines=250), file=sys.stderr)
        raise
    finally:
        if connection is not None:
            connection.close()
        data.force_kill()
        if post_create_joiner is not None:
            post_create_joiner.force_kill()
        for meta in metas:
            meta.force_kill()


def run_five_meta_response_loss_case(workdir):
    """Five voters preserve Genesis across response loss and leader restart."""
    scenario = os.path.join(workdir, "five-meta-response-loss")
    meta_workdir = os.path.join(scenario, "meta")
    os.makedirs(meta_workdir, mode=0o700)
    metas = H.make_nodes(
        META, meta_workdir, 5,
        args=H.raft_args(snapshot_distance=100_000))
    manifest = os.path.join(scenario, "cluster.toml")
    write_manifest(
        manifest, f"tcp://127.0.0.1:{H.free_port()}", metas)
    operation_id = os.urandom(16)
    try:
        for meta in metas:
            meta.start(initial_cluster_manifest=manifest)
        leader = H.find_leader(metas, timeout=20)
        H.wait_until("five Meta identities committed", 10,
                     lambda: cluster_status(leader)["meta_membership_stable"])
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(5)
        connection.connect(leader.ctl_path)
        connection.sendall(
            (create_request(
                metas, DATA_NODE, f"tcp://127.0.0.1:{H.free_port()}",
                "group-1", operation_id=operation_id) + "\n").encode())
        connection.close()

        expected_root = operation_id.hex()
        H.wait_until(
            "five-node Genesis survives response loss", 10,
            lambda: cluster_status(leader).get("root_operation_id") ==
            expected_root)
        old_leader = leader
        old_leader.terminate()
        leader = H.find_leader(
            [meta for meta in metas if meta is not old_leader], timeout=20)
        old_leader.start()
        restored = cluster_status(leader)
        if (restored.get("cluster_state") != "creating" or
                restored.get("root_operation_id") != expected_root):
            raise H.Failure(
                f"five Meta failover lost Genesis binding: {restored}")
        H.log("five Meta response loss, re-election, and restart — OK")
        for meta in metas:
            meta.terminate()
    except Exception:
        H.dump_node_logs(metas)
        raise
    finally:
        for meta in metas:
            meta.force_kill()


def write_multi_manifest(path, nodes, automatic, meta):
    by_id = {node.node_id: node for node in nodes}
    lines = ["schema_version = 1"]
    if automatic:
        lines.append('slot_strategy = "contiguous-even"')
    lines.extend([""] + meta_manifest_lines(meta))
    # Reverse every input collection to prove normalization drives preview,
    # wire encoding, and the committed command sequence.
    for node_id in reversed(sorted(by_id)):
        lines.extend([
            "[[data_nodes]]",
            f'id = "{node_id}"',
            f'client_endpoint = "{by_id[node_id].advertised_endpoint}"',
            "",
        ])
    for group_id in reversed(sorted(GROUPS)):
        primary, replica, _, _ = GROUPS[group_id]
        lines.extend([
            "[[groups]]",
            f'id = "{group_id}"',
            f'primary = "{primary}"',
            f'replicas = ["{replica}"]',
            "",
        ])
    if not automatic:
        for group_id in reversed(sorted(GROUPS)):
            _, _, first, last = GROUPS[group_id]
            lines.extend([
                "[[slot_ranges]]",
                f"first = {first}",
                f"last = {last}",
                f'group = "{group_id}"',
                "",
            ])
    with open(path, "w", encoding="utf-8") as output:
        output.write("\n".join(lines))


def endpoint_tuple(data):
    endpoint = data.advertised_endpoint.removeprefix("tcp://")
    host, port = endpoint.rsplit(":", 1)
    return host, int(port)


def redis_slot(key):
    begin = key.find("{")
    if begin >= 0:
        end = key.find("}", begin + 1)
        if end > begin + 1:
            key = key[begin + 1:end]
    crc = 0
    for byte in key.encode():
        crc ^= byte << 8
        for _ in range(8):
            crc = (((crc << 1) ^ 0x1021) & 0xffff
                   if crc & 0x8000 else (crc << 1) & 0xffff)
    return crc & 0x3fff


def key_in_range(group_id, first, last):
    for ordinal in range(100_000):
        key = f"{{gate-{group_id}-{ordinal}}}"
        if first <= redis_slot(key) <= last:
            return key
    raise H.Failure(f"could not generate a key for {group_id}")


def encode_resp(arguments):
    encoded = bytearray(f"*{len(arguments)}\r\n".encode())
    for argument in arguments:
        value = argument.encode()
        encoded.extend(f"${len(value)}\r\n".encode())
        encoded.extend(value)
        encoded.extend(b"\r\n")
    return encoded


def read_resp(reader):
    prefix = reader.read(1)
    if not prefix:
        raise H.Failure("Data closed its Redis connection")
    line = reader.readline()
    if not line.endswith(b"\r\n"):
        raise H.Failure("Data returned a truncated Redis reply")
    payload = line[:-2]
    if prefix == b"+":
        return payload.decode()
    if prefix == b"-":
        raise H.Failure(payload.decode(errors="replace"))
    if prefix == b":":
        return int(payload)
    if prefix == b"$":
        size = int(payload)
        if size == -1:
            return None
        value = reader.read(size)
        if len(value) != size or reader.read(2) != b"\r\n":
            raise H.Failure("Data returned a truncated Redis bulk reply")
        return value.decode()
    if prefix == b"*":
        return [read_resp(reader) for _ in range(int(payload))]
    raise H.Failure(f"Data returned unknown RESP prefix {prefix!r}")


def redis_connection(data):
    if data.tls is None:
        return socket.create_connection(endpoint_tuple(data), timeout=3.0)
    ca, cert, key = data.tls
    context = ssl.create_default_context(cafile=ca)
    context.load_cert_chain(cert, key)
    sock = socket.create_connection(("127.0.0.1", data.tls_port), timeout=3.0)
    try:
        return context.wrap_socket(sock, server_hostname="127.0.0.1")
    except Exception:
        sock.close()
        raise


def retry_clusterdown(description, operation, timeout=2.0):
    """Retry only the finite-lease self-fence admitted by this gate.

    Cluster READY can precede causal confirmation of the latest Grant. With
    the deliberately short process-test lease, Data may therefore reject one
    command while it reconnects for a fresh causal sequence. Every other
    transport or Redis error remains an immediate assertion failure.
    """
    deadline = time.monotonic() + timeout
    attempts = 0
    last_reply = None
    while attempts == 0 or time.monotonic() < deadline:
        if attempts != 0:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.05, remaining))
            if time.monotonic() >= deadline:
                break
        attempts += 1
        try:
            reply = operation()
        except H.Failure as error:
            reply = str(error)
            if reply != TRANSIENT_SELF_FENCE:
                raise
        if reply != TRANSIENT_SELF_FENCE:
            return reply
        last_reply = reply
    raise H.Failure(
        f"{description} remained self-fenced after {attempts} attempts: "
        f"{last_reply}")


def redis_call(data, arguments):
    def call_once():
        with redis_connection(data) as sock:
            sock.settimeout(3.0)
            sock.sendall(encode_resp(arguments))
            return read_resp(sock.makefile("rb"))

    return retry_clusterdown(
        f"Redis {arguments[0]} on {data.node_id}", call_once)


def redis_error(data, arguments):
    def call_once():
        with redis_connection(data) as sock:
            sock.settimeout(3.0)
            sock.sendall(encode_resp(arguments))
            line = sock.makefile("rb").readline()
            if not line.startswith(b"-") or not line.endswith(b"\r\n"):
                raise H.Failure(
                    f"expected Redis error for {arguments}, "
                    f"received {line!r}")
            return line[1:-2].decode(errors="replace")

    return retry_clusterdown(
        f"Redis {arguments[0]} error on {data.node_id}", call_once)


def readonly_get(data, key):
    with redis_connection(data) as sock:
        sock.settimeout(3.0)
        sock.sendall(encode_resp(["READONLY"]) + encode_resp(["GET", key]))
        reader = sock.makefile("rb")
        if read_resp(reader) != "OK":
            raise H.Failure("replica rejected READONLY")
        return read_resp(reader)


def assert_multi_status(status, nodes):
    status_text = json.dumps(status, sort_keys=True)
    actual_groups = {item.get("group_id"): item
                     for item in status.get("groups", [])}
    actual_nodes = {item.get("node_id"): item
                    for item in status.get("data_nodes", [])}
    for group_id, (primary, replica, _, _) in GROUPS.items():
        group = actual_groups.get(group_id, {})
        if (group.get("term") != "1" or
                group.get("owner_node_id") != primary or
                not group.get("serving_ready") or
                not group.get("topology_converged")):
            raise H.Failure(f"{group_id} status is not ready: {status_text}")
        for node_id, role in ((primary, "primary"), (replica, "replica")):
            node = actual_nodes.get(node_id, {})
            if (node.get("role") != role or
                    node.get("group_id") != group_id or
                    not node.get("current_session") or
                    not node.get("projection_current") or
                    not node.get("health_fresh") or
                    not node.get("population_current")):
                raise H.Failure(
                    f"{group_id}/{node_id} is not current: {status_text}")
    expected_ranges = [
        {"first": "0", "last": "8191", "group_id": "group-1"},
        {"first": "8192", "last": "16383", "group_id": "group-2"},
    ]
    if (status.get("result") != "ready" or len(actual_groups) != 2 or
            len(actual_nodes) != len(nodes) or
            status.get("slot_ranges") != expected_ranges):
        raise H.Failure(
            f"multi-Group status is not the normalized v1 state: {status_text}")


def assert_redis_topology_and_replication(nodes):
    by_id = {node.node_id: node for node in nodes}
    keys = {}
    for group_id, (primary_id, replica_id, first, last) in GROUPS.items():
        key = key_in_range(group_id, first, last)
        keys[group_id] = key
        primary = by_id[primary_id]
        value = f"initial-{group_id}"
        if (redis_call(primary, ["SET", key, value]) != "OK" or
                redis_call(primary, ["GET", key]) != value or
                redis_call(primary, ["DEL", key]) != 1):
            raise H.Failure(f"{group_id} primary failed SET/GET/DEL")

        ongoing = f"ongoing-{group_id}"
        if redis_call(primary, ["SET", key, ongoing]) != "OK":
            raise H.Failure(f"{group_id} primary rejected post-create SET")
        replica = by_id[replica_id]
        H.wait_until(
            f"{group_id} replica observes a later primary write", 20,
            lambda: readonly_get(replica, key) == ongoing)
        with open(replica.log_path, "r", encoding="utf-8",
                  errors="replace") as replica_log:
            replica_log_text = replica_log.read()
            apply_context_error = (
                "replica command arrived outside its apply context")
            if apply_context_error in replica_log_text:
                raise H.Failure(
                    f"{group_id} Follow Owner CONTINUE left the retained "
                    "population fenced and fell back to FULL")

    moved = redis_error(by_id[PRIMARY_1], ["GET", keys["group-2"]])
    expected_endpoint = endpoint_tuple(by_id[PRIMARY_2])
    if (not moved.startswith(
            f"MOVED {redis_slot(keys['group-2'])} ") or
            f"{expected_endpoint[0]}:{expected_endpoint[1]}" not in moved):
        raise H.Failure(f"wrong-primary request returned {moved!r}")
    crossslot = redis_error(
        by_id[PRIMARY_1], ["MGET", keys["group-1"], keys["group-2"]])
    if not crossslot.startswith("CROSSSLOT"):
        raise H.Failure(f"cross-Group MGET returned {crossslot!r}")

    rejected_library = (
        "#!lua name=cluster_rejected\n"
        "redis.register_function('cluster_rejected_value', "
        "function(keys, args) return 1 end)")
    for arguments, expected in (
            (["REPLICAOF", "127.0.0.1", "1"],
             "ERR REPLICAOF not allowed in cluster mode."),
            (["FLUSHDB"], "ERR FLUSHDB is not allowed in cluster mode"),
            (["FLUSHALL"], "ERR FLUSHALL is not allowed in cluster mode"),
            (["FUNCTION", "LOAD", rejected_library],
             "ERR FUNCTION LOAD is not allowed in cluster mode"),
            (["FUNCTION", "DELETE", "missing-library"],
             "ERR FUNCTION DELETE is not allowed in cluster mode"),
            (["FUNCTION", "FLUSH"],
             "ERR FUNCTION FLUSH is not allowed in cluster mode"),
            (["FUNCTION", "RESTORE", "payload"],
             "ERR FUNCTION RESTORE is not allowed in cluster mode")):
        actual = redis_error(by_id[PRIMARY_1], arguments)
        if actual != expected:
            raise H.Failure(
                f"cluster command policy for {arguments} returned {actual!r}")

    with redis_connection(by_id[PRIMARY_1]) as sock:
        sock.settimeout(3.0)
        sock.sendall(
            encode_resp(["MULTI"]) +
            encode_resp(["FUNCTION", "LOAD", rejected_library]) +
            encode_resp(["EXEC"]))
        reader = sock.makefile("rb")
        if read_resp(reader) != "OK":
            raise H.Failure("primary rejected MULTI before policy check")
        function_reply = reader.readline()
        if function_reply != (
                b"-ERR FUNCTION LOAD is not allowed in cluster mode\r\n"):
            raise H.Failure(
                "transactional FUNCTION LOAD returned "
                f"{function_reply!r}")
        exec_reply = reader.readline()
        if exec_reply != (
                b"-EXECABORT Transaction discarded because of previous "
                b"errors.\r\n"):
            raise H.Failure(
                f"rejected FUNCTION LOAD did not abort EXEC: {exec_reply!r}")
    if redis_call(by_id[PRIMARY_1], ["FUNCTION", "LIST"]) != []:
        raise H.Failure("rejected FUNCTION LOAD changed the catalog")

    source_host, source_port = endpoint_tuple(by_id[PRIMARY_1])
    followed_arguments = [
        REDIS_CLI, "-c", "--raw", "-h", source_host,
        "-p", str(source_port), "GET", keys["group-2"]]
    followed = retry_clusterdown(
        "redis-cli MOVED follow",
        lambda: command(os.environ.copy(), followed_arguments).strip())
    if followed != "ongoing-group-2":
        raise H.Failure(
            f"redis-cli did not follow MOVED to group-2: {followed!r}")

    info = redis_call(by_id[PRIMARY_1], ["CLUSTER", "INFO"])
    if "cluster_state:ok" not in info:
        raise H.Failure(f"CLUSTER INFO is incomplete: {info!r}")
    slots = redis_call(by_id[PRIMARY_1], ["CLUSTER", "SLOTS"])
    expected = []
    for group_id, (primary_id, replica_id, first, last) in GROUPS.items():
        primary_host, primary_port = endpoint_tuple(by_id[primary_id])
        replica_host, replica_port = endpoint_tuple(by_id[replica_id])
        expected.append([
            first, last,
            [primary_host, primary_port, primary_id],
            [replica_host, replica_port, replica_id],
        ])
    if slots != expected:
        raise H.Failure(f"CLUSTER SLOTS is incomplete: {slots!r}")


def stopped_replica_blocker(environment, meta, replica):
    def not_ready_with_node_blocker():
        result = subprocess.run(
            [CTL, "cluster-status", "--socket", meta.ctl_path, "--json"],
            capture_output=True, text=True, timeout=5, env=environment)
        if result.returncode != 2:
            return False
        status = json.loads(result.stdout)
        return status.get("result") == "not_ready" and any(
            blocker.get("scope") == f"node:{replica.node_id}" and
            "group=group-2" in blocker.get("detail", "")
            for blocker in status.get("blockers", []))

    H.wait_until(
        "stopped replica makes the cluster NOT READY with an exact blocker",
        20, not_ready_with_node_blocker)


def run_multi_group_case(workdir, automatic, interactive,
                         block_replica_during_create=False,
                         restart_replica_during_create=False,
                         data_first=False, worker_counts=None):
    layout = "automatic" if automatic else "explicit"
    mode = "interactive" if interactive else "yes"
    blocked = "-blocked" if block_replica_during_create else ""
    restarted = "-restart" if restart_replica_during_create else ""
    name = f"multi-{layout}-{mode}{blocked}{restarted}"
    if worker_counts is not None:
        name += "-workers-" + "-".join(map(str, worker_counts))
    scenario = os.path.join(workdir, name)
    os.makedirs(scenario, mode=0o700)
    meta_workdir = os.path.join(scenario, "meta")
    os.makedirs(meta_workdir, mode=0o700)
    meta = H.Node(META, meta_workdir, 1,
                  args=H.raft_args(snapshot_distance=100_000))
    # Hold target rebuilds after source initialization so snapshots contain
    # real data. The advertised proxy also captures reconnects and redirects.
    proxy = (DirectiveBarrier(meta.data_control_port,
                              recipients=(REPLICA_1, REPLICA_2))
             if worker_counts is not None else None)
    if proxy:
        meta.advertised_data_control_endpoint = proxy.endpoint
    nodes = [
        DataProcess(DATA, os.path.join(scenario, "primary-1"), PRIMARY_1,
                    meta.data_control_endpoint),
        DataProcess(DATA, os.path.join(scenario, "replica-1"), REPLICA_1,
                    meta.data_control_endpoint),
        DataProcess(DATA, os.path.join(scenario, "primary-2"), PRIMARY_2,
                    meta.data_control_endpoint),
        DataProcess(DATA, os.path.join(scenario, "replica-2"), REPLICA_2,
                    meta.data_control_endpoint),
    ]
    if proxy:
        for node, workers in zip(nodes, worker_counts):
            node.workers = workers
            node.seed = proxy.endpoint
    snapshot_values = {}
    environment = os.environ.copy()
    environment["PATH"] = (os.path.dirname(REDIS_CLI) + os.pathsep +
                           environment.get("PATH", ""))
    manifest = os.path.join(scenario, "cluster.toml")
    write_multi_manifest(manifest, nodes, automatic, meta)
    started_nodes = nodes[:-1] if block_replica_during_create else nodes

    def start_data_nodes():
        for node in started_nodes:
            variable = "LAVIK_REPLICATION_PAUSE_FULLSYNC_BEFORE_CATALOG_ACK_MS"
            old = os.environ.get(variable)
            try:
                if restart_replica_during_create and node is nodes[0]:
                    os.environ[variable] = "30000"
                node.start()
            finally:
                if old is None:
                    os.environ.pop(variable, None)
                else:
                    os.environ[variable] = old

    try:
        if proxy:
            proxy.start()
        # The normal acceptance path starts the complete multi-Data,
        # multi-Group cohort before Meta. Other fault cases retain their
        # targeted ordering but still use the same complete genesis manifest.
        if data_first:
            start_data_nodes()
        meta.start(initial_cluster_manifest=manifest)
        meta.wait_leader()
        if not data_first:
            start_data_nodes()
        arguments = [
            CTL, "cluster-create", "--manifest", manifest,
            "--socket", meta.ctl_path, "--timeout-ms",
            "5000" if block_replica_during_create else "120000",
        ]
        input_text = "yes\n" if interactive else None
        if not interactive:
            arguments.append("--yes")
        if restart_replica_during_create:
            created = command(
                environment, arguments, input_text=input_text, timeout=20)
            operation_match = re.search(
                r"operation=([0-9a-f]{32})", created)
            if operation_match is None:
                raise H.Failure("accepted create omitted root operation")
            operation_id = operation_match.group(1)
            H.wait_until(
                "primary pauses while replica full sync is in progress", 20,
                lambda: "native full sync holds command gates before catalog "
                        "acknowledgement" in nodes[0].log_tail())
            nodes[1].force_kill()
            nodes[1].start()

            def provisioning_failed():
                status = cluster_status(meta)
                return (status.get("cluster_state") ==
                        "provisioning-failed")

            H.wait_until("replica restart fails provisioning", 20,
                         provisioning_failed)
            if not meta.getop(operation_id).startswith("OK aborted "):
                raise H.Failure("replica restart left the root active")
            groups = {group["group_id"]: group
                      for group in cluster_status(meta)["groups"]}
            if groups["group-1"]["serving_ready"]:
                raise H.Failure("failed Group retained serving authority")
            H.log(f"{name}: accepted Genesis later fenced its Group and "
                  "reported provisioning-failed")
            for node in nodes:
                node.terminate()
            meta.terminate()
            return
        if block_replica_during_create:
            result = subprocess.run(
                arguments, input=input_text, capture_output=True, text=True,
                timeout=15, env=environment)
            if (result.returncode != 0 or
                    "Cluster create accepted:" not in result.stdout):
                raise H.Failure(
                    "stopped replica prevented Genesis acceptance: "
                    f"returncode={result.returncode} "
                    f"stdout={result.stdout!r} stderr={result.stderr!r}")
            stopped_replica_blocker(environment, meta, nodes[-1])
            status_result = subprocess.run(
                [CTL, "cluster-status", "--socket", meta.ctl_path, "--json"],
                capture_output=True, text=True, timeout=5, env=environment)
            status = json.loads(status_result.stdout)
            if (status_result.returncode != 2 or
                    status.get("cluster_state") != "creating" or
                    not status.get("next_action")):
                raise H.Failure(
                    f"creating status omitted guidance: {status_result}")
            H.log(f"{name}: accepted create stayed incomplete; status named "
                  "the blocked Group/Node and next action")
            for node in started_nodes:
                node.terminate()
            meta.terminate()
            return
        if proxy:
            creator = subprocess.Popen(
                arguments, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, env=environment)
            try:
                if input_text is not None:
                    creator.stdin.write(input_text)
                    creator.stdin.flush()
                by_id = {node.node_id: node for node in nodes}
                for group_id, (primary_id, replica_id, first, last) in GROUPS.items():
                    held, release = proxy.recipients[replica_id]
                    H.wait_until(f"{group_id} target rebuild held", 30, held.is_set)
                    primary = by_id[primary_id]
                    values = {key_in_range(f"{group_id}-snapshot-{i}", first, last):
                              f"snapshot-{i}-" + "x" * 4096 for i in range(32)}
                    key, value = next(iter(values.items()))

                    def primary_writable():
                        try:
                            return redis_call(primary, ["SET", key, value]) == "OK"
                        except H.Failure as error:
                            if str(error).startswith(("CLUSTERDOWN", "TRYAGAIN")):
                                return False
                            raise

                    H.wait_until(f"{group_id} initialized primary writable", 10,
                                 primary_writable)
                    for key, value in values.items():
                        if redis_call(primary, ["SET", key, value]) != "OK":
                            raise H.Failure("snapshot seed write failed")
                    snapshot_values[replica_id] = values
                    release.set()
                created, stderr = creator.communicate(timeout=120)
                if creator.returncode != 0:
                    raise H.Failure(f"heterogeneous creation failed: {created!r} {stderr!r}")
            finally:
                if creator.poll() is None:
                    creator.kill()
                    creator.communicate(timeout=5)
        else:
            created = command(
                environment, arguments, input_text=input_text, timeout=150)
        operation_match = re.search(r"operation=([0-9a-f]{32})", created)
        if ("WARNING: existing data on all Data nodes will be erased" not in
                created or "Cluster create accepted:" not in created or
                "Run cluster-status" not in created or
                operation_match is None):
            raise H.Failure(
                f"{name} omitted normalized preview or outcomes: {created!r}")
        markers = [
            f"Data node: {PRIMARY_1}", f"Data node: {REPLICA_1}",
            f"Data node: {PRIMARY_2}", f"Data node: {REPLICA_2}",
            "Group: group-1", "Group: group-2",
            "Slots: 0-8191 -> group-1",
            "Slots: 8192-16383 -> group-2",
        ]
        positions = [created.find(marker) for marker in markers]
        if -1 in positions or positions != sorted(positions):
            raise H.Failure(f"{name} preview was not normalized: {created!r}")
        status = wait_cluster_ready(
            meta, f"{name}: background create reaches READY", 90)
        assert_multi_status(status, nodes)
        operation_id = operation_match.group(1)
        if meta.getop(operation_id) != "OK completed cluster-created":
            raise H.Failure("root ClusterCreate operation did not complete")
        assert_redis_topology_and_replication(nodes)
        if proxy:
            by_id = {node.node_id: node for node in nodes}
            for primary_id, replica_id, _, _ in GROUPS.values():
                primary, replica = by_id[primary_id], by_id[replica_id]
                info = redis_call(replica, ["INFO", "replication"])
                for field in ("lavik_source_workers", "lavik_connected_flows"):
                    if f"{field}:{primary.workers}\r\n" not in info:
                        raise H.Failure(f"source layout was not preserved: {info!r}")
                values = snapshot_values[replica_id]
                for key, value in values.items():
                    if readonly_get(replica, key) != value:
                        raise H.Failure(f"snapshot missing {key}")
                    if redis_call(primary, ["SET", key, "delta-" + value]) != "OK":
                        raise H.Failure(f"incremental write failed for {key}")
                H.wait_until(
                    f"{primary.workers}->{replica.workers} incremental replication", 20,
                    lambda: all(readonly_get(replica, key) == "delta-" + value
                                for key, value in values.items()))
                H.log(f"{primary.workers}->{replica.workers}: snapshot and incremental writes verified")
        H.log(f"{name}: both Groups routed, replicated, and reached READY")
        for node in nodes:
            node.terminate()
        meta.terminate()
    except Exception:
        H.dump_node_logs([meta])
        for node in nodes:
            print(f"--- Data log tail ({node.log_path}) ---", file=sys.stderr)
            print(node.log_tail(lines=250), file=sys.stderr)
        raise
    finally:
        if proxy:
            proxy.close()
        for node in nodes:
            node.force_kill()
        meta.force_kill()


def run_tls_create_case(workdir, tls_only):
    """mTLS acceptance must lead to a populated replica and working writes."""
    name = "tls-only-create" if tls_only else "tls-dual-create"
    scenario = os.path.join(workdir, name)
    os.makedirs(scenario, mode=0o700)
    certdir = os.path.join(scenario, "certs")
    ca, ca_key = make_ca(certdir)
    meta_cert, meta_key = make_leaf(
        certdir, ca, ca_key, "meta", "lavik://meta/1")
    operator_cert, operator_key = make_leaf(
        certdir, ca, ca_key, "operator", "lavik://operator/create-test")
    meta = H.Node(META, scenario, 1, args=(
        H.raft_args(snapshot_distance=100_000) +
        H.tls_args(ca, meta_cert, meta_key) +
        ["--ctl-tls-ca", ca, "--ctl-tls-cert", meta_cert,
         "--ctl-tls-key", meta_key]))
    nodes = []
    for index, node_id in enumerate((PRIMARY_1, REPLICA_1)):
        cert, key = make_leaf(
            certdir, ca, ca_key, f"data-{index}", f"lavik://node/{node_id}")
        nodes.append(DataProcess(
            DATA, os.path.join(scenario, f"data-{index}"), node_id,
            meta.data_control_endpoint, tls=(ca, cert, key), tls_only=tls_only))
    manifest = os.path.join(scenario, "cluster.toml")
    lines = (['schema_version = 1', 'slot_strategy = "contiguous-even"'] +
             meta_manifest_lines(meta))
    for node in nodes:
        lines.extend(['[[data_nodes]]', f'id = "{node.node_id}"'])
        if not tls_only:
            lines.append(f'client_endpoint = "{node.advertised_endpoint}"')
        lines.append(f'tls_endpoint = "tls://127.0.0.1:{node.tls_port}"')
    lines.extend(['[[groups]]', 'id = "group-1"',
                  f'primary = "{PRIMARY_1}"', f'replicas = ["{REPLICA_1}"]'])
    with open(manifest, "w", encoding="utf-8") as output:
        output.write("\n".join(lines) + "\n")
    admin = ["--addr", meta.ctl_endpoint, "--tls-ca", ca,
             "--tls-cert", operator_cert, "--tls-key", operator_key]
    environment = os.environ.copy()
    try:
        meta.start(initial_cluster_manifest=manifest)
        meta.wait_leader()
        for node in nodes:
            node.start()
        created = command(environment, [
            CTL, "cluster-create", "--manifest", manifest, "--yes"] + admin)
        if "Cluster create accepted:" not in created:
            raise H.Failure(f"{name} did not accept Genesis: {created}")
        for node in nodes:
            if f"tls://127.0.0.1:{node.tls_port}" not in created:
                raise H.Failure(f"{name} plan omitted the Data TLS endpoint")
        # A successful Admin reply only confirms Genesis. Observe the actual
        # population/replication outcome so missing TLS ports cannot pass.
        wait_cluster_ready(meta, f"{name} reaches READY", 30, admin=admin)
        primary, replica = nodes
        for ordinal in range(3):
            key, value = f"tls-create-{ordinal}", f"replicated-{ordinal}"
            if redis_call(primary, ["SET", key, value]) != "OK":
                raise H.Failure(f"{name} primary write failed")
            if redis_call(primary, ["GET", key]) != value:
                raise H.Failure(f"{name} primary read failed")
            H.wait_until(f"{name} replica receives {key}", 10,
                         lambda: readonly_get(replica, key) == value)
        H.log(f"{name}: mTLS Admin, Data control and replication reached READY")
        for node in nodes:
            node.terminate()
        meta.terminate()
    except Exception:
        print(meta.log_tail(lines=100), file=sys.stderr)
        for node in nodes:
            print(node.log_tail(lines=100), file=sys.stderr)
        raise
    finally:
        for node in nodes:
            node.force_kill()
        meta.force_kill()


def run_group_id_probe_case(workdir):
    """An unusual Group id survives accepted Genesis and async creation."""
    scenario = os.path.join(workdir, "group-id-probe")
    os.makedirs(scenario, mode=0o700)
    meta = H.Node(META, scenario, 1,
                  args=H.raft_args(snapshot_distance=100_000))
    nodes = [DataProcess(DATA, os.path.join(scenario, str(index)), node_id,
                         meta.data_control_endpoint)
             for index, node_id in enumerate((PRIMARY_1, PRIMARY_2))]
    manifest = os.path.join(scenario, "cluster.toml")
    # Keep a delimiter-like character in the durable identifier so the
    # lifecycle path proves it does not reinterpret Group ids as probe keys.
    lines = (['schema_version = 1', 'slot_strategy = "contiguous-even"'] +
             meta_manifest_lines(meta))
    for node, group_id in zip(nodes, ("group-2}", "z")):
        lines.extend(['[[data_nodes]]', f'id = "{node.node_id}"',
                      f'client_endpoint = "{node.advertised_endpoint}"',
                      '[[groups]]', f'id = "{group_id}"',
                      f'primary = "{node.node_id}"'])
    with open(manifest, "w", encoding="utf-8") as output:
        output.write("\n".join(lines) + "\n")
    environment = os.environ.copy()
    environment["PATH"] = (os.path.dirname(REDIS_CLI) + os.pathsep +
                           environment.get("PATH", ""))
    try:
        meta.start(bootstrap=True)
        meta.wait_leader()
        for node in nodes:
            node.start()
        result = command(environment, [
            CTL, "cluster-create", "--manifest", manifest, "--socket",
            meta.ctl_path, "--yes", "--timeout-ms", "20000"], timeout=25)
        if "Cluster create accepted:" not in result:
            raise H.Failure(f"unusual Group id was not accepted: {result}")
        status = wait_cluster_ready(
            meta, "unusual Group id reaches READY", 20)
        if "group-2}" not in {group["group_id"] for group in status["groups"]}:
            raise H.Failure(f"unusual Group id was not preserved: {status}")
        H.log("Group id containing '}' survives async creation")
        for node in nodes:
            node.terminate()
        meta.terminate()
    finally:
        for node in nodes:
            node.force_kill()
        meta.force_kill()


class DirectiveBarrier(H.Proxy):
    """Hold one complete control frame without changing its bytes/identity."""

    def __init__(self, target_port, result=False, recipients=()):
        super().__init__("result" if result else "directive", target_port)
        self.result = result
        self.blocked = threading.Event()
        self.release = threading.Event()
        self.recipients = {node_id: (threading.Event(), threading.Event())
                           for node_id in recipients}

    def _pump(self, src, dst, pair):
        selected = src is pair[0] if self.result else src is pair[1]
        if not selected or self.blocked.is_set():
            return super()._pump(src, dst, pair)

        def exact(size):
            data = b""
            while len(data) < size:
                chunk = src.recv(size - len(data))
                if not chunk:
                    raise OSError("control stream ended")
                data += chunk
            return data

        try:
            while True:
                header = exact(28)
                magic, version, kind = struct.unpack_from(">IHH", header)
                size = struct.unpack_from(">I", header, 12)[0]
                if magic != 0x4c564350 or version != 1 or size > (1 << 20):
                    raise H.Failure("unexpected control frame")
                payload = exact(size)
                # NodeControlUpdate starts with request id and optional task delta.
                # The barrier holds the actual task delivery, whose body is no
                # longer resent as a separate Directive after FDS installation.
                task_offset = None
                if kind == 19 and len(payload) >= 37 and payload[16]:
                    upserts = struct.unpack_from(">I", payload, 33)[0]
                    if upserts:
                        task_offset = 37
                if (self.result and kind == 14) or (not self.result and task_offset is not None):
                    blocked, release = self.blocked, self.release
                    if self.recipients:
                        # A task starts with the Group authority anchor and
                        # operation/directive/attempt identity, then recipient.
                        group_size = struct.unpack_from(">I", payload, task_offset)[0]
                        recipient_offset = task_offset + 4 + group_size + 16 + 8 + 3 * 16 + 8
                        recipient = payload[recipient_offset:recipient_offset + 40].decode()
                        events = self.recipients.get(recipient)
                        if events is None or events[0].is_set():
                            dst.sendall(header + payload)
                            continue
                        blocked, release = events
                    blocked.set()
                    if not release.wait(20):
                        raise OSError("test barrier timed out")
                    dst.sendall(header + payload)
                    return super()._pump(src, dst, pair)
                dst.sendall(header + payload)
        except OSError:
            self._cut_pair(pair)

    def close(self):
        self.release.set()
        for _, release in self.recipients.values():
            release.set()
        super().close()


def root_operation(meta, phase=None):
    suffix = re.escape(phase) if phase else r"[^\s]+"
    matches = re.findall(r"cluster-create ([0-9a-f]{32}) phase=" + suffix,
                         meta.log_tail(lines=1000))
    return matches[-1] if matches else None


def run_recovery_case(workdir, phase, snapshot=False, wire=None, crash=False):
    name = "recover-" + phase
    scenario = os.path.join(workdir, name)
    os.makedirs(scenario, mode=0o700)
    meta = H.Node(META, scenario, 1,
                  args=H.raft_args(snapshot_distance=100_000))
    proxy = (DirectiveBarrier(meta.data_control_port, result=wire == "result")
             if wire else None)
    if proxy:
        # The manifest owns the advertised endpoint while the process flag
        # owns the local bind. Advertising the proxy keeps reconnects on the
        # same observable path instead of racing the first session's FDS.
        meta.advertised_data_control_endpoint = proxy.endpoint
    data = DataProcess(DATA, os.path.join(scenario, "data"), DATA_NODE,
                       proxy.endpoint if proxy else meta.data_control_endpoint)
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(5)
    online = wire is not None or phase in (
        "result-committed", "directive-removed", "child-completed")
    sentinel = False
    try:
        variable = "LAVIK_TEST_PAUSE_CLUSTER_CREATE_PHASE"
        old = os.environ.get(variable)
        if wire is None:
            os.environ[variable] = phase
        try:
            meta.start(bootstrap=True)
        finally:
            if old is None:
                os.environ.pop(variable, None)
            else:
                os.environ[variable] = old
        meta.wait_leader()
        H.wait_until("bootstrap identity", 5,
                     lambda: cluster_status(meta)["meta_membership_stable"])
        if proxy:
            proxy.start()
        if online and not proxy:
            data.start()
        connection.connect(meta.ctl_path)
        connection.sendall((create_request(
            meta, DATA_NODE, data.advertised_endpoint, "group-1") +
            "\n").encode())
        accepted = read_reply(connection)
        if not accepted.startswith("OK clustercreate 1 "):
            raise H.Failure(f"{name}: Genesis was not accepted: {accepted}")
        operation_id = accepted.split()[-1]
        if proxy:
            # Install the topology before connecting through the barrier. A
            # superseded initial FDS can redirect a reconnect to Meta's real
            # advertised endpoint, bypassing this test-only seed proxy.
            H.wait_until(f"{name}: topology committed", 10,
                         lambda: root_operation(meta, "wait-data-projection"))
            data.start()
            if not proxy.blocked.wait(10):
                raise H.Failure(f"{name}: control frame was not held")
        else:
            H.wait_until(f"{name}: durable cut", 10,
                         lambda: root_operation(meta, phase))
        if root_operation(meta) != operation_id:
            raise H.Failure(f"{name}: durable operation identity changed")
        if phase == "submitted":
            status = cluster_status(meta)
            if status["data_nodes"] or status["groups"]:
                raise H.Failure("topology changed before the submitted-intent cut")
        if online and not proxy:
            H.wait_until("Data can serve before Meta operation completion", 15,
                         lambda: data.command_head(["SET", "recovery-sentinel", "keep"]) == "+OK")
            sentinel = True
        if snapshot:
            H.manual_snapshot(meta)
        started = time.monotonic()
        if crash:
            meta.kill9()
        else:
            meta.terminate()
        elapsed = time.monotonic() - started
        if elapsed > 2:
            raise H.Failure(f"{name}: shutdown waited {elapsed:.3f}s for Data")
        if proxy:
            # Model loss, not delayed delivery on the obsolete socket. A
            # delayed initialization could start just before detecting EOF
            # and correctly fail closed as a cancelled destructive attempt.
            proxy.set_drop()
            proxy.release.set()
            proxy.heal()
        # No new cluster-create request: restored operation discovery alone
        # must drive all remaining commits and reuse the original child.
        meta.start(bootstrap=True)
        meta.wait_leader()
        if not online:
            data.start()
        H.wait_until(f"{name}: original operation completes after restart", 25,
                     lambda: meta.getop(operation_id) == "OK completed cluster-created")
        wait_cluster_ready(meta, f"{name}: recovered cluster READY", 20)
        if sentinel:
            arguments = [
                REDIS_CLI, "--raw", "-p", str(data.redis_port),
                "GET", "recovery-sentinel"]
            value = retry_clusterdown(
                f"{name} recovery sentinel read",
                lambda: command(os.environ.copy(), arguments).strip())
            if value != "keep":
                raise H.Failure(f"{name}: recovery repeated destructive initialization")
        if meta.ctl("removesrv 1") != "ERR cannot-remove-leader":
            raise H.Failure(f"{name}: completed task retained admission")
        H.log(f"{name}: {'SIGKILL' if crash else 'SIGTERM'} {elapsed:.3f}s; original operation recovered "
              f"from {'snapshot' if snapshot else 'WAL'} without resubmission")
        data.terminate()
        meta.terminate()
    except Exception:
        if meta.alive() and 'operation_id' in locals():
            H.log(f"{name}: retained operation: {meta.getop(operation_id)}")
        H.dump_node_logs([meta])
        print(data.log_tail(lines=150), file=sys.stderr)
        raise
    finally:
        connection.close()
        if proxy:
            proxy.close()
        data.force_kill()
        meta.force_kill()


def has_fault(binary, needle):
    # Release builds erase the hook and its arguments. Scan in bounded chunks
    # so these optional deterministic cuts also work with stripped binaries.
    tail = b""
    with open(binary, "rb") as source:
        while chunk := source.read(1 << 20):
            joined = tail + chunk
            if needle in joined:
                return True
            tail = joined[-len(needle):]
    return False


def main():
    work_argv = [sys.argv[0], META] + sys.argv[5:]
    workdir, keep = H.make_workdir(work_argv, "cluster_create_")
    try:
        run_unrelated_commit_case(workdir)
        for transports in (("unix", "unix"), ("tcp", "tcp"), ("unix", "tcp")):
            run_concurrent_case(workdir, transports)
        run_manifest_bootstrapped_multi_meta_case(
            workdir, 3, late_voter=True)
        run_manifest_bootstrapped_multi_meta_case(
            workdir, 5, late_voter=False)
        run_multi_group_case(workdir, automatic=True, interactive=True,
                             data_first=True)
        run_multi_group_case(workdir, automatic=False, interactive=False)
        run_multi_group_case(workdir, automatic=True, interactive=False,
                             worker_counts=(1, 2, 3, 2))
        run_multi_group_case(workdir, automatic=False, interactive=False,
                             block_replica_during_create=True)
        run_group_id_probe_case(workdir)
        run_tls_create_case(workdir, tls_only=False)
        run_tls_create_case(workdir, tls_only=True)
        if has_fault(DATA, b"LAVIK_REPLICATION_PAUSE_FULLSYNC_BEFORE_CATALOG_ACK_MS"):
            run_multi_group_case(workdir, automatic=True, interactive=False,
                                 restart_replica_during_create=True)
        else:
            H.log("SKIP replica restart cut: Data binary has no full-sync pause hook")
        run_case(workdir, interactive=False)
        if has_fault(META, b"LAVIK_TEST_PAUSE_CLUSTER_CREATE_PHASE"):
            for phase, snapshot in (("submitted", False), ("create-groups", True),
                                    ("wait-data-projection", False),
                                    ("result-committed", True),
                                    ("directive-removed", False),
                                    ("child-completed", False)):
                run_recovery_case(workdir, phase, snapshot=snapshot, crash=snapshot)
        else:
            H.log("SKIP phase-pause cuts: ordinary Release erases test hooks")
        run_recovery_case(workdir, "wire-directive", wire="directive")
        run_recovery_case(workdir, "wire-result", wire="result")
        # Keep the new client-loss fault cuts after the existing Data/full-sync
        # matrix so their process churn cannot perturb those timing-sensitive
        # integration scenarios.
        run_single_meta_client_loss_case(workdir)
        run_five_meta_response_loss_case(workdir)
        H.log("PASS")
        return 0
    except Exception as error:  # noqa: BLE001 - logs are test evidence
        H.log(f"FAIL: {error}")
        keep = True
        return 1
    finally:
        H.cleanup(workdir, keep)


if len(sys.argv) not in (5, 6):
    print(__doc__, file=sys.stderr)
    sys.exit(2)
META = os.path.abspath(sys.argv[1])
DATA = os.path.abspath(sys.argv[2])
CTL = os.path.abspath(sys.argv[3])
REDIS_CLI = os.path.abspath(sys.argv[4])
H.set_tag("cluster-create")
sys.exit(main())
