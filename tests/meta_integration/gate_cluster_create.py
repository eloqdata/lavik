#!/usr/bin/env python3
"""Real-process single-Meta cluster-create and recovery gate.

Usage: gate_cluster_create.py META DATA CTL REDIS_CLI [workdir]
"""

import json
import os
import re
import select
import socket
import struct
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402
from gate_data_control import DataProcess  # noqa: E402


DATA_NODE = "0123456789abcdef0123456789abcdef01234567"
PRIMARY_1 = "1111111111111111111111111111111111111111"
REPLICA_1 = "2222222222222222222222222222222222222222"
PRIMARY_2 = "3333333333333333333333333333333333333333"
REPLICA_2 = "4444444444444444444444444444444444444444"
GROUPS = {
    "group-1": (PRIMARY_1, REPLICA_1, 0, 8191),
    "group-2": (PRIMARY_2, REPLICA_2, 8192, 16383),
}


def write_manifest(path, endpoint):
    with open(path, "w", encoding="utf-8") as output:
        output.write(
            "schema_version = 1\n\n"
            "[[meta_members]]\nid = 1\n\n"
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


def cluster_status(meta):
    result = subprocess.run(
        [CTL, "cluster-status", "--socket", meta.ctl_path, "--json"],
        capture_output=True, text=True, timeout=5)
    if result.returncode not in (0, 2):
        raise H.Failure(f"cluster-status failed: {result}")
    return json.loads(result.stdout)


def create_request(node_id, endpoint, group_id, meta_id=1, timeout_ms=3000):
    """Send a normalized public v1 envelope so CLI cannot hide races."""
    payload = struct.pack(">HIHI", 1, meta_id, 0, 1)
    for value in (node_id, endpoint):
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
    payload += struct.pack(">I", timeout_ms)
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
                if (struct.unpack_from(">IHH", prefix) != (0x4b4c4350, 1, 2) or
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
        request = create_request(DATA_NODE, data.advertised_endpoint,
                                 "group-1", timeout_ms=10000)
        connection.sendall(request.encode() + b"\n")
        # Complete all topology/authority commits before starting Data, so
        # the only later metadata change is the unreferenced policy below.
        H.wait_until("creation committed its authority", 5,
                     lambda: any(group.get("owner_node_id") == DATA_NODE
                                 for group in cluster_status(meta)["groups"]))
        proxy.start()
        data.start()
        if not proxy.blocked.wait(5) or proxy.error is not None:
            raise H.Failure(f"initial FDS was not held: {proxy.error}")

        before = meta.committed()
        reply = meta.putpolicy("unreferenced-policy", 1, "unused-content")
        match = re.fullmatch(r"OK (\d+)", reply)
        if match is None or int(match.group(1)) <= before:
            raise H.Failure(f"unrelated policy did not advance Meta: {reply}")
        held = cluster_status(meta)
        if (any(node["current_session"] or node["projection_current"]
                for node in held["data_nodes"]) or
                select.select([connection], [], [], 0)[0]):
            raise H.Failure("creation advanced before Data acknowledged FDS")

        # The initial object's source index predates this commit, but its
        # semantic content is still current. No further commits are needed to
        # unblock creation: the publisher must validate the installed object.
        proxy.release.set()
        reply = read_reply(connection)
        if not reply.startswith("OK clustercreate 1 "):
            raise H.Failure(
                f"unrelated commit stalled creation: {reply}; "
                f"status={cluster_status(meta)}")
        if proxy.error is not None:
            raise H.Failure(f"initial FDS barrier failed: {proxy.error}")
        H.wait_until("creation with an unrelated commit reaches READY", 15,
                     lambda: cluster_status(meta)["result"] == "ready")
        operation_id = reply.split()[-1]
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
            node_ids[0], endpoints[0], groups[0], meta_id=2))
        if (not rejected.startswith(
                "ERR clustercreate 1 preflight non-empty-cluster ") or
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
            request = create_request(node_ids[index], endpoints[index],
                                     groups[index])
            connection.sendall(request.encode() + b"\n")
        meta.resume()

        # No Data process is running, so the admitted creator stays in its
        # projection wait. The competing request must fail before committing
        # its identity, not part-way through its topology workflow.
        readable, _, _ = select.select(connections, [], [], 5)
        if not readable:
            raise H.Failure(f"{name}: competing create was not rejected")
        loser = connections.index(readable[0])
        winner = 1 - loser
        reply = read_reply(connections[loser])
        if not reply.startswith(
                "ERR clustercreate 1 preflight domain-rejected "):
            raise H.Failure(f"{name}: create escaped admission: {reply}")
        H.wait_until(f"{name}: admitted identity committed", 2,
                     lambda: meta.getnode(node_ids[winner]).startswith("OK "))
        if meta.getnode(node_ids[loser]) != "ERR not-found":
            raise H.Failure(f"{name}: rejected create left a durable identity")
        if meta.ctl("removesrv 1") != "ERR config-changing":
            raise H.Failure(f"{name}: membership bypassed creation admission")

        reply = read_reply(connections[winner])
        if not reply.startswith(
                "ERR clustercreate 1 wait-data-projection uncertain-outcome "):
            raise H.Failure(
                f"{name}: admitted create did not reach Data wait: {reply}")
        # An Admin timeout cancels only the wait. The durable background task
        # retains the single-Meta premise until completion or explicit abort.
        if meta.ctl("removesrv 1") != "ERR config-changing":
            raise H.Failure(f"{name}: timeout abandoned durable admission")
        retry = meta.ctl(create_request(node_ids[winner], endpoints[winner],
                                       groups[winner]))
        if not retry.startswith(
                "ERR clustercreate 1 preflight domain-rejected "):
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
    write_manifest(manifest, data.advertised_endpoint)
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
                created or "Cluster READY:" not in created):
            raise H.Failure(
                f"{name} cluster-create omitted plan or success: {created!r}")
        operation_match = re.search(r"operation=([0-9a-f]{32})", created)
        if operation_match is None:
            raise H.Failure(
                f"{name} cluster-create omitted its operation id: {created!r}")

        status_text = command(
            environment,
            [CTL, "cluster-status", "--socket", meta.ctl_path, "--json"])
        status = json.loads(status_text)
        expected_group = {
            "group_id": "group-1",
            "term": "1",
            "owner_node_id": DATA_NODE,
            "config_epoch": "1",
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
        if (f"principal=keylane://node/{DATA_NODE}" not in node_record or
                "role=primary" not in node_record or
                operation.strip() != "OK completed cluster-created"):
            raise H.Failure(
                f"{name} durable identity/operation state is wrong: "
                f"node={node_record!r} operation={operation!r}")
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


def write_multi_manifest(path, nodes, automatic):
    by_id = {node.node_id: node for node in nodes}
    lines = ["schema_version = 1"]
    if automatic:
        lines.append('slot_strategy = "contiguous-even"')
    lines.extend(["", "[[meta_members]]", "id = 1", ""])
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


def redis_call(data, arguments):
    with socket.create_connection(endpoint_tuple(data), timeout=3.0) as sock:
        sock.settimeout(3.0)
        sock.sendall(encode_resp(arguments))
        return read_resp(sock.makefile("rb"))


def redis_error(data, arguments):
    with socket.create_connection(endpoint_tuple(data), timeout=3.0) as sock:
        sock.settimeout(3.0)
        sock.sendall(encode_resp(arguments))
        line = sock.makefile("rb").readline()
        if not line.startswith(b"-") or not line.endswith(b"\r\n"):
            raise H.Failure(
                f"expected Redis error for {arguments}, received {line!r}")
        return line[1:-2].decode(errors="replace")


def readonly_get(data, key):
    with socket.create_connection(endpoint_tuple(data), timeout=3.0) as sock:
        sock.settimeout(3.0)
        sock.sendall(encode_resp(["READONLY"]) + encode_resp(["GET", key]))
        reader = sock.makefile("rb")
        if read_resp(reader) != "OK":
            raise H.Failure("replica rejected READONLY")
        return read_resp(reader)


def assert_multi_status(environment, meta, nodes):
    status_text = command(
        environment,
        [CTL, "cluster-status", "--socket", meta.ctl_path, "--json"])
    status = json.loads(status_text)
    actual_groups = {item.get("group_id"): item
                     for item in status.get("groups", [])}
    actual_nodes = {item.get("node_id"): item
                    for item in status.get("data_nodes", [])}
    for group_id, (primary, replica, _, _) in GROUPS.items():
        group = actual_groups.get(group_id, {})
        if (group.get("term") != "1" or
                group.get("owner_node_id") != primary or
                group.get("config_epoch") != "1" or
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
                         restart_replica_during_create=False):
    layout = "automatic" if automatic else "explicit"
    mode = "interactive" if interactive else "yes"
    blocked = "-blocked" if block_replica_during_create else ""
    restarted = "-restart" if restart_replica_during_create else ""
    name = f"multi-{layout}-{mode}{blocked}{restarted}"
    scenario = os.path.join(workdir, name)
    os.makedirs(scenario, mode=0o700)
    meta_workdir = os.path.join(scenario, "meta")
    os.makedirs(meta_workdir, mode=0o700)
    meta = H.Node(META, meta_workdir, 1,
                  args=H.raft_args(snapshot_distance=100_000))
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
    environment = os.environ.copy()
    environment["PATH"] = (os.path.dirname(REDIS_CLI) + os.pathsep +
                           environment.get("PATH", ""))
    manifest = os.path.join(scenario, "cluster.toml")
    write_multi_manifest(manifest, nodes, automatic)
    try:
        meta.start(bootstrap=True)
        meta.wait_leader()
        started_nodes = nodes[:-1] if block_replica_during_create else nodes
        for node in started_nodes:
            variable = "KEYLANE_REPLICATION_PAUSE_FULLSYNC_BEFORE_CATALOG_ACK_MS"
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
        arguments = [
            CTL, "cluster-create", "--manifest", manifest,
            "--socket", meta.ctl_path, "--timeout-ms",
            "5000" if block_replica_during_create else "120000",
        ]
        input_text = "yes\n" if interactive else None
        if not interactive:
            arguments.append("--yes")
        if restart_replica_during_create:
            creator = subprocess.Popen(
                arguments, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, env=environment)
            try:
                if input_text is not None:
                    creator.stdin.write(input_text)
                    creator.stdin.flush()
                H.wait_until(
                    "primary pauses while replica full sync is in progress", 20,
                    lambda: "native full sync holds command gates before catalog "
                            "acknowledgement" in nodes[0].log_tail())
                operation_id = root_operation(meta)
                if operation_id is None:
                    raise H.Failure("rebuilding replica has no root operation")
                nodes[1].force_kill()
                nodes[1].start()
                stdout, stderr = creator.communicate(timeout=20)
                if (creator.returncode != 2 or "Cluster READY:" in stdout or
                        "target boot changed during replica initialization" not in stderr or
                        REPLICA_1 not in stderr):
                    raise H.Failure(
                        f"replica restart did not end creation: "
                        f"exit={creator.returncode} stdout={stdout!r} stderr={stderr!r}")
                if not meta.getop(operation_id).startswith("OK aborted "):
                    raise H.Failure("replica restart left the root active")
                groups = {group["group_id"]: group
                          for group in cluster_status(meta)["groups"]}
                if groups["group-1"]["serving_ready"]:
                    raise H.Failure("failed Group retained serving authority")
                H.log(f"{name}: full-sync target restart fenced its Group "
                      "and aborted creation")
            finally:
                if creator.poll() is None:
                    creator.kill()
                    creator.communicate(timeout=5)
            for node in nodes:
                node.terminate()
            meta.terminate()
            return
        if block_replica_during_create:
            result = subprocess.run(
                arguments, input=input_text, capture_output=True, text=True,
                timeout=15, env=environment)
            diagnostic = result.stdout + result.stderr
            expected = (
                "wait-data-projection",
                "last_blocker=node_runtime_not_ready",
                f"scope=node:{REPLICA_2}",
                "group=group-2",
            )
            if (result.returncode == 0 or "Cluster READY:" in diagnostic or
                    any(marker not in diagnostic for marker in expected)):
                raise H.Failure(
                    "stopped replica was not diagnosed during creation: "
                    f"returncode={result.returncode} output={diagnostic!r}")
            stopped_replica_blocker(environment, meta, nodes[-1])
            H.log(f"{name}: create stayed incomplete and named Group/Node")
            for node in started_nodes:
                node.terminate()
            meta.terminate()
            return
        created = command(
            environment, arguments, input_text=input_text, timeout=150)
        operations = dict(re.findall(
            r"group=(group-[12]) operation=([0-9a-f]{32})", created))
        if ("WARNING: existing data on all Data nodes will be erased" not in
                created or "Cluster READY:" not in created or
                set(operations) != set(GROUPS)):
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
        assert_multi_status(environment, meta, nodes)
        for group_id, operation_id in operations.items():
            if meta.getop(operation_id) != "OK completed cluster-created":
                raise H.Failure(f"{group_id} operation did not complete")
        assert_redis_topology_and_replication(nodes)
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
        for node in nodes:
            node.force_kill()
        meta.force_kill()


def run_group_id_probe_case(workdir):
    """A Group id must not terminate the CLI's Redis probe hash tag."""
    scenario = os.path.join(workdir, "group-id-probe")
    os.makedirs(scenario, mode=0o700)
    meta = H.Node(META, scenario, 1,
                  args=H.raft_args(snapshot_distance=100_000))
    nodes = [DataProcess(DATA, os.path.join(scenario, str(index)), node_id,
                         meta.data_control_endpoint)
             for index, node_id in enumerate((PRIMARY_1, PRIMARY_2))]
    manifest = os.path.join(scenario, "cluster.toml")
    # Without escaping, every {keylane-create-group-2}-N} hashes to 9188,
    # outside this Group's generated range 0..8191, regardless of N.
    lines = ['schema_version = 1', 'slot_strategy = "contiguous-even"',
             '[[meta_members]]', 'id = 1']
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
        if "Cluster READY:" not in result or "group=group-2}" not in result:
            raise H.Failure(f"escaped Group id did not finish verification: {result}")
        H.log("Group id containing '}' completes CLI routing verification")
        for node in nodes:
            node.terminate()
        meta.terminate()
    finally:
        for node in nodes:
            node.force_kill()
        meta.force_kill()


class DirectiveBarrier(H.Proxy):
    """Hold one complete control frame without changing its bytes/identity."""

    def __init__(self, target_port, result=False):
        super().__init__("result" if result else "directive", target_port)
        self.result = result
        self.blocked = threading.Event()
        self.release = threading.Event()

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
                if magic != 0x4b4c4350 or version != 1 or size > (1 << 20):
                    raise H.Failure("unexpected control frame")
                payload = exact(size)
                if kind == (14 if self.result else 12):
                    self.blocked.set()
                    if not self.release.wait(20):
                        raise OSError("test barrier timed out")
                    dst.sendall(header + payload)
                    return super()._pump(src, dst, pair)
                dst.sendall(header + payload)
        except OSError:
            self._cut_pair(pair)

    def close(self):
        self.release.set()
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
    data = DataProcess(DATA, os.path.join(scenario, "data"), DATA_NODE,
                       proxy.endpoint if proxy else meta.data_control_endpoint)
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(5)
    online = wire is not None or phase in (
        "result-committed", "directive-removed", "child-completed")
    sentinel = False
    try:
        variable = "KEYLANE_TEST_PAUSE_CLUSTER_CREATE_PHASE"
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
        # The maximum accepted wait must not become the process stop budget.
        connection.sendall((create_request(
            DATA_NODE, data.advertised_endpoint, "group-1",
            timeout_ms=3600000) + "\n").encode())
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
        operation_id = root_operation(meta)
        if operation_id is None:
            raise H.Failure(f"{name}: no durable operation identity")
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
        if select.select([connection], [], [], 0)[0]:
            raise H.Failure(f"{name}: Admin completed before the paused cut")
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
        H.wait_until(f"{name}: recovered cluster READY", 20,
                     lambda: cluster_status(meta)["result"] == "ready")
        if sentinel:
            value = command(os.environ.copy(), [
                REDIS_CLI, "--raw", "-p", str(data.redis_port),
                "GET", "recovery-sentinel"])
            if value.strip() != "keep":
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
        run_multi_group_case(workdir, automatic=True, interactive=True)
        run_multi_group_case(workdir, automatic=False, interactive=False)
        run_multi_group_case(workdir, automatic=False, interactive=False,
                             block_replica_during_create=True)
        run_group_id_probe_case(workdir)
        if has_fault(DATA, b"KEYLANE_REPLICATION_PAUSE_FULLSYNC_BEFORE_CATALOG_ACK_MS"):
            run_multi_group_case(workdir, automatic=True, interactive=False,
                                 restart_replica_during_create=True)
        else:
            H.log("SKIP replica restart cut: Data binary has no full-sync pause hook")
        run_case(workdir, interactive=False)
        if has_fault(META, b"KEYLANE_TEST_PAUSE_CLUSTER_CREATE_PHASE"):
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
