#!/usr/bin/env python3
"""Real-process controlled-failover acceptance gate.

Usage: gate_controlled_failover.py META DATA CTL REDIS_CLI [workdir]
"""

import json
import os
import re
import socket
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402
from gate_data_control import DataProcess  # noqa: E402


FORMER_PRIMARY = "1111111111111111111111111111111111111111"
CANDIDATE = "2222222222222222222222222222222222222222"
GROUP = "group-1"
KEY = "{controlled-failover}sentinel"
ATTEMPT_TIMEOUT_MS = 180000


def command(environment, arguments, timeout, expected=0):
    result = subprocess.run(
        arguments, capture_output=True, text=True, timeout=timeout,
        env=environment)
    if result.returncode != expected:
        raise H.Failure(
            f"command failed ({result.returncode}, want {expected}): "
            f"{' '.join(arguments)} stdout={result.stdout!r} "
            f"stderr={result.stderr!r}")
    return result.stdout.strip()


def write_manifest(path, former_primary, candidate):
    with open(path, "w", encoding="utf-8") as output:
        output.write(
            "schema_version = 1\n\n"
            "[[meta_members]]\n"
            "id = 1\n\n"
            "[[data_nodes]]\n"
            f'id = "{FORMER_PRIMARY}"\n'
            f'client_endpoint = "{former_primary.advertised_endpoint}"\n\n'
            "[[data_nodes]]\n"
            f'id = "{CANDIDATE}"\n'
            f'client_endpoint = "{candidate.advertised_endpoint}"\n\n'
            "[[groups]]\n"
            f'id = "{GROUP}"\n'
            f'primary = "{FORMER_PRIMARY}"\n'
            f'replicas = ["{CANDIDATE}"]\n\n'
            "[[slot_ranges]]\n"
            "first = 0\n"
            "last = 16383\n"
            f'group = "{GROUP}"\n')


def endpoint_tuple(data):
    endpoint = data.advertised_endpoint.removeprefix("tcp://")
    host, port = endpoint.rsplit(":", 1)
    return host, int(port)


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
        with sock.makefile("rb") as reader:
            return read_resp(reader)


def redis_error(data, arguments):
    with socket.create_connection(endpoint_tuple(data), timeout=3.0) as sock:
        sock.settimeout(3.0)
        sock.sendall(encode_resp(arguments))
        with sock.makefile("rb") as reader:
            line = reader.readline()
    if not line.startswith(b"-") or not line.endswith(b"\r\n"):
        raise H.Failure(
            f"expected Redis error for {arguments}, received {line!r}")
    return line[1:-2].decode(errors="replace")


def readonly_get(data, key):
    with socket.create_connection(endpoint_tuple(data), timeout=3.0) as sock:
        sock.settimeout(3.0)
        sock.sendall(encode_resp(["READONLY"]) + encode_resp(["GET", key]))
        with sock.makefile("rb") as reader:
            if read_resp(reader) != "OK":
                raise H.Failure("candidate rejected READONLY")
            return read_resp(reader)


def cluster_status(environment, meta):
    result = subprocess.run(
        [CTL, "--socket", meta.ctl_path, "--timeout-ms", "10000",
         "cluster-status", "--json"],
        capture_output=True, text=True, timeout=15, env=environment)
    if result.returncode not in (0, 2):
        raise H.Failure(
            f"cluster-status failed ({result.returncode}): "
            f"stdout={result.stdout!r} stderr={result.stderr!r}")
    return json.loads(result.stdout)


def final_status_matches(environment, meta):
    status = cluster_status(environment, meta)
    groups = status.get("groups", [])
    group = groups[0] if len(groups) == 1 else {}
    nodes = {node.get("node_id"): node
             for node in status.get("data_nodes", [])}
    promoted = nodes.get(CANDIDATE, {})
    return (
        status.get("result") == "ready" and
        status.get("topology_converged") is True and
        status.get("serving_ready") is True and
        group.get("group_id") == GROUP and
        group.get("term") == "2" and
        group.get("owner_node_id") == CANDIDATE and
        group.get("config_epoch") == "2" and
        group.get("serving_ready") is True and
        group.get("topology_converged") is True and
        promoted.get("current_session") is True and
        promoted.get("projection_current") is True and
        promoted.get("population_current") is True and
        promoted.get("lease") == "recently_granted" and
        status.get("slot_ranges") == [
            {"first": "0", "last": "16383", "group_id": GROUP}])


def run_case(workdir):
    scenario = os.path.join(workdir, "serving")
    os.makedirs(scenario, mode=0o700)
    meta_workdir = os.path.join(scenario, "meta")
    os.makedirs(meta_workdir, mode=0o700)
    meta = H.Node(META, meta_workdir, 1,
                  args=H.raft_args(snapshot_distance=100_000))
    former_primary = DataProcess(
        DATA, os.path.join(scenario, "former-primary"), FORMER_PRIMARY,
        meta.data_control_endpoint)
    candidate = DataProcess(
        DATA, os.path.join(scenario, "candidate"), CANDIDATE,
        meta.data_control_endpoint)
    nodes = [former_primary, candidate]
    environment = os.environ.copy()
    environment["PATH"] = (os.path.dirname(REDIS_CLI) + os.pathsep +
                           environment.get("PATH", ""))
    manifest = os.path.join(scenario, "cluster.toml")
    write_manifest(manifest, former_primary, candidate)

    try:
        meta.start(bootstrap=True)
        meta.wait_leader()
        for node in nodes:
            node.start()

        created = command(
            environment,
            [CTL, "cluster-create", "--manifest", manifest,
             "--socket", meta.ctl_path, "--timeout-ms", "90000", "--yes"],
            timeout=100)
        if ("Cluster READY:" not in created or
                f"group={GROUP} operation=" not in created):
            raise H.Failure(
                f"cluster-create did not establish the test topology: "
                f"{created!r}")

        before = "written-before-failover"
        if (redis_call(former_primary, ["SET", KEY, before]) != "OK" or
                redis_call(former_primary, ["GET", KEY]) != before):
            raise H.Failure("former primary failed the pre-failover write")
        H.wait_until(
            "candidate replicates the pre-failover write", 30,
            lambda: readonly_get(candidate, KEY) == before)
        slot = redis_call(former_primary, ["CLUSTER", "KEYSLOT", KEY])
        if not isinstance(slot, int) or not 0 <= slot <= 16383:
            raise H.Failure(f"CLUSTER KEYSLOT returned {slot!r}")

        failover = command(
            environment,
            [CTL, "--socket", meta.ctl_path, "--timeout-ms", "100000",
             "failover", "1", GROUP, "90000", str(ATTEMPT_TIMEOUT_MS)],
            timeout=110)
        success = re.fullmatch(
            rf"OK failover 1 ([1-9][0-9]*) "
            rf"operation=([0-9a-f]{{32}}) candidate={CANDIDATE} "
            r"phase=serving loss=exact recovery_required=0 "
            r"frontier=([0-9]+(?:,[0-9]+)*) "
            r"reason=candidate served the exact frozen source frontier",
            failover)
        if success is None:
            raise H.Failure(
                f"controlled failover did not report exact serving: "
                f"{failover!r}")
        operation_id = success.group(2)
        frontier = success.group(3)

        durable = command(
            environment,
            [CTL, "--socket", meta.ctl_path, "--timeout-ms", "10000",
             "getop", operation_id], timeout=15)
        expected_durable = (
            f"OK completed kind=failover group={GROUP} "
            f"candidate={CANDIDATE} "
            f"attempt_timeout_ms={ATTEMPT_TIMEOUT_MS} phase=serving "
            f"loss=exact recovery_required=0 frontier={frontier} "
            "reason=candidate served the exact frozen source frontier")
        if durable != expected_durable:
            raise H.Failure(
                f"getop did not preserve the exact terminal outcome: "
                f"{durable!r}")

        H.wait_until(
            "cluster status exposes term-2 candidate authority", 30,
            lambda: final_status_matches(environment, meta))
        status = cluster_status(environment, meta)
        group = status["groups"][0]
        if (group["term"], group["owner_node_id"], group["config_epoch"]) != (
                "2", CANDIDATE, "2"):
            raise H.Failure(f"status authority anchors changed: {status}")

        expected_slots = [[
            0, 16383,
            ["127.0.0.1", candidate.redis_port, CANDIDATE],
            ["127.0.0.1", former_primary.redis_port, FORMER_PRIMARY],
        ]]
        candidate_slots = redis_call(candidate, ["CLUSTER", "SLOTS"])
        former_slots = redis_call(former_primary, ["CLUSTER", "SLOTS"])
        if candidate_slots != expected_slots or former_slots != expected_slots:
            raise H.Failure(
                "CLUSTER SLOTS did not publish the promoted owner: "
                f"candidate={candidate_slots!r} former={former_slots!r}")

        if redis_call(candidate, ["GET", KEY]) != before:
            raise H.Failure("promoted owner lost the replicated prewrite")
        after = "written-after-failover"
        if (redis_call(candidate, ["SET", KEY, after]) != "OK" or
                redis_call(candidate, ["GET", KEY]) != after):
            raise H.Failure("promoted owner failed SET/GET")

        expected_moved = (
            f"MOVED {slot} 127.0.0.1:{candidate.redis_port}")
        for ordinal in range(5):
            observed = redis_error(
                former_primary, ["SET", KEY, f"stale-writer-{ordinal}"])
            if observed != expected_moved:
                raise H.Failure(
                    f"former primary write #{ordinal + 1} returned "
                    f"{observed!r}, want {expected_moved!r}")
        if redis_call(candidate, ["GET", KEY]) != after:
            raise H.Failure("a former-primary write changed promoted data")

        H.log(
            "controlled failover served the exact frontier, retained its "
            "prewrite, and redirected five former-primary writes")
        for node in nodes:
            node.terminate()
        meta.terminate()
    except Exception:
        H.dump_node_logs([meta], lines=250)
        for node in nodes:
            print(f"--- Data log tail ({node.log_path}) ---", file=sys.stderr)
            print(node.log_tail(lines=250), file=sys.stderr)
        raise
    finally:
        for node in nodes:
            node.force_kill()
        meta.force_kill()


def main():
    work_argv = [sys.argv[0], META] + sys.argv[5:]
    workdir, keep = H.make_workdir(work_argv, "controlled_failover_")
    try:
        run_case(workdir)
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
H.set_tag("controlled-failover")
sys.exit(main())
