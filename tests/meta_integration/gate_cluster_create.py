#!/usr/bin/env python3
"""Real-process single-Meta/single-Data cluster-create gate.

Usage: gate_cluster_create.py META DATA CTL REDIS_CLI [workdir]
"""

import json
import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402
from gate_data_control import DataProcess  # noqa: E402


DATA_NODE = "0123456789abcdef0123456789abcdef01234567"


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


def command(environment, arguments, input_text=None, timeout=90):
    result = subprocess.run(
        arguments, input=input_text, capture_output=True, text=True,
        timeout=timeout, env=environment)
    if result.returncode != 0:
        raise H.Failure(
            f"command failed ({result.returncode}): {' '.join(arguments)} "
            f"stdout={result.stdout!r} stderr={result.stderr!r}")
    return result.stdout


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
        for malformed in ("clustercreate", "clustercreate 2 00"):
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
        if ("WARNING: existing data on the Data node will be erased" not in
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


def main():
    work_argv = [sys.argv[0], META] + sys.argv[5:]
    workdir, keep = H.make_workdir(work_argv, "cluster_create_")
    try:
        run_case(workdir, interactive=True)
        run_case(workdir, interactive=False)
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
