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

"""Fixed-version standard Cluster client compatibility matrix.

Drives one Meta-managed two-Group cluster (dual plaintext+TLS endpoints,
requirepass on every Data node) through the acceptance matrix with the pinned
standard clients and their real connection pools:

- redis-py 8.1.0 ``RedisCluster`` (from the directory named by
  LAVIK_CLUSTER_CLIENT_REDIS_PY, installed by scripts/install_test_redis_py.sh;
  the interpreter asserts the exact version at startup), and
- go-redis 9.22.0 ``ClusterClient`` via the cluster_client_go driver process
  (its ``version`` op reports the compiled module version and is asserted),
- both at RESP2 and RESP3, plaintext and TLS client ports.

Scenarios: SLOTS-based slot discovery from a single seed, CLUSTER SHARDS
rejection (a documented v1 boundary), MOVED following and cross-Group
isolation, server-side CROSSSLOT rejection through each library (redis-py
rejects cross-slot mget client-side and via execute_command(target_nodes=...);
go-redis routes typed MGet by the first key and surfaces the server's
CROSSSLOT), authentication negatives, controlled failover and primary
kill -9 with the same pooled clients recovering reads and writes on the new
owner.

Usage: gate_cluster_client.py META DATA CTL GO_DRIVER [workdir]
"""

import json
import os
import re
import socket
import ssl
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402
from gate_data_control import DataProcess, make_ca, make_leaf  # noqa: E402
from gate_cluster_create import (  # noqa: E402
    encode_resp,
    key_in_range,
    read_resp,
    redis_slot,
)

REDIS_PY_DIR = os.environ.get("LAVIK_CLUSTER_CLIENT_REDIS_PY")
if REDIS_PY_DIR is None:
    print(
        "LAVIK_CLUSTER_CLIENT_REDIS_PY must name the pinned redis-py "
        "install directory (see scripts/install_test_redis_py.sh)",
        file=sys.stderr,
    )
    sys.exit(2)
sys.path.insert(0, REDIS_PY_DIR)
import redis  # noqa: E402
import redis.cluster  # noqa: E402

if redis.__version__ != "8.1.0":
    print(
        f"the pinned Cluster client is redis-py 8.1.0, found {redis.__version__}",
        file=sys.stderr,
    )
    sys.exit(2)

PRIMARY_1 = "1111111111111111111111111111111111111111"
REPLICA_1 = "2222222222222222222222222222222222222222"
PRIMARY_2 = "3333333333333333333333333333333333333333"
REPLICA_2 = "4444444444444444444444444444444444444444"
# group id -> (primary, replica, first slot, last slot)
GROUPS = {
    "group-1": (PRIMARY_1, REPLICA_1, 0, 8191),
    "group-2": (PRIMARY_2, REPLICA_2, 8192, 16383),
}
PASSWORD = "cluster-client-matrix-secret"
GO_REDIS_VERSION = "v9.22.0"

# Transient rejections a standard client may legitimately see while Meta moves
# ownership, plus the raw connection failures of talking to a killed node.
# Anything else from a client is a compatibility failure.
TRANSIENT_MARKERS = (
    "MOVED",
    "TRYAGAIN",
    "CLUSTERDOWN",
    "LOADING",
    "READONLY",
    "MASTERDOWN",
    "TTL exhausted",
    "maximum number of retries",
    "not covered",
    "Connection refused",
    "connection refused",
    "Connection reset",
    "connection reset",
    "EOF",
    "closed",
    "timed out",
    "Timeout",
    "connect",
)

# redis-py maps server error codes to typed exceptions whose messages drop the
# code prefix (e.g. BusyLoadingError for LOADING), so the Python side
# classifies by type; the go driver's text buckets match on the server text.
PY_TRANSIENT_EXC = (
    redis.exceptions.BusyLoadingError,
    redis.exceptions.ClusterDownError,
    redis.exceptions.TryAgainError,
    redis.exceptions.MovedError,
    redis.exceptions.AskError,
    redis.exceptions.MasterDownError,
    redis.exceptions.ReadOnlyError,
    redis.exceptions.SlotNotCoveredError,
    redis.exceptions.ConnectionError,
    redis.exceptions.TimeoutError,
)


def transient_bucket(text):
    for marker in TRANSIENT_MARKERS:
        if marker in text:
            return marker
    return None


def py_transient_bucket(error):
    if isinstance(error, PY_TRANSIENT_EXC):
        return type(error).__name__
    return transient_bucket(str(error))


def group_key(group_id, tag):
    """A deterministic hashtag key whose slot belongs to the group."""
    first, last = GROUPS[group_id][2], GROUPS[group_id][3]
    return key_in_range(f"cc-{group_id}-{tag}", first, last)


def group_hashtag(group_id, tag):
    key = group_key(group_id, tag)
    return key[key.index("{") + 1 : key.index("}")]


# ---------------------------------------------------------------------------
# Cluster lifecycle
# ---------------------------------------------------------------------------


def write_manifest(path, meta, nodes):
    lines = ["schema_version = 1", 'client_mode = "cluster"', ""]
    lines.extend(
        [
            "[[meta_members]]",
            f"id = {meta.id}",
            f'raft_endpoint = "tcp://{meta.endpoint}"',
            f'data_control_endpoint = "tcp://{meta.data_control_endpoint}"',
            f'ctl_endpoint = "tcp://{meta.ctl_endpoint}"',
            "",
        ]
    )
    for node in nodes:
        lines.extend(
            [
                "[[data_nodes]]",
                f'id = "{node.node_id}"',
                f'client_endpoint = "{node.advertised_endpoint}"',
                f'tls_endpoint = "tls://127.0.0.1:{node.tls_port}"',
                "",
            ]
        )
    for group_id in sorted(GROUPS):
        primary, replica, _, _ = GROUPS[group_id]
        lines.extend(
            [
                "[[groups]]",
                f'id = "{group_id}"',
                f'primary = "{primary}"',
                f'replicas = ["{replica}"]',
                "",
            ]
        )
    for group_id in sorted(GROUPS):
        _, _, first, last = GROUPS[group_id]
        lines.extend(
            [
                "[[slot_ranges]]",
                f"first = {first}",
                f"last = {last}",
                f'group = "{group_id}"',
                "",
            ]
        )
    with open(path, "w", encoding="utf-8") as output:
        output.write("\n".join(lines))


def cluster_status(meta):
    result = subprocess.run(
        [
            CTL,
            "cluster-status",
            "--json",
            "--socket",
            meta.ctl_path,
            "--allow-plaintext-admin",
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode not in (0, 2):
        raise H.Failure(f"cluster-status failed: {result}")
    return json.loads(result.stdout)


def wait_cluster_ready(meta, description, timeout=90):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = cluster_status(meta)
        if last.get("result") == "ready" and last.get("cluster_state") == "created":
            return last
        if last.get("cluster_state") == "provisioning-failed":
            raise H.Failure(
                f"{description}: provisioning-failed: "
                f"{last.get('provisioning_failure_summary')}"
            )
        time.sleep(0.25)
    raise H.Failure(f"timeout ({timeout}s) waiting for: {description}; status={last}")


def wait_group_owner(
    meta, group_id, owner_id, term, description, timeout=90, require_ready=True
):
    def converged():
        status = cluster_status(meta)
        if require_ready and status.get("result") != "ready":
            return False
        for group in status.get("groups", []):
            if group.get("group_id") == group_id:
                return (
                    group.get("owner_node_id") == owner_id
                    and str(group.get("term")) == str(term)
                    and group.get("serving_ready")
                )
        return False

    H.wait_until(
        f"{description}: {group_id} owner {owner_id[:8]} term {term}",
        timeout,
        converged,
    )


def configure_fast_policies(meta):
    for policy_id, content in (
        (
            H.AUTOMATIC_UNCONTROLLED_FAILOVER_POLICY_ID,
            H.automatic_uncontrolled_failover_policy(suspect_after_ms=1000),
        ),
        (H.AUTHORITY_LEASE_POLICY_ID, H.authority_lease_policy(duration_ms=1000)),
    ):
        current = meta.getpolicy(policy_id)
        match = re.match(r"OK version=(\d+)", current)
        if match is None:
            raise H.Failure(f"getpolicy {policy_id}: {current}")
        reply = meta.putpolicy(policy_id, int(match.group(1)) + 1, content)
        if not reply.startswith("OK "):
            raise H.Failure(f"putpolicy {policy_id}: {reply}")

    def active():
        status = cluster_status(meta)
        groups = status.get("groups", [])
        return len(groups) == 2 and all(
            str(group.get("effective_threshold_ms")) == "1000" for group in groups
        )

    H.wait_until("fast failover policies projected", 30, active)


# ---------------------------------------------------------------------------
# Raw auxiliary probes (AUTH first). These never replace a client assertion;
# they only inspect server-side state directly.
# ---------------------------------------------------------------------------


def raw_open(node, use_tls=False):
    if use_tls:
        ca, cert, key = node.tls
        context = ssl.create_default_context(cafile=ca)
        context.load_cert_chain(cert, key)
        sock = socket.create_connection(("127.0.0.1", node.tls_port), timeout=3.0)
        try:
            return context.wrap_socket(sock, server_hostname="127.0.0.1")
        except Exception:
            sock.close()
            raise
    return socket.create_connection(("127.0.0.1", node.redis_port), timeout=3.0)


def raw_session(node, commands, use_tls=False, auth=True):
    """Run commands on one connection; error replies raise Failure."""
    with raw_open(node, use_tls) as sock:
        sock.settimeout(3.0)
        reader = sock.makefile("rb")
        if auth:
            sock.sendall(encode_resp(["AUTH", PASSWORD]))
            if read_resp(reader) != "OK":
                raise H.Failure(f"raw AUTH failed on {node.node_id[:8]}")
        replies = []
        for args in commands:
            sock.sendall(encode_resp(args))
            replies.append(read_resp(reader))
        return replies


def raw_error(node, commands, use_tls=False, auth=True):
    """Return the first error reply text; fail if every command succeeds."""
    with raw_open(node, use_tls) as sock:
        sock.settimeout(3.0)
        reader = sock.makefile("rb")
        if auth:
            sock.sendall(encode_resp(["AUTH", PASSWORD]))
            read_resp(reader)
        for args in commands:
            sock.sendall(encode_resp(args))
            try:
                reply = read_resp(reader)
            except H.Failure as error:
                return str(error)
    raise H.Failure(f"expected an error for {commands}, got {reply!r}")


def raw_scan_keys(node):
    keys = []
    cursor = 0
    while True:
        reply = raw_session(node, [["SCAN", str(cursor)]])[0]
        cursor = int(reply[0])
        keys.extend(reply[1])
        if cursor == 0:
            return keys


# ---------------------------------------------------------------------------
# Client cells
# ---------------------------------------------------------------------------


class PyCell:
    def __init__(self, name, port, protocol, tls_ca=None):
        self.name = name
        kwargs = dict(
            startup_nodes=[redis.cluster.ClusterNode("127.0.0.1", port)],
            password=PASSWORD,
            protocol=protocol,
            socket_timeout=5,
            socket_connect_timeout=5,
        )
        if tls_ca is not None:
            kwargs.update(
                ssl=True,
                ssl_ca_certs=tls_ca,
                ssl_cert_reqs="required",
                ssl_check_hostname=True,
            )
        self.client = redis.cluster.RedisCluster(**kwargs)

    def set(self, key, value):
        if self.client.set(key, value) is not True:
            raise H.Failure(f"{self.name}: SET {key} was not acknowledged")
        return value

    def get(self, key):
        value = self.client.get(key)
        return None if value is None else value.decode()

    def slots(self):
        out = []
        for (first, last), info in self.client.cluster_slots().items():
            out.append(
                {
                    "first": first,
                    "last": last,
                    "primary": {"host": info["primary"][0], "port": info["primary"][1]},
                    "replicas": [
                        {"host": host, "port": port} for host, port in info["replicas"]
                    ],
                }
            )
        return out

    def close(self):
        self.client.close()


class GoCell:
    """One driver process per cell: the pool lives and dies with it."""

    def __init__(self, name, port, protocol, tls_ca=None, log=None, password=PASSWORD):
        self.name = name
        args = [
            GO_DRIVER,
            "--addrs",
            f"127.0.0.1:{port}",
            "--protocol",
            str(protocol),
            "--password",
            password,
        ]
        if tls_ca is not None:
            args.extend(["--tls-ca", tls_ca])
        self.log = log or subprocess.DEVNULL
        self.proc = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.log,
            text=True,
            bufsize=1,
        )

    def call(self, **request):
        self.proc.stdin.write(json.dumps(request) + "\n")
        self.proc.stdin.flush()
        line = self.proc.stdout.readline()
        if not line:
            raise H.Failure(f"go cell {self.name} exited (code {self.proc.poll()})")
        return json.loads(line)

    def value(self, **request):
        reply = self.call(**request)
        if not reply.get("ok"):
            raise H.Failure(f"{self.name} {request['op']}: {reply.get('error')}")
        return reply.get("value")

    def set(self, key, value):
        self.value(op="set", key=key, value=value)
        return value

    def get(self, key):
        return self.value(op="get", key=key)

    def slots(self):
        return self.value(op="cluster_slots")

    def close(self):
        if self.proc.poll() is None:
            try:
                self.call(op="close")
                self.proc.wait(timeout=10)
            except (OSError, H.Failure, subprocess.TimeoutExpired):
                self.proc.kill()
                self.proc.wait(timeout=10)


def py_tolerated(cell, operation, description, timeout=30):
    """Retry a redis-py operation through the transient failover window."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            return operation()
        except Exception as error:  # noqa: BLE001 - classified by type/text
            if py_transient_bucket(error) is None:
                raise
            if time.monotonic() >= deadline:
                raise H.Failure(
                    f"{description}: {cell.name} still failing after "
                    f"{timeout}s: {error}"
                )
            time.sleep(0.2)


def go_tolerated(cell, description, timeout=30, **request):
    deadline = time.monotonic() + timeout
    while True:
        reply = cell.call(**request)
        if reply.get("ok"):
            return reply.get("value")
        error = reply.get("error", "")
        if transient_bucket(error) is None:
            raise H.Failure(f"{description}: {cell.name} fatal: {error}")
        if time.monotonic() >= deadline:
            raise H.Failure(
                f"{description}: {cell.name} still failing after {timeout}s: {error}"
            )
        time.sleep(0.2)


def cell_set(cell, key, value, description, timeout=30):
    if isinstance(cell, PyCell):
        return py_tolerated(cell, lambda: cell.set(key, value), description, timeout)
    return go_tolerated(cell, description, timeout, op="set", key=key, value=value)


def cell_get(cell, key, description, timeout=30):
    if isinstance(cell, PyCell):
        return py_tolerated(cell, lambda: cell.get(key), description, timeout)
    return go_tolerated(cell, description, timeout, op="get", key=key)


class PyLoad:
    """Continuous SET/GET against one slot per Group via a redis-py cell."""

    def __init__(self, cell, hashtags):
        self.cell = cell
        self.hashtags = hashtags
        self.set_ok = 0
        self.get_ok = 0
        self.errors = {}
        self.fatal = None
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"load-{cell.name}", daemon=True
        )

    def start(self):
        self._thread.start()

    def _run(self):
        index = 0
        while not self._stop.is_set():
            for hashtag in self.hashtags:
                key = f"{{{hashtag}}}-{index % 64}"
                try:
                    self.cell.client.set(key, f"v{index}")
                    self.set_ok += 1
                    self.cell.client.get(key)
                    self.get_ok += 1
                except Exception as error:  # noqa: BLE001 - classified by type
                    bucket = py_transient_bucket(error)
                    if bucket is None:
                        self.fatal = f"{type(error).__name__}: {error}"
                        return
                    self.errors[bucket] = self.errors.get(bucket, 0) + 1
            index += 1

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=15)

    def report(self):
        errs = ", ".join(f"{k}={v}" for k, v in sorted(self.errors.items())) or "none"
        return (
            f"{self.cell.name}: set_ok={self.set_ok} "
            f"get_ok={self.get_ok} transient: {errs} "
            f"fatal: {self.fatal}"
        )


def cell_idle_conns(cell):
    """Idle pooled connections currently held by the cell."""
    if isinstance(cell, PyCell):
        # redis-py has no public idle-conn gauge; the pinned 8.1.0 pool layout
        # (get_nodes -> redis_connection -> connection_pool) is stable, and a
        # pin bump must revisit this introspection.
        return sum(
            len(node.redis_connection.connection_pool._available_connections)
            for node in cell.client.get_nodes()
        )
    return cell.value(op="pool_stats")["idle_conns"]


def assert_pools_idle(cells, minimum, description):
    for cell in cells:
        idle = cell_idle_conns(cell)
        if idle < minimum:
            raise H.Failure(
                f"{description}: {cell.name} holds {idle} idle "
                f"pooled connections, want >= {minimum}"
            )


def start_loads(cells, hashtags):
    """One active load per cell, alternating between both Groups."""
    loads = []
    for cell in cells:
        if isinstance(cell, PyCell):
            load = PyLoad(cell, hashtags)
            load.start()
            loads.append(("py", load))
        else:
            cell.value(op="load_start", prefixes=list(hashtags))
            loads.append(("go", cell))
    return loads


def stop_loads(loads, description):
    reports = []
    for kind, load in loads:
        if kind == "py":
            load.stop()
            if load.fatal is not None:
                raise H.Failure(f"{description}: {load.report()}")
            if load.set_ok + load.get_ok == 0:
                raise H.Failure(f"{description}: {load.report()} made no progress")
            reports.append(load.report())
        else:
            stats = load.value(op="load_stop")
            if stats["fatal"]:
                raise H.Failure(f"{description}: {load.name} fatal: {stats['fatal']}")
            if stats["set_ok"] + stats["get_ok"] == 0:
                raise H.Failure(f"{description}: {load.name} made no progress")
            reports.append(f"{load.name}: {json.dumps(stats, sort_keys=True)}")
    for report in reports:
        H.log(f"  load {report}")


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def assert_slots_shape(cell, nodes, port_attr, description):
    """The client-discovered map must be exactly the committed topology."""
    actual = cell.slots()
    expected = []
    for group_id in sorted(GROUPS):
        primary_id, replica_id, first, last = GROUPS[group_id]
        expected.append(
            {
                "first": first,
                "last": last,
                "primary_port": getattr(nodes[primary_id], port_attr),
                "replica_port": getattr(nodes[replica_id], port_attr),
            }
        )
    normalized = []
    for entry in actual:
        if entry["primary"] is None or entry["primary"]["host"] != "127.0.0.1":
            raise H.Failure(f"{description}: {cell.name} bad primary: {entry}")
        normalized.append(
            {
                "first": entry["first"],
                "last": entry["last"],
                "primary_port": entry["primary"]["port"],
                "replica_port": (
                    entry["replicas"][0]["port"] if entry["replicas"] else None
                ),
            }
        )
    if sorted(json.dumps(item, sort_keys=True) for item in normalized) != sorted(
        json.dumps(item, sort_keys=True) for item in expected
    ):
        raise H.Failure(f"{description}: {cell.name} slots {normalized} != {expected}")


def scenario_discovery(cells, tls_cells, nodes, written):
    """Single-seed discovery: clients are only told group-1's primary."""
    for cell in cells + tls_cells:
        for group_id in sorted(GROUPS):
            values = {}
            for ordinal in range(3):
                key = group_key(group_id, f"{cell.name}-{ordinal}")
                values[key] = f"value-{cell.name}-{ordinal}"
            for key, value in values.items():
                cell_set(cell, key, value, "initial discovery write", 10)
            for key, value in values.items():
                if cell_get(cell, key, "initial discovery read") != value:
                    raise H.Failure(f"{cell.name}: read-back mismatch {key}")
            written[group_id].update(values)
        assert_slots_shape(
            cell,
            nodes,
            "tls_port" if cell in tls_cells else "redis_port",
            "slot discovery",
        )
        H.log(
            f"  discovery: {cell.name} built the full slot map from one "
            "seed and read/wrote both Groups"
        )


def scenario_shards_boundary(cells):
    for cell in cells:
        if isinstance(cell, PyCell):
            try:
                cell.client.cluster_shards()
                raise H.Failure(f"{cell.name}: CLUSTER SHARDS unexpectedly succeeded")
            except redis.exceptions.ResponseError as error:
                if "Unknown CLUSTER subcommand" not in str(error):
                    raise
        else:
            reply = cell.call(op="cluster_shards")
            if reply.get("ok") or "Unknown CLUSTER subcommand" not in reply.get(
                "error", ""
            ):
                raise H.Failure(f"{cell.name}: CLUSTER SHARDS: {reply}")
        H.log(
            f"  boundary: {cell.name} surfaced the documented CLUSTER SHARDS rejection"
        )


def scenario_isolation(cells, nodes, written):
    for group_id, (primary_id, _, first, last) in sorted(GROUPS.items()):
        primary = nodes[primary_id]
        other = "group-2" if group_id == "group-1" else "group-1"
        foreign_key = next(iter(written[other]))
        own_key = next(iter(written[group_id]))
        # Client-carried isolation. Reading the foreign key through the
        # Cluster client must resolve to the owning Group's data (the client
        # transparently follows the redirect); aimed at one member through
        # the same pinned library without redirect-following, the wrong node
        # must answer MOVED instead of leaking data.
        for cell in cells:
            if (
                cell_get(cell, foreign_key, "isolation read")
                != written[other][foreign_key]
            ):
                raise H.Failure(
                    f"{cell.name}: foreign key did not resolve to the "
                    "owning Group's value"
                )
            if isinstance(cell, PyCell):
                probe = redis.Redis(
                    host="127.0.0.1",
                    port=primary.redis_port,
                    password=PASSWORD,
                    socket_timeout=3,
                )
                try:
                    try:
                        probe.get(foreign_key)
                        raise H.Failure(
                            f"{cell.name}: foreign key served without MOVED"
                        )
                    except redis.exceptions.ResponseError as error:
                        # MovedError's message carries only "slot host:port";
                        # the type carries the MOVED semantics.
                        if not isinstance(
                            error, redis.exceptions.MovedError
                        ) and "MOVED" not in str(error):
                            raise
                    if probe.get(own_key) != written[group_id][own_key].encode():
                        raise H.Failure(f"{cell.name}: own-key probe mismatch")
                finally:
                    probe.close()
            else:
                reply = cell.call(
                    op="probe_at",
                    addr=f"127.0.0.1:{primary.redis_port}",
                    args=["GET", foreign_key],
                )
                if reply.get("ok") or "MOVED" not in reply.get("error", ""):
                    raise H.Failure(f"{cell.name}: foreign key probe: {reply}")
                if (
                    cell.call(
                        op="probe_at",
                        addr=f"127.0.0.1:{primary.redis_port}",
                        args=["GET", own_key],
                    ).get("value")
                    != written[group_id][own_key]
                ):
                    raise H.Failure(f"{cell.name}: own-key probe mismatch")
        # Server-side auxiliary evidence: the raw probes inspect state
        # directly; they never stand in for the client assertions above.
        moved = raw_error(primary, [["GET", foreign_key]])
        if not moved.startswith("MOVED "):
            raise H.Failure(f"{group_id} primary leaked the other Group's key: {moved}")
        dbsize = raw_session(primary, [["DBSIZE"]])[0]
        if dbsize != len(written[group_id]):
            raise H.Failure(
                f"{group_id} primary DBSIZE {dbsize} != "
                f"{len(written[group_id])} written keys"
            )
        leaked = [
            key
            for key in raw_scan_keys(primary)
            if not first <= redis_slot(key) <= last
        ]
        if leaked:
            raise H.Failure(f"{group_id} primary holds foreign keys: {leaked}")
        H.log(
            f"  isolation: {group_id} primary MOVEDs foreign keys for every "
            f"client and holds exactly its own {dbsize} keys"
        )


def scenario_crossslot(cells, nodes, written):
    """Server-side CROSSSLOT must surface through each standard client.

    The pinned clients disagree on typed-API behavior, and the matrix records
    both: redis-py raises client-side for cross-slot mget (its fan-out lives
    in mget_nonatomic), while go-redis routes the whole typed command by its
    first key and surfaces the server's CROSSSLOT. The server-side proof uses
    each library's official escape hatch aimed at one node: redis-py
    execute_command(target_nodes=...) and go-redis Do(...).
    """
    key_1 = next(iter(written["group-1"]))
    key_2 = next(iter(written["group-2"]))
    target = redis.cluster.ClusterNode("127.0.0.1", nodes[PRIMARY_1].redis_port)
    for cell in cells:
        tag = group_hashtag("group-1", f"positive-{cell.name}")
        cell_set(cell, f"{{{tag}}}-a", "pa", "crossslot control", 10)
        cell_set(cell, f"{{{tag}}}-b", "pb", "crossslot control", 10)
        if isinstance(cell, PyCell):
            # Positive control: same-slot multi-key works through the client.
            if cell.client.mget([f"{{{tag}}}-a", f"{{{tag}}}-b"]) != [b"pa", b"pb"]:
                raise H.Failure(f"{cell.name}: same-slot mget failed")
            try:
                cell.client.mget([key_1, key_2])
                raise H.Failure(f"{cell.name}: cross-slot mget was not rejected")
            except redis.exceptions.RedisClusterException as error:
                if "same key slot" not in str(error):
                    raise
            try:
                cell.client.execute_command("MGET", key_1, key_2, target_nodes=[target])
                raise H.Failure(f"{cell.name}: targeted cross-slot MGET succeeded")
            except redis.exceptions.ResponseError as error:
                # redis-py maps the CROSSSLOT code to a typed exception whose
                # message drops the code prefix.
                if not isinstance(
                    error, redis.exceptions.ClusterCrossSlotError
                ) and "CROSSSLOT" not in str(error):
                    raise
            try:
                cell.client.execute_command(
                    "MSET",
                    key_1,
                    "clobbered",
                    key_2,
                    "clobbered",
                    target_nodes=[target],
                )
                raise H.Failure(f"{cell.name}: targeted cross-slot MSET succeeded")
            except redis.exceptions.ResponseError as error:
                if not isinstance(
                    error, redis.exceptions.ClusterCrossSlotError
                ) and "CROSSSLOT" not in str(error):
                    raise
        else:
            if cell.value(op="mget", keys=[f"{{{tag}}}-a", f"{{{tag}}}-b"]) != [
                "pa",
                "pb",
            ]:
                raise H.Failure(f"{cell.name}: same-slot MGet failed")
            reply = cell.call(op="mget", keys=[key_1, key_2])
            if reply.get("ok") or "CROSSSLOT" not in reply.get("error", ""):
                raise H.Failure(f"{cell.name}: cross-slot MGet: {reply}")
            reply = cell.call(
                op="do", args=["MSET", key_1, "clobbered", key_2, "clobbered"]
            )
            if reply.get("ok") or "CROSSSLOT" not in reply.get("error", ""):
                raise H.Failure(f"{cell.name}: cross-slot MSET: {reply}")
        # The rejected MSET must not have partially applied.
        if (
            cell_get(cell, key_1, "post-CROSSSLOT read") != written["group-1"][key_1]
            or cell_get(cell, key_2, "post-CROSSSLOT read") != written["group-2"][key_2]
        ):
            raise H.Failure(f"{cell.name}: rejected MSET partially applied")
        H.log(
            f"  crossslot: {cell.name} surfaced server CROSSSLOT for "
            "MGET/MSET and preserved both values"
        )


def scenario_auth(nodes):
    noauth = raw_error(nodes[PRIMARY_1], [["SET", "noauth-probe", "x"]], auth=False)
    if not noauth.startswith("NOAUTH"):
        raise H.Failure(f"unauthenticated write was not NOAUTH: {noauth}")
    H.log(f"  auth: unauthenticated SET rejected ({noauth})")
    bad = None
    try:
        bad = redis.cluster.RedisCluster(
            startup_nodes=[
                redis.cluster.ClusterNode("127.0.0.1", nodes[PRIMARY_1].redis_port)
            ],
            password="wrong-password",
            socket_timeout=3,
            socket_connect_timeout=3,
        )
        # Eager slot-map initialization may already fail; otherwise the first
        # command must. redis-py reports the seed-side auth failure as a
        # RedisClusterException, so any client-level rejection counts.
        bad.ping()
        raise H.Failure("redis-py accepted a wrong password")
    except (
        redis.exceptions.RedisError,
        redis.exceptions.RedisClusterException,
    ) as error:
        # RedisClusterException extends Exception, not RedisError; the
        # seed-side auth failure surfaces as either.
        H.log(f"  auth: redis-py wrong password rejected ({error})")
    finally:
        if bad is not None:
            bad.close()
    bad = GoCell(
        "go-bad-auth", nodes[PRIMARY_1].redis_port, 3, password="wrong-password"
    )
    try:
        reply = bad.call(op="ping")
        if reply.get("ok") or not any(
            marker in reply.get("error", "") for marker in ("WRONGPASS", "NOAUTH")
        ):
            raise H.Failure(f"go-redis accepted a wrong password: {reply}")
    finally:
        bad.close()
    H.log("  auth: go-redis rejects a wrong password")


def scenario_controlled_failover(meta, cells, nodes, written):
    # Warm every pool: operations against both Groups, then idle the pools so
    # the cutover hits pre-existing idle connections as well as the active
    # load below.
    for cell in cells:
        for index in range(4):
            for group_id in sorted(GROUPS):
                key = group_key(group_id, f"warm-{cell.name}-{index}")
                cell_set(cell, key, f"warm-{index}", "pool warmup")
                written[group_id][key] = f"warm-{index}"
    time.sleep(3)
    assert_pools_idle(cells, 2, "controlled failover")
    H.log("  controlled: pools warm with idle connections")

    # The lossless reference set is written before the transition starts.
    lossless = {}
    for ordinal in range(8):
        key = group_key("group-1", f"lossless-{ordinal}")
        lossless[key] = f"lossless-{ordinal}"
    for key, value in lossless.items():
        cell_set(cells[0], key, value, "lossless reference write")
    written["group-1"].update(lossless)

    hashtags = [group_hashtag("group-1", "load"), group_hashtag("group-2", "load")]
    loads = start_loads(cells, hashtags)
    reply = subprocess.run(
        [
            CTL,
            "failover",
            "group-1",
            "--socket",
            meta.ctl_path,
            "--timeout-ms",
            "10000",
            "--failover-timeout-ms",
            "60000",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if reply.returncode != 0:
        raise H.Failure(f"failover submission failed: {reply}")
    match = re.search(r"operation=([0-9a-f]{32})", reply.stdout)
    if match is None:
        raise H.Failure(f"failover reply omitted its operation: {reply.stdout}")
    operation_id = match.group(1)
    H.wait_until(
        "controlled failover completes",
        60,
        lambda: meta.getop(operation_id) == "OK completed failover-completed",
    )
    wait_group_owner(meta, "group-1", REPLICA_1, 2, "controlled cutover")
    stop_loads(loads, "controlled failover")

    for cell in cells:
        key = group_key("group-1", f"post-fo-{cell.name}")
        cell_set(cell, key, "post", "post-failover write")
        written["group-1"][key] = "post"
        # Controlled failover is loss=none: EVERY acknowledged group-1 write
        # must survive, not just the reference set written last.
        for probe, value in written["group-1"].items():
            if cell_get(cell, probe, "lossless read-back") != value:
                raise H.Failure(f"{cell.name}: controlled failover lost {probe}")
        for key_2, value_2 in written["group-2"].items():
            if cell_get(cell, key_2, "unaffected Group read") != value_2:
                raise H.Failure(
                    f"{cell.name}: group-2 key {key_2} changed during group-1 failover"
                )
        H.log(
            f"  controlled: {cell.name} recovered on the new owner with "
            "the same pool, no acknowledged write lost"
        )


def scenario_crash_failover(meta, cells, nodes, written):
    primary = nodes[PRIMARY_2]
    replica = nodes[REPLICA_2]
    # Durable reference set: verified present on the replica BEFORE the kill,
    # so post-promotion reads must always find it (uncertain-loss semantics
    # apply only to writes the replica never saw).
    durable = {}
    for ordinal in range(4):
        key = group_key("group-2", f"durable-{ordinal}")
        durable[key] = f"durable-{ordinal}"
    for key, value in durable.items():
        cell_set(cells[0], key, value, "durable reference write")
    written["group-2"].update(durable)
    for key, value in durable.items():
        H.wait_until(
            f"replica holds {key}",
            20,
            lambda k=key, v=value: raw_session(replica, [["READONLY"], ["GET", k]])[1]
            == v,
        )

    hashtags = [group_hashtag("group-1", "load"), group_hashtag("group-2", "load")]
    # The kill lands on a pool that holds both idle connections (warmed by
    # earlier scenarios and settled here) and the active load below.
    time.sleep(3)
    assert_pools_idle(cells, 2, "crash failover")
    loads = start_loads(cells, hashtags)
    time.sleep(1)
    primary.force_kill()
    H.log("  crash: group-2 primary SIGKILLed with live pools attached")
    wait_group_owner(
        meta, "group-2", REPLICA_2, 2, "crash promotion", require_ready=False
    )
    stop_loads(loads, "crash failover")

    for cell in cells:
        key = group_key("group-2", f"post-crash-{cell.name}")
        # go-redis only refreshes a crash-stale slot map once it ages past its
        # default 60s ClusterStateReloadInterval (a dead node answers no
        # MOVED); redis-py reinitializes on connection errors. Both bounds are
        # stock-client behavior, so the recovery budget is 90s and the log
        # records each cell's actual rediscovery latency.
        started = time.monotonic()
        try:
            cell_set(cell, key, "post", "post-crash write", 90)
        except H.Failure as error:
            raise H.Failure(f"{error}; client slot map: {cell.slots()}")
        elapsed = time.monotonic() - started
        written["group-2"][key] = "post"
        for probe, value in durable.items():
            if cell_get(cell, probe, "durable read-back", 90) != value:
                raise H.Failure(f"{cell.name}: crash lost replicated {probe}")
        H.log(
            f"  crash: {cell.name} rediscovered the promoted owner after "
            f"{elapsed:.1f}s and resumed reads/writes on the same pool"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    work_argv = [sys.argv[0], META] + sys.argv[5:]
    workdir, keep = H.make_workdir(work_argv, "cluster_client_")
    certdir = os.path.join(workdir, "certs")
    os.makedirs(certdir, mode=0o700)
    ca, ca_key = make_ca(certdir)
    # Data nodes run --tls-replication, which puts their Meta control session
    # on mTLS as well; Meta must therefore present a matching member cert.
    # The ctl Unix socket stays peer-credential plaintext.
    meta_cert, meta_key = make_leaf(certdir, ca, ca_key, "meta", "lavik://meta/1")
    meta_workdir = os.path.join(workdir, "meta")
    os.makedirs(meta_workdir, mode=0o700)
    meta = H.Node(
        META,
        meta_workdir,
        1,
        args=(
            H.raft_args(snapshot_distance=100_000) + H.tls_args(ca, meta_cert, meta_key)
        ),
    )
    nodes = {}
    cells = []
    tls_cells = []
    written = {group_id: {} for group_id in GROUPS}
    try:
        for index, node_id in enumerate((PRIMARY_1, REPLICA_1, PRIMARY_2, REPLICA_2)):
            cert, key = make_leaf(
                certdir, ca, ca_key, f"data-{index}", f"lavik://node/{node_id}"
            )
            nodes[node_id] = DataProcess(
                DATA,
                os.path.join(workdir, f"data-{index}"),
                node_id,
                meta.data_control_endpoint,
                tls=(ca, cert, key),
                # Native replication between Lavik nodes passes through the
                # source's Redis AUTH surface (replication.cpp
                # AuthenticateUpstream), so a password-protected cluster also
                # needs masterauth, exactly like Redis deployments.
                extra_args=("--requirepass", PASSWORD, "--masterauth", PASSWORD),
            )
        manifest = os.path.join(workdir, "cluster.toml")
        write_manifest(
            manifest,
            meta,
            [
                nodes[node_id]
                for node_id in (PRIMARY_1, REPLICA_1, PRIMARY_2, REPLICA_2)
            ],
        )
        meta.start(bootstrap=True)
        meta.wait_leader()
        for node in nodes.values():
            node.start()
        created = subprocess.run(
            [
                CTL,
                "cluster-create",
                "--manifest",
                manifest,
                "--socket",
                meta.ctl_path,
                "--yes",
                "--timeout-ms",
                "120000",
            ],
            capture_output=True,
            text=True,
            timeout=150,
        )
        if created.returncode != 0 or "Cluster create accepted:" not in created.stdout:
            raise H.Failure(f"cluster-create failed: {created}")
        wait_cluster_ready(meta, "matrix cluster reaches READY", 90)
        configure_fast_policies(meta)
        H.log("cluster READY with fast failover policies")

        # Push past the finite-lease self-fence window before clients attach.
        # The warmup key is deleted again so the isolation scenario's DBSIZE
        # sees exactly the client-written set.
        for group_id, (primary_id, _, _, _) in sorted(GROUPS.items()):
            warmup_key = group_key(group_id, "warmup")
            H.wait_until(
                f"{group_id} serves writes",
                10,
                lambda p=nodes[primary_id], k=warmup_key: raw_session(
                    p, [["SET", k, "1"]]
                )[0]
                == "OK",
            )
            raw_session(nodes[primary_id], [["DEL", warmup_key]])

        primary_1 = nodes[PRIMARY_1]
        cells = [
            PyCell("py2", primary_1.redis_port, 2),
            PyCell("py3", primary_1.redis_port, 3),
            GoCell("go2", primary_1.redis_port, 2),
            GoCell("go3", primary_1.redis_port, 3),
        ]
        tls_cells = [
            PyCell("py2-tls", primary_1.tls_port, 2, tls_ca=ca),
            PyCell("py3-tls", primary_1.tls_port, 3, tls_ca=ca),
            GoCell("go2-tls", primary_1.tls_port, 2, tls_ca=ca),
            GoCell("go3-tls", primary_1.tls_port, 3, tls_ca=ca),
        ]
        for cell in cells:
            if isinstance(cell, GoCell):
                version = cell.value(op="version")
                if version != GO_REDIS_VERSION:
                    raise H.Failure(
                        f"{cell.name}: go-redis {version} != pinned {GO_REDIS_VERSION}"
                    )
        H.log(
            "clients attached: redis-py 8.1.0 and go-redis 9.22.0, "
            "RESP2/RESP3, plaintext+TLS"
        )

        scenario_discovery(cells, tls_cells, nodes, written)
        scenario_shards_boundary(cells + tls_cells)
        scenario_isolation(cells, nodes, written)
        scenario_crossslot(cells, nodes, written)
        scenario_auth(nodes)
        scenario_controlled_failover(meta, cells + tls_cells, nodes, written)
        scenario_crash_failover(meta, cells + tls_cells, nodes, written)

        for cell in cells + tls_cells:
            cell.close()
        for node in nodes.values():
            node.terminate()
        meta.terminate()
        H.log("PASS")
        return 0
    except Exception as error:  # noqa: BLE001 - logs are test evidence
        H.log(f"FAIL: {error}")
        H.dump_node_logs([meta])
        for node in nodes.values():
            print(f"--- Data log tail ({node.log_path}) ---", file=sys.stderr)
            print(node.log_tail(lines=150), file=sys.stderr)
        keep = True
        return 1
    finally:
        for cell in cells + tls_cells:
            try:
                cell.close()
            except Exception:  # noqa: BLE001 - teardown best effort
                pass
        for node in nodes.values():
            node.force_kill()
        meta.force_kill()
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    if len(sys.argv) not in (5, 6):
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    META = os.path.abspath(sys.argv[1])
    DATA = os.path.abspath(sys.argv[2])
    CTL = os.path.abspath(sys.argv[3])
    GO_DRIVER = os.path.abspath(sys.argv[4])
    H.set_tag("cluster-client")
    sys.exit(main())
