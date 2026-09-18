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

"""Real-process failover gate across three Meta and three Data nodes.

Usage:
  gate_failover.py META DATA CTL REDIS_CLI --case=CASE [workdir]

The optional workdir is retained for diagnosis.  CTest and callers that care
where process data is allocated should pass it explicitly or set
LAVIK_TEST_DATA_DIR.
"""

import argparse
import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402
from gate_data_control import DataProcess  # noqa: E402


GROUP = "failover-group"
OWNER = "1111111111111111111111111111111111111111"
CANDIDATE = "2222222222222222222222222222222222222222"
FOLLOWER = "3333333333333333333333333333333333333333"
PAUSE_BEGIN_HOOK = b"LAVIK_TEST_PAUSE_FAILOVER_AFTER_BEGIN_MS"
PAUSE_AUTHORIZE_HOOK = b"LAVIK_TEST_PAUSE_FAILOVER_AFTER_AUTHORIZE_MS"
PAUSE_PREPARED_HOOK = b"LAVIK_TEST_PAUSE_FAILOVER_AFTER_PREPARED_MS"


class FailoverMetaNode(H.Node):
    """Starts Meta with the optional failover cut and TCP admin transport."""

    pause_after_begin_ms = None
    pause_after_authorize_ms = None
    pause_after_prepared_ms = None

    def start(self, *args, **kwargs):
        variables = {
            PAUSE_BEGIN_HOOK.decode(): self.pause_after_begin_ms,
            PAUSE_AUTHORIZE_HOOK.decode(): self.pause_after_authorize_ms,
            PAUSE_PREPARED_HOOK.decode(): self.pause_after_prepared_ms,
        }
        previous = {name: os.environ.get(name) for name in variables}
        try:
            # Commands use the advertised loopback admin endpoint. A short
            # explicit Unix path is still supplied because Meta also starts
            # that listener even though this gate never drives it.
            kwargs.setdefault("explicit_ctl_socket", True)
            for name, value in variables.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = str(value)
            return super().start(*args, **kwargs)
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value

    def ctl(self, command, timeout=5.0):
        """Send one direct admin command over the loopback TCP listener."""
        with socket.create_connection(
                ("127.0.0.1", self.ctl_port), timeout=timeout) as connection:
            connection.settimeout(timeout)
            connection.sendall(command.encode() + b"\n")
            reply = bytearray()
            while not reply.endswith(b"\n"):
                chunk = connection.recv(65536)
                if not chunk:
                    break
                reply.extend(chunk)
        return reply.decode().strip()


class IdentityDropProxy:
    """Data-control proxy that can blackhole one authenticated node only."""

    def __init__(self, name, target_port, blocked_node_id):
        self.name = name
        self.target = ("127.0.0.1", target_port)
        self.blocked = blocked_node_id.encode()
        self.listen_port = H.free_port()
        self._lock = threading.Lock()
        self._listener = None
        self._pairs = {}
        self._held = set()
        self._drop_upstream = False
        self._drop_downstream = False
        self._running = False
        self._accept_thread = None
        self._ready = threading.Event()

    @property
    def endpoint(self):
        return f"127.0.0.1:{self.listen_port}"

    def start(self):
        self._running = True
        self._accept_thread = threading.Thread(
            target=self._accept_loop,
            name=f"control-proxy-{self.name}-accept", daemon=True)
        self._accept_thread.start()
        if not self._ready.wait(timeout=3.0) or self._listener is None:
            raise H.Failure(f"control proxy {self.name} failed to listen")

    def drop_blocked(self, direction="both"):
        """Blackhole Owner traffic without exposing transport EOF.

        ``upstream`` is Data-to-Meta, ``downstream`` is Meta-to-Data, and
        ``both`` cuts both directions. New sessions cannot finish their
        handshake when ClientHello itself is in the selected direction.
        """
        if direction not in ("upstream", "downstream", "both"):
            raise H.Failure(
                f"invalid Data-control partition direction {direction!r}")
        with self._lock:
            self._drop_upstream = direction in ("upstream", "both")
            self._drop_downstream = direction in ("downstream", "both")

    def heal(self):
        with self._lock:
            self._drop_upstream = False
            self._drop_downstream = False
            held = list(self._held)
            blocked_pairs = [
                pair for pair, identity in self._pairs.items()
                if identity == self.blocked]
        # A blackholed upstream may already have observed its heartbeat
        # timeout. Close both proxy endpoints only at heal so Data reconnects
        # without ever having used peer EOF as its authority fence.
        for pair in blocked_pairs:
            self._cut_pair(pair)
        # A held ClientHello cannot be replayed into a newly opened upstream;
        # closing it makes the real client establish a fresh session promptly.
        for connection in held:
            self._close_socket(connection)

    def _open_listener(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", self.listen_port))
        listener.listen(128)
        listener.settimeout(0.2)
        return listener

    def _accept_loop(self):
        try:
            listener = self._open_listener()
        except OSError:
            self._ready.set()
            return
        with self._lock:
            self._listener = listener
        self._ready.set()
        while self._running:
            try:
                connection, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._classify_and_forward,
                             args=(connection,), daemon=True).start()

    def _classify_and_forward(self, connection):
        prefix = bytearray()
        identities = tuple(
            node_id.encode() for node_id in (OWNER, CANDIDATE, FOLLOWER))
        identity = None
        try:
            connection.settimeout(5.0)
            while len(prefix) < 4096 and identity is None:
                chunk = connection.recv(4096 - len(prefix))
                if not chunk:
                    self._close_socket(connection)
                    return
                prefix.extend(chunk)
                identity = next(
                    (candidate for candidate in identities
                     if candidate in prefix), None)
            if identity is None:
                self._close_socket(connection)
                return
            connection.settimeout(None)
            with self._lock:
                drop = self._drop_upstream and identity == self.blocked
                if drop:
                    self._held.add(connection)
            if drop:
                self._discard(connection)
                return
            upstream = socket.create_connection(self.target, timeout=5.0)
            upstream.settimeout(None)
            upstream.sendall(prefix)
            pair = (connection, upstream)
            with self._lock:
                # Recheck after dialing: a concurrent partition must not leak
                # a just-classified Owner session through the cut.
                late_drop = self._drop_upstream and identity == self.blocked
                if late_drop:
                    self._held.add(connection)
                else:
                    self._pairs[pair] = identity
            if late_drop:
                upstream.close()
                self._discard(connection)
                return
            for source, target in ((connection, upstream),
                                   (upstream, connection)):
                threading.Thread(target=self._pump,
                                 args=(source, target, pair),
                                 daemon=True).start()
        except OSError:
            self._close_socket(connection)

    def _pump(self, source, target, pair):
        preserve_half_open = False
        try:
            while True:
                data = source.recv(65536)
                with self._lock:
                    identity = self._pairs.get(pair)
                    upstream = source is pair[0]
                    dropping = identity == self.blocked and (
                        self._drop_upstream if upstream
                        else self._drop_downstream)
                if not data:
                    # In a network partition, Meta observing EOF must not
                    # notify Data. Its local finite lease is the safety proof
                    # under test; heal() will close the retained client side.
                    preserve_half_open = dropping
                    break
                if dropping:
                    continue
                target.sendall(data)
        except OSError:
            with self._lock:
                identity = self._pairs.get(pair)
                upstream = source is pair[0]
                preserve_half_open = identity == self.blocked and (
                    self._drop_upstream if upstream
                    else self._drop_downstream)
        finally:
            if not preserve_half_open:
                self._cut_pair(pair)

    def _discard(self, connection):
        try:
            while connection.recv(65536):
                pass
        except OSError:
            pass
        finally:
            with self._lock:
                self._held.discard(connection)
            self._close_socket(connection)

    def _cut_pair(self, pair):
        with self._lock:
            if pair not in self._pairs:
                return
            self._pairs.pop(pair)
        for connection in pair:
            self._close_socket(connection)

    @staticmethod
    def _close_socket(connection):
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            connection.close()
        except OSError:
            pass

    def close(self):
        self._running = False
        with self._lock:
            listener, self._listener = self._listener, None
            pairs = list(self._pairs)
            held = list(self._held)
        if listener is not None:
            self._close_socket(listener)
        for pair in pairs:
            self._cut_pair(pair)
        for connection in held:
            self._close_socket(connection)
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=3)


def binary_contains(path, needle):
    """Find a fault-hook marker without loading a large binary at once."""
    tail = b""
    with open(path, "rb") as source:
        while chunk := source.read(1 << 20):
            joined = tail + chunk
            if needle in joined:
                return True
            tail = joined[-len(needle):]
    return False


def meta_manifest_lines(metas):
    lines = []
    for meta in sorted(metas, key=lambda node: node.id):
        data_control_endpoint = getattr(
            meta, "advertised_data_control_endpoint",
            meta.data_control_endpoint)
        lines.extend([
            "[[meta_members]]",
            f"id = {meta.id}",
            f'raft_endpoint = "tcp://{meta.endpoint}"',
            f'data_control_endpoint = "tcp://{data_control_endpoint}"',
            f'ctl_endpoint = "tcp://{meta.ctl_endpoint}"',
            "",
        ])
    return lines


def write_manifest(path, metas, data_nodes, *,
                   automatic_uncontrolled_failover_suspect_after_ms=None):
    by_id = {node.node_id: node for node in data_nodes}
    replicas = sorted(node_id for node_id in by_id if node_id != OWNER)
    replica_list = ", ".join(f'"{node_id}"' for node_id in replicas)
    lines = ["schema_version = 1", ""] + meta_manifest_lines(metas)
    for node_id in sorted(by_id):
        lines.extend([
            "[[data_nodes]]",
            f'id = "{node_id}"',
            f'client_endpoint = "{by_id[node_id].advertised_endpoint}"',
            "",
        ])
    lines.extend([
        "[[groups]]",
        f'id = "{GROUP}"',
        f'primary = "{OWNER}"',
        f"replicas = [{replica_list}]",
        "",
        "[[slot_ranges]]",
        "first = 0",
        "last = 16383",
        f'group = "{GROUP}"',
        "",
    ])
    if automatic_uncontrolled_failover_suspect_after_ms is not None:
        lines.extend([
            "[bootstrap_policy]",
            "automatic_uncontrolled_failover_suspect_after_ms = "
            f"{automatic_uncontrolled_failover_suspect_after_ms}",
            "",
        ])
    with open(path, "w", encoding="utf-8") as output:
        output.write("\n".join(lines))


def run_command(arguments, timeout=150, expected=0):
    result = subprocess.run(arguments, capture_output=True, text=True,
                            timeout=timeout)
    if result.returncode != expected:
        raise H.Failure(
            f"command failed ({result.returncode}, want {expected}): "
            f"{' '.join(arguments)} stdout={result.stdout!r} "
            f"stderr={result.stderr!r}")
    return result.stdout


def cluster_status(ctl, meta, timeout=5.0):
    try:
        result = subprocess.run(
            [ctl, "cluster-status", "--addr", meta.ctl_endpoint,
             "--allow-plaintext-admin", "--json"],
            capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as error:
        raise H.Failure(
            f"cluster-status timed out against Meta {meta.id}") from error
    if result.returncode not in (0, 2):
        raise H.Failure(f"cluster-status failed: {result}")
    return json.loads(result.stdout)


def endpoint(data):
    address = data.advertised_endpoint.removeprefix("tcp://")
    host, port = address.rsplit(":", 1)
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
    line = reader.readline()
    if not prefix or not line.endswith(b"\r\n"):
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
    with socket.create_connection(endpoint(data), timeout=3.0) as connection:
        connection.settimeout(3.0)
        connection.sendall(encode_resp(arguments))
        return read_resp(connection.makefile("rb"))


def redis_error(data, arguments):
    with socket.create_connection(endpoint(data), timeout=3.0) as connection:
        connection.settimeout(3.0)
        connection.sendall(encode_resp(arguments))
        line = connection.makefile("rb").readline()
    if not line.startswith(b"-") or not line.endswith(b"\r\n"):
        raise H.Failure(
            f"expected Redis error for {arguments}, received {line!r}")
    return line[1:-2].decode(errors="replace")


def replication_info_fields(data):
    """Parse the scalar INFO replication fields used as process evidence."""
    info = redis_call(data, ["INFO", "replication"])
    fields = {}
    for line in info.splitlines():
        name, separator, value = line.partition(":")
        if separator:
            fields[name] = value
    return fields


def readonly_get(data, key):
    with socket.create_connection(endpoint(data), timeout=3.0) as connection:
        connection.settimeout(3.0)
        connection.sendall(encode_resp(["READONLY"]) +
                           encode_resp(["GET", key]))
        reader = connection.makefile("rb")
        if read_resp(reader) != "OK":
            raise H.Failure("replica rejected READONLY")
        return read_resp(reader)


class ContinuousGetProbe:
    """Fails on any read gap instead of accepting eventual recovery."""

    def __init__(self, data, key, expected):
        self.data = data
        self.key = key
        self.expected = expected
        self._stop = threading.Event()
        self._thread = None
        self._failure = None
        self._attempts = 0

    def start(self):
        self._thread = threading.Thread(
            target=self._run, name="failover-continuous-get", daemon=True)
        self._thread.start()

    def _run(self):
        while not self._stop.is_set() and self._failure is None:
            try:
                reply = redis_call(self.data, ["GET", self.key])
                self._attempts += 1
                if reply != self.expected:
                    self._failure = (
                        f"GET returned {reply!r}, want {self.expected!r}")
                    break
            except Exception as error:  # noqa: BLE001 - preserve first gap
                self._failure = f"GET failed: {error}"
                break
            self._stop.wait(0.01)

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def stop_and_assert(self):
        self.stop()
        if self._failure is not None:
            raise H.Failure(
                "old Owner read availability broke across Begin/FDS/pause: "
                + self._failure)
        if self._attempts < 2:
            raise H.Failure(
                "continuous GET probe did not span multiple observations")


class ContinuousSetProbe:
    """Records successful SET intervals and fails on transport surprises."""

    _EXPECTED_REJECTIONS = (
        "-MOVED ", "-TRYAGAIN ", "-CLUSTERDOWN ", "-LOADING ",
        "-READONLY ")

    def __init__(self, data, key, writer):
        self.data = data
        self.key = key
        self.writer = writer
        self._stop = threading.Event()
        self._thread = None
        self._lock = threading.Lock()
        self._failure = None
        self._attempts = 0
        self._successes = []

    def start(self):
        self._thread = threading.Thread(
            target=self._run, name=f"failover-set-{self.writer}",
            daemon=True)
        self._thread.start()

    def _run(self):
        sequence = 0
        while not self._stop.is_set():
            sequence += 1
            started_ns = time.monotonic_ns()
            try:
                reply = self.data.command_head(
                    ["SET", self.key, f"{self.writer}-{sequence}"])
            except Exception as error:  # noqa: BLE001 - first gap is evidence
                self._record_failure(f"SET transport failed: {error}")
                break
            completed_ns = time.monotonic_ns()
            with self._lock:
                self._attempts += 1
                if reply == "+OK":
                    self._successes.append((started_ns, completed_ns))
                elif not reply.startswith(self._EXPECTED_REJECTIONS):
                    self._failure = (
                        f"SET returned unexpected response {reply!r}")
                    break
            self._stop.wait(0.02)

    def _record_failure(self, detail):
        with self._lock:
            if self._failure is None:
                self._failure = detail

    def assert_healthy(self):
        with self._lock:
            failure = self._failure
        if failure is not None:
            raise H.Failure(f"{self.writer} write probe failed: {failure}")

    def successes(self):
        with self._lock:
            return list(self._successes)

    def wait_for_success(self, description, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.assert_healthy()
            if self.successes():
                return
            time.sleep(0.01)
        self.assert_healthy()
        raise H.Failure(f"timeout ({timeout}s) waiting for: {description}")

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def stop_and_assert(self):
        self.stop()
        self.assert_healthy()
        with self._lock:
            if self._attempts < 2:
                raise H.Failure(
                    f"{self.writer} write probe made too few observations")


def wait_ready(fixture, description, timeout=90):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        try:
            latest = fixture.cluster_status(deadline)
        except H.Failure:
            time.sleep(0.05)
            continue
        if latest.get("result") == "ready":
            return
        if latest.get("cluster_state") == "provisioning-failed":
            operation_id = latest.get("root_operation_id")
            detail = (fixture.getop(operation_id, deadline)
                      if operation_id is not None
                      else "root operation unavailable")
            raise H.Failure(
                f"{description} entered provisioning-failed: {detail}; "
                f"status={latest}")
        time.sleep(0.05)
    raise H.Failure(
        f"timeout ({timeout}s) waiting for: {description}; status={latest}")


def wait_operation(fixture, operation_id, expected, description, timeout=20):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        try:
            latest = fixture.getop(operation_id, deadline)
        except H.Failure:
            time.sleep(0.05)
            continue
        if latest == expected:
            return
        time.sleep(0.05)
    raise H.Failure(
        f"timeout ({timeout}s) waiting for: {description}; getop={latest}")


def wait_owner(fixture, owner, nodes, timeout=90):
    latest = None
    deadline = time.monotonic() + timeout

    def converged():
        nonlocal latest
        latest = fixture.cluster_status(deadline)
        groups = {item.get("group_id"): item
                  for item in latest.get("groups", [])}
        members = {item.get("node_id"): item
                   for item in latest.get("data_nodes", [])}
        group = groups.get(GROUP, {})
        return (latest.get("result") == "ready" and
                group.get("term") == "2" and
                group.get("owner_node_id") == owner and
                all(members.get(node.node_id, {}).get("current_session") and
                    members.get(node.node_id, {}).get("projection_current")
                    for node in nodes))

    try:
        H.wait_until(f"{GROUP} cutover and all Data nodes converge", timeout,
                     converged)
    except H.Failure as error:
        raise H.Failure(f"{error}; status={latest}") from error


def wait_candidate_source(fixture, owner, term, candidate_ids):
    """Wait for Meta's candidate evidence after local replication catches up."""
    source_history = replication_info_fields(
        fixture.by_id[owner])["master_replid"]
    expected = {
        "source_term": str(term), "source_node": owner,
        "source_history": source_history,
        "storage_ready": "true", "population_ready": "true",
    }

    def advertised():
        fixture.rediscover_leader(time.monotonic() + 5)
        reply = fixture.leader.observations(GROUP)
        if not reply.startswith("OK candidates="):
            raise H.Failure(f"candidate observation query failed: {reply}")
        for entry in reply.split()[2:]:
            fields = dict(field.split("=", 1) for field in entry.split(","))
            if (fields.get("node") in candidate_ids and
                    all(fields.get(key) == value
                        for key, value in expected.items())):
                return True
        return False

    H.wait_until("Meta observes a candidate in the current source domain",
                 30, advertised)


def wait_serving_owner(fixture, owner, connected_nodes, timeout=90):
    """Wait for cutover while a deliberately partitioned member is stale."""
    latest = None
    deadline = time.monotonic() + timeout

    def serving():
        nonlocal latest
        latest = fixture.cluster_status(deadline)
        groups = {item.get("group_id"): item
                  for item in latest.get("groups", [])}
        members = {item.get("node_id"): item
                   for item in latest.get("data_nodes", [])}
        group = groups.get(GROUP, {})
        return (group.get("term") == "2" and
                group.get("owner_node_id") == owner and
                group.get("serving_ready") and
                all(members.get(node.node_id, {}).get("current_session") and
                    members.get(node.node_id, {}).get("projection_current")
                    for node in connected_nodes))

    try:
        H.wait_until(f"{owner[:8]} becomes the serving term-2 Owner", timeout,
                     serving)
    except H.Failure as error:
        raise H.Failure(f"{error}; status={latest}") from error


_FAILOVER_LOG_SAFE_BYTES = frozenset(
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:")


def decode_failover_log_token(token):
    """Strict inverse of Meta's canonical percent-encoded log token."""
    try:
        encoded = token.encode("ascii")
    except UnicodeEncodeError as error:
        raise H.Failure(f"non-ASCII failover log token: {token!r}") from error
    decoded = bytearray()
    index = 0
    while index < len(encoded):
        byte = encoded[index]
        if byte in _FAILOVER_LOG_SAFE_BYTES:
            decoded.append(byte)
            index += 1
            continue
        if (byte != ord("%") or index + 2 >= len(encoded) or
                chr(encoded[index + 1]) not in "0123456789ABCDEF" or
                chr(encoded[index + 2]) not in "0123456789ABCDEF"):
            raise H.Failure(
                f"non-canonical failover log token: {token!r}")
        value = int(encoded[index + 1:index + 3], 16)
        if value in _FAILOVER_LOG_SAFE_BYTES:
            raise H.Failure(
                f"over-escaped failover log token: {token!r}")
        decoded.append(value)
        index += 3
    return bytes(decoded).decode("utf-8", errors="surrogateescape")


def failover_log_records(metas):
    text = "\n".join(meta.log_tail(lines=2000) for meta in metas)
    pattern = re.compile(
        rf"failover event=(?P<event>[a-z-]+) "
        r"mode=(?P<mode>controlled|uncontrolled) "
        r"group=(?P<group>[^ \n]+) "
        r"transition=(?P<transition>[0-9a-f]{32}|none) "
        r"action=(?P<action>[0-9a-f]{32}|none) "
        r"loss=(?P<loss>none|unknown|pending) "
        r"commit_index=(?P<index>[1-9][0-9]*)(?P<detail>[^\n]*)")
    records = []
    for match in pattern.finditer(text):
        record = match.groupdict()
        record["group"] = decode_failover_log_token(record["group"])
        if record["group"] != GROUP:
            continue
        candidate = re.search(
            r"(?:^| )candidate=([^ \n]+)(?: |$)",
            record["detail"])
        record["candidate"] = (
            None if candidate is None else
            decode_failover_log_token(candidate.group(1)))
        if (record["candidate"] is not None and
                re.fullmatch(r"[0-9a-f]{40}", record["candidate"]) is None):
            raise H.Failure(
                f"invalid candidate in failover log: {record!r}")
        reason = re.search(r"(?:^| )reason=([^ \n]+)$", record["detail"])
        record["reason"] = (
            None if reason is None else
            decode_failover_log_token(reason.group(1)))
        suspect = re.search(r"(?:^| )suspect_ms=([0-9]+)(?: |$)",
                            record["detail"])
        record["suspect_ms"] = (
            None if suspect is None else int(suspect.group(1)))
        records.append(record)
    return text, records


def require_unique_failover_event(metas, event, mode, *, loss=None):
    """Collapse replica log copies but reject two committed event identities."""
    text, records = failover_log_records(metas)
    matches = [
        record for record in records
        if (record["event"] == event and record["mode"] == mode and
            (loss is None or record["loss"] == loss))
    ]
    keys = ("event", "mode", "group", "transition", "action", "loss",
            "index", "candidate", "reason", "suspect_ms")
    distinct = {tuple(record[key] for key in keys) for record in matches}
    if len(distinct) != 1:
        raise H.Failure(
            f"structured failover log has {len(distinct)} distinct "
            f"{mode} {event} events: {matches}; tail={text[-8000:]}")
    values = next(iter(distinct))
    return dict(zip(keys, values))


def parse_failover_log(metas):
    events = {
        event: require_unique_failover_event(
            metas, event, "controlled", loss="none")
        for event in ("begin", "authorize", "cutover")
    }
    identities = {(event["transition"], event["action"])
                  for event in events.values()}
    if len(identities) != 1:
        raise H.Failure(f"failover logs disagree on identities: {events}")
    return events


class FailoverFixture:
    """Owns one isolated real-process cluster for a failover scenario."""

    def __init__(self, meta_binary, data_binary, ctl, scenario,
                 require_fault_hook, pause_after_begin_ms=8_000,
                 pause_after_authorize_ms=None,
                 pause_after_prepared_ms=None, proxy_data_control=False,
                 data_workers=1):
        self.ctl = ctl
        self.scenario = scenario
        os.makedirs(scenario, mode=0o700)
        # AF_UNIX paths cap at roughly 108 bytes. Keep the fixed suffix short
        # so sockets fit beneath either the case directory or configured root.
        meta_dir = os.path.join(scenario, "m")
        os.makedirs(meta_dir, mode=0o700)
        socket_root = os.environ.get("LAVIK_FAILOVER_SOCKET_ROOT")
        self.socket_directory = None
        if socket_root is not None:
            os.makedirs(socket_root, mode=0o700, exist_ok=True)
            self.socket_directory = tempfile.TemporaryDirectory(
                prefix="f-", dir=socket_root)
            socket_dir = self.socket_directory.name
        else:
            socket_dir = meta_dir
        configured_pauses = [
            (PAUSE_BEGIN_HOOK, pause_after_begin_ms),
            (PAUSE_AUTHORIZE_HOOK, pause_after_authorize_ms),
            (PAUSE_PREPARED_HOOK, pause_after_prepared_ms),
        ]
        configured_pauses = [item for item in configured_pauses
                             if item[1] is not None]
        if len(configured_pauses) != 1:
            raise H.Failure("one failover fixture requires exactly one cut")
        required_hook = configured_pauses[0][0]
        self.hook_available = binary_contains(meta_binary, required_hook)
        if require_fault_hook and not self.hook_available:
            raise H.Failure(
                "failover gate requires its deterministic pause hook: "
                + required_hook.decode())

        self.metas = [
            FailoverMetaNode(
                meta_binary, meta_dir, node_id,
                # The shared 300-600ms process-test election window is
                # shorter than a contended real FULL activation. These gates
                # inject leadership changes explicitly, so incidental Raft
                # churn would only make one-shot admin outcomes ambiguous.
                args=H.raft_args(
                    snapshot_distance=100_000,
                    election_ms_low=1_500,
                    election_ms_high=3_000))
            for node_id in range(1, 4)
        ]
        for meta in self.metas:
            meta.ctl_path = os.path.join(socket_dir, f"c{meta.id}.sock")
        if self.hook_available:
            for meta in self.metas:
                meta.pause_after_begin_ms = pause_after_begin_ms
                meta.pause_after_authorize_ms = pause_after_authorize_ms
                meta.pause_after_prepared_ms = pause_after_prepared_ms
        self.control_proxies = []
        if proxy_data_control:
            for meta in self.metas:
                proxy = IdentityDropProxy(
                    f"m{meta.id}", meta.data_control_port, OWNER)
                meta.advertised_data_control_endpoint = proxy.endpoint
                self.control_proxies.append(proxy)

        def data_seed(meta):
            return getattr(meta, "advertised_data_control_endpoint",
                           meta.data_control_endpoint)

        self.data_nodes = [
            DataProcess(data_binary, os.path.join(scenario, "owner"), OWNER,
                        data_seed(self.metas[0]), workers=data_workers),
            DataProcess(data_binary, os.path.join(scenario, "candidate"),
                        CANDIDATE, data_seed(self.metas[1]), workers=data_workers),
            DataProcess(data_binary, os.path.join(scenario, "follower"),
                        FOLLOWER, data_seed(self.metas[2]), workers=data_workers),
        ]
        self.by_id = {node.node_id: node for node in self.data_nodes}
        self.manifest = os.path.join(scenario, "cluster.toml")
        self.operation_id = None
        self.leader = self.metas[0]

    @staticmethod
    def _remaining(deadline, maximum):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise H.Failure("Meta query exhausted its global deadline")
        return min(maximum, remaining)

    def rediscover_leader(self, deadline):
        """Find the current leader without extending the caller's deadline."""
        ordered = [self.leader] + [
            meta for meta in self.metas if meta is not self.leader]
        for meta in ordered:
            if not meta.alive():
                continue
            try:
                reply = meta.ctl(
                    "status", timeout=self._remaining(deadline, 1.0))
            except (OSError, H.Failure):
                continue
            fields = {}
            if reply.startswith("OK "):
                for token in reply[3:].split():
                    key, _, value = token.partition("=")
                    fields[key] = value
            if fields.get("leader") == "1":
                self.leader = meta
                return meta
        raise H.Failure("no Meta seed currently reports itself as leader")

    def cluster_status(self, deadline):
        """Read status from the current leader, tolerating election races."""
        attempted = set()
        while time.monotonic() < deadline:
            leader = self.rediscover_leader(deadline)
            try:
                return cluster_status(
                    self.ctl, leader, self._remaining(deadline, 2.0))
            except (OSError, H.Failure, json.JSONDecodeError):
                attempted.add(leader.id)
                # Leadership can change between the cheap status probe and
                # cluster-status. Give Raft a chance to expose the successor;
                # the enclosing operation retains the original deadline.
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))
                if len(attempted) == len(self.metas):
                    attempted.clear()
        raise H.Failure("cluster-status exhausted its global deadline")

    def getop(self, operation_id, deadline):
        """Read durable operation state through incidental Meta elections."""
        while time.monotonic() < deadline:
            leader = self.rediscover_leader(deadline)
            try:
                return leader.ctl(
                    f"getop {operation_id}",
                    timeout=self._remaining(deadline, 1.0))
            except (OSError, H.Failure):
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        raise H.Failure("getop exhausted its global deadline")

    def start_created(self, add_follower=True, *,
                      automatic_uncontrolled_failover_suspect_after_ms=600_000):
        # Keep this failover gate independent of #40's explicit Genesis
        # replica-initialization operation. Once the Owner-only topology is
        # Created, both replicas enter through the production steady
        # FollowOwner path that failover also relies on after cutover.
        # Allow topology setup and controlled-failover fault cuts to finish
        # within a finite, long suspicion interval. Automatic detection gates
        # install their short threshold once READY.
        write_manifest(
            self.manifest, self.metas, self.data_nodes[:1],
            automatic_uncontrolled_failover_suspect_after_ms=
            automatic_uncontrolled_failover_suspect_after_ms)
        for proxy in self.control_proxies:
            proxy.start()
        for meta in self.metas:
            # Spawn all peers before waiting for the elected leader and stable
            # membership. start() mutates fault-hook environment variables, so
            # launch sequentially without introducing concurrent Python calls.
            meta.start(initial_cluster_manifest=self.manifest, wait_ready=False)
        self.leader = H.find_leader(self.metas, timeout=20)
        membership_deadline = time.monotonic() + 10
        latest = None
        while time.monotonic() < membership_deadline:
            try:
                latest = self.cluster_status(membership_deadline)
            except H.Failure:
                time.sleep(0.05)
                continue
            if latest.get("meta_membership_stable"):
                break
            time.sleep(0.05)
        else:
            raise H.Failure(
                "initial Meta identities did not converge before Cluster "
                f"Create: status={latest}")
        leader_seed = getattr(
            self.leader, "advertised_data_control_endpoint",
            self.leader.data_control_endpoint)
        starting_nodes = self.data_nodes if add_follower else self.data_nodes[:1]
        for data in starting_nodes:
            # A bootstrap seed is not a leader-discovery service. Once the
            # first FDS is installed, committed membership drives reconnects;
            # before that point a non-leader seed can only reject the client.
            data.seed = leader_seed
            data.start(wait_ready=False)
        # All Data storage initialization can overlap. Replica control sessions
        # retry at a short bounded interval until Owner-only creation finishes
        # and their identities can be registered for steady FollowOwner.
        H.wait_until(
            "initial Data metrics listeners", 20,
            lambda: all(data.alive() and data._metrics_ready()
                        for data in starting_nodes))

        created = run_command([
            self.ctl, "cluster-create", "--manifest", self.manifest,
            "--addr", self.leader.ctl_endpoint, "--allow-plaintext-admin",
            "--yes", "--timeout-ms", "120000",
        ])
        if "Cluster create accepted:" not in created:
            raise H.Failure(f"cluster-create was not accepted: {created!r}")
        wait_ready(self, "initial Owner-only cluster reaches READY")
        if add_follower:
            self.add_replicas((CANDIDATE, FOLLOWER), started=True)

    def add_replica(self, node_id):
        self.add_replicas((node_id,))

    def add_replicas(self, node_ids, *, started=False):
        """Assign replicas without waiting for each population to become ready."""
        replicas = [self.by_id[node_id] for node_id in node_ids]
        self.rediscover_leader(time.monotonic() + 5)
        seed = getattr(
            self.leader, "advertised_data_control_endpoint",
            self.leader.data_control_endpoint)
        for replica in replicas:
            node_id = replica.node_id
            registered = self.leader.registernode(
                node_id, f"lavik://node/{node_id}", "replica",
                endpoints=(replica.advertised_endpoint,))
            if not registered.startswith("OK "):
                raise H.Failure(
                    f"replica {node_id[:8]} registration failed: {registered}")
        if not started:
            for replica in replicas:
                replica.seed = seed
                replica.start(wait_ready=False)
        # Group revision and topology epoch use CAS, so commit assignments in
        # order. Their FULL populations can then run concurrently; READY is a
        # single barrier after every replica has been assigned.
        for replica in replicas:
            assigned = self.leader.assignnode(GROUP, replica.node_id, "replica")
            if not assigned.startswith("OK "):
                raise H.Failure(
                    f"replica {replica.node_id[:8]} assignment failed: {assigned}")
        wait_ready(self,
                   "assigned replicas follow Owner and cluster "
                   "returns to READY")

    def seed_and_wait_for_replicas(self, key, value, replica_ids):
        if redis_call(self.by_id[OWNER], ["SET", key, value]) != "OK":
            raise H.Failure("old Owner rejected the initial write")
        for replica_id in replica_ids:
            H.wait_until(
                f"{replica_id[:8]} receives the initial write", 20,
                lambda replica_id=replica_id:
                readonly_get(self.by_id[replica_id], key) == value)

    def submit_failover(self):
        # Reduce the election window before this one-shot mutation. If the
        # request has an uncertain outcome, do not resubmit it: a blind retry
        # could create a second durable failover operation.
        self.rediscover_leader(time.monotonic() + 5)
        accepted = run_command([
            self.ctl, "failover", GROUP, "--addr",
            self.leader.ctl_endpoint, "--allow-plaintext-admin",
            "--timeout-ms", "10000", "--failover-timeout-ms", "60000",
        ], timeout=20)
        match = re.search(r"operation=([0-9a-f]{32})", accepted)
        if match is None:
            raise H.Failure(
                f"controlled failover omitted operation id: {accepted!r}")
        self.operation_id = match.group(1)
        return self.operation_id

    def wait_post_begin_pause(self):
        if not self.hook_available:
            H.log("SKIP deterministic pause assertions: ordinary Release "
                  "erases the failover pause hook")
            return False
        H.wait_until(
            "failover reconciler reaches deterministic post-Begin cut", 20,
            lambda: any(
                "failover reconciliation paused after controlled Begin"
                in meta.log_tail(lines=500) for meta in self.metas))
        return True

    def wait_post_authorize_pause(self):
        if not self.hook_available:
            H.log("SKIP deterministic authorize-cut assertions: ordinary "
                  "Release erases the failover pause hook")
            return False
        H.wait_until(
            "failover reconciler reaches deterministic post-Authorize cut",
            30, lambda: any(
                "failover reconciliation paused after controlled Authorize"
                in meta.log_tail(lines=500) for meta in self.metas))
        return True

    def wait_post_prepared_pause(self, meta=None):
        """Wait until one exact CandidatePrepared reaches the chosen leader."""
        if not self.hook_available:
            H.log("SKIP deterministic prepared-observation assertions: "
                  "ordinary Release erases the failover pause hook")
            return False
        nodes = self.metas if meta is None else (meta,)
        H.wait_until(
            "failover reconciler observes exact CandidatePrepared", 30,
            lambda: any(
                "failover reconciliation paused after exact "
                "CandidatePrepared observation"
                in node.log_tail(lines=500) for node in nodes))
        return True

    def require_expected_processes_alive(self, *, dead_meta_ids=(),
                                         dead_data_ids=()):
        """Do not let an incidental process exit masquerade as a gate pass."""
        unexpected = []
        for meta in self.metas:
            if meta.id not in dead_meta_ids and not meta.alive():
                code = None if meta.proc is None else meta.proc.returncode
                unexpected.append(f"Meta {meta.id} exit={code}")
        for data in self.data_nodes:
            if data.node_id not in dead_data_ids and not data.alive():
                code = None if data.proc is None else data.proc.returncode
                unexpected.append(f"Data {data.node_id[:8]} exit={code}")
        if unexpected:
            raise H.Failure("unexpected process exit: " + ", ".join(unexpected))

    def partition_owner_control(self, direction="both"):
        if not self.control_proxies:
            raise H.Failure("fixture has no Data-control partition proxies")
        for proxy in self.control_proxies:
            proxy.drop_blocked(direction)

    def heal_owner_control(self):
        for proxy in self.control_proxies:
            proxy.heal()

    def clean_shutdown(self):
        for data in self.data_nodes:
            data.terminate()
        for meta in self.metas:
            meta.terminate()

    def dump_logs(self):
        if (self.operation_id is not None and
                any(meta.alive() for meta in self.metas)):
            try:
                operation = self.getop(
                    self.operation_id, time.monotonic() + 2)
                H.log("retained failover operation: " + operation)
            except H.Failure as error:
                H.log(f"retained failover operation unavailable: {error}")
        H.dump_node_logs(self.metas, lines=250)
        for data in self.data_nodes:
            print(f"--- Data log tail ({data.log_path}) ---", file=sys.stderr)
            print(data.log_tail(lines=300), file=sys.stderr)

    def force_kill(self):
        for data in self.data_nodes:
            data.force_kill()
        for meta in self.metas:
            meta.force_kill()
        for proxy in self.control_proxies:
            proxy.close()
        if self.socket_directory is not None:
            self.socket_directory.cleanup()
            self.socket_directory = None


def run_controlled(meta_binary, data_binary, ctl, redis_cli, workdir,
                   require_fault_hook):
    del redis_cli  # RESP is driven directly so errors remain inspectable.
    fixture = FailoverFixture(
        meta_binary, data_binary, ctl, os.path.join(workdir, "controlled"),
        # Exercise owner-local history reset, quota and coverage collection
        # across workers through two successive partial reparent operations.
        require_fault_hook, data_workers=2)
    read_probe = None
    try:
        fixture.start_created()
        key = "{failover-gate}key"
        initial = "before-cutover"
        fixture.seed_and_wait_for_replicas(
            key, initial, (CANDIDATE, FOLLOWER))
        full_admissions = {
            node.node_id: open(node.log_path, encoding="utf-8").read().count(
                "durably invalidated system state")
            for node in fixture.data_nodes
        }
        if fixture.hook_available:
            read_probe = ContinuousGetProbe(
                fixture.by_id[OWNER], key, initial)
            read_probe.start()
        operation_id = fixture.submit_failover()
        successor = None

        if fixture.wait_post_begin_pause():
            begin = require_unique_failover_event(
                fixture.metas, "begin", "controlled", loss="none")
            successor = begin["candidate"]
            if successor not in (CANDIDATE, FOLLOWER):
                raise H.Failure(
                    f"controlled Begin selected an invalid candidate: {begin}")

            def paused_owner_rejects_mutations():
                try:
                    return redis_error(
                        fixture.by_id[OWNER], ["SET", key, initial]
                    ).startswith("TRYAGAIN")
                except H.Failure:
                    return False

            H.wait_until(
                "old Owner enters committed mutation pause", 7,
                paused_owner_rejects_mutations)
            read_probe.stop_and_assert()
            read_probe = None
            if fixture.getop(
                    operation_id, time.monotonic() + 5) != "OK running":
                raise H.Failure(
                    "durable operation was not Running at the pause cut")
            H.log("controlled pause: continuous GET had no gap and SET "
                  "returned TRYAGAIN")

        if successor is None:
            begin = require_unique_failover_event(
                fixture.metas, "begin", "controlled", loss="none")
            successor = begin["candidate"]
        wait_owner(fixture, successor, fixture.data_nodes)
        post_cutover = "after-cutover"
        if (redis_call(fixture.by_id[successor],
                       ["SET", key, post_cutover]) != "OK" or
                redis_call(fixture.by_id[successor],
                           ["GET", key]) != post_cutover):
            raise H.Failure("new Owner did not serve the post-cutover write")
        for follower_id in (
                node_id for node_id in (OWNER, CANDIDATE, FOLLOWER)
                if node_id != successor):
            H.wait_until(
                f"non-Owner {follower_id[:8]} follows the new Owner", 30,
                lambda follower_id=follower_id:
                readonly_get(fixture.by_id[follower_id], key) == post_cutover)

        wait_operation(
            fixture, operation_id, "OK completed failover-completed",
            "controlled operation reaches its durable terminal result")
        events = parse_failover_log(fixture.metas)
        H.log("controlled cutover: new Owner wrote; old Owner and peer "
              "followed; operation terminal; structured events=" +
              ",".join(sorted(events)))

        # A successful cutover deliberately leaves its action id on the new
        # owner's grant. The next transition must treat that id as authority
        # provenance for the current owner, not as a pending activation on its
        # newly selected candidate.
        # Local reparent/data visibility can precede its next Meta heartbeat.
        # The second failover needs advertised evidence in the new source domain.
        wait_candidate_source(
            fixture, successor, 2,
            tuple(node.node_id for node in fixture.data_nodes
                  if node.node_id != successor))
        second_operation_id = fixture.submit_failover()
        if second_operation_id == operation_id:
            raise H.Failure("second controlled failover reused operation id")
        wait_operation(
            fixture, second_operation_id,
            "OK completed failover-completed",
            "second controlled operation reaches its durable terminal result",
            timeout=30)

        second_owner = None
        latest = None
        deadline = time.monotonic() + 90

        def second_cutover_converged():
            nonlocal latest, second_owner
            latest = fixture.cluster_status(deadline)
            groups = {item.get("group_id"): item
                      for item in latest.get("groups", [])}
            members = {item.get("node_id"): item
                       for item in latest.get("data_nodes", [])}
            group = groups.get(GROUP, {})
            second_owner = group.get("owner_node_id")
            return (latest.get("result") == "ready" and
                    group.get("term") == "3" and
                    second_owner in (OWNER, CANDIDATE, FOLLOWER) and
                    second_owner != successor and
                    all(members.get(node.node_id, {}).get("current_session")
                        and members.get(node.node_id, {}).get(
                            "projection_current")
                        for node in fixture.data_nodes))

        try:
            H.wait_until(
                f"{GROUP} reaches a second cutover and all Data nodes "
                "converge", 90, second_cutover_converged)
        except H.Failure as error:
            raise H.Failure(f"{error}; status={latest}") from error

        repeated_value = "after-second-cutover"
        if (redis_call(fixture.by_id[second_owner],
                       ["SET", key, repeated_value]) != "OK" or
                redis_call(fixture.by_id[second_owner],
                           ["GET", key]) != repeated_value):
            raise H.Failure(
                "second controlled failover Owner did not serve writes")
        for follower_id in (
                node_id for node_id in (OWNER, CANDIDATE, FOLLOWER)
                if node_id != second_owner):
            H.wait_until(
                f"non-Owner {follower_id[:8]} follows the second Owner", 30,
                lambda follower_id=follower_id:
                readonly_get(fixture.by_id[follower_id], key) ==
                repeated_value)
        for node in fixture.data_nodes:
            with open(node.log_path, encoding="utf-8") as log:
                current_fulls = log.read().count("durably invalidated system state")
            if current_fulls != full_admissions[node.node_id]:
                raise H.Failure(
                    f"compatible reparent destructively rebuilt {node.node_id[:8]}")
        H.log("repeated controlled cutover: term 3 served and every follower "
              "converged through partial reparent without another FULL")
        fixture.require_expected_processes_alive()
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        if read_probe is not None:
            read_probe.stop()
        fixture.force_kill()


def run_full_fallback(meta_binary, data_binary, ctl, redis_cli, workdir,
                      require_fault_hook):
    """A complete replica missing the direct parent completes destructive FULL."""
    del redis_cli
    fixture = FailoverFixture(
        meta_binary, data_binary, ctl, os.path.join(workdir, "full-fallback"),
        require_fault_hook, pause_after_begin_ms=1)
    laggard = fixture.by_id[FOLLOWER]
    paused = False
    try:
        fixture.start_created()
        key = "{failover-gate}full-fallback"
        fixture.seed_and_wait_for_replicas(key, "initial", (CANDIDATE, FOLLOWER))
        with open(laggard.log_path, encoding="utf-8") as log:
            initial_fulls = log.read().count("durably invalidated system state")
        laggard.proc.send_signal(signal.SIGSTOP)
        paused = True

        def laggard_session_expired():
            status = fixture.cluster_status(time.monotonic() + 5)
            return any(node.get("node_id") == FOLLOWER and
                       not node.get("current_session")
                       for node in status.get("data_nodes", []))

        H.wait_until("paused replica loses its Meta session", 30,
                     laggard_session_expired)
        owner = OWNER
        for term in (2, 3):
            operation = fixture.submit_failover()
            wait_operation(
                fixture, operation, "OK completed failover-completed",
                f"controlled cutover reaches term {term}", timeout=30)
            owner = CANDIDATE if owner == OWNER else OWNER

            def owner_serving():
                status = fixture.cluster_status(time.monotonic() + 5)
                return any(group.get("group_id") == GROUP and
                           group.get("term") == str(term) and
                           group.get("owner_node_id") == owner and
                           group.get("serving_ready")
                           for group in status.get("groups", []))

            H.wait_until(f"term {term} Owner serves", 30, owner_serving)
            value = f"term-{term}"
            if redis_call(fixture.by_id[owner], ["SET", key, value]) != "OK":
                raise H.Failure("new Owner rejected a post-cutover write")
            peer = CANDIDATE if owner == OWNER else OWNER
            H.wait_until(
                "live peer completes reparent before the next cutover", 30,
                lambda: readonly_get(fixture.by_id[peer], key) == value)

            # Local replay can finish before its next Meta heartbeat. The
            # next controlled transition needs a candidate in the new source
            # domain, not merely a preserved parent-domain Ready population.
            wait_candidate_source(fixture, owner, term, (peer,))

        # Only the immediate parent bridge survives. The paused replica still
        # owns term-1 Active and must now rebuild from the term-3 Owner.
        laggard.proc.send_signal(signal.SIGCONT)
        paused = False
        H.wait_until("old complete replica finishes FULL fallback", 45,
                     lambda: readonly_get(laggard, key) == "term-3")
        with open(laggard.log_path, encoding="utf-8") as log:
            final_fulls = log.read().count("durably invalidated system state")
        if final_fulls <= initial_fulls:
            raise H.Failure("missing-parent replica did not admit destructive FULL")
        if redis_call(fixture.by_id[owner], ["SET", key, "after-full"]) != "OK":
            raise H.Failure("Owner rejected the post-FULL write")
        H.wait_until("rebuilt replica follows subsequent writes", 20,
                     lambda: readonly_get(laggard, key) == "after-full")
        wait_ready(fixture, "FULL fallback restores cluster readiness")
        fixture.require_expected_processes_alive()
        H.log("trusted Active with missing parent completed FULL and resumed FOLLOW")
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        if paused and laggard.alive():
            laggard.proc.send_signal(signal.SIGCONT)
        fixture.force_kill()


def run_leader_resume(meta_binary, data_binary, ctl, redis_cli, workdir,
                      require_fault_hook):
    """Resume one committed transition after its Meta leader is killed."""
    del redis_cli
    fixture = FailoverFixture(
        meta_binary, data_binary, ctl,
        os.path.join(workdir, "leader-resume"), require_fault_hook)
    try:
        fixture.start_created()
        key = "{failover-leader-resume}key"
        initial = "before-leader-change"
        fixture.seed_and_wait_for_replicas(
            key, initial, (CANDIDATE, FOLLOWER))
        operation_id = fixture.submit_failover()
        if not fixture.wait_post_begin_pause():
            raise H.Failure(
                "leader-resume requires the deterministic post-Begin cut")
        begin = require_unique_failover_event(
            fixture.metas, "begin", "controlled", loss="none")
        successor = begin["candidate"]
        if successor not in (CANDIDATE, FOLLOWER):
            raise H.Failure(
                f"controlled Begin selected an invalid candidate: {begin}")

        old_leader = fixture.leader
        if fixture.getop(
                operation_id, time.monotonic() + 5) != "OK running":
            raise H.Failure(
                "operation was not Running before the Meta leader failure")
        old_leader.kill9()
        fixture.leader = H.find_leader(
            fixture.metas, timeout=20, exclude=(old_leader.id,))
        wait_operation(
            fixture, operation_id, "OK running",
            "replacement Meta leader restores the committed operation",
            timeout=10)

        wait_owner(fixture, successor, fixture.data_nodes)
        post_cutover = "after-leader-change"
        if redis_call(fixture.by_id[successor],
                      ["SET", key, post_cutover]) != "OK":
            raise H.Failure(
                "new Owner rejected the write after Meta leader recovery")
        for follower_id in (
                node_id for node_id in (OWNER, CANDIDATE, FOLLOWER)
                if node_id != successor):
            H.wait_until(
                f"non-Owner {follower_id[:8]} follows after leader change",
                30, lambda follower_id=follower_id:
                readonly_get(fixture.by_id[follower_id], key) == post_cutover)
        wait_operation(
            fixture, operation_id, "OK completed failover-completed",
            "resumed operation reaches its durable terminal result")
        events = parse_failover_log(fixture.metas)
        H.log(f"Meta leader {old_leader.id} failed after Begin; leader "
              f"{fixture.leader.id} resumed operation {operation_id}; "
              "structured events=" + ",".join(sorted(events)))
        fixture.require_expected_processes_alive(
            dead_meta_ids=(old_leader.id,))
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        fixture.force_kill()


def run_prepared_leader_resume(meta_binary, data_binary, ctl, redis_cli,
                               workdir, require_fault_hook):
    """Re-report one prepared action after its observing Meta leader dies."""
    del redis_cli
    fixture = FailoverFixture(
        meta_binary, data_binary, ctl,
        os.path.join(workdir, "prepared-leader-resume"), require_fault_hook,
        pause_after_begin_ms=None, pause_after_prepared_ms=8_000)
    try:
        fixture.start_created()
        key = "{failover-prepared-leader-resume}key"
        initial = "before-prepared-leader-change"
        fixture.seed_and_wait_for_replicas(
            key, initial, (CANDIDATE, FOLLOWER))
        source_history = replication_info_fields(
            fixture.by_id[OWNER]).get("master_replid")
        if source_history is None or len(source_history) != 40:
            raise H.Failure(
                f"old Owner omitted its source history: {source_history!r}")
        operation_id = fixture.submit_failover()
        if not fixture.wait_post_prepared_pause():
            raise H.Failure(
                "prepared-leader-resume requires the deterministic exact "
                "CandidatePrepared cut")

        old_leader = fixture.rediscover_leader(time.monotonic() + 5)
        if not fixture.wait_post_prepared_pause(old_leader):
            raise H.Failure(
                "current Meta leader did not own the prepared-observation cut")
        begin = require_unique_failover_event(
            fixture.metas, "begin", "controlled", loss="none")
        authorize = require_unique_failover_event(
            fixture.metas, "authorize", "controlled", loss="none")
        successor = begin["candidate"]
        if (successor not in (CANDIDATE, FOLLOWER) or
                (begin["transition"], begin["action"]) !=
                (authorize["transition"], authorize["action"])):
            raise H.Failure(
                "prepared cut did not retain the exact authorized Begin "
                f"action: {begin}, {authorize}")
        if fixture.getop(
                operation_id, time.monotonic() + 5) != "OK running":
            raise H.Failure(
                "operation was not Running while CandidatePrepared was "
                "leader-local")
        candidate = fixture.by_id[successor]
        prepared_info = replication_info_fields(candidate)
        prepared_history = prepared_info.get("master_replid")
        if (prepared_info.get("lavik_replication_state") != "syncing" or
                prepared_history is None or len(prepared_history) != 40 or
                prepared_history == source_history):
            raise H.Failure(
                "CandidatePrepared did not expose one fenced child history: "
                f"source={source_history!r} prepared={prepared_info}")
        candidate_fds_before = candidate.metric(
            "lavik_cluster_control_full_states_applied_total")

        old_leader.kill9()
        survivors = [meta for meta in fixture.metas
                     if meta.id != old_leader.id]
        fixture.leader = H.find_leader(survivors, timeout=20)
        candidate.wait_metric(
            "lavik_cluster_control_full_states_applied_total",
            lambda value: value > candidate_fds_before,
            "prepared candidate installs replacement-leader FDS", timeout=25)
        candidate.wait_metric(
            "lavik_cluster_control_connected", lambda value: value == 1,
            "prepared candidate reconnects to replacement Meta", timeout=10)
        # The replacement has a fresh ObservationStore. Reaching this hook on
        # that exact process therefore proves the still-running Data process
        # bridged its retained child history back into CandidatePrepared for
        # the same committed action; no process-local Meta phase was resumed.
        if not fixture.wait_post_prepared_pause(fixture.leader):
            raise H.Failure(
                "replacement Meta leader did not re-observe "
                "CandidatePrepared")
        if fixture.getop(
                operation_id, time.monotonic() + 5) != "OK running":
            raise H.Failure(
                "replacement Meta leader did not restore the Running "
                "operation at the prepared cut")
        resumed_info = replication_info_fields(candidate)
        if (resumed_info.get("lavik_replication_state") != "syncing" or
                resumed_info.get("master_replid") != prepared_history):
            raise H.Failure(
                "replacement Meta session did not preserve the exact "
                f"prepared child history: before={prepared_info}, "
                f"after={resumed_info}")

        wait_owner(fixture, successor, fixture.data_nodes)
        owner_info = replication_info_fields(candidate)
        if (owner_info.get("lavik_replication_state") != "master" or
                owner_info.get("master_replid") != prepared_history):
            raise H.Failure(
                "cutover did not activate the re-reported child history: "
                f"prepared={prepared_info}, owner={owner_info}")
        post_cutover = "after-prepared-leader-change"
        if redis_call(fixture.by_id[successor],
                      ["SET", key, post_cutover]) != "OK":
            raise H.Failure(
                "new Owner rejected the write after prepared-action resume")
        for follower_id in (
                node_id for node_id in (OWNER, CANDIDATE, FOLLOWER)
                if node_id != successor):
            H.wait_until(
                f"non-Owner {follower_id[:8]} follows after prepared resume",
                30, lambda follower_id=follower_id:
                readonly_get(fixture.by_id[follower_id], key) == post_cutover)
        wait_operation(
            fixture, operation_id, "OK completed failover-completed",
            "prepared-action operation reaches its durable terminal result")
        events = parse_failover_log(fixture.metas)
        if {(event["transition"], event["action"])
                for event in events.values()} != {
                    (begin["transition"], begin["action"])}:
            raise H.Failure(
                "prepared-action resume changed transition/action identity: "
                f"{events}")
        fixture.require_expected_processes_alive(
            dead_meta_ids=(old_leader.id,))
        H.log(f"Meta leader {old_leader.id} observed CandidatePrepared and "
              f"failed; leader {fixture.leader.id} collected it again from "
              f"the live Data child history and committed the same action "
              f"for operation {operation_id}")
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        fixture.force_kill()


def run_live_leader_demotion(meta_binary, data_binary, ctl, redis_cli, workdir,
                             require_fault_hook):
    """Resume a committed transition after a live Meta leader steps down."""
    del redis_cli
    fixture = FailoverFixture(
        meta_binary, data_binary, ctl,
        os.path.join(workdir, "live-leader-demotion"), require_fault_hook)
    try:
        fixture.start_created()
        key = "{failover-live-leader-demotion}key"
        initial = "before-live-leader-demotion"
        fixture.seed_and_wait_for_replicas(
            key, initial, (CANDIDATE, FOLLOWER))
        operation_id = fixture.submit_failover()
        if not fixture.wait_post_begin_pause():
            raise H.Failure(
                "live-leader-demotion requires the deterministic post-Begin "
                "cut")
        begin = require_unique_failover_event(
            fixture.metas, "begin", "controlled", loss="none")
        successor = begin["candidate"]
        if successor not in (CANDIDATE, FOLLOWER):
            raise H.Failure(
                f"controlled Begin selected an invalid candidate: {begin}")

        old_leader = fixture.leader
        followers = [meta for meta in fixture.metas
                     if meta is not old_leader]
        begin_index = int(begin["index"])
        for follower in followers:
            follower.wait_committed(begin_index, timeout=10)
        for data in fixture.data_nodes:
            data.wait_metric(
                "lavik_cluster_control_connected", lambda value: value == 1,
                f"Data {data.node_id[:8]} has a live leader session",
                timeout=10)
        fds_before = {
            data.node_id: data.metric(
                "lavik_cluster_control_full_states_applied_total")
            for data in fixture.data_nodes
        }
        follower_events_before = old_leader.log_tail(lines=2000).count(
            "[raft-cb] event=BecomeFollower")

        # Freeze the quorum rather than killing the leader. NuRaft must revoke
        # its live leadership, and the leader-scoped Data publisher must close
        # and drain every authority session before CancelAndWait returns.
        for follower in followers:
            follower.pause()
        H.wait_until(
            "isolated live Meta leader steps down", 8,
            lambda: old_leader.alive() and not old_leader.is_leader())
        if not old_leader.alive():
            raise H.Failure("old Meta leader exited instead of stepping down")
        H.wait_until(
            "live Meta leader delivers its BecomeFollower callback", 5,
            lambda: old_leader.log_tail(lines=2000).count(
                "[raft-cb] event=BecomeFollower") > follower_events_before)
        for data in fixture.data_nodes:
            data.wait_metric(
                "lavik_cluster_control_connected", lambda value: value == 0,
                f"Data {data.node_id[:8]} loses the demoted Meta session",
                timeout=10)

        # The lifecycle assertion above requires the demoted process to remain
        # alive through authority-session drain. Retire it only afterwards so
        # it cannot re-enter the election and so Data does not spend its full
        # handshake timeout on a deliberately SIGSTOPed stale endpoint.
        old_leader.kill9()
        for follower in followers:
            follower.resume()
        fixture.leader = H.find_leader(followers, timeout=20)
        if fixture.getop(
                operation_id, time.monotonic() + 5) != "OK running":
            raise H.Failure(
                "replacement Meta leader did not restore the running "
                "operation")
        for data in fixture.data_nodes:
            data.wait_metric(
                "lavik_cluster_control_full_states_applied_total",
                lambda value, data=data: value > fds_before[data.node_id],
                f"Data {data.node_id[:8]} installs replacement-leader FDS",
                timeout=25)
            data.wait_metric(
                "lavik_cluster_control_connected", lambda value: value == 1,
                f"Data {data.node_id[:8]} reconnects to replacement Meta",
                timeout=10)

        wait_owner(fixture, successor, fixture.data_nodes)
        post_cutover = "after-live-leader-demotion"
        if redis_call(fixture.by_id[successor],
                      ["SET", key, post_cutover]) != "OK":
            raise H.Failure(
                "new Owner rejected the write after live Meta demotion")
        for follower_id in (
                node_id for node_id in (OWNER, CANDIDATE, FOLLOWER)
                if node_id != successor):
            H.wait_until(
                f"non-Owner {follower_id[:8]} follows after live demotion",
                30, lambda follower_id=follower_id:
                readonly_get(fixture.by_id[follower_id], key) == post_cutover)
        wait_operation(
            fixture, operation_id, "OK completed failover-completed",
            "resumed operation reaches its durable terminal result")
        events = parse_failover_log(followers)
        if (events["begin"]["transition"] != begin["transition"] or
                events["begin"]["action"] != begin["action"]):
            raise H.Failure(
                "replacement Meta leader resumed a different transition: "
                f"before={begin}; after={events}")

        fixture.require_expected_processes_alive(
            dead_meta_ids=(old_leader.id,))
        H.log(f"Meta leader {old_leader.id} stepped down and drained Data "
              f"sessions while alive; leader {fixture.leader.id} resumed "
              f"operation {operation_id}; structured events=" +
              ",".join(sorted(events)))
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        fixture.force_kill()


def run_candidate_abort(meta_binary, data_binary, ctl, redis_cli, workdir,
                        require_fault_hook):
    """A failed controlled candidate aborts without waiting for restart."""
    del redis_cli
    fixture = FailoverFixture(
        meta_binary, data_binary, ctl,
        os.path.join(workdir, "candidate-abort"), require_fault_hook,
        pause_after_begin_ms=2_000)
    try:
        # The peer replica deliberately stays connected when the selected
        # candidate dies. That isolates candidate failure from the existing
        # source behavior that rotates history after its final flow vanishes.
        fixture.start_created()
        key = "{failover-candidate-abort}key"
        initial = "before-candidate-failure"
        fixture.seed_and_wait_for_replicas(
            key, initial, (CANDIDATE, FOLLOWER))
        operation_id = fixture.submit_failover()
        if not fixture.wait_post_begin_pause():
            raise H.Failure(
                "candidate-abort requires the deterministic post-Begin cut")
        begin = require_unique_failover_event(
            fixture.metas, "begin", "controlled", loss="none")
        selected_candidate = begin["candidate"]
        if selected_candidate not in (CANDIDATE, FOLLOWER):
            raise H.Failure(
                f"controlled Begin selected an invalid candidate: {begin}")

        fixture.by_id[selected_candidate].force_kill()
        expected = "OK aborted controlled failover candidate became unavailable"
        wait_operation(
            fixture, operation_id, expected,
            "controlled operation aborts after candidate failure")

        def original_owner_resumes():
            try:
                return (redis_call(fixture.by_id[OWNER],
                                   ["SET", key, "after-abort"]) == "OK" and
                        redis_call(fixture.by_id[OWNER], ["GET", key]) ==
                        "after-abort")
            except H.Failure:
                return False

        H.wait_until("old Owner resumes writes after controlled abort", 15,
                     original_owner_resumes)
        status = fixture.cluster_status(time.monotonic() + 10)
        groups = {item.get("group_id"): item
                  for item in status.get("groups", [])}
        group = groups.get(GROUP, {})
        if (group.get("term") != "1" or
                group.get("owner_node_id") != OWNER):
            raise H.Failure(
                f"candidate abort changed committed ownership: {group}")
        abort = require_unique_failover_event(
            fixture.metas, "abort", "controlled", loss="none")
        if ((abort["transition"], abort["action"]) !=
                (begin["transition"], begin["action"]) or
                abort["reason"] !=
                "controlled failover candidate became unavailable"):
            raise H.Failure(
                "candidate abort did not retain its exact Begin identity "
                f"and structured reason: {begin}, {abort}")
        H.log(f"exact Begin candidate {selected_candidate[:8]} failed; "
              "controlled operation aborted; term-1 Owner resumed writes "
              "without candidate restart")
        fixture.require_expected_processes_alive(
            dead_data_ids=(selected_candidate,))
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        fixture.force_kill()


def run_lease_fence(meta_binary, data_binary, ctl, redis_cli, workdir,
                    require_fault_hook):
    """Blackhole only old-Owner control traffic and prove lease fencing."""
    del redis_cli
    fixture = FailoverFixture(
        meta_binary, data_binary, ctl,
        os.path.join(workdir, "lease-fence"), require_fault_hook,
        pause_after_begin_ms=None, pause_after_authorize_ms=40_000,
        proxy_data_control=True)
    old_write_probe = None
    replica_write_probes = {}
    try:
        fixture.start_created()
        key = "{failover-lease-fence}key"
        initial = "before-control-partition"
        fixture.seed_and_wait_for_replicas(
            key, initial, (CANDIDATE, FOLLOWER))
        old_write_probe = ContinuousSetProbe(
            fixture.by_id[OWNER], "{failover-lease-fence}old-probe",
            "old-owner")
        replica_write_probes = {
            node_id: ContinuousSetProbe(
                fixture.by_id[node_id],
                f"{{failover-lease-fence}}replica-{node_id[:8]}",
                f"replica-{node_id[:8]}")
            for node_id in (CANDIDATE, FOLLOWER)
        }
        old_write_probe.start()
        for probe in replica_write_probes.values():
            probe.start()
        old_write_probe.wait_for_success(
            "old Owner write probe succeeds before failover", 5)
        for probe in replica_write_probes.values():
            probe.assert_healthy()
            if probe.successes():
                raise H.Failure(
                    "replica write probe succeeded before failover")

        lease_metric = "lavik_cluster_control_lease_expirations_total"
        expirations_before = fixture.by_id[OWNER].metric(lease_metric)
        operation_id = fixture.submit_failover()
        if not fixture.wait_post_authorize_pause():
            raise H.Failure(
                "lease-fence requires the deterministic post-Authorize cut")
        begin = require_unique_failover_event(
            fixture.metas, "begin", "controlled", loss="none")
        authorize = require_unique_failover_event(
            fixture.metas, "authorize", "controlled", loss="none")
        successor = begin["candidate"]
        if (successor not in replica_write_probes or
                (begin["transition"], begin["action"]) !=
                (authorize["transition"], authorize["action"])):
            raise H.Failure(
                "controlled authorization did not preserve the exact "
                f"committed Begin candidate/action: {begin}, {authorize}")
        new_write_probe = replica_write_probes[successor]

        fixture.partition_owner_control()
        if not fixture.by_id[OWNER].alive():
            raise H.Failure(
                "old Owner exited when its Meta control plane was blackholed")

        # The deterministic cut must outlast both half-open session detection
        # and the configured source grace. Observe that boundary explicitly so
        # this gate cannot accidentally exercise an ordinary controlled
        # cutover when either timeout changes.
        source_loss_deadline = time.monotonic() + 35

        def source_loss_observed():
            status = fixture.cluster_status(source_loss_deadline)
            source = next(
                (node for node in status.get("data_nodes", [])
                 if node.get("node_id") == OWNER), {})
            return (not source.get("current_session") and
                    not source.get("health_fresh"))

        H.wait_until(
            "old Owner Meta session and observation grace expire", 35,
            source_loss_observed)
        wait_serving_owner(
            fixture, successor,
            tuple(fixture.by_id[node_id]
                  for node_id in (CANDIDATE, FOLLOWER)))
        new_write_probe.wait_for_success(
            "new Owner write probe succeeds after cutover", 10)
        fixture.by_id[OWNER].wait_metric(
            lease_metric, lambda value: value > expirations_before,
            "partitioned old Owner detects finite lease expiry", timeout=15)
        if not fixture.by_id[OWNER].alive():
            raise H.Failure(
                "old Owner exited before its finite lease expired")
        # Keep both clients active beyond the first successor success and the
        # old finite-lease expiry. A sequential pair of SETs cannot detect an
        # authority overlap that existed briefly during the handoff.
        time.sleep(1.0)
        old_write_probe.stop_and_assert()
        for probe in replica_write_probes.values():
            probe.stop_and_assert()
        old_successes = old_write_probe.successes()
        new_successes = new_write_probe.successes()
        if not old_successes or not new_successes:
            raise H.Failure(
                "dual-write probes did not both observe their serving epoch")
        last_old_completed = max(completed for _, completed in old_successes)
        first_new_started = min(started for started, _ in new_successes)
        late_old_successes = [
            interval for interval in old_successes
            if interval[1] >= first_new_started]
        if late_old_successes:
            raise H.Failure(
                "old and new Owner successful SET intervals overlapped: "
                f"old_completed={last_old_completed} "
                f"new_started={first_new_started}")
        losing_successes = {
            node_id: probe.successes()
            for node_id, probe in replica_write_probes.items()
            if node_id != successor and probe.successes()
        }
        if losing_successes:
            raise H.Failure(
                "non-winning replica accepted writes during failover: "
                f"{losing_successes}")
        handoff_gap_ms = (first_new_started - last_old_completed) / 1_000_000
        H.log("dual-write probe: "
              f"successor={successor} "
              f"old_successes={len(old_successes)} "
              f"new_successes={len(new_successes)} "
              f"non_overlap_gap_ms={handoff_gap_ms:.3f}")

        post_cutover = "new-owner-only"
        if redis_call(fixture.by_id[successor],
                      ["SET", key, post_cutover]) != "OK":
            raise H.Failure("new Owner lacked authority during partition")
        if redis_call(fixture.by_id[OWNER], ["PING"]) != "PONG":
            raise H.Failure("partition unexpectedly stopped old Redis port")
        old_rejection = redis_error(
            fixture.by_id[OWNER], ["SET", key, "stale-owner-write"])
        if not old_rejection.startswith(("CLUSTERDOWN", "TRYAGAIN", "MOVED")):
            raise H.Failure(
                f"old Owner returned an unexpected fence: {old_rejection}")
        if redis_call(fixture.by_id[successor], ["GET", key]) != post_cutover:
            raise H.Failure("rejected stale write changed new Owner data")

        expected = "OK aborted controlled failover source became unavailable"
        wait_operation(
            fixture, operation_id, expected,
            "controlled operation records source-loss degradation")
        degrade = require_unique_failover_event(
            fixture.metas, "degrade", "uncontrolled", loss="none")
        cutover = require_unique_failover_event(
            fixture.metas, "cutover", "uncontrolled", loss="none")
        expected_identity = (begin["transition"], begin["action"])
        if (degrade["reason"] !=
                "controlled failover source became unavailable" or
                (degrade["transition"], degrade["action"]) !=
                expected_identity or
                (cutover["transition"], cutover["action"]) !=
                expected_identity or cutover["candidate"] != successor):
            raise H.Failure(
                "lease partition failed to retain the exact authorized "
                f"Begin action through degrade/cutover: {begin}, "
                f"{degrade}, {cutover}")
        _, records = failover_log_records(fixture.metas)
        reselections = [
            record for record in records
            if record["transition"] == begin["transition"] and
            record["event"] in ("candidate-selected", "candidate-replaced",
                                "candidate-cleared", "domain-fallback")
        ]
        if reselections:
            raise H.Failure(
                "authorized candidate was unexpectedly reselected: "
                f"{reselections}")

        fixture.heal_owner_control()
        wait_owner(fixture, successor, fixture.data_nodes)
        H.wait_until(
            "healed old Owner follows the new Owner", 30,
            lambda: readonly_get(fixture.by_id[OWNER], key) == post_cutover)
        fixture.require_expected_processes_alive()
        H.log("old Owner stayed live behind a Meta-control blackhole, its "
              f"finite lease expired, stale SET was rejected as "
              f"{old_rejection!r}, exact authorized successor "
              f"{successor[:8]} cut over, and heal converged it to a follower")
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        if old_write_probe is not None:
            old_write_probe.stop()
        for probe in replica_write_probes.values():
            probe.stop()
        fixture.force_kill()


def run_source_degrade_reselect(meta_binary, data_binary, ctl, redis_cli,
                                workdir, require_fault_hook):
    """Source loss wins over candidate loss and reselects a live replica."""
    del redis_cli
    fixture = FailoverFixture(
        meta_binary, data_binary, ctl,
        os.path.join(workdir, "source-degrade-reselect"),
        require_fault_hook, pause_after_begin_ms=40_000,
        proxy_data_control=True)
    try:
        fixture.start_created()
        key = "{failover-source-degrade}key"
        initial = "before-source-and-candidate-loss"
        fixture.seed_and_wait_for_replicas(
            key, initial, (CANDIDATE, FOLLOWER))
        operation_id = fixture.submit_failover()
        if not fixture.wait_post_begin_pause():
            raise H.Failure(
                "source-degrade-reselect requires the deterministic "
                "post-Begin cut")
        begin = require_unique_failover_event(
            fixture.metas, "begin", "controlled", loss="none")
        selected_candidate = begin["candidate"]
        if selected_candidate not in (CANDIDATE, FOLLOWER):
            raise H.Failure(
                "committed Begin did not identify one replica candidate: "
                f"{begin}")
        successor = (FOLLOWER if selected_candidate == CANDIDATE
                     else CANDIDATE)

        # The source remains alive and available on the replication/data
        # planes. Only its Meta session is lost. Wait for the committed
        # policy's observation grace to expire before terminating the selected
        # candidate: otherwise candidate disconnect can legitimately win the
        # race and abort a still-controlled operation. Once source loss is an
        # established planner fact, the durable transition must degrade and
        # then choose the remaining live replica instead of waiting for the
        # original candidate to restart.
        fixture.partition_owner_control()
        source_loss_deadline = time.monotonic() + 35
        source_status = None

        def source_loss_observed():
            nonlocal source_status
            source_status = fixture.cluster_status(source_loss_deadline)
            source = next(
                (node for node in source_status.get("data_nodes", [])
                 if node.get("node_id") == OWNER), {})
            return (not source.get("current_session") and
                    not source.get("health_fresh"))

        H.wait_until(
            "old source Meta session and observation grace expire", 35,
            source_loss_observed)
        fixture.by_id[selected_candidate].force_kill()
        if redis_call(fixture.by_id[OWNER], ["PING"]) != "PONG":
            raise H.Failure(
                "source control partition unexpectedly killed its data plane")

        wait_serving_owner(
            fixture, successor, (fixture.by_id[successor],))
        expected = "OK aborted controlled failover source became unavailable"
        wait_operation(
            fixture, operation_id, expected,
            "controlled operation records source-loss degradation")

        post_cutover = "after-reselection"
        if redis_call(fixture.by_id[successor],
                      ["SET", key, post_cutover]) != "OK":
            raise H.Failure(
                "reselected live replica rejected the post-cutover write")
        degrade = require_unique_failover_event(
            fixture.metas, "degrade", "uncontrolled", loss="unknown")
        selected = require_unique_failover_event(
            fixture.metas, "candidate-selected", "uncontrolled",
            loss="unknown")
        cutover = require_unique_failover_event(
            fixture.metas, "cutover", "uncontrolled", loss="unknown")
        begin_identity = (begin["transition"], begin["action"])
        selected_identity = (selected["transition"], selected["action"])
        if (degrade["reason"] !=
                "controlled failover source became unavailable" or
                (degrade["transition"], degrade["action"]) !=
                begin_identity or selected["transition"] !=
                begin["transition"] or selected["action"] ==
                begin["action"] or selected["candidate"] != successor or
                (cutover["transition"], cutover["action"]) !=
                selected_identity or cutover["candidate"] != successor):
            raise H.Failure(
                "source-loss reselection did not retain the transition and "
                f"replace the exact Begin action: {begin}, {degrade}, "
                f"{selected}, {cutover}")

        fixture.heal_owner_control()
        wait_serving_owner(
            fixture, successor,
            (fixture.by_id[OWNER], fixture.by_id[successor]))
        H.wait_until(
            "healed source follows the reselected Owner", 30,
            lambda: readonly_get(fixture.by_id[OWNER], key) == post_cutover)
        fixture.require_expected_processes_alive(
            dead_data_ids=(selected_candidate,))
        H.log("source control session and exact committed Begin candidate "
              f"{selected_candidate[:8]} failed; recovery degraded, selected "
              f"{successor[:8]}, cut over, and healed the live old Owner as "
              "a follower")
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        fixture.force_kill()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("meta")
    parser.add_argument("data")
    parser.add_argument("ctl")
    parser.add_argument("redis_cli")
    parser.add_argument("workdir", nargs="?")
    parser.add_argument(
        "--case",
        choices=("controlled", "full-fallback", "leader-resume", "prepared-leader-resume",
                 "live-leader-demotion", "candidate-abort", "lease-fence",
                 "source-degrade-reselect"),
        required=True)
    parser.add_argument("--require-fault-hook", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    binaries = {
        name: os.path.abspath(getattr(args, name))
        for name in ("meta", "data", "ctl", "redis_cli")
    }
    work_argv = [sys.argv[0], binaries["meta"]]
    if args.workdir is not None:
        work_argv.append(os.path.abspath(args.workdir))
    workdir, keep = H.make_workdir(work_argv, "meta_failover_")
    started = time.monotonic()
    try:
        if args.case == "controlled":
            run_controlled(
                binaries["meta"], binaries["data"], binaries["ctl"],
                binaries["redis_cli"], workdir, args.require_fault_hook)
        elif args.case == "full-fallback":
            run_full_fallback(
                binaries["meta"], binaries["data"], binaries["ctl"],
                binaries["redis_cli"], workdir, args.require_fault_hook)
        elif args.case == "leader-resume":
            run_leader_resume(
                binaries["meta"], binaries["data"], binaries["ctl"],
                binaries["redis_cli"], workdir, args.require_fault_hook)
        elif args.case == "prepared-leader-resume":
            run_prepared_leader_resume(
                binaries["meta"], binaries["data"], binaries["ctl"],
                binaries["redis_cli"], workdir, args.require_fault_hook)
        elif args.case == "live-leader-demotion":
            run_live_leader_demotion(
                binaries["meta"], binaries["data"], binaries["ctl"],
                binaries["redis_cli"], workdir, args.require_fault_hook)
        elif args.case == "candidate-abort":
            run_candidate_abort(
                binaries["meta"], binaries["data"], binaries["ctl"],
                binaries["redis_cli"], workdir, args.require_fault_hook)
        elif args.case == "lease-fence":
            run_lease_fence(
                binaries["meta"], binaries["data"], binaries["ctl"],
                binaries["redis_cli"], workdir, args.require_fault_hook)
        elif args.case == "source-degrade-reselect":
            run_source_degrade_reselect(
                binaries["meta"], binaries["data"], binaries["ctl"],
                binaries["redis_cli"], workdir, args.require_fault_hook)
        H.log(f"PASS case={args.case} in {time.monotonic() - started:.1f}s")
        return 0
    except Exception as error:  # noqa: BLE001 - retained logs are evidence
        H.log(f"FAIL case={args.case}: {error}")
        keep = True
        H.log(f"retained workdir: {workdir}")
        return 1
    finally:
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    H.set_tag("gate-failover")
    sys.exit(main())
