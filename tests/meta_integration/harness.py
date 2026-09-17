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

"""Shared harness for the lavik-meta process gates.

Stdlib-only building blocks for driving multi-node lavik-meta clusters:

- `Node`: process lifecycle (start on dynamic free ports with a mktemp
  data dir, clean SIGTERM stop, SIGKILL, SIGSTOP/SIGCONT pause/resume,
  restart reusing the same data dir), the SO_PEERCRED-authenticated Unix ctl
  client, and
  polling helpers (`wait_status` / `wait_leader` / `wait_committed`).
  Committed writes use the metadata command schema: `submitop` /
  `completeop` / `getop` wrap the ctl verbs, and `propose(value)` is the
  compound write — a SubmitOperation immediately followed by its
  CompleteOperation, so the non-terminal operation set stays tiny against
  the max_active_operations cap. Operation ids derive from
  a per-node counter (32 lowercase hex chars, unique cluster-wide via the
  node-id prefix).
- `Proxy`: a localhost TCP forwarding pair (advertised listen port ->
  real raft port) with injectable modes: normal forwarding, `drop`
  (half-open blackhole: accept the connection but never forward a single
  byte), `refuse` (listener closed, connect() fails with RST), per-chunk
  `delay` in both directions, and `heal` back to normal.
- `Mesh`: one destination-side Proxy per node so every byte of inter-node
  raft traffic crosses the destination node's proxy. NuRaft stores a
  single endpoint per server in the cluster config and all sources share
  127.0.0.1, so faults are injectable per destination node, not per
  (src, dst) pair: cutting node F's proxy models "F cannot receive"
  while F's outbound traffic still flows — a genuine asymmetric-link
  scenario, not a clean full isolation.
- `CommittedHistory` + `LoadThread`: a continuous propose workload and
  the convergence checker for the observable committed-state invariant.

Style note: this module intentionally mirrors smoke_3node.py's idioms
(Failure, log, free_port, wait_until) so the process tests read as one
family. Every gate is independently runnable and registered in ctest.
"""

import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time


class Failure(Exception):
    pass


_TAG = "meta-integration"
_ALLOCATED_PORTS = set()

AUTOMATIC_UNCONTROLLED_FAILOVER_POLICY_ID = (
    "lavik.automatic-uncontrolled-failover-v1")
AUTHORITY_LEASE_POLICY_ID = "lavik.authority-lease-v1"


def automatic_uncontrolled_failover_policy(suspect_after_ms=5000):
    """Return the strict compact JSON accepted by the registered family."""
    if (not isinstance(suspect_after_ms, int) or
            isinstance(suspect_after_ms, bool) or
            not 1000 <= suspect_after_ms <= 86_400_000):
        raise ValueError("automatic failover suspect_after_ms is out of range")
    return ("{\"kind\":\"automatic-uncontrolled-failover-v1\","
            f"\"suspect_after_ms\":{suspect_after_ms}}}")


def authority_lease_policy(duration_ms=5000):
    """Return the strict compact JSON accepted by the registered family."""
    if (not isinstance(duration_ms, int) or isinstance(duration_ms, bool) or
            not 100 <= duration_ms <= 86_400_000):
        raise ValueError("authority lease duration_ms is out of range")
    return ("{\"kind\":\"authority-lease-v1\","
            f"\"duration_ms\":{duration_ms}}}")


def set_tag(tag):
    global _TAG
    _TAG = tag


def log(msg):
    print(f"[{_TAG}] {msg}", flush=True)


def free_port():
    # The kernel may immediately return the same ephemeral port after the
    # probe socket closes. Keep allocations unique within one gate process so
    # the independently assigned Raft, Data-control, and Admin listeners do
    # not collide before their nodes start.
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        if port not in _ALLOCATED_PORTS:
            _ALLOCATED_PORTS.add(port)
            return port


def wait_until(desc, timeout, fn):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if fn():
                return
        except (OSError, Failure):
            pass
        time.sleep(0.05)
    raise Failure(f"timeout ({timeout}s) waiting for: {desc}")


def raft_args(snapshot_distance=30, heartbeat_ms=100, election_ms_low=300,
              election_ms_high=600, reserved_log_items=0):
    """Fast process-test timing; small snapshot distance + zero reserve
    make automatic snapshotting and compaction really fire at test scale."""
    return [
        "--heartbeat-ms", str(heartbeat_ms),
        "--election-ms-low", str(election_ms_low),
        "--election-ms-high", str(election_ms_high),
        "--snapshot-distance", str(snapshot_distance),
        "--reserved-log-items", str(reserved_log_items),
    ]


def write_initial_meta_manifest(path, members):
    """Write a complete initial-cluster manifest from Meta endpoint tuples.

    The Data/group section is deliberately inert. The production manifest is
    also the desired cluster-create topology, but most Meta process gates only
    exercise Raft and membership. Nothing persists the Data portion until an
    operator submits `cluster-create`.
    """
    lines = ["schema_version = 1", ""]
    for node_id, raft, data_control, ctl in sorted(members):
        lines.extend([
            "[[meta_members]]",
            f"id = {node_id}",
            f'raft_endpoint = "tcp://{raft}"',
            f'data_control_endpoint = "tcp://{data_control}"',
            f'ctl_endpoint = "tcp://{ctl}"',
            "",
        ])
    lines.extend([
        "[[data_nodes]]",
        'id = "ffffffffffffffffffffffffffffffffffffffff"',
        'client_endpoint = "tcp://127.0.0.1:1"',
        "",
        "[[groups]]",
        'id = "initial-meta-placeholder"',
        'primary = "ffffffffffffffffffffffffffffffffffffffff"',
        "",
        "[[slot_ranges]]",
        "first = 0",
        "last = 16383",
        'group = "initial-meta-placeholder"',
        "",
    ])
    with open(path, "w", encoding="utf-8") as output:
        output.write("\n".join(lines))


def write_initial_cluster_manifest(path, nodes, raft_endpoints=None):
    """Write one canonical genesis input shared by every initial Meta node."""
    endpoints = raft_endpoints or {node.id: node.endpoint for node in nodes}
    write_initial_meta_manifest(path, [
        (node.id, endpoints[node.id],
         getattr(node, "advertised_data_control_endpoint",
                 node.data_control_endpoint),
         getattr(node, "advertised_ctl_endpoint", node.ctl_endpoint))
        for node in nodes
    ])


class Node:
    def __init__(self, binary, workdir, node_id, args=None):
        self.binary = binary
        self.id = node_id
        self.workdir = workdir
        self.data_dir = os.path.join(workdir, f"node{node_id}")
        self.log_path = os.path.join(workdir, f"node{node_id}.log")
        self.raft_port = free_port()
        self.data_control_port = free_port()
        self.ctl_port = free_port()
        self.ctl_path = os.path.join(self.data_dir, "meta-admin.sock")
        self.args = list(args) if args is not None else raft_args()
        self.proc = None
        self.log_file = None
        self.paused = False
        # Operation-id counter: ids are 32 lowercase hex chars with the
        # node id as prefix, so two nodes can never mint the same id and a
        # restart (same Node object) keeps the sequence going.
        self._op_seq = 0

    @property
    def endpoint(self):
        return f"127.0.0.1:{self.raft_port}"

    @property
    def data_control_endpoint(self):
        return f"127.0.0.1:{self.data_control_port}"

    @property
    def ctl_endpoint(self):
        return f"127.0.0.1:{self.ctl_port}"

    def start(self, bootstrap=False, raft_port=None, wait_ready=True,
              initial_cluster_manifest=None, explicit_ctl_socket=True):
        """(Re)starts the process; the data dir is always reused, so a
        restart after kill9()/terminate() exercises WAL/snapshot replay.
        `raft_port` rebinds the raft listener (used by the mesh bootstrap;
        the advertised cluster endpoint lives in the durable config and is
        unaffected by rebinding).

        `bootstrap=True` is retained as a concise test-harness operation: on a
        pristine directory it writes a one-member initial manifest next to
        the node directory. It never replays that manifest on restart.

        `explicit_ctl_socket=False` omits the flag while retaining `ctl_path`
        as the expected default under the data directory.

        With wait_ready, start blocks until the ctl surface answers (or the
        process dies) and retries a few times: after a SIGKILL the kernel
        keeps the dead process's inbound raft connections in FIN_WAIT for
        tens of ms, during which rebinding the same port loses to
        EADDRINUSE even with SO_REUSEADDR. Instant kill/restart cycles hit
        that window routinely, so the harness absorbs it instead of
        requiring every gate to sprinkle sleeps."""
        for attempt in range(6):
            if self.alive():
                raise Failure(f"node {self.id} is already running")
            if raft_port is not None:
                self.raft_port = raft_port
            args = [
                self.binary,
                "--id", str(self.id),
                "--addr", self.endpoint,
                "--data-control-addr", self.data_control_endpoint,
                "--data-dir", self.data_dir,
                "--ctl-addr", self.ctl_endpoint,
            ] + self.args
            if explicit_ctl_socket:
                args.extend(["--ctl-socket", self.ctl_path])
            manifest = initial_cluster_manifest
            if bootstrap and manifest is not None:
                raise Failure("bootstrap and initial_cluster_manifest conflict")
            if bootstrap and not os.path.exists(
                    os.path.join(self.data_dir, "cluster_config.dat")):
                manifest = os.path.join(
                    self.workdir, f"initial-meta-{self.id}.toml")
                write_initial_cluster_manifest(manifest, [self])
            if manifest is not None:
                args.extend(["--initial-cluster-manifest", manifest])
            # Append across restarts: one file holds the node's history.
            self.log_file = open(self.log_path, "ab")
            self.proc = subprocess.Popen(
                args, stdout=self.log_file, stderr=subprocess.STDOUT)
            self.paused = False
            log(f"node {self.id} started (pid {self.proc.pid}, "
                f"raft {self.raft_port}, ctl {self.ctl_path},"
                f"{self.ctl_endpoint}, "
                f"initial_manifest={manifest is not None}, "
                f"attempt {attempt + 1})")
            if not wait_ready:
                return
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if not self.alive():
                    break
                try:
                    self.status()
                    return
                except (OSError, Failure):
                    time.sleep(0.05)
            if self.alive():
                raise Failure(
                    f"node {self.id}: ctl did not answer within 5s of start")
            self.proc.wait(timeout=5)
            self._close_log()
            log(f"node {self.id} exited during boot "
                f"(attempt {attempt + 1}); retrying after backoff")
            time.sleep(0.25)
        raise Failure(f"node {self.id} failed to start after 6 attempts")

    @property
    def pid(self):
        return self.proc.pid if self.alive() else None

    def ctl(self, command, timeout=5.0):
        """One connection per command; the server answers one line."""
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            sock.connect(self.ctl_path)
            sock.sendall(command.encode() + b"\n")
            reply = b""
            while not reply.endswith(b"\n"):
                chunk = sock.recv(65536)
                if not chunk:
                    break
                reply += chunk
        return reply.decode().strip()

    def status(self):
        reply = self.ctl("status")
        if not reply.startswith("OK "):
            raise Failure(f"node {self.id} status: {reply}")
        fields = {}
        for token in reply[3:].split():
            key, _, value = token.partition("=")
            fields[key] = value
        return fields

    def is_leader(self):
        return self.status().get("leader") == "1"

    def committed(self):
        return int(self.status()["committed"])

    def snapshot_idx(self):
        return int(self.status()["snapshot_idx"])

    def term(self):
        return int(self.status()["term"])

    def new_op_id(self):
        """Fresh operation id: 32 lowercase hex chars, unique cluster-wide
        (node-id prefix + per-node monotonic counter)."""
        self._op_seq += 1
        return f"{self.id:08x}{self._op_seq:024x}"

    @staticmethod
    def _history_id(value):
        """Canonical 160-bit replication-history id used by the ctl wire."""
        if isinstance(value, str):
            if len(value) != 40 or any(c not in "0123456789abcdef"
                                       for c in value):
                raise Failure(f"invalid replication history id: {value!r}")
            return value
        if not isinstance(value, int) or value < 0 or value >= (1 << 160):
            raise Failure(f"invalid replication history id: {value!r}")
        return f"{value:040x}"

    def submitop(self, op_id, kind, payload, timeout=5.0, history=0):
        suffix = f" {self._history_id(history)}" if history else ""
        return self.ctl(f"submitop {op_id} {kind} {payload}{suffix}",
                        timeout=timeout)

    def completeop(self, op_id, result="", timeout=5.0):
        suffix = f" {result}" if result else ""
        return self.ctl(f"completeop {op_id}{suffix}", timeout=timeout)

    def abortop(self, op_id, reason="", timeout=5.0):
        suffix = f" {reason}" if reason else ""
        return self.ctl(f"abortop {op_id}{suffix}", timeout=timeout)

    def archiveoperations(self, *seqs, timeout=5.0):
        return self.ctl("archiveoperations " + " ".join(map(str, seqs)),
                        timeout=timeout)

    def getop(self, op_id):
        return self.ctl(f"getop {op_id}")

    def registernode(self, node_id, principal, role="primary", timeout=5.0,
                     endpoints=()):
        suffix = "" if not endpoints else " " + " ".join(endpoints)
        return self.ctl(
            f"registernode {node_id} {principal} {role}{suffix}",
            timeout=timeout)

    def getnode(self, node_id):
        return self.ctl(f"getnode {node_id}")

    # -- observation-surface drivers ---------------------------------------
    # creategroup/begingroupterm/transitionop build the committed anchors
    # (group term, manifest, partition epoch, and operation history binding)
    # that observation freshness is checked against;
    # adoptsession/obs_*/observations/obsaudit drive the leader-local
    # MetaObservationStore itself.

    def creategroup(self, group_id, timeout=5.0):
        return self.ctl(f"creategroup {group_id}", timeout=timeout)

    def assignnode(self, group_id, node_id, role="primary", timeout=5.0):
        return self.ctl(f"assignnode {group_id} {node_id} {role}",
                        timeout=timeout)

    def begingroupterm(self, group_id, expected, new, timeout=5.0):
        return self.ctl(f"begingroupterm {group_id} {expected} {new}",
                        timeout=timeout)

    def putpolicy(self, policy_id, version, content, timeout=5.0):
        return self.ctl(f"putpolicy {policy_id} {version} {content}",
                        timeout=timeout)

    def put_automatic_uncontrolled_failover_policy(
            self, version, suspect_after_ms=5000,
            timeout=5.0):
        return self.putpolicy(
            AUTOMATIC_UNCONTROLLED_FAILOVER_POLICY_ID, version,
            automatic_uncontrolled_failover_policy(
                suspect_after_ms=suspect_after_ms),
            timeout=timeout)

    def put_authority_lease_policy(self, version, duration_ms=5000,
                                   timeout=5.0):
        return self.putpolicy(
            AUTHORITY_LEASE_POLICY_ID, version,
            authority_lease_policy(duration_ms), timeout=timeout)

    def getpolicy(self, policy_id):
        return self.ctl(f"getpolicy {policy_id}")

    def setslotmap(self, first, last, group_id, timeout=5.0):
        return self.ctl(
            f"setslotmap {first} {last} {group_id}",
            timeout=timeout)

    def activateauthority(self, group_id, expected_term, owner_node_id,
                          timeout=5.0):
        return self.ctl(
            f"activateauthority {group_id} {expected_term} {owner_node_id}",
            timeout=timeout)

    def fencegroup(self, group_id, expected_term, timeout=5.0):
        return self.ctl(f"fencegroup {group_id} {expected_term}",
                        timeout=timeout)

    def transitionop(self, op_id, phase, history, timeout=5.0):
        return self.ctl(
            f"transitionop {op_id} {phase} {self._history_id(history)}",
                        timeout=timeout)

    def adoptsession(self, node_id, boot_hex, generation, timeout=5.0):
        return self.ctl(f"adoptsession {node_id} {boot_hex} {generation}",
                        timeout=timeout)

    def obs_boot(self, node_id, boot_hex, generation, timeout=5.0):
        return self.ctl(f"obs boot {node_id} {boot_hex} {generation}",
                        timeout=timeout)

    def obs_health(self, node_id, boot_hex, generation, health="ok",
                   timeout=5.0):
        return self.ctl(f"obs health {node_id} {boot_hex} {generation} "
                        f"{health}", timeout=timeout)

    def obs_candidate(self, node_id, boot_hex, generation, group, term,
                      manifest, history, partition_epoch=0, timeout=5.0):
        return self.ctl(
            f"obs candidate {node_id} {boot_hex} {generation} {group} "
            f"{term} {manifest} {partition_epoch} {self._history_id(history)}",
            timeout=timeout)

    def observations(self, group=None, timeout=5.0):
        return self.ctl("observations" if group is None
                        else f"observations {group}", timeout=timeout)

    def obsaudit(self, timeout=5.0):
        return self.ctl("obsaudit", timeout=timeout)

    def propose(self, value, timeout=8.0):
        """One committed write = submitop immediately followed by
        completeop (the complete retires the operation, so the live
        non-terminal set stays tiny against max_active_operations).
        Returns (op_id, reply); the reply is the completeop line and is
        "OK <idx>" only when BOTH halves committed — a submit that landed
        while its complete failed (leader change mid-pair) has an uncertain
        outcome and is simply never recorded by the caller."""
        op_id = self.new_op_id()
        reply = self.submitop(op_id, "gate", value, timeout=timeout)
        if not reply.startswith("OK "):
            return op_id, reply
        return op_id, self.completeop(op_id, value, timeout=timeout)

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def kill9(self):
        if not self.alive():
            self._close_log()
            return
        log(f"node {self.id}: SIGKILL")
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=10)
        self.paused = False
        self._close_log()

    def terminate(self):
        if not self.alive():
            self._close_log()
            return
        self.resume()  # a SIGSTOPped process never observes SIGTERM
        self.proc.send_signal(signal.SIGTERM)
        try:
            code = self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)
            raise Failure(f"node {self.id} did not exit within 15s of SIGTERM")
        if code != 0:
            raise Failure(f"node {self.id} exited with code {code}, want 0")
        log(f"node {self.id}: clean exit 0")
        self._close_log()

    def pause(self):
        if not self.alive():
            raise Failure(f"node {self.id} not running, cannot SIGSTOP")
        log(f"node {self.id}: SIGSTOP")
        self.proc.send_signal(signal.SIGSTOP)
        self.paused = True

    def resume(self):
        if self.alive() and self.paused:
            log(f"node {self.id}: SIGCONT")
            self.proc.send_signal(signal.SIGCONT)
        self.paused = False

    def force_kill(self):
        self.resume()
        if self.alive():
            self.proc.kill()
            self.proc.wait(timeout=10)
        self._close_log()

    def _close_log(self):
        if self.log_file is not None:
            self.log_file.close()
            self.log_file = None

    def log_tail(self, lines=40):
        try:
            with open(self.log_path, "r", errors="replace") as handle:
                return "".join(handle.readlines()[-lines:])
        except OSError as exc:
            return f"<no log: {exc}>"

    def count_log_lines(self, *patterns):
        """Lines in the node's whole log containing any of `patterns`; used
        to prove new evidence appeared during a fault window (sample before
        and after and compare)."""
        count = 0
        try:
            with open(self.log_path, "r", errors="replace") as handle:
                for line in handle:
                    if any(pattern in line for pattern in patterns):
                        count += 1
        except OSError:
            pass
        return count

    def wait_status(self, predicate, desc, timeout=10.0):
        wait_until(desc, timeout,
                   lambda: self.alive() and predicate(self.status()))

    def wait_leader(self, timeout=10.0):
        self.wait_status(lambda s: s.get("leader") == "1",
                         f"node {self.id} becomes leader", timeout)

    def wait_committed(self, idx, timeout=15.0):
        self.wait_status(lambda s: int(s["committed"]) >= idx,
                         f"node {self.id} committed >= {idx}", timeout)


class Proxy:
    """Bidirectional TCP forwarder: listens on `listen_port`, dials
    ("127.0.0.1", `target_port`). One proxy fronts one node's advertised
    raft endpoint.

    Modes:
      normal : forward bytes in both directions.
      drop   : half-open blackhole — accept(), then read and discard
               without ever dialing the target; not one byte is forwarded.
      refuse : listener closed — connect() fails with ECONNREFUSED/RST.
      delay  : forward, but sleep `delay` seconds per received chunk in
               each direction.

    Entering drop/refuse also cuts already-established pairs (a partition
    kills in-flight connections too); entering delay applies to existing
    pairs dynamically. heal() returns to normal and closes blackholed
    sockets so the peer's raft client reconnects immediately instead of
    waiting out its response timeout.
    """

    NORMAL = "normal"
    DROP = "drop"
    REFUSE = "refuse"
    DELAY = "delay"

    def __init__(self, name, target_port, listen_port=None,
                 target_host="127.0.0.1"):
        self.name = name
        self.target = (target_host, target_port)
        self.listen_port = listen_port if listen_port is not None \
            else free_port()
        self._mode = self.NORMAL
        self._delay = 0.0
        self._lock = threading.Lock()
        self._listener = None
        self._pairs = set()
        self._held = set()
        self._running = False
        self._accept_thread = None
        self.bytes_forwarded = 0

    @property
    def endpoint(self):
        return f"127.0.0.1:{self.listen_port}"

    def start(self):
        self._running = True
        self._accept_thread = threading.Thread(
            target=self._accept_loop, name=f"proxy-{self.name}-accept",
            daemon=True)
        self._accept_thread.start()

    # -- mode control ---------------------------------------------------

    def set_normal(self):
        self._set_mode(self.NORMAL)

    def set_drop(self):
        self._set_mode(self.DROP)

    def set_refuse(self):
        self._set_mode(self.REFUSE)

    def set_delay(self, seconds):
        self._set_mode(self.DELAY, delay=seconds)

    def heal(self):
        with self._lock:
            held = list(self._held)
        self._set_mode(self.NORMAL)
        for sock in held:
            self._close_sock(sock)

    def _set_mode(self, mode, delay=0.0):
        with self._lock:
            self._mode = mode
            self._delay = delay
            cut = list(self._pairs) if mode in (self.DROP, self.REFUSE) \
                else []
            listener = None
            if mode == self.REFUSE and self._listener is not None:
                listener, self._listener = self._listener, None
        for pair in cut:
            self._cut_pair(pair)
        if listener is not None:
            self._close_sock(listener)

    # -- accept / pump loops ---------------------------------------------

    def _open_listener(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", self.listen_port))
        sock.listen(128)
        sock.settimeout(0.2)
        return sock

    def _accept_loop(self):
        while self._running:
            with self._lock:
                listener = self._listener
                want = self._mode != self.REFUSE
            if listener is None:
                if not want:
                    time.sleep(0.02)
                    continue
                try:
                    listener = self._open_listener()
                except OSError:
                    time.sleep(0.05)
                    continue
                with self._lock:
                    self._listener = listener
            try:
                conn, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                # Listener closed under us by a mode switch.
                with self._lock:
                    if self._listener is listener:
                        self._listener = None
                continue
            self._on_accept(conn)

    def _on_accept(self, conn):
        with self._lock:
            mode = self._mode
        if mode == self.DROP:
            with self._lock:
                self._held.add(conn)
            threading.Thread(target=self._discard_loop, args=(conn,),
                             daemon=True).start()
            return
        try:
            upstream = socket.create_connection(self.target, timeout=5)
        except OSError:
            conn.close()
            return
        pair = (conn, upstream)
        with self._lock:
            self._pairs.add(pair)
        for src, dst in ((conn, upstream), (upstream, conn)):
            threading.Thread(target=self._pump, args=(src, dst, pair),
                             daemon=True).start()

    def _pump(self, src, dst, pair):
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                with self._lock:
                    delay = self._delay if self._mode == self.DELAY else 0.0
                if delay > 0:
                    time.sleep(delay)
                dst.sendall(data)
                self.bytes_forwarded += len(data)
        except OSError:
            pass
        finally:
            self._cut_pair(pair)

    def _discard_loop(self, conn):
        try:
            while conn.recv(65536):
                pass
        except OSError:
            pass
        finally:
            with self._lock:
                self._held.discard(conn)
            self._close_sock(conn)

    # -- teardown helpers --------------------------------------------------

    def _cut_pair(self, pair):
        with self._lock:
            if pair not in self._pairs:
                return
            self._pairs.discard(pair)
        for sock in pair:
            self._close_sock(sock)

    @staticmethod
    def _close_sock(sock):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass

    def close(self):
        self._running = False
        with self._lock:
            listener, self._listener = self._listener, None
            pairs = list(self._pairs)
            held = list(self._held)
        if listener is not None:
            self._close_sock(listener)
        for pair in pairs:
            self._cut_pair(pair)
        for sock in held:
            self._close_sock(sock)
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=3)


class Mesh:
    """One destination-side Proxy per node: every node's advertised cluster
    endpoint is its proxy's listen port, so all inter-node raft traffic
    flows through the destination node's proxy."""

    def __init__(self):
        self._proxies = {}

    def attach(self, node, listen_port=None, target_port=None):
        proxy = Proxy(f"n{node.id}",
                      target_port if target_port is not None
                      else node.raft_port,
                      listen_port=listen_port)
        proxy.start()
        self._proxies[node.id] = proxy
        return proxy

    def proxy(self, node_id):
        return self._proxies[node_id]

    def endpoint(self, node_id):
        return self._proxies[node_id].endpoint

    def heal_all(self):
        for proxy in self._proxies.values():
            proxy.heal()

    def close(self):
        for proxy in self._proxies.values():
            proxy.close()


class CommittedHistory:
    """Observable committed-state checker for the process gates. Aligned with
    the cluster fault harness invariant `meta.committed-state-monotonic`
    (see tests/cluster/cluster_invariants.cpp): any (op_id, payload) whose
    propose returned OK must, once the cluster converges, be readable as a
    completed operation with the same terminal result on every member —
    never missing (committed rollback), never different (fork/divergence).
    Payloads are unique per operation, so a stale or forked read always
    shows up as a mismatch.

    Only ever call record() for proposes that returned OK. Operation ids
    are never reused, so a propose whose completeop timed out but landed
    later is simply not in the record and cannot create a false mismatch.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._kv = {}

    def record(self, op_id, payload):
        with self._lock:
            self._kv[op_id] = payload

    def size(self):
        with self._lock:
            return len(self._kv)

    def snapshot(self):
        with self._lock:
            return dict(self._kv)

    def check(self, nodes, timeout=30.0, desc="committed history"):
        """Every listed node must converge to serving every recorded
        operation as `OK completed <payload>`. The newest recorded op is
        the convergence probe: the single load thread records in propose
        order, which is commit order, so once it is complete an in-order
        state machine must have everything older; a full-sweep mismatch
        after that is a genuine safety violation, not a race."""
        records = self.snapshot()
        if not records:
            return
        # dict preserves insertion order, and the single load thread records
        # in propose (= commit) order, so items[-1] really is the newest
        # committed operation. Never sort here: lexicographic order
        # disagrees with commit order and breaks the probe.
        items = list(records.items())
        probe_key, probe_value = items[-1]
        want_probe = f"OK completed {probe_value}"
        for node in nodes:
            wait_until(f"{desc}: node {node.id} serves {probe_key}",
                       timeout,
                       lambda node=node: node.alive()
                       and node.getop(probe_key) == want_probe)
            mismatches = []
            for key, value in items:
                reply = node.getop(key)
                if reply != f"OK completed {value}":
                    mismatches.append(f"{key}: {reply!r} want "
                                      f"OK completed {value}")
                    if len(mismatches) >= 5:
                        break
            if mismatches:
                raise Failure(
                    f"{desc}: node {node.id} violated "
                    f"meta.committed-state-monotonic: "
                    + "; ".join(mismatches))
        log(f"{desc}: {len(records)} operations verified on "
            f"{len(nodes)} node(s)")


class LoadThread:
    """Continuous propose load against whoever is currently leader.

    Every attempt is a fresh operation (ids are never reused: a propose
    whose completeop failed may still land both halves later). Only
    fully-OK pairs are recorded into the CommittedHistory. `leader_picker`
    may pin the target to a fixed node; the default re-scans the cluster
    for the current leader whenever the cached target stops answering as
    leader.
    """

    def __init__(self, nodes, history, prefix="ld", interval=0.005,
                 leader_picker=None):
        self.nodes = nodes
        self.history = history
        self.prefix = prefix
        self.interval = interval
        self._picker = leader_picker
        self._stop = threading.Event()
        self._thread = None
        self.seq = 0
        self.ok_count = 0
        self.err_counts = {}

    def _scan_leader(self):
        for node in self.nodes:
            try:
                if node.alive() and node.is_leader():
                    return node
            except (OSError, Failure):
                continue
        return None

    def start(self):
        self._thread = threading.Thread(
            target=self._run, name=f"load-{self.prefix}", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def join(self, timeout=15.0):
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def stats(self):
        errs = ", ".join(f"{k}={v}" for k, v in sorted(
            self.err_counts.items())) or "none"
        return f"load {self.prefix}: ok={self.ok_count} errors: {errs}"

    def _run(self):
        picker = self._picker or self._scan_leader
        leader = None
        while not self._stop.is_set():
            if leader is None or not leader.alive():
                leader = picker()
                if leader is None:
                    time.sleep(0.05)
                    continue
            value = f"{self.prefix}{self.seq}"
            self.seq += 1
            try:
                op_id, reply = leader.propose(value, timeout=8.0)
            except (OSError, Failure):
                reply = "ERR io"
            if reply.startswith("OK "):
                self.history.record(op_id, value)
                self.ok_count += 1
                time.sleep(self.interval)
            else:
                token = reply[4:] if reply.startswith("ERR ") else reply
                self.err_counts[token] = self.err_counts.get(token, 0) + 1
                if token in ("not-leader", "io"):
                    leader = None
                time.sleep(max(self.interval, 0.02))


def make_nodes(binary, workdir, count, args=None, first_id=1):
    return [Node(binary, workdir, node_id, args=args)
            for node_id in range(first_id, first_id + count)]


def find_leader(nodes, timeout=15.0, exclude=()):
    leader = None

    def probe():
        nonlocal leader
        for node in nodes:
            if node.id in exclude or not node.alive():
                continue
            try:
                if node.is_leader():
                    leader = node
                    return True
            except (OSError, Failure):
                continue
        return False

    wait_until("a node reports leader=1", timeout, probe)
    log(f"node {leader.id} is leader")
    return leader


def join_and_verify(leader, node, endpoint=None, timeout=30.0):
    """Drive addsrv until `node` verifiably replicates a probe operation.

    The durable workflow retries invites independently of this connection.
    OK means configuration and identity completion on the leader, while a
    probe additionally verifies the joiner's state-machine catch-up. Identical
    retries attach to the retained task after an uncertain wait outcome.
    "ERR already-exists" is
    accepted: it means the node's config entry committed earlier (e.g. it
    finished joining just before a mid-invite crash), which the probe then
    confirms.
    """
    wait_until(f"node {node.id} ctl answers", 15,
               lambda: node.alive() and node.status())
    target = endpoint if endpoint is not None else node.endpoint
    deadline = time.monotonic() + timeout
    invited = False
    while time.monotonic() < deadline:
        reply = leader.ctl(
            f"addsrv {node.id} {target} {node.data_control_endpoint} "
            f"{node.ctl_endpoint}")
        acceptable = ("OK", "ERR joining", "ERR config-changing",
                      "ERR already-exists")
        if reply not in acceptable and not reply.startswith("ERR uncertain-outcome operation="):
            raise Failure(f"addsrv {node.id}: {reply}")
        invited = invited or reply in ("OK", "ERR already-exists")
        if invited:
            op_id, preply = leader.propose("1")
            if preply.startswith("OK "):
                idx = int(preply[3:])
                try:
                    wait_until(
                        f"node {node.id} replicates probe", 5,
                        lambda: node.alive()
                        and node.getop(op_id) == "OK completed 1")
                    log(f"node {node.id} joined (probe idx {idx})")
                    return
                except Failure:
                    pass  # stuck join; loop re-issues addsrv
        time.sleep(0.2)
    raise Failure(f"node {node.id} never joined the cluster")


def bootstrap_cluster(nodes, mesh=None):
    """Single-node bootstrap, then verified one-at-a-time joins. With a
    mesh, joined nodes are added by their proxy endpoint so their traffic
    is proxied; the bootstrap node's baked endpoint needs
    bootstrap_meshed_cluster instead."""
    nodes[0].start(bootstrap=True)
    leader = find_leader([nodes[0]])
    for node in nodes[1:]:
        endpoint = None
        if mesh is not None:
            mesh.attach(node)
            endpoint = mesh.endpoint(node.id)
        node.start(bootstrap=False)
        join_and_verify(leader, node, endpoint=endpoint)
    log(f"{len(nodes)}-node cluster converged")
    return leader


def bootstrap_meshed_cluster(nodes, mesh):
    """Bootstrap a cluster where EVERY node's advertised endpoint is a
    proxy port. The bootstrap node's endpoint is baked into the durable
    cluster config from --addr on first boot and NuRaft's add_srv refuses
    to update an existing server (SERVER_ALREADY_EXISTS), so the bootstrap
    node is bounced once onto a fresh bind port and its proxy takes over
    the baked port. Joined nodes are simply added by proxy endpoint."""
    first = nodes[0]
    first.start(bootstrap=True)
    find_leader([first])
    baked_port = first.raft_port
    first.terminate()
    rebind_port = free_port()
    mesh.attach(first, listen_port=baked_port, target_port=rebind_port)
    first.start(bootstrap=True, raft_port=rebind_port)
    leader = find_leader([first])
    for node in nodes[1:]:
        mesh.attach(node)
        node.start(bootstrap=False)
        join_and_verify(leader, node, endpoint=mesh.endpoint(node.id))
    log(f"{len(nodes)}-node meshed cluster converged")
    return leader


def propose_ops(leader, first, count, prefix="key", history=None):
    """`count` committed writes with unique values f"{prefix}{ii}"; records
    each into `history` when given. Returns the last commit index."""
    last_idx = 0
    for ii in range(first, first + count):
        value = f"{prefix}{ii}"
        op_id, reply = leader.propose(value)
        if not reply.startswith("OK "):
            raise Failure(f"propose {value}: {reply}")
        if history is not None:
            history.record(op_id, value)
        last_idx = int(reply[3:])
    return last_idx


def manual_snapshot(node, timeout=15.0):
    """Drive the ctl `snapshot` verb to OK and return the snapshot index.

    The snapshot's durable write runs on the SM writer thread, so a previous
    round (e.g. an automatic snapshot that
    just fired) can still be in flight — NuRaft's create_snapshot then
    fails fast and the ctl answers "ERR snapshot-failed". Retry instead of
    treating that race as a gate failure."""
    result = {}

    def attempt():
        reply = node.ctl("snapshot")
        if reply.startswith("OK "):
            result["idx"] = int(reply[3:])
            return True
        return False

    wait_until(f"node {node.id} manual snapshot", timeout, attempt)
    return result["idx"]


def wal_segment_first_indexes(node):
    """Sorted first indexes of the node's WAL v1 segments
    (log-<first_idx>.seg; nuraft_log_store.h). Compaction unlinks covered
    segments and rewrites the boundary one, so once the log prefix is
    compacted the minimum first index advances past 1."""
    firsts = []
    for name in os.listdir(node.data_dir):
        if name.startswith("log-") and name.endswith(".seg"):
            firsts.append(int(name[4:-4]))
    return sorted(firsts)


def wait_cluster_committed(nodes, idx, timeout=15.0):
    for node in nodes:
        node.wait_committed(idx, timeout=timeout)


def wait_no_regress(node, pre_committed, timeout=20.0):
    """After a (re)start the node's committed index must climb back to its
    pre-stop value: durable committed state never rolls back. Right after
    boot a follower legitimately reports only its snapshot index until the
    leader's next append_entries carries the commit index forward, so this
    is a poll, not a one-shot assert."""
    wait_until(
        f"node {node.id} committed >= {pre_committed} (no regress)",
        timeout, lambda: node.alive() and node.committed() >= pre_committed)


def max_committed(nodes):
    best = 0
    for node in nodes:
        if node.alive():
            try:
                best = max(best, node.committed())
            except (OSError, Failure):
                continue
    return best


def dump_node_logs(nodes, lines=40):
    for node in nodes:
        print(f"--- node {node.id} log tail ({node.log_path}) ---",
              file=sys.stderr)
        print(node.log_tail(lines), file=sys.stderr)


def make_workdir(argv, prefix):
    """argv[1] is the lavik-meta binary; optional argv[2] pins (and keeps)
    the workdir for debugging, mirroring smoke_3node.py."""
    if len(argv) > 2:
        workdir = argv[2]
        os.makedirs(workdir, exist_ok=True)
        return workdir, True
    test_data_dir = os.environ.get("LAVIK_TEST_DATA_DIR") or "/tmp"
    return tempfile.mkdtemp(prefix=prefix, dir=test_data_dir), False


def cleanup(workdir, keep):
    if not keep:
        shutil.rmtree(workdir, ignore_errors=True)


def tls_args(ca_cert, cert, key):
    """mTLS flag triple for lavik-meta; all raft traffic then requires
    client certs signed by ca_cert (the ctl surface remains a peer-credential
    authenticated Unix socket unless explicit ctl mTLS flags are supplied)."""
    return ["--tls-ca", ca_cert, "--tls-cert", cert, "--tls-key", key]


def read_rss_kb(pid):
    """VmRSS of a live process in kB, for bounded-memory assertions."""
    with open(f"/proc/{pid}/status", "r") as handle:
        for line in handle:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    raise Failure(f"pid {pid}: VmRSS not found")


# Coarse fatal-signature scan; a crashed process is primarily caught by
# alive()/exit-code checks (a signal death prints nothing to the node's own
# log), these markers only belt-and-brace the in-process failure modes.
CRASH_MARKERS = (
    "pure virtual",
    "std::terminate",
    "Segmentation fault",
    "AddressSanitizer",
    "Assertion failed",
)


def assert_intact(nodes, desc=""):
    """Every node still alive, no fatal signatures in any log."""
    for node in nodes:
        if not node.alive():
            raise Failure(f"{desc}: node {node.id} died unexpectedly "
                          f"(exit {node.proc.returncode})")
        hits = node.count_log_lines(*CRASH_MARKERS)
        if hits:
            raise Failure(
                f"{desc}: node {node.id} log holds {hits} crash-marker "
                f"line(s): {CRASH_MARKERS}")
