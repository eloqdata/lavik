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

"""Process gate for the Meta-to-Data control session.

The plaintext scenario uses three real lavik-meta processes and one real
lavik process.  The Data node starts with only a follower as its seed, then
must follow the committed leader redirect, install a complete desired-state
object, and publish heartbeat observations.  Killing the accepted leader must
make the same Data process discover the replacement leader and install a fresh
complete object; no Data-side persisted control state participates.

The mTLS scenario uses a real single-member Meta process.  A CA-authenticated
Data certificate with the committed ``lavik://node/<id>`` URI SAN completes
the same FDS/heartbeat flow.  A certificate signed by the same CA but naming a
different Data principal must remain connected to neither control authority
nor FDS while the process itself stays healthy.

The plaintext scenario commits one assigned slot-owning group and active
authority.  A fresh Meta-managed Data node intentionally has no ReadyToken
until a later reconciliation workflow populates it, so its merged heartbeat
must carry a lease challenge and receive a typed node-not-ready denial.  This
gate also drives a committed fence: the replacement FDS can arrive only after
the Data node has fenced locally, drained, and returned FenceAck.

The mTLS scenario remains grantless and therefore retains explicit
NoChallenge coverage independently of the authority-bearing plaintext path.
Without that first ReadyToken no keyed write can be admitted, so an
indefinitely blocked write cannot exercise a non-empty process-level drain in
this gate; the request/NodeControl drain seams cover that ordering in unit
tests until reconciliation can bootstrap the population.

Usage: gate_data_control.py /path/to/lavik-meta /path/to/lavik [workdir]
"""

import os
import re
import signal
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402


DATA_NODE = "1111111111111111111111111111111111111111"
BAD_DATA_NODE = "2222222222222222222222222222222222222222"
OTHER_DATA_NODE = "3333333333333333333333333333333333333333"
GROUP = "gate-group"
DATA_FILE_BYTES = 128 * 1024 * 1024


def expect_ok(reply, label):
    if not reply.startswith("OK"):
        raise H.Failure(f"{label}: {reply}")


def expect_commit(reply, label):
    match = re.fullmatch(r"OK (\d+)", reply)
    if match is None:
        raise H.Failure(f"{label}: {reply}")
    return int(match.group(1))


def allocate_data_file(path, size=DATA_FILE_BYTES):
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        if hasattr(os, "posix_fallocate"):
            os.posix_fallocate(fd, 0, size)
        else:
            os.ftruncate(fd, size)
    finally:
        os.close(fd)


class DataProcess:
    def __init__(self, binary, workdir, node_id, seed, tls=None, workers=1,
                 tls_only=False):
        self.binary = binary
        self.node_id = node_id
        self.workdir = workdir
        if tls_only and tls is None:
            raise H.Failure("TLS-only Data needs TLS credentials")
        self.redis_port = 0 if tls_only else H.free_port()
        self.metrics_port = H.free_port()
        while self.metrics_port == self.redis_port:
            self.metrics_port = H.free_port()
        self.tls_port = 0
        if tls is not None:
            self.tls_port = H.free_port()
            while self.tls_port in (self.redis_port, self.metrics_port):
                self.tls_port = H.free_port()
        self.seed = seed
        self.tls = tls
        self.workers = workers
        self.log_path = os.path.join(workdir, "lavik.log")
        self.data_path = os.path.join(workdir, "lavik.data")
        self.proc = None
        self.log_file = None

    @property
    def advertised_endpoint(self):
        return f"tcp://127.0.0.1:{self.redis_port}"

    def start(self, wait_ready=True):
        os.makedirs(self.workdir, exist_ok=True)
        allocate_data_file(self.data_path, DATA_FILE_BYTES * self.workers)
        args = [
            self.binary,
            "--logtostderr",
            "--port", str(self.redis_port),
            "--metrics-port", str(self.metrics_port),
            "--threads", str(self.workers),
            "--no-pin-workers",
            "--recv-buffers-per-worker", "0",
            "--registered-buffer-mb-per-worker", "64",
            "--repl-backlog-size", f"{8 * self.workers}mb",
            "--max-memory", "1073741824",
            "--flush-max-ms", "20",
            "--data-file", self.data_path,
            "--rdb-dir", self.workdir,
            "--cluster-enabled",
            "--cluster-node-id", self.node_id,
            "--cluster-meta-seed", self.seed,
            "--cluster-announce-ip", "127.0.0.1",
        ]
        if self.tls is not None:
            ca_cert, cert, key = self.tls
            args.extend([
                "--tls-replication",
                "--tls-port", str(self.tls_port),
                "--tls-ca-cert-file", ca_cert,
                "--tls-cert-file", cert,
                "--tls-key-file", key,
            ])
        self.log_file = open(self.log_path, "ab")
        self.proc = subprocess.Popen(
            args, stdout=self.log_file, stderr=subprocess.STDOUT)
        H.log(f"Data node {self.node_id[:8]} started "
              f"(pid {self.proc.pid}, seed {self.seed})")
        if wait_ready:
            H.wait_until(
                f"Data node {self.node_id[:8]} metrics listener", 20,
                lambda: self.alive() and self._metrics_ready())

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def _metrics_ready(self):
        try:
            self.metrics()
            return True
        except (OSError, H.Failure):
            return False

    def metrics(self):
        request = (b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                   b"Connection: close\r\n\r\n")
        with socket.create_connection(
                ("127.0.0.1", self.metrics_port), timeout=1.0) as sock:
            sock.settimeout(2.0)
            sock.sendall(request)
            response = bytearray()
            while True:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                response.extend(chunk)
        header, separator, body = bytes(response).partition(b"\r\n\r\n")
        if not separator or b" 200 " not in header.split(b"\r\n", 1)[0]:
            raise H.Failure("metrics endpoint did not return HTTP 200")
        return body.decode("utf-8", errors="strict")

    def metric(self, name, labels=""):
        prefix = f"{name}{labels} "
        for line in self.metrics().splitlines():
            if line.startswith(prefix):
                return int(float(line[len(prefix):]))
        raise H.Failure(f"metric {name}{labels} is missing")

    def wait_metric(self, name, predicate, desc, timeout=20, labels=""):
        H.wait_until(
            desc, timeout,
            lambda: self.alive() and predicate(self.metric(name, labels)))

    def command_head(self, args):
        """Returns the first RESP line; these gates only inspect admission."""
        encoded = bytearray(f"*{len(args)}\r\n".encode())
        for arg in args:
            value = arg.encode()
            encoded.extend(f"${len(value)}\r\n".encode())
            encoded.extend(value)
            encoded.extend(b"\r\n")
        with socket.create_connection(
                ("127.0.0.1", self.redis_port), timeout=2.0) as sock:
            sock.settimeout(2.0)
            sock.sendall(encoded)
            response = bytearray()
            while b"\r\n" not in response:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response.extend(chunk)
        head, separator, _ = bytes(response).partition(b"\r\n")
        if not separator:
            raise H.Failure(f"Data command {args[0]} returned no RESP line")
        return head.decode("utf-8", errors="strict")

    def terminate(self):
        if not self.alive():
            self._close_log()
            return
        self.resume()
        self.proc.send_signal(signal.SIGINT)
        try:
            code = self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired as exc:
            self.proc.kill()
            self.proc.wait(timeout=10)
            raise H.Failure("Data process did not stop within 30s") from exc
        self._close_log()
        if code != 0:
            raise H.Failure(f"Data process exited with code {code}, want 0")
        log = self.log_tail(lines=200)
        quiesced = log.rfind(
            "Meta control client quiesced before storage flush")
        flushing = log.rfind("all active requests drained; flushing storage")
        if quiesced < 0 or flushing < 0 or quiesced > flushing:
            raise H.Failure(
                "Data shutdown did not quiesce Meta control before storage "
                "flush")
        H.log(f"Data node {self.node_id[:8]}: clean exit 0")

    def pause(self):
        if not self.alive():
            raise H.Failure(
                f"Data node {self.node_id[:8]} is not running")
        H.log(f"Data node {self.node_id[:8]}: SIGSTOP")
        self.proc.send_signal(signal.SIGSTOP)

    def resume(self):
        if self.alive():
            self.proc.send_signal(signal.SIGCONT)

    def force_kill(self):
        if self.alive():
            self.resume()
            self.proc.kill()
            self.proc.wait(timeout=10)
        self._close_log()

    def _close_log(self):
        if self.log_file is not None:
            self.log_file.close()
            self.log_file = None

    def log_tail(self, lines=60):
        try:
            with open(self.log_path, "r", errors="replace") as handle:
                return "".join(handle.readlines()[-lines:])
        except OSError as exc:
            return f"<no Data log: {exc}>"


def observation_count(node):
    reply = node.observations()
    match = re.fullmatch(r"OK total=(\d+)", reply)
    if match is None:
        raise H.Failure(f"node {node.id} observations: {reply}")
    return int(match.group(1))


def register_data_node(leader, data):
    reply = leader.ctl(
        f"registernode {data.node_id} lavik://node/{data.node_id} "
        f"primary {data.advertised_endpoint}")
    expect_ok(reply, f"register Data node {data.node_id[:8]}")


def seed_assigned_authority(leader, data):
    expect_commit(leader.creategroup(GROUP), "create assigned group")
    expect_commit(leader.assignnode(GROUP, data.node_id),
                  "assign Data node")
    expect_commit(leader.begingroupterm(GROUP, 0, 1), "begin group term")
    expect_commit(leader.setslotmap(0, 16383, GROUP),
                  "assign all slots")
    return expect_commit(
        leader.activateauthority(GROUP, 1, data.node_id),
        "activate authority")


def assert_grantless_session(data, leader, minimum_fds=1):
    data.wait_metric(
        "lavik_cluster_control_connected", lambda value: value == 1,
        f"Data node {data.node_id[:8]} accepts Meta leader")
    data.wait_metric(
        "lavik_cluster_control_full_states_applied_total",
        lambda value: value >= minimum_fds,
        f"Data node {data.node_id[:8]} applies FDS #{minimum_fds}")
    H.wait_until(
        f"Meta leader {leader.id} ingests Data heartbeat", 15,
        lambda: observation_count(leader) >= 2)
    if data.metric("lavik_cluster_control_protocol_errors_total") != 0:
        raise H.Failure("healthy session recorded a protocol error")
    if data.metric(
            "lavik_cluster_control_lease_decisions_total",
            '{decision="granted"}') != 0:
        raise H.Failure("grantless projection unexpectedly received a lease")
    if data.metric(
            "lavik_cluster_control_lease_decisions_total",
            '{decision="denied"}') != 0:
        raise H.Failure("NoChallenge heartbeat was counted as a lease denial")


def assert_authority_challenge_denied(data, leader, minimum_fds=1,
                                      prior_denials=0):
    data.wait_metric(
        "lavik_cluster_control_connected", lambda value: value == 1,
        f"Data node {data.node_id[:8]} accepts Meta leader")
    data.wait_metric(
        "lavik_cluster_control_full_states_applied_total",
        lambda value: value >= minimum_fds,
        f"Data node {data.node_id[:8]} applies authority FDS #{minimum_fds}")
    data.wait_metric(
        "lavik_cluster_control_lease_decisions_total",
        lambda value: value > prior_denials,
        "assigned owner challenges authority and decodes LeaseDenied",
        labels='{decision="denied"}')
    # This scenario commits the exact owner/term/version anchor while the real
    # Data process has no ReadyToken. The server's typed policy unit test pins
    # that unique denial branch to LeaseDenialReason::kNodeNotReady.
    H.wait_until(
        f"Meta leader {leader.id} ingests assigned Data heartbeat", 15,
        lambda: observation_count(leader) >= 2)
    if data.metric(
            "lavik_cluster_control_lease_decisions_total",
            '{decision="granted"}') != 0:
        raise H.Failure(
            "unpopulated assignment unexpectedly received a live lease")
    if data.metric("lavik_cluster_control_protocol_errors_total") != 0:
        raise H.Failure("authority denial recorded a protocol error")


def assert_keyed_write_fenced(data, label):
    reply = data.command_head(["SET", "{gate}key", "value"])
    if not (reply.startswith("-LOADING") or
            reply.startswith("-CLUSTERDOWN")):
        raise H.Failure(f"{label}: keyed write was not fenced: {reply}")


def run_plaintext(meta_binary, data_binary, workdir):
    scenario = os.path.join(workdir, "plaintext")
    os.makedirs(scenario, exist_ok=True)
    nodes = H.make_nodes(
        meta_binary, scenario, 3,
        args=H.raft_args(snapshot_distance=100000))
    data = None
    try:
        leader = H.bootstrap_cluster(nodes)
        follower = next(node for node in nodes if node.id != leader.id)
        data = DataProcess(data_binary, os.path.join(scenario, "data"),
                           DATA_NODE, follower.data_control_endpoint)
        expect_commit(leader.put_authority_lease_policy(1),
                      "commit Authority Lease Policy")
        register_data_node(leader, data)
        seeded_through = seed_assigned_authority(leader, data)
        H.wait_until(
            "assigned authority reaches the follower seed", 15,
            lambda: int(follower.status()["committed"]) >= seeded_through)

        data.start()
        assert_authority_challenge_denied(data, leader)
        assert_keyed_write_fenced(data, "unready assignment")
        # Meta mode has only finite group authority. Process-wide mutations
        # have no group proof and remain rejected, while FUNCTION KILL/STATS
        # must bypass both loading fences so a running function cannot
        # deadlock a replacement population waiting for it to drain.
        for command in (["FLUSHDB"], ["FLUSHALL"],
                        ["FUNCTION", "LOAD", "invalid"],
                        ["FUNCTION", "DELETE", "missing"],
                        ["FUNCTION", "FLUSH"],
                        ["FUNCTION", "RESTORE", "invalid"]):
            expected = (f"-ERR {' '.join(command[:2])} is not allowed "
                        "in cluster mode")
            actual = data.command_head(command)
            if actual != expected:
                raise H.Failure(
                    f"finite-authority global mutation gate: {actual}, "
                    f"want {expected}")
        stats = data.command_head(["FUNCTION", "STATS"])
        if stats.startswith("-LOADING"):
            raise H.Failure("FUNCTION STATS was hidden by a loading gate")
        kill = data.command_head(["FUNCTION", "KILL"])
        if not kill.startswith("-NOTBUSY"):
            raise H.Failure(
                f"FUNCTION KILL did not reach run control: {kill}")
        redirected_reconnects = data.metric(
            "lavik_cluster_control_reconnects_total")
        if redirected_reconnects < 1:
            raise H.Failure(
                "follower-only seed did not produce a redirect/reconnect")
        first_fds = data.metric(
            "lavik_cluster_control_full_states_applied_total")
        denials_before_loss = data.metric(
            "lavik_cluster_control_lease_decisions_total",
            '{decision="denied"}')
        H.log("plaintext: assigned authority challenge/denial verified")

        old_leader = leader
        old_leader.kill9()
        data.wait_metric(
            "lavik_cluster_control_connected", lambda value: value == 0,
            "Data node observes authority-session loss", timeout=5)
        assert_keyed_write_fenced(data, "lost authority session")
        survivors = [node for node in nodes if node.id != old_leader.id]
        leader = H.find_leader(survivors, timeout=15)
        data.wait_metric(
            "lavik_cluster_control_full_states_applied_total",
            lambda value: value > first_fds,
            "Data node installs a fresh FDS after Meta leader change",
            timeout=25)
        data.wait_metric(
            "lavik_cluster_control_connected", lambda value: value == 1,
            "Data node reconnects to replacement Meta leader", timeout=10)
        current_reconnects = data.metric(
            "lavik_cluster_control_reconnects_total")
        if current_reconnects <= redirected_reconnects:
            raise H.Failure("leader death did not advance reconnect attempts")
        assert_authority_challenge_denied(
            data, leader, minimum_fds=first_fds + 1,
            prior_denials=denials_before_loss)
        H.log("plaintext: session loss remains fail-closed and reconnects")

        before_fence_fds = data.metric(
            "lavik_cluster_control_full_states_applied_total")
        expect_commit(leader.fencegroup(GROUP, 1), "fence active group")
        # Meta does not publish the grantless replacement until the Data node
        # has closed admission, run the superseded-anchor drain barrier, and
        # returned FenceAck. Observing that replacement is therefore the
        # process-level proof of the whole Fence/FenceAck barrier.
        data.wait_metric(
            "lavik_cluster_control_full_states_applied_total",
            lambda value: value > before_fence_fds,
            "Data FenceAck releases grantless replacement FDS", timeout=15)
        if data.metric("lavik_cluster_control_protocol_errors_total") != 0:
            raise H.Failure("Fence/FenceAck recorded a protocol error")
        assert_keyed_write_fenced(data, "committed group fence")
        H.log("plaintext: Fence/FenceAck replacement ordering verified")

        old_leader.start(bootstrap=False)
        H.wait_until(
            "old Meta leader restarts as a caught-up follower", 20,
            lambda: old_leader.getnode(DATA_NODE).startswith("OK "))
        data.terminate()
        for node in nodes:
            node.terminate()
    except Exception:
        H.dump_node_logs(nodes)
        if data is not None:
            print(f"--- Data log tail ({data.log_path}) ---",
                  file=sys.stderr)
            print(data.log_tail(), file=sys.stderr)
            if data.alive():
                try:
                    control_metrics = [
                        line for line in data.metrics().splitlines()
                        if line.startswith("lavik_cluster_control_")
                    ]
                    print("--- Data control metrics ---", file=sys.stderr)
                    print("\n".join(control_metrics), file=sys.stderr)
                except (OSError, H.Failure) as exc:
                    print(f"<Data metrics unavailable: {exc}>",
                          file=sys.stderr)
        raise
    finally:
        if data is not None:
            data.force_kill()
        for node in nodes:
            node.force_kill()


def run_openssl(args, cwd):
    process = subprocess.run(["openssl"] + args, cwd=cwd,
                             capture_output=True, text=True, timeout=60)
    if process.returncode != 0:
        raise H.Failure(
            f"openssl {args[0]}: {process.stderr.strip()[:300]}")


def make_ca(workdir):
    os.makedirs(workdir, exist_ok=True)
    run_openssl([
        "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-sha256",
        "-keyout", "ca.key", "-out", "ca.crt", "-days", "2",
        "-subj", "/CN=lavik-data-control-test-ca",
    ], workdir)
    return (os.path.join(workdir, "ca.crt"),
            os.path.join(workdir, "ca.key"))


def make_leaf(workdir, ca_cert, ca_key, name, uri):
    run_openssl([
        "req", "-newkey", "rsa:2048", "-nodes", "-sha256",
        "-keyout", f"{name}.key", "-out", f"{name}.csr",
        "-subj", f"/CN={name}",
        "-addext", f"subjectAltName=IP:127.0.0.1,URI:{uri}",
        "-addext", "extendedKeyUsage=serverAuth,clientAuth",
        "-addext", "keyUsage=critical,digitalSignature,keyEncipherment",
    ], workdir)
    run_openssl([
        "x509", "-req", "-in", f"{name}.csr",
        "-CA", ca_cert, "-CAkey", ca_key, "-CAcreateserial",
        "-out", f"{name}.crt", "-days", "2", "-sha256",
        "-copy_extensions", "copy",
    ], workdir)
    return (os.path.join(workdir, f"{name}.crt"),
            os.path.join(workdir, f"{name}.key"))


def assert_wrong_uri_rejected(data, meta):
    deadline = time.monotonic() + 4.0
    while time.monotonic() < deadline:
        if not data.alive():
            raise H.Failure("wrong-URI Data process crashed")
        connected = data.metric("lavik_cluster_control_connected")
        full_states = data.metric(
            "lavik_cluster_control_full_states_applied_total")
        if connected != 0 or full_states != 0:
            raise H.Failure(
                "wrong-URI Data certificate reached an accepted FDS")
        if observation_count(meta) != 0:
            raise H.Failure(
                "wrong-URI Data certificate published an observation")
        time.sleep(0.1)
    if data.metric("lavik_cluster_control_reconnects_total") < 1:
        raise H.Failure("wrong-URI Data process did not retry the handshake")


def run_mtls(meta_binary, data_binary, workdir):
    scenario = os.path.join(workdir, "mtls")
    cert_dir = os.path.join(scenario, "certs")
    os.makedirs(cert_dir, exist_ok=True)
    ca_cert, ca_key = make_ca(cert_dir)
    meta_cert, meta_key = make_leaf(
        cert_dir, ca_cert, ca_key, "meta-1", "lavik://meta/1")
    good_cert, good_key = make_leaf(
        cert_dir, ca_cert, ca_key, "data-good",
        f"lavik://node/{DATA_NODE}")
    bad_cert, bad_key = make_leaf(
        cert_dir, ca_cert, ca_key, "data-wrong-uri",
        f"lavik://node/{OTHER_DATA_NODE}")

    meta_dir = os.path.join(scenario, "meta")
    os.makedirs(meta_dir, exist_ok=True)
    meta = H.Node(
        meta_binary, meta_dir, 1,
        args=H.raft_args(snapshot_distance=100000) +
        H.tls_args(ca_cert, meta_cert, meta_key))
    bad_data = None
    good_data = None
    try:
        meta.start(bootstrap=True)
        meta.wait_leader()
        expect_commit(meta.put_authority_lease_policy(1),
                      "commit Authority Lease Policy")

        bad_data = DataProcess(
            data_binary, os.path.join(scenario, "data-wrong-uri"),
            BAD_DATA_NODE, meta.data_control_endpoint,
            tls=(ca_cert, bad_cert, bad_key))
        register_data_node(meta, bad_data)
        bad_data.start()
        assert_wrong_uri_rejected(bad_data, meta)
        bad_data.terminate()
        H.log("mTLS: trusted certificate with wrong Data URI SAN rejected")

        good_data = DataProcess(
            data_binary, os.path.join(scenario, "data-good"), DATA_NODE,
            meta.data_control_endpoint,
            tls=(ca_cert, good_cert, good_key))
        register_data_node(meta, good_data)
        good_data.start()
        assert_grantless_session(good_data, meta)
        good_data.terminate()
        meta.terminate()
        H.log("mTLS: matching Meta/Data URI SAN session verified")
    except Exception:
        H.dump_node_logs([meta])
        for data in (bad_data, good_data):
            if data is None:
                continue
            print(f"--- Data log tail ({data.log_path}) ---",
                  file=sys.stderr)
            print(data.log_tail(), file=sys.stderr)
            if data.alive():
                try:
                    control_metrics = [
                        line for line in data.metrics().splitlines()
                        if line.startswith("lavik_cluster_control_")
                    ]
                    print("--- Data control metrics ---", file=sys.stderr)
                    print("\n".join(control_metrics), file=sys.stderr)
                except (OSError, H.Failure) as exc:
                    print(f"<Data metrics unavailable: {exc}>",
                          file=sys.stderr)
        raise
    finally:
        for data in (bad_data, good_data):
            if data is not None:
                data.force_kill()
        meta.force_kill()


def main():
    if len(sys.argv) not in (3, 4):
        print(__doc__)
        return 2
    work_argv = [sys.argv[0], sys.argv[1]]
    if len(sys.argv) == 4:
        work_argv.append(sys.argv[3])
    workdir, keep = H.make_workdir(
        work_argv, "meta_integration_data_control_")
    started = time.monotonic()
    try:
        run_plaintext(sys.argv[1], sys.argv[2], workdir)
        run_mtls(sys.argv[1], sys.argv[2], workdir)
        H.log(f"PASS in {time.monotonic() - started:.1f}s")
        return 0
    except Exception as exc:  # noqa: BLE001 - process logs are the evidence
        print(f"[gate-data-control] FAIL: {exc}", file=sys.stderr)
        if keep:
            print(f"[gate-data-control] retained workdir: {workdir}",
                  file=sys.stderr)
        return 1
    finally:
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    H.set_tag("gate-data-control")
    sys.exit(main())
