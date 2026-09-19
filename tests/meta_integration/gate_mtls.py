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

"""Integration gate: mTLS transport for lavik-meta.

The gate creates one CA and a distinct leaf per member. Every leaf covers the
numeric endpoint and carries exactly one canonical URI SAN
(`lavik://meta/<server-id>`). The ctl surface stays on its authenticated
Unix socket, independent of Raft TLS.

Scenarios (each with hard assertions):

P.  positive: a 3-node all-TLS cluster runs the full flow — probe-verified
    joins, 30 replicated keys, automatic snapshots (distance 30), kill -9
    and restart of a follower with catch-up, clean SIGTERM of all nodes.
N1. plaintext joiner vs TLS cluster: a node started WITHOUT the TLS flags
    has a retained pending invite, but every handshake dies,
    so it never makes raft progress (committed stays 0, reads miss) while
    the quorum keeps writing. And the mirror: a TLS joiner against a
    plaintext cluster.
N2. wrong CA: a joiner holding a cert signed by a different CA (generated
    with the openssl CLI in the workdir) is equally isolated; cluster fine.
N3. expired certificate (best effort): signed by a CA the cluster trusts
    but with a validity window in the past, via `openssl ca
    -startdate/-enddate`. If this host's openssl cannot do that, the
    scenario logs SKIP with the reason instead of failing.
N4. trusted CA, wrong SAN: a joiner holding a cert signed by the very CA
    the cluster trusts but whose SAN (10.9.9.9) does not cover the
    endpoint host (127.0.0.1) is rejected by the client-side peer_name
    check; equally isolated, cluster fine. N3/N4 share one cluster whose
    CA the gate builds and controls (tests/tls ships no ca.key).
N5. trusted CA and correct endpoint SAN, but a URI SAN naming another member
    id: startup rejects the mismatched local identity; corrected credentials
    permit a subsequent join and retirement.

N1–N4 assert the isolated node stays alive with a working ctl surface and
that the cluster keeps writing. Transport-failure counters establish rejection;
N5 separately asserts local identity validation before serving.

Usage: gate_mtls.py /path/to/lavik-meta [workdir]
"""

import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
ISOLATION_WINDOW_S = 4.0


def run_openssl(args, cwd):
    proc = subprocess.run(["openssl"] + args, cwd=cwd, capture_output=True,
                          text=True, timeout=60)
    if proc.returncode != 0:
        raise H.Failure(f"openssl {args[0]}: {proc.stderr.strip()[:200]}")


def make_ca(base_dir, cn):
    os.makedirs(base_dir, exist_ok=True)
    run_openssl(["req", "-x509", "-newkey", "rsa:2048", "-nodes",
                 "-keyout", "ca.key", "-out", "ca.crt", "-days", "2",
                 "-subj", f"/CN={cn}"], base_dir)
    return (os.path.join(base_dir, "ca.crt"), os.path.join(base_dir,
                                                           "ca.key"))


def make_leaf(workdir, ca_crt, ca_key, name,
              san="IP:127.0.0.1,DNS:localhost"):
    """CSR signed by the given CA (valid dates); `san` controls the
    subjectAltName extension copied into the cert."""
    run_openssl(["req", "-newkey", "rsa:2048", "-nodes",
                 "-keyout", f"{name}.key", "-out", f"{name}.csr",
                 "-subj", "/CN=localhost",
                 "-addext", f"subjectAltName={san}"],
                workdir)
    run_openssl(["x509", "-req", "-in", f"{name}.csr",
                 "-CA", ca_crt, "-CAkey", ca_key, "-CAcreateserial",
                 "-out", f"{name}.crt", "-days", "2",
                 "-copy_extensions", "copy"], workdir)
    return (os.path.join(workdir, f"{name}.crt"),
            os.path.join(workdir, f"{name}.key"))


def member_tls_args(workdir, ca_crt, ca_key, node_id, name_prefix="member",
                    san_ip="127.0.0.1", principal_id=None):
    """Generate one endpoint + identity-bound Raft member certificate."""
    os.makedirs(workdir, exist_ok=True)
    principal_id = node_id if principal_id is None else principal_id
    cert, key = make_leaf(
        workdir, ca_crt, ca_key, f"{name_prefix}-{node_id}",
        san=(f"IP:{san_ip},DNS:localhost,"
             f"URI:lavik://meta/{principal_id}"))
    return H.raft_args() + H.tls_args(ca_crt, cert, key)


def make_tls_nodes(binary, workdir, count, ca_crt, ca_key, first_id=1):
    cert_dir = os.path.join(workdir, "member_certs")
    os.makedirs(cert_dir, exist_ok=True)
    return [H.Node(binary, workdir, node_id,
                   args=member_tls_args(cert_dir, ca_crt, ca_key, node_id))
            for node_id in range(first_id, first_id + count)]


def make_expired_leaf(workdir, ca_dir, ca_crt, ca_key, name, node_id):
    """Sign a leaf with a validity window entirely in the past, using
    `openssl ca -startdate/-enddate` (OpenSSL 1.1.1+). Raises Failure when
    the host openssl cannot; the caller turns that into a SKIP."""
    run_openssl(["req", "-newkey", "rsa:2048", "-nodes",
                 "-keyout", f"{name}.key", "-out", f"{name}.csr",
                 "-subj", "/CN=localhost",
                 "-addext", ("subjectAltName=IP:127.0.0.1,DNS:localhost,"
                             f"URI:lavik://meta/{node_id}")],
                workdir)
    index = os.path.join(ca_dir, "index.txt")
    serial = os.path.join(ca_dir, "serial")
    with open(index, "w"):
        pass
    with open(serial, "w") as handle:
        handle.write("1000\n")
    config = os.path.join(ca_dir, "openssl.cnf")
    with open(config, "w") as handle:
        handle.write(f"""
[ ca ]
default_ca = CA_default
[ CA_default ]
dir = {ca_dir}
database = $dir/index.txt
new_certs_dir = $dir
certificate = $dir/ca.crt
private_key = $dir/ca.key
serial = $dir/serial
default_md = sha256
policy = policy_loose
copy_extensions = copy
[ policy_loose ]
commonName = supplied
""")
    run_openssl(["ca", "-batch", "-config", config,
                 "-startdate", "20200101000000Z",
                 "-enddate", "20210101000000Z",
                 "-in", os.path.join(workdir, f"{name}.csr"),
                 "-out", os.path.join(workdir, f"{name}.crt")], workdir)
    return (os.path.join(workdir, f"{name}.crt"),
            os.path.join(workdir, f"{name}.key"))


def expect_isolated(leader, joiner, history, seq_start, label,
                    evidence_patterns, recovery_args):
    """Invite `joiner`, then prove for ISOLATION_WINDOW_S that the quorum
    keeps committing while the joiner makes zero raft progress."""
    if isinstance(evidence_patterns, str):
        evidence_patterns = (evidence_patterns,)
    evidence_label = "|".join(evidence_patterns)
    # Snapshot the evidence before inviting the peer. The first TLS failure
    # can be logged before the successful addsrv reply reaches this process;
    # sampling afterward would misclassify that real failure as old evidence.
    evidence0 = int(leader.status()["rpc_failures"])
    invite = leader.ctl(
        f"addsrv {joiner.id} {joiner.endpoint} "
        f"{joiner.data_control_endpoint} {joiner.ctl_endpoint}")
    H.log(f"{label}: addsrv node {joiner.id} -> {invite}")
    pending = re.fullmatch(r"ERR uncertain-outcome operation=([0-9a-f]{32})", invite)
    if pending is None:
        raise H.Failure(f"{label}: addsrv: {invite}")
    operation_id = pending.group(1)
    committed0 = leader.committed()

    seq = seq_start
    last_ok_id = None
    deadline = time.monotonic() + ISOLATION_WINDOW_S
    while time.monotonic() < deadline:
        op_id, reply = leader.propose(f"{label}{seq}")
        if not reply.startswith("OK "):
            raise H.Failure(f"{label}: quorum write failed mid-isolation: "
                            f"{reply}")
        history.record(op_id, f"{label}{seq}")
        last_ok_id = op_id
        seq += 1
        time.sleep(0.25)

    # The joiner never handshakes, so it never hears a heartbeat, never
    # campaigns (skip_initial_election_timeout_), and never commits.
    status = joiner.status()
    if status["committed"] != "0" or status["leader"] != "0":
        raise H.Failure(f"{label}: isolated node {joiner.id} made raft "
                        f"progress: {status}")
    if joiner.getop(last_ok_id) != "ERR not-found":
        raise H.Failure(f"{label}: isolated node {joiner.id} serves data")
    if not joiner.alive():
        raise H.Failure(f"{label}: isolated node {joiner.id} DIED "
                        f"(exit {joiner.proc.returncode})")
    committed1 = leader.committed()
    if committed1 <= committed0:
        raise H.Failure(f"{label}: quorum committed stalled "
                        f"({committed0} -> {committed1})")
    # The retained operation retries asynchronously, so wait for bounded new evidence rather than
    # racing the log writer at the end of the isolation window.
    evidence_deadline = time.monotonic() + 5.0
    evidence1 = int(leader.status()["rpc_failures"])
    while evidence1 <= evidence0 and time.monotonic() < evidence_deadline:
        time.sleep(0.05)
        evidence1 = int(leader.status()["rpc_failures"])
    if evidence1 <= evidence0:
        raise H.Failure(f"{label}: no '{evidence_label}' evidence in "
                        f"transport counters")
    H.log(f"{label}: node {joiner.id} isolated (alive, committed=0), "
          f"quorum committed {committed0} -> {committed1}, transport counters "
          f"'{evidence_label}' failures {evidence0} -> {evidence1}")
    # Fix only the joiner's transport credentials. No replacement addsrv:
    # the original durable task must resume after authentication succeeds.
    joiner.terminate()
    joiner.args = recovery_args
    joiner.start(bootstrap=False)
    H.wait_until(f"{label}: original invite completes after credential repair", 30,
                 lambda: leader.getop(operation_id) == "OK completed member-added")
    history.check([joiner], timeout=20, desc=f"{label}: repaired joiner")
    retire_replies = []

    def terminally_retired():
        retire_replies[:] = [leader.ctl(f"removesrv {joiner.id}")]
        return retire_replies == ["OK"]

    # The durable workflow may still be releasing its membership reservation.
    # Wait for the stable, idempotent retirement result.
    try:
        H.wait_until(f"{label}: repaired member retires", 5,
                     terminally_retired)
    except H.Failure as error:
        raise H.Failure(
            f"{label}: repaired member did not retire: {retire_replies}") from error
    joiner.terminate()
    H.log(f"{label}: same operation recovered after credential repair and member retired")
    return seq


def main():
    workdir, keep = H.make_workdir(sys.argv, "meta_integration_mtls_")
    started = time.monotonic()
    # One CommittedHistory per cluster: keys proposed on one cluster must
    # never be checked on another.
    hist_a = H.CommittedHistory()
    hist_b = H.CommittedHistory()
    hist_c = H.CommittedHistory()
    clusters = []   # (nodes, history) for teardown
    joiners = []    # isolated nodes, for teardown + log dumps
    try:
        main_ca_dir = os.path.join(workdir, "main_ca")
        main_ca, main_ca_key = make_ca(main_ca_dir, "Lavik-Gate-Main-CA")

        # ---- scenario P: full positive flow on an all-TLS cluster -------
        dir_a = os.path.join(workdir, "tls_cluster")
        os.makedirs(dir_a, exist_ok=True)
        cluster_a = make_tls_nodes(BINARY, dir_a, 3, main_ca, main_ca_key)
        clusters.append((cluster_a, hist_a))
        leader = H.bootstrap_cluster(cluster_a)
        H.log("P: 3-node mTLS cluster converged")
        last = H.propose_ops(leader, 0, 30, prefix="mt", history=hist_a)
        H.wait_cluster_committed(cluster_a, last, timeout=20)
        for node in cluster_a:
            H.wait_until(f"node {node.id} automatic snapshot", 20,
                         lambda node=node: node.snapshot_idx() > 0)
        hist_a.check(cluster_a, timeout=30, desc="P history")
        follower = next(n for n in cluster_a if n.id != leader.id)
        pre = follower.committed()
        follower.kill9()
        follower.start(bootstrap=False)
        H.wait_no_regress(follower, pre, timeout=20)
        hist_a.check(cluster_a, timeout=30, desc="P post-crash")
        H.log(f"P: 30 keys, snapshots, node {follower.id} crash/restart "
              f"catch-up over mTLS — OK")

        # ---- scenario N1a: plaintext joiner vs TLS cluster --------------
        plain_joiner = H.Node(BINARY, dir_a, 4, args=H.raft_args())
        joiners.append(plain_joiner)
        plain_joiner.start(bootstrap=False)
        expect_isolated(leader, plain_joiner, hist_a, 0, "N1a-plaintext",
                        "SSL handshake", member_tls_args(
                            os.path.join(workdir, "repaired_plain"), main_ca, main_ca_key, 4))

        # ---- scenario N2: wrong-CA joiner vs TLS cluster ----------------
        wrong_dir = os.path.join(workdir, "wrong_ca")
        wrong_ca, wrong_key = make_ca(wrong_dir, "Lavik-Wrong-CA")
        wrong_crt, wrong_leaf_key = make_leaf(
            wrong_dir, wrong_ca, wrong_key, "server",
            san="IP:127.0.0.1,DNS:localhost,URI:lavik://meta/5")
        wrong_args = H.raft_args() + H.tls_args(wrong_ca, wrong_crt,
                                                wrong_leaf_key)
        wrong_joiner = H.Node(BINARY, dir_a, 5, args=wrong_args)
        joiners.append(wrong_joiner)
        wrong_joiner.start(bootstrap=False)
        expect_isolated(leader, wrong_joiner, hist_a, 100,
                        "N2-wrong-ca", "SSL handshake", member_tls_args(
                            os.path.join(workdir, "repaired_ca"), main_ca, main_ca_key, 5))

        # ---- scenario N1b: TLS joiner vs plaintext cluster --------------
        dir_b = os.path.join(workdir, "plain_cluster")
        os.makedirs(dir_b, exist_ok=True)
        cluster_b = H.make_nodes(BINARY, dir_b, 3, args=H.raft_args())
        clusters.append((cluster_b, hist_b))
        plain_leader = H.bootstrap_cluster(cluster_b)
        op_id, reply = plain_leader.propose("warmup")
        if not reply.startswith("OK "):
            raise H.Failure(f"plaintext cluster warmup: {reply}")
        hist_b.record(op_id, "warmup")
        tls_joiner_args = member_tls_args(
            os.path.join(workdir, "main_joiner"), main_ca, main_ca_key, 4)
        tls_joiner = H.Node(BINARY, dir_b, 4, args=tls_joiner_args)
        joiners.append(tls_joiner)
        tls_joiner.start(bootstrap=False)
        # The plaintext leader's client fails against the TLS listener; the
        # transport-failure counter records the bounded handshake failure.
        expect_isolated(plain_leader, tls_joiner, hist_b, 200,
                        "N1b-tls-joiner", "rpc error response", H.raft_args())

        # ---- cluster C: nodes hold SAN=127.0.0.1 leaves from ca2 --------
        # N3 and N4 share this cluster: both need a CA the cluster trusts
        # AND whose private key the gate controls (tests/tls ships no
        # ca.key), so the gate builds ca2 itself.
        ca2_dir = os.path.join(workdir, "ca2")
        ca2_crt, ca2_key = make_ca(ca2_dir, "Lavik-Gate6-CA2")
        dir_c = os.path.join(workdir, "ca2_cluster")
        os.makedirs(dir_c, exist_ok=True)
        cluster_c = make_tls_nodes(BINARY, dir_c, 3, ca2_crt, ca2_key)
        clusters.append((cluster_c, hist_c))
        leader_c = H.bootstrap_cluster(cluster_c)

        # ---- scenario N3: expired certificate (best effort) -------------
        try:
            expired_crt, expired_key = make_expired_leaf(
                workdir, ca2_dir, ca2_crt, ca2_key, "expired", 4)
        except H.Failure as exc:
            H.log(f"N3-expired: SKIP ({exc})")
        else:
            expired_args = H.raft_args() + H.tls_args(ca2_crt, expired_crt,
                                                      expired_key)
            expired_joiner = H.Node(BINARY, dir_c, 4, args=expired_args)
            joiners.append(expired_joiner)
            expired_joiner.start(bootstrap=False)
            expect_isolated(leader_c, expired_joiner, hist_c, 300,
                            "N3-expired", "SSL handshake", member_tls_args(
                                os.path.join(workdir, "repaired_expired"), ca2_crt, ca2_key, 4))

        # ---- scenario N4: trusted CA, wrong SAN --------------------------
        # The joiner's leaf IS signed by the CA the cluster trusts, so the
        # chain verifies; the rejection comes from the client-side peer_name
        # check: the endpoint host is 127.0.0.1 but the cert's SAN only
        # covers 10.9.9.9.
        badsan_crt, badsan_key = make_leaf(workdir, ca2_crt, ca2_key,
                                           "badsan",
                                           san=("IP:10.9.9.9,"
                                                "URI:lavik://meta/5"))
        badsan_args = H.raft_args() + H.tls_args(ca2_crt, badsan_crt,
                                                 badsan_key)
        badsan_joiner = H.Node(BINARY, dir_c, 5, args=badsan_args)
        joiners.append(badsan_joiner)
        badsan_joiner.start(bootstrap=False)
        expect_isolated(leader_c, badsan_joiner, hist_c, 400,
                        "N4-wrong-san", "SSL handshake", member_tls_args(
                            os.path.join(workdir, "repaired_san"), ca2_crt, ca2_key, 5))

        # ---- scenario N5: valid endpoint, wrong member URI binding ------
        wrongid_args = member_tls_args(
            os.path.join(workdir, "wrong_identity"), ca2_crt, ca2_key, 6,
            name_prefix="wrong-id", principal_id=99)
        wrongid_joiner = H.Node(BINARY, dir_c, 6, args=wrongid_args)
        joiners.append(wrongid_joiner)
        # The local identity check now fails before ingress. Verify that the
        # mismatched certificate cannot start, then use the corrected identity
        # in a real join to exercise peer authentication as well.
        wrongid_joiner.start(bootstrap=False, wait_ready=False)
        H.wait_until("wrong local principal fails startup", 10,
                     lambda: not wrongid_joiner.alive())
        if wrongid_joiner.proc.returncode == 0:
            raise H.Failure("wrong member certificate was accepted")
        wrongid_joiner.args = member_tls_args(
            os.path.join(workdir, "repaired_id"), ca2_crt, ca2_key, 6)
        wrongid_joiner.start(bootstrap=False)
        H.join_and_verify(leader_c, wrongid_joiner)
        H.wait_until("repaired member retires", 10,
                     lambda: leader_c.ctl("removesrv 6") == "OK")
        wrongid_joiner.terminate()
        H.log("N5: wrong local principal rejected; corrected peer joined and retired")

        # ---- teardown: members must SIGTERM cleanly; the isolated joiners
        # never joined, so shutting them down cleanly is asserted too.
        for node in joiners:
            node.terminate()
        for nodes, history in clusters:
            history.check(nodes, timeout=30, desc="final")
            H.assert_intact(nodes, "final")
            for node in nodes:
                node.terminate()

        elapsed = time.monotonic() - started
        H.log(f"PASS in {elapsed:.1f}s")
        return 0
    except Exception as exc:  # noqa: BLE001 - dump everything on failure
        print(f"[gate-mtls] FAIL: {exc}", file=sys.stderr)
        all_nodes = [n for nodes, _ in clusters for n in nodes] + joiners
        H.dump_node_logs(all_nodes)
        return 1
    finally:
        for node in joiners:
            node.force_kill()
        for nodes, _ in clusters:
            for node in nodes:
                node.force_kill()
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    BINARY = sys.argv[1]
    H.set_tag("gate-mtls")
    sys.exit(main())
