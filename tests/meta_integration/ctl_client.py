#!/usr/bin/env python3
"""End-to-end gate for keylane-meta-ctl over Unix and TCP transports.

Usage: ctl_client.py /path/to/keylane-meta /path/to/keylane-meta-ctl [workdir]
"""

import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402


def run(args, expected=0, timeout=10):
    proc = subprocess.run([CTL] + args, capture_output=True, text=True,
                          timeout=timeout)
    if proc.returncode != expected:
        raise H.Failure(
            f"ctl exit {proc.returncode}, want {expected}: "
            f"stdout={proc.stdout!r} stderr={proc.stderr!r}")
    return proc.stdout.strip()


def make_leaf(directory, ca_crt, ca_key, name, san):
    key = os.path.join(directory, f"{name}.key")
    csr = os.path.join(directory, f"{name}.csr")
    cert = os.path.join(directory, f"{name}.crt")
    commands = [
        ["openssl", "req", "-newkey", "rsa:2048", "-nodes", "-sha256",
         "-subj", f"/CN={name}", "-addext", f"subjectAltName={san}",
         "-addext", "extendedKeyUsage=serverAuth,clientAuth",
         "-keyout", key, "-out", csr],
        ["openssl", "x509", "-req", "-sha256", "-days", "2", "-in", csr,
         "-CA", ca_crt, "-CAkey", ca_key, "-CAcreateserial",
         "-copy_extensions", "copy", "-out", cert],
    ]
    for command in commands:
        proc = subprocess.run(command, capture_output=True, text=True,
                              timeout=30)
        if proc.returncode != 0:
            raise H.Failure(f"{' '.join(command[:3])}: {proc.stderr}")
    return cert, key


def unix_gate(workdir):
    directory = os.path.join(workdir, "unix")
    os.makedirs(directory, exist_ok=True)
    node = H.Node(META, directory, 1)
    try:
        node.start(bootstrap=True)
        H.wait_until("Unix ctl leader", 10, node.is_leader)
        status = run(["--socket", node.ctl_path, "status"])
        if not status.startswith("OK leader=1 "):
            raise H.Failure(f"unexpected Unix status: {status}")

        op_id = "00000001000000000000000000000001"
        reply = run(["--socket", node.ctl_path, "submitop", op_id,
                     "ctl-gate", "unix"])
        if not reply.startswith("OK "):
            raise H.Failure(f"Unix submitop: {reply}")
        operation_seq = reply.split()[1]
        if run(["--socket", node.ctl_path, "getop", op_id]) != "OK submitted":
            raise H.Failure("Unix getop did not observe the committed command")
        aborted = run(["--socket", node.ctl_path, "abortop", op_id])
        if not aborted.startswith("OK "):
            raise H.Failure(f"Unix abortop: {aborted}")
        archived = run(["--socket", node.ctl_path, "archiveoperations",
                        operation_seq])
        if not archived.startswith("OK "):
            raise H.Failure(f"Unix archiveoperations: {archived}")
        pruned = run(["--socket", node.ctl_path, "pruneoperations",
                      operation_seq])
        if not pruned.startswith("OK "):
            raise H.Failure(f"Unix pruneoperations: {pruned}")
        if run(["--socket", node.ctl_path, "getop", op_id], expected=2) != \
                "ERR not-found":
            raise H.Failure("operation recovery sequence did not prune state")

        if run(["--socket", node.ctl_path, "unknown"], expected=2) != \
                "ERR unknown-command":
            raise H.Failure("ERR reply did not produce exit status 2")
        H.log("keylane-meta-ctl Unix transport and exit statuses — OK")
    finally:
        node.terminate()


def plaintext_gate(workdir):
    directory = os.path.join(workdir, "plaintext")
    data_dir = os.path.join(directory, "node1")
    os.makedirs(data_dir, mode=0o700, exist_ok=True)
    raft_port = H.free_port()
    data_control_port = H.free_port()
    ctl_port = H.free_port()
    log_path = os.path.join(directory, "node1.log")
    log_file = open(log_path, "wb")
    server = subprocess.Popen(
        [META, "--id", "1", "--addr", f"127.0.0.1:{raft_port}",
         "--data-control-addr", f"127.0.0.1:{data_control_port}",
         "--data-dir", data_dir, "--bootstrap",
         "--ctl-addr", f"127.0.0.1:{ctl_port}"] + H.raft_args(),
        stdout=log_file, stderr=subprocess.STDOUT)
    client_args = ["--addr", f"127.0.0.1:{ctl_port}"]
    try:
        def ready():
            if server.poll() is not None:
                raise H.Failure(
                    f"plaintext server exited with {server.returncode}")
            result = subprocess.run(
                [CTL] + client_args + ["status"], capture_output=True,
                text=True, timeout=3)
            return (result.returncode == 0 and
                    result.stdout.startswith("OK leader=1 "))

        H.wait_until("plaintext ctl listener", 15, ready)
        status = run(client_args + ["status"])
        if not status.startswith("OK leader="):
            raise H.Failure(f"unexpected plaintext status: {status}")

        op_id = "00000002000000000000000000000001"
        reply = run(client_args + ["submitop", op_id, "ctl-gate", "plain"])
        if not reply.startswith("OK "):
            raise H.Failure(f"plaintext submitop: {reply}")
        export = run(client_args + ["exportaudit", reply.split()[1]])
        if not export.startswith("OK "):
            raise H.Failure(f"plaintext exportaudit: {export}")
        if b"keylane://operator/plaintext" not in bytes.fromhex(export[3:]):
            raise H.Failure("plaintext audit actor was not persisted")
        H.log("keylane-meta-ctl remote plaintext transport — OK")
    finally:
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
        log_file.close()
        if server.returncode not in (0, -15):
            with open(log_path, errors="replace") as handle:
                H.log(handle.read()[-4000:])


def mtls_gate(workdir):
    directory = os.path.join(workdir, "mtls")
    data_dir = os.path.join(directory, "node1")
    os.makedirs(data_dir, mode=0o700, exist_ok=True)
    ca_key = os.path.join(directory, "ca.key")
    ca_crt = os.path.join(directory, "ca.crt")
    proc = subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-sha256", "-days", "2", "-subj", "/CN=ctl-gate-ca",
         "-addext", "basicConstraints=critical,CA:TRUE",
         "-keyout", ca_key, "-out", ca_crt],
        capture_output=True, text=True, timeout=30)
    if proc.returncode != 0:
        raise H.Failure(f"create ctl CA: {proc.stderr}")

    server_cert, server_key = make_leaf(
        directory, ca_crt, ca_key, "server",
        "IP:127.0.0.1,URI:keylane://meta/1")
    client_cert, client_key = make_leaf(
        directory, ca_crt, ca_key, "operator",
        "URI:keylane://operator/ctl-gate")
    raft_port = H.free_port()
    data_control_port = H.free_port()
    ctl_port = H.free_port()
    log_path = os.path.join(directory, "node1.log")
    log_file = open(log_path, "wb")
    server = subprocess.Popen(
        [META, "--id", "1", "--addr", f"127.0.0.1:{raft_port}",
         "--data-control-addr", f"127.0.0.1:{data_control_port}",
         "--data-dir", data_dir, "--bootstrap",
         "--ctl-addr", f"127.0.0.1:{ctl_port}",
         "--ctl-tls-ca", ca_crt, "--ctl-tls-cert", server_cert,
         "--ctl-tls-key", server_key] + H.raft_args(),
        stdout=log_file, stderr=subprocess.STDOUT)
    client_args = [
        "--addr", f"127.0.0.1:{ctl_port}", "--tls-ca", ca_crt,
        "--tls-cert", client_cert, "--tls-key", client_key,
    ]
    try:
        def ready():
            if server.poll() is not None:
                raise H.Failure(f"mTLS server exited with {server.returncode}")
            result = subprocess.run([CTL] + client_args + ["status"],
                                    capture_output=True, text=True, timeout=3)
            return result.returncode == 0

        H.wait_until("mTLS ctl listener", 15, ready)
        status = run(client_args + ["status"])
        if not status.startswith("OK leader="):
            raise H.Failure(f"unexpected mTLS status: {status}")
        wrong_name = subprocess.run(
            [CTL] + client_args + ["--tls-server-name", "wrong.invalid",
                                   "status"],
            capture_output=True, text=True, timeout=10)
        if wrong_name.returncode != 1:
            raise H.Failure(
                "mTLS server-name mismatch was not rejected: "
                f"exit={wrong_name.returncode} stdout={wrong_name.stdout!r} "
                f"stderr={wrong_name.stderr!r}")
        H.log("keylane-meta-ctl remote mTLS transport — OK")
    finally:
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
        log_file.close()
        if server.returncode not in (0, -15):
            with open(log_path, errors="replace") as handle:
                H.log(handle.read()[-4000:])


def main():
    harness_argv = [sys.argv[0], META] + sys.argv[3:]
    workdir, keep = H.make_workdir(harness_argv, "meta_ctl_client_")
    try:
        unix_gate(workdir)
        plaintext_gate(workdir)
        mtls_gate(workdir)
    except Exception:
        H.log(f"FAIL; artifacts kept at {workdir}")
        keep = True
        raise
    finally:
        H.cleanup(workdir, keep)


if len(sys.argv) < 3:
    print(__doc__, file=sys.stderr)
    sys.exit(2)
META = os.path.abspath(sys.argv[1])
CTL = os.path.abspath(sys.argv[2])
H.set_tag("meta-ctl-client")

try:
    main()
except (H.Failure, OSError, subprocess.SubprocessError) as error:
    H.log(f"FAIL: {error}")
    sys.exit(1)
