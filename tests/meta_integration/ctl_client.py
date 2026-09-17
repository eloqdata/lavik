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

"""End-to-end gate for lavik-ctl over Unix and TCP transports.

Usage: ctl_client.py /path/to/lavik-meta /path/to/lavik-ctl [workdir]
"""

import os
import re
import socket
import struct
import subprocess
import sys
import threading
import time

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


def run_cluster(args, expected, timeout=10, input_text=None, env=None):
    proc = subprocess.run([CTL] + args, capture_output=True, text=True,
                          timeout=timeout, input=input_text, env=env)
    if proc.returncode != expected:
        raise H.Failure(
            f"cluster exit {proc.returncode}, want {expected}: "
            f"stdout={proc.stdout!r} stderr={proc.stderr!r}")
    return proc


def healthy_automatic_failover_status():
    """Encode the clusterstatus v1 Group diagnostics for a healthy detector."""
    # state, current_reason presence, suspect/threshold ms,
    # blocked_reason presence.
    return struct.pack(">BBQQB", 1, 0, 0, 1000, 0)


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


def ctl_characterization_gate(workdir):
    help_result = subprocess.run(
        [CTL, "--help"], capture_output=True, text=True, timeout=3)
    if (help_result.returncode != 0 or help_result.stdout or
            "Exit status is 0 for an OK reply, 2 for an ERR reply" not in
            help_result.stderr):
        raise H.Failure(
            "lavik-ctl help contract changed: "
            f"exit={help_result.returncode} stdout={help_result.stdout!r} "
            f"stderr={help_result.stderr!r}")
    bad_args = subprocess.run(
        [CTL, "--addr", "127.0.0.1:1"], capture_output=True, text=True,
        timeout=3)
    if (bad_args.returncode != 1 or bad_args.stdout or
            "a Meta command is required" not in bad_args.stderr):
        raise H.Failure(
            "lavik-ctl argument contract changed: "
            f"exit={bad_args.returncode} stdout={bad_args.stdout!r} "
            f"stderr={bad_args.stderr!r}")

    path = os.path.join(workdir, "ctl-deadline.sock")
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(path)
    listener.listen(1)
    release = threading.Event()
    errors = []

    def stall():
        try:
            connection, _ = listener.accept()
            with connection:
                connection.recv(4096)
                release.wait(timeout=2)
        except OSError as error:
            if not release.is_set():
                errors.append(error)

    thread = threading.Thread(target=stall, daemon=True)
    thread.start()
    started = time.monotonic()
    try:
        deadline = subprocess.run(
            [CTL, "--socket", path, "--timeout-ms", "20", "status"],
            capture_output=True, text=True, timeout=3)
    finally:
        release.set()
        listener.close()
        thread.join(timeout=2)
    elapsed = time.monotonic() - started
    if (deadline.returncode != 1 or deadline.stdout or
            "timed out" not in deadline.stderr or elapsed > 1.0 or
            thread.is_alive() or errors):
        raise H.Failure(
            "lavik-ctl absolute deadline contract changed: "
            f"elapsed={elapsed:.3f}s exit={deadline.returncode} "
            f"stdout={deadline.stdout!r} stderr={deadline.stderr!r} "
            f"server_errors={errors}")
    H.log("lavik-ctl help, arguments, deadline, and exit contract — OK")


def raw_argument_gate(workdir):
    """Client-only options must not consume raw command operands."""
    path = os.path.join(workdir, "raw-arguments.sock")
    cases = [
        ["submitop", "id", "kind", "--json"],
        ["submitop", "id", "kind", "--allow-plaintext-admin"],
        ["submitop", "id", "kind", "cluster-status"],
        ["status", "--timeout-ms", "1"],
        ["--", "--help", "--json"],
        ["--", "cluster-status", "--json"],
    ]
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(path)
    listener.listen(len(cases))
    listener.settimeout(3)
    requests = []
    errors = []

    def serve():
        try:
            for _ in cases:
                connection, _ = listener.accept()
                with connection:
                    reader = connection.makefile("rb")
                    requests.append(reader.readline().decode().rstrip("\n"))
                    connection.sendall(b"OK raw-arguments\n")
        except OSError as error:
            errors.append(error)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        for arguments in cases:
            reply = run(["--socket", path] + arguments)
            if reply != "OK raw-arguments":
                raise H.Failure(f"raw argument reply: {reply!r}")
    finally:
        listener.close()
        thread.join(timeout=4)
    expected = [" ".join(args[1:] if args[0] == "--" else args)
                for args in cases]
    if thread.is_alive() or errors or requests != expected:
        raise H.Failure(
            f"raw argument boundary changed: requests={requests!r} "
            f"expected={expected!r} errors={errors}")
    H.log("raw command arguments and option terminator — OK")


def scripted_cluster_gate(workdir):
    """Drive the real CLI through its public transport with scripted wire.

    Production cannot mint the first ReadyToken yet, so a minimal valid
    operator peer is the only way to exercise external READY/0 without adding
    a production bypass.
    """
    directory = os.path.join(workdir, "scripted")
    os.makedirs(directory, mode=0o700, exist_ok=True)

    def wire_string(value):
        encoded = value.encode()
        return struct.pack(">I", len(encoded)) + encoded

    def lifecycle(state, revision, root=None, genesis=None, phase=None,
                  failure=None):
        payload = bytes([state]) + struct.pack(">Q", revision)
        payload += bytes([root is not None])
        if root is not None:
            payload += wire_string(root)
        payload += bytes([genesis is not None])
        if genesis is not None:
            payload += struct.pack(">Q", genesis)
        for value in (phase, failure):
            payload += bytes([value is not None])
            if value is not None:
                payload += wire_string(value)
        return payload

    member = struct.pack(">IBB", 1, 0, 1)
    head_payload = (
        struct.pack(">HIBQBIQI", 1, 1, 1, 1, 1, 1, 1, 1) + member)
    data_node = (
        wire_string("data-1") + bytes([0, 0, 1]) +
        wire_string("group-1") + bytes([1, 1, 1, 1, 0]))
    group = (
        wire_string("group-1") + struct.pack(">Q", 4) + bytes([1]) +
        wire_string("data-1") + struct.pack(">BB", 1, 1) +
        healthy_automatic_failover_status())
    slot_range = struct.pack(">II", 0, 16_383) + wire_string("group-1")
    status_payload = (
        struct.pack(">HIQQQQ", 1, 1, 1, 1, 1, 1) +
        lifecycle(2, 2, "00112233445566778899aabbccddeeff", 1) +
        bytes([1, 1, 1, 1, 1]) + struct.pack(">I", 1) + member +
        struct.pack(">I", 1) + data_node +
        struct.pack(">I", 1) + group +
        struct.pack(">I", 1) + slot_range + struct.pack(">I", 0))
    ready_replies = {
        "clusterhead 1": "OK clusterhead 1 " + head_payload.hex(),
        "clusterstatus 1": "OK clusterstatus 1 " + status_payload.hex(),
    }

    def run_server(name, responder, cli_args, expected, terminate_reply=True,
                   options_first=False):
        path = os.path.join(directory, name + ".sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(path)
        listener.listen(8)
        listener.settimeout(0.1)
        stopped = threading.Event()
        errors = []

        def serve():
            while not stopped.is_set():
                try:
                    connection, _ = listener.accept()
                except socket.timeout:
                    continue
                except OSError as error:
                    if not stopped.is_set():
                        errors.append(error)
                    return
                with connection:
                    request = b""
                    while not request.endswith(b"\n"):
                        chunk = connection.recv(4096)
                        if not chunk:
                            break
                        request += chunk
                    reply = responder(request.decode().strip())
                    terminator = b"\n" if terminate_reply else b""
                    connection.sendall(reply.encode() + terminator)

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        try:
            arguments = (["--socket", path] + cli_args + ["cluster-status"]
                         if options_first else
                         ["cluster-status", "--socket", path] + cli_args)
            result = run_cluster(arguments, expected=expected)
        finally:
            stopped.set()
            listener.close()
            thread.join(timeout=2)
        if thread.is_alive() or errors:
            raise H.Failure(f"scripted cluster server failed: {errors}")
        return result

    ready = run_server(
        "ready", lambda command: ready_replies.get(command, "ERR bad-request"),
        ["--json"], expected=0)
    if '"result":"ready"' not in ready.stdout:
        raise H.Failure(f"scripted READY output missing: {ready.stdout!r}")

    # Both command-first and traditional global-options-first invocations
    # reach the client command, without changing raw `status` semantics.
    options_first = run_server(
        "options-first", lambda command: ready_replies.get(command, "ERR bad-request"),
        ["--json"], expected=0, options_first=True)
    if '"result":"ready"' not in options_first.stdout:
        raise H.Failure(f"options-first READY output missing: {options_first.stdout!r}")

    for arguments, error in [
            (["cluster-status", "--socket", "/unused", "unexpected"],
             "does not take positional arguments"),
            (["--socket", "/unused", "cluster-status", "unexpected"],
             "does not take positional arguments"),
            (["cluster-status", "--socket", "/unused", "--tls-ca", "ca.pem"],
             "must be given together"),
            (["cluster-status", "--addr", "127.0.0.1:1", "--tls-ca", "ca.pem",
              "--tls-cert", "cert.pem", "--tls-key", "key.pem",
              "--tls-server-name", "meta.example"],
             "does not accept --tls-server-name"),
    ]:
        rejected = run_cluster(arguments, expected=1)
        if rejected.stdout or error not in rejected.stderr:
            raise H.Failure(
                f"invalid cluster arguments were not stdout-clean: {arguments!r} "
                f"stdout={rejected.stdout!r} stderr={rejected.stderr!r}")

    retry = run_server(
        "retry", lambda _: "ERR busy",
        ["--timeout-ms", "20", "--json"], expected=3)
    if '"result":"retryable"' not in retry.stdout:
        raise H.Failure(f"scripted RETRYABLE output missing: {retry.stdout!r}")
    truncated = run_server(
        "truncated", lambda _: "OK clusterhead 1 ",
        ["--timeout-ms", "20", "--json"], expected=1,
        terminate_reply=False)
    if (truncated.stdout or
            "terminating its reply" not in truncated.stderr or
            "status_explanation=" not in truncated.stderr or
            "next_action=" not in truncated.stderr or
            "preserve Meta data" not in truncated.stderr):
        raise H.Failure(
            "truncated cluster reply omitted fatal status guidance: "
            f"stdout={truncated.stdout!r} stderr={truncated.stderr!r}")
    empty_close = run_server(
        "empty-close", lambda _: "", ["--timeout-ms", "20", "--json"],
        expected=3, terminate_reply=False)
    if ('"result":"retryable"' not in empty_close.stdout or
            '"status_explanation":' not in empty_close.stdout or
            '"next_action":' not in empty_close.stdout):
        raise H.Failure(
            "empty connection close omitted retry guidance: "
            f"stdout={empty_close.stdout!r} stderr={empty_close.stderr!r}")
    H.log("lavik-ctl cluster-status scripted READY/0 and RETRYABLE/3 — OK")


def scripted_cluster_create_gate(workdir):
    """Pin confirmation, Genesis routing, and exit semantics."""
    directory = os.path.join(workdir, "scripted-create")
    os.makedirs(directory, mode=0o700, exist_ok=True)
    node_id = "0123456789abcdef0123456789abcdef01234567"
    manifest = os.path.join(directory, "cluster.toml")
    with open(manifest, "w", encoding="utf-8") as output:
        output.write(
            "schema_version = 1\n\n"
            "[[meta_members]]\nid = 1\n"
            'raft_endpoint = "tcp://127.0.0.1:7001"\n'
            'data_control_endpoint = "tcp://127.0.0.1:7101"\n'
            'ctl_endpoint = "tcp://127.0.0.1:7201"\n\n'
            "[[data_nodes]]\n"
            f'id = "{node_id}"\n'
            'client_endpoint = "tcp://127.0.0.1:6379"\n\n'
            "[[groups]]\nid = \"group-1\"\n"
            f'primary = "{node_id}"\n\n'
            "[[slot_ranges]]\nfirst = 0\nlast = 16383\n"
            'group = "group-1"\n')

    # Confirmation is a local safety barrier: even a case change or EOF exits
    # before leader discovery and therefore before any possible Meta write.
    missing_socket = os.path.join(directory, "must-not-connect.sock")
    cancelled = run_cluster(
        ["cluster-create", "--manifest", manifest, "--socket", missing_socket],
        expected=1, input_text="Yes\n")
    if ("Type yes to continue:" not in cancelled.stdout or
            "no mutation was sent" not in cancelled.stderr):
        raise H.Failure(
            "cluster-create did not enforce exact lowercase confirmation: "
            f"stdout={cancelled.stdout!r} stderr={cancelled.stderr!r}")
    unreachable = run_cluster(
        ["cluster-create", "--manifest", manifest, "--socket", missing_socket,
         "--yes", "--timeout-ms", "50"], expected=1)
    if ("before sending a mutation" not in unreachable.stderr or
            "partially committed" in unreachable.stderr):
        raise H.Failure(
            "pre-mutation connection failure used the uncertain exit path: "
            f"stdout={unreachable.stdout!r} stderr={unreachable.stderr!r}")

    def wire_string(value):
        encoded = value.encode()
        return struct.pack(">I", len(encoded)) + encoded

    def lifecycle(state, revision, root=None, genesis=None, phase=None,
                  failure=None):
        payload = bytes([state]) + struct.pack(">Q", revision)
        payload += bytes([root is not None])
        if root is not None:
            payload += wire_string(root)
        payload += bytes([genesis is not None])
        if genesis is not None:
            payload += struct.pack(">Q", genesis)
        for value in (phase, failure):
            payload += bytes([value is not None])
            if value is not None:
                payload += wire_string(value)
        return payload

    member = struct.pack(">IBB", 1, 0, 1)
    head_payload = (
        struct.pack(">HIBQBIQI", 1, 1, 1, 1, 1, 1, 1, 1) + member)
    empty_status = (
        struct.pack(">HIQQQQ", 1, 1, 1, 1, 2, 0) +
        lifecycle(0, 0) +
        bytes([1, 1, 0, 0, 0]) + struct.pack(">I", 1) + member +
        struct.pack(">I", 0) * 4)
    data_node = (
        wire_string(node_id) + bytes([0, 0, 1]) +
        wire_string("group-1") + bytes([1, 1, 1, 1, 0]))
    group = (
        wire_string("group-1") + struct.pack(">Q", 1) + bytes([1]) +
        wire_string(node_id) + struct.pack(">BB", 1, 1) +
        healthy_automatic_failover_status())
    slot_range = struct.pack(">II", 0, 16_383) + wire_string("group-1")
    ready_status = (
        struct.pack(">HIQQQQ", 1, 1, 1, 1, 22, 5) +
        lifecycle(2, 2, "00112233445566778899aabbccddeeff", 3) +
        bytes([1, 1, 1, 1, 1]) + struct.pack(">I", 1) + member +
        struct.pack(">I", 1) + data_node +
        struct.pack(">I", 1) + group +
        struct.pack(">I", 1) + slot_range + struct.pack(">I", 0))
    head_reply = "OK clusterhead 1 " + head_payload.hex()
    empty_reply = "OK clusterstatus 1 " + empty_status.hex()
    ready_reply = "OK clusterstatus 1 " + ready_status.hex()

    environment = os.environ.copy()

    def run_create_server(name, create_reply, expected, timeout_ms="2000",
                          process_environment=None):
        path = os.path.join(directory, name + ".sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(path)
        listener.listen(8)
        listener.settimeout(0.1)
        stopped = threading.Event()
        errors = []
        requests = []
        status_count = 0

        def serve():
            nonlocal status_count
            while not stopped.is_set():
                try:
                    connection, _ = listener.accept()
                except socket.timeout:
                    continue
                except OSError as error:
                    if not stopped.is_set():
                        errors.append(error)
                    return
                with connection:
                    reader = connection.makefile("rb")
                    request = reader.readline().decode().rstrip("\n")
                    requests.append(request)
                    if request == "clusterhead 1":
                        reply = head_reply
                    elif request == "clusterstatus 1":
                        reply = empty_reply if status_count == 0 else ready_reply
                        status_count += 1
                    elif request.startswith("clustercreate 1 "):
                        operation = bytes.fromhex(request.split()[2])[2:18].hex()
                        reply = create_reply.replace("{operation}", operation)
                    else:
                        reply = "ERR bad-request"
                    connection.sendall(reply.encode() + b"\n")

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        try:
            result = run_cluster(
                ["cluster-create", "--manifest", manifest, "--socket", path,
                 "--yes", "--timeout-ms", timeout_ms],
                expected=expected, timeout=5,
                env=process_environment or environment)
        finally:
            stopped.set()
            listener.close()
            thread.join(timeout=2)
        if thread.is_alive() or errors:
            raise H.Failure(f"scripted cluster-create server failed: {errors}")
        return result, requests

    created, requests = run_create_server(
        "success", "OK clustercreate 1 22 {operation}",
        expected=0)
    if ("Cluster create accepted: genesis committed=22 operation=" not in
            created.stdout or "Run cluster-status" not in created.stdout or
            not any(request.startswith("clustercreate 1 ")
                    for request in requests)):
        raise H.Failure(
            "cluster-create did not report atomic Genesis acceptance: "
            f"stdout={created.stdout!r} stderr={created.stderr!r} "
            f"requests={requests!r}")

    rejected, _ = run_create_server(
        "domain-reject",
        "ERR clustercreate 1 preflight non-pristine topology exists",
        expected=2)
    if "topology exists" not in rejected.stderr:
        raise H.Failure("cluster-create domain rejection lost its detail")
    bad_request, _ = run_create_server(
        "bad-request",
        "ERR clustercreate 1 preflight bad-request manifest mismatch",
        expected=2)
    if "manifest mismatch" not in bad_request.stderr:
        raise H.Failure("cluster-create bad request used the local-error exit")
    unavailable, _ = run_create_server(
        "pre-commit-unavailable",
        "ERR clustercreate 1 preflight pre-commit-failed reconciler unavailable",
        expected=2)
    if "reconciler unavailable" not in unavailable.stderr:
        raise H.Failure("pre-commit rejection lost its stable classification")
    uncertain, _ = run_create_server(
        "uncertain",
        "ERR clustercreate 1 proposal uncertain-outcome timed out",
        expected=3)
    if ("partially committed" not in uncertain.stderr or
            "cluster-status" not in uncertain.stderr or
            re.search(r"operation=[0-9a-f]{32}", uncertain.stderr) is None):
        raise H.Failure("cluster-create uncertain outcome omitted recovery advice")
    malformed, _ = run_create_server(
        "malformed", "OK clustercreate 1 malformed", expected=3)
    if ("cluster-status" not in malformed.stderr or
            re.search(r"operation=[0-9a-f]{32}", malformed.stderr) is None):
        raise H.Failure(
            "post-mutation protocol failure was not treated as uncertain")
    H.log("lavik-ctl cluster-create atomic acceptance and exit gates — OK")


def scripted_failover_gate(workdir):
    """Pin failover request identity, deadline, and uncertain exit semantics."""
    directory = os.path.join(workdir, "scripted-failover")
    os.makedirs(directory, mode=0o700, exist_ok=True)

    def wire_string(value):
        encoded = value.encode()
        return struct.pack(">I", len(encoded)) + encoded

    def lifecycle(state, revision, root=None, genesis=None, phase=None,
                  failure=None):
        payload = bytes([state]) + struct.pack(">Q", revision)
        payload += bytes([root is not None])
        if root is not None:
            payload += wire_string(root)
        payload += bytes([genesis is not None])
        if genesis is not None:
            payload += struct.pack(">Q", genesis)
        for value in (phase, failure):
            payload += bytes([value is not None])
            if value is not None:
                payload += wire_string(value)
        return payload

    member = struct.pack(">IBB", 1, 0, 1)
    head_payload = (
        struct.pack(">HIBQBIQI", 1, 1, 1, 1, 1, 1, 1, 1) + member)
    node_id = "0123456789abcdef0123456789abcdef01234567"
    data_node = (
        wire_string(node_id) + bytes([0, 0, 1]) +
        wire_string("group-1") + bytes([1, 1, 1, 1, 0]))
    group = (
        wire_string("group-1") + struct.pack(">Q", 4) + bytes([1]) +
        wire_string(node_id) + struct.pack(">BB", 1, 1) +
        healthy_automatic_failover_status())
    slot_range = struct.pack(">II", 0, 16_383) + wire_string("group-1")
    status_payload = (
        struct.pack(">HIQQQQ", 1, 1, 1, 1, 50, 3) +
        lifecycle(2, 2, "00112233445566778899aabbccddeeff", 3) +
        bytes([1, 1, 1, 1, 1]) + struct.pack(">I", 1) + member +
        struct.pack(">I", 1) + data_node +
        struct.pack(">I", 1) + group +
        struct.pack(">I", 1) + slot_range + struct.pack(">I", 0))
    head_reply = "OK clusterhead 1 " + head_payload.hex()
    status_reply = "OK clusterstatus 1 " + status_payload.hex()

    def decode_request(command):
        prefix = "failover 1 "
        if not command.startswith(prefix):
            raise H.Failure(f"unexpected failover command: {command!r}")
        try:
            payload = bytes.fromhex(command[len(prefix):])
        except ValueError as error:
            raise H.Failure("failover request was not lowercase hex") from error
        if command[len(prefix):] != command[len(prefix):].lower():
            raise H.Failure("failover request used non-canonical hex")
        if len(payload) < 26:
            raise H.Failure("failover request was truncated")
        operation = payload[:16].hex()
        group_size = struct.unpack(">H", payload[16:18])[0]
        expected_size = 16 + 2 + group_size + 8
        if len(payload) != expected_size:
            raise H.Failure("failover request has trailing or missing bytes")
        group_id = payload[18:18 + group_size].decode()
        deadline = struct.unpack(">Q", payload[-8:])[0]
        return operation, group_id, deadline

    def run_server(name, mutation_reply, expected):
        path = os.path.join(directory, name + ".sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(path)
        listener.listen(8)
        listener.settimeout(0.1)
        stopped = threading.Event()
        errors = []
        requests = []
        decoded = []

        def serve():
            while not stopped.is_set():
                try:
                    connection, _ = listener.accept()
                except socket.timeout:
                    continue
                except OSError as error:
                    if not stopped.is_set():
                        errors.append(error)
                    return
                with connection:
                    reader = connection.makefile("rb")
                    command = reader.readline().decode().rstrip("\n")
                    requests.append(command)
                    if command == "clusterhead 1":
                        reply = head_reply
                    elif command == "clusterstatus 1":
                        reply = status_reply
                    elif command.startswith("failover 1 "):
                        try:
                            request = decode_request(command)
                            decoded.append(request)
                            reply = mutation_reply.replace("{operation}",
                                                           request[0])
                        except H.Failure as error:
                            errors.append(error)
                            reply = "ERR bad-request"
                    else:
                        reply = "ERR bad-request"
                    connection.sendall(reply.encode() + b"\n")

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        try:
            before = int(time.time() * 1000)
            result = run_cluster(
                ["failover", "group-1", "--socket", path,
                 "--failover-timeout-ms", "5000", "--timeout-ms", "2000"],
                expected=expected, timeout=5)
            after = int(time.time() * 1000)
        finally:
            stopped.set()
            listener.close()
            thread.join(timeout=2)
        if thread.is_alive() or errors:
            raise H.Failure(f"scripted failover server failed: {errors}")
        if decoded:
            operation, group_id, deadline = decoded[0]
            if (group_id != "group-1" or deadline < before + 4_500 or
                    deadline > after + 5_500):
                raise H.Failure(
                    "failover request lost group/deadline binding: "
                    f"decoded={decoded[0]!r} before={before} after={after}")
            if len(operation) != 32 or operation == "0" * 32:
                raise H.Failure("failover operation id is not a fresh id")
        return result, requests, decoded

    accepted, requests, decoded = run_server(
        "success", "OK failover 1 51 {operation}", expected=0)
    if (not decoded or
            "Controlled failover accepted: commit=51 operation=" not in
            accepted.stdout or "Use getop " not in accepted.stdout or
            not any(request.startswith("failover 1 ") for request in requests)):
        raise H.Failure(
            "failover success omitted its durable recovery identity: "
            f"stdout={accepted.stdout!r} requests={requests!r}")

    rejected, _, _ = run_server(
        "rejected", "ERR failover 1 preflight no-candidate", expected=2)
    if "no-candidate" not in rejected.stderr:
        raise H.Failure("failover preflight rejection lost its cause")

    uncertain, _, decoded = run_server(
        "uncertain", "OK failover 1 malformed", expected=3)
    if (not decoded or decoded[0][0] not in uncertain.stderr or
            "untrustworthy" not in uncertain.stderr):
        raise H.Failure(
            "failover uncertain response omitted operation recovery identity")

    proposal_timeout, _, decoded = run_server(
        "proposal-timeout", "ERR failover 1 proposal timeout", expected=3)
    if (not decoded or decoded[0][0] not in proposal_timeout.stderr or
            "outcome is uncertain" not in proposal_timeout.stderr):
        raise H.Failure(
            "failover proposal timeout omitted operation recovery identity")

    resource_rejected, _, _ = run_server(
        "resource-rejected",
        "ERR failover 1 proposal resource-exhausted", expected=2)
    if ("resource-exhausted" not in resource_rejected.stderr or
            "outcome is uncertain" in resource_rejected.stderr):
        raise H.Failure(
            "pre-append failover resource gate used uncertain exit semantics")

    missing_group = run_cluster(
        ["failover", "--socket", os.path.join(directory, "unused.sock")],
        expected=1)
    if missing_group.stdout or "requires GROUP" not in missing_group.stderr:
        raise H.Failure("failover accepted a missing group")
    bad_timeout = run_cluster(
        ["failover", "group-1", "--socket",
         os.path.join(directory, "unused.sock"),
         "--failover-timeout-ms", "0"], expected=1)
    if (bad_timeout.stdout or "1 through 86400000" not in bad_timeout.stderr):
        raise H.Failure("failover accepted an invalid transition timeout")
    H.log("lavik-ctl failover request, deadline, and exit gates — OK")


def dual_listener_rollback_gate(workdir):
    directory = os.path.join(workdir, "rollback")
    data_dir = os.path.join(directory, "node1")
    os.makedirs(data_dir, mode=0o700, exist_ok=True)
    ctl_path = os.path.join(data_dir, "meta-admin.sock")
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    blocker.bind(("127.0.0.1", 0))
    blocker.listen(1)
    ctl_port = blocker.getsockname()[1]
    raft_port = H.free_port()
    data_control_port = H.free_port()
    initial_manifest = os.path.join(directory, "initial-cluster.toml")
    H.write_initial_meta_manifest(initial_manifest, [
        (1, f"127.0.0.1:{raft_port}",
         f"127.0.0.1:{data_control_port}", f"127.0.0.1:{ctl_port}")])
    try:
        proc = subprocess.run(
            [META, "--id", "1", "--addr", f"127.0.0.1:{raft_port}",
             "--data-control-addr", f"127.0.0.1:{data_control_port}",
             "--data-dir", data_dir,
             "--initial-cluster-manifest", initial_manifest,
             "--ctl-socket", ctl_path,
             "--ctl-addr", f"127.0.0.1:{ctl_port}"] + H.raft_args(),
            capture_output=True, text=True, timeout=15)
        if proc.returncode != 1:
            raise H.Failure(
                "dual-listener bind failure did not roll startup back: "
                f"exit={proc.returncode} stdout={proc.stdout!r} "
                f"stderr={proc.stderr!r}")
        if os.path.exists(ctl_path):
            raise H.Failure("failed dual-listener startup left its UDS behind")
        try:
            leaked = socket.create_connection(
                ("127.0.0.1", data_control_port), timeout=0.2)
        except OSError:
            leaked = None
        if leaked is not None:
            leaked.close()
            raise H.Failure(
                "failed dual-listener startup left Data control accepting")
        H.log("dual Admin listener startup rollback — OK")
    finally:
        blocker.close()


def admin_slow_reader_gate(node):
    # Keep the fixture compact in code but large enough to exceed a Unix
    # socket's send buffer after the binary status is hex-wrapped.
    node_count = 2_500
    commands = [
        "registernode " + f"{index:040x}" + " lavik://node/" +
        f"{index:040x}" + " primary 127.0.0.1:9000\n"
        for index in range(node_count)]
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as populate:
        populate.settimeout(30)
        populate.connect(node.ctl_path)
        reader = populate.makefile("rb")
        # Bound both directions during setup: sending every command before
        # reading replies can fill both Unix socket buffers and deadlock.
        batch_size = 128
        for first in range(0, node_count, batch_size):
            batch = commands[first:first + batch_size]
            populate.sendall("".join(batch).encode())
            for index in range(first, first + len(batch)):
                reply = reader.readline()
                if not reply.startswith(b"OK "):
                    raise H.Failure(
                        f"slow-reader fixture node {index}: {reply!r}")

    # Diagnostics may lag the last registration and yield ERR cut_changed.
    # That short reply has no send watchdog; require a successful capture
    # before testing the deadline on a parked large response.
    H.wait_until("slow-reader status snapshot catches up", 10,
                 lambda: node.ctl("clusterstatus 1").startswith(
                     "OK clusterstatus 1 "))

    slow = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    slow.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1_024)
    slow.settimeout(2)
    slow.connect(node.ctl_path)
    started = time.monotonic()
    slow.sendall(b"clusterstatus 1\n")
    try:
        # A parked large status write must not pin the Bycorf worker: unrelated
        # Admin traffic still completes while the 5-second send watchdog owns
        # the slow connection.
        time.sleep(0.25)
        probe_started = time.monotonic()
        status = run(["--socket", node.ctl_path, "status"], timeout=3)
        if not status.startswith("OK leader=1 "):
            raise H.Failure(f"status behind slow reader: {status}")
        if time.monotonic() - probe_started > 1.0:
            raise H.Failure("slow Admin reader blocked unrelated ctl traffic")

        time.sleep(max(0.0, 5.75 - (time.monotonic() - started)))
        saw_eof = False
        while True:
            chunk = slow.recv(65_536)
            if not chunk:
                saw_eof = True
                break
        if not saw_eof or time.monotonic() - started > 8.0:
            raise H.Failure("slow status reader was not closed by send deadline")
    finally:
        slow.close()

    cluster = run_cluster(
        ["cluster-status", "--socket", node.ctl_path, "--json"], expected=2,
        timeout=10)
    if '"result":"not_ready"' not in cluster.stdout:
        raise H.Failure("status capture did not recover after slow reader")
    H.log("slow Admin status reader deadline and worker isolation — OK")


def unix_gate(workdir):
    directory = os.path.join(workdir, "unix")
    os.makedirs(directory, exist_ok=True)
    node = H.Node(
        META, directory, 1, args=H.raft_args(snapshot_distance=100_000))
    try:
        node.start(bootstrap=True)
        H.wait_until("Unix ctl leader", 10, node.is_leader)
        status = run(["--socket", node.ctl_path, "status"])
        if not status.startswith("OK leader=1 "):
            raise H.Failure(f"unexpected Unix status: {status}")
        cluster = run_cluster(
            ["cluster-status", "--socket", node.ctl_path, "--json"], expected=2)
        if '"result":"not_ready"' not in cluster.stdout:
            raise H.Failure(f"unexpected Unix cluster status: {cluster.stdout}")

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
        admin_slow_reader_gate(node)
        H.log("lavik-ctl Unix transport and exit statuses — OK")
    finally:
        node.terminate()


def plaintext_gate(workdir):
    directory = os.path.join(workdir, "plaintext")
    data_dir = os.path.join(directory, "node1")
    os.makedirs(data_dir, mode=0o700, exist_ok=True)
    raft_port = H.free_port()
    data_control_port = H.free_port()
    ctl_port = H.free_port()
    initial_manifest = os.path.join(directory, "initial-cluster.toml")
    H.write_initial_meta_manifest(initial_manifest, [
        (1, f"127.0.0.1:{raft_port}",
         f"127.0.0.1:{data_control_port}", f"127.0.0.1:{ctl_port}")])
    log_path = os.path.join(directory, "node1.log")
    log_file = open(log_path, "wb")
    server = subprocess.Popen(
        [META, "--id", "1", "--addr", f"127.0.0.1:{raft_port}",
         "--data-control-addr", f"127.0.0.1:{data_control_port}",
         "--data-dir", data_dir,
         "--initial-cluster-manifest", initial_manifest,
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
        denied = run_cluster(
            ["cluster-status", "--addr", f"127.0.0.1:{ctl_port}"], expected=1)
        if denied.stdout:
            raise H.Failure("fatal plaintext policy failure wrote stdout")
        cluster = run_cluster(
            ["cluster-status", "--addr", f"127.0.0.1:{ctl_port}",
             "--allow-plaintext-admin"], expected=2)
        if not cluster.stdout.startswith("NOT READY\n"):
            raise H.Failure(f"unexpected plaintext cluster status: {cluster.stdout}")

        op_id = "00000002000000000000000000000001"
        reply = run(client_args + ["submitop", op_id, "ctl-gate", "plain"])
        if not reply.startswith("OK "):
            raise H.Failure(f"plaintext submitop: {reply}")
        export = run(client_args + ["exportaudit", reply.split()[1]])
        if not export.startswith("OK "):
            raise H.Failure(f"plaintext exportaudit: {export}")
        if b"lavik://operator/plaintext" not in bytes.fromhex(export[3:]):
            raise H.Failure("plaintext audit actor was not persisted")
        H.log("lavik-ctl remote plaintext transport — OK")
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
        "IP:127.0.0.1,URI:lavik://meta/1")
    client_cert, client_key = make_leaf(
        directory, ca_crt, ca_key, "operator",
        "URI:lavik://operator/ctl-gate")
    raft_port = H.free_port()
    data_control_port = H.free_port()
    ctl_port = H.free_port()
    initial_manifest = os.path.join(directory, "initial-cluster.toml")
    H.write_initial_meta_manifest(initial_manifest, [
        (1, f"127.0.0.1:{raft_port}",
         f"127.0.0.1:{data_control_port}", f"127.0.0.1:{ctl_port}")])
    log_path = os.path.join(directory, "node1.log")
    log_file = open(log_path, "wb")
    server = subprocess.Popen(
        [META, "--id", "1", "--addr", f"127.0.0.1:{raft_port}",
         "--data-control-addr", f"127.0.0.1:{data_control_port}",
         "--data-dir", data_dir,
         "--initial-cluster-manifest", initial_manifest,
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
        cluster = run_cluster(["cluster-status"] + client_args, expected=2)
        if not cluster.stdout.startswith("NOT READY\n"):
            raise H.Failure(f"unexpected mTLS cluster status: {cluster.stdout}")

        create_manifest = os.path.join(directory, "cluster-create.toml")
        create_node = "0123456789abcdef0123456789abcdef01234567"
        with open(create_manifest, "w", encoding="utf-8") as output:
            output.write(
                "schema_version = 1\n\n"
                "[[meta_members]]\nid = 1\n"
                f'raft_endpoint = "tcp://127.0.0.1:{raft_port}"\n'
                f'data_control_endpoint = "tcp://127.0.0.1:{data_control_port}"\n'
                f'ctl_endpoint = "tcp://127.0.0.1:{ctl_port}"\n\n'
                "[[data_nodes]]\n"
                f'id = "{create_node}"\n'
                f'client_endpoint = "tcp://127.0.0.1:{H.free_port()}"\n\n'
                "[[groups]]\nid = \"group-1\"\n"
                f'primary = "{create_node}"\n\n'
                "[[slot_ranges]]\nfirst = 0\nlast = 16383\n"
                'group = "group-1"\n')
        created = run_cluster(
            ["cluster-create", "--manifest", create_manifest, "--yes"] +
            client_args, expected=0)
        if "Cluster create accepted: genesis committed=" not in created.stdout:
            raise H.Failure(
                f"mTLS cluster-create did not confirm Genesis: {created}")
        creating = run_cluster(["cluster-status"] + client_args, expected=2)
        if "cluster_state=creating" not in creating.stdout:
            raise H.Failure(
                f"mTLS cluster-create did not expose lifecycle: {creating.stdout}")

        # The UDS seed supplies the remote leader address; its TLS options
        # must survive parsing and protect the learned TCP connection.
        seed_path = os.path.join(directory, "discovery.sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(seed_path)
        listener.listen(1)
        listener.settimeout(3)
        endpoint = f"127.0.0.1:{ctl_port}".encode()
        leader_member = (struct.pack(">IBI", 1, 1, len(endpoint)) + endpoint +
                         bytes([1]))
        seed_member = struct.pack(">IBB", 2, 0, 0)
        head = (struct.pack(">HIBQBIQI", 1, 2, 0, 1, 1, 1, 1, 2) +
                leader_member + seed_member)
        seed_errors = []

        def serve_seed():
            try:
                connection, _ = listener.accept()
                with connection:
                    reader = connection.makefile("rb")
                    request = reader.readline()
                    if request != b"clusterhead 1\n":
                        raise H.Failure(f"unexpected discovery command: {request!r}")
                    connection.sendall(
                        b"OK clusterhead 1 " + head.hex().encode() + b"\n")
            except (OSError, H.Failure) as error:
                seed_errors.append(error)

        seed_thread = threading.Thread(target=serve_seed, daemon=True)
        seed_thread.start()
        try:
            redirected = run_cluster(
                ["cluster-status", "--socket", seed_path,
                 "--tls-ca", ca_crt, "--tls-cert", client_cert,
                 "--tls-key", client_key], expected=2)
            if not redirected.stdout.startswith("NOT READY\n"):
                raise H.Failure(f"unexpected UDS-to-mTLS result: {redirected.stdout}")
        finally:
            listener.close()
            seed_thread.join(timeout=4)
        if seed_thread.is_alive() or seed_errors:
            raise H.Failure(f"UDS-to-mTLS discovery failed: {seed_errors}")

        bad_certificate = run_cluster(
            ["cluster-status", "--addr", f"127.0.0.1:{ctl_port}",
             "--tls-ca", client_cert, "--tls-cert", client_cert,
             "--tls-key", client_key], expected=1)
        if (bad_certificate.stdout or not bad_certificate.stderr or
                "check the CA" not in bad_certificate.stderr):
            raise H.Failure(
                "lavik-ctl cluster-status certificate failure did not stay fatal and "
                f"stdout-clean: stdout={bad_certificate.stdout!r} "
                f"stderr={bad_certificate.stderr!r}")
        wrong_name = subprocess.run(
            [CTL] + client_args + ["--tls-server-name", "wrong.invalid",
                                   "status"],
            capture_output=True, text=True, timeout=10)
        if wrong_name.returncode != 1:
            raise H.Failure(
                "mTLS server-name mismatch was not rejected: "
                f"exit={wrong_name.returncode} stdout={wrong_name.stdout!r} "
                f"stderr={wrong_name.stderr!r}")
        H.log("lavik-ctl remote mTLS transport — OK")
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
        ctl_characterization_gate(workdir)
        raw_argument_gate(workdir)
        scripted_cluster_gate(workdir)
        scripted_cluster_create_gate(workdir)
        scripted_failover_gate(workdir)
        dual_listener_rollback_gate(workdir)
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
