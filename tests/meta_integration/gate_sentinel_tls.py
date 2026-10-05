#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Stock Sentinel clients over authenticated TLS and real L4 port mapping.

All work goes beneath the supplied test directory/TMPDIR. Native replication
and Meta control use mTLS as well, so hostname publication cannot pass while
Follow Owner silently uses a plaintext or numeric-only shortcut.
"""

import os
from pathlib import Path
import ssl
import socket
import subprocess
import sys
import tempfile
import time

import harness as H
import gate_cluster_create as C
import gate_sentinel_discovery as D
import gate_sentinel_ha as HA
import gate_mtls as M
from gate_data_control import DataProcess, allocate_data_file
from gate_sentinel import Client, RespError

DATA_PASSWORD = "data-only-secret"


def context(ca, cert=None, key=None):
    result = ssl.create_default_context(cafile=ca)
    if cert:
        result.load_cert_chain(cert, key)
    return result


class SecureFixture(D.DiscoveryFixture):
    def __init__(self, meta, data, ctl, directory, hostname=True, tls=True):
        super().__init__(meta, data, ctl, directory)
        self.proxies = []
        self.discovery_host = "localhost" if hostname else "127.0.0.1"
        self.data_password = DATA_PASSWORD
        self.ca, ca_key = M.make_ca(str(directory / "pki"), "Sentinel acceptance")
        self.app_cert, self.app_key = M.make_leaf(
            str(directory / "pki"), self.ca, ca_key, "application", "DNS:application"
        )
        if tls:
            self.ssl_context = context(self.ca, self.app_cert, self.app_key)
            self.client_tls_kwargs = dict(
                ssl=True,
                ssl_ca_certs=self.ca,
                ssl_certfile=self.app_cert,
                ssl_keyfile=self.app_key,
                ssl_cert_reqs="required",
                ssl_check_hostname=True,
            )
            self.go_args = [
                "--data-password",
                DATA_PASSWORD,
                "--tls-ca",
                self.ca,
                "--tls-cert",
                self.app_cert,
                "--tls-key",
                self.app_key,
            ]
        else:
            self.go_args = ["--data-password", DATA_PASSWORD]
        for node in self.metas:
            cert, key = M.make_leaf(
                str(directory / "pki"),
                self.ca,
                ca_key,
                f"meta-{node.id}",
                f"IP:127.0.0.1,DNS:localhost,URI:lavik://meta/{node.id}",
            )
            sentinel_cert, sentinel_key = cert, key
            if hostname:
                # DNS-only application SANs make an accidental conversion to IP
                # fail certificate verification, including native replication.
                sentinel_cert, sentinel_key = M.make_leaf(
                    str(directory / "pki"),
                    self.ca,
                    ca_key,
                    f"sentinel-{node.id}",
                    "DNS:localhost",
                )
            if tls:
                idx = node.args.index("--sentinel-addr")
                node.args[idx] = "--sentinel-tls-addr"
                node.args += [
                    "--sentinel-tls-cert",
                    sentinel_cert,
                    "--sentinel-tls-key",
                    sentinel_key,
                    "--sentinel-tls-ca",
                    self.ca,
                    "--sentinel-data-transport",
                    "tls",
                    "--tls-ca",
                    self.ca,
                    "--tls-cert",
                    cert,
                    "--tls-key",
                    key,
                    "--ctl-tls-ca",
                    self.ca,
                    "--ctl-tls-cert",
                    cert,
                    "--ctl-tls-key",
                    key,
                ]
            node.args += [
                "--sentinel-resolve-hostnames",
                "yes",
                "--sentinel-announce-hostnames",
                "yes" if hostname else "no",
            ]
            port = self.sentinel_ports[node.id]
            proxy = HA.DataProxy(f"sentinel-{node.id}", port)
            self.proxies.append(proxy)
            port = proxy.listen_port
            self.sentinel_ports[node.id] = port
            node.sentinel_endpoint = (
                f"{'tls' if tls else 'tcp'}://{self.discovery_host}:{port}"
            )
        for i, old in enumerate(self.data_nodes):
            cert, key = M.make_leaf(
                str(directory / "pki"),
                self.ca,
                ca_key,
                f"data-{i}",
                ("" if hostname else "IP:127.0.0.1,")
                + f"DNS:localhost,URI:lavik://node/{old.node_id}",
            )
            node = DataProcess(
                data,
                old.workdir,
                old.node_id,
                old.seed,
                tls=(self.ca, cert, key) if tls else None,
                extra_args=[
                    "--masterauth",
                    DATA_PASSWORD,
                    "--requirepass",
                    DATA_PASSWORD,
                    "--tls-auth-clients",
                    "yes",
                ]
                if tls
                else ["--masterauth", DATA_PASSWORD, "--requirepass", DATA_PASSWORD],
            )
            plain = node.redis_port
            secure = node.tls_port
            proxy = HA.DataProxy(f"data-{i}-plain", plain)
            self.proxies.append(proxy)
            plain = proxy.listen_port
            if tls:
                proxy = HA.DataProxy(f"data-{i}-tls", secure)
                self.proxies.append(proxy)
                secure = proxy.listen_port
            node.discovery_endpoint = f"tcp://{self.discovery_host}:{plain}"
            if tls:
                node.discovery_tls_endpoint = f"tls://{self.discovery_host}:{secure}"
            node.public_port = secure if tls else plain
            self.data_nodes[i] = node
        self.by_id = {node.node_id: node for node in self.data_nodes}
        # Local operator provisioning is independent of application certificates.
        if tls:
            operator, operator_key = M.make_leaf(
                str(directory / "pki"),
                self.ca,
                ca_key,
                "operator",
                "URI:lavik://operator/sentinel-test",
            )
            self.admin_tls_args = [
                "--tls-ca",
                self.ca,
                "--tls-cert",
                operator,
                "--tls-key",
                operator_key,
            ]

    def start_created(self):
        for proxy in self.proxies:
            proxy.start()
        # The ctl client follows the leader's admin directory after this seed.
        super().start_created()

    def force_kill(self):
        super().force_kill()
        for proxy in self.proxies:
            proxy.close()


def require_failure(action, description):
    try:
        action()
    except Exception:
        return
    raise H.Failure(f"accepted {description}")


def check_subscriptions(clients, subscriptions, marker):
    # Retry only new publications; no application-created replacement client or
    # subscription can mask a broken library resubscription path.
    deadline = time.monotonic() + 30
    pending = dict(subscriptions)
    while pending and time.monotonic() < deadline:
        for _, client in clients:
            try:
                if isinstance(client, HA.GoClient):
                    client.call(Op="publish", Key="secure-ha", Value=marker)
                else:
                    client.publish("secure-ha", marker)
                break
            except Exception:
                pass
        for name, sub in list(pending.items()):
            try:
                if isinstance(sub, HA.GoClient):
                    value = sub.call(Op="receive")
                else:
                    message = sub.get_message(timeout=0.2)
                    value = message.get("data") if message else None
                if value in (marker, marker.encode()):
                    del pending[name]
            except Exception:
                pass
    if pending:
        raise H.Failure(f"subscriptions did not recover: {list(pending)}")


def run_cluster(meta, data, ctl, go, directory, mode):
    import redis

    tls = mode != "plaintext"
    fixture = SecureFixture(
        meta,
        data,
        ctl,
        directory,
        hostname=mode == "hostname",
        tls=tls,
    )
    clients, resources, subscriptions = [], [], []
    try:
        fixture.start_created()
        # Seed retry may reach only a subset of peers. Probe every advertised
        # listener directly so routing/TLS coverage does not require electing
        # every Meta member in each address-mode scenario.
        for host, port in fixture.sentinel_addresses():
            peer = Client(
                port, host=host, ssl_context=getattr(fixture, "ssl_context", None)
            )
            try:
                assert peer.command("AUTH", D.SENTINEL_PASSWORD) == b"OK"
                assert peer.command("PING") == b"PONG"
            finally:
                peer.close()
        ports = [H.free_port() for _ in range(3)]
        for collision in (fixture.leader.raft_port, ports[0]):
            reply = fixture.leader.ctl(
                f"addsrv 4 127.0.0.1:{ports[0]} 127.0.0.1:{ports[1]} 127.0.0.1:{ports[2]} "
                f"sentinel=tls://127.0.0.1:{collision}"
            )
            assert reply == "ERR rejected", reply
        clients, resources = HA.make_clients(fixture, go, directory)
        HA.recover(clients, "secure-initial")
        expected = [
            fixture.discovery_host.encode(),
            str(fixture.by_id[D.OWNER].public_port).encode(),
        ]
        assert (
            fixture.sentinel_command("SENTINEL", "GET-MASTER-ADDR-BY-NAME", D.GROUP)
            == expected
        )
        for protocol in (2, 3):
            assert (
                len(
                    fixture.sentinel_command(
                        "SENTINEL", "SENTINELS", D.GROUP, proto=protocol
                    )
                )
                == 2
            )
            # A standard SentinelClient targets one server. The failover
            # clients deliberately start with a follower seed, whose discovery
            # queries are closed by the leader-only serving contract.
            with open(directory / f"go-sentinel-{protocol}.log", "w") as log:
                wire = HA.GoClient(
                    go, fixture.sentinel_addresses()[:1], protocol, log, fixture.go_args
                )
                try:
                    assert wire.call(Op="sentinel-ping") == "PONG"
                    assert wire.call(Op="sentinel-address") == [
                        fixture.discovery_host,
                        str(fixture.by_id[D.OWNER].public_port),
                    ]
                finally:
                    wire.close()
        if mode == "hostname":
            for name, client in clients:
                if isinstance(client, HA.GoClient):
                    client.call(Op="subscribe", Key="secure-ha")
                    subscriptions.append((name, client))
                else:
                    sub = client.pubsub()
                    sub.subscribe("secure-ha")
                    H.wait_until(
                        "subscription ready",
                        5,
                        lambda: sub.get_message(timeout=1) is not None,
                    )
                    subscriptions.append((name, sub))
        kwargs = getattr(fixture, "client_tls_kwargs", {})
        replica = redis.Redis(
            host=fixture.discovery_host,
            port=fixture.by_id[D.REPLICA].public_port,
            password=DATA_PASSWORD,
            socket_timeout=1,
            **kwargs,
        )
        resources.append(replica)
        H.wait_until(
            "replica has actual readable data",
            30,
            lambda: replica.get("sentinel-ha-python-2") == b"secure-initial",
        )
        # Both stock libraries select and read an advertised replica.
        for protocol in (2, 3):
            discovery = redis.sentinel.Sentinel(
                fixture.sentinel_addresses(),
                sentinel_kwargs=dict(
                    password=D.SENTINEL_PASSWORD, protocol=protocol, **kwargs
                ),
                socket_timeout=1,
                socket_connect_timeout=1,
            )
            reader = discovery.slave_for(
                D.GROUP, protocol=protocol, password=DATA_PASSWORD, **kwargs
            )
            resources.append(reader)
            resources.extend(discovery.sentinels)
            H.wait_until(
                "redis-py replica read",
                30,
                lambda: reader.get("sentinel-ha-python-2") == b"secure-initial",
            )
            assert reader.role()[0] == b"slave"
            with open(directory / f"go-replica-{protocol}.log", "w") as log:
                reader = HA.GoClient(
                    go,
                    fixture.sentinel_addresses(),
                    protocol,
                    log,
                    [*fixture.go_args, "--replica"],
                )
                try:
                    H.wait_until(
                        "go-redis replica read",
                        30,
                        lambda: reader.get("sentinel-ha-python-2") == "secure-initial",
                    )
                    assert reader.call(Op="role")[0] == "slave"
                finally:
                    reader.close()
        # Authentication failures and HA use the same TLS/client paths for
        # numeric and hostname publication. Exercise that matrix once with
        # DNS-only SANs; mapped still proves positive numeric discovery,
        # primary/replica client access, and the real forwarded endpoints.
        if mode == "hostname":
            negative_clients(fixture, go, directory)
            visited = set()
            for _ in range(10):
                fixture.rediscover_leader()
                visited.add(fixture.leader.id)
                if len(visited) == 3:
                    break
                old = fixture.leader
                old.force_kill()
                fixture.leader = H.find_leader(
                    [m for m in fixture.metas if m is not old], timeout=20
                )
                HA.recover(clients, "Meta-replacement")
                old.start()
            assert len(visited) == 3, visited
            fixture.rediscover_leader()
            output = subprocess.check_output(
                [
                    ctl,
                    "failover",
                    D.GROUP,
                    *fixture.admin_connection(fixture.leader),
                    "--timeout-ms",
                    "10000",
                    "--failover-timeout-ms",
                    "60000",
                ],
                text=True,
                timeout=20,
            )
            assert "operation=" in output, output
            candidate = fixture.by_id[D.REPLICA]
            H.wait_until(
                "controlled cutover publishes TLS route",
                40,
                lambda: fixture.sentinel_command(
                    "SENTINEL", "GET-MASTER-ADDR-BY-NAME", D.GROUP
                )
                == [
                    fixture.discovery_host.encode(),
                    str(candidate.public_port).encode(),
                ],
            )
            HA.recover(clients, "controlled-TLS")
            check_subscriptions(clients, subscriptions, "controlled-new-message")
            fixture.configure_fast_policies()
            # Reparented old Owner must be readable before killing its source.
            old_owner = redis.Redis(
                host=fixture.discovery_host,
                port=fixture.by_id[D.OWNER].public_port,
                password=DATA_PASSWORD,
                socket_timeout=1,
                **kwargs,
            )
            resources.append(old_owner)
            H.wait_until(
                "reparented replica catches up",
                40,
                lambda: old_owner.get("sentinel-ha-python-2") == b"controlled-TLS",
            )
            candidate.force_kill()
            HA.recover(clients, "automatic-TLS")
            check_subscriptions(clients, subscriptions, "automatic-new-message")
        assert all(
            proxy.bytes_forwarded > 0
            for proxy in fixture.proxies
            if not (tls and "plain" in proxy.name)
        )
        H.log(f"PASS secure Sentinel {mode}")
    except BaseException:
        fixture.dump_logs()
        raise
    finally:
        for _, sub in subscriptions:
            if not isinstance(sub, HA.GoClient):
                sub.close()
        for _, client in clients:
            client.close()
        for resource in resources:
            resource.close()
        fixture.force_kill()


def negative_clients(fixture, go, directory):
    import redis

    bad_ca, _ = M.make_ca(str(directory / "wrong-ca"), "Untrusted")
    targets = [
        (fixture.sentinel_ports[fixture.leader.id], D.SENTINEL_PASSWORD),
        (fixture.by_id[D.OWNER].public_port, DATA_PASSWORD),
    ]
    for port, password in targets:
        for change in (
            {"password": "wrong"},
            {"ssl_ca_certs": bad_ca},
        ):
            opts = dict(
                host=fixture.discovery_host,
                port=port,
                password=password,
                socket_timeout=1,
                socket_connect_timeout=1,
                **fixture.client_tls_kwargs,
            )
            opts.update(change)
            with redis.Redis(**opts) as client:
                require_failure(client.ping, str(change))
        raw = Client(port, host=fixture.discovery_host, ssl_context=fixture.ssl_context)
        try:
            assert isinstance(raw.command("PING"), RespError)
            assert raw.command("AUTH", password) == b"OK"
        finally:
            raw.close()
    # A trusted application certificate grants no operator principal.
    sock = fixture.ssl_context.wrap_socket(
        __import__("socket").create_connection(("127.0.0.1", fixture.leader.ctl_port)),
        server_hostname="localhost",
    )
    try:
        sock.settimeout(2)
        sock.sendall(b"status\n")
        try:
            reply = sock.recv(128)
        except (OSError, ssl.SSLError):
            reply = b""
        assert not reply.startswith(b"OK"), reply
    finally:
        sock.close()
    for flag, value in (
        ("--password", "wrong"),
        ("--data-password", "wrong"),
        ("--tls-ca", bad_ca),
    ):
        with open(directory / f"negative-{flag[2:]}.log", "w") as log:
            client = HA.GoClient(
                go,
                fixture.sentinel_addresses(),
                3,
                log,
                [*fixture.go_args, flag, value],
            )
            try:
                require_failure(lambda: client.set("must-not-write", "bad"), flag)
            finally:
                client.close()


def handshake_matrix(meta, data, directory):
    ca, ca_key = M.make_ca(str(directory / "pki"), "Handshake matrix")
    cert, key = M.make_leaf(str(directory / "pki"), ca, ca_key, "server")
    app, app_key = M.make_leaf(
        str(directory / "pki"), ca, ca_key, "application", "DNS:application"
    )
    bad_ca, bad_ca_key = M.make_ca(str(directory / "bad-pki"), "Untrusted client")
    bad_app, bad_key = M.make_leaf(
        str(directory / "bad-pki"), bad_ca, bad_ca_key, "bad"
    )
    trusted = context(ca, app, app_key)
    anonymous = context(ca)
    untrusted = context(ca, bad_app, bad_key)
    for mode in ("no", "optional", "yes"):
        port, plain = H.free_port(), H.free_port()
        node = H.Node(
            meta,
            str(directory / mode),
            1,
            args=H.raft_args()
            + [
                "--sentinel-addr",
                f"127.0.0.1:{plain}",
                "--sentinel-tls-addr",
                f"127.0.0.1:{port}",
                "--sentinel-tls-cert",
                cert,
                "--sentinel-tls-key",
                key,
                "--sentinel-tls-ca",
                ca,
                "--sentinel-tls-auth-clients",
                mode,
                "--sentinel-requirepass",
                D.SENTINEL_PASSWORD,
            ],
        )
        Path(node.workdir).mkdir(parents=True, exist_ok=True)
        node.start()
        process = DataProcess(
            data,
            str(directory / mode / "data"),
            D.OWNER,
            node.data_control_endpoint,
            tls=(ca, cert, key),
            extra_args=["--tls-auth-clients", mode, "--requirepass", DATA_PASSWORD],
        )
        try:
            Path(process.workdir).mkdir(parents=True, exist_ok=True)
            allocate_data_file(process.data_path)
            process.log_file = open(process.log_path, "ab")
            process.proc = subprocess.Popen(
                [
                    data,
                    "--logtostderr",
                    "--port",
                    str(process.redis_port),
                    "--tls-port",
                    str(process.tls_port),
                    "--threads",
                    "1",
                    "--no-pin-workers",
                    "--registered-buffer-mb-per-worker",
                    "64",
                    "--recv-buffers-per-worker",
                    "0",
                    "--max-memory",
                    "1073741824",
                    "--data-file",
                    process.data_path,
                    "--rdb-dir",
                    process.workdir,
                    "--tls-cert-file",
                    cert,
                    "--tls-key-file",
                    key,
                    "--tls-ca-cert-file",
                    ca,
                    "--tls-auth-clients",
                    mode,
                    "--requirepass",
                    DATA_PASSWORD,
                ],
                stdout=process.log_file,
                stderr=subprocess.STDOUT,
            )

            def ready():
                try:
                    with socket.create_connection(
                        ("127.0.0.1", process.tls_port), timeout=0.1
                    ):
                        return True
                except OSError:
                    return False

            H.wait_until("standalone TLS listener", 20, ready)
            for target, password in (
                (port, D.SENTINEL_PASSWORD),
                (process.tls_port, DATA_PASSWORD),
            ):

                def ping(ctx):
                    c = Client(target, ssl_context=ctx)
                    try:
                        c.command("AUTH", password)
                        assert c.command("PING") == b"PONG"
                    finally:
                        c.close()

                ping(trusted)
                if mode == "yes":
                    require_failure(
                        lambda: ping(anonymous), "required client certificate missing"
                    )
                else:
                    ping(anonymous)
                if mode != "no":
                    require_failure(
                        lambda: ping(untrusted), "untrusted client certificate"
                    )
                else:
                    ping(untrusted)

                def wrong_name():
                    with socket.create_connection(
                        ("127.0.0.1", target), timeout=2
                    ) as raw:
                        with trusted.wrap_socket(raw, server_hostname="wrong.example"):
                            pass

                require_failure(wrong_name, "wrong server SAN")
                for protocol in (2, 3):
                    c = Client(target, ssl_context=trusted)
                    try:
                        assert isinstance(c.command("PING"), RespError)
                        assert isinstance(
                            c.command("AUTH", "unsupported", password), RespError
                        )
                        assert isinstance(
                            c.command("HELLO", protocol, "AUTH", "default", "wrong"),
                            RespError,
                        )
                        assert not isinstance(
                            c.command("HELLO", protocol, "AUTH", "default", password),
                            RespError,
                        )
                        assert c.command("PING") == b"PONG"
                    finally:
                        c.close()
                # Sending RESP to a TLS port must never obtain a plaintext reply.
                with socket.create_connection(("127.0.0.1", target), timeout=2) as raw:
                    raw.sendall(b"*1\r\n$4\r\nPING\r\n")
                    try:
                        response = raw.recv(100)
                    except OSError:
                        response = b""
                    assert not response.startswith((b"+", b"-")), response
            c = Client(plain)
            try:
                assert c.command("AUTH", D.SENTINEL_PASSWORD) == b"OK"
                assert c.command("PING") == b"PONG"
            finally:
                c.close()
        finally:
            process.force_kill()
            node.force_kill()
    # Listener startup is transactional, including certificate loading.
    broken = H.Node(
        meta,
        str(directory / "broken"),
        1,
        args=H.raft_args()
        + [
            "--sentinel-addr",
            f"127.0.0.1:{H.free_port()}",
            "--sentinel-tls-addr",
            f"127.0.0.1:{H.free_port()}",
            "--sentinel-tls-cert",
            cert,
            "--sentinel-tls-key",
            bad_key,
            "--sentinel-tls-ca",
            ca,
        ],
    )
    try:
        Path(broken.workdir).mkdir(parents=True, exist_ok=True)
        broken.start(wait_ready=False)
        H.wait_until(
            "invalid TLS credentials fail startup", 8, lambda: not broken.alive()
        )
        assert broken.proc.returncode != 0
    finally:
        broken.force_kill()
    port = H.free_port()
    limited = H.Node(
        meta,
        str(directory / "limited"),
        1,
        args=H.raft_args()
        + [
            "--sentinel-tls-addr",
            f"127.0.0.1:{port}",
            "--sentinel-maxclients",
            "1",
            "--sentinel-tls-cert",
            cert,
            "--sentinel-tls-key",
            key,
            "--sentinel-tls-ca",
            ca,
        ],
    )
    Path(limited.workdir).mkdir(parents=True, exist_ok=True)
    try:
        limited.start()
        # Admin readiness precedes Sentinel listener binding. Keep the first
        # successful connection as the stalled client: a throwaway readiness
        # probe could still occupy this listener's only admission slot.
        deadline = time.monotonic() + 5
        while True:
            try:
                stalled = socket.create_connection(("127.0.0.1", port), timeout=2)
                break
            except ConnectionRefusedError:
                if not limited.alive() or time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)
        with stalled:
            time.sleep(0.05)
            with socket.create_connection(("127.0.0.1", port), timeout=2) as rejected:
                assert rejected.recv(100) == b""  # No plaintext maxclients error.
            limited.proc.terminate()
            limited.proc.wait(timeout=5)  # Shutdown drains an unfinished handshake.
            assert limited.proc.returncode == 0
    finally:
        limited.force_kill()
    H.log("PASS Sentinel/Data TLS client-auth matrix")


def main():
    if len(sys.argv) != 6 or sys.argv[5] not in (
        "mapped",
        "hostname",
        "plaintext",
        "handshake",
    ):
        raise SystemExit(
            "gate_sentinel_tls.py META DATA CTL GO mapped|hostname|plaintext|handshake"
        )
    meta, data, ctl, go, mode = sys.argv[1:]
    if D.import_redis_py(meta) is None:
        raise H.Failure("pinned redis-py is required")
    C.CTL = ctl
    directory = Path(tempfile.mkdtemp(prefix=f"sentinel-{mode}-"))
    if mode == "handshake":
        handshake_matrix(meta, data, directory)
    else:
        run_cluster(meta, data, ctl, go, directory, mode)


if __name__ == "__main__":
    main()
