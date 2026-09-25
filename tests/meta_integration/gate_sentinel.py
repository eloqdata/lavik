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

"""Public RESP and process boundaries for the Meta Sentinel server.

Usage: gate_sentinel.py /path/to/lavik-meta [/path/to/lavik] [unittest args]
"""

import contextlib
import os
from pathlib import Path
import socket
import subprocess
import threading
import sys
import tempfile
import time
import unittest

import harness as H
import sentinel_compat

# Parsed under __main__ so sibling gates can import the RESP client helpers.
BINARY = None
DATA_BINARY = None


class RespError(bytes):
    pass


def encode(*args):
    args = [arg if isinstance(arg, bytes) else str(arg).encode() for arg in args]
    return f"*{len(args)}\r\n".encode() + b"".join(
        f"${len(arg)}\r\n".encode() + arg + b"\r\n" for arg in args
    )


class Client:
    def __init__(self, port, host="127.0.0.1"):
        self.sock = socket.create_connection((host, port), timeout=3)
        self.file = self.sock.makefile("rb")

    def close(self):
        self.file.close()
        self.sock.close()

    def read(self):
        line = self.file.readline()
        if not line:
            raise EOFError("RESP connection closed")
        if not line.endswith(b"\r\n"):
            raise AssertionError(f"invalid reply: {line!r}")
        tag, value = line[:1], line[1:-2]
        if tag == b"+":
            return value
        if tag == b"-":
            return RespError(value)
        if tag == b":":
            return int(value)
        if tag == b"_":
            return None
        if tag == b"$":
            size = int(value)
            if size == -1:
                return None
            data = self.file.read(size)
            if len(data) != size or self.file.read(2) != b"\r\n":
                raise AssertionError("truncated bulk reply")
            return data
        if tag in (b"*", b">"):
            count = int(value)
            if count == -1:
                return None
            return [self.read() for _ in range(count)]
        if tag == b"%":
            return {self.read(): self.read() for _ in range(int(value))}
        raise AssertionError(f"unexpected reply: {line!r}")

    def command(self, *args):
        self.sock.sendall(encode(*args))
        return self.read()


class SentinelTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory(prefix="lavik-sentinel-")
        self.addCleanup(self.root.cleanup)
        self.next_id = 1

    def node(
        self,
        password="sentinel-secret",
        maxclients=256,
        host="127.0.0.1",
        bootstrap=False,
    ):
        port = H.free_port()
        addr = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
        node = H.Node(
            BINARY,
            self.root.name,
            self.next_id,
            args=H.raft_args()
            + [
                "--sentinel-addr",
                addr,
                "--sentinel-requirepass",
                password,
                "--sentinel-maxclients",
                str(maxclients),
            ],
        )
        self.next_id += 1
        self.addCleanup(node.force_kill)
        node.start(wait_ready=False, bootstrap=bootstrap)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if not node.alive():
                self.fail(Path(node.log_path).read_text())
            try:
                with socket.create_connection((host, port), timeout=0.1):
                    return node, port
            except OSError:
                time.sleep(0.02)
        self.fail("Sentinel listener did not bind")

    def client(self, port, host="127.0.0.1"):
        client = Client(port, host)
        self.addCleanup(client.close)
        return client

    def error(self, reply, prefix):
        self.assertIsInstance(reply, RespError)
        self.assertTrue(reply.startswith(prefix), repr(reply))

    def test_authenticated_connection_without_data_cluster(self):
        node, port = self.node()
        client = self.client(port)
        self.error(client.command("PING"), b"NOAUTH")
        self.error(client.command("AUTH", "data-secret"), b"WRONGPASS")
        self.assertEqual(client.command("AUTH", "sentinel-secret"), b"OK")
        self.assertEqual(client.command("PING"), b"PONG")
        self.assertIn("leader=", node.ctl("status"))

    def test_subscription_protocol_and_reset(self):
        node, port = self.node(bootstrap=True)
        H.wait_until("Meta leader", 5, node.is_leader)
        for protocol in (2, 3):
            client = self.client(port)
            client.command("HELLO", protocol, "AUTH", "default", "sentinel-secret")
            self.assertEqual(
                client.command("SUBSCRIBE", "+switch-master"),
                [b"subscribe", b"+switch-master", 1],
            )
            self.assertEqual(
                client.command("SUBSCRIBE", "+switch-master"),
                [b"subscribe", b"+switch-master", 1],
            )
            expected = [b"pong", b"probe"] if protocol == 2 else b"probe"
            self.assertEqual(client.command("PING", "probe"), expected)
            self.assertEqual(
                client.command("UNSUBSCRIBE"), [b"unsubscribe", b"+switch-master", 0]
            )
            self.assertEqual(client.command("PING"), b"PONG")
            client.command("SUBSCRIBE", "+switch-master")
            self.assertEqual(client.command("RESET"), b"RESET")
            self.error(client.command("PING"), b"NOAUTH")
            client.command("AUTH", "sentinel-secret")
            client.command("HELLO", protocol)
            self.assertEqual(client.command("UNSUBSCRIBE"), [b"unsubscribe", None, 0])
            client.command("SUBSCRIBE", "a", "b")
            self.assertEqual(client.read(), [b"subscribe", b"b", 2])
            masters = client.command("SENTINEL", "MASTERS")
            if protocol == 2:
                self.error(masters, b"ERR")
            else:
                self.assertEqual(masters, [])
            self.assertEqual(client.command("QUIT"), b"OK")
            with self.assertRaises((EOFError, ConnectionResetError)):
                client.read()
        excess = self.client(port)
        excess.command("AUTH", "sentinel-secret")
        excess.sock.sendall(encode("SUBSCRIBE", *[str(i) for i in range(129)]))
        with self.assertRaises((EOFError, ConnectionResetError)):
            excess.read()

    def test_redis72_wire_contract(self):
        for password in ("sentinel-secret", ""):
            node, port = self.node(password=password)
            sentinel_compat.check_port(self, port, password)
            node.terminate()
            # Discovery exchanges carry committed topology claims, so Lavik
            # answers them only on a caught-up leader; replay them there.
            leader, leader_port = self.node(password=password, bootstrap=True)
            H.wait_until("Meta leader", 5, leader.is_leader)
            sentinel_compat.check_discovery_port(self, leader_port, password)
            leader.terminate()

    def test_connection_commands_on_leader_and_waiting_follower(self):
        leader, leader_port = self.node(bootstrap=True)
        follower, follower_port = self.node()
        H.wait_until("Meta leader", 5, leader.is_leader)
        self.assertFalse(follower.is_leader())
        for port in (leader_port, follower_port):
            client = self.client(port)
            self.assertEqual(client.command("AUTH", "sentinel-secret"), b"OK")
            self.assertEqual(client.command("HELLO", 3)[b"mode"], b"sentinel")
            self.error(client.command("AUTH", "wrong"), b"WRONGPASS")
            self.assertEqual(client.command("PING"), b"PONG")
        leader.terminate()
        follower.terminate()

    def test_hello_reset_and_connection_isolation(self):
        node, port = self.node()
        first = self.client(port)
        second = self.client(port)
        self.error(first.command("HELLO", 3), b"NOAUTH")
        self.error(
            first.command("HELLO", 3, "AUTH", "other", "sentinel-secret"), b"WRONGPASS"
        )
        self.error(
            first.command(
                "HELLO", 3, "AUTH", "default", "sentinel-secret", "SETNAME", "bad name"
            ),
            b"ERR",
        )
        self.error(first.command("PING"), b"NOAUTH")
        hello = first.command(
            "HELLO", 3, "AUTH", "default", "sentinel-secret", "SETNAME", "client-one"
        )
        self.assertIsInstance(hello, dict)
        self.assertEqual(hello[b"mode"], b"sentinel")
        self.assertEqual(hello[b"proto"], 3)
        self.assertNotIn(b"role", hello)
        self.error(second.command("PING"), b"NOAUTH")
        self.error(first.command("HELLO", 2, "AUTH", "default", "wrong"), b"WRONGPASS")
        self.assertEqual(first.command("HELLO")[b"proto"], 3)
        self.error(first.command("HELLO", 4), b"NOPROTO")
        self.assertEqual(first.command("CLIENT", "SETNAME", "name-two"), b"OK")
        self.assertEqual(
            first.command("CLIENT", "SETINFO", "LIB-NAME", "redis-py"), b"OK"
        )
        self.assertEqual(first.command("CLIENT", "SETINFO", "LIB-VER", "8.1.0"), b"OK")
        self.error(first.command("CLIENT", "SETINFO", "LIB-VER", "bad version"), b"ERR")
        # Intentional stricter attribute validation than Redis 7.2's C-string
        # validator, which stops at the first NUL and accepts the suffix.
        self.error(first.command("CLIENT", "SETNAME", b"name\x00suffix"), b"ERR")
        self.error(
            first.command("CLIENT", "SETINFO", "LIB-NAME", b"lib\x00suffix"), b"ERR"
        )
        self.error(first.command("CLIENT", "LIST"), b"ERR")
        first.sock.sendall(
            encode("RESET")
            + encode("PING")
            + encode("AUTH", "sentinel-secret")
            + encode("HELLO")
        )
        self.assertEqual(first.read(), b"RESET")
        self.error(first.read(), b"NOAUTH")
        self.assertEqual(first.read(), b"OK")
        hello2 = first.read()
        self.assertIsInstance(hello2, list)
        self.assertEqual(dict(zip(hello2[::2], hello2[1::2]))[b"proto"], 2)
        second.sock.sendall(encode("QUIT") + encode("PING"))
        self.assertEqual(second.read(), b"OK")
        with self.assertRaises(EOFError):
            second.read()
        node.terminate()

    def test_no_password_and_command_allowlist(self):
        node, port = self.node(password="")
        client = self.client(port)
        self.assertEqual(client.command("PING", b"a\x00b"), b"a\x00b")
        self.error(
            client.command("AUTH", "anything"),
            b"ERR AUTH <password> called without any password",
        )
        self.error(
            client.command("HELLO", 3, "AUTH", "other", "anything"), b"WRONGPASS"
        )
        self.assertIsInstance(
            client.command("HELLO", 3, "AUTH", "default", "anything"), dict
        )
        for args in [
            ("GET", "key"),
            ("SET", "key", "value"),
            ("MULTI",),
            ("EVAL", "return 1", 0),
            ("FCALL", "f", 0),
            ("PUBLISH", "+switch-master", "fake"),
            ("CONFIG", "SET", "requirepass", "x"),
            ("ACL", "LIST"),
            ("REPLICAOF", "127.0.0.1", 1),
            ("PSYNC", "?", -1),
            ("status",),
            ("clusterhead", 1),
            ("submitop", "x"),
            ("SENTINEL",),
            ("SENTINEL", "GARBAGE"),
            ("SENTINEL", "MONITOR", "x"),
            ("SENTINEL", "FAILOVER", "x"),
            ("SENTINEL", "IS-MASTER-DOWN-BY-ADDR", "127.0.0.1", 1, 1, "x"),
        ]:
            with self.subTest(args=args):
                self.error(client.command(*args), b"ERR")
        # This node is a waiting joiner and never becomes leader. The five
        # discovery verbs are leader-only: on a non-authoritative node they
        # close the connection without any reply, so a client must retry its
        # next seed rather than trust a stale answer.
        for args in [
            ("SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster"),
            ("SENTINEL", "MASTER", "mymaster"),
            ("SENTINEL", "MASTERS"),
            ("SENTINEL", "REPLICAS", "mymaster"),
            ("SENTINEL", "SLAVES", "mymaster"),
        ]:
            with self.subTest(args=args):
                discovery = self.client(port)
                discovery.sock.sendall(encode(*args))
                # A reply followed by a close would surface the reply first;
                # immediate EOF/reset proves the no-reply drop contract.
                with self.assertRaises((EOFError, ConnectionResetError)):
                    discovery.read()
        # Arity is validated before the authority check, so even this
        # never-leader node answers malformed discovery calls explicitly and
        # keeps the connection open.
        for args in [
            ("SENTINEL", "GET-MASTER-ADDR-BY-NAME"),
            ("SENTINEL", "GET-MASTER-ADDR-BY-NAME", "a", "b"),
            ("SENTINEL", "MASTER"),
            ("SENTINEL", "MASTERS", "extra"),
            ("SENTINEL", "REPLICAS"),
            ("SENTINEL", "SLAVES"),
            ("AUTH",),
            ("PING", "a", "b"),
            ("QUIT", "extra"),
            ("RESET", "extra"),
            ("CLIENT", "SETNAME"),
            ("HELLO", 3, "AUTH", "default"),
        ]:
            with self.subTest(args=args):
                self.error(client.command(*args), b"ERR")
        self.assertEqual(client.command("RESET"), b"RESET")
        self.assertEqual(client.command("PING"), b"PONG")
        node.terminate()

    def test_discovery_null_contract_on_bootstrap_leader(self):
        leader, leader_port = self.node(bootstrap=True)
        follower, follower_port = self.node()
        H.wait_until("Meta leader", 5, leader.is_leader)
        self.assertFalse(follower.is_leader())
        client = self.client(leader_port)
        self.assertEqual(client.command("AUTH", "sentinel-secret"), b"OK")
        # No cluster was ever created: every service name is unknown. The
        # null/error matrix matches real Redis 7.2 on a bare Sentinel.
        self.assertEqual(client.command("SENTINEL", "MASTERS"), [])
        self.assertIsNone(
            client.command("SENTINEL", "GET-MASTER-ADDR-BY-NAME", "nosuch")
        )
        for subcommand in ("MASTER", "REPLICAS", "SLAVES"):
            with self.subTest(subcommand=subcommand):
                self.error(
                    client.command("SENTINEL", subcommand, "nosuch"),
                    b"ERR No such master with that name",
                )
        hello = client.command("HELLO", 3)
        self.assertEqual(hello[b"proto"], 3)
        self.assertEqual(client.command("SENTINEL", "MASTERS"), [])
        self.assertIsNone(
            client.command("SENTINEL", "GET-MASTER-ADDR-BY-NAME", "nosuch")
        )
        for subcommand in ("MASTER", "REPLICAS", "SLAVES"):
            with self.subTest(subcommand=subcommand, proto=3):
                self.error(
                    client.command("SENTINEL", subcommand, "nosuch"),
                    b"ERR No such master with that name",
                )
        # The follower answers connection commands but drops discovery.
        follower_client = self.client(follower_port)
        self.assertEqual(follower_client.command("AUTH", "sentinel-secret"), b"OK")
        self.assertEqual(follower_client.command("PING"), b"PONG")
        discovery = self.client(follower_port)
        discovery.sock.sendall(
            encode("AUTH", "sentinel-secret") + encode("SENTINEL", "MASTERS")
        )
        self.assertEqual(discovery.read(), b"OK")
        with self.assertRaises((EOFError, ConnectionResetError)):
            discovery.read()
        leader.terminate()
        follower.terminate()

    def test_fragmentation_pipeline_malformed_and_input_limit(self):
        node, port = self.node(password="")
        client = self.client(port)
        for byte in encode("HELLO", 3):
            client.sock.sendall(bytes([byte]))
            time.sleep(0.001)
        self.assertEqual(client.read()[b"proto"], 3)
        client.sock.sendall(b"".join(encode("PING", str(i)) for i in range(200)))
        self.assertEqual(
            [client.read() for _ in range(200)], [str(i).encode() for i in range(200)]
        )
        client.sock.sendall(b"PING inline\r\n")
        self.assertEqual(client.read(), b"inline")
        for malformed in [
            b"*wat\r\n",
            b"*1\r\n$-2\r\n",
            b"*1\r\n$1\r\nx!!",
            b"*9999999999999999999999999999999999999999\r\n",
            encode("PING", b"x" * (64 * 1024)),
        ]:
            with self.subTest(input=malformed[:32]):
                bad = self.client(port)
                bad.sock.sendall(malformed)
                self.error(bad.read(), b"ERR")
                with self.assertRaises((EOFError, ConnectionResetError)):
                    bad.read()
        self.assertEqual(client.command("PING", b"x" * 65000), b"x" * 65000)
        node.terminate()

    def test_connection_limit_and_shutdown_drain(self):
        node, port = self.node(maxclients=2)
        # The readiness probe must retire before the two intentional holders.
        time.sleep(0.05)
        first, second = self.client(port), self.client(port)
        self.assertEqual(first.command("AUTH", "sentinel-secret"), b"OK")
        self.error(second.command("PING"), b"NOAUTH")
        excess = self.client(port)
        self.error(excess.read(), b"ERR max number of clients")
        with self.assertRaises((EOFError, ConnectionResetError)):
            excess.read()
        first.close()
        time.sleep(0.05)
        replacement = self.client(port)
        self.assertEqual(replacement.command("AUTH", "sentinel-secret"), b"OK")
        replacement.sock.sendall(b"*2\r\n$4\r\nPING\r\n$200\r\npartial")
        node.terminate()
        with self.assertRaises((EOFError, ConnectionResetError)):
            replacement.read()
        with self.assertRaises((EOFError, ConnectionResetError)):
            second.read()

    def test_auth_and_partial_request_deadlines(self):
        node, port = self.node()
        anonymous = self.client(port)
        partial = self.client(port)
        idle = self.client(port)
        self.assertEqual(partial.command("AUTH", "sentinel-secret"), b"OK")
        self.assertEqual(idle.command("AUTH", "sentinel-secret"), b"OK")
        partial.sock.sendall(b"*2\r\n$4\r\nPING\r\n$100\r\nx")
        # RESET is allowed before authentication but must not renew the
        # original authentication deadline and hold a client slot forever.
        for _ in range(3):
            time.sleep(2)
            self.assertEqual(anonymous.command("RESET"), b"RESET")
        for client in (anonymous, partial):
            client.sock.settimeout(5)
            with self.assertRaises((EOFError, ConnectionResetError)):
                client.read()
        self.assertEqual(idle.command("PING"), b"PONG")
        node.terminate()

    def test_slow_reader_keeps_admin_responsive_and_drains(self):
        node, port = self.node(password="", maxclients=1, bootstrap=True)
        H.wait_until("Meta leader", 5, node.is_leader)
        time.sleep(0.05)
        slow = self.client(port)
        slow.command("HELLO", 3)
        slow.command("SUBSCRIBE", "+switch-master", "+replica-reconf-done")
        slow.read()
        slow.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        stop = threading.Event()

        def flood():
            try:
                request = encode("PING", b"x" * 60000)
                while not stop.is_set():
                    slow.sock.sendall(request)
            except OSError:
                pass

        thread = threading.Thread(target=flood)
        thread.start()
        try:
            time.sleep(0.3)
            self.assertIn("leader=", node.ctl("status", timeout=2))

            def admission_recovered():
                with contextlib.closing(Client(port)) as probe:
                    return probe.command("PING") == b"PONG"

            H.wait_until(
                "slow writer deadline releases client slot", 14, admission_recovered
            )
            node.terminate()
            self.assertLessEqual(node.proc.returncode, 0)
        finally:
            stop.set()
            with contextlib.suppress(OSError):
                slow.sock.shutdown(socket.SHUT_RDWR)
            thread.join(timeout=5)
        self.assertFalse(thread.is_alive())

    def test_ipv6_and_binary_password_comparison(self):
        if not socket.has_ipv6:
            self.skipTest("host has no IPv6 support")
        node, port = self.node(password="sëcret", host="::1")
        client = self.client(port, "::1")
        self.error(client.command("AUTH", b"s\x00cret"), b"WRONGPASS")
        self.error(client.command("AUTH", "DEFAULT", "sëcret"), b"WRONGPASS")
        self.assertEqual(client.command("AUTH", "default", "sëcret"), b"OK")
        self.assertEqual(client.command("PING"), b"PONG")
        node.terminate()

    def test_configuration_rejected_before_start(self):
        for extra in [
            ["--sentinel-addr", "0.0.0.0:26379"],
            ["--sentinel-addr", "[::]:26379"],
            ["--sentinel-addr", "[::ffff:0.0.0.0]:26379"],
            ["--sentinel-addr", "localhost:26379"],
            ["--sentinel-addr", "127.0.0.1:0"],
            ["--sentinel-addr", "127.0.0.1:65001"],
            ["--sentinel-requirepass", "secret"],
            ["--sentinel-addr", "127.0.0.1:26379", "--sentinel-maxclients", "0"],
        ]:
            with self.subTest(options=extra):
                result = subprocess.run(
                    [
                        BINARY,
                        "--id",
                        "1",
                        "--addr",
                        "127.0.0.1:65001",
                        "--data-control-addr",
                        "127.0.0.1:65002",
                        "--ctl-addr",
                        "127.0.0.1:65003",
                        "--data-dir",
                        os.path.join(self.root.name, "invalid"),
                    ]
                    + extra,
                    capture_output=True,
                    timeout=5,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(b"entinel", result.stderr)
                self.assertFalse(
                    os.path.exists(os.path.join(self.root.name, "invalid"))
                )

    def test_disabled_by_default_and_bind_failure_rolls_back(self):
        node = H.Node(BINARY, self.root.name, 1)
        self.addCleanup(node.force_kill)
        node.start()
        H.wait_until(
            "default Sentinel disabled",
            3,
            lambda: "sentinel=disabled" in Path(node.log_path).read_text(),
        )
        node.terminate()
        with socket.socket() as occupied:
            occupied.bind(("127.0.0.1", 0))
            occupied.listen()
            node = H.Node(
                BINARY,
                self.root.name,
                2,
                args=H.raft_args()
                + ["--sentinel-addr", f"127.0.0.1:{occupied.getsockname()[1]}"],
            )
            self.addCleanup(node.force_kill)
            node.start(wait_ready=False)
            self.assertNotEqual(node.proc.wait(timeout=10), 0)
            self.assertIn(
                "Sentinel listener bind failed", Path(node.log_path).read_text()
            )

            def ctl_closed():
                try:
                    with socket.create_connection(
                        ("127.0.0.1", node.ctl_port), timeout=0.1
                    ):
                        return False
                except OSError:
                    return True

            # Kernel io_uring teardown may release its last accept reference
            # after waitpid returns; it must still retire within a bounded wait.
            H.wait_until("Admin listener rollback", 3, ctl_closed)

    def test_data_standalone_auth_and_sentinel_credentials_are_independent(self):
        if DATA_BINARY is None:
            self.skipTest("Data binary not supplied")
        port = H.free_port()
        data = Path(self.root.name) / "standalone.data"
        with data.open("wb") as file:
            os.posix_fallocate(file.fileno(), 0, 256 * 1024 * 1024)
        logfile = self.enterContext((Path(self.root.name) / "data.log").open("w"))
        args = [
            DATA_BINARY,
            "--bind",
            "127.0.0.1",
            "--port",
            str(port),
            "--threads",
            "2",
            "--no-pin-workers",
            "--recv-buffers-per-worker",
            "0",
            "--registered-buffer-mb-per-worker",
            "64",
            "--max-memory",
            "1G",
            "--metrics-port",
            "0",
            "--data-file",
            str(data),
            "--rdb-dir",
            self.root.name,
            "--logtostderr",
            "--requirepass",
            "data-secret",
        ]
        proc = subprocess.Popen(args, stdout=logfile, stderr=subprocess.STDOUT)

        def cleanup():
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)

        self.addCleanup(cleanup)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            self.assertIsNone(proc.poll(), Path(logfile.name).read_text())
            try:
                client = self.client(port)
                break
            except OSError:
                time.sleep(0.05)
        else:
            self.fail("standalone Data did not bind")
        # No Meta process exists during this part of the test.
        self.error(client.command("GET", "key"), b"NOAUTH")
        self.error(client.command("AUTH", "sentinel-secret"), b"WRONGPASS")
        self.error(client.command("AUTH", "other", "data-secret"), b"WRONGPASS")
        self.assertEqual(client.command("AUTH", "data-secret"), b"OK")
        self.assertEqual(client.command("SET", "key", "value"), b"OK")
        self.assertEqual(client.command("GET", "key"), b"value")
        self.assertIsInstance(client.command("HELLO", 3), dict)
        self.error(client.command("HELLO", 2, "AUTH", "default", "wrong"), b"WRONGPASS")
        self.assertIsInstance(client.command("HELLO"), dict)
        self.assertEqual(client.command("RESET"), b"RESET")
        self.error(client.command("GET", "key"), b"NOAUTH")
        self.assertIsInstance(
            client.command("HELLO", 2, "AUTH", "default", "data-secret"), list
        )
        node, sentinel_port = self.node()
        sentinel = self.client(sentinel_port)
        self.error(sentinel.command("AUTH", "data-secret"), b"WRONGPASS")
        self.assertEqual(sentinel.command("AUTH", "sentinel-secret"), b"OK")
        other_data = self.client(port)
        self.error(other_data.command("GET", "key"), b"NOAUTH")
        node.terminate()
        self.assertEqual(client.command("GET", "key"), b"value")
        proc.terminate()
        self.assertEqual(proc.wait(timeout=15), 0)


if __name__ == "__main__":
    BINARY = os.path.abspath(sys.argv.pop(1))
    if (
        len(sys.argv) > 1
        and not sys.argv[1].startswith("-")
        and os.path.isfile(sys.argv[1])
    ):
        DATA_BINARY = os.path.abspath(sys.argv.pop(1))
    unittest.main()
