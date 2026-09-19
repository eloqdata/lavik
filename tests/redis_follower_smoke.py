#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise the external follower contract against real Redis and Lavik peers."""
from contextlib import contextmanager
import os
from pathlib import Path
import signal
import selectors
import threading
import socket
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).parent / "meta_integration"))
import gate_failover as F
import harness as H


class Client:
    def __init__(self, port):
        self.socket = socket.create_connection(("127.0.0.1", port), 2)
        self.socket.settimeout(15)
        self.reader = self.socket.makefile("rb")

    def call(self, *args):
        self.socket.sendall(F.encode_resp([str(a) for a in args]))
        if self.reader.peek(1)[:1] == b"%":
            self.reader.read(1)
            count = int(self.reader.readline())
            return {F.read_resp(self.reader): F.read_resp(self.reader) for _ in range(count)}
        return F.read_resp(self.reader)

    def close(self):
        self.reader.close()
        self.socket.close()


def reject(client, args, text):
    try:
        result = client.call(*args)
    except H.Failure as error:
        assert text in str(error), str(error)
    else:
        raise AssertionError(f"{args} unexpectedly returned {result}")


@contextmanager
def process(binary, directory, name, *, redis=False, extra=(), port=None, password=None):
    port = port or H.free_port()
    directory.mkdir(exist_ok=True)
    if redis:
        args = [binary, "--bind", "127.0.0.1", "--port", str(port),
                "--save", "", "--appendonly", "no", "--dir", str(directory),
                "--repl-diskless-sync", "no"]
    else:
        data = directory / "lavik.data"
        if not data.exists():
            with data.open("wb") as file:
                os.posix_fallocate(file.fileno(), 0, 256 * 1024 * 1024)
        args = [binary, *([str(directory / "lavik.conf")] if (directory / "lavik.conf").exists() else []),
                "--bind", "127.0.0.1", "--port", str(port),
                "--threads", "2", "--no-pin-workers", "--metrics-port", "0",
                "--recv-buffers-per-worker", "0", "--max-memory", "1G",
                "--registered-buffer-mb-per-worker", "64",
                "--data-file", str(data), "--rdb-dir", str(directory), "--logtostderr"]
    log_path = directory / f"{name}.log"
    with log_path.open("w") as log:
        child = subprocess.Popen(args + list(extra), stdout=log, stderr=log)
        client = None
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                assert child.poll() is None, log_path.read_text()
                try:
                    client = Client(port)
                    if password is not None:
                        assert client.call("AUTH", password) == "OK"
                    assert client.call("PING") == "PONG"
                    break
                except OSError:
                    time.sleep(.05)
            assert client is not None, log_path.read_text()
            yield client, port, log_path
        except BaseException:
            print(log_path.read_text()[-12000:], file=sys.stderr)
            raise
        finally:
            if client:
                client.close()
            child.send_signal(signal.SIGINT) if child.poll() is None else None
            try:
                child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


class Forwarder:
    """Change an endpoint between discovery and its consuming TCP connection."""
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.accepted = 0
        self.cut_after = None
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.port = self.listener.getsockname()[1]
        self.listener.listen()
        self.listener.settimeout(.1)
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self.run)
        self.peers = []
        self.thread.start()

    def run(self):
        while not self.stopped.is_set():
            try:
                incoming, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            self.accepted += 1
            endpoint = self.endpoint(self.accepted)
            peer = threading.Thread(target=self.relay, args=(incoming, endpoint))
            self.peers.append(peer)
            peer.start()

    def relay(self, incoming, endpoint):
        with incoming:
            try:
                with socket.create_connection(("127.0.0.1", endpoint), 2) as outgoing, selectors.DefaultSelector() as poll:
                    poll.register(incoming, selectors.EVENT_READ, outgoing)
                    poll.register(outgoing, selectors.EVENT_READ, incoming)
                    while not self.stopped.is_set():
                        for key, _ in poll.select(.1):
                            data = key.fileobj.recv(65536)
                            if not data:
                                return
                            if key.fileobj is outgoing and self.cut_after is not None:
                                remaining = self.cut_after
                                self.cut_after = None
                                key.data.sendall(data[:remaining])
                                return
                            key.data.sendall(data)
            except OSError:
                return

    def close(self):
        self.stopped.set()
        self.listener.close()
        self.thread.join(timeout=3)
        for peer in self.peers:
            peer.join(timeout=3)


def changing_endpoint(lavik, redis, root):
    with process(lavik, root / "changed-native", "native") as (native, native_port, _), \
         process(redis, root / "changed-redis", "redis", redis=True) as (source, source_port, _):
        native.call("SET", "must-not-import", "native")
        source.call("SET", "redis-value", "redis")
        # Discovery uses one connection. Every consumer attempt sees Lavik,
        # including an explicit Redis alias supplied at startup.
        proxy = Forwarder(lambda count: source_port if count == 1 else native_port)
        directory = root / "changed-target"
        try:
            with process(lavik, directory, "seed") as (target, _, _):
                target.call("SET", "retained-before-full", "local")
            with process(lavik, directory, "switched", extra=(
                    "--redis-replicaof", "127.0.0.1", str(proxy.port))) as (target, _, log):
                H.wait_until("consumer rejects changed native endpoint", 15,
                             lambda: "external replication requires a Redis" in log.read_text())
                rejects_before = proxy.accepted
                time.sleep(1.2)
                assert proxy.accepted == rejects_before, "confirmed native endpoint was retried"
                reject(target, ("GET", "retained-before-full"), "LOADING")
            with process(lavik, directory, "recover") as (target, _, _):
                assert target.call("GET", "retained-before-full") == "local"
                assert target.call("EXISTS", "must-not-import") == 0
        finally:
            proxy.close()
        # A reconnect must repeat the type check before any incremental apply.
        endpoint = [source_port]
        proxy = Forwarder(lambda _: endpoint[0])
        try:
            with process(lavik, root / "changed-online", "online") as (target, _, log):
                target.call("REPLICAOF", "127.0.0.1", proxy.port)
                H.wait_until("initial Redis endpoint online", 20,
                             lambda: target.call("GET", "redis-value") == "redis")
                endpoint[0] = native_port
                source.call("CLIENT", "KILL", "TYPE", "REPLICA")
                H.wait_until("reconnect rejects changed native endpoint", 15,
                             lambda: "external replication requires a Redis" in log.read_text())
                assert "role:slave" in target.call("INFO", "replication")
                reject(target, ("SET", "unfenced", "wrong"), "READONLY")
                assert target.call("GET", "redis-value") == "redis"
                assert target.call("EXISTS", "must-not-import") == 0
        finally:
            proxy.close()


def interrupted_transaction(lavik, redis, root):
    with process(redis, root / "tx-redis", "source", redis=True) as (source, port, _), \
         process(lavik, root / "tx-target", "target") as (target, _, log):
        proxy = Forwarder(lambda _: port)
        try:
            source.call("SET", "db0-proof", "db0")
            target.call("REPLICAOF", "127.0.0.1", proxy.port)
            H.wait_until("transaction source online", 20,
                         lambda: target.call("GET", "db0-proof") == "db0")
            full_count = log.read_text().count("Redis FULLRESYNC completed")
            source.call("SELECT", 15)
            # Cut after MULTI/SELECT and inside a value. No part of this
            # transaction, including its DB cursor, may be acknowledged.
            proxy.cut_after = 70
            source.call("MULTI")
            source.call("SET", "large-tx", "x" * 16384)
            source.call("INCR", "once")
            source.call("EXEC")
            target.call("SELECT", 15)
            H.wait_until("interrupted MULTI replay", 20,
                         lambda: target.call("GET", "once") == "1")
            assert target.call("STRLEN", "large-tx") == 16384
            assert log.read_text().count("Redis FULLRESYNC completed") == full_count
            target.call("SELECT", 0)
            assert target.call("EXISTS", "once") == 0
            assert target.call("GET", "db0-proof") == "db0"
        finally:
            proxy.close()


def unavailable_startup(lavik, redis, root):
    port = H.free_port()
    with process(lavik, root / "retry-target", "target", extra=(
            "--redis-replicaof", "127.0.0.1", str(port))) as (target, _, _):
        reject(target, ("SET", "not-authoritative", "wrong"), "LOADING")
        with process(redis, root / "late-source", "source", redis=True,
                     port=port) as (source, _, _):
            source.call("SET", "late-upstream", "recovered")
            H.wait_until("unavailable startup retries", 20,
                         lambda: target.call("GET", "late-upstream") == "recovered")


def exercise(lavik, redis, root):
    with process(lavik, root / "native", "native") as (native, native_port, _), \
         process(lavik, root / "target", "target") as (target, _, target_log):
        assert target.call("SET", "retained", "original") == "OK"
        reject(target, ("REPLICAOF", "127.0.0.1", native_port), "Redis")
        reject(target, ("SLAVEOF", "127.0.0.1", native_port), "Redis")
        assert target.call("GET", "retained") == "original"
        assert "role:master" in target.call("INFO", "replication")
        target.call("SELECT", 15)
        target.call("SET", "old-db15", "must-clear")
        target.call("SELECT", 0)
        with process(redis, root / "redis", "redis", redis=True) as (source, source_port, _):
            assert source.call("SELECT", 15) == "OK"
            assert source.call("SET", "baseline", "db15") == "OK"
            assert source.call("SET", "ttl", "live", "PX", 120000) == "OK"
            assert source.call("FUNCTION", "LOAD", "#!lua name=follow\nredis.register_function{function_name='follow_value', callback=function() return 'function-value' end, flags={'no-writes'}}") == "follow"
            assert target.call("REPLICAOF", "127.0.0.1", source_port) == "OK"
            H.wait_until("Redis full sync", 30,
                         lambda: "master_link_status:up" in target.call("INFO", "replication"))
            assert target.call("SELECT", 15) == "OK"
            assert target.call("GET", "baseline") == "db15"
            assert target.call("EXISTS", "old-db15") == 0
            assert target.call("PTTL", "ttl") > 0
            assert target.call("FCALL_RO", "follow_value", 0) == "function-value"
            assert source.call("MULTI") == "OK"
            assert source.call("SET", "tx-a", "a") == "QUEUED"
            assert source.call("SET", "tx-b", "b") == "QUEUED"
            assert source.call("EXEC") == ["OK", "OK"]
            H.wait_until("transaction replay", 10,
                         lambda: target.call("MGET", "tx-a", "tx-b") == ["a", "b"])
            reject(target, ("REPLICAOF", "127.0.0.1", native_port), "Redis")
            reject(target, ("REPLICAOF", "127.0.0.1", H.free_port()), "ERR")
            assert target.call("GET", "baseline") == "db15"
            assert source.call("SET", "after-reject", "still-following") == "OK"
            H.wait_until("old subscription after rejection", 10,
                         lambda: target.call("GET", "after-reject") == "still-following")
            source.call("CLIENT", "KILL", "TYPE", "REPLICA")
            source.call("INCR", "exact-once")
            H.wait_until("partial reconnect", 15,
                         lambda: target.call("GET", "exact-once") == "1" and
                         "Redis partial resynchronization continued" in target_log.read_text())
    # Both startup forms must identify a native peer without erasing recovered data.
    with process(lavik, root / "native", "native-again") as (_, native_port, _):
        for index, option in enumerate(("replicaof", "redis-replicaof")):
            (root / "target" / "lavik.conf").write_text(f"{option} 127.0.0.1 {native_port}\n")
            with process(lavik, root / "target", f"rejected-{index}",
                         extra=()) as (target, _, log):
                H.wait_until("unsupported startup upstream", 15,
                             lambda: "requires a Redis" in log.read_text())
                reject(target, ("GET", "baseline"), "LOADING")
        (root / "target" / "lavik.conf").unlink()
        with process(lavik, root / "target", "recover") as (target, _, _):
            assert target.call("SELECT", 15) == "OK"
            assert target.call("GET", "baseline") == "db15"


def mode_contract(lavik, root):
    for args, expected in [
            (("--client-mode", "cluster"), "requires meta-managed yes"),
            (("--meta-managed", "yes"), "authority admission"),
            (("--client-mode", "bogus"), "single"),
            (("--meta-managed", "maybe"), "yes"),
            (("--cluster-enabled",), "not expected")]:
        absent = root / "must-not-create.data"
        result = subprocess.run([lavik, *args, "--data-file", str(absent)],
                                capture_output=True, text=True, timeout=10)
        assert result.returncode != 0, result
        assert expected in result.stdout + result.stderr, result
        assert not absent.exists()
    for cluster in (False, True):
        directory = root / f"mode-{cluster}"
        directory.mkdir()
        # File values are deliberately opposite to the CLI override.
        (directory / "lavik.conf").write_text(
            "client-mode single\nmeta-managed no\n" if cluster else
            "client-mode cluster\nmeta-managed yes\n")
        args = ["--client-mode", "cluster" if cluster else "single",
                "--meta-managed", "yes" if cluster else "no"]
        if cluster:
            args += ["--node-id", "1" * 40, "--meta-seed", "127.0.0.1:9"]
        with process(lavik, directory, "mode", extra=args) as (client, _, _):
            for version in (2, 3):
                hello = client.call("HELLO", version)
                if isinstance(hello, list):
                    hello = dict(zip(hello[::2], hello[1::2]))
                assert hello["mode"] == ("cluster" if cluster else "standalone")
                info = client.call("INFO", "server")
                assert "redis_mode:" + hello["mode"] in info
                assert "cluster_enabled:" + str(int(cluster)) in client.call("INFO", "cluster")
            if cluster:
                reject(client, ("SELECT", 15), "cluster mode")
                reject(client, ("SET", "fenced", "value"), "LOADING")
            else:
                assert client.call("SELECT", 15) == "OK"
                assert client.call("SET", "value", "db15") == "OK"


def authenticated_startup(lavik, redis, root):
    # Reuse a real server for both supported startup spellings and explicit CLI.
    with process(redis, root / "auth-source", "source", redis=True,
                 extra=("--requirepass", "source-secret"), password="source-secret") as (source, port, _):
        source.call("SET", "authenticated", "baseline")
        for index, spelling in enumerate(("replicaof", "redis-replicaof", "cli")):
            directory = root / f"auth-target-{index}"
            directory.mkdir()
            config = "masterauth source-secret\n"
            if spelling != "cli":
                config += f"{spelling} 127.0.0.1 {port}\n"
            (directory / "lavik.conf").write_text(config)
            extra = ("--redis-replicaof", "127.0.0.1", str(port)) if spelling == "cli" else ()
            with process(lavik, directory, "target", extra=extra) as (target, _, _):
                H.wait_until("authenticated startup full sync", 30,
                             lambda: target.call("GET", "authenticated") == "baseline")
                with process(redis, root / f"bad-auth-{index}", "bad-auth", redis=True,
                             extra=("--requirepass", "different-secret"),
                             password="different-secret") as (_, bad_port, _):
                    reject(target, ("REPLICAOF", "127.0.0.1", bad_port), "ERR")
                source.call("SET", "auth-tail", str(index))
                H.wait_until("subscription survives authentication error", 10,
                             lambda: target.call("GET", "auth-tail") == str(index))



if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="lavik-redis-follower-",
                                     dir=os.environ.get("LAVIK_TEST_DATA_DIR")) as directory:
        mode_contract(sys.argv[1], Path(directory))
        exercise(sys.argv[1], sys.argv[2], Path(directory))
        authenticated_startup(sys.argv[1], sys.argv[2], Path(directory))
        changing_endpoint(sys.argv[1], sys.argv[2], Path(directory))
        interrupted_transaction(sys.argv[1], sys.argv[2], Path(directory))
        unavailable_startup(sys.argv[1], sys.argv[2], Path(directory))
    print("Redis follower checks passed")
