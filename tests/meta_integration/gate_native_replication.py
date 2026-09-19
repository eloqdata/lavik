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

"""Native FULL, live replay and reconnect through real Meta authorization.

The old standalone REPLICAOF fixtures cannot authorize a native session. These
checks use the same manifest/bootstrap and directive barrier as cluster-create;
all native connections are admitted by production Meta and Follow Owner.
"""
from contextlib import contextmanager
import os
import concurrent.futures
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import gate_cluster_create as C
import harness as H
from gate_data_control import DataProcess


class Client:
    def __init__(self, node, readonly=False):
        self.context = C.redis_connection(node)
        self.socket = self.context.__enter__()
        self.socket.settimeout(30)
        self.reader = self.socket.makefile("rb")
        if readonly:
            assert self.call("READONLY") == "OK"

    def call(self, *args):
        self.socket.sendall(C.encode_resp([str(arg) for arg in args]))
        return C.read_resp(self.reader)

    def close(self):
        self.reader.close()
        self.context.__exit__(None, None, None)


@contextmanager
def pair(root, name, source_faults=None, target_faults=None, seed=None,
         source_workers=2, target_workers=3):
    directory = root / name
    directory.mkdir()
    meta = H.Node(C.META, str(directory), 1, args=C.creation_raft_args())
    proxy = C.DirectiveBarrier(meta.data_control_port, recipients=(C.REPLICA_1,))
    meta.advertised_data_control_endpoint = proxy.endpoint
    source = DataProcess(C.DATA, str(directory / "source"), C.PRIMARY_1,
                         proxy.endpoint, workers=source_workers,
                         environment={**os.environ, **(source_faults or {})})
    target = DataProcess(C.DATA, str(directory / "target"), C.REPLICA_1,
                         proxy.endpoint, workers=target_workers,
                         environment={**os.environ, **(target_faults or {})})
    lines = ['schema_version = 1', 'slot_strategy = "contiguous-even"']
    lines += C.meta_manifest_lines(meta)
    for node in (source, target):
        lines += ['[[data_nodes]]', f'id = "{node.node_id}"',
                  f'client_endpoint = "{node.advertised_endpoint}"']
    lines += ['[[groups]]', 'id = "group-1"', f'primary = "{source.node_id}"',
              f'replicas = ["{target.node_id}"]']
    manifest = directory / "cluster.toml"
    manifest.write_text("\n".join(lines) + "\n")
    clients = []
    try:
        proxy.start()
        meta.start(initial_cluster_manifest=str(manifest))
        meta.wait_leader()
        source.start()
        target.start()
        C.command(os.environ.copy(), [C.CTL, "cluster-create", "--manifest",
                  str(manifest), "--socket", meta.ctl_path, "--yes"])
        held, release = proxy.recipients[C.REPLICA_1]
        H.wait_until("native target directive held", 30, held.is_set)
        writer = Client(source)
        clients.append(writer)
        H.wait_until("source authority before FULL", 20,
                     lambda: writer.call("SET", "{native}seed", "baseline") == "OK")
        if seed:
            seed(writer)
        release.set()
        yield meta, source, target, writer
        for client in clients:
            client.close()
        clients.clear()
        target.terminate()
        source.terminate()
        meta.terminate()
    except BaseException:
        H.dump_node_logs([meta])
        for node in (source, target):
            print(node.log_tail(lines=150), file=sys.stderr)
        raise
    finally:
        for client in clients:
            client.close()
        proxy.close()
        target.force_kill()
        source.force_kill()
        meta.force_kill()


def ready(meta):
    C.wait_cluster_ready(meta, "native population and authority ready", 90)


def rejects(client, args, text):
    try:
        reply = client.call(*args)
    except H.Failure as error:
        assert text in str(error), error
    else:
        raise AssertionError(f"{args} unexpectedly returned {reply}")


def seed_collections(writer):
    writer.call("HSET", "{native}hash", "old", "old", "keep", "value")
    writer.call("RPUSH", "{native}list", "a", "b")
    writer.call("SADD", "{native}set", "a", "b")
    writer.call("ZADD", "{native}zset", 1, "a", 2, "b")
    writer.call("SET", "{native}ttl", "expiring", "PX", 120000)
    # A multi-page value exercises the streamed FULL path on heterogeneous
    # workers without allocating a giant fixture for each fault case.
    writer.call("SET", "{native}large", "x" * (2 * 1024 * 1024))

    fields = [part for i in range(256) for part in (f"field{i}", "v" * 8192)]
    members = [str(i) + "m" * 8192 for i in range(256)]
    writer.call("HSET", "{native}paged-hash", *fields)
    writer.call("SADD", "{native}paged-set", *members)
    writer.call("RPUSH", "{native}paged-list", *members)
    scores = [part for i, member in enumerate(members) for part in (i, member)]
    writer.call("ZADD", "{native}paged-zset", *scores)
    writer.call("XADD", "{native}stream", "1-0", "field", "first")
    writer.call("XGROUP", "CREATE", "{native}stream", "group", "0")
    writer.call("XREADGROUP", "GROUP", "group", "consumer", "STREAMS", "{native}stream", ">")


def replay_and_reconnect(root):
    with pair(root, "replay", seed=seed_collections) as (meta, source, target, writer):
        ready(meta)
        reader = Client(target, readonly=True)
        try:
            assert reader.call("GET", "{native}seed") == "baseline"
            assert reader.call("HGET", "{native}hash", "keep") == "value"
            assert reader.call("LRANGE", "{native}list", 0, -1) == ["a", "b"]
            assert reader.call("SCARD", "{native}set") == 2
            assert reader.call("ZRANGE", "{native}zset", 0, -1) == ["a", "b"]
            assert reader.call("STRLEN", "{native}large") == 2 * 1024 * 1024
            assert reader.call("PEXPIRETIME", "{native}ttl") == writer.call("PEXPIRETIME", "{native}ttl")
            for client in (writer, reader):
                for args in (("REPLICAOF", "NO", "ONE"),
                             ("SLAVEOF", "127.0.0.1", source.redis_port),
                             ("ADDREPLICAOF", "127.0.0.1", source.redis_port)):
                    rejects(client, args, "not allowed")
            for command, key in (("HLEN", "paged-hash"), ("SCARD", "paged-set"),
                                 ("LLEN", "paged-list"), ("ZCARD", "paged-zset")):
                assert reader.call(command, "{native}" + key) == 256
            assert reader.call("XINFO", "STREAM", "{native}stream", "FULL", "COUNT", 10) == writer.call("XINFO", "STREAM", "{native}stream", "FULL", "COUNT", 10)
            writer.call("XADD", "{native}stream", "2-0", "field", "second")
            writer.call("XCLAIM", "{native}stream", "group", "claimed", 0,
                        "1-0", "TIME", 123456, "RETRYCOUNT", 7, "JUSTID")
            expected_stream = writer.call("XINFO", "STREAM", "{native}stream", "FULL", "COUNT", 10)
            H.wait_until("stream consumer state replay", 20,
                         lambda: reader.call("XINFO", "STREAM", "{native}stream", "FULL", "COUNT", 10) == expected_stream)
            # Replacement and non-idempotent commands must preserve type,
            # absolute expiry, transaction ordering, and exact apply counts.
            writer.call("LAVIK.HREPLACE", "{native}hash", "new", "replacement")
            writer.call("MULTI")
            writer.call("INCR", "{native}count")
            writer.call("RPUSH", "{native}list", "c")
            writer.call("HSET", "{native}hash", "tx", "value")
            assert writer.call("EXEC") == [1, 3, 1]
            assert writer.call("WAIT", 1, 5000) == 1
            assert reader.call("GET", "{native}count") == "1"
            assert reader.call("HEXISTS", "{native}hash", "old") == 0
            assert reader.call("HGET", "{native}hash", "tx") == "value"
            assert reader.call("LRANGE", "{native}list", 0, -1) == ["a", "b", "c"]
            # Native reconnects retain source history and applied cursors.
            old_full = Path(source.log_path).read_text().count("selected=FULL")
            assert writer.call("CLIENT", "KILL", "TYPE", "replica") > 0
            writer.call("INCR", "{native}count")
            H.wait_until("Follow Owner resumes exactly once", 30,
                         lambda: reader.call("GET", "{native}count") == "2")
            H.wait_until("Follow Owner continuation", 30,
                         lambda: "selected=CONTINUE" in Path(source.log_path).read_text())
            assert Path(source.log_path).read_text().count("selected=FULL") == old_full
            # Real transactions also carry ephemeral PUBLISH under authority.
            subscriber = Client(target, readonly=True)
            try:
                assert subscriber.call("SUBSCRIBE", "{native}channel") == ["subscribe", "{native}channel", 1]
                writer.call("MULTI")
                writer.call("SET", "{native}mixed", "written")
                writer.call("PUBLISH", "{native}channel", "message")
                writer.call("EXEC")
                assert C.read_resp(subscriber.reader) == ["message", "{native}channel", "message"]
                assert reader.call("GET", "{native}mixed") == "written"
            finally:
                subscriber.close()
            # Expiration comes from the authoritative source and is replayed.
            writer.call("PEXPIRE", "{native}ttl", 20)
            H.wait_until("replicated authoritative expiration", 10,
                         lambda: reader.call("EXISTS", "{native}ttl") == 0)
        finally:
            reader.close()
        # Shutdown must join flow owners even if the upstream cannot reply.
        source.pause()
        try:
            started = time.monotonic()
            target.terminate()
            assert time.monotonic() - started < 10
        finally:
            source.resume()


def handoff_order(root):
    with pair(root, "handoff", target_faults={
            "LAVIK_REPLICATION_HOLD_FIRST_HANDOFF_UNTIL_NEXT_ACK": "1"},
            source_workers=1, target_workers=1) as (meta, source, target, writer):
        ready(meta)
        log = Path(target.log_path).read_text()
        assert log.index("holding first partition handoff") < log.index("acknowledged async partition handoff 1") < log.index("acknowledged async partition handoff 0")
        assert C.readonly_get(target, "{native}seed") == "baseline"
        writer.call("INCR", "{native}tail")
        H.wait_until("handoff tail", 20, lambda: C.readonly_get(target, "{native}tail") == "1")


def cancelled_handoff(root):
    with pair(root, "cancel-handoff", target_faults={
            "LAVIK_REPLICATION_HOLD_FIRST_HANDOFF_UNTIL_NEXT_ACK": "cancel"},
            source_workers=1, target_workers=1) as (_, source, target, _writer):
        H.wait_until("outstanding native handoff", 30,
                     lambda: "holding first partition handoff" in Path(target.log_path).read_text())
        reader = Client(target, readonly=True)
        try:
            rejects(reader, ("GET", "{native}seed"), "LOADING")
        finally:
            reader.close()
        target.terminate()
        assert "acknowledged async partition handoff 0" not in Path(target.log_path).read_text()


def rejected_full(root, name, source_faults, target_faults, marker):
    with pair(root, name, source_faults=source_faults,
              target_faults=target_faults) as (meta, source, target, _writer):
        H.wait_until(name + " fault reached", 30,
                     lambda: marker in Path(target.log_path).read_text() + Path(source.log_path).read_text())
        reader = Client(target, readonly=True)
        try:
            rejects(reader, ("GET", "{native}seed"), "LOADING")
            rejects(reader, ("REPLICAOF", "NO", "ONE"), "not allowed")
            # A failed directed rebuild cannot manufacture an autonomous new
            # attempt or publish ONLINE without another Meta authorization.
            H.wait_until(name + " reported to Meta", 30,
                         lambda: C.cluster_status(meta).get("cluster_state") == "provisioning-failed")
            assert "lavik_replication_state:online" not in reader.call("INFO", "replication")
        finally:
            reader.close()


def full_tail(root):
    with pair(root, "full-tail", source_faults={
            "LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "1000",
            "LAVIK_REPLICATION_PAUSE_FULLSYNC_BEFORE_CUT_MS": "1000"},
            seed=seed_collections) as (meta, source, target, writer):
        for i in range(32):
            writer.call("MULTI")
            writer.call("INCR", "{native}count")
            writer.call("HSET", "{native}paged-hash", "field0", str(i))
            writer.call("RPUSH", "{native}list", str(i))
            writer.call("EXEC")
        ready(meta)
        reader = Client(target, readonly=True)
        try:
            assert reader.call("GET", "{native}count") == "32"
            assert reader.call("HGET", "{native}paged-hash", "field0") == "31"
            assert reader.call("LRANGE", "{native}list", 0, -1) == ["a", "b"] + list(map(str, range(32)))
            # Pressure across changing source workers must release every
            # admission reservation, including writes on non-connection owners.
            writer.call("CONFIG", "SET", "replication-publish-queue-mb-per-worker", 1)
            for i in range(160):
                writer.call("SET", f"waterline-{i % 17}", str(i) + "x" * 32768)
            assert writer.call("WAIT", 1, 5000) == 1
            for i in range(17):
                assert reader.call("GET", f"waterline-{i}") == writer.call("GET", f"waterline-{i}")
        finally:
            reader.close()


def backpressured_shutdown(root):
    with pair(root, "backpressure", source_workers=1, target_workers=1) as (meta, source, target, writer):
        ready(meta)
        writer.call("CONFIG", "SET", "replication-publish-queue-mb-per-worker", 1)
        writer.call("SET", "ack-baseline", "ready")
        assert writer.call("WAIT", 1, 5000) == 1
        target.pause()
        try:
            payload = "x" * (4 * 1024 * 1024)
            writer.call("SET", "pressure", payload)
            writer.call("SET", "pressure", payload)
            H.wait_until("native source backlog pressure", 10,
                         lambda: 'lavik_replication_backlog_backpressured{worker="0"} 1' in source.metrics())
            def blocked_write():
                client = Client(source)
                try:
                    return client.call("SET", "blocked-writer", "value")
                except (OSError, H.Failure):
                    return "closed"
                finally:
                    client.close()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(blocked_write)
                time.sleep(.2)
                assert not pending.done()
                assert writer.call("PING") == "PONG"
                started = time.monotonic()
                source.terminate()
                assert time.monotonic() - started < 10
                pending.result(timeout=3)
        finally:
            target.resume()


def main():
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    H.set_tag("native-replication")
    with tempfile.TemporaryDirectory(prefix="lavik-meta-native-",
                                     dir=os.environ.get("LAVIK_TEST_DATA_DIR")) as directory:
        root = Path(directory)
        replay_and_reconnect(root)
        full_tail(root)
        backpressured_shutdown(root)
        if C.has_fault(C.DATA, b"LAVIK_REPLICATION_HOLD_FIRST_HANDOFF_UNTIL_NEXT_ACK"):
            handoff_order(root)
            cancelled_handoff(root)
            rejected_full(root, "checksum", {
                "LAVIK_REPLICATION_CORRUPT_FULLSYNC_RECORD_FRAME_ONCE": "1"}, {},
                "replication frame CRC32C mismatch")
            rejected_full(root, "early-online", {
                "LAVIK_REPLICATION_EARLY_ONLINE": "1"}, {},
                "injected ONLINE before local flow readiness")
    H.log("PASS")


if __name__ == "__main__":
    main()
