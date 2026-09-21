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
import struct
import sys
import tempfile
import time

import gate_cluster_create as C
import harness as H
from gate_data_control import DataProcess

CLIENT_MODE = "cluster"


class Client:
    def __init__(self, node, readonly=False):
        self.context = C.redis_connection(node)
        self.socket = self.context.__enter__()
        self.socket.settimeout(30)
        self.reader = self.socket.makefile("rb")
        if readonly:
            assert self.call("READONLY") == "OK"

    def call(self, *args):
        values = [arg if isinstance(arg, bytes) else str(arg).encode()
                  for arg in args]
        self.socket.sendall(f"*{len(values)}\r\n".encode() + b"".join(
            f"${len(value)}\r\n".encode() + value + b"\r\n"
            for value in values))
        return C.read_resp(self.reader)

    def close(self):
        self.reader.close()
        self.context.__exit__(None, None, None)


@contextmanager
def pair(root, name, source_faults=None, target_faults=None, seed=None,
         source_workers=2, target_workers=3, raft_args=None,
         require_seed_before_full=False, client_mode=None):
    client_mode = client_mode or CLIENT_MODE
    directory = root / name
    directory.mkdir()
    meta = H.Node(C.META, str(directory), 1,
                  args=C.creation_raft_args() if raft_args is None else raft_args)
    proxy = C.DirectiveBarrier(meta.data_control_port, recipients=(C.REPLICA_1,))
    meta.advertised_data_control_endpoint = proxy.endpoint
    source = DataProcess(C.DATA, str(directory / "source"), C.PRIMARY_1,
                         proxy.endpoint, workers=source_workers,
                         environment={**os.environ, **(source_faults or {})})
    target = DataProcess(C.DATA, str(directory / "target"), C.REPLICA_1,
                         proxy.endpoint, workers=target_workers,
                         environment={**os.environ, **(target_faults or {})})
    lines = ['schema_version = 1', f'client_mode = "{client_mode}"', 'slot_strategy = "contiguous-even"']
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
        if require_seed_before_full:
            # The one-frame barrier intentionally permits later sessions to
            # reconnect. Pause this registered target while a larger seed is
            # built so no reconnect can turn the FULL test into live replay.
            # Retain its boot identity throughout the initialization workflow.
            target.pause()
        writer = Client(source)
        clients.append(writer)
        H.wait_until("source authority before FULL", 20,
                     lambda: writer.call("SET", "{native}seed", "baseline") == "OK")
        if seed:
            seed(writer)
        if require_seed_before_full:
            assert "replication target session" not in Path(target.log_path).read_text(), \
                "target started FULL before the seed finished"
        release.set()
        if require_seed_before_full:
            target.resume()
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
            # The source has two data shards and the target has three. Their
            # extra Meta workers must never become replication flows.
            info = dict(line.split(":", 1) for line in
                        reader.call("INFO", "replication").splitlines()
                        if ":" in line)
            assert info.get("lavik_source_workers") == "2", info
            assert info.get("lavik_connected_flows") == "2", info
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


def dense_collection_full_sync(root):
    # Many short identities exercise duplicate validation against a growing
    # staged object. A few large values do not expose the quadratic scan that
    # previously monopolized the replica worker during FULL sync.
    count = 32768

    def dump(kind):
        # Plain RDB Hash/Set/ZSet with short binary strings, version 11 and a
        # Redis CRC64 footer. RESTORE seeds one bounded stream instead of repeatedly
        # rewriting an ever-growing object or hitting compact HREPLACE's argc
        # limit. Build payloads before holding the target's control directive.
        data = bytearray([kind, 0x80]) + count.to_bytes(4, "big")
        for i in range(count):
            member = f"member\0{i:08d}".ljust(48, "x").encode()
            data += bytes([len(member)]) + member
            if kind == 4:
                value = f"value\0{i:08d}".ljust(32, "v").encode()
                data += bytes([len(value)]) + value
            elif kind == 5:
                data += struct.pack("<d", float(i))
        data += b"\x0b\x00"
        polynomial = int(f"{0xad93d23594c935a9:064b}"[::-1], 2)
        table = []
        for byte in range(256):
            crc = byte
            for _ in range(8):
                crc = (crc >> 1) ^ (polynomial if crc & 1 else 0)
            table.append(crc)
        crc = 0
        for byte in data:
            crc = table[(crc ^ byte) & 255] ^ (crc >> 8)
        return bytes(data) + crc.to_bytes(8, "little")

    hash_payload, set_payload, zset_payload = dump(4), dump(2), dump(5)

    def seed(writer):
        H.log(f"seeding dense Hash with {count} fields")
        assert writer.call("RESTORE", "{dense}hash", 0, hash_payload) == "OK"
        H.log(f"seeding dense Set with {count} members")
        assert writer.call("RESTORE", "{dense}set", 0, set_payload) == "OK"
        H.log(f"seeding dense ZSet with {count} members")
        assert writer.call("RESTORE", "{dense}zset", 0, zset_payload) == "OK"

    # This gate verifies ingestion and payload integrity under the ordinary
    # five-second authority lease. Subsecond lease tests expose a separate
    # data-observation stall and must not prevent this ingestion test starting.
    with pair(root, "dense-collections", seed=seed, require_seed_before_full=True,
              raft_args=H.raft_args(snapshot_distance=100000,
                                    election_ms_low=5000,
                                    election_ms_high=10000)) as (meta, _, target, writer):
        started = time.monotonic()
        ready(meta)
        reader = Client(target, readonly=True)
        try:
            assert reader.call("HLEN", "{dense}hash") == count
            assert reader.call("SCARD", "{dense}set") == count
            assert reader.call("ZCARD", "{dense}zset") == count
            # Distinct values and scores expose association errors that counts
            # alone, or a fixture with one repeated Hash value, cannot detect.
            assert sorted(reader.call("SMEMBERS", "{dense}set")) == sorted(
                writer.call("SMEMBERS", "{dense}set"))
            expected = writer.call("HGETALL", "{dense}hash")
            actual = reader.call("HGETALL", "{dense}hash")
            assert dict(zip(actual[::2], actual[1::2])) == dict(
                zip(expected[::2], expected[1::2]))
            assert reader.call("ZRANGE", "{dense}zset", 0, -1, "WITHSCORES") == \
                writer.call("ZRANGE", "{dense}zset", 0, -1, "WITHSCORES")
        finally:
            reader.close()
        H.log(f"dense Hash/Set/ZSet FULL verified {count} members each in "
              f"{time.monotonic() - started:.3f}s")


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


def full_tail_publish_before_reset(root):
    # One source worker resets only the first batch before handing off slot 0.
    # Publish into the last slot while that flow is paused: both the bare
    # command and the EXEC envelope must replay before their transport slot
    # has any replica storage context.
    tag = next(f"full-publish-{i}" for i in range(100000)
               if C.redis_slot(f"full-publish-{i}") == 16383)
    channel = "{" + tag + "}channel"
    with pair(root, "full-tail-publish-before-reset", source_faults={
            "LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "3000"},
            require_seed_before_full=True,
            source_workers=1, target_workers=2) as (meta, source, target, writer):
        H.wait_until("partition zero handed off before publications", 30, lambda:
                     "paused full sync after acknowledged handoff partition 0 "
                     in Path(source.log_path).read_text())
        subscriber = Client(target)
        try:
            assert subscriber.call("SUBSCRIBE", channel) == ["subscribe", channel, 1]
            assert writer.call("PUBLISH", channel, "bare") == 0
            assert writer.call("MULTI") == "OK"
            assert writer.call("PUBLISH", channel, "first") == "QUEUED"
            assert writer.call("PUBLISH", channel, "second") == "QUEUED"
            assert writer.call("EXEC") == [0, 0]
            for message in ("bare", "first", "second"):
                assert C.read_resp(subscriber.reader) == ["message", channel, message]
            ready(meta)
            assert writer.call("PUBLISH", channel, "online") == 0
            assert writer.call("WAIT", 1, 5000) == 1
            # The next message also proves that the FULL cut did not replay
            # the preceding publications a second time through ONLINE.
            assert C.read_resp(subscriber.reader) == ["message", channel, "online"]
        finally:
            subscriber.close()
        assert Path(source.log_path).read_text().count("selected=FULL") == 1
        assert "precedes partition reset" not in Path(target.log_path).read_text()


def full_tail_expiration_effects(root):
    # The source pauses after handing off partition zero. Mutations made in
    # that window must use FULL command replay, not the initial snapshot or
    # the ONLINE backlog. Canonical TTL effects wrap these single-key writes
    # in __LAVIK_EXEC_V1; that wrapper still needs the partition apply context.
    tag = next(f"full-tail-{i}" for i in range(100000)
               if C.redis_slot(f"full-tail-{i}") == 0)
    prefix = "{" + tag + "}"
    counter, collection = prefix + "counter", prefix + "hash"

    def seed(writer):
        assert writer.call("SET", counter, 0) == "OK"
        assert writer.call("HSET", collection, "before", "snapshot") == 1
        assert writer.call("PEXPIRE", collection, 120000) == 1

    with pair(root, "full-tail-expiration-effects", source_faults={
            "LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "3000"},
            seed=seed, require_seed_before_full=True,
            source_workers=1, target_workers=2) as (meta, source, target, writer):
        H.wait_until("partition zero handed off before mutations", 30, lambda:
                     "paused full sync after acknowledged handoff partition 0 "
                     in Path(source.log_path).read_text())
        assert writer.call("INCR", counter) == 1
        assert writer.call("HSET", collection, "after", "tail") == 1
        deadline = writer.call("PEXPIRETIME", collection)
        ready(meta)
        reader = Client(target, readonly=True)
        try:
            assert reader.call("GET", counter) == "1"
            assert reader.call("PTTL", counter) == -1
            assert reader.call("HGET", collection, "before") == "snapshot"
            assert reader.call("HGET", collection, "after") == "tail"
            assert reader.call("PEXPIRETIME", collection) == deadline
            assert writer.call("INCR", counter) == 2
            assert writer.call("WAIT", 1, 5000) == 1
            assert reader.call("GET", counter) == "2"
        finally:
            reader.close()
        # A failed apply followed by a replacement snapshot could produce the
        # same values. Require this first FULL to complete without that retry.
        assert Path(source.log_path).read_text().count("selected=FULL") == 1
        assert "outside its apply context" not in Path(target.log_path).read_text()



def full_tail_type_reuse(root):
    # Keep these commands behind one acknowledged handoff so they traverse
    # FULL's command/after-image FIFO before the ONLINE boundary. Multi-key
    # writes use committed participant records; single-key writes carry their
    # original command and its TTL companion.
    tag = next(f"full-reuse-{i}" for i in range(100000)
               if C.redis_slot(f"full-reuse-{i}") == 0)
    prefix = "{" + tag + "}"
    key, counter, other, copied = [prefix + name for name in
                                  ("typed", "counter", "other", "copied")]

    def seed(writer):
        assert writer.call("SET", key, "seed") == "OK"
        assert writer.call("MSET", counter, 0, other, 0) == "OK"

    with pair(root, "full-tail-type-reuse", source_faults={
            "LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "5000"},
            seed=seed, require_seed_before_full=True,
            source_workers=1, target_workers=2,
            raft_args=H.raft_args(snapshot_distance=100000,
                                  election_ms_low=5000,
                                  election_ms_high=10000)) as (meta, source, target, writer):
        H.wait_until("type reuse partition handed off", 30, lambda:
                     "paused full sync after acknowledged handoff partition 0 "
                     in Path(source.log_path).read_text())
        for i in range(8):
            # An absolute past deadline deletes immediately, with no sleep or
            # scheduler race required to advance from one value type to another.
            assert writer.call("PEXPIREAT", key, 1) == 1
            assert writer.call("HSET", key, "f", str(i)) == 1
            assert writer.call("PEXPIREAT", key, 1) == 1
            assert writer.call("RPUSH", key, str(i)) == 1
            assert writer.call("MULTI") == "OK"
            assert writer.call("INCR", counter) == "QUEUED"
            assert writer.call("MSET", other, str(i), copied, "temporary") == "QUEUED"
            assert writer.call("RPUSH", key, "tx") == "QUEUED"
            assert writer.call("EXEC") == [i + 1, "OK", 2]
        assert writer.call("COPY", counter, copied, "REPLACE") == 1
        assert writer.call("PEXPIRE", key, 120000) == 1
        deadline = writer.call("PEXPIRETIME", key)
        ready(meta)
        reader = Client(target, readonly=True)
        try:
            assert reader.call("LRANGE", key, 0, -1) == ["7", "tx"]
            assert reader.call("GET", counter) == "8"
            assert reader.call("GET", other) == "7"
            assert reader.call("GET", copied) == "8"
            assert reader.call("PEXPIRETIME", key) == deadline
            assert writer.call("RPUSH", key, "online") == 3
            assert writer.call("WAIT", 1, 5000) == 1
            assert reader.call("LRANGE", key, 0, -1) == ["7", "tx", "online"]
        finally:
            reader.close()
        assert Path(source.log_path).read_text().count("selected=FULL") == 1
        assert "WRONGTYPE" not in Path(target.log_path).read_text()


def post_cut_reset_reconnect(root):
    with pair(root, "post-cut-reset", source_faults={
            "LAVIK_REPLICATION_POST_CUT_RESET_ONCE": "1"}) as (meta, source, target, writer):
        H.wait_until("post-cut reset injection", 30, lambda:
                     "injected post-cut reset" in Path(source.log_path).read_text())
        # The reset interrupts FULL before the source confirms every flow.
        # Recovery may need another FULL, especially with an empty flow at its
        # initial cursor. Assert the original contract: recovery preserves data
        # and resumes replication, without requiring a particular handshake.
        ready(meta)
        reader = Client(target, readonly=True)
        try:
            # Transport ONLINE can precede the new population's serving
            # projection; wait for the actual read path as well.
            H.wait_until("post-cut reconnect serves preserved data", 30, lambda:
                         "lavik_replication_state:online" in reader.call("INFO", "replication")
                         and reader.call("GET", "{native}seed") == "baseline")
            assert writer.call("INCR", "post-cut-counter") == 1
            assert writer.call("WAIT", 1, 5000) == 1
            assert reader.call("GET", "post-cut-counter") == "1"
            assert reader.call("GET", "{native}seed") == "baseline"
        finally:
            reader.close()


def committed_cursor_reconnect(root, name, target_faults):
    with pair(root, name, target_faults=target_faults) as (meta, source, target, writer):
        ready(meta)
        reader = Client(target, readonly=True)
        try:
            for i in range(64):
                writer.call("SET", f"cursor-warmup-{i}", "ready")
            assert writer.call("WAIT", 1, 5000) == 1
            old_full = Path(source.log_path).read_text().count("selected=FULL")
            writer.call("INCR", "cancelled-apply-counter")
            if target_faults and "LAVIK_REPLICATION_CANCEL_PEER_FLOW_AFTER_COMMAND_APPLY_ONCE" in target_faults:
                H.wait_until("cancel after committed apply", 30, lambda:
                             "injected peer-flow session cancellation after command apply" in Path(target.log_path).read_text())
            H.wait_until("committed cursor reconnect", 30, lambda:
                         "selected=CONTINUE" in Path(source.log_path).read_text())
            H.wait_until("committed increment applied once", 30, lambda:
                         reader.call("GET", "cancelled-apply-counter") == "1")
            assert reader.call("GET", "{native}seed") == "baseline"
            assert Path(source.log_path).read_text().count("selected=FULL") == old_full
        finally:
            reader.close()


def divergent_tail(root, flow):
    with pair(root, f"divergent-{flow}", source_faults={
            "LAVIK_REPLICATION_DIVERGENT_TAIL_ONCE": "1",
            "LAVIK_REPLICATION_DIVERGENT_TAIL_FLOW": str(flow)},
            target_workers=2) as (meta, source, target, writer):
        ready(meta)
        key = "divergent-counter"
        while writer.call("CLUSTER", "KEYSLOT", key) % 2 != flow:
            key += "x"
        assert writer.call("INCR", key) == 1
        H.wait_until("divergent tail reaches flow", 30, lambda:
                     "injected divergent replication tail" in Path(source.log_path).read_text())
        H.wait_until("all continuation cursors invalidated", 30, lambda:
                     "invalidated native replication continuation" in Path(target.log_path).read_text())
        # A gap invalidates the entire population. The follower may not use
        # the other flow's cursor to become readable without fresh authority.
        reader = Client(target, readonly=True)
        try:
            rejects(reader, ("GET", "{native}seed"), "LOADING")
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


def small_receive_window(root):
    def seed(writer):
        # Exercise FULL before steady replay grows the loopback window/MSS
        # and the target's fault reduces its receive window.
        writer.call("SET", "{window}seed", "s" * (2 * 1024 * 1024))

    with pair(root, "small-receive-window", seed=seed,
              source_workers=1, target_workers=1, target_faults={
                  "LAVIK_TEST_NATIVE_SMALL_RECEIVE_WINDOW": "24576"}) as (meta, source, target, writer):
        ready(meta)
        writer.call("SET", "{window}trigger", "online")
        # A real publisher burst must drain through native replay and ACKs.
        # The reduced window used to put each large segment behind TCP's
        # ~200ms probe timer, despite both processes remaining ONLINE.
        for batch in range(64):
            writer.socket.sendall(b"".join(C.encode_resp(
                ["SET", f"{{window}}key-{index}", "v" * 1024])
                for index in range(512)))
            for _ in range(512):
                assert C.read_resp(writer.reader) == "OK"
        assert "test native receive window reduced" in Path(target.log_path).read_text()
        assert writer.call("WAIT", 1, 12000) == 1
        assert C.readonly_get(target, "{window}key-511") == "v" * 1024


def target_queue_shutdown(root):
    # Hold the FIFO consumer until the receiver has filled its bounded queue.
    # Socket shutdown cannot wake that capacity wait: terminal stage/ACK
    # publication must explicitly notify ingress before the flow can join.
    with pair(root, "target-queue-shutdown", source_workers=1, target_workers=1,
              target_faults={
                  "LAVIK_REPLICATION_PAUSE_BEFORE_COMMAND_APPLY_MS": "8000",
                  "LAVIK_REPLICATION_REPORT_ONLINE_BACKPRESSURE": "1",
              }) as (meta, _source, target, writer):
        ready(meta)
        for i in range(600):
            assert writer.call("SET", "{queue-shutdown}key", str(i)) == "OK"
        H.wait_until("replica ingress is waiting for queue capacity", 10, lambda:
                     "replica online ingress waiting for command capacity" in
                     Path(target.log_path).read_text())
        target.terminate()
        assert "replication targets quiesced before storage flush" in Path(target.log_path).read_text()

def main():
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    H.set_tag("native-replication")
    with tempfile.TemporaryDirectory(prefix="lavik-meta-native-",
                                     dir=os.environ.get("LAVIK_TEST_DATA_DIR")) as directory:
        root = Path(directory)
        replay_and_reconnect(root)
        dense_collection_full_sync(root)
        full_tail(root)
        backpressured_shutdown(root)
        if C.has_fault(C.DATA, b"LAVIK_REPLICATION_HOLD_FIRST_HANDOFF_UNTIL_NEXT_ACK"):
            full_tail_publish_before_reset(root)
            full_tail_expiration_effects(root)
            small_receive_window(root)
            full_tail_type_reuse(root)
            target_queue_shutdown(root)
            handoff_order(root)
            cancelled_handoff(root)
            committed_cursor_reconnect(root, "cancel-apply", target_faults={
                "LAVIK_REPLICATION_CANCEL_PEER_FLOW_AFTER_COMMAND_APPLY_ONCE": "cancelled-apply-counter"})
            post_cut_reset_reconnect(root)
            # Two controlled owner changes create a replacement history while
            # preserving the laggard's old population and per-flow cursors.
            import gate_failover as F
            F.run_full_fallback(C.META, C.DATA, C.CTL, C.REDIS_CLI,
                                str(root), False, cut_disconnect=True)
            divergent_tail(root, 0)
            divergent_tail(root, 1)
            rejected_full(root, "checksum", {
                "LAVIK_REPLICATION_CORRUPT_FULLSYNC_RECORD_FRAME_ONCE": "1"}, {},
                "replication frame CRC32C mismatch")
            rejected_full(root, "early-online", {
                "LAVIK_REPLICATION_EARLY_ONLINE": "1"}, {},
                "injected ONLINE before local flow readiness")
    H.log("PASS")


if __name__ == "__main__":
    main()
