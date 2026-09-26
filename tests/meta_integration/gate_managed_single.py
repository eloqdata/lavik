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

"""Managed Single request authority and Redis replica read semantics."""

import os
import concurrent.futures
from pathlib import Path
import socket
import sys
import tempfile
import time

import gate_native_replication as N
import single_database_data as D
import gate_failover as F
from gate_native_replication import C, H, Client, pair, ready, rejects

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from redis_follower_smoke import process  # noqa: E402


def database_isolation(writer, reader):
    # The same key in every DB catches replay into the connection's default DB.
    for db in range(16):
        assert writer.call("SELECT", db) == "OK"
        assert writer.call("SET", "db-isolation", f"database-{db}") == "OK"
    for db in range(16):
        assert reader.call("SELECT", db) == "OK"
        H.wait_until(
            f"database {db} replicated",
            20,
            lambda db=db: reader.call("GET", "db-isolation") == f"database-{db}",
        )
    # SELECT is connection-local and invalid selection leaves it unchanged.
    rejects(writer, ("SELECT", 16), "DB index is out of range")
    assert writer.call("GET", "db-isolation") == "database-15"
    assert reader.call("SELECT", 0) == "OK"
    assert reader.call("GET", "db-isolation") == "database-0"
    assert writer.call("GET", "db-isolation") == "database-15"
    assert writer.call("SELECT", 0) == "OK"


def inspect_database(client):
    assert client.call("SELECT", 15) == "OK"
    assert client.call("DBSIZE") == 1
    assert client.call("KEYS", "db-*") == ["db-isolation"]
    assert client.call("RANDOMKEY") == "db-isolation"
    cursor, found = "0", set()
    while True:
        cursor, keys = client.call(
            "SCAN", cursor, "MATCH", "db-*", "TYPE", "string", "COUNT", 1
        )
        found.update(keys)
        if cursor == "0":
            break
    assert found == {"db-isolation"}
    assert client.call("SELECT", 0) == "OK"


def basic_and_stale(root):
    with pair(
        root, "single-read", client_mode="single", raft_args=C.creation_raft_args()
    ) as (
        meta,
        source,
        target,
        writer,
    ):
        ready(meta)
        reader = Client(target)
        try:
            database_isolation(writer, reader)
            inspect_database(writer)
            inspect_database(reader)
            assert reader.call("CONFIG", "GET", "replica-serve-stale-data") == [
                "replica-serve-stale-data",
                "yes",
            ]
            for key in ("first-slot", "another-slot", "{other}slot"):
                assert writer.call("SET", key, "value") == "OK"
            assert writer.call("INCR", "count") == 1
            assert writer.call("HSET", "hash", "field", "value") == 1
            assert writer.call("SADD", "set", "member") == 1
            assert writer.call("LPUSH", "list", "item") == 1
            assert writer.call("ZADD", "sorted", 1, "member") == 1
            assert writer.call("SET", "ttl", "expires", "PX", 1500) == "OK"
            H.wait_until(
                "Single all source flows replay",
                20,
                lambda: reader.call("GET", "count") == "1"
                and reader.call("HGET", "hash", "field") == "value"
                and reader.call("SISMEMBER", "set", "member") == 1
                and reader.call("LINDEX", "list", 0) == "item"
                and reader.call("ZSCORE", "sorted", "member") == "1",
            )
            assert reader.call("GET", "another-slot") == "value"
            assert reader.call("HGET", "hash", "field") == "value"
            assert reader.call("SISMEMBER", "set", "member") == 1
            assert reader.call("LINDEX", "list", 0) == "item"
            assert reader.call("ZSCORE", "sorted", "member") == "1"
            for command in (
                ("SET", "no-write", "x"),
                ("INCR", "count"),
                ("EXPIRE", "ttl", 50),
                ("PUBLISH", "channel", "x"),
            ):
                rejects(reader, command, "READONLY")
            # Cross-slot multi-key commands serve through the sole Group and
            # replicate; Cluster CROSSSLOT does not apply to Single. The keys
            # provably span both source workers, so the fan-out and per-shard
            # re-validation are actually exercised.
            assert (
                len({C.redis_slot(k) % 2 for k in ("mk-a", "mk-b", "{other}mk")}) == 2
            )
            assert (
                writer.call("MSET", "mk-a", "v1", "mk-b", "v2", "{other}mk", "v3")
                == "OK"
            )
            assert writer.call("MGET", "mk-a", "mk-b", "{other}mk") == [
                "v1",
                "v2",
                "v3",
            ]
            H.wait_until(
                "Single cross-slot MSET replicated",
                20,
                lambda: reader.call("MGET", "mk-a", "mk-b", "{other}mk")
                == ["v1", "v2", "v3"],
            )
            assert writer.call("DEL", "mk-a", "mk-b") == 2
            assert writer.call("RENAME", "{other}mk", "mk-renamed") == "OK"
            assert writer.call("MGET", "mk-a", "mk-renamed") == [None, "v3"]
            assert writer.call("BLPOP", "mk-empty", 1) == []
            for client in (writer, reader):
                for command in (("WAIT", 1, 0),):
                    rejects(client, command, "not yet supported")
                rejects(client, ("REPLICAOF", "NO", "ONE"), "not allowed")
            fulls = Path(source.log_path).read_text().count("selected=FULL")
            assert writer.call("CLIENT", "KILL", "TYPE", "replica") > 0
            assert writer.call("INCR", "count") == 2
            H.wait_until(
                "Single incremental reconnect",
                30,
                lambda: reader.call("GET", "count") == "2",
            )
            assert Path(source.log_path).read_text().count("selected=FULL") == fulls
            # Control loss alone must not close a complete replica, including
            # with stale reads disabled while its replication link stays online.
            meta.pause()
            assert (
                reader.call("CONFIG", "SET", "replica-serve-stale-data", "no") == "OK"
            )
            time.sleep(2)
            assert reader.call("GET", "count") == "2"
            # Owner loss retires the old TCP session; diagnostics/admission
            # errors remain available on a new connection.
            writer.close()
            writer = Client(source)
            rejects(writer, ("GET", "count"), "MASTERDOWN")
            rejects(writer, ("SET", "count", "bad"), "MASTERDOWN")
            for command in (("DBSIZE",), ("SCAN", 0), ("KEYS", "*"), ("RANDOMKEY",)):
                rejects(writer, command, "MASTERDOWN")
            assert (
                reader.call("CONFIG", "SET", "replica-serve-stale-data", "yes") == "OK"
            )
            source.terminate()
            H.wait_until(
                "replication link offline",
                15,
                lambda: "master_link_status:down" in reader.call("INFO", "replication"),
            )
            assert reader.call("GET", "count") == "2"
            assert reader.call("GET", "ttl") is None
            inspect_database(reader)
            rejects(reader, ("INCR", "count"), "READONLY")
            assert (
                reader.call("CONFIG", "SET", "replica-serve-stale-data", "no") == "OK"
            )
            rejects(reader, ("GET", "count"), "MASTERDOWN")
            for command in (("DBSIZE",), ("SCAN", 0), ("KEYS", "*"), ("RANDOMKEY",)):
                rejects(reader, command, "MASTERDOWN")
            assert reader.call("PING") == "PONG"
            assert (
                reader.call("CONFIG", "SET", "replica-serve-stale-data", "yes") == "OK"
            )
            assert reader.call("GET", "count") == "2"
            rejects(
                reader, ("CONFIG", "SET", "replica-serve-stale-data", "maybe"), "yes"
            )
        finally:
            meta.resume()
            reader.close()
            writer.close()

    # A clean complete former replica may start without Meta through the
    # existing standalone recovery path. No management provenance is written.
    with process(C.DATA, root / "single-read" / "target", "standalone") as (
        client,
        _,
        _,
    ):
        assert client.call("GET", "count") == "2"
        assert client.call("SELECT", 15) == "OK"
        assert client.call("SET", "standalone-db15", "value") == "OK"


def full_and_copy(root):
    expiry = []

    def dirty_target(target):
        from gate_data_control import DATA_FILE_BYTES, allocate_data_file

        directory = Path(target.workdir)
        directory.mkdir()
        allocate_data_file(target.data_path, DATA_FILE_BYTES * target.workers)
        with process(C.DATA, directory, "old-population", workers=target.workers) as (
            client,
            _,
            _,
        ):
            for db in range(16):
                client.call("SELECT", db)
                client.call("SET", "target-only", "must-disappear")
                # Distinct old DB epochs must not leak into the replacement.
                if db in (1, 15):
                    client.call("FLUSHDB")
                    client.call("SET", "target-only", "must-disappear")

    with pair(
        root,
        "multidb-full",
        client_mode="single",
        seed=lambda client: expiry.append(D.seed(client)),
        prepare_target=dirty_target,
        require_seed_before_full=True,
    ) as (meta, source, target, writer):
        ready(meta)
        deadline = expiry[0]
        D.require(writer, deadline)
        H.wait_until(
            "all databases and grouped metadata survive FULL",
            30,
            lambda: D.node_matches(target, deadline),
        )
        reader = Client(target)
        try:
            for db in range(16):
                reader.call("SELECT", db)
                assert reader.call("GET", "target-only") is None
            for source_db, destination_db in ((0, 15), (15, 0), (1, 15)):
                for kind in D.TYPES:
                    key = D.PREFIX + kind
                    # Every type exercises both the one-worker shortcut and
                    # the shared-source/destination cross-flow rendezvous.
                    for cross_worker in (False, True):
                        destination = next(
                            f"copy:{source_db}:{kind}:{i}"
                            for i in range(100)
                            if (
                                C.redis_slot(f"copy:{source_db}:{kind}:{i}") % 2
                                != C.redis_slot(key) % 2
                            )
                            == cross_worker
                        )
                        writer.call("SELECT", destination_db)
                        writer.call("SET", destination, "old-destination")
                        writer.call("SELECT", source_db)
                        assert (
                            writer.call("COPY", key, destination, "DB", destination_db)
                            == 0
                        )
                        assert (
                            writer.call(
                                "COPY",
                                key,
                                destination,
                                "DB",
                                destination_db,
                                "REPLACE",
                            )
                            == 1
                        )
                        D.require_value(writer, kind, key, source_db, deadline)
                        writer.call("SELECT", destination_db)
                        D.require_value(writer, kind, destination, source_db, deadline)
                        reader.call("SELECT", destination_db)

                        def copied():
                            try:
                                D.require_value(
                                    reader, kind, destination, source_db, deadline
                                )
                                return True
                            except AssertionError:
                                return False

                        H.wait_until("cross-DB COPY replay", 20, copied)
            writer.call("SELECT", 0)
            assert writer.call("COPY", "absent", "absent-copy", "DB", 15) == 0
            rejects(writer, ("COPY", D.PREFIX + "string", D.PREFIX + "string"), "same")
            rejects(
                writer, ("COPY", D.PREFIX + "string", "invalid", "DB", 16), "DB index"
            )
            assert (
                writer.call("COPY", D.PREFIX + "string", D.PREFIX + "string", "DB", 2)
                == 1
            )
            reader.call("SELECT", 2)
            H.wait_until(
                "same name in a different DB",
                20,
                lambda: reader.call("GET", D.PREFIX + "string") == "value-0",
            )
            # A reconnect must use retained history, including writes in DB15.
            fulls = Path(source.log_path).read_text().count("selected=FULL")
            assert writer.call("CLIENT", "KILL", "TYPE", "replica") > 0
            writer.call("SELECT", 15)
            writer.call("SET", "after-reconnect", "incremental")
            reader.call("SELECT", 15)
            H.wait_until(
                "DB15 incremental reconnect",
                30,
                lambda: reader.call("GET", "after-reconnect") == "incremental",
            )
            assert Path(source.log_path).read_text().count("selected=FULL") == fulls
        finally:
            reader.close()

    # Recovered storage must not resurrect pre-FULL data from an older epoch.
    with process(C.DATA, root / "multidb-full" / "target", "recovered", workers=3) as (
        client,
        _,
        _,
    ):
        D.require(client, deadline)
        for db in range(16):
            client.call("SELECT", db)
            assert client.call("GET", "target-only") is None

    # An entirely empty source DB must replace a populated destination DB too.
    with pair(
        root,
        "multidb-empty-full",
        client_mode="single",
        prepare_target=dirty_target,
    ) as (meta, _, target, _):
        ready(meta)
        reader = Client(target)
        try:
            for db in (1, 15):
                assert reader.call("SELECT", db) == "OK"
                assert reader.call("DBSIZE") == 0
                assert reader.call("KEYS", "*") == []
                cursor = "0"
                while True:
                    cursor, keys = reader.call("SCAN", cursor)
                    assert keys == []
                    if cursor == "0":
                        break
                assert reader.call("RANDOMKEY") is None
        finally:
            reader.close()


def copy_fenced_after_wait(root):
    with pair(
        root,
        "copy-fenced",
        client_mode="single",
        source_faults={"LAVIK_REPLICATION_ORDER_HOLD_MS": "6000"},
    ) as (meta, source, target, writer):
        ready(meta)
        assert C.redis_slot("xa") % 2 != C.redis_slot("{b}k") % 2
        writer.call("SELECT", 1)
        writer.call("SET", "xa", "source")
        writer.call("SELECT", 15)
        writer.call("SET", "{b}k", "old-target")
        writer.call("SELECT", 1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(
                writer.call, "COPY", "xa", "{b}k", "DB", 15, "REPLACE"
            )
            H.wait_until(
                "COPY holds the cross-flow order gate",
                10,
                lambda: "holding the replication order gate"
                in Path(source.log_path).read_text(),
            )
            meta.pause()
            try:
                try:
                    response = pending.result(timeout=15)
                except H.Failure as error:
                    assert "closed its Redis connection" in str(error), error
                else:
                    raise AssertionError(f"COPY wrote after lease expiry: {response}")
            finally:
                meta.resume()
        writer.close()
        writer = Client(source)
        writer.call("SELECT", 1)
        H.wait_until(
            "Owner regains authority", 20, lambda: writer.call("GET", "xa") == "source"
        )
        writer.call("SELECT", 15)
        assert writer.call("GET", "{b}k") == "old-target"
        reader = Client(target)
        try:
            reader.call("SELECT", 15)
            H.wait_until(
                "rejected COPY left replica unchanged",
                20,
                lambda: reader.call("GET", "{b}k") == "old-target",
            )
        finally:
            reader.close()
            writer.close()


def copy_hop_fencing(root, stage, fail_ingestion=False):
    destination = "copy-settle{b}"
    hold = root / f"copy-{stage}-{fail_ingestion}.hold"
    hold.touch()
    faults = {
        "LAVIK_COPY_PAUSE_KEY": destination,
        "LAVIK_COPY_PAUSE_STAGE": stage,
        "LAVIK_COPY_HOLD_FILE": str(hold),
    }
    if fail_ingestion:
        faults.update(
            {"LAVIK_FAIL_GROUP_AUX_KEY": destination, "LAVIK_FAIL_GROUP_AUX_NTH": "2"}
        )
    with pair(
        root,
        f"copy-{stage}-{fail_ingestion}",
        client_mode="single",
        source_faults=faults,
    ) as (meta, source, target, writer):
        ready(meta)
        assert C.redis_slot("xa") % 2 != C.redis_slot(destination) % 2
        writer.call("SELECT", 1)
        fields = [part for i in range(256) for part in (str(i), "v" * 8192)]
        assert writer.call("HSET", "xa", *fields) == 256
        deadline = int(time.time() * 1000) + 120000
        writer.call("PEXPIREAT", "xa", deadline)
        writer.call("SELECT", 15)
        writer.call("SET", destination, "old-target", "PXAT", deadline)
        writer.call("SELECT", 1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(
                writer.call, "COPY", "xa", destination, "DB", 15, "REPLACE"
            )
            H.wait_until(
                "COPY reached the requested hop boundary",
                10,
                lambda: f"COPY test checkpoint: {stage} hop finished"
                in Path(source.log_path).read_text(),
            )
            meta.pause()
            try:
                # A separate key avoids blocking behind COPY's held key locks.
                H.wait_until(
                    "Owner lease expired before COPY resumes",
                    5,
                    lambda: "MASTERDOWN"
                    in F.redis_error(source, ["GET", "lease-probe"]),
                )
                # Retirement may finish the client future while COPY still
                # owns its internal guards at the deterministic hold point.
                hold.unlink()
                try:
                    result = pending.result(timeout=15)
                except H.Failure as error:
                    assert "closed its Redis connection" in str(error), error
                else:
                    raise AssertionError(
                        f"retired COPY returned a fabricated result: {result}"
                    )
                assert "COPY test hold expired" not in Path(source.log_path).read_text()
            finally:
                hold.unlink(missing_ok=True)
                meta.resume()
        # The missing reply does not identify the mutation outcome: inspect
        # the settled storage state after reauthorization, without retrying COPY.
        inspector = Client(source)
        reader = Client(target)
        try:
            H.wait_until(
                "Owner regains authority after the hop fence",
                20,
                lambda: inspector.call("GET", "lease-probe") is None,
            )
            inspector.call("SELECT", 1)
            assert inspector.call("HLEN", "xa") == 256
            assert inspector.call("HGET", "xa", "255") == "v" * 8192
            for client in (inspector, reader):
                client.call("SELECT", 15)
                if stage == "write" and not fail_ingestion:
                    H.wait_until(
                        "committed COPY survives settlement and replay",
                        20,
                        lambda: client.call("TYPE", destination) == "hash",
                    )
                    assert client.call("HLEN", destination) == 256
                    assert client.call("HGET", destination, "255") == "v" * 8192
                else:
                    H.wait_until(
                        "rejected COPY preserves the predecessor",
                        20,
                        lambda: client.call("GET", destination) == "old-target",
                    )
                before = int(time.time() * 1000)
                ttl = client.call("PTTL", destination)
                after = int(time.time() * 1000)
                assert before + ttl <= deadline + 5 and after + ttl >= deadline - 5
            # A fresh mutation proves the finish hop released key locks/gates.
            assert inspector.call("SET", destination, "after-settlement") == "OK"
        finally:
            inspector.close()
            reader.close()


def failed_copy_restores_target(root):
    destination = "copy-failure{b}"
    with pair(
        root,
        "copy-rollback",
        client_mode="single",
        source_faults={
            "LAVIK_FAIL_GROUP_AUX_KEY": destination,
            "LAVIK_FAIL_GROUP_AUX_NTH": "2",
        },
    ) as (meta, source, target, writer):
        ready(meta)
        assert C.redis_slot("xa") % 2 != C.redis_slot(destination) % 2
        writer.call("SELECT", 1)
        fields = [part for i in range(256) for part in (str(i), "v" * 8192)]
        assert writer.call("HSET", "xa", *fields) == 256
        deadline = int(time.time() * 1000) + 120000
        writer.call("SELECT", 15)
        writer.call("SET", destination, "old-target", "PXAT", deadline)
        writer.call("SELECT", 1)
        try:
            result = writer.call("COPY", "xa", destination, "DB", 15, "REPLACE")
        except H.Failure:
            pass
        else:
            raise AssertionError(f"injected COPY ingestion failure returned {result}")
        assert writer.call("HLEN", "xa") == 256
        assert writer.call("HGET", "xa", "255") == "v" * 8192
        writer.call("SELECT", 15)
        assert writer.call("GET", destination) == "old-target"
        assert writer.call("PTTL", destination) > 0
        reader = Client(target)
        try:
            reader.call("SELECT", 15)
            H.wait_until(
                "failed COPY preserves replicated predecessor",
                20,
                lambda: reader.call("GET", destination) == "old-target",
            )
            assert reader.call("PTTL", destination) > 0
        finally:
            reader.close()


def keys_holds_population_until_disconnect(root):
    count = 768

    def seed(client):
        client.call("SELECT", 15)
        for i in range(count):
            client.call("SET", f"slow:{i}:" + "k" * 8192, "value")

    with pair(
        root,
        "keys-drain",
        client_mode="single",
        seed=seed,
        require_seed_before_full=True,
    ) as (meta, source, target, _writer):
        ready(meta)
        # COPY needs both DB gates even when its source is missing. Hold the
        # destination with KEYS and prove rejection cannot leave a partial key.
        owner_scan = Client(source)
        try:
            owner_scan.call("SELECT", 15)
            owner_scan.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            owner_scan.socket.sendall(F.encode_resp(["KEYS", "slow:*"]))
            assert owner_scan.reader.readline() == f"*{count}\r\n".encode()
            _writer.call("SELECT", 1)
            rejects(_writer, ("COPY", "missing", "gate-target", "DB", 15), "TRYAGAIN")
        finally:
            owner_scan.socket.shutdown(socket.SHUT_RDWR)
            owner_scan.close()
        H.wait_until(
            "COPY destination gate released after KEYS disconnect",
            10,
            lambda: _writer.call("COPY", "missing", "gate-target", "DB", 15) == 0,
        )
        slow = Client(target)
        inspector = Client(target)
        try:
            slow.call("SELECT", 15)
            slow.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            slow.socket.sendall(F.encode_resp(["KEYS", "slow:*"]))
            assert slow.reader.readline() == f"*{count}\r\n".encode()
            source.force_kill()
            H.wait_until(
                "failover selected the KEYS-serving replica",
                30,
                lambda: "event=candidate-selected" in meta.log_tail(lines=2000),
            )
            # No more reads from slow: promotion must retire the sender and
            # release its database gate without waiting for network flush.
            H.wait_until(
                "promotion retires the slow KEYS connection before drain",
                20,
                lambda: F.redis_call(target, ["SET", "after-keys", "ready"], db=15)
                == "OK",
            )
            inspector.close()
            inspector = Client(target)
            inspector.call("SELECT", 15)
            assert inspector.call("GET", "after-keys") == "ready"
        finally:
            if slow is not None:
                slow.socket.shutdown(socket.SHUT_RDWR)
                slow.close()
            inspector.close()


def main():
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    H.set_tag("managed-single")
    with tempfile.TemporaryDirectory(
        prefix="lavik-managed-single-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        basic_and_stale(Path(directory))
        full_and_copy(Path(directory))
        if C.has_fault(C.DATA, b"LAVIK_COPY_HOLD_FILE"):
            copy_fenced_after_wait(Path(directory))
            copy_hop_fencing(Path(directory), "read")
            copy_hop_fencing(Path(directory), "write")
            copy_hop_fencing(Path(directory), "write", fail_ingestion=True)
            failed_copy_restores_target(Path(directory))
        keys_holds_population_until_disconnect(Path(directory))
        N.CLIENT_MODE = "single"
        if C.has_fault(C.DATA, b"LAVIK_REPLICATION_HOLD_FIRST_HANDOFF_UNTIL_NEXT_ACK"):
            N.handoff_order(Path(directory))
            N.cancelled_handoff(Path(directory))
            N.divergent_tail(Path(directory), 0)
            N.divergent_tail(Path(directory), 1)
            N.rejected_full(
                Path(directory),
                "checksum",
                {"LAVIK_REPLICATION_CORRUPT_FULLSYNC_RECORD_FRAME_ONCE": "1"},
                {},
                "replication frame CRC32C mismatch",
            )
            N.rejected_full(
                Path(directory),
                "early-online",
                {"LAVIK_REPLICATION_EARLY_ONLINE": "1"},
                {},
                "injected ONLINE before local flow readiness",
            )
    H.log("PASS")


if __name__ == "__main__":
    main()
