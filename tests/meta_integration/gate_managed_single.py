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
from pathlib import Path
import sys
import tempfile
import time

import gate_native_replication as N
from gate_native_replication import C, H, Client, pair, ready, rejects


def basic_and_stale(root):
    with pair(root, "single-read", client_mode="single") as (
        meta,
        source,
        target,
        writer,
    ):
        ready(meta)
        reader = Client(target)
        try:
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
            for client in (writer, reader):
                for command in (
                    ("MGET", "first-slot", "another-slot"),
                    ("MULTI",),
                    ("SELECT", 1),
                    ("DBSIZE",),
                    ("EVAL", "return 1", 0),
                ):
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
            rejects(writer, ("GET", "count"), "MASTERDOWN")
            rejects(writer, ("SET", "count", "bad"), "MASTERDOWN")
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
            rejects(reader, ("INCR", "count"), "READONLY")
            assert (
                reader.call("CONFIG", "SET", "replica-serve-stale-data", "no") == "OK"
            )
            rejects(reader, ("GET", "count"), "MASTERDOWN")
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

    # A clean complete former replica may start without Meta through the
    # existing standalone recovery path. No management provenance is written.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from redis_follower_smoke import process

    with process(C.DATA, root / "single-read" / "target", "standalone") as (
        client,
        _,
        _,
    ):
        assert client.call("GET", "count") == "2"
        assert client.call("SELECT", 15) == "OK"
        assert client.call("SET", "standalone-db15", "value") == "OK"


def main():
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    H.set_tag("managed-single")
    with tempfile.TemporaryDirectory(
        prefix="lavik-managed-single-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        basic_and_stale(Path(directory))
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
