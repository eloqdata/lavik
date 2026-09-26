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

"""Group-scoped FLUSH through Redis, real Meta and native replication."""

import concurrent.futures
import os
from pathlib import Path
import sys
import tempfile

import gate_failover as F
from gate_data_control import allocate_data_file
from function_catalog_data import library, snapshot
from gate_native_replication import C, H, Client, pair, ready, rejects
from gate_population_recovery import wait_durable


def semantics(root, mode):
    with pair(root, mode, client_mode=mode) as (meta, source, target, writer):
        ready(meta)
        reader = Client(target, readonly=mode == "cluster")
        try:
            assert writer.call("FUNCTION", "LOAD", library("kept", "value")) == "kept"
            catalog = snapshot(writer)
            count = 16 if mode == "single" else 1
            for db in range(count):
                assert writer.call("SELECT", db) == "OK"
                assert writer.call("SET", "{flush}old", f"db-{db}") == "OK"
            assert writer.call("SELECT", 0) == "OK"
            for command in ("FLUSHDB", "FLUSHALL"):
                rejects(writer, (command, "invalid"), "syntax error")
                rejects(writer, (command, "SYNC", "ASYNC"), "wrong number")
                rejects(reader, (command,), "READONLY")
            assert writer.call("FLUSHDB") == "OK"
            assert writer.call("GET", "{flush}old") is None
            for db in range(1, count):
                assert writer.call("SELECT", db) == "OK"
                assert writer.call("GET", "{flush}old") == f"db-{db}"
            for command in ("FLUSHDB", "FLUSHALL"):
                for option in ("SYNC", "ASYNC"):
                    assert writer.call(command, option) == "OK"
                    assert writer.call("SET", "{flush}new", option) == "OK"
                    H.wait_until(
                        "replica readable after flush and following write",
                        30,
                        lambda: reader.call("SELECT", count - 1) == "OK"
                        and reader.call("GET", "{flush}new") == option,
                    )
                    assert snapshot(writer) == catalog
            # SYNC is local retirement, not a wait for replica acknowledgements.
            target.pause()
            try:
                assert writer.call("FLUSHALL") == "OK"
            finally:
                target.resume()
            if mode == "cluster":
                for command in ("FLUSHDB", "FLUSHALL"):
                    assert writer.call("MULTI") == "OK"
                    rejects(writer, (command,), "not allowed in transactions")
                    rejects(writer, ("EXEC",), "EXECABORT")
            else:
                rejects(writer, ("MULTI",), "not yet supported")
            for db in range(count):
                assert writer.call("SELECT", db) == "OK"
                assert writer.call("DBSIZE") == 0

                def replica_empty():
                    return (
                        reader.call("SELECT", db) == "OK"
                        and reader.call("GET", "{flush}old") is None
                        and reader.call("GET", "{flush}new") is None
                    )

                H.wait_until(f"replica DB{db} empty", 30, replica_empty)
            assert snapshot(reader) == catalog
        finally:
            reader.close()


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from redis_follower_smoke import process  # noqa: E402


def paused_at(source, variable):
    H.wait_until(
        variable,
        15,
        lambda: f"fault pause reached: {variable}" in Path(source.log_path).read_text(),
    )


def revoked(root, mode, command, boundary):
    name = f"revoke-{mode}-{command}-{boundary}"
    hold = root / f"{name}.hold"
    variable = f"LAVIK_FLUSH_{boundary}_HOLD_FILE"
    second = root / f"{name}.data"
    allocate_data_file(second)
    before = boundary in ("BEFORE_DRAIN", "BEFORE_EPOCH")
    error = "MASTERDOWN" if mode == "single" else "CLUSTERDOWN"
    with pair(
        root,
        name,
        client_mode=mode,
        source_faults={variable: str(hold)},
        source_extra_args=("--data-file", str(second)),
    ) as (meta, source, target, writer):
        ready(meta)
        reader = Client(target, readonly=mode == "cluster")
        try:
            assert writer.call("SET", "{flush}old", "old") == "OK"
            H.wait_until(
                "old data readable on replica",
                30,
                lambda: reader.call("GET", "{flush}old") == "old",
            )
            hold.touch()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(writer.call, command)
                try:
                    paused_at(source, variable)
                    meta.pause()
                    H.wait_until(
                        "lease expired",
                        10,
                        lambda: error
                        in F.redis_error(source, ["SET", "lease-probe", "x"]),
                    )
                    assert not pending.done()
                    hold.unlink()
                    if before:
                        try:
                            pending.result(timeout=20)
                        except H.Failure as failure:
                            assert error in str(failure), failure
                        else:
                            raise AssertionError(
                                "write-before-authority rejection reported success"
                            )
                    else:
                        assert pending.result(timeout=20) == "OK"
                finally:
                    hold.unlink(missing_ok=True)
                    meta.resume()
            expected = "old" if before else None
            H.wait_until(
                "Owner authority restored and outcome visible",
                30,
                lambda: writer.call("GET", "{flush}old") == expected,
            )
            H.wait_until(
                "flush outcome replayed",
                30,
                lambda: reader.call("GET", "{flush}old") == expected,
            )
            assert writer.call("SET", "{flush}new", "after") == "OK"
            H.wait_until(
                "post-flush write replayed",
                30,
                lambda: reader.call("GET", "{flush}new") == "after",
            )
        finally:
            reader.close()
    # Pair shutdown checkpoints pending allocator metadata too. Recover these
    # exact devices without Meta or a donor: a role change must not invalidate
    # this disk assertion, and a replica rebuild must not hide a polluted epoch.
    if boundary == "BEFORE_EPOCH":
        with process(
            C.DATA,
            Path(source.workdir),
            "rejected-recovery",
            port=source.redis_port,
            workers=source.workers,
            extra=("--data-file", str(second)),
        ):
            recovered = Client(source)
            try:
                assert recovered.call("GET", "{flush}old") == "old"
            finally:
                recovered.close()


def storage_failure(root, command, kind, device):
    name = f"io-{command}-{kind}-{device}"
    failure = root / f"{name}.fail"
    second = root / f"{name}.data"
    allocate_data_file(second)
    faults = {
        "LAVIK_EPOCH_IO_FAIL_FILE": str(failure),
        "LAVIK_EPOCH_IO_FAIL_KIND": kind,
        "LAVIK_EPOCH_IO_FAIL_DEVICE": str(device),
    }
    with pair(
        root,
        name,
        client_mode="single",
        source_faults=faults,
        source_extra_args=("--data-file", str(second)),
    ) as (meta, source, _, writer):
        ready(meta)
        assert writer.call("SET", "{flush}old", "old") == "OK"
        # Keep the catalog alongside the keyspace through fault recovery.
        assert writer.call("FUNCTION", "LOAD", library("kept", "value")) == "kept"
        wait_durable(source)
        failure.touch()
        try:
            rejects(
                writer,
                (command,),
                "short write" if kind == "short" else f"epoch {kind} failure",
            )
            assert "LOADING" in F.redis_error(source, ["SET", "unsafe", "x"])
            # The armed fault remains present. Returning promptly is evidence
            # that this command did not add retry-until-success behavior.
            source.force_kill()
        finally:
            failure.unlink(missing_ok=True)
    with process(
        C.DATA,
        Path(source.workdir),
        "recovered",
        port=source.redis_port,
        workers=source.workers,
        extra=("--data-file", str(second)),
    ):
        recovered = Client(source)
        try:
            if device == 2:
                # Device 1's new epoch is known durable. Epoch recovery selects
                # the maximum, unlike Function catalog common-root recovery.
                assert recovered.call("GET", "{flush}old") is None
            # First-device sync/write outcomes can be uncertain; no assertion
            # that an error means the flush did not happen.
            assert recovered.call("SET", "recovered", "usable") == "OK"
        finally:
            recovered.close()


def crash_after_durable_epoch(root, command):
    name = f"crash-{command}"
    hold = root / f"{name}.hold"
    second = root / f"{name}.data"
    allocate_data_file(second)
    variable = "LAVIK_FLUSH_AFTER_EPOCH_SYNC_HOLD_FILE"
    with pair(
        root,
        name,
        client_mode="single",
        source_faults={variable: str(hold)},
        source_extra_args=("--data-file", str(second)),
    ) as (meta, source, _, writer):
        ready(meta)
        assert writer.call("SET", "{flush}old", "old") == "OK"
        wait_durable(source)
        hold.touch()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(writer.call, command)
            try:
                paused_at(source, variable)
                source.force_kill()
                try:
                    pending.result(timeout=10)
                except (H.Failure, OSError, EOFError):
                    pass
            finally:
                hold.unlink(missing_ok=True)
    with process(
        C.DATA,
        Path(source.workdir),
        "recovered",
        port=source.redis_port,
        workers=source.workers,
        extra=("--data-file", str(second)),
    ):
        recovered = Client(source)
        try:
            assert recovered.call("GET", "{flush}old") is None
        finally:
            recovered.close()


def reclaim(root, option, fail=False):
    name = f"reclaim-{option}-{fail}"
    hold = root / f"{name}.hold"
    failure = root / f"{name}.fail"
    variable = "LAVIK_FLUSH_RECLAIM_HOLD_FILE"
    with pair(
        root,
        name,
        client_mode="single",
        source_faults={
            variable: str(hold),
            "LAVIK_FLUSH_RECLAIM_WAIT_FAIL_FILE": str(failure),
        },
    ) as (meta, source, _, writer):
        ready(meta)
        for i in range(100):
            assert writer.call("SET", f"old-{i}", "old") == "OK"
        start = len(Path(source.log_path).read_text())
        hold.touch()
        if fail:
            failure.touch()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(writer.call, "FLUSHALL", option)
            try:
                H.wait_until(
                    "all worker reclaimers started",
                    15,
                    lambda: all(
                        f"fault reclaim worker[{w}] started"
                        in Path(source.log_path).read_text()[start:]
                        for w in range(2)
                    ),
                )
                if fail:
                    try:
                        pending.result(timeout=10)
                    except H.Failure as error:
                        assert "detached reclaim wait failure" in str(error), error
                    else:
                        raise AssertionError("SYNC reclaim error was swallowed")
                elif option == "ASYNC":
                    assert pending.result(timeout=10) == "OK"
                else:
                    assert not pending.done()
                # Gates and Group drain are released while SYNC is waiting.
                assert F.redis_call(source, ["SET", "after-flush", "kept"]) == "OK"
                hold.unlink()
                failure.unlink(missing_ok=True)
                if not fail:
                    assert pending.result(timeout=15) == "OK"
                assert writer.call("GET", "after-flush") == "kept"
                # A following SYNC joins any previous ASYNC retirement.
                assert writer.call("SELECT", 15) == "OK"
                assert writer.call("FLUSHDB", "SYNC") == "OK"
                assert writer.call("SELECT", 0) == "OK"
                assert writer.call("GET", "after-flush") == "kept"
            finally:
                hold.unlink(missing_ok=True)
                failure.unlink(missing_ok=True)


def controlled_pause(root, mode, boundary):
    name = f"pause-{mode}-{boundary}"
    before = boundary == "BEFORE_EPOCH"
    expected = None
    hold = root / f"{name}.hold"
    variable = f"LAVIK_FLUSH_{boundary}_HOLD_FILE"
    fixture = F.FailoverFixture(
        C.META,
        C.DATA,
        C.CTL,
        str(root / name),
        False,
        pause_after_begin_ms=1,
        data_workers=2,
        client_mode=mode,
    )
    owner = fixture.by_id[F.OWNER]
    owner.environment = {**os.environ, variable: str(hold)}
    writer = None
    try:
        fixture.start_created()
        writer = Client(owner)
        assert writer.call("SET", "old", "old") == "OK"
        hold.touch()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(writer.call, "FLUSHALL")
            try:
                paused_at(owner, variable)
                fixture.submit_failover()
                H.wait_until(
                    "Controlled Pause blocks new FLUSH",
                    15,
                    lambda: F.redis_error(owner, ["FLUSHDB"]).startswith("TRYAGAIN"),
                )
                assert not pending.done()
                assert F.redis_call(owner, ["ROLE"])[0] == "master"
                hold.unlink()
                try:
                    assert pending.result(timeout=20) == "OK"
                except H.Failure as error:
                    # Pause blocks new registration, not an already registered
                    # Group guard. Before IO, the retained finite authority can
                    # still expire; only that pre-cut refusal may preserve old
                    # data. revoked() tests explicit lease expiry separately.
                    assert before and str(error).startswith("TRYAGAIN"), error
                    expected = "old"
            finally:
                hold.unlink(missing_ok=True)
        begin = F.require_unique_failover_event(
            fixture.metas, "begin", "controlled", loss="none"
        )
        selected = begin["candidate"]
        F.wait_serving_owner(fixture, selected, fixture.data_nodes, timeout=60)
        successor = Client(fixture.by_id[selected])
        try:
            assert successor.call("GET", "old") == expected
            assert successor.call("SET", "after-failover", "new") == "OK"
            for node in fixture.data_nodes:

                def readable(node=node):
                    client = Client(node, readonly=mode == "cluster")
                    try:
                        return (
                            client.call("GET", "old") == expected
                            and client.call("GET", "after-failover") == "new"
                        )
                    finally:
                        client.close()

                H.wait_until(
                    "new population readable after controlled cutover", 30, readable
                )
        finally:
            successor.close()
        fixture.clean_shutdown()
    except BaseException:
        fixture.dump_logs()
        raise
    finally:
        hold.unlink(missing_ok=True)
        if writer is not None:
            writer.close()
        fixture.force_kill()


def drain(root, command, revoke=False):
    name = f"drain-{command}-{revoke}"
    hold = root / f"{name}.hold"
    flush_hold = root / f"{name}-flush.hold"
    with pair(
        root,
        name,
        client_mode="single",
        source_faults={
            "LAVIK_DB_OPERATION_HOLD_KEY": "old",
            "LAVIK_DB_OPERATION_HOLD_FILE": str(hold),
            "LAVIK_FLUSH_BEFORE_DRAIN_HOLD_FILE": str(flush_hold),
        },
    ) as (meta, source, _, writer):
        ready(meta)
        assert writer.call("SET", "old", "value") == "OK"
        old_reader, flusher, new_writer = Client(source), Client(source), Client(source)
        hold.touch()
        flush_hold.touch()
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                old = pool.submit(old_reader.call, "GET", "old")
                paused_at(source, "LAVIK_DB_OPERATION_HOLD_FILE")
                pending = pool.submit(flusher.call, command)
                try:
                    paused_at(source, "LAVIK_FLUSH_BEFORE_DRAIN_HOLD_FILE")
                    flush_hold.unlink()
                    assert not pending.done()
                    if revoke:
                        meta.pause()
                        H.wait_until(
                            "lease expires while old DB operation drains",
                            10,
                            lambda: "MASTERDOWN"
                            in F.redis_error(source, ["SET", "probe", "x"]),
                        )
                        hold.unlink()
                        assert old.result(timeout=10) == "value"
                        try:
                            pending.result(timeout=10)
                        except H.Failure as error:
                            assert "MASTERDOWN" in str(error), error
                        else:
                            raise AssertionError("revoked draining flush succeeded")
                    else:
                        # With DB0 busy, FLUSHALL must already have closed DB15.
                        assert new_writer.call("SELECT", 15) == "OK"
                        new = pool.submit(new_writer.call, "SET", "after", "kept")
                        if command == "FLUSHDB":
                            assert new.result(timeout=5) == "OK"
                        else:
                            import time

                            time.sleep(0.1)
                            assert not new.done()
                        hold.unlink()
                        assert old.result(timeout=10) == "value"
                        assert pending.result(timeout=15) == "OK"
                        assert new.result(timeout=10) == "OK"
                        assert new_writer.call("GET", "after") == "kept"
                        assert writer.call("GET", "old") is None
                finally:
                    hold.unlink(missing_ok=True)
                    flush_hold.unlink(missing_ok=True)
                    meta.resume()
            if revoke:
                H.wait_until(
                    "rejected flush preserved data",
                    30,
                    lambda: writer.call("GET", "old") == "value",
                )
        finally:
            hold.unlink(missing_ok=True)
            flush_hold.unlink(missing_ok=True)
            for client in (old_reader, flusher, new_writer):
                client.close()


def replication_recovery(root, full=False):
    if full:
        assert C.has_fault(C.DATA, b"LAVIK_REPLICATION_FULLSYNC_PAUSE_ARM_FILE"), (
            "armed FULL hook required"
        )
    name = "full-invalidation" if full else "partial-publication"
    failure = root / f"{name}.fail"
    hold = root / f"{name}.hold"
    faults = (
        {
            "LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "10000",
            "LAVIK_REPLICATION_FULLSYNC_PAUSE_ARM_FILE": str(failure),
        }
        if full
        else {
            "LAVIK_FLUSH_PUBLISH_FAIL_FILE": str(failure),
            "LAVIK_FLUSH_AFTER_FIRST_FLOW_HOLD_FILE": str(hold),
        }
    )

    def seed(writer):
        for i in range(64):
            assert writer.call("SET", f"old-{i}", "old") == "OK"

    with pair(
        root,
        name,
        client_mode="single",
        source_faults=faults,
        target_faults={"LAVIK_FLUSH_OBSERVE_PARTIAL_BARRIER": "1"},
        seed=seed,
        require_seed_before_full=False,
    ) as (meta, source, target, writer):
        ready(meta)
        if full:
            # Creation's explicit rebuild is a terminal control-plane task.
            # Exercise reconnect FULL under ordinary Follow Owner instead.
            failure.touch()
            target.terminate()
            target.start()
            H.wait_until(
                "FULL handed off a partition before flush",
                30,
                lambda: "paused full sync after acknowledged handoff partition"
                in Path(source.log_path).read_text(),
            )
        original_history = writer.call("INFO", "replication")
        before_full = Path(source.log_path).read_text().count("selected=FULL")
        if not full:
            failure.touch()
        try:
            if full:
                assert writer.call("FLUSHALL") == "OK"
            else:
                hold.touch()
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    pending = pool.submit(writer.call, "FLUSHALL")
                    try:
                        paused_at(source, "LAVIK_FLUSH_AFTER_FIRST_FLOW_HOLD_FILE")
                        H.wait_until(
                            "replica has an incomplete control barrier",
                            15,
                            lambda: "partial FLUSH barrier received:"
                            in Path(target.log_path).read_text(),
                        )
                        assert F.redis_call(target, ["GET", "old-0"]) == "old"
                        hold.unlink()
                        assert pending.result(timeout=15) == "OK"
                    finally:
                        hold.unlink(missing_ok=True)
        finally:
            failure.unlink(missing_ok=True)
        for i in range(64):
            assert writer.call("SET", f"new-{i}", "new") == "OK"
        reader = Client(target)
        try:

            def matches():
                return all(
                    reader.call("GET", f"old-{i}") is None
                    and reader.call("GET", f"new-{i}") == "new"
                    for i in range(64)
                )

            H.wait_until(
                "replacement FULL is readable with post-flush writes", 90, matches
            )
            assert (
                Path(source.log_path).read_text().count("selected=FULL") > before_full
            )
            if not full:

                def history(info):
                    return next(
                        line
                        for line in info.splitlines()
                        if line.startswith("master_replid:")
                    )

                assert history(writer.call("INFO", "replication")) != history(
                    original_history
                )
            # A later all-flow barrier must not wait forever on the cancelled one.
            assert writer.call("FLUSHDB") == "OK"
            assert writer.call("SET", "after-recovery", "ok") == "OK"
            H.wait_until(
                "barrier after recovery completes",
                30,
                lambda: reader.call("GET", "after-recovery") == "ok",
            )
        finally:
            reader.close()


def main():
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    H.set_tag("managed-flush")
    with tempfile.TemporaryDirectory(
        prefix="lavik-flush-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        root = Path(directory)
        scenario = sys.argv[5] if len(sys.argv) > 5 else "semantics"
        if scenario != "semantics":
            assert C.has_fault(C.DATA, b"LAVIK_FLUSH_BEFORE_EPOCH_HOLD_FILE"), (
                "fault build required"
            )
        if scenario == "semantics":
            for mode in ("single", "cluster"):
                semantics(root, mode)
        elif scenario == "authority":
            for mode in ("single", "cluster"):
                for command in ("FLUSHDB", "FLUSHALL"):
                    for boundary in (
                        "BEFORE_DRAIN",
                        "BEFORE_EPOCH",
                        "AFTER_EPOCH_WRITE",
                        "BEFORE_PUBLICATION",
                        "BEFORE_REPLY",
                    ):
                        revoked(root, mode, command, boundary)
                for boundary in ("BEFORE_EPOCH", "AFTER_EPOCH_WRITE"):
                    controlled_pause(root, mode, boundary)
        elif scenario == "storage":
            for command in ("FLUSHDB", "FLUSHALL"):
                for kind in ("short", "write", "sync"):
                    for device in (1, 2):
                        storage_failure(root, command, kind, device)
                crash_after_durable_epoch(root, command)
        elif scenario == "replication":
            replication_recovery(root)
            replication_recovery(root, full=True)
        elif scenario == "drain":
            for command in ("FLUSHDB", "FLUSHALL"):
                drain(root, command)
                drain(root, command, revoke=True)
        elif scenario == "reclaim":
            reclaim(root, "SYNC")
            reclaim(root, "ASYNC")
            reclaim(root, "SYNC", fail=True)
        else:
            raise AssertionError(f"unknown scenario: {scenario}")
    H.log("PASS")


if __name__ == "__main__":
    main()
