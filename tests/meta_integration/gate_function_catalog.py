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

"""Managed Function catalog commands through real Meta and native replication."""

import concurrent.futures
import os
from pathlib import Path
import sys
import tempfile

import gate_failover as F
from function_catalog_data import library, snapshot, node_snapshot
from gate_data_control import allocate_data_file
from gate_native_replication import C, H, Client, pair, ready, rejects

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from redis_follower_smoke import process  # noqa: E402


def mutations(root):
    with pair(root, "mutations", client_mode="single") as (meta, _, target, writer):
        ready(meta)
        reader = Client(target)
        try:
            readonly = (
                "#!lua name=readonly\n"
                "redis.register_function{function_name='readonly_value', "
                "callback=function(keys, args) return 'ok' end, flags={'no-writes'}}"
            )
            assert writer.call("FUNCTION", "LOAD", readonly) == "readonly"
            for command in (
                ("FCALL", "readonly_value", 0),
                ("FCALL_RO", "readonly_value", 0),
            ):
                rejects(writer, command, "not yet supported")
            assert writer.call("FUNCTION", "FLUSH") == "OK"
            empty = snapshot(writer)
            assert empty[1] == []
            assert writer.call("SELECT", 15) == "OK"
            assert writer.call("FUNCTION", "LOAD", library("first", "one")) == "first"
            loaded = snapshot(writer)
            assert loaded != empty
            assert writer.call("SELECT", 0) == "OK"
            assert snapshot(writer) == loaded
            H.wait_until("catalog LOAD replay", 20, lambda: snapshot(reader) == loaded)
            for command in (
                ("FUNCTION", "LOAD", library("forbidden", "no")),
                ("FUNCTION", "DELETE", "first"),
                ("FUNCTION", "FLUSH"),
                ("FUNCTION", "RESTORE", empty[0], "FLUSH"),
            ):
                rejects(reader, command, "READONLY")
            assert snapshot(reader) == loaded
            assert writer.call("FUNCTION", "DELETE", "first") == "OK"
            H.wait_until("catalog DELETE replay", 20, lambda: snapshot(reader) == empty)
            assert writer.call("FUNCTION", "RESTORE", loaded[0], "FLUSH") == "OK"
            H.wait_until(
                "catalog RESTORE replay", 20, lambda: snapshot(reader) == loaded
            )
            assert writer.call("FUNCTION", "FLUSH", "ASYNC") == "OK"
            H.wait_until("catalog FLUSH replay", 20, lambda: snapshot(reader) == empty)
        finally:
            reader.close()


def revoked_at_commit(
    root, boundary="BEFORE_ROOT", ambiguous=False, operation="LOAD", mode="single"
):
    root = root / mode
    root.mkdir(exist_ok=True)
    authority_error = "MASTERDOWN" if mode == "single" else "CLUSTERDOWN"
    hold = root / f"{boundary}-{ambiguous}-{operation}.hold"
    failure = root / "root-sync.fail"
    variable = f"LAVIK_FUNCTION_CATALOG_{boundary}_HOLD_FILE"
    # The second device belongs to another worker. A late revocation must not
    # interrupt the mirrored-root loop, and an ambiguous first root must not
    # make restart select a generation absent from the second device.
    second_device = root / f"{boundary}-{ambiguous}-{operation}.data"
    allocate_data_file(second_device)
    faults = {variable: str(hold)}
    if ambiguous:
        faults["LAVIK_SYSTEM_STATE_ROOT_SYNC_FAIL_FILE"] = str(failure)
    with pair(
        root,
        f"revoke-{boundary}-{ambiguous}-{operation}",
        client_mode=mode,
        source_faults=faults,
        source_extra_args=("--data-file", str(second_device)),
    ) as (meta, source, target, writer):
        ready(meta)
        empty = snapshot(writer)
        assert writer.call("FUNCTION", "LOAD", library("stable", "old")) == "stable"
        original = snapshot(writer)
        command = {
            "LOAD": ("FUNCTION", "LOAD", "REPLACE", library("stable", "new")),
            "DELETE": ("FUNCTION", "DELETE", "stable"),
            "FLUSH": ("FUNCTION", "FLUSH", "ASYNC"),
            "RESTORE": ("FUNCTION", "RESTORE", empty[0], "FLUSH"),
        }[operation]
        reader = Client(target)
        try:
            H.wait_until(
                "predecessor replicated", 20, lambda: snapshot(reader) == original
            )
            hold.touch()
            if ambiguous:
                failure.touch()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(writer.call, *command)
                try:
                    H.wait_until(
                        "catalog reaches commit boundary",
                        10,
                        lambda: f"fault pause reached: {variable}"
                        in Path(source.log_path).read_text(),
                    )
                    meta.pause()
                    H.wait_until(
                        "Owner lease expires while catalog is held",
                        6,
                        lambda: authority_error
                        in F.redis_error(source, ["GET", "lease-probe"]),
                    )
                    # Losing the Owner lease retires the pending client's
                    # connection even while its catalog mutation is held.
                    # The reply may therefore be lost before the hold lifts.
                    if pending.done():
                        try:
                            result = pending.result()
                        except H.Failure as error:
                            assert "Data closed its Redis connection" in str(error), (
                                error
                            )
                        else:
                            raise AssertionError(
                                f"held catalog reported success before release: {result!r}"
                            )
                    hold.unlink()
                    if boundary == "AFTER_ROOT_WRITE" and not ambiguous:
                        try:
                            assert pending.result(timeout=15) == "stable"
                        except H.Failure as error:
                            assert "Data closed its Redis connection" in str(error), (
                                error
                            )
                    else:
                        try:
                            result = pending.result(timeout=15)
                        except H.Failure as error:
                            assert (
                                "ambiguous" if ambiguous else authority_error
                            ) in str(
                                error
                            ) or "Data closed its Redis connection" in str(error), error
                        else:
                            raise AssertionError(
                                f"revoked catalog reported success: {result!r}"
                            )
                    if ambiguous:
                        # The original connection must close and every other
                        # client must be fenced, even if the root reached disk.
                        # Lease expiry can mask the latched storage error with
                        # an authority error until the Owner is restored.
                        assert writer.reader.read(1) == b""
                        for command in (
                            ["FUNCTION", "LIST"],
                            ["SET", "unsafe", "write"],
                        ):
                            error = F.redis_error(source, command)
                            assert "LOADING" in error or authority_error in error, error
                        assert snapshot(reader) == original
                        # A latched storage failure deliberately cannot cleanly
                        # flush/shut down; recovery starts from this crash cut.
                        source.force_kill()
                finally:
                    hold.unlink(missing_ok=True)
                    failure.unlink(missing_ok=True)
                    meta.resume()
            if not ambiguous:
                H.wait_until(
                    "Owner renews authority",
                    20,
                    lambda: F.redis_call(source, ["GET", "lease-probe"]) is None,
                )
                # The original writer was retired at lease expiry; inspect
                # the committed outcome through a fresh connection.
                current = node_snapshot(source)
                if boundary == "AFTER_ROOT_WRITE":
                    assert current != original
                else:
                    assert current == original
                H.wait_until(
                    "catalog outcome replicated",
                    20,
                    lambda: snapshot(reader) == current,
                )
        finally:
            reader.close()
    if ambiguous:
        with process(
            C.DATA,
            Path(source.workdir),
            "recovered",
            port=source.redis_port,
            workers=source.workers,
            extra=("--data-file", str(second_device)),
        ):
            recovered = Client(source)
            try:
                assert snapshot(recovered) == original
                assert recovered.call("GET", "unsafe") is None
            finally:
                recovered.close()


def full_restart_and_stale_reads(root):
    expected = []

    def seed(client):
        assert (
            client.call("FUNCTION", "LOAD", library("full_catalog", "kept"))
            == "full_catalog"
        )
        expected.append(snapshot(client))

    def dirty_target(target):
        with process(
            C.DATA, Path(target.workdir), "old-catalog", workers=target.workers
        ) as (client, _, _):
            assert (
                client.call("FUNCTION", "LOAD", library("obsolete", "discard"))
                == "obsolete"
            )

    with pair(
        root,
        "full",
        client_mode="single",
        seed=seed,
        prepare_target=dirty_target,
        require_seed_before_full=True,
    ) as (meta, source, target, _):
        ready(meta)
        reader = Client(target)
        try:
            H.wait_until(
                "FULL replaces nonempty catalog",
                30,
                lambda: snapshot(reader) == expected[0],
            )
            reader.close()
            reader = None
            # A fresh replica boot must recover and follow the same Meta Owner.
            target.terminate()
            target.start()
            H.wait_until(
                "catalog survives managed restart",
                30,
                lambda: node_snapshot(target) == expected[0],
            )
            reader = Client(target)
            meta.pause()
            try:
                source.force_kill()
                H.wait_until(
                    "catalog replica disconnected",
                    15,
                    lambda: "master_link_status:down"
                    in reader.call("INFO", "replication"),
                )
                assert snapshot(reader) == expected[0]
                reader.call("CONFIG", "SET", "replica-serve-stale-data", "no")
                for subcommand in ("DUMP", "LIST"):
                    rejects(reader, ("FUNCTION", subcommand), "MASTERDOWN")
                reader.call("CONFIG", "SET", "replica-serve-stale-data", "yes")
                assert snapshot(reader) == expected[0]
            finally:
                meta.resume()
        finally:
            if reader is not None:
                reader.close()


def controlled_pause_drains_catalog(root, boundary, mode="single"):
    root = root / mode
    root.mkdir(exist_ok=True)
    hold = root / f"pause-{boundary}.hold"
    variable = f"LAVIK_FUNCTION_CATALOG_{boundary}_HOLD_FILE"
    fixture = F.FailoverFixture(
        C.META,
        C.DATA,
        C.CTL,
        str(root / f"pause-{boundary}"),
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
        assert writer.call("FUNCTION", "LOAD", library("paused", "old")) == "paused"
        hold.touch()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(
                writer.call, "FUNCTION", "LOAD", "REPLACE", library("paused", "new")
            )
            try:
                H.wait_until(
                    "catalog holds Group admission",
                    10,
                    lambda: f"fault pause reached: {variable}"
                    in Path(owner.log_path).read_text(),
                )
                fixture.submit_failover()
                H.wait_until(
                    "Controlled Pause closes new mutation admission",
                    15,
                    lambda: F.redis_error(
                        owner, ["SET", "pause-probe", "bad"]
                    ).startswith("TRYAGAIN"),
                )
                assert not pending.done()
                assert F.redis_call(owner, ["ROLE"])[0] == "master"
                assert F.redis_call(owner, ["GET", "pause-read-probe"]) is None
                # Catalog mutations share the pause gate; they do not queue
                # behind the already held catalog guard and block the drain.
                assert F.redis_error(owner, ["FUNCTION", "FLUSH"]).startswith(
                    "TRYAGAIN"
                )
                hold.unlink()
                assert pending.result(timeout=15) == "paused"
            finally:
                hold.unlink(missing_ok=True)
        begin = F.require_unique_failover_event(
            fixture.metas, "begin", "controlled", loss="none"
        )
        selected = begin["candidate"]
        assert selected in (F.CANDIDATE, F.FOLLOWER), begin
        F.wait_serving_owner(fixture, selected, fixture.data_nodes, timeout=60)
        successor = Client(fixture.by_id[selected])
        try:
            expected = snapshot(successor)
            assert len(expected[1]) == 1
            entry = dict(zip(expected[1][0][::2], expected[1][0][1::2]))
            assert entry["library_code"] == library("paused", "new"), entry
            for node in fixture.data_nodes:

                def matches(node=node):
                    client = Client(node)
                    try:
                        return snapshot(client) == expected
                    finally:
                        client.close()

                H.wait_until("catalog survives controlled cutover", 30, matches)
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


def main():
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    H.set_tag("function-catalog")
    with tempfile.TemporaryDirectory(
        prefix="lavik-functions-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        if len(sys.argv) > 5:
            revoked_at_commit(Path(directory), sys.argv[5], "ambiguous" in sys.argv[6:])
        else:
            mutations(Path(directory))
            full_restart_and_stale_reads(Path(directory))
            if C.has_fault(C.DATA, b"LAVIK_FUNCTION_CATALOG_BEFORE_ROOT_HOLD_FILE"):
                revoked_at_commit(Path(directory), "BEFORE_STAGE")
                for operation in ("LOAD", "DELETE", "FLUSH", "RESTORE"):
                    revoked_at_commit(Path(directory), operation=operation)
                revoked_at_commit(Path(directory), "AFTER_ROOT_WRITE")
                revoked_at_commit(Path(directory), "AFTER_ROOT_WRITE", ambiguous=True)
                for boundary in ("BEFORE_ROOT", "AFTER_ROOT_WRITE"):
                    controlled_pause_drains_catalog(Path(directory), boundary)
                for boundary in ("BEFORE_STAGE", "BEFORE_ROOT", "AFTER_ROOT_WRITE"):
                    revoked_at_commit(Path(directory), boundary, mode="cluster")
                revoked_at_commit(
                    Path(directory), "AFTER_ROOT_WRITE", ambiguous=True, mode="cluster"
                )
                for boundary in ("BEFORE_ROOT", "AFTER_ROOT_WRITE"):
                    controlled_pause_drains_catalog(
                        Path(directory), boundary, mode="cluster"
                    )
    H.log("PASS")


if __name__ == "__main__":
    main()
