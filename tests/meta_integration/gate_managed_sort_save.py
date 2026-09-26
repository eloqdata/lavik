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

"""Managed SORT_RO and RDB command compatibility with Redis 7.2.14."""

from contextlib import contextmanager
import concurrent.futures
import shutil
import subprocess
import os
import time
from pathlib import Path
import sys
import tempfile

import gate_failover as F
from gate_managed_composite_faults import expire, reached
from gate_native_replication import C, H, Client, pair, ready, rejects

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from redis_follower_smoke import process  # noqa: E402

REDIS = ""


@contextmanager
def reference(root, mode):
    directory = root / f"reference-{mode}"
    extra = ()
    if mode == "cluster":
        extra = (
            "--cluster-enabled",
            "yes",
            "--cluster-config-file",
            str(directory / "nodes.conf"),
            "--cluster-port",
            str(H.free_port()),
        )
    with process(REDIS, directory, "redis", redis=True, extra=extra) as (client, _, _):
        assert "redis_version:7.2.14" in client.call("INFO", "server")
        if mode == "cluster":
            assert client.call("CLUSTER", "ADDSLOTS", *range(16384)) == "OK"
            H.wait_until(
                "Redis Cluster ready",
                10,
                lambda: "cluster_state:ok" in client.call("CLUSTER", "INFO"),
            )
        yield client


def outcome(client, command):
    try:
        return ("reply", client.call(*command))
    except H.Failure as error:
        return ("error", str(error))


def compare(client, redis, command):
    expected = outcome(redis, command)
    actual = outcome(client, command)
    assert actual == expected, (command, actual, expected)
    return actual[1]


def restore(root, output, mode, name, expect_exec=False):
    directory = root / f"restore-{mode}-{name}"
    directory.mkdir()
    shutil.copyfile(output / "dump.rdb", directory / "dump.rdb")
    with process(REDIS, directory, "restored", redis=True) as (client, _, _):
        for db in (0, 15) if mode == "single" else (0,):
            assert client.call("SELECT", db) == "OK"
            assert client.call("GET", "{save}key") == f"db-{db}"
            assert client.call("GET", "{save}ttl") == "expiring"
            assert 0 < client.call("TTL", "{save}ttl") <= 3600
            assert client.call("LRANGE", "{save}list", 0, -1) == ["a", "b"]
            assert client.call("HGET", "{save}hash", "field") == "value"
            assert client.call("SMEMBERS", "{save}set") == ["member"]
            assert client.call("ZRANGE", "{save}zset", 0, -1) == ["member"]
            assert client.call("XRANGE", "{save}stream", "-", "+") == [
                ["1-0", ["field", "value"]]
            ]
            assert client.call("FCALL_RO", "saved_get", 1, "{save}key") == f"db-{db}"
        if expect_exec:
            assert client.call("GET", "{save}exec") == "after"


def sorting(root, mode):
    with (
        reference(root, mode) as redis,
        pair(root, f"sort-{mode}", client_mode=mode) as (meta, source, target, writer),
    ):
        ready(meta)
        db = 15 if mode == "single" else 0
        compare(writer, redis, ("SELECT", db))
        key = "{sort}ids"
        compare(writer, redis, ("RPUSH", "{sort}matrix", "10", "2", "-1", "3"))
        compare(writer, redis, ("SET", "{sort}wrong", "string"))
        for args in (
            (),
            ("DESC",),
            ("ALPHA",),
            ("LIMIT", -1, 2),
            ("LIMIT", 1, -1),
            ("LIMIT", 0, 0),
            ("BY", "nosort"),
            ("GET", "#"),
            ("BY", "{sort}weight_*"),
            ("GET", "{sort}label_*"),
            ("STORE", "{sort}out"),
            ("LIMIT", "invalid", 2),
            ("ALPHA", "invalid"),
        ):
            compare(writer, redis, ("SORT_RO", "{sort}matrix", *args))
        compare(writer, redis, ("SORT_RO", "{sort}missing"))
        compare(writer, redis, ("SORT_RO", "{sort}wrong"))
        assert writer.call("RPUSH", key, "3", "1", "2") == 3
        assert writer.call("SORT_RO", key) == ["1", "2", "3"]
        assert writer.call("SORT_RO", key, "DESC", "LIMIT", 1, 1) == ["2"]
        assert writer.call("LRANGE", key, 0, -1) == ["3", "1", "2"]
        rejects(writer, ("SORT_RO", key, "STORE", "{sort}out"), "syntax")
        assert writer.call(
            "EVAL_RO", "return redis.call('SORT_RO',KEYS[1])", 1, key
        ) == ["1", "2", "3"]
        assert writer.call("MULTI") == "OK"
        assert writer.call("SORT_RO", key) == "QUEUED"
        assert writer.call("EXEC") == [["1", "2", "3"]]
        if mode == "single":
            assert (
                writer.call("MSET", "weight_1", 3, "weight_2", 1, "weight_3", 2) == "OK"
            )
            assert writer.call("SORT_RO", key, "BY", "weight_*") == ["2", "3", "1"]
            assert writer.call("MULTI") == "OK"
            assert writer.call("SET", "weight_2", 4) == "QUEUED"
            assert writer.call("SORT_RO", key, "BY", "weight_*") == "QUEUED"
            assert writer.call("EXEC") == ["OK", ["3", "1", "2"]]
            script = "return redis.call('SORT_RO',KEYS[1],'BY','weight_*','GET','#')"
            assert writer.call(
                "EVAL_RO", script, 4, key, "weight_1", "weight_2", "weight_3"
            ) == ["3", "1", "2"]
            code = "#!lua name=sortlib\nredis.register_function{function_name='sorted', callback=function(keys,args) return redis.call('SORT_RO',keys[1],'BY','weight_*','GET','#') end, flags={'no-writes'}}"
            assert writer.call("FUNCTION", "LOAD", code) == "sortlib"
            assert writer.call(
                "FCALL_RO", "sorted", 4, key, "weight_1", "weight_2", "weight_3"
            ) == ["3", "1", "2"]
            assert writer.call("SELECT", 0) == "OK"
            assert writer.call("SORT_RO", key) == []
            assert writer.call("SELECT", db) == "OK"
        else:
            rejects(
                writer,
                ("SORT_RO", key, "GET", "#"),
                "GET option of SORT denied in Cluster mode",
            )
            rejects(
                writer,
                ("SORT_RO", key, "BY", "{sort}weight_*"),
                "BY option of SORT denied in Cluster mode",
            )
        reader = Client(target, readonly=mode == "cluster")
        try:
            assert reader.call("SELECT", db) == "OK"
            H.wait_until(
                "replica sortable population",
                30,
                lambda: reader.call("SORT_RO", key) == ["1", "2", "3"],
            )
        finally:
            reader.close()


def persistence(client):
    return dict(
        line.split(":", 1)
        for line in client.call("INFO", "persistence").splitlines()
        if ":" in line
    )


def backup_finished(client):
    info = persistence(client)
    # Scheduling can become an active job between commands; inspect one reply.
    return info["rdb_bgsave_in_progress"] == "0" and info["rdb_bgsave_scheduled"] == "0"


def saving(root, mode):
    hold = root / f"save-{mode}.hold"
    with (
        reference(root, mode) as redis,
        pair(
            root,
            f"save-{mode}",
            client_mode=mode,
            source_faults={"LAVIK_RDB_OUTPUT_HOLD_FILE": str(hold)},
            source_extra_args=("--dbfilename", "dump.rdb"),
        ) as (meta, source, target, writer),
    ):
        output = Path(source.workdir)
        ready(meta)
        assert writer.call("LASTSAVE") > 0
        for command in (
            ("SAVE", "extra"),
            ("BGSAVE", "invalid"),
            ("BGSAVE", "SCHEDULE", "extra"),
            ("LASTSAVE", "extra"),
        ):
            compare(writer, redis, command)
        assert persistence(writer)["rdb_last_bgsave_status"] == "ok"
        assert persistence(writer)["rdb_last_bgsave_time_sec"] == "-1"
        compare(writer, redis, ("SAVE",))
        assert (
            persistence(writer)["rdb_last_bgsave_time_sec"]
            == persistence(redis)["rdb_last_bgsave_time_sec"]
            == "-1"
        )
        code = "#!lua name=saved\nredis.register_function{function_name='saved_get', callback=function(keys,args) return redis.call('GET',keys[1]) end, flags={'no-writes'}}"
        assert writer.call("FUNCTION", "LOAD", code) == "saved"
        for db in (0, 15) if mode == "single" else (0,):
            assert writer.call("SELECT", db) == "OK"
            assert writer.call("SET", "{save}key", f"db-{db}") == "OK"
            assert writer.call("SET", "{save}ttl", "expiring", "EX", 3600) == "OK"
            assert writer.call("RPUSH", "{save}list", "a", "b") == 2
            assert writer.call("HSET", "{save}hash", "field", "value") == 1
            assert writer.call("SADD", "{save}set", "member") == 1
            assert writer.call("ZADD", "{save}zset", 1, "member") == 1
            assert writer.call("XADD", "{save}stream", "1-0", "field", "value") == "1-0"
        replica = Client(target, readonly=mode == "cluster")
        try:
            assert replica.call("SELECT", 15 if mode == "single" else 0) == "OK"
            H.wait_until(
                "replica backup population ready",
                30,
                lambda: replica.call("XRANGE", "{save}stream", "-", "+")
                == [["1-0", ["field", "value"]]],
            )
            assert replica.call("SAVE") == "OK"
            restore(root, Path(target.workdir), mode, "replica")
        finally:
            replica.close()
        assert writer.call("MULTI") == "OK"
        rejects(writer, ("SAVE",), "not allowed")
        rejects(writer, ("EXEC",), "EXECABORT")
        for cmd in ("SAVE", "BGSAVE"):
            rejects(writer, ("EVAL", f"return redis.call('{cmd}')", 0), "not allowed")
        assert (
            writer.call(
                "FUNCTION",
                "LOAD",
                "#!lua name=save_context\nredis.register_function('save_context', function(keys,args) return redis.call(args[1]) end)",
            )
            == "save_context"
        )
        for cmd in ("SAVE", "BGSAVE"):
            rejects(writer, ("FCALL", "save_context", 0, cmd), "not allowed")
        hold.touch()
        try:
            assert writer.call("BGSAVE") == "Background saving started"
            assert persistence(writer)["rdb_bgsave_in_progress"] == "1"
            rejects(writer, ("BGSAVE",), "already in progress")
            rejects(writer, ("BGSAVE", "SCHEDULE"), "already in progress")
            rejects(writer, ("SAVE",), "already in progress")
        finally:
            hold.unlink(missing_ok=True)
        H.wait_until(
            "background save complete",
            30,
            lambda: persistence(writer)["rdb_bgsave_in_progress"] == "0",
        )
        assert persistence(writer)["rdb_last_bgsave_status"] == "ok"
        restore(root, output, mode, "background")
        assert writer.call("MULTI") == "OK"
        assert writer.call("SET", "{save}exec", "before") == "QUEUED"
        assert writer.call("BGSAVE") == "QUEUED"
        assert writer.call("SET", "{save}exec", "after") == "QUEUED"
        assert writer.call("LASTSAVE") == "QUEUED"
        result = writer.call("EXEC")
        assert result[:3] == ["OK", "Background saving scheduled", "OK"], result
        H.wait_until(
            "EXEC save complete",
            30,
            lambda: backup_finished(writer),
        )
        restore(root, output, mode, "exec", expect_exec=True)
        background_duration = persistence(writer)["rdb_last_bgsave_time_sec"]
        hold.touch()
        marker = "fault pause reached: LAVIK_RDB_OUTPUT_HOLD_FILE"
        previous_holds = Path(source.log_path).read_text().count(marker)
        peer = Client(source)
        try:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                save = pool.submit(writer.call, "SAVE")
                try:
                    H.wait_until(
                        "SAVE output hold",
                        15,
                        lambda: Path(source.log_path).read_text().count(marker)
                        > previous_holds,
                    )
                    ping = pool.submit(peer.call, "PING")
                    time.sleep(0.2)
                    assert not save.done()
                    assert not ping.done(), "SAVE must suspend other client commands"
                finally:
                    hold.unlink(missing_ok=True)
                assert save.result(timeout=30) == "OK"
                assert ping.result(timeout=30) == "PONG"
        finally:
            hold.unlink(missing_ok=True)
            peer.close()
        assert persistence(writer)["rdb_last_bgsave_time_sec"] == background_duration
        restore(root, output, mode, "synchronous", expect_exec=True)
        previous = (output / "dump.rdb").read_bytes()
        saved = writer.call("LASTSAVE")
        output.chmod(0o500)
        try:
            rejects(writer, ("SAVE",), "ERR")
            assert persistence(writer)["rdb_last_bgsave_status"] == "ok"
            assert writer.call("BGSAVE") == "Background saving started"
            H.wait_until(
                "failed BGSAVE observed",
                15,
                lambda: persistence(writer)["rdb_bgsave_in_progress"] == "0",
            )
            assert persistence(writer)["rdb_last_bgsave_status"] == "err"
            assert writer.call("LASTSAVE") == saved
            assert (output / "dump.rdb").read_bytes() == previous
        finally:
            output.chmod(0o700)
        assert writer.call("SAVE") == "OK"
        assert persistence(writer)["rdb_last_bgsave_status"] == "ok"


def pattern_exec_pressure(root):
    # FULL publisher credit is deliberately smaller than the combined requests.
    # A pattern EXEC must reserve credit before closing DB/order admission.
    key = next(
        f"credit-{i}" for i in range(1000000) if C.redis_slot(f"credit-{i}") == 0
    )

    def seed(client):
        assert client.call("RPUSH", key, "2", "1") == 2
        assert client.call("MSET", "weight_1", 1, "weight_2", 2) == "OK"

    with pair(
        root,
        "pattern-credit",
        client_mode="single",
        source_workers=1,
        seed=seed,
        require_seed_before_full=True,
        source_extra_args=("--replication-publish-queue-mb-per-worker", "1"),
        source_faults={"LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "5000"},
    ) as (_, source, target, _):
        H.wait_until(
            "FULL holds publisher credit",
            15,
            lambda: "paused full sync after acknowledged handoff partition 0"
            in Path(source.log_path).read_text(),
        )
        clients = [Client(source) for _ in range(4)]
        value = "v" * 300000

        def work(index):
            client = clients[index]
            for iteration in range(8):
                if index == 0:
                    assert client.call("MULTI") == "OK"
                    assert client.call("SET", key + "-value", value) == "QUEUED"
                    assert client.call("SORT_RO", key, "BY", "weight_*") == "QUEUED"
                    assert client.call("EXEC") == ["OK", ["1", "2"]]
                else:
                    assert client.call("SET", key + f"-{index}", value) == "OK"

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                pending = [pool.submit(work, index) for index in range(4)]
                for result in pending:
                    result.result(timeout=25)
            reader = Client(target)
            try:
                H.wait_until(
                    "FULL completes after pattern transactions",
                    30,
                    lambda: reader.call("GET", key + "-value") == value,
                )
            finally:
                reader.close()
        finally:
            for client in clients:
                client.close()


def pattern_exec_full_cut(root):
    hold = root / "pattern-full-cut.hold"
    hold.touch()

    def seed(client):
        assert client.call("RPUSH", "ids", "1", "2") == 2

    with pair(
        root,
        "pattern-full-cut",
        client_mode="single",
        source_workers=1,
        seed=seed,
        require_seed_before_full=True,
        source_extra_args=("--replication-publish-queue-mb-per-worker", "1"),
        source_faults={"LAVIK_FULL_SOURCE_CUT_HOLD_FILE": str(hold)},
    ) as (_, source, target, writer):
        try:
            reached(source, "LAVIK_FULL_SOURCE_CUT_HOLD_FILE")
            # The reservation exceeds the queue waterline; retaining it while
            # waiting for FULL's closed DB gates leaves no room for its fence.
            assert writer.call("MULTI") == "OK"
            assert writer.call("SET", "large", "v" * 1200000) == "QUEUED"
            assert writer.call("SORT_RO", "ids", "BY", "weight_*") == "QUEUED"
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(writer.call, "EXEC")
                try:
                    time.sleep(0.2)
                    assert not pending.done()
                finally:
                    hold.unlink(missing_ok=True)
                assert pending.result(timeout=15) == ["OK", ["1", "2"]]
            reader = Client(target)
            try:
                H.wait_until(
                    "FULL cut and deferred EXEC reach replica",
                    30,
                    lambda: reader.call("STRLEN", "large") == 1200000,
                )
            finally:
                reader.close()
        finally:
            hold.unlink(missing_ok=True)


def save_drains_exec(root, mode):
    # SAVE must drain the admitted EXEC without making its LASTSAVE wait on
    # that same SAVE. Both holds are real production admission boundaries.
    delete_hold = root / f"exec-{mode}.hold"
    cut_hold = root / f"cut-{mode}.hold"
    with pair(
        root,
        f"exec-drain-{mode}",
        client_mode=mode,
        source_faults={
            "LAVIK_EXEC_DELETE_HOLD_KEY": "{drain}key",
            "LAVIK_EXEC_DELETE_HOLD_FILE": str(delete_hold),
            "LAVIK_BACKUP_CUT_HOLD_FILE": str(cut_hold),
        },
    ) as (meta, source, _, writer):
        ready(meta)
        saver = Client(source)
        try:
            assert writer.call("SET", "{drain}key", "old") == "OK"
            assert writer.call("MULTI") == "OK"
            assert writer.call("DEL", "{drain}key") == "QUEUED"
            assert writer.call("LASTSAVE") == "QUEUED"
            delete_hold.touch()
            cut_hold.touch()
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                executing = pool.submit(writer.call, "EXEC")
                saving = None
                try:
                    reached(source, "LAVIK_EXEC_DELETE_HOLD_FILE")
                    saving = pool.submit(saver.call, "SAVE")
                    H.wait_until(
                        "SAVE closed gates around admitted EXEC",
                        10,
                        lambda: "backup test checkpoint: database gates closed"
                        in Path(source.log_path).read_text(),
                    )
                    cut_hold.unlink()
                    delete_hold.unlink()
                    result = executing.result(timeout=10)
                    assert result[0] == 1 and result[1] > 0, result
                    assert saving.result(timeout=10) == "OK"
                finally:
                    cut_hold.unlink(missing_ok=True)
                    delete_hold.unlink(missing_ok=True)
        finally:
            saver.close()


def restore_key(root, source, name, key, value):
    directory = root / name
    directory.mkdir()
    shutil.copyfile(Path(source.workdir) / "dump.rdb", directory / "dump.rdb")
    with process(REDIS, directory, "restored", redis=True) as (client, _, _):
        assert client.call("GET", key) == value


def revoked_backup(root, mode):
    hold = root / f"revoke-{mode}.hold"
    with pair(
        root,
        f"revoke-{mode}",
        client_mode=mode,
        source_faults={"LAVIK_RDB_OUTPUT_HOLD_FILE": str(hold)},
    ) as (meta, source, _, writer):
        ready(meta)
        assert writer.call("SET", "backup-key", "before-revoke") == "OK"
        hold.touch()
        try:
            assert writer.call("BGSAVE") == "Background saving started"
            reached(source, "LAVIK_RDB_OUTPUT_HOLD_FILE")
            expire(meta, source, mode)
            hold.unlink()
            H.wait_until(
                "backup finishes after authority loss",
                15,
                lambda: Path(source.workdir, "dump.rdb").exists(),
            )
            restore_key(
                root, source, f"restored-revoke-{mode}", "backup-key", "before-revoke"
            )
        finally:
            hold.unlink(missing_ok=True)
            meta.resume()


def revoked_save(root, mode):
    hold = root / f"revoke-save-{mode}.hold"
    with pair(
        root,
        f"revoke-save-{mode}",
        client_mode=mode,
        source_faults={"LAVIK_RDB_OUTPUT_HOLD_FILE": str(hold)},
    ) as (meta, source, _, writer):
        ready(meta)
        assert writer.call("SET", "backup-key", "before-revoke") == "OK"
        hold.touch()
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(writer.call, "SAVE")
                try:
                    reached(source, "LAVIK_RDB_OUTPUT_HOLD_FILE")
                    meta.pause()
                    # Socket retirement is observable even though SAVE suspends
                    # ordinary commands. The already captured job still drains.
                    H.wait_until(
                        "SAVE client retired after lease expiry", 10, pending.done
                    )
                    try:
                        pending.result()
                    except H.Failure as error:
                        assert "closed its Redis connection" in str(error), error
                    else:
                        raise AssertionError(
                            "SAVE replied before held output completed"
                        )
                finally:
                    hold.unlink(missing_ok=True)
            H.wait_until(
                "SAVE finishes after client retirement",
                15,
                lambda: Path(source.workdir, "dump.rdb").exists(),
            )
            restore_key(
                root,
                source,
                f"restored-revoke-save-{mode}",
                "backup-key",
                "before-revoke",
            )
        finally:
            hold.unlink(missing_ok=True)
            meta.resume()


def failover_backup(root, mode):
    hold = root / f"failover-{mode}.hold"
    fixture = F.FailoverFixture(
        C.META,
        C.DATA,
        C.CTL,
        str(root / f"failover-{mode}"),
        False,
        pause_after_begin_ms=1000,
        data_workers=2,
        client_mode=mode,
    )
    owner = fixture.by_id[F.OWNER]
    owner.environment = {**os.environ, "LAVIK_RDB_OUTPUT_HOLD_FILE": str(hold)}
    writer = None
    try:
        fixture.start_created()
        fixture.seed_and_wait_for_replicas(
            "backup-key", "before-failover", (F.CANDIDATE, F.FOLLOWER)
        )
        writer = Client(owner)
        hold.touch()
        assert writer.call("BGSAVE") == "Background saving started"
        reached(owner, "LAVIK_RDB_OUTPUT_HOLD_FILE")
        fixture.submit_failover()
        assert fixture.wait_post_begin_pause()
        H.wait_until(
            "controlled pause during backup",
            15,
            lambda: F.redis_error(owner, ["SET", "pause-probe", "x"], timeout=0.1)
            == "TRYAGAIN Failover in progress",
        )
        # Finite output delay must not turn Controlled Pause into cancellation.
        hold.unlink()
        begin = F.require_unique_failover_event(
            fixture.metas, "begin", "controlled", loss="none"
        )
        F.wait_serving_owner(
            fixture, begin["candidate"], fixture.data_nodes, timeout=60
        )
        H.wait_until(
            "backup published across controlled failover",
            15,
            lambda: Path(owner.workdir, "dump.rdb").exists(),
        )
        restore_key(
            root, owner, f"restored-failover-{mode}", "backup-key", "before-failover"
        )
        assert (
            F.redis_call(
                fixture.by_id[begin["candidate"]], ["SET", "after-failover", "ok"]
            )
            == "OK"
        )
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
    global REDIS
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    REDIS = os.path.abspath(sys.argv[5])
    version = subprocess.check_output([REDIS, "--version"], text=True)
    assert "v=7.2.14 " in version, version
    H.log(version.strip())
    H.set_tag("managed-sort-save")
    with tempfile.TemporaryDirectory(
        prefix="sort-save-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        if "--save" in sys.argv:
            pattern_exec_pressure(Path(directory))
            pattern_exec_full_cut(Path(directory))
        for mode in ("single", "cluster"):
            if "--save" in sys.argv:
                saving(Path(directory), mode)
                save_drains_exec(Path(directory), mode)
                revoked_backup(Path(directory), mode)
                revoked_save(Path(directory), mode)
                failover_backup(Path(directory), mode)
            else:
                sorting(Path(directory), mode)
    H.log("PASS")


if __name__ == "__main__":
    main()
