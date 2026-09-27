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

"""Export cancellation is independent of ordinary-client and native sessions."""

import os
import concurrent.futures
import socket
import time
from pathlib import Path
import sys
import tempfile
import redis
import redis_export_protocol_e2e as P
import gate_native_replication as N
import gate_failover as F
from gate_managed_composite_faults import expire


def closed(export):
    export.sock.settimeout(15)
    while export.sock.recv(65536):
        pass


def rejected(port):
    with socket.create_connection(("127.0.0.1", port), 5) as probe:
        probe.settimeout(5)
        with probe.makefile("rb") as reader:
            probe.sendall(P.wire("REPLCONF", "capa", "eof"))
            assert reader.readline() == b"+OK\r\n"
            probe.sendall(P.wire("PSYNC", "?", "-1"))
            reply = reader.readline()
            assert not reply or reply.startswith(b"-"), reply


def expired(root, mode, ack):
    with N.pair(
        root,
        f"lease-{mode}-{ack}",
        client_mode=mode,
        source_faults=({"LAVIK_RDB_SCAN_PAUSE_MS": "5000"} if ack == "rdb" else {}),
    ) as (
        meta,
        source,
        target,
        _,
    ):
        N.ready(meta)
        src = redis.Redis(port=source.redis_port, socket_timeout=5)
        rejected(target.redis_port)
        history = src.info("replication")["master_replid"]
        export = P.Export(source.redis_port)
        try:
            if ack != "rdb":
                export.rdb()
            if ack is True:
                export.ack()
            expire(meta, source, mode)
            closed(export)
            N.H.wait_until(
                "export resources released after lease expiry",
                15,
                lambda: src.info("replication")["redis_export_active"] == 0,
            )
            rejected(source.redis_port)
            N.H.wait_until(
                "rejected admission releases export slot",
                5,
                lambda: src.info("replication")["redis_export_active"] == 0,
            )
        finally:
            export.close()
            meta.resume()
        N.ready(meta)
        assert src.info("replication")["master_replid"] == history
        assert src.set("{export}:after-expiry", "resumed")
        again = P.Export(source.redis_port)
        try:
            assert b"resumed" in again.rdb()
            again.ack()
        finally:
            again.close()
            src.close()


def controlled(root, mode, slow=False, abort=False):
    fixture = F.FailoverFixture(
        N.C.META,
        N.C.DATA,
        N.C.CTL,
        str(root / f"controlled-{mode}-{slow}-{abort}"),
        True,
        pause_after_begin_ms=8000,
        data_workers=2,
        client_mode=mode,
    )
    exports = []
    pool = concurrent.futures.ThreadPoolExecutor()
    writing = None
    try:
        fixture.start_created()
        fixture.seed_and_wait_for_replicas(
            "{export}:key", "before", (F.CANDIDATE, F.FOLLOWER)
        )
        owner = fixture.by_id[F.OWNER]
        if slow:
            config = redis.Redis(port=owner.redis_port, socket_timeout=5)
            config.config_set("repl-backlog-size", "16mb")
            config.close()
        export = P.Export(owner.redis_port)
        exports.append(export)
        export.rdb()
        export.ack()
        src = redis.Redis(port=owner.redis_port, socket_timeout=5)
        N.H.wait_until(
            "export online before pause",
            10,
            lambda: src.info("replication")["redis_export_phase"] == "online",
        )
        session = src.info("replication")["redis_export_session_id"]
        if slow:

            def writes():
                writer = redis.Redis(port=owner.redis_port, socket_timeout=30)
                try:
                    for i in range(100):
                        writer.set("{export}:slow", bytes([i]) * (1024 * 1024))
                except redis.ResponseError as error:
                    assert (
                        "Failover" in str(error)
                        or "CLUSTERDOWN" in str(error)
                        or "MASTERDOWN" in str(error)
                    ), error
                finally:
                    writer.close()

            writing = pool.submit(writes)
            time.sleep(2)
            assert not writing.done(), "exporter did not apply backpressure"
        operation = fixture.submit_failover()
        assert fixture.wait_post_begin_pause()
        N.H.wait_until(
            "controlled write pause",
            10,
            lambda: F.redis_error(owner, ["SET", "pause", "x"], timeout=0.1)
            == "TRYAGAIN Failover in progress",
        )
        if abort:
            begin = F.require_unique_failover_event(
                fixture.metas, "begin", "controlled", loss="none"
            )
            fixture.by_id[begin["candidate"]].force_kill()
            F.wait_operation(
                fixture,
                operation,
                "OK aborted controlled failover candidate became unavailable",
                "cancelled pause restores original Owner",
            )

            def resumed():
                try:
                    return src.set("{export}:abort", "resumed")
                except redis.ResponseError:
                    return False

            N.H.wait_until(
                "writes resume after cancelled pause",
                15,
                resumed,
            )
            assert src.info("replication")["redis_export_session_id"] == session
            data = b""
            while b"resumed" not in data:
                part = export.sock.recv(65536)
                assert part, "cancelled pause disconnected the valid export"
                data += part
            src.close()
            return
        if slow:
            writing.result(timeout=30)
        else:
            assert src.info("replication")["redis_export_session_id"] == session
            export.close()
            exports.remove(export)
            N.H.wait_until(
                "old export joined in pause",
                10,
                lambda: src.info("replication")["redis_export_active"] == 0,
            )
            # Paused but still authorized Primary accepts a new FULLRESYNC.
            export = P.Export(owner.redis_port)
            exports.append(export)
            export.rdb()
            export.ack()
        begin = F.require_unique_failover_event(
            fixture.metas, "begin", "controlled", loss="none"
        )
        F.wait_serving_owner(
            fixture, begin["candidate"], fixture.data_nodes, timeout=60
        )
        closed(export)
        successor = P.Export(fixture.by_id[begin["candidate"]].redis_port)
        exports.append(successor)
        assert b"before" in successor.rdb()
        successor.ack()
        src.close()
    except BaseException:
        fixture.dump_logs()
        raise
    finally:
        for export in exports:
            export.close()
        fixture.force_kill()
        pool.shutdown(wait=True)


def shake_failover(root, mode, uncontrolled):
    import redis_export_shake_e2e as E

    E.I.PASSWORD = ""
    fixture = F.FailoverFixture(
        N.C.META,
        N.C.DATA,
        N.C.CTL,
        str(root / f"shake-{mode}-{uncontrolled}"),
        True,
        pause_after_begin_ms=1000,
        data_workers=2,
        client_mode=mode,
    )
    shakes = []
    try:
        fixture.start_created()
        fixture.seed_and_wait_for_replicas(
            "{export}:failover", "complete", (F.CANDIDATE, F.FOLLOWER)
        )
        owner = fixture.by_id[F.OWNER]
        with E.S.process(
            E.I.REDIS, root / f"old-target-{uncontrolled}", "redis", redis=True
        ) as (_, dest, _):
            src = redis.Redis(port=owner.redis_port, socket_timeout=5)
            shake = E.Shake(root / f"old-shake-{uncontrolled}", owner.redis_port, dest)
            shakes.append(shake)
            E.drain(src, shake, owner.redis_port)
            E.compare(src, redis.Redis(port=dest))
            if uncontrolled:
                assert fixture.leader.put_automatic_uncontrolled_failover_policy(
                    2, suspect_after_ms=1000
                ).startswith("OK")
                owner.force_kill()
            else:
                fixture.submit_failover()
            N.H.wait_until(
                "RedisShake reports revoked source failure",
                30,
                lambda: shake.proc.poll() is not None,
            )
            assert shake.proc.returncode != 0

            def successor_ready():
                status = fixture.cluster_status(time.monotonic() + 5)
                return any(
                    g.get("owner_node_id") != F.OWNER and g.get("serving_ready")
                    for g in status["groups"]
                )

            N.H.wait_until("successor serves after export failure", 90, successor_ready)
            status = fixture.cluster_status(time.monotonic() + 5)
            successor_id = next(
                g["owner_node_id"] for g in status["groups"] if g.get("serving_ready")
            )
            successor = fixture.by_id[successor_id]
            src.close()
        # Discard the entire failed target, including its catalog. A new tool
        # and empty Redis prove this is a fresh FULL rather than continuation.
        with E.S.process(
            E.I.REDIS, root / f"new-target-{uncontrolled}", "redis", redis=True
        ) as (_, dest, _):
            src = redis.Redis(port=successor.redis_port, socket_timeout=5)
            shake = E.Shake(
                root / f"new-shake-{uncontrolled}", successor.redis_port, dest
            )
            shakes.append(shake)
            E.drain(src, shake, successor.redis_port)
            E.compare(src, redis.Redis(port=dest))
            assert src.get("{export}:failover") == b"complete"
            src.close()
    except BaseException:
        fixture.dump_logs()
        for shake in shakes:
            print((shake.root / "process.log").read_text(), file=sys.stderr)
        raise
    finally:
        for shake in shakes:
            shake.close()
        fixture.force_kill()


if __name__ == "__main__":
    import redis_export_shake_e2e as E

    N.C.DATA, N.C.META, N.C.CTL, E.I.REDIS, E.I.SHAKE, mode = sys.argv[1:]
    with tempfile.TemporaryDirectory(
        prefix="ex-ha-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as temp:
        root = Path(temp)
        for ack in ("rdb", False, True):
            expired(root, mode, ack)
        controlled(root, mode)
        controlled(root, mode, abort=True)
        controlled(root, mode, slow=True)
        for uncontrolled in (False, True):
            shake_failover(root, mode, uncontrolled)
    print("PASS export HA", mode)
