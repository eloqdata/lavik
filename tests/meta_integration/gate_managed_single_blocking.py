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

"""Blocking commands in Meta-managed Single mode.

BLPOP waiters register and wake across workers and keep Redis timeout and
CLIENT UNBLOCK semantics. A Controlled Pause rejects fresh blocking
re-admission with TRYAGAIN while a pre-pause waiter keeps its deadline, and
cutover wakes the old owner's dormant waiter into a fenced re-admission that
never consumes. Cluster mode keeps CROSSSLOT as the counter-example Single
waives.
"""

import concurrent.futures
import os
from pathlib import Path
import sys
import tempfile
import time

import gate_failover as F
from gate_native_replication import C, H, Client, pair, ready, rejects


def blocked_call(node, *args):
    """One blocked command on its own short-lived Client connection."""
    client = Client(node)
    try:
        return client.call(*args)
    finally:
        client.close()


def blocked_blpop_lines(listing):
    # CLIENT LIST prints one line per connection; a blocked waiter shows
    # flags=b. (The cmd= field is not reliable for the blocked command.)
    return sum(1 for line in listing.splitlines() if " flags=b " in line)


def cross_worker_blocking(root):
    # Acceptance: a multi-key BLPOP registers its wait across workers and a
    # push landing on another worker wakes it; CLIENT UNBLOCK error and
    # timeout-as-nil semantics hold in Meta-managed Single mode.
    with pair(root, "single-blocking", client_mode="single") as (
        meta,
        source,
        _,
        writer,
    ):
        ready(meta)
        # At two source workers, "xa" (slot 15735) and "{b}k" (slot 3300)
        # map to different workers, so the LPUSH wakes the wait cross-worker.
        assert C.redis_slot("xa") % 2 != C.redis_slot("{b}k") % 2
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(blocked_call, source, "BLPOP", "xa", "{b}k", "zzz", 0)
            H.wait_until(
                "cross-worker BLPOP registered",
                20,
                lambda: blocked_blpop_lines(writer.call("CLIENT", "LIST")) >= 1,
            )
            assert not pending.done()
            assert writer.call("LPUSH", "{b}k", "elem") == 1
            assert pending.result(timeout=30) == ["{b}k", "elem"]
        # CLIENT UNBLOCK ERROR wakes the waiter with an error reply. The id
        # belongs to the blocked connection, so it needs its own Client.
        blocked = Client(source)
        try:
            client_id = blocked.call("CLIENT", "ID")

            def unblock_target_blocked():
                listing = writer.call("CLIENT", "LIST", "ID", client_id)
                return " flags=b " in listing

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(blocked.call, "BLPOP", "never-pushed", 0)
                H.wait_until(
                    "CLIENT UNBLOCK target registered", 20, unblock_target_blocked
                )
                assert not pending.done()
                assert writer.call("CLIENT", "UNBLOCK", client_id, "ERROR") == 1
                try:
                    reply = pending.result(timeout=30)
                except H.Failure as error:
                    assert "UNBLOCKED" in str(error), error
                else:
                    raise AssertionError(f"CLIENT UNBLOCK ERROR returned {reply!r}")
        finally:
            blocked.close()
        started = time.monotonic()
        assert writer.call("BLPOP", "empty-list", 1) == []
        # A nil ahead of the server deadline would mean the wait never
        # engaged; only the far later socket timeout bounds it above.
        assert time.monotonic() - started >= 0.9
        # The sorted-set and movable-key blocking families share the same
        # admission and waiter path; one wake each proves they are admitted.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(blocked_call, source, "BZPOPMIN", "zs-block", 0)
            H.wait_until(
                "BZPOPMIN registered",
                20,
                lambda: blocked_blpop_lines(writer.call("CLIENT", "LIST")) >= 1,
            )
            assert writer.call("ZADD", "zs-block", 2.5, "member") == 1
            assert pending.result(timeout=30) == ["zs-block", "member", "2.5"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(
                blocked_call, source, "BLMPOP", 0, 2, "ml-a", "ml-b", "LEFT"
            )
            H.wait_until(
                "BLMPOP registered",
                20,
                lambda: blocked_blpop_lines(writer.call("CLIENT", "LIST")) >= 1,
            )
            assert writer.call("LPUSH", "ml-b", "elem") == 1
            assert pending.result(timeout=30) == ["ml-b", ["elem"]]


def controlled_pause_and_cutover(root):
    # Acceptance: timeout semantics survive Controlled Pause, a fresh blocking
    # re-admission is rejected TRYAGAIN during the pause, the fenced old owner
    # never consumes after cutover, and an idle timeout=0 waiter does not
    # block the failover drain.
    fixture = F.FailoverFixture(
        C.META,
        C.DATA,
        C.CTL,
        str(root / "pause-cutover"),
        False,
        pause_after_begin_ms=8000,
        data_workers=2,
        client_mode="single",
    )
    try:
        fixture.start_created(add_follower=True)
        owner = fixture.by_id[F.OWNER]
        assert F.redis_call(owner, ["SET", "pause-copy", "source"], db=1) == "OK"
        # Seed nothing: both lists stay empty so every waiter must block.
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            paused_waiter = pool.submit(blocked_call, owner, "BLPOP", "pause-list", 2)
            moved_waiter = pool.submit(blocked_call, owner, "BLPOP", "moved-list", 0)
            H.wait_until(
                "both Single waiters registered",
                20,
                lambda: blocked_blpop_lines(F.redis_call(owner, ["CLIENT", "LIST"]))
                >= 2,
            )
            assert not paused_waiter.done()
            assert not moved_waiter.done()
            operation_id = fixture.submit_failover()
            successor = None
            if fixture.wait_post_begin_pause():
                begin = F.require_unique_failover_event(
                    fixture.metas, "begin", "controlled", loss="none"
                )
                successor = begin["candidate"]
                # The waiter registered before the pause keeps its own
                # deadline and times out with nil during Controlled Pause.
                assert paused_waiter.result(timeout=30) == []

                def paused_blocking_rejected():
                    try:
                        return F.redis_error(
                            owner, ["BLPOP", "pause-list", "1"]
                        ).startswith("TRYAGAIN")
                    except H.Failure:
                        return False

                H.wait_until(
                    "blocking re-admission enters committed mutation pause",
                    7,
                    paused_blocking_rejected,
                )
                copier = Client(owner)
                try:
                    copier.call("SELECT", 1)
                    rejects(
                        copier,
                        ("COPY", "pause-copy", "pause-copy", "DB", 15),
                        "TRYAGAIN",
                    )
                finally:
                    copier.close()
                # The pause fences new admission; it does not cancel the
                # registered timeout=0 waiter, which stays dormant.
                assert not moved_waiter.done()
            else:
                # Without the deterministic pause, cutover can race the
                # two-second deadline; nil and the role-change fence are both
                # terminal, and neither consumes an element.
                try:
                    assert paused_waiter.result(timeout=30) == []
                except H.Failure as error:
                    assert any(
                        token in str(error)
                        for token in ("TRYAGAIN", "MASTERDOWN", "READONLY", "LOADING")
                    ), error
            if successor is None:
                begin = {}

                def begin_committed():
                    nonlocal begin
                    try:
                        begin = F.require_unique_failover_event(
                            fixture.metas, "begin", "controlled", loss="none"
                        )
                    except H.Failure:
                        return False
                    return True

                H.wait_until("controlled Begin committed", 30, begin_committed)
                successor = begin["candidate"]
            F.wait_owner(fixture, successor, fixture.data_nodes)
            F.wait_operation(
                fixture,
                operation_id,
                "OK completed failover-completed",
                "controlled operation reaches its durable terminal result",
            )
            # The failover drained and cut over while a timeout=0 waiter was
            # dormant, proving idle waiters do not hold the assignment drain.
            # The old owner's role change then wakes that waiter, and its
            # re-admission must fail: the fenced old owner never consumes.
            try:
                reply = moved_waiter.result(timeout=60)
            except H.Failure as error:
                # MASTERDOWN/READONLY/TRYAGAIN come from the authority
                # re-admission; LOADING is the population fence when the old
                # owner rebuilds after cutover. All are terminal; none
                # consumes an element.
                assert any(
                    token in str(error)
                    for token in ("MASTERDOWN", "READONLY", "TRYAGAIN", "LOADING")
                ), error
            else:
                raise AssertionError(f"fenced old owner consumed or replied {reply!r}")
            new_owner = fixture.by_id[successor]
            assert F.redis_call(new_owner, ["LPUSH", "moved-list", "elem"]) == 1
            assert F.redis_call(new_owner, ["BLPOP", "moved-list", "1"]) == [
                "moved-list",
                "elem",
            ]
        fixture.require_expected_processes_alive()
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        fixture.force_kill()


def uncontrolled_crash_failover(root):
    # Acceptance: with an active blocking connection, control loss (lease
    # expiry) leaves the dormant waiter honestly unanswered, and an
    # uncontrolled failover after an owner crash terminates the stale waiter
    # without consuming elements while the new primary serves.
    with pair(root, "single-crash", client_mode="single") as (
        meta,
        source,
        target,
        writer,
    ):
        ready(meta)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(blocked_call, source, "BLPOP", "crash-list", 0)
            H.wait_until(
                "crash waiter registered",
                20,
                lambda: blocked_blpop_lines(writer.call("CLIENT", "LIST")) >= 1,
            )
            assert not pending.done()
            # Lease expiry (Meta paused past the finite lease) must not
            # fabricate a waiter reply: dormant revocation is passive, the
            # waiter stays blocked and consumes nothing.
            meta.pause()
            time.sleep(3)
            assert not pending.done()
            meta.resume()
            # Crash the primary with the waiter attached. The connection dies
            # with the process: the waiter terminates honestly, never with a
            # consumed element.
            source.force_kill()
            try:
                reply = pending.result(timeout=30)
            except (OSError, H.Failure):
                pass
            else:
                raise AssertionError(
                    f"crashed owner's waiter unexpectedly replied {reply!r}"
                )
            promoted = Client(target)
            try:
                H.wait_until(
                    "replica promoted and serving writes",
                    60,
                    lambda: promoted.call("LPUSH", "crash-list", "elem") == 1,
                )
                assert promoted.call("BLPOP", "crash-list", 1) == ["crash-list", "elem"]
            finally:
                promoted.close()


def cluster_counter_examples(root):
    # Acceptance: Cluster mode keeps CROSSSLOT for cross-slot multi-key
    # commands (the counter-example Single waives) while a single-key BLPOP
    # still serves normally.
    with pair(root, "cluster-counter") as (meta, _, _target, writer):
        ready(meta)
        assert writer.call("SELECT", 0) == "OK"
        rejects(writer, ("SELECT", 15), "SELECT is not allowed in cluster mode")
        assert writer.call("SET", "{copy}source", "value") == "OK"
        rejects(
            writer,
            ("COPY", "{copy}source", "{copy}target", "DB", 15),
            "Copying to another database is not allowed in cluster mode",
        )
        # "{other}key-b" hashes by "other" (slot 11361) against "key-a"
        # (slot 6672): two distinct slots make CROSSSLOT deterministic.
        assert C.redis_slot("key-a") != C.redis_slot("{other}key-b")
        rejects(writer, ("MGET", "key-a", "{other}key-b"), "CROSSSLOT")
        rejects(writer, ("MSET", "key-a", "1", "{other}key-b", "2"), "CROSSSLOT")
        assert writer.call("LPUSH", "{single}pop", "elem") == 1
        assert writer.call("BLPOP", "{single}pop", 1) == ["{single}pop", "elem"]


def main():
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    H.set_tag("managed-single-blocking")
    with tempfile.TemporaryDirectory(
        prefix="lavik-managed-single-blocking-",
        dir=os.environ.get("LAVIK_TEST_DATA_DIR"),
    ) as directory:
        root = Path(directory)
        cross_worker_blocking(root)
        controlled_pause_and_cutover(root)
        uncontrolled_crash_failover(root)
        cluster_counter_examples(root)
    H.log("PASS")


if __name__ == "__main__":
    main()
