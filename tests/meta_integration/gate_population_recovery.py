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

"""Exercise restart eligibility and explicit operator recovery through real processes.

Assertions use Meta's public status/admin API and Redis commands. Fault sites
cut durable proof boundaries or hold startup recovery; they do not bypass
elections or leases.
"""

import argparse
from contextlib import contextmanager
import os
import re
import signal
import socket
import sys
import threading
import time
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gate_automatic_failover as A  # noqa: E402
import gate_failover as F  # noqa: E402
import harness as H  # noqa: E402
from gate_data_control import allocate_data_file  # noqa: E402

CASES = (
    "startup-barrier",
    "startup-stop",
    "clean-owner",
    "clean-replica",
    "crash-operator",
    "manual-fence",
    "before-publish",
    "after-publish",
    "before-consume",
    "after-consume",
    "incomplete-full",
    "eligible-candidate",
    "promoted-follower",
)


@contextmanager
def crash_at(point):
    old = os.environ.get("LAVIK_CRASH_POINT")
    try:
        if point is None:
            os.environ.pop("LAVIK_CRASH_POINT", None)
        else:
            os.environ["LAVIK_CRASH_POINT"] = point
        yield
    finally:
        if old is None:
            os.environ.pop("LAVIK_CRASH_POINT", None)
        else:
            os.environ["LAVIK_CRASH_POINT"] = old


def require_exit_at_fault(data):
    if data.proc.wait(timeout=30) != 86:
        raise H.Failure("armed recovery fault did not exit with code 86")
    data.force_kill()  # close the completed process's log descriptor


def restart(fixture, data, fault=None):
    data.seed = fixture.rediscover_leader(time.monotonic() + 5).data_control_endpoint
    with crash_at(fault):
        # Startup crash cuts may precede the metrics listener. The caller
        # awaits the distinct fault exit instead of mistaking it for timeout.
        data.start(wait_ready=fault is None)


class StartupProxy(H.Proxy):
    """Distinguish pre-storage bootstrap from the full control session."""

    def __init__(self, target_port):
        super().__init__("recovery-startup", target_port)
        self.connected = threading.Event()
        self.bootstrapped = threading.Event()

    def _on_accept(self, conn):
        # Peek without consuming bytes so the ordinary proxy forwards the
        # original frame. A TCP accept alone is expected before storage opens:
        # BootstrapHello (20) obtains mode without creating a Data session.
        # Treat any other prefix as control work, including malformed input,
        # so a protocol change cannot silently weaken the startup assertion.
        deadline = time.monotonic() + 3
        conn.settimeout(0.2)
        prefix = b""
        try:
            while len(prefix) < 8 and time.monotonic() < deadline:
                try:
                    prefix = conn.recv(8, socket.MSG_PEEK)
                except socket.timeout:
                    continue
                if not prefix:
                    break
                if len(prefix) < 8:
                    time.sleep(0.005)
        except OSError:
            conn.close()
            return
        conn.settimeout(None)
        if prefix == b"LVCP\x00\x01\x00\x14":
            self.bootstrapped.set()
        else:
            self.connected.set()
        super()._on_accept(conn)


def restart_at_recovery_barrier(fixture, data, workdir, *, stop=False):
    leader = fixture.rediscover_leader(time.monotonic() + 5)
    proxy = StartupProxy(int(leader.data_control_endpoint.rsplit(":", 1)[1]))
    barrier = os.path.join(workdir, "population-recovery-held")
    try:
        proxy.start()
        H.wait_until(
            "startup proxy is listening", 5, lambda: proxy._listener is not None
        )
        data.seed = proxy.endpoint
        with patch.dict(os.environ, {"LAVIK_RECOVERY_INSTALL_BARRIER_PATH": barrier}):
            data.start(wait_ready=False)
        H.wait_until(
            "population recovery reaches the install barrier",
            20,
            lambda: os.path.exists(barrier),
        )
        if not proxy.bootstrapped.is_set():
            raise H.Failure("population recovery began without mode bootstrap")
        # Keep recovery suspended across many control-client scheduler turns.
        # No full session may start, even before the install marker appears.
        if proxy.connected.wait(timeout=1):
            raise H.Failure("Meta session opened before population recovery completed")
        if data.proc.poll() is not None:
            raise H.Failure("Data exited while population recovery was held")
        if stop:
            log_offset = os.path.getsize(data.log_path)
            data.proc.send_signal(signal.SIGINT)
            # Startup has no replication shutdown monitor yet: the process
            # must exit unsuccessfully without certifying a clean checkpoint,
            # but the Meta startup wait must still quiesce without hanging.
            if data.proc.wait(timeout=20) != 1:
                raise H.Failure("interrupted startup did not fail closed")
            with open(data.log_path, encoding="utf-8") as log:
                log.seek(log_offset)
                shutdown_log = log.read()
            quiesced = "Meta control client quiesced before storage flush"
            if quiesced not in shutdown_log:
                raise H.Failure("Meta startup wait did not quiesce on shutdown")
            if "all storage buffers durably flushed" in shutdown_log:
                raise H.Failure("interrupted recovery published a clean checkpoint")
            if proxy.connected.is_set():
                raise H.Failure("Meta session opened during interrupted startup")
            return proxy
        os.unlink(barrier)
        H.wait_until(
            "Meta session opens after population recovery", 20, proxy.connected.is_set
        )
        return proxy
    except BaseException:
        proxy.close()
        raise
    finally:
        if os.path.exists(barrier):
            os.unlink(barrier)


def wait_serving(fixture, data, term):
    A.wait_group(
        fixture,
        f"recovered Owner serves at term {term}",
        lambda g: g.get("term") == str(term)
        and g.get("owner_node_id") == data.node_id
        and g.get("serving_ready"),
        timeout=60,
    )


def seed(data):
    # Multiple keys/flows, a tombstone and a grouped value
    # must all belong to the same recovered logical population.
    for i in range(32):
        if F.redis_call(data, ["SET", f"recovery:{i}", str(i)]) != "OK":
            raise H.Failure("seed write was rejected")
    F.redis_call(data, ["SET", "recovery:deleted", "gone"])
    F.redis_call(data, ["DEL", "recovery:deleted"])
    F.redis_call(data, ["HSET", "recovery:hash", "field", "v" * 20000])
    F.redis_call(data, ["MSET", "{recovery-tx}a", "alpha", "{recovery-tx}b", "beta"])
    F.redis_call(data, ["SET", "recovery:complete", "yes"])


def require_data(data):
    for i in range(32):
        if F.redis_call(data, ["GET", f"recovery:{i}"]) != str(i):
            raise H.Failure(f"recovered population lost key {i}")
    if F.redis_call(data, ["GET", "recovery:deleted"]) is not None:
        raise H.Failure("recovered population resurrected a deletion")
    if F.redis_call(data, ["HGET", "recovery:hash", "field"]) != "v" * 20000:
        raise H.Failure("recovered grouped value differs")
    if F.redis_call(data, ["MGET", "{recovery-tx}a", "{recovery-tx}b"]) != [
        "alpha",
        "beta",
    ]:
        raise H.Failure("recovered transaction differs")
    if F.redis_call(data, ["SET", "recovery:after", "accepted"]) != "OK":
        raise H.Failure("activated Owner rejected a write")


def wait_replica_cut(owner):
    # WAIT must follow a write on the same connection: its all-flow watermark
    # covers the earlier seed writes even when source workers apply separately.
    with socket.create_connection(F.endpoint(owner), timeout=15) as connection:
        connection.sendall(
            F.encode_resp(["SET", "recovery:barrier", "yes"])
            + F.encode_resp(["WAIT", "1", "10000"])
        )
        reader = connection.makefile("rb")
        if F.read_resp(reader) != "OK" or F.read_resp(reader) != 1:
            raise H.Failure("replica did not acknowledge the all-flow seed cut")


def wait_durable(data):
    # Crash recovery deliberately promises no acknowledged-write frontier.
    # Establish durable fixture data through the public durability counters,
    # rather than assuming a flush timer bounds asynchronous IO completion.
    def drained():
        info = F.redis_call(data, ["INFO", "stats"])
        fields = dict(line.split(":", 1) for line in info.splitlines() if ":" in line)
        return all(
            fields.get(name) == "0"
            for name in (
                "storage_dirty_staging_bytes",
                "storage_flushes_pending",
                "storage_tx_commits_pending",
            )
        )

    H.wait_until("seed data is durable before crash", 20, drained)


def require_fenced(fixture, data, term):
    # Metrics start before disk recovery completes; a fresh Meta observation
    # of the fence does not imply that Redis has opened its listener yet.
    def listener_ready():
        with socket.create_connection(F.endpoint(data), timeout=1):
            return True

    H.wait_until("restarted Redis listener is available", 20, listener_ready)
    A.wait_group(
        fixture,
        "restarted node has no serving authority",
        lambda g: g.get("term") == str(term) and not g.get("serving_ready"),
        timeout=40,
    )
    # Give several fresh candidate heartbeats a chance to drive selection.
    # An unknown frontier must never silently become an automatic candidate.
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if A.group_status(fixture, time.monotonic() + 5).get("serving_ready"):
            raise H.Failure("unproven population became automatically eligible")
        error = F.redis_error(data, ["SET", "recovery:forbidden", "bad"])
        if not any(word in error for word in ("LOADING", "CLUSTERDOWN", "TRYAGAIN")):
            raise H.Failure(f"unexpected fenced write result: {error}")
        time.sleep(0.1)


def promote(fixture, data):
    fixture.rediscover_leader(time.monotonic() + 5)
    command = f"promote {F.GROUP} --node {data.node_id}"
    missing_ack = fixture.leader.ctl(command)
    if not missing_ack.startswith("ERR promote expected:"):
        raise H.Failure(f"promote accepted missing data-loss consent: {missing_ack}")
    # Use the executable's direct-admin entry point, not local Data authority.
    receipt = F.run_command(
        [
            fixture.ctl,
            "--socket",
            fixture.leader.ctl_path,
            "promote",
            F.GROUP,
            "--node",
            data.node_id,
            "--accept-data-loss",
        ]
    )
    if (
        re.fullmatch(
            r"OK [1-9][0-9]* promote loss=unknown action=[0-9a-f]{32}\s*", receipt
        )
        is None
    ):
        raise H.Failure(f"operator recovery receipt is not exact: {receipt}")


def run(args, workdir):
    fixture = F.FailoverFixture(
        args.meta,
        args.data,
        args.ctl,
        os.path.join(workdir, args.case),
        True,
        pause_after_begin_ms=0,
    )
    owner = fixture.by_id[F.OWNER]
    owner.workers = 2
    replica = fixture.by_id[F.CANDIDATE]
    case = args.case
    startup_proxy = None
    if case == "promoted-follower":
        # Two promotions and repeated FULL consume more allocation generations
        # than the small default fixture can retain before reclamation.
        for member in (owner, replica):
            os.makedirs(member.workdir, exist_ok=True)
            allocate_data_file(member.data_path, 1024 * 1024 * 1024)
    publish_fault = (
        "recovery_" + case.replace("-", "_proof_") if case.endswith("publish") else None
    )
    try:
        with crash_at(publish_fault):
            fixture.start_created(add_follower=False)
        if case in ("clean-replica", "eligible-candidate", "promoted-follower"):
            fixture.add_replica(F.CANDIDATE)
        seed(owner)
        if case == "promoted-follower":
            # A promoted cluster Owner persists the logical Meta group name.
            # Crash it before it can adopt a successor's wire identity, then
            # require ordinary FOLLOW/FULL after the remaining member takes over.
            wait_replica_cut(owner)
            F.wait_candidate_source(fixture, F.OWNER, 1, {F.CANDIDATE})
            operation = fixture.submit_failover()
            F.wait_operation(
                fixture,
                operation,
                "OK completed failover-completed",
                "first replica is promoted",
                timeout=60,
            )
            wait_serving(fixture, replica, 2)
            F.wait_candidate_source(fixture, F.CANDIDATE, 2, {F.OWNER})
            wait_replica_cut(replica)
            wait_durable(replica)
            reply = fixture.leader.put_automatic_uncontrolled_failover_policy(
                2, suspect_after_ms=5000
            )
            if not reply.startswith("OK "):
                raise H.Failure(f"could not configure recovery detector: {reply}")
            replica.force_kill()
            wait_serving(fixture, owner, 3)
            restart(fixture, replica)
            F.wait_ready(fixture, "restarted promoted member rejoins", timeout=40)
            info = F.replication_info_fields(replica)
            if info.get("lavik_replication_group_id") != F.GROUP.encode().hex():
                raise H.Failure(f"recovered cluster wire group differs: {info}")
            if (
                F.redis_call(owner, ["SET", "recovery:after-rejoin", "verified"])
                != "OK"
            ):
                raise H.Failure("recovered Owner rejected post-rejoin write")
            H.wait_until(
                "restarted member receives new replicated writes",
                20,
                lambda: F.readonly_get(replica, "recovery:after-rejoin") == "verified",
            )
            require_data(owner)
            fixture.clean_shutdown()
            return
        if case == "incomplete-full":
            restart(fixture, replica, "system-state-device-root-durable")
            fixture.leader.registernode(
                replica.node_id,
                f"lavik://node/{replica.node_id}",
                "replica",
                endpoints=(replica.advertised_endpoint,),
            )
            fixture.leader.assignnode(F.GROUP, replica.node_id, "replica")
            require_exit_at_fault(replica)
        if case == "clean-replica":
            wait_replica_cut(owner)
            # Shutdown the replica before cutting its source; the restarted
            # candidate must retain the source's two-flow layout, not its own.
            replica.terminate()
        short_threshold = case not in ("manual-fence", "eligible-candidate")
        reply = fixture.leader.put_automatic_uncontrolled_failover_policy(
            2, suspect_after_ms=1000 if short_threshold else 600_000
        )
        if not reply.startswith("OK "):
            raise H.Failure(f"could not configure recovery detector: {reply}")
        if publish_fault:
            owner.proc.send_signal(signal.SIGINT)
            require_exit_at_fault(owner)
        elif case in (
            "crash-operator",
            "manual-fence",
            "clean-replica",
            "incomplete-full",
            "eligible-candidate",
        ):
            wait_durable(owner)
            owner.force_kill()
        else:
            owner.terminate()
        target = replica if case in ("clean-replica", "incomplete-full") else owner
        if case.endswith("consume"):
            restart(fixture, target, "recovery_" + case.replace("-", "_proof_"))
            require_exit_at_fault(target)
        if case in ("startup-barrier", "startup-stop"):
            startup_proxy = restart_at_recovery_barrier(
                fixture, target, workdir, stop=case == "startup-stop"
            )
            if case == "startup-stop":
                fixture.clean_shutdown()
                return
        else:
            restart(fixture, target)
        if case in ("incomplete-full", "eligible-candidate"):
            require_fenced(fixture, target, 2 if short_threshold else 1)
            reply = fixture.leader.ctl(
                f"promote {F.GROUP} --node {target.node_id} --accept-data-loss"
            )
            expected = (
                "no-readable-recovered-population"
                if case == "incomplete-full"
                else "eligible-candidate-exists"
            )
            if reply != f"ERR promote {expected}":
                raise H.Failure(f"unsafe promote was not rejected: {reply}")
            fixture.clean_shutdown()
            return
        eligible = case in (
            "startup-barrier",
            "clean-owner",
            "clean-replica",
            "after-publish",
            "before-consume",
        )
        if not eligible:
            require_fenced(fixture, target, 2 if short_threshold else 1)
            if case == "manual-fence":
                # Promote after a lease-only heartbeat has replaced the role
                # report. Recovery availability must survive this interval;
                # sampling an arbitrary heartbeat hid this regression.
                H.wait_until(
                    "Meta observes the recovered population",
                    20,
                    lambda: fixture.leader.observations(F.GROUP).startswith(
                        "OK candidates=1 "
                    ),
                )
                metric = "lavik_cluster_control_lease_decisions_total"
                labels = '{decision="denied"}'
                before = target.metric(metric, labels)
                target.wait_metric(
                    metric,
                    lambda value: value > before,
                    "next lease heartbeat is denied",
                    labels=labels,
                )
            if short_threshold:
                # The one-second threshold accelerates the initial fence above.
                # Operator recovery creates a fresh source history and reconnects
                # Meta. Restore the default detection window for that handoff so a
                # normal reconnect cannot immediately fence the recovered Owner.
                reply = fixture.leader.put_automatic_uncontrolled_failover_policy(3)
                if not reply.startswith("OK "):
                    raise H.Failure(f"could not restore recovery detector: {reply}")
            promote(fixture, target)
        wait_serving(fixture, target, 2)
        require_data(target)
        if case in ("clean-owner", "crash-operator"):
            # A later-term Owner must record its current source domain too.
            # Also exercise the proof alongside the optional index checkpoint;
            # the first shutdown used the default checkpoint-disabled path.
            if (
                F.redis_call(target, ["CONFIG", "SET", "shutdown-checkpoint", "yes"])
                != "OK"
            ):
                raise H.Failure("could not enable the optional shutdown checkpoint")
            target.terminate()
            restart(fixture, target)
            wait_serving(fixture, target, 3)
            require_data(target)
        fixture.clean_shutdown()
    except BaseException:
        fixture.dump_logs()
        raise
    finally:
        fixture.force_kill()
        if startup_proxy is not None:
            startup_proxy.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("meta")
    parser.add_argument("data")
    parser.add_argument("ctl")
    parser.add_argument("workdir", nargs="?")
    parser.add_argument("--case", choices=CASES, required=True)
    args = parser.parse_args()
    for name in ("meta", "data", "ctl"):
        setattr(args, name, os.path.abspath(getattr(args, name)))
    work_argv = [sys.argv[0], args.meta]
    if args.workdir is not None:
        work_argv.append(os.path.abspath(args.workdir))
    workdir, keep = H.make_workdir(work_argv, "meta_population_recovery_")
    started = time.monotonic()
    try:
        run(args, workdir)
        H.log(f"PASS case={args.case} in {time.monotonic() - started:.1f}s")
        return 0
    except Exception as error:  # noqa: BLE001 - preserve process evidence
        H.log(f"FAIL case={args.case}: {error}; retained workdir: {workdir}")
        keep = True
        return 1
    finally:
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    H.set_tag("gate-population-recovery")
    sys.exit(main())
