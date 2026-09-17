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

"""Real-process Automatic Failover Detector fault matrix.

Usage:
  gate_automatic_failover.py META DATA CTL --case=CASE [workdir]

The gate reuses the #41 three-Meta/three-Data fixture, but never submits a
manual failover. Each case proves that current Owner evidence alone drives one
automatic uncontrolled Begin and that the committed fence is safely consumed
by the existing transition executor.
"""

import argparse
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402
import gate_failover as F  # noqa: E402


PAUSE_BEFORE_PROPOSE = "LAVIK_TEST_PAUSE_AUTOMATIC_BEFORE_PROPOSE_MS"
PAUSE_AFTER_COMMIT = "LAVIK_TEST_PAUSE_FAILOVER_AFTER_AUTOMATIC_BEGIN_MS"


def group_status(fixture, deadline):
    status = fixture.cluster_status(deadline)
    group = next(
        (item for item in status.get("groups", [])
         if item.get("group_id") == F.GROUP), None)
    if group is None:
        raise H.Failure(f"automatic failover group is absent: {status}")
    return group


def wait_group(fixture, description, predicate, timeout=90):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        try:
            group = group_status(fixture, deadline)
            latest = group
            if predicate(group):
                return group
        except (H.Failure, OSError):
            pass
        time.sleep(0.05)
    raise H.Failure(
        f"timeout ({timeout}s) waiting for: {description}; group={latest}")


def configure_fast_policies(fixture, suspect_after_ms=1000,
                            lease_duration_ms=1000):
    fixture.rediscover_leader(time.monotonic() + 5)
    # The lease Policy is projected asynchronously into every Data FDS. A
    # transient READY result from the pre-update projection is not sufficient:
    # cutting the Owner at that point can strand both replicas midway through
    # adopting the final FDS, leaving the deliberately candidate-less Begin
    # with no completed population to select. Observe one post-commit FDS on
    # every member before accepting readiness for the fault cut.
    fds_metric = "lavik_cluster_control_full_states_applied_total"
    fds_before = {data.node_id: data.metric(fds_metric)
                  for data in fixture.data_nodes}
    lease_reply = fixture.leader.put_authority_lease_policy(
        2, duration_ms=lease_duration_ms)
    if re.fullmatch(r"OK [1-9][0-9]*", lease_reply) is None:
        raise H.Failure(
            f"fast Authority Lease Policy update failed: {lease_reply}")
    for data in fixture.data_nodes:
        data.wait_metric(
            fds_metric,
            lambda value, node_id=data.node_id:
            value > fds_before[node_id],
            f"Data node {data.node_id[:8]} applies the lease Policy FDS",
            timeout=30)
    F.wait_ready(fixture, "fast Policy projection reaches READY")

    threshold_reply = fixture.leader.put_automatic_uncontrolled_failover_policy(
        2, suspect_after_ms=suspect_after_ms)
    if re.fullmatch(r"OK [1-9][0-9]*", threshold_reply) is None:
        raise H.Failure(
            "automatic-failover Policy threshold update failed: " + threshold_reply)

    def healthy(group):
        elapsed = int(group.get("suspect_elapsed_ms", "0"))
        return (group.get("automatic_failover_state") == "healthy" and
                elapsed == 0 and
                int(group.get("effective_threshold_ms", "0")) ==
                suspect_after_ms)

    wait_group(fixture, "automatic detector observes healthy Owner", healthy,
               timeout=30)


def wait_suspect(fixture, *, reason=None, timeout=20):
    def suspect(group):
        return (group.get("automatic_failover_state") == "suspect" and
                int(group.get("suspect_elapsed_ms", "0")) <
                int(group.get("effective_threshold_ms", "0")) and
                (reason is None or group.get("current_reason") == reason))

    return wait_group(fixture, "automatic detector enters SUSPECT", suspect,
                      timeout=timeout)


def wait_successor(fixture, timeout=90):
    def serving(group):
        return (group.get("term") == "2" and
                group.get("owner_node_id") in (F.CANDIDATE, F.FOLLOWER) and
                group.get("serving_ready"))

    group = wait_group(fixture, "automatic transition reaches serving term 2",
                       serving, timeout=timeout)
    return group["owner_node_id"]


def require_successor_write(fixture, successor, scenario):
    key = f"{{automatic-{scenario}}}post-meta-failover"
    if F.redis_call(fixture.by_id[successor], ["SET", key, "served"]) != "OK":
        raise H.Failure(
            f"{scenario} successor reported ready but rejected a write")


def require_automatic_begin(fixture, expected_reason=None):
    begin = F.require_unique_failover_event(
        fixture.metas, "begin", "uncontrolled")
    if begin["reason"] not in ("session_missing", "heartbeat_expired"):
        raise H.Failure(f"automatic Begin has wrong trigger audit: {begin}")
    if expected_reason is not None and begin["reason"] != expected_reason:
        raise H.Failure(
            f"automatic Begin reason {begin['reason']!r}, "
            f"want {expected_reason!r}")
    if begin["suspect_ms"] is None or begin["suspect_ms"] < 1000:
        raise H.Failure(f"automatic Begin lacks suspect duration: {begin}")
    return begin


def run_owner_loss(meta, data, ctl, workdir, mode, require_fault_hook):
    del require_fault_hook
    fixture = F.FailoverFixture(
        meta, data, ctl, os.path.join(workdir, mode), False)
    try:
        fixture.start_created()
        configure_fast_policies(fixture)
        key = f"{{automatic-{mode}}}key"
        fixture.seed_and_wait_for_replicas(
            key, "before-owner-loss", (F.CANDIDATE, F.FOLLOWER))

        if mode == "owner-kill":
            fixture.by_id[F.OWNER].force_kill()
            expected_reason = "session_missing"
        else:
            fixture.by_id[F.OWNER].pause()
            expected_reason = "heartbeat_expired"
        wait_suspect(fixture, reason=expected_reason)
        successor = wait_successor(fixture)
        if mode == "owner-pause":
            fixture.by_id[F.OWNER].resume()
            F.wait_owner(fixture, successor, fixture.data_nodes)

        after = "after-automatic-cutover"
        if F.redis_call(fixture.by_id[successor], ["SET", key, after]) != "OK":
            raise H.Failure("automatic successor rejected a write")
        if mode == "owner-pause":
            rejection = F.redis_error(
                fixture.by_id[F.OWNER], ["SET", key, "stale-owner-write"])
            if not rejection.startswith(("CLUSTERDOWN", "TRYAGAIN", "MOVED",
                                         "READONLY")):
                raise H.Failure(
                    f"resumed old Owner was not fenced: {rejection}")
            # Projection convergence and replication delivery are separate
            # asynchronous cuts. The old Owner must remain write-fenced while
            # it eventually catches the successor's post-cutover value.
            H.wait_until(
                "resumed old Owner follows successor", 30,
                lambda: F.readonly_get(fixture.by_id[F.OWNER], key) == after)

        begin = require_automatic_begin(fixture)
        if begin["reason"] != expected_reason:
            raise H.Failure(
                f"{mode} committed reason {begin['reason']!r}, "
                f"want {expected_reason!r}")
        fixture.require_expected_processes_alive(
            dead_data_ids=(F.OWNER,) if mode == "owner-kill" else ())
        H.log(f"{mode}: automatic Begin fenced term 1 and successor "
              f"{successor[:8]} served term 2")
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        fixture.force_kill()


def assert_non_overlapping_writes(old_probe, replica_probes, successor):
    old_probe.stop_and_assert()
    for probe in replica_probes.values():
        probe.stop_and_assert()
    old_successes = old_probe.successes()
    new_successes = replica_probes[successor].successes()
    if not old_successes or not new_successes:
        raise H.Failure("partition probes missed a serving epoch")
    first_new_started = min(started for started, _ in new_successes)
    late_old = [interval for interval in old_successes
                if interval[1] >= first_new_started]
    if late_old:
        raise H.Failure(
            "old and automatic successor write successes overlapped: "
            f"first_new_started={first_new_started} late_old={late_old[-3:]}")
    losing = {node_id: probe.successes()
              for node_id, probe in replica_probes.items()
              if node_id != successor and probe.successes()}
    if losing:
        raise H.Failure(
            f"non-selected replicas accepted writes: {losing}")


def run_partition(meta, data, ctl, workdir, direction, require_fault_hook):
    del require_fault_hook
    name = "one-way-partition" if direction == "downstream" \
        else "two-way-partition"
    fixture = F.FailoverFixture(
        meta, data, ctl, os.path.join(workdir, name), False,
        proxy_data_control=True)
    old_probe = None
    replica_probes = {}
    try:
        fixture.start_created()
        configure_fast_policies(fixture)
        key = f"{{automatic-{name}}}key"
        fixture.seed_and_wait_for_replicas(
            key, "before-partition", (F.CANDIDATE, F.FOLLOWER))
        old_probe = F.ContinuousSetProbe(
            fixture.by_id[F.OWNER], f"{{automatic-{name}}}old", "old")
        replica_probes = {
            node_id: F.ContinuousSetProbe(
                fixture.by_id[node_id],
                f"{{automatic-{name}}}{node_id[:8]}", node_id[:8])
            for node_id in (F.CANDIDATE, F.FOLLOWER)
        }
        old_probe.start()
        for probe in replica_probes.values():
            probe.start()
        old_probe.wait_for_success("old Owner serves before partition", 5)
        lease_metric = "lavik_cluster_control_lease_expirations_total"
        expirations_before = fixture.by_id[F.OWNER].metric(lease_metric)

        fixture.partition_owner_control(direction)
        expected_reason = "heartbeat_expired" \
            if direction == "downstream" else None
        wait_suspect(fixture, reason=expected_reason, timeout=20)
        successor = wait_successor(fixture)
        replica_probes[successor].wait_for_success(
            "automatic successor serves writes", 10)
        fixture.by_id[F.OWNER].wait_metric(
            lease_metric, lambda value: value > expirations_before,
            "partitioned Owner self-fences on finite lease expiry", timeout=15)
        time.sleep(1.0)
        assert_non_overlapping_writes(old_probe, replica_probes, successor)

        post_cutover = "automatic-successor-only"
        if F.redis_call(fixture.by_id[successor],
                        ["SET", key, post_cutover]) != "OK":
            raise H.Failure("automatic successor lacked authority")
        rejection = F.redis_error(
            fixture.by_id[F.OWNER], ["SET", key, "stale-owner-write"])
        if not rejection.startswith(("CLUSTERDOWN", "TRYAGAIN", "MOVED")):
            raise H.Failure(f"partitioned old Owner was not fenced: {rejection}")

        require_automatic_begin(fixture, expected_reason)
        fixture.heal_owner_control()
        F.wait_owner(fixture, successor, fixture.data_nodes)
        if F.readonly_get(fixture.by_id[F.OWNER], key) != post_cutover:
            raise H.Failure("healed old Owner did not follow successor")
        fixture.require_expected_processes_alive()
        H.log(f"{name}: old Owner self-fenced, writes never overlapped, "
              f"and successor {successor[:8]} served term 2")
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        if old_probe is not None:
            old_probe.stop()
        for probe in replica_probes.values():
            probe.stop()
        fixture.force_kill()


def kill_logged_meta(fixture, marker, timeout=40):
    paused = None

    def observed():
        nonlocal paused
        paused = next((node for node in fixture.metas
                       if marker in node.log_tail(lines=1000)), None)
        return paused is not None

    H.wait_until(marker, timeout, observed)
    paused.kill9()
    # rediscover_leader is an election-race helper for an already elected
    # cluster and intentionally probes each seed once. Here the old leader was
    # just killed, so wait through NuRaft's failure detector and election.
    fixture.leader = H.find_leader(
        [meta for meta in fixture.metas if meta.id != paused.id], timeout=20)
    return paused


def run_leader_threshold(meta, data, ctl, workdir, require_fault_hook):
    del require_fault_hook
    fixture = F.FailoverFixture(
        meta, data, ctl, os.path.join(workdir, "leader-threshold"), False)
    try:
        fixture.start_created()
        configure_fast_policies(fixture, suspect_after_ms=3000)
        fixture.by_id[F.OWNER].force_kill()
        old_suspect = wait_group(
            fixture, "old leader accumulates a material SUSPECT interval",
            lambda group:
            group.get("automatic_failover_state") == "suspect" and
            group.get("current_reason") == "session_missing" and
            int(group.get("suspect_elapsed_ms", "0")) >= 1500 and
            int(group.get("suspect_elapsed_ms", "0")) <
            int(group.get("effective_threshold_ms", "0")),
            timeout=20)
        old_elapsed_ms = int(old_suspect["suspect_elapsed_ms"])
        old_leader = fixture.leader
        old_leader.kill9()
        fixture.leader = H.find_leader(
            [meta for meta in fixture.metas if meta.id != old_leader.id],
            timeout=20)
        wait_group(
            fixture, "replacement leader reports observation warmup",
            lambda group:
            group.get("automatic_failover_state") == "blocked" and
            group.get("blocked_reason") == "leadership_warmup", timeout=10)
        wait_suspect(fixture, reason="session_missing", timeout=20)
        # A replacement Leader must not inherit the old Leader's elapsed
        # suspicion. Stay inside the fresh 3s interval and prove term 1 holds.
        time.sleep(2.0)
        before = group_status(fixture, time.monotonic() + 5)
        if before.get("term") != "1" or before.get("owner_node_id") != F.OWNER:
            raise H.Failure(
                f"replacement leader reused prior debounce: {before}")
        successor = wait_successor(fixture)
        require_successor_write(fixture, successor, "leader-threshold")
        require_automatic_begin(fixture)
        fixture.require_expected_processes_alive(
            dead_meta_ids=(old_leader.id,), dead_data_ids=(F.OWNER,))
        H.log("leader-threshold: replacement discarded "
              f"{old_elapsed_ms}ms of prior suspicion; warmup and full "
              f"debounce preceded successor {successor[:8]}")
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        fixture.force_kill()


def run_leader_cut(meta, data, ctl, workdir, phase, require_fault_hook):
    variable = PAUSE_BEFORE_PROPOSE if phase == "proposal" else PAUSE_AFTER_COMMIT
    marker = ("automatic failover paused before Begin proposal" if
              phase == "proposal" else
              "failover reconciliation paused after automatic "
              "uncontrolled Begin group=")
    if require_fault_hook and not F.binary_contains(meta, variable.encode()):
        raise H.Failure(f"automatic failover gate requires {variable}")
    previous = os.environ.get(variable)
    os.environ[variable] = "15000"
    fixture = F.FailoverFixture(
        meta, data, ctl, os.path.join(workdir, f"leader-{phase}"),
        require_fault_hook)
    try:
        fixture.start_created()
        # This gate requires exactly one Begin across Meta recovery. Promotion
        # rotates replication history and reconnects the new Owner's session;
        # a 1s debounce can legitimately declare that reconnect a second Owner
        # failure. Use the bootstrap-default debounce for this recovery gate,
        # retaining the exact term/unique-Begin assertions below. The dedicated
        # Owner-loss and partition gates still exercise the 1s threshold.
        configure_fast_policies(fixture, suspect_after_ms=5000)
        fixture.by_id[F.OWNER].force_kill()
        paused = kill_logged_meta(fixture, marker, timeout=30)
        if phase == "proposal":
            cut = group_status(fixture, time.monotonic() + 5)
            if cut.get("term") != "1":
                raise H.Failure(
                    f"pre-proposal leader appended before its cut: {cut}")
        successor = wait_successor(fixture, timeout=120)
        require_successor_write(fixture, successor, f"leader-{phase}")
        require_automatic_begin(fixture)
        fixture.require_expected_processes_alive(
            dead_meta_ids=(paused.id,), dead_data_ids=(F.OWNER,))
        if phase == "commit":
            H.log("leader-commit: durable transition resumed on replacement "
                  f"leader and successor {successor[:8]} served")
        else:
            H.log("leader-proposal: replacement leader performed a fresh "
                  f"detection and successor {successor[:8]} served")
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        fixture.force_kill()
        if previous is None:
            os.environ.pop(variable, None)
        else:
            os.environ[variable] = previous


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("meta")
    parser.add_argument("data")
    parser.add_argument("ctl")
    parser.add_argument("workdir", nargs="?")
    parser.add_argument(
        "--case",
        choices=("owner-kill", "owner-pause", "one-way-partition",
                 "two-way-partition", "leader-threshold",
                 "leader-proposal", "leader-commit"),
        required=True)
    parser.add_argument("--require-fault-hook", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    binaries = {name: os.path.abspath(getattr(args, name))
                for name in ("meta", "data", "ctl")}
    work_argv = [sys.argv[0], binaries["meta"]]
    if args.workdir is not None:
        work_argv.append(os.path.abspath(args.workdir))
    workdir, keep = H.make_workdir(work_argv, "meta_automatic_failover_")
    started = time.monotonic()
    try:
        common = (binaries["meta"], binaries["data"], binaries["ctl"],
                  workdir)
        if args.case in ("owner-kill", "owner-pause"):
            run_owner_loss(*common, args.case, args.require_fault_hook)
        elif args.case == "one-way-partition":
            run_partition(*common, "downstream", args.require_fault_hook)
        elif args.case == "two-way-partition":
            run_partition(*common, "both", args.require_fault_hook)
        elif args.case == "leader-threshold":
            run_leader_threshold(*common, args.require_fault_hook)
        elif args.case == "leader-proposal":
            run_leader_cut(*common, "proposal", args.require_fault_hook)
        elif args.case == "leader-commit":
            run_leader_cut(*common, "commit", args.require_fault_hook)
        H.log(f"PASS case={args.case} in {time.monotonic() - started:.1f}s")
        return 0
    except Exception as error:  # noqa: BLE001 - retain process evidence
        H.log(f"FAIL case={args.case}: {error}")
        keep = True
        H.log(f"retained workdir: {workdir}")
        return 1
    finally:
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    H.set_tag("gate-automatic-failover")
    sys.exit(main())
