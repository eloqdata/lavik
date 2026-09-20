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

"""Real two-flow, two-donor Candidate Recovery in either client mode."""
import argparse
import binascii
import os
from pathlib import Path
import re
import sys
import tempfile
import time

import gate_failover as F
import harness as H


def flow_key(flow):
    return next(f"recovery-flow-{i}" for i in range(100)
                if binascii.crc_hqx(f"recovery-flow-{i}".encode(), 0) % 2 == flow)


def run(root, meta, data, ctl, mode, case):
    fixture = F.FailoverFixture(meta, data, ctl, str(root / (mode + "-" + case)), True,
                                pause_after_begin_ms=1, data_workers=2,
                                client_mode=mode, four_data=True)
    for meta_node in fixture.metas:
        # Begin advances the Group term. Hold the existing planner seam until
        # every real replica has reported progress for that new projection;
        # otherwise the first heartbeat can legitimately win selection alone.
        meta_node.pause_after_automatic_begin_ms = 2000
    source, candidate, donor1, donor2 = fixture.data_nodes
    cut_file = root / (mode + "-cuts")
    for node in fixture.data_nodes:
        node.environment = {**os.environ, "LAVIK_TEST_RECOVERY_TRACE": "1"}
    source.environment["LAVIK_TEST_NATIVE_EVENT_CUT_FILE"] = str(cut_file)
    keys = [flow_key(flow) for flow in range(2)]
    if case in ("budget", "donor-failure", "leader-change", "candidate-replace"):
        donor1.environment["LAVIK_TEST_RECOVERY_EFFECT_DELAY_MS"] = "3000"
        if case != "donor-failure":
            donor2.environment["LAVIK_TEST_RECOVERY_EFFECT_DELAY_MS"] = "3000"
    large = case == "coverage-gap"
    def values(node):
        return [F.readonly_get(node, key).split(":", 1)[0] for key in keys]
    dead_data = {F.OWNER}
    dead_meta = set()
    expected_owner = F.CANDIDATE
    promoted = candidate

    try:
        fixture.start_created()
        if large:
            assert F.redis_call(source, ["CONFIG", "SET", "repl-backlog-size", str(128 * 1024 * 1024)]) == "OK"
        if case in ("leader-change", "candidate-replace"):
            reply = fixture.leader.putpolicy("lavik.candidate-recovery-v1", 2,
                '{"kind":"candidate-recovery-v1","budget_ms":12000}')
            assert reply.startswith("OK "), reply
        for key in keys:
            fixture.seed_and_wait_for_replicas(key, "0", (F.CANDIDATE, F.FOLLOWER, F.SECOND_DONOR))
        baseline = [source.metric("lavik_replication_backlog_tail_lsn",
                                  f'{{worker="{flow}"}}') + 1 for flow in range(2)]
        assert all(lsn > 0 for lsn in baseline), baseline
        # File installation only controls complete wire events. Cursors and
        # observations come exclusively from actual FULL and command replay.
        limits = {F.CANDIDATE: (3, 3), F.FOLLOWER: (5, 0), F.SECOND_DONOR: (0, 5)}
        staged = cut_file.with_suffix(".new")
        staged.write_text("".join(f"{node} {flow} {baseline[flow] + count}\n"
                                  for node, counts in limits.items()
                                  for flow, count in enumerate(counts)))
        staged.replace(cut_file)
        for i in range(5):
            for key in keys:
                if large:
                    # Each complete event fits a replica's 8 MiB flow log, but
                    # two canonical effects exceed one retained-history block.
                    # The fifth real apply therefore evicts the missing fourth.
                    assert F.redis_call(source, ["SET", key, str(i + 1) + ":" + "v" * (4 * 1024 * 1024)]) == "OK"
                else:
                    assert F.redis_call(source, ["INCR", key]) == i + 1
        for node in (candidate, donor1, donor2):
            counts = limits[node.node_id]
            H.wait_until("real " + node.node_id[:8] + " vector " + str(counts), 20,
                         lambda node=node, counts=counts:
                         values(node) == list(map(str, counts)))
        for flow in range(2):
            assert source.metric("lavik_replication_backlog_tail_lsn",
                                 f'{{worker="{flow}"}}') + 1 == baseline[flow] + 5
        assert Path(source.log_path).read_text().count("test native event send paused") >= 4
        full_counts = {node.node_id: Path(node.log_path).read_text().count(
            "durably activated population") for node in (candidate, donor1, donor2)}
        # Let production observations see C=[3,3] and donors=[5,0]/[0,5].
        time.sleep(1)
        fixture.rediscover_leader(time.monotonic() + 5)
        reply = fixture.leader.put_automatic_uncontrolled_failover_policy(2, 1000)
        assert reply.startswith("OK "), reply
        source.force_kill()
        if case in ("donor-failure", "leader-change", "candidate-replace"):
            H.wait_until("donor receives real recovery request", 30, lambda:
                "test recovery donor waiting" in Path(donor1.log_path).read_text())
            if case == "donor-failure":
                donor1.force_kill()
                dead_data.add(donor1.node_id)
            elif case == "candidate-replace":
                candidate.force_kill()
                dead_data.add(candidate.node_id)
                expected_owner, promoted = F.FOLLOWER, donor1
            else:
                fixture.rediscover_leader(time.monotonic() + 5)
                dead_meta.add(fixture.leader.id)
                fixture.leader.kill9()
        alive = [node for node in fixture.data_nodes if node.node_id not in dead_data]
        F.wait_serving_owner(fixture, expected_owner, alive, timeout=90)
        _, records = F.failover_log_records(fixture.metas)
        selections = {record["index"]: record for record in records
                      if record["event"] in ("candidate-selected", "candidate-replaced")}
        selected = [selections[index] for index in sorted(selections, key=int)]
        assert selected[0]["candidate"] == F.CANDIDATE, selected
        if case == "candidate-replace":
            assert len(selected) == 2 and selected[1]["candidate"] == F.FOLLOWER, selected
            assert selected[0]["transition"] == selected[1]["transition"]
            assert selected[0]["action"] != selected[1]["action"]
        deadlines = {(record["transition"], re.search(r"recovery_deadline_ms=(\d+)", record["detail"])[1])
                     for record in records if record["event"] == "recovery-start"}
        assert len(deadlines) == 1, deadlines
        executions = {match for node in (candidate, donor1, donor2)
                      for match in re.findall(
                          r"test candidate recovery started transition=([0-9a-f]+) action=([0-9a-f]+) deadline=(\d+)",
                          Path(node.log_path).read_text())}
        assert {(transition, deadline) for transition, _, deadline in executions} == deadlines, executions
        assert len(executions) == (2 if case == "candidate-replace" else 1), executions
        candidate_log = Path(promoted.log_path).read_text()
        completed = re.findall(r"candidate recovery completed .*? reason=([^ ]+) applied=([0-9,]+)", candidate_log)
        assert completed, candidate_log[-10000:]
        reason, frontier = completed[-1]
        frontier = list(map(int, frontier.split(",")))
        recovered = [frontier[i] - baseline[i] for i in range(2)]
        assert all(0 <= count <= 5 for count in recovered), recovered
        if case in ("complete", "candidate-replace"):
            assert reason == "target-reached" and recovered == [5, 5], (reason, recovered)
        elif case == "leader-change":
            # Losing the authenticated control session revokes donor exports.
            # Optional gathering may end early, but never invents applied
            # progress or extends the transition's committed deadline.
            assert reason in ("target-reached", "coverage-unavailable", "deadline"), reason
            assert all(count >= 3 for count in recovered), recovered
        elif case == "budget":
            assert reason == "deadline" and recovered == [3, 3], (reason, recovered)
        elif case == "donor-failure":
            assert recovered == [3, 5], (reason, recovered)
        elif case == "coverage-gap":
            assert reason == "coverage-unavailable" and recovered == [3, 3], (reason, recovered)
        if case == "complete":
            for donor, flow in ((donor1, 0), (donor2, 1)):
                log = Path(donor.log_path).read_text()
                for delta in (3, 4):
                    assert f"candidate={F.CANDIDATE} flow={flow} lsn={baseline[flow] + delta}" in log, log[-10000:]
        for node in alive:
            H.wait_until("reparent preserves recovered data", 60, lambda node=node:
                         values(node) == list(map(str, recovered)))
            if all(old <= new for old, new in zip(limits[node.node_id], recovered)):
                assert Path(node.log_path).read_text().count("durably activated population") == full_counts[node.node_id]
        for key in keys:
            assert F.redis_call(promoted, ["SET", key, "6"]) == "OK"
        for node in alive:
            H.wait_until("post-promotion replay", 30, lambda node=node: values(node) == ["6", "6"])
        fixture.require_expected_processes_alive(dead_data_ids=dead_data, dead_meta_ids=dead_meta)
        fixture.clean_shutdown()
        H.log(f"{mode}/{case}: real B={baseline}, recovered={recovered}, deadline={deadlines}")
    except BaseException:
        fixture.dump_logs()
        raise
    finally:
        fixture.force_kill()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("meta")
    parser.add_argument("data")
    parser.add_argument("ctl")
    parser.add_argument("--mode", choices=("single", "cluster"), required=True)
    parser.add_argument("--workdir")
    parser.add_argument("--case", choices=("complete", "donor-failure", "budget", "coverage-gap", "candidate-replace", "leader-change"), default="complete")
    args = parser.parse_args()
    H.set_tag("candidate-recovery")
    if args.workdir:
        root = Path(args.workdir)
        root.mkdir(parents=True)
        run(root, *map(os.path.abspath, (args.meta, args.data, args.ctl)), args.mode, args.case)
    else:
        with tempfile.TemporaryDirectory(prefix="lavik-recovery-",
                                         dir=os.environ.get("LAVIK_TEST_DATA_DIR")) as directory:
            run(Path(directory), *map(os.path.abspath, (args.meta, args.data, args.ctl)), args.mode, args.case)
    H.log("PASS")


if __name__ == "__main__":
    main()
