#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

"""Check native fixture admission and asynchronous failure reporting."""

from concurrent.futures import Future
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import gate_cluster_create as C
import gate_data_control as D
import gate_failover as F
import gate_native_replication as N


class ReachedClusterCreate(Exception):
    """Stop after startup, before the scenario's unrelated FULL assertions."""


class NativeFixtureStartupTest(unittest.TestCase):
    def test_pair_accepts_leader_published_after_long_election_window(self):
        now = 0.0

        def sleep(seconds):
            nonlocal now
            now += seconds

        def wait_leader(timeout=10):
            # The Raft election is allowed to consume its whole ten-second
            # window, followed by one status-publication/scheduling interval.
            N.H.wait_until("leader published", timeout, lambda: now >= 10.05)

        meta = Mock()
        meta.wait_leader.side_effect = wait_leader
        source, target = Mock(), Mock()
        source.log_tail.return_value = target.log_tail.return_value = ""
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.multiple(N.C, META="meta", DATA="data", CTL="ctl", create=True),
            patch.object(N.H, "Node", return_value=meta),
            patch.object(N, "DataProcess", side_effect=[source, target]),
            patch.object(N.C, "DirectiveBarrier"),
            patch.object(N.C, "meta_manifest_lines", return_value=[]),
            patch.object(N.C, "command", side_effect=ReachedClusterCreate),
            patch.object(N.H, "dump_node_logs"),
            patch.object(
                N.H, "time", SimpleNamespace(monotonic=lambda: now, sleep=sleep)
            ),
        ):
            with self.assertRaises(ReachedClusterCreate):
                with N.pair(
                    Path(directory),
                    "long-election",
                    raft_args=N.H.raft_args(
                        election_ms_low=5000, election_ms_high=10000
                    ),
                ):
                    self.fail("cluster creation should stop the fixture")
        source.start.assert_called_once()
        target.start.assert_called_once()
        self.assertGreaterEqual(now, 10.05)

    def check_startup(self, scenario):
        membership_stable = False
        status_calls = 0
        follower_status_calls = 0
        follower_applied = False
        start_created = F.FailoverFixture.start_created

        def cluster_status(_deadline):
            nonlocal membership_stable, status_calls
            status_calls += 1
            membership_stable = status_calls > 1
            return {"meta_membership_stable": membership_stable}

        def follower_status(**_kwargs):
            nonlocal follower_status_calls, follower_applied
            follower_status_calls += 1
            follower_applied = follower_status_calls > 1
            return {"initial_bindings_pending": "0" if follower_applied else "1"}

        def start_data(**_kwargs):
            # A complete leader directory exposes followers as bootstrap
            # candidates even while a follower has not applied its own identity.
            if not membership_stable or not follower_applied:
                raise N.H.Failure(
                    "bootstrap Meta identity absent from committed directory"
                )

        leader = SimpleNamespace(
            data_control_endpoint="127.0.0.1:12345",
            ctl_endpoint="127.0.0.1:12346",
        )
        fixture = SimpleNamespace(
            manifest="unused-manifest",
            client_mode=N.CLIENT_MODE,
            control_proxies=[],
            metas=[
                SimpleNamespace(
                    id=i,
                    start=Mock(),
                    status=Mock(
                        side_effect=follower_status if i == 3 else None,
                        return_value={"initial_bindings_pending": "0"},
                    ),
                )
                for i in range(1, 4)
            ],
            data_nodes=[
                SimpleNamespace(
                    start=Mock(side_effect=start_data),
                    alive=lambda: True,
                    _metrics_ready=lambda: True,
                )
                for _ in range(3)
            ],
            cluster_status=Mock(side_effect=cluster_status),
            dump_logs=Mock(),
            force_kill=Mock(),
        )
        with (
            tempfile.TemporaryDirectory(
                prefix="native-startup-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
            ) as directory,
            patch.object(F, "FailoverFixture", return_value=fixture),
            patch.object(F, "write_manifest"),
            patch.multiple(N.C, META="meta", DATA="data", CTL="ctl", create=True),
            patch.object(N.H, "find_leader", return_value=leader),
            patch.object(N.C, "command", side_effect=ReachedClusterCreate),
            patch.object(F, "run_command", side_effect=ReachedClusterCreate),
        ):
            # Exercise the scenario's actual orchestration. Election alone is
            # insufficient: both the leader's pending membership and a lagging
            # follower must be tolerated before any Data process bootstraps.
            with self.assertRaises(ReachedClusterCreate):
                if scenario is None:
                    fixture.ctl = "ctl"
                    start_created(fixture)
                else:
                    scenario(Path(directory))
        self.assertGreaterEqual(status_calls, 2)
        self.assertGreaterEqual(follower_status_calls, 2)
        for node in fixture.data_nodes:
            node.start.assert_called_once()
            self.assertEqual(node.seed, leader.data_control_endpoint)
        if scenario is not None:
            fixture.force_kill.assert_called_once()

    def test_follow_owner_waits_for_every_meta_before_data_bootstrap(self):
        self.check_startup(None)

    def test_explicit_full_waits_for_membership_before_data_bootstrap(self):
        self.check_startup(N.explicit_full_limit)

    def test_mixed_full_waits_for_membership_before_data_bootstrap(self):
        self.check_startup(N.mixed_full_limit)


class BootstrapCreationAdmissionTest(unittest.TestCase):
    busy = (
        "ERR clustercreate 1 preflight pre-commit-failed "
        "another cluster creation or Meta membership change is in progress"
    )

    def setUp(self):
        self.now = 0.0
        self.clock = SimpleNamespace(monotonic=lambda: self.now, sleep=self.advance)
        self.patch = patch.object(C, "time", self.clock)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def advance(self, duration):
        self.now += duration

    def test_busy_retries_exact_request_then_stops_on_success(self):
        meta = Mock()
        meta.ctl.side_effect = [self.busy, "OK clustercreate 1"]
        self.assertEqual(
            C.create_after_membership_admission(meta, "fixed request"),
            "OK clustercreate 1",
        )
        self.assertEqual(meta.ctl.call_count, 2)
        for call in meta.ctl.call_args_list:
            self.assertEqual(call.args, ("fixed request",))

    def test_data_control_retries_without_regenerating_creation_identity(self):
        meta = Mock()
        meta.ctl.side_effect = [self.busy, "OK clustercreate 1"]
        with (
            patch.object(C, "create_request", return_value="fixed request") as request,
            patch.object(
                C, "cluster_status", return_value={"meta_membership_stable": True}
            ) as status,
            patch.object(C.H, "free_port", return_value=12345),
        ):
            D.commit_service_mode(meta, [meta])
        request.assert_called_once()
        status.assert_called_once_with(meta)
        self.assertEqual(meta.ctl.call_count, 2)
        for call in meta.ctl.call_args_list:
            self.assertEqual(call.args, ("fixed request",))

    def test_other_rejections_are_not_retried(self):
        for reply in (
            "ERR clustercreate 1 preflight pre-commit-failed Meta is shutting down",
            "ERR clustercreate 1 commit unknown outcome",
            "",
        ):
            with self.subTest(reply=reply):
                meta = Mock()
                meta.ctl.return_value = reply
                with self.assertRaises(C.H.Failure):
                    C.create_after_membership_admission(meta, "fixed request")
                meta.ctl.assert_called_once()

    def test_transport_failure_is_not_retried(self):
        meta = Mock()
        meta.ctl.side_effect = TimeoutError("unknown outcome")
        with self.assertRaises(TimeoutError):
            C.create_after_membership_admission(meta, "fixed request")
        meta.ctl.assert_called_once()

    def test_persistent_busy_stops_at_deadline(self):
        meta = Mock()
        meta.ctl.return_value = self.busy
        with self.assertRaisesRegex(C.H.Failure, "bootstrap creation exceeded 10s"):
            C.create_after_membership_admission(meta, "fixed request")
        self.assertEqual(self.now, 10)

    def test_call_timeout_uses_remaining_budget(self):
        meta = Mock()

        def reply(_request, timeout):
            if meta.ctl.call_count == 1:
                self.advance(8)
                return self.busy
            self.assertAlmostEqual(timeout, 1.95)
            return "OK clustercreate 1"

        meta.ctl.side_effect = reply
        C.create_after_membership_admission(meta, "fixed request")
        self.assertEqual(meta.ctl.call_count, 2)

    def test_success_after_budget_is_not_accepted_or_retried(self):
        meta = Mock()

        def reply(_request, timeout):
            self.advance(11)
            return "OK clustercreate 1"

        meta.ctl.side_effect = reply
        with self.assertRaisesRegex(C.H.Failure, "bootstrap creation exceeded 10s"):
            C.create_after_membership_admission(meta, "fixed request")
        meta.ctl.assert_called_once()


class ReplicaBackupAdmissionTest(unittest.TestCase):
    def setUp(self):
        self.now = 0.0
        self.clock = SimpleNamespace(monotonic=lambda: self.now, sleep=self.advance)
        self.clock_patch = patch.object(N, "time", self.clock)
        self.clock_patch.start()
        self.addCleanup(self.clock_patch.stop)

    def advance(self, duration):
        self.now += duration

    def test_startup_loading_and_previous_backup_busy_retry_then_start_once(self):
        reader = Mock()
        reader.call.side_effect = [
            N.H.Failure("LOADING Redis is loading the dataset in memory"),
            N.H.Failure("LOADING Lavik is loading the dataset from the primary"),
            N.H.Failure("ERR Background save already in progress"),
            "Background saving started",
        ]
        N.start_replica_backup(reader)
        self.assertEqual(reader.call.call_count, 4)
        self.assertTrue(
            all(call.args == ("BGSAVE",) for call in reader.call.call_args_list)
        )

    def test_other_errors_and_unknown_outcomes_are_not_retried(self):
        for error in (
            N.H.Failure("ERR RDB cut failed"),
            N.H.Failure("Data closed its Redis connection"),
            N.H.Failure("CLUSTERDOWN Hash slot not served"),
            OSError("connection reset"),
            TimeoutError("unknown backup outcome"),
        ):
            with self.subTest(error=error):
                reader = Mock()
                reader.call.side_effect = error
                with self.assertRaises(type(error)) as raised:
                    N.start_replica_backup(reader)
                self.assertIs(raised.exception, error)
                reader.call.assert_called_once_with("BGSAVE")

    def test_persistent_loading_reports_its_error_at_deadline(self):
        reader = Mock()
        error = N.H.Failure("LOADING Redis is loading the dataset in memory")
        reader.call.side_effect = error
        with self.assertRaises(N.H.Failure) as raised:
            N.start_replica_backup(reader)
        self.assertIs(raised.exception, error)
        self.assertEqual(self.now, 10)


class ReplicaBackupCheckpointTest(unittest.TestCase):
    def setUp(self):
        self.backup = Future()
        self.transactions = Future()
        self.now = 0.0
        self.clock = SimpleNamespace(monotonic=lambda: self.now, sleep=self.advance)
        self.clock_patch = patch.object(N, "time", self.clock)
        self.clock_patch.start()
        self.addCleanup(self.clock_patch.stop)
        self.log_patch = patch.object(N, "Path")
        self.log = self.log_patch.start().return_value
        self.log.read_text.return_value = ""
        self.addCleanup(self.log_patch.stop)

    def advance(self, duration):
        self.now += duration

    def wait(self):
        N.wait_backup_checkpoint(
            "target.log", "database gates closed", self.backup, self.transactions
        )

    def test_checkpoint_requires_live_producers(self):
        self.log.read_text.side_effect = ["", "database gates closed"]
        self.wait()
        self.assertGreater(self.now, 0)

    def test_background_errors_are_not_replaced_by_timeout(self):
        for producer in ("backup", "transactions"):
            for error in (
                N.H.Failure("Redis rejected request"),
                OSError("peer closed"),
            ):
                with self.subTest(producer=producer, error=error):
                    self.backup, self.transactions = Future(), Future()
                    getattr(self, producer).set_exception(error)
                    with self.assertRaises(type(error)) as raised:
                        self.wait()
                    self.assertIs(raised.exception, error)
                    self.assertEqual(self.now, 0)

    def test_existing_checkpoint_does_not_hide_a_failed_backup(self):
        self.log.read_text.return_value = "database gates closed"
        self.backup.set_exception(N.H.Failure("RDB cut failed"))
        with self.assertRaisesRegex(N.H.Failure, "RDB cut failed"):
            self.wait()

    def test_early_backup_completion_is_not_a_held_cut(self):
        self.backup.set_result(True)
        with self.assertRaisesRegex(
            N.H.Failure, "backup finished before backup release"
        ):
            self.wait()
        self.assertEqual(self.now, 0)

    def test_missing_checkpoint_still_times_out(self):
        with self.assertRaisesRegex(N.H.Failure, "timeout .*database gates closed"):
            self.wait()
        self.assertGreaterEqual(self.now, 10)


if __name__ == "__main__":
    unittest.main()
