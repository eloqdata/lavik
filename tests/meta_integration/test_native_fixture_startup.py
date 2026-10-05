#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

"""Keep native FULL fixtures behind the committed Meta bootstrap boundary."""

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
    def check_startup(self, scenario):
        membership_stable = False
        status_calls = 0

        def cluster_status(_deadline):
            nonlocal membership_stable, status_calls
            status_calls += 1
            membership_stable = status_calls > 1
            return {"meta_membership_stable": membership_stable}

        def start_data():
            # A leader may expose a partially committed initial directory.
            # Data correctly rejects that response if its Meta is absent.
            if not membership_stable:
                raise N.H.Failure(
                    "bootstrap Meta identity absent from committed directory"
                )

        leader = SimpleNamespace(
            data_control_endpoint="127.0.0.1:12345",
            ctl_endpoint="127.0.0.1:12346",
        )
        fixture = SimpleNamespace(
            manifest="unused-manifest",
            metas=[SimpleNamespace(start=Mock()) for _ in range(3)],
            data_nodes=[
                SimpleNamespace(start=Mock(side_effect=start_data)) for _ in range(3)
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
        ):
            # Exercise the scenario's actual orchestration. Election alone is
            # insufficient: one pending membership response must be tolerated
            # before any Data process bootstraps or Cluster Create is issued.
            with self.assertRaises(ReachedClusterCreate):
                scenario(Path(directory))
        self.assertGreaterEqual(status_calls, 2)
        for node in fixture.data_nodes:
            node.start.assert_called_once()
            self.assertEqual(node.seed, leader.data_control_endpoint)
        fixture.force_kill.assert_called_once()

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
            patch.object(C.H, "free_port", return_value=12345),
        ):
            D.commit_service_mode(meta, [meta])
        request.assert_called_once()
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


if __name__ == "__main__":
    unittest.main()
