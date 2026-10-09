#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Startup probes tolerate slow progress while retaining failure deadlines."""

import json
import subprocess
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import gate_cluster_create as C
import gate_sentinel_discovery as S
import gate_transport as T


class ClusterStatusProbeTest(unittest.TestCase):
    def setUp(self):
        self.meta = SimpleNamespace(ctl_path="/test/meta.sock")
        self.cli = patch.object(C, "CTL", "/test/lavik-ctl", create=True)
        self.cli.start()
        self.addCleanup(self.cli.stop)

    def test_cli_can_report_unavailable_before_process_watchdog(self):
        status = {
            "result": "retryable",
            "meta_membership_stable": False,
            "retry": {"reason": "ERR leader_not_caught_up"},
        }

        def deadline_result(argv, **kwargs):
            cli_ms = (
                int(argv[argv.index("--timeout-ms") + 1])
                if "--timeout-ms" in argv
                else 5000
            )
            # A legitimate not-caught-up retry consumes the entire CLI budget.
            # The old equal deadlines killed it before it could report JSON.
            if cli_ms / 1000 >= kwargs["timeout"]:
                raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
            return subprocess.CompletedProcess(argv, 3, json.dumps(status), "")

        with patch.object(C.subprocess, "run", side_effect=deadline_result):
            with self.assertRaisesRegex(C.H.Failure, "ERR leader_not_caught_up"):
                C.cluster_status(self.meta)

    def test_process_watchdog_allows_alternate_sentinel_seed(self):
        first, second = Mock(), Mock()
        first.ctl_path, second.ctl_path = "/first.sock", "/second.sock"
        fixture = S.DiscoveryFixture.__new__(S.DiscoveryFixture)
        fixture.leader, fixture.metas = first, [first, second]
        status = {"meta_membership_stable": True}
        replies = [
            subprocess.TimeoutExpired("lavik-ctl", 5),
            subprocess.CompletedProcess([], 0, json.dumps(status), ""),
        ]
        with patch.object(C.subprocess, "run", side_effect=replies) as run:
            self.assertEqual(fixture.cluster_status(), status)
        self.assertEqual(run.call_count, 2)
        self.assertIn("/second.sock", run.call_args.args[0])

    def test_all_timed_out_seeds_remain_a_failed_probe(self):
        with patch.object(
            C.subprocess,
            "run",
            side_effect=subprocess.TimeoutExpired("lavik-ctl", 5),
        ):
            with self.assertRaisesRegex(
                C.H.Failure, "cluster-status process timed out"
            ):
                C.cluster_status(self.meta)

    def test_nonretryable_cli_failure_is_not_success(self):
        reply = subprocess.CompletedProcess([], 1, "", "invalid TLS credentials")
        with patch.object(C.subprocess, "run", return_value=reply):
            with self.assertRaisesRegex(C.H.Failure, "invalid TLS credentials"):
                C.cluster_status(self.meta)


class LoadReadinessTest(unittest.TestCase):
    def run_probe(self, ready_at):
        now = [0.0]

        def sleep(seconds):
            now[0] += seconds

        class Load:
            @property
            def ok_count(self):
                return 5 if now[0] >= ready_at else 4

            def stats(self):
                return f"ok={self.ok_count} errors: none"

        clock = SimpleNamespace(monotonic=lambda: now[0], sleep=sleep)
        with patch.object(T.H, "time", clock):
            T.wait_for_load(Load(), timeout=10)
        return now[0]

    def test_slow_successful_writes_can_finish_after_one_second(self):
        elapsed = self.run_probe(ready_at=1.5)
        self.assertGreaterEqual(elapsed, 1.5)
        self.assertLess(elapsed, 10)

    def test_stalled_load_still_fails_with_progress_diagnostics(self):
        with self.assertRaisesRegex(T.H.Failure, "ok=4 errors: none"):
            self.run_probe(ready_at=float("inf"))

    def test_ready_load_needs_no_fixed_sleep(self):
        self.assertEqual(self.run_probe(ready_at=0), 0)


if __name__ == "__main__":
    unittest.main()
