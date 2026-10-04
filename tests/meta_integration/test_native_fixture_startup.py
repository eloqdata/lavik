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


if __name__ == "__main__":
    unittest.main()
