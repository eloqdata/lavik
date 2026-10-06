#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

"""Keep Data-control Genesis behind committed Meta identity recovery."""

from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import gate_cluster_create as C
import gate_data_control as D


class DataControlFixtureStartupTest(unittest.TestCase):
    def test_waits_for_membership_before_committing_service_mode(self):
        stable = False
        status_calls = 0

        def cluster_status(_leader):
            nonlocal stable, status_calls
            status_calls += 1
            stable = status_calls > 1
            return {"meta_membership_stable": stable}

        def create(_request, *, timeout):
            self.assertGreater(timeout, 0)
            self.assertLessEqual(timeout, 5)
            if not stable:
                return (
                    "ERR clustercreate 1 preflight pre-commit-failed "
                    "another cluster creation or Meta membership change is in progress"
                )
            return "OK clustercreate 1 operation-id"

        leader = SimpleNamespace(ctl=Mock(side_effect=create))
        with (
            patch.object(C, "cluster_status", side_effect=cluster_status),
            patch.object(C, "create_request", return_value="create-request"),
        ):
            D.commit_service_mode(leader, [leader])
        self.assertGreaterEqual(status_calls, 2)
        leader.ctl.assert_called_once_with("create-request", timeout=5.0)


if __name__ == "__main__":
    unittest.main()
