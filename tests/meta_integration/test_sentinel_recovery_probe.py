#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Recovery deadlines belong to independent clients, not their probe order."""

import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import gate_sentinel_ha as HA


class RecoveryProbeTest(unittest.TestCase):
    def test_all_clients_begin_before_any_discovery_completes(self):
        ready = threading.Barrier(4, timeout=2)
        clients = []
        for index in range(4):
            # A serial implementation breaks this barrier. No wall-clock
            # throughput threshold or real network service is needed.
            client = Mock()
            client.set.side_effect = lambda *_: ready.wait()
            client.get.return_value = b"fault"
            clients.append((str(index), client))
        with patch.object(HA.H, "log"):
            HA.recover(clients, "fault")
        for _, client in clients:
            client.set.assert_called_once()
            client.get.assert_called_once()

    def test_success_after_original_fault_deadline_is_rejected(self):
        now = [129.0]
        client = Mock()

        def late_get(_key):
            now[0] = 131.0
            return b"fault"

        client.get.side_effect = late_get
        clock = SimpleNamespace(monotonic=lambda: now[0], sleep=Mock())
        with patch.object(HA, "time", clock), patch.object(HA.H, "log") as log:
            with self.assertRaisesRegex(HA.H.Failure, "successful command exceeded"):
                HA.recover([("late", client)], "fault", start=100.0)
        log.assert_not_called()
        client.set.assert_called_once()

    def test_expired_fault_budget_does_not_start_another_command(self):
        client = Mock()
        clock = SimpleNamespace(monotonic=lambda: 131.0, sleep=Mock())
        with patch.object(HA, "time", clock):
            with self.assertRaisesRegex(HA.H.Failure, "30s budget exceeded"):
                HA.recover([("expired", client)], "fault", start=100.0)
        client.set.assert_not_called()

    def test_transient_failure_is_retried_within_same_budget(self):
        client = Mock()
        client.set.side_effect = [ConnectionError("disconnected"), None]
        client.get.return_value = "fault"
        clock = SimpleNamespace(monotonic=lambda: 101.0, sleep=Mock())
        with patch.object(HA, "time", clock), patch.object(HA.H, "log") as log:
            HA.recover([("retry", client)], "fault", start=100.0)
        self.assertEqual(client.set.call_count, 2)
        self.assertIn("disconnected", log.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
