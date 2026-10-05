#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exercise gate orchestration across valid asynchronous leadership windows."""

from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import gate_leader_change as L
import gate_sentinel as S


class MetaGateReadinessTest(unittest.TestCase):
    def test_wire_replay_waits_for_discovery_authority(self):
        # Raft can report leader before the control worker publishes its term
        # and eligibility. Drive the real wire-test orchestration through that
        # window; no RESP retry may hide a failed contract exchange.
        ready = False
        calls = 0

        def status(command):
            nonlocal ready, calls
            self.assertEqual(command, "clusterstatus 1")
            calls += 1
            ready = calls % 2 == 0
            return "OK clusterstatus 1 fixture" if ready else "ERR leader_not_caught_up"

        node = SimpleNamespace(is_leader=lambda: True, ctl=status, terminate=Mock())

        def make_node(**kwargs):
            nonlocal ready
            ready = False
            return node, 12345

        def replay(*_args):
            if not ready:
                raise AssertionError("truncated RESP line: b''")

        gate = S.SentinelTest("test_redis72_wire_contract")
        with (
            patch.object(gate, "node", side_effect=make_node),
            patch.object(S.sentinel_compat, "check_port"),
            patch.object(
                S.sentinel_compat, "check_discovery_port", side_effect=replay
            ) as wire,
        ):
            gate.test_redis72_wire_contract()
        self.assertEqual(wire.call_count, 2)
        self.assertEqual(calls, 4)

    def run_link_fault(self, replies):
        nodes = [
            SimpleNamespace(
                id=i,
                alive=lambda: True,
                is_leader=lambda: True,
                propose=Mock(return_value=(f"op-{i}", reply)),
            )
            for i, reply in enumerate(replies, 1)
        ]
        proxy = Mock()
        mesh = SimpleNamespace(proxy=lambda _id: proxy)
        history, probe_history = Mock(), Mock()
        with (
            patch.object(L.H, "max_committed", return_value=12),
            patch.object(L.H, "CommittedHistory", return_value=probe_history),
            patch.object(L.time, "sleep"),
        ):
            L.link_fault_round(nodes, mesh, history, nodes[0], nodes[0], "refuse", 0)
        return nodes, history, probe_history

    def test_healed_link_reselects_leader_after_not_leader(self):
        nodes, history, probe_history = self.run_link_fault(["ERR not-leader", "OK 13"])
        for node in nodes:
            node.propose.assert_called_once_with("post-refuse", timeout=1)
        probe_history.record.assert_called_once_with("op-2", "post-refuse")
        probe_history.check.assert_called_once_with(
            nodes, timeout=30, desc="post-refuse probe"
        )
        history.record.assert_not_called()
        history.check.assert_called_once_with(nodes, timeout=30, desc="post-refuse")

    def test_healed_link_reselects_after_cancelled_proposal(self):
        nodes, history, probe_history = self.run_link_fault(["ERR cancelled", "OK 13"])
        probe_history.record.assert_called_once_with("op-2", "post-refuse")
        history.record.assert_not_called()

    def test_healed_link_does_not_hide_other_proposal_errors(self):
        for reply in ("ERR rejected", "ERR timeout", "ERR propose-failed"):
            with (
                self.subTest(reply=reply),
                self.assertRaisesRegex(AssertionError, reply),
            ):
                self.run_link_fault([reply])

    def test_healed_link_requires_progress_before_deadline(self):
        now = 0.0

        def sleep(duration):
            nonlocal now
            now += duration

        node = SimpleNamespace(
            id=1,
            alive=lambda: True,
            is_leader=lambda: True,
            propose=Mock(return_value=("uncertain-op", "ERR not-leader")),
        )
        history, probe_history = Mock(), Mock()
        with (
            patch.object(L.H, "max_committed", return_value=12),
            patch.object(L.H, "CommittedHistory", return_value=probe_history),
            patch.object(L.time, "monotonic", side_effect=lambda: now),
            patch.object(L.time, "sleep", side_effect=sleep),
            self.assertRaisesRegex(L.H.Failure, "a fresh write commits"),
        ):
            L.link_fault_round([node], Mock(), history, node, node, "refuse", 0)
        history.record.assert_not_called()
        history.check.assert_not_called()
        probe_history.record.assert_not_called()
        probe_history.check.assert_not_called()


if __name__ == "__main__":
    unittest.main()
