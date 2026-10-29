#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Fault injection must exclude late connections and allow pre-cut Raft work."""

import socket
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import gate_transport as T
import harness as H


class ProxyFaultTest(unittest.TestCase):
    def sockets(self):
        pair = socket.socketpair()
        for sock in pair:
            sock.settimeout(2)
            self.addCleanup(H.Proxy._close_sock, sock)
        return pair

    def proxy(self):
        proxy = H.Proxy("test", 1)
        proxy.start()
        self.addCleanup(proxy.close)
        return proxy

    def dial_across(self, transition):
        proxy = self.proxy()
        client, accepted = self.sockets()
        upstream, target = self.sockets()
        dialing, resume = threading.Event(), threading.Event()

        def connect(*_args, **_kwargs):
            dialing.set()
            if not resume.wait(2):
                raise TimeoutError("test did not release dial")
            return upstream

        # Queue data before the cut: a late pair must be discarded, including
        # when the fault has already healed by the time its dial finishes.
        client.sendall(b"stale")
        with patch.object(H.socket, "create_connection", side_effect=connect):
            thread = threading.Thread(target=proxy._on_accept, args=(accepted,))
            thread.start()
            try:
                self.assertTrue(dialing.wait(2))
                transition(proxy)
            finally:
                resume.set()
                thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(target.recv(16), b"")

    def test_inflight_dial_cannot_survive_drop_or_refuse(self):
        for mode in ("drop", "refuse"):
            with self.subTest(mode=mode):
                self.dial_across(lambda p: getattr(p, f"set_{mode}")())

    def test_heal_does_not_resurrect_a_pre_fault_dial(self):
        for mode in ("drop", "refuse"):
            with self.subTest(mode=mode):

                def fault_then_heal(proxy):
                    getattr(proxy, f"set_{mode}")()
                    proxy.heal()

                self.dial_across(fault_then_heal)

    def test_close_discards_an_inflight_dial(self):
        self.dial_across(lambda p: p.close())

    def test_refuse_rejects_an_already_accepted_socket_without_dialing(self):
        proxy = self.proxy()
        client, accepted = self.sockets()
        upstream, _target = self.sockets()
        proxy.set_refuse()
        with patch.object(H.socket, "create_connection", return_value=upstream) as dial:
            proxy._on_accept(accepted)
        dial.assert_not_called()
        self.assertEqual(client.recv(1), b"")

    def test_drop_holds_without_dialing_and_heal_releases_the_socket(self):
        proxy = self.proxy()
        client, accepted = self.sockets()
        proxy.set_drop()
        with patch.object(H.socket, "create_connection") as dial:
            proxy._on_accept(accepted)
        dial.assert_not_called()
        proxy.heal()
        self.assertEqual(client.recv(1), b"")

    def test_established_pairs_are_cut_and_new_pairs_work_after_heal(self):
        proxy = self.proxy()
        for payload in (b"before", b"after"):
            client, accepted = self.sockets()
            upstream, target = self.sockets()
            with patch.object(H.socket, "create_connection", return_value=upstream):
                proxy._on_accept(accepted)
            client.sendall(payload)
            self.assertEqual(target.recv(16), payload)
            target.sendall(payload)
            self.assertEqual(client.recv(16), payload)
            proxy.set_drop()
            self.assertEqual(client.recv(1), b"")
            self.assertEqual(target.recv(1), b"")
            proxy.heal()

    def test_refuse_during_listener_creation_cannot_publish_the_listener(self):
        opening, resume, closed = (threading.Event() for _ in range(3))

        class SlowListener(H.Proxy):
            def _open_listener(self):
                listener = super()._open_listener()
                self.opening_listener = listener
                opening.set()
                if not resume.wait(2):
                    listener.close()
                    raise TimeoutError("test did not release listener creation")
                return listener

            def _close_sock(self, sock):
                super()._close_sock(sock)
                if sock is getattr(self, "opening_listener", None):
                    closed.set()

        proxy = SlowListener("listener-test", 1)
        self.addCleanup(proxy.close)
        proxy.start()
        try:
            self.assertTrue(opening.wait(2))
            proxy.set_refuse()
        finally:
            resume.set()
        self.assertTrue(closed.wait(2), "listener escaped the refuse transition")


class TransportOracleTest(unittest.TestCase):
    def run_round(
        self, *, follower_index=174, mode="drop", rpc_evidence=True, quorum_end=400
    ):
        now = [0.0]
        fault = [False]
        proposal_times = []

        def sleep(seconds):
            now[0] += seconds

        proxy = Mock()
        proxy.set_drop.side_effect = lambda: fault.__setitem__(0, True)
        proxy.set_refuse.side_effect = lambda: fault.__setitem__(0, True)
        leader, follower, other = [Mock(id=i) for i in (1, 2, 3)]
        leader.is_leader.return_value = True
        other.is_leader.return_value = False
        follower.committed.side_effect = lambda: (
            (follower_index(now[0]) if callable(follower_index) else follower_index)
            if fault[0]
            else 100
        )

        def propose(_payload):
            self.assertTrue(fault[0], "probe must be submitted after isolation")
            proposal_times.append(now[0])
            return "probe", "OK 200"

        leader.propose.side_effect = propose
        mesh = SimpleNamespace(proxy=lambda _id: proxy)
        history = Mock()
        clock = SimpleNamespace(monotonic=lambda: now[0], sleep=sleep)
        # Evidence arrives during the grace period. The final isolation check
        # must still reject a leak that becomes visible during that grace.
        evidence = lambda _nodes: int(rpc_evidence and now[0] >= 4.1)
        with (
            patch.object(T, "time", clock),
            patch.object(H, "find_leader", return_value=leader),
            patch.object(H, "max_committed", side_effect=[150, quorum_end]),
            patch.object(T, "rpc_failure_count", side_effect=evidence),
            patch.object(T, "timed_catchup") as catchup,
            patch.object(H, "assert_intact"),
            patch.object(H, "log"),
        ):
            T.fault_round([leader, follower, other], mesh, history, mode, 4, "test")
        self.assertLess(proposal_times[0], 4, "probe must precede observation")
        self.assertGreaterEqual(now[0] - proposal_times[0], 4)
        proxy.heal.assert_called_once()
        catchup.assert_called_once()
        history.check.assert_called_once()

    def test_pre_cut_backlog_can_advance_by_more_than_ten(self):
        for mode in ("drop", "refuse"):
            with self.subTest(mode=mode):
                self.run_round(mode=mode)

    def test_committing_the_post_cut_probe_is_rejected(self):
        with self.assertRaisesRegex(H.Failure, "post-fault probe"):
            self.run_round(follower_index=200)

    def test_probe_leaking_during_counter_grace_is_rejected(self):
        with self.assertRaisesRegex(H.Failure, "post-fault probe"):
            self.run_round(follower_index=lambda now: 200 if now >= 4.1 else 100)

    def test_missing_transport_failure_evidence_still_fails(self):
        with self.assertRaisesRegex(H.Failure, "no peer RPC failure evidence"):
            self.run_round(rpc_evidence=False)

    def test_stalled_quorum_still_fails(self):
        with self.assertRaisesRegex(H.Failure, "quorum stalled"):
            self.run_round(quorum_end=150)


if __name__ == "__main__":
    unittest.main()
