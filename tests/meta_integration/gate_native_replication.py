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

"""Native FULL, live replay and reconnect through real Meta authorization.

The old standalone REPLICAOF fixtures cannot authorize a native session. These
checks use the same manifest/bootstrap and directive barrier as cluster-create;
all native connections are admitted by production Meta and Follow Owner.
"""

from contextlib import contextmanager
import os
import concurrent.futures
from pathlib import Path
import re
import signal
import struct
import sys
import tempfile
import threading
import time

import gate_cluster_create as C
import harness as H
from gate_data_control import DataProcess

CLIENT_MODE = "cluster"


class Client:
    def __init__(self, node, readonly=False):
        self.context = C.redis_connection(node)
        self.socket = self.context.__enter__()
        self.socket.settimeout(30)
        self.reader = self.socket.makefile("rb")
        if readonly:
            assert self.call("READONLY") == "OK"

    def call(self, *args, decode=True):
        values = [arg if isinstance(arg, bytes) else str(arg).encode() for arg in args]
        self.socket.sendall(
            f"*{len(values)}\r\n".encode()
            + b"".join(
                f"${len(value)}\r\n".encode() + value + b"\r\n" for value in values
            )
        )
        return C.read_resp(self.reader, decode=decode)

    def close(self):
        self.reader.close()
        self.context.__exit__(None, None, None)


@contextmanager
def pair(
    root,
    name,
    source_faults=None,
    target_faults=None,
    seed=None,
    source_workers=2,
    target_workers=3,
    raft_args=None,
    require_seed_before_full=False,
    client_mode=None,
    prepare_target=None,
    source_extra_args=(),
):
    client_mode = client_mode or CLIENT_MODE
    directory = root / name
    directory.mkdir()
    if raft_args is None:
        # Meta caps grants at the Raft election lower bound. The 500 ms
        # cluster-create fixture can legitimately expire a healthy Owner's
        # lease during slow FULL or hosted-runner scheduling, which now retires
        # every persistent test client. Keep these unrelated replication
        # checks on a two-second bound; expiry gates supply short args.
        raft_args = H.raft_args(
            snapshot_distance=100_000, election_ms_low=2000, election_ms_high=4000
        )
    meta = H.Node(
        C.META,
        str(directory),
        1,
        args=raft_args,
    )
    proxy = C.DirectiveBarrier(meta.data_control_port, recipients=(C.REPLICA_1,))
    meta.advertised_data_control_endpoint = proxy.endpoint
    source = DataProcess(
        C.DATA,
        str(directory / "source"),
        C.PRIMARY_1,
        proxy.endpoint,
        workers=source_workers,
        environment={**os.environ, **(source_faults or {})},
        extra_args=source_extra_args,
    )
    target = DataProcess(
        C.DATA,
        str(directory / "target"),
        C.REPLICA_1,
        proxy.endpoint,
        workers=target_workers,
        environment={**os.environ, **(target_faults or {})},
    )
    lines = [
        "schema_version = 1",
        f'client_mode = "{client_mode}"',
        'slot_strategy = "contiguous-even"',
    ]
    lines += C.meta_manifest_lines(meta)
    for node in (source, target):
        lines += [
            "[[data_nodes]]",
            f'id = "{node.node_id}"',
            f'client_endpoint = "{node.advertised_endpoint}"',
        ]
    lines += [
        "[[groups]]",
        'id = "group-1"',
        f'primary = "{source.node_id}"',
        f'replicas = ["{target.node_id}"]',
    ]
    manifest = directory / "cluster.toml"
    manifest.write_text("\n".join(lines) + "\n")
    clients = []
    try:
        if prepare_target is not None:
            prepare_target(target)
        proxy.start()
        meta.start(initial_cluster_manifest=str(manifest))
        meta.wait_leader()
        source.start()
        target.start()
        C.command(
            os.environ.copy(),
            [
                C.CTL,
                "cluster-create",
                "--manifest",
                str(manifest),
                "--socket",
                meta.ctl_path,
                "--yes",
            ],
        )
        held, release = proxy.recipients[C.REPLICA_1]
        H.wait_until("native target directive held", 30, held.is_set)
        if require_seed_before_full:
            # The one-frame barrier intentionally permits later sessions to
            # reconnect. Pause this registered target while a larger seed is
            # built so no reconnect can turn the FULL test into live replay.
            # Retain its boot identity throughout the initialization workflow.
            target.pause()
        writer = Client(source)
        clients.append(writer)
        H.wait_until(
            "source authority before FULL",
            20,
            lambda: writer.call("SET", "{native}seed", "baseline") == "OK",
        )
        if seed:
            seed(writer)
        if require_seed_before_full:
            assert (
                "replication target session" not in Path(target.log_path).read_text()
            ), "target started FULL before the seed finished"
        release.set()
        if require_seed_before_full:
            target.resume()
        yield meta, source, target, writer
        for client in clients:
            client.close()
        clients.clear()
        target.terminate()
        source.terminate()
        meta.terminate()
    except BaseException:
        H.dump_node_logs([meta])
        for node in (source, target):
            print(node.log_tail(lines=150), file=sys.stderr)
        raise
    finally:
        for client in clients:
            client.close()
        proxy.close()
        target.force_kill()
        source.force_kill()
        meta.force_kill()


def ready(meta):
    C.wait_cluster_ready(meta, "native population and authority ready", 90)


def tomb_raider(root):
    def info(node, section="stats"):
        probe = Client(node)
        try:
            return dict(
                line.split(":", 1)
                for line in probe.call("INFO", section).splitlines()
                if ":" in line
            )
        finally:
            probe.close()

    with pair(root, "tomb-raider") as (meta, source, target, writer):
        ready(meta)
        H.wait_until(
            "complete native follower",
            30,
            lambda: info(target, "replication").get("master_link_status") == "up",
        )
        reader = Client(target, readonly=True)
        keys = [f"{{native}}tomb-raider-{index}" for index in range(24)]
        try:
            for client in (writer, reader):
                assert client.call("CONFIG", "SET", "tomb-raider-mode", "off") == "OK"
                assert client.call("CONFIG", "SET", "tomb-raider-sleep-ms", 0) == "OK"
                assert client.call("DEFRAG", "PAUSE") == "OK"
            before_source = int(info(source)["tomb_raider_reaped"])
            before_target = int(info(target)["tomb_raider_reaped"])
            for key in keys:
                assert writer.call("SET", key, "buried-value", "PX", 6000) == "OK"
            assert writer.call("WAIT", 1, 5000) == 1
            assert reader.call("MGET", *keys) == ["buried-value"] * len(keys)
            assert writer.call("DEL", *keys) == len(keys)
            assert writer.call("WAIT", 1, 5000) == 1
            assert reader.call("EXISTS", *keys) == 0
            assert writer.call("CONFIG", "SET", "tomb-raider-interval-ms", 20) == "OK"
            H.wait_until(
                "managed Owner physically reaps expired buried values",
                20,
                lambda: int(info(source)["tomb_raider_reaped"])
                >= before_source + len(keys),
            )
            assert int(info(target)["tomb_raider_reaped"]) == before_target
        finally:
            reader.close()

        # Keep Meta from electing a different Owner while isolating the complete
        # follower. No live source transport or serving lease is needed for its
        # local reclamation proof. Fresh INFO clients survive serving retirement.
        meta.pause()
        try:
            assert writer.call("CLIENT", "KILL", "TYPE", "replica") > 0
            source.pause()
            try:
                H.wait_until(
                    "complete follower disconnected",
                    10,
                    lambda: info(target, "replication").get("master_link_status")
                    == "down",
                )
                probe = Client(target)
                try:
                    assert (
                        probe.call("CONFIG", "SET", "tomb-raider-interval-ms", 20)
                        == "OK"
                    )
                finally:
                    probe.close()
                H.wait_until(
                    "disconnected managed follower physically reaps",
                    20,
                    lambda: int(info(target)["tomb_raider_reaped"])
                    >= before_target + len(keys),
                )
                assert info(target)["tomb_raider_eligible"] == "1"
            finally:
                source.resume()
            H.wait_until(
                "Owner serving lease expired",
                10,
                lambda: source.command_head(["SET", "{native}lease-probe", "x"]).split(
                    " ", 1
                )[0]
                in ("-LOADING", "-CLUSTERDOWN"),
            )
            source_round = int(info(source)["tomb_raider_rounds"])
            H.wait_until(
                "physical maintenance continues without Owner authority",
                10,
                lambda: int(info(source)["tomb_raider_rounds"]) > source_round,
            )
            assert info(source)["tomb_raider_eligible"] == "1"
        finally:
            meta.resume()
        ready(meta)
        H.wait_until(
            "native follower reconnected after reclamation",
            30,
            lambda: info(target, "replication").get("master_link_status") == "up",
        )
        for node in (source, target):
            probe = Client(node, readonly=node == target)
            try:
                assert probe.call("EXISTS", *keys) == 0
                assert probe.call("GET", "{native}seed") == "baseline"
                assert info(node)["tomb_raider_enabled"] == "1"
            finally:
                probe.close()


def full_session_lifecycle(root):
    # A session owns one FULL across unequal source/target worker layouts.
    # Source work can outlive its control connection, while a completed FULL
    # must stop counting even though all ONLINE connections remain open.
    for cancel in (False, True):
        with pair(
            root,
            "full-lifecycle-cancel" if cancel else "full-lifecycle-complete",
            source_faults={"LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "5000"},
        ) as (meta, source, target, writer):
            H.wait_until(
                "FULL source work paused",
                30,
                lambda: "paused full sync after acknowledged handoff partition"
                in Path(source.log_path).read_text(),
            )
            assert "lavik_full_sync_sessions:1\r\n" in writer.call(
                "INFO", "replication"
            )
            if cancel:
                target.force_kill()
                H.wait_until(
                    "FULL control retired before source flow drained",
                    3,
                    lambda: "connected_slaves:0\r\n"
                    in writer.call("INFO", "replication"),
                )
                assert "lavik_full_sync_sessions:1\r\n" in writer.call(
                    "INFO", "replication"
                )
            else:
                ready(meta)
            H.wait_until(
                "FULL work released",
                30,
                lambda: "lavik_full_sync_sessions:0\r\n"
                in writer.call("INFO", "replication"),
            )
            if not cancel:
                assert ",state=online," in writer.call("INFO", "replication")
                reader = Client(target, readonly=True)
                try:
                    assert reader.call("GET", "{native}seed") == "baseline"
                finally:
                    reader.close()


def full_completion_reconnect(root):
    hold = root / "promotion-ack.hold"

    def seed(writer):
        # Both flows need a non-initial continuation cursor even if the first
        # control connection closes before it supplies an ORIGIN capability.
        for key in ("cut-counter-{foo}", "cut-counter-{user1000}"):
            writer.call("SET", key, "baseline")

    try:
        with pair(
            root,
            "full-cut-reconnect",
            source_faults={"LAVIK_FULL_AFTER_PROMOTION_ACK_HOLD_FILE": str(hold)},
            seed=seed,
        ) as (meta, source, target, writer):
            ready(meta)
            H.wait_until(
                "initial population is ONLINE",
                30,
                lambda: ",state=online," in writer.call("INFO", "replication"),
            )
            # Exercise steady following after genesis has removed its explicit
            # directives. Their retirement intentionally joins old exports.
            hold.touch()
            target.force_kill()
            target.environment = {
                **os.environ,
                "LAVIK_REPLICATION_DROP_AFTER_FULLSYNC_CUT": "1",
            }
            target.start()
            H.wait_until(
                "old FULL holds acknowledged promotion during cancellation",
                30,
                lambda: "paused after promotion acknowledgement"
                in Path(source.log_path).read_text(),
            )
            H.wait_until(
                "replacement CONTINUE is ONLINE beside old FULL drain",
                30,
                lambda: ",state=online," in writer.call("INFO", "replication"),
            )
            assert "lavik_full_sync_sessions:1\r\n" in writer.call(
                "INFO", "replication"
            )
            hold.unlink()
            H.wait_until(
                "late old-session completion releases only old FULL",
                30,
                lambda: "lavik_full_sync_sessions:0\r\n"
                in writer.call("INFO", "replication"),
            )
            info = writer.call("INFO", "replication")
            assert "connected_slaves:1\r\n" in info and ",state=online," in info
            ready(meta)
            writer.call("SET", "{native}seed", "after-old-drain")
            H.wait_until(
                "replacement still applies after old callback",
                20,
                lambda: C.readonly_get(target, "{native}seed") == "after-old-drain",
            )
    finally:
        hold.unlink(missing_ok=True)


def follow_full_limit(root):
    import gate_failover as F

    for cancel in (False, True):
        name = "follow-full-failure" if cancel else "follow-full-success"
        fixture = F.FailoverFixture(
            C.META,
            C.DATA,
            C.CTL,
            str(root / name),
            False,
            data_workers=2,
            four_data=True,
            client_mode=CLIENT_MODE,
        )
        source, first, second, healthy = fixture.data_nodes
        arm = root / (name + ".arm")
        source.environment = {
            **os.environ,
            "LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "10000",
            "LAVIK_REPLICATION_FULLSYNC_PAUSE_ARM_FILE": str(arm),
        }
        continue_key = "quota-continue-{foo}"
        healthy.environment = {
            **os.environ,
            "LAVIK_REPLICATION_CANCEL_PEER_FLOW_AFTER_COMMAND_APPLY_ONCE": continue_key,
        }
        writer = None
        try:
            fixture.start_created()
            writer = Client(source)
            for key in ("quota-{foo}", "quota-{user1000}"):
                writer.call("SET", key, "before-full")
                H.wait_until(
                    "every follower has resumable data",
                    20,
                    lambda: all(
                        C.readonly_get(n, key) == "before-full"
                        for n in (first, second, healthy)
                    ),
                )
            first.force_kill()
            second.force_kill()
            H.wait_until(
                "only healthy ONLINE remains",
                10,
                lambda: "connected_slaves:1\r\n"
                in C.redis_call(source, ["INFO", "replication"]),
            )
            arm.touch()
            source_log_start = len(Path(source.log_path).read_text())
            first.start()
            H.wait_until(
                "first automatic FULL holds source work",
                30,
                lambda: "paused full sync after acknowledged handoff partition"
                in Path(source.log_path).read_text()[source_log_start:],
            )
            second_log_start = len(Path(second.log_path).read_text())
            second.start()
            H.wait_until(
                "second automatic FULL receives busy",
                10,
                lambda: "native FULL admission is busy"
                in Path(second.log_path).read_text()[second_log_start:],
            )
            assert (
                "durably invalidated system state"
                not in Path(second.log_path).read_text()[second_log_start:]
            )
            assert "lavik_full_sync_sessions:1\r\n" in writer.call(
                "INFO", "replication"
            )
            # Existing ONLINE traffic and a fresh CONTINUE must pass a held FULL.
            continue_before = (
                Path(source.log_path).read_text().count("selected=CONTINUE")
            )
            # Trigger the healthy target's own existing cancellation seam. A
            # cached source CLIENT id can retire during unrelated reconnects.
            assert writer.call("SET", continue_key, "reconnect") == "OK"
            H.wait_until(
                "healthy follower cancels its own native session",
                5,
                lambda: "injected peer-flow session cancellation after command apply"
                in Path(healthy.log_path).read_text(),
            )
            H.wait_until(
                "healthy follower continues beside FULL",
                5,
                lambda: Path(source.log_path).read_text().count("selected=CONTINUE")
                > continue_before,
            )
            if cancel:
                first.force_kill()
                time.sleep(0.2)
                assert "lavik_full_sync_sessions:1\r\n" in writer.call(
                    "INFO", "replication"
                )
            else:
                # A new Meta control session must retain source live accounting.
                fixture.leader.force_kill()
                H.wait_until(
                    "Meta replacement leader",
                    10,
                    lambda: fixture.rediscover_leader(time.monotonic() + 1),
                )
            arm.unlink()
            H.wait_until(
                "waiting follower admitted after source work drains",
                60,
                lambda: "durably activated population"
                in Path(second.log_path).read_text()[second_log_start:]
                and "master_link_status:up\r\n"
                in C.redis_call(second, ["INFO", "replication"])
                and C.readonly_get(second, "quota-{foo}") == "before-full",
            )
            H.wait_until(
                "FULL quota released while completed exports remain ONLINE",
                30,
                lambda: "lavik_full_sync_sessions:0\r\n"
                in C.redis_call(source, ["INFO", "replication"]),
            )
            if cancel:
                assert not first.alive()
            else:
                H.wait_until(
                    "first FULL still serves after next admission",
                    20,
                    lambda: C.readonly_get(first, "quota-{foo}") == "before-full",
                )
            # Readable retained data and source slot release do not establish
            # a new causal serving lease after Meta replacement.
            H.wait_until(
                "source serving authority after control replacement",
                20,
                lambda: C.redis_call(source, ["SET", "quota-{foo}", "after-full"])
                == "OK",
            )
            H.wait_until(
                "healthy and rebuilt followers receive tail",
                20,
                lambda: all(
                    C.readonly_get(n, "quota-{foo}") == "after-full"
                    for n in (second, healthy)
                ),
            )
        except BaseException:
            fixture.dump_logs()
            raise
        finally:
            arm.unlink(missing_ok=True)
            if writer is not None:
                writer.close()
            fixture.force_kill()


def explicit_full_limit(root):
    import gate_failover as F

    for cancel in (False, True):
        name = "explicit-full-cancel" if cancel else "explicit-full-leader"
        fixture = F.FailoverFixture(
            C.META,
            C.DATA,
            C.CTL,
            str(root / name),
            False,
            data_workers=2,
            client_mode=CLIENT_MODE,
        )
        # Keep the deliberate election plus lease quarantine inside the
        # separate three-second lease retry budget. Busy itself is unbounded.
        for meta in fixture.metas:
            meta.args = H.raft_args(
                snapshot_distance=100_000, election_ms_low=700, election_ms_high=1400
            )
        source, first, second = fixture.data_nodes
        hold = root / (name + ".hold")
        hold.touch()
        source.environment = {
            **os.environ,
            "LAVIK_FULL_AFTER_PROMOTION_ACK_HOLD_FILE": str(hold),
        }
        try:
            F.write_manifest(
                fixture.manifest,
                fixture.metas,
                fixture.data_nodes,
                client_mode=CLIENT_MODE,
                automatic_uncontrolled_failover_suspect_after_ms=600_000,
            )
            for meta in fixture.metas:
                meta.start(initial_cluster_manifest=fixture.manifest, wait_ready=False)
            fixture.leader = H.find_leader(fixture.metas, timeout=20)
            # Election precedes initial identity reconciliation. A partial
            # committed directory can omit the leader itself, which correctly
            # makes Data bootstrap fail closed before storage opens.
            H.wait_until(
                "Meta membership stable before Data bootstrap",
                15,
                lambda: fixture.cluster_status(time.monotonic() + 2).get(
                    "meta_membership_stable"
                ),
            )
            for node in fixture.data_nodes:
                node.seed = fixture.leader.data_control_endpoint
                node.start()
            C.command(
                os.environ.copy(),
                [
                    C.CTL,
                    "cluster-create",
                    "--manifest",
                    fixture.manifest,
                    "--addr",
                    fixture.leader.ctl_endpoint,
                    "--allow-plaintext-admin",
                    "--yes",
                ],
            )
            H.wait_until(
                "explicit winner holds source slot after target promotion",
                30,
                lambda: "paused after promotion acknowledgement"
                in Path(source.log_path).read_text(),
            )
            H.wait_until(
                "explicit loser retries busy beyond lease retry budget",
                15,
                lambda: any(
                    Path(n.log_path).read_text().count("native FULL admission is busy")
                    >= 4
                    for n in (first, second)
                ),
            )
            waiting = next(
                n
                for n in (first, second)
                if "native FULL admission is busy" in Path(n.log_path).read_text()
            )
            assert (
                "durably invalidated system state"
                not in Path(waiting.log_path).read_text()
            )
            assert "lavik_full_sync_sessions:1\r\n" in C.redis_call(
                source, ["INFO", "replication"]
            )
            if cancel:
                reply = fixture.leader.fencegroup(F.GROUP, 1)
                assert reply.startswith("OK "), reply
                H.wait_until(
                    "waiting directive revoked by committed fence",
                    10,
                    lambda: any(
                        g.get("term") == "2"
                        for g in fixture.cluster_status(time.monotonic() + 2).get(
                            "groups", []
                        )
                    ),
                )
            else:
                fixture.leader.force_kill()
                H.wait_until(
                    "replacement Meta leader",
                    10,
                    lambda: fixture.rediscover_leader(time.monotonic() + 1),
                )
                # Source authority replacement may cancel the admitted export.
                # Its held drain must remain accounted until explicitly released.
                assert "lavik_full_sync_sessions:1\r\n" in C.redis_call(
                    source, ["INFO", "replication"]
                )
            hold.unlink()
            if cancel:
                time.sleep(2)
                assert (
                    "durably invalidated system state"
                    not in Path(waiting.log_path).read_text()
                )
                H.wait_until(
                    "cancelled explicit FULL releases its source slot",
                    30,
                    lambda: "lavik_full_sync_sessions:0\r\n"
                    in C.redis_call(source, ["INFO", "replication"]),
                )
            else:
                F.wait_ready(
                    fixture, "both explicit rebuilds finish their original operation"
                )
                H.wait_until(
                    "explicit quota released with live replicas",
                    30,
                    lambda: "lavik_full_sync_sessions:0\r\n"
                    in C.redis_call(source, ["INFO", "replication"]),
                )
                C.redis_call(source, ["SET", "quota-{foo}", "explicit-ready"])
                H.wait_until(
                    "both explicit targets read tail",
                    20,
                    lambda: all(
                        C.readonly_get(n, "quota-{foo}") == "explicit-ready"
                        for n in (first, second)
                    ),
                )
        except BaseException:
            fixture.dump_logs()
            raise
        finally:
            hold.unlink(missing_ok=True)
            fixture.force_kill()


def mixed_full_limit(root):
    import gate_failover as F

    # Real Meta and Data processes; only the projection synthesizes a mixed
    # request set. Genesis itself normally schedules these entrypoints apart.
    for follow_first in (True, False):
        name = "mixed-follow-first" if follow_first else "mixed-explicit-first"
        fixture = F.FailoverFixture(
            C.META,
            C.DATA,
            C.CTL,
            str(root / name),
            False,
            data_workers=2,
            client_mode=CLIENT_MODE,
        )
        source, follow, explicit = fixture.data_nodes
        first, waiting = (follow, explicit) if follow_first else (explicit, follow)
        source_hold = root / (name + ".source")
        target_hold = root / (name + ".target")
        source_hold.touch()
        target_hold.touch()
        source.environment = {
            **os.environ,
            "LAVIK_FULL_AFTER_PROMOTION_ACK_HOLD_FILE": str(source_hold),
        }
        waiting.environment = {
            **os.environ,
            "LAVIK_TEST_NATIVE_ADMISSION_HOLD_FILE": str(target_hold),
        }
        try:
            F.write_manifest(
                fixture.manifest,
                fixture.metas,
                fixture.data_nodes,
                client_mode=CLIENT_MODE,
                automatic_uncontrolled_failover_suspect_after_ms=600_000,
            )
            os.environ["LAVIK_TEST_MIXED_FULL_FOLLOW_NODE"] = F.CANDIDATE
            try:
                for meta in fixture.metas:
                    meta.start(
                        initial_cluster_manifest=fixture.manifest, wait_ready=False
                    )
            finally:
                os.environ.pop("LAVIK_TEST_MIXED_FULL_FOLLOW_NODE", None)
            fixture.leader = H.find_leader(fixture.metas, timeout=20)
            # Data must not bootstrap against the leader's partially committed
            # initial identity directory, even before Cluster Create is issued.
            H.wait_until(
                "mixed fixture Meta membership stable before Data bootstrap",
                15,
                lambda: fixture.cluster_status(time.monotonic() + 2).get(
                    "meta_membership_stable"
                ),
            )
            for node in fixture.data_nodes:
                node.seed = fixture.leader.data_control_endpoint
                node.start()
            C.command(
                os.environ.copy(),
                [
                    C.CTL,
                    "cluster-create",
                    "--manifest",
                    fixture.manifest,
                    "--addr",
                    fixture.leader.ctl_endpoint,
                    "--allow-plaintext-admin",
                    "--yes",
                ],
            )
            H.wait_until(
                "mixed winner holds admitted FULL",
                30,
                lambda: "paused after promotion acknowledgement"
                in Path(source.log_path).read_text(),
            )
            assert (
                "durably invalidated system state" in Path(first.log_path).read_text()
            )
            target_hold.unlink()
            H.wait_until(
                "other native entrypoint retries shared busy slot",
                15,
                lambda: Path(waiting.log_path)
                .read_text()
                .count("native FULL admission is busy")
                >= 3,
            )
            assert (
                "durably invalidated system state"
                not in Path(waiting.log_path).read_text()
            )
            assert "lavik_full_sync_sessions:1\r\n" in C.redis_call(
                source, ["INFO", "replication"]
            )
            source_hold.unlink()
            H.wait_until(
                "both mixed native sessions finish",
                30,
                lambda: "lavik_full_sync_sessions:0\r\n"
                in C.redis_call(source, ["INFO", "replication"])
                and all(
                    "master_link_status:up\r\n"
                    in C.redis_call(node, ["INFO", "replication"])
                    for node in (follow, explicit)
                ),
            )
            # The hidden explicit receipt deliberately keeps the Meta operation
            # in Replicate. Read actual Data population and incremental flow.
            C.redis_call(source, ["SET", "quota-{foo}", name])
            H.wait_until(
                "mixed followers receive incremental data",
                20,
                lambda: all(
                    C.readonly_get(node, "quota-{foo}") == name
                    for node in (follow, explicit)
                ),
            )
        except BaseException:
            fixture.dump_logs()
            raise
        finally:
            target_hold.unlink(missing_ok=True)
            source_hold.unlink(missing_ok=True)
            fixture.force_kill()


def rejects(client, args, text):
    try:
        reply = client.call(*args)
    except H.Failure as error:
        assert text in str(error), error
    else:
        raise AssertionError(f"{args} unexpectedly returned {reply}")


def seed_collections(writer):
    writer.call("HSET", "{native}hash", "old", "old", "keep", "value")
    writer.call("RPUSH", "{native}list", "a", "b")
    writer.call("SADD", "{native}set", "a", "b")
    writer.call("ZADD", "{native}zset", 1, "a", 2, "b")
    writer.call("SET", "{native}ttl", "expiring", "PX", 120000)
    # A multi-page value exercises the streamed FULL path on heterogeneous
    # workers without allocating a giant fixture for each fault case.
    writer.call("SET", "{native}large", "x" * (2 * 1024 * 1024))

    fields = [part for i in range(256) for part in (f"field{i}", "v" * 8192)]
    members = [str(i) + "m" * 8192 for i in range(256)]
    writer.call("HSET", "{native}paged-hash", *fields)
    writer.call("SADD", "{native}paged-set", *members)
    writer.call("RPUSH", "{native}paged-list", *members)
    scores = [part for i, member in enumerate(members) for part in (i, member)]
    writer.call("ZADD", "{native}paged-zset", *scores)
    writer.call("XADD", "{native}stream", "1-0", "field", "first")
    writer.call("XGROUP", "CREATE", "{native}stream", "group", "0")
    writer.call(
        "XREADGROUP", "GROUP", "group", "consumer", "STREAMS", "{native}stream", ">"
    )


def grouped_streams(root):
    key = "{native-stream}log"

    def seed(writer):
        for number in range(1, 501):
            assert (
                writer.call("XADD", key, f"{number}-0", "f", "v" * 4096)
                == f"{number}-0"
            )
        assert writer.call("XGROUP", "CREATE", key, "g", "0") == "OK"
        writer.call("XREADGROUP", "GROUP", "g", "a", "COUNT", 100, "STREAMS", key, ">")

    with pair(root, "grouped-streams", seed=seed, require_seed_before_full=True) as (
        meta,
        source,
        target,
        writer,
    ):
        ready(meta)
        reader = Client(target, readonly=True)
        try:

            def state(client):
                return client.call("XINFO", "STREAM", key, "FULL", "COUNT", 0)

            expected = state(writer)
            H.wait_until(
                "grouped Stream FULL preserves groups and PEL",
                30,
                lambda: state(reader) == expected,
            )
            assert writer.call("XADD", key, "501-0", "f", "tail") == "501-0"
            for _ in range(8):
                writer.call(
                    "XREADGROUP", "GROUP", "g", "b", "COUNT", 3, "STREAMS", key, ">"
                )
            assert writer.call("XACK", key, "g", "1-0", "120-0") == 2
            writer.call(
                "XCLAIM",
                key,
                "g",
                "b",
                0,
                "2-0",
                "TIME",
                123456,
                "RETRYCOUNT",
                7,
                "JUSTID",
            )
            expected = state(writer)
            H.wait_until(
                "grouped Stream delivery/ACK/claim replay",
                30,
                lambda: state(reader) == expected,
            )
            assert reader.call("XLEN", key) == 501
            # Group creation and deletion must use sparse replay after FULL too.
            assert writer.call("XGROUP", "CREATE", key, "later", "$") == "OK"
            assert writer.call("XGROUP", "DESTROY", key, "later") == 1
            assert writer.call("XDEL", key, "2-0", "2-0", "500-0") == 2
            writer.call("XAUTOCLAIM", key, "g", "b", 0, "0", "COUNT", 10)
            assert writer.call("XGROUP", "DELCONSUMER", key, "g", "a") > 0
            writer.call(
                "XADD", key, "MAXLEN", "~", 450, "LIMIT", 1000, "502-0", "f", "trimmed"
            )
            writer.call("XTRIM", key, "MAXLEN", "~", 350, "LIMIT", 1000)
            expected = state(writer)
            H.wait_until(
                "grouped delete/consumer/approximate-trim replay",
                30,
                lambda: state(reader) == expected,
            )
            writer.call(
                "XADD",
                "{native-stream}compact",
                "MAXLEN",
                "~",
                0,
                "LIMIT",
                1,
                "1-0",
                "f",
                "v",
            )
            H.wait_until(
                "compact approximate LIMIT replay",
                30,
                lambda: reader.call("TYPE", "{native-stream}compact") == "stream"
                and reader.call("XLEN", "{native-stream}compact") == 0,
            )

            writer.call("XTRIM", key, "MAXLEN", 0)
            expected = state(writer)
            H.wait_until(
                "empty grouped Stream keeps replicated PEL",
                30,
                lambda: state(reader) == expected,
            )
        finally:
            reader.close()


def replay_and_reconnect(root):
    with pair(root, "replay", seed=seed_collections) as (meta, source, target, writer):
        ready(meta)

        # Created replaces the initial population directive with Follow Owner.
        # Meta readiness can precede that ingress reconnect; observe the live
        # data plane before retaining the client used by the WATCH regression.
        def flows_ready():
            probe = Client(target, readonly=True)
            try:
                info = dict(
                    line.split(":", 1)
                    for line in probe.call("INFO", "replication").splitlines()
                    if ":" in line
                )
                # The source has two data shards and the target has three.
                # Their extra Meta workers must never become replication flows.
                return (
                    info.get("master_link_status") == "up"
                    and info.get("lavik_source_workers") == "2"
                    and info.get("lavik_connected_flows") == "2"
                    and probe.call("GET", "{native}seed") == "baseline"
                )
            finally:
                probe.close()

        H.wait_until("two source data flows online and seed readable", 30, flows_ready)
        reader = Client(target, readonly=True)
        try:
            assert reader.call("GET", "{native}seed") == "baseline"
            assert reader.call("HGET", "{native}hash", "keep") == "value"
            assert reader.call("LRANGE", "{native}list", 0, -1) == ["a", "b"]
            assert reader.call("SCARD", "{native}set") == 2
            assert reader.call("ZRANGE", "{native}zset", 0, -1) == ["a", "b"]
            assert reader.call("STRLEN", "{native}large") == 2 * 1024 * 1024
            assert reader.call("GET", "{native}large") == "x" * (2 * 1024 * 1024)
            # Fixed String segments keep byte offsets and TTL across FULL
            # snapshots and incremental command replay on different workers.
            writer.call("MULTI")
            writer.call("SETRANGE", "{native}large", 8191, "AB")
            writer.call("APPEND", "{native}large", "tail")
            writer.call("PEXPIRE", "{native}large", 120000)
            assert writer.call("EXEC") == [
                2 * 1024 * 1024,
                2 * 1024 * 1024 + 4,
                1,
            ]
            assert writer.call("WAIT", 1, 5000) == 1
            assert reader.call("GET", "{native}large") == (
                "x" * 8191 + "AB" + "x" * (2 * 1024 * 1024 - 8193) + "tail"
            )
            assert reader.call("PEXPIRETIME", "{native}large") == writer.call(
                "PEXPIRETIME", "{native}large"
            )
            assert reader.call("PEXPIRETIME", "{native}ttl") == writer.call(
                "PEXPIRETIME", "{native}ttl"
            )
            for client in (writer, reader):
                for args in (
                    ("REPLICAOF", "NO", "ONE"),
                    ("SLAVEOF", "127.0.0.1", source.redis_port),
                    ("ADDREPLICAOF", "127.0.0.1", source.redis_port),
                ):
                    rejects(client, args, "not allowed")
            for command, key in (
                ("HLEN", "paged-hash"),
                ("SCARD", "paged-set"),
                ("LLEN", "paged-list"),
                ("ZCARD", "paged-zset"),
            ):
                assert reader.call(command, "{native}" + key) == 256
            assert reader.call(
                "XINFO", "STREAM", "{native}stream", "FULL", "COUNT", 10
            ) == writer.call("XINFO", "STREAM", "{native}stream", "FULL", "COUNT", 10)
            writer.call("XADD", "{native}stream", "2-0", "field", "second")
            writer.call(
                "XCLAIM",
                "{native}stream",
                "group",
                "claimed",
                0,
                "1-0",
                "TIME",
                123456,
                "RETRYCOUNT",
                7,
                "JUSTID",
            )
            expected_stream = writer.call(
                "XINFO", "STREAM", "{native}stream", "FULL", "COUNT", 10
            )
            H.wait_until(
                "stream consumer state replay",
                20,
                lambda: reader.call(
                    "XINFO", "STREAM", "{native}stream", "FULL", "COUNT", 10
                )
                == expected_stream,
            )
            # Replacement and non-idempotent commands must preserve type,
            # absolute expiry, transaction ordering, and exact apply counts.
            writer.call("LAVIK.HREPLACE", "{native}hash", "new", "replacement")
            writer.call("MULTI")
            writer.call("INCR", "{native}count")
            writer.call("RPUSH", "{native}list", "c")
            writer.call("HSET", "{native}hash", "tx", "value")
            assert writer.call("EXEC") == [1, 3, 1]
            assert writer.call("WAIT", 1, 5000) == 1
            assert reader.call("GET", "{native}count") == "1"
            assert reader.call("HEXISTS", "{native}hash", "old") == 0
            assert reader.call("HGET", "{native}hash", "tx") == "value"
            assert reader.call("LRANGE", "{native}list", 0, -1) == ["a", "b", "c"]
            # The retained WATCH must survive CONTINUE on the original socket;
            # the unrelated counter update must not invalidate its population.
            assert reader.call("WATCH", "{native}seed") == "OK"
            # Native reconnects retain source history and applied cursors.
            old_full = Path(source.log_path).read_text().count("selected=FULL")
            assert writer.call("CLIENT", "KILL", "TYPE", "replica") > 0
            writer.call("INCR", "{native}count")
            H.wait_until(
                "Follow Owner resumes exactly once",
                30,
                lambda: reader.call("GET", "{native}count") == "2",
            )
            H.wait_until(
                "Follow Owner continuation",
                30,
                lambda: "selected=CONTINUE" in Path(source.log_path).read_text(),
            )
            assert Path(source.log_path).read_text().count("selected=FULL") == old_full
            assert reader.call("MULTI") == "OK"
            assert reader.call("GET", "{native}seed") == "QUEUED"
            assert reader.call("EXEC") == ["baseline"]
            # Real transactions also carry ephemeral PUBLISH under authority.
            subscriber = Client(target, readonly=True)
            try:
                assert subscriber.call("SUBSCRIBE", "{native}channel") == [
                    "subscribe",
                    "{native}channel",
                    1,
                ]
                writer.call("MULTI")
                writer.call("SET", "{native}mixed", "written")
                writer.call("PUBLISH", "{native}channel", "message")
                writer.call("EXEC")
                assert C.read_resp(subscriber.reader) == [
                    "message",
                    "{native}channel",
                    "message",
                ]
                assert reader.call("GET", "{native}mixed") == "written"
            finally:
                subscriber.close()
            # Expiration comes from the authoritative source and is replayed.
            writer.call("PEXPIRE", "{native}ttl", 20)
            H.wait_until(
                "replicated authoritative expiration",
                10,
                lambda: reader.call("EXISTS", "{native}ttl") == 0,
            )
        finally:
            reader.close()
        # Shutdown must join flow owners even if the upstream cannot reply.
        source.pause()
        try:
            started = time.monotonic()
            target.terminate()
            assert time.monotonic() - started < 10
        finally:
            source.resume()


def dense_collection_full_sync(root):
    # Many short identities exercise duplicate validation against a growing
    # staged object. A few large values do not expose the quadratic scan that
    # previously monopolized the replica worker during FULL sync.
    count = 32768

    def dump(kind):
        # Plain RDB Hash/Set/ZSet with short binary strings, version 11 and a
        # Redis CRC64 footer. RESTORE seeds one bounded stream instead of repeatedly
        # rewriting an ever-growing object or hitting compact HREPLACE's argc
        # limit. Build payloads before holding the target's control directive.
        data = bytearray([kind, 0x80]) + count.to_bytes(4, "big")
        for i in range(count):
            member = f"member\0{i:08d}".ljust(48, "x").encode()
            data += bytes([len(member)]) + member
            if kind == 4:
                value = f"value\0{i:08d}".ljust(32, "v").encode()
                data += bytes([len(value)]) + value
            elif kind == 5:
                data += struct.pack("<d", float(i))
        data += b"\x0b\x00"
        polynomial = int(f"{0xAD93D23594C935A9:064b}"[::-1], 2)
        table = []
        for byte in range(256):
            crc = byte
            for _ in range(8):
                crc = (crc >> 1) ^ (polynomial if crc & 1 else 0)
            table.append(crc)
        crc = 0
        for byte in data:
            crc = table[(crc ^ byte) & 255] ^ (crc >> 8)
        return bytes(data) + crc.to_bytes(8, "little")

    hash_payload, set_payload, zset_payload = dump(4), dump(2), dump(5)

    def seed(writer):
        H.log(f"seeding dense Hash with {count} fields")
        assert writer.call("RESTORE", "{dense}hash", 0, hash_payload) == "OK"
        H.log(f"seeding dense Set with {count} members")
        assert writer.call("RESTORE", "{dense}set", 0, set_payload) == "OK"
        H.log(f"seeding dense ZSet with {count} members")
        assert writer.call("RESTORE", "{dense}zset", 0, zset_payload) == "OK"

    # This gate verifies ingestion and payload integrity under the ordinary
    # five-second authority lease. Subsecond lease tests expose a separate
    # data-observation stall and must not prevent this ingestion test starting.
    with pair(
        root,
        "dense-collections",
        seed=seed,
        require_seed_before_full=True,
        raft_args=H.raft_args(
            snapshot_distance=100000, election_ms_low=5000, election_ms_high=10000
        ),
    ) as (meta, _, target, writer):
        started = time.monotonic()
        ready(meta)
        reader = Client(target, readonly=True)
        try:
            # Cluster readiness proves Owner service, not that the replica's
            # asynchronous FULL has opened its local population for reads.
            H.wait_until(
                "dense FULL is readable on the replica",
                30,
                lambda: reader.call("HLEN", "{dense}hash") == count
                and reader.call("SCARD", "{dense}set") == count
                and reader.call("ZCARD", "{dense}zset") == count,
            )
            # Distinct values and scores expose association errors that counts
            # alone, or a fixture with one repeated Hash value, cannot detect.
            assert sorted(reader.call("SMEMBERS", "{dense}set")) == sorted(
                writer.call("SMEMBERS", "{dense}set")
            )
            expected = writer.call("HGETALL", "{dense}hash")
            actual = reader.call("HGETALL", "{dense}hash")
            assert dict(zip(actual[::2], actual[1::2], strict=True)) == dict(
                zip(expected[::2], expected[1::2], strict=True)
            )
            assert reader.call(
                "ZRANGE", "{dense}zset", 0, -1, "WITHSCORES"
            ) == writer.call("ZRANGE", "{dense}zset", 0, -1, "WITHSCORES")
        finally:
            reader.close()
        H.log(
            f"dense Hash/Set/ZSet FULL verified {count} members each in "
            f"{time.monotonic() - started:.3f}s"
        )


def handoff_order(root):
    with pair(
        root,
        "handoff",
        target_faults={"LAVIK_REPLICATION_HOLD_FIRST_HANDOFF_UNTIL_NEXT_ACK": "1"},
        source_workers=1,
        target_workers=1,
    ) as (meta, source, target, writer):
        ready(meta)
        log = Path(target.log_path).read_text()
        assert (
            log.index("holding first partition handoff")
            < log.index("acknowledged async partition handoff 1")
            < log.index("acknowledged async partition handoff 0")
        )
        assert C.readonly_get(target, "{native}seed") == "baseline"
        writer.call("INCR", "{native}tail")
        H.wait_until(
            "handoff tail", 20, lambda: C.readonly_get(target, "{native}tail") == "1"
        )


def require_incomplete_population(reader):
    rejects(reader, ("GET", "{native}seed"), "LOADING")
    if CLIENT_MODE == "single":
        for db in (0, 1, 15):
            assert reader.call("SELECT", db) == "OK"
            for command in (
                ("GET", "{native}seed"),
                ("DBSIZE",),
                ("KEYS", "*"),
                ("SCAN", 0),
                ("RANDOMKEY",),
            ):
                rejects(reader, command, "LOADING")
        assert reader.call("SELECT", 0) == "OK"


def cancelled_handoff(root):
    def seed(writer):
        writer.call("SET", "{native}seed", "DB0")
        if CLIENT_MODE == "single":
            writer.call("SELECT", 15)
            writer.call("SET", "{native}seed", "DB15")
            writer.call("SELECT", 0)

    with pair(
        root,
        "cancel-handoff",
        target_faults={"LAVIK_REPLICATION_HOLD_FIRST_HANDOFF_UNTIL_NEXT_ACK": "cancel"},
        source_workers=1,
        target_workers=1,
        seed=seed,
        require_seed_before_full=True,
    ) as (_, source, target, _writer):
        H.wait_until(
            "outstanding native handoff",
            30,
            lambda: "holding first partition handoff"
            in Path(target.log_path).read_text(),
        )
        reader = Client(target, readonly=True)
        try:
            require_incomplete_population(reader)
        finally:
            reader.close()
        target.terminate()
        assert (
            "acknowledged async partition handoff 0"
            not in Path(target.log_path).read_text()
        )


def rejected_full(root, name, source_faults, target_faults, marker, **pair_options):
    with pair(
        root,
        name,
        source_faults=source_faults,
        target_faults=target_faults,
        **pair_options,
    ) as (
        meta,
        source,
        target,
        _writer,
    ):
        H.wait_until(
            name + " fault reached",
            30,
            lambda: marker
            in Path(target.log_path).read_text() + Path(source.log_path).read_text(),
        )
        reader = Client(target, readonly=True)
        try:
            require_incomplete_population(reader)
            rejects(reader, ("REPLICAOF", "NO", "ONE"), "not allowed")
            # A failed directed rebuild cannot manufacture an autonomous new
            # attempt or publish ONLINE without another Meta authorization.
            H.wait_until(
                name + " reported to Meta",
                30,
                lambda: C.cluster_status(meta).get("cluster_state")
                == "provisioning-failed",
            )
            assert "lavik_replication_state:online" not in reader.call(
                "INFO", "replication"
            )
        finally:
            reader.close()


def population_finalization_failure(root):
    with pair(
        root,
        "population-finalization-failure",
        target_faults={"LAVIK_REPLICATION_FAIL_POPULATION_FINALIZE": "1"},
    ) as (meta, source, target, _writer):
        H.wait_until(
            "post-promotion finalization failure latched",
            30,
            lambda: "replication failed-stopped until restart: promoted population "
            "finalization failed: injected full-sync population finalization failure"
            in Path(target.log_path).read_text(),
        )
        # The root, flow cut and durable identity already exist. Failure of
        # the final local completion must resolve the rebuild with an error,
        # rather than leave a Ready attempt or retry it through CONTINUE.
        H.wait_until(
            "finalization failure reported to Meta",
            30,
            lambda: C.cluster_status(meta).get("cluster_state")
            == "provisioning-failed",
        )
        reader = Client(target, readonly=True)
        try:
            require_incomplete_population(reader)
            replication = reader.call("INFO", "replication")
            assert "lavik_replication_failed_stopped:1\r\n" in replication
            assert "lavik_replication_state:online\r\n" not in replication
            stats = reader.call("INFO", "stats")
            assert "tomb_raider_eligible:0\r\n" in stats
            assert "tomb_raider_blocked_reason:population_change\r\n" in stats
            assert "selected=CONTINUE" not in Path(source.log_path).read_text()
        finally:
            reader.close()
        # Failed-stop deliberately withholds a clean checkpoint. Verify that
        # shutdown reports the unsafe state before pair's ordinary cleanup.
        target.proc.send_signal(signal.SIGINT)
        assert target.proc.wait(timeout=30) == 1
        assert "skipping normal storage flush" in Path(target.log_path).read_text()


def checksum_rejection(root):
    def seed(writer):
        # Partition 1 carries the corrupt record before the next reset batch,
        # which must join the deliberately held partition-0 handoff.
        key = "{checksum-18743}:seed"
        assert writer.call("CLUSTER", "KEYSLOT", key) == 1
        assert writer.call("SET", key, "baseline") == "OK"

    rejected_full(
        root,
        "checksum",
        {"LAVIK_REPLICATION_CORRUPT_FULLSYNC_RECORD_FRAME_ONCE": "1"},
        # Keep a handoff pending on the receiving flow until CRC rejection
        # cancels it. Cleanup must preserve the corruption diagnosis.
        {"LAVIK_REPLICATION_HOLD_FIRST_HANDOFF_UNTIL_NEXT_ACK": "cancel"},
        "replication frame CRC32C mismatch",
        source_workers=1,
        target_workers=1,
        seed=seed,
        require_seed_before_full=True,
    )


def full_tail(root):
    with pair(
        root,
        "full-tail",
        source_faults={
            "LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "1000",
            "LAVIK_REPLICATION_PAUSE_FULLSYNC_BEFORE_CUT_MS": "1000",
        },
        seed=seed_collections,
    ) as (meta, source, target, writer):
        for i in range(32):
            writer.call("MULTI")
            writer.call("INCR", "{native}count")
            writer.call("HSET", "{native}paged-hash", "field0", str(i))
            writer.call("RPUSH", "{native}list", str(i))
            writer.call("EXEC")
        ready(meta)
        reader = Client(target, readonly=True)
        try:
            assert reader.call("GET", "{native}count") == "32"
            assert reader.call("HGET", "{native}paged-hash", "field0") == "31"
            assert reader.call("LRANGE", "{native}list", 0, -1) == ["a", "b"] + list(
                map(str, range(32))
            )
            # Pressure across changing source workers must release every
            # admission reservation, including writes on non-connection owners.
            writer.call("CONFIG", "SET", "replication-publish-queue-mb-per-worker", 1)
            for i in range(160):
                writer.call("SET", f"waterline-{i % 17}", str(i) + "x" * 32768)
            assert writer.call("WAIT", 1, 5000) == 1
            for i in range(17):
                assert reader.call("GET", f"waterline-{i}") == writer.call(
                    "GET", f"waterline-{i}"
                )
        finally:
            reader.close()


def full_tail_publish_before_reset(root):
    # One source worker resets only the first batch before handing off slot 0.
    # Publish into the last slot while that flow is paused: both the bare
    # command and the EXEC envelope must replay before their transport slot
    # has any replica storage context.
    tag = next(
        f"full-publish-{i}"
        for i in range(100000)
        if C.redis_slot(f"full-publish-{i}") == 16383
    )
    channel = "{" + tag + "}channel"
    with pair(
        root,
        "full-tail-publish-before-reset",
        source_faults={"LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "3000"},
        require_seed_before_full=True,
        source_workers=1,
        target_workers=2,
    ) as (meta, source, target, writer):
        H.wait_until(
            "partition zero handed off before publications",
            30,
            lambda: "paused full sync after acknowledged handoff partition 0 "
            in Path(source.log_path).read_text(),
        )
        subscriber = Client(target)
        try:
            assert subscriber.call("SUBSCRIBE", channel) == ["subscribe", channel, 1]
            assert writer.call("PUBLISH", channel, "bare") == 0
            assert writer.call("MULTI") == "OK"
            assert writer.call("PUBLISH", channel, "first") == "QUEUED"
            assert writer.call("PUBLISH", channel, "second") == "QUEUED"
            assert writer.call("EXEC") == [0, 0]
            for message in ("bare", "first", "second"):
                assert C.read_resp(subscriber.reader) == ["message", channel, message]
            ready(meta)
            assert writer.call("PUBLISH", channel, "online") == 0
            assert writer.call("WAIT", 1, 5000) == 1
            # The next message also proves that the FULL cut did not replay
            # the preceding publications a second time through ONLINE.
            assert C.read_resp(subscriber.reader) == ["message", channel, "online"]
        finally:
            subscriber.close()
        assert Path(source.log_path).read_text().count("selected=FULL") == 1
        assert "precedes partition reset" not in Path(target.log_path).read_text()


def full_tail_expiration_effects(root):
    # The source pauses after handing off partition zero. Mutations made in
    # that window must use FULL command replay, not the initial snapshot or
    # the ONLINE backlog. Canonical TTL effects wrap these single-key writes
    # in __LAVIK_EXEC_V1; that wrapper still needs the partition apply context.
    tag = next(
        f"full-tail-{i}" for i in range(100000) if C.redis_slot(f"full-tail-{i}") == 0
    )
    prefix = "{" + tag + "}"
    counter, collection = prefix + "counter", prefix + "hash"

    def seed(writer):
        assert writer.call("SET", counter, 0) == "OK"
        assert writer.call("HSET", collection, "before", "snapshot") == 1
        assert writer.call("PEXPIRE", collection, 120000) == 1

    with pair(
        root,
        "full-tail-expiration-effects",
        source_faults={"LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "3000"},
        seed=seed,
        require_seed_before_full=True,
        source_workers=1,
        target_workers=2,
    ) as (meta, source, target, writer):
        H.wait_until(
            "partition zero handed off before mutations",
            30,
            lambda: "paused full sync after acknowledged handoff partition 0 "
            in Path(source.log_path).read_text(),
        )
        assert writer.call("INCR", counter) == 1
        assert writer.call("HSET", collection, "after", "tail") == 1
        deadline = writer.call("PEXPIRETIME", collection)
        ready(meta)
        reader = Client(target, readonly=True)
        try:
            # Replacing the initialization directive with Follow Owner can
            # reconnect after Meta reports ready. Cluster read admission may
            # briefly close while the complete population remains intact.
            # Retry only LOADING; wrong values and every other error still
            # fail immediately, and the single-FULL assertion below remains.
            read_deadline = time.monotonic() + 30
            while True:
                try:
                    assert reader.call("GET", counter) == "1"
                    assert reader.call("PTTL", counter) == -1
                    assert reader.call("HGET", collection, "before") == "snapshot"
                    assert reader.call("HGET", collection, "after") == "tail"
                    assert reader.call("PEXPIRETIME", collection) == deadline
                    break
                except H.Failure as error:
                    if (
                        not str(error).startswith("LOADING ")
                        or time.monotonic() >= read_deadline
                    ):
                        raise
                    time.sleep(0.01)
            assert writer.call("INCR", counter) == 2
            assert writer.call("WAIT", 1, 5000) == 1
            assert reader.call("GET", counter) == "2"
        finally:
            reader.close()
        # A failed apply followed by a replacement snapshot could produce the
        # same values. Require this first FULL to complete without that retry.
        assert Path(source.log_path).read_text().count("selected=FULL") == 1
        assert "outside its apply context" not in Path(target.log_path).read_text()


def full_tail_type_reuse(root):
    # Keep these commands behind one acknowledged handoff so they traverse
    # FULL's command/after-image FIFO before the ONLINE boundary. Multi-key
    # writes use committed participant records; single-key writes carry their
    # original command and its TTL companion.
    tag = next(
        f"full-reuse-{i}" for i in range(100000) if C.redis_slot(f"full-reuse-{i}") == 0
    )
    prefix = "{" + tag + "}"
    key, counter, other, copied = [
        prefix + name for name in ("typed", "counter", "other", "copied")
    ]

    def seed(writer):
        assert writer.call("SET", key, "seed") == "OK"
        assert writer.call("MSET", counter, 0, other, 0) == "OK"

    with pair(
        root,
        "full-tail-type-reuse",
        source_faults={"LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "5000"},
        seed=seed,
        require_seed_before_full=True,
        source_workers=1,
        target_workers=2,
        raft_args=H.raft_args(
            snapshot_distance=100000, election_ms_low=5000, election_ms_high=10000
        ),
    ) as (meta, source, target, writer):
        H.wait_until(
            "type reuse partition handed off",
            30,
            lambda: "paused full sync after acknowledged handoff partition 0 "
            in Path(source.log_path).read_text(),
        )
        for i in range(8):
            # An absolute past deadline deletes immediately, with no sleep or
            # scheduler race required to advance from one value type to another.
            assert writer.call("PEXPIREAT", key, 1) == 1
            assert writer.call("HSET", key, "f", str(i)) == 1
            assert writer.call("PEXPIREAT", key, 1) == 1
            assert writer.call("RPUSH", key, str(i)) == 1
            assert writer.call("MULTI") == "OK"
            assert writer.call("INCR", counter) == "QUEUED"
            assert writer.call("MSET", other, str(i), copied, "temporary") == "QUEUED"
            assert writer.call("RPUSH", key, "tx") == "QUEUED"
            assert writer.call("EXEC") == [i + 1, "OK", 2]
        assert writer.call("COPY", counter, copied, "REPLACE") == 1
        assert writer.call("PEXPIRE", key, 120000) == 1
        deadline = writer.call("PEXPIRETIME", key)
        ready(meta)
        reader = Client(target, readonly=True)
        try:
            assert reader.call("LRANGE", key, 0, -1) == ["7", "tx"]
            assert reader.call("GET", counter) == "8"
            assert reader.call("GET", other) == "7"
            assert reader.call("GET", copied) == "8"
            assert reader.call("PEXPIRETIME", key) == deadline
            assert writer.call("RPUSH", key, "online") == 3
            assert writer.call("WAIT", 1, 5000) == 1
            assert reader.call("LRANGE", key, 0, -1) == ["7", "tx", "online"]
        finally:
            reader.close()
        assert Path(source.log_path).read_text().count("selected=FULL") == 1
        assert "WRONGTYPE" not in Path(target.log_path).read_text()


def full_tail_collection_transactions(root, cross_worker=False):
    tag = next(
        f"full-collections-{i}"
        for i in range(100000)
        if C.redis_slot(f"full-collections-{i}") == 0
    )
    prefix = "{" + tag + "}"
    cases = []
    for grouped in (False, True):
        # A large member also exercises grouped transaction receipts. All
        # effects happen after the baseline handoff, so a later source scan
        # cannot conceal missing publication from a command family.
        value = "v" * 16384 if grouped else "a"
        stem = prefix + ("grouped-" if grouped else "compact-")
        for command in ("LMOVE", "RPOPLPUSH", "BLMOVE", "BRPOPLPUSH"):
            src, dst = stem + command, stem + command + "-dst"
            args = [command, src, dst]
            if command in ("LMOVE", "BLMOVE"):
                args += ["RIGHT", "LEFT"]
            if command in ("BLMOVE", "BRPOPLPUSH"):
                args += ["0.01"]
            cases.append(
                (
                    ["RPUSH", src, "b", value],
                    args,
                    value,
                    [["LRANGE", src, 0, -1], ["LRANGE", dst, 0, -1]],
                )
            )
        src, dst = stem + "set", stem + "set-dst"
        cases.append(
            (
                ["SADD", src, "b", value],
                ["SMOVE", src, dst, value],
                1,
                [["SMEMBERS", src], ["SMEMBERS", dst]],
            )
        )
        src, dst = stem + "union", stem + "union-dst"
        cases.append(
            (
                ["SADD", src, "b", value],
                ["SUNIONSTORE", dst, src],
                2,
                [["SMEMBERS", dst]],
            )
        )
        src, dst = stem + "zset", stem + "zset-dst"
        cases.append(
            (
                ["ZADD", src, 1, "b", 2, value],
                ["ZUNIONSTORE", dst, 1, src],
                2,
                [["ZRANGE", dst, 0, -1, "WITHSCORES"]],
            )
        )
        src, dst = stem + "sort", stem + "sort-dst"
        cases.append(
            (
                ["RPUSH", src, "b", value],
                ["SORT", src, "ALPHA", "STORE", dst],
                2,
                [["LRANGE", dst, 0, -1]],
            )
        )

    cases_by_slot = {0: cases}
    if cross_worker:
        other_tag = next(
            f"full-collections-{i}"
            for i in range(100000)
            if C.redis_slot(f"full-collections-{i}") == 1
        )
        prefixes = [prefix, "{" + other_tag + "}"]
        cases_by_slot = {}
        # Either source flow may reach the pause first. Seed both layouts,
        # then mutate the one whose source baseline is known to be handed off.
        # Destinations live on the other worker, exercising owner-local finish
        # callbacks under managed Single's cross-slot command admission.
        for slot in (0, 1):

            def remap(args):
                return [
                    prefixes[1 - slot if arg.endswith("-dst") else slot]
                    + arg[len(prefix) :]
                    if isinstance(arg, str) and arg.startswith(prefix)
                    else arg
                    for arg in args
                ]

            cases_by_slot[slot] = [
                (
                    remap(initial),
                    remap(command),
                    result,
                    [remap(read) for read in reads],
                )
                for initial, command, result, reads in cases
            ]
        cases = [case for layout in cases_by_slot.values() for case in layout]

    def seed(writer):
        for initial, _, _, _ in cases:
            assert writer.call(*initial) == 2

    with pair(
        root,
        "full-tail-collection-transactions" + ("-cross-worker" if cross_worker else ""),
        source_faults={"LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS": "5000"},
        seed=seed,
        require_seed_before_full=True,
        source_workers=2 if cross_worker else 1,
        target_workers=3 if cross_worker else 2,
        client_mode="single" if cross_worker else "cluster",
        raft_args=H.raft_args(
            snapshot_distance=100000, election_ms_low=5000, election_ms_high=10000
        ),
    ) as (meta, source, target, writer):
        H.wait_until(
            "collection baseline handed off",
            30,
            lambda: re.search(
                r"paused full sync after acknowledged handoff partition ([01]) ",
                Path(source.log_path).read_text(),
            ),
        )
        handed_off = int(
            re.search(
                r"paused full sync after acknowledged handoff partition ([01]) ",
                Path(source.log_path).read_text(),
            ).group(1)
        )
        expected = []
        for _, command, result, reads in cases_by_slot[handed_off]:
            assert writer.call(*command) == result
            for read in reads:
                value = writer.call(*read)
                expected.append(
                    (read, sorted(value) if read[0] == "SMEMBERS" else value)
                )
        ready(meta)
        reader = Client(target, readonly=True)
        try:
            for read, value in expected:
                actual = reader.call(*read)
                if read[0] == "SMEMBERS":
                    actual = sorted(actual)
                assert actual == value, (read, actual, value)
        finally:
            reader.close()
        assert Path(source.log_path).read_text().count("selected=FULL") == (
            2 if cross_worker else 1
        )


def post_cut_reset_reconnect(root):
    with pair(
        root,
        "post-cut-reset",
        source_faults={"LAVIK_REPLICATION_POST_CUT_RESET_ONCE": "1"},
    ) as (meta, source, target, writer):
        H.wait_until(
            "post-cut reset injection",
            30,
            lambda: "injected post-cut reset" in Path(source.log_path).read_text(),
        )
        # The reset interrupts FULL before the source confirms every flow.
        # Recovery may need another FULL, especially with an empty flow at its
        # initial cursor. Assert the original contract: recovery preserves data
        # and resumes replication, without requiring a particular handshake.
        ready(meta)

        def readable():
            # Population replacement retires existing clients. Probe with a
            # fresh connection until both transport and data path are ready.
            probe = Client(target, readonly=True)
            try:
                return (
                    "lavik_replication_state:online"
                    in probe.call("INFO", "replication")
                    and probe.call("GET", "{native}seed") == "baseline"
                )
            finally:
                probe.close()

        H.wait_until("post-cut reconnect serves preserved data", 30, readable)
        reader = Client(target, readonly=True)
        try:
            assert writer.call("INCR", "post-cut-counter") == 1
            assert writer.call("WAIT", 1, 5000) == 1
            assert reader.call("GET", "post-cut-counter") == "1"
            assert reader.call("GET", "{native}seed") == "baseline"
        finally:
            reader.close()


def committed_cursor_reconnect(root, name, target_faults):
    with pair(root, name, target_faults=target_faults) as (
        meta,
        source,
        target,
        writer,
    ):
        ready(meta)
        reader = Client(target, readonly=True)
        try:
            for i in range(64):
                writer.call("SET", f"cursor-warmup-{i}", "ready")
            assert writer.call("WAIT", 1, 5000) == 1
            old_full = Path(source.log_path).read_text().count("selected=FULL")
            writer.call("INCR", "cancelled-apply-counter")
            if (
                target_faults
                and "LAVIK_REPLICATION_CANCEL_PEER_FLOW_AFTER_COMMAND_APPLY_ONCE"
                in target_faults
            ):
                H.wait_until(
                    "cancel after committed apply",
                    30,
                    lambda: "injected peer-flow session cancellation after command apply"
                    in Path(target.log_path).read_text(),
                )
            H.wait_until(
                "committed cursor reconnect",
                30,
                lambda: "selected=CONTINUE" in Path(source.log_path).read_text(),
            )
            H.wait_until(
                "committed increment applied once",
                30,
                lambda: reader.call("GET", "cancelled-apply-counter") == "1",
            )
            assert reader.call("GET", "{native}seed") == "baseline"
            assert Path(source.log_path).read_text().count("selected=FULL") == old_full
        finally:
            reader.close()


def divergent_tail(root, flow):
    with pair(
        root,
        f"divergent-{flow}",
        source_faults={
            "LAVIK_REPLICATION_DIVERGENT_TAIL_ONCE": "1",
            "LAVIK_REPLICATION_DIVERGENT_TAIL_FLOW": str(flow),
        },
        target_workers=2,
    ) as (meta, source, target, writer):
        ready(meta)
        key = "divergent-counter"
        while writer.call("CLUSTER", "KEYSLOT", key) % 2 != flow:
            key += "x"
        assert writer.call("INCR", key) == 1
        H.wait_until(
            "divergent tail reaches flow",
            30,
            lambda: "injected divergent replication tail"
            in Path(source.log_path).read_text(),
        )
        H.wait_until(
            "all continuation cursors invalidated",
            30,
            lambda: "invalidated native replication continuation"
            in Path(target.log_path).read_text(),
        )
        # A gap invalidates the entire population. The follower may not use
        # the other flow's cursor to become readable without fresh authority.
        reader = Client(target, readonly=True)
        try:
            rejects(reader, ("GET", "{native}seed"), "LOADING")
        finally:
            reader.close()


def backpressured_shutdown(root):
    with pair(root, "backpressure", source_workers=1, target_workers=1) as (
        meta,
        source,
        target,
        writer,
    ):
        ready(meta)
        writer.call("CONFIG", "SET", "replication-publish-queue-mb-per-worker", 1)
        writer.call("SET", "ack-baseline", "ready")
        assert writer.call("WAIT", 1, 5000) == 1
        target.pause()
        try:
            payload = "x" * (4 * 1024 * 1024)
            writer.call("SET", "pressure", payload)
            writer.call("SET", "pressure", payload)
            H.wait_until(
                "native source backlog pressure",
                10,
                lambda: 'lavik_replication_backlog_backpressured{worker="0"} 1'
                in source.metrics(),
            )

            def blocked_write():
                client = Client(source)
                try:
                    return client.call("SET", "blocked-writer", "value")
                except (OSError, H.Failure):
                    return "closed"
                finally:
                    client.close()

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(blocked_write)
                time.sleep(0.2)
                assert not pending.done()
                assert writer.call("PING") == "PONG"
                started = time.monotonic()
                source.terminate()
                assert time.monotonic() - started < 10
                pending.result(timeout=3)
        finally:
            target.resume()


def replica_backup_during_exec(root):
    # Release builds keep the concurrent smoke test; fault-enabled binaries
    # additionally prove that replay reaches the closed backup gate.
    deterministic = C.has_fault(C.DATA, b"LAVIK_BACKUP_CUT_HOLD_FILE")
    hold = root / "backup-cut.hold"
    faults = {"LAVIK_BACKUP_CUT_HOLD_FILE": str(hold)} if deterministic else {}
    with pair(root, "replica-backup-exec", target_faults=faults) as (
        meta,
        source,
        target,
        writer,
    ):
        ready(meta)
        reader = Client(target, readonly=True)
        stopped = threading.Event()
        started = threading.Event()

        def write_transactions():
            client = Client(source)
            count = 0
            try:
                while not stopped.is_set():
                    assert client.call("MULTI") == "OK"
                    assert client.call("INCR", "{backup}counter") == "QUEUED"
                    assert (
                        client.call(
                            "COPY", "{backup}counter", "{backup}copy", "REPLACE"
                        )
                        == "QUEUED"
                    )
                    count += 1
                    assert client.call("EXEC") == [count, 1]
                    started.set()
                assert client.call("WAIT", 1, 10000) == 1
                return count
            finally:
                client.close()

        old_full = Path(source.log_path).read_text().count("selected=FULL")

        def start_backup():
            try:
                assert reader.call("BGSAVE") == "Background saving started"
                return True
            except H.Failure as error:
                # File publication precedes clearing the active-job flag.
                # Only this definite non-admission can be retried.
                if str(error) == "ERR Background save already in progress":
                    return False
                raise

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                pending = pool.submit(write_transactions)
                try:
                    assert started.wait(timeout=10)
                    dump = Path(target.workdir) / "dump.rdb"
                    for iteration in range(16):
                        previous = dump.stat().st_mtime_ns if dump.exists() else 0
                        completed = (
                            Path(target.log_path)
                            .read_text()
                            .count("RDB backup completed:")
                        )
                        if deterministic and iteration == 0:
                            hold.touch()
                            # BGSAVE replies only after its cut reopens. Run it
                            # separately while observing the target checkpoint.
                            backup = pool.submit(start_backup)
                            try:
                                H.wait_until(
                                    "backup gates closed",
                                    10,
                                    lambda: "backup test checkpoint: database gates closed"
                                    in Path(target.log_path).read_text(),
                                )
                                H.wait_until(
                                    "replica EXEC blocked by backup",
                                    10,
                                    lambda: "backup test checkpoint: replica EXEC waiting for database admission"
                                    in Path(target.log_path).read_text(),
                                )
                                assert not backup.done(), (
                                    "backup cut reopened before release"
                                )
                            finally:
                                hold.unlink(missing_ok=True)
                            assert backup.result(timeout=10)
                        else:
                            H.wait_until("replica backup admitted", 10, start_backup)
                        H.wait_until(
                            "replica backup completed",
                            30,
                            lambda: dump.exists()
                            and dump.stat().st_mtime_ns != previous
                            and Path(target.log_path)
                            .read_text()
                            .count("RDB backup completed:")
                            > completed,
                        )
                        assert "lavik_replication_state:online" in reader.call(
                            "INFO", "replication"
                        )
                finally:
                    hold.unlink(missing_ok=True)
                    stopped.set()
                count = pending.result(timeout=15)
            assert count > 0
            assert reader.call("GET", "{backup}counter") == str(count)
            assert reader.call("GET", "{backup}copy") == str(count)
            assert Path(source.log_path).read_text().count("selected=FULL") == old_full
            assert (
                "invalidated native replication continuation"
                not in Path(target.log_path).read_text()
            )
        finally:
            reader.close()


def small_receive_window(root):
    def seed(writer):
        # Exercise FULL before steady replay grows the loopback window/MSS
        # and the target's fault reduces its receive window.
        writer.call("SET", "{window}seed", "s" * (2 * 1024 * 1024))

    with pair(
        root,
        "small-receive-window",
        seed=seed,
        source_workers=1,
        target_workers=1,
        target_faults={"LAVIK_TEST_NATIVE_SMALL_RECEIVE_WINDOW": "24576"},
    ) as (meta, source, target, writer):
        ready(meta)
        writer.call("SET", "{window}trigger", "online")
        # A real publisher burst must drain through native replay and ACKs.
        # The reduced window used to put each large segment behind TCP's
        # ~200ms probe timer, despite both processes remaining ONLINE.
        for batch in range(64):
            writer.socket.sendall(
                b"".join(
                    C.encode_resp(["SET", f"{{window}}key-{index}", "v" * 1024])
                    for index in range(512)
                )
            )
            for _ in range(512):
                assert C.read_resp(writer.reader) == "OK"
        assert "test native receive window reduced" in Path(target.log_path).read_text()
        assert writer.call("WAIT", 1, 12000) == 1
        assert C.readonly_get(target, "{window}key-511") == "v" * 1024


def target_queue_shutdown(root):
    # Hold the FIFO consumer until the receiver has filled its bounded queue.
    # Socket shutdown cannot wake that capacity wait: terminal stage/ACK
    # publication must explicitly notify ingress before the flow can join.
    with pair(
        root,
        "target-queue-shutdown",
        source_workers=1,
        target_workers=1,
        target_faults={
            "LAVIK_REPLICATION_PAUSE_BEFORE_COMMAND_APPLY_MS": "8000",
            "LAVIK_REPLICATION_REPORT_ONLINE_BACKPRESSURE": "1",
        },
    ) as (meta, _source, target, writer):
        ready(meta)
        for i in range(600):
            assert writer.call("SET", "{queue-shutdown}key", str(i)) == "OK"
        H.wait_until(
            "replica ingress is waiting for queue capacity",
            10,
            lambda: "replica online ingress waiting for command capacity"
            in Path(target.log_path).read_text(),
        )
        target.terminate()
        assert (
            "replication targets quiesced before storage flush"
            in Path(target.log_path).read_text()
        )


def main():
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    H.set_tag("native-replication")
    with tempfile.TemporaryDirectory(
        prefix="lavik-meta-native-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        root = Path(directory)
        if len(sys.argv) > 5:
            assert sys.argv[5:] == ["tomb_raider"], sys.argv[5:]
            tomb_raider(root)
            if C.has_fault(C.DATA, b"LAVIK_REPLICATION_FAIL_POPULATION_FINALIZE"):
                population_finalization_failure(root)
            H.log("PASS")
            return
        grouped_streams(root)
        replay_and_reconnect(root)
        replica_backup_during_exec(root)
        dense_collection_full_sync(root)
        full_tail(root)
        backpressured_shutdown(root)
        if C.has_fault(C.DATA, b"LAVIK_REPLICATION_HOLD_FIRST_HANDOFF_UNTIL_NEXT_ACK"):
            population_finalization_failure(root)
            full_session_lifecycle(root)
            full_completion_reconnect(root)
            follow_full_limit(root)
            explicit_full_limit(root)
            # Meta's test hooks follow its own Debug build policy.
            if C.has_fault(C.META, b"LAVIK_TEST_MIXED_FULL_FOLLOW_NODE"):
                mixed_full_limit(root)
            full_tail_publish_before_reset(root)
            full_tail_expiration_effects(root)
            small_receive_window(root)
            full_tail_type_reuse(root)
            full_tail_collection_transactions(root)
            full_tail_collection_transactions(root, cross_worker=True)
            target_queue_shutdown(root)
            handoff_order(root)
            cancelled_handoff(root)
            committed_cursor_reconnect(
                root,
                "cancel-apply",
                target_faults={
                    "LAVIK_REPLICATION_CANCEL_PEER_FLOW_AFTER_COMMAND_APPLY_ONCE": "cancelled-apply-counter"
                },
            )
            post_cut_reset_reconnect(root)
            # Two controlled owner changes create a replacement history while
            # preserving the laggard's old population and per-flow cursors.
            import gate_failover as F

            F.run_full_fallback(
                C.META,
                C.DATA,
                C.CTL,
                C.REDIS_CLI,
                str(root),
                False,
                cut_disconnect=True,
            )
            divergent_tail(root, 0)
            divergent_tail(root, 1)
            checksum_rejection(root)
            rejected_full(
                root,
                "early-online",
                {"LAVIK_REPLICATION_EARLY_ONLINE": "1"},
                {},
                "injected ONLINE before local flow readiness",
            )
    H.log("PASS")


if __name__ == "__main__":
    main()
