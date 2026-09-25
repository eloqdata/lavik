#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Original stock pools and subscription objects survive Data retirement."""

import subprocess
import signal
import threading
import time
import uuid

import gate_sentinel_ha as HA
from gate_sentinel_ha import H, C, D, Client


def run(fixture, go_binary, directory, scenario):
    fixture.start_created()
    fixture.configure_fast_policies(
        suspect_after_ms=600000 if scenario == "expiry" else 1000
    )
    clients, resources = HA.make_clients(fixture, go_binary, directory)
    subscriptions = []
    channel = "retirement-" + uuid.uuid4().hex
    try:
        HA.recover(clients, "before retirement")
        for name, client in clients:
            if isinstance(client, HA.GoClient):
                client.call(op="subscribe", key=channel)
                subscriptions.append((name, client))
            else:
                sub = client.pubsub()
                sub.subscribe(channel)
                H.wait_until(
                    "subscription acknowledged",
                    5,
                    lambda sub=sub: sub.get_message(timeout=1) is not None,
                )
                subscriptions.append((name, sub))
        owner = fixture.by_id[D.OWNER]
        candidate = fixture.by_id[D.REPLICA]
        H.wait_until(
            "replica has actual client data",
            30,
            lambda: candidate.command_head(["GET", "sentinel-ha-python-2"]) == "$17",
        )
        start = time.monotonic()
        if scenario == "expiry":
            old = Client(owner.redis_port)
            old.command("PING")
            for meta in fixture.metas:
                meta.proc.send_signal(signal.SIGSTOP)
            try:
                time.sleep(3)
                HA.assert_closed(old)
                # No discovery is available during this deliberate isolation.
                fresh = Client(owner.redis_port)
                try:
                    assert fresh.command("ROLE")[0] == b"master"
                    assert fresh.command("SUBSCRIBE", channel)[0] == b"subscribe"
                finally:
                    fresh.close()
            finally:
                old.close()
                for meta in fixture.metas:
                    meta.proc.send_signal(signal.SIGCONT)
            start = time.monotonic()
        elif scenario == "controlled":
            accepted = subprocess.check_output(
                [
                    C.CTL,
                    "failover",
                    D.GROUP,
                    "--addr",
                    fixture.leader.ctl_endpoint,
                    "--allow-plaintext-admin",
                    "--timeout-ms",
                    "10000",
                    "--failover-timeout-ms",
                    "60000",
                ],
                text=True,
                timeout=20,
            )
            if "operation=" not in accepted:
                raise H.Failure(f"failover was not accepted: {accepted}")
        else:
            owner.force_kill()
        if scenario == "controlled":
            H.wait_until(
                "old living Owner is reconfigured",
                30,
                lambda: owner.command_head(["ROLE"]) == "*5",
            )
        HA.recover(clients, "after " + scenario, start)
        # Receivers keep the exact pre-fault objects. Repeated NEW publications
        # cover the permitted resubscription gap; old buffered messages cannot
        # satisfy this assertion. Only the library reconnects/resubscribes.
        payload = uuid.uuid4().hex
        stop = threading.Event()
        errors = []

        def publish():
            try:
                import redis
                from redis.sentinel import Sentinel

                with Sentinel(
                    fixture.sentinel_addresses(),
                    sentinel_kwargs={
                        "password": D.SENTINEL_PASSWORD,
                        "socket_timeout": 1,
                    },
                    socket_timeout=1,
                    socket_connect_timeout=1,
                ) as sentinel:
                    with sentinel.master_for(D.GROUP) as publisher:
                        while not stop.is_set():
                            try:
                                publisher.publish(channel, payload)
                            except redis.RedisError:
                                pass
                            stop.wait(0.1)
            except Exception as error:
                errors.append(error)

        publisher = threading.Thread(target=publish)
        publisher.start()
        try:
            pending = dict(subscriptions)
            deadline = time.monotonic() + 30
            while pending and time.monotonic() < deadline:
                for name, sub in list(pending.items()):
                    try:
                        message = (
                            sub.call(op="receive")
                            if isinstance(sub, HA.GoClient)
                            else sub.get_message(timeout=0.2)
                        )
                        value = (
                            message.get("data")
                            if isinstance(message, dict)
                            else message
                        )
                        if value in (payload, payload.encode()):
                            del pending[name]
                            H.log(f"{scenario}: original {name} subscription recovered")
                    except Exception:
                        pass
            if pending or errors:
                raise H.Failure(
                    f"original subscriptions failed: {list(pending)}, {errors}"
                )
        finally:
            stop.set()
            publisher.join(timeout=5)
        if scenario != "fault" and not owner.alive():
            raise H.Failure("old Owner unexpectedly died")
    finally:
        for _, sub in subscriptions:
            if not isinstance(sub, HA.GoClient):
                sub.close()
        for _, client in clients:
            client.close()
        for resource in resources:
            resource.close()
