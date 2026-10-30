#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
"""Native progress semantics through INFO and real HTTP scrapes."""

from contextlib import ExitStack, contextmanager
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import threading
import time
from urllib.request import urlopen

from redis_follower_smoke import Client, H, process

# Retained evidence, also used by the native ACK wire-peer test.
METRIC_SAMPLES = {}


def info(client):
    return dict(
        line.split(":", 1)
        for line in client.call("INFO", "replication").splitlines()
        if ":" in line
    )


def scrape(port):
    with urlopen(f"http://127.0.0.1:{port}/metrics", timeout=10) as response:
        text = response.read().decode()
    METRIC_SAMPLES.setdefault(port, []).append({"time": time.time(), "metrics": text})
    return text


def values(text, name, **labels):
    result = []
    for line in text.splitlines():
        if not re.match(re.escape(name) + r"(?:\{| )", line):
            continue
        if all(f'{key}="{value}"' in line for key, value in labels.items()):
            result.append(float(line.rsplit(" ", 1)[1]))
    return result


def wait(description, predicate, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError(description)


@contextmanager
def faults(**settings):
    saved = {key: os.environ.get(key) for key in settings}
    os.environ.update(settings)
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def record(root, name, client, metrics_port):
    (root / f"{name}.info.json").write_text(json.dumps(info(client), indent=2))
    (root / f"{name}.prom").write_text(scrape(metrics_port))


def online_progress(binary, root):
    source_metrics, target_metrics = H.free_port(), H.free_port()
    with process(binary, root / "source", "source", metrics_port=source_metrics) as (
        source,
        source_port,
        _,
    ):
        assert source.call("SET", "seed", "snapshot") == "OK"
        with faults(LAVIK_REPLICATION_PAUSE_BEFORE_COMMAND_APPLY_MS="6500"):
            with process(
                binary,
                root / "target",
                "target",
                workers=1,
                metrics_port=target_metrics,
            ) as (target, _, _):
                assert target.call("READONLY") == "OK"
                assert target.call("LAVIK.REPLICAOF", "127.0.0.1", source_port) == "OK"
                wait(
                    "native FULL did not complete",
                    lambda: info(target).get("lavik_replication_last_full_result")
                    == "success",
                )
                assert target.call("GET", "seed") == "snapshot"
                for client, metrics in [
                    (source, source_metrics),
                    (target, target_metrics),
                ]:
                    # Each process samples terminal results independently.
                    wait(
                        "local FULL result was not sampled",
                        lambda: sum(
                            values(
                                scrape(metrics),
                                "lavik_replication_fullsync_results_total",
                                result="success",
                            )
                        )
                        == 1,
                    )
                    record(
                        root,
                        "source-idle" if client is source else "target-idle",
                        client,
                        metrics,
                    )
                    text = scrape(metrics)
                    assert (
                        sum(
                            values(
                                text,
                                "lavik_replication_fullsync_results_total",
                                result="success",
                            )
                        )
                        == 1
                    ), text
                    assert values(text, "lavik_replication_pending_events") == [0, 0], (
                        text
                    )
                    assert not any(values(text, "lavik_replication_pending_work")), text
                    assert not any(
                        values(text, "lavik_replication_no_progress_seconds")
                    ), text
                assert source.call("SET", "paused-apply", "online") == "OK"
                wait(
                    "pending apply not observable",
                    lambda: any(
                        values(
                            scrape(target_metrics),
                            "lavik_replication_pending_work",
                            step="apply",
                        )
                    ),
                )
                wait(
                    "pending apply age did not advance",
                    lambda: max(
                        values(
                            scrape(target_metrics),
                            "lavik_replication_no_progress_seconds",
                            step="apply",
                        ),
                        default=0,
                    )
                    >= 2,
                )
                record(root, "target-pending", target, target_metrics)
                wait(
                    "apply did not recover",
                    lambda: target.call("GET", "paused-apply") == "online",
                )
                wait(
                    "pending ages did not clear",
                    lambda: not any(
                        values(
                            scrape(target_metrics),
                            "lavik_replication_no_progress_seconds",
                        )
                    ),
                )
                wait(
                    "applied counter missing",
                    lambda: sum(
                        values(
                            scrape(target_metrics),
                            "lavik_replication_applied_events_total",
                        )
                    )
                    >= 1,
                )
                assert (
                    sum(
                        values(
                            scrape(target_metrics),
                            "lavik_replication_fullsync_results_total",
                            result="success",
                        )
                    )
                    == 1
                )
                record(root, "target-recovered", target, target_metrics)
                assert target.call("LAVIK.REPLICAOF", "NO", "ONE") == "OK"
                text = scrape(target_metrics)
                assert not values(
                    text, "lavik_replication_cursor_next_lsn", side="target"
                ), text
                assert not values(
                    text, "lavik_replication_pending_events", side="target"
                ), text
                assert info(target)["lavik_replication_last_full_result"] == "success"
                record(root, "target-promoted", target, target_metrics)


def failed_finalization(binary, root, fault):
    root.mkdir()
    source_metrics, target_metrics = H.free_port(), H.free_port()
    with process(binary, root / "source", "source", metrics_port=source_metrics) as (
        source,
        source_port,
        _,
    ):
        assert source.call("SET", "seed", "snapshot") == "OK"
        with faults(**{fault: "1"}):
            with process(
                binary, root / "target", "target", metrics_port=target_metrics
            ) as (target, _, log):
                assert target.call("LAVIK.REPLICAOF", "127.0.0.1", source_port) == "OK"
                wait(
                    "FULL failure not retained",
                    lambda: info(target).get("lavik_replication_last_full_result")
                    == "failure",
                )
                observation = info(target)
                assert observation["lavik_replication_failed_stopped"] == "1", (
                    observation
                )
                assert observation["lavik_replication_population_ready"] == "0", (
                    observation
                )
                assert observation["lavik_replication_catalog_ready"] == "1", (
                    observation
                )
                assert observation["lavik_replication_loading"] == "1", observation
                assert observation["lavik_replication_last_full_phase"] == "promote", (
                    observation
                )
                assert observation["lavik_replication_last_full_reason"] == "storage", (
                    observation
                )
                assert observation[
                    "lavik_replication_last_full_promotion_complete"
                ] == (
                    "1"
                    if fault == "LAVIK_REPLICATION_FAIL_POPULATION_FINALIZE"
                    else "0"
                ), observation
                assert (
                    "injected" in observation["lavik_replication_last_full_detail"]
                ), observation
                text = scrape(target_metrics)
                assert values(
                    text,
                    "lavik_replication_fullsync_results_total",
                    side="target",
                    result="success",
                ) == [0], text
                assert values(
                    text,
                    "lavik_replication_fullsync_results_total",
                    side="target",
                    result="failure",
                ) == [1], text
                record(root, "failed", target, target_metrics)
                assert "native FULL" in log.read_text()
        # A process restart deliberately has no retained last-result record.
        with process(
            binary, root / "target", "restarted", metrics_port=target_metrics
        ) as (target, _, _):
            assert info(target)["lavik_replication_last_full_available"] == "0"
            assert values(
                scrape(target_metrics),
                "lavik_replication_fullsync_results_total",
                side="target",
                result="failure",
            ) == [0]
            record(root, "restart-empty-observation", target, target_metrics)
            assert target.call("LAVIK.REPLICAOF", "127.0.0.1", source_port) == "OK"
            wait(
                "restart did not complete a new FULL",
                lambda: info(target).get("lavik_replication_last_full_result")
                == "success",
            )
            record(root, "recovered", target, target_metrics)


def full_cancel_during_scrapes(binary, root):
    root.mkdir()
    source_metrics, target_metrics = H.free_port(), H.free_port()
    with faults(LAVIK_REPLICATION_PAUSE_FULLSYNC_BEFORE_CUT_MS="4500"):
        with process(
            binary, root / "source", "source", metrics_port=source_metrics
        ) as (source, source_port, _):
            assert source.call("SET", "seed", "snapshot") == "OK"
            # The target need not inherit the source-only fault setting.
            with process(
                binary, root / "target", "target", metrics_port=target_metrics
            ) as (target, _, _):
                stop = threading.Event()
                failures = []
                samples = []

                def collect():
                    try:
                        while not stop.is_set():
                            text = scrape(target_metrics)
                            samples.append(text)
                            stop.wait(0.025)
                    except Exception as error:
                        failures.append(str(error))

                thread = threading.Thread(target=collect)
                thread.start()
                try:
                    assert (
                        target.call("LAVIK.REPLICAOF", "127.0.0.1", source_port) == "OK"
                    )
                    wait(
                        "catalog completion before population missing",
                        lambda: info(target).get("lavik_replication_catalog_ready")
                        == "1"
                        and info(target).get("lavik_replication_population_ready")
                        == "0",
                    )
                    record(root, "catalog-only", target, target_metrics)
                    text = scrape(target_metrics)
                    assert not values(
                        text, "lavik_replication_cursor_next_lsn", side="target"
                    ), text
                    assert not values(
                        text, "lavik_replication_pending_events", side="target"
                    ), text
                    assert target.call("LAVIK.REPLICAOF", "NO", "ONE") == "OK"
                    wait(
                        "normal replacement must be cancelled",
                        lambda: info(target).get("lavik_replication_last_full_result")
                        == "cancelled",
                    )
                    assert values(
                        scrape(target_metrics),
                        "lavik_replication_fullsync_results_total",
                        side="target",
                        result="failure",
                    ) == [0]
                    record(root, "cancelled", target, target_metrics)
                    text = scrape(target_metrics)
                    assert not values(
                        text,
                        "lavik_replication_fullsync_elapsed_seconds",
                        side="target",
                    ), text
                    assert (
                        target.call("LAVIK.REPLICAOF", "127.0.0.1", source_port) == "OK"
                    )
                    assert (
                        info(target)["lavik_replication_last_full_result"]
                        == "cancelled"
                    )
                    wait(
                        "replacement did not finish",
                        lambda: info(target).get("lavik_replication_last_full_result")
                        == "success",
                    )
                    record(root, "replacement", target, target_metrics)
                finally:
                    stop.set()
                    thread.join(timeout=15)
                    assert not thread.is_alive()
                assert not failures, failures
                assert len(samples) > 10
                (root / "scrapes.json").write_text(json.dumps(samples))


def multiple_peers_and_owners(binary, root):
    """Owner batching preserves each peer, flow and final counter across retirement."""
    root.mkdir()
    ports = [H.free_port() for _ in range(3)]
    with ExitStack() as stack:
        source, source_port, _ = stack.enter_context(
            process(
                binary,
                root / "source",
                "source",
                workers=3,
                max_memory="4G",
                metrics_port=ports[0],
            )
        )
        assert source.call("SET", "seed", "snapshot") == "OK"
        targets = []
        for index, workers in enumerate((1, 2)):
            target, _, _ = stack.enter_context(
                process(
                    binary,
                    root / f"target-{index}",
                    "target",
                    workers=workers,
                    metrics_port=ports[index + 1],
                )
            )
            targets.append(target)
            assert target.call("LAVIK.REPLICAOF", "127.0.0.1", source_port) == "OK"
            wait(
                "peer FULL did not complete",
                lambda: info(target).get("lavik_replication_last_full_result")
                == "success",
            )
            observation = info(target)
            assert observation["lavik_replication_progress_flows"] == "3", observation
        wait(
            "source did not observe both peers",
            lambda: values(
                scrape(ports[0]),
                "lavik_replication_native_flow_connections",
                side="source",
            )
            == [2, 2, 2],
        )

        def total(port, suffix):
            return sum(values(scrape(port), "lavik_replication_" + suffix))

        # FULL result sampling has already drained its byte deltas on each node.
        wait(
            "source FULL totals missing",
            lambda: sum(
                values(
                    scrape(ports[0]),
                    "lavik_replication_fullsync_results_total",
                    result="success",
                )
            )
            == 2,
        )
        before = [total(port, "transfer_bytes_total") for port in ports]
        stop, failures = threading.Event(), []

        def collect():
            client = Client(source_port)
            try:
                while not stop.is_set():
                    observation = info(client)
                    if observation["lavik_replication_observation_available"] == "1":
                        rows = [
                            dict(
                                part.split("=", 1)
                                for part in observation[
                                    f"lavik_replication_progress{i}"
                                ].split(",")
                            )
                            for i in range(
                                int(observation["lavik_replication_progress_flows"])
                            )
                        ]
                        assert len(
                            {(row["session"], row["flow"]) for row in rows}
                        ) == len(rows)
                    for port in ports:
                        scrape(port)
                    stop.wait(0.01)
            except Exception as error:
                failures.append(repr(error))
            finally:
                client.close()

        thread = threading.Thread(target=collect)
        thread.start()
        try:
            for index in range(128):
                assert source.call("SET", f"online-{index}", "value") == "OK"
            assert source.call("SET", "fragmented", "x" * (2 * 1024 * 1024)) == "OK"
            assert source.call("WAIT", 2, 10000) == 2
            wait(
                "batched collector lost ONLINE events",
                lambda: [total(port, "transfer_events_total") for port in ports]
                == [258, 129, 129],
            )
            after = [total(port, "transfer_bytes_total") for port in ports]
            delta = [end - start for start, end in zip(before, after)]
            assert delta[0] == delta[1] + delta[2] and delta[1] == delta[2], delta
            assert targets[0].call("LAVIK.REPLICAOF", "NO", "ONE") == "OK"
            wait(
                "retired peer remained in source rows",
                lambda: values(
                    scrape(ports[0]),
                    "lavik_replication_native_flow_connections",
                    side="source",
                )
                == [1, 1, 1],
            )
            assert source.call("SET", "remaining-peer", "value") == "OK"
            assert source.call("WAIT", 1, 10000) == 1
            wait(
                "retirement lost or duplicated process counters",
                lambda: [total(port, "transfer_events_total") for port in ports]
                == [259, 129, 130],
            )
            for index, client in enumerate((source, *targets)):
                record(root, f"retired-{index}", client, ports[index])
        finally:
            stop.set()
            thread.join(timeout=15)
            assert not thread.is_alive()
        assert not failures, failures


def main():
    binary = sys.argv[1]
    parent = os.environ.get("LAVIK_TEST_DATA_DIR")
    root = Path(tempfile.mkdtemp(prefix="replication-observability-", dir=parent))
    print(f"evidence: {root}", flush=True)
    online_progress(binary, root)
    full_cancel_during_scrapes(binary, root / "cancel")
    multiple_peers_and_owners(binary, root / "multiple-peers")
    for fault in (
        "LAVIK_REPLICATION_FAIL_PROMOTE_ONCE",
        "LAVIK_REPLICATION_FAIL_POPULATION_FINALIZE",
    ):
        failed_finalization(binary, root / fault.lower(), fault)
    (root / "metric-traces.json").write_text(json.dumps(METRIC_SAMPLES))
    print("native observability: PASS", flush=True)


if __name__ == "__main__":
    main()
