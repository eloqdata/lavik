#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
"""Run production rule expressions through real Prometheus and Alertmanager.

Only timing is accelerated. HTTP fixtures stand in for controllable metric sources
and notification channels; --lavik-bin additionally exercises a real paused server.
Artifacts are retained in --output, including webhook bodies and process logs.
"""

import argparse
import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time
import urllib.request

import yaml

ROOT = Path(__file__).resolve().parents[2]
MONITORING = ROOT / "deploy/monitoring"


def wait_for(description, predicate, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (OSError, ValueError):
            pass
        time.sleep(0.25)
    raise AssertionError(f"Timed out: {description}")


def fetch(url):
    with urllib.request.urlopen(url, timeout=2) as response:
        return response.read()


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Endpoints:
    """Thread-safe fixture exporter and recording webhook with bounded test life."""

    def __init__(self, output):
        self.lock = threading.Lock()
        self.metrics = "lavik_server_ready 0\n"
        self.available = True
        self.deliveries = []
        self.output = output
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                with owner.lock:
                    body = owner.metrics.encode()
                    status = 200 if owner.available else 503
                self.send_response(status)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                length = int(self.headers["Content-Length"])
                if length > 1024 * 1024:
                    self.send_error(413)
                    return
                delivery = {
                    "path": self.path,
                    "body": json.loads(self.rfile.read(length)),
                }
                with owner.lock:
                    owner.deliveries.append(delivery)
                    with (owner.output / "webhooks.jsonl").open("a") as stream:
                        stream.write(json.dumps(delivery) + "\n")
                self.send_response(200)
                self.end_headers()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self):
        self.thread.start()
        return self

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def set(self, metrics=None, available=True):
        with self.lock:
            if metrics is not None:
                self.metrics = metrics
            self.available = available

    def matching(self, alert, status="firing", receiver=None, instance=None):
        with self.lock:
            return [
                d
                for d in self.deliveries
                if (receiver is None or d["path"] == "/" + receiver)
                and any(
                    a["labels"]["alertname"] == alert
                    and a["status"] == status
                    and (instance is None or a["labels"].get("instance") == instance)
                    for a in d["body"]["alerts"]
                )
            ]


def write_yaml(path, contents):
    path.write_text(yaml.safe_dump(contents, sort_keys=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tools", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--lavik-bin", type=Path)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    processes = []
    with contextlib.ExitStack() as stack:

        def start(name, command, env=None):
            log = stack.enter_context((args.output / f"{name}.log").open("w"))
            proc = subprocess.Popen(
                [str(c) for c in command], stdout=log, stderr=subprocess.STDOUT, env=env
            )
            processes.append(proc)
            return proc

        def stop_all():
            for proc in reversed(processes):
                if proc.poll() is None:
                    proc.send_signal(signal.SIGCONT)
                    proc.terminate()
            for proc in reversed(processes):
                try:
                    proc.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()

        stack.callback(stop_all)
        fixture = Endpoints(args.output).start()
        stack.callback(fixture.close)
        fixture_address = f"127.0.0.1:{fixture.server.server_port}"
        prom_port, am_port = port(), port()
        prom_url = f"http://127.0.0.1:{prom_port}"
        am_url = f"http://127.0.0.1:{am_port}"
        rules = yaml.safe_load((MONITORING / "prometheus/rules/alerts.yml").read_text())
        for group in rules["groups"]:
            for rule in group["rules"]:
                rule["for"] = "3s"
                rule["keep_firing_for"] = "6s"
                # The full 15-minute failure window is checked by promtool.
                rule["expr"] = rule["expr"].replace("[15m]", "[15s]")
        write_yaml(args.output / "alerts.yml", rules)
        recordings = yaml.safe_load(
            (MONITORING / "prometheus/rules/recording.yml").read_text()
        )
        write_yaml(args.output / "recording.yml", recordings)
        am_config = yaml.safe_load(
            (MONITORING / "alertmanager/alertmanager.yml").read_text()
        )
        am_config["templates"] = [str(MONITORING / "alertmanager/templates/*.tmpl")]
        am_config["route"].update(
            group_wait="2s", group_interval="2s", repeat_interval="1h"
        )
        for receiver in am_config["receivers"]:
            receiver["webhook_configs"] = [
                {
                    "url": f"http://{fixture_address}/{receiver['name']}",
                    "send_resolved": True,
                }
            ]
        write_yaml(args.output / "alertmanager.yml", am_config)
        # Exercise external Alertmanager file discovery: Prometheus has no local
        # service dependency or knowledge of how the receiver process was started.
        (args.output / "alertmanagers.json").write_text(
            json.dumps([{"targets": [f"127.0.0.1:{am_port}"]}])
        )
        targets = [
            {
                "targets": [fixture_address],
                "labels": {"cluster": "drill", "lavik_mode": "standalone"},
            }
        ]
        lavik = None
        if args.lavik_bin:
            lavik_metrics, lavik_redis = port(), port()
            data = args.output / "lavik.data"
            with data.open("wb") as stream:
                os.posix_fallocate(stream.fileno(), 0, 256 * 1024 * 1024)
            lavik = start(
                "lavik",
                [
                    args.lavik_bin.resolve(),
                    "--logtostderr",
                    "--bind",
                    "127.0.0.1",
                    "--port",
                    lavik_redis,
                    "--metrics-port",
                    lavik_metrics,
                    "--threads",
                    "2",
                    "--no-pin-workers",
                    "--recv-buffers-per-worker",
                    "0",
                    "--max-memory",
                    "1GiB",
                    "--data-file",
                    data,
                ],
            )
            real_address = f"127.0.0.1:{lavik_metrics}"
            wait_for(
                "real Lavik ready",
                lambda: b"lavik_server_ready 1"
                in fetch(f"http://{real_address}/metrics"),
            )
            targets.append(
                {
                    "targets": [real_address],
                    "labels": {"cluster": "drill-real", "lavik_mode": "standalone"},
                }
            )
        (args.output / "targets.json").write_text(json.dumps(targets))
        prom_config = {
            "global": {
                "scrape_interval": "1s",
                "scrape_timeout": "500ms",
                "evaluation_interval": "1s",
                "external_labels": {"grafana_url": "http://monitor.example:3000"},
            },
            "rule_files": ["recording.yml", "alerts.yml"],
            "scrape_configs": [
                {
                    "job_name": "lavik",
                    "file_sd_configs": [
                        {"files": ["targets.json"], "refresh_interval": "1s"}
                    ],
                }
            ],
            "alerting": {
                "alertmanagers": [
                    {
                        "file_sd_configs": [
                            {"files": ["alertmanagers.json"], "refresh_interval": "1s"}
                        ]
                    }
                ]
            },
        }
        write_yaml(args.output / "prometheus.yml", prom_config)
        for tool, arguments in [
            ("promtool", ["check", "config", args.output / "prometheus.yml"]),
            ("amtool", ["check-config", args.output / "alertmanager.yml"]),
        ]:
            subprocess.run([str(args.tools / tool), *map(str, arguments)], check=True)
        start(
            "alertmanager",
            [
                args.tools / "alertmanager",
                "--config.file=" + str(args.output / "alertmanager.yml"),
                "--storage.path=" + str(args.output / "am-data"),
                f"--web.listen-address=127.0.0.1:{am_port}",
                "--cluster.listen-address=",
            ],
        )
        start(
            "prometheus",
            [
                args.tools / "prometheus",
                "--config.file=" + str(args.output / "prometheus.yml"),
                "--storage.tsdb.path=" + str(args.output / "prom-data"),
                f"--web.listen-address=127.0.0.1:{prom_port}",
                "--rules.alert.resend-delay=1s",
            ],
        )
        wait_for("Prometheus ready", lambda: fetch(prom_url + "/-/ready"))
        wait_for("Alertmanager ready", lambda: fetch(am_url + "/-/ready"))

        def active(name, severity=None):
            alerts = json.loads(fetch(am_url + "/api/v2/alerts"))
            return [
                a
                for a in alerts
                if a["labels"].get("instance") == fixture_address
                and a["labels"]["alertname"] == name
                and (severity is None or a["labels"].get("severity") == severity)
            ]

        wait_for(
            "not-ready critical route",
            lambda: fixture.matching("LavikServerNotReady", receiver="critical"),
        )
        warning = wait_for(
            "warning inhibited by critical",
            lambda: [
                a
                for a in active("LavikServerNotReady", "warning")
                if a["status"]["inhibitedBy"]
            ],
        )
        assert warning
        assert not fixture.matching("LavikServerNotReady", receiver="warning")
        count = len(fixture.matching("LavikServerNotReady", receiver="critical"))
        # Longer than several Prometheus resend cycles; no repeat notification.
        time.sleep(6)
        assert (
            len(fixture.matching("LavikServerNotReady", receiver="critical")) == count
        )
        fixture.set(available=False)
        wait_for(
            "target down critical route",
            lambda: fixture.matching("LavikTargetDown", receiver="critical"),
        )
        wait_for(
            "target down inhibits previously firing readiness",
            lambda: [
                a
                for a in active("LavikServerNotReady", "critical")
                if a["status"]["inhibitedBy"]
            ],
        )
        fixture.set("lavik_server_ready 1\n")
        wait_for(
            "target down resolved",
            lambda: fixture.matching("LavikTargetDown", "resolved", "critical"),
        )
        wait_for(
            "missing telemetry warning route",
            lambda: fixture.matching("LavikMetricsMissing", receiver="warning"),
        )
        # Returning to recovery makes these families legitimately absent.
        fixture.set("lavik_server_ready 0\n")
        wait_for(
            "missing telemetry resolved",
            lambda: fixture.matching("LavikMetricsMissing", "resolved", "warning"),
        )
        healthy = (Path(__file__).parent / "healthy.prom").read_text()
        pressure = healthy.replace(
            'lavik_fullsync_sessions{worker="0"} 0',
            'lavik_fullsync_sessions{worker="0"} 1',
        ).replace(
            'lavik_fullsync_publish_queue_bytes{worker="0"} 0',
            'lavik_fullsync_publish_queue_bytes{worker="0"} 85',
        )
        fixture.set(pressure)
        wait_for(
            "FULL queue pressure warning",
            lambda: fixture.matching("LavikFullSyncQueuePressure", receiver="warning"),
        )
        fixture.set(healthy)
        wait_for(
            "FULL queue pressure resolved",
            lambda: fixture.matching(
                "LavikFullSyncQueuePressure", "resolved", "warning"
            ),
        )
        # Exercise the production progress predicate with synthetic ages; the
        # sampler's wall-clock behavior is covered by native process tests.
        stalled = healthy + "\n".join(
            [
                'lavik_replication_native_flow_connections{side="target",worker="0",flow="0"} 1',
                'lavik_replication_pending_work{side="target",worker="0",flow="0",step="apply"} 1',
                'lavik_replication_no_progress_seconds{side="target",worker="0",flow="0",step="apply"} 500',
                "",
            ]
        )
        fixture.set(stalled)
        wait_for(
            "native stall critical notification",
            lambda: fixture.matching("LavikReplicationStalled", receiver="critical"),
        )
        fixture.set(healthy)
        wait_for(
            "native stall resolved notification",
            lambda: fixture.matching("LavikReplicationStalled", "resolved", "critical"),
        )
        loading = healthy + "\n".join(
            [
                "lavik_replication_loading_no_progress_seconds 901",
                "lavik_replication_loading 1",
                "lavik_replication_population_ready 0",
                "lavik_replication_waiting 0",
                "",
            ]
        )
        fixture.set(loading)
        wait_for(
            "population recovery critical notification",
            lambda: fixture.matching(
                "LavikPopulationRecoveryStalled", receiver="critical"
            ),
        )
        fixture.set(healthy)
        wait_for(
            "population recovery resolved notification",
            lambda: fixture.matching(
                "LavikPopulationRecoveryStalled", "resolved", "critical"
            ),
        )
        fixture.set(
            healthy
            + 'lavik_replication_fullsync_results_total{side="target",result="failure"} 0\n'
        )
        time.sleep(3)
        fixture.set(
            healthy
            + 'lavik_replication_fullsync_results_total{side="target",result="failure"} 4\n'
        )
        wait_for(
            "FULL failures warning notification",
            lambda: fixture.matching("LavikFullSyncFailures", receiver="warning"),
        )
        fixture.set(healthy)
        wait_for(
            "FULL failures resolved notification",
            lambda: fixture.matching("LavikFullSyncFailures", "resolved", "warning"),
        )
        assert not fixture.matching("LavikMetaDisconnected")
        if lavik:
            lavik.send_signal(signal.SIGSTOP)
            wait_for(
                "real paused Lavik firing",
                lambda: fixture.matching(
                    "LavikTargetDown", receiver="critical", instance=real_address
                ),
            )
            lavik.send_signal(signal.SIGCONT)
            wait_for(
                "real resumed Lavik resolved",
                lambda: fixture.matching(
                    "LavikTargetDown", "resolved", "critical", real_address
                ),
            )
        for delivery in fixture.deliveries:
            for alert in delivery["body"]["alerts"]:
                assert alert["annotations"]["dashboard_url"].startswith(
                    "http://monitor.example:3000/d/lavik-overview/"
                )
                assert "viewPanel=" in alert["annotations"]["dashboard_url"]
                assert (
                    "/docs/operations/alerts.md#" in alert["annotations"]["runbook_url"]
                )
        (args.output / "result.json").write_text(
            json.dumps(
                {
                    "status": "passed",
                    "real_lavik": bool(lavik),
                    "notifications": len(fixture.deliveries),
                },
                indent=2,
            )
            + "\n"
        )
        print(
            f"PASS: notifications, deduplication, inhibition and recovery; artifacts: {args.output}"
        )


if __name__ == "__main__":
    main()
