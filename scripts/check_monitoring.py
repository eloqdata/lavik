#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
"""Validate shipped config, rule behavior, and alert navigation with pinned tools."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

import yaml

ROOT = Path(__file__).resolve().parents[1]
MONITORING = ROOT / "deploy/monitoring"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tools", type=Path, required=True)
    args = parser.parse_args()

    def run(tool, *arguments):
        subprocess.run([str(args.tools / tool), *map(str, arguments)], check=True)

    run(
        "promtool",
        "check",
        "rules",
        *sorted((MONITORING / "prometheus/rules").glob("*.yml")),
    )
    run(
        "promtool",
        "test",
        "rules",
        *sorted((MONITORING / "prometheus/tests").glob("*.test.yml")),
    )
    with tempfile.TemporaryDirectory() as directory:
        stage = Path(directory)
        shutil.copytree(MONITORING / "prometheus", stage / "prometheus")
        env = dict(
            os.environ,
            LAVIK_TARGETS="data:9100",
            TARGET_DIR=str(stage / "prometheus/targets"),
        )
        subprocess.run(
            ["sh", str(MONITORING / "configure-targets.sh")], env=env, check=True
        )
        run("promtool", "check", "config", stage / "prometheus/prometheus.yml")
        manager = yaml.safe_load(
            (MONITORING / "alertmanager/alertmanager.yml").read_text()
        )
        manager["templates"] = [str(MONITORING / "alertmanager/templates/*.tmpl")]
        config = stage / "alertmanager.yml"
        config.write_text(yaml.safe_dump(manager))
        run("amtool", "check-config", config)
        receivers = yaml.safe_load(
            (MONITORING / "alertmanager/receivers.example.yml").read_text()
        )
        secret = stage / "webhook-url"
        secret.write_text("http://127.0.0.1:1/test")
        for receiver in receivers["receivers"]:
            for webhook in receiver.get("webhook_configs", []):
                webhook["url_file"] = str(secret)
        manager["receivers"] = receivers["receivers"]
        config.write_text(yaml.safe_dump(manager))
        run("amtool", "check-config", config)

    # Grafana stores PromQL as strings; JSON/navigation checks alone cannot
    # catch a malformed label_replace chain or new panel expression.
    def dashboard_queries(panels):
        for panel in panels:
            for target in panel.get("targets", []):
                if "expr" not in target:
                    continue
                expression = (
                    target["expr"]
                    .replace("$instance", ".*")
                    .replace("$__rate_interval", "1m")
                    .replace("$__range", "1h")
                    .replace("@ end()", "@ 1700000000")
                )
                yield {
                    "record": f"dashboard_panel_{panel['id']}_{target['refId']}",
                    "expr": expression,
                }
            yield from dashboard_queries(panel.get("panels", []))

    for dashboard_name, table_id, runbook_name, rule_name in (
        ("lavik-overview", 37, "alerts", "alerts"),
        ("lavik-meta", 65, "meta-monitoring", "meta-alerts"),
    ):
        dashboard = json.loads(
            (MONITORING / f"grafana/dashboards/{dashboard_name}.json").read_text()
        )

        if dashboard_name == "lavik-overview":
            with tempfile.TemporaryDirectory() as directory:
                queries = Path(directory) / "dashboard-queries.yml"
                queries.write_text(
                    yaml.safe_dump(
                        {
                            "groups": [
                                {
                                    "name": "dashboard-syntax",
                                    "rules": list(
                                        dashboard_queries(dashboard["panels"])
                                    ),
                                }
                            ]
                        }
                    )
                )
                run("promtool", "check", "rules", queries)

        def panel_ids(panels):
            return {p["id"] for p in panels} | {
                i for p in panels for i in panel_ids(p.get("panels", []))
            }

        ids = panel_ids(dashboard["panels"])
        current_alerts = next(p for p in dashboard["panels"] if p["id"] == table_id)
        # ALERTS contains labels, not annotations. Keep its dashboard navigation map
        # aligned with the rule annotations used by notification receivers.
        alert_routes = {
            name: (anchor, int(panel))
            for _, anchor, panel, name in re.findall(
                r'"detail", "([^"|]+)\|([^"|]+)\|(\d+)", "alertname", "([^"|]+)"',
                current_alerts["targets"][0]["expr"],
            )
        }
        runbook = (ROOT / f"docs/operations/{runbook_name}.md").read_text()
        anchors = {
            line[3:].strip().lower().replace(" ", "-")
            for line in runbook.splitlines()
            if line.startswith("## ")
        }
        rules = yaml.safe_load(
            (MONITORING / f"prometheus/rules/{rule_name}.yml").read_text()
        )
        for group in rules["groups"]:
            for rule in group["rules"]:
                annotations = rule["annotations"]
                assert (
                    int(re.search(r"viewPanel=(\d+)", annotations["dashboard_url"])[1])
                    in ids
                ), rule["alert"]
                assert annotations["runbook_url"].split("#")[1] in anchors, rule[
                    "alert"
                ]
                assert annotations["summary"] and annotations["description"], rule[
                    "alert"
                ]
                assert alert_routes[rule["alert"]] == (
                    annotations["runbook_url"].split("#")[1],
                    int(re.search(r"viewPanel=(\d+)", annotations["dashboard_url"])[1]),
                ), rule["alert"]
    print("PASS: monitoring configuration, rules and navigation")


if __name__ == "__main__":
    main()
