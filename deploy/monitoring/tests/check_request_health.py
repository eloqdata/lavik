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

"""Exercise the actual dashboard ratio queries using promtool's rule engine.

Requires promtool on PATH; uses only the Python standard library. JSON is a
YAML subset, so fixtures need no PyYAML dependency and no production rules.
"""

import json
from pathlib import Path
import subprocess
import tempfile

DASHBOARD = (
    Path(__file__).resolve().parents[1] / "grafana/dashboards/lavik-overview.json"
)
TITLES = ("Execution Failure Ratio", "Admission Rejection Ratio")
LABELS = 'job="lavik",instance="node:9100",command="get"'
OUTPUT = '{command="get",instance="node:9100"}'


def counters(calls, failed="0+0x10", rejected="0+0x10"):
    return [
        {"series": f"lavik_command_{name}_total{{{LABELS}}}", "values": values}
        for name, values in (
            ("executions", calls),
            ("failed_calls", failed),
            ("rejected_calls", rejected),
        )
    ]


def main():
    dashboard = DASHBOARD.read_text()
    # Generic regex formatting bypasses the Prometheus datasource's extra
    # escaping for quoted PromQL strings (for example config|get).
    assert "${command:regex}" not in dashboard
    panels = json.loads(dashboard)["panels"]
    queries = {}
    for panel in panels:
        if panel["title"] in TITLES:
            expr = panel["targets"][0]["expr"]
            for name, value in {
                "$instance": "node:9100",
                "$__rate_interval": "5m",
            }.items():
                expr = expr.replace(name, value)
            queries[panel["title"]] = expr
    assert set(queries) == set(TITLES)
    cases = [
        (
            "client errors and rejections",
            counters("0+570x10", "0+30x10", "0+30x10"),
            (30 / 570, 0.05),
            "1+0x10",
        ),
        ("observed command, zero traffic", counters("0+0x10"), (0, 0), "1+0x10"),
        ("low traffic", counters("0+0.06x10", "0+0.006x10"), (0.1, 0), "1+0x10"),
        (
            "counter reset",
            counters(
                "0 60 120 180 240 300 0 60 120 180 240", "0 6 12 18 24 30 0 6 12 18 24"
            ),
            (0.1, 0),
            "1+0x10",
        ),
        (
            "redirects are rejections",
            counters("0+0x10", rejected="0+60x10"),
            (0, 1),
            "1+0x10",
        ),
        (
            "failed scrape with recent samples",
            counters("0+60x10"),
            None,
            "1 1 1 1 1 1 1 1 1 1 0",
        ),
        (
            "missing target with recent samples",
            counters("0+60x10"),
            None,
            "1 1 1 1 1 1 stale _ _ _ _",
        ),
        ("missing command metrics", [], None, "1+0x10"),
    ]
    tests = []
    for command, selector in (
        ("get", ".*"),
        ("get", "get"),
        ("config|get", r"config\\|get"),
    ):
        for name, inputs, expected, up in cases:
            selected_inputs = [
                {
                    **sample,
                    "series": sample["series"].replace(
                        'command="get"', f'command="{command}"'
                    ),
                }
                for sample in inputs
            ]
            if command == "config|get":
                # An unescaped | would incorrectly select these two commands.
                selected_inputs += [
                    {
                        **sample,
                        "series": sample["series"].replace(
                            'command="get"', f'command="{other}"'
                        ),
                    }
                    for other in ("config", "get")
                    for sample in counters("0+600x10", "0+600x10", "0+600x10")
                ]
            tests.append(
                {
                    "name": f"{name}, selector={selector}",
                    "interval": "1m",
                    "input_series": selected_inputs
                    + [
                        {"series": 'up{job="lavik",instance="node:9100"}', "values": up}
                    ],
                    "promql_expr_test": [
                        {
                            "expr": "round(("
                            + queries[title].replace("$command", selector)
                            + "), 1e-12)",
                            "eval_time": "10m",
                            "exp_samples": []
                            if expected is None
                            else [
                                {
                                    "labels": OUTPUT.replace(
                                        'command="get"', f'command="{command}"'
                                    ),
                                    "value": round(expected[i], 12),
                                }
                            ],
                        }
                        for i, title in enumerate(TITLES)
                    ],
                }
            )
    with tempfile.TemporaryDirectory(prefix="lavik-request-health-") as tmp:
        path = Path(tmp) / "tests.json"
        path.write_text(
            json.dumps({"rule_files": [], "evaluation_interval": "1m", "tests": tests})
        )
        subprocess.run(["promtool", "test", "rules", str(path)], check=True)


if __name__ == "__main__":
    main()
