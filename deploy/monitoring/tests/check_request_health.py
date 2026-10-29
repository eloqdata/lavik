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
TITLES = ("Execution Failure Ratio", "Admission Rejection Ratio", "Service Fault Ratio")
LABELS = 'job="lavik",instance="node:9100",command="get",context="client"'
OUTPUT = '{command="get",context="client",instance="node:9100"}'


def series(result, values):
    return {
        "series": f'lavik_command_results_total{{{LABELS},result="{result}"}}',
        "values": values,
    }


def error(reason, result, values):
    return {
        "series": f'lavik_command_errors_total{{{LABELS},result="{result}",reason="{reason}"}}',
        "values": values,
    }


def main():
    panels = json.loads(DASHBOARD.read_text())["panels"]
    queries = {}
    for panel in panels:
        if panel["title"] in TITLES:
            expr = panel["targets"][0]["expr"]
            for name, value in {
                "$instance": "node:9100",
                "${command:regex}": ".*",
                "${context:regex}": "client",
                "$__rate_interval": "5m",
            }.items():
                expr = expr.replace(name, value)
            queries[panel["title"]] = expr
    assert set(queries) == set(TITLES)
    cases = [
        (
            "normal and client errors",
            [
                series("success", "0+540x10"),
                series("failed", "0+30x10"),
                series("rejected", "0+30x10"),
                error("storage", "failed", "0+12x10"),
                error("wrongtype", "failed", "0+18x10"),
                error("noauth", "rejected", "0+30x10"),
            ],
            (0.05, 0.05, 0.02),
            "1+0x10",
        ),
        (
            "zero traffic, unseen errors",
            [series("success", "0+0x10")],
            (0, 0, 0),
            "1+0x10",
        ),
        (
            "low traffic",
            [
                series("success", "0+0.054x10"),
                series("failed", "0+0.006x10"),
                error("internal", "failed", "0+0.006x10"),
            ],
            (0.1, 0, 0.1),
            "1+0x10",
        ),
        (
            "counter reset",
            [
                series("success", "0 54 108 162 216 270 0 54 108 162 216"),
                series("failed", "0 6 12 18 24 30 0 6 12 18 24"),
                error("storage", "failed", "0 6 12 18 24 30 0 6 12 18 24"),
            ],
            (0.1, 0, 0.1),
            "1+0x10",
        ),
        (
            "routing and queueing",
            [
                series("redirected", "0+60x10"),
                series("queued", "0+600x10"),
                error("moved", "redirected", "0+60x10"),
            ],
            (0, 0, 0),
            "1+0x10",
        ),
        (
            "failed scrape with recent samples",
            [series("success", "0+60x10")],
            None,
            "1 1 1 1 1 1 1 1 1 1 0",
        ),
        (
            "missing target with recent samples",
            [series("success", "0+60x10")],
            None,
            "1 1 1 1 1 1 stale _ _ _ _",
        ),
        ("missing command metrics", [], None, "1+0x10"),
    ]
    tests = []
    for name, inputs, expected, up in cases:
        tests.append(
            {
                "name": name,
                "interval": "1m",
                "input_series": inputs
                + [{"series": 'up{job="lavik",instance="node:9100"}', "values": up}],
                "promql_expr_test": [
                    {
                        "expr": "round((" + queries[title] + "), 1e-12)",
                        "eval_time": "10m",
                        "exp_samples": []
                        if expected is None
                        else [{"labels": OUTPUT, "value": expected[i]}],
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
