#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
"""Exercise the operator-facing target configuration command."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
MONITORING = ROOT / "deploy/monitoring"


class TargetConfiguration(unittest.TestCase):
    def run_config(self, **settings):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                ["sh", str(MONITORING / "configure-targets.sh")],
                env={"PATH": os.environ["PATH"], "TARGET_DIR": directory, **settings},
                capture_output=True,
                text=True,
            )
            files = {
                p.name: json.loads(p.read_text())
                for p in Path(directory).glob("*.json")
            }
            return result, files

    def test_legacy_targets_default_to_standalone(self):
        result, files = self.run_config(LAVIK_TARGETS="one:9100,[::1]:9101")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            files["lavik.json"],
            [
                {
                    "targets": ["one:9100", "[::1]:9101"],
                    "labels": {
                        "service": "lavik",
                        "cluster": "default",
                        "lavik_mode": "standalone",
                    },
                }
            ],
        )
        self.assertEqual(files["alertmanagers.json"], [])

    def test_managed_cluster_and_external_alertmanager(self):
        result, files = self.run_config(
            LAVIK_TARGETS="data:9100",
            LAVIK_MODE="meta-managed",
            LAVIK_CLUSTER="orders",
            ALERTMANAGER_TARGETS="am:9093",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(files["lavik.json"][0]["labels"]["lavik_mode"], "meta-managed")
        self.assertEqual(files["lavik.json"][0]["labels"]["cluster"], "orders")
        self.assertEqual(files["alertmanagers.json"], [{"targets": ["am:9093"]}])

    def test_invalid_or_duplicate_targets_are_rejected(self):
        for targets in (
            "",
            "one:9100,",
            "one:9100,,two:9100",
            "one:9100,one:9100",
            "one:9100\n",
            'one:9100"',
            "one:99999",
            "one:0",
            "http://one:9100",
        ):
            with self.subTest(targets=targets):
                result, _ = self.run_config(LAVIK_TARGETS=targets)
                self.assertNotEqual(result.returncode, 0)

    def test_label_injection_and_unknown_mode_are_rejected(self):
        for settings in ({"LAVIK_CLUSTER": 'x"'}, {"LAVIK_MODE": "single"}):
            result, _ = self.run_config(LAVIK_TARGETS="data:9100", **settings)
            self.assertNotEqual(result.returncode, 0)

    def test_explicit_file_preserves_mixed_target_labels(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as source:
            contents = [
                {
                    "targets": ["managed:9100"],
                    "labels": {"cluster": "a", "lavik_mode": "meta-managed"},
                },
                {
                    "targets": ["alone:9100"],
                    "labels": {"cluster": "b", "lavik_mode": "standalone"},
                },
            ]
            json.dump(contents, source)
            source.flush()
            result, files = self.run_config(
                LAVIK_TARGETS_FILE=source.name, TARGET_INPUT=source.name
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(files["lavik.json"], contents)


if __name__ == "__main__":
    unittest.main()
