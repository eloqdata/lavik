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

"""Exercise coverage accounting and the runner against a tiny real CTest tree."""

from collections import Counter
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "ci_test_plan", ROOT / "scripts/ci_test_plan.py"
)
PLANNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PLANNER)
ESTIMATES = {
    "default_seconds": 1,
    "prefix_seconds": {"meta.": 20, "meta.slow.": 50},
    "ctest_seconds": {"large": 100},
    "external_seconds": {name: 40 for name in PLANNER.EXTERNAL_SUITES},
}


def inventory(names):
    return {"tests": [{"name": name} for name in names]}


class PlannerTests(unittest.TestCase):
    def test_exact_coverage_including_new_cases_and_external_suites(self):
        names = [
            "large",
            "meta.slow.new",
            "new.[case]+",
            *[f"small.{i}" for i in range(31)],
        ]
        plan = PLANNER.make_plan(inventory(names), 6, ESTIMATES)
        items = [item for shard in plan["shards"] for item in shard["items"]]
        self.assertEqual(
            Counter(item["name"] for item in items if item["kind"] == "ctest"),
            Counter(names),
        )
        self.assertEqual(
            Counter(item["name"] for item in items if item["kind"] == "external"),
            Counter(PLANNER.EXTERNAL_SUITES),
        )
        self.assertEqual(
            sorted(item["ctest_index"] for item in items if item["kind"] == "ctest"),
            list(range(1, len(names) + 1)),
        )
        self.assertEqual(PLANNER.estimated_seconds("meta.slow.new", ESTIMATES), 50)
        self.assertEqual(PLANNER.estimated_seconds("new.[case]+", ESTIMATES), 1)

    def test_deterministic_assignment_even_if_discovery_order_changes(self):
        names = ["large", "meta.a", "meta.b", "meta.c", "small.a", "small.b"]
        first = PLANNER.make_plan(inventory(names), 3, ESTIMATES)
        self.assertEqual(first, PLANNER.make_plan(inventory(names), 3, ESTIMATES))
        second = PLANNER.make_plan(inventory(list(reversed(names))), 3, ESTIMATES)
        self.assertEqual(
            [{item["name"] for item in shard["items"]} for shard in first["shards"]],
            [{item["name"] for item in shard["items"]} for shard in second["shards"]],
        )
        self.assertNotEqual(
            first["ctest_inventory_sha256"], second["ctest_inventory_sha256"]
        )

    def test_long_suites_are_balanced(self):
        plan = PLANNER.make_plan(
            inventory([f"meta.slow.{i}" for i in range(8)]), 4, ESTIMATES
        )
        self.assertEqual(
            [shard["estimated_seconds"] for shard in plan["shards"]], [140] * 4
        )

    def test_fixture_and_dependency_components_stay_together(self):
        tests = inventory(["setup", "meta.user", "cleanup", "dependent", "other"])
        for index, key, values in (
            (0, "FIXTURES_SETUP", ["server"]),
            (1, "FIXTURES_REQUIRED", ["server"]),
            (2, "FIXTURES_CLEANUP", ["server"]),
            (3, "DEPENDS", ["meta.user"]),
        ):
            tests["tests"][index]["properties"] = [{"name": key, "value": values}]
        plan = PLANNER.make_plan(tests, 4, ESTIMATES)
        assignments = {
            item["name"]: shard["index"]
            for shard in plan["shards"]
            for item in shard["items"]
        }
        self.assertEqual(
            len(
                {
                    assignments[name]
                    for name in ("setup", "meta.user", "cleanup", "dependent")
                }
            ),
            1,
        )

    def test_invalid_inventory_or_estimates_fail_closed(self):
        for names in ([], ["duplicate", "duplicate"]):
            with self.assertRaises(ValueError):
                PLANNER.make_plan(inventory(names), 2, ESTIMATES)
        with self.assertRaises(ValueError):
            PLANNER.make_plan(inventory(["a"]), 0, ESTIMATES)
        for seconds in (float("nan"), float("inf"), -1, 0):
            with self.assertRaises(ValueError):
                PLANNER.make_plan(
                    inventory(["a"]), 2, dict(ESTIMATES, default_seconds=seconds)
                )


@unittest.skipUnless(shutil.which("ctest"), "CTest is required for runner integration")
class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(
            prefix="ci-plan-test-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
        )
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.build = self.directory / "build with spaces"
        self.build.mkdir()
        self.trace = self.directory / "trace.jsonl"
        self.names = ["literal.[case]+(x)|$", "literalXcase", "meta.new", "small.new"]
        worker = self.directory / "worker.py"
        worker.write_text(
            "import json, os, sys\n"
            "with open(os.environ['CI_TEST_TRACE'], 'a') as f:\n"
            "    f.write(json.dumps(sys.argv[1]) + '\\n')\n"
            "sys.exit(2 if os.environ.get('CI_TEST_FAIL_CASE') == sys.argv[1] else 0)\n"
        )
        self.write_ctest(worker)
        self.fake_bin = self.directory / "bin"
        self.fake_bin.mkdir()
        timeout = self.fake_bin / "timeout"
        timeout.write_text(
            f"#!{sys.executable}\n"
            "import json, os, sys\n"
            "name = ('large-native-list' if '--large-list' in sys.argv else\n"
            "        'large-native-hash' if '--large-hash' in sys.argv else\n"
            "        'large-rdb' if 'LAVIK_RUN_LARGE_RDB=1' in sys.argv else 'valkey-tcl')\n"
            "with open(os.environ['CI_TEST_TRACE'], 'a') as f:\n"
            "    f.write(json.dumps(name) + '\\n')\n"
            "sys.exit(3 if os.environ.get('CI_TEST_FAIL_CASE') == name else 0)\n"
        )
        timeout.chmod(0o755)
        self.environment = dict(
            os.environ,
            PATH=str(self.fake_bin) + os.pathsep + os.environ["PATH"],
            CI_TEST_TRACE=str(self.trace),
            LAVIK_TEST_DATA_DIR=str(self.directory),
        )

    def write_ctest(self, worker):
        def quote(text):
            return '"' + str(text).replace("\\", "\\\\").replace('"', '\\"') + '"'

        (self.build / "CTestTestfile.cmake").write_text(
            "".join(
                "add_test("
                + " ".join(map(quote, (name, sys.executable, worker, name)))
                + ")\n"
                for name in self.names
            )
        )

    def run_runner(self, *arguments):
        return subprocess.run(
            [
                "bash",
                str(ROOT / "scripts/run_ci_tests.sh"),
                str(self.build),
                *arguments,
            ],
            env=self.environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

    def trace_items(self):
        return [json.loads(line) for line in self.trace.read_text().splitlines()]

    def test_real_ctest_shards_run_each_case_and_external_suite_once(self):
        for index in range(6):
            result = self.run_runner("--shard-index", str(index), "--shard-count", "6")
            self.assertEqual(result.returncode, 0, result.stdout)
            plan_path = (
                self.build / "test-results" / f"shard-{index}-of-6" / "plan.json"
            )
            plan = json.loads(plan_path.read_text())
            self.assertEqual(plan["ctest_count"], len(self.names))
        self.assertEqual(
            Counter(self.trace_items()),
            Counter(self.names + list(PLANNER.EXTERNAL_SUITES)),
        )

    def test_unsharded_invocation_retains_complete_suite(self):
        result = self.run_runner()
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(
            Counter(self.trace_items()),
            Counter(self.names + list(PLANNER.EXTERNAL_SUITES)),
        )
        self.assertTrue((self.build / "test-results/ctest.xml").is_file())

    def test_ctest_failure_still_runs_later_suites_and_fails(self):
        self.environment["CI_TEST_FAIL_CASE"] = self.names[0]
        result = self.run_runner()
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertEqual(
            Counter(self.trace_items()),
            Counter(self.names + list(PLANNER.EXTERNAL_SUITES)),
        )
        self.assertIn("FAIL: ctest", result.stdout)

    def test_external_failure_still_runs_later_suites_and_fails(self):
        self.environment["CI_TEST_FAIL_CASE"] = "large-native-hash"
        result = self.run_runner()
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertEqual(
            Counter(self.trace_items()),
            Counter(self.names + list(PLANNER.EXTERNAL_SUITES)),
        )
        self.assertIn("FAIL: large-native-hash", result.stdout)
        self.assertIn("PASS: valkey-tcl", result.stdout)

    def test_plan_only_does_not_require_data_directory_or_run_tests(self):
        self.environment["LAVIK_TEST_DATA_DIR"] = str(self.directory / "absent")
        result = self.run_runner(
            "--shard-index", "1", "--shard-count", "6", "--plan-only"
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertFalse(self.trace.exists())
        self.assertTrue((self.build / "test-results/shard-1-of-6/plan.json").is_file())

    def test_invalid_shard_or_empty_discovery_never_executes_external_suites(self):
        for arguments in (
            ("--shard-index", "6", "--shard-count", "6"),
            ("--shard-count", "0"),
            ("--shard-index", "-1"),
            ("--shard-count",),
        ):
            result = self.run_runner(*arguments)
            self.assertNotEqual(result.returncode, 0, result.stdout)
        (self.build / "CTestTestfile.cmake").write_text("")
        result = self.run_runner()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertFalse(self.trace.exists())


if __name__ == "__main__":
    unittest.main()
