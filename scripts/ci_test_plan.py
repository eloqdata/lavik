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

"""Partition the discovered CI inventory without maintaining a test allowlist."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys


EXTERNAL_SUITES = (
    "large-native-list",
    "large-native-hash",
    "large-rdb",
    "valkey-tcl",
)


def estimated_seconds(name, estimates):
    """Estimate a CTest case, using the longest matching family for new cases."""
    if name in estimates["ctest_seconds"]:
        seconds = estimates["ctest_seconds"][name]
    else:
        prefixes = estimates["prefix_seconds"]
        prefix = max((p for p in prefixes if name.startswith(p)), key=len, default=None)
        seconds = (
            prefixes[prefix] if prefix is not None else estimates["default_seconds"]
        )
    return checked_seconds(seconds)


def checked_seconds(seconds):
    """Reject estimates that would make partition ordering undefined."""
    seconds = float(seconds)
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("duration estimates must be finite and positive")
    return seconds


def make_plan(inventory, shard_count, estimates):
    """Assign every CTest case and external suite to exactly one serial shard.

    CTest fixture setup/cleanup and explicit ordering dependencies must remain
    on the same runner. Group their connected components before balancing so
    CTest cannot implicitly pull a fixture from a different shard.
    """
    if shard_count < 1:
        raise ValueError("shard count must be positive")
    tests = inventory["tests"]
    if not tests:
        raise ValueError("CTest inventory is empty; build all test targets first")
    by_name = {test["name"]: index for index, test in enumerate(tests)}
    if len(by_name) != len(tests):
        raise ValueError("CTest inventory contains duplicate test names")
    parents = list(range(len(tests)))

    def root(index):
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def join(left, right):
        parents[root(left)] = root(right)

    fixture_members = {}
    for index, test in enumerate(tests):
        properties = {p["name"]: p["value"] for p in test.get("properties", [])}
        for dependency in properties.get("DEPENDS", []):
            if dependency not in by_name:
                raise ValueError(
                    f"{test['name']}: unknown CTest dependency {dependency}"
                )
            join(index, by_name[dependency])
        for property_name in (
            "FIXTURES_SETUP",
            "FIXTURES_REQUIRED",
            "FIXTURES_CLEANUP",
        ):
            for fixture in properties.get(property_name, []):
                if fixture in fixture_members:
                    join(index, fixture_members[fixture])
                else:
                    fixture_members[fixture] = index

    groups = {}
    for index, test in enumerate(tests):
        groups.setdefault(root(index), []).append(
            {
                "kind": "ctest",
                "name": test["name"],
                # -I accepts numeric indices in CTest 3.28, unlike the newer
                # --tests-from-file. Names containing regex syntax stay literal.
                "ctest_index": index + 1,
                "estimated_seconds": estimated_seconds(test["name"], estimates),
            }
        )
    work = list(groups.values())
    for name in EXTERNAL_SUITES:
        work.append(
            [
                {
                    "kind": "external",
                    "name": name,
                    "estimated_seconds": checked_seconds(
                        estimates["external_seconds"][name]
                    ),
                }
            ]
        )

    def group_key(group):
        return (
            -sum(item["estimated_seconds"] for item in group),
            sorted((item["kind"], item["name"]) for item in group),
        )

    shards = [
        {"index": index, "estimated_seconds": 0.0, "items": []}
        for index in range(shard_count)
    ]
    for group in sorted(work, key=group_key):
        shard = min(shards, key=lambda s: (s["estimated_seconds"], s["index"]))
        shard["items"].extend(group)
        shard["estimated_seconds"] += sum(item["estimated_seconds"] for item in group)
    for shard in shards:
        # CTest still runs its original order within each shard. External
        # suites retain the order of the unsharded runner as well.
        shard["items"].sort(
            key=lambda item: (
                0 if item["kind"] == "ctest" else 1,
                item.get("ctest_index", 0)
                if item["kind"] == "ctest"
                else EXTERNAL_SUITES.index(item["name"]),
            )
        )
        shard["estimated_seconds"] = round(shard["estimated_seconds"], 2)

    inventory_names = [test["name"] for test in tests]
    fingerprint = hashlib.sha256(json.dumps(inventory_names).encode()).hexdigest()
    return {
        "ctest_inventory_sha256": fingerprint,
        "ctest_count": len(tests),
        "external_count": len(EXTERNAL_SUITES),
        "shard_count": shard_count,
        "shards": shards,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if not 0 <= args.shard_index < args.shard_count:
        parser.error("shard index must be in [0, shard count)")

    try:
        inventory = json.loads(
            subprocess.check_output(
                ["ctest", "--test-dir", str(args.build_dir), "--show-only=json-v1"],
                text=True,
            )
        )
        estimates = json.loads(
            Path(__file__).with_name("ci_test_durations.json").read_text()
        )
        plan = make_plan(inventory, args.shard_count, estimates)
        selected = plan["shards"][args.shard_index]
        indices = [
            str(item["ctest_index"])
            for item in selected["items"]
            if item["kind"] == "ctest"
        ]
        external = [
            item["name"] for item in selected["items"] if item["kind"] == "external"
        ]
        args.output_dir.mkdir(parents=True, exist_ok=True)
        (args.output_dir / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
        # Explicit zeros disable the start/end range; only the listed indices
        # execute. An empty file tells the shell to skip CTest on an empty shard.
        (args.output_dir / "ctest-indices.txt").write_text(
            "0,0,0," + ",".join(indices) + "\n" if indices else ""
        )
        (args.output_dir / "external-suites.txt").write_text(
            "".join(name + "\n" for name in external)
        )
        print(
            f"Discovered {plan['ctest_count']} CTest cases and "
            f"{plan['external_count']} external suites; inventory {plan['ctest_inventory_sha256']}"
        )
        for shard in plan["shards"]:
            print(
                f"Shard {shard['index']}/{args.shard_count}: {len(shard['items'])} items, "
                f"estimated {shard['estimated_seconds'] / 60:.1f} minutes"
            )
        print(
            f"Selected shard {args.shard_index}; full manifest: {args.output_dir / 'plan.json'}"
        )
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        print(f"Cannot plan CI tests: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
