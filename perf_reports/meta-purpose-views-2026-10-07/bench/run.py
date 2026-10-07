#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Run the prebuilt benchmark matrix serially; keep all artifacts in root."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def output(*command):
    return subprocess.check_output(command, text=True).strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--process-only", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    if not root.is_relative_to("/mnt/local_nvme"):
        raise ValueError("measurement root must be under /mnt/local_nvme")
    result = root / "measurements"
    result.mkdir(exist_ok=args.process_only)
    source = root / "src"
    bench = Path(__file__).resolve().parent
    binaries = {
        "before_meta": root / "release-before/lavik-meta",
        "after_meta": root / "release-after/lavik-meta",
        "data": root / "build/lavik",
        "ctl": root / "build/lavik-ctl",
        "before_component": root / "bench-before/component",
        "after_component": root / "bench-after/component",
    }
    metadata = {
        "started_utc": output("date", "-u", "+%FT%TZ"),
        "uname": output("uname", "-a"),
        "lscpu": json.loads(output("lscpu", "-J")),
        "affinity": sorted(os.sched_getaffinity(0)),
        "compiler": output("/usr/bin/c++", "--version"),
        "libc": output("getconf", "GNU_LIBC_VERSION"),
        "go_builds": {
            label: output("go", "version", "-m", str(binaries[f"{label}_meta"]))
            for label in ("before", "after")
        },
        "state_machine_compile_commands": {
            label: next(
                entry["command"]
                for entry in json.loads(
                    (root / f"release-{label}/compile_commands.json").read_text()
                )
                if entry["file"].endswith("/src/meta/state_machine.cpp")
            )
            for label in ("before", "after")
        },
        "before_revision": output(
            "git", "-C", str(root / "baseline"), "rev-parse", "HEAD"
        ),
        "after_base_revision": output("git", "-C", str(source), "rev-parse", "HEAD"),
        "after_diff": output("git", "-C", str(source), "diff", "--", "include", "src"),
        "rounds": args.rounds,
        "component_warmup_ms": 25,
        "order": ["AB", "BA", "AB"][: args.rounds],
        "binaries": {
            name: {
                "path": str(path),
                "sha256": hashlib.file_digest(path.open("rb"), "sha256").hexdigest(),
            }
            for name, path in binaries.items()
        },
        "bench_sha256": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(bench.iterdir())
            if path.is_file()
        },
        "environment": {
            key: value
            for key, value in os.environ.items()
            if key
            in (
                "TMPDIR",
                "LAVIK_TEST_DATA_DIR",
                "XDG_CACHE_HOME",
                "PYTHONPYCACHEPREFIX",
            )
        },
        "ambient_before": output("ps", "-eo", "pid,pcpu,comm", "--sort=-pcpu"),
    }
    metadata_path = result / (
        "process-metadata.json" if args.process_only else "metadata.json"
    )
    if args.process_only:
        original = json.loads((result / "metadata.json").read_text())
        if original["binaries"] != metadata["binaries"]:
            raise ValueError("process-only run requires the original binaries")
        for name in ("combined", "strict"):
            if (result / name).exists():
                raise ValueError(f"archive the previous {name} attempt first")
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
    for round_id in range(0 if args.process_only else args.rounds):
        for label in ["before", "after"] if round_id % 2 == 0 else ["after", "before"]:
            print(f"component {round_id} {label}", flush=True)
            with (
                (result / f"component-{round_id}-{label}.csv").open("w") as stdout,
                (result / f"component-{round_id}-{label}.log").open("w") as stderr,
            ):
                subprocess.run(
                    [str(binaries[f"{label}_component"]), "100", "25"],
                    stdout=stdout,
                    stderr=stderr,
                    check=True,
                )
            time.sleep(2)
    for mode in ("combined", "strict"):
        print(f"process {mode}", flush=True)
        command = [
            sys.executable,
            str(bench / "process.py"),
            "--source",
            str(source),
            "--before",
            str(binaries["before_meta"]),
            "--after",
            str(binaries["after_meta"]),
            "--data",
            str(binaries["data"]),
            "--ctl",
            str(binaries["ctl"]),
            "--output",
            str(result / mode),
            "--mode",
            mode,
            "--rounds",
            str(args.rounds),
        ]
        with (result / f"{mode}.log").open("w") as log:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
    metadata["finished_utc"] = output("date", "-u", "+%FT%TZ")
    metadata["ambient_after"] = output("ps", "-eo", "pid,pcpu,comm", "--sort=-pcpu")
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
