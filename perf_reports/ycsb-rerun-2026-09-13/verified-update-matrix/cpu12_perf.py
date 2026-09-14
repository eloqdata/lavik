#!/usr/bin/env python3
"""Collect bounded CPU profiles; these instrumented runs are never score runs.

The existing 12-worker server and 100M-key dataset are required. This tool
neither switches binaries nor clears/reloads data. perf records only this
server's threads; the remote YCSB client remains uninstrumented.
"""
import argparse
import hashlib
import json
from pathlib import Path
import signal
import subprocess

import cpu12_run as isolated
import run as bench


def summarize(prefix):
    data = prefix.with_suffix(".perf.data")
    for name, flags in (("self", ["--no-children"]), ("children", ["--children"])):
        with prefix.with_suffix(f".perf-{name}.txt").open("w") as output:
            subprocess.run(["sudo", "-n", "perf", "report", "--stdio", "-i", str(data),
                            "--percent-limit", "0.3", "--sort", "symbol", "--field-separator", "|",
                            "--no-call-graph", *flags], stdout=output, check=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("label")
    parser.add_argument("--workload", choices=("A", "C"), default="A")
    parser.add_argument("--mode", choices=("hmset", "hreplace"), default="hmset")
    parser.add_argument("--operations", type=int, default=20_000_000)
    parser.add_argument("--summarize-only", action="store_true")
    args = parser.parse_args()
    if args.summarize_only:
        summarize(bench.ROOT / args.label)
        return
    isolated.no_client()
    state = isolated.verify()
    if state["dbsize"] != isolated.COUNT or "paused=0" not in state["defrag"]:
        raise RuntimeError("Expected the loaded 100M dataset with defrag enabled")
    prefix = bench.ROOT / args.label
    data = prefix.with_suffix(".perf.data")
    receipt = prefix.with_suffix(".profile.json")
    if data.exists() or receipt.exists():
        raise RuntimeError("Use a new label: existing profile evidence is immutable")
    env = {"server": state, "binary_sha256": hashlib.sha256(Path(isolated.BINARY).read_bytes()).hexdigest(),
           "workload": args.workload, "mode": args.mode, "operations": args.operations,
           "note": "Instrumented CPU-clock profile, excluded from performance comparisons"}
    bench.run(args.mode, args.workload, 256, "warmup", label=args.label)
    # Hardware PMU events are unavailable on this VM. A software CPU-clock
    # event samples actual on-CPU stacks, not wall-clock or storage latency.
    command = ["sudo", "-n", "perf", "record", "-F", "99", "-e", "cpu-clock:u",
               "--call-graph", "dwarf,8192", "-p", str(state["pid"]), "-o", str(data)]
    env["perf_command"] = command
    with prefix.with_suffix(".perf.stderr").open("w") as output:
        perf = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT)
        try:
            bench.COUNTS["measured"] = args.operations
            env["measurement"] = bench.run(args.mode, args.workload, 256, "measured", label=args.label)
        finally:
            if perf.poll() is None:
                perf.send_signal(signal.SIGINT)
            env["perf_exit_code"] = perf.wait(timeout=30)
    receipt.write_text(json.dumps(env, indent=2) + "\n")
    # sudo may propagate the requested graceful SIGINT as 130 rather than 0.
    # perf report below must still parse the finalized data successfully.
    if env["perf_exit_code"] not in (0, 130, -signal.SIGINT) or not data.exists() or env["measurement"]["failed"]:
        raise RuntimeError("Profile or workload failed; inspect preserved evidence")
    summarize(prefix)
    print(f"PROFILE COMPLETE: {receipt}", flush=True)


if __name__ == "__main__":
    main()
