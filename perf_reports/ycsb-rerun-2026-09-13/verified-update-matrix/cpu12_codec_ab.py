#!/usr/bin/env python3
"""Alternate two fixed binaries on the same 12-worker, GC-enabled dataset.

Only the stopped benchmark executable is replaced. Neither database contents,
device provisioning nor affinity policy is reset. Every variant restarts and
has a separate 1M warmup JVM before each 5M measurement JVM. Profile runs are
kept outside this comparison. An existing evidence prefix is never reused.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

import cpu12_run as isolated
import run as bench


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def activate(source):
    isolated.no_client()
    if bench.matching_pids(isolated.BINARY):
        # Refuse to stop an unrelated process merely sharing the executable.
        isolated.verify()
        subprocess.run(["sudo", "-n", "systemctl", "stop", isolated.UNIT],
                       check=True, timeout=200)
    if bench.matching_pids(isolated.BINARY):
        raise RuntimeError("Refusing to overwrite a running server")
    shutil.copy2(source, isolated.BINARY)
    state = isolated.start()
    if state["dbsize"] != isolated.COUNT or "paused=0" not in state["defrag"]:
        raise RuntimeError("Expected 100M keys and enabled defrag after restart")
    if sha(Path(isolated.BINARY)) != sha(source):
        raise RuntimeError("Running binary copy differs from preserved variant")
    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--label", default="cpu12-codec")
    parser.add_argument("--mode", choices=("hmset", "hreplace"), default="hmset")
    parser.add_argument("--restore-baseline", action="store_true")
    args = parser.parse_args()
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()}
    for path in binaries.values():
        if not path.is_file() or path == Path(isolated.BINARY).resolve():
            raise RuntimeError("Variants must be preserved executable copies")
    prefix = bench.ROOT / args.label
    if args.restore_baseline:
        state = activate(binaries["baseline"])
        state["binary_sha256"] = sha(Path(isolated.BINARY))
        state["decision"] = "Restore baseline: candidate failed the read-performance guardrail"
        prefix.with_suffix(".restored.json").write_text(json.dumps(state, indent=2) + "\n")
        print(json.dumps(state, indent=2), flush=True)
        return
    environment_path = prefix.with_suffix(".environment.json")
    results_path = prefix.with_suffix(".results.json")
    if environment_path.exists() or results_path.exists():
        raise RuntimeError("Use a fresh label; do not overwrite an experiment")
    environment = {"commit": bench.execute(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip(),
                   "source_diff": bench.execute(["git", "diff", "--", "src", "include"], capture_output=True).stdout,
                   "variants": {name: {"path": str(path), "sha256": sha(path)} for name, path in binaries.items()},
                   "rounds": 3, "order": ["baseline", "candidate"], "workloads": ["C", "A"],
                   "rates": [100000, 0], "counts": bench.COUNTS, "mode": args.mode,
                   "monitoring": bench.remote(["systemctl", "is-active", "prometheus", "grafana-server"]),
                   "note": "Same 12+4 policy, no reload, no perf or compilation during scored runs"}
    environment_path.write_text(json.dumps(environment, indent=2) + "\n")
    results = []
    for round_number in (1, 2, 3):
        for variant, source in binaries.items():
            state = activate(source)
            variant_sha = sha(source)
            label = f"{args.label}-r{round_number}-{variant}"
            (bench.ROOT / f"{label}.server.json").write_text(json.dumps(state, indent=2) + "\n")
            for workload in ("C", "A"):
                for target in (100000, 0):
                    for phase in ("warmup", "measured"):
                        cell = bench.run(args.mode, workload, 256, phase,
                                         target=target, measurement_interval="both", label=label)
                        if cell["failed"]:
                            raise RuntimeError("Failed YCSB operations; preserve and stop")
                        if phase == "measured":
                            results.append({"round": round_number, "variant": variant,
                                            "binary_sha256": variant_sha, **cell})
                            results_path.write_text(json.dumps(results, indent=2) + "\n")
                            print(f"SCORE {len(results)}/24 {label} {workload} target={target} qps={cell['success_qps']:.0f}", flush=True)
    final = isolated.verify()
    (bench.ROOT / f"{args.label}.final.json").write_text(json.dumps(final, indent=2) + "\n")
    print("COMPLETE: 24 measurements; candidate remains running", flush=True)


if __name__ == "__main__":
    main()
