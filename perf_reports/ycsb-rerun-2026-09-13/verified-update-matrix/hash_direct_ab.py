#!/usr/bin/env python3
"""Independent incremental comparisons: compact reads (C), then reserve (A).

Never load, insert, provision disks or change monitoring/defrag. A updates
existing records with the same 10 x 128-byte shape. Archive each binary before
running this script; do not build or run correctness tests during measurement.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

import run as bench

ROOT = bench.ROOT
NORMAL = "/mnt/dev/keylane/build/keylane"
ARCHIVE = Path("/mnt/dev/keylane-hash-direct-ab-NpOcpB")
ALL_BINARIES = {
    "move": "/mnt/dev/keylane-hash-move-ab-Kxn3Ec/optimized/keylane",
    "direct": str(ARCHIVE / "direct" / "keylane"),
    "reserve": str(ARCHIVE / "reserve" / "keylane"),
}
STAGES = {"read": ("C", "move", "direct"),
          "reserve": ("A", "direct", "reserve")}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def activate(binary):
    for known in [NORMAL, *ALL_BINARIES.values()]:
        bench.stop(known)
    bench.KEYLANE = binary
    bench.switch("hmset")
    pids = bench.matching_pids(binary)
    if len(pids) != 1:
        raise RuntimeError(f"Expected exactly one server: {binary}: {pids}")
    return pids[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=STAGES)
    stage = parser.parse_args().stage
    workload, baseline, candidate = STAGES[stage]
    prefix = f"hash-direct-{stage}"
    env_path = ROOT / f"{prefix}-environment.json"
    if env_path.exists():
        raise RuntimeError("This stage already has evidence; use a new prefix for a new experiment")
    check = subprocess.run(["ssh", bench.CLIENT, "pgrep -a java"],
                           capture_output=True, text=True)
    if check.returncode not in (0, 1) or check.stdout.strip():
        raise RuntimeError(f"Client check failed or Java busy: {check.stdout} {check.stderr}")
    binaries = {v: {"path": ALL_BINARIES[v], "sha256": sha(ALL_BINARIES[v])}
                for v in (baseline, candidate)}
    if binaries[baseline]["sha256"] == binaries[candidate]["sha256"]:
        raise RuntimeError("Compared binaries are identical")
    meta = {
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "stage": stage, "baseline": baseline, "candidate": candidate,
        "workload": workload, "workers": 256, "rounds": 3,
        "rates": [100000, 0], "counts": bench.COUNTS,
        "binaries": binaries,
        "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "tracked_diff": subprocess.check_output(["git", "diff"], text=True),
        "monitoring": bench.remote(["systemctl", "is-active", "prometheus", "grafana-server"]),
        "dataset": "Existing first 100M keys, Uniform, 10 x 128B, no reload; A updates all fields",
        "server_config": "run.py switch(): io_uring /dev/md0p1, 16 pinned workers, default GC/defrag enabled",
    }
    env_path.write_text(json.dumps(meta, indent=2) + "\n")
    results = []
    try:
        for round_number in (1, 2, 3):
            for variant in (baseline, candidate):
                label = f"{prefix}-r{round_number}-{variant}"
                binary = binaries[variant]["path"]
                print(f"ACTIVATE {label}", flush=True)
                pid = activate(binary)
                state = bench.execute(["redis-cli", "-h", bench.SERVER, "-p", "16379",
                                       "DEFRAG", "STATUS"], capture_output=True).stdout
                (ROOT / f"{label}-defrag.txt").write_text(state)
                for target in (100000, 0):
                    for phase in ("warmup", "measured"):
                        cell = bench.run("hmset", workload, 256, phase, target=target,
                                         measurement_interval="both", label=label)
                        ops = ("READ",) if workload == "C" else ("READ", "UPDATE")
                        if cell["failed"] or sum(cell["metrics"][op]["Operations"] for op in ops) != bench.COUNTS[phase]:
                            raise RuntimeError(f"Bad result: {label} {target} {phase}")
                        for op in ops:
                            if cell["metrics"][f"Intended-{op}"]["Operations"] != cell["metrics"][op]["Operations"]:
                                raise RuntimeError(f"Mismatched intended histogram: {label}")
                        if phase == "measured":
                            results.append({"stage": stage, "round": round_number,
                                            "variant": variant, "server_pid": pid,
                                            "binary_sha256": binaries[variant]["sha256"], **cell})
                            (ROOT / f"{prefix}-results.json").write_text(json.dumps(results, indent=2) + "\n")
                            parts = []
                            for op in ops:
                                v = cell["metrics"][op]
                                parts.append(f"{op}: qps={v['Operations']*1000/cell['runtime_ms']:.0f} "
                                             f"p99/p999/p9999={v['99thPercentileLatency(us)']}/"
                                             f"{v['99.9PercentileLatency(us)']}/"
                                             f"{v['99.99PercentileLatency(us)']}us")
                            print(f"RESULT {len(results)}/12 {label} target={target} " + "; ".join(parts), flush=True)
    finally:
        activate(NORMAL)
    print(f"COMPLETE {prefix}: 12 measured cells, normal server restored", flush=True)


if __name__ == "__main__":
    main()
