#!/usr/bin/env python3
"""Alternate immutable baseline/move binaries, both 100K and unlimited C.

No load, writes or disk provisioning. Each cell has an independent 1M warmup
and 5M measured JVM. Server is restarted before each variant's two rate cells;
GC, defrag, dataset, monitoring and client parameters stay unchanged.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import time

import run as bench

BINARIES = {
    "baseline": "/mnt/dev/keylane-hash-move-ab-Kxn3Ec/baseline/keylane",
    "move": "/mnt/dev/keylane-hash-move-ab-Kxn3Ec/optimized/keylane",
}
NORMAL = "/mnt/dev/keylane/build/keylane"
ROOT = bench.ROOT


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def activate(binary):
    for known in [NORMAL, *BINARIES.values()]:
        bench.stop(known)
    bench.KEYLANE = binary
    bench.switch("hmset")
    pids = bench.matching_pids(binary)
    if len(pids) != 1:
        raise RuntimeError(f"Expected one server: {binary}: {pids}")
    return pids[0]


def main():
    check = subprocess.run(["ssh", bench.CLIENT, "pgrep -a java"],
                           capture_output=True, text=True)
    if check.returncode not in (0, 1) or check.stdout.strip():
        raise RuntimeError(f"Client not idle or check failed: {check.stdout} {check.stderr}")
    meta = {
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "binaries": {name: {"path": path, "sha256": sha(path)} for name, path in BINARIES.items()},
        "tracked_diff": subprocess.check_output(["git", "diff", "--", "src/storage/engine/hash_tree.cpp"], text=True),
        "rates": [100000, 0], "rounds": 3, "workers": 256,
        "counts": bench.COUNTS,
    }
    if meta["binaries"]["baseline"]["sha256"] == meta["binaries"]["move"]["sha256"]:
        raise RuntimeError("Binaries are identical")
    (ROOT / "hash-move-ab-environment.json").write_text(json.dumps(meta, indent=2) + "\n")
    results = []
    try:
        for round_number in range(1, 4):
            for variant, binary in BINARIES.items():
                label = f"hash-move-r{round_number}-{variant}"
                print(f"ACTIVATE {label}", flush=True)
                pid = activate(binary)
                for target in (100000, 0):
                    for phase in ("warmup", "measured"):
                        cell = bench.run("hmset", "C", 256, phase, target=target,
                                         measurement_interval="both", label=label)
                        if cell["failed"] or cell["metrics"]["Intended-READ"]["Operations"] != bench.COUNTS[phase]:
                            raise RuntimeError(f"Invalid read histogram: {label} {target} {phase}")
                        if phase == "measured":
                            results.append({"round": round_number, "variant": variant,
                                            "server_pid": pid, "binary_sha256": sha(binary), **cell})
                            (ROOT / "hash-move-ab-results.json").write_text(json.dumps(results, indent=2) + "\n")
                            values = cell["metrics"]["READ"]
                            print(f"RESULT {len(results)}/12 {label} target={target} qps={cell['success_qps']:.0f} "
                                  f"p99={values['99thPercentileLatency(us)']}us "
                                  f"p999={values['99.9PercentileLatency(us)']}us "
                                  f"p9999={values['99.99PercentileLatency(us)']}us", flush=True)
    finally:
        activate(NORMAL)
    print("COMPLETE hash-move A/B: 12 measured cells, production path restored", flush=True)


if __name__ == "__main__":
    main()
