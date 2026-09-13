#!/usr/bin/env python3
"""Read-only, aggregate 100K ops/s, same 256 workers and original dataset."""
import json
import subprocess
from run import ROOT, CLIENT, AERO, switch, run, stop, COUNTS


def main():
    check = subprocess.run(["ssh", CLIENT, "pgrep -a java"], capture_output=True, text=True)
    if check.returncode == 0:
        raise RuntimeError(f"Client already has Java workloads: {check.stdout}")
    results = []
    for mode in ("hmset", "aerospike"):
        switch(mode)
        for phase in ("warmup", "measured"):
            cell = run(mode, "C", 256, phase, target=100_000, measurement_interval="both")
            if cell["failed"]:
                raise RuntimeError(f"Read failures: {mode} {phase}")
            if cell["metrics"]["Intended-READ"]["Operations"] != COUNTS[phase]:
                raise RuntimeError("Scheduled-start histogram count mismatch")
            if phase == "measured":
                results.append(cell)
                (ROOT / "rate-limit-results.json").write_text(json.dumps(results, indent=2) + "\n")
    stop(AERO)
    switch("hmset")
    print("COMPLETE rate-limit 100000: two databases", flush=True)


if __name__ == "__main__":
    main()
