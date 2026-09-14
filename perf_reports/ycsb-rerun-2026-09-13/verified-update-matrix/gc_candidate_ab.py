#!/usr/bin/env python3
"""Compare GC predicate lookup with the saved binary, without clearing data.

Three alternating baseline/candidate pairs run A at 256 connections for 300 s
at both 100K and unlimited rates. A final pair runs C as a read-path guard.
Only the recorded benchmark service is restarted; no load, format, or disk
provisioning is performed. All measurements are unprofiled client YCSB data.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

import cpu12_run as isolated
import run as bench

ROOT = bench.ROOT
PREFIX = "gc-candidate-20260914"
UNIT = isolated.UNIT
BASE_SHA = "8ded8812356d872eabb05bcd0ff828dcf8185771c50ef1d3b566d5d48403ab1d"


def save(name, value):
    (ROOT / f"{PREFIX}-{name}.json").write_text(json.dumps(value, indent=2) + "\n")


def sha(path):
    return hashlib.file_digest(Path(path).open("rb"), "sha256").hexdigest()


def verify(binary):
    pid = int(bench.execute(["systemctl", "show", UNIT, "-p", "MainPID", "--value"],
                            capture_output=True).stdout)
    actual = bench.execute(["sudo", "-n", "readlink", f"/proc/{pid}/exe"],
                           capture_output=True).stdout.strip()
    if actual != str(binary):
        raise RuntimeError(f"Wrong executable: {actual}")
    affinities = {p.name: sorted(os.sched_getaffinity(int(p.name)))
                  for p in Path(f"/proc/{pid}/task").iterdir()}
    if any(not cpus or not set(cpus) <= set(range(12)) for cpus in affinities.values()):
        raise RuntimeError(f"Affinity escaped CPUs 0-11: {affinities}")
    count = int(isolated.redis("DBSIZE"))
    defrag = isolated.redis("DEFRAG", "STATUS")
    if count != 100_000_000 or "paused=0" not in defrag:
        raise RuntimeError(f"Unexpected dataset or defrag state: {count}, {defrag}")
    return {"pid": pid, "binary": str(binary), "sha256": sha(binary),
            "dbsize": count, "defrag": defrag, "affinities": affinities,
            "cgroup": Path(f"/proc/{pid}/cgroup").read_text().strip()}


def start(binary, label):
    isolated.no_client()
    if bench.matching_pids(bench.AERO):
        raise RuntimeError("Aerospike is running; refusing overlapping benchmark")
    state = bench.execute(["systemctl", "show", UNIT, "-p", "ActiveState", "--value"],
                          capture_output=True).stdout.strip()
    if state in ("active", "activating", "deactivating"):
        bench.execute(["sudo", "-n", "systemctl", "stop", UNIT], capture_output=True)
    # A live owner outside this known service must never be silently killed.
    owners = subprocess.run(["sudo", "-n", "fuser", "/dev/md0p1"],
                            text=True, capture_output=True)
    if owners.returncode != 1:
        raise RuntimeError(f"Partition owner/check failure: {owners.stdout} {owners.stderr}")
    args = ["sudo", "-n", "systemd-run", "--unit", UNIT, "--collect",
            "--slice=keylane-bench.slice", "-p", "AllowedCPUs=0-11", "-p", "CPUAffinity=0-11",
            "-p", "LimitNOFILE=20000", "-p", "TimeoutStopSec=180", "-p", "KillSignal=SIGTERM",
            "-p", f"StandardOutput=append:{ROOT / (label + '-server.stdout')}",
            "-p", "StandardError=inherit", "/usr/bin/taskset", "-c", "0-11", str(binary),
            "--bind", bench.SERVER, "--port", "16379", "--metrics-port", "19100",
            "--threads", "12", "--pin-workers", "--shutdown-checkpoint",
            "--log-dir", "/mnt/dev/keylane-md0-keylane", "--data-file", "/dev/md0p1"]
    print(f"START SERVER {label}", flush=True)
    bench.execute(args, capture_output=True)
    deadline = time.monotonic() + 1200
    while not bench.ready("hreplace"):
        if time.monotonic() > deadline:
            raise RuntimeError("Server recovery timeout")
        time.sleep(2)
    receipt = {"command": args, "utc": time.strftime("%FT%TZ", time.gmtime()),
               "server": verify(binary)}
    (ROOT / (label + "-server.json")).write_text(json.dumps(receipt, indent=2) + "\n")
    print(f"READY {label} count={receipt['server']['dbsize']}", flush=True)
    return receipt


def main(artifacts):
    binaries = {name: artifacts / f"{name}-keylane" for name in ("baseline", "candidate")}
    if sha(binaries["baseline"]) != BASE_SHA:
        raise RuntimeError("Saved baseline SHA changed")
    marker = ROOT / f"{PREFIX}-environment.json"
    if marker.exists():
        raise RuntimeError("Existing experiment; preserve its receipts")
    environment = {"start_utc": time.strftime("%FT%TZ", time.gmtime()),
                   "source_head": bench.execute(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip(),
                   "binaries": {name: {"path": str(path), "sha256": sha(path)} for name, path in binaries.items()},
                   "seconds": 300, "connections": 256, "rates": [100000, 0], "rounds": 3,
                   "scope": "GC lookup only; main Find unchanged; same 100M dataset, no reload",
                   "placement": "12 workers on CPUs 0-11, housekeeping 12-15; GC enabled",
                   "monitoring": bench.remote(["systemctl", "is-active", "prometheus", "grafana-server"])}
    save("environment", environment)
    results = []
    # Freeze the exact source delta independently of any later report edits.
    patch = bench.execute(["git", "diff", "--", "include/keylane/storage/scan_hash_map.h",
                           "src/storage/engine/defrag.cpp", "tests/scan_hash_map_test.cpp"],
                          capture_output=True).stdout
    (ROOT / f"{PREFIX}-source.patch").write_text(patch)
    try:
        for round_number, workload in [(1, "A"), (2, "A"), (3, "A"), (1, "C")]:
            for version, binary in binaries.items():
                label = f"{PREFIX}-{workload.lower()}-r{round_number}-{version}"
                server = start(binary, label)
                for target in (100000, 0):
                    for phase in ("warmup", "measured"):
                        cell = bench.run("hreplace", workload, 256, phase, target=target,
                                         measurement_interval="both", label=label,
                                         duration_seconds=300 if phase == "measured" else 0)
                        if cell["failed"]:
                            raise RuntimeError("YCSB operation failure")
                        if phase == "measured":
                            checked = verify(binary)
                            if checked["sha256"] != environment["binaries"][version]["sha256"]:
                                raise RuntimeError("Binary changed during measurement")
                            results.append({"version": version, "round": round_number,
                                            "server": server["server"], "server_after": checked, **cell})
                            save("results", results)
                            print(f"RESULT {len(results)}/16 {version} {workload} "
                                  f"r{round_number} target={target}: {cell['success_qps']:.0f} ops/s", flush=True)
        save("complete", {"count": len(results), "end_utc": time.strftime("%FT%TZ", time.gmtime())})
    finally:
        # Keep the known baseline serving after an experiment, including failures.
        # no_client() refuses restoration while a stranded client is still live.
        restored = start(binaries["baseline"], f"{PREFIX}-restored-baseline")
        save("final-server", restored)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", type=Path)
    main(parser.parse_args().artifacts.resolve())
