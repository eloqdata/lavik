#!/usr/bin/env python3
"""Refresh Aerospike A/C on its preserved partition and restore Keylane.

Aerospike retains all 16 available CPUs, despite the current housekeeping
cpuset. Both databases are measured serially. No data, RAID, namespace config,
or IRQ policy is changed. The independent service is stopped even on failure.
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default="cpu12-hreplace-aero")
    args = parser.parse_args()
    prefix = bench.ROOT / args.label
    environment_path = prefix.with_suffix(".environment.json")
    results_path = prefix.with_suffix(".results.json")
    if environment_path.exists() or results_path.exists():
        raise RuntimeError("Use a fresh label; preserve completed evidence")
    isolated.no_client()
    before = isolated.verify()
    if bench.matching_pids(bench.AERO):
        raise RuntimeError("An Aerospike server is already running")
    subprocess.run(["sudo", "-n", "systemctl", "stop", isolated.UNIT], check=True, timeout=200)
    unit = "aerospike-ycsb-compare.service"
    command = ["sudo", "-n", "systemd-run", "--unit", unit, "--collect",
               "--slice=keylane-bench.slice", "-p", "AllowedCPUs=0-15",
               "-p", "CPUAffinity=0-15", "-p", "LimitNOFILE=20000",
               "-p", "TimeoutStopSec=180", "-p", "KillSignal=SIGTERM",
               "-p", f"StandardOutput=append:{prefix}.stdout", "-p", "StandardError=inherit",
               bench.AERO, "--config-file", "/mnt/dev/aerospike-md0.conf", "--foreground"]
    environment = {"command": command, "keylane_before": before,
                   "start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   "aero_config": Path("/mnt/dev/aerospike-md0.conf").read_text(),
                   "binary_sha256": hashlib.sha256(Path(bench.AERO).read_bytes()).hexdigest(),
                   "rounds": 3, "counts": bench.COUNTS, "rates": [100000, 0],
                   "note": "Aero all 16 CPUs available; unchanged 12+4 housekeeping/NIC policy; no reload"}
    environment_path.write_text(json.dumps(environment, indent=2) + "\n")
    try:
        subprocess.run(command, check=True, timeout=30)
        print("Aerospike recovering its preserved partition", flush=True)
        deadline = time.monotonic() + 1200
        while not bench.ready("aerospike"):
            if time.monotonic() >= deadline:
                raise RuntimeError("Aerospike recovery timeout")
            time.sleep(2)
        pid = int(bench.execute(["systemctl", "show", unit, "-p", "MainPID", "--value"], capture_output=True).stdout)
        if bench.matching_pids(bench.AERO) != [pid]:
            raise RuntimeError("Wrong Aerospike executable/PID")
        environment["cgroup"] = Path(f"/proc/{pid}/cgroup").read_text()
        environment["affinity"] = sorted(os.sched_getaffinity(pid))
        if "keylane-bench.slice" not in environment["cgroup"] or environment["affinity"] != list(range(16)):
            raise RuntimeError("Aerospike inherited incorrect CPU restrictions")
        namespace = bench.execute(["asinfo", "-h", "127.0.0.1", "-p", "3000", "-v", "namespace/ycsb"], capture_output=True).stdout
        environment["namespace_before"] = namespace
        stats = dict(item.split("=", 1) for item in namespace.strip().split(";") if "=" in item)
        if int(stats["objects"]) < isolated.COUNT or stats["stop_writes"] != "false":
            raise RuntimeError("Aerospike dataset incomplete or stop-writes active")
        environment_path.write_text(json.dumps(environment, indent=2) + "\n")
        results = []
        for round_number in (1, 2, 3):
            for workload in ("C", "A"):
                for target in (100000, 0):
                    for phase in ("warmup", "measured"):
                        cell = bench.run("aerospike", workload, 256, phase, target=target,
                                         measurement_interval="both", label=f"{args.label}-r{round_number}")
                        if cell["failed"]:
                            raise RuntimeError("Failed Aerospike YCSB operations")
                        if phase == "measured":
                            results.append({"round": round_number, **cell})
                            results_path.write_text(json.dumps(results, indent=2) + "\n")
                            print(f"AERO SCORE {len(results)}/12 qps={cell['success_qps']:.0f}", flush=True)
    finally:
        # Do not return the user's benchmark service in a different CPU mode.
        if bench.matching_pids(bench.AERO):
            subprocess.run(["sudo", "-n", "systemctl", "stop", unit], check=True, timeout=200)
        restored = isolated.start()
        prefix.with_suffix(".restored.json").write_text(json.dumps(restored, indent=2) + "\n")
    print("COMPLETE: 12 Aero cells; Keylane restored", flush=True)


if __name__ == "__main__":
    main()
