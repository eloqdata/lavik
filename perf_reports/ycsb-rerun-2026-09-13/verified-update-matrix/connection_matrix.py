#!/usr/bin/env python3
"""Measure current-main HREPLACE and Aerospike at four client connection counts.

Uses the existing data and CPU/IRQ policy: no reload, repartitioning, compilation,
or perf during scoring. Each cell has a separate 1M warmup JVM and a measured
JVM that runs for 300 seconds, terminated by YCSB's own timer.
Only the benchmark services are switched; Keylane is restored on exit.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

import cpu12_affinity as affinity
import cpu12_run as isolated
import run as bench


CONNECTIONS = (64, 128, 256, 512)
WORKLOADS = ("C", "A")
TARGETS = (100000, 0)
AERO_UNIT = "aerospike-ycsb-compare.service"


def targets(connections):
    """Only the 256-thread reference point is rate-limited; sweep saturation."""
    return TARGETS if connections == 256 else (0,)


def identity(cell):
    return tuple(cell[k] for k in ("mode", "workers", "workload", "target_ops_sec"))


def now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def keylane_state(expected_sha):
    """Reject unexpected dataset, binary, CPU placement, or paused defrag."""
    state = isolated.verify()
    assert state["dbsize"] == isolated.COUNT
    assert "paused=0" in state["defrag"]
    assert isolated.redis("EXISTS", "_indices") == "0"
    digest = hashlib.sha256(Path(isolated.BINARY).read_bytes()).hexdigest()
    assert digest == expected_sha
    state["binary_sha256"] = digest
    return state


def start_aerospike(prefix):
    """Start only the dedicated benchmark instance, preserving its partition."""
    if bench.matching_pids(bench.AERO):
        raise RuntimeError("Unexpected Aerospike process is already running")
    subprocess.run(["sudo", "-n", "systemctl", "stop", isolated.UNIT], check=True, timeout=200)
    command = ["sudo", "-n", "systemd-run", "--unit", AERO_UNIT, "--collect",
               "--slice=keylane-bench.slice", "-p", "AllowedCPUs=0-15",
               "-p", "CPUAffinity=0-15", "-p", "LimitNOFILE=20000",
               "-p", "TimeoutStopSec=180", "-p", "KillSignal=SIGTERM",
               "-p", f"StandardOutput=append:{prefix}.aerospike.stdout", "-p", "StandardError=inherit",
               bench.AERO, "--config-file", "/mnt/dev/aerospike-md0.conf", "--foreground"]
    subprocess.run(command, check=True, timeout=30)
    print("RECOVER Aerospike on its preserved partition", flush=True)
    deadline = time.monotonic() + 1200
    while not bench.ready("aerospike"):
        if time.monotonic() >= deadline:
            raise RuntimeError("Aerospike recovery timeout")
        time.sleep(2)
    pid = int(bench.execute(["systemctl", "show", AERO_UNIT, "-p", "MainPID", "--value"],
                           capture_output=True).stdout)
    assert bench.matching_pids(bench.AERO) == [pid]
    cgroup = Path(f"/proc/{pid}/cgroup").read_text()
    cpus = sorted(os.sched_getaffinity(pid))
    assert "keylane-bench.slice" in cgroup and cpus == list(range(16))
    namespace = bench.execute(["asinfo", "-h", "127.0.0.1", "-p", "3000", "-v", "namespace/ycsb"],
                              capture_output=True).stdout
    stats = dict(item.split("=", 1) for item in namespace.strip().split(";") if "=" in item)
    assert int(stats["objects"]) >= isolated.COUNT and stats["stop_writes"] == "false"
    return {"pid": pid, "cgroup": cgroup, "affinity": cpus, "namespace_before": namespace,
            "command": command, "ready_utc": now()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--resume", action="store_true",
                        help="reuse validated completed cells after a recorded interruption")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9-]+", args.label):
        raise ValueError("Label must be a simple experiment name")
    prefix = bench.ROOT / args.label
    env_path = prefix.with_suffix(".environment.json")
    results_path = prefix.with_suffix(".results.json")
    previous = json.loads(env_path.read_text()) if args.resume else None
    results = json.loads(results_path.read_text()) if args.resume else []
    expected = {(m, c, w, t) for m in ("hreplace", "aerospike") for c in CONNECTIONS
                for w in WORKLOADS for t in targets(c)}
    if not args.resume and list(bench.ROOT.glob(args.label + "*")):
        raise RuntimeError("Use a new label; do not overwrite experiment evidence")
    isolated.no_client()
    if bench.matching_pids(bench.AERO):
        raise RuntimeError("Aerospike must be stopped before this experiment")
    production_diff = bench.execute(["git", "diff", "a1b90e6", "--", "app", "src", "include",
                                     "CMakeLists.txt", "celer"], capture_output=True).stdout
    if production_diff:
        raise RuntimeError("Production source no longer matches the recorded baseline binary")
    expected_sha = "8ded8812356d872eabb05bcd0ff828dcf8185771c50ef1d3b566d5d48403ab1d"
    env = {"label": args.label, "start_utc": now(), "rounds": 1, "connections": CONNECTIONS,
           "workloads": WORKLOADS, "targets_ops_sec": TARGETS,
           "warmup_operations": bench.COUNTS["warmup"], "measurement_seconds": 300,
           "measured_operation_limit": 2_000_000_000,
           "commit": bench.execute(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip(),
           "production_matches_a1b90e6": True, "keylane_binary_sha256": expected_sha,
           "aerospike_binary_sha256": hashlib.sha256(Path(bench.AERO).read_bytes()).hexdigest(),
           "keylane_before": keylane_state(expected_sha), "affinity_before": affinity.snapshot(),
           "aerospike_config": Path("/mnt/dev/aerospike-md0.conf").read_text(),
           "monitoring": bench.remote(["systemctl", "is-active", "prometheus", "grafana-server"]),
           "order": "Keylane then Aerospike; connections ascending; C then A; 100K then unlimited",
           "note": "One scored run per cell as requested; no medians or claimed causal improvement"}
    if previous is not None:
        # Resume does not silently reuse a new build, changed duration, or failed
        # operation. The user's narrower rate sweep excludes cells by identity,
        # never by their throughput or latency. Original receipts stay intact.
        for key in ("label", "commit", "keylane_binary_sha256", "aerospike_binary_sha256",
                    "measurement_seconds", "warmup_operations"):
            assert previous[key] == env[key], key
        assert "completed_utc" not in previous and "failure" in previous
        excluded = [identity(cell) for cell in results if identity(cell) not in expected]
        results = [cell for cell in results if identity(cell) in expected]
        assert len({identity(cell) for cell in results}) == len(results)
        for cell in results:
            mode, connections, workload, target = identity(cell)
            for phase in ("warmup", "measured"):
                suffix = f"-target{target}" if target else ""
                name = f"{args.label}-{mode}-{workload.lower()}-c{connections}{suffix}-{phase}"
                assert (bench.ROOT / f"{name}.json").is_file(), name
                checked = bench.run(mode, workload, connections, phase, target=target,
                                    measurement_interval="both", label=args.label,
                                    duration_seconds=300 if phase == "measured" else 0)
                assert checked["failed"] == 0
                if phase == "measured":
                    assert all(cell[k] == v for k, v in checked.items())
        previous.setdefault("interruptions", []).append({
            **previous.pop("failure"), "resumed_utc": now(),
            "reason": "User narrowed 100K to 256 threads; unlimited retains all four counts",
            "excluded_completed_identities": excluded, "retained_cells": len(results),
            "keylane_at_resume": env["keylane_before"]})
        env = previous
    env["rate_limited_connections"] = [256]
    env["expected_scored_cells"] = len(expected)
    env["targets_by_connections"] = {str(c): targets(c) for c in CONNECTIONS}
    env["order"] = "Keylane then Aerospike; connections ascending; C then A; 100K only at 256, then unlimited"
    save(env_path, env)
    save(results_path, results)
    try:
        for mode in ("hreplace", "aerospike"):
            done = {identity(cell) for cell in results}
            if all(item in done for item in expected if item[0] == mode):
                continue
            if mode == "aerospike":
                if "aerospike_server" in env:
                    env.setdefault("aerospike_server_history", []).append(env["aerospike_server"])
                env["aerospike_server"] = start_aerospike(prefix)
                save(prefix.with_suffix(".environment.json"), env)
            for connections in CONNECTIONS:
                for workload in WORKLOADS:
                    for target in targets(connections):
                        if (mode, connections, workload, target) in done:
                            continue
                        isolated.no_client()
                        if mode == "hreplace":
                            keylane_state(expected_sha)
                        for phase in ("warmup", "measured"):
                            cell = bench.run(mode, workload, connections, phase, target=target,
                                             measurement_interval="both", label=args.label,
                                             duration_seconds=300 if phase == "measured" else 0)
                            if cell["failed"]:
                                raise RuntimeError("YCSB operation failure")
                            if phase == "measured":
                                results.append({"round": 1, "binary_sha256": env[
                                    "keylane_binary_sha256" if mode == "hreplace" else "aerospike_binary_sha256"],
                                    **cell})
                                save(prefix.with_suffix(".results.json"), results)
                                print(f"SCORE {len(results)}/{len(expected)} {mode} c{connections} {workload} "
                                      f"target={target} qps={cell['success_qps']:.0f}", flush=True)
            if mode == "hreplace":
                env["keylane_after"] = keylane_state(expected_sha)
                save(prefix.with_suffix(".environment.json"), env)
        env["completed_utc"] = now()
    except BaseException as error:
        env["failure"] = {"utc": now(), "message": str(error), "completed_cells": len(results)}
        raise
    finally:
        if bench.matching_pids(bench.AERO):
            subprocess.run(["sudo", "-n", "systemctl", "stop", AERO_UNIT], check=True, timeout=200)
        isolated.start()
        env["keylane_restored"] = keylane_state(expected_sha)
        env["affinity_after"] = affinity.snapshot()
        save(prefix.with_suffix(".environment.json"), env)
    assert {identity(cell) for cell in results} == expected
    print(f"COMPLETE {len(expected)} scored cells + {len(expected)} warmups; "
          "Keylane restored with defrag enabled", flush=True)


if __name__ == "__main__":
    main()
