#!/usr/bin/env python3
"""Rerun two databases, A/B/C/D-Uniform, 256 threads, 100K and unlimited.

The existing 100M read domain is preserved. D alone grows the databases using
fresh, disjoint 2B-ID reservations for each phase/rate; both databases use the
same reservation starts. GC remains enabled, CPU/IRQ settings are unchanged,
and each scored cell is a fresh JVM timed by YCSB for 300 seconds.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

import connection_matrix as matrix
import cpu12_affinity as affinity
import cpu12_run as isolated
import gc_candidate_ab as server
import run as bench

COMMIT = "f666837b0038ab65564a17cb3a0bca8530f8e1be"
PREFIX = "main-f666837-abcd-20260914"
ROOT = bench.ROOT
WORKLOADS = ("C", "A", "B", "D")
RATES = (100000, 0)
# Each phase has <=2B operations, hence cannot exhaust its insert reservation.
D_STARTS = {(100000, "warmup"): 10_000_000_000,
            (100000, "measured"): 12_000_000_000,
            (0, "warmup"): 14_000_000_000,
            (0, "measured"): 16_000_000_000}


def save(name, value):
    path = ROOT / f"{PREFIX}-{name}.json"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def verify_keylane(binary, expected_count):
    state = isolated.verify()
    actual = bench.execute(["sudo", "-n", "readlink", f"/proc/{state['pid']}/exe"],
                           capture_output=True).stdout.strip()
    if actual != str(binary) or state["dbsize"] != expected_count:
        raise RuntimeError(f"Unexpected Keylane binary/count: {actual}, {state}")
    if "paused=0" not in state["defrag"] or isolated.redis("EXISTS", "_indices") != "0":
        raise RuntimeError("GC disabled or scan index present")
    state["active_expiration"] = isolated.redis("CONFIG", "GET", "active-expiration-*")
    state["sha256"] = server.sha(binary)
    return state


def verify_aerospike(expected_count=None):
    pid = int(bench.execute(["systemctl", "show", matrix.AERO_UNIT, "-p", "MainPID", "--value"],
                           capture_output=True).stdout)
    if bench.matching_pids(bench.AERO) != [pid]:
        raise RuntimeError("Unexpected Aerospike owner")
    namespace = bench.execute(["asinfo", "-h", "127.0.0.1", "-p", "3000", "-v", "namespace/ycsb"],
                              capture_output=True).stdout.strip()
    stats = dict(v.split("=", 1) for v in namespace.split(";") if "=" in v)
    count = int(stats["objects"])
    if stats["stop_writes"] != "false" or count < 100_000_000:
        raise RuntimeError("Aerospike dataset unavailable or stop-writes active")
    if expected_count is not None and count != expected_count:
        raise RuntimeError(f"Aerospike expected {expected_count} records, found {count}")
    cpus = sorted(os.sched_getaffinity(pid))
    if cpus != list(range(16)):
        raise RuntimeError(f"Aerospike CPU policy changed: {cpus}")
    return {"pid": pid, "dbsize": count, "namespace": namespace, "affinity": cpus,
            "cgroup": Path(f"/proc/{pid}/cgroup").read_text(), "sha256": server.sha(bench.AERO)}


def main(binary):
    lock = (ROOT / f"{PREFIX}.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if (ROOT / f"{PREFIX}-environment.json").exists():
        raise RuntimeError("Existing experiment; do not silently reuse D ranges or results")
    isolated.no_client()
    for unit in ("keylane-gc-cross-bench.service", "keylane-gc-single-bench.service"):
        state = bench.execute(["systemctl", "show", unit, "-p", "ActiveState", "--value"],
                              capture_output=True).stdout.strip()
        if state in ("active", "activating", "deactivating"):
            raise RuntimeError(f"Competing benchmark: {unit}")
    remote_main = bench.execute(["git", "ls-remote", "origin", "refs/heads/main"],
                                capture_output=True).stdout.split()[0]
    if remote_main != COMMIT:
        raise RuntimeError(f"Remote main changed before measurement: {remote_main}")
    # The build checkout includes a report-only branch. Its entire tracked
    # non-report tree must match the actual main commit, not just selected files.
    source_diff = bench.execute(["git", "diff", COMMIT, "--", ".", ":!perf_reports"],
                                capture_output=True).stdout
    if source_diff:
        raise RuntimeError("Compiled checkout differs from published main source")
    isolated.BINARY = str(binary)
    keylane_count = 100_000_000
    environment = {
        "start_utc": matrix.now(), "main_commit": COMMIT, "remote_main_verified": remote_main,
        "build_checkout": bench.execute(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip(),
        "non_report_source_matches_main": True,
        "binary": str(binary), "keylane_sha256": server.sha(binary),
        "aerospike_sha256": server.sha(bench.AERO), "expected_cells": 16,
        "threads": 256, "measurement_seconds": 300, "warmup_operations": 1_000_000,
        "workloads_in_execution_order": WORKLOADS, "targets_ops_sec": RATES,
        "requestdistribution": "uniform", "read_domain": [0, 99_999_999],
        "fieldcount": 10, "fieldlength": 128, "readallfields": True, "writeallfields": True,
        "D_insert_reservations": {f"{rate}-{phase}": start for (rate, phase), start in D_STARTS.items()},
        "D_semantics": "95% READ from original fixed 100M Uniform domain; 5% INSERT into fresh ranges; not standard latest-D",
        "order": "Keylane then Aerospike; C/A/B/D; 100K then unlimited; one formal run per cell",
        "cpu_caveat": "Keylane 12 workers/CPU0-11; Aerospike CPU0-15, sharing 12-15 with housekeeping; not equal CPU allocations",
        "data_caveat": "Existing aged datasets retained without reload; Aerospike has additional historical records",
        "affinity_before": affinity.snapshot(),
        "aerospike_config": Path("/mnt/dev/aerospike-md0.conf").read_text(),
        "monitoring": bench.remote(["systemctl", "is-active", "prometheus", "grafana-server"]),
        "scripts_sha256": {name: server.sha(ROOT / name) for name in
                           ("main_abcd.py", "run.py", "connection_matrix.py", "cpu12_run.py", "gc_candidate_ab.py")},
    }
    save("environment", environment)
    results = []
    save("results", results)
    failed = True
    keylane_started = False
    try:
        save("progress", {"state": "recovering", "mode": "hreplace", "completed": 0, "expected": 16,
                          "utc": matrix.now()})
        environment["keylane_start"] = server.start(binary, PREFIX)
        keylane_started = True
        save("environment", environment)
        for mode in ("hreplace", "aerospike"):
            if mode == "aerospike":
                save("progress", {"state": "recovering", "mode": mode, "completed": len(results),
                                  "expected": 16, "utc": matrix.now()})
                environment["aerospike_start"] = matrix.start_aerospike(ROOT / PREFIX)
                aero_count = verify_aerospike()["dbsize"]
                save("environment", environment)
            for workload in WORKLOADS:
                for rate in RATES:
                    for phase in ("warmup", "measured"):
                        isolated.no_client()
                        before = (verify_keylane(binary, keylane_count) if mode == "hreplace"
                                  else verify_aerospike(aero_count))
                        save("progress", {"state": phase, "mode": mode, "workload": workload,
                                          "target": rate, "completed": len(results), "expected": 16,
                                          "utc": matrix.now()})
                        cell = bench.run(mode, workload, 256, phase, target=rate,
                                         measurement_interval="both", label=PREFIX,
                                         duration_seconds=300 if phase == "measured" else 0,
                                         d_insert_start=D_STARTS[(rate, phase)] if workload == "D" else None)
                        if cell["failed"]:
                            raise RuntimeError("YCSB returned operation errors")
                        expected_ops = {"READ"} | ({"UPDATE"} if workload in ("A", "B")
                                                 else {"INSERT"} if workload == "D" else set())
                        actual_ops = set(cell["metrics"]) & {"READ", "UPDATE", "INSERT"}
                        if actual_ops != expected_ops:
                            raise RuntimeError(f"Unexpected workload operations: {actual_ops}")
                        inserted = cell["metrics"].get("INSERT", {}).get("Return=OK", 0)
                        if mode == "hreplace":
                            keylane_count += inserted
                            after = verify_keylane(binary, keylane_count)
                        else:
                            aero_count += inserted
                            after = verify_aerospike(aero_count)
                        if before["pid"] != after["pid"] or before["sha256"] != after["sha256"]:
                            raise RuntimeError("Server changed during measurement")
                        if phase == "measured":
                            results.append({"server_before": before, "server_after": after, **cell})
                            save("results", results)
                            print(f"RESULT {len(results)}/16 {mode} {workload} target={rate}: "
                                  f"{cell['success_qps']:.0f} QPS, errors={cell['failed']}", flush=True)
        failed = False
        save("measured-complete", {"count": len(results), "end_utc": matrix.now()})
    except BaseException as error:
        save("failure", {"utc": matrix.now(), "completed": len(results),
                         "type": type(error).__name__, "message": str(error)})
        raise
    finally:
        # Never stop a stranded client or an unrelated device owner implicitly.
        isolated.no_client()
        save("progress", {"state": "restoring", "completed": len(results), "expected": 16,
                          "utc": matrix.now()})
        if bench.matching_pids(bench.AERO):
            bench.execute(["sudo", "-n", "systemctl", "stop", matrix.AERO_UNIT], timeout=200)
        if keylane_started:
            # start() verifies placement; post-D counts legitimately exceed 100M.
            restored = isolated.start()
            final = verify_keylane(binary, keylane_count)
            save("final-server", final)
        save("progress", {"state": "failed" if failed else "complete", "completed": len(results),
                          "expected": 16, "utc": matrix.now()})
        if not failed:
            save("complete", {"count": len(results), "end_utc": matrix.now(),
                              "keylane_restored": True, "keylane_count": keylane_count})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    main(parser.parse_args().binary.resolve())
