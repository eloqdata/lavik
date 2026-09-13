#!/usr/bin/env python3
"""Re-load DB0 and measure the merged main on isolated server CPUs 0-11.

clear-load is destructive ONLY to DB0 on the recorded benchmark instance;
it requires the observed old count and refuses reuse of a prior clear receipt.
The raw RAID layout and Aerospike partition are never changed. run never loads
or clears data. All YCSB activity stays on .5 with its existing CPU placement.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import time

import run as bench

ROOT = bench.ROOT
BINARY = "/mnt/dev/keylane/build/keylane"
UNIT = "keylane-ycsb-cpu12.service"
COUNT = 100_000_000


def host_snapshot(name):
    state = {path: Path(path).read_text() for path in
             ("/proc/interrupts", "/proc/net/softnet_stat", "/proc/stat")}
    state["utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    (ROOT / name).write_text(json.dumps(state, indent=2) + "\n")


def redis(*args):
    return bench.execute(["redis-cli", "--raw", "-h", bench.SERVER, "-p", "16379",
                          "-n", "0", *args], capture_output=True).stdout.strip()


def no_client():
    check = subprocess.run(["ssh", bench.CLIENT, "pgrep -a java"], capture_output=True, text=True)
    if check.returncode not in (0, 1) or check.stdout.strip():
        raise RuntimeError(f"Client Java busy/check failed: {check.stdout} {check.stderr}")


def start():
    if bench.matching_pids(BINARY):
        pid = int(bench.execute(["systemctl", "show", UNIT, "-p", "MainPID", "--value"], capture_output=True).stdout)
        if bench.matching_pids(BINARY) != [pid]:
            raise RuntimeError("Another non-isolated Keylane is running")
        return verify()
    bench.stop(bench.AERO)
    args = ["sudo", "-n", "systemd-run", "--unit", UNIT, "--collect",
            "--slice=keylane-bench.slice", "-p", "AllowedCPUs=0-11", "-p", "CPUAffinity=0-11",
            "-p", "LimitNOFILE=20000", "-p", "TimeoutStopSec=180", "-p", "KillSignal=SIGTERM",
            "-p", f"StandardOutput=append:{ROOT / 'cpu12-server.stdout'}", "-p", "StandardError=inherit",
            "/usr/bin/taskset", "-c", "0-11", BINARY, "--bind", bench.SERVER, "--port", "16379",
            "--metrics-port", "19100", "--threads", "12", "--pin-workers", "--shutdown-checkpoint",
            "--log-dir", "/mnt/dev/keylane-md0-keylane", "--data-file", "/dev/md0p1"]
    bench.execute(args, capture_output=True)
    (ROOT / "cpu12-server-command.json").write_text(json.dumps(args, indent=2) + "\n")
    deadline = time.monotonic() + 1200
    while not bench.ready("hmset"):
        if time.monotonic() > deadline:
            raise RuntimeError("12-worker readiness timeout")
        time.sleep(2)
    return verify()


def verify():
    import os
    pid = int(bench.execute(["systemctl", "show", UNIT, "-p", "MainPID", "--value"], capture_output=True).stdout)
    if bench.matching_pids(BINARY) != [pid]:
        raise RuntimeError("Wrong server PID/executable")
    cg = (Path(f"/proc/{pid}") / "cgroup").read_text().strip()
    if "keylane-bench.slice" not in cg:
        raise RuntimeError(f"Keylane must not inherit housekeeping cpuset: {cg}")
    affinities = {p.name: sorted(os.sched_getaffinity(int(p.name)))
                  for p in Path(f"/proc/{pid}/task").iterdir()}
    if any(not cpus or not set(cpus) <= set(range(12)) for cpus in affinities.values()):
        raise RuntimeError(f"Worker escaped CPUs 0-11: {affinities}")
    return {"pid": pid, "cgroup": cg, "affinities": affinities,
            "defrag": redis("DEFRAG", "STATUS"), "dbsize": int(redis("DBSIZE")),
            "keyspace": redis("INFO", "keyspace")}


def clear_load(expected):
    no_client()
    marker = ROOT / "cpu12-clear-db0.json"
    if marker.exists():
        raise RuntimeError("DB0 was already cleared for this experiment; refusing another deletion")
    state = verify()
    if state["dbsize"] != expected or expected != 100_605_038:
        raise RuntimeError(f"Old DB0 count changed; inspect before clearing: {state['dbsize']}")
    receipt = {"before": state, "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "method": "FLUSHDB SYNC on DB0, no disk format/discard", "command": ["FLUSHDB", "SYNC"]}
    marker.write_text(json.dumps(receipt, indent=2) + "\n")
    result = redis("FLUSHDB", "SYNC")
    if result != "OK" or int(redis("DBSIZE")) != 0:
        raise RuntimeError(f"DB0 clear did not complete: {result}")
    receipt["after"] = verify()
    marker.write_text(json.dumps(receipt, indent=2) + "\n")
    print(f"CLEARED DB0: {expected} old keys removed; RAID and Aerospike preserved", flush=True)
    # Confirm the epoch decision survives restart before inserting the new set.
    bench.execute(["sudo", "-n", "systemctl", "stop", UNIT], capture_output=True)
    state = start()
    if state["dbsize"] != 0:
        raise RuntimeError("DB0 clear was not durable")
    receipt["after_restart"] = state
    marker.write_text(json.dumps(receipt, indent=2) + "\n")
    dist = "/mnt/dev/YCSB-hreplace-dist"
    props = {"recordcount": COUNT, "insertstart": 0, "insertcount": COUNT,
             "fieldcount": 10, "fieldlength": 128, "fieldlengthdistribution": "constant",
             "readallfields": "true", "writeallfields": "true", "insertorder": "hashed",
             "requestdistribution": "uniform", "redis.host": bench.SERVER, "redis.port": 16379,
             "redis.scanindex": "none", "redis.timeout": 10000, "redis.cluster": "false",
             "measurementtype": "hdrhistogram", "measurement.interval": "both",
             "hdrhistogram.percentiles": "50,95,99,99.9,99.99", "status.interval": 10}
    args = [dist + "/bin/ycsb", "load", "redis", "-s", "-threads", "256", "-P", dist + "/workloads/workloada"]
    for key, value in props.items():
        args += ["-p", f"{key}={value}"]
    load = {"command": args, "start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    (ROOT / "cpu12-load.command.json").write_text(json.dumps(load, indent=2) + "\n")
    print("LOAD 100M records, 10 x 128B, client .5, no scan index", flush=True)
    bench.snapshot("hmset", ROOT / "cpu12-load.before.txt")
    host_snapshot("cpu12-load.host-before.json")
    with (ROOT / "cpu12-load.log").open("w") as output:
        process = subprocess.run(["ssh", bench.CLIENT, shlex.join(args)], stdout=output,
                                 stderr=subprocess.STDOUT, timeout=7200)
    load.update(bench.parse(ROOT / "cpu12-load.log", COUNT))
    load.update({"exit_code": process.returncode, "end_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                 "server": verify()})
    (ROOT / "cpu12-load.json").write_text(json.dumps(load, indent=2) + "\n")
    if process.returncode or load["failed"] or load["server"]["dbsize"] != COUNT:
        raise RuntimeError("Load incomplete or failed")
    bench.snapshot("hmset", ROOT / "cpu12-load.after.txt")
    host_snapshot("cpu12-load.host-after.json")
    print(f"LOADED {COUNT} keys, {load['success_qps']:.0f} inserts/s, zero errors", flush=True)


def run():
    no_client()
    env_path = ROOT / "cpu12-environment.json"
    if env_path.exists():
        raise RuntimeError("Preserve this experiment; use new filenames for a repeat")
    loaded = json.loads((ROOT / "cpu12-load.json").read_text())
    if loaded["failed"] or loaded["server"]["dbsize"] != COUNT:
        raise RuntimeError("A complete clean load is required")
    environment = {"commit": bench.execute(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip(),
                   "binary": BINARY, "sha256": hashlib.sha256(Path(BINARY).read_bytes()).hexdigest(),
                   "server": verify(), "workers": 12, "client_workers": 256,
                   "rates": [100000, 0], "rounds": 3, "workloads": ["C", "A"], "counts": bench.COUNTS,
                   "monitoring": bench.remote(["systemctl", "is-active", "prometheus", "grafana-server"]),
                   "comparison_note": "Historical 16-worker results use an older main and aged dataset; not an IRQ-only causal A/B."}
    env_path.write_text(json.dumps(environment, indent=2) + "\n")
    results = []
    for round_number in (1, 2, 3):
        for workload in ("C", "A"):
            for target in (100000, 0):
                label = f"cpu12-r{round_number}"
                for phase in ("warmup", "measured"):
                    stem = f"{label}-{workload.lower()}-target{target}-{phase}"
                    host_snapshot(stem + ".host-before.json")
                    cell = bench.run("hmset", workload, 256, phase, target=target,
                                     measurement_interval="both", label=label)
                    host_snapshot(stem + ".host-after.json")
                    if cell["failed"]:
                        raise RuntimeError("YCSB operation failure")
                    if phase == "measured":
                        results.append({"round": round_number, "binary_sha256": environment["sha256"], **cell})
                        (ROOT / "cpu12-results.json").write_text(json.dumps(results, indent=2) + "\n")
                        parts = []
                        for op in (("READ",) if workload == "C" else ("READ", "UPDATE")):
                            v = cell["metrics"][op]
                            parts.append(f"{op} qps={v['Operations']*1000/cell['runtime_ms']:.0f} "
                                         f"p99/p999/p9999={v['99thPercentileLatency(us)']}/"
                                         f"{v['99.9PercentileLatency(us)']}/{v['99.99PercentileLatency(us)']}us")
                        print(f"RESULT {len(results)}/12 {label} {workload} target={target} " + "; ".join(parts), flush=True)
    (ROOT / "cpu12-final-server.json").write_text(json.dumps(verify(), indent=2) + "\n")
    print("COMPLETE CPU12: 12 measured cells; isolated service remains running", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "clear-load", "run", "verify"))
    parser.add_argument("--expected-old-count", type=int)
    args = parser.parse_args()
    if args.action == "start":
        print(json.dumps(start(), indent=2))
    elif args.action == "clear-load":
        clear_load(args.expected_old_count)
    elif args.action == "verify":
        print(json.dumps(verify(), indent=2))
    else:
        run()
