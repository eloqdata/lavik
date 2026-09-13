#!/usr/bin/env python3
"""Run one YCSB client and one database at a time; preserve every receipt.

The preloaded first 100M keys are retained. No load/delete/partitioning is
performed. D inserts into disjoint fresh ranges without changing the Uniform
read range. A separate 1M-operation JVM warms server state before each 5M
measurement; measured JVM startup/JIT remains part of the bounded experiment.
"""
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess
import time

ROOT = Path(__file__).resolve().parent
CLIENT = "172.16.0.5"
SERVER = "172.16.0.4"
KEYLANE = "/mnt/dev/keylane/build/keylane"
AERO = "/usr/bin/asd"
COUNTS = {"warmup": 1_000_000, "measured": 5_000_000}
WORKLOADS = {"A": (0.5, 0.5, 0), "B": (0.95, 0.05, 0), "C": (1, 0, 0), "D": (0.95, 0, 0.05)}
WORKERS = (256,)


def execute(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def remote(args):
    return execute(["ssh", CLIENT, shlex.join(args)], capture_output=True).stdout


def matching_pids(binary):
    # Match executable paths, not shell command lines containing the path.
    result = subprocess.run(["sudo", "-n", "pgrep", "-x", Path(binary).name],
                            capture_output=True, text=True)
    if result.returncode not in (0, 1):
        raise RuntimeError(result.stderr)
    pids = []
    for raw in result.stdout.split():
        target = subprocess.run(["sudo", "-n", "readlink", f"/proc/{raw}/exe"],
                                capture_output=True, text=True)
        if target.stdout.strip() == binary:
            pids.append(int(raw))
    return pids


def stop(binary):
    pids = matching_pids(binary)
    for pid in pids:
        execute(["sudo", "-n", "kill", "-TERM", str(pid)])
    deadline = time.monotonic() + 180
    while matching_pids(binary):
        if time.monotonic() > deadline:
            raise RuntimeError(f"Graceful stop did not finish: {binary}")
        time.sleep(1)


def ready(mode):
    if mode == "aerospike":
        args = ["asinfo", "-h", "127.0.0.1", "-p", "3000", "-v", "namespace/ycsb"]
        result = subprocess.run(args, capture_output=True, text=True)
        return result.returncode == 0 and "stop_writes=false" in result.stdout
    result = subprocess.run(["redis-cli", "-h", SERVER, "-p", "16379", "ping"],
                            capture_output=True, text=True, timeout=10)
    return result.returncode == 0 and result.stdout.strip() == "PONG"


def switch(mode):
    binary = AERO if mode == "aerospike" else KEYLANE
    stop(KEYLANE if mode == "aerospike" else AERO)
    if not matching_pids(binary):
        if mode == "aerospike":
            command = [AERO, "--config-file", "/mnt/dev/aerospike-md0.conf", "--foreground"]
        else:
            command = ["taskset", "-c", "0-15", KEYLANE, "--bind", SERVER,
                       "--port", "16379", "--metrics-port", "19100", "--threads", "16",
                       "--pin-workers", "--shutdown-checkpoint", "--log-dir",
                       "/mnt/dev/keylane-md0-keylane", "--data-file", "/dev/md0p1"]
        with (ROOT / f"{mode}-server.stdout").open("a") as output:
            subprocess.Popen(["sudo", "-n", "bash", "-c",
                              "ulimit -n 20000; exec " + shlex.join(command)],
                             stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                             start_new_session=True)
    # CE rebuilds its in-memory index from the raw partition after restart.
    # This happens before warmup/measurement and can take several minutes.
    deadline = time.monotonic() + 1200
    while not ready(mode):
        if time.monotonic() > deadline:
            raise RuntimeError(f"Server readiness timeout: {mode}")
        time.sleep(2)


def snapshot(mode, path):
    if mode == "aerospike":
        args = ["asinfo", "-h", "127.0.0.1", "-p", "3000", "-v", "namespace/ycsb"]
    else:
        args = ["curl", "--fail", "--silent", "--max-time", "15",
                f"http://{SERVER}:19100/metrics"]
    path.write_text(execute(args, capture_output=True).stdout)


def parse(path, expected):
    text = path.read_text()
    values = {}
    for line in text.splitlines():
        match = re.fullmatch(r"\[([^]]+)\], ([^,]+), (.+)", line)
        if match:
            group, name, raw = match.groups()
            try:
                value = float(raw) if any(c in raw for c in ".eE") else int(raw)
            except ValueError:
                value = raw
            values.setdefault(group, {})[name] = value
    runtime = values.get("OVERALL", {}).get("RunTime(ms)", 0)
    operations = {op: values[op] for op in ("READ", "UPDATE", "INSERT", "DELETE", "SCAN", "READ-MODIFY-WRITE") if op in values}
    attempted = sum(v for fields in operations.values() for k, v in fields.items() if k.startswith("Return="))
    succeeded = sum(fields.get("Return=OK", 0) for fields in operations.values())
    errors = attempted - succeeded
    if runtime <= 0 or attempted != expected:
        raise RuntimeError(f"Incomplete run {path}: runtime={runtime}, attempted={attempted}")
    if any(op not in ("READ", "UPDATE", "INSERT") for op in operations):
        raise RuntimeError(f"Unexpected operation in A/B/C/D matrix: {path}")
    for fields in operations.values():
        if fields.get("Operations", 0) != fields.get("Return=OK", 0):
            raise RuntimeError(f"Successful latency count does not match return count: {path}")
        for p in ("99thPercentileLatency(us)", "99.9PercentileLatency(us)", "99.99PercentileLatency(us)"):
            if p not in fields:
                raise RuntimeError(f"Missing latency percentile {p}: {path}")
    return {"runtime_ms": runtime, "attempted": attempted, "succeeded": succeeded,
            "failed": errors, "success_qps": succeeded * 1000 / runtime,
            "metrics": values, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def run(mode, workload, workers, phase, *, target=0, measurement_interval="op", label=""):
    rate_suffix = f"-target{target}" if target else ""
    prefix = f"{label}-" if label else ""
    name = f"{prefix}{mode}-{workload.lower()}-c{workers}{rate_suffix}-{phase}"
    saved_path = ROOT / f"{name}.json"
    if saved_path.exists():
        saved = json.loads(saved_path.read_text())
        checked = parse(ROOT / f"{name}.log", COUNTS[phase])
        if saved.get("exit_code") != 0 or any(saved[k] != v for k, v in checked.items()):
            raise RuntimeError(f"Invalid saved receipt: {name}")
        print(f"REUSE {name} qps={saved['success_qps']:.0f} errors={saved['failed']}", flush=True)
        return saved
    dist = "/mnt/dev/YCSB-aerospike-dist" if mode == "aerospike" else "/mnt/dev/YCSB-hreplace-dist"
    binding = "aerospike" if mode == "aerospike" else "redis"
    read, update, insert = WORKLOADS[workload]
    props = {"recordcount": 100_000_000, "operationcount": COUNTS[phase],
             "fieldcount": 10, "fieldlength": 128, "fieldlengthdistribution": "constant",
             "readallfields": "true", "writeallfields": "true", "insertorder": "hashed",
             "requestdistribution": "uniform", "readproportion": read, "updateproportion": update,
             "insertproportion": insert, "scanproportion": 0, "readmodifywriteproportion": 0,
             "measurementtype": "hdrhistogram", "measurement.interval": measurement_interval,
             "hdrhistogram.percentiles": "50,95,99,99.9,99.99", "status.interval": 10}
    if workload == "D":
        # CoreWorkload starts transaction inserts at recordcount; insertstart
        # and insertcount separately bound the Uniform read chooser. Distinct
        # warmup/measured and HMSET/HREPLACE ranges avoid CREATE_ONLY collisions.
        props.update({"recordcount": 200_000_000 + (20_000_000 if mode == "hreplace" else 0)
                      + (10_000_000 if phase == "measured" else 0),
                      "insertstart": 0, "insertcount": 100_000_000})
    if mode == "aerospike":
        props.update({"as.host": SERVER, "as.port": 3000, "as.namespace": "ycsb", "as.timeout": 10000})
    else:
        props.update({"redis.host": SERVER, "redis.port": 16379, "redis.scanindex": "none",
                      "redis.timeout": 10000, "redis.cluster": "false",
                      "redis.updatecommand": "keylane.hreplace" if mode == "hreplace" else "hmset"})
    args = [dist + "/bin/ycsb", "run", binding, "-s", "-threads", str(workers),
            "-P", dist + "/workloads/workloada"]
    if target:
        args += ["-target", str(target)]
    for key, value in props.items():
        args += ["-p", f"{key}={value}"]
    receipt = {"mode": mode, "workload": workload, "workers": workers, "phase": phase,
               "label": label,
               "target_ops_sec": target, "measurement_interval": measurement_interval,
               "command": args, "start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    (ROOT / f"{name}.command.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(f"START {name}", flush=True)
    snapshot(mode, ROOT / f"{name}.before.txt")
    with (ROOT / f"{name}.log").open("w") as output:
        process = subprocess.run(["ssh", CLIENT, shlex.join(args)], stdout=output,
                                 stderr=subprocess.STDOUT, timeout=600)
    receipt.update(parse(ROOT / f"{name}.log", COUNTS[phase]))
    receipt["exit_code"] = process.returncode
    receipt["end_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    snapshot(mode, ROOT / f"{name}.after.txt")
    (ROOT / f"{name}.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(f"DONE {name} qps={receipt['success_qps']:.0f} errors={receipt['failed']}", flush=True)
    if process.returncode != 0:
        raise RuntimeError(f"YCSB process failed: {name}")
    return receipt


def main():
    # No concurrent clients: the runner issues one blocking SSH run at a time.
    check = subprocess.run(["ssh", CLIENT, "pgrep -a java"], capture_output=True, text=True)
    if check.returncode == 0:
        raise RuntimeError(f"Client already has Java workloads: {check.stdout}")
    results = []
    for mode in ("hmset", "hreplace", "aerospike"):
        pending = any(not (ROOT / f"{mode}-{workload.lower()}-c{workers}-measured.json").exists()
                      for workload in WORKLOADS for workers in WORKERS)
        if pending:
            switch(mode)
        for workload in WORKLOADS:
            # C invokes no update command; collect it for both Keylane client
            # configurations as a guard, never call it HREPLACE write performance.
            for workers in WORKERS:
                warmup = run(mode, workload, workers, "warmup")
                if warmup["failed"]:
                    raise RuntimeError("Warmup failed; investigate before further measurement")
                measured = run(mode, workload, workers, "measured")
                results.append(measured)
                (ROOT / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    stop(AERO)
    switch("hmset")
    print(f"COMPLETE {len(results)} cells", flush=True)


if __name__ == "__main__":
    main()
