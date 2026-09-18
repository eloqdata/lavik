#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run one YCSB client and one database at a time; preserve every receipt.

The preloaded first 100M keys are retained. No load/delete/partitioning is
performed. D inserts into disjoint fresh ranges without changing the Uniform
read range. A separate 1M-operation JVM warms server state before each count-
bounded (default 5M) or explicitly timed measurement. Every phase uses a new
JVM; latency and runtime are retained exactly as reported by YCSB.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time

ROOT = Path(__file__).resolve().parent
CLIENT = "172.16.0.5"
SERVER = "172.16.0.4"
LAVIK = "/mnt/dev/lavik/build/lavik"
AERO = "/usr/bin/asd"
COUNTS = {"warmup": 1_000_000, "measured": 5_000_000}
WORKLOADS = {"A": (0.5, 0.5, 0), "B": (0.95, 0.05, 0), "C": (1, 0, 0), "D": (0.95, 0, 0.05)}
WORKERS = (256,)


def execute(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def ssh_argv():
    command = ["ssh", "-o", "BatchMode=yes", "azureuser@" + CLIENT]
    return (["runuser", "-u", "azureuser", "--"] + command) if os.geteuid() == 0 else command


def remote(args):
    return execute(ssh_argv() + [shlex.join(args)], capture_output=True).stdout


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
        args = ["asinfo", "-h", SERVER, "-p", "3000", "-v", "namespace/ycsb"]
        result = subprocess.run(args, capture_output=True, text=True)
        return result.returncode == 0 and "stop_writes=false" in result.stdout
    result = subprocess.run(["redis-cli", "-h", SERVER, "-p", "16379", "ping"],
                            capture_output=True, text=True, timeout=10)
    return result.returncode == 0 and result.stdout.strip() == "PONG"


def switch(mode):
    binary = AERO if mode == "aerospike" else LAVIK
    stop(LAVIK if mode == "aerospike" else AERO)
    if not matching_pids(binary):
        if mode == "aerospike":
            command = [AERO, "--config-file", "/mnt/dev/aerospike-md0.conf", "--foreground"]
        else:
            command = ["taskset", "-c", "0-15", LAVIK, "--bind", SERVER,
                       "--port", "16379", "--metrics-port", "19100", "--threads", "16",
                       "--pin-workers", "--shutdown-checkpoint", "--log-dir",
                       "/mnt/dev/lavik-md0-lavik", "--data-file", "/dev/md0p1"]
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
        args = ["asinfo", "-h", SERVER, "-p", "3000", "-v", "namespace/ycsb"]
    else:
        args = ["curl", "--fail", "--silent", "--max-time", "15",
                f"http://{SERVER}:19100/metrics"]
    path.write_text(execute(args, capture_output=True).stdout)


def parse(path, expected, *, duration_seconds=0):
    """Validate a count-bounded run or a YCSB-terminated timed measurement.

    Timed runs must reach the timer and emit complete final counters; a killed
    client or an operation-cap exit must not masquerade as a five-minute run.
    """
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
    if runtime <= 0 or attempted <= 0 or (expected is not None and attempted != expected):
        raise RuntimeError(f"Incomplete run {path}: runtime={runtime}, attempted={attempted}")
    if duration_seconds and (runtime < duration_seconds * 1000 or
                             "Maximum time elapsed. Requesting stop for the workload." not in text):
        raise RuntimeError(f"Timed run did not reach its YCSB timer: {path}")
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


def run(mode, workload, workers, phase, *, target=0, measurement_interval="op", label="",
        duration_seconds=0, d_insert_start=None):
    """Run one cell; timed measurements use a non-binding 2B operation cap."""
    if duration_seconds < 0 or (duration_seconds and phase != "measured"):
        raise ValueError("Only formal measurements may use a positive time limit")
    if d_insert_start is not None and (workload != "D" or d_insert_start < 100_000_000):
        raise ValueError("An explicit transaction insert range is only valid for D")
    operation_count = 2_000_000_000 if duration_seconds else COUNTS[phase]
    expected = None if duration_seconds else operation_count
    rate_suffix = f"-target{target}" if target else ""
    prefix = f"{label}-" if label else ""
    name = f"{prefix}{mode}-{workload.lower()}-c{workers}{rate_suffix}-{phase}"
    saved_path = ROOT / f"{name}.json"
    if saved_path.exists():
        saved = json.loads(saved_path.read_text())
        checked = parse(ROOT / f"{name}.log", expected, duration_seconds=duration_seconds)
        if saved.get("requested_duration_seconds", 0) != duration_seconds:
            raise RuntimeError(f"Saved receipt has a different measurement duration: {name}")
        if saved.get("d_insert_start") != d_insert_start:
            raise RuntimeError(f"Saved receipt has a different D insert range: {name}")
        if saved.get("exit_code") != 0 or any(saved[k] != v for k, v in checked.items()):
            raise RuntimeError(f"Invalid saved receipt: {name}")
        print(f"REUSE {name} qps={saved['success_qps']:.0f} errors={saved['failed']}", flush=True)
        return saved
    dist = "/mnt/dev/YCSB-aerospike-dist" if mode == "aerospike" else "/mnt/dev/YCSB-hreplace-dist"
    binding = "aerospike" if mode == "aerospike" else "redis"
    read, update, insert = WORKLOADS[workload]
    props = {"recordcount": 100_000_000, "operationcount": operation_count,
             "fieldcount": 10, "fieldlength": 128, "fieldlengthdistribution": "constant",
             "readallfields": "true", "writeallfields": "true", "insertorder": "hashed",
             "requestdistribution": "uniform", "readproportion": read, "updateproportion": update,
             "insertproportion": insert, "scanproportion": 0, "readmodifywriteproportion": 0,
             "measurementtype": "hdrhistogram", "measurement.interval": measurement_interval,
             "hdrhistogram.percentiles": "50,95,99,99.9,99.99", "status.interval": 10}
    if duration_seconds:
        props["maxexecutiontime"] = duration_seconds
    if workload == "D":
        # CoreWorkload starts transaction inserts at recordcount; insertstart
        # and insertcount separately bound the Uniform read chooser. Distinct
        # warmup/measured and HMSET/HREPLACE ranges avoid CREATE_ONLY collisions.
        props.update({"recordcount": 200_000_000 + (20_000_000 if mode == "hreplace" else 0)
                      + (10_000_000 if phase == "measured" else 0),
                      "insertstart": 0, "insertcount": 100_000_000})
        # A rerun must explicitly reserve fresh transaction IDs. The historical
        # defaults above are retained only to reproduce historical receipts;
        # reusing them would turn inserts into overwrites or CREATE_ONLY errors.
        if d_insert_start is not None:
            props["recordcount"] = d_insert_start
    if mode == "aerospike":
        props.update({"as.host": SERVER, "as.port": 3000, "as.namespace": "ycsb", "as.timeout": 10000})
    else:
        props.update({"redis.host": SERVER, "redis.port": 16379, "redis.scanindex": "none",
                      "redis.timeout": 10000, "redis.cluster": "false",
                      "redis.updatecommand": "lavik.hreplace" if mode == "hreplace" else "hmset"})
    args = [dist + "/bin/ycsb", "run", binding, "-s", "-threads", str(workers),
            "-P", dist + "/workloads/workloada"]
    if target:
        args += ["-target", str(target)]
    for key, value in props.items():
        args += ["-p", f"{key}={value}"]
    receipt = {"mode": mode, "workload": workload, "workers": workers, "phase": phase,
               "label": label,
               "requested_duration_seconds": duration_seconds,
               "d_insert_start": d_insert_start,
               "target_ops_sec": target, "measurement_interval": measurement_interval,
               "command": args, "start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    (ROOT / f"{name}.command.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(f"START {name}", flush=True)
    snapshot(mode, ROOT / f"{name}.before.txt")
    with (ROOT / f"{name}.log").open("w") as output:
        process = subprocess.run(ssh_argv() + [shlex.join(args)], stdout=output,
                                 stderr=subprocess.STDOUT, timeout=max(600, duration_seconds + 120))
    receipt.update(parse(ROOT / f"{name}.log", expected, duration_seconds=duration_seconds))
    receipt["exit_code"] = process.returncode
    receipt["end_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    snapshot(mode, ROOT / f"{name}.after.txt")
    (ROOT / f"{name}.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(f"DONE {name} qps={receipt['success_qps']:.0f} errors={receipt['failed']}", flush=True)
    if process.returncode != 0:
        raise RuntimeError(f"YCSB process failed: {name}")
    return receipt



if __name__ == "__main__":
    raise SystemExit("Use runner.py to enforce dataset and per-product host-policy checks.")
