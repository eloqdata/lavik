#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Matched baseline/always-on/sampled GET+SET benchmark with raw memtier JSON.

Requires Linux, taskset, memtier_benchmark, and the repository's tests directory.
CPU lists are explicit so the operator controls physical-core/SMT placement.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from latency_diagnostics_e2e import Client, free_port  # noqa: E402

MODES = ["baseline", "always-on", "sample-64", "sample-1", "sample-1-threshold-0"]


def cpu_seconds(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().split(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def thread_cpu(pid):
    """Kernel per-thread runtime has ns resolution, unlike /proc stat ticks."""
    result = {}
    for path in Path(f"/proc/{pid}/task").iterdir():
        try:
            result[path.name] = {
                "runtime_ns": int((path / "schedstat").read_text().split()[0]),
                "name": (path / "comm").read_text().strip(),
                "affinity": sorted(os.sched_getaffinity(int(path.name))),
            }
        except FileNotFoundError:
            pass
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--server-cpus", required=True)
    parser.add_argument("--client-cpus", required=True)
    parser.add_argument("--single-client-cpus", required=True)
    parser.add_argument("--seconds", type=int, default=15)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--logging-only", action="store_true")
    parser.add_argument("--log-limit", type=int, default=20)
    parser.add_argument("--modes", help="optional comma-separated subset of modes")
    parser.add_argument(
        "--workers", default="1,2", help="comma-separated worker counts"
    )
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {
        "baseline": args.baseline.resolve(),
        "candidate": args.candidate.resolve(),
    }
    metadata = {
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "arguments": vars(args)
        | {
            "baseline": str(args.baseline),
            "candidate": str(args.candidate),
            "output": str(args.output),
        },
        "binary_sha256": {
            k: hashlib.sha256(p.read_bytes()).hexdigest() for k, p in binaries.items()
        },
        "uname": os.uname()._asdict()
        if hasattr(os.uname(), "_asdict")
        else list(os.uname()),
        "memtier_version": subprocess.check_output(
            ["memtier_benchmark", "--version"], text=True
        ),
    }
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    modes_to_test = (
        ["log-off-64", "log-on-64", "log-off-1", "log-on-1"]
        if args.logging_only
        else MODES
    )
    if args.modes:
        requested = args.modes.split(",")
        assert all(mode in modes_to_test for mode in requested), requested
        modes_to_test = requested
    rows = []
    worker_counts = [int(w) for w in args.workers.split(",")]
    for workers in worker_counts:
        cpus = ",".join(args.server_cpus.split(",")[:workers])
        clients = args.single_client_cpus if workers == 1 else args.client_cpus
        for repeat in range(args.rounds):
            # Rotate order to distribute warming and time-of-run bias.
            shift = repeat % len(modes_to_test)
            modes = modes_to_test[shift:] + modes_to_test[:shift]
            for mode in modes:
                run = args.output / f"w{workers}-r{repeat}-{mode}"
                run.mkdir()
                data = run / "data"
                with data.open("wb") as f:
                    os.posix_fallocate(f.fileno(), 0, 2 * 1024**3)
                port, metrics = free_port(), free_port()
                while metrics == port:
                    metrics = free_port()
                binary = binaries["baseline" if mode == "baseline" else "candidate"]
                server_args = [
                    str(binary),
                    "--cpus",
                    cpus,
                    "--port",
                    str(port),
                    "--metrics-port",
                    str(metrics),
                    "--data-file",
                    str(data),
                    "--logtostderr",
                    "--log-dir",
                    str(run / "logs"),
                ]
                (run / "server-args.json").write_text(json.dumps(server_args))
                with (run / "server.log").open("wb") as log:
                    server = subprocess.Popen(server_args, stdout=log, stderr=log)
                    client = None
                    try:
                        for _ in range(300):
                            if server.poll() is not None:
                                raise RuntimeError(
                                    (run / "server.log").read_text()[-6000:]
                                )
                            try:
                                client = Client(port)
                                assert client.command("PING") == "PONG"
                                break
                            except OSError:
                                time.sleep(0.05)
                        assert client is not None
                        common = [
                            "taskset",
                            "-c",
                            clients,
                            "memtier_benchmark",
                            "--port",
                            str(port),
                            "--data-size",
                            "512",
                            "--key-maximum",
                            "9999",
                        ]
                        preload = common + [
                            "--threads",
                            "1",
                            "--clients",
                            "1",
                            "--ratio",
                            "1:0",
                            "--key-pattern",
                            "S:S",
                        ]
                        with (run / "preload.log").open("wb") as out:
                            subprocess.run(
                                preload,
                                stdout=out,
                                stderr=subprocess.STDOUT,
                                check=True,
                            )
                        if mode.startswith("log-"):
                            for key, value in [
                                ("sample-every", 64 if mode.endswith("64") else 1),
                                ("slow-threshold-us", 0),
                                ("log-max-per-second", args.log_limit),
                                (
                                    "log-enabled",
                                    "yes" if mode.startswith("log-on") else "no",
                                ),
                                ("enabled", "yes"),
                            ]:
                                assert (
                                    client.command(
                                        "CONFIG",
                                        "SET",
                                        "latency-diagnostics-" + key,
                                        value,
                                    )
                                    == "OK"
                                )
                        if mode.startswith("sample"):
                            for key, value in [
                                ("sample-every", 64 if mode == "sample-64" else 1),
                                (
                                    "slow-threshold-us",
                                    0 if mode.endswith("threshold-0") else 10000,
                                ),
                                ("enabled", "yes"),
                            ]:
                                assert (
                                    client.command(
                                        "CONFIG",
                                        "SET",
                                        "latency-diagnostics-" + key,
                                        value,
                                    )
                                    == "OK"
                                )
                        workload = common + [
                            "--threads",
                            "2",
                            "--clients",
                            "16",
                            "--ratio",
                            "1:1",
                        ]
                        with (run / "warmup.log").open("wb") as out:
                            subprocess.run(
                                workload + ["--test-time", "3"],
                                stdout=out,
                                stderr=subprocess.STDOUT,
                                check=True,
                            )
                        command = workload + [
                            "--test-time",
                            str(args.seconds),
                            "--json-out-file",
                            str(run / "memtier.json"),
                        ]
                        (run / "commands.json").write_text(
                            json.dumps(
                                {
                                    "preload": preload,
                                    "warmup": workload + ["--test-time", "3"],
                                    "measurement": command,
                                },
                                indent=2,
                            )
                        )
                        log_files = lambda: list(
                            (run / "logs").glob("latency-diagnostics*.log")
                        )

                        def log_stats():
                            files = log_files()
                            return (
                                sum(p.stat().st_size for p in files),
                                sum(p.read_bytes().count(b"\n") for p in files),
                            )

                        log_bytes_before, log_records_before = log_stats()
                        threads_before = thread_cpu(server.pid)
                        before = cpu_seconds(server.pid)
                        started = time.monotonic()
                        with (run / "memtier.log").open("wb") as out:
                            subprocess.run(
                                command,
                                stdout=out,
                                stderr=subprocess.STDOUT,
                                check=True,
                            )
                        elapsed = time.monotonic() - started
                        cpu = cpu_seconds(server.pid) - before
                        threads_after = thread_cpu(server.pid)
                        log_bytes_after, log_records_after = log_stats()
                        result = json.loads((run / "memtier.json").read_text())[
                            "ALL STATS"
                        ]
                        totals = result["Totals"]
                        assert totals["Connection Errors"] == 0
                        assert result["Gets"]["Misses/sec"] == 0, result["Gets"]
                        with urllib.request.urlopen(
                            f"http://127.0.0.1:{metrics}/metrics", timeout=10
                        ) as response:
                            (run / "metrics.prom").write_bytes(response.read())
                        if mode.startswith("log-off"):
                            assert log_bytes_after == 0 and log_records_after == 0
                        if mode.startswith("log-on"):
                            assert log_records_after > log_records_before, (
                                "log branch did not write records"
                            )
                        row = {
                            "workers": workers,
                            "round": repeat,
                            "mode": mode,
                            "ops_sec": totals["Ops/sec"],
                            "p99_ms": totals["Percentile Latencies"]["p99.00"],
                            "p999_ms": totals["Percentile Latencies"]["p99.90"],
                            "server_cpu_seconds": cpu,
                            "cpu_us_per_op": cpu * 1e6 / totals["Count"],
                            "log_bytes": log_bytes_after - log_bytes_before,
                            "log_records": log_records_after - log_records_before,
                            "measurement_seconds": totals["Count"] / totals["Ops/sec"],
                            "thread_cpu": {
                                tid: values
                                | {
                                    "runtime_ns": values["runtime_ns"]
                                    - threads_before.get(tid, {}).get("runtime_ns", 0),
                                    "main": tid == str(server.pid),
                                }
                                for tid, values in threads_after.items()
                            },
                            "client_cpu_cores": result.get("CPU", {}).get(
                                "cpu_cores_used"
                            ),
                            "elapsed_seconds": elapsed,
                            "rss_kib": int(
                                next(
                                    line.split()[1]
                                    for line in Path(f"/proc/{server.pid}/status")
                                    .read_text()
                                    .splitlines()
                                    if line.startswith("VmRSS:")
                                )
                            ),
                            "raw": str(run.relative_to(args.output) / "memtier.json"),
                        }
                        rows.append(row)
                        (args.output / "runs.json").write_text(
                            json.dumps(rows, indent=2) + "\n"
                        )
                        print(json.dumps(row), flush=True)
                    finally:
                        if client:
                            client.close()
                        server.terminate()
                        try:
                            server.wait(timeout=30)
                        except subprocess.TimeoutExpired:
                            server.kill()
                            server.wait()
                        data.unlink(missing_ok=True)
    summary = []
    for workers in worker_counts:
        for mode in modes_to_test:
            selected = [
                r for r in rows if r["workers"] == workers and r["mode"] == mode
            ]
            entry = {"workers": workers, "mode": mode, "runs": len(selected)}
            for name in [
                "ops_sec",
                "p99_ms",
                "p999_ms",
                "cpu_us_per_op",
                "rss_kib",
                "log_bytes",
                "log_records",
            ]:
                values = [r[name] for r in selected]
                entry[name] = {
                    "median": statistics.median(values),
                    "min": min(values),
                    "max": max(values),
                }
            summary.append(entry)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
