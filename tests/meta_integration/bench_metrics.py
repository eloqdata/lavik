#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
"""Paired Release Meta metrics benchmark; writes raw per-run JSONL outside source.

A logical operation is submitop + completeop over Unix Admin sockets (two
proposals). Production Raft defaults are retained. Each run uses a fresh
three-member cluster. CPU and RSS aggregate the three Meta processes; client
CPU is reported separately so load-generator saturation remains visible.
"""

import argparse
import concurrent.futures
import http.client
import json
import multiprocessing as mp
import os
from pathlib import Path
import socket
import threading
import time

import harness as H


def command(path, text):
    with socket.socket(socket.AF_UNIX) as conn:
        conn.settimeout(8)
        conn.connect(path)
        conn.sendall(text.encode() + b"\n")
        data = b""
        while not data.endswith(b"\n"):
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
        if not data.startswith(b"OK "):
            raise RuntimeError(data.decode())


def load(path, client, start, duration, interval, result):
    latencies, errors = [], []
    sequence = 0
    cpu0 = None
    while time.monotonic() < start + duration:
        began = time.monotonic()
        if began >= start and cpu0 is None:
            cpu0 = time.process_time()
        sequence += 1
        op = f"{client:08x}{sequence:024x}"
        try:
            command(path, f"submitop {op} metrics-benchmark payload")
            command(path, f"completeop {op} payload")
            if began >= start:
                latencies.append(time.monotonic() - began)
        except Exception as error:
            errors.append(str(error))
        if interval:
            time.sleep(max(0, interval - (time.monotonic() - began)))
    result.put(
        (latencies, errors, time.process_time() - cpu0 if cpu0 is not None else 0)
    )


def host_usage():
    fields = [
        int(value)
        for value in Path("/proc/stat").read_text().splitlines()[0].split()[1:]
    ]
    ticks = os.sysconf("SC_CLK_TCK")
    # guest and guest_nice are already included in user and nice.
    return (sum(fields[:8]) - fields[3] - fields[4]) / ticks, fields[4] / ticks


def usage(nodes):
    cpu, rss = 0, 0
    for node in nodes:
        fields = Path(f"/proc/{node.pid}/stat").read_text().rsplit(")", 1)[1].split()
        cpu += (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
        rss += H.read_rss_kb(node.pid) * 1024
    return cpu, rss


def run(args, pair, variant, clients):
    root = Path(args.output).parent / f"pair-{pair}-{variant}-clients-{clients}"
    root.mkdir(parents=True)
    binary = args.baseline if variant == "baseline" else args.candidate
    nodes = H.make_nodes(binary, str(root), 3, args=[])
    active = variant in ("idle", "scrape", "stress")
    for node in nodes:
        node.metrics_port = H.free_port()
        if active:
            node.args += ["--metrics-port", str(node.metrics_port)]
    stopped = threading.Event()
    scrape_counts = {}
    lock = threading.Lock()

    def collect(node, delay):
        while not stopped.is_set():
            code = 0
            conn = http.client.HTTPConnection("127.0.0.1", node.metrics_port, timeout=3)
            try:
                conn.request("GET", "/metrics")
                response = conn.getresponse()
                code = response.status
                response.read()
            except (OSError, http.client.HTTPException):
                pass
            finally:
                conn.close()
            with lock:
                scrape_counts[code] = scrape_counts.get(code, 0) + 1
            if delay:
                stopped.wait(delay)

    pool, children = None, []
    try:
        leader = H.bootstrap_cluster(nodes)
        queue = mp.Queue()
        start = time.monotonic() + args.warmup
        for client in range(clients):
            child = mp.Process(
                target=load,
                args=(
                    leader.ctl_path,
                    client + 100000,
                    start,
                    args.duration,
                    0.02 if clients == 1 else 0,
                    queue,
                ),
            )
            child.start()
            children.append(child)
        if variant == "scrape":
            pool = concurrent.futures.ThreadPoolExecutor(max_workers=3)
            for node in nodes:
                pool.submit(collect, node, 5)
        elif variant == "stress":
            pool = concurrent.futures.ThreadPoolExecutor(max_workers=32)
            for _ in range(32):
                pool.submit(collect, leader, 0)
        time.sleep(max(0, start - time.monotonic()))
        parent_cpu0 = time.process_time()
        host_cpu0, host_wait0 = host_usage()
        cpu0, rss_peak = usage(nodes)
        while time.monotonic() < start + args.duration:
            _, rss = usage(nodes)
            rss_peak = max(rss_peak, rss)
            time.sleep(0.1)
        cpu1, rss = usage(nodes)
        host_cpu1, host_wait1 = host_usage()
        parent_cpu1 = time.process_time()
        results = [queue.get(timeout=30) for _ in children]
        for child in children:
            child.join(timeout=10)
            assert child.exitcode == 0
        stopped.set()
        if pool:
            pool.shutdown()
        latencies = sorted(v for values, _, _ in results for v in values)
        errors = [v for _, values, _ in results for v in values]

        def percentile(q):
            return (
                latencies[min(len(latencies) - 1, int(q * len(latencies)))] * 1000
                if latencies
                else None
            )

        samples = root / "latencies-seconds.json"
        samples.write_text(json.dumps(latencies))
        shutdown = time.monotonic()
        for node in nodes:
            node.terminate()
        record = dict(
            pair=pair,
            variant=variant,
            clients=clients,
            binary=binary,
            duration_seconds=args.duration,
            warmup_seconds=args.warmup,
            operations=len(latencies),
            errors=len(errors),
            first_error=errors[:1],
            ops_per_second=len(latencies) / args.duration,
            p99_ms=percentile(0.99),
            p999_ms=percentile(0.999),
            meta_cpu_seconds=cpu1 - cpu0,
            host_busy_cpu_seconds=host_cpu1 - host_cpu0,
            host_iowait_cpu_seconds=host_wait1 - host_wait0,
            cpu_us_per_op=(cpu1 - cpu0) * 1e6 / max(1, len(latencies)),
            client_cpu_seconds=sum(cpu for _, _, cpu in results),
            driver_cpu_seconds=parent_cpu1 - parent_cpu0,
            rss_peak_bytes=rss_peak,
            shutdown_seconds=time.monotonic() - shutdown,
            scrape_http_counts=scrape_counts,
            latency_samples_file=str(samples),
        )
        with open(args.output, "a") as output:
            output.write(json.dumps(record) + "\n")
        print(json.dumps(record), flush=True)
    finally:
        stopped.set()
        if pool:
            pool.shutdown()
        for child in children:
            if child.is_alive():
                child.terminate()
            child.join()
        for node in nodes:
            node.force_kill()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--warmup", type=float, default=5)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 8])
    parser.add_argument(
        "--variants", nargs="+", default=["disabled", "idle", "scrape", "stress"]
    )
    args = parser.parse_args()
    args.baseline, args.candidate = (
        os.path.abspath(args.baseline),
        os.path.abspath(args.candidate),
    )
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    for clients in args.clients:
        for variant in args.variants:
            for pair in range(args.pairs):
                # A distinct baseline run for each variant keeps comparisons paired.
                for version in (
                    ["baseline", variant] if pair % 2 == 0 else [variant, "baseline"]
                ):
                    # Keep unique state paths across independent paired baselines.
                    run(args, f"{variant}-{pair}", version, clients)


if __name__ == "__main__":
    main()
