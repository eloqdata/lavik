#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Measure one native FULL with real, isolated network delay and exact binaries.

Requires Linux, sudo -n, ip, tc, setpriv, and optionally tcpdump. Only a uniquely
named network namespace and its dedicated veth qdiscs are modified. The source
runs on the host, the target in that namespace. Clients reach the source over
loopback, so writer latency does not include the injected replication RTT.

Each invocation writes result.json, samples.jsonl and logs under a NEW run
directory. Repeat invocations with different binaries and run directories;
do not compare a Release binary with a fault-instrumented or Debug binary.
Reported FULL time includes native preflight and the all-flow ONLINE boundary,
but excludes seeding, process startup, and the subsequent complete data digest.
The optional packet capture records actual record-frame partition/DB sizes;
its overhead should be checked against a run without --capture.

Example (run from a checkout with all artifacts on task-owned NVMe):
  python3 scripts/bench_native_full_sync.py --binary /path/build/lavik \
    --revision COMMIT --label main --run-dir /mnt/local_nvme/i131/run-001 \
    --workload large-string --keys 8 --value-bytes 8388608 --rtt-ms 5 \
    --source-workers 1 --target-workers 1 --source-cpus 0 --target-cpus 1 \
    --client-cpus 6-7 --capture

For ordinary records compare BOTH uniform keys and --workload dense. Dense
keys concentrate in --hot-slots per flow: they are deliberately a favorable
window workload, not a substitute for sparse per-partition/DB measurements.
"""

import argparse
import binascii
import collections
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import urllib.request


class RespError(RuntimeError):
    pass


def encode(args):
    values = [a if isinstance(a, bytes) else str(a).encode() for a in args]
    return b"*%d\r\n" % len(values) + b"".join(
        b"$%d\r\n" % len(v) + v + b"\r\n" for v in values
    )


def read_resp(reader):
    line = reader.readline()
    if not line:
        raise EOFError("RESP peer disconnected")
    kind, payload = line[:1], line[1:-2]
    if kind == b"-":
        raise RespError(payload.decode(errors="replace"))
    if kind == b"+":
        return payload
    if kind == b":":
        return int(payload)
    if kind == b"$":
        size = int(payload)
        if size == -1:
            return None
        value = reader.read(size)
        if len(value) != size or reader.read(2) != b"\r\n":
            raise EOFError("truncated RESP bulk string")
        return value
    if kind == b"*":
        count = int(payload)
        return None if count == -1 else [read_resp(reader) for _ in range(count)]
    raise ValueError(f"unsupported RESP prefix: {kind!r}")


class Client:
    def __init__(self, host, port, timeout=30):
        self.socket = socket.create_connection((host, port), timeout=timeout)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.reader = self.socket.makefile("rb")

    def call(self, *args):
        self.socket.sendall(encode(args))
        return read_resp(self.reader)

    def pipeline(self, commands):
        self.socket.sendall(b"".join(encode(c) for c in commands))
        return [read_resp(self.reader) for _ in commands]

    def close(self):
        self.reader.close()
        self.socket.close()


def command(args, **kwargs):
    return subprocess.run(
        [str(a) for a in args], check=True, capture_output=True, text=True, **kwargs
    ).stdout.strip()


def privileged(*args):
    return command(["sudo", "-n", *args])


def cpus(text):
    result = set()
    for part in text.split(","):
        ends = list(map(int, part.split("-")))
        result.update(range(ends[0], ends[-1] + 1))
    return result


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Network:
    def __init__(self, rtt_ms):
        # Each run has its own namespace, route, interfaces, and qdiscs. Refuse
        # a colliding name rather than adopting resources owned by another run.
        suffix = str(os.getpid())
        self.name, self.host_if, self.peer_if = (
            "lvfull-" + suffix,
            "lvh" + suffix,
            "lvp" + suffix,
        )
        subnet = 1 + os.getpid() % 250
        self.source_ip, self.target_ip = f"198.18.{subnet}.1", f"198.18.{subnet}.2"
        self.rtt_ms = rtt_ms
        self.created = False
        self.link_created = False

    def start(self):
        privileged("ip", "netns", "add", self.name)
        self.created = True
        privileged(
            "ip",
            "link",
            "add",
            self.host_if,
            "type",
            "veth",
            "peer",
            "name",
            self.peer_if,
        )
        self.link_created = True
        privileged("ip", "link", "set", self.peer_if, "netns", self.name)
        privileged("ip", "addr", "add", self.source_ip + "/24", "dev", self.host_if)
        privileged("ip", "link", "set", self.host_if, "up")
        privileged(
            "ip",
            "-n",
            self.name,
            "addr",
            "add",
            self.target_ip + "/24",
            "dev",
            self.peer_if,
        )
        privileged("ip", "-n", self.name, "link", "set", self.peer_if, "up")
        privileged("ip", "-n", self.name, "link", "set", "lo", "up")
        if self.rtt_ms:
            delay = f"{self.rtt_ms / 2:.6f}ms"
            privileged(
                "tc",
                "qdisc",
                "add",
                "dev",
                self.host_if,
                "root",
                "netem",
                "delay",
                delay,
                "limit",
                "100000",
            )
            privileged(
                "ip",
                "netns",
                "exec",
                self.name,
                "tc",
                "qdisc",
                "add",
                "dev",
                self.peer_if,
                "root",
                "netem",
                "delay",
                delay,
                "limit",
                "100000",
            )

    def prefix(self):
        return [
            "sudo",
            "-n",
            "ip",
            "netns",
            "exec",
            self.name,
            "setpriv",
            f"--reuid={os.getuid()}",
            f"--regid={os.getgid()}",
            "--clear-groups",
        ]

    def stop(self):
        if self.created:
            # Any surviving process is task-owned. Normally Server.stop has
            # already joined the target; this also handles interrupted startup.
            for pid in privileged("ip", "netns", "pids", self.name).split():
                subprocess.run(
                    ["sudo", "-n", "kill", "-KILL", pid],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            privileged("ip", "netns", "del", self.name)
        if self.link_created:
            subprocess.run(
                ["sudo", "-n", "ip", "link", "del", self.host_if],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


class Server:
    def __init__(self, args, directory, network, target=False):
        self.directory, self.target, self.network = directory, target, network
        directory.mkdir()
        self.port, self.metrics_port = free_port(), free_port()
        self.host = network.target_ip if target else "127.0.0.1"
        workers = args.target_workers if target else args.source_workers
        affinity = args.target_cpus if target else args.source_cpus
        data = directory / "data"
        with data.open("wb") as f:
            os.posix_fallocate(f.fileno(), 0, args.data_file_mib * 1024 * 1024)
        self.argv = [
            args.binary,
            "--bind",
            "0.0.0.0",
            "--port",
            str(self.port),
            "--threads",
            str(workers),
            "--no-pin-workers",
            "--metrics-port",
            str(self.metrics_port),
            "--recv-buffers-per-worker",
            "0",
            "--registered-buffer-mb-per-worker",
            "64",
            "--max-memory",
            str(args.max_memory_mib * 1024 * 1024),
            "--repl-backlog-size",
            f"{args.backlog_mib}mb",
            "--replication-publish-queue-mb-per-worker",
            str(args.queue_mib),
            "--flush-max-ms",
            str(args.flush_ms),
            "--data-file",
            str(data),
            "--rdb-dir",
            str(directory),
            "--logtostderr",
        ]
        if affinity:
            self.argv = ["taskset", "-c", affinity, *self.argv]
        if target:
            self.argv = [*network.prefix(), *self.argv]
        self.proc = None
        self.log = None
        self.pid = None

    def start(self):
        self.log = (self.directory / "server.log").open("wb")
        self.proc = subprocess.Popen(self.argv, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited: {self.directory / 'server.log'}")
            try:
                client = self.client(timeout=1)
                try:
                    if client.call("PING") == b"PONG":
                        self.pid = int(info(client, "server")["process_id"])
                        return
                finally:
                    client.close()
            except (OSError, EOFError, RespError):
                time.sleep(0.05)
        raise TimeoutError(f"server did not start: {self.directory}")

    def client(self, timeout=30):
        client = Client(self.host, self.port, timeout)
        if self.target:
            try:
                assert client.call("READONLY") == b"OK"
            except BaseException:
                client.close()
                raise
        return client

    def stop(self):
        if self.proc is not None and self.proc.poll() is None:
            if self.pid is not None:
                os.kill(self.pid, signal.SIGINT)
            else:
                self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(30)
            except subprocess.TimeoutExpired:
                if self.pid is not None:
                    os.kill(self.pid, signal.SIGKILL)
                self.proc.kill()
                self.proc.wait()
        if self.log:
            self.log.close()


def info(client, section):
    return dict(
        line.split(":", 1)
        for line in client.call("INFO", section).decode().splitlines()
        if ":" in line and not line.startswith("#")
    )


def slot(key):
    start = key.find(b"{")
    end = key.find(b"}", start + 1) if start >= 0 else -1
    if end > start + 1:
        key = key[start + 1 : end]
    return binascii.crc_hqx(key, 0) % 16384


def dense_tags(workers, count):
    tags, occupied = [[] for _ in range(workers)], set()
    candidate = 0
    while any(len(t) < count for t in tags):
        tag = str(candidate).encode()
        partition = slot(tag)
        owner = partition % workers
        if len(tags[owner]) < count and partition not in occupied:
            tags[owner].append(tag)
            occupied.add(partition)
        candidate += 1
    return [tag for group in tags for tag in group]


def rdb_length(size):
    if size < 64:
        return bytes([size])
    if size < 16384:
        return bytes([0x40 | (size >> 8), size & 255])
    return b"\x80" + size.to_bytes(4, "big")


def hash_dump(members, value_bytes):
    data = bytearray(b"\x04" + rdb_length(members))
    value = b"v" * value_bytes
    for i in range(members):
        field = f"field-{i:08d}".encode()
        data += rdb_length(len(field)) + field + rdb_length(len(value)) + value
    data += b"\x0b\x00"
    polynomial = int(f"{0xAD93D23594C935A9:064b}"[::-1], 2)
    table = []
    for byte in range(256):
        crc = byte
        for _ in range(8):
            crc = (crc >> 1) ^ (polynomial if crc & 1 else 0)
        table.append(crc)
    crc = 0
    for byte in data:
        crc = table[(crc ^ byte) & 255] ^ (crc >> 8)
    return bytes(data) + crc.to_bytes(8, "little")


def seed(args, source):
    client = source.client(timeout=args.timeout)
    tags = dense_tags(args.source_workers, args.hot_slots)
    items, distribution = [], collections.Counter()
    payload = (
        hash_dump(args.members, args.value_bytes)
        if args.workload == "large-hash"
        else b"v" * args.value_bytes
    )
    started = time.monotonic()
    try:
        for db in args.databases:
            assert client.call("SELECT", db) == b"OK"
            pending = []
            for i in range(args.keys):
                if args.workload == "dense":
                    key = b"seed:{" + tags[i % len(tags)] + b"}:" + str(i).encode()
                else:
                    key = f"seed:{i:09d}".encode()
                items.append(
                    (db, key, "hash" if args.workload == "large-hash" else "string")
                )
                distribution[f"{slot(key)}:{db}"] += 1
                pending.append(
                    ("RESTORE", key, 0, payload)
                    if args.workload == "large-hash"
                    else ("SET", key, payload)
                )
                if len(pending) >= min(128, max(1, 2 * 1024 * 1024 // len(payload))):
                    assert all(r == b"OK" for r in client.pipeline(pending))
                    pending.clear()
            if pending:
                assert all(r == b"OK" for r in client.pipeline(pending))
    finally:
        client.close()
    return items, {
        "seconds": time.monotonic() - started,
        "payload_bytes": len(payload) * len(items),
        "keys": len(items),
        "partition_db_key_counts": dict(distribution),
    }


def digest(server, items, timeout, batch_size):
    client = server.client(timeout=timeout)
    hasher, selected = hashlib.sha256(), None
    try:
        ordered = sorted(items)
        offset = 0
        while offset < len(ordered):
            db = ordered[offset][0]
            if db != selected:
                assert client.call("SELECT", db) == b"OK"
                selected = db
            batch = []
            while (
                offset < len(ordered)
                and ordered[offset][0] == db
                and len(batch) < batch_size
            ):
                batch.append(ordered[offset])
                offset += 1
            values = client.pipeline(
                [
                    ("HGETALL" if kind == "hash" else "GET", key)
                    for _, key, kind in batch
                ]
            )
            for (db, key, kind), value in zip(batch, values):
                if kind == "hash":
                    if not value:
                        raise AssertionError(f"missing hash {key!r}")
                    value = b"".join(
                        encode((k, v)) for k, v in sorted(zip(value[::2], value[1::2]))
                    )
                if value is None:
                    raise AssertionError(f"missing key {key!r}")
                hasher.update(encode((db, key, kind, value)))
        # Also reject unexpected keys. Every benchmark-created key is in items.
        for db in sorted({item[0] for item in items}):
            client.call("SELECT", db)
            expected = len({key for item_db, key, _ in items if item_db == db})
            if client.call("DBSIZE") != expected:
                raise AssertionError(f"unexpected database size in DB {db}")
    finally:
        client.close()
    return hasher.hexdigest()


def quantiles(samples):
    ordered = sorted(samples)
    if not ordered:
        return {"count": 0}
    return {
        "count": len(ordered),
        "max_ms": ordered[-1],
        **{
            name: ordered[min(len(ordered) - 1, math.ceil(q * len(ordered)) - 1)]
            for name, q in (("p50_ms", 0.5), ("p99_ms", 0.99), ("p999_ms", 0.999))
        },
    }


class Writer(threading.Thread):
    def __init__(self, source, args, seed_items):
        super().__init__(daemon=True)
        self.source, self.rate = source, args.write_rate
        self.mode, self.value_bytes = args.writer_mode, args.value_bytes
        self.seed_items = seed_items
        self.stop_event = threading.Event()
        self.samples, self.keys, self.errors = [], set(), []

    def run(self):
        client = self.source.client()
        due, index, selected = time.monotonic(), 0, 0
        pending = None
        try:
            while not self.stop_event.is_set():
                self.stop_event.wait(max(0, due - time.monotonic()))
                if self.stop_event.is_set():
                    break
                if self.mode == "transactional-set":
                    db, key, _ = self.seed_items[index % len(self.seed_items)]
                    if db != selected:
                        assert client.call("SELECT", db) == b"OK"
                        selected = db
                    value = (f"{index:016d}".encode() + b"w" * self.value_bytes)[
                        : self.value_bytes
                    ]
                else:
                    key = f"writer:{index % 1024}".encode()
                    value = f"{index:016d}".encode() + b"w" * 112
                started = time.monotonic()
                pending = {
                    "begin_monotonic": started,
                    "db": selected,
                    "key_hex": key.hex(),
                    "value_bytes": len(value),
                    "mode": self.mode,
                }
                if self.mode == "transactional-set":
                    assert client.pipeline(
                        [("MULTI",), ("SET", key, value), ("EXEC",)]
                    ) == [b"OK", b"QUEUED", [b"OK"]]
                else:
                    assert client.call("SET", key, value) == b"OK"
                    self.keys.add(key)
                ended = time.monotonic()
                self.samples.append(
                    {
                        **pending,
                        "end_monotonic": ended,
                        "latency_ms": (ended - started) * 1000,
                        "success": True,
                    }
                )
                pending = None
                index += 1
                # No accumulated catch-up burst after a blocked write.
                due = max(due + 1 / self.rate, ended)
        except Exception as error:
            self.errors.append(repr(error))
            if pending is not None:
                ended = time.monotonic()
                self.samples.append(
                    {
                        **pending,
                        "end_monotonic": ended,
                        "latency_ms": (ended - pending["begin_monotonic"]) * 1000,
                        "success": False,
                        "error": repr(error),
                    }
                )
        finally:
            client.close()

    def summary(self, start, end):
        # Join the writer before summarizing. Include the eventual latency of
        # every successful operation begun during the interval: omitting a
        # request that finishes just after ONLINE hides final-cut stalls.
        begun = [
            sample
            for sample in self.samples
            if start <= sample["begin_monotonic"] < end
        ]
        samples = [sample["latency_ms"] for sample in begun if sample["success"]]
        # Throughput is completion-based and has a separate population from
        # latency; a warmup request may finish inside the measured interval.
        completed = sum(
            sample["success"] and start <= sample["end_monotonic"] < end
            for sample in self.samples
        )
        return {
            **quantiles(samples),
            "completed_count": completed,
            "completed_qps": completed / (end - start),
            "begun_count": len(begun),
            "failed_begun_count": sum(not sample["success"] for sample in begun),
            "crossed_interval_end_count": sum(
                sample["end_monotonic"] >= end for sample in begun
            ),
            "latency_population": "successful operations begun in [start,end), including later completion",
            "requested_qps": self.rate,
            "mode": self.mode,
            "errors": self.errors,
        }

    def save_samples(self, path):
        with path.open("w") as output:
            for sample in self.samples:
                output.write(json.dumps(sample) + "\n")


class Sampler(threading.Thread):
    def __init__(self, servers, path, interval):
        super().__init__(daemon=True)
        self.servers, self.path, self.interval = servers, path, interval
        self.stop_event = threading.Event()
        self.peaks = collections.defaultdict(float)
        self.errors = []

    def run(self):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with self.path.open("w") as output:
            while not self.stop_event.is_set():
                sample = {"monotonic": time.monotonic()}
                for label, server in self.servers:
                    try:
                        stat = Path(f"/proc/{server.pid}/stat").read_text().split()
                        status = (
                            Path(f"/proc/{server.pid}/status").read_text().splitlines()
                        )
                        rss = (
                            int(
                                next(
                                    s.split()[1]
                                    for s in status
                                    if s.startswith("VmRSS:")
                                )
                            )
                            * 1024
                        )
                        values = {
                            "rss_bytes": rss,
                            "cpu_ticks": int(stat[13]) + int(stat[14]),
                        }
                        url = f"http://{server.host}:{server.metrics_port}/metrics"
                        with opener.open(url, timeout=2) as response:
                            for line in response.read().decode().splitlines():
                                if line.startswith(
                                    (
                                        "lavik_fullsync_",
                                        "lavik_worker_retained_memory_bytes",
                                        "lavik_replication_backlog_",
                                        "lavik_replication_publish_queue_",
                                    )
                                ):
                                    key, value = line.rsplit(" ", 1)
                                    values[key] = float(value)
                        sample[label] = values
                        for key, value in values.items():
                            self.peaks[f"{label}.{key}"] = max(
                                self.peaks[f"{label}.{key}"], value
                            )
                    except Exception as error:
                        self.errors.append(f"{label}: {error!r}")
                output.write(json.dumps(sample) + "\n")
                output.flush()
                self.stop_event.wait(self.interval)


class Capture:
    def __init__(self, network, source, directory):
        self.network, self.source, self.directory = network, source, directory
        self.proc, self.log = None, None

    def start(self):
        self.log = (self.directory / "tcpdump.log").open("wb")
        # Run as the invoking user after opening the interface, preserving
        # normal artifact ownership. Capture only native source output.
        user = command(["id", "-un"])
        self.proc = subprocess.Popen(
            [
                "sudo",
                "-n",
                "tcpdump",
                "-i",
                self.network.host_if,
                "-n",
                "-s",
                "0",
                "-B",
                "16384",
                "--immediate-mode",
                "-U",
                "-Z",
                user,
                "-w",
                str(self.directory / "frames.pcap"),
                "src",
                "host",
                self.network.source_ip,
                "and",
                "src",
                "port",
                str(self.source.port),
            ],
            stdout=self.log,
            stderr=self.log,
        )
        time.sleep(0.15)
        if self.proc.poll() is not None:
            raise RuntimeError("tcpdump failed; see tcpdump.log")

    def stop(self):
        if self.proc is not None and self.proc.poll() is None:
            # sudo forwards the signal to tcpdump. SIGINT flushes its pcap and
            # emits drop statistics; do not silently treat drops as full counts.
            privileged("kill", "-INT", str(self.proc.pid))
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                privileged("kill", "-KILL", str(self.proc.pid))
                self.proc.wait()
        if self.log:
            self.log.close()


class FrameStream:
    def __init__(self):
        self.next_sequence = None
        self.pending = {}
        self.buffer = bytearray()
        self.found = False
        self.counts, self.bytes = collections.Counter(), collections.Counter()
        self.partition_db = collections.defaultdict(
            lambda: {"frames": 0, "bytes": 0, "records": 0}
        )
        self.record_kinds = collections.Counter()
        self.large_value_begins = collections.Counter()

    def append(self, sequence, data):
        if self.next_sequence is None:
            self.next_sequence = sequence
        delta = (sequence - self.next_sequence + (1 << 31)) % (1 << 32) - (1 << 31)
        if delta > 0:
            self.pending[sequence] = data
            return
        if -delta >= len(data):
            return
        data = data[-delta:]
        self.next_sequence = (self.next_sequence + len(data)) % (1 << 32)
        self.buffer.extend(data)
        self.parse()
        while self.next_sequence in self.pending:
            sequence = self.next_sequence
            data = self.pending.pop(sequence)
            self.append(sequence, data)

    def parse(self):
        if not self.found:
            where = self.buffer.find(b"LVF1")
            if where < 0:
                self.buffer[:] = self.buffer[-3:]
                return
            del self.buffer[:where]
            self.found = True
        while len(self.buffer) >= 16:
            magic, version, kind, header, size, _ = struct.unpack_from(
                "<IBBHII", self.buffer
            )
            if (
                magic != 0x3146564C
                or version != 1
                or header != 16
                or size > 12 * 1024 * 1024
            ):
                raise ValueError("invalid captured native frame; capture is incomplete")
            if len(self.buffer) < 16 + size:
                return
            payload = bytes(self.buffer[16 : 16 + size])
            del self.buffer[: 16 + size]
            self.counts[kind] += 1
            self.bytes[kind] += size + 16
            if kind == 2:
                partition, count = struct.unpack_from("<HI", payload, 8)
                offset, frame_dbs = 14, collections.Counter()
                for _ in range(count):
                    record_kind, db, _, _, _, _, _, _, _, key_size, value_size = (
                        struct.unpack_from("<BBBQQQQIIII", payload, offset)
                    )
                    size_record = 51 + key_size + value_size
                    self.record_kinds[f"{db}:{record_kind}"] += 1
                    if record_kind == 3:
                        key = payload[offset + 51 : offset + 51 + key_size]
                        self.large_value_begins[f"{db}:{key.hex()}"] += 1
                    group = self.partition_db[f"{partition}:{db}"]
                    group["records"] += 1
                    group["bytes"] += size_record
                    frame_dbs[db] += 1
                    offset += size_record
                if offset != len(payload):
                    raise ValueError("captured records did not consume their frame")
                for db in frame_dbs:
                    self.partition_db[f"{partition}:{db}"]["frames"] += 1


def parse_capture(path):
    streams = {}
    with path.open("rb") as source:
        global_header = source.read(24)
        if global_header[:4] not in (b"\xd4\xc3\xb2\xa1", b"\x4d\x3c\xb2\xa1"):
            raise ValueError("capture requires little-endian pcap")
        if struct.unpack_from("<I", global_header, 20)[0] != 1:
            raise ValueError("capture requires Ethernet link type")
        while header := source.read(16):
            if len(header) != 16:
                raise ValueError("truncated packet header")
            size = struct.unpack_from("<I", header, 8)[0]
            packet = source.read(size)
            if len(packet) != size:
                raise ValueError("truncated packet")
            if len(packet) < 54 or packet[12:14] != b"\x08\x00" or packet[23] != 6:
                continue
            ip_header = (packet[14] & 15) * 4
            ip_size = struct.unpack_from("!H", packet, 16)[0]
            tcp = 14 + ip_header
            tcp_header = (packet[tcp + 12] >> 4) * 4
            sport, dport, sequence = struct.unpack_from("!HHI", packet, tcp)
            flags = packet[tcp + 13]
            identity = (
                socket.inet_ntoa(packet[26:30]),
                sport,
                socket.inet_ntoa(packet[30:34]),
                dport,
            )
            stream = streams.setdefault(identity, FrameStream())
            if flags & 2:  # SYN consumes one TCP sequence number.
                stream.next_sequence = (sequence + 1) % (1 << 32)
                sequence = (sequence + 1) % (1 << 32)
            payload = packet[tcp + tcp_header : 14 + ip_size]
            if payload:
                stream.append(sequence, payload)
    return {
        str(key): {
            "frame_counts": dict(s.counts),
            "wire_bytes": dict(s.bytes),
            "partition_db": dict(s.partition_db),
            "record_kinds_db_kind": dict(s.record_kinds),
            "large_value_begins_db_key_hex": dict(s.large_value_begins),
            "unparsed_bytes": len(s.buffer) if s.found else 0,
            "tcp_gap_segments": len(s.pending),
        }
        for key, s in streams.items()
        if s.found
    }


def run(args):
    directory = Path(args.run_dir).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    args.binary = str(Path(args.binary).resolve())
    with open(args.binary, "rb") as binary:
        binary_sha = hashlib.file_digest(binary, "sha256").hexdigest()
    if args.client_cpus:
        os.sched_setaffinity(0, cpus(args.client_cpus))
    result = {
        "schema_version": 1,
        "arguments": vars(args),
        "binary_sha256": binary_sha,
        "binary_version": command([args.binary, "--version"]),
        "host": {
            "kernel": platform.release(),
            "machine": platform.machine(),
            "cpu_model": next(
                (
                    line.split(":", 1)[1].strip()
                    for line in Path("/proc/cpuinfo").read_text().splitlines()
                    if line.startswith("model name")
                ),
                None,
            ),
            "client_affinity": sorted(os.sched_getaffinity(0)),
            "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
        },
        "network": {"type": "dedicated-veth-netem", "added_rtt_ms": args.rtt_ms},
        "verified": False,
    }
    network, source, target, capture, sampler, writer = (
        Network(args.rtt_ms),
        None,
        None,
        None,
        None,
        None,
    )
    try:
        network.start()
        result["network"].update(
            {
                "namespace": network.name,
                "source_ip": network.source_ip,
                "target_ip": network.target_ip,
            }
        )
        source = Server(args, directory / "source", network)
        target = Server(args, directory / "target", network, target=True)
        source.start()
        target.start()
        result["server_commands"] = {"source": source.argv, "target": target.argv}
        probe = target.client()
        rtts = []
        for _ in range(20):
            started = time.monotonic()
            assert probe.call("PING") == b"PONG"
            rtts.append((time.monotonic() - started) * 1000)
        probe.close()
        result["network"]["target_ping_ms"] = quantiles(rtts)
        items, result["seed"] = seed(args, source)
        sampler = Sampler(
            (("source", source), ("target", target)),
            directory / "samples.jsonl",
            args.sample_interval,
        )
        sampler.start()
        if args.write_rate:
            writer = Writer(source, args, items)
            writer.start()
        warmup_start = time.monotonic()
        time.sleep(args.writer_warmup)
        if args.capture:
            capture = Capture(network, source, directory)
            capture.start()
        client = target.client(timeout=args.timeout)
        started = time.monotonic()
        try:
            assert (
                client.call("LAVIK.REPLICAOF", network.source_ip, source.port) == b"OK"
            )
            deadline = started + args.timeout
            while time.monotonic() < deadline:
                observed = info(client, "replication")
                if observed.get("lavik_replication_state") == "online":
                    break
                if source.proc.poll() is not None or target.proc.poll() is not None:
                    raise RuntimeError("server exited during FULL")
                time.sleep(args.poll_interval)
            else:
                raise TimeoutError(f"FULL timeout; last INFO: {observed}")
            finished = time.monotonic()
            result["full"] = {
                "seconds": finished - started,
                "start_monotonic": started,
                "end_monotonic": finished,
                "final_replication_info": observed,
                "seed_payload_mib_per_second": result["seed"]["payload_bytes"]
                / (1024 * 1024 * (finished - started)),
            }
        finally:
            client.close()
        if writer:
            writer.stop_event.set()
            writer.join(40)
            if writer.is_alive() or writer.errors:
                raise RuntimeError(f"writer failed or did not stop: {writer.errors}")
            result["writer"] = {
                "before_full": writer.summary(warmup_start, started),
                "during_full": writer.summary(started, finished),
            }
            items.extend((0, key, "string") for key in writer.keys)
        sampler.stop_event.set()
        sampler.join(10)
        result["metric_peaks"] = dict(sampler.peaks)
        result["metric_sample_errors"] = sampler.errors
        if capture:
            # Let the capture reader consume the final kernel packet batch.
            # This wait is after the measured ONLINE timestamp.
            time.sleep(0.2)
            capture.stop()
            capture = None
            result["frames"] = parse_capture(directory / "frames.pcap")
            result["tcpdump_statistics"] = (directory / "tcpdump.log").read_text()
            full_flows = [
                flow
                for flow in result["frames"].values()
                if flow["frame_counts"].get(7) == 1
            ]
            result["capture_complete"] = (
                len(full_flows) == args.source_workers
                and all(
                    not flow["tcp_gap_segments"] and not flow["unparsed_bytes"]
                    for flow in full_flows
                )
                and "\n0 packets dropped by kernel" in result["tcpdump_statistics"]
            )
            if not result["capture_complete"]:
                raise AssertionError(
                    "packet capture is incomplete; frame counts cannot be used"
                )
        # A fence in each source-worker flow follows all completed writes on
        # that flow. One key or cluster readiness alone cannot prove all-flow
        # catchup after the last foreground write.
        fences = [
            b"benchmark:{" + tag + b"}:completion"
            for tag in dense_tags(args.source_workers, 1)
        ]
        client = source.client(timeout=args.timeout)
        assert all(
            reply == b"OK"
            for reply in client.pipeline([("SET", key, "verified") for key in fences])
        )
        client.close()
        items.extend((0, key, "string") for key in fences)
        client = target.client(timeout=args.timeout)
        try:
            deadline = time.monotonic() + args.timeout
            while any(
                value != b"verified"
                for value in client.pipeline([("GET", key) for key in fences])
            ):
                if time.monotonic() > deadline:
                    raise TimeoutError("post-FULL continuation did not become readable")
                time.sleep(0.01)
        finally:
            client.close()
        estimated_value_bytes = args.value_bytes * (
            args.members if args.workload == "large-hash" else 1
        )
        digest_batch = max(1, min(128, 8 * 1024 * 1024 // estimated_value_bytes))
        result["source_digest"] = digest(source, items, args.timeout, digest_batch)
        result["target_digest"] = digest(target, items, args.timeout, digest_batch)
        if result["source_digest"] != result["target_digest"]:
            raise AssertionError("source and target data digests differ")
        result["verified"] = True
    except BaseException as error:
        result["error"] = repr(error)
        raise
    finally:
        cleanup_errors = []
        if writer:
            writer.stop_event.set()
            writer.join(40)
            try:
                writer.save_samples(directory / "writer-samples.jsonl")
                result["writer_samples_file"] = "writer-samples.jsonl"
            except Exception as error:
                cleanup_errors.append(f"writer samples: {error!r}")
        if sampler:
            sampler.stop_event.set()
            sampler.join(10)
        # A failed process shutdown must not prevent namespace cleanup or hide
        # the original failure evidence. Every cleanup target belongs to this run.
        for owner in (capture, target, source, network):
            if owner is not None:
                try:
                    owner.stop()
                except Exception as error:
                    cleanup_errors.append(f"{type(owner).__name__}: {error!r}")
        result["cleanup_errors"] = cleanup_errors
        if result["verified"] and not cleanup_errors and not args.keep_data:
            for server in (source, target):
                (server.directory / "data").unlink()
            result["data_files_removed"] = True
        (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        if cleanup_errors and "error" not in result:
            raise RuntimeError(f"benchmark cleanup failed: {cleanup_errors}")
    print(
        json.dumps(
            {
                "result": str(directory / "result.json"),
                "verified": result["verified"],
                "full_seconds": result["full"]["seconds"],
            }
        )
    )


def arguments():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--binary", required=True)
    parser.add_argument(
        "--revision",
        required=True,
        help="exact Lavik source commit for the supplied binary",
    )
    parser.add_argument(
        "--runtime-revision", default="unspecified", help="exact Bycorf commit"
    )
    parser.add_argument(
        "--build-description",
        default="unspecified",
        help="compiler, build type and CMake options",
    )
    parser.add_argument("--label", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument(
        "--workload",
        choices=("uniform", "dense", "large-string", "large-hash"),
        default="uniform",
    )
    parser.add_argument("--keys", type=int, default=4096, help="keys per database")
    parser.add_argument(
        "--value-bytes",
        type=int,
        default=1024,
        help="String bytes, or bytes per Hash field value",
    )
    parser.add_argument(
        "--members", type=int, default=32768, help="fields in each large-hash"
    )
    parser.add_argument(
        "--hot-slots", type=int, default=1, help="dense partitions per source worker"
    )
    parser.add_argument(
        "--databases", type=lambda s: [int(v) for v in s.split(",")], default=[0]
    )
    parser.add_argument("--source-workers", type=int, default=1)
    parser.add_argument("--target-workers", type=int, default=1)
    parser.add_argument("--source-cpus", default="")
    parser.add_argument("--target-cpus", default="")
    parser.add_argument("--client-cpus", default="")
    parser.add_argument("--rtt-ms", type=float, default=0)
    parser.add_argument(
        "--write-rate",
        type=float,
        default=0,
        help="one foreground client, fixed maximum SET QPS",
    )
    parser.add_argument(
        "--writer-mode",
        choices=("separate-keys", "transactional-set"),
        default="separate-keys",
        help="transactional-set rotates existing seeded String keys at their original value size",
    )
    parser.add_argument("--writer-warmup", type=float, default=2)
    parser.add_argument("--data-file-mib", type=int, default=2048)
    parser.add_argument("--max-memory-mib", type=int, default=2048)
    parser.add_argument("--backlog-mib", type=int, default=64)
    parser.add_argument("--queue-mib", type=int, default=16)
    parser.add_argument("--flush-ms", type=int, default=100)
    parser.add_argument("--sample-interval", type=float, default=0.5)
    parser.add_argument("--poll-interval", type=float, default=0.02)
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--capture", action="store_true")
    parser.add_argument(
        "--keep-data",
        action="store_true",
        help="retain successful runs' data files; failures always retain them",
    )
    args = parser.parse_args()
    if (
        min(
            args.keys,
            args.value_bytes,
            args.members,
            args.source_workers,
            args.target_workers,
            args.hot_slots,
            args.sample_interval,
            args.poll_interval,
            args.timeout,
        )
        <= 0
    ):
        parser.error("counts, sizes, sampling periods and timeout must be positive")
    if args.rtt_ms < 0 or args.write_rate < 0 or args.writer_warmup < 0:
        parser.error("RTT, write rate and warmup must not be negative")
    if args.writer_mode == "transactional-set" and args.workload == "large-hash":
        parser.error("transactional-set requires a String seed workload")
    if (
        not args.databases
        or any(db not in range(16) for db in args.databases)
        or len(set(args.databases)) != len(args.databases)
    ):
        parser.error("databases must be distinct integers in 0..15")
    return args


if __name__ == "__main__":
    run(arguments())
