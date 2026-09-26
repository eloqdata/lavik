#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Pinned RedisShake sync_reader -> ordinary writes on Meta-managed Lavik."""

from contextlib import contextmanager, ExitStack
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

import redis

sys.path.insert(0, str(Path(__file__).parent / "meta_integration"))
import gate_cluster_create as C
import harness as H
from gate_data_control import DataProcess
import redis_follower_smoke as S

PASSWORD = "migration-test-secret"


def client(port, db=0, protocol=2):
    return redis.Redis(
        host="127.0.0.1",
        port=port,
        db=db,
        protocol=protocol,
        password=PASSWORD,
        socket_timeout=10,
        socket_connect_timeout=2,
    )


@contextmanager
def source(root, cluster):
    """Use three source primaries and two target Groups to test re-routing."""
    with ExitStack() as stack:
        ports, clients = [], []
        for i in range(3 if cluster else 1):
            port = H.free_port()
            while port > 55000:
                port = H.free_port()
            extra = ["--requirepass", PASSWORD, "--repl-ping-replica-period", "1"]
            if cluster:
                extra += ["--cluster-enabled", "yes", "--cluster-node-timeout", "5000"]
            _, port, _ = stack.enter_context(
                S.process(
                    REDIS,
                    root / f"redis-{i}",
                    "redis",
                    redis=True,
                    port=port,
                    extra=extra,
                    password=PASSWORD,
                )
            )
            ports.append(port)
            connection = client(port)
            stack.callback(connection.close)
            clients.append(connection)
        if cluster:
            subprocess.run(
                [
                    CLI,
                    "--cluster",
                    "create",
                    *[f"127.0.0.1:{p}" for p in ports],
                    "--cluster-yes",
                ],
                env={**os.environ, "REDISCLI_AUTH": PASSWORD},
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=30,
            )
            H.wait_until(
                "source cluster ready",
                20,
                lambda: all(
                    c.cluster("INFO")["cluster_state"] == "ok" for c in clients
                ),
            )
        yield ports, clients


@contextmanager
def target(root, mode, hold_baseline=False):
    directory = root / "target"
    directory.mkdir()
    meta = H.Node(META, str(directory), 1, args=C.creation_raft_args())
    count = 2 if mode == "cluster" else 1
    nodes = [
        DataProcess(
            LAVIK,
            str(directory / f"data-{i}"),
            f"{i + 1:040x}",
            meta.data_control_endpoint,
            workers=2,
            environment={
                **os.environ,
                **(
                    {"LAVIK_COMMAND_PAUSE_BEFORE_DB_ADMISSION_MS": "8000"}
                    if hold_baseline and i % 2 == 0
                    else {}
                ),
            },
            extra_args=("--requirepass", PASSWORD, "--masterauth", PASSWORD),
        )
        for i in range(count * 2)
    ]
    lines = [
        "schema_version = 1",
        f'client_mode = "{mode}"',
        'slot_strategy = "contiguous-even"',
        *C.meta_manifest_lines(meta),
    ]
    for node in nodes:
        lines += [
            "[[data_nodes]]",
            f'id = "{node.node_id}"',
            f'client_endpoint = "{node.advertised_endpoint}"',
        ]
    for i in range(count):
        lines += [
            "[[groups]]",
            f'id = "import-{i}"',
            f'primary = "{nodes[2 * i].node_id}"',
            f'replicas = ["{nodes[2 * i + 1].node_id}"]',
        ]
    manifest = directory / "cluster.toml"
    manifest.write_text("\n".join(lines) + "\n")
    try:
        meta.start(initial_cluster_manifest=str(manifest))
        meta.wait_leader()
        for node in nodes:
            node.start()
        H.wait_until(
            "Meta membership stable",
            30,
            lambda: C.cluster_status(meta)["meta_membership_stable"],
        )
        C.command(
            os.environ.copy(),
            [
                CTL,
                "cluster-create",
                "--manifest",
                str(manifest),
                "--socket",
                meta.ctl_path,
                "--yes",
            ],
        )
        C.wait_cluster_ready(meta, "import target ready", 60)
        yield meta, nodes
    except BaseException:
        H.dump_node_logs([meta])
        for node in nodes:
            print(node.log_tail(lines=70), file=sys.stderr)
        raise
    finally:
        for node in nodes:
            node.force_kill()
        meta.force_kill()


class Shake:
    def __init__(
        self,
        root,
        source_port,
        target_port,
        source_cluster,
        mode,
        bulk=512 * 1024 * 1024,
    ):
        root.mkdir()
        self.root = root
        self.started = time.monotonic()
        self.port = H.free_port()
        config = root / "shake.toml"
        config.write_text(f'''[sync_reader]
address = "127.0.0.1:{source_port}"
password = "{PASSWORD}"
cluster = {str(source_cluster).lower()}
prefer_replica = false
sync_rdb = true
sync_aof = true

[redis_writer]
address = "127.0.0.1:{target_port}"
password = "{PASSWORD}"
cluster = {str(mode == "cluster").lower()}

[advanced]
dir = "{root / "spool"}"
log_file = "{root / "shake.log"}"
status_port = {self.port}
log_interval = 1
rdb_restore_command_behavior = "panic"
target_redis_proto_max_bulk_len = {bulk}
''')
        self.log = (root / "process.log").open("w")
        self.proc = subprocess.Popen(
            [SHAKE, str(config)], cwd=root, stdout=self.log, stderr=subprocess.STDOUT
        )

    def status(self):
        assert self.proc.poll() is None, (self.root / "process.log").read_text()
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{self.port}/status", timeout=2
            ) as response:
                return json.load(response)
        except (OSError, ValueError):
            return None

    def close(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log.close()


def library(value):
    return f"#!lua name=imported\nredis.register_function('imported_value', function() return '{value}' end)"


def seed(src, prefix, streams=True):
    src.set(prefix + b"cutover", "stopped")
    src.set(prefix + b"binary\x00\xff", b"value\x00\xfe\r\n")
    src.set(prefix + b"large", b"x" * (2 * 1024 * 1024))
    src.rpush(prefix + b"list", b"first", b"second\x00")
    src.hset(prefix + b"hash", mapping={b"field\xff": b"value\x00", b"other": b"42"})
    src.sadd(prefix + b"set", b"one", b"two\x00")
    src.zadd(prefix + b"zset", {b"one": 1.25, b"two\x00": -2})
    # Force ordinary (non-listpack/intset) representations as well.
    src.hset(prefix + b"wide-hash", mapping={f"f{i}": "h" * 96 for i in range(520)})
    src.sadd(prefix + b"wide-set", *[f"m{i}" for i in range(520)])
    src.zadd(prefix + b"wide-zset", {f"m{i}": i / 2 for i in range(160)})
    src.rpush(prefix + b"wide-list", *[f"entry-{i}" * 20 for i in range(200)])
    src.set(prefix + b"ttl", "expires", px=120000)
    src.set(prefix + b"delete", "gone-later")
    src.set(prefix + b"rename", "renamed-value")
    if streams:
        src.xadd(prefix + b"stream", {b"field": b"first"}, id="1-0")
        src.xadd(prefix + b"stream", {b"field": b"second"}, id="2-0")
        src.xgroup_create(prefix + b"stream", "group", "0")
        src.xreadgroup("group", "consumer", {prefix + b"stream": ">"}, count=1)
        # Redis 7.2 propagates XREADGROUP as XCLAIM, which does not carry the
        # entries-read counter even to a Redis replica. Explicit SETID carries
        # it, allowing exact metadata comparison for both RDB and AOF fixtures.
        src.xgroup_setid(prefix + b"stream", "group", "1-0", entries_read=1)


def catalog(connection):
    libraries = connection.execute_command("FUNCTION", "LIST", "WITHCODE")
    result = []
    for row in libraries:
        # Redis 7.2 sends alternating fields and values in RESP2, but maps in
        # RESP3. Compare the catalog independently of the client's protocol.
        if not isinstance(row, dict):
            assert len(row) % 2 == 0, row
            row = dict(zip(row[::2], row[1::2]))
        result.append((row[b"library_name"], row[b"library_code"]))
    return sorted(result)


def snapshot(connection):
    result = {}
    for key in connection.scan_iter(count=100):
        kind = connection.type(key)
        if kind == b"string":
            value = connection.get(key)
        elif kind == b"list":
            value = connection.lrange(key, 0, -1)
        elif kind == b"hash":
            value = connection.hgetall(key)
        elif kind == b"set":
            value = connection.smembers(key)
        elif kind == b"zset":
            value = connection.zrange(key, 0, -1, withscores=True)
        elif kind == b"stream":
            groups = connection.xinfo_groups(key)
            pending = []
            for group in groups:
                pending += [
                    (p["message_id"], p["consumer"], p["times_delivered"])
                    for p in connection.xpending_range(
                        key, group["name"], "-", "+", 100
                    )
                ]
            info = connection.xinfo_stream(key)
            value = (
                connection.xrange(key),
                groups,
                pending,
                {
                    k: info[k]
                    for k in (
                        "last-generated-id",
                        "entries-added",
                        "max-deleted-entry-id",
                    )
                },
            )
        else:
            raise AssertionError(f"unsupported source type {kind!r} for {key!r}")
        result[key] = (kind, value, connection.pexpiretime(key))
    return result


def equal_dataset(expected, actual, ttl_slack_ms):
    assert actual.keys() == expected.keys(), (
        expected.keys() - actual.keys(),
        actual.keys() - expected.keys(),
    )
    for key, (kind, value, expiry) in expected.items():
        got_kind, got_value, got_expiry = actual[key]
        assert (got_kind, got_value) == (kind, value), (
            key,
            kind,
            value if kind == b"stream" else "value differs",
            got_value if kind == b"stream" else "value differs",
        )
        # RedisShake emits relative TTLs during RDB import; allow the bounded
        # import/queue delay, while requiring identical persistent/expiring state.
        assert (
            got_expiry == expiry
            if expiry < 0
            else -100 <= got_expiry - expiry <= ttl_slack_ms
        ), (key, expiry, got_expiry)


def drained(shake, offsets):
    status = shake.status()
    if not status or not status.get("reader"):
        return False
    readers = status["reader"]
    if not isinstance(readers, list):
        readers = [readers]
    writers = status["writer"]
    if not isinstance(writers, list):
        writers = [writers]
    return (
        len(readers) == len(offsets)
        and all(
            r["status"] == "syncing aof"
            and r["aof_sent_offset"] >= offsets[r["address"]]
            and r["aof_received_offset"] >= offsets[r["address"]]
            for r in readers
        )
        and all(
            w["unanswered_entries"] == 0 and w["unanswered_bytes"] == 0 for w in writers
        )
        and status["consistent"]
    )


def verify_targets(nodes, mode, databases, expected, expected_catalog, ttl_slack_ms):
    for db in databases:
        primary_data, replica_data = {}, {}
        for index in range(0, len(nodes), 2):
            with (
                client(nodes[index].redis_port, db) as writer,
                client(nodes[index + 1].redis_port, db) as reader,
            ):
                if mode == "cluster":
                    reader.execute_command("READONLY")
                current = snapshot(writer)
                # Same-connection write then WAIT fences all publisher flows;
                # a fresh connection's bare WAIT cannot prove migration drain.
                key = next((k for k, v in current.items() if v[0] == b"string"), None)
                with writer.pipeline(transaction=False) as pipeline:
                    if key is not None:
                        pipeline.set(key, current[key][1], keepttl=True)
                    else:
                        # Empty DBs still need a publisher fence for the FLUSH.
                        first = index * 16384 // len(nodes)
                        last = (index + 2) * 16384 // len(nodes) - 1
                        barrier = C.key_in_range("empty-import-barrier", first, last)
                        pipeline.set(barrier, "barrier")
                        pipeline.delete(barrier)
                    pipeline.execute_command("WAIT", 1, 10000)
                    assert pipeline.execute() == (
                        [True, 1] if key is not None else [True, 1, 1]
                    )
                assert not primary_data.keys() & current.keys()
                primary_data.update(current)
                replica_data.update(snapshot(reader))
                assert catalog(writer) == expected_catalog
                assert catalog(reader) == expected_catalog
        equal_dataset(expected[db], primary_data, ttl_slack_ms)
        equal_dataset(expected[db], replica_data, ttl_slack_ms)


def cutover(nodes, mode, expected):
    driver = os.environ["LAVIK_IMPORT_GO_CLIENT"]
    keys = [key for key in expected if key.endswith(b"cutover")]
    replica_error = (
        "WAIT cannot be used with replica instances. Please also note that "
        "since Redis 4.0 if a replica is configured to be writable (which is "
        "not the default) writes to replicas are just local and are not "
        "propagated."
    )
    for protocol in (2, 3):
        options = dict(
            host="127.0.0.1",
            port=nodes[0].redis_port,
            password=PASSWORD,
            protocol=protocol,
            socket_timeout=10,
        )
        consumer = (
            redis.RedisCluster(**options)
            if mode == "cluster"
            else redis.Redis(**options)
        )
        with consumer:
            # Both RESP versions retain Redis's integer and error reply types.
            assert consumer.execute_command("WAIT", -1, 1) == 1
            try:
                consumer.execute_command("WAIT", 1, -1)
                raise AssertionError("negative WAIT timeout returned success")
            except redis.ResponseError as error:
                assert str(error) == "timeout is negative", error
            try:
                consumer.execute_command("WAIT", 0, 9223372036854775807)
                raise AssertionError("overflowing WAIT timeout returned success")
            except redis.ResponseError as error:
                assert str(error) == "timeout is out of range", error
            with client(nodes[1].redis_port, protocol=protocol) as replica:
                if mode == "cluster":
                    replica.execute_command("READONLY")
                try:
                    replica.execute_command("WAIT", "invalid", 1)
                    raise AssertionError("replica accepted WAIT")
                except redis.ResponseError as error:
                    assert str(error) == replica_error, error
            for key in keys:
                # Each client reads the previous client's value and can then
                # perform ordinary application writes on the verified target.
                assert consumer.get(key) == (
                    b"stopped" if protocol == 2 else b"after-cutover"
                )
                assert consumer.set(key, "python-cutover")
                assert consumer.get(key) == b"python-cutover"
                result = subprocess.run(
                    [
                        driver,
                        "--address",
                        f"127.0.0.1:{nodes[0].redis_port}",
                        f"--cluster={str(mode == 'cluster').lower()}",
                        "--password",
                        PASSWORD,
                        "--protocol",
                        str(protocol),
                        "--key",
                        key.decode(),
                        "--value",
                        "python-cutover",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=15,
                )
                assert result.returncode == 0, result.stdout + result.stderr


def checkpoint(shake, ports, sources, prefixes, nodes, mode, databases):
    offsets = {}
    for i, src in enumerate(sources):
        src.set(prefixes[i] + b"tail", "stopped")
        offsets[f"127.0.0.1:{ports[i]}"] = src.info("replication")["master_repl_offset"]
    H.wait_until(
        "every source tail and writer replies drained",
        30,
        lambda: drained(shake, offsets),
    )
    expected = {}
    for db in databases:
        expected[db] = {}
        for port in ports:
            with client(port, db) as selected:
                expected[db].update(snapshot(selected))
    verify_targets(
        nodes,
        mode,
        databases,
        expected,
        catalog(sources[0]),
        int((time.monotonic() - shake.started) * 1000) + 100,
    )
    return expected


def failed_import(root, mode, phase, failure):
    """Uncertain writes require a new empty target baseline, never AOF retries."""
    root.mkdir()
    with (
        source(root, False) as (ports, sources),
        target(root, mode, hold_baseline=phase == "full") as (meta, nodes),
    ):
        src = sources[0]
        prefix = (C.key_in_range("fault-import", 0, 8191) + ":").encode()
        other = (C.key_in_range("fault-other", 8192, 16383) + ":").encode()
        for tag in (prefix, other):
            seed(src, tag)
        src.execute_command("FUNCTION", "LOAD", library("baseline"))
        shake = Shake(root / "interrupted", ports[0], nodes[0].redis_port, False, mode)
        try:
            if phase == "full":
                H.wait_until(
                    "RESTORE in flight",
                    30,
                    lambda: "client command admitted; pausing before database admission"
                    in Path(nodes[0].log_path).read_text(),
                )
                # The source snapshot was already parsed. These writes genuinely
                # overlap destination baseline loading and exercise the AOF tail.
                src.incr(prefix + b"counter")
            else:
                checkpoint(shake, ports, sources, [prefix], nodes, mode, (0,))
                src.incr(prefix + b"counter")
                checkpoint(shake, ports, sources, [prefix], nodes, mode, (0,))
            if failure == "tool":
                shake.proc.kill()
            elif failure == "connection":
                with client(nodes[0].redis_port) as connection:
                    assert (
                        connection.execute_command(
                            "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes"
                        )
                        >= 1
                    )
            else:
                assert meta.put_automatic_uncontrolled_failover_policy(
                    2, suspect_after_ms=1000
                ).startswith("OK")
                old, promoted = nodes[0:2]
                old.force_kill()
                H.wait_until(
                    "automatic target promotion",
                    40,
                    lambda: any(
                        g["group_id"] == "import-0"
                        and g.get("owner_node_id") == promoted.node_id
                        and g.get("serving_ready")
                        for g in C.cluster_status(meta)["groups"]
                    ),
                )
                # Single writer sockets die; Cluster writers also fail explicitly
                # on a changed route instead of replaying ambiguous commands.
                old.environment = os.environ.copy()
                old.start()
                nodes[0], nodes[1] = promoted, old
                C.wait_cluster_ready(meta, "recovered target replica", 60)
            # Force activity when failure raced an otherwise idle AOF stream.
            src.incr(prefix + b"counter")
            assert shake.proc.wait(timeout=20) != 0
        finally:
            shake.close()
        # Kill/EOF is not a statement that all admitted writes were rolled back.
        # Wait for retired request coroutines before resetting the isolated target.
        for node in nodes[::2]:
            with client(node.redis_port) as connection:
                H.wait_until(
                    "old writer drained",
                    20,
                    lambda: connection.info("clients")["connected_clients"] == 1,
                )
                connection.execute_command("FLUSHALL")
                connection.execute_command("FUNCTION", "FLUSH")
                assert connection.execute_command("WAIT", 1, 15000) == 1
        src.delete(prefix + b"delete")
        src.incr(prefix + b"counter")
        retry = Shake(
            root / "fresh-baseline", ports[0], nodes[0].redis_port, False, mode
        )
        try:
            expected = checkpoint(retry, ports, sources, [prefix], nodes, mode, (0,))
            retry.close()
            cutover(nodes, mode, expected[0])
        finally:
            retry.close()


def migrate(root, mode, clustered, fallback=False):
    root.mkdir()
    overlap_baseline = (
        mode == "single"
        and not clustered
        and not fallback
        and os.environ.get("LAVIK_TEST_FAULTS_AVAILABLE", "1") == "1"
    )
    with (
        source(root, clustered) as (ports, sources),
        target(root, mode, hold_baseline=overlap_baseline) as (_, nodes),
    ):
        databases = (0, 15) if mode == "single" and not clustered else (0,)
        prefixes = []
        for i, src in enumerate(sources):
            first = i * 16384 // len(sources) + 64
            last = (i + 1) * 16384 // len(sources) - 65
            if not clustered:
                last = 8000
            prefix = (C.key_in_range(f"import-{i}", first, last) + ":").encode()
            prefixes.append(prefix)
            src.execute_command("FUNCTION", "LOAD", library("baseline"))
            for db in databases:
                with client(ports[i], db) as selected:
                    seed(selected, prefix, streams=not fallback)
                    if not clustered:
                        other = (
                            C.key_in_range("import-other", 8192, 16383) + ":"
                        ).encode()
                        seed(selected, other, streams=not fallback)
        shake = Shake(
            root / "shake",
            ports[0],
            nodes[0].redis_port,
            clustered,
            mode,
            bulk=1024 if fallback else 512 * 1024 * 1024,
        )
        try:
            if overlap_baseline:
                H.wait_until(
                    "baseline RESTORE in flight",
                    30,
                    lambda: "client command admitted; pausing before database admission"
                    in Path(nodes[0].log_path).read_text(),
                )
            # In the held case the RDB cut already exists; these updates must
            # survive the baseline-to-AOF handoff, not merely enter the snapshot.
            for i, src in enumerate(sources):
                src.incr(prefixes[i] + b"counter")
            H.wait_until(
                "RDB followed by live AOF",
                60,
                lambda: drained(shake, {f"127.0.0.1:{port}": 0 for port in ports}),
            )
            for i, src in enumerate(sources):
                prefix = prefixes[i]
                for db in databases:
                    with client(ports[i], db) as selected:
                        selected.delete(prefix + b"delete")
                        selected.rename(prefix + b"rename", prefix + b"renamed")
                        selected.pexpire(prefix + b"ttl", 90000)
                        selected.rpush(prefix + b"list", "incremental")
                        selected.incr(prefix + b"counter")
                        if not fallback:
                            selected.xadd(
                                prefix + b"stream", {b"field": b"tail"}, id="3-0"
                            )
                if not clustered:
                    src.execute_command(
                        "FUNCTION", "LOAD", "REPLACE", library("incremental")
                    )
            expected = checkpoint(
                shake, ports, sources, prefixes, nodes, mode, databases
            )
            if not clustered:
                # A single source's keyless operations address the whole logical
                # target. Cluster-source broadcast/merge policy belongs to the tool.
                for command in ("FLUSHDB", "FLUSHALL"):
                    sources[0].execute_command(command)
                    seed(sources[0], prefixes[0], streams=not fallback)
                    if mode == "cluster":
                        seed(sources[0], other, streams=not fallback)
                    expected = checkpoint(
                        shake, ports, sources, prefixes, nodes, mode, databases
                    )
                for command in (
                    ("FUNCTION", "DELETE", "imported"),
                    ("FUNCTION", "FLUSH"),
                ):
                    sources[0].execute_command(*command)
                    checkpoint(shake, ports, sources, prefixes, nodes, mode, databases)
                    sources[0].execute_command(
                        "FUNCTION", "LOAD", library("after-catalog-change")
                    )
                expected = checkpoint(
                    shake, ports, sources, prefixes, nodes, mode, databases
                )
            shake.close()
            cutover(nodes, mode, expected[0])
        finally:
            shake.close()


if __name__ == "__main__":
    LAVIK, REDIS, SHAKE, META, CTL, CLI = map(
        lambda p: str(Path(p).resolve()), sys.argv[1:7]
    )
    assert redis.__version__ == "8.1.0", redis.__version__
    assert "v=7.2.14 " in subprocess.check_output([REDIS, "--version"], text=True)
    C.META, C.DATA, C.CTL, C.REDIS_CLI = META, LAVIK, CTL, CLI
    cases = {
        "single-single": ("single", False, False),
        "single-cluster": ("cluster", False, False),
        "cluster-single": ("single", True, False),
        "cluster-cluster": ("cluster", True, False),
        "type-commands": ("single", False, True),
    }
    faults = {
        f"{mode}-{phase}-{failure}": (mode, phase, failure)
        for mode in ("single", "cluster")
        for phase in ("full", "incremental")
        for failure in ("tool", "connection", "failover")
    }
    selected = sys.argv[7:] or list(cases)
    with tempfile.TemporaryDirectory(
        prefix="import-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        for name in selected:
            print(f"Running {name}", flush=True)
            if name in faults:
                failed_import(Path(directory) / name, *faults[name])
            else:
                migrate(Path(directory) / name, *cases[name])
    print("RedisShake SyncReader checks passed")
