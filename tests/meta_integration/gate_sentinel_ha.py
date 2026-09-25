#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Persistent stock client HA gates. Every fault recovery has a 30 second budget.

Raw queries assert safety/publication only; recovery is ordinary SET/GET through
unchanged redis-py and go-redis pools. TCP proxies force new Data connections so
continued success on an old socket cannot masquerade as discovery recovery.
"""

import json
import os
from pathlib import Path
import queue
import select
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

import harness as H
import gate_cluster_create as C
import gate_sentinel_discovery as D
from gate_data_control import DataProcess
from gate_sentinel import Client

THIRD = "3333333333333333333333333333333333333333"


class RaftProxy(H.Proxy):
    """Partition by the authenticated transport's source id, in both directions.

    Lavik's plaintext Raft preface is magic/from/to, three big-endian uint64s.
    A destination-only blackhole is insufficient: the old leader could retain
    outbound quorum. Track every pair's source and cut both sides of the split.
    """

    def __init__(self, node):
        super().__init__(f"raft-{node.id}", node.raft_port)
        self.node_id = node.id
        self.isolated = None
        self.sources = {}

    def partition(self, node_id):
        with self._lock:
            self.isolated = node_id
            pairs = [
                p for p, src in self.sources.items() if node_id in (src, self.node_id)
            ]
        for pair in pairs:
            self._cut_pair(pair)

    def _on_accept(self, conn):
        threading.Thread(target=self._route, args=(conn,), daemon=True).start()

    def _route(self, conn):
        upstream = None
        try:
            conn.settimeout(3)
            header = b""
            while len(header) < 24:
                chunk = conn.recv(24 - len(header))
                if not chunk:
                    return
                header += chunk
            source = int.from_bytes(header[8:16], "big")
            upstream = socket.create_connection(self.target, timeout=3)
            pair = (conn, upstream)
            with self._lock:
                if self.isolated in (source, self.node_id):
                    return
                self._pairs.add(pair)
                self.sources[pair] = source
            upstream.sendall(header)
            conn.settimeout(None)
            upstream.settimeout(None)
            for src, dst in ((conn, upstream), (upstream, conn)):
                threading.Thread(
                    target=self._pump, args=(src, dst, pair), daemon=True
                ).start()
            conn = upstream = None
        except OSError:
            pass
        finally:
            for sock in (conn, upstream):
                if sock is not None:
                    self._close_sock(sock)

    def _cut_pair(self, pair):
        super()._cut_pair(pair)
        with self._lock:
            self.sources.pop(pair, None)


class DataProxy(H.Proxy):
    # The connect timeout must not become an idle replication-stream timeout.
    def _pump(self, src, dst, pair):
        src.settimeout(None)
        super()._pump(src, dst, pair)


class Fixture(D.DiscoveryFixture):
    def __init__(self, meta, data, ctl, directory, third=False):
        super().__init__(meta, data, ctl, directory)
        if third:
            node = DataProcess(
                data, str(directory / "third"), THIRD, self.leader.data_control_endpoint
            )
            self.data_nodes.append(node)
            self.by_id[node.node_id] = node
        self.raft_proxies = [RaftProxy(node) for node in self.metas]
        for node, proxy in zip(self.metas, self.raft_proxies):
            node.advertised_raft_endpoint = proxy.endpoint
        self.data_proxies = {}
        for node in self.data_nodes:
            proxy = DataProxy(f"data-{node.node_id[:4]}", node.redis_port)
            node.discovery_endpoint = "tcp://" + proxy.endpoint
            self.data_proxies[node.node_id] = proxy
        for proxy in self.raft_proxies + list(self.data_proxies.values()):
            proxy.start()

    def force_kill(self):
        super().force_kill()
        for proxy in self.raft_proxies + list(self.data_proxies.values()):
            proxy.close()

    def cut_data_connections(self):
        for proxy in self.data_proxies.values():
            proxy.set_drop()
            proxy.heal()


class GoClient:
    def __init__(self, binary, addresses, protocol, log):
        self.proc = subprocess.Popen(
            [
                binary,
                "--addrs",
                ",".join(f"{h}:{p}" for h, p in addresses),
                "--protocol",
                str(protocol),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
            bufsize=1,
        )

    def call(self, **request):
        self.proc.stdin.write(json.dumps(request) + "\n")
        self.proc.stdin.flush()
        if not select.select([self.proc.stdout], [], [], 5)[0]:
            raise H.Failure("Go driver exceeded operation deadline")
        line = self.proc.stdout.readline()
        if not line:
            raise H.Failure("Go driver exited")
        reply = json.loads(line)
        if not reply["ok"]:
            raise RuntimeError(reply.get("error"))
        return reply["value"]

    def set(self, key, value):
        return self.call(op="set", key=key, value=value)

    def get(self, key):
        return self.call(op="get", key=key)

    def close(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=5)


def make_clients(fixture, go_binary, directory, learned=False):
    import redis
    from redis.sentinel import Sentinel

    if redis.__version__ != "8.1.0":
        raise H.Failure("redis-py 8.1.0 is mandatory")
    seeds = fixture.sentinel_addresses(leader_first=False)
    seeds.insert(1, ("127.0.0.1", H.free_port()))  # unreachable seed among real peers
    clients, resources = [], []
    for protocol in (2, 3):
        sentinel = Sentinel(
            seeds,
            min_other_sentinels=2,
            sentinel_kwargs={
                "password": D.SENTINEL_PASSWORD,
                "socket_timeout": 1,
                "socket_connect_timeout": 1,
            },
            socket_timeout=1,
            socket_connect_timeout=1,
        )
        clients.append(
            (f"python-{protocol}", sentinel.master_for(D.GROUP, protocol=protocol))
        )
        resources.append(sentinel)
        log = open(directory / f"go-{protocol}.log", "w")
        go_seeds = fixture.sentinel_addresses()[:1] if learned else seeds
        go = GoClient(go_binary, go_seeds, protocol, log)
        clients.append((f"go-{protocol}", go))
        resources.append(log)
    return clients, resources


def recover(clients, description, start=None):
    start = time.monotonic() if start is None else start
    errors = {}
    pending = dict(clients)
    while pending and time.monotonic() - start < 30:
        for name, client in list(pending.items()):
            try:
                client.set("sentinel-ha-" + name, description)
                value = client.get("sentinel-ha-" + name)
                if value not in (description, description.encode()):
                    raise H.Failure(f"{name}: wrong value {value!r}")
                del pending[name]
                H.log(
                    f"{description}: {name} recovered at {time.monotonic() - start:.3f}s; errors={errors.get(name, [])}"
                )
            except Exception as error:
                errors.setdefault(name, []).append(
                    type(error).__name__ + ": " + str(error)
                )
        time.sleep(0.05)
    if pending:
        raise H.Failure(f"{description}: 30s budget exceeded: {errors}")


class Subscription:
    def __init__(self, fixture, protocol):
        self.client = Client(fixture.sentinel_ports[fixture.leader.id])
        self.client.command("AUTH", D.SENTINEL_PASSWORD)
        if protocol == 3:
            self.client.command("HELLO", 3)
        self.client.command("SUBSCRIBE", "+switch-master", "+replica-reconf-done")
        self.client.read()  # second acknowledgement
        self.client.sock.settimeout(None)
        self.messages = queue.Queue()
        self.expected_tag = b">" if protocol == 3 else b"*"
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self):
        try:
            while True:
                tag = self.client.file.peek(1)[:1]
                if tag and tag != self.expected_tag:
                    self.messages.put([b"invalid-pubsub-frame", tag])
                    return
                self.messages.put(self.client.read())
        except (OSError, EOFError):
            self.messages.put(None)

    def close(self):
        self.client.sock.shutdown(socket.SHUT_RDWR)
        self.thread.join(timeout=3)
        self.client.close()


def assert_closed(client):
    client.sock.settimeout(1)
    try:
        value = client.sock.recv(1)
    except ConnectionResetError:
        return
    if value != b"":
        raise H.Failure(f"revoked connection produced bytes: {value!r}")


def directory(fixture, expected):
    peers = fixture.sentinel_command("SENTINEL", "SENTINELS", D.GROUP)
    master = D.as_dict(fixture.sentinel_command("SENTINEL", "MASTER", D.GROUP), 2)
    if len(peers) != expected or int(master[b"num-other-sentinels"]) != expected:
        raise H.Failure(f"directory/count mismatch: {peers}, {master}")
    return peers


def meta_case(fixture, go_binary, directory_path):
    fixture.start_created()
    clients, resources = make_clients(fixture, go_binary, directory_path, learned=True)
    old_connections = []
    try:
        recover(clients, "initial follower-first / learned-single-seed")
        directory(fixture, 2)
        # Threshold failure proves the count is consumed by the stock library.
        from redis.sentinel import Sentinel, MasterNotFoundError

        with Sentinel(
            fixture.sentinel_addresses(),
            min_other_sentinels=3,
            sentinel_kwargs={
                "password": D.SENTINEL_PASSWORD,
                "socket_timeout": 1,
                "socket_connect_timeout": 1,
            },
        ) as too_many:
            try:
                too_many.discover_master(D.GROUP)
            except MasterNotFoundError:
                pass
            else:
                raise H.Failure("min_other_sentinels=3 unexpectedly accepted two peers")
        old = fixture.leader
        for command in (
            ("SENTINEL", "MASTERS"),
            ("SUBSCRIBE", "+switch-master"),
            ("SENTINEL", "MASTERS"),
        ):
            client = Client(fixture.sentinel_ports[old.id])
            client.command("AUTH", D.SENTINEL_PASSWORD)
            client.command(*command)
            if len(old_connections) == 2:
                client.command("RESET")
            old_connections.append(client)
        # Go's learned peers are independently evidenced by the library log;
        # only the old leader was configured, so recovery requires SENTINELS.
        start = time.monotonic()
        for proxy in fixture.raft_proxies:
            proxy.partition(old.id)
        majority = [m for m in fixture.metas if m is not old]
        fixture.leader = H.find_leader(majority, timeout=15)
        H.wait_until(
            "old living leader loses authority", 10, lambda: not old.is_leader()
        )
        for client in old_connections:
            assert_closed(client)
        fixture.cut_data_connections()
        recover(clients, "minority partition / learned peers", start)
        if not old.alive():
            raise H.Failure("partition killed old Meta process")
        for proxy in fixture.raft_proxies:
            proxy.partition(None)
        fixture.cut_data_connections()
        recover(clients, "network healed")
        for client in old_connections:
            assert_closed(client)
        # A registered but unreachable peer remains in the committed directory.
        old.force_kill()
        directory(fixture, 2)
        fixture.cut_data_connections()
        recover(clients, "registered seed down")
        old.start(wait_ready=False)  # durable member advertisement survives WAL restart
        H.wait_until("restarted member catches up", 20, old.alive)
        directory(fixture, 2)
        H.wait_until(
            "restarted peer catches up before next fault",
            20,
            lambda: old.committed() >= fixture.leader.committed(),
        )
        lost = fixture.leader
        start = time.monotonic()
        lost.force_kill()
        fixture.leader = H.find_leader(
            [m for m in fixture.metas if m is not lost], timeout=15
        )
        fixture.cut_data_connections()
        recover(clients, "active Meta seed killed", start)
        lost.start(wait_ready=False)
        H.wait_until(
            "second restarted member catches up",
            20,
            lambda: lost.committed() >= fixture.leader.committed(),
        )
        # Remove a follower through the real membership operation, then restart
        # the responder: both listing and count must follow effective membership.
        fixture.rediscover_leader()
        removed = next(m for m in fixture.metas if m is not fixture.leader)
        reply = fixture.leader.ctl(f"removesrv {removed.id}")
        if not reply.startswith("OK"):
            raise H.Failure(f"remove member: {reply}")
        H.wait_until(
            "directory reflects committed removal",
            20,
            lambda: len(fixture.sentinel_command("SENTINEL", "SENTINELS", D.GROUP))
            == 1,
        )
        directory(fixture, 1)
        port = H.free_port()
        joiner = H.Node(
            fixture.metas[0].binary,
            str(directory_path / "cluster" / "meta"),
            4,
            args=H.raft_args(election_ms_low=1500, election_ms_high=3000)
            + [
                "--sentinel-addr",
                f"127.0.0.1:{port}",
                "--sentinel-requirepass",
                D.SENTINEL_PASSWORD,
            ],
        )
        fixture.metas.append(joiner)
        fixture.sentinel_ports[joiner.id] = port
        joiner.start(wait_ready=False)
        command = (
            f"addsrv {joiner.id} {joiner.endpoint} "
            f"{joiner.data_control_endpoint} {joiner.ctl_endpoint}"
        )
        if fixture.leader.ctl(command) != "ERR inconsistent-sentinel-coverage":
            raise H.Failure("Single admitted a member without discovery coverage")
        reply = fixture.leader.ctl(command + f" sentinel=127.0.0.1:{port}")
        if not reply.startswith("OK"):
            raise H.Failure(f"add registered member: {reply}")
        H.wait_until(
            "new registered peer becomes effective",
            30,
            lambda: len(fixture.sentinel_command("SENTINEL", "SENTINELS", D.GROUP))
            == 2,
        )
        peers = directory(fixture, 2)
        if str(port).encode() not in [D.as_dict(peer, 2)[b"port"] for peer in peers]:
            raise H.Failure("new peer advertisement is missing")
        if not fixture.leader.ctl(
            command + f" sentinel=127.0.0.1:{H.free_port()}"
        ).startswith("ERR"):
            raise H.Failure("member advertisement changed in place")
        snapshot_leader = fixture.leader
        H.manual_snapshot(snapshot_leader)
        snapshot_leader.force_kill()
        fixture.leader = H.find_leader(
            [m for m in fixture.metas if m not in (removed, snapshot_leader)],
            timeout=15,
        )
        directory(fixture, 2)
        snapshot_leader.start(wait_ready=False)
        H.wait_until("snapshot member restarts", 15, snapshot_leader.alive)
        directory(fixture, 2)
    finally:
        for client in old_connections:
            client.close()
        for _, client in clients:
            client.close()
        for resource in resources:
            resource.close()


def data_case(fixture, go_binary, directory_path):
    fixture.start_created()
    clients, resources = make_clients(fixture, go_binary, directory_path)
    subscriptions = []
    try:
        recover(clients, "initial data")
        fixture.configure_fast_policies()
        # Wait on real replica data, not only Meta's READY lifecycle.
        for node in fixture.data_nodes[1:]:
            H.wait_until(
                "replica actually readable",
                30,
                lambda node=node: node.command_head(["GET", "sentinel-ha-python-2"])
                == "$12",
            )
        subscriptions = [Subscription(fixture, p) for p in (2, 3)]
        owner, candidate, third = (
            fixture.by_id[D.OWNER],
            fixture.by_id[D.REPLICA],
            fixture.by_id[THIRD],
        )
        fds_before = third.metric("lavik_cluster_control_full_states_applied_total")
        candidate.pause()
        third.pause()
        before = {
            name: client.call(op="stats")
            for name, client in clients
            if name.startswith("go")
        }
        owner.force_kill()
        H.wait_until(
            "fence has no publishable primary",
            20,
            lambda: fixture.sentinel_command(
                "SENTINEL", "GET-MASTER-ADDR-BY-NAME", D.GROUP
            )
            is None,
        )
        for subscription in subscriptions:
            if not subscription.messages.empty():
                raise H.Failure("notification inside the masterless fence")
        fixture.data_proxies[D.REPLICA].set_drop()
        candidate.resume()
        target_port = str(fixture.data_proxies[D.REPLICA].listen_port).encode()
        H.wait_until(
            "B becomes publishable",
            20,
            lambda: fixture.sentinel_command(
                "SENTINEL", "GET-MASTER-ADDR-BY-NAME", D.GROUP
            )
            == [b"127.0.0.1", target_port],
        )
        for subscription in subscriptions:
            event = subscription.messages.get(timeout=5)
            expected = (
                f"{D.GROUP} 127.0.0.1 {fixture.data_proxies[D.OWNER].listen_port} "
                f"127.0.0.1 {fixture.data_proxies[D.REPLICA].listen_port}"
            ).encode()
            if event != [b"message", b"+switch-master", expected]:
                raise H.Failure(f"incorrect switch event: {event}")
        # No business command has run since the fault. Pool closure must have
        # happened proactively in the stock Go subscription handler.
        for name, client in clients:
            if name.startswith("go"):
                old_addr = fixture.data_proxies[D.OWNER].endpoint
                H.wait_until(
                    "Go retires idle old-primary connections",
                    5,
                    lambda client=client, name=name: client.call(op="stats")[
                        "closed"
                    ].get(old_addr, 0)
                    > before[name]["closed"].get(old_addr, 0),
                )
        metric = "lavik_cluster_control_full_states_applied_total"
        # Metrics cannot be read while SIGSTOP'ed. Resume then observe term 2
        # through Meta's applied projection while B's network remains cut.
        third.resume()
        H.wait_until(
            "C applies new FDS while B is unreachable",
            15,
            lambda: third.metric(metric) > fds_before,
        )

        def retained_old_source():
            observations = fixture.leader.ctl(f"observations {D.GROUP}")
            proof = next(
                (
                    item
                    for item in observations.split()
                    if item.startswith(f"node={THIRD},")
                ),
                "",
            )
            return ",term=2," in proof and f",source_node={D.OWNER}," in proof

        H.wait_until(
            "C reports current term with retained old-source lineage",
            15,
            retained_old_source,
        )
        readable = Client(third.redis_port)
        try:
            if readable.command("GET", "sentinel-ha-python-2") != b"initial data":
                raise H.Failure(
                    "blocked C did not retain a complete readable population"
                )
        finally:
            readable.close()
        time.sleep(1)
        for subscription in subscriptions:
            while not subscription.messages.empty():
                event = subscription.messages.get_nowait()
                if event is None or (
                    event[1] == b"+replica-reconf-done"
                    and str(fixture.data_proxies[THIRD].listen_port).encode()
                    in event[2]
                ):
                    raise H.Failure(
                        f"false replica completion while B blocked: {event}"
                    )
        start = time.monotonic()
        fixture.data_proxies[D.REPLICA].heal()
        recover(clients, "Data cutover", start)
        for subscription in subscriptions:
            deadline = start + 30
            while time.monotonic() < deadline:
                event = subscription.messages.get(
                    timeout=max(0.1, deadline - time.monotonic())
                )
                if event is None:
                    raise H.Failure(
                        "Meta subscription lost during continuous leadership"
                    )
                if (
                    event[1] == b"+replica-reconf-done"
                    and str(fixture.data_proxies[THIRD].listen_port).encode()
                    in event[2]
                ):
                    break
            else:
                raise H.Failure("C never reported actual new-source adoption")
    finally:
        for node in fixture.data_nodes:
            if node.alive():
                node.resume()
        for subscription in subscriptions:
            subscription.close()
        for _, client in clients:
            client.close()
        for resource in resources:
            resource.close()


def main():
    meta, data, ctl, go_binary = map(os.path.abspath, sys.argv[1:5])
    scenario = sys.argv[5]
    if D.import_redis_py(meta) is None:
        raise H.Failure("Sentinel HA gate requires redis-py 8.1.0")
    directory_path = Path(
        tempfile.mkdtemp(
            prefix=f"sentinel-ha-{scenario}-", dir=os.environ["LAVIK_TEST_DATA_DIR"]
        )
    )
    H.set_tag(f"sentinel-ha-{scenario}")
    C.META, C.DATA, C.CTL = meta, data, ctl
    fixture = Fixture(
        meta, data, ctl, directory_path / "cluster", third=scenario == "data"
    )
    try:
        (meta_case if scenario == "meta" else data_case)(
            fixture, go_binary, directory_path
        )
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        H.log(f"retained failed gate artifacts: {directory_path}")
        raise
    else:
        shutil.rmtree(directory_path)
    finally:
        fixture.force_kill()
    H.log("PASS")


if __name__ == "__main__":
    main()
