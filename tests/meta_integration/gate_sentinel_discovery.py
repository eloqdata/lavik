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

"""Sentinel topology discovery against a real managed Single cluster.

Usage: gate_sentinel_discovery.py META DATA CTL [workdir]

Three Meta nodes each expose the Sentinel entry (--sentinel-addr with its own
--sentinel-requirepass); two Data nodes form one managed Single group whose
committed Group ID is the Sentinel service name. The gate drives the pinned
redis-py 8.1.0 client (RESP3 by default, RESP2 via protocol overrides) and a
raw RESP socket for the exact wire shape, through the publication lifecycle:
healthy discovery and reads/writes, unknown service, follower-seed retry, the
no-Primary fence window of an uncontrolled failover, rediscovery of the new
Owner, replica flags for a killed member, and committed member removal.

redis-py is an optional dependency: when it cannot be imported the gate skips
(exit 0), unless LAVIK_REQUIRE_REDIS_PY=1 turns the skip into a failure (CI).
Resolution order: $LAVIK_SENTINEL_REDIS_PY, else <build>/test_tools/redis_py
derived from the META binary path (see scripts/install_test_redis_py.sh).
"""

import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import time
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as H  # noqa: E402
import gate_cluster_create as C  # noqa: E402
import gate_failover as F  # noqa: E402
from gate_data_control import DataProcess  # noqa: E402
from gate_sentinel import Client, RespError  # noqa: E402

GROUP = "single-discovery"
OWNER = "1111111111111111111111111111111111111111"
REPLICA = "2222222222222222222222222222222222222222"
SENTINEL_PASSWORD = "sentinel-secret"


def import_redis_py(meta_binary):
    candidates = []
    override = os.environ.get("LAVIK_SENTINEL_REDIS_PY")
    if override:
        candidates.append(override)
    candidates.append(
        os.path.join(os.path.dirname(meta_binary), "test_tools", "redis_py")
    )
    for path in candidates:
        if not os.path.isfile(os.path.join(path, "redis", "__init__.py")):
            continue
        sys.path.insert(0, path)
        try:
            import redis
            from redis.sentinel import MasterNotFoundError, Sentinel
        except ImportError:
            sys.path.remove(path)
            continue
        if redis.__version__ != "8.1.0":
            raise H.Failure(
                f"redis-py at {path} is version {redis.__version__}, but this "
                "gate is pinned to 8.1.0 (requirements-redis-py.txt)"
            )
        return types.SimpleNamespace(
            Sentinel=Sentinel, MasterNotFoundError=MasterNotFoundError
        )
    if os.environ.get("LAVIK_REQUIRE_REDIS_PY") == "1":
        raise H.Failure(
            "LAVIK_REQUIRE_REDIS_PY=1 but redis-py is not importable; "
            f"looked at {candidates}. Run scripts/install_test_redis_py.sh."
        )
    return None


def poll_call(description, timeout, fn):
    """wait_until for client calls that may raise while servers converge."""

    def probe():
        try:
            return bool(fn())
        except Exception:  # noqa: BLE001 - client errors while converging
            return False

    H.wait_until(description, timeout, probe)


# -- wire-shape (go-redis contract) assertions --------------------------------
# The RESP client itself is shared with gate_sentinel.py so reply-shape fixes
# (for example the RESP2 *-1 null array) apply to both gates.


def as_dict(entry, proto):
    """Flatten one Sentinel entry to a dict, enforcing the wire contract that
    stock clients rely on: RESP2 entries are flat key/value arrays (even
    length), RESP3 entries are maps, and every key and value is a non-null,
    non-empty bulk string in both."""
    if proto == 2:
        if not isinstance(entry, list) or len(entry) % 2 != 0:
            raise H.Failure(f"RESP2 entry is not an even kv array: {entry!r}")
        pairs = entry
    else:
        if not isinstance(entry, dict):
            raise H.Failure(f"RESP3 entry is not a map: {entry!r}")
        pairs = [part for kv in entry.items() for part in kv]
    for part in pairs:
        if not isinstance(part, bytes) or not part:
            raise H.Failure(f"non-null bulk-string contract broken: {entry!r}")
    return dict(entry) if proto == 3 else dict(zip(entry[::2], entry[1::2]))


# -- fixture -----------------------------------------------------------------


class DiscoveryFixture:
    """One managed Single cluster: 3 Meta with Sentinel entries, 2 Data."""

    def __init__(self, meta_binary, data_binary, ctl, directory):
        self.ctl = ctl
        directory.mkdir(parents=True, exist_ok=True)
        meta_dir = directory / "meta"
        meta_dir.mkdir()
        self.sentinel_ports = {}
        self.metas = []
        for node_id in (1, 2, 3):
            port = H.free_port()
            # The projected authority lease is min(policy, election_ms_low)
            # (leadership validity clamp). The fault case lowers the policy to
            # 1000 ms and proves its projection through the Data FDS metric;
            # that change is only visible when election_ms_low exceeds it.
            # Mirror gate_automatic_failover's proven 1500-3000 ms window.
            node = H.Node(
                meta_binary,
                str(meta_dir),
                node_id,
                args=H.raft_args(
                    snapshot_distance=100_000,
                    election_ms_low=1_500,
                    election_ms_high=3_000,
                )
                + [
                    "--sentinel-addr",
                    f"127.0.0.1:{port}",
                    "--sentinel-requirepass",
                    SENTINEL_PASSWORD,
                ],
            )
            self.metas.append(node)
            self.sentinel_ports[node_id] = port
        seed = self.metas[0].data_control_endpoint
        self.data_nodes = [
            DataProcess(data_binary, str(directory / "owner"), OWNER, seed),
            DataProcess(data_binary, str(directory / "replica"), REPLICA, seed),
        ]
        self.by_id = {node.node_id: node for node in self.data_nodes}
        self.manifest = str(directory / "cluster.toml")
        self.leader = self.metas[0]

    def start_created(self):
        lines = ["schema_version = 1", 'client_mode = "single"', ""]
        lines += C.meta_manifest_lines(*self.metas)
        for node in self.data_nodes:
            lines += [
                "[[data_nodes]]",
                f'id = "{node.node_id}"',
                f'client_endpoint = "{node.advertised_endpoint}"',
                "",
            ]
        lines += [
            "[[groups]]",
            f'id = "{GROUP}"',
            f'primary = "{OWNER}"',
            f'replicas = ["{REPLICA}"]',
            "",
            "[[slot_ranges]]",
            "first = 0",
            "last = 16383",
            f'group = "{GROUP}"',
            "",
            # Keep automatic failover effectively disabled during setup;
            # the fault case lowers the threshold through the Policy.
            "[bootstrap_policy]",
            "automatic_uncontrolled_failover_suspect_after_ms = 600000",
            "",
        ]
        Path(self.manifest).write_text("\n".join(lines))
        for meta in self.metas:
            meta.start(initial_cluster_manifest=self.manifest, wait_ready=False)
        self.leader = H.find_leader(self.metas, timeout=20)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                if self.cluster_status().get("meta_membership_stable"):
                    break
            except H.Failure:
                pass
            time.sleep(0.05)
        else:
            raise H.Failure("initial Meta identities did not converge")
        seed = self.leader.data_control_endpoint
        for data in self.data_nodes:
            data.seed = seed
            # wait_ready=True blocks on the metrics-listener/bootstrap barrier
            # inside DataProcess.start; with two nodes the sequential starts
            # cost less than a hand-rolled copy of that check.
            data.start()
        created = F.run_command(
            [
                self.ctl,
                "cluster-create",
                "--manifest",
                self.manifest,
                "--addr",
                self.leader.ctl_endpoint,
                "--allow-plaintext-admin",
                "--yes",
                "--timeout-ms",
                "120000",
            ]
        )
        if "Cluster create accepted:" not in created:
            raise H.Failure(f"cluster-create was not accepted: {created!r}")
        C.wait_cluster_ready(self.leader, "managed Single cluster reaches READY", 150)

    def rediscover_leader(self):
        for meta in [self.leader] + [m for m in self.metas if m is not self.leader]:
            if meta.alive() and meta.is_leader():
                self.leader = meta
                return meta
        raise H.Failure("no Meta seed currently reports itself as leader")

    def cluster_status(self):
        last = H.Failure("no Meta seed answered cluster-status")
        for meta in [self.leader] + [m for m in self.metas if m is not self.leader]:
            if not meta.alive():
                continue
            try:
                return C.cluster_status(meta)
            except H.Failure as error:
                last = error
        raise last

    def group_status(self):
        group = next(
            (
                item
                for item in self.cluster_status().get("groups", [])
                if item.get("group_id") == GROUP
            ),
            None,
        )
        if group is None:
            raise H.Failure(f"{GROUP} is absent from cluster-status")
        return group

    def wait_group(self, description, predicate, timeout=90):
        latest = None

        def probe():
            nonlocal latest
            try:
                latest = self.group_status()
            except H.Failure:
                return False
            return predicate(latest)

        try:
            H.wait_until(description, timeout, probe)
        except H.Failure as error:
            raise H.Failure(f"{error}; group={latest}") from error
        return latest

    def sentinel_addresses(self, leader_first=True):
        ordered = [self.leader] + [m for m in self.metas if m is not self.leader]
        if not leader_first:
            ordered.reverse()
        return [("127.0.0.1", self.sentinel_ports[meta.id]) for meta in ordered]

    def sentinel_command(self, *args, proto=2):
        """One authenticated shot against the current leader's entry."""
        self.rediscover_leader()
        client = Client(self.sentinel_ports[self.leader.id])
        try:
            if client.command("AUTH", SENTINEL_PASSWORD) != b"OK":
                raise H.Failure("Sentinel entry rejected its password")
            if proto == 3:
                client.command("HELLO", 3)
            return client.command(*args)
        finally:
            client.close()

    def configure_fast_policies(self, suspect_after_ms=1000, lease_duration_ms=1000):
        # Same sequencing as gate_automatic_failover: project the lease Policy
        # into a fresh FDS on every Data member before arming the detector, so
        # the fault cut cannot strand the transition on a stale projection.
        self.rediscover_leader()
        fds_metric = "lavik_cluster_control_full_states_applied_total"
        fds_before = {data.node_id: data.metric(fds_metric) for data in self.data_nodes}
        lease = self.leader.put_authority_lease_policy(2, duration_ms=lease_duration_ms)
        if re.fullmatch(r"OK [1-9][0-9]*", lease) is None:
            raise H.Failure(f"fast Authority Lease Policy failed: {lease}")
        for data in self.data_nodes:
            data.wait_metric(
                fds_metric,
                lambda value, node_id=data.node_id: value > fds_before[node_id],
                f"Data node {data.node_id[:8]} applies the lease Policy FDS",
                timeout=30,
            )
        C.wait_cluster_ready(self.leader, "fast Policy projection reaches READY", 60)
        threshold = self.leader.put_automatic_uncontrolled_failover_policy(
            2, suspect_after_ms=suspect_after_ms
        )
        if re.fullmatch(r"OK [1-9][0-9]*", threshold) is None:
            raise H.Failure(f"automatic-failover Policy update failed: {threshold}")

        def healthy(group):
            return (
                group.get("automatic_failover_state") == "healthy"
                and int(group.get("suspect_elapsed_ms", "0")) == 0
                and int(group.get("effective_threshold_ms", "0")) == suspect_after_ms
            )

        self.wait_group(
            "automatic detector observes healthy Owner", healthy, timeout=30
        )

    def clean_shutdown(self):
        for data in self.data_nodes:
            data.terminate()
        for meta in self.metas:
            meta.terminate()

    def dump_logs(self):
        H.dump_node_logs(self.metas, lines=150)
        for data in self.data_nodes:
            print(f"--- Data log tail ({data.log_path}) ---", file=sys.stderr)
            print(data.log_tail(lines=200), file=sys.stderr)

    def force_kill(self):
        for data in self.data_nodes:
            data.force_kill()
        for meta in self.metas:
            meta.force_kill()


# -- redis-py helpers ---------------------------------------------------------


def make_sentinel(redis, addresses, protocol=None):
    kwargs = {
        "password": SENTINEL_PASSWORD,
        "socket_timeout": 5,
        "socket_connect_timeout": 5,
    }
    if protocol is not None:
        # RESP2 pass; the default (unset) is redis-py 8.1.0's RESP3.
        kwargs["protocol"] = protocol
    return redis.Sentinel(
        addresses, sentinel_kwargs=kwargs, socket_timeout=5, socket_connect_timeout=5
    )


def discover_master_poll(redis, sentinel, description, timeout=60):
    """Poll discover_master; publication lags cluster readiness a little."""
    result = {}

    def probe():
        try:
            result["address"] = sentinel.discover_master(GROUP)
            return True
        except redis.MasterNotFoundError:
            return False

    H.wait_until(description, timeout, probe)
    return result["address"]


def endpoint(data):
    return "127.0.0.1", data.redis_port


# -- cases --------------------------------------------------------------------


def case_discovery_and_readwrite(fixture, redis, sentinel_proto, data_proto, label):
    sentinel = make_sentinel(redis, fixture.sentinel_addresses(), sentinel_proto)
    try:
        address = discover_master_poll(
            redis, sentinel, f"redis-py discovers {GROUP} ({label})"
        )
        if address != endpoint(fixture.by_id[OWNER]):
            raise H.Failure(
                f"discovered {address}, want Owner {endpoint(fixture.by_id[OWNER])}"
            )
        master = sentinel.master_for(GROUP, socket_timeout=5, protocol=data_proto)
        try:
            if not master.set(f"sentinel-discovery-{label}", label):
                raise H.Failure("SET through discovered master failed")
            if master.get(f"sentinel-discovery-{label}") != label.encode():
                raise H.Failure("GET through discovered master failed")
        finally:
            master.close()
        H.log(f"{label}: discovery + SET/GET through master_for pool OK")
    finally:
        sentinel.close()


def case_unknown_service(fixture, redis, sentinel_proto, label):
    sentinel = make_sentinel(redis, fixture.sentinel_addresses(), sentinel_proto)
    try:
        try:
            sentinel.discover_master("no-such-service")
        except redis.MasterNotFoundError:
            pass
        else:
            raise H.Failure(
                f"unknown service did not raise MasterNotFoundError ({label})"
            )
        H.log(f"{label}: unknown service -> MasterNotFoundError")
    finally:
        sentinel.close()


def case_follower_seed_first(fixture, redis):
    # The follower's entry closes the discovery exchange without a reply;
    # redis-py must treat that as a dead seed and retry the next one.
    sentinel = make_sentinel(redis, fixture.sentinel_addresses(leader_first=False))
    try:
        address = discover_master_poll(
            redis, sentinel, "follower-first seeds still discover the Owner"
        )
        if address != endpoint(fixture.by_id[OWNER]):
            raise H.Failure(f"follower-first discovery returned {address}")
        H.log("follower-seed-first: silent drop drove seed retry to leader")
    finally:
        sentinel.close()


def case_replica_reads(fixture, redis, sentinel_proto, data_proto, label):
    sentinel = make_sentinel(redis, fixture.sentinel_addresses(), sentinel_proto)
    master = replica = None
    key = f"sentinel-discovery-replica-read-{label}"
    try:
        master = sentinel.master_for(GROUP, socket_timeout=5, protocol=data_proto)
        if not master.set(key, "from-master"):
            raise H.Failure("master rejected the replica-read seed write")
        slaves = discover_slaves_poll(redis, sentinel)
        if slaves != [endpoint(fixture.by_id[REPLICA])]:
            raise H.Failure(f"discover_slaves returned {slaves}")
        replica = sentinel.replica_for(GROUP, socket_timeout=5, protocol=data_proto)
        poll_call(
            "replica_for serves the replicated read",
            30,
            lambda: replica.get(key) == b"from-master",
        )
        H.log(f"{label}: replica_for discovered and read from the managed replica")
    finally:
        if replica is not None:
            replica.close()
        if master is not None:
            master.close()
        sentinel.close()


def discover_slaves_poll(redis, sentinel, timeout=30):
    result = {}

    def probe():
        slaves = sentinel.discover_slaves(GROUP)
        if slaves:
            result["slaves"] = slaves
            return True
        return False

    H.wait_until("discover_slaves lists the replica", timeout, probe)
    return result["slaves"]


def case_reply_contract(fixture):
    fixture.rediscover_leader()
    owner = fixture.by_id[OWNER]
    replica = fixture.by_id[REPLICA]
    group = fixture.leader.ctl(f"getgroup {GROUP}")
    match = re.search(r"\bterm=([0-9]+)", group)
    if match is None:
        raise H.Failure(f"getgroup did not report a term: {group!r}")
    term = match.group(1)
    for proto in (2, 3):
        masters = fixture.sentinel_command("SENTINEL", "MASTERS", proto=proto)
        if not isinstance(masters, list) or len(masters) != 1:
            raise H.Failure(f"MASTERS did not list exactly one service: {masters!r}")
        entry = as_dict(masters[0], proto)
        expected = {
            b"name": GROUP.encode(),
            b"ip": b"127.0.0.1",
            b"port": str(owner.redis_port).encode(),
            b"runid": OWNER.encode(),
            b"flags": b"master",
            b"role-reported": b"master",
            b"config-epoch": term.encode(),
            b"num-slaves": b"1",
            b"num-other-sentinels": b"0",
            b"quorum": b"1",
            b"failover-timeout": b"180000",
            b"parallel-syncs": b"1",
            b"down-after-milliseconds": b"30000",
        }
        for key, value in expected.items():
            if entry.get(key) != value:
                raise H.Failure(
                    f"proto{proto} MASTER field {key}: {entry.get(key)!r} "
                    f"want {value!r} (entry {entry!r})"
                )
        for omitted in (b"master-link-status", b"master-link-down-time"):
            if omitted in entry:
                raise H.Failure(f"proto{proto} MASTER unexpectedly carries {omitted}")
        address = fixture.sentinel_command(
            "SENTINEL", "GET-MASTER-ADDR-BY-NAME", GROUP, proto=proto
        )
        want_address = [b"127.0.0.1", str(owner.redis_port).encode()]
        if (
            not isinstance(address, list)
            or len(address) != 2
            or not all(isinstance(part, bytes) for part in address)
            or address != want_address
        ):
            raise H.Failure(f"proto{proto} GET-MASTER-ADDR-BY-NAME shape: {address!r}")
        replicas = fixture.sentinel_command("SENTINEL", "REPLICAS", GROUP, proto=proto)
        if not isinstance(replicas, list) or len(replicas) != 1:
            raise H.Failure(
                f"proto{proto} REPLICAS did not list the "
                f"committed replica: {replicas!r}"
            )
        rentry = as_dict(replicas[0], proto)
        rexpected = {
            b"name": f"127.0.0.1:{replica.redis_port}".encode(),
            b"ip": b"127.0.0.1",
            b"port": str(replica.redis_port).encode(),
            b"runid": REPLICA.encode(),
            b"flags": b"slave",
            b"role-reported": b"slave",
            b"master-host": b"127.0.0.1",
            b"master-port": str(owner.redis_port).encode(),
            b"slave-priority": b"100",
            b"slave-repl-offset": b"0",
            b"replica-announced": b"1",
        }
        for key, value in rexpected.items():
            if rentry.get(key) != value:
                raise H.Failure(
                    f"proto{proto} REPLICAS field {key}: "
                    f"{rentry.get(key)!r} want {value!r} (entry {rentry!r})"
                )
        for omitted in (b"master-link-status", b"master-link-down-time"):
            if omitted in rentry:
                raise H.Failure(f"proto{proto} REPLICAS unexpectedly carries {omitted}")
        slaves = fixture.sentinel_command("SENTINEL", "SLAVES", GROUP, proto=proto)
        if slaves != replicas:
            raise H.Failure(f"proto{proto} SLAVES diverged from REPLICAS")
    H.log(
        "reply contract: RESP2 flat kv / RESP3 map shapes, field values, "
        "2-element address array all match"
    )


def wait_replica_healthy(fixture):
    """Healthy committed replica: flags read exactly 'slave' once the leader
    holds TTL-fresh observation; poll past the post-election grace window."""

    def healthy():
        try:
            replicas = fixture.sentinel_command("SENTINEL", "REPLICAS", GROUP)
        except Exception:  # noqa: BLE001 - poll across elections
            return False
        if not isinstance(replicas, list) or len(replicas) != 1:
            return False
        return as_dict(replicas[0], 2).get(b"flags") == b"slave"

    H.wait_until("committed replica listed with clean flags", 90, healthy)


def case_no_primary_window(fixture, redis):
    fixture.configure_fast_policies()
    owner, replica = fixture.by_id[OWNER], fixture.by_id[REPLICA]
    # Freeze the only candidate first so the committed fence (Begin) cannot be
    # consumed by a cutover while the window assertions run; resume it after.
    replica.pause()
    try:
        owner.force_kill()

        def withdrawn():
            try:
                masters = fixture.sentinel_command("SENTINEL", "MASTERS")
                address = fixture.sentinel_command(
                    "SENTINEL", "GET-MASTER-ADDR-BY-NAME", GROUP
                )
            except Exception:  # noqa: BLE001 - poll across elections
                return False
            return masters == [] and address is None

        H.wait_until("Owner loss fence retracts publication", 90, withdrawn)
        error = fixture.sentinel_command("SENTINEL", "MASTER", GROUP)
        if not (
            isinstance(error, RespError)
            and error.startswith(b"ERR No such master with that name")
        ):
            raise H.Failure(f"known-but-withdrawn MASTER replied {error!r}")

        def replica_marked():
            try:
                replicas = fixture.sentinel_command("SENTINEL", "REPLICAS", GROUP)
            except Exception:  # noqa: BLE001 - poll across elections
                return False
            if not isinstance(replicas, list) or len(replicas) != 1:
                return False
            flags = as_dict(replicas[0], 2).get(b"flags", b"").split(b",")
            return b"master_down" in flags

        H.wait_until("frozen replica carries master_down", 60, replica_marked)
        sentinel = make_sentinel(redis, fixture.sentinel_addresses())
        try:
            try:
                sentinel.discover_master(GROUP)
            except redis.MasterNotFoundError:
                pass
            else:
                raise H.Failure(
                    "redis-py discovered a Primary inside the fenced window"
                )
        finally:
            sentinel.close()
        H.log(
            "fence window: MASTERS empty, address null, MASTER error, "
            "master_down set, redis-py raises MasterNotFoundError"
        )
    finally:
        replica.resume()

    sentinel = make_sentinel(redis, fixture.sentinel_addresses())
    try:
        address = discover_master_poll(
            redis, sentinel, "uncontrolled cutover publishes the new Owner", timeout=120
        )
        if address != endpoint(replica):
            raise H.Failure(
                f"post-cutover discovery returned {address}, want the "
                f"promoted replica {endpoint(replica)}"
            )
        fixture.wait_group(
            "cutover commits term 2 with the promoted replica",
            lambda group: group.get("term") == "2"
            and group.get("owner_node_id") == REPLICA,
            timeout=90,
        )
        master = sentinel.master_for(GROUP, socket_timeout=5, protocol=3)
        try:
            poll_call(
                "new Owner accepts writes through a fresh pool",
                60,
                lambda: master.set("sentinel-discovery-after", "cutover"),
            )
            if master.get("sentinel-discovery-after") != b"cutover":
                raise H.Failure("new Owner did not serve the post-cutover key")
        finally:
            master.close()
        H.log("cutover: a new redis-py pool discovered and wrote to the promoted Owner")
    finally:
        sentinel.close()


def case_killed_member_flags_and_removal(fixture):
    owner, replica = fixture.by_id[OWNER], fixture.by_id[REPLICA]

    def killed_owner_flags():
        try:
            replicas = fixture.sentinel_command("SENTINEL", "REPLICAS", GROUP)
        except Exception:  # noqa: BLE001 - poll across elections
            return False
        if not isinstance(replicas, list) or len(replicas) != 1:
            return False
        entry = as_dict(replicas[0], 2)
        flags = entry.get(b"flags", b"").split(b",")
        return (
            entry.get(b"runid") == OWNER.encode()
            and b"s_down" in flags
            and b"disconnected" in flags
            and b"master_down" not in flags
        )

    H.wait_until(
        "killed former Owner lists as s_down,disconnected replica",
        90,
        killed_owner_flags,
    )
    replicas = fixture.sentinel_command("SENTINEL", "REPLICAS", GROUP)
    entry = as_dict(replicas[0], 2)
    if (
        entry.get(b"master-host") != b"127.0.0.1"
        or entry.get(b"master-port") != str(replica.redis_port).encode()
    ):
        raise H.Failure(f"replica entry points at the wrong master: {entry}")

    def unassign():
        try:
            fixture.rediscover_leader()
            group = fixture.leader.ctl(f"getgroup {GROUP}")
            match = re.search(r"\brevision=([0-9]+).*?\btransition=0", group)
            if match is None:
                return False
            reply = fixture.leader.ctl(f"unassignnode {GROUP} {OWNER} {match.group(1)}")
            if reply.startswith("OK "):
                return True
            if reply.startswith("ERR stale-revision"):
                return False
            raise H.Failure(f"unassignnode failed: {reply}")
        except (OSError, H.Failure):
            return False

    H.wait_until("killed member is unassigned from the committed group", 60, unassign)

    def empty():
        try:
            return fixture.sentinel_command("SENTINEL", "REPLICAS", GROUP) == []
        except Exception:  # noqa: BLE001 - poll across elections
            return False

    H.wait_until("REPLICAS reads as an empty array with zero members", 60, empty)
    H.log(
        "killed member: s_down,disconnected flags, then removal leaves an "
        "empty replica array"
    )


def main():
    meta_binary, data_binary, ctl_binary = map(os.path.abspath, sys.argv[1:4])
    if len(sys.argv) > 4:
        workdir = sys.argv[4]
        os.makedirs(workdir, exist_ok=True)
        keep = True
    else:
        test_data_dir = os.environ.get("LAVIK_TEST_DATA_DIR") or "/tmp"
        workdir = tempfile.mkdtemp(
            prefix="lavik-sentinel-discovery-", dir=test_data_dir
        )
        keep = False
    C.META, C.DATA, C.CTL = meta_binary, data_binary, ctl_binary
    H.set_tag("sentinel-discovery")
    redis = import_redis_py(meta_binary)
    if redis is None:
        H.log(
            "SKIP: redis-py is not importable (set LAVIK_SENTINEL_REDIS_PY "
            "or run scripts/install_test_redis_py.sh; CI sets "
            "LAVIK_REQUIRE_REDIS_PY=1 to fail instead)"
        )
        return
    fixture = DiscoveryFixture(
        meta_binary, data_binary, ctl_binary, Path(workdir) / "discovery"
    )
    try:
        fixture.start_created()
        wait_replica_healthy(fixture)
        for sentinel_proto, data_proto, label in ((None, 3, "resp3"), (2, 2, "resp2")):
            case_discovery_and_readwrite(
                fixture, redis, sentinel_proto, data_proto, label
            )
            case_unknown_service(fixture, redis, sentinel_proto, label)
            case_replica_reads(fixture, redis, sentinel_proto, data_proto, label)
        case_follower_seed_first(fixture, redis)
        case_reply_contract(fixture)
        case_no_primary_window(fixture, redis)
        case_killed_member_flags_and_removal(fixture)
        for meta in fixture.metas:
            if not meta.alive():
                raise H.Failure(f"Meta {meta.id} died unexpectedly")
        if not fixture.by_id[REPLICA].alive():
            raise H.Failure("promoted Owner died unexpectedly")
        fixture.clean_shutdown()
    except Exception:
        fixture.dump_logs()
        raise
    finally:
        fixture.force_kill()
        if not keep:
            shutil.rmtree(workdir, ignore_errors=True)
    H.log("PASS")


if __name__ == "__main__":
    main()
