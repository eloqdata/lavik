#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Exercise locally built images with private volumes and random host ports."""

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import uuid

SINGLE = "eloqdata/lavik:0.1.0-beta.1"
CLUSTER = SINGLE + "-cluster"


def run(*args, check=True, timeout=240):
    """Run a bounded Docker command, retaining diagnostics on failure."""
    result = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"{' '.join(args)}\n{result.stdout}\n{result.stderr}")
    return result.stdout.strip()


def wait_for(description, predicate, timeout=120):
    """Poll transient startup/replication errors, but bound every wait."""
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                print(f"PASS: {description}", flush=True)
                return value
        except (OSError, RuntimeError, ValueError) as error:
            last_error = error
        time.sleep(1)
    raise RuntimeError(f"Timed out: {description}; last error: {last_error}")


def redis(port, *commands):
    """Issue commands on one RESP connection, including READONLY then GET."""
    with socket.create_connection(("127.0.0.1", port), timeout=3) as connection:
        with connection.makefile("rb") as reader:
            value = None
            for command in commands:
                parts = [str(item).encode() for item in command]
                request = f"*{len(parts)}\r\n".encode()
                for part in parts:
                    request += f"${len(part)}\r\n".encode() + part + b"\r\n"
                connection.sendall(request)
                line = reader.readline()
                if line.startswith(b"+"):
                    value = line[1:-2].decode()
                elif line.startswith(b"$"):
                    length = int(line[1:-2])
                    value = (
                        None if length == -1 else reader.read(length + 2)[:-2].decode()
                    )
                else:
                    raise RuntimeError(f"Unexpected Redis reply: {line!r}")
            return value


def main():
    """Verify persistence, native replication, controlled failover, and recovery."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", choices=("linux/arm64", "linux/amd64"))
    args = parser.parse_args()
    if args.platform:
        os.environ["DOCKER_DEFAULT_PLATFORM"] = args.platform
    os.environ["LAVIK_PRIMARY_PORT"] = "0"
    os.environ["LAVIK_FOLLOWER_PORT"] = "0"
    os.environ["LAVIK_DATA_SIZE"] = "1G"
    os.environ["LAVIK_THREADS"] = "2"
    project = "lavik-beta-smoke-" + uuid.uuid4().hex[:10]
    standalone = project + "-single"
    volume = standalone + "-data"
    compose = [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(Path(__file__).with_name("compose.yaml")),
    ]

    def port(container):
        state = json.loads(run("docker", "inspect", container))[0]
        return int(state["NetworkSettings"]["Ports"]["6379/tcp"][0]["HostPort"])

    def start_single(size):
        run(
            "docker",
            "run",
            "-d",
            "--name",
            standalone,
            "--security-opt",
            "seccomp=unconfined",
            "-p",
            "127.0.0.1::6379",
            "-v",
            f"{volume}:/data",
            "-e",
            f"LAVIK_DATA_SIZE={size}",
            SINGLE,
        )
        endpoint = port(standalone)
        wait_for("standalone PING", lambda: redis(endpoint, ["PING"]) == "PONG")
        return endpoint

    def cluster_status():
        return json.loads(
            run(
                *compose,
                "exec",
                "-T",
                "meta-1",
                "lavik-ctl",
                "cluster-status",
                "--socket",
                "/data/meta/meta-admin.sock",
                "--allow-plaintext-admin",
                "--timeout-ms",
                "3000",
                "--json",
                check=False,
                timeout=10,
            )
        )

    def bootstrap_finished():
        container = run(*compose, "ps", "-a", "-q", "bootstrap")
        if not container:
            return False
        state = json.loads(run("docker", "inspect", container))[0]["State"]
        if state["Status"] != "exited":
            return False
        if state["ExitCode"]:
            raise RuntimeError(run(*compose, "logs", "--no-color", "bootstrap"))
        return True

    try:
        for image in (SINGLE, CLUSTER):
            version = run("docker", "run", "--rm", image, "lavik", "--version")
            if "0.1.0-beta.1" not in version:
                raise RuntimeError(f"Unexpected release version: {version}")
            if run("docker", "run", "--rm", image, "id", "-u") != "10001":
                raise RuntimeError("Image must run without root")
        endpoint = start_single("1G")
        assert redis(endpoint, ["SET", "docker-release-smoke", "persistent"]) == "OK"
        original_size = run(
            "docker", "exec", standalone, "stat", "-c", "%s", "/data/lavik.data"
        )
        run("docker", "stop", "-t", "60", standalone)
        run("docker", "rm", standalone)
        endpoint = start_single("2G")
        assert redis(endpoint, ["GET", "docker-release-smoke"]) == "persistent"
        assert (
            run("docker", "exec", standalone, "stat", "-c", "%s", "/data/lavik.data")
            == original_size
        )
        print("PASS: standalone recovery preserves data and allocation", flush=True)
        run("docker", "stop", "-t", "60", standalone)

        run(*compose, "up", "-d", "--pull", "never")
        wait_for("cluster bootstrap", bootstrap_finished, timeout=200)
        initial = wait_for(
            "cluster READY",
            lambda: (s if (s := cluster_status()).get("cluster_ready") else None),
        )
        operation_id = initial["root_operation_id"]
        primary_port = port(run(*compose, "ps", "-q", "primary"))
        follower_port = port(run(*compose, "ps", "-q", "follower"))
        assert (
            redis(primary_port, ["SET", "docker-release-smoke", "replicated"]) == "OK"
        )
        wait_for(
            "follower receives writes",
            lambda: redis(follower_port, ["READONLY"], ["GET", "docker-release-smoke"])
            == "replicated",
        )

        run(
            *compose,
            "exec",
            "-T",
            "meta-1",
            "lavik-ctl",
            "failover",
            "group-1",
            "--socket",
            "/data/meta/meta-admin.sock",
            "--allow-plaintext-admin",
            "--failover-timeout-ms",
            "60000",
            timeout=90,
        )
        wait_for(
            "follower promoted by lavik-ctl",
            lambda: redis(
                follower_port, ["SET", "docker-release-smoke", "after-failover"]
            )
            == "OK",
        )
        wait_for(
            "old primary follows new primary",
            lambda: redis(primary_port, ["READONLY"], ["GET", "docker-release-smoke"])
            == "after-failover",
        )

        # Recreate containers while preserving volumes. Bootstrap must observe
        # the old Genesis. Automatic failover may elect either node while they
        # recover, so ask Meta for the owner instead of trusting service names.
        run(*compose, "down", "--timeout", "60")
        run(*compose, "up", "-d", "--pull", "never")
        wait_for("bootstrap accepts recovered state", bootstrap_finished, timeout=200)
        recovered = wait_for(
            "recovered cluster READY",
            lambda: (s if (s := cluster_status()).get("cluster_ready") else None),
        )
        assert recovered["root_operation_id"] == operation_id
        owner = recovered["groups"][0]["owner_node_id"]
        assert owner in ("1" * 40, "2" * 40)
        primary_port = port(run(*compose, "ps", "-q", "primary"))
        follower_port = port(run(*compose, "ps", "-q", "follower"))
        for endpoint in (primary_port, follower_port):
            wait_for(
                "recovered node retains replicated data",
                lambda: redis(endpoint, ["READONLY"], ["GET", "docker-release-smoke"])
                == "after-failover",
            )
        owner_port = primary_port if owner == "1" * 40 else follower_port
        assert (
            redis(owner_port, ["SET", "docker-release-recovered", "writable"]) == "OK"
        )
        print(
            "PASS: cluster recovery preserves Genesis and data, and serves writes",
            flush=True,
        )
        print("All Docker release smoke checks passed.", flush=True)
    except Exception:
        print(
            run("docker", "logs", "--tail", "80", standalone, check=False), flush=True
        )
        print(
            run(*compose, "logs", "--no-color", "--tail", "50", check=False), flush=True
        )
        raise
    finally:
        # Only delete resources created with this invocation's random prefix.
        run(*compose, "down", "--volumes", "--timeout", "60", check=False)
        run("docker", "rm", "-f", "-v", standalone, check=False)
        run("docker", "volume", "rm", volume, check=False)


if __name__ == "__main__":
    main()
