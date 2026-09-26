#!/usr/bin/python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Initialize this disposable Compose cluster once, then wait for readiness."""

import json
import subprocess
import sys
import time


def main():
    """Observe lifecycle before mutation; never replay an uncertain create."""
    if len(sys.argv) != 3:
        sys.exit("Usage: bootstrap-cluster META_IP:PORT MANIFEST")
    address, manifest = sys.argv[1:]
    connection = ["--addr", address, "--allow-plaintext-admin", "--timeout-ms", "5000"]
    deadline = time.monotonic() + 180
    submitted = False
    previous = None
    last_status = "Meta has not answered yet"
    while time.monotonic() < deadline:
        try:
            result = subprocess.run(
                ["lavik-ctl", "cluster-status", *connection, "--json"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            last_status = result.stdout or result.stderr
            status = json.loads(result.stdout)
        except (subprocess.TimeoutExpired, json.JSONDecodeError):
            time.sleep(1)
            continue
        state = status.get("cluster_state")
        if state != previous:
            print(f"Cluster lifecycle: {state}", flush=True)
            previous = state
        if state == "created" and status.get("cluster_ready"):
            print("Cluster is READY (one primary, one follower).", flush=True)
            return 0
        if state in ("provisioning-failed", "non-pristine"):
            sys.exit(f"Refusing to initialize existing or failed state:\n{last_status}")
        if state == "uninitialized" and not submitted:
            submitted = True
            print("Creating the cluster from the Compose manifest.", flush=True)
            try:
                result = subprocess.run(
                    [
                        "lavik-ctl",
                        "cluster-create",
                        *connection,
                        "--manifest",
                        manifest,
                        "--yes",
                    ],
                    timeout=30,
                    check=False,
                )
                if result.returncode not in (0, 3):
                    sys.exit(
                        f"cluster-create failed (exit {result.returncode}); inspect logs."
                    )
            except subprocess.TimeoutExpired:
                pass
            # Exit 3/timeout may have committed Genesis. Only read status from
            # here; a second mutation could overwrite an uncertain operation.
        time.sleep(1)
    sys.exit(f"Timed out waiting for cluster readiness; inspect logs.\n{last_status}")


if __name__ == "__main__":
    sys.exit(main())
