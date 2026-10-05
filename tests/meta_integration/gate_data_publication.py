#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

"""Replace a shared publication cut after a real transfer's first chunk."""

import os
from pathlib import Path
import sys
import tempfile
import time

import gate_cluster_create as C
import harness as H
from gate_data_control import DataProcess
from gate_directive_results import ResultProxy

HOLD = "LAVIK_TEST_META_PUBLICATION_CHUNK_HOLD_FILE"


def run(root):
    (root / "meta").mkdir()
    meta = H.Node(C.META, str(root / "meta"), 1, args=C.creation_raft_args())
    proxy = ResultProxy(meta.data_control_port, copies=1)
    meta.advertised_data_control_endpoint = proxy.endpoint
    data = DataProcess(C.DATA, str(root / "data"), C.DATA_NODE, proxy.endpoint)
    manifest = root / "cluster.toml"
    C.write_manifest(manifest, data.advertised_endpoint, meta)
    hold = root / "hold"
    previous = os.environ.get(HOLD)
    try:
        proxy.start()
        os.environ[HOLD] = str(hold)
        meta.start(initial_cluster_manifest=str(manifest))
        if previous is None:
            os.environ.pop(HOLD)
        else:
            os.environ[HOLD] = previous
        meta.wait_leader()
        data.start()
        C.command(
            os.environ.copy(),
            [
                C.CTL,
                "cluster-create",
                "--manifest",
                str(manifest),
                "--socket",
                meta.ctl_path,
                "--yes",
            ],
        )
        C.wait_cluster_ready(meta, "publication fixture READY", 20)
        baseline = time.monotonic()
        accepted = sum(e[2] == 2 and e[4][0] == 1 for e in proxy.snapshot())
        hold.touch()
        # Unassigned active nodes are still part of the global routing
        # directory. Grow a real update past the single-frame size, without
        # adding any extra Data processes or changing the serving Group.
        for n in range(1, 1025):
            node_id = f"{n:040x}"
            reply = meta.registernode(
                node_id,
                f"lavik://node/{node_id}",
                endpoints=(f"tcp://127.0.0.1:{10000 + n}",),
            )
            if not reply.startswith("OK"):
                raise H.Failure(reply)
            if f"fault pause reached: {HOLD}" in Path(meta.log_path).read_text():
                break
        else:
            raise H.Failure("routing update never reached a chunk boundary")
        H.wait_until(
            "first publication chunk reached Data",
            5,
            lambda: any(e[2] == 4 and e[3] > baseline for e in proxy.snapshot()),
        )
        chunk = next(e for e in proxy.snapshot() if e[2] == 4 and e[3] > baseline)
        old_object = chunk[4][:16]
        # The capture held by this publisher must remain valid while apply
        # changes Policy; boundary revalidation must select the new cut.
        reply = meta.put_authority_lease_policy(2, 250)
        if not reply.startswith("OK"):
            raise H.Failure(reply)
        hold.unlink()
        H.wait_until(
            "obsolete transfer aborted",
            10,
            lambda: any(
                e[2] == 6 and e[4][:16] == old_object for e in proxy.snapshot()
            ),
        )
        C.wait_cluster_ready(meta, "replacement projection READY", 20)
        events = proxy.snapshot()
        assert not any(e[2] == 5 and e[4][:16] == old_object for e in events), (
            "superseded transfer reached TransferEnd"
        )
        assert any(e[1] and e[2] == 7 and e[3] > chunk[3] for e in events), (
            "replacement never received FullStateApplied"
        )
        assert sum(e[2] == 2 and e[4][0] == 1 for e in events) == accepted, (
            "projection replacement reconnected the Data session"
        )
        assert not proxy.errors, proxy.errors
        H.log(
            "PASS: committed replacement aborts a partial transfer and adopts the next cut in-session"
        )
    finally:
        hold.unlink(missing_ok=True)
        if previous is None:
            os.environ.pop(HOLD, None)
        else:
            os.environ[HOLD] = previous
        data.force_kill()
        meta.force_kill()
        proxy.close()


if __name__ == "__main__":
    C.META, C.DATA, C.CTL, C.REDIS_CLI = map(os.path.abspath, sys.argv[1:5])
    if not C.has_fault(C.META, HOLD.encode()):
        H.log("SKIP: publication chunk fault hook is disabled")
        sys.exit(77)
    with tempfile.TemporaryDirectory(
        prefix="lavik-publish-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        run(Path(directory))
