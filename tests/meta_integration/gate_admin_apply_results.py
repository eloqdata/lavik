#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

"""An accepted Admin mutation keeps its reply after later commits.

Usage: gate_admin_apply_results.py /path/to/lavik-meta [workdir]
"""

import os
from pathlib import Path
import re
import socket
import sys

import harness as H

HOLD = "LAVIK_TEST_META_ADMIN_AFTER_APPLY_HOLD_FILE"
TAG = "LAVIK_TEST_META_ADMIN_AFTER_APPLY_TAG"
# Append-only MetaCommandTag values from include/lavik/meta/commands.h.
SET_SLOT_MAP_TAG = 7
FENCE_GROUP_TAG = 12
COMPLETE_OPERATION_TAG = 17


def commit_index(reply):
    match = re.fullmatch(r"OK ([0-9]+)", reply)
    if not match:
        raise H.Failure(f"expected committed reply, got {reply!r}")
    return int(match.group(1))


def hold_then_advance(meta, hold, tag, command, advance):
    log_start = Path(meta.log_path).stat().st_size
    marker = re.compile(rf"fault admin apply held: tag={tag} log_index=([0-9]+)")

    def accepted_index():
        with open(meta.log_path) as log:
            log.seek(log_start)
            match = marker.search(log.read())
        return int(match.group(1)) if match else None

    hold.touch()
    try:
        # Keep the original real Admin session pending while another session
        # commits a change. The application-result acknowledgement, not a sleep
        # or a guessed scheduling window, establishes the ordering.
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as pending:
            pending.settimeout(10)
            pending.connect(meta.ctl_path)
            pending.sendall(command.encode() + b"\n")
            H.wait_until("Admin accepted apply is held", 10, accepted_index)
            original_index = accepted_index()
            later_index = advance()
            assert later_index > original_index, (original_index, later_index)
            hold.unlink()
            with pending.makefile("rb") as replies:
                reply = replies.readline().decode().strip()
            H.log(
                f"{command}: accepted={original_index}, later={later_index}, "
                f"reply={reply!r}"
            )
            assert reply == f"OK {original_index}", (
                f"accepted {command!r} at {original_index} was rewritten "
                f"after commit {later_index}: {reply!r}"
            )
    finally:
        hold.unlink(missing_ok=True)


def slot_map(meta, hold):
    commit_index(meta.creategroup("g1"))
    hold_then_advance(
        meta,
        hold,
        SET_SLOT_MAP_TAG,
        "setslotmap 0 16383 g1",
        lambda: commit_index(meta.creategroup("unrelated")),
    )
    # This passes parsing but fails deterministic apply: the named Group does
    # not exist. Removing the stale post-read must not turn rejection into OK.
    before = meta.committed()
    assert meta.setslotmap(0, 16383, "missing") == "ERR rejected"
    H.wait_until(
        "rejected slot-map entry committed", 5, lambda: meta.committed() > before
    )


def fence(meta, hold):
    commit_index(meta.creategroup("g1"))
    commit_index(meta.begingroupterm("g1", 0, 1))

    def advance_term():
        index = commit_index(meta.begingroupterm("g1", 2, 3))
        assert " term=3 " in meta.ctl("getgroup g1")
        return index

    hold_then_advance(meta, hold, FENCE_GROUP_TAG, "fencegroup g1 1", advance_term)
    assert meta.fencegroup("g1", 1) == "ERR rejected"


def completed_operation(meta, hold):
    operation = meta.new_op_id()
    sequence = commit_index(meta.submitop(operation, "gate", "original"))

    def archive():
        assert meta.getop(operation) == "OK completed done"
        index = commit_index(meta.archiveoperations(sequence))
        assert meta.getop(operation) == "ERR not-found"
        return index

    hold_then_advance(
        meta, hold, COMPLETE_OPERATION_TAG, f"completeop {operation} done", archive
    )
    assert meta.submitop(operation, "gate", "different") == "ERR rejected"


def run(binary, root, name, tag, scenario):
    directory = root / name
    directory.mkdir(parents=True, exist_ok=True)
    hold = directory / "hold"
    meta = H.Node(binary, str(directory), 1, args=H.raft_args(snapshot_distance=0))
    previous = {key: os.environ.get(key) for key in (HOLD, TAG)}
    try:
        os.environ[HOLD] = str(hold)
        os.environ[TAG] = str(tag)
        meta.start(bootstrap=True)
        meta.wait_leader()
        scenario(meta, hold)
        H.log(f"PASS: {name}")
    except Exception:
        H.dump_node_logs([meta])
        raise
    finally:
        hold.unlink(missing_ok=True)
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        meta.force_kill()


def main():
    if len(sys.argv) not in (2, 3):
        print(__doc__)
        return 2
    binary = os.path.abspath(sys.argv[1])
    if HOLD.encode() not in Path(binary).read_bytes():
        H.log("SKIP: Admin application-result fault hook is disabled")
        return 77
    workdir, keep = H.make_workdir(sys.argv, "lavik-admin-apply-")
    failures = []
    try:
        for name, tag, scenario in (
            ("slot-map", SET_SLOT_MAP_TAG, slot_map),
            ("fence", FENCE_GROUP_TAG, fence),
            ("completed-operation", COMPLETE_OPERATION_TAG, completed_operation),
        ):
            try:
                run(binary, Path(workdir), name, tag, scenario)
            except Exception as error:
                failures.append(f"{name}: {error}")
        for failure in failures:
            H.log(f"FAIL: {failure}")
        return 1 if failures else 0
    finally:
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    H.set_tag("gate-admin-apply-results")
    sys.exit(main())
