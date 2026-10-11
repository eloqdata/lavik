#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exercise connection-worker disk GET with writes, transactions and detach."""

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import sys
import tempfile
import time

import defrag_payload_e2e as support


class Client(support.Client):
    def response(self):
        head = self.stream.readline()
        assert head.endswith(b"\r\n"), head
        assert not head.startswith(b"-"), head
        if head.startswith(b"*"):
            return [self.response() for _ in range(int(head[1:-2]))]
        if head.startswith(b"$"):
            size = int(head[1:-2])
            if size < 0:
                return None
            data = self.stream.read(size)
            assert self.stream.read(2) == b"\r\n"
            return data
        return head[1:-2]


def stats(client):
    return dict(
        line.split(b":", 1)
        for line in client.batch([[b"INFO", b"stats"]])[0].splitlines()
        if b":" in line
    )


def drain(client):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        values = stats(client)
        if all(
            values[key] == b"0"
            for key in (
                b"storage_dirty_staging_bytes",
                b"storage_flushes_pending",
                b"storage_tx_commits_pending",
            )
        ):
            return
        time.sleep(0.02)
    raise AssertionError("durability did not drain")


def check():
    keys = [b"remote-" + str(i).encode() for i in range(6000)]
    before = b"a" * 4096
    after = b"b" * 4096
    with support.server(2) as (admin, _):
        port = admin.socket.getpeername()[1]
        for batch in support.chunks(keys):
            assert admin.batch([[b"SET", key, before] for key in batch]) == [
                b"OK"
            ] * len(batch)
        drain(admin)
        for batch in support.chunks(keys):
            assert admin.batch([[b"GET", key] for key in batch]) == [before] * len(
                batch
            )
        # Hash tags select populated indexes while the complete keys differ.
        missing = [b"{" + key + b"}:missing" for key in keys[:100]]
        assert admin.batch([[b"GET", key] for key in missing]) == [None] * len(missing)

        def read():
            client = Client(port)
            try:
                for i in range(6000):
                    assert client.batch([[b"GET", keys[i % 1000]]])[0] in (
                        before,
                        after,
                    )
            finally:
                client.close()

        def write():
            client = Client(port)
            try:
                # Retire enough of the first sealed blocks to overlap reads
                # with the ordinary defrag admission and last-unpin retry.
                for i in range(4500):
                    key = keys[i]
                    value = after if i % 2 else before
                    assert client.batch(
                        [[b"MULTI"], [b"SET", key, value], [b"GET", key], [b"EXEC"]]
                    ) == [b"OK", b"QUEUED", b"QUEUED", [b"OK", value]]
            finally:
                client.close()

        with ThreadPoolExecutor(max_workers=5) as pool:
            tasks = [pool.submit(read) for _ in range(4)] + [pool.submit(write)]
            for task in tasks:
                task.result()

        # Clear while requests are arriving. Either generation is valid for
        # an overlapping GET, but stale bytes must never reappear after FLUSHDB.
        def read_during_flush():
            client = Client(port)
            try:
                for i in range(1000):
                    assert client.batch([[b"GET", keys[i]]])[0] in (None, before, after)
            finally:
                client.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            task = pool.submit(read_during_flush)
            assert admin.batch([[b"FLUSHDB"]]) == [b"OK"]
            task.result()
        for batch in support.chunks(keys[:1000]):
            assert admin.batch([[b"GET", key] for key in batch]) == [None] * len(batch)
        assert admin.batch([[b"SET", b"survivor", before]]) == [b"OK"]
        drain(admin)
    with support.server(2) as (admin, _):
        assert admin.batch([[b"GET", b"survivor"]]) == [before]
        for batch in support.chunks(keys[:1000]):
            assert admin.batch([[b"GET", key] for key in batch]) == [None] * len(batch)


def main():
    support.BINARY = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(
        prefix="lavik-remote-get-", dir=os.environ.get("LAVIK_TEST_DATA_DIR")
    ) as directory:
        support.WORKDIR = Path(directory)
        with (support.WORKDIR / "data").open("wb") as file:
            file.truncate(512 * 1024 * 1024)
        check()
    print("remote GET, concurrent transactions, FLUSHDB and cold recovery passed")


if __name__ == "__main__":
    main()
