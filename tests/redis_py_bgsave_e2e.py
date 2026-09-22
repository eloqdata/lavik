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

"""Exercise redis-py's default BGSAVE SCHEDULE request against Lavik."""

import os
import pathlib
import socket
import subprocess
import sys
import tempfile
import time

import redis


def reserve_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def main() -> None:
    lavik_bin = pathlib.Path(sys.argv[1]).resolve()
    with tempfile.TemporaryDirectory(
        prefix="lavik-redis-py-bgsave-",
        dir=os.environ.get("LAVIK_TEST_DATA_DIR"),
    ) as raw_dir:
        case_dir = pathlib.Path(raw_dir)
        data_file = case_dir / "lavik.data"
        with data_file.open("wb") as output:
            output.truncate(1024 * 1024 * 1024)

        port = reserve_port()
        log = (case_dir / "lavik.log").open("wb")
        server = subprocess.Popen(
            [
                str(lavik_bin),
                "--logtostderr",
                "--port",
                str(port),
                "--threads",
                "2",
                "--no-pin-workers",
                "--recv-buffers-per-worker",
                "0",
                "--data-file",
                str(data_file),
                "--rdb-dir",
                str(case_dir),
                "--dbfilename",
                "redis-py.rdb",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        client = redis.Redis(host="127.0.0.1", port=port)
        failed = False
        try:
            deadline = time.monotonic() + 10
            while True:
                try:
                    if client.ping():
                        break
                except redis.ConnectionError:
                    pass
                if time.monotonic() >= deadline:
                    raise RuntimeError("Lavik did not become ready")
                time.sleep(0.01)

            client.set("redis-py-bgsave", "value")
            # redis-py 8.1.0 defaults schedule=True and therefore emits the
            # two-word BGSAVE SCHEDULE form that motivated this regression.
            assert client.bgsave() is True
            deadline = time.monotonic() + 30
            output = case_dir / "redis-py.rdb"
            while not output.exists() or output.stat().st_size == 0:
                if time.monotonic() >= deadline:
                    raise RuntimeError("BGSAVE did not produce an RDB file")
                time.sleep(0.01)
        except Exception:
            failed = True
            raise
        finally:
            client.close()
            server.kill()
            server.wait(timeout=10)
            log.close()
            if failed:
                print((case_dir / "lavik.log").read_text(errors="replace"),
                      file=sys.stderr)


if __name__ == "__main__":
    main()
