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

"""Redis Sentinel 7.2 wire contract, independent of Lavik's reply parser.

Replay checked-in reference responses against Redis itself:
  python3 sentinel_compat.py /path/to/redis-7.2.14/src/redis-server

The fixture records actual Redis 7.2.14 Sentinel replies. Only successful
HELLO's product name, version and connection ID are normalized; framing, field
order/types and all error bytes remain exact. QUIT/RESET, unsupported commands,
malformed RESP, resource limits and embedded-NUL attributes have separate Lavik
contracts and are deliberately not represented as Redis-equal behavior.
"""

import argparse
import json
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import time

import harness as H

FIXTURE = Path(__file__).with_name("sentinel_redis72.json")


def encode(args):
    parts = [value.encode("latin1") for value in args]
    return f"*{len(parts)}\r\n".encode() + b"".join(
        f"${len(value)}\r\n".encode() + value + b"\r\n" for value in parts
    )


def frame(stream):
    line = stream.readline()
    if not line.endswith(b"\r\n"):
        raise AssertionError(f"truncated RESP line: {line!r}")
    kind = line[:1]
    if kind == b"$":
        size = int(line[1:-2])
        body = stream.read(size + 2) if size >= 0 else b""
        if size >= 0 and (len(body) != size + 2 or not body.endswith(b"\r\n")):
            raise AssertionError("truncated RESP bulk string")
        return line + body
    if kind in (b"*", b"%"):
        count = int(line[1:-2]) * (2 if kind == b"%" else 1)
        return line + b"".join(frame(stream) for _ in range(count))
    if kind not in (b"+", b"-", b":", b"_"):
        raise AssertionError(f"unexpected RESP type: {kind!r}")
    return line


def normalize_hello(reply):
    # Match the complete HELLO shape before masking its three variable values.
    # Requiring bulk strings / an integer here prevents normalization from
    # hiding a type, field-order or RESP2-versus-RESP3 regression.
    pattern = (
        rb"(\*12|%6)\r\n\$6\r\nserver\r\n\$\d+\r\n([^\r\n]*)\r\n"
        rb"\$7\r\nversion\r\n\$\d+\r\n([^\r\n]*)\r\n"
        rb"\$5\r\nproto\r\n:([23])\r\n\$2\r\nid\r\n:([0-9]+)\r\n"
        rb"\$4\r\nmode\r\n\$8\r\nsentinel\r\n\$7\r\nmodules\r\n\*0\r\n"
    )
    match = re.fullmatch(pattern, reply)
    if not match:
        raise AssertionError(f"unexpected HELLO shape: {reply!r}")
    for value in match.group(2), match.group(3):
        token = f"${len(value)}\r\n".encode() + value + b"\r\n"
        if token not in reply:
            raise AssertionError("invalid HELLO bulk length")
    reply = reply.replace(
        f"${len(match.group(2))}\r\n".encode() + match.group(2) + b"\r\n",
        b"$8\r\n<server>\r\n",
        1,
    )
    reply = reply.replace(
        f"${len(match.group(3))}\r\n".encode() + match.group(3) + b"\r\n",
        b"$9\r\n<version>\r\n",
        1,
    )
    return reply.replace(
        b"$2\r\nid\r\n:" + match.group(5) + b"\r\n", b"$2\r\nid\r\n:0\r\n", 1
    )


def replay(port, case):
    with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
        with sock.makefile("rb") as stream:
            replies = []
            for args in case["requests"]:
                sock.sendall(encode(args))
                reply = frame(stream)
                if args[0].upper() == "HELLO" and reply[:1] != b"-":
                    reply = normalize_hello(reply)
                replies.append(reply.decode("latin1"))
            return replies


def check_port(test, port, password):
    fixture = json.loads(FIXTURE.read_text())
    for case in fixture["cases"]:
        if case["password"] == password:
            with test.subTest(contract=case["name"]):
                test.assertEqual(replay(port, case), case["replies"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("redis_server")
    parser.add_argument(
        "--record",
        action="store_true",
        help="replace replies from the pinned reference; review fixture diff",
    )
    args = parser.parse_args()
    version = subprocess.check_output([args.redis_server, "--version"], text=True)
    if "v=7.2.14 " not in version:
        parser.error("reference fixture requires Redis 7.2.14")
    fixture = json.loads(FIXTURE.read_text())
    with tempfile.TemporaryDirectory(prefix="redis-sentinel-contract-") as root:
        for password in ("sentinel-secret", ""):
            port = H.free_port()
            conf = Path(root) / "sentinel.conf"
            conf.write_text(
                f"port {port}\nbind 127.0.0.1\nprotected-mode no\ndir {root}\n"
                + (f"requirepass {password}\n" if password else "")
            )
            with (Path(root) / "redis.log").open("w") as log:
                process = subprocess.Popen(
                    [args.redis_server, str(conf), "--sentinel"], stdout=log, stderr=log
                )
                try:
                    deadline = time.monotonic() + 5
                    while True:
                        if process.poll() is not None:
                            raise AssertionError(
                                "reference Sentinel exited during startup"
                            )
                        try:
                            with socket.create_connection(
                                ("127.0.0.1", port), timeout=0.2
                            ):
                                break
                        except OSError:
                            if time.monotonic() >= deadline:
                                raise
                            time.sleep(0.02)
                    for case in fixture["cases"]:
                        if case["password"] != password:
                            continue
                        replies = replay(port, case)
                        if args.record:
                            case["replies"] = replies
                        elif replies != case["replies"]:
                            raise AssertionError(
                                f"reference drift in {case['name']}: {replies!r}"
                            )
                finally:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
    if args.record:
        text = json.dumps(fixture, indent=2, ensure_ascii=True)
        # Keep each request on one line so its arguments are easy to inspect.
        for case in fixture["cases"]:
            for request in case["requests"]:
                expanded = json.dumps(request, indent=2).replace("\n", "\n        ")
                text = text.replace(expanded, json.dumps(request))
        FIXTURE.write_text(text + "\n")
    print(f"Redis 7.2.14 Sentinel: {len(fixture['cases'])} wire scenarios verified")


if __name__ == "__main__":
    main()
