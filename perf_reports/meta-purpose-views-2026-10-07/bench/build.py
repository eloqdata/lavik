#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Link task-only measurement objects against a configured Release build."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--legacy", action="store_true")
    args = parser.parse_args()
    source, build, output = (
        p.resolve() for p in (args.source, args.build, args.output)
    )
    output.mkdir(parents=True, exist_ok=True)
    bench = Path(__file__).resolve().parent
    entries = json.loads((build / "compile_commands.json").read_text())
    hashes = {}
    objects = []
    for name in ("state_machine", "admin_views", "failover_views"):
        path = source / "src/meta" / f"{name}.cpp"
        if not path.exists():
            continue
        original = path.read_text()
        # Only guards over the state machine's mutex are instrumented. The
        # actual mutex and lock order are unchanged; consumers are not hooked.
        patched, count = re.subn(
            r"std::(?:lock_guard(?:<std::mutex>)?|unique_lock(?:<std::mutex>)?)\s+(\w+)\(mutex_\);",
            r"measurement::Lock \1(mutex_);",
            original,
        )
        if count == 0:
            raise RuntimeError(f"No state locks found in {path}")
        generated = output / f"{name}.cpp"
        generated.write_text('#include "metrics.h"\n' + patched)
        hashes[str(path)] = {
            "sha256": hashlib.sha256(original.encode()).hexdigest(),
            "locks": count,
        }
        entry = next(e for e in entries if e["file"] == str(path))
        command = shlex.split(entry["command"])
        obj = output / f"{name}.o"
        command[command.index("-o") + 1] = str(obj)
        command[-1] = str(generated)
        command += [f"-I{bench}", f"-I{source / 'src/meta'}"]
        subprocess.run(command, cwd=build, check=True)
        objects.append(str(obj))
    entry = next(
        e for e in entries if e["file"] == str(source / "src/meta/state_machine.cpp")
    )
    for name in ("component", "alloc"):
        command = shlex.split(entry["command"])
        obj = output / f"{name}.o"
        command[command.index("-o") + 1] = str(obj)
        command[-1] = str(bench / f"{name}.cpp")
        command += [f"-I{bench}"]
        if args.legacy:
            command += ["-DLEGACY=1"]
        subprocess.run(command, cwd=build, check=True)
        objects.insert(0, str(obj))
    lines = subprocess.check_output(
        ["ninja", "-C", str(build), "-t", "commands", "lavik-meta"], text=True
    ).splitlines()
    command = shlex.split(lines[-1])
    if command[:2] != [":", "&&"] or command[-2:] != ["&&", ":"]:
        raise RuntimeError("Unexpected Ninja link command")
    command = command[2:-2]
    command[command.index("-o") + 1] = str(output / "component")
    index = command.index("CMakeFiles/lavik-meta.dir/app/lavik_meta.cpp.o")
    command[index : index + 1] = objects
    subprocess.run(command, cwd=build, check=True)
    (output / "provenance.json").write_text(
        json.dumps(
            {"sources": hashes, "link": command, "legacy": args.legacy}, indent=2
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
