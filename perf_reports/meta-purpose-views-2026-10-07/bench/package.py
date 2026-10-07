#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Package this acceptance run's text evidence, excluding binaries and data files."""

import argparse
import hashlib
from pathlib import Path
import tarfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    if not root.is_relative_to("/mnt/local_nvme"):
        raise ValueError("evidence root must be under /mnt/local_nvme")
    paths = set()
    directories = [root / "measurements", root / "measurements-initial"]
    directories += sorted(root.glob("process-*"))
    directories += sorted(root.glob("strict-smoke*"))
    directories += [root / "evidence/bench-aggressive", root / "evidence/bench-unpaced"]
    suffixes = {".json", ".csv", ".log", ".toml", ".py", ".h", ".cpp"}
    for directory in directories:
        paths.update(
            path
            for path in directory.rglob("*")
            if path.is_file() and path.suffix in suffixes
        )
    for name in (
        "acceptance.xml",
        "final-focused.xml",
        "aggregate-inventory.txt",
        "child-prs.json",
        "main-ci.json",
        "issue256-state.json",
        "pr257.json",
        "pr277.json",
        "pr278.json",
        "pr279.json",
        "pr281.json",
        "pr284.json",
        "pr284-failed.log",
    ):
        paths.add(root / "evidence" / name)
    for label in ("before", "after"):
        paths.add(root / f"bench-{label}/provenance.json")
    paths.add(root / "env.sh")
    for name in ("acceptance.log", "final-focused.log"):
        paths.add(root / "logs" / name)
    paths = sorted(paths)
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = args.output / "raw-SHA256SUMS"
    manifest.write_text(
        "".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(root)}\n"
            for path in paths
        )
    )
    archive = args.output / "evidence.tar.gz"
    with tarfile.open(archive, "w:gz") as output:
        for path in paths:
            output.add(path, arcname=str(path.relative_to(root)), recursive=False)
    (args.output / "evidence.tar.gz.sha256").write_text(
        f"{hashlib.sha256(archive.read_bytes()).hexdigest()}  evidence.tar.gz\n"
    )
    print(f"Packaged {len(paths)} files, {archive.stat().st_size} bytes")


if __name__ == "__main__":
    main()
