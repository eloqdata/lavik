#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

"""Transfer a completed CTest build between runners with identical checkout paths."""

import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys


SOURCE_ROOT = Path(__file__).resolve().parent.parent
MANIFEST = "ci-build-bundle.json"


def checkout_identity(source_root, build_dir):
    """Bind non-relocatable test binaries and CMake metadata to their checkout."""
    revision = subprocess.check_output(
        ["git", "-C", str(source_root), "rev-parse", "HEAD"], text=True
    ).strip()
    return {
        "version": 1,
        "source_dir": str(source_root.resolve()),
        "build_dir": str(build_dir.resolve()),
        "revision": revision,
        "platform": sys.platform,
        "architecture": platform.machine(),
    }


def test_inventory(build_dir):
    """Detect missing discovery files or executables before a shard can skip them."""
    result = subprocess.check_output(
        ["ctest", "--test-dir", str(build_dir), "--show-only=json-v1"], text=True
    )
    tests = json.loads(result)["tests"]
    if not tests:
        raise ValueError("CTest inventory is empty; build all test targets first")
    inventory = []
    for test in tests:
        if test["name"].endswith("_NOT_BUILT") or not test.get("command"):
            raise ValueError(
                f"CTest executable or discovery file missing: {test['name']}"
            )
        # Backtrace indices describe CMake configuration, not runnable coverage.
        inventory.append(
            {key: value for key, value in test.items() if key != "backtrace"}
        )
    encoded = json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()
    return {"count": len(tests), "sha256": hashlib.sha256(encoded).hexdigest()}


def create_bundle(build_dir, archive, source_root=SOURCE_ROOT):
    """Archive runtime files and full debug symbols, preserving modes and links."""
    build_dir = build_dir.resolve()
    archive = archive.resolve()
    if archive.is_relative_to(build_dir):
        raise ValueError("Bundle destination must be outside the build directory")
    manifest = checkout_identity(source_root, build_dir)
    manifest["tests"] = test_inventory(build_dir)
    (build_dir / MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n")

    # CTest consumes generated .cmake files without reconfiguring or linking.
    # Preserve their directories, including empty fixture roots, and exclude
    # only compilation inputs/outputs. Runtime tools live under test_tools/.
    exclusions = (
        "*/CMakeFiles",
        "*.o",
        "*.a",
        "*/build.ninja",
        "*/.ninja_*",
        "*/CMakeCache.txt",
        "*/compile_commands.json",
        "*/_deps/*-src",
        "*/_deps/*-subbuild",
        f"{build_dir.name}/Testing",
        f"{build_dir.name}/test-results",
    )
    subprocess.run(
        [
            "tar",
            "--use-compress-program=zstd -T0 -3",
            "-cf",
            str(archive),
            *(f"--exclude={pattern}" for pattern in exclusions),
            "-C",
            str(build_dir.parent),
            "--",
            build_dir.name,
        ],
        check=True,
    )
    print(f"Created {archive} with {manifest['tests']['count']} CTest cases")


def validate_bundle(build_dir, source_root=SOURCE_ROOT):
    """Reject bundles from another path, commit, platform, or incomplete restore."""
    build_dir = build_dir.resolve()
    manifest = json.loads((build_dir / MANIFEST).read_text())
    for key, expected in checkout_identity(source_root, build_dir).items():
        if manifest.get(key) != expected:
            raise ValueError(
                f"Bundle {key} mismatch: saved {manifest.get(key)!r}, current {expected!r}"
            )
    inventory = test_inventory(build_dir)
    if manifest.get("tests") != inventory:
        raise ValueError("Restored CTest inventory differs from the completed build")
    print(f"Validated {build_dir}: {inventory['count']} CTest cases")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser(
        "create", help="create an archive from a completed build"
    )
    create.add_argument("build_dir", type=Path)
    create.add_argument("archive", type=Path)
    validate = commands.add_parser(
        "validate", help="validate after extracting the archive"
    )
    validate.add_argument("build_dir", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "create":
            create_bundle(args.build_dir, args.archive)
        else:
            validate_bundle(args.build_dir)
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"CI build bundle: {error}\n")


if __name__ == "__main__":
    main()
