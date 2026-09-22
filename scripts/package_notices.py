#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

"""Collect release notices from pinned sources and the Ubuntu build toolchain.

This is an explicit packaging manifest, not a license classifier. When adding
a native dependency, update the manifest below. Within each enabled native
dependency we conservatively retain source notices, including code the linker
may discard. Go dependencies come from the same package/toolchain as the Meta
archive. Missing required inputs fail packaging instead of emitting a partial
notice file. No network license lookup is performed during packaging.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess


SOURCE_SUFFIXES = {".c", ".cc", ".cpp", ".h", ".hpp", ".inc", ".go", ".s", ".S", ".asm"}
LEGAL_NAME = re.compile(r"^(LICENSE|LICENCE|COPYING|COPYRIGHT|NOTICE|PATENTS)([.-].*)?$", re.I)
# Skip quoted strings before matching comments, so comment-like literals in
# parsers and tests do not turn program text into a purported license notice.
COMMENTS = re.compile(
    r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|'
    r"/\*.*?\*/|//[^\n]*(?:\n[ \t]*//[^\n]*)*", re.S
)
NOTICE_MARKER = re.compile(
    r"copyright|SPDX-License-Identifier|permission is hereby granted|"
    r"redistribution and use|licensed under", re.I
)


def run(*args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def read(path):
    text = path.read_text(encoding="utf-8", errors="replace").strip()
    if not text:
        raise ValueError(f"Empty notice input: {path}")
    return text


def source_notices(path):
    """Return original legal comments, including licenses at the end of files."""
    # Empty source stubs are legitimate (for example FreeBSD's trap.h);
    # required license files, unlike source inputs, must not be empty.
    content = path.read_text(encoding="utf-8", errors="replace")
    for match in COMMENTS.finditer(content):
        text = match.group()
        if text.startswith(("/*", "//")) and NOTICE_MARKER.search(text):
            yield text
    if path.suffix == ".asm":
        for match in re.finditer(r"^[ \t]*;[^\n]*(?:\n[ \t]*;[^\n]*)*", content, re.M):
            if NOTICE_MARKER.search(match.group()):
                yield match.group()


class Notices:
    """Deduplicate exact texts while retaining their component/source attribution."""

    def __init__(self, apache_license):
        self.apache = self.normalize(apache_license)
        self.apache_terms = self.normalize(apache_license.split("END OF TERMS AND CONDITIONS")[0])
        self.entries = {}

    @staticmethod
    def normalize(text):
        return " ".join(text.replace("https://", "http://").split())

    def add(self, origin, text):
        # Only substitute identical terms. Keep customized appendices and
        # additional copyright/NOTICE content even when the terms are shared.
        reference = "Apache License, Version 2.0. See LICENSE in this archive."
        if self.normalize(text) == self.apache:
            text = reference
        elif "END OF TERMS AND CONDITIONS" in text:
            terms, appendix = text.split("END OF TERMS AND CONDITIONS", 1)
            if self.normalize(terms) == self.apache_terms:
                text = reference + "\n\n" + appendix.strip()
        self.entries.setdefault(text.strip(), set()).add(origin)

    def file(self, origin, path):
        self.add(origin, read(path))

    def sources(self, origin, path):
        if not path.exists():
            raise FileNotFoundError(path)
        files = [path] if path.is_file() else sorted(
            p for p in path.rglob("*") if p.suffix in SOURCE_SUFFIXES
            and p.is_file() and not any(part.startswith(".") for part in p.relative_to(path).parts)
        )
        for source in files:
            label = origin if path.is_file() else f"{origin}/{source.relative_to(path)}"
            for notice in source_notices(source):
                self.add(label, notice)

    def legal_files(self, origin, root):
        for path in sorted(root.iterdir()):
            if path.is_file() and LEGAL_NAME.match(path.name):
                self.file(f"{origin}/{path.relative_to(root)}", path)

    def render(self):
        sections = [
            "Lavik — project and third-party notices\n\n"
            "Apache-2.0 terms are provided in the accompanying LICENSE file.\n"
            "Other license terms and upstream attributions follow. Notices are\n"
            "retained conservatively within enabled dependency source trees;\n"
            "this document is not an inventory of linked objects.\n"
            "Upstream notices may mention source paths or optional components."
        ]
        for text, origins in self.entries.items():
            sections.append("\n".join(sorted(origins)) + "\n\n" + text)
        return "\n\n".join("=" * 72 + "\n" + s for s in sections) + "\n"


def native_notices(notices, root, bypass):
    # The full spdlog license was omitted from the vendored header-only copy.
    # scripts/licenses/spdlog-LICENSE is upstream v1.14.1's license text:
    # https://github.com/gabime/spdlog/blob/v1.14.1/LICENSE
    required = [
        "NOTICE", "bycorf/LICENSE", "bycorf/NOTICE",
        "bycorf/third_party/abseil/LICENSE", "third_party/mimalloc/LICENSE",
        "bycorf/third_party/liburing/LICENSE", "third_party/valkey/COPYING",
        "scripts/licenses/spdlog-LICENSE",
        "include/spdlog/fmt/bundled/fmt.license.rst",
        "bycorf/third_party/spdlog/fmt/bundled/fmt.license.rst",
    ]
    sources = [
        "bycorf/third_party/abseil/absl", "third_party/mimalloc/include",
        "third_party/mimalloc/src", "bycorf/third_party/liburing/src",
        "third_party/lua/src", "include/lavik/CLI11.hpp",
        "include/lavik/storage/scan_hash_map.h", "include/spdlog",
        "bycorf/third_party/spdlog", "bycorf/include/bycorf/runtime/concurrentqueue.h",
    ]
    if bypass:
        required += [
            "bycorf/third_party/freebsd/COPYRIGHT",
            "bycorf/third_party/spdk/LICENSE",
            "bycorf/third_party/spdk/licenses/bsd-2-clause.txt",
            "bycorf/third_party/spdk/licenses/bsd-3-clause.txt",
            "bycorf/third_party/spdk/isa-l/LICENSE",
            "bycorf/third_party/spdk/isa-l-crypto/LICENSE",
        ]
        # DPDK uses SPDX-only headers; include its complete license directory.
        for path in sorted((root / "bycorf/third_party/dpdk/license").glob("*")):
            if path.is_file():
                required.append(str(path.relative_to(root)))
        required.append("bycorf/third_party/dpdk/license/bsd-3-clause.txt")
        sources += [
            "bycorf/third_party/freebsd/sys", "bycorf/src/io/freebsd",
            "bycorf/third_party/dpdk/lib", "bycorf/third_party/dpdk/drivers",
            "bycorf/third_party/spdk/include", "bycorf/third_party/spdk/lib",
            "bycorf/third_party/spdk/isa-l", "bycorf/third_party/spdk/isa-l-crypto",
        ]
    for name in required:
        notices.file(name, root / name)
    for name in sources:
        notices.sources(name, root / name)


def json_objects(text):
    decoder = json.JSONDecoder()
    while text.strip():
        value, end = decoder.raw_decode(text.lstrip())
        yield value
        text = text.lstrip()[end:]


def read_cache(path):
    """Read CMake cache values without consuming comments or blank lines."""
    return dict(re.findall(r"^([^/#\s][^:=\r\n]*):[^=\r\n]+=(.*)$", read(path), re.M))


def go_notices(notices, root, executable, cc):
    toolchain = re.search(r"^toolchain (\S+)$", read(root / "raft/go.mod"), re.M)
    if not toolchain:
        raise ValueError("raft/go.mod must pin the Go toolchain")
    env = dict(os.environ, GOTOOLCHAIN=toolchain[1], CGO_ENABLED="1", CC=cc)
    kwargs = {"cwd": root / "raft", "env": env}
    goroot = Path(run(executable, "env", "GOROOT", **kwargs))
    notices.file(f"Go {toolchain[1]}/LICENSE", goroot / "LICENSE")
    packages = list(json_objects(run(
        executable, "list", "-mod=readonly", "-deps", "-json", "./bridge", **kwargs
    )))
    modules = {}
    legal_directories = set()
    for package in packages:
        module = package.get("Module")
        if module:
            if module.get("Main"):
                continue
            if "Replace" in module:
                raise ValueError("Release notices do not support replaced Go modules")
            modules[module["Path"]] = module
        boundary = Path(module["Dir"]) if module else goroot
        prefix = f"{module['Path']}@{module['Version']}" if module else f"Go {toolchain[1]}"
        directory = Path(package["Dir"])
        # Retain package-local and inherited notices, without pulling licenses
        # from unrelated tools or test packages in the Go source distribution.
        while directory.is_relative_to(boundary):
            relative = directory.relative_to(boundary)
            origin = prefix if directory == boundary else f"{prefix}/{relative}"
            legal_directories.add((origin, directory))
            if directory == boundary:
                break
            directory = directory.parent
        for field in ("GoFiles", "CgoFiles", "CFiles", "HFiles", "SFiles"):
            for name in package.get(field, []):
                notices.sources(f"Go/{package['ImportPath']}/{name}", Path(package["Dir"]) / name)
    for name, module in sorted(modules.items()):
        directory = Path(module["Dir"])
        if not any(p.is_file() and p.name.upper().startswith(("LICENSE", "COPYING"))
                   for p in directory.iterdir()):
            raise ValueError(f"No license for Go module {name}")
    for origin, directory in sorted(legal_directories):
        notices.legal_files(origin, directory)


def system_notices(notices, cache):
    # Resolve the packages owning the selected archives, not the host's default
    # OpenSSL/compiler version. Official releases use Ubuntu-provided archives.
    cxx = cache["CMAKE_CXX_COMPILER"]
    archives = [cache["OPENSSL_CRYPTO_LIBRARY"], cache["OPENSSL_SSL_LIBRARY"]]
    archives += [run(cxx, f"-print-file-name={name}") for name in ("libstdc++.a", "libgcc.a")]
    packages = set()
    for archive in archives:
        path = Path(archive)
        if not path.is_absolute() or not path.is_file():
            raise ValueError(f"Cannot locate static dependency: {archive}")
        owner = run("dpkg-query", "-S", str(path.resolve())).splitlines()
        if len(owner) != 1:
            raise ValueError(f"Ambiguous package owner: {archive}")
        packages.add(owner[0].rsplit(": ", 1)[0])
    for package in sorted(packages):
        version = run("dpkg-query", "-W", "-f=${Version}", package)
        path = Path("/usr/share/doc") / package.split(":")[0] / "copyright"
        content = read(path)
        notices.add(f"Ubuntu {package} {version}/copyright", content)
        # Debian copyright files reference common license files rather than
        # embedding all terms. Resolve those references inside the archive too.
        for license_name in sorted({name.rstrip(".") for name in re.findall(
                r"/usr/share/common-licenses/([A-Za-z0-9.+-]+)", content)}):
            notices.file(f"Common license: {license_name}", Path("/usr/share/common-licenses") / license_name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    cache = read_cache(args.build_dir / "CMakeCache.txt")
    bypass = cache["LAVIK_KERNEL_BYPASS"]
    if bypass not in ("ON", "OFF"):
        raise ValueError("Expected a release build with an explicit bypass variant")
    notices = Notices(read(root / "LICENSE"))
    native_notices(notices, root, bypass == "ON")
    go_notices(notices, root, cache["LAVIK_GO_EXECUTABLE"], cache["CMAKE_C_COMPILER"])
    system_notices(notices, cache)
    # Do not leave a partially populated output if collection fails.
    args.output.write_text(notices.render(), encoding="utf-8")


if __name__ == "__main__":
    main()
