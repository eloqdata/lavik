#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
"""Stateless SSH helper. Persist ownership before provisioning, never erase data."""

import base64
import ctypes
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time

LIMIT = 8 * 1024 * 1024


def run(argv, timeout=15):
    """Run fixed argv without a shell; preserve uncertain command outcomes."""
    result = subprocess.run(argv, capture_output=True, timeout=timeout)
    if len(result.stdout) + len(result.stderr) > LIMIT:
        raise RuntimeError("Remote command exceeded output limit")
    return {
        "code": result.returncode,
        "stdout": result.stdout.decode(errors="replace").strip(),
        "stderr": result.stderr.decode(errors="replace").strip(),
    }


def root_path(request):
    base = Path(request["baseDir"]).expanduser()
    if not base.is_absolute():
        base = Path.home() / base
    root = base / request["cluster"]
    # An operator-owned directory must not redirect writes through a symlink.
    for part in [root, *root.parents]:
        if part.is_symlink():
            raise RuntimeError(f"Deployment path contains a symlink: {part}")
    return root


def owned(request, create=False):
    root = root_path(request)
    marker = root / "deployment.json"
    if not root.exists() and create:
        root.mkdir(parents=True, mode=0o700)
        marker.write_text(json.dumps({"owner": request["owner"]}))
        marker.chmod(0o600)
    if (
        not marker.is_file()
        or json.loads(marker.read_text()).get("owner") != request["owner"]
    ):
        raise RuntimeError(f"Refusing unowned deployment directory: {root}")
    return root


def write_once(path, content, mode=0o600):
    """A retry can reuse identical configuration, never replace reviewed settings."""
    if path.exists():
        if path.read_text() != content:
            raise RuntimeError(f"Existing configuration differs: {path}")
        return
    with path.open("x") as output:
        output.write(content)
    path.chmod(mode)


def probe(request):
    errors = []
    arch = {"arm64": "aarch64", "amd64": "x86_64"}.get(
        platform.machine(), platform.machine()
    )
    if platform.system() != "Linux" or arch not in ("aarch64", "x86_64"):
        errors.append("Hosts must run Linux on AMD64 or ARM64")
    kernel = tuple(int(p) for p in platform.release().split("-")[0].split(".")[:2])
    if kernel < (6, 1):
        errors.append("Linux 6.1 or newer is required")
    libc_name, libc_version = platform.libc_ver()
    if libc_name != "glibc" or tuple(map(int, libc_version.split("."))) < (2, 39):
        errors.append("Use Ubuntu 24.04 or a compatible glibc 2.39+ host")
    if arch in ("aarch64", "x86_64"):
        libc = ctypes.CDLL(None, use_errno=True)
        params = ctypes.create_string_buffer(256)
        fd = libc.syscall(425, 2, ctypes.byref(params))
        if fd < 0:
            errors.append(
                f"io_uring is unavailable (errno {ctypes.get_errno()}); check host/container policy"
            )
        else:
            os.close(fd)
    root = root_path(request)
    if root.exists():
        if request.get("allowOwned"):
            owned(request)
        else:
            errors.append(
                f"Deployment directory already exists: {root}; resume the existing deployment instead"
            )
    parent = root.parent
    while not parent.exists():
        parent = parent.parent
    free = shutil.disk_usage(parent).free
    if not os.access(parent, os.W_OK):
        errors.append(f"SSH user cannot write to {parent}")
    if free < request.get("bytes", 0) + 1024**3:
        errors.append("Insufficient disk space for the data files plus 1 GiB reserve")
    errors.extend(supervision_errors(request.get("supervisor")))
    for port in request.get("ports", []):
        try:
            with socket.socket(
                socket.AF_INET6 if ":" in request["address"] else socket.AF_INET
            ) as listener:
                listener.bind((request["address"], port))
        except OSError as error:
            errors.append(
                f"Address/port {request['address']}:{port} is unavailable: {error}"
            )
    return {
        "ok": not errors,
        "errors": errors,
        "arch": arch,
        "kernel": platform.release(),
        "cpus": os.cpu_count(),
        "freeBytes": free,
        "directory": str(root),
        "systemdAvailable": systemd_available(),
    }


def systemd_available():
    """Report host capability separately from the operator's selected lifecycle."""
    return bool(shutil.which("systemctl") and Path("/run/systemd/system").is_dir())


def supervision_errors(supervisor):
    """Validate the requested lifecycle; never silently drop boot persistence."""
    if supervisor != "systemd":
        return []
    # Installing systemctl or enabling lingering cannot create a system manager
    # in an ordinary SSH-only container. Give the lab's actual remedy first.
    if not systemd_available():
        return [
            "Systemd service management is unavailable on this host. "
            "For Docker/lab hosts, use the Service lifecycle setting in Node placement, "
            "choose Development processes (no automatic restart), and check hosts again."
        ]
    errors = []
    if run(["systemctl", "--user", "show-environment"])["code"]:
        errors.append(
            "A systemd user manager is required; enable lingering and reconnect over SSH"
        )
    if not shutil.which("loginctl"):
        errors.append(
            "loginctl is required to verify systemd boot persistence on this host"
        )
    elif (
        "Linger=yes"
        not in run(["loginctl", "show-user", str(os.getuid()), "-p", "Linger"])[
            "stdout"
        ]
    ):
        errors.append(
            "Enable boot persistence once: sudo loginctl enable-linger "
            + platform_user()
        )
    return errors


def platform_user():
    import pwd

    return pwd.getpwuid(os.getuid()).pw_name


def prepare(request):
    root = owned(request, create=True)
    asset = request["asset"]
    release = root / "release"
    if release.exists():
        if (
            json.loads((release / "asset.json").read_text())["sha256"]
            != asset["sha256"]
        ):
            raise RuntimeError(
                "This deployment already has a different release; refusing replacement"
            )
    else:
        if not asset["url"].startswith(
            "https://github.com/eloqdata/lavik/releases/download/"
        ):
            raise RuntimeError("Only official Lavik release assets are accepted")
        with tempfile.TemporaryDirectory(dir=root, prefix="download-") as temporary:
            archive = root / (".incoming-" + asset["sha256"])
            digest = hashlib.sha256()
            total = 0
            with archive.open("rb") as response:
                while chunk := response.read(1024 * 1024):
                    total += len(chunk)
                    if total > 1024**3:
                        raise RuntimeError("Release archive exceeds 1 GiB")
                    digest.update(chunk)
            if digest.hexdigest() != asset["sha256"]:
                raise RuntimeError(
                    "Release checksum changed; review a fresh plan (nightly may have advanced)"
                )
            stage = Path(temporary) / "stage"
            stage.mkdir()
            names = {
                "lavik",
                "lavik-meta",
                "lavik-ctl",
                "VERSION",
                "REVISION",
                "LICENSE",
                "NOTICE",
                "THIRD_PARTY_NOTICES",
                "OPENSSL-LICENSE.txt",
            }
            with tarfile.open(archive, "r:gz") as source:
                for member in source:
                    parts = Path(member.name).parts
                    if len(parts) != 2 or parts[1] not in names:
                        continue
                    if not member.isfile() or member.size > 512 * 1024 * 1024:
                        raise RuntimeError(
                            "Invalid executable or notice in release archive"
                        )
                    target = stage / parts[1]
                    with target.open("xb") as output:
                        shutil.copyfileobj(source.extractfile(member), output)
                    target.chmod(
                        0o755
                        if parts[1] in ("lavik", "lavik-meta", "lavik-ctl")
                        else 0o644
                    )
            for name in (
                "lavik",
                "lavik-meta",
                "lavik-ctl",
                "VERSION",
                "REVISION",
                "LICENSE",
            ):
                if not (stage / name).is_file():
                    raise RuntimeError(f"Release is missing {name}")
            (stage / "asset.json").write_text(json.dumps(asset))
            stage.rename(release)
            archive.unlink(missing_ok=True)
    # A repeated install can upload the same archive again. Its installed
    # release digest was checked above; do not retain an unused second copy.
    (root / (".incoming-" + asset["sha256"])).unlink(missing_ok=True)
    help_result = run([str(release / "lavik"), "--help"])
    if help_result["code"]:
        raise RuntimeError(help_result["stderr"] or "Release cannot run on this host")
    return {
        "modern": "--meta-seed" in help_result["stdout"],
        "version": (release / "VERSION").read_text().strip(),
        "revision": (release / "REVISION").read_text().strip(),
    }


def start_node(request):
    root = owned(request)
    node = request["node"]
    directory = root / node["name"]
    directory.mkdir(mode=0o700, exist_ok=True)
    if node["kind"] == "data" and not (directory / "lavik.data").exists():
        # Exclusive allocation survives interruption without ever truncating a
        # previous population. Publish the file only after allocation succeeds.
        fd, temporary = tempfile.mkstemp(dir=directory, prefix="allocate-")
        try:
            os.posix_fallocate(fd, 0, request["dataGiB"] * 1024**3)
            os.fsync(fd)
            os.link(temporary, directory / "lavik.data")
        finally:
            os.close(fd)
            os.unlink(temporary)
    write_once(root / "cluster.toml", request["manifest"])
    argv = [
        str(root / "release" / ("lavik-meta" if node["kind"] == "meta" else "lavik")),
        *node["args"],
    ]
    argv = [a.replace("@ROOT@", str(root)) for a in argv]
    # Restart bootstrap follows the release's durable Meta marker. This wrapper
    # is also used by systemd on host reboot, not just by the initial SSH call.
    launch = directory / "launch.py"
    launch_source = (
        "#!/usr/bin/env python3\nimport os\nfrom pathlib import Path\nargv = "
        + repr(argv)
        + "\n"
    )
    if node["kind"] == "meta":
        launch_source += f"if not any((Path({str(directory / 'state')!r}) / name).exists() for name in ('RAFT', 'cluster_config.dat')):\n    argv += ['--initial-cluster-manifest', {str(root / 'cluster.toml')!r}]\n"
    launch_source += "os.execv(argv[0], argv)\n"
    write_once(launch, launch_source, 0o700)
    if request["supervisor"] == "systemd":
        units = Path.home() / ".config/systemd/user"
        units.mkdir(parents=True, exist_ok=True)
        unit_name = f"lavik-{request['cluster']}-{node['name']}.service"
        # Paths are validated by Admin; % is escaped for systemd specifiers.
        unit = (
            "[Unit]\nDescription=Lavik "
            + request["cluster"]
            + " "
            + node["name"]
            + "\nAfter=network-online.target\n[Service]\nType=simple\nExecStart=/usr/bin/python3 "
            + str(launch).replace("%", "%%")
            + "\nRestart=on-failure\nRestartSec=3\nTimeoutStopSec=60\nLimitNOFILE=65536\n"
            + "[Install]\nWantedBy=default.target\n"
        )
        write_once(units / unit_name, unit)
        for args in (["daemon-reload"], ["enable", "--now", unit_name]):
            result = run(["systemctl", "--user", *args], 30)
            if result["code"]:
                raise RuntimeError(result["stderr"])
        return {"service": unit_name}
    pid_file = directory / "process.json"
    boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    if pid_file.exists():
        identity = json.loads(pid_file.read_text())
        proc = Path(f"/proc/{identity['pid']}")
        if (
            identity.get("boot") == boot
            and proc.exists()
            and (proc / "stat").read_text().split()[21] == identity["start"]
        ):
            return {"pid": identity["pid"], "existing": True}
    with (directory / "console.log").open("ab", buffering=0) as output:
        child = subprocess.Popen(
            [sys.executable, str(launch)],
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=output,
            start_new_session=True,
            close_fds=True,
        )
    identity = {
        "pid": child.pid,
        "boot": boot,
        "start": Path(f"/proc/{child.pid}/stat").read_text().split()[21],
    }
    pid_file.write_text(json.dumps(identity))
    time.sleep(0.15)
    if child.poll() is not None:
        raise RuntimeError(
            (directory / "console.log").read_text(errors="replace")[-3000:]
        )
    return identity


def main(request):
    action = request["action"]
    if action == "probe":
        return probe(request)
    if action == "prepare":
        return prepare(request)
    if action == "reserve":
        return {"directory": str(owned(request, create=True))}
    if action == "start":
        return start_node(request)
    root = owned(request)
    if action == "ctl":
        args = request["args"]
        if "--manifest" in args:
            args[args.index("--manifest") + 1] = str(root / "cluster.toml")
        return run([str(root / "release/lavik-ctl"), *args], 12)
    if action == "connectivity":
        pending = request["endpoints"]
        deadline = time.monotonic() + 20
        # systemd can acknowledge a started unit before its listeners bind.
        while pending:
            failures = []
            for endpoint in pending:
                try:
                    with socket.create_connection(
                        (endpoint["host"], endpoint["port"]), timeout=1
                    ):
                        pass
                except OSError:
                    failures.append(endpoint)
            if not failures:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "Open cluster ports between hosts: "
                    + ", ".join(f"{e['host']}:{e['port']}" for e in failures)
                )
            pending = failures
            time.sleep(0.5)
        return {"ok": True}
    if action == "resp":
        with socket.create_connection(
            (request["host"], request["port"]), timeout=5
        ) as connection:
            connection.sendall(base64.b64decode(request["payload"]))
            reader = connection.makefile("rb")
            output = io.BytesIO()

            def read(depth=0):
                if depth > 32:
                    raise RuntimeError("RESP nesting limit exceeded")
                line = reader.readline(LIMIT + 1)
                if not line.endswith(b"\r\n"):
                    raise RuntimeError("Incomplete RESP reply")
                output.write(line)
                if output.tell() > LIMIT:
                    raise RuntimeError("Reply exceeds 8 MiB")
                if line[:1] == b"$":
                    size = int(line[1:-2])
                    if size < -1 or size > LIMIT - output.tell():
                        raise RuntimeError("RESP bulk size limit exceeded")
                    if size >= 0:
                        data = reader.read(size + 2)
                        if len(data) != size + 2:
                            raise RuntimeError("Incomplete RESP bulk reply")
                        output.write(data)
                elif line[:1] == b"*":
                    count = int(line[1:-2])
                    if count < -1 or count > 100000:
                        raise RuntimeError("RESP array limit exceeded")
                    for _ in range(max(0, count)):
                        read(depth + 1)

            read()
            return {"reply": base64.b64encode(output.getvalue()).decode()}
    raise RuntimeError("Unknown SSH helper action")


if __name__ == "__main__":
    os.umask(0o077)
    try:
        print(json.dumps(main(json.load(sys.stdin))))
    except Exception as error:
        print(json.dumps({"error": str(error)}))
