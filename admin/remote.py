#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
"""SSH helper: provision non-destructively; teardown requires retained ownership."""

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
import secrets
import signal
import select
import urllib.request
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
    spdk = None
    if request.get("spdk"):
        try:
            spdk = spdk_check(request)
        except Exception as error:
            errors.append(str(error))
    if request.get("monitoring"):
        try:
            checked(["docker", "compose", "version"])
            checked(["docker", "info", "--format", "{{.ServerVersion}}"])
        except Exception as error:
            errors.append(
                f"Monitoring requires Docker Engine and Compose v2 accessible to the SSH user: {error}"
            )
        request["ports"] = [*request.get("ports", []), request.get("grafanaPort", 3000)]
        try:
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", request.get("prometheusPort", 9090)))
        except OSError as error:
            errors.append(
                f"Monitoring port 127.0.0.1:{request.get('prometheusPort', 9090)} unavailable: {error}"
            )
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
        "spdk": spdk,
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


def checked(argv, timeout=30):
    """Stop a provisioning phase on command failure, including bounded diagnostics."""
    result = run(argv, timeout)
    if result["code"]:
        raise RuntimeError(result["stderr"] or result["stdout"] or f"Failed: {argv[0]}")
    return result["stdout"]


def spdk_device(request):
    match = re.fullmatch(
        r"spdk://([0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7])/([1-9][0-9]*)",
        request["spdk"],
    )
    if not match:
        raise RuntimeError("Select a full PCI address and namespace ID for SPDK")
    return match[1], int(match[2])


def spdk_check(request):
    """Fail closed before unbinding: one dedicated NVMe namespace in its own IOMMU group.

    An owned controller can contain Lavik data on resume/reboot. Unowned controllers
    must still use the kernel driver so every namespace and open user can be checked.
    No check or setup action erases media or enables unsafe no-IOMMU mode.
    """
    if os.geteuid() != 0:
        raise RuntimeError("SPDK setup requires root SSH on each Data host")
    bdf, nsid = spdk_device(request)
    device = Path("/sys/bus/pci/devices") / bdf
    if (
        not device.is_dir()
        or device.joinpath("class").read_text().strip() != "0x010802"
    ):
        raise RuntimeError(f"{bdf}: selected device is not an NVMe controller")
    group = device / "iommu_group"
    if not group.exists() or sorted(p.name for p in (group / "devices").iterdir()) != [
        bdf
    ]:
        raise RuntimeError(
            f"{bdf}: enable IOMMU; the controller must have an isolated IOMMU group"
        )
    unsafe = Path("/sys/module/vfio/parameters/enable_unsafe_noiommu_mode")
    if unsafe.exists() and unsafe.read_text().strip() in ("Y", "1"):
        raise RuntimeError(
            "Disable unsafe VFIO no-IOMMU mode before production SPDK setup"
        )
    memory = int(request["hugepageMiB"])
    if memory < 1024 or memory > 1048576 or memory % 2:
        raise RuntimeError(
            "SPDK memory must be an even MiB value between 1024 and 1048576"
        )
    if not Path("/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages").exists():
        raise RuntimeError("This host does not support 2 MiB hugepages")
    host_claim = Path("/var/lib/lavik-admin/spdk/host.json")
    if host_claim.exists() and json.loads(host_claim.read_text()) != {
        "owner": request.get("owner"),
        "spdk": request["spdk"],
    }:
        raise RuntimeError(
            "Automated SPDK setup reserves one Data host per deployment; this host is already claimed"
        )
    claim = Path("/var/lib/lavik-admin/spdk") / (bdf + ".json")
    retained = None
    if claim.exists():
        retained = json.loads(claim.read_text())
        if (
            retained["owner"] != request.get("owner")
            or retained["spdk"] != request["spdk"]
        ):
            raise RuntimeError(f"{bdf}: controller belongs to another deployment")
    driver = (device / "driver").resolve().name if (device / "driver").exists() else ""
    if driver == "vfio-pci" and retained:
        return retained
    if not retained:
        hugepages = Path("/sys/kernel/mm/hugepages/hugepages-2048kB")
        free_pages = int((hugepages / "free_hugepages").read_text())
        available = re.search(
            r"^MemAvailable:\s+(\d+)", Path("/proc/meminfo").read_text(), re.M
        )
        if (
            not available
            or int(available[1]) < max(0, memory // 2 - free_pages) * 2048 + 1024**2
        ):
            raise RuntimeError(
                "Insufficient memory for the SPDK hugepage reservation plus 1 GiB host reserve"
            )
    if driver != "nvme":
        raise RuntimeError(
            f"{bdf}: cannot inspect an unowned or unbound controller; restore its nvme driver before review"
        )
    controllers = list((device / "nvme").glob("nvme*"))
    if len(controllers) != 1:
        raise RuntimeError(f"{bdf}: cannot identify a unique NVMe controller")
    controller = controllers[0]
    serial = (controller / "serial").read_text().strip()
    if (retained and retained["serial"] != serial) or (
        request.get("expectedSerial") and request["expectedSerial"] != serial
    ):
        raise RuntimeError(f"{bdf}: controller serial changed since review")
    namespaces = [p for p in controller.glob("nvme*n*") if (p / "nsid").exists()]
    if len(namespaces) != 1 or int((namespaces[0] / "nsid").read_text()) != nsid:
        raise RuntimeError(
            f"{bdf}: automated setup requires exactly one namespace matching the selected ID"
        )
    block = "/dev/" + namespaces[0].name
    for tool in ("lsblk", "wipefs", "fuser", "modprobe", "mount", "findmnt"):
        if not shutil.which(tool):
            raise RuntimeError(f"Install {tool} before SPDK setup")
    checked(["modprobe", "--dry-run", "vfio-pci"])
    disk = json.loads(
        checked(["lsblk", "--json", "--output", "NAME,TYPE,FSTYPE,MOUNTPOINTS", block])
    )["blockdevices"][0]
    if disk.get("children") or any(disk.get("mountpoints") or []) or disk.get("fstype"):
        raise RuntimeError(
            f"{block}: partitions, filesystems, mounts, or swap make this device unsuitable"
        )
    if list((Path("/sys/class/block") / namespaces[0].name / "holders").iterdir()):
        raise RuntimeError(f"{block}: device is held by another block device")
    users = run(["fuser", block])
    if users["code"] != 1 or users["stdout"] or users["stderr"]:
        raise RuntimeError(f"{block}: device is open or its users cannot be checked")
    signatures = json.loads(checked(["wipefs", "--json", "--no-act", block])).get(
        "signatures", []
    )
    if signatures:
        raise RuntimeError(
            f"{block}: existing disk signatures found; choose dedicated empty media"
        )
    # The first storage block contains Lavik's durable format marker. Require
    # fresh media for initial admission; never erase or adopt another storage set.
    if not retained:
        with open(block, "rb", buffering=0) as media:
            if any(media.read(4096)):
                raise RuntimeError(
                    f"{block}: nonempty storage header; choose fresh dedicated media"
                )
    return {"owner": request.get("owner"), "spdk": request["spdk"], "serial": serial}


def configure_spdk(request):
    """Claim the reviewed controller before any kernel mutation; retries retain data."""
    retained = spdk_check(request)
    bdf, _ = spdk_device(request)
    device = Path("/sys/bus/pci/devices") / bdf
    claims = Path("/var/lib/lavik-admin/spdk")
    claims.mkdir(mode=0o700, parents=True, exist_ok=True)
    write_once(
        claims / "host.json",
        json.dumps({"owner": request["owner"], "spdk": request["spdk"]}),
    )
    write_once(claims / (bdf + ".json"), json.dumps(retained))
    pages = Path("/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages")
    requested = int(request["hugepageMiB"]) // 2
    # Never shrink a shared hugepage pool. A single Data process per host is the
    # automated model; its boot service restores at least the reviewed reservation.
    if int(pages.read_text()) < requested:
        pages.write_text(str(requested))
    free = pages.with_name("free_hugepages")
    if int(pages.read_text()) < requested:
        raise RuntimeError(
            "Hugepage allocation failed; free memory or reserve hugepages at boot, then resume"
        )
    driver = (device / "driver").resolve().name if (device / "driver").exists() else ""
    if driver != "vfio-pci" and int(free.read_text()) < requested:
        raise RuntimeError("Insufficient free hugepages for SPDK")
    mount = Path("/dev/hugepages")
    mount.mkdir(exist_ok=True)
    result = run(["findmnt", "-n", "-o", "FSTYPE", "--mountpoint", str(mount)])
    if result["code"]:
        checked(["mount", "-t", "hugetlbfs", "-o", "pagesize=2M", "none", str(mount)])
    elif result["stdout"] != "hugetlbfs":
        raise RuntimeError("/dev/hugepages must be a hugetlbfs mount")
    options = checked(["findmnt", "-n", "-o", "OPTIONS", "--mountpoint", str(mount)])
    if not re.search(r"(?:^|,)pagesize=(?:2M|2048K|2097152)(?:,|$)", options):
        raise RuntimeError("/dev/hugepages must use 2 MiB pages")
    checked(["modprobe", "vfio-pci"])
    if driver != "vfio-pci":
        (device / "driver_override").write_text("vfio-pci")
        (device / "driver/unbind").write_text(bdf)
        Path("/sys/bus/pci/drivers_probe").write_text(bdf)
    if (device / "driver").resolve().name != "vfio-pci":
        raise RuntimeError(
            f"{bdf}: VFIO binding failed; inspect the host before resuming"
        )
    (device / "driver_override").write_text("\n")
    return {"ok": True, "serial": retained["serial"]}


def setup_spdk(request):
    root = owned(request)
    # Keep a root-owned, fixed helper and retained request for boot persistence.
    # Configuration is installed before binding so an interrupted first run has
    # the exact same reviewed inputs available for an explicit resume.
    write_once(root / "spdk-helper.py", request["helperSource"], 0o700)
    boot = {
        k: request[k]
        for k in (
            "baseDir",
            "cluster",
            "owner",
            "spdk",
            "hugepageMiB",
            "expectedSerial",
        )
    }
    boot["action"] = "spdk-configure"
    write_once(root / "spdk-request.json", json.dumps(boot))
    result = configure_spdk(request)
    unit_name = f"lavik-{request['cluster']}-spdk.service"
    unit = (
        "[Unit]\nDescription=Lavik SPDK host configuration\nAfter=systemd-udev-settle.service\nBefore=network-online.target\n"
        "[Service]\nType=oneshot\nRemainAfterExit=yes\nExecStart=/usr/bin/python3 "
        + str(root / "spdk-helper.py")
        + " "
        + str(root / "spdk-request.json")
        + "\n[Install]\nWantedBy=multi-user.target\n"
    )
    write_once(Path("/etc/systemd/system") / unit_name, unit)
    checked(["systemctl", "daemon-reload"])
    checked(["systemctl", "enable", "--now", unit_name])
    return result


def monitoring(request):
    """Provision monitoring under deployment ownership, without returning credentials."""
    root = owned(request) / "monitoring"
    root.mkdir(mode=0o700, exist_ok=True)
    for name, content in request["files"].items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise RuntimeError("Invalid monitoring configuration path")
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        for directory in path.parents:
            if directory == root:
                break
            directory.chmod(0o755)
        write_once(path, content, 0o644)
    secret = root / "grafana-password"
    if not secret.exists():
        write_once(secret, secrets.token_hex(24))
    # The service account owns the private parent directory. Container mounts
    # require readable configuration; only this environment contains a secret.
    env = root / ".env"
    environment = "\n".join(
        [
            "GRAFANA_ADMIN_PASSWORD=" + secret.read_text(),
            "GRAFANA_BIND_ADDRESS=" + request["address"],
            "PROMETHEUS_BIND_ADDRESS=127.0.0.1",
            "PROMETHEUS_PORT=" + str(request.get("prometheusPort", 9090)),
            "GRAFANA_PORT=" + str(request.get("grafanaPort", 3000)),
            "LAVIK_TARGETS=" + ",".join(request["targets"]),
            "",
        ]
    )
    # Atomic target changes survive a later manual Compose restart too.
    temporary = root / ".env.next"
    temporary.write_text(environment)
    temporary.chmod(0o600)
    temporary.replace(env)
    # Targets are the only mutable configuration. Pass them to Compose without
    # exposing the generated password in argv, logs, the plan, or the browser.
    argv = [
        "docker",
        "compose",
        "--project-name",
        "lavik-" + request["cluster"],
        "--project-directory",
        str(root),
        "-f",
        str(root / "compose.yaml"),
    ]
    previous = os.environ.get("LAVIK_TARGETS")
    os.environ["LAVIK_TARGETS"] = ",".join(request["targets"])
    try:
        checked([*argv, "up", "-d"], 180)
        checked([*argv, "up", "-d", "--force-recreate", "target-config"], 30)
    finally:
        if previous is None:
            os.environ.pop("LAVIK_TARGETS", None)
        else:
            os.environ["LAVIK_TARGETS"] = previous
    prometheus_url = f"http://127.0.0.1:{request.get('prometheusPort', 9090)}"
    deadline = time.monotonic() + 60
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    while True:
        try:
            with opener.open(prometheus_url + "/-/ready", timeout=3) as response:
                if response.status != 200:
                    raise RuntimeError("Prometheus is not ready")
            with opener.open(prometheus_url + "/api/v1/targets", timeout=3) as response:
                targets = json.load(response).get("data", {}).get("activeTargets", [])
                healthy = {
                    target.get("labels", {}).get("instance")
                    for target in targets
                    if target.get("health") == "up"
                }
                missing = set(request["targets"]) - healthy
                if missing:
                    raise RuntimeError(
                        "Prometheus cannot scrape: " + ", ".join(sorted(missing))
                    )
            address = request["address"]
            host = "[" + address + "]" if ":" in address else address
            with opener.open(
                f"http://{host}:{request.get('grafanaPort', 3000)}/api/health",
                timeout=3,
            ) as response:
                if response.status != 200:
                    raise RuntimeError("Grafana is not ready")
            break
        except Exception as error:
            if time.monotonic() > deadline:
                raise RuntimeError(f"Monitoring readiness failed: {error}") from error
            time.sleep(1)
    return {"ok": True}


def start_node(request):
    root = owned(request)
    node = request["node"]
    directory = root / node["name"]
    directory.mkdir(mode=0o700, exist_ok=True)
    if (
        node["kind"] == "data"
        and not node.get("spdk")
        and not (directory / "lavik.data").exists()
    ):
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
    if node.get("spdk"):
        bdf, _ = spdk_device({"spdk": node["spdk"]})
        launch_source += (
            "os.environ.update("
            + repr(
                {
                    "BYCORF_EAL_ARGS": f"-a {bdf} --huge-dir=/dev/hugepages",
                    "BYCORF_DPDK_MEMORY_MB": str(request["hugepageMiB"]),
                }
            )
            + ")\n"
        )
    launch_source += "os.execv(argv[0], argv)\n"
    write_once(launch, launch_source, 0o700)
    if request["supervisor"] == "systemd":
        units = (
            Path("/etc/systemd/system")
            if node.get("spdk")
            else Path.home() / ".config/systemd/user"
        )
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
        if node.get("spdk"):
            unit = (
                unit.replace(
                    "After=network-online.target",
                    f"After=network-online.target lavik-{request['cluster']}-spdk.service\nRequires=lavik-{request['cluster']}-spdk.service",
                )
                .replace(
                    "LimitNOFILE=65536", "LimitNOFILE=65536\nLimitMEMLOCK=infinity"
                )
                .replace("WantedBy=default.target", "WantedBy=multi-user.target")
            )
        write_once(units / unit_name, unit)
        for args in (["daemon-reload"], ["enable", "--now", unit_name]):
            result = run(
                ["systemctl", *([] if node.get("spdk") else ["--user"]), *args], 30
            )
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


def teardown_units(root, request):
    """Only exact retained launchers establish ownership of a service unit."""
    units = []
    if request["supervisor"] != "systemd":
        return units
    for node in request["nodes"]:
        name = f"lavik-{request['cluster']}-{node['name']}.service"
        path = Path.home() / ".config/systemd/user" / name
        if path.is_symlink():
            raise RuntimeError(f"Refusing symlinked service unit: {path}")
        if path.exists():
            expected = "ExecStart=/usr/bin/python3 " + str(
                root / node["name"] / "launch.py"
            ).replace("%", "%%")
            if expected not in path.read_text().splitlines():
                raise RuntimeError(f"Refusing foreign service unit: {path}")
        # A missing file does not prove that an already-loaded service stopped.
        info = run(
            [
                "systemctl",
                "--user",
                "show",
                name,
                "--property=LoadState,ActiveState,FragmentPath",
            ]
        )
        fields = dict(
            line.split("=", 1) for line in info["stdout"].splitlines() if "=" in line
        )
        if not fields or (info["code"] and fields.get("LoadState") != "not-found"):
            raise RuntimeError(f"Cannot inspect service {name}: {info['stderr']}")
        fragment = fields.get("FragmentPath")
        if fragment and fragment != str(path):
            raise RuntimeError(f"Service uses a foreign unit: {name}")
        if not path.exists() and fields.get("ActiveState") not in (
            "inactive",
            "failed",
        ):
            raise RuntimeError(f"Missing unit still has active state: {name}")
        if path.exists():
            units.append((name, path))
    return units


def stop_process(directory, root):
    """A pidfd pins the checked identity; PID reuse must never signal another job."""
    identity_path = directory / "process.json"
    if not identity_path.exists():
        return
    identity = json.loads(identity_path.read_text())
    if (
        identity.get("boot")
        != Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    ):
        return
    pid = int(identity["pid"])
    try:
        fd = os.pidfd_open(pid)
    except ProcessLookupError:
        return
    try:
        proc = Path(f"/proc/{pid}")
        try:
            # Field 2 can contain spaces and parentheses; split after its final ')'.
            fields = (proc / "stat").read_text().rsplit(")", 1)[1].split()
            if fields[19] != identity["start"] or fields[0] == "Z":
                return
            argv = (proc / "cmdline").read_bytes().split(b"\0")
        except FileNotFoundError:
            return
        permitted = {
            str(root / "release/lavik").encode(),
            str(root / "release/lavik-meta").encode(),
        }
        launcher = str(directory / "launch.py").encode()
        if not argv or not (
            argv[0] in permitted or (len(argv) > 1 and argv[1] == launcher)
        ):
            raise RuntimeError(f"Process identity does not belong to {directory}")
        signal.pidfd_send_signal(fd, signal.SIGTERM)
        poller = select.poll()
        poller.register(fd, select.POLLIN)
        if not poller.poll(60000):
            raise RuntimeError(f"Process {pid} did not stop; data retained")
    finally:
        os.close(fd)


def check_no_processes(root):
    """A stale/missing process record never authorizes deleting live file storage."""
    executables = {str(root / "release/lavik"), str(root / "release/lavik-meta")}
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            executable = os.readlink(proc / "exe").removesuffix(" (deleted)")
            argv = (proc / "cmdline").read_bytes().split(b"\0")
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError:
            # Other UIDs cannot belong to this unprivileged deployment.
            # Same-UID processes can still deny procfs inspection. Keep data
            # when ownership cannot be ruled out, even if argv looks unrelated.
            if proc.stat().st_uid == os.geteuid():
                raise RuntimeError(f"Cannot inspect owned process {proc.name}")
            continue
        if executable in executables or any(
            arg.startswith((str(root) + "/").encode()) for arg in argv[:2]
        ):
            raise RuntimeError(
                f"Deployment process {proc.name} is still running; data retained"
            )


def teardown(request):
    """Idempotent file-deployment removal; never follow mounts or erase raw/SPDK media."""
    if request.get("phase") not in ("check", "stop", "delete"):
        raise RuntimeError("Invalid teardown phase")
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,39}", request["cluster"]):
        raise RuntimeError("Invalid cluster name")
    root = root_path(request)
    if not root.exists():
        # A previous delete may have succeeded before the SSH reply was lost.
        return {"removed": True}
    # A small sibling receipt survives the final marker unlink. It proves the
    # empty directory left by a crash at that boundary belongs to this removal.
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", request["owner"]):
        raise RuntimeError("Invalid deployment owner")
    receipt = root.parent / (
        ".lavik-removed-" + request["cluster"] + "-" + request["owner"] + ".json"
    )
    receipt_data = json.dumps({"owner": request["owner"], "root": str(root)})
    if receipt.is_symlink():
        raise RuntimeError("Refusing symlinked teardown receipt")
    if (
        not (root / "deployment.json").exists()
        and receipt.is_file()
        and receipt.read_text() == receipt_data
        and not any(root.iterdir())
    ):
        if request["phase"] == "delete":
            root.rmdir()
        return {"removed": True}
    root = owned(request)
    if (root / "spdk-request.json").exists() or any(
        n.get("spdk") for n in request["nodes"]
    ):
        raise RuntimeError("SPDK storage requires manual decommissioning")
    for node in request["nodes"]:
        if not re.fullmatch(r"(?:meta|data)-[0-9]+", node["name"]):
            raise RuntimeError("Invalid retained node name")
    # Refuse bind mounts and nested filesystems as well as symlinked metadata.
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        mount = line.split()[4]
        mount = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), mount)
        if mount == str(root) or mount.startswith(str(root) + "/"):
            raise RuntimeError(f"Refusing mounted deployment path: {mount}")
    for directory, names, files in os.walk(root, followlinks=False):
        for name in names + files:
            if (Path(directory) / name).is_symlink():
                raise RuntimeError(
                    "Refusing symlink inside deployment; inspect before teardown"
                )
    units = teardown_units(root, request)
    monitor = root / "monitoring"
    compose = monitor / "compose.yaml"
    if compose.exists():
        if not request.get("monitoring"):
            raise RuntimeError("Monitoring is absent from the retained plan")
        ids = checked(
            [
                "docker",
                "ps",
                "-aq",
                "--filter",
                "label=com.docker.compose.project=lavik-" + request["cluster"],
            ]
        ).split()
        if ids:
            containers = json.loads(checked(["docker", "inspect", *ids]))
            for container in containers:
                labels = container["Config"].get("Labels") or {}
                if labels.get("com.docker.compose.project.working_dir") != str(
                    monitor
                ) or labels.get("com.docker.compose.project.config_files") != str(
                    compose
                ):
                    raise RuntimeError(
                        "Monitoring project belongs to another directory"
                    )
    if request["phase"] == "check":
        return {"ok": True}
    for name, path in units:
        checked(["systemctl", "--user", "disable", "--now", name], 90)
        state = checked(
            ["systemctl", "--user", "show", name, "--property=ActiveState", "--value"]
        )
        if state not in ("inactive", "failed"):
            raise RuntimeError(f"Service did not stop: {name}")
    if request["supervisor"] == "process":
        for node in request["nodes"]:
            stop_process(root / node["name"], root)
    if compose.exists():
        checked(
            [
                "docker",
                "compose",
                "--project-name",
                "lavik-" + request["cluster"],
                "--project-directory",
                str(monitor),
                "-f",
                str(compose),
                "down",
                *(["--volumes"] if request["phase"] == "delete" else []),
            ],
            180,
        )
    check_no_processes(root)
    if request["phase"] == "delete":
        write_once(receipt, receipt_data)
        for _, path in units:
            path.unlink()
        if units:
            checked(["systemctl", "--user", "daemon-reload"])
        # Keep the ownership marker until all other content is gone, so a
        # interrupted recursive removal can still be retried with the nonce.
        for child in root.iterdir():
            if child.name == "deployment.json":
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
        (root / "deployment.json").unlink()
        root.rmdir()
    return {"ok": True, "phase": request["phase"]}


def main(request):
    action = request["action"]
    if action == "teardown":
        return teardown(request)
    if action == "probe":
        return probe(request)
    if action == "prepare":
        return prepare(request)
    if action == "reserve":
        return {"directory": str(owned(request, create=True))}
    if action == "monitoring":
        return monitoring(request)
    if action == "spdk-setup":
        return setup_spdk(request)
    if action == "spdk-configure":
        owned(request)
        return configure_spdk(request)
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
        print(
            json.dumps(
                main(
                    json.loads(Path(sys.argv[1]).read_text())
                    if len(sys.argv) > 1
                    else json.load(sys.stdin)
                )
            )
        )
    except Exception as error:
        print(json.dumps({"error": str(error)}))
        if len(sys.argv) > 1:
            sys.exit(1)
