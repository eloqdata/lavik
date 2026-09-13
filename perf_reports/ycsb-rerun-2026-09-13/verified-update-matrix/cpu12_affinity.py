#!/usr/bin/env python3
"""Reversible host-local 12+4 CPU placement; never changes storage or the client.

Requires root. Save original runtime policy before changing it. Kernel per-CPU
threads and managed NVMe IRQs are not movable service processes; report them
instead of claiming boot-time strict isolation. Only this NIC's discovered
MSI IRQs are modified, not persisted interrupt numbers from another boot.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parent
STATE = ROOT / "cpu12-affinity-original.json"
HOUSE = {12, 13, 14, 15}
UNITS = ("system.slice", "user.slice", "init.scope")
NIC = "enP46392s1"
WORKQUEUE = Path("/sys/devices/virtual/workqueue/cpumask")


def command(*args, check=True):
    return subprocess.run(args, check=check, capture_output=True, text=True).stdout.strip()


def threads():
    result = []
    for proc in Path("/proc").glob("[0-9]*"):
        try:
            # stat's comm can contain parentheses/spaces; fields after its last
            # ')' begin at state (field 3), followed by PPID and flags at 4/9.
            raw = (proc / "stat").read_text()
            rest = raw[raw.rfind(")") + 2:].split()
            ppid, flags = int(rest[1]), int(rest[6])
            comm = (proc / "comm").read_text().strip()
            for task in (proc / "task").iterdir():
                tid = int(task.name)
                result.append({"pid": int(proc.name), "tid": tid, "ppid": ppid,
                               "comm": comm, "flags": flags,
                               "affinity": sorted(os.sched_getaffinity(tid))})
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return result


def irqs():
    directory = Path(f"/sys/class/net/{NIC}/device/msi_irqs")
    ids = sorted(int(p.name) for p in directory.iterdir())
    if len(ids) != 17:
        raise RuntimeError(f"Unexpected NIC IRQ topology: {ids}")
    return {str(i): {"configured": Path(f"/proc/irq/{i}/smp_affinity_list").read_text().strip(),
                     "effective": Path(f"/proc/irq/{i}/effective_affinity_list").read_text().strip()}
            for i in ids}


def snapshot():
    policies = {u: command("systemctl", "show", u, "--property=AllowedCPUs", "--value")
                for u in UNITS}
    return {"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "hostname": os.uname().nodename, "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "units": policies, "workqueue": WORKQUEUE.read_text().strip(),
            "irqbalance": command("systemctl", "is-active", "irqbalance", check=False),
            "irqs": irqs(), "threads": threads(),
            "network_queue_masks": {str(p): p.read_text().strip()
                                    for n in ("eth0", NIC)
                                    for pattern in ("rx-*/rps_cpus", "tx-*/xps_cpus")
                                    for p in Path(f"/sys/class/net/{n}/queues").glob(pattern)}}


def apply():
    if STATE.exists():
        raise RuntimeError("Original state already exists; inspect it before applying again")
    if command("pgrep", "-x", "keylane", check=False):
        raise RuntimeError("Stop Keylane before moving user.slice off its CPUs")
    original = snapshot()
    STATE.write_text(json.dumps(original, indent=2) + "\n")
    if original["irqbalance"] == "active":
        command("systemctl", "stop", "irqbalance")
    for unit in UNITS:
        command("systemctl", "set-property", "--runtime", unit, "AllowedCPUs=12-15")
    WORKQUEUE.write_text("f000\n")
    for index, irq in enumerate(original["irqs"]):
        Path(f"/proc/irq/{irq}/smp_affinity_list").write_text(str(12 + index % 4) + "\n")
    moved, skipped = [], []
    for thread in original["threads"]:
        # Cgroup cpusets already constrain user services. Setting task affinity
        # also handles root init and movable kernel daemons. Workqueue placement
        # belongs to the kernel pool policy, and PF_NO_SETAFFINITY is respected.
        if thread["flags"] & 0x04000000 or thread["comm"].startswith("kworker/"):
            skipped.append({**thread, "reason": "kernel-bound or workqueue-managed"})
            continue
        try:
            os.sched_setaffinity(thread["tid"], HOUSE)
            moved.append(thread)
        except (OSError, ProcessLookupError) as error:
            skipped.append({**thread, "reason": str(error)})
    receipt = {"moved": moved, "skipped": skipped, "after": snapshot()}
    (ROOT / "cpu12-affinity-applied.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(f"Applied runtime placement: moved {len(moved)} tasks; {len(skipped)} kernel-bound/exited tasks recorded")


def restore():
    original = json.loads(STATE.read_text())
    if original["boot_id"] != Path("/proc/sys/kernel/random/boot_id").read_text().strip():
        raise RuntimeError("Do not restore stale IRQ/TID numbers after reboot")
    if command("pgrep", "-x", "keylane", check=False):
        raise RuntimeError("Stop the isolated Keylane service before restoring policy")
    for unit, value in original["units"].items():
        command("systemctl", "set-property", "--runtime", unit, "AllowedCPUs=" + value)
    WORKQUEUE.write_text(original["workqueue"] + "\n")
    for irq, values in original["irqs"].items():
        Path(f"/proc/irq/{irq}/smp_affinity_list").write_text(values["configured"] + "\n")
    live = {t["tid"]: t for t in threads()}
    for thread in original["threads"]:
        current = live.get(thread["tid"])
        if current is None or (current["pid"], current["comm"]) != (thread["pid"], thread["comm"]):
            continue
        if thread["flags"] & 0x04000000 or thread["comm"].startswith("kworker/"):
            continue
        try:
            os.sched_setaffinity(thread["tid"], set(thread["affinity"]))
        except OSError:
            pass
    if original["irqbalance"] == "active":
        command("systemctl", "start", "irqbalance")
    (ROOT / "cpu12-affinity-restored.json").write_text(json.dumps(snapshot(), indent=2) + "\n")
    print("Restored recorded task/policy affinities; tasks created later retain inherited affinities")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("apply", "restore", "inspect"))
    action = parser.parse_args().action
    if os.geteuid() != 0:
        raise RuntimeError("Run via sudo")
    if Path("/sys/devices/system/cpu/online").read_text().strip() != "0-15":
        raise RuntimeError("This experiment requires the recorded 16-CPU topology")
    if action == "apply":
        apply()
    elif action == "restore":
        restore()
    else:
        print(json.dumps(snapshot(), indent=2))
