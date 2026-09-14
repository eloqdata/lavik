#!/usr/bin/env python3
"""Bounded HREPLACE reconnaissance on the existing isolated 100M-key service.

Probe scores and the separately instrumented profile cannot replace a paired
three-round comparison. No database is cleared and no binary is switched.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import cpu12_run as isolated
import run as bench

isolated.no_client()
state = isolated.verify()
if state["dbsize"] != isolated.COUNT or "paused=0" not in state["defrag"]:
    raise RuntimeError("Expected the 100M-key, GC-enabled 12-worker service")
label = "cpu12-hreplace-initial"
receipt = bench.ROOT / f"{label}.json"
if receipt.exists():
    raise RuntimeError("Preserve the completed probe; do not overwrite it")
cells = []
for target in (100000, 0):
    for phase in ("warmup", "measured"):
        cell = bench.run("hreplace", "A", 256, phase, target=target,
                         measurement_interval="both", label=label)
        if cell["failed"]:
            raise RuntimeError("HREPLACE probe failed")
        if phase == "measured":
            cells.append(cell)
receipt.write_text(json.dumps({"server": state, "measurements": cells,
                   "binary_sha256": hashlib.sha256(Path(isolated.BINARY).read_bytes()).hexdigest()}, indent=2) + "\n")
subprocess.run([sys.executable, "-B", str(bench.ROOT / "cpu12_perf.py"),
                "cpu12-perf-hreplace-baseline-a", "--mode", "hreplace"], check=True)
