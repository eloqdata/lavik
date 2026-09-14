#!/usr/bin/env python3
"""Export GC/IO counters for interpretation, never as latency measurements."""
import argparse
import csv
import json
import re

import run as bench


def counters(path):
    result = {}
    for line in path.read_text().splitlines():
        if line.startswith("#"):
            continue
        match = re.fullmatch(r"(\S+) ([\d.eE+\-]+)", line)
        if match:
            result[match[1]] = float(match[2])
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    args = parser.parse_args()
    cells = json.loads((bench.ROOT / f"{args.label}.results.json").read_text())
    if len(cells) != 24:
        raise RuntimeError("Wait for the complete paired experiment")
    rows = []
    for cell in cells:
        target = cell["target_ops_sec"]
        suffix = f"-target{target}" if target else ""
        stem = f"{cell['label']}-hreplace-{cell['workload'].lower()}-c256{suffix}-measured"
        before = counters(bench.ROOT / f"{stem}.before.txt")
        after = counters(bench.ROOT / f"{stem}.after.txt")
        defrag = 'keylane_storage_defrag_runs_total{result="success"}'
        row = {"round": cell["round"], "variant": cell["variant"],
               "workload": cell["workload"], "target": target,
               "qps": cell["success_qps"], "runtime_ms": cell["runtime_ms"],
               "jvm_gc_count": cell["metrics"]["TOTAL_GCs"]["Count"],
               "jvm_gc_ms": cell["metrics"]["TOTAL_GC_TIME"]["Time(ms)"],
               "defrag_completed": after[defrag] - before[defrag],
               "defrag_active_before": before["keylane_storage_defrag_active"],
               "defrag_active_after": after["keylane_storage_defrag_active"]}
        rows.append(row)
        if cell["workload"] == "A" and not target:
            print(json.dumps(row), flush=True)
    with (bench.ROOT / f"{args.label}.background.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
