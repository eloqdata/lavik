#!/usr/bin/env python3
"""Validate all new Aero logs and compare with the paired Keylane experiment.

Each percentile column is the median of three per-run operation histograms,
not a percentile of pooled samples. Intended histograms remain in raw evidence
and the per-run CSV; they are never substituted for operation latency.
"""
import argparse
import csv
import json
from statistics import median

import run as bench


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keylane-label", default="cpu12-hreplace-prep")
    parser.add_argument("--aero-label", default="cpu12-hreplace-aero")
    args = parser.parse_args()
    root = bench.ROOT
    cells = json.loads((root / f"{args.aero_label}.results.json").read_text())
    expected = {(r, w, t) for r in (1, 2, 3) for w in ("C", "A") for t in (100000, 0)}
    seen, rows = set(), []
    metrics = {"avg_us": "AverageLatency(us)", "p50_us": "50thPercentileLatency(us)",
               "p95_us": "95thPercentileLatency(us)", "p99_us": "99thPercentileLatency(us)",
               "p999_us": "99.9PercentileLatency(us)", "p9999_us": "99.99PercentileLatency(us)",
               "max_us": "MaxLatency(us)"}
    for cell in cells:
        ident = (cell["round"], cell["workload"], cell["target_ops_sec"])
        assert ident not in seen
        seen.add(ident)
        assert cell["workers"] == 256 and cell["exit_code"] == 0 and cell["failed"] == 0
        suffix = f"-target{cell['target_ops_sec']}" if cell["target_ops_sec"] else ""
        stem = f"{cell['label']}-aerospike-{cell['workload'].lower()}-c256{suffix}"
        for phase in ("warmup", "measured"):
            saved = json.loads((root / f"{stem}-{phase}.json").read_text())
            parsed = bench.parse(root / f"{stem}-{phase}.log", bench.COUNTS[phase])
            assert all(saved[k] == v for k, v in parsed.items())
            assert saved["failed"] == 0 and saved["exit_code"] == 0
            if phase == "measured":
                assert all(cell[k] == v for k, v in saved.items())
        for op in (("READ",) if cell["workload"] == "C" else ("READ", "UPDATE")):
            for histogram in (op, "Intended-" + op):
                values = cell["metrics"][histogram]
                assert values["Operations"] == cell["metrics"][op]["Return=OK"]
                rows.append({"round": cell["round"], "variant": "aerospike",
                             "workload": cell["workload"], "target": cell["target_ops_sec"],
                             "histogram": histogram, "total_qps": cell["success_qps"],
                             "op_qps": values["Operations"] * 1000 / cell["runtime_ms"],
                             **{label: values[key] for label, key in metrics.items()},
                             "count": values["Operations"], "failed": 0,
                             "log_sha256": cell["sha256"]})
    assert seen == expected, f"Incomplete or unexpected Aero cells: {seen ^ expected}"
    with (root / f"{args.aero_label}.summary.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    combined = json.loads((root / f"{args.keylane_label}.medians.json").read_text())
    for workload in ("C", "A"):
        for target in (100000, 0):
            for op in (("READ",) if workload == "C" else ("READ", "UPDATE")):
                group = [r for r in rows if (r["workload"], r["target"], r["histogram"])
                         == (workload, target, op)]
                assert len(group) == 3
                combined.append({"workload": workload, "target": target, "histogram": op,
                                 "variant": "aerospike", "rounds": 3,
                                 **{k: median(r[k] for r in group)
                                    for k in ("total_qps", "op_qps", *metrics)}})
    (root / f"{args.keylane_label}.comparison.json").write_text(json.dumps(combined, indent=2) + "\n")
    for r in sorted(combined, key=lambda r: (r["workload"], r["target"], r["histogram"], r["variant"])):
        print(r["workload"], r["target"], r["histogram"], r["variant"],
              f"QPS={r['total_qps']:.0f}/{r['op_qps']:.0f}",
              f"p99/p999/p9999={r['p99_us']/1000:.3f}/{r['p999_us']/1000:.3f}/{r['p9999_us']/1000:.3f}ms")


if __name__ == "__main__":
    main()
