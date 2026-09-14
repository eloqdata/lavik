#!/usr/bin/env python3
"""Validate recorded YCSB logs and export all per-run codec A/B percentiles.

This is read-only with respect to databases and the human-maintained report.
Only derived CSV/JSON evidence is written; missing cells fail closed rather
than supplying a zero or selecting the best of incomplete trials.
"""
import argparse
import csv
import json
from statistics import median

import run as bench


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default="cpu12-codec")
    args = parser.parse_args()
    root = bench.ROOT
    env = json.loads((root / f"{args.label}.environment.json").read_text())
    cells = json.loads((root / f"{args.label}.results.json").read_text())
    mode = env.get("mode", "hmset")
    expected = {(r, v, w, t) for r in (1, 2, 3)
                for v in ("baseline", "candidate") for w in ("C", "A") for t in (100000, 0)}
    seen = set()
    rows = []
    metrics = {"avg_us": "AverageLatency(us)", "p50_us": "50thPercentileLatency(us)",
               "p95_us": "95thPercentileLatency(us)", "p99_us": "99thPercentileLatency(us)",
               "p999_us": "99.9PercentileLatency(us)", "p9999_us": "99.99PercentileLatency(us)",
               "max_us": "MaxLatency(us)"}
    for cell in cells:
        ident = (cell["round"], cell["variant"], cell["workload"], cell["target_ops_sec"])
        assert ident not in seen
        seen.add(ident)
        assert cell["binary_sha256"] == env["variants"][cell["variant"]]["sha256"]
        assert cell["workers"] == 256 and cell["exit_code"] == 0 and cell["failed"] == 0
        suffix = f"-target{cell['target_ops_sec']}" if cell["target_ops_sec"] else ""
        stem = f"{cell['label']}-{mode}-{cell['workload'].lower()}-c256{suffix}"
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
                rows.append({"round": cell["round"], "variant": cell["variant"],
                             "workload": cell["workload"], "target": cell["target_ops_sec"],
                             "histogram": histogram, "total_qps": cell["success_qps"],
                             "op_qps": values["Operations"] * 1000 / cell["runtime_ms"],
                             **{label: values[key] for label, key in metrics.items()},
                             "count": values["Operations"], "failed": cell["failed"],
                             "log_sha256": cell["sha256"], "binary_sha256": cell["binary_sha256"]})
    assert seen == expected, f"Incomplete or unexpected cells: {seen ^ expected}"
    with (root / f"{args.label}.summary.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summaries = []
    for workload in ("C", "A"):
        for target in (100000, 0):
            for op in (("READ",) if workload == "C" else ("READ", "UPDATE")):
                for variant in ("baseline", "candidate"):
                    group = [r for r in rows if (r["workload"], r["target"], r["histogram"], r["variant"])
                             == (workload, target, op, variant)]
                    summaries.append({"workload": workload, "target": target, "histogram": op,
                                      "variant": variant, "rounds": len(group),
                                      **{k: median(r[k] for r in group)
                                         for k in ("total_qps", "op_qps", *metrics)}})
    (root / f"{args.label}.medians.json").write_text(json.dumps(summaries, indent=2) + "\n")
    print("Verified 24 scored runs + 24 warmups; all successful", flush=True)
    for r in summaries:
        print(r["workload"], r["target"], r["histogram"], r["variant"],
              f"QPS={r['total_qps']:.0f}/{r['op_qps']:.0f}",
              f"p99/p999/p9999={r['p99_us']/1000:.3f}/{r['p999_us']/1000:.3f}/{r['p9999_us']/1000:.3f}ms")


if __name__ == "__main__":
    main()
