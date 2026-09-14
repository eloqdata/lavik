#!/usr/bin/env python3
"""Validate the new 16-cell matrix from raw YCSB receipts, without DB access."""
import argparse
import csv
import io
import json
import re

import main_abcd as experiment
import run as bench

ROOT = bench.ROOT
PREFIX = experiment.PREFIX
PERCENTILES = ("99thPercentileLatency(us)", "99.9PercentileLatency(us)",
               "99.99PercentileLatency(us)")


def collect(partial=False):
    env = json.loads((ROOT / f"{PREFIX}-environment.json").read_text())
    cells = json.loads((ROOT / f"{PREFIX}-results.json").read_text())
    expected = {(mode, w, rate) for mode in ("hreplace", "aerospike")
                for w in "ABCD" for rate in (100000, 0)}
    seen, rows, warmup_inserts = set(), [], {"hreplace": 0, "aerospike": 0}
    assert env["main_commit"] == experiment.COMMIT
    for cell in cells:
        mode, workload, target = (cell[k] for k in ("mode", "workload", "target_ops_sec"))
        identity = (mode, workload, target)
        assert identity in expected and identity not in seen
        seen.add(identity)
        stem = f"{PREFIX}-{mode}-{workload.lower()}-c256" + (f"-target{target}" if target else "")
        for phase in ("warmup", "measured"):
            receipt = json.loads((ROOT / f"{stem}-{phase}.json").read_text())
            duration = 300 if phase == "measured" else 0
            parsed = bench.parse(ROOT / f"{stem}-{phase}.log", None if duration else 1_000_000,
                                 duration_seconds=duration)
            assert all(receipt[k] == v for k, v in parsed.items())
            assert receipt["exit_code"] == receipt["failed"] == 0
            assert receipt["workers"] == 256 and receipt["phase"] == phase
            assert receipt["label"] == PREFIX and receipt["workload"] == workload
            assert receipt["mode"] == mode and receipt["target_ops_sec"] == target
            assert receipt["requested_duration_seconds"] == duration
            command = json.loads((ROOT / f"{stem}-{phase}.command.json").read_text())
            assert all(receipt[k] == v for k, v in command.items())
            args = receipt["command"]
            assert args[args.index("-threads") + 1] == "256"
            assert (int(args[args.index("-target") + 1]) if "-target" in args else 0) == target
            props = dict(v.split("=", 1) for v in args if "=" in v)
            for k, v in {"fieldcount": "10", "fieldlength": "128", "readallfields": "true",
                         "writeallfields": "true", "requestdistribution": "uniform",
                         "measurement.interval": "both", "measurementtype": "hdrhistogram",
                         "scanproportion": "0", "readmodifywriteproportion": "0"}.items():
                assert props[k] == v, (stem, k)
            assert int(props.get("maxexecutiontime", 0)) == duration
            assert int(props["operationcount"]) == (2_000_000_000 if duration else 1_000_000)
            if duration:
                assert receipt["attempted"] < 2_000_000_000
            for key, proportion in zip(("readproportion", "updateproportion", "insertproportion"),
                                       bench.WORKLOADS[workload]):
                assert float(props[key]) == proportion
            if workload == "D":
                assert int(props["recordcount"]) == experiment.D_STARTS[(target, phase)]
                assert props["insertstart"] == "0" and props["insertcount"] == "100000000"
                assert receipt["d_insert_start"] == int(props["recordcount"])
                if phase == "warmup":
                    warmup_inserts[mode] += receipt["metrics"]["INSERT"]["Return=OK"]
            else:
                assert props["recordcount"] == "100000000"
            if mode == "hreplace":
                assert props["redis.scanindex"] == "none"
                assert props["redis.updatecommand"] == "keylane.hreplace"
            if phase == "measured":
                assert all(cell[k] == v for k, v in receipt.items())
        before, after = cell["server_before"], cell["server_after"]
        assert before["pid"] == after["pid"]
        expected_sha = env["keylane_sha256" if mode == "hreplace" else "aerospike_sha256"]
        assert before["sha256"] == after["sha256"] == expected_sha
        inserted = cell["metrics"].get("INSERT", {}).get("Return=OK", 0)
        assert after["dbsize"] - before["dbsize"] == inserted
        if mode == "hreplace":
            assert "paused=0" in before["defrag"] and "paused=0" in after["defrag"]
        ops = [op for op in ("READ", "UPDATE", "INSERT") if op in cell["metrics"]]
        assert ops == (["READ"] if workload == "C" else ["READ", "INSERT"] if workload == "D"
                       else ["READ", "UPDATE"])
        for op in ops:
            for histogram in (op, "Intended-" + op):
                values = cell["metrics"][histogram]
                assert values["Operations"] == cell["metrics"][op]["Return=OK"]
                ps = [values[p] for p in PERCENTILES]
                assert 0 <= ps[0] <= ps[1] <= ps[2] <= values["MaxLatency(us)"]
            v = cell["metrics"][op]
            rows.append({"mode": mode, "workload": workload, "target": target, "operation": op,
                         "qps": v["Return=OK"] * 1000 / cell["runtime_ms"],
                         "p99_ms": v[PERCENTILES[0]] / 1000, "p999_ms": v[PERCENTILES[1]] / 1000,
                         "p9999_ms": v[PERCENTILES[2]] / 1000,
                         "average_us": v["AverageLatency(us)"], "p50_us": v["50thPercentileLatency(us)"],
                         "p95_us": v["95thPercentileLatency(us)"], "max_us": v["MaxLatency(us)"],
                         "successful": v["Return=OK"], "raw_log": stem + "-measured.log"})
    if not partial:
        assert seen == expected, f"Incomplete matrix: {len(seen)}/16"
        complete = json.loads((ROOT / f"{PREFIX}-complete.json").read_text())
        assert complete["count"] == 16 and complete["keylane_restored"]
        final = json.loads((ROOT / f"{PREFIX}-final-server.json").read_text())
        expected_count = 100_000_000 + warmup_inserts["hreplace"] + sum(
            r["successful"] for r in rows if r["mode"] == "hreplace" and r["operation"] == "INSERT")
        assert final["dbsize"] == expected_count
        assert final["sha256"] == env["keylane_sha256"] and "paused=0" in final["defrag"]
    return {"main_commit": env["main_commit"], "complete": seen == expected, "cells": len(cells),
            "measured_cutoff_utc": max((c["end_utc"] for c in cells), default=None),
            "formal_successes": sum(c["succeeded"] for c in cells), "warmup_operations": len(cells) * 1_000_000,
            "failed_operations": sum(c["failed"] for c in cells), "rows": rows,
            "total_qps": [{"mode": c["mode"], "workload": c["workload"],
                           "target": c["target_ops_sec"], "qps": c["success_qps"]} for c in cells],
            "percentile_definition": "Per-run YCSB operation HDR percentiles, not server metrics or pooled/averaged percentiles",
            "D_warmup_inserted": warmup_inserts}


def markdown(data):
    lines = ["每格：**QPS；p99 / p999 / p9999（ms）**。总 QPS 不混合读写百分位。", "",
             "| Workload | 限速 | 操作 | Aerospike CE | Keylane main HREPLACE |",
             "|---|---|---|---:|---:|"]
    index = {(r["mode"], r["workload"], r["target"], r["operation"]): r for r in data["rows"]}
    totals = {(r["mode"], r["workload"], r["target"]): r["qps"] for r in data["total_qps"]}
    for w in "ABCD":
        for rate in (0, 100000):
            label = "100K" if rate else "不限速"
            if w != "C":
                values = [f"{totals[(m, w, rate)]:,.0f}" if (m, w, rate) in totals else "待测"
                          for m in ("aerospike", "hreplace")]
                lines.append(f"| {w} | {label} | 总 QPS | " + " | ".join(values) + " |")
            for op in (["READ"] if w == "C" else ["READ", "INSERT"] if w == "D" else ["READ", "UPDATE"]):
                values = []
                for mode in ("aerospike", "hreplace"):
                    r = index.get((mode, w, rate, op))
                    values.append("待测" if r is None else
                                  f"{r['qps']:,.0f}；{r['p99_ms']:.3f} / {r['p999_ms']:.3f} / {r['p9999_ms']:.3f}")
                lines.append(f"| {w} | {label} | {op} | " + " | ".join(values) + " |")
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partial", action="store_true")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    data = collect(args.partial)
    if args.write:
        experiment.save("summary", data)
        output = io.StringIO()
        if data["rows"]:
            writer = csv.DictWriter(output, fieldnames=data["rows"][0])
            writer.writeheader()
            writer.writerows(data["rows"])
        (ROOT / f"{PREFIX}-summary.csv").write_text(output.getvalue())
    print(f"Validated {data['cells']}/16 cells, {data['failed_operations']} errors")
    print(markdown(data))
