#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Render medians of the independent trial statistics, not pooled quantiles."""

import argparse
import csv
import json
from pathlib import Path
import statistics


def median(values):
    return statistics.median(float(value) for value in values)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("measurements", type=Path)
    args = parser.parse_args()
    rows = []
    component_paths = sorted(args.measurements.glob("component-*-*.csv"))
    expected_components = {
        f"component-{trial}-{label}.csv"
        for trial in range(3)
        for label in ("before", "after")
    }
    if {path.name for path in component_paths} != expected_components:
        raise ValueError("summary requires exactly three complete component pairs")
    processes = json.loads((args.measurements / "combined/results.json").read_text())
    strict = json.loads((args.measurements / "strict/results.json").read_text())
    expected_processes = {
        (audit, operations, trial, label)
        for audit in (0, 1024, 65536)
        for operations in (0, 128)
        for trial in range(3)
        for label in ("before", "after")
    }
    actual_processes = {
        (row["audit_target"], row["operations"], row["round"], row["label"])
        for row in processes
    }
    if actual_processes != expected_processes or len(processes) != 36:
        raise ValueError("summary requires exactly 36 combined trials")
    if {(row["round"], row["label"]) for row in strict} != {
        (trial, label) for trial in range(3) for label in ("before", "after")
    } or len(strict) != 6:
        raise ValueError("summary requires exactly six strict trials")
    for path in component_paths:
        _, trial, label = path.stem.split("-")
        with path.open() as stream:
            rows.extend(
                dict(row, trial=trial, label=label) for row in csv.DictReader(stream)
            )

    def component(shape, reader, phase, label, field):
        return median(
            row[field]
            for row in rows
            if (row["shape"], row["reader"], row["phase"], row["label"])
            == (shape, reader, phase, label)
        )

    readers = [
        "proposal",
        "facts",
        "create_discovery",
        "membership_discovery",
        "failover_discovery",
        "automatic_detection",
        "publication",
        "admin_group",
        "operation_status",
        "cursor",
    ]
    print("## Unrelated Growth Invariance\n")
    print(
        "All candidate ordinary capture allocation bytes/counts are checked against small at fixed selected records.\n"
    )
    for trial in sorted({row["trial"] for row in rows}):
        for reader in readers:
            selected = {
                row["shape"]: row
                for row in rows
                if row["label"] == "after"
                and row["trial"] == trial
                and row["reader"] == reader
                and row["phase"] == "capture"
            }
            for shape in (
                "unrelated_ops_1",
                "unrelated_ops_16",
                "unrelated_ops_128",
                "archives_1",
                "archives_128",
                "policies_32",
                "manifests_128",
            ):
                for field in ("requested_bytes", "allocations"):
                    if float(selected[shape][field]) != float(selected["small"][field]):
                        raise RuntimeError(
                            f"growth mismatch: {trial} {reader} {shape} {field}"
                        )
    print("PASS: 3 trials x 10 readers x 7 growth fixtures x 2 allocation fields.\n")
    print("## Combined 65536 Component Costs\n")
    print(
        "| Reader | Bytes before/after | Allocations before/after | Capture p50 us before/after | Destroy p50 us before/after | Hold us before/after | Wait us before/after |"
    )
    print("|---|---:|---:|---:|---:|---:|---:|")
    for reader in readers + [
        "audit_export",
        "operation_export",
        "snapshot",
        "command_encode",
    ]:
        cells = []
        for phase, field, scale in [
            ("capture", "requested_bytes", 1),
            ("capture", "allocations", 1),
            ("capture", "p50_ns", 1000),
            ("destroy", "p50_ns", 1000),
            ("capture", "lock_hold_ns", 1000),
            ("capture", "lock_wait_ns", 1000),
        ]:
            cells.append(
                " / ".join(
                    f"{component('combined_65536', reader, phase, label, field) / scale:.3f}"
                    for label in ("before", "after")
                )
            )
        print(f"| {reader} | " + " | ".join(cells) + " |")

    print("\n## Combined 65536 Destruction Accounting\n")
    print(
        "| Reader | Requested bytes before/after | Allocations before/after | Frees before/after | Freed usable bytes before/after |"
    )
    print("|---|---:|---:|---:|---:|")
    for reader in readers + [
        "audit_export",
        "operation_export",
        "snapshot",
        "command_encode",
    ]:
        cells = [
            " / ".join(
                f"{component('combined_65536', reader, 'destroy', label, field):.3f}"
                for label in ("before", "after")
            )
            for field in (
                "requested_bytes",
                "allocations",
                "frees",
                "freed_usable_bytes",
            )
        ]
        print(f"| {reader} | " + " | ".join(cells) + " |")

    print("\n## Successful Apply With Paced Publication\n")
    print(
        "| Shape | Apply/s before/after | Apply p99 us before/after | Writer lock wait us before/after | Publication capture + destruction p99 us before/after |"
    )
    print("|---|---:|---:|---:|---:|")
    for shape in ("small", "combined_0", "combined_1024", "combined_65536"):
        rates = {}
        for label in ("before", "after"):
            values = []
            for path in args.measurements.glob(f"component-*-{label}.log"):
                for line in path.read_text().splitlines():
                    fields = line.split(",")
                    if fields[:2] == ["apply_per_second", shape]:
                        values.append(fields[2])
            rates[label] = median(values)
        cells = [" / ".join(f"{rates[label]:.1f}" for label in ("before", "after"))]
        for reader, phase, field in [
            ("apply", "contended", "p99_ns"),
            ("apply", "contended", "lock_wait_ns"),
            ("publication", "paced_100hz", "p99_ns"),
        ]:
            cells.append(
                " / ".join(
                    f"{component(shape, reader, phase, label, field) / 1000:.3f}"
                    for label in ("before", "after")
                )
            )
        print(f"| {shape} | " + " | ".join(cells) + " |")

    print("\n## Concurrent Real Processes\n")
    print(
        "| Audit target | Unrelated operations | Proposal p50/p95/p99 ms before -> after | Publication p99 ms before/after | Admin p99 ms before/after | Sentinel p99 ms before/after | Proposals/s before/after |"
    )
    print("|---:|---:|---|---:|---:|---:|---:|")
    for audit in (0, 1024, 65536):
        for operations in (0, 128):
            selected = {
                label: [
                    row
                    for row in processes
                    if row["audit_target"] == audit
                    and row["operations"] == operations
                    and row["label"] == label
                ]
                for label in ("before", "after")
            }
            proposal = " -> ".join(
                "/".join(
                    f"{median(row['distributions']['proposal'][quantile] for row in selected[label]) / 1000:.3f}"
                    for quantile in ("p50_us", "p95_us", "p99_us")
                )
                for label in selected
            )
            cells = [proposal]
            for reader in ("publication_from_submit", "admin", "sentinel"):
                cells.append(
                    " / ".join(
                        f"{median(row['distributions'][reader]['p99_us'] for row in selected[label]) / 1000:.3f}"
                        for label in selected
                    )
                )
            cells.append(
                " / ".join(
                    f"{median(row['completed_proposals_per_overlap_second'] for row in selected[label]):.1f}"
                    for label in selected
                )
            )
            print(f"| {audit} | {operations} | " + " | ".join(cells) + " |")
    print("\n## Process Validity Context\n")
    print(
        "Sentinel counts below sum all three complete measurement phases, not only overlap.\n"
    )
    print(
        "| Audit target | Unrelated operations | Variant | Initial audit range | Redis SET p99 ms (median) | Sentinel disconnected / total | Snapshot changed in overlap | Manual snapshot ms (median) |"
    )
    print("|---:|---:|---|---|---:|---:|---|---:|")
    for audit in (0, 1024, 65536):
        for operations in (0, 128):
            for label in ("before", "after"):
                selected = [
                    row
                    for row in processes
                    if (row["audit_target"], row["operations"], row["label"])
                    == (audit, operations, label)
                ]
                initial = [row["seeded_audit"] for row in selected]
                disconnected = sum(
                    row["sentinel_flags"].get("master,disconnected", 0)
                    for row in selected
                )
                total = sum(sum(row["sentinel_flags"].values()) for row in selected)
                redis = (
                    median(row["distributions"]["redis"]["p99_us"] for row in selected)
                    / 1000
                )
                changed = any(
                    row["before"]["snapshot_idx"] != row["after"]["snapshot_idx"]
                    for row in selected
                )
                snapshot = median(row["manual_snapshot_us"] for row in selected) / 1000
                print(
                    f"| {audit} | {operations} | {label} | {min(initial)}-{max(initial)} | {redis:.3f} | {disconnected} / {total} | {changed} | {snapshot:.3f} |"
                )
    print("\n## Strict Admission\n")
    print("| Path | Variant | p50 us | p95 us | p99 us |")
    print("|---|---|---:|---:|---:|")
    for kind in ("near_full_success", "full_rejected"):
        for label in ("before", "after"):
            values = [row for row in strict if row["label"] == label]
            cells = [
                f"{median(row[kind][quantile] for row in values):.3f}"
                for quantile in ("p50_us", "p95_us", "p99_us")
            ]
            print(f"| {kind} | {label} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
