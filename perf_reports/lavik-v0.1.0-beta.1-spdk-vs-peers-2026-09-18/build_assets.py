#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Render the committed benchmark CSV in the previous report's visual style.

Requires Matplotlib 3.11.2. Run from any directory; no benchmark host, raw
devices, or executable downloads are needed to regenerate the five figures.
"""

import csv
import hashlib
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter, MultipleLocator

ROOT = Path(__file__).resolve().parent
CONNECTIONS = [80, 160, 320, 640, 1280]
THREADS = [1, 2, 4, 8, 16]
COLORS = {"lavik-spdk": "#1473E6", "redis": "#E97827", "valkey": "#C84C8A",
          "dragonfly": "#E97827", "garnet": "#C84C8A"}
HATCHES = {"lavik-spdk": "///", "redis": "..", "valkey": "xx",
           "dragonfly": "..", "garnet": "xx"}
NAMES = {"lavik-spdk": "Lavik SPDK", "redis": "Redis", "valkey": "Valkey",
         "dragonfly": "Dragonfly · Tiered Storage", "garnet": "Garnet · Storage Tier"}
THREAD_COLORS = ["#566573", "#168F8C", "#E97827", "#7D5CC6", "#1473E6"]
THREAD_STYLES = [":", "--", "-.", (0, (3, 2)), "-"]


def read_rows():
    """Require the complete matrix, so missing configurations cannot vanish."""
    rows = list(csv.DictReader((ROOT / "results.csv").open()))
    expected = set()
    for group, systems in [("memory", ["lavik-spdk", "redis", "valkey"]),
                           ("storage", ["lavik-spdk", "dragonfly", "garnet"])]:
        for system in systems:
            for threads in THREADS if system in ["redis", "valkey"] else [16]:
                for workload in ["GET", "SET"]:
                    for connections in CONNECTIONS + ([2560] if group == "storage" else []):
                        expected.add((group, system, threads, workload, connections))
    observed = set()
    for row in rows:
        for key in ["server_threads", "connections", "keys", "value_bytes", "test_seconds"]:
            row[key] = int(row[key])
        for key in ["qps", "p99_ms", "p999_ms"]:
            row[key] = float(row[key])
            assert math.isfinite(row[key]) and row[key] > 0, row
        key = tuple(row[k] for k in ["group", "system", "server_threads", "workload", "connections"])
        assert key not in observed, key
        observed.add(key)
        assert row["value_bytes"] == 1024 and int(row["connection_errors"]) == 0
        assert row["keys"] == (10**7 if row["group"] == "memory" else 10**9)
        assert row["test_seconds"] == (30 if row["group"] == "memory" else 60)
    assert observed == expected and len(rows) == 146
    return rows


def series(rows, group, system, workload, threads=None):
    candidates = [r for r in rows if (r["group"], r["system"], r["workload"]) ==
                  (group, system, workload)]
    # Select a single I/O-thread configuration per command, then show its whole
    # connection sweep. A pointwise maximum would hide configuration changes.
    if threads is None:
        threads = max(candidates, key=lambda r: r["qps"])["server_threads"]
    return sorted((r for r in candidates if r["server_threads"] == threads),
                  key=lambda r: r["connections"])


def frame(title, subtitle, *, thread_grid=False):
    fig = plt.figure(figsize=(16, 12.1 if thread_grid else 11.7), facecolor="white")
    fig.text(.045, .955, title, fontsize=24, weight="bold", va="top")
    fig.text(.045, .91, subtitle, fontsize=14, color="#566573")
    positions = ([.09, .54, .38, .26], [.57, .54, .38, .26],
                 [.09, .13, .38, .26], [.57, .13, .38, .26]) if thread_grid else (
                 [.09, .50, .875, .28], [.09, .115, .875, .28])
    return fig, [fig.add_axes(position) for position in positions]


def style_axis(ax, title, connections, *, metric="qps", maximum=None):
    ax.set_title(title, loc="left", fontsize=18, weight="bold", pad=15)
    ax.set_axisbelow(True)
    ax.grid(axis="y", color="#DDE3E8", linewidth=.8)
    ax.spines[["top", "right"]].set_visible(False)
    for spine in ["left", "bottom"]:
        ax.spines[spine].set_color("#7B8794")
    ax.tick_params(axis="both", length=0, pad=10, labelsize=12, colors="#566573")
    ax.set_xticks(range(len(connections)), [f"{n:,}" for n in connections])
    ax.set_xlabel("Concurrent connections", fontsize=13, weight="bold", labelpad=13)
    ax.set_ylim(0, maximum)
    ax.set_ylabel("QPS" if metric == "qps" else "p99.9 latency (ms)", fontsize=12, labelpad=12)
    if metric == "qps":
        ax.yaxis.set_major_locator(MultipleLocator(200_000))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value / 1000:g}k"))


def save(fig, name, footer):
    fig.text(.045, .045, footer, fontsize=11.5, color="#566573")
    for extension in ["svg", "png"]:
        # Omit wall-clock metadata and fix SVG IDs for reproducible review diffs.
        fig.savefig(ROOT / f"{name}.{extension}", dpi=150,
                    metadata={"Date": None} if extension == "svg" else {})
        if extension == "svg":
            path = ROOT / f"{name}.svg"
            path.write_text("\n".join(line.rstrip() for line in path.read_text().splitlines()) + "\n")
    plt.close(fig)


def comparisons(rows, group, *, metric="qps"):
    systems = ["lavik-spdk"] + (["redis", "valkey"] if group == "memory" else ["dragonfly", "garnet"])
    connections = CONNECTIONS + ([2560] if group == "storage" else [])
    throughput = metric == "qps"
    title = ("Lavik SPDK versus tuned in-memory Redis and Valkey" if group == "memory" else
             "Lavik SPDK, Dragonfly, and Garnet storage-tier comparison")
    if not throughput:
        title = "p99.9 latency · " + ("Lavik SPDK, Redis, and Valkey" if group == "memory" else
                                      "Lavik SPDK, Dragonfly, and Garnet")
    fig, axes = frame(title, ("10M keys × 1 KiB · 30 s/run" if group == "memory" else
                             "1B keys × 1 KiB · 60 s/run") + " · 16 memtier threads · pipeline 1")
    selected = {(system, workload): series(rows, group, system, workload)
                for system in systems for workload in ["GET", "SET"]}
    peak = max(r[metric] for values in selected.values() for r in values)
    maximum = math.ceil(peak / 200_000) * 200_000 if throughput else peak * 1.12
    labels = []
    for system in systems:
        if system == "lavik-spdk":
            label = "Lavik SPDK · 16 workers"
        elif system in ["redis", "valkey"]:
            get = selected[system, "GET"][0]["server_threads"]
            put = selected[system, "SET"][0]["server_threads"]
            counts = str(get) if get == put else f"GET {get} / SET {put}"
            label = f"{NAMES[system]} · {counts} I/O threads"
        else:
            label = NAMES[system]
        labels.append(label)
    handles = [Patch(facecolor=COLORS[s], edgecolor="white", hatch=HATCHES[s], label=label)
               if throughput else Line2D([], [], color=COLORS[s], marker=["s", "^", "D"][i], label=label)
               for i, (s, label) in enumerate(zip(systems, labels))]
    fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(.52, .88), ncol=3,
               frameon=False, fontsize=11.5, handlelength=2.6, columnspacing=2.7)
    for ax, workload in zip(axes, ["GET", "SET"]):
        style_axis(ax, workload + (" throughput" if throughput else " p99.9"), connections,
                   metric=metric, maximum=maximum)
        for index, system in enumerate(systems):
            values = [r[metric] for r in selected[system, workload]]
            if throughput:
                ax.bar([x + (index - 1) * .235 for x in range(len(connections))], values,
                       width=.21, color=COLORS[system], edgecolor="white", linewidth=.5,
                       hatch=HATCHES[system])
            else:
                ax.plot(range(len(connections)), values, color=COLORS[system],
                        marker=["s", "^", "D"][index], linewidth=2.6, markersize=6)
        ax.set_xlim(-.55, len(connections) - .45)
    footer = "Lavik v0.1.0-beta.1 · six NVMe devices via SPDK · " + (
        "Redis/Valkey in DRAM" if group == "memory" else "peers on RAID0/XFS storage tiers")
    save(fig, group + ("-qps" if throughput else "-p999"), footer)


def thread_scaling(rows):
    fig, axes = frame("Redis and Valkey I/O-thread scaling",
                      "10M keys × 1 KiB · 30 s/run · 16 memtier threads · pipeline 1", thread_grid=True)
    handles = [Line2D([], [], color=color, linestyle=style, marker="o", label=f"{n} thread" + ("s" if n != 1 else ""))
               for n, color, style in zip(THREADS, THREAD_COLORS, THREAD_STYLES)]
    fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(.52, .88), ncol=5,
               frameon=False, fontsize=12, handlelength=3)
    for ax, (system, version, workload) in zip(axes, [("redis", "8.8.0", "GET"), ("redis", "8.8.0", "SET"),
                                                     ("valkey", "9.1.0", "GET"), ("valkey", "9.1.0", "SET")]):
        style_axis(ax, f"{NAMES[system]} {version} · {workload}", CONNECTIONS, maximum=1_050_000)
        for n, color, style in zip(THREADS, THREAD_COLORS, THREAD_STYLES):
            values = [r["qps"] for r in series(rows, "memory", system, workload, n)]
            ax.plot(range(len(CONNECTIONS)), values, color=color, linestyle=style, linewidth=2.4,
                    marker="o", markersize=5, markeredgecolor="white", markeredgewidth=.6)
        ax.set_xlim(-.08, len(CONNECTIONS) - .92)
    save(fig, "iothread-scaling", "Zero-baseline QPS · lines connect ordered connection-count categories; they do not imply interpolation")


def main():
    """Validate the committed rows and regenerate SVG/PNG report assets."""
    plt.rcParams.update({"font.family": "DejaVu Sans", "text.color": "#17202A",
                         "axes.labelcolor": "#17202A", "svg.fonttype": "none",
                         "svg.hashsalt": "lavik-beta1-spdk-20260918", "hatch.linewidth": .8})
    rows = read_rows()
    for group in ["memory", "storage"]:
        comparisons(rows, group)
        comparisons(rows, group, metric="p999_ms")
    thread_scaling(rows)
    (ROOT / "chart-SHA256SUMS").write_text("".join(
        hashlib.file_digest(path.open("rb"), "sha256").hexdigest() + "  " + path.name + "\n"
        for path in sorted(ROOT.iterdir()) if path.suffix in [".svg", ".png"]))
    print("Validated 146 rows; rendered five figures as SVG and PNG")


if __name__ == "__main__":
    main()
