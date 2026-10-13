#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Render the README charts from this report's matched-concurrency table.

Requires Matplotlib 3.10.8. Run from any directory. The main charts show
16–640 connections; the report retains the complete measured sweep.
Style matches the 16-vCPU report's combined-qps.svg and combined-p999.svg.
"""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter, MultipleLocator, NullLocator

ROOT = Path(__file__).resolve().parent
CONNECTIONS = [16, 32, 80, 160, 320, 640]
COLORS = ["#1473E6", "#168F8C", "#E97827", "#C84C8A"]
STYLES = ["-", "--", "-.", ":"]
MARKERS = ["o", "s", "^", "D"]
LABELS = [
    "Lavik v0.1.0 · 1M keys · 2 workers",
    "Lavik v0.1.0 · 100M keys · 2 workers",
    "Redis 8.10.2 · 1M keys · 2 I/O threads",
    "Valkey 9.1.2 · 1M keys · 2 I/O threads",
]


def read_rows():
    """Use the published table without duplicating or rounding its values."""
    rows = {}
    for line in (ROOT / "README.md").read_text().splitlines():
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) != 6 or cells[0] not in ("GET", "SET"):
            continue
        connection = int(cells[1])
        if connection not in CONNECTIONS:
            continue
        key = (cells[0], connection)
        assert key not in rows, key
        rows[key] = [tuple(float(value.replace(",", "")) for value in cell.split(" / "))
                     for cell in cells[2:]]
        assert all(len(values) == 2 for values in rows[key])
    assert set(rows) == {(op, n) for op in ("GET", "SET") for n in CONNECTIONS}
    return rows


def render(rows, *, latency=False):
    fig = plt.figure(figsize=(16, 6.8), facecolor="white")
    title = ("p99.9 latency · Lavik 1M / 100M, Redis and Valkey" if latency else
             "Lavik 1M / 100M keys versus Redis and Valkey")
    fig.text(.045, .955, title, fontsize=24, weight="bold", va="top")
    fig.text(.045, .88,
             "1 KiB values · 16 memtier threads · pipeline 1 · Lavik SPDK / peers in DRAM",
             fontsize=14, color="#566573")
    handles = [Line2D([], [], color=color, linestyle=style, marker=marker,
                      linewidth=2.6, label=label)
               for color, style, marker, label in zip(COLORS, STYLES, MARKERS, LABELS)]
    fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(.52, .84),
               ncol=2, frameon=False, fontsize=11.5, handlelength=2.6, columnspacing=2.7)
    for left, op in zip((.065, .565), ("GET", "SET")):
        ax = fig.add_axes([left, .15, .405, .50])
        ax.set_title(op + (" p99.9 latency" if latency else " throughput"),
                     loc="left", fontsize=18, weight="bold", pad=15)
        ax.set_axisbelow(True)
        ax.grid(axis="y", color="#DDE3E8", linewidth=.8)
        ax.spines[["top", "right"]].set_visible(False)
        for spine in ("left", "bottom"):
            ax.spines[spine].set_color("#7B8794")
        ax.tick_params(axis="both", length=0, pad=10, labelsize=12, colors="#566573")
        ax.set_xscale("log", base=2)
        ax.set_xticks(CONNECTIONS, [f"{n:,}" for n in CONNECTIONS])
        ax.xaxis.set_minor_locator(NullLocator())
        ax.set_xlabel("Concurrent connections (log₂ scale)", fontsize=13,
                      weight="bold", labelpad=13)
        ax.set_ylabel("p99.9 latency (ms)" if latency else "QPS", fontsize=12, labelpad=12)
        ax.set_ylim(0, 12 if latency else 250_000)
        ax.yaxis.set_major_locator(MultipleLocator(2 if latency else 50_000))
        if not latency:
            ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value / 1000:g}k"))
        for index, (color, style, marker) in enumerate(zip(COLORS, STYLES, MARKERS)):
            values = [rows[op, n][index][int(latency)] for n in CONNECTIONS]
            ax.plot(CONNECTIONS, values, color=color, linestyle=style, marker=marker,
                    linewidth=2.6, markersize=7, markeredgecolor="white", markeredgewidth=.6)
    path = ROOT / ("latency.svg" if latency else "throughput.svg")
    fig.savefig(path, metadata={"Date": None})
    path.write_text("\n".join(line.rstrip() for line in path.read_text().splitlines()) + "\n")
    plt.close(fig)


if __name__ == "__main__":
    plt.rcParams.update({"font.family": "DejaVu Sans", "text.color": "#17202A",
                         "axes.labelcolor": "#17202A", "axes.titlecolor": "#17202A",
                         "svg.fonttype": "none", "svg.hashsalt": "lavik-2vcpu-readme"})
    rows = read_rows()
    render(rows)
    render(rows, latency=True)
