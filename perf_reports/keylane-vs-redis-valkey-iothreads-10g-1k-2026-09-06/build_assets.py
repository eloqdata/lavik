#!/usr/bin/env python3
"""Validate raw memtier logs and build CSV/SVG assets for the report."""

import csv
import hashlib
import html
import re
from pathlib import Path


REPORT_ROOT = Path(__file__).resolve().parent
RAW_ROOT = Path("/mnt/dev/peer-bench/results-2026-09-06/redis-valkey-iothreads-10g-1k")
CONNECTIONS = (80, 160, 320, 640, 1280)
IO_THREADS = (1, 2, 4, 8, 16)
PRODUCTS = {
    "redis": "Redis 8.8.0",
    "valkey": "Valkey 9.1.0",
    "keylane": "Keylane raw io_uring",
}


def parse_log(path: Path, product: str, server_threads: int, thread_role: str, workload: str, connections: int):
    body = path.read_text(encoding="utf-8", errors="replace")
    totals = [line.split() for line in body.splitlines() if line.startswith("Totals")]
    if len(totals) != 1 or len(totals[0]) != 10:
        raise RuntimeError(f"incomplete Totals row: {path}")
    row = totals[0]
    if workload == "GET":
        gets = [line.split() for line in body.splitlines() if line.startswith("Gets")]
        if len(gets) != 1 or float(gets[0][3]) != 0.0:
            raise RuntimeError(f"GET miss or malformed GET row: {path}")
        if abs(float(gets[0][1]) - float(gets[0][2])) > 0.01:
            raise RuntimeError(f"GET QPS does not equal hit rate: {path}")
    cpu = re.search(r"Cores used:\s+([0-9.]+)", body)
    if not cpu:
        raise RuntimeError(f"missing client CPU summary: {path}")
    return {
        "product": PRODUCTS[product],
        "server_threads": server_threads,
        "thread_role": thread_role,
        "workload": workload,
        "connections": connections,
        "qps": float(row[1]),
        "avg_latency_ms": float(row[4]),
        "p50_ms": float(row[5]),
        "p99_ms": float(row[6]),
        "p999_ms": float(row[7]),
        "p9999_ms": float(row[8]),
        "client_cores": float(cpu.group(1)),
    }


rows = []
source_files = []
for product in ("redis", "valkey"):
    for thread_count in IO_THREADS:
        config = RAW_ROOT / product / f"io{thread_count}-config.txt"
        if config.read_text().splitlines() != ["io-threads", str(thread_count)]:
            raise RuntimeError(f"server config mismatch: {config}")
        source_files.append(config)
        for workload in ("GET", "SET"):
            for connections in CONNECTIONS:
                path = RAW_ROOT / product / f"io{thread_count}-{workload.lower()}-c{connections}.txt"
                rows.append(parse_log(path, product, thread_count, "io-threads", workload, connections))
                source_files.append(path)

for workload in ("GET", "SET"):
    for connections in CONNECTIONS:
        path = RAW_ROOT / "keylane" / f"{workload.lower()}-c{connections}.txt"
        rows.append(parse_log(path, "keylane", 16, "workers", workload, connections))
        source_files.append(path)

if len(rows) != 110:
    raise RuntimeError(f"expected 110 formal rows, got {len(rows)}")

columns = (
    "product", "server_threads", "thread_role", "workload", "connections",
    "qps", "avg_latency_ms", "p50_ms", "p99_ms", "p999_ms", "p9999_ms",
    "client_cores",
)
with (REPORT_ROOT / "results.csv").open("w", newline="", encoding="utf-8") as output:
    # Keep the checked-in CSV free of platform-specific CRLF so Git's
    # whitespace validation remains useful on Linux.
    writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)

with (REPORT_ROOT / "raw-SHA256SUMS").open("w", encoding="utf-8") as output:
    for path in sorted(source_files):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        output.write(f"{digest}  {path.relative_to(RAW_ROOT)}\n")


def svg_text(x, y, value, *, size=20, anchor="start", weight=400, fill="#17202A"):
    return (
        f'<text x="{x}" y="{y}" font-family="Arial, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" '
        f'fill="{fill}">{html.escape(str(value))}</text>'
    )


def value(workload, product, connections, threads):
    matches = [
        row for row in rows
        if row["workload"] == workload and row["product"] == product
        and row["connections"] == connections and row["server_threads"] == threads
    ]
    if len(matches) != 1:
        raise RuntimeError((workload, product, connections, threads, len(matches)))
    return matches[0]["qps"]


def render_iothread_scaling():
    width, height = 1600, 1210
    colors = {1: "#566573", 2: "#168F8C", 4: "#E97827", 8: "#7D5CC6", 16: "#1473E6"}
    dashes = {1: "2 8", 2: "10 6", 4: "16 5 3 5", 8: "6 4", 16: "none"}
    panels = (
        ("Redis 8.8.0", "GET", 120, 235),
        ("Redis 8.8.0", "SET", 865, 235),
        ("Valkey 9.1.0", "GET", 120, 720),
        ("Valkey 9.1.0", "SET", 865, 720),
    )
    panel_width, panel_height = 615, 330
    maximum = 1_000_000
    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#FFFFFF"/>',
        svg_text(70, 65, "Redis and Valkey I/O-thread scaling", size=35, weight=700),
        svg_text(70, 105, "10M keys × 1 KiB · 30 s/run · 16 memtier threads · pipeline 1", size=21, fill="#566573"),
    ]
    for index, thread_count in enumerate(IO_THREADS):
        x = 470 + index * 205
        svg.append(f'<line x1="{x}" y1="157" x2="{x+48}" y2="157" stroke="{colors[thread_count]}" stroke-width="5" stroke-dasharray="{dashes[thread_count]}"/>')
        svg.append(f'<circle cx="{x+24}" cy="157" r="6" fill="{colors[thread_count]}" stroke="#FFFFFF" stroke-width="2"/>')
        svg.append(svg_text(x + 62, 164, f"{thread_count} threads", size=18))

    for product, workload, left, top in panels:
        bottom = top + panel_height
        svg.append(svg_text(left, top - 25, f"{product} · {workload}", size=25, weight=700))
        for tick in range(0, maximum + 1, 200_000):
            y = bottom - panel_height * tick / maximum
            svg.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left+panel_width}" y2="{y:.2f}" stroke="#DDE3E8"/>')
            svg.append(svg_text(left - 15, y + 6, f"{tick//1000}k", size=17, anchor="end", fill="#566573"))
        svg.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{bottom}" stroke="#7B8794" stroke-width="1.5"/>')
        svg.append(f'<line x1="{left}" y1="{bottom}" x2="{left+panel_width}" y2="{bottom}" stroke="#7B8794" stroke-width="1.5"/>')
        for group_index, connections in enumerate(CONNECTIONS):
            x = left + panel_width * group_index / (len(CONNECTIONS) - 1)
            svg.append(svg_text(x, bottom + 31, f"{connections:,}", size=17, anchor="middle", fill="#34495E"))
        for thread_count in IO_THREADS:
            points = []
            for group_index, connections in enumerate(CONNECTIONS):
                x = left + panel_width * group_index / (len(CONNECTIONS) - 1)
                y = bottom - panel_height * value(workload, product, connections, thread_count) / maximum
                points.append((x, y))
            encoded = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
            svg.append(f'<polyline points="{encoded}" fill="none" stroke="{colors[thread_count]}" stroke-width="4" stroke-linejoin="round" stroke-dasharray="{dashes[thread_count]}"/>')
            for x, y in points:
                svg.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="5" fill="{colors[thread_count]}" stroke="#FFFFFF" stroke-width="2"/>')
        svg.append(svg_text(left + panel_width / 2, bottom + 69, "Concurrent connections", size=18, anchor="middle", weight=700))
    svg.append(svg_text(70, 1180, "QPS uses a zero baseline. Lines connect ordered connection-count categories; they do not imply interpolation.", size=17, fill="#566573"))
    svg.append("</svg>")
    (REPORT_ROOT / "iothread-scaling-qps.svg").write_text("\n".join(svg) + "\n", encoding="utf-8")


def render_best_comparison():
    width, height = 1600, 1170
    product_specs = (
        ("Keylane raw io_uring", "Keylane · 16 workers", 16, "#1473E6", "diag"),
        ("Redis 8.8.0", "Redis · 16 I/O threads", 16, "#E97827", "dots"),
    )
    # Valkey's measured optimum differs by command, so its selected thread
    # count is resolved per panel instead of hiding that distinction.
    valkey_threads = {"GET": 16, "SET": 8}
    maximum = 1_050_000
    left, right = 145, 55
    chart_width, panel_height = width - left - right, 340
    panel_tops = {"GET": 250, "SET": 690}
    group_width, bar_width, bar_gap = chart_width / len(CONNECTIONS), 58, 7
    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#FFFFFF"/>',
        '<defs>',
        '<pattern id="diag" width="10" height="10" patternUnits="userSpaceOnUse"><path d="M-2,10 L10,-2 M4,12 L12,4" stroke="#FFFFFF" stroke-width="2" opacity="0.45"/></pattern>',
        '<pattern id="dots" width="10" height="10" patternUnits="userSpaceOnUse"><circle cx="3" cy="3" r="1.6" fill="#FFFFFF" opacity="0.55"/></pattern>',
        '<pattern id="cross" width="12" height="12" patternUnits="userSpaceOnUse"><path d="M2,2 L10,10 M10,2 L2,10" stroke="#FFFFFF" stroke-width="1.5" opacity="0.45"/></pattern>',
        '</defs>',
        svg_text(70, 65, "Keylane versus tuned in-memory Redis and Valkey", size=35, weight=700),
        svg_text(70, 105, "Same 10M-key × 1 KiB workload; each memory system uses its measured best thread count", size=21, fill="#566573"),
    ]
    legends = (
        ("Keylane · 16 workers", "#1473E6", "diag"),
        ("Redis · 16 I/O threads", "#E97827", "dots"),
        ("Valkey · GET 16 / SET 8 I/O threads", "#C84C8A", "cross"),
    )
    for index, (label, color, pattern) in enumerate(legends):
        x = 360 + index * 390
        svg.append(f'<rect x="{x}" y="145" width="48" height="26" rx="3" fill="{color}"/>')
        svg.append(f'<rect x="{x}" y="145" width="48" height="26" rx="3" fill="url(#{pattern})"/>')
        svg.append(svg_text(x + 60, 166, label, size=18))

    for workload, top in panel_tops.items():
        bottom = top + panel_height
        svg.append(svg_text(left, top - 25, f"{workload} throughput", size=25, weight=700))
        for tick in range(0, 1_000_001, 200_000):
            y = bottom - panel_height * tick / maximum
            svg.append(f'<line x1="{left}" y1="{y:.2f}" x2="{width-right}" y2="{y:.2f}" stroke="#DDE3E8"/>')
            svg.append(svg_text(left - 15, y + 6, f"{tick//1000}k", size=17, anchor="end", fill="#566573"))
        svg.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{bottom}" stroke="#7B8794" stroke-width="1.5"/>')
        svg.append(f'<line x1="{left}" y1="{bottom}" x2="{width-right}" y2="{bottom}" stroke="#7B8794" stroke-width="1.5"/>')
        specs = product_specs + (("Valkey 9.1.0", "Valkey", valkey_threads[workload], "#C84C8A", "cross"),)
        for group_index, connections in enumerate(CONNECTIONS):
            center = left + group_width * (group_index + 0.5)
            total = len(specs) * bar_width + (len(specs) - 1) * bar_gap
            start = center - total / 2
            for product_index, (product, _, threads, color, pattern) in enumerate(specs):
                qps = value(workload, product, connections, threads)
                bar_height = panel_height * qps / maximum
                x = start + product_index * (bar_width + bar_gap)
                y = bottom - bar_height
                svg.append(f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_width}" height="{bar_height:.2f}" rx="2" fill="{color}"/>')
                svg.append(f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_width}" height="{bar_height:.2f}" rx="2" fill="url(#{pattern})"/>')
            svg.append(svg_text(center, bottom + 31, f"{connections:,}", size=18, anchor="middle", fill="#34495E"))
        svg.append(svg_text(left + chart_width / 2, bottom + 67, "Concurrent connections", size=18, anchor="middle", weight=700))
    svg.append(svg_text(70, 1145, "Keylane reads values from six raw NVMe devices; Redis and Valkey keep the complete dataset in DRAM.", size=17, fill="#566573"))
    svg.append("</svg>")
    (REPORT_ROOT / "best-memory-vs-keylane-qps.svg").write_text("\n".join(svg) + "\n", encoding="utf-8")


render_iothread_scaling()
render_best_comparison()
print("validated 110 formal rows; wrote results.csv, raw-SHA256SUMS, and two SVG charts")
