#!/usr/bin/env python3
"""Render the audited connection sweep as a dependency-free SVG preview."""

import csv
import html
from pathlib import Path


ROOT = Path(__file__).resolve().parent
INPUT = ROOT / "connection-sweep.csv"
OUTPUT = ROOT / "connection-sweep-qps.svg"

PRODUCTS = (
    ("Keylane raw io_uring", "Keylane", "#1473E6", "diag"),
    ("Dragonfly v1.40.2", "Dragonfly", "#E97827", "dots"),
    ("Garnet v2.1.5", "Garnet", "#C84C8A", "cross"),
)
CONNECTIONS = (80, 160, 320, 640, 1280, 2560)


def text(x, y, value, *, size=22, anchor="start", weight=400, fill="#17202A"):
    return (
        f'<text x="{x}" y="{y}" font-family="Arial, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" '
        f'fill="{fill}">{html.escape(str(value))}</text>'
    )


with INPUT.open(newline="", encoding="utf-8") as source:
    rows = list(csv.DictReader(source))

values = {
    (row["workload"], row["product"], int(row["connections"])): float(row["qps"])
    for row in rows
}

width, height = 1600, 1120
left, right = 145, 55
chart_width = width - left - right
panel_height = 350
panel_tops = {"GET": 250, "SET": 690}
max_qps = 900_000
group_width = chart_width / len(CONNECTIONS)
bar_width = 58
bar_gap = 7

svg = [
    f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
    '<rect width="100%" height="100%" fill="#FFFFFF"/>',
    "<defs>",
    '<pattern id="diag" width="10" height="10" patternUnits="userSpaceOnUse"><path d="M-2,10 L10,-2 M4,12 L12,4" stroke="#FFFFFF" stroke-width="2" opacity="0.45"/></pattern>',
    '<pattern id="dots" width="10" height="10" patternUnits="userSpaceOnUse"><circle cx="3" cy="3" r="1.6" fill="#FFFFFF" opacity="0.55"/></pattern>',
    '<pattern id="cross" width="12" height="12" patternUnits="userSpaceOnUse"><path d="M2,2 L10,10 M10,2 L2,10" stroke="#FFFFFF" stroke-width="1.5" opacity="0.45"/></pattern>',
    "</defs>",
    text(70, 70, "GET and SET QPS by connection count", size=36, weight=700),
    text(70, 112, "1B keys × 1 KiB · 60 s/run · 16 memtier threads · pipeline 1", size=22, fill="#566573"),
]

# Stable product identity uses both color and texture so the preview remains
# distinguishable in grayscale or for readers with color-vision deficiencies.
legend_x = 620
for index, (_, label, color, pattern) in enumerate(PRODUCTS):
    x = legend_x + index * 290
    svg.extend(
        [
            f'<rect x="{x}" y="145" width="48" height="26" rx="3" fill="{color}"/>',
            f'<rect x="{x}" y="145" width="48" height="26" rx="3" fill="url(#{pattern})"/>',
            text(x + 62, 166, label, size=21),
        ]
    )

for workload, top in panel_tops.items():
    bottom = top + panel_height
    svg.append(text(left, top - 28, f"{workload} throughput", size=27, weight=700))

    for tick in range(0, max_qps + 1, 100_000):
        y = bottom - panel_height * tick / max_qps
        svg.append(f'<line x1="{left}" y1="{y:.2f}" x2="{width-right}" y2="{y:.2f}" stroke="#DDE3E8" stroke-width="1"/>')
        svg.append(text(left - 18, y + 7, f"{tick // 1000:,}k", size=18, anchor="end", fill="#566573"))

    svg.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{bottom}" stroke="#7B8794" stroke-width="1.5"/>')
    svg.append(f'<line x1="{left}" y1="{bottom}" x2="{width-right}" y2="{bottom}" stroke="#7B8794" stroke-width="1.5"/>')

    for group_index, connection_count in enumerate(CONNECTIONS):
        center = left + group_width * (group_index + 0.5)
        total_bar_width = len(PRODUCTS) * bar_width + (len(PRODUCTS) - 1) * bar_gap
        group_left = center - total_bar_width / 2
        for product_index, (product, _, color, pattern) in enumerate(PRODUCTS):
            qps = values[(workload, product, connection_count)]
            bar_height = panel_height * qps / max_qps
            x = group_left + product_index * (bar_width + bar_gap)
            y = bottom - bar_height
            svg.append(f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_width}" height="{bar_height:.2f}" rx="2" fill="{color}"/>')
            svg.append(f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_width}" height="{bar_height:.2f}" rx="2" fill="url(#{pattern})"/>')
        svg.append(text(center, bottom + 32, f"{connection_count:,}", size=19, anchor="middle", fill="#34495E"))

    svg.append(text(48, top + panel_height / 2, "QPS", size=20, anchor="middle", weight=700))
    svg.append(text(left + chart_width / 2, bottom + 69, "Concurrent connections", size=20, anchor="middle", weight=700))

svg.append(text(70, 1090, "Raw block-device Keylane; RAID0/XFS-backed Dragonfly and Garnet. Uniform random GET and overwrite SET.", size=18, fill="#566573"))
svg.append("</svg>")
OUTPUT.write_text("\n".join(svg) + "\n", encoding="utf-8")
