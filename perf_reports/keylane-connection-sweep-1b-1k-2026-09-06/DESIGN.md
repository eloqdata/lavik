# Connection-sweep report design

## Reporting contract

- Audience: technical readers comparing storage-backed Redis-compatible systems.
- Question: how GET and SET throughput scale from 80 to 2,560 concurrent
  connections, and where each system reaches saturation.
- Takeaway: Keylane raw io_uring has the highest GET and SET peak; all three
  systems stop gaining throughput by 1,280 connections and regress at 2,560.
- Surface: one portable HTML technical report with native grouped-bar charts
  and exact audit tables.

## Chart map

| Section | Question | Family/type | Fields | Supported claim | Palette/non-color plan |
| --- | --- | --- | --- | --- | --- |
| GET scaling | How does random GET QPS change with connections? | Comparison/grouped bar | connections, product, qps | Keylane peaks at 784k QPS and keeps lower tail latency than Dragonfly at saturation. | Categorical blue/orange/pink roots; fixed product order, legend, and direct tooltips preserve identity without color. |
| SET scaling | How does random overwrite SET QPS change with connections? | Comparison/grouped bar | connections, product, qps | Keylane peaks at 857k QPS; Garnet is second at 765k and Dragonfly peaks at 595k. | Same categorical mapping and product order as GET. |

Both charts use a zero QPS baseline, full-width layout, six ordered connection
categories, 18 reviewed rows, and a 60-second measurement window. Exact p50,
p99, and p99.9 values remain in adjacent tables. Final QA checks desktop and
narrow layouts through the packaged portable-report verifier.
