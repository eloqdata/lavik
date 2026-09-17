<!--
Copyright (C) 2026 EloqData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Chart contract

## Redis and Valkey I/O-thread scaling

- Question: how do GET and SET throughput change with connection count and
  configured I/O-thread count?
- Takeaway: 1–2 threads plateau near 150k QPS; 8–16 threads are required to
  approach the single-client ceiling. Valkey SET regresses at 16 threads.
- Form: four small-multiple line charts, split by product and workload.
- Data: 100 reviewed rows; five connection counts and five I/O-thread counts.
- Scale: zero-based, shared 0–1M QPS scale; connections are ordered categories.
- Identity: fixed colors, dash styles, and point markers distinguish thread
  counts without relying only on color.
- Delivery: `iothread-scaling-qps.svg` and its inspected PNG rendering.

## Best in-memory configurations versus Keylane

- Question: at each connection count, how close is raw-device Keylane to the
  best measured pure-memory Redis and Valkey configuration?
- Takeaway: Keylane GET peaks 13–14% below the memory systems, while Keylane
  SET is faster than both.
- Form: two vertically stacked grouped-bar charts for GET and SET.
- Data: 30 reviewed rows. Redis uses 16 I/O threads; Valkey uses 16 for GET and
  8 for SET; Keylane uses 16 workers.
- Scale: zero-based, shared 0–1.05M QPS scale.
- Identity: stable blue/orange/pink product colors plus distinct fill textures.
- Delivery: `best-memory-vs-keylane-qps.svg` and its inspected PNG rendering.

## One-terabyte storage-tier comparison

- Question: how do Keylane raw io_uring, Dragonfly Tiered Storage, and Garnet
  Storage Tier scale from 80 to 2,560 concurrent connections on a 1B-key
  working set?
- Takeaway: Keylane has the highest GET and SET peak throughput; every product
  regresses at 2,560 connections, while Garnet leads SET at 80 connections.
- Form: two vertically stacked grouped-bar charts for GET and SET.
- Data: 36 reviewed rows; six connection counts, two workloads, three products.
- Scale: zero-based, shared 0–900k QPS scale.
- Identity: stable blue/orange/pink product colors plus distinct fill textures.
- Delivery: `storage-tier-comparison-qps.svg` and its inspected PNG rendering.
