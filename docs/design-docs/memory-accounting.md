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

# Memory accounting

## Limit source

`--max-memory` sets the process memory budget. A zero value selects 80% of the
smaller non-zero capacity reported by host `MemTotal` and the cgroup v1/v2
memory limit. The server fails startup if neither source is available and no
explicit limit was supplied.

The mimalloc C++ new/delete hooks charge allocator usable bytes to a
cache-line-separated counter for the current worker. Allocations outside worker
threads use a fallback counter. Worker 0 sums the counters every 100 ms and
publishes the result for limit enforcement; the periodic path does not read
`/proc` and creates no extra thread.

Release builds keep mimalloc's generic per-allocation statistics disabled
(`MI_STAT=0`). With that setting the malloc class counters are intentionally not
balanced on every allocation and free, so they must not be interpreted as live
bytes. Lavik's hooks maintain a thread-local usable-byte total and publish it
with a relaxed store to the current worker's private cache line; there is no
locked read-modify-write in the worker allocation path. Mimalloc's page-level
committed and reserved counters are sampled only for an explicit metrics or
INFO request and do not control admission.

RSS is also diagnostic-only. `/proc/self/statm` is opened once for the process
lifetime, the page size is cached, and each explicit diagnostic refresh uses a
single `pread` without shared file-offset state.

## Admission model

The command path never reads `/proc` and never asks mimalloc for global stats.
For memory-growing commands it reads the cached allocator usage and limit with
relaxed atomics, then reserves conservatively for parsed argument bytes plus 512
bytes per affected key. SET, MSET, INCR, and queued EXEC writes use this check.
Other commands do not claim new keyspace memory.

This is a low-overhead guard, not an allocator-level hard wall. Sampling can lag
by up to 100 ms, concurrent workers can pass against the same sample, non-C++
allocations are outside the usable-byte hooks, and the estimate deliberately
overstates typical index growth. Operators should leave headroom between
`--max-memory` and a container or system OOM limit.

## Exported values

Prometheus exports:

- `lavik_memory_current_bytes`: cached allocator bytes used for admission.
- `lavik_memory_used_bytes`: allocator usable bytes.
- `lavik_memory_rss_bytes`: RSS sampled on the metrics/INFO request.
- `lavik_memory_committed_bytes`: mimalloc committed pages.
- `lavik_memory_reserved_bytes`: mimalloc reserved virtual address space.
- `lavik_memory_max_bytes`: configured budget.
- `lavik_memory_rejected_commands_total`: commands rejected before execution.

`INFO memory` exposes the same model through `used_memory`, `used_memory_rss`,
`used_memory_peak`, `maxmemory`, `allocator_active`, `allocator_resident`,
`allocator_reserved`, and `oom_rejected_commands`. `mem_fragmentation_ratio`
is RSS divided by allocator committed bytes. RSS is diagnostic only and never
participates in admission.

## Index memory layout

The in-memory index does not retain the 20-byte persistent digest. It stores a
64-bit hash for bucket routing and always compares the complete key before
accepting a match, so hash collisions cannot return another key.

`RecordLocation` is ordered by alignment and stores only common record
metadata. External-value extent manifests live in a sparse per-worker side
table keyed by the address-stable index entry. Normal keys therefore do not pay
for a `shared_ptr`.

The index entry stores its immutable key in bytes immediately following the
entry header. Entry and key use one allocation for every key length; there is
no 32-byte `std::string` object and no second allocation after an SSO boundary.
Compile-time layout checks currently enforce:

- `RecordLocation`: 48 bytes (previously 96).
- index entry header: 64 bytes (previously 152, before separately allocated key
  characters).

The hash, location, and key length occupy the 64-byte entry header, and the
immutable key bytes begin immediately after it. The allocator charge is the
size class containing `64 + key length` bytes. Bucket pointer/tag overhead is
separate: each 64-byte bucket
targets six entries, about 10.7 bytes per key at the expansion threshold and
about twice that immediately after a table doubles. Incremental rehashing can
temporarily retain both bucket arrays, but entries and key bytes are never
duplicated.
