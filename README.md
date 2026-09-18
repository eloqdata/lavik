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

<div align="center">

# Lavik

### Redis-class performance. NVMe-scale capacity.

Lavik is built around a simple idea: use high-performance NVMe instead of
DRAM for the data capacity tier, then optimize every layer of the I/O path
until disk-resident workloads reach performance territory once reserved for
in-memory services.

**Keep the Redis interface. Break the memory-capacity ceiling.**

[![C++23](https://img.shields.io/badge/C%2B%2B-23-00599C?style=for-the-badge&logo=cplusplus)](CMakeLists.txt)
[![Redis Compatible](https://img.shields.io/badge/Redis-Compatible-DC382D?style=for-the-badge&logo=redis&logoColor=white)](tests/valkey/README.md)
[![Linux](https://img.shields.io/badge/Linux-x86__64%20%7C%20aarch64-FCC624?style=for-the-badge&logo=linux&logoColor=black)](docs/operations/building-and-packaging.md)
[![NVMe](https://img.shields.io/badge/Storage-io__uring%20%7C%20SPDK-5C2D91?style=for-the-badge)](docs/architecture/04-storage-and-recovery.md)

[![Star Lavik](https://img.shields.io/github/stars/eloqdata/lavik?style=for-the-badge&logo=github&label=Star%20Lavik&color=gold)](https://github.com/eloqdata/lavik/stargazers)

</div>

Lavik is a Linux C++23 key-value server that speaks RESP2 and RESP3. It keeps
a compact key index in DRAM and stores data on files, raw block devices, or
SPDK NVMe namespaces, so capacity scales with storage.

> [!IMPORTANT]
> Lavik implements a broad Redis-compatible surface, but it is not a claim of
> complete Redis command or operational compatibility. Review the
> [current architecture](docs/architecture/README.md) and test your workload
> before adopting it.

## Highlights

- **Grow your dataset beyond RAM.** Keep a compact key index in memory and put
  the data on NVMe. Lavik serves datasets with hundreds of millions of keys
  without requiring every value to fit in DRAM, making storage capacity the
  main lever for dataset growth. Deploy on preallocated files, raw block
  devices, or directly on NVMe through SPDK.

- **Multiples of the throughput of disk-backed alternatives.** In the
  **one-billion-key, 1 TB** test, Lavik reached **784,179 GET QPS**:
  **2.07× Dragonfly's and 2.36× Garnet's peak read throughput**. In the broader
  200-million-key comparison, Lavik SPDK delivered **2.44–4.93× the throughput
  of Pika, Apache Kvrocks, and Tendis** across the tested read, write, and mixed
  workloads. See [disk-backed comparisons](#disk-backed-kv-systems).

- **NVMe storage, near in-memory performance.** On an AMD EPYC 9V74 server
  with 125 GiB RAM and six raw NVMe drives, Lavik reached **828,502 GET QPS**
  and **984,452 SET QPS** over **10 million keys with 1 KiB values**. That is
  **86–87% of tuned in-memory Redis/Valkey read throughput**, while writes
  were **12.5% faster than Redis and 23.7% faster than Valkey**. GET/SET p99
  latency was **4.543 / 4.575 ms**, comparable to both in-memory systems at
  peak throughput. See [in-memory comparisons](#in-memory-redis-and-valkey).

- **Put multiple CPU cores to work in one server.** Worker threads own their
  data partitions and execute requests in parallel. A C++23 coroutine runtime
  overlaps network and storage I/O, while CPU affinity and asynchronous
  io_uring access keep the serving path tuned for modern multicore NVMe hosts.
  The [startup tuning script](docs/operations/quick-start-tuning.md) generates
  a CPU plan for your machine.

- **Keep the Redis tools and data structures you know.** Use RESP2/RESP3
  clients with Strings, Lists, Hashes, Sets, Sorted Sets, and Streams. Build
  application logic with pipelining, Pub/Sub, Lua scripts, and Functions;
  connect with authentication and TLS. Redis-compatible RDB import/export and
  PSYNC replication provide migration paths for existing data, with
  compatibility exercised by the vendored Valkey test suites.

- **Atomic operations across worker partitions.** Multi-key commands,
  `MULTI`/`EXEC`, `WATCH`, and declared-key Lua/Function calls coordinate access
  across workers inside a node. Applications can combine related changes
  atomically while the server processes independent work in parallel. Cluster
  mode retains Redis Cluster's same-slot requirement for multi-key commands.

- **From a local process to a replicated cluster.** Redis Cluster routing,
  native replication, and a Raft-backed Meta control plane provide partitioned
  serving and coordinated failover. Prometheus metrics, `INFO`, and `SLOWLOG`
  expose operational health through familiar tools. The
  [cluster quick start](docs/operations/cluster-deployment.md) launches three
  Meta nodes and two primary/replica groups with one bootstrap command.

## Installation

### Prerequisites

The standard io_uring build requires:

- Linux 6.1 or newer with io_uring enabled (`x86_64` and `aarch64` are the release-package targets)
- CMake 3.26 or newer (required by the bundled Meta dependency)
- a C++23 compiler (GCC 13+ or a recent Clang is recommended)
- GNU Make, Git, and OpenSSL development headers/static libraries

On Ubuntu 24.04, install Git to obtain the source:

```bash
sudo apt-get update
sudo apt-get install -y git
```

### Build from source

```bash
git clone https://github.com/eloqdata/lavik.git
cd lavik
./scripts/install_build_deps.sh
git submodule update --init bycorf third_party/mimalloc third_party/nuraft
git -C third_party/nuraft submodule update --init asio
git -C bycorf submodule update --init third_party/liburing third_party/abseil

./scripts/build_release.sh
sudo install -m 0755 build/lavik build/lavik-meta build/lavik-ctl /usr/local/bin/
```

The dependency installer is shared with release CI. It supports Ubuntu 24.04
on x86_64 and ARM64; `--dry-run` shows the packages without installing them.
The commands above build with kernel TCP/io_uring. To include DPDK/SPDK in a
local build, run these additional commands in the same checkout:

```bash
./scripts/install_build_deps.sh --with-bypass
git -C bycorf submodule update --init third_party/spdk third_party/dpdk
git -C bycorf/third_party/spdk submodule update --init isa-l isa-l-crypto
CC=gcc-13 CXX=g++-13 ./scripts/build_release.sh -DLAVIK_KERNEL_BYPASS=ON
```

Local release builds use `-march=native`; use the packaging script below for
portable artifacts. Bypass remains an explicit runtime choice via
`--network=dpdk` and/or `--storage=spdk`.

To create a portable archive for the current architecture, run:

```bash
./scripts/package_release.sh
```

After installing the bypass dependencies, use
`LAVIK_PACKAGE_KERNEL_BYPASS=ON ./scripts/package_release.sh` for a standard
archive with DPDK/SPDK support.

The archive contains `lavik`, `lavik-meta`, `lavik-ctl`, `LICENSE`, and notices
under `dist/`. Without the bypass option, packaging builds the `minimal` variant. Main-branch
[Ubuntu release CI](.github/workflows/release.yml) builds both variants for
x86_64 and ARM64. Download the four archives and their SHA-256 files from
[Nightly](https://github.com/eloqdata/lavik/releases/tag/nightly), which is
updated after successful builds of the latest main. CI artifacts are also
retained for 30 days.

### Release hardware and platform requirements

| Package | Backends included | x86_64 CPU target | ARM64 CPU target |
|---|---|---|---|
| `lavik-<version>-linux-<arch>-minimal.tar.gz` | Kernel TCP and io_uring | Compiler default (`x86-64` with the CI toolchain) | Compiler default |
| `lavik-<version>-linux-<arch>.tar.gz` | Kernel TCP/io_uring, DPDK networking, and SPDK storage | `x86-64-v2` | `armv8-a+crc` |

Nightly uses `nightly` as the filename version so download URLs stay stable.
The archive's `VERSION` and `REVISION` files identify the source build.
Executable names are the same in both variants: `lavik`, `lavik-meta`, and
`lavik-ctl`. Minimal rejects `--network=dpdk` and `--storage=spdk` with a
configuration error; `--network=kernel` and `--storage=uring` remain available.

Neither CI variant uses `-march=native`. On x86, the standard package requires
all x86-64-v2 features, including SSE3, SSSE3, SSE4.1, SSE4.2, POPCNT,
CMPXCHG16B, and LAHF/SAHF in 64-bit mode. CPUs or VMs lacking any of these
features must use `minimal`. On ARM64, the standard package additionally
requires the CRC32 instruction extension. The requirement applies to the
package even when bypass is not selected at startup.

Prebuilt packages use these CPU targets for portability across supported
machines. To enable additional CPU optimizations available on your deployment
machine, [build from source](#build-from-source) there with
`./scripts/build_release.sh`, which uses `-march=native`. Performance gains
depend on the workload and toolchain; the resulting binaries may not run on
CPUs with fewer instruction-set features.

Both variants require **Linux 6.1 or newer** with usable io_uring for `lavik`
and `lavik-meta`, including when DPDK/SPDK is selected. Worker initialization
requires `IORING_SETUP_DEFER_TASKRUN`, introduced in Linux 6.1, with no fallback
to older kernels. This is the API compatibility floor, not a tested-kernel
matrix; see [Kernel requirements](docs/operations/building-and-packaging.md#kernel-requirements).
CI builds on Ubuntu 24.04; artifacts also require a compatible glibc and are
not intended for older glibc environments.
The standard package additionally needs the NUMA and UUID runtime libraries
(`libnuma1` and `libuuid1` on Ubuntu). It defaults to kernel TCP/io_uring;
DPDK/SPDK must be explicitly selected and provisioned. Its bundled DPDK network
drivers are TAP, ring, and virtio, so it does not support every physical NIC.
SPDK mode requires an SPDK-accessible NVMe device.

See [Building and packaging](docs/operations/building-and-packaging.md)
for dependency setup, standard-package builds, and compatibility details.

## Quick Start

For deployment beyond the basic example below:

- [Quick startup tuning](docs/operations/quick-start-tuning.md): generate this
  machine's CPU plan and start Lavik.
- [Cluster deployment](docs/operations/cluster-deployment.md): launch the local
  Meta-managed cluster example.

For a throwaway local instance, provision a file and start the server:

```bash
mkdir -p /tmp/lavik-quickstart
fallocate -l 1G /tmp/lavik-quickstart/lavik.data

./build/lavik \
  --data-file /tmp/lavik-quickstart/lavik.data
```

In another terminal, use any Redis-compatible client:

```bash
redis-cli PING
# PONG

redis-cli SET greeting "hello from Lavik"
# OK

redis-cli GET greeting
# "hello from Lavik"

redis-cli HSET user:42 name Ada language C++
redis-cli HGETALL user:42
```

Stop the server with `Ctrl-C`. See [Multi-Device Storage](docs/operations/multi-device-storage.md)
for persistent files, raw devices, and storage expansion.

## Benchmark

**Lavik delivers multi-fold throughput gains over several disk-backed KV
systems and approaches tuned in-memory Redis and Valkey performance.** The
measurements below show where those gains occur, including the remaining read
throughput gap and Lavik's higher peak write throughput in the in-memory test.

Browse the [performance reports](perf_reports/README.md) for full results in
English and Simplified Chinese.

### Disk-backed KV systems

Our [Redis-compatible storage-tier benchmark](perf_reports/lavik-vs-dragonfly-tiering-2026-08-11/README.md)
compares Lavik with Garnet, Dragonfly, Pika, Apache Kvrocks,
Tendis, and KeyDB On Flash. Server and client ran on separate Azure
`Standard_L16s_v3` VMs. Each backend used two NVMe drives and **200 million
keys with uniformly random 1–4 KB values**, measured with 80 connections over
300 seconds per workload, without a QPS limit. Results below are from the
report's August 12, 2026 rerun.

![Read, mixed, and write throughput across Redis-compatible storage tiers](perf_reports/charts/tiering-throughput.svg)

| System / storage backend | Read-only QPS | Write-only QPS | 1:1 read/write QPS |
|---|---:|---:|---:|
| **Lavik SPDK** | **310,387** | **392,284** | **352,442** |
| **Lavik io_uring — raw devices** | **278,925** | **393,733** | **328,831** |
| **Lavik io_uring — XFS files** | **272,727** | **385,324** | **326,110** |
| Garnet Storage Tier | 205,297 | 361,960 | 209,366 |
| Dragonfly Tiered Storage | 187,653 | 199,234 | 207,248 |
| Pika | 86,669 | 79,537 | 75,324 |
| Apache Kvrocks | 70,672 | 110,167 | 88,084 |
| Tendis | 68,860 | 160,642 | 102,213 |
| KeyDB On Flash | 6,146 | 5,395 | 5,197 |

Across these read, write, and mixed workloads, **Lavik SPDK delivered
2.44–4.93× the throughput of Pika, Apache Kvrocks, and Tendis**. Its read
throughput was **1.65× Dragonfly's and 1.51× Garnet's**; write throughput was
**1.97× and 1.08×**, respectively.

The separate [1 TB storage-tier test](perf_reports/lavik-vs-redis-valkey-iothreads-10g-1k-2026-09-06/README.md#1-tb-storage-tier-lavik-leads-dragonfly-and-garnet)
used **one billion 1 KiB values** on an AMD EPYC 9V74 server with six NVMe
drives. Lavik raw io_uring peaked at **784,179 GET QPS**, or **2.07× Dragonfly's
and 2.36× Garnet's peak read throughput**. Its **856,523 SET QPS** was
**1.44× Dragonfly's and 1.12× Garnet's**. These are peak-to-peak comparisons
within that test's own concurrency sweep, separate from the dual-NVMe table
above.

Lavik SPDK's read p99 was **0.455 ms**, versus 2.303 ms for Garnet and
2.911 ms for Dragonfly. These results use each system's recorded
cache, warmup, and persistence settings; uniform random access is unfavorable
to KeyDB On Flash's hot-tier design. The report documents those differences
and reproduction commands. Its separate Azure Managed Redis test used a
different dataset and duration and is excluded from this chart and table.

See also the [100-million-record YCSB comparison with Aerospike](perf_reports/ycsb-rerun-2026-09-13/README.md),
covering workloads A/B/C/D and their tail latencies.

### In-memory Redis and Valkey

The [Redis/Valkey comparison](perf_reports/lavik-vs-redis-valkey-iothreads-10g-1k-2026-09-06/README.md)
used an AMD EPYC 9V74 server with 125 GiB RAM and **10 million keys with 1 KiB
values** (about 10 GB). Lavik used io_uring on six raw NVMe devices; Redis
8.8.0 and Valkey 9.1.0 held the complete dataset in memory. Tests swept
80–1,280 connections with pipeline=1 and 30-second measurement windows,
selecting each in-memory system's best measured I/O-thread setting per command.

![Lavik versus tuned in-memory Redis and Valkey across connection counts](perf_reports/lavik-vs-redis-valkey-iothreads-10g-1k-2026-09-06/best-memory-vs-lavik-qps.svg)

| System | Peak GET QPS | GET p99 | Peak SET QPS | SET p99 |
|---|---:|---:|---:|---:|
| **Lavik — raw io_uring** | **828,502** | **4.543 ms** | **984,452** | **4.575 ms** |
| Redis 8.8.0 | 964,267 | 4.671 ms | 874,879 | 4.767 ms |
| Valkey 9.1.0 | 948,300 | 4.479 ms | 796,145 | 4.319 ms |

**Lavik approaches in-memory read throughput and exceeds both systems' peak
write throughput in this test.** It achieved **85.9% of Redis's and 87.4% of
Valkey's peak GET QPS**—a read gap of 14.1% and 12.6%—while SET throughput was
**12.5% above Redis and 23.7% above Valkey**, with similar p99 latency. All
peaks occurred at 1,280 connections. Redis and Valkey
had AOF and automatic RDB saves disabled; Lavik had defragmentation paused.
This 10 GB test is independent of the larger storage-tier benchmark above.

## Important notes

- A successful write is not a synchronous `fsync` fence; see the
  [storage architecture](docs/architecture/04-storage-and-recovery.md) before
  selecting failure semantics.
- A replication restart can require a full synchronization.
- Raw block and SPDK paths require exclusive device ownership.

## Development

```bash
./scripts/build_debug.sh
ctest --test-dir build_debug --output-on-failure

# Vendored Valkey data-structure compatibility suites
LAVIK_BIN="$PWD/build_debug/lavik" tests/valkey/run-lavik
```

Useful references:

- [Documentation index](docs/README.md)
- [Architecture](docs/architecture/README.md)
- [Operations](docs/operations/README.md)
- [Prometheus metrics](docs/operations/metrics.md)
- [Network IRQ affinity tuning](docs/operations/irq-affinity-tuning.md)
- [Performance reports](perf_reports/README.md)

---

<div align="center">

### Help Lavik grow

If Lavik looks useful, please
**[star the project on GitHub](https://github.com/eloqdata/lavik)**.
It helps more Redis users and storage engineers discover the project.

[![Star Lavik on GitHub](https://img.shields.io/github/stars/eloqdata/lavik?style=for-the-badge&logo=github&label=Give%20Lavik%20a%20Star&color=gold)](https://github.com/eloqdata/lavik)

</div>

## License

EloqData-authored code is licensed under [Apache-2.0](LICENSE).
Third-party components retain their original copyright notices and licenses;
see [NOTICE](NOTICE) and the notices in each dependency.
