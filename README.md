<div align="center">

# Keylane

### Redis-class performance. NVMe-scale capacity.

Keylane is built around a simple idea: use high-performance NVMe instead of
DRAM for the data capacity tier, then optimize every layer of the I/O path
until disk-resident workloads reach performance territory once reserved for
in-memory services.

**Keep the Redis interface. Break the memory-capacity ceiling.**

[![C++23](https://img.shields.io/badge/C%2B%2B-23-00599C?style=for-the-badge&logo=cplusplus)](CMakeLists.txt)
[![Redis Compatible](https://img.shields.io/badge/Redis-Compatible-DC382D?style=for-the-badge&logo=redis&logoColor=white)](tests/valkey/README.md)
[![Linux](https://img.shields.io/badge/Linux-x86__64%20%7C%20aarch64-FCC624?style=for-the-badge&logo=linux&logoColor=black)](docs/operations/building-and-packaging.md)
[![NVMe](https://img.shields.io/badge/Storage-io__uring%20%7C%20SPDK-5C2D91?style=for-the-badge)](docs/architecture/04-storage-and-recovery.md)

[![Star Keylane](https://img.shields.io/github/stars/thweetkomputer/keylane?style=for-the-badge&logo=github&label=Star%20Keylane&color=gold)](https://github.com/thweetkomputer/keylane/stargazers)

</div>

Keylane is a Linux C++23 key-value server that speaks RESP2 and RESP3. It keeps
a compact top-level key index in DRAM while storing data on existing files, raw
block devices, or SPDK NVMe namespaces. Capacity therefore scales primarily
with storage instead of requiring the complete dataset to remain in memory as
it does with Redis.

Keylane is designed for low-latency access to hundreds of millions of keys,
online space reclamation, and compatibility with existing Redis clients and
operational tooling.

> [!IMPORTANT]
> Keylane implements a broad Redis-compatible surface, but it is not a claim of
> complete Redis command or operational compatibility. Review the
> [current architecture](docs/architecture/README.md) and test your workload
> before adopting it.

## Highlights

- **NVMe-scale capacity.** Store data in preallocated regular files, raw Linux
  block devices, or NVMe namespaces accessed directly through SPDK.
- **Redis-compatible interface.** RESP2/RESP3, common Redis data structures,
  pipelining, authentication, TLS, Pub/Sub, Lua scripts, and Functions.
- **Rich data model.** Strings, Lists, Hashes, Sets, Sorted Sets, and Streams,
  with semantics exercised by vendored Valkey compatibility tests.
- **Atomic multi-key operations.** Cross-worker coordination for multi-key
  commands, `MULTI`/`EXEC`, `WATCH`, and declared-key Lua/Function calls.
- **Crash-consistent storage.** Checksummed append-only records, A/B metadata,
  transaction commit records, parallel recovery, TTL, and online defragmentation.
- **Replication and migration.** Native Keylane replication, Redis PSYNC
  following/export, Redis Sentinel integration, and Redis-compatible RDB
  import/export.
- **Operational visibility.** Prometheus metrics, Redis `INFO`, `SLOWLOG`,
  bounded memory admission, graceful shutdown, and configurable background
  maintenance.

## Architecture

Keylane is a single-process, layered system. Mutable state is partitioned by
worker, the complete top-level key index stays in memory, and record data is
placed on high-performance storage.

```text
┌─────────────────────────────────────────────────────┐
│ Redis compatibility & service layer                 │
│ RESP2/RESP3 · commands · sessions · Lua · Pub/Sub   │
├─────────────────────────────────────────────────────┤
│ Thread-per-worker coroutine runtime                 │
│ CPU affinity · busy polling · async I/O · mailboxes │
├─────────────────────────────────────────────────────┤
│ Routing & transaction coordination                  │
│ 16,384 hash slots · key intents · multi-key ops     │
├─────────────────────────────────────────────────────┤
│ In-memory hash index layer                          │
│ worker-local indexes · compact record locations     │
├─────────────────────────────────────────────────────┤
│ Durable storage engine                              │
│ append · flush · recovery · expiry · defragmentation│
├─────────────────┬─────────────────┬─────────────────┤
│ regular files   │ raw block I/O   │ SPDK NVMe       │
└─────────────────┴─────────────────┴─────────────────┘
```

| Layer | Responsibility | Key design choices |
|---|---|---|
| **Redis service** | Owns protocol compatibility and connection state | RESP2/RESP3, command dispatch, Lua/Functions, Pub/Sub, TLS, and Redis administration surfaces |
| **Worker runtime** | Executes network, command, and storage work | One native thread and coroutine scheduler per worker; workers can be pinned one-to-one to CPUs, busy-poll before parking, and exchange work through cross-core mailboxes |
| **Coordination** | Routes keys and serializes conflicting operations | Redis hash-slot ownership, worker-local shared/exclusive intents, and cross-worker transactions for atomic multi-key commands |
| **In-memory hash index** | Locates the newest logical version of every key | Worker-owned partition indexes retain compact key and record-location metadata in DRAM; values remain staged or storage-backed, and recovery rebuilds the indexes from durable records |
| **Storage engine** | Owns durable data and device capacity | Immutable record versions, checksummed metadata, batched direct I/O, parallel recovery, TTL, online defragmentation, and file/raw/SPDK backends |

Replication, memory admission, metrics, and graceful lifecycle management span
these layers rather than belonging to only one of them.

See the [architecture index](docs/architecture/README.md) for the authoritative
module map and deeper descriptions of request serving, transactions, storage,
recovery, and replication.

## Installation

### Prerequisites

The standard io_uring build requires:

- Linux (`x86_64` and `aarch64` are the release-package targets)
- CMake 3.20 or newer
- a C++23 compiler (GCC 13+ or a recent Clang is recommended)
- GNU Make, Git, and OpenSSL development headers/static libraries

On Ubuntu 24.04:

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake git libssl-dev
```

### Build from source

```bash
git clone https://github.com/thweetkomputer/keylane.git
cd keylane
git submodule update --init celer third_party/mimalloc third_party/nuraft
git -C third_party/nuraft submodule update --init asio
git -C celer submodule update --init third_party/liburing third_party/abseil

./scripts/build_release.sh
sudo install -m 0755 build/keylane /usr/local/bin/keylane
```

These commands initialize the default build's dependencies. DPDK/SPDK
dependencies are only needed for the optional kernel bypass build below.

The local release build uses `-march=native`. To create a portable archive for
the current architecture instead, run:

```bash
./scripts/package_release.sh
```

The archive is written under `dist/` using an `x86-64-v2` or `armv8-a` CPU
baseline. See [Building and packaging](docs/operations/building-and-packaging.md)
for compiler, sanitizer, CPU-target, and packaging details.

### Optional kernel bypass build

The default build includes kernel networking and io_uring storage only;
`keylane-meta` and ordinary Keylane deployments need no DPDK/SPDK dependencies.
For DPDK networking or userspace NVMe access, install the bypass dependencies
described in the build guide and enable both capabilities with:

```bash
cmake -S . -B build-bypass \
  -DCMAKE_BUILD_TYPE=Release \
  -DKEYLANE_ENABLE_OPT=ON \
  -DKEYLANE_KERNEL_BYPASS=ON
cmake --build build-bypass --target keylane -j"$(nproc)"
```

Select `--storage=spdk` at startup; compiling support alone keeps io_uring as
the default. SPDK device paths use the form
`spdk://<PCI-domain>:<bus>:<device>.<function>/<nsid>`.
Networking is selected independently with `--network=kernel|dpdk`; see
[runtime backend selection](docs/operations/building-and-packaging.md#runtime-backend-selection).
SPDK requires exclusive device ownership, host driver binding, DMA-capable
memory, and deployment-specific CPU/IRQ planning.

## Quick Start

Keylane never creates, extends, or truncates a storage path. For a throwaway
local instance, first provision a file and then start the server:

```bash
mkdir -p /tmp/keylane-quickstart
fallocate -l 1G /tmp/keylane-quickstart/keylane.data

./build/keylane \
  --data-file /tmp/keylane-quickstart/keylane.data
```

By default, Keylane listens on `127.0.0.1:6379`, uses every CPU in its inherited
affinity mask, and pins one worker to each CPU. It configures 256 MiB of storage
buffers per worker and writes logs to `./logs/keylane.log`. Use `taskset` or the
corresponding command-line options when the process should use fewer resources.
Production settings should be sized and benchmarked for the host and workload.

In another terminal, use any Redis-compatible client:

```bash
redis-cli PING
# PONG

redis-cli SET greeting "hello from Keylane"
# OK

redis-cli GET greeting
# "hello from Keylane"

redis-cli HSET user:42 name Ada language C++
redis-cli HGETALL user:42
```

Stop the server with `Ctrl-C`. A graceful shutdown drains admitted requests and
flushes active storage buffers; starting it again with the same `--data-file`
recovers the stored data before becoming ready.

For production storage:

- use persistent absolute paths rather than `/tmp`;
- provision every file before startup;
- make each fresh file an 8 MiB multiple and at least 80 MiB; Function catalog
  updates use the same foreground capacity as ordinary data;
- repeat `--data-file` to use multiple files or devices;
- always provide the complete device set when restarting an initialized
  multi-device instance.

Read [Multi-Device Storage](docs/operations/multi-device-storage.md) before
using raw devices or expanding an existing storage set.

Run `keylane --help` for all command-line options. Keylane also accepts a
Redis-style configuration file as its first argument.

## Benchmark

Read-only measurements on one server and one client, with **80 connections,
one outstanding request per connection, and 2 KiB (2,048-byte) values**.
Each configuration ran three 300-second rounds; the server CPU budget was
12 logical CPUs (6 physical cores with SMT).

| Configuration | Records | GET/s | Mean latency (µs) |
|---|---:|---:|---:|
| Keylane: kernel network + io_uring, raw RAID0 | 500 million | 218,223 | 366.23 |
| Keylane: kernel network + SPDK | 500 million | 237,099 | 337.04 |
| Keylane: DPDK + SPDK | 500 million | 303,841 | 262.93 |
| Aerospike CE: kernel network, raw RAID0 | 10 million | 212,075 | 376.80 |

Keylane used 1.024 TB of values and memtier/RESP; Aerospike used 20.48 GB
and asbench/native protocol with its read caches disabled. Dataset size,
record layout, storage topology, and client tools differ. The figures compare
these specific configurations; all formal rounds completed with zero errors
and misses. The io_uring row uses the default build with kernel bypass disabled.

See the [Keylane–Aerospike report](perf_reports/keylane-vs-aerospike-80conn-2k-2026-09-15/README.md)
for per-round results, versions, commands, raw evidence, and an offline
verification script.

### One million QPS on a 16-vCPU server

On a server with 16 logical CPUs and 16 pinned workers, Keylane served random
GETs from a one-billion-key SPDK dataset at **1,007,197 QPS** with 1,024-byte
values, 640 connections, and pipeline depth 1. All 64 million measured GETs
found an existing key, and no errors were reported. The 128-, 256-, and
512-byte workloads also exceeded one million QPS. See the
[one-billion-key, 16-worker benchmark report](perf_reports/keylane-spdk-dfly-bench-1b-value-size-limit-16worker-2026-08-31.md)
for the complete setup, latency measurements, and reproduction commands.

## Durability and compatibility notes

- An ordinary successful write is not a synchronous `fsync` durability fence.
  Keylane batches data and header flushes; graceful shutdown drains them. Read
  the [storage architecture](docs/architecture/04-storage-and-recovery.md)
  before selecting failure semantics for a deployment.
- Native and Redis replication continuation cursors are process-local. A
  restart can require a new full synchronization.
- Raw block and SPDK paths are destructive deployment boundaries: verify
  device identity and exclusive ownership before use.
- The current repository does not ship a Keylane systemd unit or an
  orchestration manifest. The deployment layer owns service supervision,
  persistent path provisioning, and resource isolation.

## Development and documentation

```bash
./scripts/build_debug.sh
ctest --test-dir build_debug --output-on-failure

# Vendored Valkey data-structure compatibility suites
KEYLANE_BIN="$PWD/build_debug/keylane" tests/valkey/run-keylane
```

Useful references:

- [Documentation index](docs/README.md)
- [Architecture](docs/architecture/README.md)
- [Operations](docs/operations/README.md)
- [Prometheus metrics](docs/operations/metrics.md)
- [Network IRQ affinity tuning](docs/operations/irq-affinity-tuning.md)
- [Performance reports](perf_reports/)

---

<div align="center">

### Help Keylane grow

If Keylane looks useful, please
**[star the project on GitHub](https://github.com/thweetkomputer/keylane)**.
It helps more Redis users and storage engineers discover the project.

[![Star Keylane on GitHub](https://img.shields.io/github/stars/thweetkomputer/keylane?style=for-the-badge&logo=github&label=Give%20Keylane%20a%20Star&color=gold)](https://github.com/thweetkomputer/keylane)

</div>
