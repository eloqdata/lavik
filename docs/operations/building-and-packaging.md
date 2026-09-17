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

# Building and packaging

## Kernel requirements

Running `lavik` or `lavik-meta` requires **Linux 6.1 or newer** with io_uring
enabled. This applies to both `minimal` and standard packages on x86_64 and
ARM64. Selecting DPDK/SPDK does not remove the requirement: workers retain an
io_uring instance for runtime services, including timers and wakeups.

Bycorf creates each worker ring with `IORING_SETUP_DEFER_TASKRUN`, available
since Linux 6.1, and does not retry initialization without that flag. The other
required setup flags, `SINGLE_ISSUER`, `COOP_TASKRUN`, and `TASKRUN_FLAG`, were
available by Linux 6.0. See the upstream
[io_uring setup manual](https://github.com/axboe/liburing/blob/master/man/io_uring_setup.2).
The runtime's separate `IORING_FEAT_NODROP` check mentions Linux 5.5, but that
is only the minimum for that feature, not for the complete runtime.

Linux 6.1 is the API compatibility floor, not a claim that every 6.1 kernel
has been tested. Use an updated distribution kernel with its bug fixes;
release CI builds on Ubuntu 24.04 and does not test a Linux 6.1 kernel matrix.
For containers, the host kernel must meet this requirement and the container
policy must allow `io_uring_setup`, `io_uring_enter`, and `io_uring_register`.
CPU instruction-set and glibc requirements remain separate constraints.

## Local builds

On Ubuntu 24.04 (x86_64 or ARM64), install build packages with the same script
used by release CI:

```bash
./scripts/install_build_deps.sh
```

The script invokes `sudo` only for apt when needed. Add `--dry-run` to inspect
the commands without changing the system. It does not initialize Git
submodules; follow the complete source checkout steps in the
[README](../../README.md#build-from-source). Other distributions require
manual installation of equivalent dependencies.

Optimized local builds use the current machine's instruction set by default:

```bash
./scripts/build_release.sh
```

Additional arguments are forwarded to CMake, for example
`./scripts/build_release.sh -DLAVIK_KERNEL_BYPASS=ON` after installing and
initializing the bypass dependencies below.

`LAVIK_KERNEL_BYPASS` defaults to `OFF`: Lavik and `lavik-meta` build
with kernel networking and io_uring and do not configure or link DPDK, SPDK or
the private FreeBSD stack. Set `-DLAVIK_KERNEL_BYPASS=ON` to include both
bypass capabilities. Lavik sets `BYCORF_KERNEL_BYPASS` from this single
option, including when reconfiguring an existing build directory.

This configures `LAVIK_MARCH=native`, including Bycorf, mimalloc, and the
Abseil CRC translation units used by the durable storage format. The latter is
important because Abseil compiles its hardware CRC engine only when the target
exposes the required instruction macros; leaving those translation units at
the compiler baseline silently selects its generic table implementation.
OpenSSL is linked statically, so the resulting executable does not depend on
`libssl.so` or `libcrypto.so`. The build machine still needs the OpenSSL
headers and static archives (`libssl-dev` on Ubuntu, which provides `libssl.a`
and `libcrypto.a`).

For a different local CPU target, configure CMake directly with
`-DLAVIK_MARCH=<target>`. An empty value disables the explicit `-march` flag.

Use `-DLAVIK_BYCORF_SOURCE_DIR=/absolute/path/to/bycorf-worktree` to build and
test a separate Bycorf checkout without replacing the repository's submodule.
The default remains the pinned `bycorf/` checkout. Record both revisions when
comparing performance with an alternate runtime.

The pinned runtime is hosted at [eloqdata/bycorf](https://github.com/eloqdata/bycorf).
In an existing checkout, refresh the locally cached submodule URL before updating:

```bash
git submodule sync -- bycorf
git submodule update --init bycorf
```

When aggressive optimization is enabled, CMake's IPO support configures both
compilation and linking for the non-Debug server and every bundled runtime
library that feeds it, including Bycorf and the C libraries. Test-only
executables omit IPO because their deliberately oversized coroutine stress
cases can trigger GCC compiler failures; they still link against the optimized
production libraries. Clang test links enable its LLVM bitcode reader without
compiling the test sources with IPO. Debug builds also omit IPO to keep iteration time
predictable. LTO is not required for functional correctness.

The release build produces `lavik`, `lavik-meta`, and the Raft-free
`lavik-ctl` operator client. `lavik-meta` and `lavik-ctl` provide direct
administration and cluster readiness. To build them from an existing build
directory:

```bash
cmake --build <build-dir> --target lavik-meta lavik-ctl
```

The downloadable release archive below includes all three executables.

### Experimental DPDK networking

On Ubuntu 24.04, install the additional build dependencies before configuring
bypass support:

```bash
./scripts/install_build_deps.sh --with-bypass
```

This includes the native build tools and SPDK's RPC generator dependencies,
`python3-jinja2` and `python3-tabulate`, without relying on preinstalled packages
on a developer machine or CI image.

The pinned Bycorf includes an optional FreeBSD/DPDK IPv4 TCP backend for
AArch64 and x86-64. The default network backend remains Linux TCP/io_uring.
Initialize the required dependencies explicitly; SPDK uses Bycorf's direct DPDK
submodule, so its nested DPDK checkout is not needed:

```bash
git submodule update --init bycorf third_party/mimalloc third_party/nuraft
git -C third_party/nuraft submodule update --init asio
git -C bycorf submodule update --init third_party/liburing third_party/abseil \
  third_party/spdk third_party/dpdk
git -C bycorf/third_party/spdk submodule update --init isa-l isa-l-crypto
cmake -S . -B build-dpdk-net -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo -DLAVIK_ENABLE_OPT=OFF \
  -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ \
  -DLAVIK_KERNEL_BYPASS=ON -DBUILD_TESTING=OFF
cmake --build build-dpdk-net --target lavik -j4
```

On AArch64, use GCC for the bypass build because the pinned SPDK ISA-L Crypto
dependency requires GCC. The private FreeBSD stack is built separately with
Clang by the BSD build helper; both compilers are therefore required. Ordinary
kernel/io_uring builds support Clang. CMake invokes the BSD build helper
automatically; Python 3 remains a build dependency. See Bycorf's
[prototype runbook](../../bycorf/docs/dpdk-prototype.md) for prerequisites, TAP
setup, physical-device selection, poll/adaptive mode,
and queue configuration. The default device is a virtual TAP. Ordinary data
files use `--storage=uring`; the bypass build also supports
`--storage=spdk` and `spdk://` NVMe paths. Select `--network=dpdk` explicitly;
compiling support alone leaves the default kernel network active. Use a fresh
disposable file and disable the metrics listener with
`--metrics-port=0` for the initial standalone SET/GET run. TLS, replication,
and cluster use are outside this prototype's validation scope.

The default DPDK build supports up to 128 network workers. Set
`-DBYCORF_DPDK_MAX_WORKERS=N` to change this capacity (1–1023); Bycorf builds a
matching FreeBSD stack and DPDK with `N+1` lcore registration slots, and links
SPDK against that same DPDK. An external `BYCORF_DPDK_PREFIX` must have enough
slots or configuration fails. This is a build capacity, not the active thread
count; `--threads` chooses that at startup. RSS still requires a queue pair per
worker; hash steering can use fewer queues. See Bycorf's
[worker capacity guide](../../bycorf/docs/dpdk-prototype.md#worker-capacity).

### Runtime backend selection

Build with `-DLAVIK_KERNEL_BYPASS=ON` to include all four
combinations in one executable. Startup defaults are `--network=kernel
--storage=uring`, independent of build capabilities.

| Network | Storage | Flags |
|---|---|---|
| Kernel TCP | Kernel file/block I/O | `--network=kernel --storage=uring` |
| Kernel TCP | SPDK NVMe | `--network=kernel --storage=spdk` |
| DPDK/FreeBSD TCP | Kernel file/block I/O | `--network=dpdk --storage=uring` |
| DPDK/FreeBSD TCP | SPDK NVMe | `--network=dpdk --storage=spdk` |

SPDK selection requires every `--data-file` to use `spdk://`; io_uring requires
kernel paths. Unsupported compiled capabilities and mismatched paths fail
before device initialization. Existing SPDK launch commands must now include
`--storage=spdk`, and existing DPDK network commands must include
`--network=dpdk`. Backend selection is not a live `CONFIG SET` option.

Device binding remains an operator step. Supply the complete selected NIC and
NVMe allowlist in `BYCORF_EAL_ARGS` before launch; either accelerator can be the
first EAL user. With neither selected, EAL and its device discovery are inactive.
Changing modes requires a clean process stop and appropriate device binding.
The private TCP stack still has the prototype compatibility limits above.

The disposable-file regression covers kernel networking and recovery:

```bash
python3 tests/runtime_backends_smoke.py build-dpdk-net/lavik
# Optional TAP test, without physical NIC rebinding:
sudo python3 tests/runtime_backends_smoke.py build-dpdk-net/lavik --dpdk
```

### AddressSanitizer builds

AddressSanitizer builds use Clang so coroutine symmetric transfers remain tail
calls under sanitizer instrumentation:

```bash
./scripts/build_asan.sh
ctest --test-dir build_asan --output-on-failure
```

The script defaults to `clang-18` and `clang++-18`. Override them with
`LAVIK_ASAN_CC`, `LAVIK_ASAN_CXX`, and use `LAVIK_ASAN_BUILD_DIR` to select
a different build directory. It enables the non-packageable fault-server
variant so crash-safety hooks remain available even though RelWithDebInfo may
define `NDEBUG`.

Tests place disposable data under `/tmp` by default. Set
`LAVIK_TEST_DATA_DIR` to an existing, writable directory to relocate test
devices, logs, snapshots, and other generated artifacts for CTest, direct
test-binary, shell, and Python test runs:

```bash
LAVIK_TEST_DATA_DIR=/path/to/test-data \
  ctest --test-dir <build-dir> --output-on-failure
```

An unset or empty value uses `/tmp`. Trailing directory separators are
accepted. An explicit work-directory argument to a Python harness takes
precedence over this environment variable. The focused failover gates inherit
the same setting; their auxiliary Unix sockets use short build-directory
paths to stay within the platform's socket-name limit.

The focused large-Hash durability suite uses its own temporary 128 MiB files
and local child servers. Run it against a Debug build or a build configured
with `LAVIK_BUILD_FAULT_SERVER=ON` to exercise the crash injections:

```bash
cmake --build bld-clang18-debug --target lavik lavik_list_e2e_test -j 8
ctest --test-dir bld-clang18-debug -R '^lavik_large_hash_durability_e2e$' --output-on-failure
```

Substitute your configured build directory. This suite covers large Hash
command compatibility, extent durability, grouped writes and bounded-device
reclamation. The dedicated grouped-storage suites cover additional graph
publication, relocation and snapshot boundaries.
Crash cases require exit code 86 at their armed boundary; ordinary
release builds without test instrumentation skip those cases and the injected
storage-admission failure case. Bounded-device reclamation and command-level
RESP OOM cases run without fault instrumentation. The grouped side-index
memory/admission tests are part of `lavik_unit_tests`.

The grouped-storage recovery suite constructs its own temporary disk images
and starts local child servers. It exercises actual group-record recovery,
transaction decisions, GC and snapshot lifetimes without touching configured
benchmark devices:

```bash
cmake --build bld-clang18-debug --target lavik lavik_grouped_recovery_e2e_test -j 8
ctest --test-dir bld-clang18-debug -R '^lavik_grouped_recovery_e2e$' --output-on-failure
```

Finish linking the child server before running either integration suite; do
not rebuild that executable while its test fixture starts server processes.
The foreground suites use the actual command handlers and their own temporary
devices, including oversized elements, transaction failure and cold restart:

```bash
cmake --build bld-clang18-debug --target lavik lavik_grouped_hash_write_e2e_test lavik_grouped_ordered_write_e2e_test -j 8
ctest --test-dir bld-clang18-debug -R '^lavik_grouped_(hash|ordered)_write_e2e$' --output-on-failure
```

The collection write-concurrency suite uses cold compact and grouped records
and deterministic Debug pauses to verify that unrelated keys progress while
the same key stays locked. Grouped cases cover page preparation, Sorted Set
member-index preparation, oversized extent IO and shutdown during that IO.
It also checks command semantics, TTL, WATCH, EXEC/Lua, cold recovery and grouped
promotion. Creation cases cover missing keys, compact and directly grouped
values, both Sorted Set indexes, oversized extents, expired/deleted predecessors
and allocation failure before publication:

```bash
cmake --build bld-clang18-debug --target lavik lavik_compact_collection_write_e2e_test -j 8
ctest --test-dir bld-clang18-debug -R '^lavik_compact_collection_write_e2e$' --output-on-failure
```

Hash, Set, List and Sorted Set automatically promote to grouped storage at
16 KiB of encoded collection data in both ordinary and Debug builds. Promotion
does not need an environment switch. Debug/fault builds additionally provide
the crash and admission hooks exercised by the integration tests.
The current adapter and integration limits are documented in
[Grouped collections](../architecture/09-grouped-collections.md).

### Adding deterministic fault sites

Use `include/lavik/fault_injection.h` for internal crash, allocation-failure
and scheduling hooks. Its single build policy enables hooks in Debug or with
`LAVIK_BUILD_FAULT_SERVER=ON`; ordinary Release builds erase the hook bodies
and their arguments, including environment lookups and injected suspension
points. Set fault environment variables before launching the server, not
concurrently with its workers.

```cpp
LAVIK_FAULT_BAD_ALLOC("LAVIK_FAIL_GROUP_HANDOFF_KEY", key);
LAVIK_MAYBE_CRASH_AT("group-batch-before-root");
LAVIK_FAULT_INJECT(
    if (LAVIK_FAULT_MATCHES("LAVIK_TEST_PAUSE_KEY", key)) {
      // Keep the existing coroutine, lock ownership and error handling.
      auto status = co_await bycorf::SleepFor(worker, delay);
      if (!status.ok()) co_return status;
    });
```

Use `LAVIK_FAULT_MATCHES_NTH` for an exact key plus a one-based position
within the current operation. It does not introduce a shared hit counter.
Keep fault effects inside the existing rollback/commit boundary.
`LAVIK_FAULT_INJECT` introduces a block, not a coroutine or lambda;
cross-scope diagnostic declarations and outer-loop `break`/`continue` need
the central `#if LAVIK_FAULTS_ENABLED` guard instead. The crash selector
`LAVIK_CRASH_POINT` is cached on first use and terminates with exit code 86
without flushing or unwinding.

The `lavik_fault_injection_*` CTest cases independently compile the helper
in Debug, ordinary Release and fault-enabled Release modes. Integration
fixtures that require a hook must skip against ordinary Release servers.

### TTL index memory reclamation

The Tomb Raider suite checks that TTL expiration and tombstone reaping release
retained index memory. It writes 102,400 inline keys of 1 KiB each (100 MiB of
key bytes), with one-byte values:

```bash
cmake --build build-clang --target lavik lavik_tomb_raider_e2e_test -j 8
ctest --test-dir build-clang -R '^lavik_tomb_raider_e2e$' --output-on-failure
```

Substitute the configured build directory as needed. The suite owns a
temporary 1 GiB data file under `/tmp`, pauses defrag, and checks `used_memory`
after all expiring keys have been reaped. One permanent key remains. It can
take several minutes and has a 600-second CTest timeout. Process RSS is reported
separately as `used_memory_rss` and is not the reclamation assertion.

### Large collection stress tests

The opt-in aggregate-size tests exercise collections whose encoded contents
exceed 1 GiB, without a single aggregate import or COPY buffer:

```bash
cmake --build bld-clang18-debug --target lavik_replica_abort_reclaim_e2e_test lavik_grouped_ordered_write_e2e_test lavik -j 8
bld-clang18-debug/lavik_replica_abort_reclaim_e2e_test --large-list
bld-clang18-debug/lavik_replica_abort_reclaim_e2e_test --large-hash
LAVIK_RUN_LARGE_RDB=1 bld-clang18-debug/lavik_grouped_ordered_write_e2e_test \
  bld-clang18-debug/lavik \
  --gtest_filter=GroupedRdbStreamE2e.LargeListOverOneGiBImportsAndExportsWithoutAggregate
```

These are correctness tests, not throughput benchmarks. They use temporary
files under `/mnt/dev`, require several GiB of free space per concurrent test,
and can take tens of minutes with Debug instrumentation. The native tests use
8 GiB sparse device files. The RDB test additionally retains input and output
files, validates every exported item, and performs a cold restart; its longer
startup/shutdown deadlines apply only to this opt-in case. A failed large RDB
case preserves its files and phase logs for diagnosis. Do not point these
fixtures at an existing database or benchmark block device.

### Cluster fault tests

Cluster fault tests use a dedicated build and bounded tier runner:

```bash
cmake -S . -B build_cluster_fault -DCMAKE_BUILD_TYPE=Debug \
  -DLAVIK_ENABLE_OPT=OFF -DLAVIK_STATIC_OPENSSL=ON \
  -DBUILD_TESTING=ON -DLAVIK_BUILD_FAULT_SERVER=ON
./scripts/run_cluster_fault_tests.sh --tier model
./scripts/run_cluster_fault_tests.sh --tier integration
./scripts/run_cluster_fault_tests.sh --tier soak --duration 600
```

See [`tests/cluster/README.md`](../../tests/cluster/README.md) for the
determinism boundary, invariant matrix, trace/replay commands, and hardware
allowlist rules. The CTest labels are `cluster-model`,
`cluster-integration`, `cluster-soak`, and `cluster-hardware`.

## Continuous integration

The [CI workflow](../../.github/workflows/ci.yml) runs on pushes to `main`, pull
requests, and manual dispatch. A formatting job checks every maintained Lavik C/C++
source through the pinned pre-commit hook. Two independent test jobs build and
run natively on `ubuntu-24.04` (AMD64) and `ubuntu-24.04-arm` (ARM64).

The workflow initializes the public `eloqdata/bycorf` submodule at the exact
gitlink revision from the tested Lavik commit. It does not need a deploy key or
an extra Actions secret, so fork pull requests can run the same software suite.
The main checkout does not persist credentials.

Both use Clang 18, Debug, `BUILD_TESTING=ON`, `LAVIK_BUILD_META=ON`,
`LAVIK_BUILD_FAULT_SERVER=ON`, and `LAVIK_ENABLE_OPT=OFF`. Debug is required
for the Meta fault gates; the Data fault server alone does not enable them.
Redis, Python, and TCL are installed before configuration so the conditional
integration targets are present. The jobs fetch the pinned io_uring runtime
dependencies; SPDK is not part of this build.
`BUILD_TESTING=ON` also enables Bycorf's registered software regressions,
including connection/timer, connection-storage lifetime, and backend-selection
checks, through Bycorf's own CTest definitions.
Process fixtures that use more than two workers disable CPU pinning, preserving
cross-worker coverage on two-CPU runners.

After building all targets, run the same suite locally with:

```bash
./scripts/run_ci_tests.sh build_ci
```

The runner executes all registered CTest cases serially, the three opt-in large
codec regressions, native large-List and large-Hash tests, the >1 GiB RDB
import/export test, and every vendored Valkey TCL suite under its compatibility
harness policy. It continues with the remaining suites after a failure and
returns nonzero if any suite fails. The ordinary CTest report still marks the
large codec cases disabled and the large RDB case skipped; their explicit runs
have separate logs. The hardware safety gate skips because hosted runners have
no allowlisted scratch block device. Raw-device/SPDK verification requires a
separate hardware host.

Allow several GiB of free space for private test files under `/mnt/dev` and
`/tmp`, and enable io_uring with a sufficient memlock limit. CI prepares these
on its disposable VMs. Test logs and JUnit results live in
`<build-dir>/test-results/`; CI retains them as a per-architecture artifact for
seven days. The test jobs run independently of formatting and of each other's
outcome.

## Source formatting

Lavik uses the Google style, parses source as C++23, and pins clang-format
23.1.1. Its Bycorf submodule maintains its own formatter pin. Install `pre-commit`
once and enable the repository hook:

```bash
sudo apt-get install pre-commit
pre-commit install
```

The first run creates an isolated hook environment and downloads the pinned
formatter; clang-format is not a Lavik runtime or build dependency. Commits
then format staged first-party C and C++ files. When formatting changes a file,
the commit stops so the result can be reviewed and staged before retrying. To
format every maintained source file explicitly, run:

```bash
pre-commit run clang-format --all-files
```

The CMake `format` and `format-check` targets use a system installation only
when it reports exactly version 23.1.1. This exact check prevents a local tool
upgrade from silently rewriting unrelated code. The pre-commit hook is the
portable path when that system binary is unavailable.

## Downloadable release package

```bash
./scripts/package_release.sh
```

The packaging script builds `lavik`, `lavik-meta`, and `lavik-ctl` in Release
mode, statically links OpenSSL plus the GNU C++/compiler runtimes, strips staged
copies, verifies each executable's linkage and `--help`, and writes a
versioned archive and `.tar.gz.sha256` file under `dist/`. The checksum uses a
relative archive name so `sha256sum --check *.sha256` works after downloading.
The archive carries the project LICENSE and NOTICE, plus
the Apache-2.0 license text required by the statically linked OpenSSL code.
It explicitly configures `LAVIK_BUILD_META=ON`, `BUILD_TESTING=OFF`, and
`LAVIK_BUILD_FAULT_SERVER=OFF`; CMake also rejects the fault-server option
whenever `BUILD_TESTING` is off.

Unlike a local build, a package never selects `native` by default. The `minimal`
variant uses the compiler's default CPU target on both x86_64 and aarch64,
with no explicit `-march` flag for Lavik. The standard package uses the fixed
targets listed below.

For `minimal`, the script explicitly passes `-DLAVIK_MARCH=` so that
CMake's local `native` default and cached CPU targets cannot leak into the
package. Check the release toolchain's default target: it determines the CPU
baseline when `-march` is omitted. On GCC, `c++ -Q --help=target` reports it.
Toolchain files and externally supplied compiler flags must also be considered.

Override the Lavik target with `LAVIK_PACKAGE_MARCH` when producing a package
for a specific fleet; an explicitly empty value selects the compiler default
on either architecture. This does not lower DPDK/SPDK's CPU requirements in a
standard package. Other useful overrides are `LAVIK_PACKAGE_BUILD_DIR`,
`LAVIK_PACKAGE_OUTPUT_DIR`, and `LAVIK_PACKAGE_JOBS`.
`LAVIK_PACKAGE_VERSION=nightly` selects stable nightly archive names without
changing the source version recorded in `VERSION` or the full commit in
`REVISION`. The moving `nightly` tag is excluded from source-version discovery.

The bundled Abseil CRC32C engine requires both SSE4.2 and PCLMUL at compile time
to enable its x86 hardware implementation. A baseline `x86-64` build uses its
software implementation even on a CPU that supports these instructions;
`x86-64-v2` also lacks PCLMUL and therefore does not enable that engine.
Globally enabling these instructions raises the package's CPU requirements;
it does not provide runtime fallback for older x86 CPUs.

The release remains a normal Linux ELF executable and therefore uses the
platform C library. Build official artifacts in the oldest supported Linux
environment so their glibc requirement remains compatible with newer systems.

### Standard packages and main-branch CI

After installing the prerequisites and initializing the pinned submodules in
[Experimental DPDK networking](#experimental-dpdk-networking), build a package
with both network and storage bypass capabilities:

```bash
CC=gcc-13 CXX=g++-13 LAVIK_PACKAGE_KERNEL_BYPASS=ON ./scripts/package_release.sh
```

This produces a `lavik-<version>-linux-<arch>.tar.gz` archive containing
the same three applications. Without this environment variable the default is
`OFF`, producing `lavik-<version>-linux-<arch>-minimal.tar.gz`. The script sets
the CMake option explicitly to prevent a previous build directory's setting
from leaking into the package. A standard
binary still starts with kernel TCP and io_uring; select `--network=dpdk`
and/or `--storage=spdk` to activate those capabilities. The bundled network
PMDs are Bycorf's current defaults: TAP, ring, and virtio; this package does not
include drivers for every physical NIC.

| Architecture | `minimal` target | Standard target (DPDK/SPDK) |
|---|---|---|
| x86_64 | Compiler default | `x86-64-v2` |
| aarch64 / ARM64 | Compiler default | `armv8-a+crc` |

Bypass dependencies have their own CPU baselines: the pinned DPDK uses
`platform=generic` (x86 `corei7`/SSE4.2; ARM `armv8-a+crc`). The bypass targets
above satisfy its public inline headers, including SPDK's DPDK adapter;
forcing that adapter to plain `x86-64` fails to compile. A bypass package does
not support every CPU allowed by plain compiler-default code.
The script overrides SPDK's independent native default through its supported
`TARGET_ARCHITECTURE` make variable, using the bypass target. It cleans previous
SPDK build outputs first because SPDK builds in its source tree and does not
track CPU flag changes. Do not build SPDK concurrently from another build directory
using the same checkout.

The bypass binaries additionally use system libraries such as NUMA and UUID;
on Ubuntu install `libnuma1` and `libuuid1`. Static OpenSSL and C++ runtime
checks apply to all three executables in either package variant.

[Ubuntu release packages](../../.github/workflows/release.yml) runs on every
push to `main` and can also be started with `workflow_dispatch`. It builds on
native `ubuntu-24.04` (x86_64) and `ubuntu-24.04-arm` (aarch64) runners using
GCC 13, producing four packages: both variants for both architectures.
Each job checks the license files and all three executables after extracting
the archive, runs kernel/io_uring SET/GET and recovery smoke checks on disposable
files, and uploads the archive and checksum as an Actions artifact for 30 days.
CI calls `scripts/package_release.sh` directly with the selected variant and
`LAVIK_PACKAGE_VERSION=nightly`; tar creation, license inclusion, executable
checks and checksums are shared with local packaging.

After all four jobs succeed on `main`, a separate job updates the
[nightly prerelease](https://github.com/eloqdata/lavik/releases/tag/nightly).
It verifies all four archives and checksums before replacing the eight assets,
moves the `nightly` tag to the built commit, and records that commit in the
release notes. Stable asset names keep download links unchanged:

```text
lavik-nightly-linux-x86_64.tar.gz
lavik-nightly-linux-x86_64-minimal.tar.gz
lavik-nightly-linux-aarch64.tar.gz
lavik-nightly-linux-aarch64-minimal.tar.gz
```

Each archive has a matching `.tar.gz.sha256` asset. A failed build does not
publish, and a completed build skips publication if `main` has already
advanced. Workflow runs are serialized so a new push cannot cancel an active
asset upload; GitHub keeps the newest pending run. Manual builds on other
branches upload Actions artifacts without updating nightly. The publication
job alone has `contents: write`; it uses the workflow's `GITHUB_TOKEN`.
No scheduled build is needed: a push to main triggers the replacement.

The Ubuntu 24.04 build environment determines the glibc compatibility floor;
these are not packages targeting older Ubuntu releases.
