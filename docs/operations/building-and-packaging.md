# Building and packaging

## Local builds

Optimized local builds use the current machine's instruction set by default:

```bash
./scripts/build_release.sh
```

This configures `KEYLANE_MARCH=native`, including Celer, mimalloc, and the
Abseil CRC translation units used by the durable storage format. The latter is
important because Abseil compiles its hardware CRC engine only when the target
exposes the required instruction macros; leaving those translation units at
the compiler baseline silently selects its generic table implementation.
OpenSSL is linked statically, so the resulting executable does not depend on
`libssl.so` or `libcrypto.so`. The build machine still needs the OpenSSL
headers and static archives (`libssl-dev` on Ubuntu, which provides `libssl.a`
and `libcrypto.a`).

For a different local CPU target, configure CMake directly with
`-DKEYLANE_MARCH=<target>`. An empty value disables the explicit `-march` flag.

Use `-DKEYLANE_CELER_SOURCE_DIR=/absolute/path/to/celer-worktree` to build and
test a separate Celer checkout without replacing the repository's submodule.
The default remains the pinned `celer/` checkout. Record both revisions when
comparing performance with an alternate runtime.

When aggressive optimization is enabled, CMake's IPO support configures both
compilation and linking for the non-Debug server and every bundled runtime
library that feeds it, including Celer and the C libraries. Test-only
executables omit IPO because their deliberately oversized coroutine stress
cases can trigger GCC compiler failures; they still link against the optimized
production libraries. Debug builds also omit IPO to keep iteration time
predictable. LTO is not required for functional correctness.

When `KEYLANE_BUILD_META=ON`, the source build also provides `keylane-meta`
and the Raft-free `keylane-ctl` operator target for direct administration
and cluster readiness:

```bash
cmake --build <build-dir> --target keylane-meta keylane-ctl
```

The downloadable release archive below continues to contain only `keylane`;
build the Meta and operator binaries from source for this release.

### Experimental DPDK networking

The pinned Celer includes an optional FreeBSD/DPDK IPv4 TCP backend for
AArch64 and x86-64. The default network backend remains Linux TCP/io_uring.
Initialize the required dependencies explicitly; SPDK uses Celer's direct DPDK
submodule, so its nested DPDK checkout is not needed:

```bash
git submodule update --init celer third_party/mimalloc third_party/nuraft
git -C third_party/nuraft submodule update --init asio
git -C celer submodule update --init third_party/liburing third_party/abseil \
  third_party/spdk third_party/dpdk
git -C celer/third_party/spdk submodule update --init isa-l isa-l-crypto
cmake -S . -B build-dpdk-net -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo -DKEYLANE_ENABLE_OPT=OFF \
  -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18 \
  -DCELER_WITH_DPDK=ON -DBUILD_TESTING=OFF
cmake --build build-dpdk-net --target keylane -j4
```

CMake invokes the BSD build helper automatically; Python 3 remains a build
dependency. See Celer's [prototype runbook](../../celer/docs/dpdk-prototype.md)
for prerequisites, TAP setup, physical-device selection, poll/adaptive mode,
and queue configuration. The default device is a virtual TAP. Ordinary data
files still use io_uring; `KEYLANE_WITH_SPDK=ON` independently enables NVMe
storage. Use a fresh disposable file and disable the metrics listener with
`--metrics-port=0` for the initial standalone SET/GET run. TLS, replication,
and cluster use are outside this prototype's validation scope.

### AddressSanitizer builds

AddressSanitizer builds use Clang so coroutine symmetric transfers remain tail
calls under sanitizer instrumentation:

```bash
./scripts/build_asan.sh
ctest --test-dir build_asan --output-on-failure
```

The script defaults to `clang-18` and `clang++-18`. Override them with
`KEYLANE_ASAN_CC`, `KEYLANE_ASAN_CXX`, and use `KEYLANE_ASAN_BUILD_DIR` to select
a different build directory. It enables the non-packageable fault-server
variant so crash-safety hooks remain available even though RelWithDebInfo may
define `NDEBUG`.

Tests place disposable data under `/tmp` by default. Set
`KEYLANE_TEST_DATA_DIR` to an existing, writable directory to relocate test
devices, logs, snapshots, and other generated artifacts for CTest, direct
test-binary, shell, and Python test runs:

```bash
KEYLANE_TEST_DATA_DIR=/path/to/test-data \
  ctest --test-dir <build-dir> --output-on-failure
```

An unset or empty value uses `/tmp`. Trailing directory separators are
accepted. An explicit work-directory argument to a Python harness takes
precedence over this environment variable. The focused failover gates inherit
the same setting; their auxiliary Unix sockets use short build-directory
paths to stay within the platform's socket-name limit.

The focused large-Hash durability suite uses its own temporary 128 MiB files
and local child servers. Run it against a Debug build or a build configured
with `KEYLANE_BUILD_FAULT_SERVER=ON` to exercise the crash injections:

```bash
cmake --build bld-clang18-debug --target keylane keylane_list_e2e_test -j 8
ctest --test-dir bld-clang18-debug -R '^keylane_large_hash_durability_e2e$' --output-on-failure
```

Substitute your configured build directory. This suite covers large Hash
command compatibility, extent durability, grouped writes and bounded-device
reclamation. The dedicated grouped-storage suites cover additional graph
publication, relocation and snapshot boundaries.
Crash cases require exit code 86 at their armed boundary; ordinary
release builds without test instrumentation skip those cases and the injected
storage-admission failure case. Bounded-device reclamation and command-level
RESP OOM cases run without fault instrumentation. The grouped side-index
memory/admission tests are part of `keylane_unit_tests`.

The grouped-storage recovery suite constructs its own temporary disk images
and starts local child servers. It exercises actual group-record recovery,
transaction decisions, GC and snapshot lifetimes without touching configured
benchmark devices:

```bash
cmake --build bld-clang18-debug --target keylane keylane_grouped_recovery_e2e_test -j 8
ctest --test-dir bld-clang18-debug -R '^keylane_grouped_recovery_e2e$' --output-on-failure
```

Finish linking the child server before running either integration suite; do
not rebuild that executable while its test fixture starts server processes.
The foreground suites use the actual command handlers and their own temporary
devices, including oversized elements, transaction failure and cold restart:

```bash
cmake --build bld-clang18-debug --target keylane keylane_grouped_hash_write_e2e_test keylane_grouped_ordered_write_e2e_test -j 8
ctest --test-dir bld-clang18-debug -R '^keylane_grouped_(hash|ordered)_write_e2e$' --output-on-failure
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
cmake --build bld-clang18-debug --target keylane keylane_compact_collection_write_e2e_test -j 8
ctest --test-dir bld-clang18-debug -R '^keylane_compact_collection_write_e2e$' --output-on-failure
```

Hash, Set, List and Sorted Set automatically promote to grouped storage at
16 KiB of encoded collection data in both ordinary and Debug builds. Promotion
does not need an environment switch. Debug/fault builds additionally provide
the crash and admission hooks exercised by the integration tests.
The current adapter and integration limits are documented in
[Grouped collections](../architecture/09-grouped-collections.md).

### Adding deterministic fault sites

Use `include/keylane/fault_injection.h` for internal crash, allocation-failure
and scheduling hooks. Its single build policy enables hooks in Debug or with
`KEYLANE_BUILD_FAULT_SERVER=ON`; ordinary Release builds erase the hook bodies
and their arguments, including environment lookups and injected suspension
points. Set fault environment variables before launching the server, not
concurrently with its workers.

```cpp
KEYLANE_FAULT_BAD_ALLOC("KEYLANE_FAIL_GROUP_HANDOFF_KEY", key);
KEYLANE_MAYBE_CRASH_AT("group-batch-before-root");
KEYLANE_FAULT_INJECT(
    if (KEYLANE_FAULT_MATCHES("KEYLANE_TEST_PAUSE_KEY", key)) {
      // Keep the existing coroutine, lock ownership and error handling.
      auto status = co_await celer::SleepFor(worker, delay);
      if (!status.ok()) co_return status;
    });
```

Use `KEYLANE_FAULT_MATCHES_NTH` for an exact key plus a one-based position
within the current operation. It does not introduce a shared hit counter.
Keep fault effects inside the existing rollback/commit boundary.
`KEYLANE_FAULT_INJECT` introduces a block, not a coroutine or lambda;
cross-scope diagnostic declarations and outer-loop `break`/`continue` need
the central `#if KEYLANE_FAULTS_ENABLED` guard instead. The crash selector
`KEYLANE_CRASH_POINT` is cached on first use and terminates with exit code 86
without flushing or unwinding.

The `keylane_fault_injection_*` CTest cases independently compile the helper
in Debug, ordinary Release and fault-enabled Release modes. Integration
fixtures that require a hook must skip against ordinary Release servers.

### TTL index memory reclamation

The Tomb Raider suite checks that TTL expiration and tombstone reaping release
retained index memory. It writes 102,400 inline keys of 1 KiB each (100 MiB of
key bytes), with one-byte values:

```bash
cmake --build build-clang --target keylane keylane_tomb_raider_e2e_test -j 8
ctest --test-dir build-clang -R '^keylane_tomb_raider_e2e$' --output-on-failure
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
cmake --build bld-clang18-debug --target keylane_replica_abort_reclaim_e2e_test keylane_grouped_ordered_write_e2e_test keylane -j 8
bld-clang18-debug/keylane_replica_abort_reclaim_e2e_test --large-list
bld-clang18-debug/keylane_replica_abort_reclaim_e2e_test --large-hash
KEYLANE_RUN_LARGE_RDB=1 bld-clang18-debug/keylane_grouped_ordered_write_e2e_test \
  bld-clang18-debug/keylane \
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
  -DKEYLANE_ENABLE_OPT=OFF -DKEYLANE_STATIC_OPENSSL=ON \
  -DBUILD_TESTING=ON -DKEYLANE_BUILD_FAULT_SERVER=ON
./scripts/run_cluster_fault_tests.sh --tier model
./scripts/run_cluster_fault_tests.sh --tier integration
./scripts/run_cluster_fault_tests.sh --tier soak --duration 600
```

See [`tests/cluster/README.md`](../../tests/cluster/README.md) for the
determinism boundary, invariant matrix, trace/replay commands, and hardware
allowlist rules. The CTest labels are `cluster-model`,
`cluster-integration`, `cluster-soak`, and `cluster-hardware`.

## Source formatting

Keylane and its Celer submodule use the Google style, parse source as C++23,
and pin clang-format 23.1.0. Install `pre-commit` once and enable the repository
hook:

```bash
sudo apt-get install pre-commit
pre-commit install
```

The first run creates an isolated hook environment and downloads the pinned
formatter; clang-format is not a Keylane runtime or build dependency. Commits
then format staged first-party C and C++ files. When formatting changes a file,
the commit stops so the result can be reviewed and staged before retrying. To
format every maintained source file explicitly, run:

```bash
pre-commit run clang-format --all-files
```

The CMake `format` and `format-check` targets use a system installation only
when it reports exactly version 23.1.0. This exact check prevents a local tool
upgrade from silently rewriting unrelated code. The pre-commit hook is the
portable path when that system binary is unavailable.

## Downloadable release package

```bash
./scripts/package_release.sh
```

The packaging script performs a Release build, statically links OpenSSL plus
the GNU C++/compiler runtimes, strips a staged copy of the executable, verifies
that no dynamic OpenSSL or C++ runtime dependency remains, and writes a
versioned archive and SHA-256 checksum under `dist/`. The archive also carries
the Apache-2.0 license text required by the statically linked OpenSSL code.
It explicitly configures `KEYLANE_BUILD_FAULT_SERVER=OFF`; CMake also rejects
that option whenever `BUILD_TESTING` is off.

Unlike a local build, a package uses a portable CPU baseline:

- `x86_64`: `-march=x86-64-v2`
- `aarch64`: `-march=armv8-a`

Override it with `KEYLANE_PACKAGE_MARCH` when producing a package for a more
specific fleet. Other useful overrides are `KEYLANE_PACKAGE_BUILD_DIR`,
`KEYLANE_PACKAGE_OUTPUT_DIR`, and `KEYLANE_PACKAGE_JOBS`.

The release remains a normal Linux ELF executable and therefore uses the
platform C library. Build official artifacts in the oldest supported Linux
environment so their glibc requirement remains compatible with newer systems.
