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
When aggressive optimization is enabled, CMake's IPO support configures both
compilation and linking for the non-Debug server and every bundled runtime
library that feeds it, including Celer and the C libraries. Test-only
executables omit IPO because their deliberately oversized coroutine stress
cases can trigger GCC compiler failures; they still link against the optimized
production libraries. Debug builds also omit IPO to keep iteration time
predictable. LTO is not required for functional correctness.

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
