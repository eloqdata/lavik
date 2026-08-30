# Building and packaging

## Local builds

Optimized local builds use the current machine's instruction set by default:

```bash
./scripts/build_release.sh
```

This configures `KEYLANE_MARCH=native`, including Celer and mimalloc. OpenSSL
is linked statically, so the resulting executable does not depend on
`libssl.so` or `libcrypto.so`. The build machine still needs the OpenSSL
headers and static archives (`libssl-dev` on Ubuntu, which provides `libssl.a`
and `libcrypto.a`).

For a different local CPU target, configure CMake directly with
`-DKEYLANE_MARCH=<target>`. An empty value disables the explicit `-march` flag.
When aggressive optimization is enabled, CMake's IPO support configures both
compilation and linking for every non-Debug Keylane target. Optimized Clang
builds can therefore link the server and test executables without setting
`CMAKE_EXE_LINKER_FLAGS` manually. Debug builds omit IPO to keep iteration
time predictable; LTO is not required for functional correctness.

AddressSanitizer builds use Clang so coroutine symmetric transfers remain tail
calls under sanitizer instrumentation:

```bash
./scripts/build_asan.sh
ctest --test-dir build_asan --output-on-failure
```

The script defaults to `clang-18` and `clang++-18`. Override them with
`KEYLANE_ASAN_CC`, `KEYLANE_ASAN_CXX`, and use `KEYLANE_ASAN_BUILD_DIR` to select
a different build directory. It intentionally keeps `NDEBUG` disabled because
the crash-safety fault-injection tests compile their crash points out otherwise.

## Downloadable release package

```bash
./scripts/package_release.sh
```

The packaging script performs a Release build, statically links OpenSSL plus
the GNU C++/compiler runtimes, strips a staged copy of the executable, verifies
that no dynamic OpenSSL or C++ runtime dependency remains, and writes a
versioned archive and SHA-256 checksum under `dist/`. The archive also carries
the Apache-2.0 license text required by the statically linked OpenSSL code.

Unlike a local build, a package uses a portable CPU baseline:

- `x86_64`: `-march=x86-64-v2`
- `aarch64`: `-march=armv8-a`

Override it with `KEYLANE_PACKAGE_MARCH` when producing a package for a more
specific fleet. Other useful overrides are `KEYLANE_PACKAGE_BUILD_DIR`,
`KEYLANE_PACKAGE_OUTPUT_DIR`, and `KEYLANE_PACKAGE_JOBS`.

The release remains a normal Linux ELF executable and therefore uses the
platform C library. Build official artifacts in the oldest supported Linux
environment so their glibc requirement remains compatible with newer systems.
