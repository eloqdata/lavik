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

# Contributing to Lavik

This guide covers building, testing, formatting, and committing a change.
Run the commands from the repository root after cloning. Before changing a
subsystem, read the [documentation index](docs/README.md) and its relevant
[architecture document](docs/architecture/README.md).

## Set up and build

The commands below target Ubuntu 24.04 on x86_64 or ARM64. Running Lavik and
its runtime tests requires Linux 6.1 or newer with io_uring enabled. See
[Building and packaging](docs/operations/building-and-packaging.md) for full
requirements, other build options, sanitizers, and DPDK/SPDK setup.

```bash
git clone https://github.com/eloqdata/lavik.git
cd lavik
./scripts/install_build_deps.sh
git submodule update --init bycorf third_party/mimalloc
git -C bycorf submodule update --init third_party/liburing third_party/abseil

./scripts/configure_debug.sh
cmake --build build_debug --parallel
```

The dependency installer uses sudo for apt when needed; pass `--dry-run` to
preview its commands. The first configuration/build downloads the Go toolchain
and modules pinned by `raft/go.mod` for the always-built Meta targets, so
network access is needed unless the caches are already populated.

The Debug build enables tests and produces `build_debug/lavik`,
`build_debug/lavik-meta`, and `build_debug/lavik-ctl`. After editing, rebuild
only the relevant targets, for example
`cmake --build build_debug --target lavik lavik_unit_tests --parallel`.
For an optimized local build, run `./scripts/configure_release.sh` and then
`cmake --build build --parallel`; its output is under `build/` and targets the
build machine's CPU by default.

Project options belong to the configure command. Both configure scripts
forward additional CMake arguments, for example
`./scripts/configure_debug.sh -DLAVIK_KERNEL_BYPASS=ON`; subsequent builds use
the cached configuration through `cmake --build`. The scripts explicitly reset
kernel bypass to `OFF` unless it is overridden on that invocation.

## Run tests

```bash
ctest --test-dir build_debug --output-on-failure
```

For a focused change, select the relevant tests with
`ctest --test-dir build_debug -R '<test-name-pattern>' --output-on-failure`.
Some integration tests need additional tools installed before CMake
configuration. See the [continuous integration guide](docs/operations/building-and-packaging.md#continuous-integration)
for prerequisites and the complete CI runner.

For the vendored Valkey compatibility suites:

```bash
LAVIK_BIN="$PWD/build_debug/lavik" tests/valkey/run-lavik
```

See the [Valkey test guide](tests/valkey/README.md) for dependencies and
suite selection. In your pull request, state which checks you ran and any
checks you could not run.

## Set up formatting once

```bash
sudo apt-get install pre-commit
pre-commit install
```

The first hook run downloads an isolated formatter environment. The
[hook configuration](.pre-commit-config.yaml) pins clang-format 23.1.1;
there is no need to install clang-format separately. Lavik uses Google style
with C++23 parsing, as configured in [.clang-format](.clang-format).

The hook formats first-party C/C++ files under `app/`, `include/lavik/`,
`src/`, and `tests/`, excluding the vendored `CLI11.hpp`. Bycorf maintains
its own formatting configuration in its submodule.

To format all maintained source files immediately:

```bash
pre-commit run clang-format --all-files
```

To format only selected files, use
`pre-commit run clang-format --files src/config.cpp` with your file paths.
Review the changes before staging them.

## Commit a change

Create a branch before editing:

```bash
git switch -c my-change
```

After building and running the relevant tests, review and stage the files
that belong to your change. Replace `<files>` with their paths:

```bash
git status --short
git diff
git add <files>
pre-commit run
```

`pre-commit run` checks staged files. If the formatter modifies a file, the
hook reports failure so you can review its edits and stage them again:

```bash
git diff
git add <files>
pre-commit run
```

Once the checks pass, review the staged patch and commit:

```bash
git diff --cached
git commit -m "Describe the change"
```

With the hook installed, `git commit` also runs pre-commit automatically.
If it modifies files and stops the commit, follow the same review, stage,
and retry steps above. Use a concise commit title explaining the change;
include non-obvious motivation in the commit body when needed.

Push your branch to a repository you can write to and open a pull request
against `eloqdata/lavik`'s `main` branch. Describe the problem, resulting
behavior, and validation. Keep relevant documentation synchronized with the
change; the [architecture authoring standard](docs/architecture/README.md#authoring-standard)
explains when an architecture update is needed.
