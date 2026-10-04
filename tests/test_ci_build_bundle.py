#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

"""Round-trip the runtime archive and reject incomplete or stale CTest builds."""

import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location(
    "ci_build_bundle", ROOT / "scripts/ci_build_bundle.py"
)
bundle = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bundle)


class CiBuildBundleTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.build = self.root / "build_ci"
        self.build.mkdir()
        self.archive = self.root / "bundle.tar.zst"
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(self.root),
                "-c",
                "user.name=CI Test",
                "-c",
                "user.email=ci@example.invalid",
                "commit",
                "-qm",
                "fixture",
                "--allow-empty",
            ],
            check=True,
        )
        self.write("smoke", "#!/bin/sh\nexit 0\n").chmod(0o755)
        self.write(
            "CTestTestfile.cmake",
            f'include("{self.build}/smoke[1]_include.cmake")\nsubdirs("bycorf")\n',
        )
        self.write(
            "smoke[1]_include.cmake",
            f'include("{self.build}/smoke[1]_tests.cmake")\n',
        )
        self.write("smoke[1]_tests.cmake", f'add_test(smoke "{self.build}/smoke")\n')
        self.write("bycorf/CTestTestfile.cmake", "# no extra tests\n")

    def write(self, name, contents):
        path = self.build / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)
        return path

    def create(self):
        bundle.create_bundle(self.build, self.archive, self.root)

    def validate(self):
        bundle.validate_bundle(self.build, self.root)

    def test_archive_round_trip_preserves_ctest_and_runtime_files(self):
        excluded = (
            "CMakeFiles/target.dir/test.cpp.o",
            "lib/libtest.a",
            "bycorf/CMakeFiles/runtime.dir/flags.make",
            "CMakeCache.txt",
            "build.ninja",
            ".ninja_log",
            "compile_commands.json",
            "_deps/googletest-src/CMakeLists.txt",
            "_deps/googletest-subbuild/build.ninja",
            "Testing/Temporary/LastTest.log",
            "test-results/ctest.xml",
        )
        retained = (
            "_deps/googletest-build/CTestTestfile.cmake",
            "lavik_large_codec_properties.cmake",
            "liblavik_slow_dns_shim.so",
            "test_tools/redis_py/redis/__init__.py",
            "test_tools/redis/redis-server",
            "test_tools/redis-shake/redis-shake",
            "cluster-client-go/cluster-client-go",
            "import-client-go",
            "sentinel-client-go",
        )
        for name in (*excluded, *retained):
            self.write(name, "fixture\n")
        (self.build / "fs").mkdir()
        (self.build / "smoke-link").symlink_to("smoke")
        inventory = bundle.test_inventory(self.build)
        self.create()
        shutil.rmtree(self.build)
        subprocess.run(
            ["tar", "--zstd", "-xf", str(self.archive), "-C", str(self.root)],
            check=True,
        )
        self.assertEqual((self.build / "smoke").stat().st_mode & 0o777, 0o755)
        self.assertTrue((self.build / "fs").is_dir())
        self.assertTrue((self.build / "smoke-link").is_symlink())
        for name in retained:
            self.assertTrue((self.build / name).exists(), name)
        for name in excluded:
            self.assertFalse((self.build / name).exists(), name)
        self.validate()
        self.assertEqual(bundle.test_inventory(self.build), inventory)
        subprocess.run(
            ["ctest", "--test-dir", str(self.build), "--output-on-failure"], check=True
        )

    def test_identity_mismatches_are_rejected_before_running_ctest(self):
        self.create()
        path = self.build / bundle.MANIFEST
        original = json.loads(path.read_text())
        for key in ("source_dir", "build_dir", "revision", "platform", "architecture"):
            with self.subTest(key=key):
                altered = dict(original)
                altered[key] = "wrong"
                path.write_text(json.dumps(altered))
                with self.assertRaisesRegex(ValueError, f"Bundle {key} mismatch"):
                    self.validate()

    def test_dropped_or_changed_test_is_rejected(self):
        self.create()
        self.write(
            "smoke[1]_tests.cmake", f'add_test(replacement "{self.build}/smoke")\n'
        )
        with self.assertRaisesRegex(ValueError, "inventory differs"):
            self.validate()

    def test_missing_executable_and_discovery_are_rejected(self):
        self.create()
        (self.build / "smoke").unlink()
        with self.assertRaisesRegex(ValueError, "executable or discovery file missing"):
            self.validate()
        self.write("smoke[1]_tests.cmake", 'add_test(smoke_NOT_BUILT "true")\n')
        with self.assertRaisesRegex(ValueError, "executable or discovery file missing"):
            self.validate()

    def test_archive_cannot_be_written_inside_build(self):
        with self.assertRaisesRegex(ValueError, "outside the build directory"):
            bundle.create_bundle(self.build, self.build / "bundle.tar.zst", self.root)


if __name__ == "__main__":
    unittest.main()
