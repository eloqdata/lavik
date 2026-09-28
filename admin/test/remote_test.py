#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
"""Exercise remote ownership and restart contracts without requiring SSH."""

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "remote", Path(__file__).parents[1] / "remote.py"
)
remote = importlib.util.module_from_spec(spec)
spec.loader.exec_module(remote)


class RemoteSafety(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name).resolve()
        self.request = {
            "baseDir": str(self.base),
            "cluster": "test",
            "owner": "retained-owner",
        }

    def tearDown(self):
        self.temporary.cleanup()

    def test_existing_data_is_never_claimed_or_reconfigured(self):
        root = self.base / "test"
        root.mkdir()
        data = root / "important.data"
        data.write_bytes(b"existing database")
        with self.assertRaisesRegex(RuntimeError, "unowned"):
            remote.owned(self.request, create=True)
        self.assertEqual(data.read_bytes(), b"existing database")
        config = root / "configuration"
        remote.write_once(config, "original")
        remote.write_once(config, "original")
        with self.assertRaisesRegex(RuntimeError, "differs"):
            remote.write_once(config, "replacement")
        self.assertEqual(config.read_text(), "original")

    def test_symlink_does_not_redirect_provisioning(self):
        (self.base / "test").symlink_to(self.base)
        with self.assertRaisesRegex(RuntimeError, "symlink"):
            remote.owned(self.request, create=True)

    def test_bad_checksum_never_installs_executables(self):
        root = remote.owned(self.request, create=True)
        digest = "a" * 64
        (root / (".incoming-" + digest)).write_bytes(b"not the reviewed release")
        with self.assertRaisesRegex(RuntimeError, "checksum"):
            remote.prepare(
                {
                    **self.request,
                    "asset": {
                        "sha256": digest,
                        "url": "https://github.com/eloqdata/lavik/releases/download/nightly/test.tar.gz",
                    },
                }
            )
        self.assertFalse((root / "release").exists())

    def test_non_systemd_hosts_get_lab_guidance_without_lingering_commands(self):
        for tools in (None, "/usr/bin/systemctl"):
            with (
                self.subTest(tools=tools),
                patch.object(remote.shutil, "which", return_value=tools),
                patch.object(Path, "is_dir", return_value=False),
                patch.object(remote, "run") as runner,
            ):
                errors = remote.supervision_errors("systemd")
                self.assertEqual(len(errors), 1)
                self.assertIn("Development processes", errors[0])
                self.assertNotIn("sudo", errors[0])
                runner.assert_not_called()

    def test_real_systemd_hosts_still_require_user_manager_and_lingering(self):
        with (
            patch.object(remote.shutil, "which", return_value="/usr/bin/tool"),
            patch.object(Path, "is_dir", return_value=True),
            patch.object(remote, "platform_user", return_value="lavik"),
            patch.object(remote, "run") as runner,
        ):
            runner.side_effect = [{"code": 1}, {"stdout": "Linger=no"}]
            errors = remote.supervision_errors("systemd")
            self.assertEqual(len(errors), 2)
            self.assertIn("systemd user manager", errors[0])
            self.assertIn("sudo loginctl enable-linger lavik", errors[1])
            runner.side_effect = [{"code": 0}, {"stdout": "Linger=yes"}]
            self.assertEqual(remote.supervision_errors("systemd"), [])

    def test_process_mode_does_not_require_systemd(self):
        with patch.object(remote, "run") as runner:
            self.assertEqual(remote.supervision_errors("process"), [])
            runner.assert_not_called()

    def test_systemd_launcher_bootstraps_only_pristine_meta_state(self):
        root = remote.owned(self.request, create=True)
        (root / "release").mkdir()
        fake_meta = root / "release/lavik-meta"
        fake_meta.write_text(
            "#!/usr/bin/env python3\nimport json,sys\nfrom pathlib import Path\np=Path(sys.argv[sys.argv.index('--data-dir')+1]);p.mkdir(exist_ok=True)\nif '--initial-cluster-manifest' in sys.argv and (p/'RAFT').exists(): sys.exit(9)\n(p/'RAFT').write_text('owned')\nprint(json.dumps(sys.argv[1:]))\n"
        )
        fake_meta.chmod(0o755)
        node = {
            "name": "meta-1",
            "kind": "meta",
            "args": ["--data-dir", "@ROOT@/meta-1/state"],
        }
        with (
            patch.object(Path, "home", return_value=self.base),
            patch.object(remote, "run", return_value={"code": 0}) as runner,
        ):
            remote.start_node(
                {
                    **self.request,
                    "node": node,
                    "manifest": "schema_version=1\n",
                    "supervisor": "systemd",
                }
            )
            self.assertEqual(
                runner.call_args_list[-1].args[0],
                ["systemctl", "--user", "enable", "--now", "lavik-test-meta-1.service"],
            )
        launcher = root / "meta-1/launch.py"
        first = json.loads(subprocess.check_output(["python3", str(launcher)]))
        second = json.loads(subprocess.check_output(["python3", str(launcher)]))
        self.assertIn("--initial-cluster-manifest", first)
        self.assertNotIn("--initial-cluster-manifest", second)
        self.assertFalse((root / "meta-1/state/launch.py").exists())


if __name__ == "__main__":
    unittest.main()
