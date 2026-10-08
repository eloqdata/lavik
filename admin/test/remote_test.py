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


class SpdkSafety(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.request = {
            "owner": "deployment",
            "spdk": "spdk://0000:01:00.0/1",
            "hugepageMiB": 8192,
        }
        self.bdf = "0000:01:00.0"
        self.device = self.root / "sys/bus/pci/devices" / self.bdf
        self.device.mkdir(parents=True)
        (self.device / "class").write_text("0x010802")
        (self.device / "iommu_group/devices" / self.bdf).mkdir(parents=True)
        (self.device / "nvme/nvme0/nvme0n1").mkdir(parents=True)
        (self.device / "nvme/nvme0/serial").write_text("SERIAL-001")
        (self.device / "nvme/nvme0/nvme0n1/nsid").write_text("1")
        (self.root / "sys/class/block/nvme0n1/holders").mkdir(parents=True)
        (self.root / "sys/bus/pci/drivers/nvme").mkdir(parents=True)
        (self.device / "driver").symlink_to(self.root / "sys/bus/pci/drivers/nvme")
        huge = self.root / "sys/kernel/mm/hugepages/hugepages-2048kB"
        huge.mkdir(parents=True)
        (huge / "nr_hugepages").write_text("0")
        (huge / "free_hugepages").write_text("0")
        (self.root / "proc").mkdir()
        (self.root / "proc/meminfo").write_text("MemAvailable: 16777216 kB\n")
        self.block = {
            "name": "nvme0n1",
            "type": "disk",
            "fstype": None,
            "mountpoints": [None],
        }
        self.signatures = []
        self.users = {"code": 1, "stdout": "", "stderr": ""}
        self.path_patch = patch.object(
            remote, "Path", side_effect=lambda path: self.root / str(path).lstrip("/")
        )
        self.path_patch.start()
        self.uid_patch = patch.object(remote.os, "geteuid", return_value=0)
        self.uid_patch.start()
        self.tools_patch = patch.object(
            remote.shutil, "which", return_value="/usr/bin/tool"
        )
        self.tools_patch.start()
        self.runner_patch = patch.object(remote, "run", side_effect=self.run_command)
        self.runner = self.runner_patch.start()
        self.media_patch = patch(
            "builtins.open", unittest.mock.mock_open(read_data=b"\0" * 4096)
        )
        self.media_patch.start()

    def tearDown(self):
        patch.stopall()
        self.temporary.cleanup()

    def run_command(self, argv, *args):
        if argv[0] == "fuser":
            return self.users
        payload = (
            {"blockdevices": [self.block]}
            if argv[0] == "lsblk"
            else {"signatures": self.signatures}
        )
        return {"code": 0, "stdout": json.dumps(payload), "stderr": ""}

    def test_preview_records_identity_without_mutating_device(self):
        result = remote.spdk_check(self.request)
        self.assertEqual(result["serial"], "SERIAL-001")
        self.assertEqual((self.device / "driver").resolve().name, "nvme")
        self.assertFalse((self.root / "var").exists())
        self.assertTrue(
            all(
                call.args[0][0] in ("lsblk", "wipefs", "fuser")
                or call.args[0] == ["modprobe", "--dry-run", "vfio-pci"]
                for call in self.runner.call_args_list
            )
        )

    def test_rejects_mounts_partitions_signatures_open_users_and_serial_changes(self):
        for field, value in (
            ("mountpoints", ["/"]),
            ("children", [{}]),
            ("fstype", "swap"),
        ):
            with self.subTest(field=field):
                original = self.block.copy()
                self.block[field] = value
                with self.assertRaisesRegex(RuntimeError, "unsuitable"):
                    remote.spdk_check(self.request)
                self.block = original
        self.signatures = [{"type": "gpt"}]
        with self.assertRaisesRegex(RuntimeError, "signatures"):
            remote.spdk_check(self.request)
        self.signatures = []
        self.users = {"code": 0, "stdout": "123", "stderr": ""}
        with self.assertRaisesRegex(RuntimeError, "open"):
            remote.spdk_check(self.request)
        self.users = {"code": 1, "stdout": "", "stderr": ""}
        with self.assertRaisesRegex(RuntimeError, "serial changed"):
            remote.spdk_check({**self.request, "expectedSerial": "OTHER"})
        with patch(
            "builtins.open", unittest.mock.mock_open(read_data=b"existing database")
        ):
            with self.assertRaisesRegex(RuntimeError, "nonempty"):
                remote.spdk_check(self.request)

    def test_unsupported_iommu_privilege_memory_and_foreign_claim_fail_closed(self):
        with patch.object(remote.os, "geteuid", return_value=1000):
            with self.assertRaisesRegex(RuntimeError, "root SSH"):
                remote.spdk_check(self.request)
        sibling = self.device / "iommu_group/devices/0000:02:00.0"
        sibling.mkdir()
        with self.assertRaisesRegex(RuntimeError, "isolated IOMMU"):
            remote.spdk_check(self.request)
        sibling.rmdir()
        (self.root / "proc/meminfo").write_text("MemAvailable: 1000 kB\n")
        with self.assertRaisesRegex(RuntimeError, "Insufficient memory"):
            remote.spdk_check(self.request)
        claims = self.root / "var/lib/lavik-admin/spdk"
        claims.mkdir(parents=True)
        (claims / "host.json").write_text(
            json.dumps({"owner": "someone-else", "spdk": self.request["spdk"]})
        )
        with self.assertRaisesRegex(RuntimeError, "already claimed"):
            remote.spdk_check(self.request)
        self.assertEqual((self.device / "driver").resolve().name, "nvme")


if __name__ == "__main__":
    unittest.main()
