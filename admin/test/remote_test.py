#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
"""Exercise remote ownership and restart contracts without requiring SSH."""

import importlib.util
import json
from pathlib import Path
import subprocess
import shutil
import sys
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


class TeardownSafety(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name).resolve()
        self.request = {
            "baseDir": str(self.base),
            "cluster": "test",
            "owner": "original-owner",
            "supervisor": "process",
            "nodes": [{"name": "data-1", "kind": "data"}],
            "phase": "delete",
        }
        self.root = remote.owned(self.request, create=True)
        (self.root / "data-1").mkdir()
        (self.root / "data-1/lavik.data").write_bytes(b"database")
        self.mounts = patch.object(Path, "read_text", autospec=True)
        self.real_read = Path.read_text
        mock = self.mounts.start()
        mock.side_effect = (
            lambda path, *a, **kw: ""
            if str(path) == "/proc/self/mountinfo"
            else self.real_read(path, *a, **kw)
        )
        self.proc = patch.object(remote, "check_no_processes")
        self.proc.start()

    def tearDown(self):
        patch.stopall()
        self.temporary.cleanup()

    def test_wrong_owner_and_symlinks_never_delete_files(self):
        with self.assertRaisesRegex(RuntimeError, "unowned"):
            remote.teardown({**self.request, "owner": "someone-else"})
        (self.root / "escape").symlink_to(self.base)
        with self.assertRaisesRegex(RuntimeError, "symlink"):
            remote.teardown(self.request)
        self.assertEqual((self.root / "data-1/lavik.data").read_bytes(), b"database")

    def test_raw_spdk_media_is_not_implicitly_erased(self):
        (self.root / "spdk-request.json").write_text("{}")
        with self.assertRaisesRegex(RuntimeError, "SPDK"):
            remote.teardown(self.request)
        self.assertTrue((self.root / "data-1/lavik.data").exists())

    def test_check_and_stop_retain_data_delete_is_retryable_and_scoped(self):
        sibling = self.base / "other"
        sibling.mkdir()
        (sibling / "keep").write_text("other cluster")
        for phase in ("check", "stop"):
            remote.teardown({**self.request, "phase": phase})
            self.assertTrue((self.root / "data-1/lavik.data").exists())
        remote.teardown(self.request)
        self.assertFalse(self.root.exists())
        remote.teardown(self.request)
        self.assertEqual((sibling / "keep").read_text(), "other cluster")
        # A replacement deployment cannot be deleted with the previous nonce.
        remote.owned({**self.request, "owner": "replacement"}, create=True)
        with self.assertRaisesRegex(RuntimeError, "unowned"):
            remote.teardown(self.request)

    def test_resume_after_final_marker_unlink_uses_retained_receipt(self):
        real_rmdir = Path.rmdir

        def interrupted(path):
            if path == self.root:
                raise OSError("interrupted before final directory removal")
            return real_rmdir(path)

        with patch.object(Path, "rmdir", autospec=True, side_effect=interrupted):
            with self.assertRaisesRegex(OSError, "interrupted"):
                remote.teardown(self.request)
        self.assertTrue(self.root.exists())
        self.assertEqual(list(self.root.iterdir()), [])
        remote.teardown(self.request)
        self.assertFalse(self.root.exists())

    def test_running_process_or_foreign_service_blocks_deletion(self):
        with patch.object(
            remote, "check_no_processes", side_effect=RuntimeError("still running")
        ):
            with self.assertRaisesRegex(RuntimeError, "still running"):
                remote.teardown(self.request)
        units = self.base / ".config/systemd/user"
        units.mkdir(parents=True)
        unit = units / "lavik-test-data-1.service"
        unit.write_text("ExecStart=/bin/sleep infinity\n")
        with patch.object(Path, "home", return_value=self.base):
            with self.assertRaisesRegex(RuntimeError, "foreign service"):
                remote.teardown({**self.request, "supervisor": "systemd"})
        self.assertTrue((self.root / "data-1/lavik.data").exists())

    def test_systemd_stops_and_disables_exact_owned_unit_before_removing_it(self):
        units = self.base / ".config/systemd/user"
        units.mkdir(parents=True)
        unit = units / "lavik-test-data-1.service"
        unit.write_text(
            "ExecStart=/usr/bin/python3 " + str(self.root / "data-1/launch.py") + "\n"
        )

        def output(argv, *args):
            if "--property=LoadState,ActiveState,FragmentPath" in argv:
                return {
                    "code": 0,
                    "stdout": f"LoadState=loaded\nActiveState=active\nFragmentPath={unit}",
                    "stderr": "",
                }
            return {"code": 0, "stdout": "inactive", "stderr": ""}

        with (
            patch.object(Path, "home", return_value=self.base),
            patch.object(remote, "run", side_effect=output) as runner,
        ):
            remote.teardown({**self.request, "supervisor": "systemd"})
            self.assertIn(
                ["systemctl", "--user", "disable", "--now", unit.name],
                [c.args[0] for c in runner.call_args_list],
            )
        self.assertFalse(unit.exists())
        self.assertFalse(self.root.exists())

    def test_monitoring_project_collision_never_stops_containers(self):
        monitor = self.root / "monitoring"
        monitor.mkdir()
        (monitor / "compose.yaml").write_text("services: {}")

        def checked(argv, *args):
            if argv[1] == "ps":
                return "foreign-container"
            return json.dumps(
                [
                    {
                        "Config": {
                            "Labels": {
                                "com.docker.compose.project.working_dir": "/different"
                            }
                        }
                    }
                ]
            )

        with patch.object(remote, "checked", side_effect=checked) as runner:
            with self.assertRaisesRegex(RuntimeError, "another directory"):
                remote.teardown({**self.request, "monitoring": True})
            self.assertTrue(
                all(c.args[0][1] in ("ps", "inspect") for c in runner.call_args_list)
            )
        self.assertTrue(self.root.exists())


class ProcessScanSafety(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / "deployment"
        self.proc = Path(temporary.name) / "proc"
        self.process = self.proc / "123"
        self.process.mkdir(parents=True)
        (self.process / "exe").symlink_to("/usr/bin/sleep")
        (self.process / "cmdline").write_bytes(b"sleep\0infinity\0")
        # Model the process inventory without depending on host UIDs, ptrace
        # policy, or runner agents. All reads still exercise the real scanner.
        self.enterContext(
            patch.object(
                remote,
                "Path",
                side_effect=lambda path: self.proc if path == "/proc" else Path(path),
            )
        )

    def test_unrelated_process_is_allowed(self):
        remote.check_no_processes(self.root)

    def test_owned_executable_blocks_even_with_unrelated_argv(self):
        for name in ("lavik", "lavik-meta", "lavik (deleted)"):
            with self.subTest(name=name):
                (self.process / "exe").unlink()
                (self.process / "exe").symlink_to(self.root / "release" / name)
                with self.assertRaisesRegex(
                    RuntimeError, "still running; data retained"
                ):
                    remote.check_no_processes(self.root)

    def test_launcher_blocks_before_exec(self):
        (self.process / "cmdline").write_bytes(
            b"python3\0" + str(self.root / "data-1/launch.py").encode() + b"\0"
        )
        with self.assertRaisesRegex(RuntimeError, "still running; data retained"):
            remote.check_no_processes(self.root)

    def test_uninspectable_same_uid_process_fails_closed(self):
        for method, target in (("readlink", remote.os), ("read_bytes", Path)):
            with (
                self.subTest(method=method),
                patch.object(target, method, side_effect=PermissionError),
                self.assertRaisesRegex(
                    RuntimeError, "Cannot inspect owned process 123"
                ),
            ):
                remote.check_no_processes(self.root)

    def test_uninspectable_other_uid_process_is_allowed(self):
        with (
            patch.object(remote.os, "readlink", side_effect=PermissionError),
            patch.object(
                remote.os, "geteuid", return_value=self.process.stat().st_uid + 1
            ),
        ):
            remote.check_no_processes(self.root)

    def test_exited_process_is_allowed(self):
        for error in (FileNotFoundError, ProcessLookupError):
            with (
                self.subTest(error=error),
                patch.object(remote.os, "readlink", side_effect=error),
            ):
                remote.check_no_processes(self.root)


@unittest.skipUnless(sys.platform == "linux", "process teardown requires Linux pidfds")
class LinuxProcessTeardown(unittest.TestCase):
    def test_real_process_stops_before_its_owned_files_are_removed(self):
        with tempfile.TemporaryDirectory() as temporary:
            request = {
                "baseDir": temporary,
                "cluster": "test",
                "owner": "process-owner",
                "supervisor": "process",
                "nodes": [{"name": "data-1", "kind": "data", "args": ["300"]}],
            }
            root = remote.owned(request, create=True)
            (root / "release").mkdir()
            shutil.copyfile("/bin/sleep", root / "release/lavik")
            (root / "release/lavik").chmod(0o700)
            (root / "data-1").mkdir()
            (root / "data-1/lavik.data").write_bytes(b"retained data")
            identity = remote.start_node(
                {**request, "node": request["nodes"][0], "manifest": "test"}
            )
            # Exercise real procfs reads and pidfd signals for our child only.
            # A host process with our UID may deny /proc/PID/exe access; that
            # fail-closed policy is covered separately by ProcessScanSafety.
            real_iterdir = Path.iterdir
            self.enterContext(
                patch.object(
                    Path,
                    "iterdir",
                    autospec=True,
                    side_effect=lambda path: iter([Path(f"/proc/{identity['pid']}")])
                    if path == Path("/proc")
                    else real_iterdir(path),
                )
            )
            try:
                remote.teardown({**request, "phase": "check"})
                self.assertTrue(Path(f"/proc/{identity['pid']}").exists())
                with self.assertRaisesRegex(RuntimeError, "still running"):
                    remote.check_no_processes(root)
                # PID start-time mismatch does not stop the actual process and
                # the final live-process check must refuse to delete its data.
                (root / "data-1/process.json").write_text(
                    json.dumps({**identity, "start": "wrong"})
                )
                with self.assertRaisesRegex(RuntimeError, "still running"):
                    remote.teardown({**request, "phase": "delete"})
                self.assertTrue((root / "data-1/lavik.data").exists())
                (root / "data-1/process.json").write_text(json.dumps(identity))
                remote.teardown({**request, "phase": "stop"})
                self.assertTrue(root.exists())
                remote.teardown({**request, "phase": "delete"})
                self.assertFalse(root.exists())
            finally:
                # Reap the stopped child without touching any unrelated process.
                import os

                try:
                    os.kill(identity["pid"], 15)
                    os.waitpid(identity["pid"], 0)
                except (ProcessLookupError, ChildProcessError):
                    pass


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
