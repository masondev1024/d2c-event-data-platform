"""Offline evidence and ownership checks; no Docker or installer is executed."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from scripts import install_trivy as installer


class FakeDocker:
    image_id = "sha256:" + "a" * 64
    container_id = "b" * 64
    volume = "c" * 64

    def __init__(self, native="x86_64", corrupt=None):
        self.native, self.corrupt = native, corrupt
        self.arch, self.leaf, self.machine = installer.PLATFORMS[native]
        self.created, self.removed, self.calls = False, False, []

    def __call__(self, command, timeout=30):
        self.calls.append(command)
        def result(code=0, stdout="", stderr=""):
            return subprocess.CompletedProcess(command, code, stdout, stderr)
        if "--kind" in command:
            guard = Path(command[command.index("--evidence-dir") + 1])
            guard.mkdir()
            if self.corrupt != "missing_guard":
                (guard / "summary.json").write_text(json.dumps({"outcome": "blocked" if self.corrupt == "blocked_guard" else "completed",
                    "child_exit": 0, "exit": 0, "abort": None, "cleanup_errors": []}))
            return result()
        if command[:3] == ["docker", "image", "inspect"]:
            value = {"Id": self.image_id, "Os": "linux", "Architecture": self.arch,
                     "RepoDigests": [installer.IMAGE + "@" + self.leaf]}
            if self.corrupt == "wrong_arch": value["Architecture"] = "foreign"
            if self.corrupt == "wrong_digest": value["RepoDigests"] = [installer.IMAGE + "@sha256:" + "0" * 64]
            if self.corrupt == "wrong_id": value["Id"] = "mutable-tag"
            return result(stdout=json.dumps([value]))
        if command[:4] == ["docker", "volume", "ls", "--quiet"]:
            return result(stdout="preexisting-private-volume\n")
        if command[:3] == ["docker", "container", "inspect"]:
            name = command[3]
            if self.corrupt == "cleanup_offline" and self.removed:
                return result(1, stderr="Cannot connect to the Docker daemon: connect: no such file or directory")
            if not self.created or self.removed:
                return result(1, stderr="Error response from daemon: No such container: " + name)
            value = {"Name": "/" + self.name, "Id": self.container_id, "Image": self.image_id,
                     "Config": {"Labels": {"d2c.security.owner": self.owner}},
                     "State": {"Running": False, "Status": "created"},
                     "Mounts": [{"Type": "volume", "Name": self.volume}]}
            if self.corrupt == "foreign_owner": value["Config"]["Labels"]["d2c.security.owner"] = "other"
            if self.corrupt == "preexisting_mount": value["Mounts"][0]["Name"] = "preexisting-private-volume"
            return result(stdout=json.dumps([value]))
        if command[:3] == ["docker", "container", "create"]:
            self.created = True
            self.name = command[command.index("--name") + 1]
            self.owner = command[command.index("--label") + 1].split("=", 1)[1]
            return result(stdout=self.container_id + "\n")
        if command[:2] == ["docker", "cp"]:
            if self.corrupt == "copy_failed": return result(1)
            header = bytearray(32)
            header[:6] = b"\x7fELF\x02\x01"
            header[18:20] = (0 if self.corrupt == "wrong_elf" else self.machine).to_bytes(2, "little")
            Path(command[-1]).write_bytes(header)
            return result()
        if command[:3] == ["docker", "container", "rm"]:
            if command[-1] != self.container_id: raise AssertionError("foreign container removal")
            self.removed = True
            return result()
        if command[:3] == ["docker", "volume", "inspect"]:
            return result(1, stderr="Error response from daemon: No such volume: " + command[3])
        if command[1:] == ["--version", "--format", "json"]:
            return result(stdout=json.dumps({"Version": "0.76.0" if self.corrupt == "wrong_version" else "0.76.1"}))
        raise AssertionError("unexpected command: " + repr(command))


class InstallTrivyTest(unittest.TestCase):
    def invoke(self, root, docker):
        root = root.resolve()
        (root / "github-path").touch()
        with patch.object(installer, "validate_host", return_value=(root.resolve() / "workspace", root.resolve())), \
                patch.object(installer.platform, "machine", return_value=docker.native), \
                patch.object(installer, "command", side_effect=docker), \
                patch.object(installer, "run_guard", side_effect=docker), \
                patch.object(installer.shutil, "disk_usage", return_value=type("Space", (), {"free": 32 * installer.GIB})()):
            code = installer.main(["--install-dir", str(root / "bin"), "--evidence-dir", str(root / "evidence"),
                                   "--github-path", str(root / "github-path")])
        return code, json.loads((root / "evidence/summary.json").read_text())

    def test_both_native_platforms_install_verified_cli_after_owned_cleanup(self):
        for native in installer.PLATFORMS:
            with self.subTest(native=native), TemporaryDirectory() as temporary:
                root, docker = Path(temporary), FakeDocker(native)
                code, proof = self.invoke(root, docker)
                self.assertEqual(code, 0)
                self.assertEqual(proof["outcome"], "installed")
                self.assertEqual(proof["version"], "0.76.1")
                self.assertEqual(proof["actual_image_id"], docker.image_id)
                self.assertEqual(proof["expected_platform_manifest"], docker.leaf)
                self.assertRegex(proof["binary_sha256"], "^[a-f0-9]{64}$")
                self.assertEqual(proof["cleanup"]["anonymous_volumes"], [docker.volume])
                self.assertNotIn("preexisting-private-volume", json.dumps(proof))
                self.assertTrue(docker.removed)
                self.assertEqual((root / "github-path").read_text(), str(root.resolve() / "bin") + "\n")
                self.assertEqual((root / "bin/trivy").stat().st_mode & 0o777, 0o755)
                self.assertEqual((root / "bin").stat().st_mode & 0o777, 0o700)
                self.assertEqual((root / "evidence").stat().st_mode & 0o777, 0o700)
                self.assertFalse(any(command[:2] in (["docker", "run"], ["docker", "start"], ["docker", "exec"])
                                     for command in docker.calls))
                self.assertFalse(any("prune" in command or "--privileged" in command for command in docker.calls))
                pull = next(command for command in docker.calls if "--kind" in command)
                self.assertIn("10", pull)
                self.assertIn("6", pull)
                self.assertEqual(pull[-5:], ["docker", "pull", "--platform", "linux/" + docker.arch, installer.IMAGE + "@" + docker.leaf])

    def test_invalid_pull_or_image_identity_fails_before_create_or_registration(self):
        for corrupt in ("missing_guard", "blocked_guard", "wrong_arch", "wrong_digest", "wrong_id"):
            with self.subTest(corrupt=corrupt), TemporaryDirectory() as temporary:
                root, docker = Path(temporary), FakeDocker(corrupt=corrupt)
                code, proof = self.invoke(root, docker)
                self.assertEqual(code, 70)
                self.assertEqual(proof["outcome"], "failed")
                self.assertFalse(docker.created)
                self.assertFalse((root / "bin").exists())
                self.assertEqual((root / "github-path").read_text(), "")

    def test_copy_elf_version_or_cleanup_failure_cannot_register_cli(self):
        for corrupt in ("copy_failed", "wrong_elf", "wrong_version", "cleanup_offline"):
            with self.subTest(corrupt=corrupt), TemporaryDirectory() as temporary:
                root, docker = Path(temporary), FakeDocker(corrupt=corrupt)
                code, proof = self.invoke(root, docker)
                self.assertEqual(code, 70)
                self.assertTrue(docker.removed)
                self.assertFalse((root / "bin").exists())
                self.assertEqual((root / "github-path").read_text(), "")
                if corrupt == "cleanup_offline": self.assertEqual(proof["cleanup"]["outcome"], "uncertain")

    def test_foreign_owner_or_preexisting_mount_is_never_removed(self):
        for corrupt in ("foreign_owner", "preexisting_mount"):
            with self.subTest(corrupt=corrupt), TemporaryDirectory() as temporary:
                root, docker = Path(temporary), FakeDocker(corrupt=corrupt)
                code, proof = self.invoke(root, docker)
                self.assertEqual(code, 70)
                self.assertFalse(docker.removed)
                self.assertEqual(proof["cleanup"]["outcome"], "uncertain")

    def test_existing_install_is_preserved(self):
        with TemporaryDirectory() as temporary:
            root, docker = Path(temporary), FakeDocker()
            (root / "bin").mkdir()
            (root / "bin/trivy").write_text("preserved")
            self.assertEqual(self.invoke(root, docker)[0], 70)
            self.assertEqual((root / "bin/trivy").read_text(), "preserved")
            self.assertEqual(docker.calls, [])

    def test_non_ephemeral_host_is_rejected_before_any_docker_command(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(installer, "validate_host", side_effect=installer.PreparationBlocked("unsupported host")), \
                    patch.object(installer, "command") as command:
                self.assertEqual(installer.main(["--install-dir", str(root / "bin"), "--evidence-dir", str(root / "evidence")]), 70)
                command.assert_not_called()

    def test_exact_absence_is_distinct_from_disconnected_daemon(self):
        for message, absent in (("Error response from daemon: No such container: owned", True),
                                ("Cannot connect: no such file or directory", False)):
            with patch.object(installer, "command", return_value=subprocess.CompletedProcess([], 1, "", message)):
                if absent: self.assertIsNone(installer.inspect_container("owned"))
                else:
                    with self.assertRaises(installer.InstallError): installer.inspect_container("owned")

    def test_release_index_and_native_leaves_are_fixed_digests(self):
        self.assertEqual(installer.VERSION, "0.76.1")
        self.assertRegex(installer.INDEX, "^sha256:[0-9a-f]{64}$")
        self.assertEqual(set(installer.PLATFORMS), {"x86_64", "aarch64"})
        for arch, leaf, machine in installer.PLATFORMS.values():
            self.assertIn(arch, {"amd64", "arm64"})
            self.assertRegex(leaf, "^sha256:[0-9a-f]{64}$")
            self.assertIn(machine, {62, 183})

    def test_guard_cancellation_is_forwarded_and_waited_without_kill(self):
        from unittest.mock import Mock
        import signal
        process = Mock()
        process.poll.return_value = None
        handlers = {}
        def handler(number, value):
            handlers[number] = value
            return signal.SIG_DFL
        def wait():
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            return 0
        process.wait.side_effect = wait
        with patch.object(installer.subprocess, "Popen", return_value=process), patch.object(installer.signal, "signal", side_effect=handler):
            result = installer.run_guard(["owned-guard"])
        self.assertEqual(result.returncode, 143)
        process.send_signal.assert_called_once_with(signal.SIGTERM)
        process.kill.assert_not_called()
        process.wait.assert_called_once_with()

    def test_cancellation_before_guard_spawn_is_forwarded_after_spawn(self):
        from unittest.mock import Mock
        import signal
        process = Mock()
        process.poll.return_value = None
        process.wait.return_value = 0
        handlers = {}
        def handler(number, value):
            handlers[number] = value
            return signal.SIG_DFL
        def spawn(*args, **kwargs):
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            return process
        with patch.object(installer.subprocess, "Popen", side_effect=spawn), patch.object(installer.signal, "signal", side_effect=handler):
            self.assertEqual(installer.run_guard(["owned-guard"]).returncode, 143)
        process.send_signal.assert_called_once_with(signal.SIGTERM)
        process.kill.assert_not_called()

    def test_symlink_install_ancestor_is_refused_before_docker(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            (root / "target").mkdir()
            (root / "linked").symlink_to(root / "target", target_is_directory=True)
            with patch.object(installer, "validate_host", return_value=(root / "workspace", root)), \
                    patch.object(installer.platform, "machine", return_value="x86_64"), \
                    patch.object(installer, "command") as command:
                self.assertEqual(installer.main(["--install-dir", str(root / "linked/bin"),
                                                "--evidence-dir", str(root / "evidence")]), 70)
                command.assert_not_called()
                self.assertFalse((root / "target/bin").exists())

    def test_unacknowledged_create_is_uncertain_even_if_inspect_is_absent(self):
        with patch.object(installer, "inspect_container", return_value=None), \
                patch.object(installer, "command") as command:
            with self.assertRaises(installer.InstallError):
                installer.cleanup("new", "owner", "sha256:" + "a" * 64, None, set(), None, {})
            command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
