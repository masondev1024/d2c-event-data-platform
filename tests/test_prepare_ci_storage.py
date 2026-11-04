"""No SDK removal: mock hosted markers, capacity and privileged dispatch."""
from __future__ import annotations

import argparse
import contextlib
import io
import json
from pathlib import Path
import stat
import subprocess
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import prepare_ci_storage as storage


class CIStoragePreparationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.workspace = self.root / "checkout"
        self.workspace.mkdir()
        self.options = argparse.Namespace(evidence_dir=self.root / "evidence", minimum_free_gib=32,
                                          allow_ephemeral_sdk_cleanup=False)

    def run_preparer(self, *, allow=False, grow=False, eligible=True, delete_code=0):
        self.options.allow_ephemeral_sdk_cleanup = allow
        free = {"value": 10 * storage.GIB}

        def remove(path, **kwargs):
            if grow:
                free["value"] = 40 * storage.GIB
            return delete_code

        with patch.object(storage, "validate_host", return_value=(self.workspace, self.root)), \
                patch.object(storage, "available_bytes", side_effect=lambda path: free["value"]), \
                patch.object(storage, "eligible_sdk", return_value=eligible), \
                patch.object(storage, "sdk_fingerprint", return_value=(("fixture", 1, 2, 0, 0o755),)), \
                patch.object(storage.os.path, "lexists", return_value=False), \
                patch.object(storage, "delete_sdk", side_effect=remove) as deletion, \
                contextlib.redirect_stdout(io.StringIO()):
            code = storage.prepare(self.options)
        return code, json.loads((self.options.evidence_dir / "summary.json").read_text()), deletion

    def test_default_never_deletes_and_insufficient_capacity_is_blocked(self):
        code, report, deletion = self.run_preparer()
        self.assertEqual(code, 75)
        self.assertEqual(report["outcome"], "blocked")
        self.assertFalse(report["cleanup_explicitly_enabled"])
        deletion.assert_not_called()

    def test_opt_in_stops_after_first_sdk_when_measured_goal_is_reached(self):
        code, report, deletion = self.run_preparer(allow=True, grow=True)
        self.assertEqual(code, 0)
        self.assertEqual(report["outcome"], "ready")
        self.assertEqual(len(report["actions"]), 1)
        self.assertEqual(deletion.call_args.args[0], storage.SDK_PATHS[0])
        self.assertTrue(deletion.call_args.kwargs["allow"])
        self.assertIn("expected_identity", deletion.call_args.kwargs)

    def test_exhausted_exact_whitelist_is_failure_without_broader_cleanup(self):
        code, report, deletion = self.run_preparer(allow=True)
        self.assertEqual(code, 75)
        self.assertEqual([call.args[0] for call in deletion.call_args_list], list(storage.SDK_PATHS))
        self.assertEqual(len(report["actions"]), 3)

    def test_missing_sdk_roots_do_not_trigger_any_removal(self):
        code, report, deletion = self.run_preparer(allow=True, eligible=False)
        self.assertEqual(code, 75)
        self.assertTrue(all(action["outcome"] == "absent" for action in report["actions"]))
        deletion.assert_not_called()

    def test_failed_removal_does_not_try_more_paths(self):
        code, report, deletion = self.run_preparer(allow=True, delete_code=1)
        self.assertEqual(code, 75)
        self.assertEqual(deletion.call_count, 1)
        self.assertEqual(report["actions"][0]["outcome"], "failed")

    def test_already_sufficient_capacity_performs_host_validation_but_no_sdk_inspection(self):
        with patch.object(storage, "validate_host", return_value=(self.workspace, self.root)) as host, \
                patch.object(storage, "available_bytes", return_value=40 * storage.GIB), \
                patch.object(storage, "eligible_sdk") as eligibility, \
                patch.object(storage, "delete_sdk") as deletion, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(storage.prepare(self.options), 0)
        host.assert_called_once()
        eligibility.assert_not_called()
        deletion.assert_not_called()

    def test_spoofed_or_local_environment_is_rejected_even_with_sufficient_space(self):
        with patch.object(storage, "validate_host", side_effect=storage.PreparationBlocked("untrusted host")), \
                patch.object(storage, "available_bytes", return_value=100 * storage.GIB) as free, \
                patch.object(storage, "delete_sdk") as deletion, contextlib.redirect_stdout(io.StringIO()):
            self.options.allow_ephemeral_sdk_cleanup = True
            self.assertEqual(storage.prepare(self.options), 75)
        free.assert_not_called()
        deletion.assert_not_called()

    def test_host_markers_reject_mac_self_hosted_and_missing_actions(self):
        good = {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted", "RUNNER_OS": "Linux"}
        for system, env in (("Darwin", good), ("Linux", {**good, "RUNNER_ENVIRONMENT": "self-hosted"}),
                            ("Linux", {**good, "GITHUB_ACTIONS": "false"}), ("Linux", {})):
            with self.subTest(system=system, env=env), patch.object(storage.platform, "system", return_value=system):
                with self.assertRaises(storage.PreparationBlocked):
                    storage.validate_host(env)

    def test_forged_markers_do_not_accept_wrong_runner_uid_or_workspace(self):
        env = {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted", "RUNNER_OS": "Linux",
               "GITHUB_WORKSPACE": "/Users/other/checkout", "RUNNER_TEMP": "/home/runner/work/_temp"}
        with patch.object(storage.platform, "system", return_value="Linux"), \
                patch.object(storage.os, "geteuid", return_value=1001), patch.object(storage.os, "getuid", return_value=1001), \
                patch.object(storage.pwd, "getpwuid", return_value=SimpleNamespace(pw_name="runner", pw_dir="/home/runner")), \
                patch.object(storage, "real_directory", side_effect=lambda path: path):
            with self.assertRaises(storage.PreparationBlocked):
                storage.validate_host(env)

    def sdk_metadata(self, path, *, bad_parent_uid=None, bad_mode=None, changed_inode=False):
        leaf = storage.SDK_PATHS[0]
        uid = 1001 if path == leaf else 0
        mode = stat.S_IFDIR | 0o755
        if path == leaf.parent and bad_parent_uid is not None:
            uid = bad_parent_uid
        if bad_mode and path == bad_mode[0]:
            mode = bad_mode[1]
        return SimpleNamespace(st_uid=uid, st_mode=mode, st_dev=1, st_ino=2 if changed_inode else 1)

    def test_owner_writable_symlink_and_anchor_checks_are_fail_closed(self):
        leaf = storage.SDK_PATHS[0]
        cases = ({"bad_parent_uid": 1001},
                 {"bad_mode": (leaf.parent, stat.S_IFDIR | 0o777)},
                 {"bad_mode": (leaf, stat.S_IFDIR | 0o775)},
                 {"bad_mode": (leaf.parent, stat.S_IFLNK | 0o777)},
                 {"bad_mode": (Path("/"), stat.S_IFDIR | 0o777)})
        for fault in cases:
            with self.subTest(fault=fault), patch.object(Path, "lstat", autospec=True,
                    side_effect=lambda path: self.sdk_metadata(path, **fault)):
                with self.assertRaises(storage.PreparationBlocked):
                    storage.sdk_fingerprint(leaf, 1001)

    def test_mounts_and_nonwhitelisted_paths_are_never_eligible(self):
        leaf = storage.SDK_PATHS[0]
        with patch.object(storage, "sdk_fingerprint", return_value=()), patch.object(storage.os, "geteuid", return_value=1001):
            for mount in (leaf, leaf / "mounted-child"):
                with self.subTest(mount=mount), patch.object(storage, "mounted_paths", return_value={mount}):
                    with self.assertRaises(storage.PreparationBlocked):
                        storage.eligible_sdk(leaf, self.workspace, self.root)
        with self.assertRaises(storage.PreparationBlocked):
            storage.eligible_sdk(Path("/home/runner/work"), self.workspace, self.root)

    def test_privileged_dispatch_requires_opt_in_and_stable_validated_identity(self):
        leaf = storage.SDK_PATHS[0]
        with patch.object(storage.subprocess, "run") as run:
            with self.assertRaises(storage.PreparationBlocked):
                storage.delete_sdk(leaf)
            with self.assertRaises(storage.PreparationBlocked):
                storage.delete_sdk(Path("/usr"), allow=True)
        run.assert_not_called()
        with patch.object(storage, "validate_host", return_value=(self.workspace, self.root)), \
                patch.object(storage, "eligible_sdk", return_value=True), \
                patch.object(storage, "sdk_fingerprint", return_value=(("new",),)), \
                patch.object(storage.subprocess, "run") as run:
            with self.assertRaises(storage.PreparationBlocked):
                storage.delete_sdk(leaf, allow=True, expected_identity=(("old",),))
        run.assert_not_called()

    def test_valid_privileged_command_has_one_exact_path_and_no_shell(self):
        identity = (("same",),)
        with patch.object(storage, "validate_host", return_value=(self.workspace, self.root)), \
                patch.object(storage, "eligible_sdk", return_value=True), \
                patch.object(storage, "sdk_fingerprint", return_value=identity), \
                patch.object(storage.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as run:
            self.assertEqual(storage.delete_sdk(storage.SDK_PATHS[0], allow=True, expected_identity=identity), 0)
        self.assertEqual(run.call_args.args[0], ["sudo", "-n", "rm", "--recursive", "--force", "--one-file-system", "--", "/usr/local/lib/android"])
        self.assertNotIn("shell", run.call_args.kwargs)

    def test_cli_cannot_add_paths_or_lower_the_capacity_requirement(self):
        for extra in (("--cleanup-path", "/tmp"), ("--minimum-free-gib", "16"), ("--minimum-free-gib", "nan")):
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()), patch.object(storage, "prepare") as prepare:
                with self.assertRaises(SystemExit):
                    storage.main(["--evidence-dir", str(self.options.evidence_dir), *extra])
                prepare.assert_not_called()
        self.assertEqual(storage.SDK_PATHS, (Path("/usr/local/lib/android"), Path("/usr/share/dotnet"), Path("/opt/ghc")))


if __name__ == "__main__":
    unittest.main()
