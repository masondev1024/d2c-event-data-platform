"""Exercise the printed Make recipe without invoking Make or Docker builds."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class MakefileContainerGuardTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # -n prints this nonrecursive recipe; it never executes its Python/guard.
        printed = subprocess.check_output(
            ["make", "--no-print-directory", "-n", "lakehouse-image", f"PYTHON={sys.executable}"],
            cwd=ROOT, text=True, env={**os.environ, "MAKEFLAGS": "", "MFLAGS": ""},
        )
        command = shlex.split(printed, comments=True)
        if len(command) != 3 or command[:2] != [sys.executable, "-c"]:
            raise AssertionError("lakehouse-image must dispatch one guarded Python command")
        cls.recipe = compile(command[2], "Makefile:lakehouse-image", "exec")

    def dispatch(self, root=ROOT, extra=None, temporary=None):
        environment = {} if extra is None else {"D2C_DOCKER_STORAGE_PATH": extra}
        temporary = tempfile.gettempdir() if temporary is None else temporary
        with patch.dict(os.environ, environment, clear=True), \
                patch("pathlib.Path.cwd", return_value=root), \
                patch("tempfile.gettempdir", return_value=temporary), \
                patch("os.execv") as execute:
            exec(self.recipe, {})
        execute.assert_called_once()
        executable, arguments = execute.call_args.args
        self.assertEqual(executable, sys.executable)
        return arguments

    @staticmethod
    def option(arguments, flag):
        return arguments[arguments.index(flag) + 1]

    def test_complete_build_uses_guard_and_preserves_the_image_target(self):
        arguments = self.dispatch()
        self.assertEqual(arguments[:2], [sys.executable, str(ROOT / "scripts/container_security_guard.py")])
        for flag, value in (("--kind", "build"), ("--minimum-free-gib", "20"),
                            ("--reserve-free-gib", "6"), ("--timeout-seconds", "1800")):
            self.assertEqual(self.option(arguments, flag), value)
        self.assertEqual(arguments[arguments.index("--") + 1:],
                         ["docker", "build", "--file", "lakehouse/Dockerfile",
                          "--tag", "d2c-iceberg-stream:local", "."])
        storage = [arguments[index + 1] for index, item in enumerate(arguments) if item == "--storage-path"]
        self.assertIn(str(ROOT), storage)
        self.assertIn(str(Path(tempfile.gettempdir()).resolve()), storage)

    def test_builder_state_is_stable_and_separate_for_each_repository(self):
        first, second = self.dispatch(), self.dispatch()
        alternate = self.dispatch(root=ROOT.parent / "another checkout;literal")
        state = Path(self.option(first, "--build-state-dir"))
        self.assertEqual(state, Path(self.option(second, "--build-state-dir")))
        self.assertNotEqual(state, Path(self.option(alternate, "--build-state-dir")))
        self.assertFalse(state.is_relative_to(ROOT))
        self.assertEqual(state.name, "build-state")

    def test_each_invocation_has_fresh_evidence_outside_the_build_context(self):
        first, second = self.dispatch(), self.dispatch()
        first_path = Path(self.option(first, "--evidence-dir"))
        second_path = Path(self.option(second, "--evidence-dir"))
        self.assertNotEqual(first_path, second_path)
        self.assertEqual(first_path.parent, second_path.parent)
        self.assertFalse(first_path.is_relative_to(ROOT))
        self.assertRegex(first_path.name, r"^[0-9a-f]{32}$")
        self.assertFalse(first_path.exists())

    def test_additional_storage_path_is_forwarded_as_one_literal_argument(self):
        extra = "/Docker disk/path with spaces;literal"
        arguments = self.dispatch(extra=extra)
        self.assertIn(["--storage-path", extra],
                      [arguments[index:index + 2] for index in range(len(arguments) - 1)])

    def test_temp_directory_inside_build_context_fails_before_guard_dispatch(self):
        with patch("pathlib.Path.cwd", return_value=ROOT), \
                patch("tempfile.gettempdir", return_value=str(ROOT / "temporary")), \
                patch("os.execv") as execute, self.assertRaisesRegex(SystemExit, "outside the build context"):
            exec(self.recipe, {})
        execute.assert_not_called()

    def test_symlinked_private_state_into_checkout_fails_before_dispatch(self):
        with tempfile.TemporaryDirectory() as scratch:
            base = Path(scratch).resolve()
            root, temporary = base / "checkout", base / "temporary"
            root.mkdir()
            temporary.mkdir()
            namespace = "d2c-container-security-" + hashlib.sha256(os.fsencode(root)).hexdigest()[:24]
            (temporary / namespace).symlink_to(root, target_is_directory=True)
            with patch("pathlib.Path.cwd", return_value=root), \
                    patch("tempfile.gettempdir", return_value=str(temporary)), \
                    patch("os.execv") as execute, self.assertRaisesRegex(SystemExit, "outside the build context"):
                exec(self.recipe, {})
            execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
