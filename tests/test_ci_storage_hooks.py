"""Check storage admission wiring without running its helper or any cleanup."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
JOBS = {"ci": "python", "security": "image-scan", "release-images": "publish"}
OPT_IN = "${{ github.event_name != 'workflow_dispatch' || inputs.allow_ephemeral_sdk_cleanup }}"


def condition(expression, *, event="push", ref_type="tag", opt_in=False, storage="success", dependency_result="success"):
    source = expression.strip().removeprefix("${{").removesuffix("}}")
    source = source.replace("github.event_name", repr(event)).replace("github.ref_type", repr(ref_type))
    source = source.replace("github.ref", repr("refs/tags/v1.2.3" if ref_type == "tag" else "refs/heads/main"))
    source = re.sub(r"startsWith\(([^,]+),\s*([^\)]+)\)", r"\1.startswith(\2)", source)
    source = source.replace("inputs.allow_ephemeral_sdk_cleanup", repr(opt_in))
    source = source.replace("success()", "True").replace("!cancelled()", "True").replace("&&", "and").replace("||", "or")
    source = re.sub(r"steps\.([a-z_-]+)\.outcome",
                    lambda match: repr(storage if match[1] == "storage" else "success"), source)
    source = re.sub(r"needs\.[a-z_-]+\.result", repr(dependency_result), source)
    return bool(eval("(" + source + ")", {"__builtins__": {}}, {}))


class CIStorageHookTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflows = {name: yaml.load((ROOT / f".github/workflows/{name}.yml").read_text(),
                                         Loader=yaml.BaseLoader) for name in JOBS}

    def hook(self, name):
        return next(step for step in self.workflows[name]["jobs"][JOBS[name]]["steps"]
                    if step.get("id") == "storage")

    def invoke_hook(self, name, *, event="push", opt_in=False, helper_exit=0):
        # Substitute only python3 with an argument recorder. The real storage
        # helper is never loaded, including when the explicit opt-in is true.
        hook = self.hook(name)
        with TemporaryDirectory() as scratch:
            root = Path(scratch)
            binary = root / "python3"
            binary.write_text(f"#!{sys.executable}\nimport json, os, sys\n"
                              "with open(os.environ['TEST_CAPTURE'], 'w') as stream:\n"
                              "    json.dump(sys.argv[1:], stream)\n"
                              "raise SystemExit(int(os.environ['TEST_EXIT']))\n")
            binary.chmod(0o700)
            capture = root / "arguments.json"
            environment = {**os.environ, "PATH": str(root) + os.pathsep + os.environ.get("PATH", ""),
                           "RUNNER_TEMP": str(root), "GITHUB_RUN_ID": "12345", "GITHUB_RUN_ATTEMPT": "2",
                           "ROLE": "lakehouse", "ARCH": "arm64", "TEST_CAPTURE": str(capture),
                           "TEST_EXIT": str(helper_exit), "ALLOW_EPHEMERAL_SDK_CLEANUP":
                               str(condition(hook["env"]["ALLOW_EPHEMERAL_SDK_CLEANUP"],
                                             event=event, opt_in=opt_in)).lower()}
            result = subprocess.run(["bash", "-e", "-c", hook["run"]], cwd=ROOT, env=environment,
                                    capture_output=True, text=True, timeout=10)
            return result.returncode, json.loads(capture.read_text())

    def test_manual_input_is_a_typed_boolean_defaulting_to_false(self):
        for name, value in self.workflows.items():
            with self.subTest(workflow=name):
                inputs = value["on"]["workflow_dispatch"]["inputs"]
                self.assertEqual(set(inputs), {"allow_ephemeral_sdk_cleanup"})
                self.assertEqual(inputs["allow_ephemeral_sdk_cleanup"]["type"], "boolean")
                self.assertEqual(inputs["allow_ephemeral_sdk_cleanup"]["default"], "false")
                self.assertEqual(inputs["allow_ephemeral_sdk_cleanup"]["required"], "false")

    def test_admission_is_immediately_after_checkout_before_setup_and_heavy_work(self):
        for name, job_name in JOBS.items():
            with self.subTest(workflow=name):
                job = self.workflows[name]["jobs"][job_name]
                steps = job["steps"]
                checkout = next(index for index, step in enumerate(steps)
                                if step.get("uses", "").startswith("actions/checkout@"))
                hook = self.hook(name)
                self.assertEqual(steps.index(hook), checkout + 1)
                self.assertIn('python3 scripts/prepare_ci_storage.py "${arguments[@]}"', hook["run"])
                self.assertIn("--minimum-free-gib 32", hook["run"])
                self.assertNotIn("continue-on-error", hook)
                self.assertFalse(any("runner." in str(value) for value in job.get("env", {}).values()))
                for step in steps[:steps.index(hook)]:
                    self.assertFalse(step.get("uses", "").startswith("actions/setup-python@"))

    def test_proposed_automatic_cleanup_is_explicit_and_manual_runs_require_true(self):
        contexts = (("push", False), ("push", True), ("pull_request", True),
                    ("schedule", True), ("workflow_dispatch", False), ("workflow_dispatch", True))
        for name in JOBS:
            self.assertEqual(self.hook(name)["env"]["ALLOW_EPHEMERAL_SDK_CLEANUP"], OPT_IN)
            for event, opt_in in contexts:
                with self.subTest(workflow=name, event=event, opt_in=opt_in):
                    code, arguments = self.invoke_hook(name, event=event, opt_in=opt_in)
                    self.assertEqual(code, 0)
                    self.assertEqual(arguments[0], "scripts/prepare_ci_storage.py")
                    self.assertEqual(arguments[arguments.index("--minimum-free-gib") + 1], "32")
                    self.assertEqual("--allow-ephemeral-sdk-cleanup" in arguments,
                                     event != "workflow_dispatch" or opt_in)

    def test_capacity_failure_is_preserved_and_storage_evidence_always_uploads(self):
        for name, job_name in JOBS.items():
            with self.subTest(workflow=name):
                code, arguments = self.invoke_hook(name, event="workflow_dispatch", helper_exit=75)
                self.assertEqual(code, 75)
                self.assertNotIn("--allow-ephemeral-sdk-cleanup", arguments)
                evidence = arguments[arguments.index("--evidence-dir") + 1]
                self.assertIn("12345-2", evidence)
                steps = self.workflows[name]["jobs"][job_name]["steps"]
                upload = steps[steps.index(self.hook(name)) + 1]
                self.assertTrue(upload["uses"].startswith("actions/upload-artifact@"))
                self.assertEqual(upload["if"], "${{ always() }}")
                self.assertEqual(upload["with"]["if-no-files-found"], "error")
                for key in ("name", "path"):
                    self.assertIn("github.run_id", upload["with"][key])
                    self.assertIn("github.run_attempt", upload["with"][key])
                if name == "security":
                    self.assertIn("lakehouse-arm64", evidence)
                    for key in ("name", "path"):
                        self.assertIn("matrix.role", upload["with"][key])
                        self.assertIn("matrix.arch", upload["with"][key])

    def test_blocked_capacity_cannot_be_bypassed_by_unconditional_scan_steps(self):
        for name in ("security", "release-images"):
            steps = self.workflows[name]["jobs"][JOBS[name]]["steps"]
            scan = next(step for step in steps if "scripts/scan_container_images.py" in step.get("run", ""))
            with self.subTest(workflow=name):
                self.assertFalse(condition(scan["if"], storage="failure"))
                self.assertTrue(condition(scan["if"], storage="success"))

    def test_permissions_are_unchanged_and_workflows_have_no_sdk_removal_commands(self):
        for name, value in self.workflows.items():
            expected = {"contents": "read", **({"packages": "write"} if name == "release-images" else {})}
            self.assertEqual(value["permissions"], expected)
            for job in value["jobs"].values():
                self.assertNotIn("permissions", job)
            source = (ROOT / f".github/workflows/{name}.yml").read_text()
            for sdk in ("/usr/share/dotnet", "/usr/local/lib/android", "/opt/ghc"):
                self.assertNotIn(sdk, source)
            self.assertNotRegex(self.hook(name)["run"], r"\b(?:sudo|rm|rmdir|prune)\b")

    def test_manual_release_cannot_login_or_publish_even_when_all_gates_pass(self):
        steps = self.workflows["release-images"]["jobs"]["publish"]["steps"]
        protected = [step for step in steps if step.get("id") == "publish"
                     or step.get("uses", "").startswith("docker/login-action@")]
        self.assertEqual(len(protected), 2)
        for step in protected:
            for event, ref_type in (("workflow_dispatch", "branch"), ("workflow_dispatch", "tag"),
                                    ("push", "branch"), ("pull_request", "tag"), ("schedule", "tag")):
                with self.subTest(step=step["name"], event=event, ref_type=ref_type):
                    self.assertFalse(condition(step["if"], event=event, ref_type=ref_type))
            self.assertTrue(condition(step["if"], event="push", ref_type="tag"))
            self.assertFalse(condition(step["if"], event="push", ref_type="tag", dependency_result="failure"))


if __name__ == "__main__":
    unittest.main()
