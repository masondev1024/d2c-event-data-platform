"""Check security workflow failure propagation without starting any CI jobs."""
from __future__ import annotations

import json
from pathlib import Path
import unittest

import yaml


ROOT = Path(__file__).parents[1]


def workflow():
    return yaml.load((ROOT / ".github/workflows/security.yml").read_text(), Loader=yaml.BaseLoader)


class ContainerSecurityWorkflowTest(unittest.TestCase):
    def test_each_role_and_architecture_is_independent_and_does_not_fail_fast(self):
        job = workflow()["jobs"]["image-scan"]
        self.assertEqual(job["strategy"]["fail-fast"], "false")
        self.assertEqual(set(job["strategy"]["matrix"]["role"]), {"worker", "api", "catalog"})
        self.assertEqual(set(job["strategy"]["matrix"]["arch"]), {"amd64", "arm64"})
        self.assertEqual({item["arch"]: item["runner"] for item in job["strategy"]["matrix"]["include"]},
                         {"amd64": "ubuntu-24.04", "arm64": "ubuntu-24.04-arm"})
        self.assertEqual(job["runs-on"], "${{ matrix.runner }}")
        self.assertNotIn("needs", job)
        build = next(s for s in job["steps"] if s.get("id") == "build")
        self.assertEqual(build["continue-on-error"], "true")
        self.assertIn("--kind build", build["run"])
        self.assertIn("--minimum-free-gib 20", build["run"])
        self.assertIn("--iidfile", build["run"])
        scanner = next(s for s in job["steps"] if "scripts/scan_container_images.py" in s.get("run", ""))
        self.assertEqual(scanner["if"], "${{ !cancelled() && steps.storage.outcome == 'success' }}")
        self.assertIn('--required-role "$ROLE"', scanner["run"])
        self.assertIn('image="$(cat "$RUNNER_TEMP/$ROLE.iid")"', scanner["run"])
        self.assertIn("--guard-script scripts/container_security_guard.py", scanner["run"])
        self.assertIn("--total-timeout 4500", scanner["run"])
        self.assertGreaterEqual(int(job["timeout-minutes"]), 150)
        self.assertTrue(any("steps.build.outcome != 'success'" in s.get("if", "") and s.get("run") == "exit 1" for s in job["steps"]))

    def test_repository_failures_cannot_cancel_image_evidence_collection(self):
        jobs = workflow()["jobs"]
        self.assertNotIn("needs", jobs["repository-scan"])
        self.assertNotIn("needs", jobs["image-scan"])
        for name in ("repository-scan", "image-scan", "lakehouse-scan"):
            upload = next(s for s in jobs[name]["steps"] if s.get("uses", "").startswith("actions/upload-artifact@")
                          and ("container-security/" in s["with"]["path"] or s["with"]["path"] == "repository-security/"))
            self.assertEqual(upload["if"], "always()")
            self.assertEqual(upload["with"]["if-no-files-found"], "error")

    def test_final_gate_rejects_failed_cancelled_and_skipped_dependencies(self):
        gate = workflow()["jobs"]["container-scan"]
        self.assertEqual(gate["if"], "always()")
        self.assertEqual(set(gate["needs"]), {"dependency-and-code-scan", "repository-scan", "image-scan",
                                              "lakehouse-source", "lakehouse-scan"})
        code = "\n".join(gate["steps"][0]["run"].splitlines()[1:-1])
        from unittest.mock import patch
        for dependency in gate["needs"]:
            for status in ("failure", "cancelled", "skipped"):
                results = {name: {"result": status if name == dependency else "success"} for name in gate["needs"]}
                with self.subTest(dependency=dependency, status=status), patch.dict("os.environ", {"RESULTS": json.dumps(results)}):
                    with self.assertRaises(SystemExit) as error:
                        exec(compile(code, "security-gate", "exec"), {})
                    self.assertEqual(error.exception.code, 1)
        with patch.dict("os.environ", {"RESULTS": json.dumps({name: {"result": "success"} for name in gate["needs"]})}):
            exec(compile(code, "security-gate", "exec"), {})

    def test_permissions_and_gate_threshold_remain_strict(self):
        value = workflow()
        self.assertEqual(value["permissions"], {"contents": "read"})
        repository = next(s["run"] for s in value["jobs"]["repository-scan"]["steps"] if "trivy-config.yaml fs" in s.get("run", ""))
        for flag in ("--severity HIGH,CRITICAL", "--ignore-unfixed", "--exit-code 1", "--ignorefile /dev/null"):
            self.assertIn(flag, repository)


if __name__ == "__main__":
    unittest.main()
