"""Check that the ordinary CI cannot bypass guarded build/test admission."""
from __future__ import annotations

from pathlib import Path
import re
import subprocess
import unittest

import yaml


ROOT = Path(__file__).parents[1]


def workflow():
    return yaml.load((ROOT / ".github/workflows/ci.yml").read_text(), Loader=yaml.BaseLoader)


class OrdinaryCIContainerGuardTest(unittest.TestCase):
    def setUp(self):
        self.value = workflow()
        self.job = self.value["jobs"]["python"]
        self.steps = {step["name"]: step for step in self.job["steps"]}

    def test_every_docker_build_uses_complete_guard_admission_and_shared_private_state(self):
        builds = [step for step in self.steps.values() if "docker build " in step.get("run", "")]
        self.assertEqual(len(builds), 2)
        for step in builds:
            with self.subTest(step=step["name"]):
                script = step["run"]
                self.assertIn("scripts/container_security_guard.py", script)
                self.assertIn("--kind build", script)
                self.assertIn("--minimum-free-gib 20", script)
                self.assertIn("--reserve-free-gib 6", script)
                self.assertIn("--storage-path /var/lib/docker", script)
                self.assertIn("--build-state-dir", script)
                self.assertIn("--iidfile", script)
                self.assertIn('"$RUNNER_TEMP/d2c-ci-build-state-', script)
                self.assertNotIn('"$RUNNER_TEMP/$CI_CONTAINER_EVIDENCE/build-state', script)
        self.assertIn("--target catalog-portal", builds[0]["run"])
        self.assertIn("--file lakehouse/Dockerfile", builds[1]["run"])

    def test_existing_smoke_coverage_uses_exact_built_ids_and_owned_containers(self):
        for name in ("Smoke test catalog portal target", "Smoke test local Iceberg catalog"):
            with self.subTest(name=name):
                script = self.steps[name]["run"]
                for value in ("scripts/container_security_guard.py", "--kind test", "--minimum-free-gib 8",
                              "--reserve-free-gib 6", '--container-name "$name" --owner-token "$owner"',
                              "d2c.security.owner=", 'image="$(cat '):
                    self.assertIn(value, script)
                self.assertNotIn("docker rm --force", script)
                self.assertNotIn("--volume", script)
        catalog = self.steps["Smoke test catalog portal target"]["run"]
        self.assertIn("/healthz", catalog)
        self.assertIn("/api/v1/data-products", catalog)
        self.assertIn("--publish 127.0.0.1::8082", catalog)
        local = self.steps["Smoke test local Iceberg catalog"]["run"]
        self.assertIn("/opt/spark/bin/spark-submit", local)
        self.assertIn("validate_d2c_iceberg.py", local)
        self.assertIn("--master 'local[1]'", local)

    def test_noncontainer_regressions_and_permissions_are_preserved(self):
        for name in ("Compile Python modules", "Run unit tests", "Check schema compatibility",
                     "Validate pipeline evidence invariants", "Validate data product catalog evidence",
                     "Validate local lakehouse Compose profile", "Enforce local-only docs policy",
                     "Render Kubernetes manifests"):
            self.assertIn(name, self.steps)
        self.assertEqual(self.value["permissions"], {"contents": "read"})
        self.assertIn("terraform", self.value["jobs"])
        self.assertGreaterEqual(int(self.job["timeout-minutes"]), 150)

    def test_evidence_is_uploaded_after_failure_without_private_builder_credentials(self):
        upload = self.steps["Preserve guarded build and smoke outcomes"]
        self.assertEqual(upload["if"], "always()")
        self.assertEqual(upload["with"]["if-no-files-found"], "error")
        self.assertEqual(upload["with"]["path"], "${{ runner.temp }}/${{ env.CI_CONTAINER_EVIDENCE }}")
        self.assertFalse(any("runner." in value for value in self.job.get("env", {}).values()))

    def test_source_is_compiled_once_and_consumed_without_fallback_rebuild(self):
        jobs = self.value["jobs"]
        source = jobs["lakehouse-source"]
        compile_steps = [step for step in source["steps"] if "lakehouse_artifacts.py build" in step.get("run", "")]
        self.assertEqual(len(compile_steps), 1)
        self.assertIn("--native-platform linux/amd64", compile_steps[0]["run"])
        self.assertIn("--discard-created-builder-cache", compile_steps[0]["run"])
        self.assertEqual(self.job["needs"], ["lakehouse-source"])
        self.assertEqual(self.job["if"], "always()")
        self.assertIn("Require a successful verified source export", self.steps)
        self.assertIn("SOURCE_RESULT", self.steps["Require a successful verified source export"]["run"])
        final = self.steps["Build Iceberg streaming image"]["run"]
        self.assertIn("--build-context", final)
        self.assertIn("D2C_ARTIFACT_STAGE=d2c_artifacts", final)
        self.assertIn("lakehouse_artifacts.py verify", self.steps["Verify exact source recipe and exported payloads"]["run"])

    def test_shell_steps_parse_without_executing_any_docker_commands(self):
        for name, step in self.steps.items():
            if "run" not in step:
                continue
            script = re.sub(r"\$\{\{.*?\}\}", "fixture", step["run"])
            result = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True)
            with self.subTest(step=name):
                self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
