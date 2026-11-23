"""Check failed analysis and genuine findings without running Bandit itself."""
from __future__ import annotations

from contextlib import redirect_stdout
import copy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

import yaml

from scripts.validate_bandit_report import evaluate_report, main


ROOT = Path(__file__).resolve().parents[1]
CLEAN = {"errors": [], "results": [], "metrics": {"_totals": {"loc": 6706}}}
# Captured errors from the Bandit 1.8.6 / Python 3.14.2 frozen host report.
FAILED_FILES = ("app/app.py", "app/contracts.py", "app/outbox_publisher.py", "app/schema.py",
                "lakehouse/__init__.py", "lakehouse/d2c_iceberg_stream.py", "lakehouse/validate_d2c_iceberg.py",
                "scripts/__init__.py", "scripts/check_python_runtime.py", "scripts/check_schema_compatibility.py",
                "scripts/container_security_guard.py", "scripts/export_sensor_duckdb_to_lake.py",
                "scripts/lakehouse_artifacts.py", "scripts/oci_release.py", "scripts/prepare_ci_storage.py",
                "scripts/scan_container_images.py", "scripts/test_lakehouse_streaming.py", "scripts/validate_catalog.py",
                "scripts/validate_lake_contract.py", "scripts/validate_lakehouse_trivy_exceptions.py",
                "scripts/validate_pipeline_evidence.py", "services/__init__.py", "services/catalog.py",
                "services/catalog_portal.py", "services/contract.py", "services/d2c_event_consumer.py",
                "services/lake_sink.py", "services/metrics.py", "services/processor.py", "services/quality.py", "services/sink.py")


def failed_host_fixture():
    report = copy.deepcopy(CLEAN)
    report["errors"] = [{"filename": name, "reason": "exception while scanning file"} for name in FAILED_FILES]
    return report


class BanditReportTest(unittest.TestCase):
    def test_clean_report_requires_successful_analysis_and_exit_zero(self):
        result = evaluate_report(CLEAN, 0)
        self.assertEqual(result["outcome"], "pass")
        self.assertEqual(result["analyzed_lines"], 6706)

    def test_actual_31_errors_zero_findings_exit_zero_fixture_is_rejected(self):
        report = failed_host_fixture()
        self.assertEqual(len(report["errors"]), 31)
        result = evaluate_report(report, 0)
        self.assertEqual(result["outcome"], "infrastructure_fail")
        self.assertEqual(result["scan_errors"], 31)
        self.assertIn("skipped or failed", result["reason"])

    def test_findings_fail_and_exit_mismatch_is_an_infrastructure_failure(self):
        report = copy.deepcopy(CLEAN)
        report["results"] = [{"test_id": "B108", "issue_severity": "MEDIUM", "issue_confidence": "MEDIUM"}]
        self.assertEqual(evaluate_report(report, 1)["outcome"], "security_fail")
        self.assertEqual(evaluate_report(report, 0)["outcome"], "infrastructure_fail")
        self.assertEqual(evaluate_report(CLEAN, 1)["outcome"], "infrastructure_fail")

    def test_invalid_exit_or_report_structure_never_passes(self):
        for status in (-1, 2, 127, None, True, "0"):
            with self.subTest(status=status): self.assertNotEqual(evaluate_report(CLEAN, status)["outcome"], "pass")
        documents = [None, [], {}, {**CLEAN, "errors": None}, {**CLEAN, "errors": {}},
                     {**CLEAN, "results": ""}, {**CLEAN, "results": [True]}, {**CLEAN, "errors": ["failed"]}]
        for document in documents:
            with self.subTest(document=document): self.assertNotEqual(evaluate_report(document, 0)["outcome"], "pass")
        for loc in (0, -1, True, "1", None, 1.5):
            document = {**CLEAN, "metrics": {"_totals": {"loc": loc}}}
            with self.subTest(loc=loc): self.assertNotEqual(evaluate_report(document, 0)["outcome"], "pass")

    def test_cli_rejects_missing_malformed_duplicate_and_nonfinite_json(self):
        with TemporaryDirectory() as scratch:
            report = Path(scratch) / "report.json"
            for content in (None, "not-json", "[]", '{"errors": [], "errors": []}', '{"metrics": NaN}'):
                if content is not None: report.write_text(content)
                output = io.StringIO()
                with self.subTest(content=content), redirect_stdout(output):
                    self.assertEqual(main(["--report", str(report), "--bandit-exit-code", "0"]), 1)
                self.assertEqual(json.loads(output.getvalue())["outcome"], "infrastructure_fail")

    def test_workflow_retains_original_exit_and_fails_closed_on_fake_bandit_errors(self):
        value = yaml.load((ROOT / ".github/workflows/security.yml").read_text(), Loader=yaml.BaseLoader)
        steps = value["jobs"]["dependency-and-code-scan"]["steps"]
        scan = next(step for step in steps if step.get("name") == "Scan Python source")
        upload = steps[steps.index(scan) + 1]
        self.assertEqual(upload["if"], "always()")
        self.assertEqual(upload["with"]["retention-days"], "14")
        self.assertEqual(upload["with"]["if-no-files-found"], "error")
        self.assertEqual(upload["with"]["path"], "${{ env.BANDIT_EVIDENCE }}")
        findings = {**CLEAN, "results": [{"test_id": "B108", "issue_severity": "MEDIUM", "issue_confidence": "MEDIUM"}]}
        for payload, bandit_exit, expected in ((CLEAN, 0, 0), (findings, 1, 1),
                                               (failed_host_fixture(), 0, 1), (CLEAN, 2, 1)):
            with self.subTest(errors=len(payload["errors"]), bandit_exit=bandit_exit), TemporaryDirectory() as scratch:
                root = Path(scratch)
                binary = root / "bandit"
                binary.write_text(f"#!{sys.executable}\nimport json, os, pathlib, sys\n"
                                  "arguments = sys.argv[1:]\n"
                                  "assert arguments[arguments.index('--severity-level')+1] == 'medium'\n"
                                  "assert arguments[arguments.index('--confidence-level')+1] == 'medium'\n"
                                  "pathlib.Path(arguments[arguments.index('--output')+1]).write_text(os.environ['FAKE_REPORT'])\n"
                                  "print('fake scanner log; no Bandit was executed')\n"
                                  "raise SystemExit(int(os.environ['FAKE_EXIT']))\n")
                binary.chmod(0o700)
                environment = {**os.environ, "PATH": str(root) + os.pathsep + os.environ.get("PATH", ""),
                               "RUNNER_TEMP": str(root), "GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1",
                               "GITHUB_ENV": str(root / "github.env"), "FAKE_REPORT": json.dumps(payload), "FAKE_EXIT": str(bandit_exit)}
                result = subprocess.run(["bash", "-e", "-c", scan["run"]], cwd=ROOT, env=environment,
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, expected, result.stderr)
                evidence = root / "d2c-bandit-123-1"
                self.assertEqual(json.loads((evidence / "report.json").read_text()), payload)
                self.assertEqual((evidence / "bandit.exit").read_text().strip(), str(bandit_exit))
                self.assertTrue((evidence / "bandit.log").is_file())
                verdict = json.loads((evidence / "validation.json").read_text())
                self.assertEqual(verdict["outcome"] == "pass", expected == 0)
                self.assertFalse(evidence.is_relative_to(ROOT))
                # A repeat must fail before dispatch and preserve the first run.
                environment["FAKE_REPORT"] = "malformed replacement must never be written"
                repeated = subprocess.run(["bash", "-e", "-c", scan["run"]], cwd=ROOT, env=environment,
                                          capture_output=True, text=True, timeout=10)
                self.assertNotEqual(repeated.returncode, 0)
                self.assertEqual(json.loads((evidence / "report.json").read_text()), payload)


if __name__ == "__main__":
    unittest.main()
