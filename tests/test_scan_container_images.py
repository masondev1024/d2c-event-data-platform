from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
import types
import unittest
from unittest.mock import patch

from scripts.scan_container_images import ImageScanner, ROLES, main, parser, timestamp


IDS = {role: "sha256:" + str(index) * 64 for index, role in enumerate(ROLES, 1)}


class FakeScanner(ImageScanner):
    """Exercise real aggregation/report validation without Docker or Trivy."""
    def __init__(self, options, faults=None):
        super().__init__(options)
        self.faults = faults or {}
        self.calls = []

    def command(self, label, command, *, guarded=False, capture=False):
        self.calls.append((label, command))
        started = time.time()
        log = self.output / (label + ".log")
        log.write_text("")
        result = {"started_at_unix": started, "finished_at_unix": time.time(),
                  "exit_code": 0, "error": None, "log": log.name}
        fault = self.faults.get(label)
        if label.endswith("-inspect"):
            role = label.removesuffix("-inspect")
            result["stdout"] = json.dumps([{"Id": IDS[role], "Os": "linux", "Architecture": "amd64"}])
            return result
        if label == "trivy-version":
            result["stdout"] = json.dumps({"Version": self.options.expected_trivy_version or "0.72.0"})
            return result
        if label.startswith("download-"):
            return result
        role, mode = label.rsplit("-", 1)
        target = Path(command[command.index("--output") + 1])
        if fault == "missing":
            return result
        if mode == "sbom":
            data = {"spdxVersion": "SPDX-2.3", "packages": [{"name": "test"}]}
        else:
            expected_id = self.summary["images"].get(role, {}).get("image_id", IDS[role])
            data = {"Metadata": {"ImageID": expected_id}, "Trivy": {"Version": self.options.expected_trivy_version or "0.72.0"},
                    "CreatedAt": timestamp(), "Results": [{"Target": "debian", "Vulnerabilities": []}]}
            if fault in ("finding", "finding-with-exit0", "fatal"):
                data["Results"][0]["Vulnerabilities"] = [{"VulnerabilityID": "CVE-2026-1234",
                    "Severity": "HIGH", "FixedVersion": "2.0"}]
                result["exit_code"] = 0 if fault == "finding-with-exit0" else 1
            if fault == "wrong-id":
                data["Metadata"]["ImageID"] = IDS["api"]
            if fault == "suppressed":
                data["Results"][0]["ExperimentalModifiedFindings"] = [{"Type": "ignored"}]
            if fault == "stale-content":
                data["CreatedAt"] = "2026-01-01T00:00:00Z"
        target.write_text(json.dumps(data))
        if fault == "malformed":
            target.write_text("[]")
        if fault == "stale":
            os.utime(target, (started - 60, started - 60))
        if fault in ("infrastructure", "fatal"):
            log.write_text("FATAL failed to export image\n")
            result["exit_code"] = 1
        return result


class ContainerImageScanTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.policy = self.root / "policy.yaml"
        self.policy.write_text("vulnerabilities: []\n")
        self.cache = self.root / "cache"
        for relative in ("db/trivy.db", "java-db/trivy-java.db"):
            path = self.cache / relative
            path.parent.mkdir(parents=True)
            path.write_bytes(b"database fixture")
            (path.parent / "metadata.json").write_text(json.dumps({"Version": 1, "UpdatedAt": "2026-11-04T00:00:00Z"}))

    def scanner(self, faults=None, extra=()):
        arguments = ["--output-dir", str(self.root / "result"), "--policy", str(self.policy),
                     "--cache-dir", str(self.cache), "--platform", "linux/amd64"]
        for role in ROLES:
            arguments += ["--image", role + "=" + IDS[role]]
        return FakeScanner(parser().parse_args([*arguments, *extra]), faults)

    def assert_complete_attempts(self, scanner):
        for role in ROLES:
            for mode in ("raw", "gate", "sbom"):
                self.assertTrue((scanner.output / f"{role}-{mode}.status.json").exists())
                self.assertTrue((scanner.output / f"{role}-{mode}.log").exists())
                self.assertIn(f"{role}-{mode}", [name for name, _ in scanner.calls])

    def test_first_security_failure_preserves_all_evidence_and_final_failure(self):
        scanner = self.scanner({"worker-gate": "finding"})
        self.assertEqual(scanner.run(), 1)
        self.assert_complete_attempts(scanner)
        self.assertEqual(scanner.summary["outcome"], "security_fail")
        self.assertEqual(scanner.summary["images"]["worker"]["gate"]["counts"]["HIGH"], 1)
        self.assertEqual(scanner.summary["images"]["lakehouse"]["gate"]["outcome"], "pass")
        for role in ROLES:
            self.assertTrue((scanner.output / f"{role}.sbom.spdx.json").exists())

    def test_first_infrastructure_failure_does_not_skip_later_modes_or_roles(self):
        scanner = self.scanner({"worker-raw": "infrastructure"})
        self.assertEqual(scanner.run(), 1)
        self.assert_complete_attempts(scanner)
        self.assertEqual(scanner.summary["outcome"], "infrastructure_fail")

    def test_invalid_policy_is_not_used_but_every_image_is_still_examined(self):
        self.policy.write_text("vulnerabilities: false\n")
        scanner = self.scanner()
        self.assertEqual(scanner.run(), 1)
        self.assertEqual(scanner.summary["outcome"], "policy_fail")
        self.assert_complete_attempts(scanner)
        for label, command in scanner.calls:
            if label.endswith(("-raw", "-gate")):
                self.assertEqual(command[command.index("--ignorefile") + 1], os.devnull)
                self.assertEqual(command[command.index("--ignore-policy") + 1], "")
                self.assertEqual(command[command.index("--secret-config") + 1], "")
                self.assertEqual(Path(command[command.index("--config") + 1]).read_text(), "{}\n")

    def test_stale_missing_wrong_target_malformed_and_fatal_reports_are_not_zero_findings(self):
        for fault in ("stale", "stale-content", "missing", "wrong-id", "malformed", "fatal", "suppressed", "finding-with-exit0"):
            with self.subTest(fault=fault), TemporaryDirectory(dir=self.root) as directory:
                scanner = self.scanner({"worker-gate": fault}, ("--output-dir", str(Path(directory) / "result")))
                self.assertEqual(scanner.run(), 1)
                self.assertEqual(scanner.summary["images"]["worker"]["gate"]["outcome"], "infrastructure_fail")
                self.assert_complete_attempts(scanner)

    def test_missing_required_image_is_explicit_and_other_images_still_scanned(self):
        scanner = self.scanner()
        del scanner.images["worker"]
        self.assertEqual(scanner.run(), 1)
        self.assertIn("inspection_error", scanner.summary["images"]["worker"])
        self.assertEqual(scanner.summary["images"]["api"]["gate"]["outcome"], "pass")
        self.assertTrue((scanner.output / "worker-raw.status.json").exists())

    def test_missing_database_is_fail_closed_even_if_fake_scans_succeed(self):
        (self.cache / "java-db/trivy-java.db").unlink()
        scanner = self.scanner()
        self.assertEqual(scanner.run(), 1)
        self.assert_complete_attempts(scanner)
        self.assertEqual(scanner.summary["outcome"], "infrastructure_fail")

    def test_existing_evidence_is_never_overwritten(self):
        result = self.root / "result"
        result.mkdir()
        sentinel = result / "worker-gate.json"
        sentinel.write_text("old evidence")
        with self.assertRaises(FileExistsError):
            self.scanner()
        self.assertEqual(sentinel.read_text(), "old evidence")

    def test_all_pass_requires_all_three_modes_and_preserves_immutable_ids(self):
        scanner = self.scanner()
        self.assertEqual(scanner.run(), 0)
        self.assert_complete_attempts(scanner)
        for role in ROLES:
            for mode in ("raw", "gate", "sbom"):
                self.assertEqual(scanner.summary["images"][role][mode]["image_id"], IDS[role])
        self.assertEqual(scanner.summary["outcome"], "pass")

    def test_guard_wraps_scan_with_storage_budget_and_bounded_timeout(self):
        scanner = self.scanner(extra=("--guard-script", str(self.root / "guard.py"),
                                      "--guard-storage-path", "/var/lib/docker"))
        with patch.dict(os.environ, {"TRIVY_SKIP_DIRS": "/", "TRIVY_IGNORE_STATUS": "affected"}), patch("scripts.scan_container_images.subprocess.run", return_value=subprocess.CompletedProcess([], 0, "", "")) as run:
            ImageScanner.command(scanner, "worker-raw", ["trivy", "image", IDS["worker"]], guarded=True)
        arguments = run.call_args.args[0]
        self.assertIn("--kind", arguments)
        self.assertIn("--reserve-free-gib", arguments)
        self.assertIn("/var/lib/docker", arguments)
        self.assertEqual(arguments[-3:], ["trivy", "image", IDS["worker"]])
        self.assertFalse(any(key.startswith("TRIVY_") for key in run.call_args.kwargs["env"]))

    def test_duplicate_role_arguments_are_rejected_before_any_commands(self):
        with self.assertRaises(SystemExit), patch("scripts.scan_container_images.subprocess.run") as run:
            main(["--output-dir", str(self.root / "result"), "--image", "worker=a", "--image", "worker=b"])
        run.assert_not_called()

    def test_guard_abort_is_infrastructure_failure_even_if_exit_is_one(self):
        scanner = self.scanner(extra=("--guard-script", str(self.root / "guard.py")))
        guard = scanner.output / "guard" / "worker-gate"
        guard.mkdir(parents=True)
        (guard / "summary.json").write_text(json.dumps({"outcome": "aborted", "child_exit": 1,
                                                        "abort": "space reserve"}))
        with patch("scripts.scan_container_images.subprocess.run", return_value=subprocess.CompletedProcess([], 1, "", "")):
            result = ImageScanner.command(scanner, "worker-gate", ["trivy", "image", IDS["worker"]], guarded=True)
        self.assertIn("guard did not confirm", result["error"])

    def test_wrong_platform_fails_closed_without_losing_other_status_artifacts(self):
        scanner = self.scanner(extra=("--platform", "linux/arm64"))
        self.assertEqual(scanner.run(), 1)
        for role in ROLES:
            self.assertIn("inspection_error", scanner.summary["images"][role])
            self.assertTrue((scanner.output / f"{role}-raw.status.json").exists())

    def oci_scanner(self):
        original = self.root / "original-oci"
        (original / "blobs").mkdir(parents=True)
        identity = {"root_digest": "sha256:" + "a" * 64,
                    "runtime_leaf_digest": "sha256:" + "b" * 64,
                    "config_digest": IDS["worker"], "expected_trivy_image_id": IDS["worker"],
                    "metadata_sha256": "c" * 64, "platform": "linux/amd64", "layout_path": str(original)}
        module = types.ModuleType("scripts.oci_release")

        def prepare(source, view, platform):
            view.mkdir()
            (view / "index.json").write_text('{"manifests": []}')
            (view / "oci-layout").write_text('{"imageLayoutVersion": "1.0.0"}')
            (view / "blobs").symlink_to(original / "blobs", target_is_directory=True)
            return {**identity, "scan_input_path": str(view)}

        module.prepare_scan_layout = prepare
        module.validate_layout = lambda source, platform: identity.copy()
        options = parser().parse_args(["--output-dir", str(self.root / "result"),
            "--oci-layout", "worker=" + str(original), "--required-role", "worker",
            "--expected-trivy-version", "0.76.1", "--platform", "linux/amd64",
            "--policy", str(self.policy), "--cache-dir", str(self.cache)])
        return FakeScanner(options), module

    def test_verified_oci_leaf_is_scanned_without_guessing_a_docker_image_id(self):
        scanner, module = self.oci_scanner()
        with patch.dict(sys.modules, {"scripts.oci_release": module}):
            self.assertEqual(scanner.run(), 0)
        image = scanner.summary["images"]["worker"]
        self.assertEqual(image["source_kind"], "oci_layout")
        self.assertEqual(image["image_id"], IDS["worker"])
        self.assertNotEqual(image["oci_identity"]["root_digest"], image["image_id"])
        self.assertFalse(any(label.endswith("-inspect") for label, command in scanner.calls))
        for label, command in scanner.calls:
            if label in ("worker-raw", "worker-gate", "worker-sbom"):
                self.assertIn("--input", command)
                self.assertNotIn("--image-src", command)

    def test_changed_oci_scan_view_is_an_infrastructure_failure_and_stops_reading_it(self):
        scanner, module = self.oci_scanner()
        execute = scanner.command

        def changing_command(label, command, **kwargs):
            result = execute(label, command, **kwargs)
            if label == "worker-raw":
                (scanner.output / "worker-scan-view/index.json").write_text("changed view")
            return result

        scanner.command = changing_command
        with patch.dict(sys.modules, {"scripts.oci_release": module}):
            self.assertEqual(scanner.run(), 1)
        self.assertEqual(scanner.summary["images"]["worker"]["raw"]["outcome"], "infrastructure_fail")
        self.assertFalse(any(label == "worker-gate" for label, _ in scanner.calls))
        self.assertTrue((scanner.output / "worker-gate.status.json").exists())

    def test_real_oci_graph_scanner_summary_and_publisher_preflight_agree(self):
        from test_release_security_gate import layout_fixture
        from scripts.oci_release import verify_release
        arguments = ["--output-dir", str(self.root / "result"), "--platform", "linux/amd64",
                     "--expected-trivy-version", "0.76.1", "--policy", str(self.policy),
                     "--cache-dir", str(self.cache)]
        layouts = {}
        for role in ROLES:
            layouts[role] = self.root / "original" / role
            layout_fixture(layouts[role], role=role, attestation_first=True)
            arguments += ["--oci-layout", role + "=" + str(layouts[role])]
        scanner = FakeScanner(parser().parse_args(arguments))
        self.assertEqual(scanner.run(), 0)
        verified = verify_release(scanner.output / "summary.json", layouts, "linux/amd64")
        self.assertEqual(set(verified), set(ROLES))
        for role in ROLES:
            self.assertEqual(verified[role]["config_digest"], scanner.summary["images"][role]["image_id"])

    def test_exhausted_global_budget_never_starts_commands_and_still_writes_all_statuses(self):
        scanner = self.scanner()
        scanner.deadline = time.monotonic() - 1
        with patch("scripts.scan_container_images.subprocess.run") as run:
            result = ImageScanner.command(scanner, "expired", ["trivy", "image", IDS["worker"]])
        run.assert_not_called()
        self.assertIn("not executed", result["error"])
        self.assertEqual(scanner.run(), 1)
        for role in ROLES:
            for mode in ("raw", "gate", "sbom"):
                self.assertTrue((scanner.output / f"{role}-{mode}.status.json").exists())
        self.assertEqual(scanner.summary["outcome"], "infrastructure_fail")


if __name__ == "__main__":
    unittest.main()
