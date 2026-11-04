"""Metadata fixtures and mocked copies; never build, scan, or access a registry."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import gzip
import json
import os
from pathlib import Path
import re
import subprocess
import shutil
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import patch

import yaml

from scripts.oci_release import (
    CONFIG, INDEX, MANIFEST, SPDX, STATEMENT, OCIError, ROLES,
    prepare_scan_layout, publish, validate_layout, verify_release, graph_identity, local_copy,
    LOCAL_COPY_TIMEOUT_SECONDS, REGISTRY_COPY_TIMEOUT_SECONDS,
)

ROOT = Path(__file__).parents[1]
WORKFLOW = ROOT / ".github/workflows/release-images.yml"


def digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def write_blob(root: Path, value, media_type: str, **extra) -> dict:
    data = value if isinstance(value, bytes) else json.dumps(value, sort_keys=True).encode()
    descriptor = {"mediaType": media_type, "digest": digest(data), "size": len(data), **extra}
    (root / "blobs/sha256" / descriptor["digest"][7:]).write_bytes(data)
    return descriptor


def layout_fixture(root: Path, *, role: str = "fixture", wrong_subject: bool = False,
                   omit_provenance: bool = False, extra_runtime: bool = False,
                   attestation_first: bool = False, config_arch: str = "amd64",
                   gzip_layer: bool = False, wrong_diff_id: bool = False) -> dict:
    (root / "blobs/sha256").mkdir(parents=True)
    (root / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}')
    content = b"metadata-only-layer-fixture"
    layer = write_blob(root, gzip.compress(content, mtime=0) if gzip_layer else content,
                       "application/vnd.oci.image.layer.v1.tar" + ("+gzip" if gzip_layer else ""))
    config = write_blob(root, {"architecture": config_arch, "os": "linux", "config": {"Env": ["ROLE=" + role]},
        "rootfs": {"type": "layers", "diff_ids": ["sha256:" + "a" * 64 if wrong_diff_id else digest(content)]}}, CONFIG)
    image = write_blob(root, {"schemaVersion": 2, "mediaType": MANIFEST,
        "config": config, "layers": [layer]}, MANIFEST,
        platform={"os": "linux", "architecture": "amd64"})
    subject = "sha256:" + "a" * 64 if wrong_subject else image["digest"]
    predicates = [SPDX] + ([] if omit_provenance else ["https://slsa.dev/provenance/v0.2"])
    statements = [write_blob(root, {"_type": "https://in-toto.io/Statement/v0.1", "subject": [
        {"name": "fixture", "digest": {"sha256": subject[7:]}}], "predicateType": predicate,
        "predicate": {"fixture": True}}, STATEMENT,
        annotations={"in-toto.io/predicate-type": predicate}) for predicate in predicates]
    dummy = write_blob(root, {}, CONFIG)
    attestation = write_blob(root, {"schemaVersion": 2, "mediaType": MANIFEST,
        "config": dummy, "layers": statements}, MANIFEST,
        platform={"os": "unknown", "architecture": "unknown"},
        annotations={"vnd.docker.reference.type": "attestation-manifest", "vnd.docker.reference.digest": image["digest"]})
    children = [attestation, image] if attestation_first else [image, attestation]
    if extra_runtime:
        children.append(image)
    top = write_blob(root, {"schemaVersion": 2, "mediaType": INDEX, "manifests": children}, INDEX)
    (root / "index.json").write_text(json.dumps({"schemaVersion": 2, "mediaType": INDEX, "manifests": [top]}))
    return {"config": config, "image": image, "attestation": attestation, "top": top, "layer": layer}


def release_fixture(root: Path) -> tuple[Path, dict[str, Path]]:
    scan = root / "scan"
    scan.mkdir()
    layouts = {role: root / "original" / role for role in ROLES}
    summary = {"schema_version": 1, "outcome": "pass", "exit_code": 0, "failures": [],
        "required_roles": list(ROLES), "policy": {"outcome": "pass", "exceptions": 0},
        "tool": {"Version": "0.76.1"}, "images": {},
        "gate_criteria": {"severity": ["HIGH", "CRITICAL"], "scanners": ["vuln", "secret", "misconfig"], "ignore_unfixed": True}}
    for role, original in layouts.items():
        layout_fixture(original, role=role)
        identity = validate_layout(original, "linux/amd64")
        image = {"source_kind": "oci_layout", "oci_identity": identity, "image_id": identity["config_digest"]}
        for mode in ("raw", "gate", "sbom"):
            now = time.time()
            report = ({"spdxVersion": "SPDX-2.3", "packages": [{"name": "fixture"}]} if mode == "sbom" else
                {"CreatedAt": datetime.fromtimestamp(now, timezone.utc).isoformat(),
                 "Metadata": {"ImageID": image["image_id"]}, "Results": [{"Target": "fixture"}]})
            name = role + ".sbom.spdx.json" if mode == "sbom" else role + "-" + mode + ".json"
            data = json.dumps(report).encode()
            (scan / name).write_bytes(data)
            image[mode] = {"outcome": "pass" if mode == "gate" else "completed", "exit_code": 0,
                "image_id": image["image_id"], "report": name, "report_sha256": hashlib.sha256(data).hexdigest(),
                "started_at_unix": now - 1, "finished_at_unix": now + 1}
        summary["images"][role] = image
    path = scan / "summary.json"
    path.write_text(json.dumps(summary))
    return path, layouts


class OCIGraphTest(unittest.TestCase):
    def test_config_leaf_and_attested_root_are_distinct_and_view_selects_runtime(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            descriptors = layout_fixture(root / "original", attestation_first=True)
            identity = prepare_scan_layout(root / "original", root / "view", "linux/amd64")
            self.assertEqual(identity["config_digest"], descriptors["config"]["digest"])
            self.assertEqual(identity["runtime_leaf_digest"], descriptors["image"]["digest"])
            self.assertEqual(identity["root_digest"], descriptors["top"]["digest"])
            self.assertEqual(len({identity[k] for k in ("config_digest", "runtime_leaf_digest", "root_digest")}), 3)
            self.assertNotEqual(identity["root_digest"], digest((root / "original/index.json").read_bytes()))
            self.assertEqual(identity["expected_trivy_image_id"], identity["config_digest"])
            self.assertTrue((root / "view/blobs").samefile(root / "original/blobs"))
            view = json.loads((root / "view/index.json").read_text())
            self.assertEqual(view["manifests"], [descriptors["image"]])
            self.assertEqual(validate_layout(root / "original", "linux/amd64")["metadata_sha256"], identity["metadata_sha256"])

    def test_missing_attestation_wrong_subject_extra_runtime_and_wrong_platform_fail(self):
        cases = ({"omit_provenance": True}, {"wrong_subject": True}, {"extra_runtime": True},
                 {"config_arch": "arm64"}, {"gzip_layer": True, "wrong_diff_id": True})
        for case in cases:
            with self.subTest(case=case), TemporaryDirectory() as temp:
                root = Path(temp)
                layout_fixture(root, **case)
                with self.assertRaises(OCIError):
                    validate_layout(root, "linux/amd64")

    def test_gzip_layer_links_compressed_manifest_blob_to_uncompressed_config_id(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            item = layout_fixture(root, gzip_layer=True)
            identity = validate_layout(root, "linux/amd64")
            self.assertNotEqual(item["layer"]["digest"], identity["rootfs_diff_ids"][0])

    def test_blob_tampering_size_missing_and_symlink_fail_closed(self):
        for corruption in ("content", "size", "missing", "symlink"):
            with self.subTest(corruption=corruption), TemporaryDirectory() as temp:
                root = Path(temp)
                item = layout_fixture(root)
                blob = root / "blobs/sha256" / item["layer"]["digest"][7:]
                if corruption == "content":
                    blob.write_bytes(b"X" * blob.stat().st_size)
                elif corruption == "size":
                    blob.write_bytes(b"short")
                elif corruption == "missing":
                    blob.unlink()
                else:
                    external = root / "external"
                    external.write_bytes(blob.read_bytes())
                    blob.unlink()
                    blob.symlink_to(external)
                with self.assertRaises(OCIError):
                    validate_layout(root, "linux/amd64")

    def test_duplicate_json_keys_and_ambiguous_roots_are_rejected(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            layout_fixture(root)
            (root / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0","imageLayoutVersion":"1.0.0"}')
            with self.assertRaisesRegex(OCIError, "duplicate"):
                validate_layout(root, "linux/amd64")

    def test_scan_view_cannot_overwrite_source_or_an_earlier_view(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            layout_fixture(root / "original")
            with self.assertRaises(OCIError):
                prepare_scan_layout(root / "original", root / "original/view", "linux/amd64")
            prepare_scan_layout(root / "original", root / "view", "linux/amd64")
            with self.assertRaises(FileExistsError):
                prepare_scan_layout(root / "original", root / "view", "linux/amd64")


class OCIPublisherTest(unittest.TestCase):
    def options(self, root, summary, layouts, *, prepare_proof=True):
        options = argparse.Namespace(scan_summary=summary, oci_layout=list(layouts.items()), platform="linux/amd64",
            registry_owner="Owner", tag="v1.2.3", evidence_dir=root / "publication", skopeo="skopeo",
            publish_state_dir=root / "publish-private", expected_skopeo_version="1.13.3",
            authfile=root / "auth.json", guard_script=None, storage_path=[],
            local_copy_dir=root / "local-copies", local_copy_proof=root / "local-copy-proof.json")
        if prepare_proof:
            proof = {"schema_version": 1, "outcome": "local_copy_verified", "skopeo_version": "skopeo version 1.13.3",
                     "scan_summary_sha256": hashlib.sha256(summary.read_bytes()).hexdigest(), "sources": {}, "copies": {}}
            for role, original in layouts.items():
                destination = options.local_copy_dir / role
                shutil.copytree(original, destination)
                identity = graph_identity(validate_layout(original, "linux/amd64"))
                proof["sources"][role] = identity
                proof["copies"][role] = {"destination_path": str(destination.resolve()), "graph": identity}
            options.local_copy_proof.write_text(json.dumps(proof))
        return options

    def test_local_copy_uses_no_registry_or_auth_and_accepts_equivalent_outer_wrapper(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            summary, layouts = release_fixture(root)
            options = self.options(root, summary, layouts, prepare_proof=False)
            options.evidence_dir = root / "local-copy-evidence"
            commands = []
            def copy(command, **kwargs):
                commands.append(command)
                self.assertEqual(kwargs["timeout"], LOCAL_COPY_TIMEOUT_SECONDS)
                self.assertNotIn("--dest-authfile", command)
                self.assertTrue(command[-1].startswith("oci:"))
                source, destination = Path(command[-2][4:]), Path(command[-1][4:])
                shutil.copytree(source, destination, symlinks=False)
                wrapper = json.loads((destination / "index.json").read_text())
                wrapper["annotations"] = {"skopeo-wrapper": "different"}
                (destination / "index.json").write_text(json.dumps(wrapper))
                Path(command[command.index("--digestfile") + 1]).write_text(wrapper["manifests"][0]["digest"])
                return subprocess.CompletedProcess(command, 0)
            with patch("subprocess.check_output", return_value="skopeo version 1.13.3"), patch("subprocess.run", side_effect=copy):
                local_copy(options)
            proof = json.loads((options.evidence_dir / "local-copy.json").read_text())
            self.assertEqual(proof["outcome"], "local_copy_verified")
            self.assertEqual(len(commands), 4)
            self.assertFalse(any("docker://" in part for command in commands for part in command))

    def test_failed_local_copy_proof_prevents_all_registry_operations(self):
        for state in ("failed", "missing"):
            with self.subTest(state=state), TemporaryDirectory() as temp:
                root = Path(temp)
                summary, layouts = release_fixture(root)
                options = self.options(root, summary, layouts)
                if state == "missing":
                    options.local_copy_proof.unlink()
                else:
                    proof = json.loads(options.local_copy_proof.read_text())
                    proof["outcome"] = "failed_local_copy"
                    options.local_copy_proof.write_text(json.dumps(proof))
                with patch("subprocess.run") as copy, patch("subprocess.check_output") as tool, self.assertRaises((OCIError, FileNotFoundError)):
                    publish(options)
                copy.assert_not_called()
                tool.assert_not_called()

    def test_copies_original_attested_graphs_with_all_and_preserve_digests(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            summary, layouts = release_fixture(root)
            commands = []
            def copy(command, **kwargs):
                commands.append(command)
                source = Path(command[-2].removeprefix("oci:"))
                root_digest = json.loads((source / "index.json").read_text())["manifests"][0]["digest"]
                Path(command[command.index("--digestfile") + 1]).write_text(root_digest)
                return subprocess.CompletedProcess(command, 0)
            with patch("subprocess.check_output", return_value="skopeo version 1.13.3\n"), patch("subprocess.run", side_effect=copy):
                publish(self.options(root, summary, layouts))
            self.assertEqual(len(commands), 4)
            for command, original in zip(commands, layouts.values()):
                self.assertIn("--all", command)
                self.assertIn("--preserve-digests", command)
                source = Path(command[-2].removeprefix("oci:"))
                self.assertTrue((source / "blobs").samefile(original / "blobs"))
                self.assertEqual(json.loads((source / "index.json").read_text())["manifests"][0]["digest"],
                                 validate_layout(original, "linux/amd64")["root_digest"])
                self.assertNotIn("docker-daemon:", " ".join(command))
                self.assertFalse(any("build" == argument for argument in command))
            result = json.loads((root / "publication/publication.json").read_text())
            self.assertEqual(result["outcome"], "published_verified_original_graphs")

    def test_any_failed_role_blocks_all_registry_operations(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            path, layouts = release_fixture(root)
            summary = json.loads(path.read_text())
            summary["images"]["lakehouse"]["gate"]["outcome"] = "security_fail"
            path.write_text(json.dumps(summary))
            with patch("subprocess.run") as copy, patch("subprocess.check_output") as version, self.assertRaises(OCIError):
                publish(self.options(root, path, layouts))
            copy.assert_not_called()
            version.assert_not_called()

    def test_changed_report_hash_missing_report_and_stale_createdat_fail_before_copy(self):
        for case in ("changed", "missing", "stale"):
            with self.subTest(case=case), TemporaryDirectory() as temp:
                root = Path(temp)
                path, layouts = release_fixture(root)
                report = path.parent / "lakehouse-gate.json"
                if case == "missing":
                    report.unlink()
                elif case == "changed":
                    report.write_bytes(report.read_bytes() + b" ")
                else:
                    data = json.loads(report.read_text())
                    data["CreatedAt"] = "2000-01-01T00:00:00Z"
                    report.write_text(json.dumps(data))
                    summary = json.loads(path.read_text())
                    summary["images"]["lakehouse"]["gate"]["report_sha256"] = hashlib.sha256(report.read_bytes()).hexdigest()
                    path.write_text(json.dumps(summary))
                with patch("subprocess.run") as copy, self.assertRaises((OCIError, FileNotFoundError)):
                    publish(self.options(root, path, layouts))
                copy.assert_not_called()

    def test_mutated_source_root_or_missing_required_role_blocks_copy(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            path, layouts = release_fixture(root)
            source = layouts["lakehouse"] / "index.json"
            data = json.loads(source.read_text())
            data["annotations"] = {"changed": "after scan"}
            source.write_text(json.dumps(data))
            with self.assertRaisesRegex(OCIError, "changed after scanning"):
                verify_release(path, layouts, "linux/amd64")
            with self.assertRaises(OCIError):
                verify_release(path, {"worker": layouts["worker"]}, "linux/amd64")

    def test_registry_digest_mismatch_is_failure_and_records_partial_publication_scope(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            path, layouts = release_fixture(root)
            def wrong_digest(command, **kwargs):
                Path(command[command.index("--digestfile") + 1]).write_text("sha256:" + "0" * 64)
                return subprocess.CompletedProcess(command, 0)
            with patch("subprocess.check_output", return_value="skopeo version 1.13.3"), patch("subprocess.run", side_effect=wrong_digest), self.assertRaisesRegex(OCIError, "published root digest"):
                publish(self.options(root, path, layouts))
            result = json.loads((root / "publication/publication.json").read_text())
            self.assertIn("failed", result["outcome"])
            self.assertNotEqual(result["outcome"], "published_verified_original_graphs")

    def test_unreviewed_skopeo_version_records_failure_without_registry_copy(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            path, layouts = release_fixture(root)
            with patch("subprocess.check_output", return_value="skopeo version 9.9.9"), patch("subprocess.run") as copy, self.assertRaisesRegex(OCIError, "binary version"):
                publish(self.options(root, path, layouts))
            copy.assert_not_called()
            result = json.loads((root / "publication/publication.json").read_text())
            self.assertEqual(result["copy_attempts"], [])
            self.assertIn("failed", result["outcome"])


class ReleaseWorkflowTest(unittest.TestCase):
    def workflow(self):
        return yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)

    def test_builds_and_scans_original_oci_without_pre_gate_push(self):
        value = self.workflow()
        steps = value["jobs"]["publish"]["steps"]
        by_id = {s["id"]: s for s in steps if "id" in s}
        for role in ROLES:
            build = by_id[role]
            self.assertEqual(build["continue-on-error"], "true")
            for required in ("--provenance=mode=min", "--sbom=true", "type=oci,", "tar=false", "compression=gzip", "--build-state-dir"):
                self.assertIn(required, build["run"])
            self.assertNotIn("--push", build["run"])
            self.assertNotIn("--load", build["run"])
            self.assertIn(f'--oci-layout "{role}=$RELEASE_PRIVATE/{role}"', by_id["scan"]["run"])
        self.assertEqual(by_id["scan"]["if"], "${{ !cancelled() && steps.storage.outcome == 'success' }}")
        self.assertEqual(by_id["evidence"]["if"], "${{ always() }}")
        self.assertNotIn("RELEASE_PRIVATE", by_id["evidence"]["with"]["path"])
        self.assertNotIn("scan/**", by_id["evidence"]["with"]["path"])
        self.assertIn("scripts/oci_release.py", by_id["publish"]["run"])
        self.assertNotIn("docker push", by_id["publish"]["run"])

    def test_runner_paths_are_created_in_a_step_not_unavailable_job_env_context(self):
        job = self.workflow()["jobs"]["publish"]
        self.assertNotIn("runner.", str(job.get("env", {})))
        self.assertNotIn("RELEASE_EVIDENCE", job.get("env", {}))
        self.assertNotIn("RELEASE_PRIVATE", job.get("env", {}))
        prepare = next(s for s in job["steps"] if s["name"] == "Prepare fresh release directories")
        self.assertIn("$RUNNER_TEMP", prepare["run"])
        self.assertIn('>> "$GITHUB_ENV"', prepare["run"])
        self.assertIn('mkdir --parents "$evidence" "$private"', prepare["run"])

    def test_release_uses_the_pinned_trivy_helper_and_preserves_install_evidence(self):
        steps = self.workflow()["jobs"]["publish"]["steps"]
        self.assertFalse(any("aquasecurity/setup-trivy@" in step.get("uses", "") for step in steps))
        installer = next(step for step in steps if step.get("name") == "Set up Trivy")
        self.assertEqual(installer["if"], "${{ !cancelled() && steps.storage.outcome == 'success' }}")
        self.assertIn("python3 scripts/install_trivy.py", installer["run"])
        self.assertIn('--install-dir "$RUNNER_TEMP/d2c-trivy/bin"', installer["run"])
        self.assertIn('--evidence-dir "$RELEASE_EVIDENCE/trivy-install"', installer["run"])
        self.assertIn('--github-path "$GITHUB_PATH"', installer["run"])
        prepare = next(step for step in steps if step.get("name") == "Prepare fresh release directories")
        self.assertLess(steps.index(prepare), steps.index(installer))
        self.assertIn('test -n "${RELEASE_EVIDENCE:-}"', installer["run"])
        upload = next(step for step in steps if step.get("id") == "evidence")
        self.assertEqual(upload["if"], "${{ always() }}")
        self.assertIn("/trivy-install/*.json", upload["with"]["path"])
        self.assertIn("/trivy-install/*.log", upload["with"]["path"])

    def test_every_failed_or_skipped_gate_prevents_registry_login_and_publish(self):
        steps = self.workflow()["jobs"]["publish"]["steps"]
        for step in steps:
            if step.get("id") != "publish" and not step.get("uses", "").startswith("docker/login-action@"):
                continue
            for failed in (*ROLES, "scan", "evidence", "source_verify", "source_job", "local_copy", "local_copy_evidence"):
                for outcome in ("failure", "skipped"):
                    expression = step["if"].removeprefix("${{").removesuffix("}}").replace("success()", "True").replace("&&", "and")
                    expression = expression.replace("github.event_name", repr("push")).replace("github.ref_type", repr("tag"))
                    expression = expression.replace("startsWith(github.ref, 'refs/tags/v')", "True")
                    expression = expression.replace("needs.source_artifacts.result", repr(outcome if failed == "source_job" else "success"))
                    expression = re.sub(r"steps\.([a-z_]+)\.outcome", lambda m: repr(outcome if m[1] == failed else "success"), expression)
                    self.assertFalse(eval("(" + expression + ")", {"__builtins__": {}}, {}))
        self.assertEqual(self.workflow()["permissions"], {"contents": "read", "packages": "write"})

    def test_official_distribution_package_and_binary_version_are_verified(self):
        value = self.workflow()
        self.assertEqual(value["jobs"]["publish"]["runs-on"], "ubuntu-24.04")
        setup = next(s for s in value["jobs"]["publish"]["steps"] if s["name"].startswith("Install and verify the official"))
        for required in ('"skopeo=$SKOPEO_DEB_VERSION"', "dpkg-query", "--version", "--preserve-digests"):
            self.assertIn(required, setup["run"])
        self.assertNotIn("curl", setup["run"])

    def test_job_budget_covers_build_scan_copy_and_always_upload_reserve(self):
        job = self.workflow()["jobs"]["publish"]
        steps = job["steps"]
        by_id = {step["id"]: step for step in steps if "id" in step}
        build_seconds = sum(int(re.search(r"--timeout-seconds\s+(\d+)", by_id[role]["run"])[1])
                            for role in ROLES)
        scan_seconds = int(re.search(r"--total-timeout\s+(\d+)", by_id["scan"]["run"])[1])
        self.assertEqual(scan_seconds, 5400)
        # Local compatibility and registry copies have separate bounded budgets.
        required_seconds = build_seconds + scan_seconds + len(ROLES) * (LOCAL_COPY_TIMEOUT_SECONDS + REGISTRY_COPY_TIMEOUT_SECONDS) + 30 * 60
        self.assertGreaterEqual(int(job["timeout-minutes"]) * 60, required_seconds)
        self.assertLessEqual(int(job["timeout-minutes"]), 360)
        uploads = [step for step in steps if step.get("uses", "").startswith("actions/upload-artifact@")]
        self.assertEqual(len(uploads), 4)
        self.assertTrue(all(step["if"] == "${{ always() }}" for step in uploads))
        publish_index = steps.index(by_id["publish"])
        self.assertTrue(any(steps.index(step) > publish_index for step in uploads))

    def test_artifact_retention_is_explicit_and_large_payload_expires_after_one_day(self):
        for job in self.workflow()["jobs"].values():
            for step in job["steps"]:
                if not step.get("uses", "").startswith("actions/upload-artifact@"):
                    continue
                expected = "1" if step.get("name") == "Share only the sealed source artifact tree" else "14"
                self.assertEqual(step["with"]["retention-days"], expected)

    def test_source_components_compile_once_and_transferred_artifact_name_is_exact(self):
        value = self.workflow()
        source, job = value["jobs"]["source_artifacts"], value["jobs"]["publish"]
        self.assertEqual(source["runs-on"], "ubuntu-24.04")
        self.assertGreaterEqual(int(source["timeout-minutes"]), 5 * 60 + 60)
        source_build = next(step for step in source["steps"] if step.get("id") == "source")
        for argument in ("lakehouse_artifacts.py build", "--native-platform linux/amd64", "--discard-created-builder-cache",
                         '--artifacts "$RUNNER_TEMP/d2c-artifacts"', '--storage-path "$RUNNER_TEMP"'):
            self.assertIn(argument, source_build["run"])
        self.assertEqual(sum("lakehouse_artifacts.py build" in step.get("run", "")
                             for candidate in value["jobs"].values() for step in candidate["steps"]), 1)
        self.assertEqual(source["outputs"]["artifact_name"], "${{ steps.source.outputs.artifact_name }}")
        self.assertEqual(job["needs"], ["source_artifacts"])
        self.assertEqual(job["if"], "${{ always() }}")
        by_id = {step["id"]: step for step in job["steps"] if "id" in step}
        self.assertEqual(by_id["source_download"]["with"]["name"], "${{ needs.source_artifacts.outputs.artifact_name }}")
        self.assertIn("lakehouse_artifacts.py verify", by_id["source_verify"]["run"])
        self.assertIn("steps.source_verify.outcome == 'success'", by_id["lakehouse"]["if"])
        self.assertIn('--build-context "d2c_artifacts=$RUNNER_TEMP/d2c-artifacts"', by_id["lakehouse"]["run"])
        self.assertIn("--build-arg D2C_ARTIFACT_STAGE=d2c_artifacts", by_id["lakehouse"]["run"])
        for role in ("worker", "api", "catalog"):
            self.assertNotIn("source_artifacts", by_id[role]["if"])
        aggregate = next(step for step in job["steps"] if step["name"] == "Require successful transferred source artifacts")
        self.assertEqual(aggregate["if"], "${{ always() }}")
        self.assertIn("exit 1", aggregate["run"])

    def test_source_receipts_exclude_original_engine_inventory_and_private_caches(self):
        source = self.workflow()["jobs"]["source_artifacts"]
        receipt = next(step for step in source["steps"] if step.get("name") == "Prepare bounded source receipt without engine inventory")
        upload = next(step for step in source["steps"] if step.get("name") == "Preserve bounded source receipts after success or failure")
        self.assertEqual(receipt["if"], "${{ always() }}")
        self.assertEqual(upload["if"], "${{ always() }}")
        for forbidden in ("engine-before.json", "builder.json", "buildx", "export/", "**"):
            self.assertNotIn(forbidden, upload["with"]["path"])
        with TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "d2c-source-build").mkdir()
            (root / "d2c-source-build/state.json").write_text(json.dumps({"before": {"images": ["private-image"],
                "containers": ["private-container"], "volumes": ["private-volume"]},
                "engine_endpoint_candidates": ["private-endpoint"],
                "phases": [{"engine_endpoint_candidates": ["private-endpoint"]}]}))
            code = "\n".join(receipt["run"].splitlines()[1:-1])
            with patch.dict(os.environ, {"RUNNER_TEMP": temp}):
                exec(compile(code, str(WORKFLOW), "exec"), {})
            saved = (root / "d2c-source-receipts/state.json").read_text()
            for private in ("private-image", "private-container", "private-volume", "private-endpoint"):
                self.assertNotIn(private, saved)
            self.assertEqual(json.loads(saved)["pre_existing_inventory_receipt"]["counts"]["images"], 1)

    def test_manual_dispatch_can_validate_but_cannot_enter_registry_steps(self):
        value = self.workflow()
        self.assertEqual(value["on"]["workflow_dispatch"]["inputs"]["allow_ephemeral_sdk_cleanup"]["default"], "false")
        for step in value["jobs"]["publish"]["steps"]:
            if step.get("id") != "publish" and not step.get("uses", "").startswith("docker/login-action@"):
                continue
            expression = step["if"].removeprefix("${{").removesuffix("}}").replace("success()", "True").replace("&&", "and")
            expression = expression.replace("github.event_name", repr("workflow_dispatch")).replace("github.ref_type", repr("branch"))
            expression = expression.replace("startsWith(github.ref, 'refs/tags/v')", "False")
            expression = expression.replace("needs.source_artifacts.result", repr("success"))
            expression = re.sub(r"steps\.([a-z_]+)\.outcome", lambda _: repr("success"), expression)
            self.assertFalse(eval("(" + expression + ")", {"__builtins__": {}}, {}))


if __name__ == "__main__":
    unittest.main()
