"""Static and host-mocked checks for native, shared-source security CI wiring."""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import yaml


ROOT = Path(__file__).resolve().parents[1]
KAFKA = "apache/kafka:3.7.0@sha256:41c20d65b0a2180b9deca056d47540771752e69fe9225d5748dd65a25a9675d7"
PHASES = ("initial", "resume", "crash", "recover")


def expression(value, **outcomes):
    source = value.strip().removeprefix("${{").removesuffix("}}")
    source = source.replace("!cancelled()", "True").replace("success()", "False").replace("&&", "and")
    source = re.sub(r"steps\.([a-z_-]+)\.outcome", lambda match: repr(outcomes.get(match[1], "success")), source)
    return bool(eval("(" + source + ")", {"__builtins__": {}}, {}))


def heredoc(step):
    return re.search(r"python3 - <<'PYTHON'\n(.*?)\nPYTHON(?:\n|$)", step["run"], re.DOTALL)[1]


class SecurityRuntimeWorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.value = yaml.load((ROOT / ".github/workflows/security.yml").read_text(), Loader=yaml.BaseLoader)
        cls.jobs = cls.value["jobs"]
        cls.runtime = cls.jobs["lakehouse-scan"]
        cls.steps = {step["id"]: step for step in cls.runtime["steps"] if "id" in step}

    def test_one_native_source_compile_is_shared_by_two_native_final_runtimes(self):
        source = self.jobs["lakehouse-source"]
        self.assertEqual(source["runs-on"], "ubuntu-24.04")
        commands = [step["run"] for job in self.jobs.values() for step in job["steps"]
                    if "scripts/lakehouse_artifacts.py build" in step.get("run", "")]
        self.assertEqual(len(commands), 1)
        for required in ("python3 -B scripts/lakehouse_artifacts.py build --source-root .",
                         '--artifacts "$RUNNER_TEMP/d2c-artifacts"', '--work-dir "$RUNNER_TEMP/d2c-source-build"',
                         '--storage-path "$RUNNER_TEMP"', "--native-platform linux/amd64", "--discard-created-builder-cache"):
            self.assertIn(required, commands[0])
        self.assertGreaterEqual(int(source["timeout-minutes"]), 330)
        self.assertEqual(self.runtime["needs"], ["lakehouse-source"])
        self.assertEqual(self.runtime["runs-on"], "${{ matrix.runner }}")
        self.assertEqual({item["arch"]: item["runner"] for item in self.runtime["strategy"]["matrix"]["include"]},
                         {"amd64": "ubuntu-24.04", "arm64": "ubuntu-24.04-arm"})
        self.assertFalse(any("setup-qemu" in step.get("uses", "") for job in self.jobs.values() for step in job["steps"]))
        self.assertNotIn("needs", self.jobs["image-scan"])

    def test_download_identity_and_verification_precede_final_build(self):
        steps = self.runtime["steps"]
        download = next(step for step in steps if step.get("uses", "").startswith("actions/download-artifact@"))
        verify = next(step for step in steps if "lakehouse_artifacts.py verify" in step.get("run", ""))
        self.assertEqual(download["with"]["name"], "${{ needs.lakehouse-source.outputs.artifact_name }}")
        self.assertEqual(self.jobs["lakehouse-source"]["outputs"]["artifact_name"], "${{ steps.source.outputs.artifact_name }}")
        self.assertLess(steps.index(download), steps.index(verify))
        self.assertLess(steps.index(verify), steps.index(self.steps["build"]))
        self.assertIn('--artifacts "$RUNNER_TEMP/d2c-artifacts" --source-root .', verify["run"])
        build = self.steps["build"]["run"]
        for required in ("--kind build --minimum-free-gib 20 --reserve-free-gib 6", "--timeout-seconds 1800",
                         '--platform "linux/$ARCH"', '--build-context "d2c_artifacts=$RUNNER_TEMP/d2c-artifacts"',
                         "--build-arg D2C_ARTIFACT_STAGE=d2c_artifacts", '--iidfile "$RUNNER_TEMP/lakehouse.iid"'):
            self.assertIn(required, build)
        self.assertNotIn("lakehouse_artifacts.py build", build)

    def test_source_evidence_is_bounded_and_sealed_payload_is_separate(self):
        steps = self.jobs["lakehouse-source"]["steps"]
        receipts = next(step for step in steps if step.get("name", "").startswith("Preserve bounded source"))
        self.assertEqual(receipts["if"], "always()")
        for required in ("/d2c-source-receipts/*.json", "/guard/*.json", "/guard/*.log", "/builder-state/discard-plan.json",
                         "/d2c-artifacts/manifest.json", "/d2c-artifacts/*/build-status.json"):
            self.assertIn(required, receipts["with"]["path"])
        for forbidden in ("/**", "/export", "/builder.json", "/buildx", "/blobs",
                          "/d2c-source-build/state.json", "/d2c-source-build/engine-before.json"):
            self.assertNotIn(forbidden, receipts["with"]["path"])
        payload = next(step for step in steps if step.get("name") == "Share only the sealed source artifact tree")
        self.assertEqual(payload["with"]["path"], "${{ runner.temp }}/d2c-artifacts")
        self.assertEqual(payload["with"]["include-hidden-files"], "true")
        self.assertNotIn("if", payload)  # A failed compile must not publish a partial payload.

    def test_receipt_copy_redacts_inventory_and_endpoints_without_mutating_originals(self):
        step = next(step for step in self.jobs["lakehouse-source"]["steps"]
                    if step.get("name", "").startswith("Redact source receipt"))
        self.assertEqual(step["if"], "always()")
        code = compile(heredoc(step), "source-receipt-redaction", "exec")
        before = {"images": ["private-image"], "containers": ["private-container"], "volumes": ["private-volume"]}
        state = {"before": before, "engine_endpoint_candidates": ["unix:///private/socket"],
                 "phases": [{"role": "hadoop", "engine_endpoint_candidates": ["private-context"]}]}
        with TemporaryDirectory() as scratch:
            root = Path(scratch)
            (root / "d2c-source-build").mkdir()
            (root / "d2c-artifacts").mkdir()
            originals = {root / "d2c-source-build/state.json": json.dumps(state),
                         root / "d2c-source-build/engine-before.json": json.dumps(before),
                         root / "d2c-artifacts/manifest.json": "original-sealed-artifact"}
            for path, content in originals.items(): path.write_text(content)
            with patch.dict(os.environ, {"RUNNER_TEMP": str(root)}): exec(code, {})
            receipt = (root / "d2c-source-receipts/state.json").read_text()
            for private in ("private-image", "private-container", "private-volume", "private-context", "private/socket"):
                self.assertNotIn(private, receipt)
            self.assertEqual(json.loads(receipt)["before"]["counts"], {"images": 1, "containers": 1, "volumes": 1})
            self.assertRegex(json.loads(receipt)["before"]["sha256"], r"^[a-f0-9]{64}$")
            for path, content in originals.items(): self.assertEqual(path.read_text(), content)

    def test_new_heavy_jobs_keep_admission_and_explicit_ephemeral_runner_provisioning(self):
        for name in ("lakehouse-source", "lakehouse-scan"):
            steps = self.jobs[name]["steps"]
            self.assertEqual(steps[1]["id"], "storage")
            self.assertIn("--minimum-free-gib 32", steps[1]["run"])
            self.assertEqual(steps[1]["env"]["ALLOW_EPHEMERAL_SDK_CLEANUP"],
                             "${{ github.event_name != 'workflow_dispatch' || inputs.allow_ephemeral_sdk_cleanup }}")
            self.assertEqual(steps[2]["if"], "always()")
            self.assertEqual(steps[2]["with"]["if-no-files-found"], "error")
        self.assertEqual(self.value["permissions"], {"contents": "read"})

    def test_failed_security_scan_does_not_skip_real_kafka_but_failed_build_does(self):
        for name in ("broker", "streaming"):
            condition = self.steps[name]["if"]
            self.assertTrue(expression(condition, scan="failure"))
            for outcome in ("failure", "skipped", "cancelled"):
                self.assertFalse(expression(condition, build=outcome))
                self.assertFalse(expression(condition, storage=outcome))
        self.assertFalse(expression(self.steps["streaming"]["if"], broker="failure"))
        self.assertEqual(self.runtime["env"]["KAFKA_INDEX"], KAFKA)
        pull = self.steps["broker"]["run"]
        for required in ("scripts/container_security_guard.py", "--timeout-seconds 1200",
                         'docker pull --platform "linux/$ARCH" "$KAFKA_INDEX"'):
            self.assertIn(required, pull)
        harness = self.steps["streaming"]["run"]
        for required in ("scripts/test_lakehouse_streaming.py", '--image "$image" --broker-image "$broker"',
                         '--platform "linux/$ARCH"', "--phase-timeout-seconds 300 --total-timeout-seconds 1200",
                         "--guard-script scripts/container_security_guard.py"):
            self.assertIn(required, harness)
        # 30m build + 75m scan + 20m pull + 20m harness + 6m Java checks + 15m setup/upload.
        self.assertGreaterEqual(int(self.runtime["timeout-minutes"]), 166)

    def test_six_non_lakehouse_native_images_have_owned_python_probe_after_the_scan(self):
        job = self.jobs["image-scan"]
        probe = next(step for step in job["steps"] if step.get("id") == "python-runtime")
        scan = next(step for step in job["steps"] if "scripts/scan_container_images.py" in step.get("run", ""))
        self.assertLess(job["steps"].index(scan), job["steps"].index(probe))
        self.assertTrue(expression(probe["if"], scan="failure"))
        self.assertFalse(expression(probe["if"], build="failure"))
        self.assertFalse(expression(probe["if"], storage="failure"))
        for required in ("--kind test --minimum-free-gib 8 --reserve-free-gib 6 --timeout-seconds 90",
                         '--container-name "$name" --owner-token "$owner"',
                         'docker run --rm -i --network none --platform "linux/$ARCH"',
                         '--name "$name" --label "d2c.security.owner=$owner"',
                         '--entrypoint python "$image" - "$ROLE" --timeout-seconds 45',
                         "< scripts/check_python_runtime.py"):
            self.assertIn(required, probe["run"])
        self.assertNotIn("--publish", probe["run"])
        self.assertNotIn("--volume", probe["run"])
        upload = next(step for step in job["steps"] if "container-security/" in step.get("with", {}).get("path", ""))
        self.assertEqual(upload["if"], "always()")
        self.assertIn("d2c-python-runtime-${{ matrix.role }}-${{ matrix.arch }}", upload["with"]["path"])

    def test_python_probe_validator_rejects_wrong_identity_failed_or_ambiguous_proofs(self):
        step = next(step for step in self.jobs["image-scan"]["steps"] if step.get("id") == "python-runtime")
        code = compile(heredoc(step), "python-runtime-workflow-proof", "exec")
        guard = {"outcome": "completed", "child_exit": 0, "exit": 0, "abort": None, "cleanup_errors": []}
        for role in ("worker", "api", "catalog"):
            for corruption in (None, "chainguard", "other-os", "status", "machine", "duplicate", "cleanup"):
                with self.subTest(role=role, corruption=corruption), TemporaryDirectory() as scratch:
                    root = Path(scratch)
                    evidence = root / ("d2c-python-runtime-" + role + "-amd64")
                    evidence.mkdir()
                    (root / (role + ".iid")).write_text("sha256:" + "1" * 64)
                    proof = {"schema_version": 1, "status": "PASS", "role": role, "uid": 10001,
                             "python": "3.12.15", "python_implementation": "cpython", "virtual_environment": "/opt/venv",
                             "os_id": "wolfi", "os_release": {"ID": "wolfi", "NAME": "unmodified release identity"},
                             "apk_versions": {"python-3.12": "3.12.15-r0"},
                             "machine": "x86_64", "openssl": "OpenSSL fixture", "elapsed_seconds": 1,
                             "checks": {"native_fixture": True, "normal_gunicorn": {"owned_child_reaped": True},
                                        "normal_catalog_server": {"owned_child_reaped": True}}, "limits": "local ABI/startup only"}
                    state = copy.deepcopy(guard)
                    if corruption in ("chainguard", "other-os"):
                        proof["os_id"] = "chainguard" if corruption == "chainguard" else "debian"
                        proof["os_release"]["ID"] = proof["os_id"]
                    if corruption == "status": proof["status"] = "FAIL"
                    if corruption == "machine": proof["machine"] = "aarch64"
                    if corruption == "cleanup": state["cleanup_errors"] = ["unconfirmed"]
                    line = "D2C_PYTHON_RUNTIME_PROOF=" + json.dumps(proof) + "\n"
                    (evidence / "command.log").write_text(line * (2 if corruption == "duplicate" else 1))
                    (evidence / "summary.json").write_text(json.dumps(state))
                    with patch.dict(os.environ, {"RUNNER_TEMP": str(root), "ROLE": role, "ARCH": "amd64"}):
                        if corruption not in (None, "chainguard"):
                            with self.assertRaises(SystemExit): exec(code, {})
                            self.assertFalse((evidence / "runtime-proof.json").exists())
                        else:
                            exec(code, {})
                            actual = json.loads((evidence / "runtime-proof.json").read_text())
                            self.assertEqual(actual["image_id"], "sha256:" + "1" * 64)
                            self.assertEqual(actual["os_release"], proof["os_release"])
                            self.assertEqual(actual["apk_versions"], proof["apk_versions"])

    def test_broker_inspection_uses_actual_id_and_platform_without_leaking_environment(self):
        code = compile(heredoc(self.steps["broker"]), "broker-workflow-proof", "exec")
        valid = {"Id": "sha256:" + "1" * 64, "Os": "linux", "Architecture": "amd64",
                 "RepoDigests": [KAFKA.split(":3.7.0")[0] + "@" + KAFKA.rsplit("@", 1)[1]],
                 "Config": {"Env": ["SECRET=never-copy-this"]}}
        for corruption in (None, "platform-leaf", "architecture", "id", "index"):
            with self.subTest(corruption=corruption), TemporaryDirectory() as scratch:
                root = Path(scratch)
                output = root / "d2c-lakehouse-runtime-amd64"
                output.mkdir()
                image = copy.deepcopy(valid)
                if corruption == "platform-leaf": image["RepoDigests"] = ["apache/kafka@sha256:" + "a" * 64]
                if corruption == "architecture": image["Architecture"] = "arm64"
                if corruption == "id": image["Id"] = "mutable-tag"
                if corruption == "index": image["RepoDigests"] = []
                with patch.dict(os.environ, {"RUNNER_TEMP": str(root), "ARCH": "amd64", "KAFKA_INDEX": KAFKA,
                                            "KAFKA_EXPECTED_LEAF": "sha256:" + "a" * 64}), \
                        patch("subprocess.check_output", return_value=json.dumps([image])) as inspect:
                    if corruption in ("architecture", "id", "index"):
                        with self.assertRaises(SystemExit): exec(code, {})
                        self.assertFalse((output / "broker.iid").exists())
                    else:
                        exec(code, {})
                        self.assertEqual((output / "broker.iid").read_text().strip(), valid["Id"])
                        metadata = (output / "broker-image.json").read_text()
                        self.assertNotIn("SECRET", metadata)
                        self.assertNotIn("Config", json.loads(metadata))
                    inspect.assert_called_once_with(["docker", "image", "inspect", KAFKA], text=True, timeout=30)

    def test_streaming_summary_requires_four_phases_cleanup_and_both_actual_ids(self):
        code = compile(heredoc(self.steps["streaming"]), "streaming-workflow-proof", "exec")
        valid = {"outcome": "PASS", "failures": [], "cleanup": {"errors": []}, "platform": "linux/amd64",
                 "phases": {phase: {"status": "PASS"} for phase in PHASES},
                 "images": {"lakehouse": {"Id": "sha256:" + "1" * 64}, "broker": {"Id": "sha256:" + "2" * 64}}}
        for corruption in (None, "phase", "cleanup", "identity", "outcome"):
            with self.subTest(corruption=corruption), TemporaryDirectory() as scratch:
                root = Path(scratch)
                runtime = root / "d2c-lakehouse-runtime-amd64"
                (runtime / "streaming").mkdir(parents=True)
                (root / "lakehouse.iid").write_text(valid["images"]["lakehouse"]["Id"])
                (runtime / "broker.iid").write_text(valid["images"]["broker"]["Id"])
                proof = copy.deepcopy(valid)
                if corruption == "phase": proof["phases"].pop("recover")
                if corruption == "cleanup": proof["cleanup"]["errors"] = ["uncertain"]
                if corruption == "identity": proof["images"]["lakehouse"]["Id"] = "sha256:" + "3" * 64
                if corruption == "outcome": proof["outcome"] = "FAIL"
                (runtime / "streaming/summary.json").write_text(json.dumps(proof))
                with patch.dict(os.environ, {"RUNNER_TEMP": str(root), "ARCH": "amd64"}):
                    if corruption:
                        with self.assertRaises(SystemExit): exec(code, {})
                    else:
                        exec(code, {})

    def test_hadoop_final_image_probe_is_guarded_offline_and_does_not_skip_kafka_on_failure(self):
        step = self.steps["hadoop-local"]
        steps = self.runtime["steps"]
        self.assertLess(steps.index(self.steps["build"]), steps.index(step))
        self.assertLess(steps.index(step), steps.index(self.steps["broker"]))
        self.assertNotIn("continue-on-error", step)
        self.assertTrue(expression(step["if"], scan="failure"))
        self.assertFalse(expression(step["if"], build="failure"))
        self.assertFalse(expression(step["if"], storage="failure"))
        for name in ("broker", "streaming"):
            self.assertTrue(expression(self.steps[name]["if"], **{"hadoop-local": "failure"}))
        for required in ("--kind test --minimum-free-gib 8 --reserve-free-gib 6 --timeout-seconds 180",
                         '--container-name "$name" --owner-token "$owner"',
                         'docker run --rm --name "$name" --label "d2c.security.owner=$owner"',
                         '--platform "linux/$ARCH" --network none --memory 1g --cpus 2 --read-only',
                         '--tmpfs /tmp:rw,nosuid,nodev,size=128m "$actualIID"',
                         "java --class-path '/opt/spark/jars/*'",
                         "/usr/share/d2c-security/recipe-source/container-build/lakehouse/hadoop/VerifyHadoopLocalProfile.java ."):
            self.assertIn(required, step["run"])
        self.assertNotIn("--volume", step["run"])
        self.assertNotIn("--publish", step["run"])
        upload = next(item for item in steps if "container-security/" in item.get("with", {}).get("path", ""))
        self.assertEqual(upload["if"], "always()")
        self.assertIn("d2c-lakehouse-runtime-${{ matrix.arch }}", upload["with"]["path"])

    def test_hadoop_success_requires_exact_marker_actual_id_and_complete_guard(self):
        code = compile(heredoc(self.steps["hadoop-local"]), "hadoop-workflow-proof", "exec")
        marker = "hadoop_local_profile_boot=PASS;jackson=2.21.7;remote_operations=NONE"
        valid = {"outcome": "completed", "child_exit": 0, "exit": 0, "abort": None, "cleanup_errors": []}
        for corruption in (None, "missing", "substring", "wrong-jackson", "duplicate", "child-exit", "exit",
                           "cleanup", "abort", "missing-abort", "boolean-exit", "mutable-id"):
            with self.subTest(corruption=corruption), TemporaryDirectory() as scratch:
                root = Path(scratch)
                evidence = root / "d2c-lakehouse-runtime-amd64/hadoop-local"
                evidence.mkdir(parents=True)
                (root / "lakehouse.iid").write_text("tag:latest" if corruption == "mutable-id" else "sha256:" + "1" * 64)
                guard = copy.deepcopy(valid)
                text = "ordinary bootstrap log\n" + marker + "\n"
                if corruption == "missing": text = "bootstrap exited without a proof\n"
                if corruption == "substring": text = "prefix:" + marker + "\n"
                if corruption == "wrong-jackson": text = marker.replace("2.21.7", "2.21.6") + "\n"
                if corruption == "duplicate": text = marker + "\n" + marker + "\n"
                if corruption == "child-exit": guard["child_exit"] = 1
                if corruption == "exit": guard["exit"] = 70
                if corruption == "cleanup": guard["cleanup_errors"] = ["unknown"]
                if corruption == "abort": guard["abort"] = "storage reserve reached"
                if corruption == "missing-abort": guard.pop("abort")
                if corruption == "boolean-exit": guard["exit"] = False
                (evidence / "summary.json").write_text(json.dumps(guard))
                (evidence / "command.log").write_text(text)
                with patch.dict(os.environ, {"RUNNER_TEMP": str(root), "ARCH": "amd64"}):
                    if corruption:
                        with self.assertRaises(SystemExit): exec(code, {})
                        self.assertFalse((evidence / "runtime-proof.json").exists())
                    else:
                        exec(code, {})
                        proof = json.loads((evidence / "runtime-proof.json").read_text())
                        self.assertEqual(proof["image_id"], "sha256:" + "1" * 64)
                        self.assertEqual(proof["marker"], marker)
                        self.assertEqual(proof["remote_operations"], "NONE")

    def test_spark_protobuf_probe_is_a_separate_guarded_final_image_check(self):
        step = self.steps["spark-protobuf"]
        steps = self.runtime["steps"]
        self.assertLess(steps.index(self.steps["hadoop-local"]), steps.index(step))
        self.assertLess(steps.index(step), steps.index(self.steps["broker"]))
        self.assertNotIn("continue-on-error", step)
        self.assertTrue(expression(step["if"], scan="failure", **{"hadoop-local": "failure"}))
        for outcome in ("failure", "skipped", "cancelled"):
            self.assertFalse(expression(step["if"], build=outcome))
            self.assertFalse(expression(step["if"], storage=outcome))
        for name in ("broker", "streaming"):
            self.assertTrue(expression(self.steps[name]["if"], **{"spark-protobuf": "failure"}))
        for required in ('actualIID="$(cat "$RUNNER_TEMP/lakehouse.iid")"',
                         '[[ "$actualIID" =~ ^sha256:[0-9a-f]{64}$ ]]',
                         "--kind test --minimum-free-gib 8 --reserve-free-gib 6 --timeout-seconds 180",
                         '--container-name "$name" --owner-token "$owner"',
                         'docker run --rm --pull never --name "$name" --label "d2c.security.owner=$owner"',
                         '--platform "linux/$ARCH" --network none --memory 1g --cpus 2 --read-only',
                         '--tmpfs /tmp:rw,nosuid,nodev,size=128m --entrypoint /bin/sh "$actualIID"',
                         "javac --release 17 -cp /opt/spark/jars/spark-core_2.13-4.2.1.jar",
                         "/usr/share/d2c-security/recipe-source/container-build/lakehouse/spark/VerifySparkProtobuf.java",
                         "VerifySparkProtobuf 21 /opt/spark/jars/spark-core_2.13-4.2.1.jar"):
            self.assertIn(required, step["run"])
        self.assertNotIn("--volume", step["run"])
        self.assertNotIn("--publish", step["run"])
        upload = next(item for item in steps if "container-security/" in item.get("with", {}).get("path", ""))
        self.assertEqual(upload["if"], "always()")
        self.assertIn("d2c-lakehouse-runtime-${{ matrix.arch }}", upload["with"]["path"])

    def test_spark_proof_requires_runtime21_rebuilt_jar_all_cases_guard_cleanup_and_same_iid(self):
        code = compile(heredoc(self.steps["spark-protobuf"]), "spark-protobuf-workflow-proof", "exec")
        expected_jar = "/opt/spark/jars/spark-core_2.13-4.2.1.jar"
        classes = ("StoreTypes", "StoreTypes$SparkPlanGraphNodeWrapper", "StoreTypes$SparkPlanGraphNodeWrapper$Builder",
                   "StoreTypes$SparkPlanGraphNodeWrapper$WrapperCase", "StoreTypes$SparkPlanGraphNode",
                   "StoreTypes$SparkPlanGraphClusterWrapper")
        source = b"fixture representing a verified rebuilt core artifact"
        source_hash = hashlib.sha256(source).hexdigest()
        valid = {"schema_version": 1, "status": "passed", "actual_java_major": 21, "expected_java_major": 21,
                 "java_runtime_version": "21.0.13+fixture", "java_vendor": "fixture vendor",
                 "expected_core_jar": expected_jar, "actual_core_jar": expected_jar, "core_jar_sha256": source_hash,
                 "class_code_sources": {"org.apache.spark.status.protobuf." + name: expected_jar for name in classes},
                 "cases": ["NODE", "CLUSTER", "WRAPPER_NOT_SET"], "merge_cases_verified": 3,
                 "wire_roundtrips_verified": 3, "unset_merge_preserves_existing": True}
        guard_ok = {"outcome": "completed", "child_exit": 0, "exit": 0, "abort": None, "cleanup_errors": []}
        prefix = "D2C_SPARK_PROTOBUF_PROOF="
        corruptions = (None, "java17", "expected17", "schema-bool", "runtime-missing", "vendor-missing", "status",
                       "core-hash", "core-origin", "foreign-class", "missing-class", "missing-case", "wrong-unset-case",
                       "merge-count", "wire-count", "unset-merge", "missing-marker", "duplicate", "child-exit",
                       "boolean-exit", "cleanup", "abort", "missing-abort", "mutable-id", "source-changed")
        for arch in ("amd64", "arm64"):
            for corruption in corruptions:
                with self.subTest(arch=arch, corruption=corruption), TemporaryDirectory() as scratch:
                    root = Path(scratch)
                    evidence = root / ("d2c-lakehouse-runtime-" + arch) / "spark-protobuf"
                    evidence.mkdir(parents=True)
                    source_jar = root / "d2c-artifacts/spark/spark-core_2.13-4.2.1-d2c-jetty12.1.14.jar"
                    source_jar.parent.mkdir(parents=True)
                    source_jar.write_bytes(source + b"changed" if corruption == "source-changed" else source)
                    (root / "lakehouse.iid").write_text("tag:latest" if corruption == "mutable-id" else "sha256:" + "1" * 64)
                    proof, guard = copy.deepcopy(valid), copy.deepcopy(guard_ok)
                    if corruption == "java17": proof["actual_java_major"] = 17
                    if corruption == "expected17": proof["expected_java_major"] = 17
                    if corruption == "schema-bool": proof["schema_version"] = True
                    if corruption == "runtime-missing": proof["java_runtime_version"] = ""
                    if corruption == "vendor-missing": proof["java_vendor"] = ""
                    if corruption == "status": proof["status"] = "PASS"
                    if corruption == "core-hash": proof["core_jar_sha256"] = "0" * 64
                    if corruption == "core-origin": proof["actual_core_jar"] = "/vendor/original-spark-core.jar"
                    if corruption == "foreign-class": proof["class_code_sources"]["org.apache.spark.status.protobuf.StoreTypes"] = "/vendor/original-spark-core.jar"
                    if corruption == "missing-class": proof["class_code_sources"].pop("org.apache.spark.status.protobuf.StoreTypes$SparkPlanGraphNodeWrapper$WrapperCase")
                    if corruption == "missing-case": proof["cases"].remove("CLUSTER")
                    if corruption == "wrong-unset-case": proof["cases"][-1] = "NOT_SET"
                    if corruption == "merge-count": proof["merge_cases_verified"] = 2
                    if corruption == "wire-count": proof["wire_roundtrips_verified"] = 2
                    if corruption == "unset-merge": proof["unset_merge_preserves_existing"] = False
                    if corruption == "child-exit": guard["child_exit"] = 1
                    if corruption == "boolean-exit": guard["exit"] = False
                    if corruption == "cleanup": guard["cleanup_errors"] = ["uncertain"]
                    if corruption == "abort": guard["abort"] = "reserve reached"
                    if corruption == "missing-abort": guard.pop("abort")
                    marker = prefix + json.dumps(proof) + "\n"
                    log = "bootstrap log\n" + marker
                    if corruption == "missing-marker": log = "prefix:" + marker
                    if corruption == "duplicate": log = marker + marker
                    (evidence / "summary.json").write_text(json.dumps(guard))
                    (evidence / "command.log").write_text(log)
                    with patch.dict(os.environ, {"RUNNER_TEMP": str(root), "ARCH": arch}):
                        if corruption:
                            with self.assertRaises(SystemExit): exec(code, {})
                            self.assertFalse((evidence / "runtime-proof.json").exists())
                        else:
                            exec(code, {})
                            actual = json.loads((evidence / "runtime-proof.json").read_text())
                            self.assertEqual(actual["image_id"], "sha256:" + "1" * 64)
                            self.assertEqual(actual["platform"], "linux/" + arch)
                            self.assertEqual(actual["verified_source_core_jar_sha256"], source_hash)
                            self.assertEqual(actual["actual_java_major"], 21)

    def test_shell_steps_parse_without_dispatching_any_commands(self):
        for job in self.jobs.values():
            for step in job["steps"]:
                if "run" not in step:
                    continue
                script = re.sub(r"\$\{\{.*?\}\}", "fixture", step["run"])
                result = subprocess.run(["bash", "-n"], input=script, capture_output=True, text=True)
                with self.subTest(step=step.get("name")):
                    self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
