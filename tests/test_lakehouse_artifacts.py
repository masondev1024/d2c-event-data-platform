"""Identity/ownership tests only. No Docker command or source compilation runs."""
from __future__ import annotations

import argparse
from copy import deepcopy
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest import mock
from zipfile import ZipFile, ZipInfo


SCRIPT = Path(__file__).parents[1] / "scripts/lakehouse_artifacts.py"
spec = importlib.util.spec_from_file_location("lakehouse_artifacts_test_subject", SCRIPT)
subject = importlib.util.module_from_spec(spec)
spec.loader.exec_module(subject)


def jar_fixture() -> bytes:
    # Deterministic ZIP fixture for seal structure tests, not an executable
    # artifact, compilation result or vulnerability-scan input.
    output = io.BytesIO()
    with ZipFile(output, "w") as jar:
        for name, data in (("META-INF/MANIFEST.MF", b"Manifest-Version: 1.0\n"),
                           ("fixture/Example.class", b"\xca\xfe\xba\xbe\x00\x00")):
            jar.writestr(ZipInfo(name, (2026, 11, 4, 0, 0, 0)), data)
    return output.getvalue()


class ArtifactFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.source = root / "recipe"
        self.artifacts = root / "artifacts"
        for name in ("lakehouse/Dockerfile", "scripts/lakehouse_artifacts.py",
                     "scripts/container_security_guard.py", "container-build/lakehouse/example/recipe.py"):
            path = self.source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("source fixture\n")
        self.source_hashes = subject.recipe_hashes(self.source)
        for role in subject.ROLES:
            directory = self.artifacts / role
            for name in subject.REQUIRED[role]:
                path = directory / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(jar_fixture() if name.endswith(".jar") else b"{\"fixture\": true}\n")
            status = {"component": role, "command_exit": 0,
                      "source_recipe_files_sha256": self.source_hashes,
                      "source_recipe_sha256": subject.recipe_digest(self.source_hashes),
                      "native_source_compile_platform": "linux/arm64"}
            (directory / "build-status.json").write_text(json.dumps(status))
        dependency_dir = self.artifacts / "dependencies"
        dependency_provenance = {"spark_version": "4.2.1",
            "requested_packages": ["com.google.guava:guava:33.7.2-jre",
                                   "com.google.guava:failureaccess:1.0.3",
                                   "org.xerial.snappy:snappy-java:1.1.10.9"], "artifact_sha256": {
            path.name: subject.sha256(path) for path in dependency_dir.glob("*.jar")}}
        (dependency_dir / subject.PROVENANCE["dependencies"]).write_text(json.dumps(dependency_provenance))
        for role in subject.ROLES:
            directory = self.artifacts / role
            path = directory / "build-status.json"
            status = json.loads(path.read_text())
            status["produced_files_sha256"] = {name: subject.sha256(item)
                for name, item in subject.files_under(directory).items() if name != "build-status.json"}
            path.write_text(json.dumps(status))


class ArtifactSealTest(ArtifactFixture):
    def test_seal_verify_are_git_free_and_verify_is_read_only(self) -> None:
        manifest = subject.seal(self.artifacts, self.source)
        before = {name: (path.stat().st_mtime_ns, path.read_bytes())
                  for name, path in subject.files_under(self.artifacts).items()}
        self.assertEqual(subject.verify(self.artifacts, self.source), manifest)
        after = {name: (path.stat().st_mtime_ns, path.read_bytes())
                 for name, path in subject.files_under(self.artifacts).items()}
        self.assertEqual(before, after)
        self.assertEqual(manifest["runtime_platform_validation"], {"linux/amd64": "NOT_RUN", "linux/arm64": "NOT_RUN"})

    def test_payload_change_or_extra_file_is_rejected(self) -> None:
        subject.seal(self.artifacts, self.source)
        (self.artifacts / "hadoop" / "extra.txt").write_text("unsealed payload")
        with self.assertRaisesRegex(subject.ArtifactError, "exact file list|changed since"):
            subject.verify(self.artifacts, self.source)

    def test_source_change_cannot_be_relabelled_as_current_execution(self) -> None:
        (self.source / "container-build/lakehouse/example/recipe.py").write_text("changed source")
        with self.assertRaisesRegex(subject.ArtifactError, "different source recipe"):
            subject.seal(self.artifacts, self.source)

    def test_generated_python_cache_is_not_a_build_recipe(self) -> None:
        directory = self.source / "container-build/lakehouse/example/__pycache__"
        directory.mkdir()
        (directory / "recipe.cpython-314.pyc").write_bytes(b"generated cache")
        self.assertEqual(subject.recipe_hashes(self.source), self.source_hashes)
        (self.source / "container-build/lakehouse/example/future.build-input").write_text("new real input")
        self.assertNotEqual(subject.recipe_hashes(self.source), self.source_hashes)

    def test_artifact_symlink_is_rejected(self) -> None:
        (self.artifacts / "hadoop" / "linked").symlink_to(self.source / "lakehouse/Dockerfile")
        with self.assertRaisesRegex(subject.ArtifactError, "symlink rejected"):
            subject.seal(self.artifacts, self.source)

    def test_missing_role_jar_and_failed_status_are_not_sealable(self) -> None:
        path = self.artifacts / "hadoop" / "build-status.json"
        status = json.loads(path.read_text())
        status["command_exit"] = 1
        path.write_text(json.dumps(status))
        with self.assertRaisesRegex(subject.ArtifactError, "command-exit"):
            subject.seal(self.artifacts, self.source)

    def test_parquet_repository_copy_must_match_runtime_jar(self) -> None:
        path = self.artifacts / "parquet/maven/org/apache/parquet/parquet-jackson/1.17.1-d2c-jackson2.21.7/parquet-jackson-1.17.1-d2c-jackson2.21.7.jar"
        path.write_bytes(jar_fixture() + b"changed bytes")
        with self.assertRaisesRegex(subject.ArtifactError, "Maven JAR differs"):
            subject.seal(self.artifacts, self.source)

    def test_dependency_implementation_leak_is_rejected(self) -> None:
        path = self.artifacts / "dependencies/org.apache.hadoop_hadoop-client-api-3.5.0.jar"
        path.write_bytes(jar_fixture())
        with self.assertRaisesRegex(subject.ArtifactError, "leaked"):
            subject.seal(self.artifacts, self.source)

    def test_original_network_common_cannot_override_the_source_rebuilt_module(self) -> None:
        path = self.artifacts / "dependencies/org.apache.spark_spark-network-common_2.13-4.2.1.jar"
        path.write_bytes(jar_fixture())
        with self.assertRaisesRegex(subject.ArtifactError, "leaked"):
            subject.seal(self.artifacts, self.source)

    def test_old_direct_guava_or_snappy_cannot_accompany_the_patched_dependency(self) -> None:
        for filename in ("com.google.guava_guava-33.7.1-jre.jar",
                         "com.google.guava_failureaccess-1.0.2.jar",
                         "org.xerial.snappy_snappy-java-1.1.10.8.jar"):
            with self.subTest(filename=filename):
                path = self.artifacts / "dependencies" / filename
                path.write_bytes(jar_fixture())
                try:
                    with self.assertRaisesRegex(subject.ArtifactError, "unexpected direct runtime dependency"):
                        subject.seal(self.artifacts, self.source)
                finally:
                    path.unlink()

    def test_resolver_receipt_requires_the_actual_fixed_dependency_requests(self) -> None:
        path = self.artifacts / "dependencies" / subject.PROVENANCE["dependencies"]
        original = json.loads(path.read_text())
        for requests in (None, [], ["com.google.guava:guava:33.7.1-jre"],
                         original["requested_packages"] + [False]):
            with self.subTest(requests=requests):
                changed = deepcopy(original)
                changed["requested_packages"] = requests
                path.write_text(json.dumps(changed))
                with self.assertRaisesRegex(subject.ArtifactError, "patched direct"):
                    subject.seal(self.artifacts, self.source)
        path.write_text(json.dumps(original))

    def test_invalid_jar_does_not_pass_on_filename_alone(self) -> None:
        path = self.artifacts / "hadoop/hadoop-client-api-3.5.0-d2c-local1.jar"
        path.write_text("not an actual JAR")
        with self.assertRaisesRegex(subject.ArtifactError, "invalid JAR"):
            subject.seal(self.artifacts, self.source)

    def test_structurally_valid_jar_replacement_cannot_be_resealed_as_helper_output(self) -> None:
        path = self.artifacts / "hadoop/hadoop-client-api-3.5.0-d2c-local1.jar"
        path.write_bytes(jar_fixture() + b"different valid ZIP bytes")
        with self.assertRaisesRegex(subject.ArtifactError, "changed since"):
            subject.seal(self.artifacts, self.source)


class ComponentReceiptTest(ArtifactFixture):
    def args(self) -> argparse.Namespace:
        directory = Path(self.temp.name) / "component-output"
        directory.mkdir()
        return argparse.Namespace(component="hadoop", directory=directory, source_root=self.source,
                                  command=["--", "helper-command"])

    def test_receipt_binds_actual_zero_exit_to_before_after_recipe(self) -> None:
        args = self.args()
        with mock.patch.object(subject.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as run, \
                mock.patch.object(subject.platform, "system", return_value="Linux"), \
                mock.patch.object(subject.platform, "machine", return_value="aarch64"):
            receipt = subject.component(args)
        run.assert_called_once_with(["helper-command"])
        self.assertEqual(receipt["source_recipe_files_sha256"], self.source_hashes)
        self.assertEqual(receipt["native_source_compile_platform"], "linux/arm64")

    def test_failed_helper_never_writes_success_receipt(self) -> None:
        args = self.args()
        with mock.patch.object(subject.subprocess, "run", return_value=subprocess.CompletedProcess([], 5)):
            with self.assertRaisesRegex(subject.ArtifactError, "exited 5"):
                subject.component(args)
        self.assertFalse((args.directory / "build-status.json").exists())

    def test_source_mutation_during_helper_fails_before_receipt(self) -> None:
        args = self.args()
        def change(_):
            (self.source / "lakehouse/Dockerfile").write_text("changed during command")
            return subprocess.CompletedProcess([], 0)
        with mock.patch.object(subject.subprocess, "run", side_effect=change):
            with self.assertRaisesRegex(subject.ArtifactError, "changed during"):
                subject.component(args)
        self.assertFalse((args.directory / "build-status.json").exists())


def ownership_fixture():
    token = "a" * 32
    name = "d2c-security-" + token[:24]
    before = {"images": ["old-image"], "containers": ["old-container"], "volumes": ["old-volume"]}
    phase = {"role": "hadoop", "outcome": "export_verified", "payload_sha256": {"hadoop/proof": "hash"},
             "created_new_build_state": True, "engine_endpoint_candidates": ["desktop-linux", "unix:///socket"]}
    saved = {"name": name, "token": token}
    summary = {"outcome": "completed", "exit": 0, "child_exit": 0, "cleanup_errors": [],
               "builder": {"name": name, "container_id": "new-container"}}
    container = {"Id": "new-container", "Name": "/buildx_buildkit_" + name + "0", "State": {"Running": False},
                 "Config": {"Env": ["D2C_SECURITY_OWNER=" + token]},
                 "Mounts": [{"Type": "volume", "Name": "new-volume"}]}
    consumers = {"new-volume": {"new-container"}}
    registration = {"Name": name, "Driver": "docker-container", "Nodes": [{"Name": name + "0",
                    "Endpoint": "desktop-linux", "DriverOpts": {"env.D2C_SECURITY_OWNER": token}}]}
    return before, phase, saved, summary, container, consumers, registration


class ExactBuilderCleanupTest(unittest.TestCase):
    def test_plan_accepts_only_stopped_new_owned_single_node_builder(self) -> None:
        plan = subject.cleanup_plan(*ownership_fixture())
        self.assertEqual(plan["container_id"], "new-container")
        self.assertEqual(plan["volumes"], ["new-volume"])

    def test_existing_volume_running_builder_foreign_consumer_or_node_rejects(self) -> None:
        for mutation in ("existing-volume", "running", "foreign-consumer", "second-node", "wrong-owner", "wrong-endpoint"):
            values = deepcopy(ownership_fixture())
            before, phase, saved, summary, container, consumers, registration = values
            if mutation == "existing-volume": before["volumes"].append("new-volume")
            elif mutation == "running": container["State"]["Running"] = True
            elif mutation == "foreign-consumer": consumers["new-volume"].add("other-container")
            elif mutation == "second-node": registration["Nodes"].append(registration["Nodes"][0].copy())
            elif mutation == "wrong-owner": container["Config"]["Env"] = []
            elif mutation == "wrong-endpoint": registration["Nodes"][0]["Endpoint"] = "another-engine"
            with self.subTest(mutation=mutation), self.assertRaises(subject.ArtifactError):
                subject.cleanup_plan(*values)

    def test_failed_guard_unverified_export_or_reused_state_rejects(self) -> None:
        for mutation in ("guard-failed", "cleanup-uncertain", "export-not-verified", "reused-state"):
            values = deepcopy(ownership_fixture())
            phase, summary = values[1], values[3]
            if mutation == "guard-failed": summary["exit"] = 70
            elif mutation == "cleanup-uncertain": summary["cleanup_errors"] = ["unknown"]
            elif mutation == "export-not-verified": phase["outcome"] = "dispatching"
            elif mutation == "reused-state": phase["created_new_build_state"] = False
            with self.subTest(mutation=mutation), self.assertRaises(subject.ArtifactError):
                subject.cleanup_plan(*values)

    def test_no_such_message_must_match_the_exact_target_and_stand_alone(self) -> None:
        good = subprocess.CompletedProcess([], 1, "[]\n", "Error response from daemon: No such container: target\n")
        self.assertTrue(subject.absent(good, "container", "target"))
        self.assertTrue(subject.absent(subprocess.CompletedProcess([], 1, "[]\n",
            "Error response from daemon: get target: no such volume\n"), "volume", "target"))
        self.assertFalse(subject.absent(subprocess.CompletedProcess([], 1, "[]\n",
            "Error response from daemon: get other: no such volume\n"), "volume", "target"))
        for result in (subprocess.CompletedProcess([], 1, "[]", "No such container: other"),
                       subprocess.CompletedProcess([], 1, "[]", "No such container: target\nengine unavailable"),
                       subprocess.CompletedProcess([], 1, "[{}]", "No such container: target")):
            self.assertFalse(subject.absent(result, "container", "target"))


class CleanupDispatchTest(ArtifactFixture):
    def prepare_cleanup(self):
        before, phase, saved, summary, container, consumers, registration = ownership_fixture()
        phase["payload_sha256"] = {"hadoop/" + name: subject.sha256(path)
                                   for name, path in subject.files_under(self.artifacts / "hadoop").items()}
        root = Path(self.temp.name)
        state, evidence = root / "builder-state", root / "guard-evidence"
        (state / "buildx/instances").mkdir(parents=True)
        evidence.mkdir()
        (state / "builder.json").write_text(json.dumps(saved))
        (state / "buildx/instances" / saved["name"]).write_text(json.dumps(registration))
        (evidence / "summary.json").write_text(json.dumps(summary))
        def dispatch(command, **kwargs):
            if command[:3] == ["docker", "buildx", "rm"]:
                return subprocess.CompletedProcess(command, 0, "", "")
            if command[:3] == ["docker", "container", "ls"]:
                return subprocess.CompletedProcess(command, 0, "new-container\n", "")
            if command == ["docker", "volume", "inspect", "new-volume"]:
                return subprocess.CompletedProcess(command, 1, "[]\n", "Error response from daemon: No such volume: new-volume\n")
            if command == ["docker", "container", "inspect", "new-container"]:
                dispatch.inspections += 1
                if dispatch.inspections == 1:
                    return subprocess.CompletedProcess(command, 0, json.dumps([container]), "")
                return subprocess.CompletedProcess(command, 1, "[]\n", "Error response from daemon: No such container: new-container\n")
            raise AssertionError("Unexpected/unmocked Docker command: " + str(command))
        dispatch.inspections = 0
        return before, phase, saved, state, evidence, dispatch

    def test_only_one_exact_rm_uses_private_configuration_after_all_proofs(self) -> None:
        before, phase, saved, state, evidence, dispatch = self.prepare_cleanup()
        with mock.patch.object(subject, "run_capture", side_effect=dispatch) as run, \
                mock.patch.object(subject, "engine_snapshot", return_value=before), \
                mock.patch.object(subject, "engine_endpoints", return_value=phase["engine_endpoint_candidates"]), \
                mock.patch.object(subject, "LOCK_PATH", Path(self.temp.name) / "lock"):
            receipt = subject.discard_created_builder(before, phase, state, evidence, self.artifacts)
        mutations = [call for call in run.call_args_list if call.args[0][:3] == ["docker", "buildx", "rm"]]
        self.assertEqual(len(mutations), 1)
        self.assertEqual(mutations[0].args[0], ["docker", "buildx", "rm", saved["name"]])
        self.assertEqual(mutations[0].kwargs["env"]["BUILDX_CONFIG"], str(state / "buildx"))
        self.assertTrue(receipt["pre_existing_resources_preserved"])

    def test_inconsistent_post_snapshot_stops_without_deletion_retry(self) -> None:
        before, phase, saved, state, evidence, dispatch = self.prepare_cleanup()
        after = deepcopy(before)
        after["volumes"].append("new-volume")
        with mock.patch.object(subject, "run_capture", side_effect=dispatch) as run, \
                mock.patch.object(subject, "engine_snapshot", return_value=after), \
                mock.patch.object(subject, "engine_endpoints", return_value=phase["engine_endpoint_candidates"]), \
                mock.patch.object(subject, "LOCK_PATH", Path(self.temp.name) / "lock"):
            with self.assertRaisesRegex(subject.ArtifactError, "remain"):
                subject.discard_created_builder(before, phase, state, evidence, self.artifacts)
        self.assertEqual(sum(call.args[0][:3] == ["docker", "buildx", "rm"] for call in run.call_args_list), 1)

    def test_lock_contention_prevents_even_cleanup_inspection(self) -> None:
        before, phase, saved, state, evidence, _ = self.prepare_cleanup()
        with mock.patch.object(subject, "run_capture") as run, \
                mock.patch.object(subject.fcntl, "flock", side_effect=BlockingIOError), \
                mock.patch.object(subject, "LOCK_PATH", Path(self.temp.name) / "lock"):
            with self.assertRaisesRegex(subject.ArtifactError, "engine lock"):
                subject.discard_created_builder(before, phase, state, evidence, self.artifacts)
        run.assert_not_called()


class PhasedBuildPlanTest(ArtifactFixture):
    def test_mocked_phases_export_assemble_seal_once_and_completed_resume_does_not_rebuild(self) -> None:
        root = Path(self.temp.name)
        output, work = root / "assembled", root / "phased-work"
        args = argparse.Namespace(discard_created_builder_cache=True, artifacts=output,
            source_root=self.source, work_dir=work, storage_path=root,
            native_platform="linux/arm64", resume=False)
        commands = []
        def guard_run(command, **kwargs):
            commands.append(command)
            target = command[command.index("--target") + 1]
            role = next(role for role, value in subject.TARGETS.items() if value == target)
            destination = Path(command[command.index("--output") + 1].removeprefix("type=local,dest="))
            destination.mkdir()
            shutil.copytree(self.artifacts / role, destination / role)
            evidence = Path(command[command.index("--evidence-dir") + 1])
            evidence.mkdir()
            (evidence / "summary.json").write_text(json.dumps({"outcome": "completed", "exit": 0,
                "child_exit": 0, "cleanup_errors": [], "min_free_bytes": 10 * subject.GIB}))
            return subprocess.CompletedProcess(command, 0)
        before = {"images": ["old-image"], "containers": ["old-container"], "volumes": ["old-volume"]}
        with mock.patch.object(subject, "assert_native") as native, \
                mock.patch.object(subject, "engine_endpoints", return_value=["desktop-linux", "unix:///socket"]), \
                mock.patch.object(subject, "engine_snapshot", return_value=before), \
                mock.patch.object(subject, "free_bytes", return_value=17 * subject.GIB), \
                mock.patch.object(subject.subprocess, "run", side_effect=guard_run) as run, \
                mock.patch.object(subject, "discard_created_builder", return_value={"pre_existing_resources_preserved": True}):
            manifest = subject.build(args)
            self.assertEqual([command[command.index("--target") + 1] for command in commands],
                             [subject.TARGETS[role] for role in subject.BUILD_ORDER])
            self.assertEqual(len(manifest["execution"]["phase_receipts"]), 5)
            self.assertEqual(set(path.name for path in output.iterdir()), set(subject.ROLES) | {"manifest.json"})
            args.resume = True
            run.reset_mock()
            native.reset_mock()
            self.assertEqual(subject.build(args), manifest)
            run.assert_not_called()
            native.assert_called_once_with("linux/arm64")

    def test_guard_plan_has_fixed_component_budget_and_reuses_parquet_only_for_iceberg(self) -> None:
        base = Path(self.temp.name)
        for role in subject.BUILD_ORDER:
            command = subject.guard_command(self.source, role, base / "new-output", base / "new-evidence",
                base / "new-state", base, "linux/arm64", self.artifacts)
            self.assertEqual(command[command.index("--minimum-free-gib") + 1], "16")
            self.assertEqual(command[command.index("--reserve-free-gib") + 1], "6")
            expected_target = "dependency-artifacts" if role == "dependencies" else role + "-artifacts"
            self.assertEqual(command[command.index("--target") + 1], expected_target)
            self.assertEqual("--build-context" in command, role == "iceberg")
            self.assertNotIn("--push", command)

    def test_remote_docker_context_cannot_be_hidden_by_local_docker_host(self) -> None:
        with mock.patch.dict(subject.os.environ, {"DOCKER_CONTEXT": "remote", "DOCKER_HOST": "unix:///local"}, clear=True), \
                mock.patch.object(subject, "checked_capture", side_effect=["remote\n", "ssh://remote\n"]):
            with self.assertRaisesRegex(subject.ArtifactError, "unambiguous local"):
                subject.engine_endpoints()

    def test_low_budget_build_without_discard_opt_in_never_calls_docker(self) -> None:
        args = argparse.Namespace(discard_created_builder_cache=False)
        with mock.patch.object(subject, "run_capture") as run:
            with self.assertRaisesRegex(subject.ArtifactError, "explicit"):
                subject.build(args)
        run.assert_not_called()

    def test_resume_rejects_uncertain_phase_without_automatic_retry(self) -> None:
        state = {"source_recipe_files_sha256": self.source_hashes, "artifacts": str(self.artifacts),
                 "native_platform": "linux/arm64", "phases": [{"role": "hadoop", "outcome": "dispatching"}]}
        # Remove none of the test payload; extra role directories themselves
        # already prove this is an unrecorded/partial resume tree.
        with self.assertRaises(subject.ArtifactError):
            subject.validate_resume(state, self.source_hashes, self.artifacts, "linux/arm64")


if __name__ == "__main__":
    unittest.main()
