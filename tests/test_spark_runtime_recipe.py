"""Small fixtures test rejection paths; no Spark download or JVM compilation."""

from __future__ import annotations

import ast
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import re
import struct
import subprocess
import sys
import tarfile
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
import zipfile


RECIPE = Path(__file__).resolve().parents[1] / "container-build/lakehouse/spark"


def load_module(name: str):
    spec = importlib.util.spec_from_file_location(name, RECIPE / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


verify = load_module("verify_runtime")
build = load_module("build_core")
MANIFEST = json.loads((RECIPE / "inputs.json").read_text())


def class_file(name: str, descriptor: str = "()I", constant: int = 42, major: int = 61) -> bytes:
    """Minimal valid public interface, including an inline constant and method."""
    pool = []

    def utf8(text):
        raw = text.encode()
        pool.append(b"\x01" + struct.pack(">H", len(raw)) + raw)
        return len(pool)

    def reference(tag, index):
        pool.append(bytes([tag]) + struct.pack(">H", index))
        return len(pool)

    this = reference(7, utf8(name))
    superclass = reference(7, utf8("java/lang/Object"))
    field_name, field_type = utf8("ANSWER"), utf8("I")
    constant_attribute = utf8("ConstantValue")
    pool.append(b"\x03" + struct.pack(">i", constant))
    constant_index = len(pool)
    method_name, method_type = utf8("value"), utf8(descriptor)
    header = struct.pack(">IHHH", 0xCAFEBABE, 0, major, len(pool) + 1) + b"".join(pool)
    # Public abstract interface; public static final field; abstract method.
    body = struct.pack(">HHHHH", 0x0601, this, superclass, 0, 1)
    body += struct.pack(">HHHHHIH", 0x0019, field_name, field_type, 1, constant_attribute, 2, constant_index)
    body += struct.pack(">HHHHHH", 1, 0x0401, method_name, method_type, 0, 0)
    return header + body


def jar_fixture(path: Path, *, jetty_version="12.1.14", spark_version="4.2.1", descriptor="()I", constant=42, major=61):
    with zipfile.ZipFile(path, "w") as jar:
        jar.writestr(verify.SPARK_PROPERTIES, f"groupId=org.apache.spark\nartifactId=spark-core_2.13\nversion={spark_version}\n")
        for module in verify.REQUIRED_JETTY_MODULES:
            jar.writestr(f"META-INF/maven/org.eclipse.jetty/{module}/pom.properties", f"groupId=org.eclipse.jetty\nartifactId={module}\nversion={jetty_version}\n")
        for name in verify.REQUIRED_CLASSES:
            jar.writestr(name, class_file(name[:-6], descriptor, constant, major))
        jar.writestr("META-INF/LICENSE", "Fixture Apache license")
        jar.writestr("META-INF/NOTICE", "Fixture notice")


def dependency_tree():
    return {
        "groupId": "org.apache.spark", "artifactId": "spark-core_2.13", "version": "4.2.1",
        "children": [
            {"groupId": "org.apache.spark", "artifactId": "spark-common-utils_2.13", "version": "4.2.1"},
            {"groupId": "org.eclipse.jetty", "artifactId": "jetty-http", "version": "12.1.14"},
            {"groupId": "org.scala-lang", "artifactId": "scala-library", "version": "2.13.18"},
        ],
    }


def protobuf_proof(target: Path):
    path = str(target.resolve())
    return {
        "schema_version": 1, "status": "passed", "actual_java_major": 17,
        "expected_java_major": 17, "java_runtime_version": "17.0.21+9",
        "java_vendor": "Eclipse Adoptium", "expected_core_jar": path,
        "actual_core_jar": path, "core_jar_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
        "class_code_sources": {name: path for name in build.PROTOBUF_CLASSES},
        "cases": ["NODE", "CLUSTER", "WRAPPER_NOT_SET"],
        "merge_cases_verified": 3, "wire_roundtrips_verified": 3,
        "unset_merge_preserves_existing": True,
    }


def docker_stages():
    text = (RECIPE.parents[2] / "lakehouse/Dockerfile").read_text()
    matches = list(re.finditer(r"^FROM (?:--platform=\$BUILDPLATFORM )?(\S+) AS (\S+)\s*$", text, re.M))
    return text, {
        match.group(2): (match.group(1), text[match.end():matches[index + 1].start() if index + 1 < len(matches) else None])
        for index, match in enumerate(matches)
    }


class SparkRuntimeRecipeTests(unittest.TestCase):
    def setUp(self):
        scratch = TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)

    def test_fixture_structural_abi_and_local_jetty_variant(self):
        baseline, rebuilt = self.root / "baseline.jar", self.root / "rebuilt.jar"
        jar_fixture(baseline, jetty_version="12.1.8")
        jar_fixture(rebuilt)
        result = verify.verify_runtime(rebuilt, baseline)
        self.assertIs(result["structural_abi_matches_baseline"], True)
        self.assertEqual(result["spark_classes_compared"], 2)
        self.assertNotEqual(result["rebuilt_sha256"], result["baseline_sha256"])

    def assert_rejected_jar(self, changes, message):
        baseline, rebuilt = self.root / "baseline.jar", self.root / "rebuilt.jar"
        jar_fixture(baseline, jetty_version="12.1.8")
        jar_fixture(rebuilt, **changes)
        with self.assertRaisesRegex(ValueError, message):
            verify.verify_runtime(rebuilt, baseline)

    def test_rejects_wrong_spark_version(self):
        self.assert_rejected_jar({"spark_version": "4.1.3"}, "Expected Spark core")

    def test_rejects_stale_jetty_version(self):
        self.assert_rejected_jar({"jetty_version": "12.1.8"}, "Jetty version mismatch")

    def test_rejects_changed_method_descriptor(self):
        self.assert_rejected_jar({"descriptor": "()J"}, "structural ABI changed")

    def test_rejects_changed_public_constant(self):
        self.assert_rejected_jar({"constant": 43}, "structural ABI changed")

    def test_rejects_changed_classfile_version(self):
        self.assert_rejected_jar({"major": 65}, "structural ABI changed")

    def test_missing_compiler_generated_class_still_fails_exact_class_set(self):
        baseline, rebuilt = self.root / "baseline.jar", self.root / "rebuilt.jar"
        jar_fixture(baseline)
        jar_fixture(rebuilt)
        name = "org/apache/spark/status/protobuf/StoreTypes$1.class"
        with zipfile.ZipFile(baseline, "a") as jar:
            jar.writestr(name, class_file(name[:-6]))
        with self.assertRaisesRegex(ValueError, r"Spark class set changed:.*StoreTypes\$1"):
            verify.verify_runtime(rebuilt, baseline)

    def test_rejects_unrelocated_jetty_and_missing_implementation(self):
        baseline, rebuilt = self.root / "baseline.jar", self.root / "rebuilt.jar"
        jar_fixture(baseline)
        jar_fixture(rebuilt)
        with zipfile.ZipFile(rebuilt, "a") as jar:
            jar.writestr("org/eclipse/jetty/http/HttpParser.class", class_file("org/eclipse/jetty/http/HttpParser"))
        with self.assertRaisesRegex(ValueError, "Unrelocated"):
            verify.verify_runtime(rebuilt, baseline)
        missing = self.root / "missing.jar"
        with zipfile.ZipFile(baseline) as source, zipfile.ZipFile(missing, "w") as destination:
            for name in source.namelist():
                if name != "org/sparkproject/jetty/http/HttpParser.class":
                    destination.writestr(name, source.read(name))
        with self.assertRaisesRegex(ValueError, "Missing implementation"):
            verify.verify_runtime(missing, baseline)

    def assert_rejected_dependency_version(self, child, version):
        tree = dependency_tree()
        tree["children"][child]["version"] = version
        with self.assertRaises(ValueError):
            build.verify_dependency_tree(tree, MANIFEST)

    def test_dependency_tree_rejects_mixed_spark_version(self):
        self.assert_rejected_dependency_version(0, "4.1.3")

    def test_dependency_tree_rejects_stale_jetty_version(self):
        self.assert_rejected_dependency_version(1, "12.1.8")

    def test_dependency_tree_rejects_mixed_scala_version(self):
        self.assert_rejected_dependency_version(2, "2.13.17")

    def test_recipe_retains_original_core_build_and_only_jetty_override(self):
        self.assertEqual(MANIFEST["overrides"], {"jetty.version": "12.1.14"})
        self.assertEqual(len(MANIFEST["source"]["sha512"]), 128)
        self.assertEqual(len(MANIFEST["maven"]["sha512"]), 128)
        command = build.maven_command(Path("/tools/mvn"), Path("/source"), Path("/cache"), MANIFEST)
        self.assertEqual(command[command.index("--file") + 1], "/source/core/pom.xml")
        self.assertIn("-Djetty.version=12.1.14", command)
        self.assertIn("--strict-checksums", command)
        for value in ("-am", "-pl", "-Denforcer.skip=true"):
            self.assertNotIn(value, command)
        self.assertEqual(build.verify_dependency_tree(dependency_tree(), MANIFEST)["scala_library"], ["2.13.18"])

    def test_corrupt_cached_archive_is_not_used_or_refetched(self):
        (self.root / "source.tgz").write_bytes(b"wrong bytes")
        with patch.object(build, "urlopen", side_effect=AssertionError("Digest mismatch must not trigger another download")) as fetch:
            with self.assertRaisesRegex(ValueError, "SHA512 mismatch"):
                build.download_verified({"filename": "source.tgz", "sha512": "0" * 128}, self.root)
        fetch.assert_not_called()

    def assert_unsafe_archive_rejected(self, name, link):
        path = self.root / "fixture.tgz"
        with tarfile.open(path, "w:gz") as archive:
            entry = tarfile.TarInfo(name)
            if link:
                entry.type, entry.linkname = tarfile.SYMTYPE, link
                archive.addfile(entry)
            else:
                entry.size = 1
                archive.addfile(entry, io.BytesIO(b"x"))
        with self.assertRaisesRegex(ValueError, "Unsafe archive"):
            build.extract_verified(path, self.root / "extract", "spark-4.2.1")

    def test_archive_rejects_parent_path_traversal(self):
        self.assert_unsafe_archive_rejected("../outside", None)

    def test_archive_rejects_absolute_symlink(self):
        self.assert_unsafe_archive_rejected("spark-4.2.1/link", "/outside")

    def test_source_pom_version_drift_fails_before_build(self):
        source = self.root / "source"
        (source / "core").mkdir(parents=True)
        root = '<project xmlns="http://maven.apache.org/POM/4.0.0"><version>4.1.3</version></project>'
        (source / "pom.xml").write_text(root)
        (source / "core/pom.xml").write_text("fixture")
        manifest = copy.deepcopy(MANIFEST)
        manifest["source"]["pom_sha256"] = {
            name: hashlib.sha256((source / name).read_bytes()).hexdigest()
            for name in ("pom.xml", "core/pom.xml")
        }
        with self.assertRaisesRegex(ValueError, "Unexpected Spark source version"):
            build.verify_source(source, manifest)

    def build_jdk_fixture(self, **changes):
        home = self.root / "jdk"
        (home / "bin").mkdir(parents=True, exist_ok=True)
        for name in ("java", "javac"):
            (home / "bin" / name).write_text("Fixture executable; never run")
        values = {"JAVA_VERSION": "17.0.21", "JAVA_RUNTIME_VERSION": "17.0.21+9", "IMPLEMENTOR": "Eclipse Adoptium"}
        values.update(changes)
        (home / "release").write_text("".join(f'{name}="{value}"\n' for name, value in values.items()))
        return home

    def jdk_version_reports(self, *, javac="javac 17.0.21", java='openjdk version "17.0.21"\nOpenJDK Runtime Environment Temurin-17.0.21+9 (build 17.0.21+9)'):
        return [subprocess.CompletedProcess([], 0, "", java), subprocess.CompletedProcess([], 0, javac, "")]

    def test_build_jdk_requires_actual_pinned_release_and_absolute_executables(self):
        home = self.build_jdk_fixture()
        environment = {"JAVA_HOME": str(home), "PATH": "/unrelated/bin"}
        with patch.object(build.subprocess, "run", side_effect=self.jdk_version_reports()) as run:
            selected, evidence = build.verify_build_jdk(MANIFEST, environment)
        self.assertEqual(selected, home.resolve())
        self.assertEqual(run.call_args_list[0].args[0], [str(home.resolve() / "bin/java"), "-version"])
        self.assertEqual(run.call_args_list[1].args[0], [str(home.resolve() / "bin/javac"), "-version"])
        self.assertEqual(environment["PATH"].split(":")[0], str(home.resolve() / "bin"))
        self.assertEqual(evidence["release_sha256"], hashlib.sha256((home / "release").read_bytes()).hexdigest())
        self.assertEqual(evidence["expected_toolchain"]["major"], 17)

    def test_build_jdk_rejects_wrong_major_patch_vendor_or_build_before_execution(self):
        for values in ({"JAVA_VERSION": "21.0.13"}, {"JAVA_VERSION": "17.0.20.1"},
                       {"IMPLEMENTOR": "Unknown vendor"}, {"JAVA_RUNTIME_VERSION": "17.0.21+90"}):
            with self.subTest(values=values):
                home = self.build_jdk_fixture(**values)
                with patch.object(build.subprocess, "run") as run:
                    with self.assertRaisesRegex(ValueError, "Unexpected Spark build JDK"):
                        build.verify_build_jdk(MANIFEST, {"JAVA_HOME": str(home)})
                run.assert_not_called()

    def test_build_jdk_rejects_executable_release_mismatch(self):
        home = self.build_jdk_fixture()
        for reports in (self.jdk_version_reports(java='openjdk version "21.0.13"'),
                        self.jdk_version_reports(javac="javac 21.0.13")):
            with self.subTest(reports=reports):
                with patch.object(build.subprocess, "run", side_effect=reports):
                    with self.assertRaisesRegex(ValueError, "Actual (Java|javac) executable"):
                        build.verify_build_jdk(MANIFEST, {"JAVA_HOME": str(home)})

    def test_wrong_build_jdk_fails_before_source_download(self):
        baseline = self.root / "baseline.jar"
        jar_fixture(baseline)
        home = self.build_jdk_fixture(JAVA_VERSION="21.0.13")
        with patch.dict(build.os.environ, {"JAVA_HOME": str(home)}):
            with patch.object(build, "download_verified") as download:
                with self.assertRaisesRegex(ValueError, "Unexpected Spark build JDK"):
                    build.build(self.root / "work", self.root / "output", baseline, self.root / "repository")
        download.assert_not_called()

    def test_actual_protobuf_probe_proof_requires_jar_identity_and_complete_cases(self):
        target = self.root / "core.jar"
        target.write_bytes(b"Fixture JAR bytes; never loaded")
        proof = protobuf_proof(target)
        self.assertEqual(build.validate_protobuf_proof(build.PROTOBUF_PROOF_MARKER + json.dumps(proof), target, 17), proof)
        corruptions = (
            {"schema_version": True}, {"actual_java_major": 17.0},
            {"actual_java_major": 21}, {"expected_java_major": 21}, {"status": "skipped"},
            {"actual_core_jar": "/other/core.jar"}, {"expected_core_jar": "/other/core.jar"},
            {"core_jar_sha256": "0" * 64}, {"class_code_sources": {}},
            {"class_code_sources": {name: "/baseline/core.jar" for name in build.PROTOBUF_CLASSES}},
            {"cases": ["NODE", "CLUSTER"]}, {"merge_cases_verified": 2},
            {"wire_roundtrips_verified": 2}, {"unset_merge_preserves_existing": False},
            {"java_runtime_version": ""}, {"java_vendor": ""}, {"java_vendor": True},
            {"unset_merge_preserves_existing": 1}, {"merge_cases_verified": 3.0},
        )
        for corruption in corruptions:
            with self.subTest(corruption=corruption):
                bad = {**proof, **corruption}
                with self.assertRaises(ValueError):
                    build.validate_protobuf_proof(build.PROTOBUF_PROOF_MARKER + json.dumps(bad), target, 17)
        with self.assertRaisesRegex(ValueError, "not an object"):
            build.validate_protobuf_proof(build.PROTOBUF_PROOF_MARKER + "[]", target, 17)

    def test_protobuf_probe_rejects_missing_or_ambiguous_execution_marker(self):
        target = self.root / "core.jar"
        target.write_bytes(b"fixture")
        marker = build.PROTOBUF_PROOF_MARKER + json.dumps(protobuf_proof(target))
        for stdout in ("", "BUILD SUCCESS", marker + "\n" + marker, "unexpected\n" + marker):
            with self.subTest(stdout=stdout):
                with self.assertRaisesRegex(ValueError, "Missing or ambiguous"):
                    build.validate_protobuf_proof(stdout, target, 17)

    def test_protobuf_probe_runner_uses_only_rebuilt_jar_and_records_actual_commands(self):
        target = self.root / "core.jar"
        target.write_bytes(b"fixture")
        work = self.root / "work"
        work.mkdir()
        jdk = self.root / "jdk"
        reports = [subprocess.CompletedProcess([], 0, "", ""),
                   subprocess.CompletedProcess([], 0, build.PROTOBUF_PROOF_MARKER + json.dumps(protobuf_proof(target)), "")]
        with patch.object(build.subprocess, "run", side_effect=reports) as run:
            report = build.run_protobuf_probe(jdk, target, work, {})
        compilation = run.call_args_list[0].args[0]
        execution = run.call_args_list[1].args[0]
        self.assertEqual(compilation[:4], [str(jdk / "bin/javac"), "-J-Xmx256m", "--release", "17"])
        self.assertEqual(compilation[compilation.index("-classpath") + 1], str(target))
        self.assertEqual(execution[0], str(jdk / "bin/java"))
        self.assertEqual(execution[execution.index("-classpath") + 1], str(work / "protobuf-probe-classes") + ":" + str(target))
        self.assertEqual(execution[-3:], ["VerifySparkProtobuf", "17", str(target)])
        self.assertEqual(report["result"]["core_jar_sha256"], hashlib.sha256(target.read_bytes()).hexdigest())
        self.assertEqual(report["probe_source_sha256"], hashlib.sha256((RECIPE / "VerifySparkProtobuf.java").read_bytes()).hexdigest())

    def test_protobuf_probe_cannot_run_after_compilation_failure(self):
        work = self.root / "work"
        work.mkdir()
        with patch.object(build.subprocess, "run", side_effect=subprocess.CalledProcessError(1, ["javac"])) as run:
            with self.assertRaises(subprocess.CalledProcessError):
                build.run_protobuf_probe(self.root / "jdk", self.root / "core.jar", work, {})
        self.assertEqual(run.call_count, 1)

    def test_portable_probe_exercises_original_enum_switch_and_checks_loaded_origins(self):
        source = (RECIPE / "VerifySparkProtobuf.java").read_text()
        for value in (".mergeFrom(input)", ".parseFrom(merged.toByteArray())", "getCodeSource()",
                      "expectedJar.equals(actualJar)", "actualJava == expectedJava", "getWrapperCase()",
                      "checkCase(node, WrapperCase.NODE, cluster)",
                      "checkCase(cluster, WrapperCase.CLUSTER, node)",
                      "checkCase(unset, WrapperCase.WRAPPER_NOT_SET, unset)",
                      "checkCase(unset, WrapperCase.WRAPPER_NOT_SET, node)",
                      "checkCase(unset, WrapperCase.WRAPPER_NOT_SET, cluster)",
                      "D2C_SPARK_PROTOBUF_PROOF=", "MessageDigest.getInstance(\"SHA-256\")"):
            self.assertIn(value, source)
        self.assertNotRegex(source, r"\bassert\s+")
        self.assertNotIn("java.net.", source)

    def assert_scratch_export(self, stage, builder, source, destination):
        _, stages = docker_stages()
        self.assertEqual(stages[stage][0], "scratch")
        self.assertIn(f"COPY --from={builder} {source} {destination}", stages[stage][1])

    def test_hadoop_scratch_export_contract(self):
        self.assert_scratch_export("hadoop-artifacts", "hadoop-runtime-builder", "/out/hadoop/", "/hadoop/")

    def test_parquet_scratch_export_contract(self):
        self.assert_scratch_export("parquet-artifacts", "parquet-runtime-builder", "/work/output/", "/parquet/")

    def test_iceberg_scratch_export_contract(self):
        self.assert_scratch_export("iceberg-artifacts", "iceberg-runtime-builder", "/artifacts/", "/iceberg/")

    def test_spark_scratch_export_contract(self):
        self.assert_scratch_export("spark-artifacts", "spark-runtime-builder", "/out/spark/", "/spark/")

    def test_dependency_scratch_export_contract(self):
        self.assert_scratch_export("dependency-artifacts", "dependency-resolver", "/opt/spark/extra-jars/", "/dependencies/")

    def test_coherent_official_base_keeps_its_patched_jdk(self):
        text, stages = docker_stages()
        pinned = "apache/spark:4.2.1@sha256:0cb70cf0193ec81c93c6c47078208512bbe659dd9dc89535dd2773826f0ea2bf"
        self.assertEqual(stages["spark-base"][0], pinned)
        self.assertEqual(stages["source-build-base"][0], pinned)
        self.assertNotIn("SapMachine", text)
        self.assertNotIn("AS java-runtime", text)
        self.assertNotIn("rm -rf /opt/java/openjdk", text)
        self.assertIn('JAVA_VERSION="21.0.13"', stages["runtime"][1])
        self.assertIn("spark-sql-kafka-0-10_2.13:4.2.1", text)
        self.assertIn(MANIFEST["maven"]["sha512"], stages["source-build-base"][1])

    def test_pinned_java17_compiler_is_confined_to_spark_builder(self):
        text, stages = docker_stages()
        self.assertEqual(stages["spark-build-jdk"][0], MANIFEST["build_jdk"]["image"])
        self.assertRegex(MANIFEST["build_jdk"]["image"], r"^eclipse-temurin:17\.0\.21_9-jdk-jammy@sha256:[a-f0-9]{64}$")
        spark = stages["spark-runtime-builder"][1]
        self.assertIn("COPY --from=spark-build-jdk /opt/java/openjdk/ /opt/spark-build-jdk/", spark)
        self.assertIn("JAVA_HOME=/opt/spark-build-jdk", spark)
        self.assertIn("PATH=/opt/spark-build-jdk/bin:$PATH", spark)
        self.assertIn('JAVA_VERSION="17.0.21"', spark)
        for name, (_, body) in stages.items():
            if name != "spark-runtime-builder":
                self.assertNotIn("/opt/spark-build-jdk", body, name)
        self.assertIn('JAVA_VERSION="21.0.13"', stages["runtime"][1])
        self.assertIn("sh /build/spark/rebuild.sh", spark)
        self.assertIn("--maven-home /opt/apache-maven-3.9.15", spark)

    def test_artifact_selection_seals_and_verifies_before_jar_installation(self):
        text, stages = docker_stages()
        header = text[:text.index("FROM ")]
        self.assertIn("ARG D2C_ARTIFACT_STAGE=compiled-artifacts", header)
        self.assertIn("ARG D2C_PARQUET_STAGE=parquet-artifacts", header)
        self.assertEqual(stages["selected-artifacts"][0], "${D2C_ARTIFACT_STAGE}")
        self.assertEqual(stages["selected-parquet"][0], "${D2C_PARQUET_STAGE}")
        self.assertIn("COPY --from=selected-parquet /parquet/maven/ /rebuilt-repository/", stages["iceberg-runtime-builder"][1])
        self.assertIn("lakehouse_artifacts.py seal", stages["artifact-sealer"][1])
        self.assertIn("COPY --from=artifact-sealer /artifacts/ /", stages["compiled-artifacts"][1])
        runtime = stages["runtime"][1]
        self.assertIn("type=bind,from=selected-artifacts,target=/tmp/d2c-artifacts,ro", runtime)
        self.assertLess(runtime.index("lakehouse_artifacts.py verify"), runtime.index("&& cp /tmp/d2c-artifacts/dependencies/*.jar"))
        self.assertNotIn("COPY --from=hadoop-runtime-builder", runtime)
        for source in ("hadoop-client-api-3.5.0-d2c-local1.jar", "hadoop-client-runtime-3.5.0-d2c-local1.jar",
                       "iceberg-spark-runtime-4.2_2.13-1.12.0-d2c-security1.jar",
                       "spark-core_2.13-4.2.1-d2c-jetty12.1.14.jar"):
            self.assertIn(source, runtime)

    def test_each_component_records_actual_exit_and_unchanged_recipes_via_wrapper(self):
        text, stages = docker_stages()
        for stage, role in (("hadoop-runtime-builder", "hadoop"), ("parquet-runtime-builder", "parquet"),
                            ("iceberg-runtime-builder", "iceberg"), ("spark-runtime-builder", "spark"),
                            ("dependency-resolver", "dependencies")):
            self.assertIn("lakehouse_artifacts.py component", stages[stage][1])
            self.assertIn(f"--component {role} ", stages[stage][1])
            self.assertIn("--source-root /recipe-source", stages[stage][1])
        self.assertNotIn("build-status.json", text)
        self.assertIn("COPY lakehouse/Dockerfile /recipe-source/lakehouse/Dockerfile", stages["source-build-base"][1])

    def test_dependency_heredoc_shell_and_python_parse_without_execution(self):
        text, _ = docker_stages()
        shell_match = re.search(r"<<'SH'\n(.*?)\nSH", text, re.S)
        self.assertIsNotNone(shell_match)
        shell = shell_match.group(1)
        result = subprocess.run(["sh", "-n"], input=shell, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        python_match = re.search(r"<<'PY'\n(.*?)\nPY", shell, re.S)
        self.assertIsNotNone(python_match)
        ast.parse(python_match.group(1))


if __name__ == "__main__":
    unittest.main()
