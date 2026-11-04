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
