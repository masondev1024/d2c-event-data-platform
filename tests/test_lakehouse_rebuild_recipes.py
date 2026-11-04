"""Offline checks for source selection and fail-closed runtime verification."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import struct
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from zipfile import ZipFile


HELPERS = Path(__file__).parents[1] / "container-build/lakehouse"


def load(relative):
    path = HELPERS / relative
    spec = importlib.util.spec_from_file_location(path.stem.replace("-", "_"), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def class_file(owner, *, descriptor="()Ljava/lang/String;", method=True):
    def utf8(value):
        data = value.encode()
        return b"\x01" + struct.pack(">H", len(data)) + data

    pool = (utf8(owner), b"\x07\x00\x01", utf8("java/lang/Object"),
            b"\x07\x00\x03", utf8("read"), utf8(descriptor))
    header = b"\xca\xfe\xba\xbe" + struct.pack(">HHH", 0, 61, 7) + b"".join(pool)
    declaration = struct.pack(">HHHHH", 0x0421, 2, 4, 0, 0)
    methods = struct.pack(">H", int(method))
    if method:
        methods += struct.pack(">HHHH", 0x0401, 5, 6, 0)
    return header + declaration + methods + b"\x00\x00"


class SourceSelectionTest(unittest.TestCase):
    def test_spark42_fetches_no_fat_jar(self):
        fetch = load("iceberg/fetch-inputs.py")
        content = b"pinned source"
        digest = hashlib.sha256(content).hexdigest()
        with TemporaryDirectory() as directory, patch.object(fetch, "inputs", {
            "iceberg-source.tar.gz": ("https://official.example/source", digest),
            "gradle-wrapper.jar": ("https://official.example/wrapper", digest),
            "iceberg-runtime-original.jar": ("https://official.example/oracle", digest),
        }), patch.object(fetch.urllib.request, "urlopen", side_effect=lambda *a, **k: io.BytesIO(content)) as request:
            destination = Path(directory)
            fetch.fetch_inputs(destination, source_only=True)
            self.assertEqual(request.call_count, 2)
            self.assertFalse((destination / "iceberg-runtime-original.jar").exists())

    def test_spark42_changes_only_pinned_build_versions(self):
        module = load("iceberg/patch-spark42-source.py")
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "gradle").mkdir()
            catalog = root / "gradle/libs.versions.toml"
            catalog.write_text("\n".join(module.REPLACEMENTS) + '\nkeep = "unchanged"\n')
            module.patch_source(root)
            for value in module.REPLACEMENTS.values():
                self.assertIn(value, catalog.read_text())
            self.assertIn('keep = "unchanged"', catalog.read_text())
            with self.assertRaises(RuntimeError):
                module.patch_source(root)

    def test_parquet_matches_spark_and_iceberg_versions(self):
        sources = json.loads((HELPERS / "parquet/sources.json").read_text())
        self.assertEqual([source["version"] for source in sources], ["1.17.0", "1.17.1"])
        self.assertEqual(sources[0]["algorithm"], "sha512")
        self.assertEqual(len(sources[0]["digest"]), 128)

    def test_parquet_uses_the_same_verified_maven_as_the_shared_builder(self):
        module = load("parquet/verify-toolchain.py")
        spark = json.loads((HELPERS / "spark/inputs.json").read_text())["maven"]
        self.assertEqual(module.TOOLCHAIN["version"], spark["version"])
        self.assertEqual(module.TOOLCHAIN["archive_sha512"], spark["sha512"])
        fetch = load("parquet/fetch-inputs.py")
        self.assertFalse(any("maven" in name for name, *_ in fetch.INPUTS))
        good = subprocess.CompletedProcess([], 0, "Apache Maven 3.9.15 (publisher build)\nJava version: 21.0.13\n", "")
        with patch.object(module.subprocess, "run", return_value=good):
            record = module.verify_toolchain(Path(module.TOOLCHAIN["executable"]))
        self.assertEqual(record["actual_maven_version"], "3.9.15")
        self.assertIn("independently checks", record["archive_checksum_verification"])

    def test_parquet_rejects_an_older_actual_maven_executable(self):
        module = load("parquet/verify-toolchain.py")
        old = subprocess.CompletedProcess([], 0, "Apache Maven 3.9.11\n", "")
        with patch.object(module.subprocess, "run", return_value=old), self.assertRaises(RuntimeError):
            module.verify_toolchain(Path(module.TOOLCHAIN["executable"]))

    def test_parquet_1170_uses_real_annotation_version_and_preserves_shade_rules(self):
        module = load("parquet/prepare-source.py")
        original = """<project><artifactId>parquet-jackson</artifactId><properties></properties>
<artifactId>jackson-annotations</artifactId>
      <version>${jackson.version}</version>
<relocation><pattern>com.fasterxml</pattern><shadedPattern>shaded.parquet.com.fasterxml</shadedPattern></relocation>
<profiles><profile><properties><shade.createSourcesJar>true</shade.createSourcesJar></properties></profile></profiles></project>"""
        result = module.patch_module(original, "1.17.0")
        self.assertIn("<jackson-annotations.version>2.21</jackson-annotations.version>", result)
        self.assertIn("<version>${jackson-annotations.version}</version>", result)
        self.assertIn("1.17.0-d2c-jackson2.21.7", result)
        self.assertIn(original[original.index("<relocation>"):], result)
        with self.assertRaises(RuntimeError):
            module.patch_module("<project/>", "1.17.0")


class RuntimeVerifierTest(unittest.TestCase):
    def setUp(self):
        self.module = load("iceberg/verify-source-runtime.py")

    def fixture(self, root):
        upstream = root / "upstream"
        runtime = upstream / "spark/v4.2/spark-runtime"
        runtime.mkdir(parents=True)
        for name in ("LICENSE", "NOTICE"):
            (runtime / name).write_text("Apache " + name)
        (upstream / "spark/v4.2/build.gradle").write_text(
            "relocate 'com.fasterxml', 'org.apache.iceberg.shaded.com.fasterxml'\n")
        entries = {name: class_file(name[:-6]) for name in self.module.ENTRY_POINTS}
        entries["META-INF/maven/example/library/pom.properties"] = b"version=1.0\n"
        entries["licenses/LICENSE.txt"] = b"dependency license"
        input_jar = root / "source-input.jar"
        with ZipFile(input_jar, "w") as jar:
            for name, value in entries.items():
                jar.writestr(name, value)
        inputs = [{"coordinate": "org.apache.iceberg:" + name + ":1.12.0",
                   "path": str(input_jar), "sha256": hashlib.sha256(input_jar.read_bytes()).hexdigest(),
                   "source_project": True} for name in ("iceberg-spark-4.2_2.13", "iceberg-spark-extensions-4.2_2.13")]
        entries.update({name: b"version bytecode" for name in self.module.JACKSON_CLASSES})
        entries.update({name: (runtime / name).read_bytes() for name in ("LICENSE", "NOTICE")})
        return upstream, inputs, entries

    def run_verifier(self, root, upstream, inputs, entries):
        rebuilt = root / "rebuilt.jar"
        with ZipFile(rebuilt, "w") as jar:
            for name, value in entries.items():
                jar.writestr(name, value)
        java = subprocess.CompletedProcess([], 0, '{"status":"PASS"}\n{"status":"PASS"}\n', "")
        with patch.object(self.module.subprocess, "run", return_value=java):
            return self.module.verify(inputs, rebuilt, upstream)

    def test_preserved_source_inputs_pass_and_no_published_oracle_is_claimed(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            upstream, inputs, entries = self.fixture(root)
            report = self.run_verifier(root, upstream, inputs, entries)
            self.assertEqual(report["status"], "PASS")
            self.assertIn("no published Spark 4.2 fat JAR oracle", report["verification_basis"])

    def test_removed_public_method_fails_even_if_class_name_survives(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            upstream, inputs, entries = self.fixture(root)
            name = "org/apache/iceberg/Table.class"
            entries[name] = class_file(name[:-6], method=False)
            report = self.run_verifier(root, upstream, inputs, entries)
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["classes"]["public_protected_api_changed"], [name])

    def test_license_named_class_uses_api_checks_and_does_not_count_as_a_document(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            upstream, inputs, entries = self.fixture(root)
            name = "org/apache/iceberg/License.class"
            original = class_file(name[:-6], descriptor="()Lcom/fasterxml/jackson/JsonNode;")
            input_jar = Path(inputs[0]["path"])
            with ZipFile(input_jar, "a") as jar:
                jar.writestr(name, original)
            for artifact in inputs:
                artifact["sha256"] = hashlib.sha256(input_jar.read_bytes()).hexdigest()
            entries[name] = class_file(name[:-6], descriptor="()Lorg/apache/iceberg/shaded/com/fasterxml/jackson/JsonNode;")
            report = self.run_verifier(root, upstream, inputs, entries)
            self.assertEqual(report["status"], "PASS")
            self.assertEqual(report["input_license_notice_entries_checked"], 1)
            entries[name] = class_file(name[:-6], method=False)
            report = self.run_verifier(root, upstream, inputs, entries)
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["classes"]["public_protected_api_changed"], [name])

    def test_dropped_metadata_or_licenses_fail(self):
        for name in ("META-INF/maven/example/library/pom.properties", "licenses/LICENSE.txt", "LICENSE"):
            with self.subTest(name=name), TemporaryDirectory() as directory:
                root = Path(directory)
                upstream, inputs, entries = self.fixture(root)
                entries.pop(name)
                self.assertEqual(self.run_verifier(root, upstream, inputs, entries)["status"], "FAIL")

    def test_extra_nested_jackson_layer_fails(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            upstream, inputs, entries = self.fixture(root)
            entries["hidden/com/fasterxml/jackson/core/json/PackageVersion.class"] = b"old"
            report = self.run_verifier(root, upstream, inputs, entries)
            self.assertFalse(report["complete_jackson_package_version_inventory"])
            self.assertEqual(report["status"], "FAIL")

    def test_upstream_relocation_changes_no_api_contract(self):
        original = class_file("org/apache/iceberg/Table", descriptor="()Lcom/fasterxml/jackson/JsonNode;")
        shaded = class_file("org/apache/iceberg/Table", descriptor="()Lorg/apache/iceberg/shaded/com/fasterxml/jackson/JsonNode;")
        self.assertEqual(self.module.public_api(original), self.module.public_api(
            shaded, [("com.fasterxml", "org.apache.iceberg.shaded.com.fasterxml")]))


if __name__ == "__main__":
    unittest.main()
