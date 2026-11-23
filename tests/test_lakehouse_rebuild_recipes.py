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


def write_jar(path, entries):
    with ZipFile(path, "w") as jar:
        for name, value in entries.items():
            jar.writestr(name, value)


def artifact(path, coordinate, source_project=False):
    return {"coordinate": coordinate, "filename": path.name, "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "source_project": source_project}


def guava_fixture(root, module):
    inputs, bundled = [], {}
    for name, version, classes in (
        ("guava", module.GUAVA_VERSION,
         ("base/Preconditions", "collect/ImmutableList", "collect/CompactHashMap",
          "collect/CompactHashMap$EntrySetView", "collect/CompactHashSet", "collect/MapMakerInternalMap")),
        ("failureaccess", module.FAILUREACCESS_VERSION,
         ("util/concurrent/internal/InternalFutureFailureAccess",)),
    ):
        metadata = f"META-INF/maven/com.google.guava/{name}/pom.properties"
        entries = {metadata: f"groupId=com.google.guava\nartifactId={name}\nversion={version}\n".encode()}
        bundled[metadata] = entries[metadata]
        pom = metadata.replace("pom.properties", "pom.xml")
        entries[pom] = f"<project><groupId>com.google.guava</groupId><artifactId>{name}</artifactId><version>{version}</version></project>".encode()
        bundled[pom] = entries[pom]
        for suffix in classes:
            owner = "com/google/common/" + suffix
            entries[owner + ".class"] = class_file(owner)
            relocated = module.GUAVA_PREFIX + suffix
            bundled[relocated + ".class"] = class_file(relocated)
        path = root / f"{name}-{version}.jar"
        write_jar(path, entries)
        inputs.append(artifact(path, f"com.google.guava:{name}:{version}"))
    bundle = root / "iceberg-bundled-guava-1.12.0.jar"
    write_jar(bundle, bundled)
    return artifact(bundle, "org.apache.iceberg:iceberg-bundled-guava:1.12.0", True), inputs, bundled


def subprocess_fixture(command, **_kwargs):
    if Path(command[0]).name == "javap":
        return subprocess.CompletedProcess(command, 0, "fixed private code #123 // Method java/util/Map.put\n", "")
    closure = [index for index, value in enumerate(command) if str(value).endswith("VerifyIcebergGuavaClosure.java")]
    if closure:
        with ZipFile(command[closure[0] + 3]) as jar:
            count = sum(name.endswith(".class") for name in jar.namelist())
        return subprocess.CompletedProcess(command, 0, json.dumps({"status": "PASS", "class_count": count,
            "complete_canonical_class_sha256": "a" * 64, "jdk_version": "fixture-only"}), "")
    return subprocess.CompletedProcess(command, 0, '{"status":"PASS"}\n{"status":"PASS"}\n', "")


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
        bundle, self.guava_inputs, guava_entries = guava_fixture(root, self.module)
        inputs.append(bundle)
        entries.update(guava_entries)
        entries.update({name: b"version bytecode" for name in self.module.JACKSON_CLASSES})
        entries.update({name: (runtime / name).read_bytes() for name in ("LICENSE", "NOTICE")})
        return upstream, inputs, entries

    def run_verifier(self, root, upstream, inputs, entries):
        rebuilt = root / "rebuilt.jar"
        with ZipFile(rebuilt, "w") as jar:
            for name, value in entries.items():
                jar.writestr(name, value)
        with patch.object(self.module.subprocess, "run", side_effect=subprocess_fixture):
            return self.module.verify(inputs, rebuilt, upstream, guava_inputs=self.guava_inputs)

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
            for artifact in inputs[:2]:
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


class NestedGuavaVerifierTest(unittest.TestCase):
    def setUp(self):
        self.module = load("iceberg/verify-source-runtime.py")
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        bundle, self.guava_inputs, self.entries = guava_fixture(self.root, self.module)
        self.inputs = [bundle]
        self.rebuilt = self.root / "runtime.jar"

    def verify(self, *, subprocess_effect=subprocess_fixture):
        write_jar(self.rebuilt, self.entries)
        with patch.object(self.module.subprocess, "run", side_effect=subprocess_effect):
            return self.module.verify_guava(self.inputs, self.guava_inputs, self.rebuilt)

    def rewrite_bundle(self, entries):
        path = Path(self.inputs[0]["path"])
        write_jar(path, entries)
        self.inputs[0]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()

    def test_fixed_actual_inputs_and_final_byte_closure_pass(self):
        proof = self.verify()
        self.assertEqual(proof["status"], "PASS")
        self.assertEqual(proof["version"], "33.7.2-jre")
        self.assertEqual(proof["failureaccess_version"], "1.0.3")
        self.assertEqual(proof["retained_class_count"], 7)
        self.assertEqual(len(proof["cve_affected_retained_classes"]), 4)
        self.assertEqual(len(proof["cve_affected_implementation_disassembly_sha256"]), 64)
        self.assertIn("not an independently obtained publisher checksum", proof["limits"])

    def test_missing_actual_nested_receipt_or_changed_input_hash_fails(self):
        for records in (None, [], [{**self.guava_inputs[0], "sha256": "0" * 64}, self.guava_inputs[1]]):
            with self.subTest(records=records), self.assertRaises(RuntimeError):
                self.module.verify_guava(self.inputs, records, self.rebuilt)

    def test_old_selected_version_or_changed_maven_identity_fails(self):
        self.guava_inputs[0]["coordinate"] = "com.google.guava:guava:33.7.1-jre"
        with self.assertRaisesRegex(RuntimeError, "fixed Guava"):
            self.verify()
        self.guava_inputs[0]["coordinate"] = "com.google.guava:guava:33.7.2-jre"
        path = Path(self.guava_inputs[0]["path"])
        with ZipFile(path) as jar:
            entries = {name: jar.read(name) for name in jar.namelist()}
        entries[self.module.GUAVA_METADATA] = entries[self.module.GUAVA_METADATA].replace(b"33.7.2", b"33.7.1")
        write_jar(path, entries)
        self.guava_inputs[0]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        with self.assertRaisesRegex(RuntimeError, "Maven identity"):
            self.verify()

    def test_edited_final_code_or_new_namespace_fails_even_with_fixed_metadata(self):
        name = self.module.GUAVA_PREFIX + "collect/CompactHashMap.class"
        original = self.entries[name]
        self.entries[name] = class_file(name[:-6], descriptor="()I")
        with self.assertRaisesRegex(RuntimeError, "class payload mismatch"):
            self.verify()
        self.entries[name] = original
        self.entries["hidden/com/google/common/collect/CompactHashMap.class"] = original
        with self.assertRaisesRegex(RuntimeError, "class closure"):
            self.verify()

    def test_old_implementation_behind_matching_metadata_and_public_api_fails(self):
        calls = []
        def different_code(command, **kwargs):
            calls.append(command)
            result = subprocess_fixture(command, **kwargs)
            if Path(command[command.index("-classpath") + 1]).name.startswith("iceberg-bundled"):
                result.stdout = result.stdout.replace("fixed private code", "vulnerable private code")
            return result
        with self.assertRaisesRegex(RuntimeError, "CVE-affected retained Guava implementation"):
            self.verify(subprocess_effect=different_code)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all("-p" in command and "-c" in command for command in calls))
        self.assertTrue(any("CompactHashMap$EntrySetView" in name for name in calls[0]))

    def test_disassembly_normalizes_only_shade_names_and_constant_pool_indexes(self):
        outputs = [
            "class com.google.common.collect.CompactHashMap {\n    Code:\n 4: invokevirtual #12 // Method com/google/common/Foo.bar:()I\n}",
            "class org.apache.iceberg.relocated.com.google.common.collect.CompactHashMap {\n    Code:\n 4: invokevirtual #456 // Method org/apache/iceberg/relocated/com/google/common/Foo.bar:()I\n}",
        ]
        def disassembly(command, **kwargs):
            return subprocess.CompletedProcess(command, 0, outputs.pop(0), "")
        with patch.object(self.module.subprocess, "run", side_effect=disassembly):
            before = self.module.normalized_disassembly(self.rebuilt, ["before.class"], "javap")
            after = self.module.normalized_disassembly(self.rebuilt, ["after.class"], "javap")
        self.assertEqual(before, after)
        self.assertIn("L0: invokevirtual #CP", before)

    def test_shade_widened_constant_load_preserves_control_flow_but_not_literal_values(self):
        before = """class Example {
    Code:
       0: ldc #12 // String fixed #123
       2: ifnonnull 8
       5: goto 9
       8: iconst_1
       9: ireturn
      Exception table:
         from to target type
             0 8 8 Class java/lang/Exception
}"""
        after = before.replace("ldc #12", "ldc_w #456").replace("2: ifnonnull 8", "3: ifnonnull 9").replace("5: goto 9", "6: goto 10").replace("8: iconst_1", "9: iconst_1").replace("9: ireturn", "10: ireturn").replace("0 8 8 Class", "0 9 9 Class")
        self.assertEqual(self.module.normalize_javap_output(before), self.module.normalize_javap_output(after))
        self.assertNotEqual(self.module.normalize_javap_output(before),
                            self.module.normalize_javap_output(after.replace("fixed #123", "fixed #456")))
        self.assertNotEqual(self.module.normalize_javap_output(before),
                            self.module.normalize_javap_output(after.replace("iconst_1", "iconst_2")))

    def test_original_upstream_minimize_is_preserved_but_cannot_drop_final_classes(self):
        optional = self.module.GUAVA_PREFIX + "collect/CompactHashSet.class"
        self.entries.pop(optional)
        self.rewrite_bundle(self.entries)
        proof = self.verify()
        self.assertEqual(proof["upstream_minimized_class_count"], 1)
        self.assertNotIn(optional, proof["cve_affected_retained_classes"])
        self.entries.pop(self.module.GUAVA_PREFIX + "collect/ImmutableList.class")
        with self.assertRaisesRegex(RuntimeError, "class closure"):
            self.verify()

    def test_original_empty_listenablefuture_is_allowed_only_without_class_payload(self):
        path = self.root / "listenablefuture.jar"
        write_jar(path, {"META-INF/NOTICE": b"empty conflict avoidance dependency"})
        self.guava_inputs.append(artifact(path, "com.google.guava:listenablefuture:9999.0-empty-to-avoid-conflict-with-guava"))
        self.assertEqual(len(self.verify()["empty_listenablefuture_input"]), 1)
        write_jar(path, {"com/google/common/util/concurrent/ListenableFuture.class": b"old payload"})
        self.guava_inputs[-1]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        with self.assertRaisesRegex(RuntimeError, "empty listenablefuture"):
            self.verify()

    def test_javap_failure_is_not_reported_as_a_verified_implementation(self):
        for code, stdout, stderr in ((1, "", "missing"), (0, "", ""), (0, "body", "warning")):
            with self.subTest(code=code, stderr=stderr), self.assertRaises(RuntimeError):
                self.verify(subprocess_effect=lambda command, **kwargs: subprocess.CompletedProcess(command, code, stdout, stderr))

    def test_complete_class_verification_cannot_be_skipped_or_replaced_by_an_invalid_receipt(self):
        for code, payload, stderr in ((1, {"status": "PASS", "class_count": 7}, "mismatch"),
                                      (0, {"status": "FAIL", "class_count": 7}, ""),
                                      (0, {"status": "PASS", "class_count": 0}, ""),
                                      (0, {"status": "PASS", "class_count": 7}, ""),
                                      (0, None, "")):
            def invalid_closure(command, **kwargs):
                if any(str(value).endswith("VerifyIcebergGuavaClosure.java") for value in command):
                    return subprocess.CompletedProcess(command, code, json.dumps(payload), stderr)
                return subprocess_fixture(command, **kwargs)
            with self.subTest(code=code, payload=payload), self.assertRaises(RuntimeError):
                self.verify(subprocess_effect=invalid_closure)


if __name__ == "__main__":
    unittest.main()
