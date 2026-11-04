"""Offline fixtures for reviewed Parquet resources and their JAR preservation."""

import hashlib
import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from zipfile import ZipFile


HELPERS = Path(__file__).resolve().parents[1] / "container-build/lakehouse/parquet"
SPEC = importlib.util.spec_from_file_location("parquet_source_recipe", HELPERS / "prepare-source.py")
recipe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(recipe)


class ParquetSourceRecipeTests(unittest.TestCase):
    def setUp(self):
        scratch = TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        self.module = self.root / "sources/parquet-1.17.0/parquet-jackson"
        self.license = self.module / "src/main/resources/META-INF/LICENSE"
        self.license.parent.mkdir(parents=True)
        self.license.write_bytes(b"Fixture original Apache/Jackson license\n")
        self.source = {
            "version": "1.17.0",
            "main_files_sha256": {"resources/META-INF/LICENSE": hashlib.sha256(self.license.read_bytes()).hexdigest()},
        }
        (self.module.parent / "pom.xml").write_text(
            '<project xmlns="http://maven.apache.org/POM/4.0.0"><properties>'
            '<shade.prefix>shaded.parquet</shade.prefix></properties></project>')
        self.prefix = "shaded/parquet/com/fasterxml/jackson/"
        self.entries = {
            "META-INF/LICENSE": self.license.read_bytes(),
            self.prefix + "core/json/PackageVersion.class": b"\xca\xfe\xba\xbefixture version 2.21.7",
            self.prefix + "databind/cfg/PackageVersion.class": b"\xca\xfe\xba\xbefixture version 2.21.7",
            self.prefix + "annotation/JsonProperty.class": b"\xca\xfe\xba\xbefixture annotation",
        }
        for coordinate, version in {
            "org.apache.parquet/parquet-jackson": "1.17.0-d2c-jackson2.21.7",
            "com.fasterxml.jackson.core/jackson-core": "2.21.7",
            "com.fasterxml.jackson.core/jackson-databind": "2.21.7",
            "com.fasterxml.jackson.core/jackson-annotations": "2.21",
        }.items():
            group, artifact = coordinate.split("/", 1)
            self.entries[f"META-INF/maven/{coordinate}/pom.properties"] = f"groupId={group}\nartifactId={artifact}\nversion={version}\n".encode()

    def verify_jar(self):
        jar = self.root / "fixture.jar"
        with ZipFile(jar, "w") as archive:
            for name, content in self.entries.items():
                archive.writestr(name, content)
        return recipe.verify_bundle(self.module, jar, self.source)

    def test_both_pinned_releases_declare_the_reviewed_license_only(self):
        sources = json.loads((HELPERS / "sources.json").read_text())
        expected = {"resources/META-INF/LICENSE": "c188e919587b5b8c99ecebd872b93de6676480a7d5507d328156a0c083b5f48e"}
        self.assertEqual([source["version"] for source in sources], ["1.17.0", "1.17.1"])
        self.assertTrue(all(source["main_files_sha256"] == expected for source in sources))

    def test_original_main_license_is_accepted_and_not_modified(self):
        original = self.license.read_bytes()
        self.assertEqual(recipe.validate_main_inventory(self.module, self.source), self.source["main_files_sha256"])
        self.assertEqual(self.license.read_bytes(), original)

    def test_new_java_implementation_is_still_rejected(self):
        java = self.module / "src/main/java/example/Implementation.java"
        java.parent.mkdir(parents=True)
        java.write_text("package example; public class Implementation {}")
        with self.assertRaisesRegex(RuntimeError, "inventory changed"):
            recipe.validate_main_inventory(self.module, self.source)

    def test_unreviewed_resource_is_still_rejected(self):
        (self.license.parent / "NOTICE").write_text("Unexpected resource")
        with self.assertRaisesRegex(RuntimeError, "inventory changed"):
            recipe.validate_main_inventory(self.module, self.source)

    def test_missing_or_changed_license_is_rejected(self):
        self.license.write_text("Changed license")
        with self.assertRaisesRegex(RuntimeError, "inventory changed"):
            recipe.validate_main_inventory(self.module, self.source)
        self.license.unlink()
        with self.assertRaisesRegex(RuntimeError, "inventory changed"):
            recipe.validate_main_inventory(self.module, self.source)

    def test_symlinked_main_entry_is_rejected(self):
        (self.license.parent / "linked").symlink_to(self.license)
        with self.assertRaisesRegex(RuntimeError, "Symlink"):
            recipe.validate_main_inventory(self.module, self.source)

    def test_prepare_preserves_license_and_original_build_rules(self):
        pom = self.module / "pom.xml"
        build_rules = '<build><plugins><plugin><artifactId>maven-shade-plugin</artifactId></plugin></plugins></build>'
        pom.write_text('<project><artifactId>parquet-jackson</artifactId><properties></properties>'
                       '<dependencies><dependency><artifactId>jackson-annotations</artifactId>\n'
                       '      <version>${jackson.version}</version></dependency></dependencies>'
                       + build_rules + '</project>')
        archive = self.root / "fixture-archive"
        archive.write_bytes(b"small verified source fixture")
        self.source.update(archive=str(archive), algorithm="sha256", digest=hashlib.sha256(archive.read_bytes()).hexdigest())
        original = self.license.read_bytes()
        with patch.object(recipe.subprocess, "run") as extraction:
            recipe.prepare(self.source, self.root / "sources")
        extraction.assert_called_once()
        self.assertEqual(self.license.read_bytes(), original)
        self.assertIn(build_rules, pom.read_text())
        self.assertIn("<version>1.17.0-d2c-jackson2.21.7</version>", pom.read_text())

    def test_packaged_license_metadata_and_implementation_markers_pass(self):
        result = self.verify_jar()
        self.assertEqual(result["status"], "PASS")
        self.assertTrue(result["source_resource_byte_parity"])
        self.assertEqual(result["source_main_files_sha256"], self.source["main_files_sha256"])
        self.assertEqual(len(result["checked_actual_package_version_classes"]), 2)

    def test_packaged_license_deletion_or_change_is_rejected(self):
        original = self.entries.pop("META-INF/LICENSE")
        with self.assertRaisesRegex(RuntimeError, "resource missing or changed"):
            self.verify_jar()
        self.entries["META-INF/LICENSE"] = original + b"changed"
        with self.assertRaisesRegex(RuntimeError, "resource missing or changed"):
            self.verify_jar()

    def test_missing_maven_metadata_is_rejected(self):
        self.entries.pop("META-INF/maven/com.fasterxml.jackson.core/jackson-core/pom.properties")
        with self.assertRaisesRegex(RuntimeError, "metadata missing"):
            self.verify_jar()

    def test_old_jackson_bytes_fail_even_when_metadata_says_new_version(self):
        self.entries[self.prefix + "core/json/PackageVersion.class"] = b"\xca\xfe\xba\xbefixture version 2.21.3"
        with self.assertRaisesRegex(RuntimeError, "actual shaded Jackson"):
            self.verify_jar()

    def test_extra_jackson_layer_is_rejected(self):
        self.entries["hidden/com/fasterxml/jackson/core/json/PackageVersion.class"] = b"\xca\xfe\xba\xbefixture version 2.21.3"
        with self.assertRaisesRegex(RuntimeError, "Unexpected Jackson implementation layer"):
            self.verify_jar()


if __name__ == "__main__":
    unittest.main()
