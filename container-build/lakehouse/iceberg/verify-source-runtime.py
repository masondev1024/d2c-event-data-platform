"""Check source-built runtime APIs, licenses, metadata and both Jackson layers."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import struct
import subprocess
from zipfile import ZipFile


ENTRY_POINTS = {
    "org/apache/iceberg/Table.class",
    "org/apache/iceberg/hadoop/HadoopCatalog.class",
    "org/apache/iceberg/spark/SparkCatalog.class",
    "org/apache/iceberg/spark/source/IcebergSource.class",
    "org/apache/iceberg/spark/extensions/IcebergSparkSessionExtensions.class",
}
JACKSON_CLASSES = {
    prefix + "/" + suffix
    for prefix in (
        "org/apache/iceberg/shaded/com/fasterxml/jackson",
        "org/apache/iceberg/shaded/org/apache/parquet/shaded/com/fasterxml/jackson",
    )
    for suffix in ("core/json/PackageVersion.class", "databind/cfg/PackageVersion.class")
}


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def application_class(name):
    return name.endswith(".class") and (
        (name.startswith("org/apache/iceberg/")
         and not name.startswith("org/apache/iceberg/shaded/"))
        or name.startswith("org/apache/spark/")
    )


def public_api(content, relocations=()):
    """Read public/protected JVM declarations, undoing upstream shade mappings."""
    position = 0

    def take(size):
        nonlocal position
        result = content[position:position + size]
        if len(result) != size:
            raise ValueError("Truncated class file")
        position += size
        return result

    def number(size):
        return int.from_bytes(take(size), "big")

    def normalize(text):
        for original, relocated in sorted(relocations, key=lambda pair: -len(pair[1])):
            text = text.replace(relocated.replace(".", "/"), original.replace(".", "/"))
        return text

    if take(4) != b"\xca\xfe\xba\xbe":
        raise ValueError("Not a JVM class file")
    take(4)
    pool = [None] * number(2)
    index = 1
    sizes = {3: 4, 4: 4, 5: 8, 6: 8, 8: 2, 9: 4, 10: 4, 11: 4,
             12: 4, 15: 3, 16: 2, 17: 4, 18: 4, 19: 2, 20: 2}
    while index < len(pool):
        tag = number(1)
        if tag == 1:
            pool[index] = take(number(2)).decode("utf-8", errors="replace")
        elif tag == 7:
            pool[index] = number(2)
        elif tag in sizes:
            take(sizes[tag])
        else:
            raise ValueError(f"Unknown constant pool tag {tag}")
        index += 2 if tag in (5, 6) else 1

    def name(index):
        return normalize(pool[pool[index]]) if index else None

    def attributes():
        signature = None
        for _ in range(number(2)):
            label, data = pool[number(2)], take(number(4))
            if label == "Signature":
                signature = normalize(pool[struct.unpack(">H", data)[0]])
        return signature

    flags, owner, parent = number(2), number(2), number(2)
    interfaces = sorted(name(number(2)) for _ in range(number(2)))
    members = []
    for kind in ("field", "method"):
        for _ in range(number(2)):
            access, member, descriptor = number(2), pool[number(2)], pool[number(2)]
            signature = attributes()
            if access & 0x0005:
                members.append((kind, access, member, normalize(descriptor), signature))
    signature = attributes()
    if position != len(content):
        raise ValueError("Unexpected trailing class data")
    return (flags, name(owner), name(parent), interfaces, signature, sorted(members))


def verify(inputs, rebuilt_path, upstream, java="java"):
    source_build = upstream / "spark/v4.2/build.gradle"
    relocations = re.findall(r"relocate\s+'([^']+)',\s*'([^']+)'", source_build.read_text())
    if not relocations:
        raise RuntimeError("Pinned upstream runtime has no relocation rules")
    expected, expected_metadata, expected_licenses = {}, {}, {}
    for artifact in inputs:
        path = Path(artifact["path"])
        if sha256(path.read_bytes()) != artifact["sha256"]:
            raise RuntimeError(f"Runtime input checksum changed: {artifact['coordinate']}")
        with ZipFile(path) as jar:
            for name in jar.namelist():
                if application_class(name):
                    api = public_api(jar.read(name))
                    if name in expected and expected[name] != api:
                        raise RuntimeError(f"Conflicting input API class: {name}")
                    expected[name] = api
                if name.startswith("META-INF/maven/") and name.endswith(("/pom.xml", "/pom.properties")):
                    expected_metadata[name] = sha256(jar.read(name))
                basename = name.rsplit("/", 1)[-1].upper()
                if (basename.startswith(("LICENSE", "NOTICE")) and not name.endswith("/")
                        and not name.endswith(".class")
                        and name not in ("LICENSE", "NOTICE")
                        and not name.startswith(("META-INF/LICENSE", "META-INF/NOTICE"))):
                    expected_licenses[name] = sha256(jar.read(name))
    coordinates = {artifact["coordinate"] for artifact in inputs if artifact.get("source_project")}
    source_modules = {
        "org.apache.iceberg:iceberg-spark-4.2_2.13:1.12.0",
        "org.apache.iceberg:iceberg-spark-extensions-4.2_2.13:1.12.0",
    }
    with ZipFile(rebuilt_path) as jar:
        names = set(jar.namelist())
        actual = {name for name in names if application_class(name)}
        changed = sorted(name for name in actual & expected.keys()
                         if public_api(jar.read(name), relocations) != expected[name])
        metadata_missing = sorted(name for name, digest in expected_metadata.items()
                                  if name not in names or sha256(jar.read(name)) != digest)
        license_missing = sorted(name for name, digest in expected_licenses.items()
                                 if name not in names or sha256(jar.read(name)) != digest)
        root_licenses = all(name in names and jar.read(name) ==
                            (upstream / "spark/v4.2/spark-runtime" / name).read_bytes()
                            for name in ("LICENSE", "NOTICE"))
        jackson = {name for name in names if
                   name.endswith("/com/fasterxml/jackson/core/json/PackageVersion.class")
                   or name.endswith("/com/fasterxml/jackson/databind/cfg/PackageVersion.class")}
    java_source = Path(__file__).with_name("VerifyIcebergShadedJackson.java")
    runtime = subprocess.run([java, "--class-path", str(rebuilt_path.resolve()), str(java_source)],
                             capture_output=True, text=True, timeout=120)
    checks = [json.loads(line) for line in runtime.stdout.splitlines() if line.startswith("{")]
    passed = (bool(expected) and actual == expected.keys() and not changed
              and ENTRY_POINTS <= actual and source_modules <= coordinates
              and not metadata_missing and not license_missing and root_licenses
              and jackson == JACKSON_CLASSES and runtime.returncode == 0 and len(checks) == 2)
    return {
        "status": "PASS" if passed else "FAIL",
        "verification_basis": "Actual source-built runtime inputs; no published Spark 4.2 fat JAR oracle exists at the checked coordinates",
        "rebuilt_sha256": sha256(rebuilt_path.read_bytes()),
        "classes": {"input_count": len(expected), "rebuilt_count": len(actual),
                    "missing": sorted(expected.keys() - actual), "added": sorted(actual - expected.keys()),
                    "public_protected_api_changed": changed,
                    "required_entry_points_missing": sorted(ENTRY_POINTS - actual)},
        "required_source_modules_missing": sorted(source_modules - coordinates),
        "root_license_notice_byte_parity": root_licenses,
        "input_license_notice_entries_checked": len(expected_licenses),
        "input_license_notice_entries_missing_or_changed": license_missing,
        "input_maven_metadata_entries_checked": len(expected_metadata),
        "input_maven_metadata_entries_missing_or_changed": metadata_missing,
        "complete_jackson_package_version_inventory": jackson == JACKSON_CLASSES,
        "runtime": {"exit_code": runtime.returncode, "checks": checks,
                    "stdout": runtime.stdout, "stderr": runtime.stderr},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--rebuilt", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--java", default="java")
    args = parser.parse_args()
    inputs = json.loads(args.inputs.read_text())
    report = verify(inputs, args.rebuilt, args.upstream, args.java)
    args.artifacts.mkdir(parents=True, exist_ok=True)
    (args.artifacts / "iceberg-rebuild-verification.json").write_text(json.dumps(report, indent=2) + "\n")
    provenance = {
        "component": "iceberg-spark-runtime-4.2_2.13", "upstream_version": "1.12.0",
        "custom_version": "1.12.0-d2c-security1", "spark_compile_version": "4.2.1",
        "source_tag": "apache-iceberg-1.12.0",
        "source_commit": "212aa894e919ff9fdf89160da4aadcbe6858ac01",
        "source_url": "https://codeload.github.com/apache/iceberg/tar.gz/refs/tags/apache-iceberg-1.12.0",
        "source_sha256": "6dfa5e1e5a47061b070a9c265f8006afa3bf5e549e129700375f91c290921811",
        "assembly": "Original upstream Spark 4.2 Gradle source projects and shadowJar task",
        "upstream_runtime_gradle_sha256": sha256((args.upstream / "spark/v4.2/build.gradle").read_bytes()),
        "patched_version_catalog_sha256": sha256((args.upstream / "gradle/libs.versions.toml").read_bytes()),
        "gradle_version": "8.14.5", "shadow_plugin": "com.gradleup.shadow:shadow-gradle-plugin:8.3.11",
        "gradle_distribution_sha256": "6f74b601422d6d6fc4e1f9a1ab6522f642c2fdcbc15ae33ebd30ba3d7198e854",
        "gradle_wrapper_sha256": "7d3a4ac4de1c32b59bc6a4eb8ecb8e612ccd0cf1ae1e99f66902da64df296172",
        "patched_dependencies": {"jackson-core": "2.21.7", "jackson-databind": "2.21.7",
                                 "jackson-annotations": "2.21", "jackson-bom": "2.21.7",
                                 "parquet-jackson": "1.17.1-d2c-jackson2.21.7",
                                 "at.yawk.lz4:lz4-java": "1.11.4"},
        "original_fat_jar_usage": "none", "binary_edits": False,
        "verification_status": report["status"], "rebuilt_sha256": report["rebuilt_sha256"],
    }
    (args.artifacts / "iceberg-source-provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
