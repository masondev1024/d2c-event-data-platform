"""Check source-built runtime APIs, licenses, Jackson and nested Guava inputs."""
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
GUAVA_VERSION = "33.7.2-jre"
FAILUREACCESS_VERSION = "1.0.3"
GUAVA_PREFIX = "org/apache/iceberg/relocated/com/google/common/"
GUAVA_METADATA = "META-INF/maven/com.google.guava/guava/pom.properties"
GUAVA_RELOCATION = ("com.google.common", "org.apache.iceberg.relocated.com.google.common")
GUAVA_SECURITY_FAMILIES = ("CompactHashMap", "CompactHashSet", "MapMakerInternalMap")


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def checked_inputs(artifacts, label):
    if not isinstance(artifacts, list) or not artifacts:
        raise RuntimeError(f"Missing actual {label} artifact receipt")
    for artifact in artifacts:
        path = Path(artifact["path"])
        if not path.is_file() or path.is_symlink() or sha256(path.read_bytes()) != artifact["sha256"]:
            raise RuntimeError(f"{label} input checksum changed: {artifact['coordinate']}")
    return artifacts


def maven_properties(content):
    return dict(line.split("=", 1) for line in content.decode().splitlines()
                if line.strip() and not line.startswith(("#", "!")))


def guava_class(name):
    return name.endswith(".class") and (name.startswith((GUAVA_PREFIX, "com/google/thirdparty/"))
                                        or "/com/google/common/" in name
                                        or name.startswith("com/google/common/"))


def normalized_disassembly(path, names, javap):
    result = subprocess.run([javap, "-classpath", str(path.resolve()), "-c", "-p", "-constants",
                             *(name[:-6].replace("/", ".") for name in names)],
                            capture_output=True, text=True, timeout=120)
    if result.returncode or not result.stdout.strip() or result.stderr.strip():
        raise RuntimeError(f"Could not inspect the actual Guava implementation in {path.name}")
    return normalize_javap_output(result.stdout)


def normalize_javap_output(text):
    for separator in ("/", "."):
        text = text.replace(GUAVA_RELOCATION[1].replace(".", separator),
                            GUAVA_RELOCATION[0].replace(".", separator))
    # javap resolves constant-pool indexes to symbolic references in comments.
    # ASM relocation can also widen ldc and move byte offsets. Compare the same
    # opcode/control-flow graph by instruction ordinal, preserving all literal
    # values, symbolic targets, exception ranges and switch keys.
    lines, output, index = text.splitlines(), [], 0
    while index < len(lines):
        line = lines[index]
        output.append(line.rstrip())
        index += 1
        if line != "    Code:":
            continue
        end = index
        while end < len(lines) and not re.match(r"^(?:  \S|})", lines[end]):
            end += 1
        block = lines[index:end]
        instructions = [re.match(r"^\s*(\d+):\s+([a-z][a-z0-9_]*)(?:\s+(.*))?$", item)
                        for item in block]
        positions = [int(item[1]) for item in instructions if item]
        if not positions:
            raise RuntimeError("javap produced a Code block without instructions")
        targets = {position: f"L{ordinal}" for ordinal, position in enumerate(positions)}
        def label(value):
            position = int(value)
            if position in targets:
                return targets[position]
            if position > positions[-1]:
                return "END"
            raise RuntimeError("javap produced an invalid control-flow target")
        exception_table = False
        for item, instruction in zip(block, instructions):
            stripped = item.strip()
            if instruction:
                position, opcode, operand = instruction[1], instruction[2], instruction[3] or ""
                opcode = {"ldc_w": "ldc", "goto_w": "goto", "jsr_w": "jsr"}.get(opcode, opcode)
                if opcode.startswith("if") or opcode in ("goto", "jsr"):
                    operand = label(operand)
                before, separator, symbolic = operand.partition("//")
                operand = re.sub(r"#\d+", "#CP", before.rstrip())
                if separator:
                    operand += " // " + symbolic.lstrip()
                output.append(f"{label(position)}: {opcode} {operand}".rstrip())
            elif re.match(r"^(?:-?\d+|default):\s+\d+$", stripped):
                key, destination = stripped.split(":", 1)
                output.append(f"{key}: {label(destination)}")
            elif exception_table and re.match(r"^\d+\s+\d+\s+\d+\s+", stripped):
                start, finish, destination, kind = stripped.split(maxsplit=3)
                output.append(f"{label(start)} {label(finish)} {label(destination)} {kind}")
            else:
                output.append(stripped)
                exception_table = exception_table or stripped == "Exception table:"
        index = end
    return "\n".join(output).strip()


def verify_guava(inputs, guava_inputs, rebuilt_path, javap="javap", java="java", relocations=()):
    """Bind the final payload to the original project's actual fixed shade inputs.

    Iceberg already minimizes its bundled Guava. Preserve that original task,
    verify every retained class API and complete normalized class payload, and
    independently compare private code in retained CVE-affected families.
    """
    guava_inputs = checked_inputs(guava_inputs, "bundled Guava")
    required = {f"com.google.guava:guava:{GUAVA_VERSION}",
                f"com.google.guava:failureaccess:{FAILUREACCESS_VERSION}"}
    empty_future = "com.google.guava:listenablefuture:9999.0-empty-to-avoid-conflict-with-guava"
    family = [item for item in guava_inputs if item["coordinate"].startswith("com.google.guava:")]
    selected = [item for item in family if item["coordinate"] in required]
    if ({item["coordinate"] for item in selected} != required or len(selected) != len(required)
            or any(item["coordinate"] not in required | {empty_future} for item in family)):
        raise RuntimeError("Bundled Guava must resolve only the fixed Guava and failureaccess versions")
    if len({item["coordinate"] for item in family}) != len(family):
        raise RuntimeError("Duplicate nested Guava dependency receipts")
    for artifact in family:
        if artifact["coordinate"] == empty_future:
            with ZipFile(artifact["path"]) as jar:
                if any(name.endswith(".class") for name in jar.namelist()):
                    raise RuntimeError("The declared empty listenablefuture artifact contains class payloads")
    bundles = [item for item in inputs if item["coordinate"] ==
               "org.apache.iceberg:iceberg-bundled-guava:1.12.0" and item.get("source_project") is True]
    if len(bundles) != 1:
        raise RuntimeError("Expected the one actual source project's bundled Guava shade output")
    checked_inputs(bundles, "bundled project")
    expected, expected_metadata = {}, {}
    guava_input = next(item for item in selected if item["coordinate"] == f"com.google.guava:guava:{GUAVA_VERSION}")
    original_metadata = None
    for artifact in selected:
        group, name, version = artifact["coordinate"].split(":")
        with ZipFile(artifact["path"]) as jar:
            metadata_path = f"META-INF/maven/{group}/{name}/pom.properties"
            identity = maven_properties(jar.read(metadata_path))
            if identity != {"groupId": group, "artifactId": name, "version": version}:
                raise RuntimeError(f"Fixed Guava input Maven identity mismatch: {artifact['coordinate']}")
            for metadata_name in (metadata_path, metadata_path.replace("pom.properties", "pom.xml")):
                content = jar.read(metadata_name)
                if not content.strip():
                    raise RuntimeError(f"Fixed Guava input Maven metadata is empty: {metadata_name}")
                expected_metadata[metadata_name] = content
            if name == "guava":
                original_metadata = jar.read(GUAVA_METADATA)
            for entry in jar.namelist():
                if entry.endswith(".class") and entry.startswith(("com/google/common/", "com/google/thirdparty/")):
                    relocated = entry.replace("com/google/common/", GUAVA_PREFIX, 1) if entry.startswith("com/google/common/") else entry
                    if relocated in expected:
                        raise RuntimeError(f"Conflicting fixed Guava input class: {entry}")
                    expected[relocated] = jar.read(entry)
    bundle_path = Path(bundles[0]["path"])
    with ZipFile(bundle_path) as bundle, ZipFile(rebuilt_path) as rebuilt:
        if len(bundle.namelist()) != len(set(bundle.namelist())) or len(rebuilt.namelist()) != len(set(rebuilt.namelist())):
            raise RuntimeError("Duplicate entries in bundled Guava or final runtime")
        retained = {name for name in bundle.namelist() if guava_class(name)}
        final = {name for name in rebuilt.namelist() if guava_class(name)}
        required_classes = {GUAVA_PREFIX + suffix for suffix in
                            ("base/Preconditions.class", "collect/ImmutableList.class",
                             "util/concurrent/internal/InternalFutureFailureAccess.class")}
        if not required_classes <= retained or not retained <= expected.keys() or final != retained:
            raise RuntimeError("The actual Guava implementation class closure is missing, extra or unrelocated")
        for name in sorted(retained):
            if (public_api(bundle.read(name), (GUAVA_RELOCATION,)) != public_api(expected[name])
                    or public_api(rebuilt.read(name), (GUAVA_RELOCATION, *relocations)) != public_api(expected[name])):
                raise RuntimeError(f"Fixed Guava API or final-runtime class payload mismatch: {name}")
        for jar in (bundle, rebuilt):
            if (jar.read(GUAVA_METADATA) != original_metadata
                    or any(jar.read(name) != content for name, content in expected_metadata.items())):
                raise RuntimeError("The original fixed Guava Maven metadata was changed or lost")
        security_classes = sorted(name for name in retained if any(
            name == GUAVA_PREFIX + f"collect/{family}.class"
            or name.startswith(GUAVA_PREFIX + f"collect/{family}$")
            for family in GUAVA_SECURITY_FAMILIES))
        security_code = None
        if security_classes:
            original_names = [name.replace(GUAVA_PREFIX, "com/google/common/", 1) for name in security_classes]
            fixed_code = normalized_disassembly(Path(guava_input["path"]), original_names, javap)
            actual_code = normalized_disassembly(bundle_path, security_classes, javap)
            if actual_code != fixed_code:
                raise RuntimeError("CVE-affected retained Guava implementation differs from the actual fixed dependency")
            security_code = sha256(fixed_code.encode())
    failureaccess = next(item for item in selected if item["coordinate"] ==
                         f"com.google.guava:failureaccess:{FAILUREACCESS_VERSION}")
    closure_source = Path(__file__).with_name("VerifyIcebergGuavaClosure.java")
    closure = subprocess.run([
        java, "--add-exports", "java.base/jdk.internal.org.objectweb.asm=ALL-UNNAMED",
        "--add-exports", "java.base/jdk.internal.org.objectweb.asm.commons=ALL-UNNAMED",
        str(closure_source), guava_input["path"], failureaccess["path"], str(bundle_path), str(rebuilt_path),
        *(f"{original}={relocated}" for original, relocated in (GUAVA_RELOCATION, *relocations)),
    ], capture_output=True, text=True, timeout=120)
    try:
        closure_record = json.loads(closure.stdout)
    except (TypeError, ValueError) as error:
        raise RuntimeError("Complete Guava class payload verification produced no valid proof") from error
    if (closure.returncode or closure.stderr.strip() or not isinstance(closure_record, dict)
            or closure_record.get("status") != "PASS" or closure_record.get("class_count") != len(retained)
            or not re.fullmatch(r"[0-9a-f]{64}", str(closure_record.get("complete_canonical_class_sha256", "")))):
        raise RuntimeError("Complete Guava class payload differs from the fixed inputs under original shade mappings")
    return {
        "status": "PASS", "version": GUAVA_VERSION, "failureaccess_version": FAILUREACCESS_VERSION,
        "fixed_inputs": [{key: item[key] for key in ("coordinate", "filename", "sha256")} for item in selected],
        "empty_listenablefuture_input": [{key: item[key] for key in ("coordinate", "filename", "sha256")}
                                          for item in family if item["coordinate"] == empty_future],
        "source_bundled_jar_sha256": bundles[0]["sha256"],
        "retained_class_count": len(retained), "upstream_minimized_class_count": len(expected.keys() - retained),
        "retained_public_protected_api_matches_fixed_inputs": True,
        "complete_class_payload_matches_fixed_inputs_under_original_relocations": closure_record,
        "original_fixed_maven_metadata_retained": True,
        "cve_affected_retained_classes": security_classes,
        "cve_affected_implementation_disassembly_sha256": security_code,
        "verification_basis": "Actual resolved fixed JAR SHA256 and unchanged upstream shade/minimize; full class payload canonicalization in verifier memory plus independent private opcode/symbolic-reference comparison for retained CVE class families",
        "limits": "Resolved-file hashes are build evidence, not an independently obtained publisher checksum; final image scanning and end-to-end runtime regression are separate checks.",
    }


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


def verify(inputs, rebuilt_path, upstream, java="java", guava_inputs=None):
    source_build = upstream / "spark/v4.2/build.gradle"
    relocations = re.findall(r"relocate\s+'([^']+)',\s*'([^']+)'", source_build.read_text())
    if not relocations:
        raise RuntimeError("Pinned upstream runtime has no relocation rules")
    expected, expected_metadata, expected_licenses = {}, {}, {}
    for artifact in checked_inputs(inputs, "runtime"):
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
    guava = verify_guava(inputs, guava_inputs, rebuilt_path, str(Path(java).with_name("javap")), java, relocations)
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
        "nested_guava": guava,
        "runtime": {"exit_code": runtime.returncode, "checks": checks,
                    "stdout": runtime.stdout, "stderr": runtime.stderr},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--guava-inputs", type=Path, required=True)
    parser.add_argument("--rebuilt", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--java", default="java")
    args = parser.parse_args()
    inputs = json.loads(args.inputs.read_text())
    guava_inputs = json.loads(args.guava_inputs.read_text())
    report = verify(inputs, args.rebuilt, args.upstream, args.java, guava_inputs)
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
        "upstream_guava_build_gradle_sha256": sha256((args.upstream / "build.gradle").read_bytes()),
        "guava_input_receipt_sha256": sha256(args.guava_inputs.read_bytes()),
        "patched_version_catalog_sha256": sha256((args.upstream / "gradle/libs.versions.toml").read_bytes()),
        "gradle_version": "8.14.5", "shadow_plugin": "com.gradleup.shadow:shadow-gradle-plugin:8.3.11",
        "gradle_distribution_sha256": "6f74b601422d6d6fc4e1f9a1ab6522f642c2fdcbc15ae33ebd30ba3d7198e854",
        "gradle_wrapper_sha256": "7d3a4ac4de1c32b59bc6a4eb8ecb8e612ccd0cf1ae1e99f66902da64df296172",
        "patched_dependencies": {"com.google.guava:guava": GUAVA_VERSION,
                                 "com.google.guava:failureaccess": FAILUREACCESS_VERSION,
                                 "jackson-core": "2.21.7", "jackson-databind": "2.21.7",
                                 "jackson-annotations": "2.21", "jackson-bom": "2.21.7",
                                 "parquet-jackson": "1.17.1-d2c-jackson2.21.7",
                                 "at.yawk.lz4:lz4-java": "1.11.4"},
        "original_fat_jar_usage": "none", "binary_edits": False,
        "nested_guava": report["nested_guava"],
        "verification_status": report["status"], "rebuilt_sha256": report["rebuilt_sha256"],
    }
    (args.artifacts / "iceberg-source-provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
