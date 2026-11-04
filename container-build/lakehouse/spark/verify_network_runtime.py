"""Verify a source-rebuilt Spark network-common; never rewrite JAR contents."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
import zipfile

from verify_runtime import properties, structural_class_api


ARTIFACT_ID = "spark-network-common_2.13"
SPARK_PROPERTIES = f"META-INF/maven/org.apache.spark/{ARTIFACT_ID}/pom.properties"
GUAVA_PROPERTIES = "META-INF/maven/com.google.guava/guava/pom.properties"
RELOCATIONS = {
    "com/google/common": "org/sparkproject/guava",
    "com/google/thirdparty": "org/sparkproject/guava/thirdparty",
}
REQUIRED_CLASSES = {
    "org/apache/spark/network/TransportContext.class",
    "org/apache/spark/network/client/TransportClient.class",
    "org/apache/spark/network/util/TransportConf.class",
    "org/sparkproject/guava/base/Preconditions.class",
    "org/sparkproject/guava/cache/CacheBuilder.class",
    "org/sparkproject/guava/collect/ImmutableList.class",
    "org/sparkproject/guava/util/concurrent/internal/InternalFutureFailureAccess.class",
}


def verify_network_identity(jar: zipfile.ZipFile, version: str) -> None:
    try:
        identity = properties(jar.read(SPARK_PROPERTIES))
    except KeyError as error:
        raise ValueError("Spark network-common Maven identity metadata is missing") from error
    expected = {"groupId": "org.apache.spark", "artifactId": ARTIFACT_ID, "version": version}
    if any(identity.get(name) != value for name, value in expected.items()):
        raise ValueError(f"Expected Spark network-common 2.13/{version}, found {identity}")


def relocate_structure(value):
    """Apply the unchanged upstream shade rules to class-file API strings."""
    if isinstance(value, str):
        for before, after in RELOCATIONS.items():
            value = value.replace(before, after).replace(before.replace("/", "."), after.replace("/", "."))
        return value
    if isinstance(value, list):
        return [relocate_structure(item) for item in value]
    if isinstance(value, dict):
        return {name: relocate_structure(item) for name, item in value.items()}
    return value


def relocated_class_api(content: bytes) -> dict:
    """Keep ClassReader's member ordering canonical after descriptor relocation."""
    result = relocate_structure(structural_class_api(content))
    # ClassReader sorts (name, descriptor). Package mapping can change that
    # order: Absent.or(com...Optional) sorts before or(java...Object), but
    # or(org...Optional) sorts after it. Retain every complete member record,
    # and apply exactly the same key as the parser of the shaded class bytes.
    for name in ("fields", "methods"):
        result[name].sort(key=lambda member: (member[0], member[1]))
    return result


def structural_difference(expected: dict, actual: dict) -> str:
    """Report a few public class-structure differences, never dump a JAR."""
    pending = [("$", expected, actual)]
    details = []
    missing = object()
    while pending and len(details) < 4:
        path, before, after = pending.pop()
        if before == after:
            continue
        if isinstance(before, dict) and isinstance(after, dict):
            pending.extend((f"{path}.{key}", before.get(key, missing), after.get(key, missing))
                           for key in sorted(before.keys() | after.keys(), reverse=True))
        elif isinstance(before, list) and isinstance(after, list):
            if len(before) != len(after):
                details.append(f"{path}.length expected={len(before)} actual={len(after)}")
            pending.extend((f"{path}[{index}]", before[index], after[index])
                           for index in reversed(range(min(len(before), len(after)))))
        else:
            render = lambda value: "<missing>" if value is missing else repr(value)[:180]
            details.append(f"{path} expected={render(before)} actual={render(after)}")
    return "; ".join(details)[:1600]


def verify_resolved_guava(rebuilt: Path, guava: Path, failureaccess: Path,
                          guava_version: str, failureaccess_version: str) -> dict:
    """Bind real shaded classes to the exact dependency JARs Maven resolved.

    These SHA256 values describe actual CI inputs, not independently published
    publisher checksums. Maven strict transport/checksum resolution and the
    source recipe are separate provenance requirements.
    """
    inputs = (("guava", guava_version, guava), ("failureaccess", failureaccess_version, failureaccess))
    expected_classes = {}
    evidence = {}
    with zipfile.ZipFile(rebuilt) as new:
        new_names = set(new.namelist())
        for artifact, version, path in inputs:
            prefix = f"META-INF/maven/com.google.guava/{artifact}/"
            with zipfile.ZipFile(path) as dependency:
                names = dependency.namelist()
                if len(set(names)) != len(names):
                    raise ValueError(f"Duplicate resolved {artifact} JAR entries")
                metadata = properties(dependency.read(prefix + "pom.properties"))
                identity = {"groupId": "com.google.guava", "artifactId": artifact, "version": version}
                if any(metadata.get(name) != value for name, value in identity.items()):
                    raise ValueError(f"Resolved {artifact} Maven identity/version mismatch: {metadata}")
                for name in (prefix + "pom.properties", prefix + "pom.xml"):
                    if not dependency.read(name).strip() or new.read(name) != dependency.read(name):
                        raise ValueError(f"Resolved {artifact} Maven metadata was not retained byte-for-byte")
                class_hashes = {}
                for name in sorted(names):
                    if not name.endswith(".class") or not any(before + "/" in name for before in RELOCATIONS):
                        continue  # The upstream shade tool omits JPMS module-info.
                    target = relocate_structure(name)
                    if target in expected_classes:
                        raise ValueError(f"Overlapping resolved Guava classes: {target}")
                    raw = dependency.read(name)
                    class_hashes[name] = sha256(raw).hexdigest()
                    expected_classes[target] = relocated_class_api(raw)
                if not class_hashes:
                    raise ValueError(f"Resolved {artifact} JAR has no Guava implementation classes")
                # Canonical sorted input names+hashes bind the complete inventory
                # without writing thousands of repetitive records to provenance.
                inventory = "".join(f"{name} {digest}\n" for name, digest in sorted(class_hashes.items()))
                evidence[artifact] = {
                    **identity, "resolved_jar_sha256": sha256(path.read_bytes()).hexdigest(),
                    "implementation_classes": len(class_hashes),
                    "class_inventory_sha256": sha256(inventory.encode()).hexdigest(),
                    "maven_metadata_retained_byte_for_byte": True,
                }
        actual_classes = {name for name in new_names if name.endswith(".class") and "org/sparkproject/guava/" in name}
        if actual_classes != expected_classes.keys():
            raise ValueError(f"Resolved Guava class inventory mismatch: missing={sorted(expected_classes.keys()-actual_classes)[:10]}, added={sorted(actual_classes-expected_classes.keys())[:10]}")
        for name, expected in sorted(expected_classes.items()):
            actual = structural_class_api(new.read(name))
            if actual != expected:
                sections = sorted(key for key in expected if expected[key] != actual.get(key))
                raise ValueError(f"Resolved Guava relocated class structural mismatch: {name}; sections={sections}; "
                                 + structural_difference(expected, actual))
    return {
        "resolved_dependencies": evidence,
        "relocated_implementation_classes_verified": len(expected_classes),
        "complete_relocated_class_inventory_matches": True,
        "relocated_structural_apis_match_resolved_inputs": True,
        "limits": "Structural verification is not an instruction-by-instruction comparison or a runtime behavioral test; genuine unchanged source shading, final probes, and scans remain required.",
    }


def verify_network_runtime(rebuilt: Path, baseline: Path, guava_version: str = "33.7.2-jre", spark_version: str = "4.2.1") -> dict:
    with zipfile.ZipFile(rebuilt) as new, zipfile.ZipFile(baseline) as old:
        verify_network_identity(new, spark_version)
        verify_network_identity(old, spark_version)
        names = set(new.namelist())
        if len(names) != len(new.namelist()) or len(set(old.namelist())) != len(old.namelist()):
            raise ValueError("Duplicate network-common JAR entries")
        if REQUIRED_CLASSES - names:
            raise ValueError(f"Missing network-common implementation classes: {sorted(REQUIRED_CLASSES - names)}")
        for name in sorted(REQUIRED_CLASSES):
            if structural_class_api(new.read(name))["name"] != name[:-6]:
                raise ValueError(f"Network-common implementation class identity mismatch: {name}")
        if any(name.startswith(("com/google/common/", "com/google/thirdparty/")) and name.endswith(".class") for name in names):
            raise ValueError("Unrelocated Guava class in rebuilt Spark network-common")
        guava = properties(new.read(GUAVA_PROPERTIES)) if GUAVA_PROPERTIES in names else {}
        expected_guava = {"groupId": "com.google.guava", "artifactId": "guava", "version": guava_version}
        if any(guava.get(name) != value for name, value in expected_guava.items()):
            raise ValueError(f"Guava Maven identity/version mismatch in network-common: {guava}")
        for metadata in (SPARK_PROPERTIES.replace("pom.properties", "pom.xml"), GUAVA_PROPERTIES.replace("pom.properties", "pom.xml")):
            if metadata not in names or not new.read(metadata).strip():
                raise ValueError(f"Missing upstream Maven POM in network-common: {metadata}")
        for document in ("META-INF/LICENSE", "META-INF/NOTICE"):
            if document not in names or not new.read(document).strip():
                raise ValueError(f"Missing upstream network-common license document: {document}")
            if document not in old.namelist() or new.read(document) != old.read(document):
                raise ValueError(f"Original Spark network-common license document changed: {document}")
        select = lambda entries: {name for name in entries if name.startswith("org/apache/spark/") and name.endswith(".class")}
        old_classes, new_classes = select(old.namelist()), select(names)
        if not old_classes or old_classes != new_classes:
            raise ValueError(f"Spark network-common class set changed: removed={sorted(old_classes-new_classes)[:10]}, added={sorted(new_classes-old_classes)[:10]}")
        for name in sorted(old_classes):
            if structural_class_api(old.read(name)) != structural_class_api(new.read(name)):
                raise ValueError(f"Spark network-common structural ABI changed: {name}")
    return {
        "spark_version": spark_version, "scala_binary_version": "2.13", "artifact_id": ARTIFACT_ID,
        "shaded_guava": expected_guava, "spark_classes_compared": len(old_classes),
        "rebuilt_sha256": sha256(rebuilt.read_bytes()).hexdigest(),
        "baseline_sha256": sha256(baseline.read_bytes()).hexdigest(),
        "structural_abi_matches_baseline": True, "upstream_maven_metadata_retained": True,
        "original_license_and_notice_match_baseline": True,
        "limits": "Original source compilation/shading and structural verification; final network/Kafka regression and actual image scanning are separate requirements.",
    }
