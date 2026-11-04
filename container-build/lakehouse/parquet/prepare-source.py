"""Prepare pinned shade-module inputs and verify their packaged resources."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET
from zipfile import ZipFile


def validate_main_inventory(module, source):
    """Allow only the reviewed files, including their exact original bytes."""
    expected = source.get("main_files_sha256")
    if not isinstance(expected, dict) or not expected or any(not name.startswith("resources/") for name in expected):
        raise RuntimeError("Pinned resource-only main inventory is required")
    main = module / "src/main"
    if not main.is_dir() or main.is_symlink():
        raise RuntimeError("Pinned parquet-jackson main resource directory is missing or symlinked")
    actual = {}
    for path in main.rglob("*"):
        if path.is_symlink():
            raise RuntimeError("Symlink in parquet-jackson main inventory; review required")
        if path.is_file():
            actual[path.relative_to(main).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
        elif not path.is_dir():
            raise RuntimeError("Nonregular parquet-jackson main entry; review required")
    if actual != expected:
        raise RuntimeError("Pinned parquet-jackson main inventory changed; review required")
    return actual


def read_properties(content):
    result = {}
    for line in content.decode("utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith(("#", "!")) and "=" in line:
            key, value = line.split("=", 1)
            result[key.strip()] = value.strip()
    return result


def verify_bundle(module, jar, source):
    inventory = validate_main_inventory(module, source)
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    parent = ET.parse(module.parent / "pom.xml").getroot()
    prefix = parent.findtext("m:properties/m:shade.prefix", namespaces=namespace)
    if not prefix:
        raise RuntimeError("Original upstream Parquet relocation prefix is missing")
    jackson = prefix.replace(".", "/") + "/com/fasterxml/jackson/"
    versions = {
        "org.apache.parquet/parquet-jackson": source["version"] + "-d2c-jackson2.21.7",
        "com.fasterxml.jackson.core/jackson-core": "2.21.7",
        "com.fasterxml.jackson.core/jackson-databind": "2.21.7",
        "com.fasterxml.jackson.core/jackson-annotations": "2.21",
    }
    package_versions = {jackson + suffix for suffix in ("core/json/PackageVersion.class", "databind/cfg/PackageVersion.class")}
    with ZipFile(jar) as archive:
        names = set(archive.namelist())
        if len(names) != len(archive.namelist()):
            raise RuntimeError("Duplicate entries in rebuilt Parquet JAR")
        for relative, digest in inventory.items():
            entry = relative.removeprefix("resources/")
            if entry not in names or hashlib.sha256(archive.read(entry)).hexdigest() != digest:
                raise RuntimeError("Original main resource missing or changed in rebuilt JAR: " + entry)
        for coordinate, version in versions.items():
            entry = "META-INF/maven/" + coordinate + "/pom.properties"
            if entry not in names:
                raise RuntimeError("Required Maven metadata missing from rebuilt JAR: " + coordinate)
            group, artifact = coordinate.split("/", 1)
            identity = read_properties(archive.read(entry))
            if identity.get("groupId") != group or identity.get("artifactId") != artifact or identity.get("version") != version:
                raise RuntimeError("Incorrect rebuilt Maven identity: " + coordinate)
        actual_versions = {name for name in names if name.endswith(("/core/json/PackageVersion.class", "/databind/cfg/PackageVersion.class"))}
        if actual_versions != package_versions:
            raise RuntimeError("Unexpected Jackson implementation layer in rebuilt Parquet JAR")
        for entry in package_versions:
            content = archive.read(entry)
            if not content.startswith(b"\xca\xfe\xba\xbe") or b"2.21.7" not in content:
                raise RuntimeError("Incorrect actual shaded Jackson implementation: " + entry)
        if jackson + "annotation/JsonProperty.class" not in names:
            raise RuntimeError("Shaded Jackson annotation implementation is missing")
        if any(name.endswith(".class") and name.startswith("com/fasterxml/jackson/") for name in names):
            raise RuntimeError("Unrelocated Jackson implementation in rebuilt Parquet JAR")
    return {
        "status": "PASS", "artifact_sha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
        "source_main_files_sha256": inventory,
        "source_resource_byte_parity": True,
        "retained_maven_versions": versions,
        "checked_actual_package_version_classes": sorted(package_versions),
        "limits": "Original shade-module resources, Maven identities and implementation markers only; final runtime regression and image scanning remain separate.",
    }


def patch_module(text, version):
    old = '<artifactId>jackson-annotations</artifactId>\n      <version>${jackson.version}</version>'
    if version == "1.17.0":
        if text.count(old) != 1:
            raise RuntimeError("Expected pinned upstream Jackson annotation declaration")
        text = text.replace(old, old.replace("${jackson.version}", "${jackson-annotations.version}"))
    marker = "<artifactId>parquet-jackson</artifactId>"
    root = ET.fromstring(text)
    properties = [child for child in root if child.tag.rsplit("}", 1)[-1] == "properties"]
    if text.count(marker) != 1 or len(properties) != 1:
        raise RuntimeError("Pinned parquet-jackson POM structure changed")
    text = text.replace(marker, marker + f"\n  <version>{version}-d2c-jackson2.21.7</version>", 1)
    return text.replace("<properties>", """<properties>
    <jackson.version>2.21.7</jackson.version>
    <jackson-databind.version>2.21.7</jackson-databind.version>
    <jackson-annotations.version>2.21</jackson-annotations.version>""", 1)


def prepare(source, directory):
    archive = Path(source["archive"])
    if hashlib.new(source["algorithm"], archive.read_bytes()).hexdigest() != source["digest"]:
        raise RuntimeError(f"Source archive checksum mismatch: {archive}")
    destination = directory / f"parquet-{source['version']}"
    destination.mkdir(parents=True, exist_ok=True)
    # Apache releases and GitHub archives have different root names.
    subprocess.run(["tar", "--extract", "--gzip", "--file", str(archive),
                    "--strip-components=1", "--directory", str(destination)], check=True)
    module = destination / "parquet-jackson"
    validate_main_inventory(module, source)
    pom = module / "pom.xml"
    pom.write_text(patch_module(pom.read_text(), source["version"]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", type=Path, required=True)
    parser.add_argument("--directory", type=Path, default=Path("sources"))
    parser.add_argument("--verify-version")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    sources = json.loads(args.sources.read_text())
    if bool(args.verify_version) != bool(args.report):
        parser.error("verify-version and report must be supplied together")
    if args.verify_version:
        matches = [source for source in sources if source["version"] == args.verify_version]
        if len(matches) != 1:
            parser.error("verify-version must select exactly one pinned source")
        module = args.directory / f"parquet-{args.verify_version}" / "parquet-jackson"
        jar = module / "target" / f"parquet-jackson-{args.verify_version}-d2c-jackson2.21.7.jar"
        report = verify_bundle(module, jar, matches[0])
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    else:
        for source in sources:
            prepare(source, args.directory)
