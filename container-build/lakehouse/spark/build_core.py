"""Compile official Spark core source and run its original Maven shade recipe."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile
from urllib.request import urlopen
import xml.etree.ElementTree as ET
import zipfile

from verify_runtime import verify_runtime, verify_spark_identity


HELPER_DIR = Path(__file__).resolve().parent
MANIFEST = HELPER_DIR / "inputs.json"
NAMESPACE = {"m": "http://maven.apache.org/POM/4.0.0"}
MAX_ARCHIVE_BYTES = 256 * 1024 * 1024
PROTOBUF_PROOF_MARKER = "D2C_SPARK_PROTOBUF_PROOF="
PROTOBUF_CLASSES = {
    "org.apache.spark.status.protobuf.StoreTypes",
    "org.apache.spark.status.protobuf.StoreTypes$SparkPlanGraphNodeWrapper",
    "org.apache.spark.status.protobuf.StoreTypes$SparkPlanGraphNodeWrapper$Builder",
    "org.apache.spark.status.protobuf.StoreTypes$SparkPlanGraphNodeWrapper$WrapperCase",
    "org.apache.spark.status.protobuf.StoreTypes$SparkPlanGraphNode",
    "org.apache.spark.status.protobuf.StoreTypes$SparkPlanGraphClusterWrapper",
}


def digest_file(path: Path, algorithm: str = "sha256") -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_verified(item: dict, directory: Path) -> Path:
    """A mismatching local/downloaded archive never reaches extraction."""
    destination = directory / item["filename"]
    if not destination.exists():
        partial = destination.with_suffix(destination.suffix + ".download")
        size = 0
        with urlopen(item["url"], timeout=120) as response, partial.open("wb") as stream:
            for chunk in iter(lambda: response.read(1024 * 1024), b""):
                size += len(chunk)
                if size > MAX_ARCHIVE_BYTES:
                    raise ValueError(f"Archive exceeds the source/tool size bound: {item['filename']}")
                stream.write(chunk)
        if digest_file(partial, "sha512") != item["sha512"]:
            raise ValueError(f"SHA512 mismatch: {item['filename']}")
        partial.replace(destination)
    if digest_file(destination, "sha512") != item["sha512"]:
        raise ValueError(f"SHA512 mismatch: {item['filename']}")
    return destination


def extract_verified(archive_path: Path, destination: Path, top_directory: str) -> Path:
    """Extract a hash-verified upstream archive within its declared root only."""
    destination.mkdir(parents=True, exist_ok=True)
    root = (destination / top_directory).resolve()
    if root.exists():
        raise ValueError(f"Refusing to merge an archive into an existing source/tool tree: {root}")
    with tarfile.open(archive_path, "r:gz") as archive:
        for member in archive.getmembers():
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts or not path.parts or path.parts[0] != top_directory:
                raise ValueError(f"Unsafe archive path: {member.name}")
            target = (destination / member.name).resolve()
            if not target.is_relative_to(root):
                raise ValueError(f"Archive path leaves its root: {member.name}")
            if not (member.isfile() or member.isdir() or member.issym() or member.islnk()):
                raise ValueError(f"Unsupported archive entry: {member.name}")
            if member.issym() or member.islnk():
                link = PurePosixPath(member.linkname)
                link_parent = target.parent if member.issym() else destination
                if link.is_absolute() or not (link_parent / member.linkname).resolve().is_relative_to(root):
                    raise ValueError(f"Unsafe archive link: {member.name}")
        # Recheck paths as earlier entries may have introduced symlinks.
        for member in archive.getmembers():
            if not (destination / member.name).resolve().is_relative_to(root):
                raise ValueError(f"Archive symlink traversal: {member.name}")
            archive.extract(member, destination)
    return root


def verify_source(source: Path, manifest: dict) -> dict:
    for name, expected in manifest["source"]["pom_sha256"].items():
        if digest_file(source / name) != expected:
            raise ValueError(f"Upstream source POM hash mismatch: {name}")
    root = ET.parse(source / "pom.xml").getroot()
    if root.findtext("m:version", namespaces=NAMESPACE) != manifest["spark_version"]:
        raise ValueError("Unexpected Spark source version")
    properties = root.find("m:properties", NAMESPACE)
    actual = {node.tag.rsplit("}", 1)[-1]: node.text for node in properties}
    for name, expected in manifest["upstream_properties"].items():
        if actual.get(name) != expected:
            raise ValueError(f"Unexpected upstream property {name}: {actual.get(name)}")
    core = ET.parse(source / "core/pom.xml").getroot()
    if core.findtext("m:parent/m:version", namespaces=NAMESPACE) != manifest["spark_version"]:
        raise ValueError("Core does not belong to the selected Spark release")
    text = (source / "core/pom.xml").read_text()
    if "maven-shade-plugin" not in text or "<pattern>org.eclipse.jetty</pattern>" not in text:
        raise ValueError("Original core Jetty shade recipe is missing")
    if not (source / "core/src/main/scala/org/apache/spark/SparkContext.scala").is_file():
        raise ValueError("Spark core Scala implementation source is missing")
    documents = {}
    for name in ("LICENSE", "NOTICE"):
        path = source / name
        if not path.is_file() or not path.read_bytes().strip():
            raise ValueError(f"Missing upstream {name}")
        documents[name] = digest_file(path)
    return {"pom_sha256": manifest["source"]["pom_sha256"], "license_sha256": documents, "upstream_properties": manifest["upstream_properties"]}


def verify_dependency_tree(tree: dict, manifest: dict) -> dict:
    """Check selected dependencies, not omitted/evicted dependency guesses."""
    spark_version, jetty_version = manifest["spark_version"], manifest["jetty_version"]
    if (tree.get("groupId"), tree.get("artifactId"), tree.get("version")) != ("org.apache.spark", "spark-core_2.13", spark_version):
        raise ValueError("Dependency tree is not the selected Spark core")
    stack, jetty, spark, scala = [tree], {}, {}, set()
    while stack:
        node = stack.pop()
        group, artifact, version = (node.get(key, "") for key in ("groupId", "artifactId", "version"))
        if group.startswith("org.eclipse.jetty"):
            if version != jetty_version:
                raise ValueError(f"Selected Jetty dependency is stale: {group}:{artifact}:{version}")
            jetty[f"{group}:{artifact}"] = version
        if group == "org.apache.spark":
            if version != spark_version or not artifact.endswith("_2.13"):
                raise ValueError(f"Mixed Spark dependency: {group}:{artifact}:{version}")
            spark[artifact] = version
        if group == "org.scala-lang" and artifact == "scala-library":
            scala.add(version)
        stack.extend(node.get("children", []))
    if not jetty:
        raise ValueError("No Jetty dependencies in the build tree")
    if scala != {manifest["upstream_properties"]["scala.version"]}:
        raise ValueError(f"Unexpected Scala library selection: {sorted(scala)}")
    return {"jetty": jetty, "spark": spark, "scala_library": sorted(scala)}


def maven_command(maven: Path, source: Path, repository: Path, manifest: dict) -> list[str]:
    # Deliberately no -pl/-am reactor fallback: dependencies are the published
    # artifacts of this exact Spark release. The original core POM is untouched.
    return [
        str(maven), "--batch-mode", "--no-transfer-progress", "--strict-checksums", "--file", str(source / "core/pom.xml"),
        f"-Dmaven.repo.local={repository}",
        *[f"-D{name}={value}" for name, value in sorted(manifest["overrides"].items())],
        "-DskipTests=true", "-Dmaven.test.skip=true", "-Dcheckstyle.skip=true",
    ]


def verify_build_jdk(manifest: dict, environment: dict) -> tuple[Path, dict]:
    """Match the baseline compiler generation without weakening class-set checks."""
    if not environment.get("JAVA_HOME"):
        raise ValueError("JAVA_HOME must select the pinned Spark build-only Java 17 JDK")
    home = Path(environment["JAVA_HOME"]).resolve()
    release = home / "release"
    values = {}
    for line in release.read_text().splitlines():
        if "=" in line:
            name, value = line.split("=", 1)
            values[name] = value.strip().strip('"')
    expected = manifest["build_jdk"]
    if values.get("JAVA_VERSION") != expected["java_version"] or values.get("IMPLEMENTOR") != expected["implementor"]:
        raise ValueError("Unexpected Spark build JDK release/vendor")
    runtime = values.get("JAVA_RUNTIME_VERSION", "")
    prefix = expected["runtime_version_prefix"]
    if runtime != prefix and not runtime.startswith(prefix + "-"):
        raise ValueError("Unexpected Spark build JDK update/build")
    environment["JAVA_HOME"] = str(home)
    environment["PATH"] = str(home / "bin") + os.pathsep + environment.get("PATH", "")
    reports = {}
    for tool in ("java", "javac"):
        executable = home / "bin" / tool
        if not executable.is_file():
            raise ValueError(f"Missing build-only JDK executable: {tool}")
        report = subprocess.run([str(executable), "-version"], check=True, text=True, capture_output=True, env=environment)
        reports[tool] = (report.stdout + report.stderr).strip()
    if f'version "{expected["java_version"]}"' not in reports["java"] or prefix not in reports["java"]:
        raise ValueError("Actual Java executable does not match the pinned Spark compiler JDK")
    if reports["javac"] != f'javac {expected["java_version"]}':
        raise ValueError("Actual javac executable does not match the pinned Spark compiler JDK")
    return home, {
        "expected_toolchain": expected, "java_home": str(home),
        "release_sha256": digest_file(release), "release_properties": values,
        "java_version_output": reports["java"], "javac_version_output": reports["javac"],
    }


def validate_protobuf_proof(stdout: str, target: Path, java_major: int) -> dict:
    lines = stdout.strip().splitlines()
    if len(lines) != 1 or not lines[0].startswith(PROTOBUF_PROOF_MARKER):
        raise ValueError("Missing or ambiguous actual Spark protobuf probe proof")
    proof = json.loads(lines[0][len(PROTOBUF_PROOF_MARKER):])
    expected_path = str(target.resolve())
    if not isinstance(proof, dict):
        raise ValueError("Spark protobuf probe proof is not an object")
    if proof.get("status") != "passed" or any(type(proof.get(name)) is not int or proof[name] != value for name, value in {
        "schema_version": 1, "actual_java_major": java_major, "expected_java_major": java_major,
    }.items()):
        raise ValueError("Unexpected actual Spark protobuf probe status/Java version")
    if proof.get("expected_core_jar") != expected_path or proof.get("actual_core_jar") != expected_path:
        raise ValueError("Spark protobuf probe loaded a different core JAR")
    origins = proof.get("class_code_sources")
    if origins != {name: expected_path for name in PROTOBUF_CLASSES}:
        raise ValueError("Spark protobuf implementation origins are not the rebuilt core JAR")
    if proof.get("core_jar_sha256") != digest_file(target):
        raise ValueError("Spark protobuf probe core JAR hash mismatch")
    if (proof.get("cases") != ["NODE", "CLUSTER", "WRAPPER_NOT_SET"] or proof.get("unset_merge_preserves_existing") is not True
            or any(type(proof.get(name)) is not int or proof[name] != 3 for name in ("merge_cases_verified", "wire_roundtrips_verified"))):
        raise ValueError("Spark protobuf probe did not cover every enum merge/wire branch")
    if any(not isinstance(proof.get(name), str) or not proof[name] for name in ("java_runtime_version", "java_vendor")):
        raise ValueError("Spark protobuf probe omitted actual JVM identity")
    return proof


def run_protobuf_probe(jdk: Path, target: Path, work: Path, environment: dict) -> dict:
    """Execute original generated classes using only this rebuilt JAR."""
    source = HELPER_DIR / "VerifySparkProtobuf.java"
    classes = work / "protobuf-probe-classes"
    classes.mkdir(exist_ok=False)
    compile_command = [str(jdk / "bin/javac"), "-J-Xmx256m", "--release", "17", "-classpath", str(target), "-d", str(classes), str(source)]
    subprocess.run(compile_command, check=True, text=True, capture_output=True, env=environment)
    command = [str(jdk / "bin/java"), "-Xmx128m", "-classpath", str(classes) + os.pathsep + str(target), "VerifySparkProtobuf", "17", str(target)]
    report = subprocess.run(command, check=True, text=True, capture_output=True, env=environment)
    proof = validate_protobuf_proof(report.stdout, target, 17)
    return {"probe_source_sha256": digest_file(source), "compile_command": compile_command, "command": command, "result": proof}


def build(work: Path, output: Path, baseline: Path, repository: Path,
          shared_maven_home: Path | None = None, shared_maven_archive: Path | None = None) -> None:
    manifest = json.loads(MANIFEST.read_text())
    # Fail before any large fetch when the Docker stage still has Spark 4.1.x.
    with zipfile.ZipFile(baseline) as jar:
        verify_spark_identity(jar, manifest["spark_version"])
    environment = os.environ.copy()
    environment.pop("MAVEN_ARGS", None)
    environment["MAVEN_OPTS"] = "-Xmx1536m"
    jdk, jdk_evidence = verify_build_jdk(manifest, environment)
    work.mkdir(parents=True, exist_ok=True)
    output.mkdir(parents=True, exist_ok=True)
    downloads = work / "downloads"
    downloads.mkdir(exist_ok=True)
    source_archive = download_verified(manifest["source"], downloads)
    source = extract_verified(source_archive, work / "source", manifest["source"]["directory"])
    source_evidence = verify_source(source, manifest)
    if shared_maven_home is not None:
        if shared_maven_archive is None or digest_file(shared_maven_archive, "sha512") != manifest["maven"]["sha512"]:
            raise ValueError("Shared Maven release archive SHA512 mismatch")
        maven_root = shared_maven_home
        tool_installation = "shared source-build-base; release archive independently checked"
    else:
        maven_archive = download_verified(manifest["maven"], downloads)
        maven_root = extract_verified(maven_archive, work / "tools", manifest["maven"]["directory"])
        tool_installation = "downloaded and extracted verified release archive"
    maven = maven_root / "bin/mvn"
    version = subprocess.run([str(maven), "--version"], check=True, text=True, capture_output=True, env=environment)
    if f"Apache Maven {manifest['maven']['version']}" not in version.stdout:
        raise ValueError("Unexpected Maven executable version")
    if not re.search(r"Java version:\s*" + re.escape(manifest["build_jdk"]["java_version"]) + r"(?:,|\s)", version.stdout):
        raise ValueError("Maven did not select the pinned Spark Java 17 compiler")
    command = maven_command(maven, source, repository, manifest)
    build_command = command + ["clean", "package"]
    subprocess.run(build_command, cwd=source, env=environment, check=True)
    tree_path = output / "spark-core-build-dependencies.json"
    subprocess.run(command + ["dependency:tree", "-DoutputType=json", f"-DoutputFile={tree_path}"], cwd=source, env=environment, check=True)
    dependencies = verify_dependency_tree(json.loads(tree_path.read_text()), manifest)
    target = source / f"core/target/spark-core_2.13-{manifest['spark_version']}.jar"
    verification = verify_runtime(target, baseline, manifest["jetty_version"], manifest["spark_version"])
    verification["protobuf_builder_probe"] = run_protobuf_probe(jdk, target, work, environment)
    artifact = output / f"spark-core_2.13-{manifest['spark_version']}-d2c-jetty{manifest['jetty_version']}.jar"
    shutil.copyfile(target, artifact)
    for document in ("LICENSE", "NOTICE"):
        shutil.copyfile(source / document, output / f"{document}.spark-{manifest['spark_version']}")
    (output / "spark-core-verification.json").write_text(json.dumps(verification, indent=2, sort_keys=True) + "\n")
    provenance = {
        "schema_version": 1, "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": manifest["source"], "maven": manifest["maven"],
        "maven_installation": tool_installation,
        "source_evidence": source_evidence, "source_edits": [],
        "build_strategy": "standalone original core POM, published same-release Spark dependencies",
        "build_command": build_command, "dependency_overrides": manifest["overrides"],
        "maven_version_output": version.stdout.strip(),
        "java_version_output": jdk_evidence["java_version_output"],
        "build_java": jdk_evidence,
        "selected_dependencies": dependencies,
        "artifact": {"filename": artifact.name, "sha256": digest_file(artifact)},
        "verification": verification,
        "local_variant": True,
        "scope": manifest["compatibility_scope"],
        "limits": "Source compilation and structural checks only. This does not establish startup/behavioral compatibility or a clean container scan.",
    }
    (output / "spark-source-provenance.json").write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n")
    print(f"Verified source-built artifact: {artifact.name} sha256={provenance['artifact']['sha256']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=Path("/work/spark"))
    parser.add_argument("--output-dir", type=Path, default=Path("/out/spark"))
    parser.add_argument("--baseline-jar", type=Path, default=Path("/opt/spark/jars/spark-core_2.13-4.2.1.jar"))
    parser.add_argument("--maven-repo", type=Path, default=Path("/root/.m2/repository"))
    parser.add_argument("--maven-home", type=Path)
    parser.add_argument("--maven-archive", type=Path)
    args = parser.parse_args()
    if bool(args.maven_home) != bool(args.maven_archive):
        parser.error("maven-home and maven-archive must be supplied together")
    build(args.work_dir.resolve(), args.output_dir.resolve(), args.baseline_jar.resolve(), args.maven_repo.resolve(),
          args.maven_home.resolve() if args.maven_home else None,
          args.maven_archive.resolve() if args.maven_archive else None)


if __name__ == "__main__":
    main()
