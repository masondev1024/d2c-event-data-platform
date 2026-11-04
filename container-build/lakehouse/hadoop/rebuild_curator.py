"""Assemble full official Curator modules with fixed Guava and their original ABI.

This is a normal source-free Maven assembly. It never edits a finished JAR.
The official complete pre-Shade classifiers are inputs; all three module APIs
are compared with the official standard artifacts before Hadoop consumes them.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
from zipfile import ZipFile

from curator_compat import verify_curator_shaded_module
from runtime_profile import (CURATOR_MODULES, CURATOR_REVIEWED_METADATA, CURATOR_VARIANT_GROUP, CURATOR_VARIANT_VERSION,
                             CURATOR_VERSION, FAILUREACCESS_VERSION, GUAVA_VERSION)

REPOSITORY = Path("/root/.m2/repository")
NS = "http://maven.apache.org/POM/4.0.0"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def resolved(group: str, artifact: str, version: str, classifier: str = "") -> Path:
    suffix = "-" + classifier if classifier else ""
    return REPOSITORY / group.replace(".", "/") / artifact / version / f"{artifact}-{version}{suffix}.jar"


def dependency(group: str, artifact: str, version: str, *, classifier: str = "",
               scope: str = "", no_transitives: bool = False) -> str:
    fields = f"<groupId>{group}</groupId><artifactId>{artifact}</artifactId><version>{version}</version>"
    if classifier:
        fields += f"<classifier>{classifier}</classifier>"
    if scope:
        fields += f"<scope>{scope}</scope>"
    if no_transitives:
        fields += "<exclusions><exclusion><groupId>*</groupId><artifactId>*</artifactId></exclusion></exclusions>"
    return "<dependency>" + fields + "</dependency>"


def project_pom(module: str, *, multi_release: bool = False) -> str:
    """Full unshaded upstream module input plus explicitly selected dependencies."""
    if module not in (*CURATOR_MODULES, "curator-guava-reference"):
        raise ValueError("unsupported Curator assembly module")
    deps, included = [], []
    if module in CURATOR_MODULES:
        # The whole module is retained; wildcard exclusion only prevents its
        # unclassified siblings and old dependency graph from re-entering.
        deps.append(dependency("org.apache.curator", module, CURATOR_VERSION,
                               classifier="osgi", no_transitives=True))
        included.append(f"org.apache.curator:{module}:jar:osgi")
    if module in ("curator-client", "curator-guava-reference"):
        deps += [dependency("com.google.guava", "guava", GUAVA_VERSION),
                 dependency("com.google.guava", "failureaccess", FAILUREACCESS_VERSION)]
        included += ["com.google.guava:guava", "com.google.guava:failureaccess"]
    if module == "curator-client":
        # Preserve the original external runtime API dependencies. These are
        # the versions already selected by Hadoop 3.5; neither is embedded.
        deps += [dependency("org.apache.zookeeper", "zookeeper", "3.8.6"),
                 dependency("org.slf4j", "slf4j-api", "1.7.36")]
    if module == "curator-framework":
        deps += [dependency(CURATOR_VARIANT_GROUP, "curator-client", CURATOR_VARIANT_VERSION),
                 dependency("com.fasterxml.jackson.core", "jackson-core", "2.21.7", scope="provided"),
                 dependency("com.fasterxml.jackson.core", "jackson-databind", "2.21.7", scope="provided")]
    if module == "curator-recipes":
        deps += [dependency(CURATOR_VARIANT_GROUP, "curator-framework", CURATOR_VARIANT_VERSION)]
    includes = "".join(f"<include>{name}</include>" for name in included)
    manifest = ("<configuration><archive><manifestEntries><Multi-Release>true</Multi-Release>"
                "</manifestEntries></archive></configuration>" if multi_release else "")
    return f'''<project xmlns="{NS}">
  <modelVersion>4.0.0</modelVersion>
  <groupId>{CURATOR_VARIANT_GROUP}</groupId><artifactId>{module}</artifactId>
  <version>{CURATOR_VARIANT_VERSION}</version><packaging>jar</packaging>
  <name>D2C complete Curator compatible fixed Guava assembly: {module}</name>
  <description>Official complete 5.2.0 pre-shade module and fixed dependency assembly; no upstream Java or finished JAR is patched.</description>
  <licenses><license><name>Apache License, Version 2.0</name><url>https://www.apache.org/licenses/LICENSE-2.0.txt</url></license></licenses>
  <properties><project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <maven.compiler.release>17</maven.compiler.release><project.build.outputTimestamp>2026-11-04T00:00:00Z</project.build.outputTimestamp>
  </properties>
  <dependencies>{''.join(deps)}</dependencies>
  <build><plugins>
    <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-resources-plugin</artifactId><version>3.3.1</version></plugin>
    <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>3.13.0</version></plugin>
    <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-jar-plugin</artifactId><version>3.4.2</version>{manifest}</plugin>
    <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-install-plugin</artifactId><version>3.1.3</version></plugin>
    <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-shade-plugin</artifactId><version>3.6.1</version>
      <executions><execution><phase>package</phase><goals><goal>shade</goal></goals><configuration>
        <createDependencyReducedPom>true</createDependencyReducedPom><promoteTransitiveDependencies>true</promoteTransitiveDependencies>
        <artifactSet><includes>{includes}</includes></artifactSet>
        <relocations><relocation><pattern>com.google</pattern><shadedPattern>org.apache.curator.shaded.com.google</shadedPattern></relocation></relocations>
      </configuration></execution></executions>
    </plugin>
  </plugins></build>
</project>
'''


def license_entries(jar: ZipFile) -> list[str]:
    return sorted(name for name in jar.namelist() if not name.endswith("/")
                  and re.search(r"(?:^|/)(?:LICENSE|NOTICE)(?:[._-][^/]*)?$", name, re.IGNORECASE))


def preserve_license_sources(root: Path, sources: dict[str, Path]) -> dict:
    proof = {}
    for library, path in sources.items():
        with ZipFile(path) as jar:
            names = license_entries(jar)
            if not names:
                raise ValueError(f"input license/notice document absent: {library}")
            for name in names:
                if name.startswith("/") or ".." in name.split("/"):
                    raise ValueError("unsafe input document path")
                data = jar.read(name)
                if not data.strip():
                    raise ValueError("empty input license/notice")
                # Hadoop's original resource transformer excludes names ending
                # LICENSE/NOTICE. A supplemental source document keeps its
                # original bytes under an explicit non-matching suffix.
                entry = f"META-INF/d2c-licenses/{library}/{name}.original"
                target = root / "src/main/resources" / entry
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
                proof[entry] = {"input_jar_sha256": digest(path), "source_entry": name,
                                "document_sha256": hashlib.sha256(data).hexdigest()}
    return proof


def preserve_manifest_sources(root: Path, sources: dict[str, Path]) -> dict:
    proof = {}
    for library, path in sources.items():
        with ZipFile(path) as jar:
            name = "META-INF/MANIFEST.MF"
            content = jar.read(name)
            if not content.strip():
                raise ValueError("empty input manifest")
            entry = f"META-INF/d2c-upstream/{library}/{name}.original"
            target = root / "src/main/resources" / entry
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            proof[entry] = {"input_jar_sha256": digest(path), "source_entry": name,
                            "document_sha256": hashlib.sha256(content).hexdigest()}
    return proof


def has_multi_release_implementation(path: Path) -> bool:
    with ZipFile(path) as jar:
        return any(re.fullmatch(r"META-INF/versions/[1-9][0-9]*/.+\.class", name)
                   and not name.endswith("/module-info.class") for name in jar.namelist())


def preserve_module_descriptors(root: Path, sources: dict[str, Path]) -> dict:
    proof = {}
    for library, path in sources.items():
        with ZipFile(path) as jar:
            for name in jar.namelist():
                if re.fullmatch(r"(?:META-INF/versions/[1-9][0-9]*/)?module-info\.class", name):
                    content = jar.read(name)
                    entry = f"META-INF/d2c-upstream/{library}/{name}.original"
                    target = root / "src/main/resources" / entry
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(content)
                    proof[entry] = {"input_jar_sha256": digest(path), "source_entry": name,
                                    "document_sha256": hashlib.sha256(content).hexdigest()}
    return proof


def build(maven: Path, source_root: Path, output: Path) -> dict:
    work = source_root / "curator-assemblies"
    work.mkdir(parents=True, exist_ok=True)
    commands = []
    log_path = output / "curator-bundle-build.log"
    if log_path.exists():
        raise ValueError("refuse to overwrite an existing Curator build receipt")

    def run(arguments: list[str], directory: Path) -> None:
        command = [str(maven), "--batch-mode", "--no-transfer-progress", *arguments]
        commands.append({"arguments": command, "directory": str(directory)})
        with log_path.open("ab") as log:
            result = subprocess.run(command, cwd=directory, stdout=log, stderr=subprocess.STDOUT, check=False)
        commands[-1]["exit"] = result.returncode
        if result.returncode:
            # Bounded evidence enters the enclosing CI guard log even when
            # the dedicated receipt never reaches the final artifact tree.
            print(log_path.read_text(errors="replace")[-16000:], flush=True)
            raise ValueError(f"Curator source-free Maven assembly failed: {directory.name}")

    reference_dir = work / "curator-guava-reference"
    reference_dir.mkdir(parents=True, exist_ok=True)
    (reference_dir / "pom.xml").write_text(project_pom("curator-guava-reference"))
    coordinates = [("com.google.guava", "guava", GUAVA_VERSION, ""),
                   ("com.google.guava", "failureaccess", FAILUREACCESS_VERSION, "")]
    coordinates += [("org.apache.curator", name, CURATOR_VERSION, classifier)
                    for name in CURATOR_MODULES for classifier in ("osgi", "")]
    for group, artifact, version, classifier in coordinates:
        gav = f"{group}:{artifact}:{version}:jar" + (f":{classifier}" if classifier else "")
        run(["org.apache.maven.plugins:maven-dependency-plugin:3.8.1:get", f"-Dartifact={gav}",
             "-Dtransitive=false"], reference_dir)
    fixed_inputs = {"guava": resolved("com.google.guava", "guava", GUAVA_VERSION),
                    "failureaccess": resolved("com.google.guava", "failureaccess", FAILUREACCESS_VERSION)}
    multi_release = any(has_multi_release_implementation(path) for path in fixed_inputs.values())
    (reference_dir / "pom.xml").write_text(project_pom("curator-guava-reference", multi_release=multi_release))
    documents = preserve_license_sources(reference_dir, fixed_inputs)
    manifests = preserve_manifest_sources(reference_dir, fixed_inputs)
    jpms = preserve_module_descriptors(reference_dir, fixed_inputs)
    run(["-Dmaven.test.skip=true", "package"], reference_dir)
    reference = reference_dir / "target" / f"curator-guava-reference-{CURATOR_VARIANT_VERSION}.jar"
    modules = {}
    for module in CURATOR_MODULES:
        directory = work / module
        directory.mkdir(parents=True, exist_ok=True)
        pom = project_pom(module, multi_release=multi_release and module == "curator-client")
        (directory / "pom.xml").write_text(pom)
        original = resolved("org.apache.curator", module, CURATOR_VERSION, "osgi")
        preserve_license_sources(directory, {module: original})
        preserve_manifest_sources(directory, {f"{module}/osgi": original,
            f"{module}/standard": resolved("org.apache.curator", module, CURATOR_VERSION)})
        if module == "curator-client":
            preserve_license_sources(directory, fixed_inputs)
            preserve_manifest_sources(directory, fixed_inputs)
            preserve_module_descriptors(directory, fixed_inputs)
        run(["-Dmaven.test.skip=true", "install"], directory)
        target = directory / "target" / f"{module}-{CURATOR_VARIANT_VERSION}.jar"
        installed = resolved(CURATOR_VARIANT_GROUP, module, CURATOR_VARIANT_VERSION)
        if digest(target) != digest(installed):
            raise ValueError("installed compatible Curator assembly differs from Maven output")
        report = verify_curator_shaded_module(target,
            resolved("org.apache.curator", module, CURATOR_VERSION, "osgi"),
            resolved("org.apache.curator", module, CURATOR_VERSION), module,
            fixed_inputs["guava"], fixed_inputs["failureaccess"], reference,
            reviewed_metadata=CURATOR_REVIEWED_METADATA)
        resources = directory / "src/main/resources"
        preserved = {str(path.relative_to(resources)): digest(path) for path in resources.rglob("*") if path.is_file()}
        with ZipFile(target) as jar:
            for entry, expected in preserved.items():
                if jar.namelist().count(entry) != 1 or hashlib.sha256(jar.read(entry)).hexdigest() != expected:
                    raise ValueError(f"source-time upstream document copy not preserved by Curator assembly: {entry}")
        report["qualified_source_documents"] = preserved
        report["maven_target_sha256"] = digest(target)
        report["maven_installed_jar_sha256"] = digest(installed)
        report["source_pom_sha256"] = hashlib.sha256(pom.encode()).hexdigest()
        installed_pom = installed.with_suffix(".pom")
        reduced_pom = directory / "dependency-reduced-pom.xml"
        if installed_pom.read_bytes() != reduced_pom.read_bytes():
            raise ValueError("installed compatible Curator POM differs from Maven dependency-reduced POM")
        report["installed_pom_sha256"] = digest(installed_pom)
        modules[module] = report
    result = {"method": "official complete pre-Shade osgi modules assembled by Maven with original Curator relocation and fixed dependencies",
              "original_curator_version": CURATOR_VERSION, "custom_group": CURATOR_VARIANT_GROUP,
              "custom_version": CURATOR_VARIANT_VERSION, "upstream_java_source_changes": False,
              "finished_jar_edits": False, "guava_version": GUAVA_VERSION,
              "failureaccess_version": FAILUREACCESS_VERSION,
              "fixed_input_sha256": {name: digest(path) for name, path in fixed_inputs.items()},
              "fixed_only_reference_sha256": digest(reference), "reference_is_build_only": True,
              "multi_release_implementation_inputs": multi_release,
              "qualified_unchanged_dependency_manifests": manifests,
              "qualified_unchanged_input_jpms_descriptors": jpms,
              "qualified_unchanged_dependency_documents": documents, "modules": modules,
              "commands": commands, "build_log_sha256": digest(log_path), "command_exit": 0,
              "limits": "Only the client contains fixed Guava. Module class sets, public/protected ABI and actual fixed Guava class bytes are checked before Hadoop assembly. Final-image linkage/regression and fresh scans remain required."}
    (output / "curator-bundle-build.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--maven", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    build(args.maven, args.source_root, args.output)
