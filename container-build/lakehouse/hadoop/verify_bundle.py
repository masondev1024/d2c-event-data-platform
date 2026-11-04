"""Verify the resolved module graph and generated JARs without editing either."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import struct
from zipfile import ZipFile

from patch_pom import patch_source
from runtime_profile import (
    DEPENDENCY_VERSIONS, EXCLUDED_MODULES, FILESYSTEM_PROVIDERS, LOCAL_PROVIDER,
    MAVEN_VERSION, MODULES, PROTOCOL_SERVICE, REQUIRED_MODULES, SOURCE_INPUTS, SOURCE_ROOT_URL,
    SUPPORTED_PROFILE, UNSUPPORTED_CAPABILITIES, UPSTREAM_VERSION, VARIANT_VERSION,
)

REQUIRED_CLASSES = (
    "org/apache/hadoop/conf/Configuration", "org/apache/hadoop/fs/FileSystem",
    "org/apache/hadoop/fs/LocalFileSystem", "org/apache/hadoop/fs/RawLocalFileSystem",
    "org/apache/hadoop/fs/Path", "org/apache/hadoop/fs/FSDataInputStream",
    "org/apache/hadoop/fs/FSDataOutputStream", "org/apache/hadoop/io/Text",
    "org/apache/hadoop/security/UserGroupInformation", "org/apache/hadoop/mapred/JobConf",
    "org/apache/hadoop/mapred/LocalJobRunner", "org/apache/hadoop/mapred/LocalClientProtocolProvider",
    "org/apache/hadoop/mapreduce/Job", "org/apache/hadoop/mapreduce/TaskAttemptContext",
    "org/apache/hadoop/mapreduce/lib/input/FileInputFormat",
    "org/apache/hadoop/mapreduce/lib/output/FileOutputFormat",
    "org/apache/hadoop/mapreduce/lib/output/FileOutputCommitter",
    "org/apache/hadoop/mapreduce/task/JobContextImpl",
    "org/apache/hadoop/mapreduce/task/TaskAttemptContextImpl",
)
# This provider is retained, not hidden. Its renew/cancel operations require
# TimelineClient, a YARN capability outside this local-only image's contract.
OPTIONAL_YARN_CALLERS = {
    "org/apache/hadoop/yarn/security/client/TimelineDelegationTokenIdentifier$Renewer",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def validate_maven_version(report: str) -> str:
    plain = re.sub(r"\x1b\[[0-9;]*m", "", report)
    match = re.search(r"^Apache Maven ([0-9.]+)(?:[ \r]|$)", plain, re.MULTILINE)
    require(match is not None and match.group(1) == MAVEN_VERSION,
            f"expected actual Maven {MAVEN_VERSION} --version report")
    return match.group(1)


def class_references(content: bytes) -> set[str]:
    """Read JVM Class constants and descriptors; do not mistake package names for ownership."""
    require(content[:4] == b"\xca\xfe\xba\xbe", "invalid class header")
    count = struct.unpack_from(">H", content, 8)[0]
    utf8 = {}
    names = []
    index, offset = 1, 10
    while index < count:
        tag = content[offset]
        offset += 1
        if tag == 1:
            size = struct.unpack_from(">H", content, offset)[0]
            offset += 2
            utf8[index] = content[offset:offset + size].decode("utf-8", errors="replace")
            offset += size
        elif tag == 7:
            names.append(struct.unpack_from(">H", content, offset)[0])
            offset += 2
        elif tag in (8, 16, 19, 20):
            offset += 2
        elif tag in (3, 4, 9, 10, 11, 12, 17, 18):
            offset += 4
        elif tag in (5, 6):
            offset += 8
            index += 1
        elif tag == 15:
            offset += 3
        else:
            raise ValueError(f"unknown JVM constant-pool tag: {tag}")
        index += 1
    refs = {utf8[name] for name in names}
    for value in utf8.values():
        refs.update(re.findall(r"L([a-zA-Z_$][\w$/]*);", value))
    return refs


def excluded_client_reference(name: str) -> bool:
    return name.startswith((
        "org/apache/hadoop/yarn/client/api/", "org/apache/hadoop/yarn/client/cli/",
        "org/apache/hadoop/yarn/client/util/",
    )) or name == "org/apache/hadoop/yarn/client/SCMAdmin"


def validate_graph(graph: dict, module: str) -> dict:
    require(graph.get("artifactId") == module and graph.get("version") == VARIANT_VERSION,
            f"incorrect graph root: {module}")
    coordinates = set()
    edges = []

    def visit(node: dict, parent: str | None = None) -> None:
        coordinate = f"{node['groupId']}:{node['artifactId']}:{node['version']}"
        coordinates.add(coordinate)
        if parent is not None:
            edges.append([parent, coordinate])
        if node["groupId"] == "org.apache.hadoop":
            allowed = VARIANT_VERSION if node["artifactId"] in MODULES else UPSTREAM_VERSION
            require(node["version"] == allowed, f"mixed Hadoop versions: {coordinate}")
            require(node["artifactId"] not in EXCLUDED_MODULES, f"unsupported module reintroduced: {coordinate}")
        require(not (node["groupId"] == "org.eclipse.jetty" and node["artifactId"] in ("jetty-client", "jetty-http")),
                f"unexpected HTTP client/parser dependency: {coordinate}")
        require(not node["groupId"].startswith("org.eclipse.jetty.websocket"),
                f"unexpected WebSocket dependency: {coordinate}")
        for child in node.get("children", []):
            visit(child, coordinate)

    visit(graph)
    for required in REQUIRED_MODULES:
        require(f"org.apache.hadoop:{required}:{UPSTREAM_VERSION}" in coordinates,
                f"required local data-processing module absent: {required}")
    if module == "hadoop-client-runtime":
        require(f"org.apache.hadoop:hadoop-client-api:{VARIANT_VERSION}" in coordinates,
                "runtime must resolve the matching locally rebuilt API")
    return {"coordinates": sorted(coordinates), "edges": edges}


def service_providers(content: bytes) -> list[str]:
    return [line.split("#", 1)[0].strip() for line in content.decode("utf-8").splitlines()
            if line.split("#", 1)[0].strip()]


def validate_services(services: dict[str, set[str]], classes: set[str]) -> None:
    for service, providers in services.items():
        for provider in providers:
            require(provider.replace(".", "/") in classes,
                    f"service provider missing from generated pair: {service}: {provider}")
    require(services.get(PROTOCOL_SERVICE) == {LOCAL_PROVIDER},
            "local ClientProtocolProvider must be retained without a dangling YARN provider")
    require(services.get("org.apache.hadoop.fs.FileSystem") == set(FILESYSTEM_PROVIDERS),
            "upstream local/HDFS filesystem service providers changed")


def properties(content: bytes) -> dict[str, str]:
    result = {}
    for line in content.decode("utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith(("#", "!")):
            key, value = line.split("=", 1)
            result[key.strip()] = value.strip()
    return result


def verify(source_root: Path, output: Path) -> dict:
    version_report = (output / "maven-version.txt").read_bytes()
    maven_version = validate_maven_version(version_report.decode())
    sources = []
    for relative, expected in SOURCE_INPUTS.items():
        path = source_root / relative
        if path.name == "pom.xml":
            path = path.with_name("upstream.pom.xml")
        content = path.read_bytes()
        require(hashlib.sha256(content).hexdigest() == expected, f"source hash mismatch: {relative}")
        sources.append({"url": SOURCE_ROOT_URL + relative, "sha256": expected})
    graphs, recipes = {}, {}
    for module in MODULES:
        directory = source_root / "hadoop-client-modules" / module
        original = (directory / "upstream.pom.xml").read_text()
        patched = (directory / "pom.xml").read_text()
        require(patched == patch_source(original, module), f"unexpected source recipe edit: {module}")
        recipes[module] = hashlib.sha256(patched.encode()).hexdigest()
        graph_file = output / f"{module}-dependency-tree.json"
        graphs[module] = validate_graph(json.loads(graph_file.read_text()), module)

    classes, services, artifacts, jetty_modules = set(), {}, [], set()
    yarn_references, jetty_references = {}, {}
    for module in MODULES:
        path = output / f"{module}-{VARIANT_VERSION}.jar"
        with ZipFile(path) as jar:
            metadata = properties(jar.read(f"META-INF/maven/org.apache.hadoop/{module}/pom.properties"))
            require(metadata.get("version") == VARIANT_VERSION, f"incorrect local artifact metadata: {module}")
            for name in jar.namelist():
                if name.startswith("META-INF/services/") and not name.endswith("/"):
                    services.setdefault(name.removeprefix("META-INF/services/"), set()).update(service_providers(jar.read(name)))
                if name.startswith("META-INF/maven/org.eclipse.jetty") and name.endswith("/pom.properties"):
                    jetty_modules.add(name.removeprefix("META-INF/maven/").removesuffix("/pom.properties"))
                if not name.endswith(".class"):
                    continue
                binary_name = name.removesuffix(".class")
                classes.add(binary_name)
                require(not excluded_client_reference(binary_name), f"excluded YARN implementation present: {name}")
                require(binary_name not in ("org/apache/hadoop/mapred/YarnClientProtocolProvider",
                        "org/apache/hadoop/mapred/YARNRunner", "org/apache/hadoop/mapred/ResourceMgrDelegate"),
                        f"excluded jobclient implementation present: {name}")
                require(not binary_name.startswith((
                    "org/apache/hadoop/shaded/org/eclipse/jetty/client/",
                    "org/apache/hadoop/shaded/org/eclipse/jetty/http/",
                    "org/apache/hadoop/shaded/org/eclipse/jetty/websocket/",
                )), f"unexpected excluded subsystem implementation: {name}")
                if binary_name.startswith("org/apache/hadoop/") and "/shaded/" not in binary_name:
                    refs = class_references(jar.read(name))
                    yarn = sorted(ref for ref in refs if excluded_client_reference(ref))
                    jetty = sorted(ref for ref in refs if "/org/eclipse/jetty/" in ref)
                    if yarn:
                        yarn_references[binary_name] = yarn
                        require(binary_name in OPTIONAL_YARN_CALLERS,
                                f"unreviewed reference to removed YARN client: {binary_name}: {yarn}")
                    if jetty:
                        jetty_references[binary_name] = jetty
                    if binary_name in REQUIRED_CLASSES:
                        require(not yarn and not jetty, f"required local API references an excluded subsystem: {name}")
            if module == "hadoop-client-runtime":
                for coordinate, version in DEPENDENCY_VERSIONS.items():
                    value = properties(jar.read(f"META-INF/maven/{coordinate}/pom.properties"))
                    require(value.get("version") == version, f"incorrect patched library: {coordinate}")
                for package in ("core/json", "databind/cfg"):
                    name = f"org/apache/hadoop/shaded/com/fasterxml/jackson/{package}/PackageVersion.class"
                    require(b"2.21.7" in jar.read(name), f"incorrect actual Jackson implementation: {name}")
            require("META-INF/LICENSE.txt" in jar.namelist() and "META-INF/NOTICE.txt" in jar.namelist(),
                    f"upstream license/notice missing: {module}")
        artifacts.append({"upstream": f"org.apache.hadoop:{module}:{UPSTREAM_VERSION}",
                          "coordinate": f"org.apache.hadoop:{module}:{VARIANT_VERSION}",
                          "file": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    require(set(REQUIRED_CLASSES) <= classes, "required local filesystem/MapReduce API classes missing")
    validate_services(services, classes)
    return {
        "variant": VARIANT_VERSION, "profile": SUPPORTED_PROFILE,
        "method": "upstream source-free API/runtime shade assemblies; whole-module selection before assembly",
        "excluded_modules": list(EXCLUDED_MODULES), "unsupported_capabilities": list(UNSUPPORTED_CAPABILITIES),
        "additional_unsupported_capability": "YARN Timeline delegation-token renewal/cancellation; retained provider's lazy TimelineClient reference is reported, not removed",
        "sources": sources, "patched_recipe_sha256": recipes, "artifacts": artifacts,
        "tools": {"maven": maven_version, "maven-shade-plugin": "3.6.1", "maven-dependency-plugin": "3.8.1",
                  "maven_version_report_sha256": hashlib.sha256(version_report).hexdigest()},
        "verified_dependency_versions": DEPENDENCY_VERSIONS, "resolved_graphs": graphs,
        "service_providers": {name: sorted(values) for name, values in sorted(services.items())},
        "yarn_client_symbolic_references": yarn_references,
        "jetty_symbolic_references_in_optional_hadoop_classes": jetty_references,
        "retained_jetty_module_metadata": sorted(jetty_modules),
        "verification": {"source_graph_and_generated_artifact_checks": "PASS",
                         "dynamic_boot_and_service_instantiation": "NOT_RUN",
                         "D2C_functional_regression": "NOT_RUN", "vulnerability_scan": "NOT_RUN"},
        "limits": "Static checks do not prove runtime closure, vulnerability absence or whole-Hadoop support. Final image boot, ServiceLoader and D2C regressions plus fresh scans are required.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-maven", type=Path)
    parser.add_argument("source_root", type=Path, nargs="?")
    parser.add_argument("output", type=Path, nargs="?")
    args = parser.parse_args()
    if args.check_maven is not None:
        validate_maven_version(args.check_maven.read_text())
    else:
        if args.source_root is None or args.output is None:
            parser.error("source_root and output are required for artifact verification")
        result = verify(args.source_root, args.output)
        (args.output / "provenance.json").write_text(json.dumps(result, indent=2) + "\n")
