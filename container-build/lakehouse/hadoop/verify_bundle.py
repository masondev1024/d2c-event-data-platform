"""Verify the resolved module graph and generated JARs without editing either."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import struct
from zipfile import ZipFile
import xml.etree.ElementTree as ET

from patch_pom import patch_source
from curator_compat import verify_curator_shaded_module
from rebuild_curator import project_pom
from jline_resources import (LOADER_SHA256, LOADER_SOURCE, SOURCE_ROOT as JLINE_SOURCE_ROOT,
                             VERSION as JLINE_VERSION, resource_kind, validate_jline_resources)
from runtime_profile import (
    CURATOR_CLASSIFIER, CURATOR_MODULES, CURATOR_REVIEWED_METADATA, CURATOR_SOURCE_INPUTS, CURATOR_SOURCE_ROOT_URL, CURATOR_VERSION,
    CURATOR_VARIANT_GROUP, CURATOR_VARIANT_VERSION, FAILUREACCESS_VERSION,
    DEPENDENCY_VERSIONS, EXCLUDED_ARTIFACTS, EXCLUDED_MODULES, FILESYSTEM_PROVIDERS, LOCAL_PROVIDER,
    GUAVA_VERSION, HADOOP_SHADED_GUAVA_VERSION, MAVEN_VERSION, MODULES, PROTOCOL_SERVICE, REQUIRED_MODULES, SOURCE_INPUTS, SOURCE_ROOT_URL,
    SNAPPY_VERSION, SOURCE_REVIEWS, SUPPORTED_PROFILE, THIRDPARTY_SOURCE_INPUTS, THIRDPARTY_SOURCE_ROOT_URL,
    UNSUPPORTED_CAPABILITIES, UPSTREAM_VERSION, VARIANT_VERSION,
)

REQUIRED_CLASSES = (
    "org/apache/hadoop/conf/Configuration", "org/apache/hadoop/fs/FileSystem",
    "org/apache/hadoop/fs/LocalFileSystem", "org/apache/hadoop/fs/RawLocalFileSystem",
    "org/apache/hadoop/fs/Path", "org/apache/hadoop/fs/FSDataInputStream",
    "org/apache/hadoop/fs/FSDataOutputStream", "org/apache/hadoop/io/Text",
    "org/apache/hadoop/security/UserGroupInformation", "org/apache/hadoop/mapred/JobConf",
    "org/apache/hadoop/mapred/Master", "org/apache/hadoop/yarn/client/util/YarnClientUtils",
    "org/apache/hadoop/mapred/LocalJobRunner", "org/apache/hadoop/mapred/LocalClientProtocolProvider",
    "org/apache/hadoop/mapreduce/Job", "org/apache/hadoop/mapreduce/TaskAttemptContext",
    "org/apache/hadoop/mapreduce/lib/input/FileInputFormat",
    "org/apache/hadoop/mapreduce/lib/output/FileOutputFormat",
    "org/apache/hadoop/mapreduce/lib/output/FileOutputCommitter",
    "org/apache/hadoop/mapreduce/task/JobContextImpl",
    "org/apache/hadoop/mapreduce/task/TaskAttemptContextImpl",
)
TRANSPORT_PREFIX = "org/apache/hadoop/shaded/org/eclipse/jetty/websocket/"
REMOVED_JOBCLIENT_CLASSES = {
    "org/apache/hadoop/mapred/YarnClientProtocolProvider",
    "org/apache/hadoop/mapred/YARNRunner", "org/apache/hadoop/mapred/ResourceMgrDelegate",
}
GUAVA_PREFIXES = (
    "org/apache/hadoop/shaded/com/google/common/",
    "org/apache/hadoop/thirdparty/com/google/common/",
    "org/apache/hadoop/shaded/org/apache/curator/shaded/com/google/common/",
)


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


def excluded_transport_reference(name: str) -> bool:
    return name.startswith((TRANSPORT_PREFIX, "org/eclipse/jetty/websocket/"))


def validate_class_references(binary_name: str, refs: set[str]) -> list[str]:
    missing_transport = {ref for ref in refs if excluded_transport_reference(ref)}
    allowed = {TRANSPORT_PREFIX + suffix for suffix in SOURCE_REVIEWS.get(binary_name, {}).get("missing_transport_targets", ())}
    require(missing_transport <= allowed,
            f"unreviewed reference to removed WebSocket transport: {binary_name}: {sorted(missing_transport)}")
    require(not refs & REMOVED_JOBCLIENT_CLASSES, f"reference to excluded remote jobclient: {binary_name}")
    if binary_name in REQUIRED_CLASSES:
        jetty = {ref for ref in refs if ref.startswith(("org/eclipse/jetty/", "org/apache/hadoop/shaded/org/eclipse/jetty/"))}
        require(not jetty, f"required local API references a Jetty subsystem: {binary_name}")
    return sorted(missing_transport)


def validate_graph(graph: dict, module: str) -> dict:
    require(graph.get("groupId") == "org.apache.hadoop" and graph.get("artifactId") == module
            and graph.get("version") == VARIANT_VERSION,
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
        if node["groupId"] == "org.jline":
            require(node["version"] == JLINE_VERSION, f"unpatched interactive library: {coordinate}")
        if (node["groupId"], node["artifactId"]) == ("com.google.guava", "guava"):
            require(node["version"] == GUAVA_VERSION, f"unpatched direct Guava library: {coordinate}")
        if (node["groupId"], node["artifactId"]) == ("com.google.guava", "failureaccess"):
            require(node["version"] == FAILUREACCESS_VERSION, f"incorrect Guava failureaccess dependency: {coordinate}")
        if (node["groupId"], node["artifactId"]) == ("org.apache.hadoop.thirdparty", "hadoop-shaded-guava"):
            require(node["version"] == HADOOP_SHADED_GUAVA_VERSION,
                    f"unpatched pre-shaded Hadoop Guava library: {coordinate}")
        if node["groupId"] == "org.apache.curator":
            raise ValueError(f"original Curator input reintroduced into final Hadoop graph: {coordinate}")
        if node["groupId"] == CURATOR_VARIANT_GROUP:
            require(node["artifactId"] in CURATOR_MODULES and node["version"] == CURATOR_VARIANT_VERSION
                    and not node.get("classifier"),
                    f"unexpected partial/unverified Curator assembly: {coordinate}: {node.get('classifier')}")
        if (node["groupId"], node["artifactId"]) == ("org.xerial.snappy", "snappy-java"):
            require(node["version"] == SNAPPY_VERSION, f"unpatched external native Snappy dependency: {coordinate}")
        require((node["groupId"], node["artifactId"]) not in EXCLUDED_ARTIFACTS,
                f"excluded profile artifact reintroduced: {coordinate}")
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
    for coordinate in (f"com.google.guava:guava:{GUAVA_VERSION}",
                       f"org.apache.hadoop.thirdparty:hadoop-shaded-guava:{HADOOP_SHADED_GUAVA_VERSION}"):
        require(coordinate in coordinates, f"required patched Guava dependency absent: {coordinate}")
    for name in CURATOR_MODULES:
        require(f"{CURATOR_VARIANT_GROUP}:{name}:{CURATOR_VARIANT_VERSION}" in coordinates,
                f"required complete Curator module absent: {name}")
    require(f"org.xerial.snappy:snappy-java:{SNAPPY_VERSION}" in coordinates,
            "required patched external native Snappy dependency absent")
    if module == "hadoop-client-runtime":
        require(f"org.apache.hadoop:hadoop-client-api:{VARIANT_VERSION}" in coordinates,
                "runtime must resolve the matching locally rebuilt API")
    return {"coordinates": sorted(coordinates), "edges": edges,
            "complete_curator_compatible_assemblies": {name: f"{CURATOR_VARIANT_GROUP}:{name}:{CURATOR_VARIANT_VERSION}"
                                                       for name in CURATOR_MODULES}}


def service_providers(content: bytes) -> list[str]:
    providers = [line.split("#", 1)[0].strip() for line in content.decode("utf-8").splitlines()
                 if line.split("#", 1)[0].strip()]
    require(all(re.fullmatch(r"[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)+", name, re.ASCII)
                for name in providers), "malformed Java SPI provider class name")
    return providers


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


def required_jar_entry(jar: ZipFile, name: str) -> bytes:
    require(jar.namelist().count(name) == 1, f"required unique JAR metadata absent/duplicated: {name}")
    return jar.read(name)


def verify_maven_properties(content: bytes, group: str, artifact: str, version: str) -> dict:
    actual = properties(content)
    expected = {"groupId": group, "artifactId": artifact, "version": version}
    require(all(actual.get(key) == value for key, value in expected.items()),
            f"incorrect Maven metadata: {group}:{artifact}:{version}")
    return expected


def validate_retained_dependency_metadata(jar: ZipFile) -> dict:
    paths = {coordinate: f"META-INF/maven/{coordinate}/pom.properties" for coordinate in DEPENDENCY_VERSIONS}
    invalid = [name for name in paths.values() if jar.namelist().count(name) != 1]
    require(not invalid, f"retained dependency Maven metadata missing/duplicated: {invalid}")
    return {coordinate: verify_maven_properties(jar.read(paths[coordinate]), *coordinate.split("/"), version)
            for coordinate, version in DEPENDENCY_VERSIONS.items()}


def validate_guava_implementations(jar: ZipFile, direct: ZipFile, thirdparty: ZipFile,
                                   curator: ZipFile, failureaccess: ZipFile) -> dict:
    """Bind all three generated Guava namespaces to complete fixed inputs."""
    verify_maven_properties(required_jar_entry(direct, "META-INF/maven/com.google.guava/guava/pom.properties"),
                            "com.google.guava", "guava", GUAVA_VERSION)
    verify_maven_properties(required_jar_entry(failureaccess, "META-INF/maven/com.google.guava/failureaccess/pom.properties"),
                            "com.google.guava", "failureaccess", FAILUREACCESS_VERSION)
    verify_maven_properties(required_jar_entry(thirdparty, "META-INF/maven/org.apache.hadoop.thirdparty/hadoop-shaded-guava/pom.properties"),
                            "org.apache.hadoop.thirdparty", "hadoop-shaded-guava", HADOOP_SHADED_GUAVA_VERSION)
    # This input is pre-shaded; its own version alone does not prove its embedded
    # Guava version. Require the retained original Guava descriptor as well.
    verify_maven_properties(required_jar_entry(thirdparty, "META-INF/maven/com.google.guava/guava/pom.properties"),
                            "com.google.guava", "guava", GUAVA_VERSION)
    names = jar.namelist()
    google_prefixes = tuple(prefix.removesuffix("common/") for prefix in GUAVA_PREFIXES)
    implementation = lambda name: (name.endswith(".class") and any(
        token in "/" + name for token in ("/com/google/common/", "/com/google/thirdparty/")))
    actual = {name for name in names if implementation(name)}
    expected_direct = {name.replace("com/google/", google_prefixes[0], 1)
                       for name in (*direct.namelist(), *failureaccess.namelist())
                       if implementation(name)}
    expected_thirdparty = {name for name in thirdparty.namelist()
                          if implementation(name) and google_prefixes[1] in name}
    def outer_relocation(name):
        version = re.match(r"META-INF/versions/[1-9][0-9]*/", name)
        offset = version.end() if version else 0
        return name[:offset] + "org/apache/hadoop/shaded/" + name[offset:]
    expected_curator = {outer_relocation(name) for name in curator.namelist() if implementation(name)}
    expected = expected_direct | expected_thirdparty | expected_curator
    require(bool(expected_direct) and bool(expected_thirdparty) and bool(expected_curator),
            "fixed Guava input implementation absent")
    require(actual == expected,
            f"generated Guava implementation namespaces/classes differ from fixed complete inputs: "
            f"missing={sorted(expected - actual)[:10]}, unexpected={sorted(actual - expected)[:10]}")
    require(all(names.count(name) == 1 for name in actual), "duplicate generated Guava implementation class")
    for prefix in GUAVA_PREFIXES:
        for name in ("CompactHashMap", "CompactHashSet", "MapMakerInternalMap"):
            require(prefix + "collect/" + name + ".class" in actual,
                    f"required actual Guava implementation missing: {prefix}{name}")
    return {"guava_version": GUAVA_VERSION, "hadoop_shaded_guava_version": HADOOP_SHADED_GUAVA_VERSION,
            "direct_namespace": GUAVA_PREFIXES[0], "direct_classes": len(expected_direct),
            "failureaccess_version": FAILUREACCESS_VERSION,
            "pre_shaded_namespace": GUAVA_PREFIXES[1], "pre_shaded_classes": len(expected_thirdparty),
            "compatible_curator_namespace": GUAVA_PREFIXES[2], "compatible_curator_classes": len(expected_curator),
            "complete_fixed_input_class_inventory_matches": True,
            "limits": "The original Maven assembly selects these resolved inputs. Inventory and identity checks do not independently prove executable method behavior; runtime and fresh image scans remain required."}


def validate_guava_references(binary_name: str, refs: set[str]) -> None:
    for ref in refs:
        name = ref.lstrip("[")
        if name.startswith("L") and name.endswith(";"):
            name = name[1:-1]
        if any(token in "/" + name for token in ("/com/google/common/", "/com/google/thirdparty/")):
            require(name.startswith(tuple(prefix.removesuffix("common/") for prefix in GUAVA_PREFIXES)),
                    f"unexpected Guava reference namespace: {binary_name}: {ref}")


def validate_no_embedded_snappy(name: str, refs: set[str] = frozenset()) -> None:
    # Upstream explicitly leaves the complete native snappy-java artifact out
    # of both assemblies. Its normal external dependency remains visible in the
    # Maven graph. Any pre-shaded implementation/native payload is unexpected.
    require(not ("org/xerial/snappy/" in name or "org.xerial.snappy/" in name
                 or re.search(r"(?i)snappyjava\.(?:so|dll|dylib)(?:$|\.)", name)),
            f"unexpected embedded Snappy implementation/metadata/native payload: {name}")
    for ref in refs:
        target = ref.lstrip("[")
        if target.startswith("L") and target.endswith(";"):
            target = target[1:-1]
        if "/org/xerial/snappy/" in "/" + target:
            require(target.startswith("org/xerial/snappy/"),
                    f"unexpected pre-shaded Snappy reference: {name}: {ref}")


def validate_final_curator_inventory(jar: ZipFile, selected_modules: dict[str, Path]) -> dict:
    expected, module_counts = set(), {}
    for module, path in selected_modules.items():
        with ZipFile(path) as source:
            own = {name for name in source.namelist()
                   if name.startswith("org/apache/curator/") and name.endswith(".class")}
        require(bool(own), f"selected Curator module has no implementations: {module}")
        require(not expected & own, f"selected Curator modules overlap own classes: {module}")
        expected.update(own)
        module_counts[module] = len(own)
    prefix = "org/apache/hadoop/shaded/"
    actual = {name.removeprefix(prefix) for name in jar.namelist()
              if name.startswith(prefix + "org/apache/curator/") and name.endswith(".class")}
    require(actual == expected,
            f"generated complete Curator module class inventory changed: "
            f"missing={sorted(expected - actual)[:10]}, unexpected={sorted(actual - expected)[:10]}")
    require(all(jar.namelist().count(prefix + name) == 1 for name in expected),
            "duplicate generated Curator implementation class")
    return {"complete_selected_module_classes_match": True, "module_class_counts": module_counts,
            "generated_namespace": prefix + "org/apache/curator/"}


def validate_final_curator_documents(jar: ZipFile, modules: dict) -> dict:
    expected = {}
    for module, report in modules.items():
        docs = report["qualified_source_documents"]
        require(bool(docs), f"upstream Curator document receipt absent: {module}")
        for entry, digest in docs.items():
            require(entry.startswith(("META-INF/d2c-licenses/", "META-INF/d2c-upstream/"))
                    and entry.endswith(".original") and re.fullmatch(r"[a-f0-9]{64}", digest) is not None,
                    f"unexpected upstream document preservation receipt: {entry}")
            require(entry not in expected or expected[entry] == digest, "upstream document receipt collision")
            expected[entry] = digest
    for entry, digest in expected.items():
        require(hashlib.sha256(required_jar_entry(jar, entry)).hexdigest() == digest,
                f"original Curator/Guava document lost or changed during Hadoop assembly: {entry}")
    return expected


def generated_pom_gav(content: bytes) -> tuple[str, str, str]:
    root = ET.fromstring(content)
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    group = root.findtext("m:groupId", namespaces=namespace) or root.findtext("m:parent/m:groupId", namespaces=namespace)
    artifact = root.findtext("m:artifactId", namespaces=namespace)
    version = root.findtext("m:version", namespaces=namespace)
    require((root.findtext("m:packaging", namespaces=namespace) or "jar") == "jar", "generated artifact POM must describe a JAR")
    return group, artifact, version


def runtime_descriptor_filter_contract(original: bytes) -> dict:
    relative = "hadoop-client-modules/hadoop-client-runtime/pom.xml"
    digest = hashlib.sha256(original).hexdigest()
    require(digest == SOURCE_INPUTS[relative], "runtime identity requires the exact pinned upstream POM hash")
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    root = ET.fromstring(original)
    filters = []
    for profile in root.findall("m:profiles/m:profile", namespace):
        if profile.findtext("m:id", namespaces=namespace) != "shade":
            continue
        for plugin in profile.findall("m:build/m:plugins/m:plugin", namespace):
            if plugin.findtext("m:artifactId", namespaces=namespace) == "maven-shade-plugin":
                filters.extend(plugin.findall("m:executions/m:execution/m:configuration/m:filters/m:filter", namespace))
    matches = [item for item in filters if item.findtext("m:artifact", namespaces=namespace) == "org.apache.hadoop:*"]
    require(len(matches) == 1, "upstream runtime Hadoop artifact filter changed")
    item = matches[0]
    excludes = [node.text for node in item.findall("m:excludes/m:exclude", namespace)]
    require(excludes == ["**/*", "*"] and item.find("m:includes", namespace) is None,
            "upstream runtime descriptor-absence contract changed")
    contract = {"artifact": "org.apache.hadoop:*", "excludes": excludes}
    return {"upstream_pom_sha256": digest,
            "filter": contract,
            "filter_sha256": hashlib.sha256(json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
            "filter_hash_format": "SHA-256 of canonical JSON for the exact artifact/excludes filter",
            "behavior": "The unchanged upstream runtime filter removes all entries of Hadoop artifacts, including its own Maven descriptor. Retained third-party Maven descriptors remain mandatory."}


def validate_artifact_identity(module: str, jar: ZipFile, *, original_pom: bytes,
                               exported_source_pom: bytes, installed_pom: bytes,
                               generated_dependency_reduced_pom: bytes, resolved_graph: dict,
                               artifact_sha256: str, built_artifact_sha256: str) -> dict:
    require(module in MODULES, "unknown Hadoop assembly identity")
    require(re.fullmatch(r"[a-f0-9]{64}", artifact_sha256) is not None
            and artifact_sha256 == built_artifact_sha256,
            f"exported JAR differs from the actual Maven target artifact: {module}")
    expected_source = patch_source(original_pom.decode("utf-8"), module).encode("utf-8")
    require(exported_source_pom == expected_source, f"exported generated source POM differs from the reviewed recipe: {module}")
    require(installed_pom == generated_dependency_reduced_pom,
            f"actual installed POM differs from Maven's generated dependency-reduced POM: {module}")
    gav = ("org.apache.hadoop", module, VARIANT_VERSION)
    require(generated_pom_gav(exported_source_pom) == gav, f"generated source POM has the wrong root GAV: {module}")
    require(generated_pom_gav(installed_pom) == gav, f"Maven installed POM has the wrong root GAV: {module}")
    validate_graph(resolved_graph, module)
    descriptor_root = f"META-INF/maven/org.apache.hadoop/{module}/"
    if module == "hadoop-client-runtime":
        contract = runtime_descriptor_filter_contract(original_pom)
        expected_absent = [descriptor_root + name for name in ("pom.properties", "pom.xml")]
        require(all(name not in jar.namelist() for name in expected_absent),
                "runtime own descriptor unexpectedly present; review the upstream assembly contract")
        method = "exact upstream descriptor absence; generated/installed POM and resolved root GAV plus matching Maven-target/exported JAR SHA-256"
        descriptors = {"expected_absent": expected_absent, "unchanged_upstream_filter": contract}
    else:
        verify_maven_properties(required_jar_entry(jar, descriptor_root + "pom.properties"), *gav)
        method = "required embedded project Maven properties plus generated/installed POM, resolved root GAV and matching Maven-target/exported JAR SHA-256"
        descriptors = {"required_embedded_properties": descriptor_root + "pom.properties"}
    return {"method": method, "gav": ":".join(gav), "artifact_sha256": artifact_sha256,
            "maven_target_artifact_sha256": built_artifact_sha256,
            "generated_source_pom_sha256": hashlib.sha256(exported_source_pom).hexdigest(),
            "maven_installed_pom_sha256": hashlib.sha256(installed_pom).hexdigest(),
            "generated_dependency_reduced_pom_sha256": hashlib.sha256(generated_dependency_reduced_pom).hexdigest(),
            "installed_pom_matches_generated_dependency_reduced_pom": True,
            "resolved_root_gav": ":".join((resolved_graph["groupId"], resolved_graph["artifactId"], resolved_graph["version"])),
            "project_descriptor_contract": descriptors}


def verify(source_root: Path, output: Path) -> dict:
    version_report = (output / "maven-version.txt").read_bytes()
    maven_version = validate_maven_version(version_report.decode())
    loader = (source_root / "jline-source" / LOADER_SOURCE).read_bytes()
    require(hashlib.sha256(loader).hexdigest() == LOADER_SHA256, "pinned JLine loader source hash mismatch")
    adapter = json.loads((output / "jline-transformer-build.json").read_text())
    require(adapter["self_test"] == "PASS" and adapter["compiler_release"] == 17
            and adapter["shade_api_version"] == "3.6.1" and adapter["command_exit"] == 0,
            "source-time JLine transformer build/self-test did not succeed")
    require(hashlib.sha256((output / "jline-transformer-build.log").read_bytes()).hexdigest() == adapter["build_log_sha256"],
            "JLine transformer build log differs from successful receipt")
    adapter_sources = Path(__file__).resolve().parent / "jline-transformer"
    expected_sources = {str(path.relative_to(adapter_sources)): hashlib.sha256(path.read_bytes()).hexdigest()
                        for path in adapter_sources.rglob("*") if path.is_file() and path.suffix in (".xml", ".java")}
    require(adapter["source_sha256"] == expected_sources, "JLine transformer sources differ from the compiled receipt")
    require(adapter["coordinate"] == "io.github.masondev1024.d2c:hadoop-jline-resource-transformer:1.0.0"
            and adapter["artifact_sha256"] == adapter["installed_artifact_sha256"]
            and re.fullmatch(r"[a-f0-9]{64}", adapter["artifact_sha256"]) is not None,
            "JLine build-only adapter identity mismatch")
    sources = []
    for relative, expected in SOURCE_INPUTS.items():
        path = source_root / relative
        if path.name == "pom.xml":
            path = path.with_name("upstream.pom.xml")
        content = path.read_bytes()
        require(hashlib.sha256(content).hexdigest() == expected, f"source hash mismatch: {relative}")
        sources.append({"url": SOURCE_ROOT_URL + relative, "sha256": expected})
    for folder, root_url, inputs in (
        ("curator-review", CURATOR_SOURCE_ROOT_URL, CURATOR_SOURCE_INPUTS),
        ("thirdparty-review", THIRDPARTY_SOURCE_ROOT_URL, THIRDPARTY_SOURCE_INPUTS),
    ):
        for relative, expected in inputs.items():
            path = source_root / folder / relative
            if path.name == "pom.xml":
                path = path.with_name("upstream.pom.xml")
            require(hashlib.sha256(path.read_bytes()).hexdigest() == expected,
                    f"official Guava/Curator source review hash mismatch: {folder}/{relative}")
            sources.append({"url": root_url + relative, "sha256": expected, "purpose": "immutable upstream packaging/version review"})
    repository = Path("/root/.m2/repository")
    direct_guava = repository / "com/google/guava/guava" / GUAVA_VERSION / f"guava-{GUAVA_VERSION}.jar"
    failureaccess = (repository / "com/google/guava/failureaccess" / FAILUREACCESS_VERSION
                     / f"failureaccess-{FAILUREACCESS_VERSION}.jar")
    curator_build = json.loads((output / "curator-bundle-build.json").read_text())
    require(curator_build["command_exit"] == 0 and curator_build["finished_jar_edits"] is False
            and curator_build["upstream_java_source_changes"] is False
            and curator_build["custom_group"] == CURATOR_VARIANT_GROUP
            and curator_build["custom_version"] == CURATOR_VARIANT_VERSION,
            "complete compatible Curator assembly did not succeed")
    require(curator_build["fixed_input_sha256"] == {
        "guava": hashlib.sha256(direct_guava.read_bytes()).hexdigest(),
        "failureaccess": hashlib.sha256(failureaccess.read_bytes()).hexdigest()},
        "actual fixed Curator Guava/failureaccess inputs differ from the build receipt")
    require(hashlib.sha256((output / "curator-bundle-build.log").read_bytes()).hexdigest()
            == curator_build["build_log_sha256"], "compatible Curator build log differs from successful receipt")
    reference = (source_root / "curator-assemblies/curator-guava-reference/target"
                 / f"curator-guava-reference-{CURATOR_VARIANT_VERSION}.jar")
    require(hashlib.sha256(reference.read_bytes()).hexdigest() == curator_build["fixed_only_reference_sha256"],
            "fixed-only Curator Guava bytecode reference differs from successful receipt")
    curator_verification, curator_selected_modules = {}, {}
    for module in CURATOR_MODULES:
        original = repository / "org/apache/curator" / module / CURATOR_VERSION
        directory = repository / CURATOR_VARIANT_GROUP.replace(".", "/") / module / CURATOR_VARIANT_VERSION
        selected = directory / f"{module}-{CURATOR_VARIANT_VERSION}.jar"
        curator_selected_modules[module] = selected
        curator_verification[module] = verify_curator_shaded_module(selected,
            original / f"{module}-{CURATOR_VERSION}-{CURATOR_CLASSIFIER}.jar",
            original / f"{module}-{CURATOR_VERSION}.jar", module, direct_guava, failureaccess, reference,
            reviewed_metadata=CURATOR_REVIEWED_METADATA)
        receipt = curator_build["modules"][module]
        require(all(receipt.get(key) == value for key, value in curator_verification[module].items()),
                f"actual complete compatible Curator artifact differs from successful receipt: {module}")
        require(hashlib.sha256(project_pom(module, multi_release=curator_build["multi_release_implementation_inputs"]
            and module == "curator-client").encode()).hexdigest() == receipt["source_pom_sha256"],
                f"Curator assembly source POM differs from the reviewed recipe: {module}")
        require(hashlib.sha256(selected.with_suffix(".pom").read_bytes()).hexdigest() == receipt["installed_pom_sha256"],
                f"actual installed compatible Curator POM differs from successful receipt: {module}")
    shaded_guava = (repository / "org/apache/hadoop/thirdparty/hadoop-shaded-guava" / HADOOP_SHADED_GUAVA_VERSION
                    / f"hadoop-shaded-guava-{HADOOP_SHADED_GUAVA_VERSION}.jar")
    graphs, recipes, assembly_inputs = {}, {}, {}
    for module in MODULES:
        directory = source_root / "hadoop-client-modules" / module
        original = (directory / "upstream.pom.xml").read_text()
        patched = (directory / "pom.xml").read_text()
        require(patched == patch_source(original, module), f"unexpected source recipe edit: {module}")
        recipes[module] = hashlib.sha256(patched.encode()).hexdigest()
        graph_file = output / f"{module}-dependency-tree.json"
        graph = json.loads(graph_file.read_text())
        graphs[module] = validate_graph(graph, module)
        assembly_inputs[module] = {"original_pom": (directory / "upstream.pom.xml").read_bytes(),
            "exported_source_pom": (output / f"{module}-{VARIANT_VERSION}-source-pom.xml").read_bytes(),
            "installed_pom": (output / f"{module}-{VARIANT_VERSION}-installed-pom.xml").read_bytes(),
            "generated_dependency_reduced_pom": (directory / "dependency-reduced-pom.xml").read_bytes(),
            "resolved_graph": graph,
            "built_artifact_sha256": hashlib.sha256((directory / "target" / f"{module}-{VARIANT_VERSION}.jar").read_bytes()).hexdigest()}

    classes, services, artifacts, jetty_modules = set(), {}, [], set()
    yarn_references, jetty_references, transport_references, identities = {}, {}, {}, {}
    jline_verification = None
    for module in MODULES:
        path = output / f"{module}-{VARIANT_VERSION}.jar"
        artifact_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        with ZipFile(path) as jar:
            identities[module] = validate_artifact_identity(module, jar, artifact_sha256=artifact_hash,
                                                            **assembly_inputs[module])
            identities[module]["resolved_tree_json_sha256"] = hashlib.sha256(
                (output / f"{module}-dependency-tree.json").read_bytes()).hexdigest()
            if module == "hadoop-client-api":
                for original in ("hadoop-mapreduce-client-core", "hadoop-yarn-client"):
                    verify_maven_properties(required_jar_entry(jar, f"META-INF/maven/org.apache.hadoop/{original}/pom.properties"),
                                            "org.apache.hadoop", original, UPSTREAM_VERSION)
            for name in jar.namelist():
                validate_no_embedded_snappy(name)
                if not name.endswith("/") and resource_kind(name) == "java-spi":
                    require(jar.namelist().count(name) == 1, f"duplicate Java SPI descriptor: {name}")
                    services.setdefault(name.removeprefix("META-INF/services/"), set()).update(service_providers(jar.read(name)))
                if name.startswith("META-INF/maven/org.eclipse.jetty") and name.endswith("/pom.properties"):
                    jetty_modules.add(name.removeprefix("META-INF/maven/").removesuffix("/pom.properties"))
                if not name.endswith(".class"):
                    continue
                binary_name = name.removesuffix(".class")
                classes.add(binary_name)
                refs = class_references(jar.read(name))
                validate_guava_references(binary_name, refs)
                validate_no_embedded_snappy(name, refs)
                require(binary_name not in REMOVED_JOBCLIENT_CLASSES,
                        f"excluded jobclient implementation present: {name}")
                require(not binary_name.startswith((
                    "org/apache/hadoop/shaded/org/eclipse/jetty/client/",
                    "org/apache/hadoop/shaded/org/eclipse/jetty/http/",
                    "org/apache/hadoop/shaded/org/eclipse/jetty/websocket/",
                    "org/eclipse/jetty/client/", "org/eclipse/jetty/http/", "org/eclipse/jetty/websocket/",
                )), f"unexpected excluded subsystem implementation: {name}")
                if binary_name.startswith("org/apache/hadoop/") and "/shaded/" not in binary_name:
                    yarn = sorted(ref for ref in refs if ref.startswith("org/apache/hadoop/yarn/client/"))
                    jetty = sorted(ref for ref in refs if ref.startswith(("org/eclipse/jetty/", "org/apache/hadoop/shaded/org/eclipse/jetty/")))
                    transport = validate_class_references(binary_name, refs)
                    if yarn:
                        yarn_references[binary_name] = yarn
                    if transport:
                        transport_references[binary_name] = transport
                    if jetty:
                        jetty_references[binary_name] = jetty
            if module == "hadoop-client-runtime":
                validate_retained_dependency_metadata(jar)
                final_curator_inventory = validate_final_curator_inventory(jar, curator_selected_modules)
                final_curator_documents = validate_final_curator_documents(jar, curator_build["modules"])
                if curator_build["multi_release_implementation_inputs"]:
                    require(re.search(rb"(?mi)^Multi-Release:\s*true\s*$", jar.read("META-INF/MANIFEST.MF")) is not None,
                            "final Hadoop runtime manifest does not activate fixed multi-release implementations")
                with ZipFile(direct_guava) as direct, ZipFile(shaded_guava) as shaded, \
                        ZipFile(curator_selected_modules["curator-client"]) as curator, ZipFile(failureaccess) as failure:
                    guava_verification = validate_guava_implementations(jar, direct, shaded, curator, failure)
                guava_verification["resolved_direct_input_sha256"] = hashlib.sha256(direct_guava.read_bytes()).hexdigest()
                guava_verification["resolved_pre_shaded_input_sha256"] = hashlib.sha256(shaded_guava.read_bytes()).hexdigest()
                for package in ("core/json", "databind/cfg"):
                    name = f"org/apache/hadoop/shaded/com/fasterxml/jackson/{package}/PackageVersion.class"
                    require(b"2.21.7" in jar.read(name), f"incorrect actual Jackson implementation: {name}")
                for name in ("org/jline/builtins/telnet/Telnet.class", "org/jline/builtins/telnet/TelnetIO.class"):
                    jar.getinfo("org/apache/hadoop/shaded/" + name)
                for prefix in ("org/apache/hadoop/shaded/com/google/common/collect/",
                               "org/apache/hadoop/thirdparty/com/google/common/collect/"):
                    for implementation in ("MapMaker", "CompactHashMap", "CompactHashSet"):
                        jar.getinfo(prefix + implementation + ".class")
                original_path = Path(f"/root/.m2/repository/org/jline/jline/{JLINE_VERSION}/jline-{JLINE_VERSION}.jar")
                with ZipFile(original_path) as original:
                    verify_maven_properties(required_jar_entry(original, "META-INF/maven/org.jline/jline/pom.properties"),
                                            "org.jline", "jline", JLINE_VERSION)
                    jline_verification = validate_jline_resources(original, jar, classes)
                jline_verification["official_resolved_aggregate_sha256"] = hashlib.sha256(original_path.read_bytes()).hexdigest()
            require("META-INF/LICENSE.txt" in jar.namelist() and "META-INF/NOTICE.txt" in jar.namelist(),
                    f"upstream license/notice missing: {module}")
        artifacts.append({"upstream": f"org.apache.hadoop:{module}:{UPSTREAM_VERSION}",
                          "coordinate": f"org.apache.hadoop:{module}:{VARIANT_VERSION}",
                          "file": path.name, "sha256": artifact_hash})
    require(set(REQUIRED_CLASSES) <= classes, "required local filesystem/MapReduce API classes missing")
    require("org/apache/hadoop/yarn/client/util/YarnClientUtils" in yarn_references.get("org/apache/hadoop/mapred/Master", []),
            "Master's actual required utility reference changed; review upstream behavior")
    validate_services(services, classes)
    reviews = {name: {**review, "source_url": SOURCE_ROOT_URL + review["source"],
                       "source_sha256": SOURCE_INPUTS[review["source"]]}
               for name, review in SOURCE_REVIEWS.items()}
    return {
        "variant": VARIANT_VERSION, "profile": SUPPORTED_PROFILE,
        "method": "upstream source-free API/runtime shade assemblies; whole-module selection before assembly",
        "upstream_java_source_changes": False,
        "required_local_principal_utility_modules": {"hadoop-mapreduce-client-core": UPSTREAM_VERSION,
                                                      "hadoop-yarn-client": UPSTREAM_VERSION},
        "excluded_modules": list(EXCLUDED_MODULES), "unsupported_capabilities": list(UNSUPPORTED_CAPABILITIES),
        "excluded_artifacts": [f"{group}:{artifact}" for group, artifact in EXCLUDED_ARTIFACTS],
        "profile_selection_note": "WebSocket transport is an upstream compile dependency, not optional=true. Omitting that complete artifact is a D2C local capability choice; no upstream Java class or JAR metadata is edited.",
        "sources": sources, "patched_recipe_sha256": recipes, "artifacts": artifacts,
        "artifact_identity": identities,
        "jline_source_time_resource_adapter": adapter,
        "jline_typed_resource_verification": jline_verification,
        "curator_source_time_compatible_bundle_build": curator_build,
        "curator_complete_compatible_module_verification": curator_verification,
        "curator_final_complete_assembly": final_curator_inventory,
        "curator_final_preserved_upstream_documents_sha256": final_curator_documents,
        "guava_complete_fixed_input_assembly": guava_verification,
        "snappy_assembly_boundary": {"embedded_implementation_native_and_metadata_absent": True,
                                     "pre_shaded_snappy_references_absent": True,
                                     "external_artifact": "org.xerial.snappy:snappy-java",
                                     "external_version": SNAPPY_VERSION,
                                     "method": "unchanged upstream artifactSet exclusion; actual final generated pair checked without editing either JAR",
                                     "limits": "The final image must separately verify and exercise its external patched native Snappy artifact on both platforms."},
        "tools": {"maven": maven_version, "maven-shade-plugin": "3.6.1", "maven-dependency-plugin": "3.8.1",
                  "maven_version_report_sha256": hashlib.sha256(version_report).hexdigest()},
        "verified_dependency_versions": DEPENDENCY_VERSIONS, "resolved_graphs": graphs,
        "service_providers": {name: sorted(values) for name, values in sorted(services.items())},
        "yarn_client_symbolic_references": yarn_references,
        "reviewed_source_paths": reviews,
        "missing_websocket_symbolic_references_in_unsupported_shell_classes": transport_references,
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
