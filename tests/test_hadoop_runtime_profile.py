"""Recipe/validator tests; these do not build or run a Hadoop distribution."""

from __future__ import annotations

import importlib.util
import hashlib
import io
from pathlib import Path
import struct
import sys
import tempfile
import unittest
import warnings
from zipfile import ZipFile


HELPERS = Path(__file__).parents[1] / "container-build" / "lakehouse" / "hadoop"


def load_helper(name: str):
    spec = importlib.util.spec_from_file_location(name, HELPERS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sys.path.insert(0, str(HELPERS))
try:
    recipe = load_helper("patch_pom")
    verification = load_helper("verify_bundle")
    profile = load_helper("runtime_profile")
    jline = load_helper("jline_resources")
    curator = load_helper("curator_compat")
finally:
    sys.path.remove(str(HELPERS))


def source_fixture(module: str) -> str:
    api_dependency = """
    <dependency><groupId>org.apache.hadoop</groupId><artifactId>hadoop-client-api</artifactId><scope>runtime</scope></dependency>
""" if module == "hadoop-client-runtime" else ""
    return f"""<project xmlns="http://maven.apache.org/POM/4.0.0">
  <parent><groupId>org.apache.hadoop</groupId><artifactId>hadoop-project</artifactId><version>3.5.0</version></parent>
  <artifactId>{module}</artifactId><version>3.5.0</version>
  <!-- We contain no source -->
  <dependencies>
    <dependency><groupId>org.apache.hadoop</groupId><artifactId>hadoop-client</artifactId>
      <optional>true</optional><exclusions>
        <exclusion><groupId>xml-apis</groupId><artifactId>xml-apis</artifactId></exclusion>
      </exclusions>
    </dependency>{api_dependency}
  </dependencies>
  <profiles><profile><id>shade</id><build><plugins><plugin>
    <artifactId>maven-shade-plugin</artifactId><configuration>
      <filters><filter><artifact>org.apache.hadoop:*</artifact><excludes><exclude>**/*</exclude></excludes></filter></filters>
      <relocations><relocation><pattern>org/</pattern><shadedPattern>org.apache.hadoop.shaded.org.</shadedPattern></relocation></relocations>
      <transformers><transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/></transformers>
    </configuration></plugin></plugins></build></profile></profiles>
</project>"""


def dependency(artifact: str, version: str = "3.5.0", group: str = "org.apache.hadoop", children=(), classifier=None) -> dict:
    result = {"groupId": group, "artifactId": artifact, "version": version, "children": list(children)}
    if classifier is not None:
        result["classifier"] = classifier
    return result


def graph_fixture(module: str) -> dict:
    selected = [dependency(name) for name in profile.REQUIRED_MODULES]
    selected += [dependency("guava", profile.GUAVA_VERSION, "com.google.guava"),
                 dependency("hadoop-shaded-guava", profile.HADOOP_SHADED_GUAVA_VERSION, "org.apache.hadoop.thirdparty")]
    selected += [dependency(name, profile.CURATOR_VERSION, "org.apache.curator", classifier="osgi")
                 for name in profile.CURATOR_MODULES]
    selected += [dependency("snappy-java", profile.SNAPPY_VERSION, "org.xerial.snappy")]
    children = [dependency("hadoop-client", children=selected)]
    if module == "hadoop-client-runtime":
        children.append(dependency("hadoop-client-api", profile.VARIANT_VERSION))
    return dependency(module, profile.VARIANT_VERSION, children=children)


class HadoopLocalSourceRecipeTest(unittest.TestCase):
    def test_both_assemblies_select_whole_modules_and_keep_upstream_shade_recipe(self) -> None:
        for name in profile.MODULES:
            with self.subTest(module=name):
                source = source_fixture(name)
                old = recipe.ET.fromstring(source)
                result = recipe.ET.fromstring(recipe.patch_source(source, name))
                self.assertEqual(recipe.text(result, "m:version"), "3.5.0-d2c-local1")
                self.assertEqual(recipe.text(result, "m:parent/m:version"), "3.5.0")
                managed = {(recipe.text(node, "m:groupId"), recipe.text(node, "m:artifactId")): recipe.text(node, "m:version")
                           for node in result.findall("m:dependencyManagement/m:dependencies/m:dependency", recipe.NS)}
                self.assertEqual(managed[("com.google.guava", "guava")], "33.7.2-jre")
                self.assertEqual(managed[("org.apache.hadoop.thirdparty", "hadoop-shaded-guava")], "1.6.0")
                self.assertEqual(managed[("org.xerial.snappy", "snappy-java")], "1.1.10.9")
                self.assertEqual(recipe.upstream_profiles(old, "hadoop-client-api"),
                                 recipe.upstream_profiles(result, name))
                aggregator = result.find("m:dependencies/m:dependency", recipe.NS)
                self.assertEqual(recipe.text(aggregator, "m:version"), "3.5.0")
                excluded = {(recipe.text(node, "m:groupId"), recipe.text(node, "m:artifactId"))
                            for node in aggregator.findall("m:exclusions/m:exclusion", recipe.NS)}
                self.assertEqual(excluded, {("xml-apis", "xml-apis"),
                    ("org.eclipse.jetty.websocket", "websocket-client"),
                    ("org.apache.hadoop", "hadoop-mapreduce-client-jobclient"), *profile.REPLACED_CURATOR_ARTIFACTS})
                dependencies = {recipe.text(node, "m:artifactId"): node
                                for node in result.findall("m:dependencies/m:dependency", recipe.NS)}
                common = dependencies["hadoop-mapreduce-client-common"]
                self.assertEqual(recipe.text(common, "m:version"), "3.5.0")
                self.assertEqual({(recipe.text(node, "m:groupId"), recipe.text(node, "m:artifactId"))
                    for node in common.findall("m:exclusions/m:exclusion", recipe.NS)},
                    set((*profile.EXCLUDED_ARTIFACTS, *profile.REPLACED_CURATOR_ARTIFACTS)))
                for name in profile.CURATOR_MODULES:
                    selected = dependencies[name]
                    self.assertEqual(recipe.text(selected, "m:version"), "5.2.0")
                    self.assertEqual(recipe.text(selected, "m:classifier"), "osgi")
                    self.assertEqual({(recipe.text(node, "m:groupId"), recipe.text(node, "m:artifactId"))
                        for node in selected.findall("m:exclusions/m:exclusion", recipe.NS)},
                        set(profile.REPLACED_CURATOR_ARTIFACTS))
                if name == "hadoop-client-runtime":
                    api = dependencies["hadoop-client-api"]
                    self.assertEqual(recipe.text(api, "m:version"), "3.5.0-d2c-local1")
                    plugin = result.find("m:profiles/m:profile/m:build/m:plugins/m:plugin", recipe.NS)
                    extension = plugin.find("m:dependencies/m:dependency", recipe.NS)
                    self.assertEqual(tuple(recipe.text(extension, "m:" + key) for key in ("groupId", "artifactId", "version")),
                                     recipe.TRANSFORMER_GAV)
                    self.assertNotIn(recipe.TRANSFORMER_GAV[1], dependencies)
                    transformers = plugin.find("m:configuration/m:transformers", recipe.NS)
                    self.assertEqual(transformers[0].attrib["implementation"], recipe.TRANSFORMER_CLASS)
                    self.assertEqual(transformers[1].attrib["implementation"],
                                     "org.apache.maven.plugins.shade.resource.ServicesResourceTransformer")

    def test_unknown_upstream_or_sourceful_module_requires_review(self) -> None:
        source = source_fixture("hadoop-client-api")
        for changed in (source.replace("3.5.0", "3.6.0"),
                        source.replace("<!-- We contain no source -->", "")):
            with self.subTest(source=changed[:100]), self.assertRaises(ValueError):
                recipe.patch_source(changed, "hadoop-client-api")

    def test_resolved_graph_requires_fs_and_local_mapreduce_modules(self) -> None:
        graph = graph_fixture("hadoop-client-api")
        verification.validate_graph(graph, "hadoop-client-api")
        graph["children"][0]["children"] = [node for node in graph["children"][0]["children"]
                                               if node["artifactId"] != "hadoop-mapreduce-client-core"]
        with self.assertRaisesRegex(ValueError, "required local data-processing module absent"):
            verification.validate_graph(graph, "hadoop-client-api")

    def test_resolved_graph_rejects_remote_jobclient_transport_or_unpatched_jline(self) -> None:
        for node in (dependency("hadoop-mapreduce-client-jobclient"),
                     dependency("websocket-client", "9.4.58.v20250814", "org.eclipse.jetty.websocket"),
                     dependency("jetty-http", "9.4.58.v20250814", "org.eclipse.jetty"),
                     dependency("jline", "3.30.15", "org.jline"),
                     dependency("jline", "3.9.0", "org.jline")):
            with self.subTest(artifact=node["artifactId"]):
                graph = graph_fixture("hadoop-client-api")
                graph["children"][0]["children"].append(node)
                with self.assertRaises(ValueError):
                    verification.validate_graph(graph, "hadoop-client-api")

    def test_local_principal_utility_module_cannot_be_removed(self) -> None:
        graph = graph_fixture("hadoop-client-api")
        graph["children"][0]["children"] = [node for node in graph["children"][0]["children"]
                                               if node["artifactId"] != "hadoop-yarn-client"]
        with self.assertRaisesRegex(ValueError, "required local data-processing module absent: hadoop-yarn-client"):
            verification.validate_graph(graph, "hadoop-client-api")

    def test_both_direct_and_pre_shaded_guava_require_fixed_versions(self) -> None:
        old = [dependency("guava", version, "com.google.guava")
               for version in ("33.4.8-jre", "33.6.0-jre", "33.7.1-jre", "33.7.2-android")]
        old.append(dependency("hadoop-shaded-guava", "1.5.0", "org.apache.hadoop.thirdparty"))
        for node in old:
            graph = graph_fixture("hadoop-client-runtime")
            graph["children"][0]["children"].append(node)
            with self.subTest(coordinate=node), self.assertRaisesRegex(ValueError, "unpatched .*Guava library"):
                verification.validate_graph(graph, "hadoop-client-runtime")
        for artifact in ("guava", "hadoop-shaded-guava"):
            graph = graph_fixture("hadoop-client-runtime")
            graph["children"][0]["children"] = [node for node in graph["children"][0]["children"] if node["artifactId"] != artifact]
            with self.subTest(removed=artifact), self.assertRaisesRegex(ValueError, "required patched Guava dependency absent"):
                verification.validate_graph(graph, "hadoop-client-runtime")

    def test_runtime_graph_must_use_the_matching_local_api(self) -> None:
        graph = graph_fixture("hadoop-client-runtime")
        verification.validate_graph(graph, "hadoop-client-runtime")
        graph["children"][1]["version"] = "3.5.0"
        with self.assertRaisesRegex(ValueError, "mixed Hadoop versions"):
            verification.validate_graph(graph, "hadoop-client-runtime")

    def test_complete_curator_osgi_set_cannot_admit_a_standard_or_mixed_version_sibling(self):
        for name in profile.CURATOR_MODULES:
            for version, classifier in (("5.2.0", None), ("5.2.0", ""), ("5.9.0", "osgi")):
                graph = graph_fixture("hadoop-client-runtime")
                graph["children"][0]["children"].append(
                    dependency(name, version, "org.apache.curator", classifier=classifier))
                with self.subTest(name=name, classifier=classifier, version=version):
                    with self.assertRaisesRegex(ValueError, "unexpected shaded/partial Curator dependency"):
                        verification.validate_graph(graph, "hadoop-client-runtime")
            graph = graph_fixture("hadoop-client-runtime")
            graph["children"][0]["children"] = [node for node in graph["children"][0]["children"]
                                                   if node["artifactId"] != name]
            with self.subTest(missing=name), self.assertRaisesRegex(ValueError, "required complete Curator module absent"):
                verification.validate_graph(graph, "hadoop-client-runtime")

    def test_unshaded_external_native_snappy_remains_required_at_fixed_version(self):
        for old in ("1.1.10.4", "1.1.10.8"):
            graph = graph_fixture("hadoop-client-runtime")
            graph["children"][0]["children"].append(dependency("snappy-java", old, "org.xerial.snappy"))
            with self.subTest(version=old), self.assertRaisesRegex(ValueError, "unpatched external native Snappy"):
                verification.validate_graph(graph, "hadoop-client-runtime")
        graph = graph_fixture("hadoop-client-runtime")
        graph["children"][0]["children"] = [node for node in graph["children"][0]["children"] if node["artifactId"] != "snappy-java"]
        with self.assertRaisesRegex(ValueError, "patched external native Snappy dependency absent"):
            verification.validate_graph(graph, "hadoop-client-runtime")


class HadoopProfileArtifactValidatorTest(unittest.TestCase):
    def test_tool_provenance_requires_the_actual_selected_maven_version(self) -> None:
        self.assertEqual(verification.validate_maven_version(
            "\x1b[1mApache Maven 3.9.15 (publisher-build)\x1b[m\nJava version: 21.0.13\n"), "3.9.15")
        for report in ("Apache Maven 3.9.11 (old)\n", "Apache Maven 3.9.150 (different)\n",
                       "/opt/apache-maven-3.9.15/bin/mvn\n"):
            with self.subTest(report=report), self.assertRaisesRegex(ValueError, "actual Maven"):
                verification.validate_maven_version(report)

    def service_fixture(self):
        services = {profile.PROTOCOL_SERVICE: {profile.LOCAL_PROVIDER},
                    "org.apache.hadoop.fs.FileSystem": set(profile.FILESYSTEM_PROVIDERS),
                    "org.apache.hadoop.security.token.TokenRenewer": {
                        "org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier$Renewer"}}
        classes = {name.replace(".", "/") for values in services.values() for name in values}
        return services, classes

    def test_services_keep_local_and_all_filesystem_providers(self) -> None:
        services, classes = self.service_fixture()
        verification.validate_services(services, classes)
        self.assertEqual(verification.service_providers(b"# upstream comment\nlocal.Provider # comment\n\n"),
                         ["local.Provider"])

    def test_dangling_service_provider_fails_even_if_optional(self) -> None:
        services, classes = self.service_fixture()
        classes.remove("org/apache/hadoop/yarn/security/client/TimelineDelegationTokenIdentifier$Renewer")
        with self.assertRaisesRegex(ValueError, "service provider missing"):
            verification.validate_services(services, classes)

    def test_yarn_protocol_provider_cannot_silently_survive_module_selection(self) -> None:
        services, classes = self.service_fixture()
        name = "org.apache.hadoop.mapred.YarnClientProtocolProvider"
        services[profile.PROTOCOL_SERVICE].add(name)
        classes.add(name.replace(".", "/"))
        with self.assertRaisesRegex(ValueError, "dangling YARN provider"):
            verification.validate_services(services, classes)

    def test_class_reference_reader_handles_wide_constants_and_descriptors(self) -> None:
        def utf(value: str) -> bytes:
            encoded = value.encode()
            return b"\x01" + struct.pack(">H", len(encoded)) + encoded

        caller = "org/apache/hadoop/yarn/security/client/TimelineDelegationTokenIdentifier$Renewer"
        target = "org/apache/hadoop/yarn/client/api/TimelineClient"
        other = "org/apache/hadoop/yarn/client/ClientRMProxy"
        # A constant-pool fixture, not an executable or a fabricated scan input.
        content = b"\xca\xfe\xba\xbe\x00\x00\x00\x3d" + struct.pack(">H", 9)
        content += utf(caller) + b"\x07\x00\x01" + utf(target) + b"\x07\x00\x03"
        content += utf(f"(L{other};)V") + b"\x05" + b"\x00" * 8 + utf("unused")
        self.assertEqual(verification.class_references(content), {caller, target, other})
        self.assertTrue(verification.excluded_transport_reference(verification.TRANSPORT_PREFIX + "client/WebSocketClient"))
        self.assertFalse(verification.excluded_transport_reference(other))

    def test_master_retains_its_required_local_utility_instead_of_ignoring_the_reference(self) -> None:
        master = "org/apache/hadoop/mapred/Master"
        utility = "org/apache/hadoop/yarn/client/util/YarnClientUtils"
        self.assertEqual(verification.validate_class_references(master, {utility}), [])
        self.assertIn(utility, verification.REQUIRED_CLASSES)
        with self.assertRaisesRegex(ValueError, "unreviewed reference"):
            verification.validate_class_references(master, {verification.TRANSPORT_PREFIX + "client/WebSocketClient"})

    def test_transport_reference_review_is_target_and_caller_specific(self) -> None:
        caller = "org/apache/hadoop/yarn/client/api/impl/YarnClientImpl"
        reviewed = verification.TRANSPORT_PREFIX + "client/WebSocketClient"
        self.assertEqual(verification.validate_class_references(caller, {reviewed}), [reviewed])
        with self.assertRaisesRegex(ValueError, "unreviewed reference"):
            verification.validate_class_references(caller, {verification.TRANSPORT_PREFIX + "unreviewed/NewTransport"})
        with self.assertRaisesRegex(ValueError, "unreviewed reference"):
            verification.validate_class_references("org/apache/hadoop/fs/LocalFileSystem", {reviewed})

    def test_each_reviewed_path_is_bound_to_a_pinned_official_source_input(self) -> None:
        for review in profile.SOURCE_REVIEWS.values():
            with self.subTest(source=review["source"]):
                self.assertRegex(profile.SOURCE_INPUTS[review["source"]], r"^[a-f0-9]{64}$")

    def test_dependency_version_requires_exact_metadata_value(self) -> None:
        self.assertEqual(verification.properties(b"# release\nversion=2.21.7\n")["version"], "2.21.7")
        self.assertNotEqual(verification.properties(b"version=2.21.7\n")["version"], "2.21")


class HadoopAssemblyIdentityTest(unittest.TestCase):
    """Descriptor/identity fixtures only; they are not executable Hadoop JARs."""

    def retained_metadata(self):
        return {f"META-INF/maven/{coordinate}/pom.properties":
                f"groupId={coordinate.split('/')[0]}\nartifactId={coordinate.split('/')[1]}\nversion={version}\n".encode()
                for coordinate, version in profile.DEPENDENCY_VERSIONS.items()}

    def archive(self, entries):
        stream = io.BytesIO()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            with ZipFile(stream, "w") as archive:
                for name, content in entries:
                    archive.writestr(name, content)
        return stream.getvalue()

    def identity_fixture(self, module="hadoop-client-runtime", extra=()):
        original = (HELPERS / "fixtures/hadoop-client-runtime-3.5.0.upstream.pom.xml").read_bytes() \
            if module == "hadoop-client-runtime" else source_fixture(module).encode()
        generated = recipe.patch_source(original.decode(), module).encode()
        installed = f"""<project xmlns="http://maven.apache.org/POM/4.0.0">
          <modelVersion>4.0.0</modelVersion><groupId>org.apache.hadoop</groupId>
          <artifactId>{module}</artifactId><version>{profile.VARIANT_VERSION}</version>
        </project>""".encode()
        data = self.archive([*self.retained_metadata().items(), *extra])
        digest = hashlib.sha256(data).hexdigest()
        return data, {"original_pom": original, "exported_source_pom": generated,
            "installed_pom": installed, "generated_dependency_reduced_pom": installed,
            "resolved_graph": graph_fixture(module),
            "artifact_sha256": digest, "built_artifact_sha256": digest}

    def test_expected_runtime_descriptor_absence_has_a_pinned_source_and_independent_identity(self) -> None:
        data, arguments = self.identity_fixture()
        with ZipFile(io.BytesIO(data)) as archive:
            identity = verification.validate_artifact_identity("hadoop-client-runtime", archive, **arguments)
            metadata = verification.validate_retained_dependency_metadata(archive)
        contract = identity["project_descriptor_contract"]["unchanged_upstream_filter"]
        self.assertEqual(contract["upstream_pom_sha256"], profile.SOURCE_INPUTS[
            "hadoop-client-modules/hadoop-client-runtime/pom.xml"])
        self.assertEqual(contract["filter"], {"artifact": "org.apache.hadoop:*", "excludes": ["**/*", "*"]})
        self.assertEqual(identity["resolved_root_gav"], "org.apache.hadoop:hadoop-client-runtime:3.5.0-d2c-local1")
        self.assertEqual(set(metadata), set(profile.DEPENDENCY_VERSIONS))

    def test_wrong_resolved_group_or_version_is_not_an_identity(self) -> None:
        for key, changed in (("groupId", "wrong.group"), ("version", "3.5.0")):
            data, arguments = self.identity_fixture()
            arguments["resolved_graph"][key] = changed
            with self.subTest(key=key), ZipFile(io.BytesIO(data)) as archive:
                with self.assertRaisesRegex(ValueError, "incorrect graph root"):
                    verification.validate_artifact_identity("hadoop-client-runtime", archive, **arguments)

    def test_wrong_maven_target_hash_or_generated_pom_is_rejected(self) -> None:
        for changed in ("jar-hash", "source-pom", "installed-pom", "both-installed-poms-wrong-gav"):
            data, arguments = self.identity_fixture()
            if changed == "jar-hash":
                arguments["built_artifact_sha256"] = "0" * 64
            elif changed == "source-pom":
                arguments["exported_source_pom"] += b"\n"
            elif changed == "installed-pom":
                arguments["installed_pom"] = arguments["installed_pom"].replace(b"3.5.0-d2c-local1", b"3.5.0-wrong")
            else:
                wrong = arguments["installed_pom"].replace(b"3.5.0-d2c-local1", b"3.5.0-wrong")
                arguments["installed_pom"] = wrong
                arguments["generated_dependency_reduced_pom"] = wrong
            with self.subTest(changed=changed), ZipFile(io.BytesIO(data)) as archive:
                with self.assertRaises(ValueError):
                    verification.validate_artifact_identity("hadoop-client-runtime", archive, **arguments)

    def test_runtime_absence_contract_does_not_accept_an_edited_upstream_filter(self) -> None:
        data, arguments = self.identity_fixture()
        changed = arguments["original_pom"].replace(b"<exclude>**/*</exclude>", b"<exclude>**/*.class</exclude>", 1)
        with self.assertRaisesRegex(ValueError, "exact pinned upstream POM hash"):
            verification.runtime_descriptor_filter_contract(changed)

    def test_runtime_does_not_silently_ignore_an_unexpected_project_descriptor(self) -> None:
        own = "META-INF/maven/org.apache.hadoop/hadoop-client-runtime/pom.properties"
        data, arguments = self.identity_fixture(extra=[(own, b"version=3.5.0-d2c-local1\n")])
        with ZipFile(io.BytesIO(data)) as archive:
            with self.assertRaisesRegex(ValueError, "own descriptor unexpectedly present"):
                verification.validate_artifact_identity("hadoop-client-runtime", archive, **arguments)

    def test_api_project_metadata_remains_mandatory(self) -> None:
        data, arguments = self.identity_fixture("hadoop-client-api")
        with ZipFile(io.BytesIO(data)) as archive:
            with self.assertRaisesRegex(ValueError, "required unique JAR metadata"):
                verification.validate_artifact_identity("hadoop-client-api", archive, **arguments)

    def test_multiple_missing_retained_descriptors_are_all_reported(self) -> None:
        entries = self.retained_metadata()
        removed = ("META-INF/maven/com.fasterxml.jackson.core/jackson-core/pom.properties",
                   "META-INF/maven/org.jline/jline-remote-telnet/pom.properties")
        data = self.archive([(name, content) for name, content in entries.items() if name not in removed])
        with ZipFile(io.BytesIO(data)) as archive:
            with self.assertRaisesRegex(ValueError, "retained dependency Maven metadata missing") as failure:
                verification.validate_retained_dependency_metadata(archive)
        for name in removed:
            self.assertIn(name, str(failure.exception))

    def test_duplicate_or_wrong_retained_dependency_metadata_is_rejected(self) -> None:
        entries = self.retained_metadata()
        name = "META-INF/maven/org.jline/jline/pom.properties"
        for changed in ("duplicate", "wrong-version"):
            modified = list(entries.items())
            if changed == "duplicate":
                modified.append((name, entries[name]))
            else:
                modified = [(key, value.replace(b"version=3.30.17", b"version=3.9.0") if key == name else value)
                            for key, value in modified]
            with self.subTest(changed=changed), ZipFile(io.BytesIO(self.archive(modified))) as archive:
                with self.assertRaises(ValueError):
                    verification.validate_retained_dependency_metadata(archive)

    def test_old_direct_or_pre_shaded_guava_metadata_cannot_pass_with_patched_graph(self) -> None:
        entries = self.retained_metadata()
        for coordinate, old in (("com.google.guava/guava", "33.4.8-jre"),
                                ("org.apache.hadoop.thirdparty/hadoop-shaded-guava", "1.5.0")):
            path = f"META-INF/maven/{coordinate}/pom.properties"
            modified = [(name, content.replace(profile.DEPENDENCY_VERSIONS[coordinate].encode(), old.encode())
                         if name == path else content) for name, content in entries.items()]
            with self.subTest(coordinate=coordinate), ZipFile(io.BytesIO(self.archive(modified))) as archive:
                with self.assertRaisesRegex(ValueError, "incorrect Maven metadata"):
                    verification.validate_retained_dependency_metadata(archive)


class GuavaCompleteAssemblyTest(unittest.TestCase):
    """Small non-executable inventory fixtures; no synthetic scan artifacts."""

    def entries(self, prefix):
        return [(prefix + "collect/" + name + ".class", b"inventory-fixture")
                for name in ("CompactHashMap", "CompactHashSet", "MapMakerInternalMap", "MapMaker")]

    def metadata(self, group, artifact, version):
        return (f"META-INF/maven/{group}/{artifact}/pom.properties",
                f"groupId={group}\nartifactId={artifact}\nversion={version}\n".encode())

    def check(self, generated, direct=None, thirdparty=None):
        archive = HadoopAssemblyIdentityTest().archive
        direct = direct if direct is not None else self.entries("com/google/common/") + [
            self.metadata("com.google.guava", "guava", "33.7.2-jre")]
        thirdparty = thirdparty if thirdparty is not None else self.entries(verification.GUAVA_PREFIXES[1]) + [
            self.metadata("com.google.guava", "guava", "33.7.2-jre"),
            self.metadata("org.apache.hadoop.thirdparty", "hadoop-shaded-guava", "1.6.0")]
        with ZipFile(io.BytesIO(archive(generated))) as new, ZipFile(io.BytesIO(archive(direct))) as first, \
                ZipFile(io.BytesIO(archive(thirdparty))) as second:
            return verification.validate_guava_implementations(new, first, second)

    def test_complete_two_fixed_namespaces_are_required(self):
        generated = self.entries(verification.GUAVA_PREFIXES[0]) + self.entries(verification.GUAVA_PREFIXES[1])
        result = self.check(generated)
        self.assertTrue(result["complete_fixed_input_class_inventory_matches"])
        self.assertEqual(result["pre_shaded_classes"], 4)
        for changed in (generated[1:], generated + [("hidden/com/google/common/collect/CompactHashMap.class", b"old")],
                        generated + [generated[0]]):
            with self.subTest(size=len(changed)), self.assertRaises(ValueError):
                self.check(changed)

    def test_pre_shaded_artifact_version_cannot_hide_an_old_embedded_guava(self):
        generated = self.entries(verification.GUAVA_PREFIXES[0]) + self.entries(verification.GUAVA_PREFIXES[1])
        old = self.entries(verification.GUAVA_PREFIXES[1]) + [
            self.metadata("com.google.guava", "guava", "33.4.8-jre"),
            self.metadata("org.apache.hadoop.thirdparty", "hadoop-shaded-guava", "1.6.0")]
        with self.assertRaisesRegex(ValueError, "incorrect Maven metadata"):
            self.check(generated, thirdparty=old)

    def test_all_class_reference_namespaces_and_snappy_payloads_are_checked(self):
        good = verification.GUAVA_PREFIXES[0] + "collect/CompactHashMap"
        verification.validate_guava_references("fixture", {good, "[L" + good + ";"})
        with self.assertRaisesRegex(ValueError, "unexpected Guava reference"):
            verification.validate_guava_references("fixture", {"unreviewed/com/google/common/collect/CompactHashMap"})
        verification.validate_no_embedded_snappy("org/apache/hadoop/io/compress/SnappyCodec.class",
                                                {"org/xerial/snappy/Snappy"})
        for name in ("org/xerial/snappy/Snappy.class", "hidden/org/xerial/snappy/Snappy.class",
                     "META-INF/maven/org.xerial.snappy/snappy-java/pom.properties", "native/libsnappyjava.so"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "embedded Snappy"):
                verification.validate_no_embedded_snappy(name)
        with self.assertRaisesRegex(ValueError, "pre-shaded Snappy"):
            verification.validate_no_embedded_snappy("fixture.class", {"hidden/org/xerial/snappy/Snappy"})


class CuratorCompleteModuleCompatibilityTest(unittest.TestCase):
    """Class-file and metadata fixtures, not runnable Curator distributions."""

    def class_file(self, name="org/apache/curator/Fixture", descriptor="()I", constant=42, protected_nested=False):
        pool = []

        def utf(text):
            value = text.encode()
            pool.append(b"\x01" + struct.pack(">H", len(value)) + value)
            return len(pool)

        def ref(tag, index):
            pool.append(bytes([tag]) + struct.pack(">H", index))
            return len(pool)

        owner = ref(7, utf(name))
        superclass = ref(7, utf("java/lang/Object"))
        field, field_type, attribute = utf("ANSWER"), utf("I"), utf("ConstantValue")
        pool.append(b"\x03" + struct.pack(">i", constant))
        value = len(pool)
        method, signature = utf("value"), utf(descriptor)
        if protected_nested:
            inner_attr = utf("InnerClasses")
            outer = ref(7, utf(name.split("$")[0]))
            simple = utf(name.split("$")[-1])
        header = struct.pack(">IHHH", 0xCAFEBABE, 0, 52, len(pool) + 1) + b"".join(pool)
        body = struct.pack(">HHHHH", 0x0601, owner, superclass, 0, 1)
        body += struct.pack(">HHHHHIH", 0x0019, field, field_type, 1, attribute, 2, value)
        body += struct.pack(">HHHHH", 1, 0x0401, method, signature, 0)
        body += (struct.pack(">HHIHHHHH", 1, inner_attr, 10, 1, owner, outer, simple, 0x0604)
                 if protected_nested else struct.pack(">H", 0))
        return header + body

    def entries(self, module="curator-client", **class_options):
        root = f"META-INF/maven/org.apache.curator/{module}/"
        pom = (f'<project xmlns="http://maven.apache.org/POM/4.0.0"><groupId>org.apache.curator</groupId>'
               f'<artifactId>{module}</artifactId><version>5.2.0</version></project>').encode()
        name = class_options.get("name", "org/apache/curator/Fixture")
        return [(root + "pom.properties", f"groupId=org.apache.curator\nartifactId={module}\nversion=5.2.0\n".encode()),
                (root + "pom.xml", pom), ("META-INF/LICENSE", b"licensed fixture"),
                ("META-INF/NOTICE", b"notice fixture"), (name + ".class", self.class_file(**class_options))]

    def check(self, selected, baseline=None):
        baseline = selected if baseline is None else baseline
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, entries in (("osgi.jar", selected), ("standard.jar", baseline)):
                (root / name).write_bytes(HadoopAssemblyIdentityTest().archive(entries))
            return curator.verify_curator_module(root / "osgi.jar", root / "standard.jar", "curator-client")

    def test_complete_module_inventory_identity_and_public_abi_match(self):
        result = self.check(self.entries())
        self.assertEqual(result["selected_classifier"], "osgi")
        self.assertTrue(result["own_class_set_matches"])
        self.assertTrue(result["public_protected_structural_abi_matches"])
        for changed in (self.entries(descriptor="()J"), self.entries(constant=43),
                        self.entries()[:-1], self.entries() + [self.entries()[-1]]):
            with self.subTest(entries=len(changed)), self.assertRaises(ValueError):
                self.check(changed, self.entries())

    def test_protected_nested_type_api_is_not_treated_as_private(self):
        options = {"name": "org/apache/curator/Fixture$Nested", "protected_nested": True}
        original = self.entries(**options)
        result = self.check(original)
        self.assertGreaterEqual(result["public_protected_types_compared"], 1)
        with self.assertRaisesRegex(ValueError, "structural ABI changed"):
            self.check(self.entries(**options, descriptor="()J"), original)

    def test_old_hidden_guava_code_resources_and_maven_identity_changes_fail(self):
        original = self.entries()
        for extra in (("org/apache/curator/shaded/com/google/common/collect/CompactHashMap.class", b"old"),
                      ("META-INF/services/fixture.Provider", b"org.apache.curator.shaded.com.google.fixture"),
                      ("com/google/common/collect/CompactHashMap.class", b"embedded")):
            with self.subTest(extra=extra[0]), self.assertRaises(ValueError):
                self.check(original + [extra], original)
        for document in ("META-INF/LICENSE", "META-INF/NOTICE"):
            changed = [(name, content + b" changed" if name == document else content) for name, content in original]
            with self.subTest(document=document), self.assertRaisesRegex(ValueError, "document changed"):
                self.check(changed, original)

    def test_final_curator_class_inventory_cannot_drop_or_add_implementation(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "osgi.jar"
            source.write_bytes(HadoopAssemblyIdentityTest().archive(self.entries()))
            class_name = "org/apache/hadoop/shaded/org/apache/curator/Fixture.class"
            for entries, okay in (([(class_name, b"inventory")], True), ([], False),
                                   ([(class_name, b"inventory"), (class_name.replace("Fixture", "Extra"), b"bad")], False)):
                with ZipFile(io.BytesIO(HadoopAssemblyIdentityTest().archive(entries))) as jar:
                    if okay:
                        self.assertTrue(verification.validate_final_curator_inventory(jar, {"curator-client": source})[
                            "complete_selected_module_classes_match"])
                    else:
                        with self.assertRaisesRegex(ValueError, "Curator module class inventory changed"):
                            verification.validate_final_curator_inventory(jar, {"curator-client": source})

    def test_only_exact_reviewed_original_pom_can_document_an_old_shade_recipe(self):
        name = "META-INF/maven/org.apache.curator/curator-client/pom.xml"
        content = dict(self.entries())[name].replace(b"</project>",
            b"<description>org.apache.curator.shaded.com.google</description></project>")
        review = {name: hashlib.sha256(content).hexdigest()}
        self.assertTrue(curator.validate_final_curator_guava_namespace(name, content, reviewed_metadata=review))
        for path, value, known in ((name, content + b" ", review), (name, content, {}),
                                   ("META-INF/services/fixture", content, {"META-INF/services/fixture": review[name]}),
                                   ("fixture.class", content, {"fixture.class": review[name]})):
            with self.subTest(path=path), self.assertRaises(ValueError):
                curator.validate_final_curator_guava_namespace(path, value, reviewed_metadata=known)

    def test_string_constant_modified_utf8_never_aliases_literal_escape_bytes(self):
        def constant(value):
            reader = curator.ClassReader(b"")
            reader.pool = [None, (1, value), (8, 1)]
            return reader.constant(2)

        self.assertNotEqual(constant(b"\xc0\x80"), constant(b"\\xc0\\x80"))
        self.assertEqual(constant(b"same"), constant(b"same"))
        names = []
        for raw in (b"\xc0\x80", b"\\xc0\\x80"):
            reader = curator.ClassReader(b"")
            reader.pool = [None, (1, raw)]
            names.append(reader.utf8(1))
            self.assertEqual(reader.utf8(1).encode("utf-8", errors="surrogateescape"), raw)
        self.assertNotEqual(*names)


class JlineTypedResourceTest(unittest.TestCase):
    """Small descriptor fixtures, not executable library or scan artifacts."""

    def inputs(self):
        return {path: (HELPERS / "fixtures/jline-3.30.17" / path.rsplit("/", 1)[-1]).read_bytes()
                for path in jline.PROVIDERS}

    def archive(self, entries):
        return HadoopAssemblyIdentityTest().archive(entries)

    def fixture(self):
        resources = self.inputs()
        original = list(resources.items()) + [(name.replace(".", "/") + ".class", b"fixture")
                                             for name, _ in jline.PROVIDERS.values()]
        generated = [(path, jline.expected_resource(path, content)) for path, content in resources.items()]
        classes = {(jline.SHADED_PREFIX + name).replace(".", "/") for name, _ in jline.PROVIDERS.values()}
        return original, generated, classes

    def check(self, original, generated, classes):
        with ZipFile(io.BytesIO(self.archive(original))) as source, ZipFile(io.BytesIO(self.archive(generated))) as output:
            return jline.validate_jline_resources(source, output, classes)

    def test_six_exact_official_descriptors_preserve_comments_and_relocate_only_class_token(self):
        original, generated, classes = self.fixture()
        report = self.check(original, generated, classes)
        self.assertEqual(report["version"], "3.30.17")
        self.assertEqual(len(report["provider_resources"]), 6)
        self.assertEqual(report["loader_source_sha256"], jline.LOADER_SHA256)
        for path, content in self.inputs().items():
            transformed = dict(generated)[path]
            self.assertEqual(transformed.replace(jline.SHADED_PREFIX.encode(), b""), content)
            self.assertIn(b"Licensed", content)

    def test_custom_legacy_properties_are_not_java_spi_classnames(self):
        self.assertEqual(jline.resource_kind(jline.LEGACY_PREFIX + "jna"), "jline-legacy-properties")
        self.assertEqual(jline.resource_kind("META-INF/services/java.lang.Runnable"), "java-spi")
        for path in (jline.MODERN_PREFIX + "unknown", jline.LEGACY_PREFIX + "unknown",
                     "META-INF/services/not/a/binary/name"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                jline.resource_kind(path)
        with self.assertRaisesRegex(ValueError, "malformed Java SPI"):
            verification.service_providers(b"class = org.example.Provider\n")

    def test_missing_active_provider_class_fails_without_optional_skip(self):
        original, generated, classes = self.fixture()
        classes.remove((jline.SHADED_PREFIX + jline.PROVIDERS[jline.MODERN_PREFIX + "dumb"][0]).replace(".", "/"))
        with self.assertRaisesRegex(ValueError, "provider class missing"):
            self.check(original, generated, classes)

    def test_wrong_unshaded_escape_or_multiple_provider_values_fail(self):
        original, generated, classes = self.fixture()
        path = jline.MODERN_PREFIX + "dumb"
        for content in (self.inputs()[path], dict(generated)[path].replace(b"org.apache.hadoop.shaded.", b"escape."),
                        dict(generated)[path] + b"\norg.example.ExtraProvider\n"):
            with self.subTest(content=content[-100:]), self.assertRaises(ValueError):
                self.check(original, [(name, content if name == path else value) for name, value in generated], classes)
        legacy = jline.LEGACY_PREFIX + "jna"
        with self.assertRaisesRegex(ValueError, "legacy class property"):
            jline.provider_token(legacy, b"implementation = org.example.Provider\n", shaded=True)

    def test_duplicate_missing_unknown_or_modified_source_resource_fails(self):
        original, generated, classes = self.fixture()
        first = generated[0]
        for changed in (generated + [first], generated[1:], generated + [(jline.MODERN_PREFIX + "unknown", b"bad")]):
            with self.subTest(paths=[p for p, _ in changed]), self.assertRaisesRegex(ValueError, "missing, duplicate or unknown"):
                self.check(original, changed, classes)
        changed = [(name, content + b"# changed\n" if name in jline.PROVIDERS else content) for name, content in original]
        with self.assertRaisesRegex(ValueError, "differs from pinned source"):
            self.check(changed, generated, classes)

    def test_legacy_class_presence_must_match_exact_official_baseline(self):
        original, generated, classes = self.fixture()
        path = jline.LEGACY_PREFIX + "jna"
        name = jline.PROVIDERS[path][0]
        classes.remove((jline.SHADED_PREFIX + name).replace(".", "/"))
        with self.assertRaisesRegex(ValueError, "provider class missing"):
            self.check(original, generated, classes)
        original = [(p, content) for p, content in original if p != name.replace(".", "/") + ".class"]
        report = self.check(original, generated, classes)
        self.assertFalse(report["provider_resources"][path]["class_present"])
        self.assertIn("not read", report["provider_resources"][path]["loader_status"])


if __name__ == "__main__":
    unittest.main()
