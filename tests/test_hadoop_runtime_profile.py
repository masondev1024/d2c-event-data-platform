"""Recipe/validator tests; these do not build or run a Hadoop distribution."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import struct
import sys
import unittest


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


def dependency(artifact: str, version: str = "3.5.0", group: str = "org.apache.hadoop", children=()) -> dict:
    return {"groupId": group, "artifactId": artifact, "version": version, "children": list(children)}


def graph_fixture(module: str) -> dict:
    children = [dependency("hadoop-client", children=[dependency(name) for name in profile.REQUIRED_MODULES])]
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
                self.assertEqual(recipe.ET.tostring(old.find("m:profiles", recipe.NS)),
                                 recipe.ET.tostring(result.find("m:profiles", recipe.NS)))
                aggregator = result.find("m:dependencies/m:dependency", recipe.NS)
                self.assertEqual(recipe.text(aggregator, "m:version"), "3.5.0")
                excluded = {(recipe.text(node, "m:groupId"), recipe.text(node, "m:artifactId"))
                            for node in aggregator.findall("m:exclusions/m:exclusion", recipe.NS)}
                self.assertEqual(excluded, {("xml-apis", "xml-apis"),
                    ("org.apache.hadoop", "hadoop-yarn-client"),
                    ("org.apache.hadoop", "hadoop-mapreduce-client-jobclient")})
                dependencies = {recipe.text(node, "m:artifactId"): node
                                for node in result.findall("m:dependencies/m:dependency", recipe.NS)}
                common = dependencies["hadoop-mapreduce-client-common"]
                self.assertEqual(recipe.text(common, "m:version"), "3.5.0")
                self.assertEqual({recipe.text(node, "m:artifactId") for node in common.findall(
                    "m:exclusions/m:exclusion", recipe.NS)}, set(profile.EXCLUDED_MODULES))
                if name == "hadoop-client-runtime":
                    api = dependencies["hadoop-client-api"]
                    self.assertEqual(recipe.text(api, "m:version"), "3.5.0-d2c-local1")

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

    def test_resolved_graph_rejects_transitive_yarn_or_http_reintroduction(self) -> None:
        for node in (dependency("hadoop-yarn-client"), dependency("hadoop-mapreduce-client-jobclient"),
                     dependency("jetty-http", "9.4.58.v20250814", "org.eclipse.jetty")):
            with self.subTest(artifact=node["artifactId"]):
                graph = graph_fixture("hadoop-client-api")
                graph["children"][0]["children"].append(node)
                with self.assertRaises(ValueError):
                    verification.validate_graph(graph, "hadoop-client-api")

    def test_runtime_graph_must_use_the_matching_local_api(self) -> None:
        graph = graph_fixture("hadoop-client-runtime")
        verification.validate_graph(graph, "hadoop-client-runtime")
        graph["children"][1]["version"] = "3.5.0"
        with self.assertRaisesRegex(ValueError, "mixed Hadoop versions"):
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
        self.assertTrue(verification.excluded_client_reference(target))
        self.assertFalse(verification.excluded_client_reference(other))

    def test_dependency_version_requires_exact_metadata_value(self) -> None:
        self.assertEqual(verification.properties(b"# release\nversion=2.21.7\n")["version"], "2.21.7")
        self.assertNotEqual(verification.properties(b"version=2.21.7\n")["version"], "2.21")


if __name__ == "__main__":
    unittest.main()
