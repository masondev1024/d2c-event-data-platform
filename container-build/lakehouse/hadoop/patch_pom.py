"""Select the local-only module graph in Hadoop's unmodified shade recipes."""

import argparse
import copy
import hashlib
from pathlib import Path
import re
import xml.etree.ElementTree as ET

from runtime_profile import (CURATOR_CLASSIFIER, CURATOR_MODULES, CURATOR_VERSION,
                             EXCLUDED_ARTIFACTS, GUAVA_VERSION, HADOOP_SHADED_GUAVA_VERSION,
                             MODULES, REPLACED_CURATOR_ARTIFACTS, SNAPPY_VERSION, SOURCE_INPUTS,
                             UPSTREAM_VERSION, VARIANT_VERSION)

NS = {"m": "http://maven.apache.org/POM/4.0.0"}
TRANSFORMER_CLASS = "d2c.shade.hadoop.JlineProviderResourceTransformer"
TRANSFORMER_GAV = ("io.github.masondev1024.d2c", "hadoop-jline-resource-transformer", "1.0.0")


def upstream_profiles(result: ET.Element, module: str) -> bytes:
    """Remove only our exact build-plugin extension before comparing upstream."""
    profiles = copy.deepcopy(result.find("m:profiles", NS))
    if module == "hadoop-client-runtime":
        plugins = [p for p in profiles.findall("m:profile/m:build/m:plugins/m:plugin", NS)
                   if text(p, "m:artifactId") == "maven-shade-plugin"]
        if len(plugins) != 1:
            raise ValueError("expected one upstream runtime shade plugin")
        plugin = plugins[0]
        dependencies = plugin.find("m:dependencies", NS)
        if dependencies is None or len(dependencies) != 1:
            raise ValueError("expected exactly our build-only plugin dependency")
        dependency = dependencies[0]
        if tuple(text(dependency, "m:" + key) for key in ("groupId", "artifactId", "version")) != TRANSFORMER_GAV:
            raise ValueError("incorrect build-only transformer coordinate")
        plugin.remove(dependencies)
        containers = plugin.findall(".//m:transformers", NS)
        if len(containers) != 1 or not len(containers[0]):
            raise ValueError("expected one upstream transformer list")
        first = containers[0][0]
        if first.attrib != {"implementation": TRANSFORMER_CLASS} or len(first):
            raise ValueError("build-only JLine transformer must precede existing transformers")
        containers[0].remove(first)
    # XML indentation is not semantic. Ignore only formatting, not attributes/text.
    for item in profiles.iter():
        item.tail = None
        if item.text is not None and not item.text.strip():
            item.text = None
    return ET.tostring(profiles)


def text(element: ET.Element, path: str) -> str | None:
    return element.findtext(path, namespaces=NS)


def patch_source(source: str, module: str) -> str:
    if module not in MODULES:
        raise ValueError(f"unsupported assembly module: {module}")
    original = ET.fromstring(source)
    if text(original, "m:artifactId") != module or text(original, "m:version") != UPSTREAM_VERSION:
        raise ValueError("unexpected upstream module/version")
    if text(original, "m:parent/m:version") != UPSTREAM_VERSION:
        raise ValueError("unexpected upstream parent version")
    if "<!-- We contain no source -->" not in source:
        raise ValueError("upstream module is no longer source-free")
    if original.find("m:dependencyManagement", NS) is not None:
        raise ValueError("review upstream dependency management before applying overrides")
    dependency_pattern = re.compile(
        r"<dependency>\s*<groupId>org\.apache\.hadoop</groupId>\s*"
        r"<artifactId>hadoop-client</artifactId>[\s\S]*?</dependency>"
    )
    matches = list(dependency_pattern.finditer(source))
    if len(matches) != 1:
        raise ValueError("expected one Hadoop client aggregation dependency")
    dependency = matches[0].group()
    if dependency.count("</exclusions>") != 1:
        raise ValueError("expected upstream aggregation exclusions")
    additions = "".join(
        f"        <exclusion>\n          <groupId>{group}</groupId>\n"
        f"          <artifactId>{name}</artifactId>\n        </exclusion>\n"
        for group, name in (*EXCLUDED_ARTIFACTS, *REPLACED_CURATOR_ARTIFACTS)
    )
    dependency = dependency.replace(
        "</exclusions>", "<!-- D2C local profile: omit remote transports; select complete unshaded Curator modules explicitly below. -->\n"
        + additions + "      </exclusions>", 1,
    )
    dependency = dependency.replace(
        "<artifactId>hadoop-client</artifactId>",
        f"<artifactId>hadoop-client</artifactId>\n      <version>{UPSTREAM_VERSION}</version>", 1,
    )
    # In the upstream aggregation, jobclient is the path to common. Select
    # common explicitly so its LocalClientProtocolProvider/LocalJobRunner survive
    # without jobclient's YARN provider. Keep YarnClientUtils and apply the same
    # transport/jobclient exclusions to this new path.
    local_common = """
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-mapreduce-client-common</artifactId>
      <version>3.5.0</version>
      <exclusions>
""" + additions + """      </exclusions>
    </dependency>"""
    # Classifier is part of Maven's identity. Explicitly retain each full osgi
    # module, excluding the standard shaded siblings on EVERY transitive path.
    # dependencyManagement alone cannot replace an unclassified dependency.
    curator_exclusions = "".join(
        f"        <exclusion><groupId>{group}</groupId><artifactId>{name}</artifactId></exclusion>\n"
        for group, name in REPLACED_CURATOR_ARTIFACTS)
    curator_dependencies = "".join(f"""
    <dependency>
      <groupId>org.apache.curator</groupId>
      <artifactId>{module}</artifactId>
      <version>{CURATOR_VERSION}</version>
      <classifier>{CURATOR_CLASSIFIER}</classifier>
      <exclusions>
{curator_exclusions}      </exclusions>
    </dependency>""" for module in CURATOR_MODULES)
    patched = (source[:matches[0].start()] + dependency + local_common
               + curator_dependencies + source[matches[0].end():])
    version_pattern = rf"(<artifactId>{re.escape(module)}</artifactId>\s*<version>){re.escape(UPSTREAM_VERSION)}(</version>)"
    patched, count = re.subn(version_pattern, rf"\g<1>{VARIANT_VERSION}\g<2>", patched)
    if count != 1:
        raise ValueError("cannot identify the assembly's own version")
    if module == "hadoop-client-runtime":
        token = "<artifactId>hadoop-client-api</artifactId>"
        if patched.count(token) != 1:
            raise ValueError("expected one API dependency")
        patched = patched.replace(token, token + f"\n      <version>{VARIANT_VERSION}</version>", 1)
        shade = "<artifactId>maven-shade-plugin</artifactId>"
        if patched.count(shade) != 1 or patched.count("<transformers>") != 1:
            raise ValueError("review the runtime shade extension points")
        group, artifact, version = TRANSFORMER_GAV
        extension = f"""
            <dependencies>
              <dependency><groupId>{group}</groupId><artifactId>{artifact}</artifactId><version>{version}</version></dependency>
            </dependencies>"""
        patched = patched.replace(shade, shade + extension, 1)
        patched = patched.replace("<transformers>", "<transformers>\n"
            + f'                    <transformer implementation="{TRANSFORMER_CLASS}"/>', 1)
    # Jackson's official 2.21.7 release uses annotations 2.21, not 2.21.7.
    management = f"""  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-annotations</artifactId>
        <version>2.21</version>
      </dependency>
      <dependency>
        <groupId>com.google.guava</groupId>
        <artifactId>guava</artifactId>
        <version>{GUAVA_VERSION}</version>
      </dependency>
      <dependency>
        <groupId>org.apache.hadoop.thirdparty</groupId>
        <artifactId>hadoop-shaded-guava</artifactId>
        <version>{HADOOP_SHADED_GUAVA_VERSION}</version>
      </dependency>
      <dependency>
        <groupId>org.xerial.snappy</groupId>
        <artifactId>snappy-java</artifactId>
        <version>{SNAPPY_VERSION}</version>
      </dependency>
    </dependencies>
  </dependencyManagement>
"""
    if len(re.findall(r"(?m)^  <dependencies>", patched)) != 1:
        raise ValueError("expected one top-level dependency list")
    patched = re.sub(r"(?m)^  <dependencies>", management + "  <dependencies>", patched, count=1)
    result = ET.fromstring(patched)
    # Includes every original filter, relocation and service transformer.
    if upstream_profiles(original, "hadoop-client-api") != upstream_profiles(result, module):
        raise ValueError("upstream shade profiles changed")
    return patched


def prepare(source_root: Path) -> None:
    for module in MODULES:
        relative = f"hadoop-client-modules/{module}/pom.xml"
        upstream = (source_root / relative).with_name("upstream.pom.xml")
        content = upstream.read_bytes()
        if hashlib.sha256(content).hexdigest() != SOURCE_INPUTS[relative]:
            raise ValueError(f"upstream source hash mismatch: {module}")
        (upstream.parent / "pom.xml").write_text(patch_source(content.decode(), module), encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_root", type=Path)
    prepare(parser.parse_args().source_root)
