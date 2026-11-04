"""Select the local-only module graph in Hadoop's unmodified shade recipes."""

import argparse
import hashlib
from pathlib import Path
import re
import xml.etree.ElementTree as ET

from runtime_profile import EXCLUDED_ARTIFACTS, MODULES, SOURCE_INPUTS, UPSTREAM_VERSION, VARIANT_VERSION

NS = {"m": "http://maven.apache.org/POM/4.0.0"}


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
        for group, name in EXCLUDED_ARTIFACTS
    )
    dependency = dependency.replace(
        "</exclusions>", "<!-- D2C local profile: omit remote jobclient and interactive WebSocket transport artifacts. -->\n"
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
    patched = source[:matches[0].start()] + dependency + local_common + source[matches[0].end():]
    version_pattern = rf"(<artifactId>{re.escape(module)}</artifactId>\s*<version>){re.escape(UPSTREAM_VERSION)}(</version>)"
    patched, count = re.subn(version_pattern, rf"\g<1>{VARIANT_VERSION}\g<2>", patched)
    if count != 1:
        raise ValueError("cannot identify the assembly's own version")
    if module == "hadoop-client-runtime":
        token = "<artifactId>hadoop-client-api</artifactId>"
        if patched.count(token) != 1:
            raise ValueError("expected one API dependency")
        patched = patched.replace(token, token + f"\n      <version>{VARIANT_VERSION}</version>", 1)
    # Jackson's official 2.21.7 release uses annotations 2.21, not 2.21.7.
    management = """  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-annotations</artifactId>
        <version>2.21</version>
      </dependency>
    </dependencies>
  </dependencyManagement>
"""
    if patched.count("  <dependencies>") != 1:
        raise ValueError("expected one top-level dependency list")
    patched = patched.replace("  <dependencies>", management + "  <dependencies>", 1)
    result = ET.fromstring(patched)
    # Includes every original filter, relocation and service transformer.
    if ET.tostring(original.find("m:profiles", NS)) != ET.tostring(result.find("m:profiles", NS)):
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
