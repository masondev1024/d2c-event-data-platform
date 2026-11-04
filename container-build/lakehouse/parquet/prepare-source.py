"""Extract verified sources and patch only parquet-jackson build inputs."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET


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
    if any(path.is_file() for path in (module / "src/main").glob("**/*")):
        raise RuntimeError("Upstream parquet-jackson gained main sources; review required")
    pom = module / "pom.xml"
    pom.write_text(patch_module(pom.read_text(), source["version"]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", type=Path, required=True)
    parser.add_argument("--directory", type=Path, default=Path("sources"))
    args = parser.parse_args()
    for source in json.loads(args.sources.read_text()):
        prepare(source, args.directory)
