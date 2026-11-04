"""Patch only pinned build dependencies; leave Iceberg implementation sources intact."""
import argparse
from pathlib import Path


REPLACEMENTS = {
    'jackson-annotations = "2.22"': 'jackson-annotations = "2.21"',
    'jackson-bom = "2.22.2"': 'jackson-bom = "2.21.7"',
    'jackson221 = { strictly = "2.21.4"}': 'jackson221 = { strictly = "2.21.7"}',
    'lz4Java = "1.11.3"': 'lz4Java = "1.11.4"',
    'spark42 = "4.2.0"': 'spark42 = "4.2.1"',
}


def patch_source(root):
    catalog = root / "gradle/libs.versions.toml"
    text = catalog.read_text()
    for old, new in REPLACEMENTS.items():
        if text.count(old) != 1:
            raise RuntimeError(f"Expected one pinned upstream declaration: {old}")
        text = text.replace(old, new)
    catalog.write_text(text)
    (root / "version.txt").write_text("1.12.0\n")
    (root / "iceberg-build.properties").write_text(
        "git.build.version=1.12.0-d2c-security1\n"
        "git.closest.tag.name=apache-iceberg-1.12.0\n"
        "git.commit.id=212aa894e919ff9fdf89160da4aadcbe6858ac01\n"
        "build.security.patch=spark4.2.1-jackson2.21.7-parquet-jackson2.21.7\n"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    patch_source(parser.parse_args().root)
