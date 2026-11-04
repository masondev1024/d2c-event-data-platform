"""Fetch only the small, hash-pinned upstream assembly inputs in a build stage."""

import argparse
import hashlib
from pathlib import Path
from urllib.request import Request, urlopen

from runtime_profile import SOURCE_INPUTS, SOURCE_ROOT_URL
from jline_resources import LOADER_SHA256, LOADER_SOURCE, SOURCE_ROOT as JLINE_SOURCE_ROOT


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_root", type=Path)
    args = parser.parse_args()
    for relative, expected in SOURCE_INPUTS.items():
        request = Request(SOURCE_ROOT_URL + relative, headers={"User-Agent": "D2C-source-assembly"})
        with urlopen(request, timeout=120) as response:
            content = response.read(1_000_001)
        if len(content) > 1_000_000:
            raise ValueError(f"unexpectedly large source input: {relative}")
        if hashlib.sha256(content).hexdigest() != expected:
            raise ValueError(f"upstream source hash mismatch: {relative}")
        target = args.source_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.name == "pom.xml":
            target = target.with_name("upstream.pom.xml")
        target.write_bytes(content)
    request = Request(JLINE_SOURCE_ROOT + LOADER_SOURCE, headers={"User-Agent": "D2C-source-assembly"})
    with urlopen(request, timeout=120) as response:
        content = response.read(1_000_001)
    if len(content) > 1_000_000 or hashlib.sha256(content).hexdigest() != LOADER_SHA256:
        raise ValueError("upstream JLine loader source hash mismatch")
    target = args.source_root / "jline-source" / LOADER_SOURCE
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)


if __name__ == "__main__":
    main()
