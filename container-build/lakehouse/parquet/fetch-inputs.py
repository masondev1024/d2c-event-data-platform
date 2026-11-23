"""Fetch immutable upstream build inputs and verify their pinned digests."""
from hashlib import sha256, sha512
import argparse
import json
from pathlib import Path
from urllib.request import urlopen

INPUTS = tuple((source["archive"], source["url"], source["algorithm"], source["digest"])
          for source in json.loads(Path(__file__).with_name("sources.json").read_text()))


def fetch_inputs(*, verify_only=False):
    for name, url, algorithm, expected in INPUTS:
        destination = Path(name)
        if verify_only and not destination.is_file():
            raise RuntimeError(f"Verified build input missing: {name}")
        content = destination.read_bytes() if destination.exists() else urlopen(url, timeout=120).read()
        actual = {"sha256": sha256, "sha512": sha512}[algorithm](content).hexdigest()
        if actual != expected:
            raise RuntimeError(f"{name}: expected {algorithm} {expected}, found {actual}")
        if not destination.exists():
            destination.write_bytes(content)
        print(f"verified {name}: {algorithm} {actual}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-only", action="store_true")
    fetch_inputs(verify_only=parser.parse_args().verify_only)
