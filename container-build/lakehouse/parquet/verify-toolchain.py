"""Check the shared builder's actual Maven executable and record its identity."""
import argparse
import json
from pathlib import Path
import re
import subprocess


TOOLCHAIN = json.loads(Path(__file__).with_name("toolchain.json").read_text())


def verify_toolchain(executable):
    if str(executable) != TOOLCHAIN["executable"]:
        raise RuntimeError("Parquet must use the pinned shared Maven executable")
    version = subprocess.run(
        [str(executable), "--batch-mode", "-Dstyle.color=never", "--version"],
        check=True, capture_output=True, text=True,
    )
    stdout = re.sub(r"\x1b\[[0-9;]*m", "", version.stdout)
    match = re.search(r"^Apache Maven (\S+)", stdout, re.MULTILINE)
    if match is None or match.group(1) != TOOLCHAIN["version"]:
        raise RuntimeError(f"Expected actual Maven {TOOLCHAIN['version']} executable")
    return {
        "expected_shared_stage_tool": TOOLCHAIN,
        "archive_checksum_verification": "The shared source-build-base verifies the archive; this helper independently checks the installed executable version",
        "actual_maven_version": match.group(1),
        "maven_version_output": stdout.strip(),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--maven", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    record = verify_toolchain(args.maven)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(record, indent=2) + "\n")
