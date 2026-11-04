"""Verify real Jackson code, preserved application classes, and license parity."""
import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from zipfile import ZipFile


def digest(content):
    return hashlib.sha256(content).hexdigest()


def required_class(name):
    return name.endswith(".class") and (
        (name.startswith("org/apache/iceberg/")
         and not name.startswith("org/apache/iceberg/shaded/"))
        or name.startswith("org/apache/spark/")
    )


def license_entry(name):
    base = name.rsplit("/", 1)[-1].upper()
    return not name.endswith(".class") and base.startswith(("LICENSE", "NOTICE"))


parser = argparse.ArgumentParser()
parser.add_argument("--original", type=Path, required=True)
parser.add_argument("--rebuilt", type=Path, required=True)
parser.add_argument("--report", type=Path)
parser.add_argument("--java", default="java")
args = parser.parse_args()
if not args.original.is_file() or not args.rebuilt.is_file():
    parser.error("both completed original and rebuilt JAR files must exist")

with ZipFile(args.original) as original, ZipFile(args.rebuilt) as rebuilt:
    original_names, rebuilt_names = set(original.namelist()), set(rebuilt.namelist())
    before = {name for name in original_names if required_class(name)}
    after = {name for name in rebuilt_names if required_class(name)}
    missing, added = sorted(before - after), sorted(after - before)
    changed = sorted(name for name in before & after
                     if original.read(name) != rebuilt.read(name))
    jackson_versions = sorted(name for name in rebuilt_names if
        name.endswith('/com/fasterxml/jackson/core/json/PackageVersion.class')
        or name.endswith('/com/fasterxml/jackson/databind/cfg/PackageVersion.class'))
    expected_jackson_versions = sorted(
        prefix + '/' + suffix
        for prefix in (
            'org/apache/iceberg/shaded/com/fasterxml/jackson',
            'org/apache/iceberg/shaded/org/apache/parquet/shaded/com/fasterxml/jackson',
        )
        for suffix in ('core/json/PackageVersion.class', 'databind/cfg/PackageVersion.class')
    )
    complete_jackson_inventory = jackson_versions == expected_jackson_versions
    license_names = sorted({n for n in original_names | rebuilt_names if license_entry(n)})
    licenses = []
    for name in license_names:
        old = original.read(name) if name in original_names else None
        new = rebuilt.read(name) if name in rebuilt_names else None
        licenses.append({
            "path": name,
            "status": ("missing" if new is None else "added" if old is None
                       else "identical" if old == new else "changed"),
            "original_sha256": digest(old) if old is not None else None,
            "rebuilt_sha256": digest(new) if new is not None else None,
        })

java_source = Path(__file__).with_name("VerifyIcebergShadedJackson.java")
runtime = subprocess.run(
    [args.java, "--class-path", str(args.rebuilt.resolve()), str(java_source)],
    capture_output=True, text=True, timeout=120,
)
checks = [json.loads(line) for line in runtime.stdout.splitlines() if line.startswith("{")]
license_roots = [item for item in licenses
                 if item["path"] in ("LICENSE", "NOTICE", "META-INF/LICENSE", "META-INF/NOTICE")
                 and item["original_sha256"] is not None]
root_license_parity = bool(license_roots) and all(
    item["status"] == "identical" for item in license_roots)
passed = (not missing and not added and not changed
          and runtime.returncode == 0 and len(checks) == 2 and root_license_parity
          and complete_jackson_inventory
          and all(item['status'] == 'identical' for item in licenses))
report = {
    "status": "PASS" if passed else "FAIL",
    "original": str(args.original), "original_sha256": digest(args.original.read_bytes()),
    "rebuilt": str(args.rebuilt), "rebuilt_sha256": digest(args.rebuilt.read_bytes()),
    "classes": {
        "scope": "all nonshaded org/apache/iceberg/ and all org/apache/spark/ .class entries",
        "original_count": len(before), "rebuilt_count": len(after),
        "missing": missing, "added": added,
        "preserved_count": len(before & after),
        "byte_identical_count": len(before & after) - len(changed),
        "byte_changed": changed,
    },
    "root_license_notice_byte_parity": root_license_parity,
    "complete_jackson_package_version_inventory": complete_jackson_inventory,
    "jackson_package_version_classes": jackson_versions,
    "license_notice_entries": licenses,
    "runtime": {"exit_code": runtime.returncode, "checks": checks,
                "stdout": runtime.stdout, "stderr": runtime.stderr},
}
encoded = json.dumps(report, indent=2) + "\n"
if args.report:
    args.report.write_text(encoded)
print(encoded, end="")
sys.exit(0 if passed else 1)
