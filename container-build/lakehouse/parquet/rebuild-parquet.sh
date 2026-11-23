#!/bin/sh
set -eu

# parquet-jackson is an upstream shade-only source module. Re-run that module's
# complete packaging recipe with patched Jackson inputs; do not edit JARs.
d2c_parquet_helpers=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
d2c_maven=/opt/apache-maven-3.9.15/bin/mvn
python3 "$d2c_parquet_helpers/verify-toolchain.py" --maven "$d2c_maven" --output output/maven-toolchain.json
python3 "$d2c_parquet_helpers/fetch-inputs.py" --verify-only

mkdir -p sources output
# Parquet 1.17.0 needs the annotation version-property split already in 1.17.1.
python3 "$d2c_parquet_helpers/prepare-source.py" --sources "$d2c_parquet_helpers/sources.json"

export MAVEN_OPTS='-Xmx512m'
for d2c_version_commit in \
  1.17.0:fac0c746532e133beb928a7f6a7e57b510b477a1 \
  1.17.1:78a8d3230eb4769db93de5f2f2e18363c04cae81
do
  d2c_version=${d2c_version_commit%%:*}
  d2c_commit=${d2c_version_commit#*:}
  d2c_custom_version="$d2c_version-d2c-jackson2.21.7"
  "$d2c_maven" --batch-mode --no-transfer-progress --strict-checksums \
    -f "sources/parquet-$d2c_version/parquet-jackson/pom.xml" \
    -Dmaven.repo.local=/work/m2 \
    -Dmaven.buildNumber.skip=true -DbuildNumber="$d2c_commit" \
    -Dproject.build.outputTimestamp=2026-10-22T00:00:00Z \
    -DskipTests -Dmaven.test.skip=true -Drat.skip=true package
  d2c_module="sources/parquet-$d2c_version/parquet-jackson"
  python3 "$d2c_parquet_helpers/prepare-source.py" \
    --sources "$d2c_parquet_helpers/sources.json" --verify-version "$d2c_version" \
    --report "output/parquet-jackson-$d2c_custom_version-verification.json"
  d2c_repo="output/maven/org/apache/parquet/parquet-jackson/$d2c_custom_version"
  mkdir -p "$d2c_repo"
  cp "$d2c_module/target/parquet-jackson-$d2c_custom_version.jar" "output/"
  cp "$d2c_module/target/parquet-jackson-$d2c_custom_version.jar" "$d2c_repo/"
  cp "$d2c_module/dependency-reduced-pom.xml" "$d2c_repo/parquet-jackson-$d2c_custom_version.pom"
  cp "$d2c_module/pom.xml" "output/parquet-jackson-$d2c_custom_version-source-pom.xml"
  cp "$d2c_module/target/bom.json" "output/parquet-jackson-$d2c_custom_version-cyclonedx.json"
done
sha256sum output/*.jar > output/SHA256SUMS
python3 - "$d2c_parquet_helpers/sources.json" <<'PY'
from pathlib import Path
import hashlib
import json
import sys

records = []
toolchain = json.loads(Path('output/maven-toolchain.json').read_text())
for source in json.loads(Path(sys.argv[1]).read_text()):
    version, commit = source['version'], source['commit']
    archive = Path(source['archive'])
    jar = Path(f'output/parquet-jackson-{version}-d2c-jackson2.21.7.jar')
    records.append({
        'component': 'org.apache.parquet:parquet-jackson',
        'upstream_version': version,
        'custom_version': f'{version}-d2c-jackson2.21.7',
        'source_commit': commit,
        'source_url': source['url'],
        'source_checksum_algorithm': source['algorithm'],
        'source_checksum': source['digest'],
        'source_sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
        'source_main_files_sha256': source['main_files_sha256'],
        'bundle_verification': json.loads(Path(f'output/parquet-jackson-{version}-d2c-jackson2.21.7-verification.json').read_text()),
        'jackson_core_version': '2.21.7',
        'jackson_databind_version': '2.21.7',
        'jackson_annotations_version': '2.21',
        'artifact_sha256': hashlib.sha256(jar.read_bytes()).hexdigest(),
        'build_toolchain': toolchain,
        'assembly': 'Original upstream shade-only Maven source module package lifecycle',
        'binary_edits': False,
    })
Path('output/parquet-source-provenance.json').write_text(json.dumps(records, indent=2) + '\n')
PY
