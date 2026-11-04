#!/bin/sh
set -eu

# Build-stage-only entry point: Python 3, JDK 21 and Maven 3.9.15 are required.
# Java outputs are platform-neutral. Mount /root/.m2 as a locked Maven cache.
helper_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
source_root=/work/hadoop
output_dir=/out/hadoop
maven=${MAVEN_BIN:-/opt/apache-maven-3.9.15/bin/mvn}
variant=3.5.0-d2c-local1
mkdir -p "$source_root" "$output_dir"
"$maven" --version > "$output_dir/maven-version.txt"
python3 "$helper_dir/verify_bundle.py" --check-maven "$output_dir/maven-version.txt"
python3 "$helper_dir/fetch-inputs.py" "$source_root"
python3 "$helper_dir/patch_pom.py" "$source_root"
export MAVEN_OPTS=-Xmx768m

run_maven() {
    "$maven" --batch-mode --no-transfer-progress \
        -Dhadoop.version=3.5.0 \
        -Djackson2.version=2.21.7 -Djackson2.databind.version=2.21.7 \
        -Djline.version=3.30.15 \
        -Dmaven-shade-plugin.version=3.6.1 \
        -Dproject.build.outputTimestamp=2026-11-04T00:00:00Z \
        -DskipTests -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true "$@"
}

# Install the selected API variant first so runtime resolves exactly that API.
# Neither command modifies an upstream JAR; Maven assembles both from modules.
for module in hadoop-client-api hadoop-client-runtime; do
    cd "$source_root/hadoop-client-modules/$module"
    run_maven install
    run_maven org.apache.maven.plugins:maven-dependency-plugin:3.8.1:tree \
        -DoutputType=json -DoutputFile="$output_dir/$module-dependency-tree.json"
    cp "target/$module-$variant.jar" "$output_dir/"
done
python3 "$helper_dir/verify_bundle.py" "$source_root" "$output_dir"
# provenance records static checks only. The final image must separately execute
# VerifyHadoopLocalProfile.java, D2C data/checkpoint regressions and fresh scans.
