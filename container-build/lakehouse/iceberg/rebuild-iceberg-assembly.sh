#!/bin/sh
set -eu

d2c_iceberg_helpers=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
python3 "$d2c_iceberg_helpers/fetch-inputs.py"
mkdir --parents /upstream /assembly/gradle/wrapper /artifacts
tar --extract --gzip --file /verified-inputs/iceberg-source.tar.gz --strip-components=1 --directory /upstream
cp "$d2c_iceberg_helpers/assembly/build.gradle" "$d2c_iceberg_helpers/assembly/settings.gradle" /assembly/
cp /upstream/gradlew /assembly/
cp /upstream/gradle/wrapper/gradle-wrapper.properties /assembly/gradle/wrapper/
cp /verified-inputs/gradle-wrapper.jar /assembly/gradle/wrapper/
cp /upstream/spark/v4.1/spark-runtime/LICENSE /upstream/spark/v4.1/spark-runtime/NOTICE /assembly/
python3 "$d2c_iceberg_helpers/verify-assembly-recipe.py"
cd /assembly

./gradlew --no-daemon --console plain --max-workers=1 \
  -Dorg.gradle.jvmargs='-Xmx768m -XX:MaxMetaspaceSize=256m' \
  shadowJar recordRuntimeDependencies

d2c_iceberg_jar=/assembly/build/libs/iceberg-spark-runtime-4.1_2.13-1.12.0-d2c-security1.jar
python3 "$d2c_iceberg_helpers/verify_iceberg_runtime.py" \
  --original /verified-inputs/iceberg-runtime-original.jar \
  --rebuilt "$d2c_iceberg_jar" \
  --report /artifacts/iceberg-rebuild-verification.json
cp "$d2c_iceberg_jar" /artifacts/
cp build/runtime-dependencies.txt build/security-runtime-artifacts.json /artifacts/
