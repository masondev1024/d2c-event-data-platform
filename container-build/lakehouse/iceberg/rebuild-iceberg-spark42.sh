#!/bin/sh
set -eu

# Spark 4.2 thin artifacts are not published for Iceberg 1.12.0. Build the
# original source modules instead of substituting an incompatible 4.1 binary.
d2c_iceberg_helpers=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
python3 "$d2c_iceberg_helpers/fetch-inputs.py" --source-only
mkdir --parents /upstream /artifacts
tar --extract --gzip --file /verified-inputs/iceberg-source.tar.gz --strip-components=1 --directory /upstream
cp /verified-inputs/gradle-wrapper.jar /upstream/gradle/wrapper/
python3 "$d2c_iceberg_helpers/patch-spark42-source.py" /upstream
cd /upstream

./gradlew --no-daemon --console plain --max-workers=1 \
  -Dorg.gradle.jvmargs='-Xmx768m -XX:MaxMetaspaceSize=256m' \
  -Dorg.gradle.parallel=false -DflinkVersions= -DkafkaVersions= \
  -DsparkVersions=4.2 -DscalaVersion=2.13 \
  --init-script "$d2c_iceberg_helpers/spark42-source.init.gradle" \
  :iceberg-spark:iceberg-spark-runtime-4.2_2.13:shadowJar \
  :iceberg-spark:iceberg-spark-runtime-4.2_2.13:d2cRecordRuntimeInputs

d2c_iceberg_jar=/upstream/spark/v4.2/spark-runtime/build/libs/iceberg-spark-runtime-4.2_2.13-1.12.0-d2c-security1.jar
python3 "$d2c_iceberg_helpers/verify-source-runtime.py" \
  --upstream /upstream --inputs /artifacts/security-runtime-artifacts.json \
  --rebuilt "$d2c_iceberg_jar" --artifacts /artifacts
cp "$d2c_iceberg_jar" /artifacts/
