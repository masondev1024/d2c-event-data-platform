# syntax=docker/dockerfile:1.7
ARG D2C_ARTIFACT_STAGE=compiled-artifacts
ARG D2C_PARQUET_STAGE=parquet-artifacts

FROM apache/spark:4.2.1@sha256:0cb70cf0193ec81c93c6c47078208512bbe659dd9dc89535dd2773826f0ea2bf AS spark-base

FROM --platform=$BUILDPLATFORM apache/spark:4.2.1@sha256:0cb70cf0193ec81c93c6c47078208512bbe659dd9dc89535dd2773826f0ea2bf AS source-build-base
USER root
RUN java -version \
    && grep --fixed-strings --line-regexp 'JAVA_VERSION="21.0.13"' "$JAVA_HOME/release" \
    && mkdir --parents /opt/build-tools \
    && curl --fail --silent --show-error --location \
        https://archive.apache.org/dist/maven/maven-3/3.9.15/binaries/apache-maven-3.9.15-bin.tar.gz \
        --output /opt/build-tools/apache-maven-3.9.15-bin.tar.gz \
    && echo '33d81e0ec785f0207e3e5e3ffb61863e1dca5784c15ac3fb5ff105f69cffbea484eb8d473ea60467a63f7b0570eef8622f2fed8eee96acbe668aa313391cddb3  /opt/build-tools/apache-maven-3.9.15-bin.tar.gz' | sha512sum --check \
    && tar --extract --gzip --file /opt/build-tools/apache-maven-3.9.15-bin.tar.gz --directory /opt
COPY lakehouse/Dockerfile /recipe-source/lakehouse/Dockerfile
COPY container-build/lakehouse/ /recipe-source/container-build/lakehouse/
COPY scripts/lakehouse_artifacts.py scripts/container_security_guard.py /recipe-source/scripts/

# These upstream modules assemble shaded dependencies. Rebuild their original
# recipes with patched implementations rather than editing existing fat JARs.
# Java artifacts are platform-neutral; execute their build on the host platform.
FROM source-build-base AS hadoop-runtime-builder
COPY container-build/lakehouse/hadoop/ /build/hadoop/
RUN --mount=type=cache,id=d2c-hadoop-rebuild-m2-20261022,target=/root/.m2,sharing=locked \
    python3 /recipe-source/scripts/lakehouse_artifacts.py component \
        --component hadoop --directory /out/hadoop --source-root /recipe-source \
        -- sh /build/hadoop/rebuild.sh

FROM scratch AS hadoop-artifacts
COPY --from=hadoop-runtime-builder /out/hadoop/ /hadoop/

FROM source-build-base AS parquet-runtime-builder
WORKDIR /work
COPY container-build/lakehouse/parquet/ /build/parquet/
RUN --mount=type=cache,id=d2c-parquet-security-m2-20261022,target=/work/m2,sharing=locked \
    python3 /recipe-source/scripts/lakehouse_artifacts.py component \
        --component parquet --directory /work/output --source-root /recipe-source \
        -- sh -eu -c 'python3 /build/parquet/fetch-inputs.py && sh /build/parquet/rebuild-parquet.sh'

FROM scratch AS parquet-artifacts
COPY --from=parquet-runtime-builder /work/output/ /parquet/

# A verified named build context can supply this one prerequisite when exporting
# Iceberg separately; the default graph compiles the Parquet source recipe.
FROM ${D2C_PARQUET_STAGE} AS selected-parquet

FROM source-build-base AS iceberg-runtime-builder
COPY container-build/lakehouse/iceberg/ /build/iceberg/
COPY --from=selected-parquet /parquet/maven/ /rebuilt-repository/
RUN --mount=type=cache,id=d2c-iceberg-security-gradle,target=/root/.gradle,sharing=locked \
    python3 /recipe-source/scripts/lakehouse_artifacts.py component \
        --component iceberg --directory /artifacts --source-root /recipe-source \
        -- sh /build/iceberg/rebuild-iceberg-spark42.sh

FROM scratch AS iceberg-artifacts
COPY --from=iceberg-runtime-builder /artifacts/ /iceberg/

# Match the official core's Java 17 compiler, including its synthetic protobuf
# enum-switch classes. This complete vendor JDK is confined to Spark's builder.
FROM --platform=$BUILDPLATFORM eclipse-temurin:17.0.21_9-jdk-jammy@sha256:057a0daec60e87c96571bf5c9c7792065bcb19e804d584cd37d4144038a35717 AS spark-build-jdk

FROM source-build-base AS spark-runtime-builder
COPY --from=spark-build-jdk /opt/java/openjdk/ /opt/spark-build-jdk/
ENV JAVA_HOME=/opt/spark-build-jdk \
    JAVA_VERSION=jdk-17.0.21+9 \
    PATH=/opt/spark-build-jdk/bin:$PATH
RUN java -version \
    && javac -version \
    && grep --fixed-strings --line-regexp 'JAVA_VERSION="17.0.21"' "$JAVA_HOME/release"
COPY container-build/lakehouse/spark/ /build/spark/
RUN --mount=type=cache,id=d2c-security-maven-20261022,target=/root/.m2,sharing=locked \
    python3 /recipe-source/scripts/lakehouse_artifacts.py component \
        --component spark --directory /out/spark --source-root /recipe-source \
        -- sh /build/spark/rebuild.sh \
        --maven-home /opt/apache-maven-3.9.15 \
        --maven-archive /opt/build-tools/apache-maven-3.9.15-bin.tar.gz

FROM scratch AS spark-artifacts
COPY --from=spark-runtime-builder /out/spark/ /spark/

FROM source-build-base AS dependency-resolver

USER root

ENV HOME=/tmp \
    IVY_HOME=/tmp/ivy \
    SPARK_LOCAL_IP=127.0.0.1 \
    D2C_EXTRA_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1,org.apache.kafka:kafka-clients:3.9.2,com.google.guava:guava:33.7.2-jre,com.google.guava:failureaccess:1.0.3,org.xerial.snappy:snappy-java:1.1.10.9,at.yawk.lz4:lz4-java:1.11.4,io.netty:netty-all:4.2.17.Final,com.fasterxml.jackson.core:jackson-core:2.21.7,com.fasterxml.jackson.core:jackson-databind:2.21.7,com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.21.7,com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.21.7,com.fasterxml.jackson.module:jackson-module-scala_2.13:2.21.7,org.apache.httpcomponents.core5:httpcore5:5.4.3,org.apache.httpcomponents.core5:httpcore5-h2:5.4.3"

RUN python3 /recipe-source/scripts/lakehouse_artifacts.py component \
    --component dependencies --directory /opt/spark/extra-jars --source-root /recipe-source \
    -- sh -eu <<'SH'
mkdir --parents /opt/spark/extra-jars "$IVY_HOME"
/opt/spark/bin/spark-submit \
        --master local[1] \
        --driver-memory 512m \
        --conf "spark.jars.ivy=$IVY_HOME" \
        --conf "spark.ui.enabled=false" \
        --packages "$D2C_EXTRA_PACKAGES" \
        /opt/spark/examples/src/main/python/pi.py 1
python3 - <<'PY'
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import shutil
root = Path('/opt/spark/extra-jars')
excluded = ('org.apache.hadoop_hadoop-client-api-*.jar',
            'org.apache.hadoop_hadoop-client-runtime-*.jar',
            'org.apache.spark_spark-core_*.jar',
            'org.apache.spark_spark-network-common_*.jar')
for source in sorted((Path(os.environ['IVY_HOME']) / 'jars').rglob('*.jar')):
    if not any(fnmatch.fnmatch(source.name, pattern) for pattern in excluded):
        shutil.copyfile(source, root / source.name)
jars = sorted(root.glob('*.jar'))
assert jars, 'Ivy produced no runtime JARs'
record = {
    'schema_version': 1,
    'spark_version': '4.2.1',
    'method': 'Official Spark 4.2.1 Ivy resolver and local[1] Pi completed with exit zero',
    'requested_packages': os.environ['D2C_EXTRA_PACKAGES'].split(','),
    'artifact_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in jars},
    'excluded_replaced_artifacts': ['hadoop-client-api', 'hadoop-client-runtime', 'spark-core_2.13', 'spark-network-common_2.13'],
    'limits': 'Build-stage resolution/startup only; final runtime regression and scans are separate',
}
(root / 'dependency-source-provenance.json').write_text(json.dumps(record, indent=2, sort_keys=True) + '\n')
PY
SH

FROM scratch AS dependency-artifacts
COPY --from=dependency-resolver /opt/spark/extra-jars/ /dependencies/

# Default builds compile every source component. The sealer binds all exported
# bytes to the current recipes before a scratch export or runtime import.
FROM source-build-base AS artifact-sealer
COPY --from=hadoop-artifacts /hadoop/ /artifacts/hadoop/
COPY --from=parquet-artifacts /parquet/ /artifacts/parquet/
COPY --from=iceberg-artifacts /iceberg/ /artifacts/iceberg/
COPY --from=spark-artifacts /spark/ /artifacts/spark/
COPY --from=dependency-artifacts /dependencies/ /artifacts/dependencies/
RUN python3 /recipe-source/scripts/lakehouse_artifacts.py seal \
    --artifacts /artifacts --source-root /recipe-source

FROM scratch AS compiled-artifacts
COPY --from=artifact-sealer /artifacts/ /

FROM ${D2C_ARTIFACT_STAGE} AS selected-artifacts

FROM spark-base AS runtime

USER root

# The official pinned Spark image now supplies Temurin's Java 21 October PSU.
# Preserve that one JVM and its vendor metadata on both target architectures.
RUN java -version \
    && grep --fixed-strings --line-regexp 'JAVA_VERSION="21.0.13"' "$JAVA_HOME/release"

COPY lakehouse/Dockerfile /recipe-source/lakehouse/Dockerfile
COPY container-build/lakehouse/ /recipe-source/container-build/lakehouse/
COPY scripts/lakehouse_artifacts.py scripts/container_security_guard.py /recipe-source/scripts/

# This bounded streaming profile has no Spark Connect, Hive Thrift, Derby, or
# ZooKeeper role. Remove those unused distribution components and the older
# direct Netty/LZ4 libraries before adding patched runtime dependencies.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends --only-upgrade libssl3 openssl \
    # Native compilation belongs in build stages. Keep runtime libraries, Python,
    # and the JVM; do not auto-remove libraries that Spark/JNI may load.
    && apt-get purge --yes \
        build-essential cpp cpp-11 dpkg-dev g++ g++-11 gcc gcc-11 \
        libc6-dev libcrypt-dev libexpat1-dev libgcc-11-dev libnsl-dev \
        libpython3-dev libpython3.10-dev libstdc++-11-dev libtirpc-dev \
        linux-libc-dev make python3-dev python3.10-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/* /opt/spark/examples /opt/spark/jars/connect-repl \
    && rm -f \
        /opt/spark/jars/derby*.jar \
        /opt/spark/jars/guava-*.jar \
        /opt/spark/jars/failureaccess-*.jar \
        /opt/spark/jars/jackson-annotations-*.jar \
        /opt/spark/jars/jackson-core-*.jar \
        /opt/spark/jars/jackson-databind-*.jar \
        /opt/spark/jars/jackson-dataformat-yaml-*.jar \
        /opt/spark/jars/jackson-datatype-jsr310-*.jar \
        /opt/spark/jars/jackson-module-scala_2.13-*.jar \
        /opt/spark/jars/jline-3.*.jar \
        /opt/spark/jars/libthrift-*.jar \
        /opt/spark/jars/lz4-java-*.jar \
        /opt/spark/jars/netty-*.jar \
        /opt/spark/jars/hadoop-client-api-*.jar \
        /opt/spark/jars/hadoop-client-runtime-*.jar \
        /opt/spark/jars/parquet-jackson-*.jar \
        /opt/spark/jars/snappy-java-*.jar \
        /opt/spark/jars/spark-network-common_2.13-*.jar \
        /opt/spark/jars/spark-hive-thriftserver_*.jar \
        /opt/spark/jars/zookeeper-*.jar

# Verify current recipe hashes and every imported byte before installing any
# artifact. The read-only mount avoids committing duplicate staging JAR layers.
RUN --mount=type=bind,from=selected-artifacts,target=/tmp/d2c-artifacts,ro \
    python3 /recipe-source/scripts/lakehouse_artifacts.py verify \
        --artifacts /tmp/d2c-artifacts --source-root /recipe-source \
    && cp /tmp/d2c-artifacts/dependencies/*.jar /opt/spark/jars/ \
    && cp /tmp/d2c-artifacts/hadoop/hadoop-client-api-3.5.0-d2c-local1.jar /opt/spark/jars/ \
    && cp /tmp/d2c-artifacts/hadoop/hadoop-client-runtime-3.5.0-d2c-local1.jar /opt/spark/jars/ \
    && cp /tmp/d2c-artifacts/parquet/parquet-jackson-1.17.0-d2c-jackson2.21.7.jar /opt/spark/jars/ \
    && cp /tmp/d2c-artifacts/iceberg/iceberg-spark-runtime-4.2_2.13-1.12.0-d2c-security1.jar /opt/spark/jars/ \
    && cp /tmp/d2c-artifacts/spark/spark-core_2.13-4.2.1-d2c-jetty12.1.14.jar /opt/spark/jars/spark-core_2.13-4.2.1.jar \
    && cp /tmp/d2c-artifacts/spark/spark-network-common_2.13-4.2.1-d2c-guava33.7.2-jre.jar /opt/spark/jars/spark-network-common_2.13-4.2.1.jar \
    && mkdir --parents /usr/share/d2c-security/artifacts \
    && cp /tmp/d2c-artifacts/hadoop/provenance.json /usr/share/d2c-security/hadoop-source-provenance.json \
    && cp /tmp/d2c-artifacts/parquet/parquet-source-provenance.json /usr/share/d2c-security/ \
    && cp /tmp/d2c-artifacts/iceberg/iceberg-source-provenance.json /tmp/d2c-artifacts/iceberg/security-runtime-artifacts.json /tmp/d2c-artifacts/iceberg/iceberg-rebuild-verification.json /usr/share/d2c-security/ \
    && cp /tmp/d2c-artifacts/spark/spark-source-provenance.json /tmp/d2c-artifacts/spark/spark-core-verification.json /usr/share/d2c-security/ \
    && cp --recursive /recipe-source /usr/share/d2c-security/recipe-source \
    && cd /tmp/d2c-artifacts \
    && find . -type f ! -name '*.jar' -exec cp --parents '{}' /usr/share/d2c-security/artifacts/ ';'
COPY --chown=spark:root d2c_contract.py /opt/d2c-lakehouse/d2c_contract.py
COPY --chown=spark:root lakehouse /opt/d2c-lakehouse/lakehouse

RUN mkdir --parents /warehouse /checkpoints \
    && chown --recursive spark:root /warehouse /checkpoints /opt/d2c-lakehouse \
    && chmod --recursive g=u /warehouse /checkpoints /opt/d2c-lakehouse

ENV JAVA_HOME=/opt/java/openjdk \
    PYTHONPATH=/opt/d2c-lakehouse \
    SPARK_LOCAL_IP=127.0.0.1

WORKDIR /opt/d2c-lakehouse
USER spark
