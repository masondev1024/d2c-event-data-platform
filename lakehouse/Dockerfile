FROM apache/spark:4.1.3@sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211 AS spark-base

# Keep the JDK supplied by the pinned Spark image. Installing a second Java
# runtime mutates the OS dependency surface without improving this workload.

FROM spark-base AS dependency-resolver

USER root

ENV HOME=/tmp \
    IVY_HOME=/tmp/ivy \
    SPARK_LOCAL_IP=127.0.0.1

RUN mkdir --parents /opt/spark/extra-jars "$IVY_HOME" \
    && /opt/spark/bin/spark-submit \
        --master local[1] \
        --conf "spark.jars.ivy=$IVY_HOME" \
        --conf "spark.ui.enabled=false" \
        --packages "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.kafka:kafka-clients:3.9.2,org.lz4:lz4-java:1.8.1,io.netty:netty-all:4.2.17.Final,com.fasterxml.jackson.core:jackson-core:2.21.4,com.fasterxml.jackson.core:jackson-databind:2.21.4,com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.21.4,com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.21.4,com.fasterxml.jackson.module:jackson-module-scala_2.13:2.21.4,org.apache.httpcomponents.core5:httpcore5:5.4.3,org.apache.httpcomponents.core5:httpcore5-h2:5.4.3" \
        /opt/spark/examples/src/main/python/pi.py 1 \
    && find "$IVY_HOME/jars" -type f -name '*.jar' \
        ! -name 'org.apache.hadoop_hadoop-client-api-*.jar' \
        ! -name 'org.apache.hadoop_hadoop-client-runtime-*.jar' \
        -exec cp '{}' /opt/spark/extra-jars/ ';' \
    && test -n "$(find /opt/spark/extra-jars -type f -name '*.jar' -print -quit)"

FROM spark-base

USER root

# This bounded streaming profile has no Spark Connect, Hive Thrift, Derby, or
# ZooKeeper role. Remove those unused distribution components and the older
# direct Netty/LZ4 libraries before adding patched runtime dependencies.
RUN apt-get update \
    && apt-get install --yes --only-upgrade linux-libc-dev \
    && rm -rf /var/lib/apt/lists/* /opt/spark/examples /opt/spark/jars/connect-repl \
    && rm -f \
        /opt/spark/jars/derby*.jar \
        /opt/spark/jars/jackson-core-*.jar \
        /opt/spark/jars/jackson-databind-*.jar \
        /opt/spark/jars/jackson-dataformat-yaml-*.jar \
        /opt/spark/jars/jackson-datatype-jsr310-*.jar \
        /opt/spark/jars/jackson-module-scala_2.13-*.jar \
        /opt/spark/jars/jline-3.*.jar \
        /opt/spark/jars/libthrift-*.jar \
        /opt/spark/jars/lz4-java-*.jar \
        /opt/spark/jars/netty-*.jar \
        /opt/spark/jars/spark-hive-thriftserver_*.jar \
        /opt/spark/jars/zookeeper-*.jar

COPY --from=dependency-resolver /opt/spark/extra-jars/ /opt/spark/jars/
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
