FROM apache/spark:3.5.9@sha256:af02a459c8706e031c835c16f9db3c463816776e2543dd0f828af65606bcf392 AS spark-base

USER root

# Iceberg 1.11 requires Java 17. Keep the upstream Spark image but make its
# existing JAVA_HOME path resolve to the installed cross-architecture JRE.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless \
    && JDK_HOME="$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" \
    && rm -rf /opt/java/openjdk \
    && ln -s "$JDK_HOME" /opt/java/openjdk \
    && java -version \
    && rm -rf /var/lib/apt/lists/*

FROM spark-base AS dependency-resolver

ENV HOME=/tmp \
    IVY_HOME=/tmp/ivy \
    SPARK_LOCAL_IP=127.0.0.1

RUN mkdir --parents /opt/spark/extra-jars "$IVY_HOME" \
    && /opt/spark/bin/spark-submit \
        --master local[1] \
        --conf "spark.jars.ivy=$IVY_HOME" \
        --conf "spark.ui.enabled=false" \
        --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0" \
        /opt/spark/examples/src/main/python/pi.py 1 \
    && find "$IVY_HOME/jars" -type f -name '*.jar' -exec cp '{}' /opt/spark/extra-jars/ ';' \
    && test -n "$(find /opt/spark/extra-jars -type f -name '*.jar' -print -quit)"

FROM spark-base

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
