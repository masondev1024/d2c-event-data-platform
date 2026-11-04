# Spark 4.2.1 core and network-common source rebuild

This recipe compiles the official Spark **4.2.1 source** and executes its
unchanged `core/pom.xml` and `common/network-common/pom.xml` Maven shade recipes
with `jetty.version=12.1.14` and `guava.version=33.7.2-jre`. The upstream
`guava.failureaccess.version=1.0.3` remains unchanged. It supplies locally rebuilt
modules for a **complete Spark 4.2.1 distribution**. These modules must not
replace modules in an otherwise Spark 4.1.x distribution.

On 2026-11-04 the original core Maven build with Java 21 completed but failed
the exact class-set verifier because `StoreTypes$1.class` was absent. The
subsequent build-only Java 17 recipe passed the core class/ABI and protobuf
checks in CI at revision `c5940910e02ecbee0dcc6d38abcb3c700b0552a8`.
That revision's final amd64 and arm64 Java 21 protobuf probes also passed;
both image scans still reported HIGH findings including bundled Guava. The
new network-common rebuild and Guava upgrade require a fresh CI build,
complete native runtime regression, and image scan. Offline fixtures do not
establish a verified artifact, working image, or clean scan.

## Build-stage contract

Run `sh /build/spark/rebuild.sh` after copying this directory to `/build/spark`.
The stage needs Python 3.10+ and the pinned Temurin **17.0.21+9** build JDK,
network access to official release/Maven endpoints, and the original
`/opt/spark/jars/spark-core_2.13-4.2.1.jar` and
`/opt/spark/jars/spark-network-common_2.13-4.2.1.jar`. Run on `BUILDPLATFORM`: Java class
artifacts are architecture independent; final amd64 and arm64 images still
require separate startup/regression/scanner verification.

The official core's manifest declares `Build-Jdk-Spec: 17`. The original Java
17 compiler generates a synthetic enum-switch mapping class for the generated
`SparkPlanGraphNodeWrapper.Builder.mergeFrom` implementation. Java 21 instead
uses the enum's ordinal directly when the enum is compiled in the same unit.
The official Maven baseline's `StoreTypes$1.class` is a nonpublic synthetic
class with a real static switch-map initializer, not an empty marker. We keep
the exact class-set and structural ABI checks and use the baseline's compiler
generation. No missing-class exemption or copying of a finished binary is used.

`inputs.json` pins the current official `17.0.21_9-jdk-jammy` image index,
both native platform manifests/configs, and its immutable Adoptium Dockerfile.
The publisher recipe uses Ubuntu 22.04, matching the Spark build base. The
Dockerfile copies the complete vendor JDK, including `release` and legal files,
only into `spark-runtime-builder`. Before fetching source, the driver verifies
the release's version/build/vendor and executes that JDK's absolute `java` and
`javac` paths. Maven must also report Java 17.0.21. These actual reports and the
release-file hash are recorded in provenance. Other source builders and the
final Spark runtime retain the original image's Temurin **21.0.13**.

The integrated Dockerfile supplies `--maven-home /opt/apache-maven-3.9.15`
and `--maven-archive /opt/build-tools/apache-maven-3.9.15-bin.tar.gz` to reuse
the common build-stage installation. The driver independently verifies that
archive's pinned SHA512 and the executable's version. Without both arguments,
the standalone recipe fetches and installs the same pinned tool itself.

Use a BuildKit cache mounted on `/root/.m2`, with `sharing=locked`. Compilation
is sequential, Maven heap is bounded at 1536 MiB, and there is no automatic
reactor fallback. Do not run multiple heavy build/scan jobs concurrently.

`inputs.json` pins the official source archive and Maven 3.9.15 with publisher
SHA512 values. Both archive and downloads Apache Spark checksum endpoints
returned the same source digest on 2026-11-04. The source archive itself was not
downloaded during recipe preparation. The build additionally checks root/core
and network-common POM SHA256 against the official `v4.2.1` tag. A source-release/tag mismatch fails
closed and requires examining the real source; it is not silently accepted.

The original LICENSE/NOTICE documents and upstream shaded Maven metadata are
retained. No existing binary is unpacked, edited, or reassembled. No Maven
metadata is removed or renamed to change scanner results. Spark's Maven version
remains its true source version 4.2.1; the output filename and provenance identify
these local Jetty and Guava variants explicitly.

## Standalone module strategy and its limits

The commands use `mvn --file common/network-common/pom.xml clean package`, then
`mvn --file core/pom.xml clean package`, and published **4.2.1** Spark
dependencies. The network module's published prerequisite is
`spark-common-utils-java_2.13:4.2.1`. All six direct main core Spark module POMs (launcher, kvstore,
network-common, network-shuffle, unsafe, common-utils) are Maven artifacts in the
upstream recipe. All six POMs returned HTTP 200 on 2026-11-04. Their JARs and
core source compilation succeeded in the Java 21 CI attempt; its output was
rejected by the exact class-set guard and was not exported. The subsequent
Java 17 core build passed its complete structural/protobuf verification in CI.
The final runtime installs the independently verified rebuilt network JAR.
If unpublished/generated reactor
prerequisites cause a future failure, report it before expanding to `-pl core -am`.

Scala **2.13.18**, Hadoop **3.5.0**, Kafka client **3.9.2**, Netty **4.2.13.Final**,
Jackson **2.21.2**, Jersey **3.1.11**, Servlet **6.0.0**, Java target **17**, and
Maven **3.9.15** are verified against the upstream source POM. The only dependency
overrides here are Jetty **12.1.14** and Guava **33.7.2-jre**. Patched final-image Netty/Jackson/Hadoop and
the matching Iceberg 4.2 bundle are separate integration requirements. This
recipe does not fix the separately bundled Hadoop Jetty 9 implementation.

## Outputs and checks

Outputs under `/out/spark`:

- `spark-core_2.13-4.2.1-d2c-jetty12.1.14.jar`
- `spark-network-common_2.13-4.2.1-d2c-guava33.7.2-jre.jar`
- `spark-source-provenance.json`, including source/tool digests, exact command,
  selected dependency tree summary, Java version, baseline/output hashes
- `spark-core-verification.json`, `spark-core-build-dependencies.json`
- `spark-network-common-verification.json`, `spark-network-common-build-dependencies.json`
- `LICENSE.spark-4.2.1`, `NOTICE.spark-4.2.1`

The verifier requires real relocated Jetty HTTP/server/EE10 servlet classes and
the retained Jetty Maven module versions. Every selected Jetty dependency must
be 12.1.14; all selected Spark modules must be 4.2.1/Scala 2.13. It compares the
complete `org/apache/spark` class set, without a synthetic-class exception, and
public/protected member descriptors,
access flags, superclass/interfaces, generic signatures, checked exceptions,
public constants, and class-file versions against the official 4.2.1 baseline.

The network verifier independently compares every original Spark implementation
class and its public/protected structural API with the original network-common
baseline. It requires actual relocated Guava/failureaccess classes, their true
Maven coordinates/POMs, and byte-identical original LICENSE/NOTICE documents.
It checks the full relocated class inventory and structural APIs against the
Guava 33.7.2-jre and failureaccess 1.0.3 JARs actually resolved in the same Maven
repository, and verifies byte-identical retained dependency Maven metadata.
Provenance records each resolved JAR SHA256, class count, and a canonical sorted
class-name/hash inventory digest. These are measured build-input hashes, not
independently authenticated publisher checksums. They do not compare every JVM
instruction or replace real runtime behavior checks.

After those checks, the build compiles `VerifySparkProtobuf.java` with Java 17
and runs it against only the rebuilt core JAR. The probe verifies the actual JVM
major, six generated classes' canonical code-source paths, and the core's SHA256.
It exercises `NODE`, `CLUSTER`, and `WRAPPER_NOT_SET` through `mergeFrom` and
protobuf wire roundtrips with IDs and Unicode names. It also checks that merging
an unset wrapper preserves an existing node or cluster. The three reported case
counts represent unique enum branches; the preservation checks are additional.
Compilation, execution, or proof validation failure prevents artifact export.

The same source must run separately in each actual final image with Java 21;
the builder's Java 17 result is not a final-image result. For example, using a
temporary writable directory in an otherwise read-only image:

```sh
javac --release 17 -cp /opt/spark/jars/spark-core_2.13-4.2.1.jar \
  -d /tmp/d2c-spark-protobuf \
  /usr/share/d2c-security/recipe-source/container-build/lakehouse/spark/VerifySparkProtobuf.java
java -cp /tmp/d2c-spark-protobuf:/opt/spark/jars/spark-core_2.13-4.2.1.jar \
  VerifySparkProtobuf 21 /opt/spark/jars/spark-core_2.13-4.2.1.jar
```

Success emits exactly `D2C_SPARK_PROTOBUF_PROOF=` followed by schema-version 1
JSON. The image check must compare its JAR hash to the verified source artifact,
not merely accept a familiar filename. Runtime regression and scanning remain
separate mandatory checks.

This structural ABI comparison does not prove behavioral compatibility,
annotations/ScalaSignature payload equivalence, or runtime linkage. Required
integration checks include Spark startup/shutdown, Kafka ingestion, Iceberg
write/read, Parquet output, checkpoint replay/idempotency, and an actual scan of
each final platform image. Build success must not be reported as full security
resolution.

Official inputs:

- https://archive.apache.org/dist/spark/spark-4.2.1/spark-4.2.1.tgz.sha512
- https://raw.githubusercontent.com/apache/spark/v4.2.1/pom.xml
- https://raw.githubusercontent.com/apache/spark/v4.2.1/core/pom.xml
- https://raw.githubusercontent.com/apache/spark/v4.2.1/common/network-common/pom.xml
- https://github.com/google/guava/releases/tag/v33.7.2
- https://raw.githubusercontent.com/google/guava/v33.7.2/guava/pom.xml
- https://archive.apache.org/dist/maven/maven-3/3.9.15/binaries/apache-maven-3.9.15-bin.tar.gz.sha512
- https://jetty.org/security.html
- https://raw.githubusercontent.com/adoptium/containers/7b7fa6e65eeac043b6ddcc56e7efb36c6fe928e7/17/jdk/ubuntu/jammy/Dockerfile
- https://raw.githubusercontent.com/openjdk/jdk17u/jdk-17.0.21%2B9/src/jdk.compiler/share/classes/com/sun/tools/javac/comp/Lower.java
- https://github.com/openjdk/jdk/blob/jdk-21%2B35/src/jdk.compiler/share/classes/com/sun/tools/javac/comp/Lower.java#L456-L507

The exact current Java 17 update's `Lower.java` SHA256 was
`428d3f826d9688bea127bcb3d2e742a2bab1a06a659aa6cb27cf5e073bba970f`
on 2026-11-04. It still uses runtime enum mapping; the Java 21 optimization has
not been backported there. Bounded byte-range reads of the official Maven core
and sources JARs established the baseline helper SHA256
`a1c7e7c4b8bb332fe5c6b582528ced3adf790c24c2b3c26e2c256f9e64730e5a`
and generated `StoreTypes.java` SHA256
`4eadcdc483dc8d27c9d38b318d73e305b2aa358fac04b7563936ea1c9592059e`.
These diagnose the compiler difference; they do not replace the real artifact
comparison or establish the new network-common build's result.
