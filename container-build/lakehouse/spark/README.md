# Spark 4.2.1 core source rebuild

This recipe compiles the official Spark **4.2.1 source** and executes its
unchanged `core/pom.xml` Maven shade recipe with only `jetty.version=12.1.14`.
It supplies a locally rebuilt core for a **complete Spark 4.2.1 distribution**.
It must not replace the core in an otherwise Spark 4.1.x distribution.

The recipe has not yet completed a real source compilation. Unit tests exercise
input rejection and verification using small fixtures; they do not establish a
working build, API compatibility in a running JVM, or a clean image scan.

## Build-stage contract

Run `sh /build/spark/rebuild.sh` after copying this directory to `/build/spark`.
The stage needs Python 3.10+, a JDK (the intended build/runtime is Java 21),
network access to official release/Maven endpoints, and the original
`/opt/spark/jars/spark-core_2.13-4.2.1.jar`. Run on `BUILDPLATFORM`: Java class
artifacts are architecture independent; final amd64 and arm64 images still
require separate startup/regression/scanner verification.

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
POM SHA256 against the official `v4.2.1` tag. A source-release/tag mismatch fails
closed and requires examining the real source; it is not silently accepted.

The original LICENSE/NOTICE documents and upstream shaded Maven metadata are
retained. No existing binary is unpacked, edited, or reassembled. No Maven
metadata is removed or renamed to change scanner results. Spark's Maven version
remains its true source version 4.2.1; the output filename and provenance identify
this local Jetty variant explicitly.

## Standalone core strategy and its limits

The command uses `mvn --file core/pom.xml clean package` and published **4.2.1**
Spark dependencies. All six direct main Spark module POMs (launcher, kvstore,
network-common, network-shuffle, unsafe, common-utils) are Maven artifacts in the
upstream recipe. All six POMs returned HTTP 200 on 2026-11-04. Their JARs and
complete resolution/compilation must still be proven by the real build. This is
a concrete strategy, not a claim that standalone compilation
has succeeded. If it fails because unpublished/generated reactor prerequisites
are needed, stop and report that failure before expanding to `-pl core -am`.

Scala **2.13.18**, Hadoop **3.5.0**, Kafka client **3.9.2**, Netty **4.2.13.Final**,
Jackson **2.21.2**, Jersey **3.1.11**, Servlet **6.0.0**, Java target **17**, and
Maven **3.9.15** are verified against the upstream source POM. The only dependency
override here is Jetty **12.1.14**. Patched final-image Netty/Jackson/Hadoop and
the matching Iceberg 4.2 bundle are separate integration requirements. This
recipe does not fix the separately bundled Hadoop Jetty 9 implementation.

## Outputs and checks

Outputs under `/out/spark`:

- `spark-core_2.13-4.2.1-d2c-jetty12.1.14.jar`
- `spark-source-provenance.json`, including source/tool digests, exact command,
  selected dependency tree summary, Java version, baseline/output hashes
- `spark-core-verification.json`, `spark-core-build-dependencies.json`
- `LICENSE.spark-4.2.1`, `NOTICE.spark-4.2.1`

The verifier requires real relocated Jetty HTTP/server/EE10 servlet classes and
the retained Jetty Maven module versions. Every selected Jetty dependency must
be 12.1.14; all selected Spark modules must be 4.2.1/Scala 2.13. It compares the
complete `org/apache/spark` class set and public/protected member descriptors,
access flags, superclass/interfaces, generic signatures, checked exceptions,
public constants, and class-file versions against the official 4.2.1 baseline.

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
- https://archive.apache.org/dist/maven/maven-3/3.9.15/binaries/apache-maven-3.9.15-bin.tar.gz.sha512
- https://jetty.org/security.html
