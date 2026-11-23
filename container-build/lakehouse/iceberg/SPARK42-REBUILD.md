The Spark 4.2 entry point is `sh /build/iceberg/rebuild-iceberg-spark42.sh`.
The existing 4.1 packaging-only recipe remains available for reproducing its
previous evidence. It is not a compatible 4.2 input.

Iceberg 1.12.0's pinned official source tag contains the Spark 4.2 integration,
extensions and runtime modules. On 2026-11-04 the individual official Maven
Central POM URLs for `iceberg-spark-4.2_2.13:1.12.0` and
`iceberg-spark-extensions-4.2_2.13:1.12.0`, and the 4.2 runtime JAR SHA256 URL,
returned HTTP 404. Therefore the new entry point compiles the original source
projects and runs their original `shadowJar` task. It fetches no fat JAR and
does not claim byte equality to an unavailable published 4.2 runtime.

The build patches only the version catalog: Guava 33.7.1-jre to 33.7.2-jre,
Spark 4.2.0 to 4.2.1, Jackson core
and databind 2.21.4 to 2.21.7, Jackson BOM 2.22.2 to aligned 2.21.7, annotations
2.22 to 2.21, and LZ4 1.11.3 to 1.11.4. Its Parquet 1.17.1 nested Jackson
module comes from the separately source-built local Maven repository at
`/rebuilt-repository`. Iceberg implementation, grammar, runtime dependency
list, relocations and LICENSE/NOTICE sources are unchanged.

The builder needs JDK 17 or 21, Python 3, tar, and network access for its pinned
source archive, Gradle 8.14.5 and official dependencies. Reuse cache
`d2c-iceberg-security-gradle` at `/root/.gradle`. Gradle runs one worker with a
768 MiB daemon heap and a 768 MiB Scala compiler limit; builds must be
serialized against other large compilation stages on the small Docker VM.

Output under `/artifacts`:

- `iceberg-spark-runtime-4.2_2.13-1.12.0-d2c-security1.jar`
- `security-runtime-artifacts.json`, with actual source/module input hashes
- `security-guava-artifacts.json`, with the original bundled Guava project's
  actual compile classpath coordinates and resolved file hashes
- `runtime-dependencies.txt`
- `iceberg-rebuild-verification.json`
- `iceberg-source-provenance.json`

The verifier fails on missing/added application classes, changed public or
protected JVM declarations (normalizing the original upstream relocations),
missing Spark/catalog entry points, changed input Maven metadata, dropped
retained dependency licenses, changed source LICENSE/NOTICE, extra Jackson
layers, or failure of either real shaded Jackson 2.21.7 JSON roundtrip.
It also requires actual resolved Guava 33.7.2-jre and failureaccess 1.0.3 JARs,
retained original Maven metadata, every retained Guava class API matching those
inputs, and complete class payloads matching those inputs after the original
upstream relocation rules through the final runtime. This verifier uses the
pinned builder JDK's ASM implementation in memory to canonicalize the constant
pool; it retains private code, annotations, fields and other class attributes
and never writes a runtime JAR. For retained CompactHashMap, CompactHashSet and
MapMakerInternalMap class families, it compares private JVM opcode and symbolic
reference disassembly against the fixed original input, normalizing only the
upstream package relocation, constant-pool indexes and equivalent byte offsets.
This detects old implementation code behind a changed version string. The
original upstream Guava `minimize()` task is unchanged; its omitted class count
is recorded separately. These locally computed resolved-file hashes are build
evidence and are not represented as independently obtained publisher checksums.

Parquet now builds 1.17.0 for Spark 4.2.1 and 1.17.1 for Iceberg, retaining its
existing `/work/output/maven` contract and patched Jackson versions. Its
`sources.json` pins the Apache 1.17.0 release archive SHA512 and immutable
1.17.1 source archive SHA256. The 1.17.0 annotation-property split is the same
build-only fix already present upstream in 1.17.1.

These recipes have offline/mock tests and a tiny real javac/javap fixture.
The updated Guava inputs still require a new Spark 4.2 source build, actual-JAR
verification, full-image scan and runtime regression in CI.
