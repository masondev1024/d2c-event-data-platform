# Hadoop assembly for the existing D2C local profile

This recipe selects whole modules in the official Hadoop 3.5.0 source-free
`hadoop-client-api` and `hadoop-client-runtime` assemblies. It produces the
distinct coordinates `3.5.0-d2c-local1`; every selected upstream Hadoop module
remains 3.5.0. It does not edit an upstream binary, remove class files, discard
Maven metadata or change the upstream shade profiles.

The supported application boundary is Spark `local[...]`, a local `file://`
Iceberg warehouse and absolute local checkpoints. Hadoop configuration, local
filesystem and MapReduce input/output APIs remain required. This is not a
general Hadoop/YARN distribution or a Jetty 12 port.

Both source POMs exclude `hadoop-yarn-client` and
`hadoop-mapreduce-client-jobclient` from their `hadoop-client` aggregation
dependency. The latter supplies the YARN ClientProtocolProvider, so excluding
only the former leaves a dangling provider. The recipe explicitly selects
`mapreduce-client-common` (otherwise reachable only through jobclient) with the
same module exclusions. Common/core and their
LocalClientProtocolProvider/LocalJobRunner remain selected. The original
ServicesResourceTransformer generates descriptors from those selected modules.

YARN submission/CLI/interactive container shells, remote MapReduce jobclient,
daemon web roles, and YARN Timeline token renewal/cancellation are outside this
profile. The retained Timeline TokenRenewer has a lazy reference to the omitted
TimelineClient in renew/cancel. The recipe reports it and retains its descriptor;
it does not invoke remote token operations or claim universal bytecode closure.
`org.apache.hadoop.yarn.client.ClientRMProxy` belongs to retained yarn-common,
so the entire `yarn.client` namespace must not be treated as an omitted module.

Build-stage requirements: Python 3, JDK 21, Maven 3.9.15 at
`/opt/apache-maven-3.9.15` (or `MAVEN_BIN` pointing to that version), and a locked
cache mount at `/root/.m2`. The actual `mvn --version` report is verified before
source fetching and its hash is recorded in provenance. Run
`sh /build/hadoop/rebuild.sh`; the default working source root is `/work/hadoop`
and outputs are `/out/hadoop/hadoop-client-{api,runtime}-3.5.0-d2c-local1.jar`,
two complete JSON dependency trees and `provenance.json`. Original small source
inputs are fetched over HTTPS with SHA-256 checks and saved unchanged; generated
POMs have separate hashes. Input sources are build-stage data.

The build performs static graph/JAR checks only. Generated provenance marks
dynamic boot, D2C functional regression and vulnerability scans `NOT_RUN`.
Run the authored smoke source in the final image with its actual Spark classpath:

```
java --class-path '/opt/spark/jars/*' /mounted-tests/VerifyHadoopLocalProfile.java
```

The smoke test rejects old/shadowing Hadoop JARs, eagerly instantiates filesystem,
protocol and security/token services, checks patched Jackson, and exercises
local filesystem and MapReduce boot APIs using newly created temporary data.
It makes no remote filesystem or YARN call. It is not a Parquet/Iceberg/checkpoint
recovery test. Those actual D2C regressions and fresh scans of the final immutable
images, on both target platforms, are independent acceptance requirements.

As of preparation on 2026-11-04, this 3.5.0 local-profile recipe has **not been
built or run**. Passing the helper's own unit tests is not build/scan evidence.
