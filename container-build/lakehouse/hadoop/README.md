# Hadoop assembly for the existing D2C local profile

This recipe selects whole artifacts in the official Hadoop 3.5.0 source-free
`hadoop-client-api` and `hadoop-client-runtime` assemblies. It produces the
distinct coordinates `3.5.0-d2c-local1`; every selected upstream Hadoop module
remains 3.5.0. It does not edit an upstream binary, remove class files, discard
Maven metadata or change the upstream shade profiles.

The supported application boundary is Spark `local[...]`, a local `file://`
Iceberg warehouse and absolute local checkpoints. Hadoop configuration, local
filesystem and MapReduce input/output APIs remain required. This is not a
general Hadoop/YARN distribution or a Jetty 12 port.

Both source POMs retain `hadoop-yarn-client` because Hadoop's original
`Master.getMasterPrincipal()` calls `YarnClientUtils.getRmPrincipal()` for
`framework=local` too: only `classic` has a separate branch. The utility's static
initialization creates a Logger and Commons Codec Base64; principal calculation
uses configuration, HAUtil and SecurityUtil without a Jetty/WebSocket/CLI call.
Keeping this original utility preserves both the null result for an unset
principal and configured principal calculation. No upstream Java code is patched.

The POMs omit `hadoop-mapreduce-client-jobclient`, which supplies the remote YARN
ClientProtocolProvider, and the separate
`org.eclipse.jetty.websocket:websocket-client` transport artifact. The latter is
an upstream **compile** dependency, not `optional=true`; omitting it is an explicit
D2C capability-profile decision. The recipe explicitly selects
`mapreduce-client-common` (otherwise reachable only through jobclient), applying
the same two artifact exclusions to that path. Common/core and their
LocalClientProtocolProvider/LocalJobRunner remain selected. The original
ServicesResourceTransformer generates descriptors from those selected modules.
JLine is retained as a complete dependency with its version fixed to 3.30.15;
actual Telnet implementation classes and all relevant version metadata are checked.

YARN submission/CLI/interactive container shells, remote MapReduce jobclient,
daemon web roles, and YARN Timeline token renewal/cancellation are outside this
profile. YarnClientImpl's interactive `shellToContainer()` and
ContainerShellWebSocket retain their now-unavailable transport references, and
the generated provenance reports them. Their reviewed source hashes and exact
reference targets are pinned; new callers or transport targets fail validation.
The recipe does not hide those API classes, edit service descriptors, invoke
remote token operations or claim universal bytecode closure. The four reviewed
official Java files are downloaded as immutable review evidence, not patched or
compiled as replacement classes. Their URLs, SHA-256 hashes and branch findings
are recorded in provenance alongside the original assembly POMs and metadata.

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

The smoke test checks JAR origins of selected Hadoop/Jackson entrypoints and
rejects explicitly excluded subsystem classes. It eagerly instantiates filesystem,
protocol and security/token services, checks patched Jackson, and exercises
local filesystem and MapReduce boot APIs using newly created temporary data.
It initializes Master and YarnClientUtils, checks their actual JAR origins, and
executes local Master address/principal lookups with absent and explicit fixture
principals. Missing utility dependencies cannot pass by ignoring a symbolic
reference. The test also verifies that the excluded WebSocket implementation
cannot be supplied by another classpath JAR.
It makes no remote filesystem or YARN call. It is not a Parquet/Iceberg/checkpoint
recovery test. Those actual D2C regressions and fresh scans of the final immutable
images, on both target platforms, are independent acceptance requirements.

This corrected artifact selection awaits a new source build and actual
final-image runtime checks. Passing the helper's own unit tests is not
build, runtime-compatibility or vulnerability-scan evidence.
