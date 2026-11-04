# Hadoop assembly for the existing D2C local profile

This recipe selects whole artifacts in the official Hadoop 3.5.0 source-free
`hadoop-client-api` and `hadoop-client-runtime` assemblies. It produces the
distinct coordinates `3.5.0-d2c-local1`; every selected upstream Hadoop module
remains 3.5.0. It does not edit an upstream binary, remove class files or strip
metadata after packaging. All upstream shade filters, relocations and existing
transformers remain unchanged. A build-only extension handles JLine's own
provider descriptor formats during the original Maven shading step.

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
JLine is retained as a complete dependency with its version fixed to 3.30.17;
actual Telnet implementation classes and all relevant version metadata are checked.
The patch release fixes the upstream
[Telnet descriptor leak](https://github.com/jline/jline3/security/advisories/GHSA-c87g-867h-cqr6)
and [Windows native-library issue](https://github.com/jline/jline3/security/advisories/GHSA-6r3w-6jpj-x5w6).

JLine 3.30.17's original `TerminalProvider.load(String)` reads plain class names
from `META-INF/jline/providers/{dumb,exec,jni,ffm}`. It does not use the legacy
slash-named `META-INF/services/org/jline/terminal/provider/{jna,jansi}` properties
files. Maven Shade's general ServicesResourceTransformer handles dotted Java SPI
descriptors, but leaves these JLine-specific values unrelocated. Without an
extension the active loader therefore asks for an unshaded implementation that
does not exist in the newly shaded runtime.

The authored `d2c.shade.hadoop.JlineProviderResourceTransformer` is installed only
into Maven's build-plugin classpath, before the existing general transformer.
It accepts exactly those six paths/formats, uses Maven's actual Relocator for the
single implementation token, and preserves paths, comments, licenses and all
other bytes. Unknown formats, relocation escapes and conflicting duplicates fail
the assembly. This is source-time assembly configuration; no finished Hadoop
JAR is edited. The extension and its contract self-test are compiled with JDK21
`javac --release 17` against the existing Shade 3.6.1 API. Its local coordinate is
`io.github.masondev1024.d2c:hadoop-jline-resource-transformer:1.0.0`; that plugin JAR
is not exported as a runtime dependency. Compiler/build log, source hashes,
compiled/installed JAR hashes and successful contract execution are recorded in
`jline-transformer-build.json` and `jline-transformer-build.log` only after success.

The typed verifier checks the actual resolved official JLine aggregate JAR's
version and six source-pinned resource hashes. Generated resources must equal
the exact class-token relocation of those original bytes. All four active
provider classes must be present. A legacy implementation's presence must match
the original aggregate, with any original absence recorded as inactive, never
deleted or fabricated. Standard Java SPI descriptors retain strict provider
class checks. The six small licensed resource fixtures are exact official-hash
test inputs, not binaries or replacement runtime inputs.

YARN submission/CLI/interactive container shells, remote MapReduce jobclient,
daemon web roles, and YARN Timeline token renewal/cancellation are outside this
profile. YarnClientImpl's interactive `shellToContainer()` and
ContainerShellWebSocket retain their now-unavailable transport references, and
the generated provenance reports them. Their reviewed source hashes and exact
reference targets are pinned; new callers or transport targets fail validation.
The recipe does not hide those API classes, remove service descriptors, invoke
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

The official runtime shade profile filters all entries of `org.apache.hadoop:*`
using `**/*` and `*`. This also omits the runtime project's own `pom.properties`
and `pom.xml`; the recipe does not create, delete or relabel these inside the
JAR. The identity verifier accepts only that exact absence, bound to the pinned
original runtime POM and its recorded filter hash. It requires the generated
source POM, actual installed local-repository POM and resolved tree root to
name the same custom GAV, and the exported JAR SHA-256 to match the actual Maven
target JAR. The installed POM must also match Maven's generated dependency-reduced
POM byte for byte. Both exported POMs and their hashes are retained. The API's project descriptor,
original retained Hadoop module descriptors, and every required patched-library
descriptor remain mandatory and unique, with exact GAV/version checks. There is
no general metadata-absence fallback. The small original runtime POM in
`fixtures/` is a hash-verified, licensed test input, not a replacement build input.

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
It calls the actual shaded `TerminalProvider.load("dumb")` with the final image's
classloader and requires a shaded DumbTerminalProvider from the rebuilt Hadoop
runtime JAR. Finding a class without executing JLine's reflective loader cannot
pass this check. Optional native providers and the FFM backend (which requires
newer Java APIs) are not instantiated or claimed to work on Java21.
It makes no remote filesystem or YARN call. It is not a Parquet/Iceberg/checkpoint
recovery test. Those actual D2C regressions and fresh scans of the final immutable
images, on both target platforms, are independent acceptance requirements.

This corrected artifact selection awaits a new source build and actual
final-image runtime checks. Passing the helper's own unit tests is not
build, runtime-compatibility or vulnerability-scan evidence.
