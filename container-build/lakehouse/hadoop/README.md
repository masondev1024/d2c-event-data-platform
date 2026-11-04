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
the same transport exclusions and Curator replacements to that path. Common/core and their
LocalClientProtocolProvider/LocalJobRunner remain selected. The original
ServicesResourceTransformer generates descriptors from those selected modules.
JLine is retained as a complete dependency with its version fixed to 3.30.17;
actual Telnet implementation classes and all relevant version metadata are checked.
The patch release fixes the upstream
[Telnet descriptor leak](https://github.com/jline/jline3/security/advisories/GHSA-c87g-867h-cqr6)
and [Windows native-library issue](https://github.com/jline/jline3/security/advisories/GHSA-6r3w-6jpj-x5w6).

Both ordinary Guava and Hadoop's already shaded Guava must be updated. The
recipe selects Guava **33.7.2-jre** and the official
`hadoop-shaded-guava` **1.6.0**, whose pinned original parent POM selects that
same fixed Guava. Only `hadoop-thirdparty-guava.version` is overridden; the
separate Hadoop protobuf dependency remains unchanged. The runtime verifier
checks both original resolved input JAR hashes, the inner and outer Maven
identities, and complete Guava class inventories of each declared namespace.
An unexpected namespace or old nested version cannot pass using the
ordinary Guava descriptor. All generated class references are checked as well.

Curator 5.2.0's standard client embeds another Guava 27.0.1 copy, under
`org.apache.curator.shaded.com.google`, with its descriptor removed by the
original Curator build. Updating an external Maven Guava property cannot replace
those pre-shaded classes. The official `osgi` classifiers contain the complete
pre-Shade modules, but direct substitution is not ABI compatible:
[`CuratorTransactionResult.ofTypeAndPath()`](https://raw.githubusercontent.com/apache/curator/apache-curator-5.2.0/curator-framework/src/main/java/org/apache/curator/framework/api/transaction/CuratorTransactionResult.java)
publicly returns a Guava Predicate. The strict standard/OSGi comparison detected
this real mismatch in CI. Removing ListenerContainer in Curator 5.0 did not
eliminate every public Guava type.

`rebuild_curator.py` therefore uses the three complete official 5.2.0 `osgi`
artifacts as inputs to normal source-free Maven assemblies. It reapplies the
[original Curator relocation](https://raw.githubusercontent.com/apache/curator/apache-curator-5.2.0/pom.xml),
`com.google` to `org.apache.curator.shaded.com.google`, with whole Guava
**33.7.2-jre** and failureaccess **1.0.3** inputs. Only the client embeds those
dependencies; complete framework and recipes remain separate modules. The new
coordinates are `io.github.masondev1024.d2c.curator:curator-{client,framework,recipes}:5.2.0-d2c-guava33.7.2-jre`.
Their original `org.apache.curator` 5.2.0 Maven identities remain present,
alongside the distinct custom assembly identities and actual input/output hashes.
Original normal or OSGi artifacts cannot re-enter the final Hadoop dependency
graph; only the verified custom trio is selected there.

A separate build-only Maven assembly contains only the two fixed Guava inputs
under that same original relocation. Every embedded Guava implementation class
in the new client must exactly match its reference bytes, and the reference
must match both complete original input class inventories. Actual fixed GAVs,
source JAR hashes, all Guava references and provider descriptors are checked.
The reference JAR is not exported to the runtime or published. No upstream Java
or completed JAR is patched. Maven compiles no replacement Curator Java classes.
The original namespace is now retained with fixed code, rather than being
declared vulnerable solely because its name is unchanged.

Every rebuilt module must retain the complete own class set and the original
standard artifact's public/protected structural ABI, including protected nested
types and exposed internal ancestors. Original Curator POM/properties and
LICENSE/NOTICE remain byte-identical; fixed Guava/failureaccess POM/properties
are retained, including their actual patched versions. Exact source-POM hashes
distinguish packaging descriptions from executable references. Guava documents,
each Curator module's license documents, and original input manifests are also
copied before assembly into qualified `META-INF/d2c-*` paths ending `.original`.
That suffix prevents Hadoop's unchanged `endsWith(LICENSE/NOTICE)` transformer
from discarding supplemental copies. Final Hadoop verification requires every
recorded copy to retain its original bytes/hash. The active custom manifest is
for the existing Java classpath profile; OSGi and JPMS module support are not
claimed. Actual multi-release implementation inputs require activating manifests
and fail verification if that separate contract is unmet.

The final runtime must contain every selected Curator class and all three
complete fixed Guava namespaces after Hadoop's original relocation. Resolved
trees, installed POMs, original source recipes, built JAR hashes and successful
build logs bind those checks to the actual Maven assemblies. These static checks
require real CI artifacts and do not replace final-image linkage or regressions.

The external native `snappy-java` dependency is fixed at **1.1.10.9** in both
generated source POMs and resolved graphs. Hadoop's unchanged original
artifact-set exclusions leave that complete dependency outside both fat JARs.
Checks reject embedded Snappy classes, Maven descriptors, native payloads or
pre-shaded symbolic references. The final image separately supplies and tests
the patched native artifact on both supported platforms.

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
remote token operations or claim universal bytecode closure. The five reviewed
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
The Hadoop 3.5 Cluster probe uses its explicit `close()` in a `finally` block:
the original class does not implement AutoCloseable. It still executes the
actual local protocol/filesystem bootstrap before closing.
It also serializes and deserializes small valid CompactHashMap, CompactHashSet
and MapMakerInternalMap values through all three actual Guava namespaces and checks
their JAR origins. This is a normal compatibility exercise, not an oversized
allocation proof. An unstarted Curator client and recipe are constructed and
closed to exercise all three retained modules and their fixed Guava linkage;
ZooKeeper connections are never started. The actual public transaction predicate
factory must retain the original shaded Predicate return type and successfully
apply its predicate to a newly constructed transaction result.
It makes no remote filesystem or YARN call. It is not a Parquet/Iceberg/checkpoint
recovery test. Those actual D2C regressions and fresh scans of the final immutable
images, on both target platforms, are independent acceptance requirements.

This corrected artifact selection awaits a new source build and actual
final-image runtime checks. Passing the helper's own unit tests is not
build, runtime-compatibility or vulnerability-scan evidence.
