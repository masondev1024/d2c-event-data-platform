"""Pinned inputs and declared capability boundary for the D2C Hadoop assembly."""

UPSTREAM_VERSION = "3.5.0"
VARIANT_VERSION = "3.5.0-d2c-local1"
MAVEN_VERSION = "3.9.15"
GUAVA_VERSION = "33.7.2-jre"
HADOOP_SHADED_GUAVA_VERSION = "1.6.0"
SNAPPY_VERSION = "1.1.10.9"
CURATOR_VERSION = "5.2.0"
CURATOR_MODULES = ("curator-client", "curator-framework", "curator-recipes")
# Official classifiers attach the complete pre-shading module JAR. They are
# build inputs, not direct replacements: CuratorTransactionResult exposes a
# shaded Guava Predicate in its public ABI. Reassembly preserves that API.
CURATOR_CLASSIFIER = "osgi"
CURATOR_VARIANT_GROUP = "io.github.masondev1024.d2c.curator"
CURATOR_VARIANT_VERSION = "5.2.0-d2c-guava33.7.2-jre"
FAILUREACCESS_VERSION = "1.0.3"
CURATOR_SOURCE_ROOT_URL = "https://raw.githubusercontent.com/apache/curator/apache-curator-5.2.0/"
CURATOR_SOURCE_INPUTS = {
    "pom.xml": "23fec610ccbcad44efb6afbf26f8a0882c8160370bbf07bc3606f8695c0421b7",
    "curator-client/pom.xml": "c594b85f1f0e3a8cb04787ff5f835b9aa56b83740056df1337f148c677cae999",
    "curator-framework/pom.xml": "ea05969c12127f92e87a1f2d24ae690cc55e2206527eee48440abc9ea18b5c41",
    "curator-recipes/pom.xml": "7277b588fdc968f6bdabf44da98ff1e914366f20a0cc3a1cd9a41377374d870e",
    "src/site/confluence/breaking-changes.confluence": "43f19f2cfc09f49e97029d5aebf93e7daa65c3c0ff87dde310aad73bccb09bb0",
}
CURATOR_REVIEWED_METADATA = {
    "META-INF/maven/org.apache.curator/apache-curator/pom.xml": CURATOR_SOURCE_INPUTS["pom.xml"],
    **{f"META-INF/maven/org.apache.curator/{module}/pom.xml": CURATOR_SOURCE_INPUTS[f"{module}/pom.xml"]
       for module in CURATOR_MODULES},
}
THIRDPARTY_SOURCE_ROOT_URL = "https://raw.githubusercontent.com/apache/hadoop-thirdparty/rel/release-1.6.0/"
THIRDPARTY_SOURCE_INPUTS = {
    "pom.xml": "bd32fedb74f7bed84b210f280e70205be54ab17bc4a964aa89e28770d6cb8ab1",
    "hadoop-shaded-guava/pom.xml": "9ac65d44a14eac04112c50e08623282e79cd1cbf937594c90ca92728e45c9b30",
}
SOURCE_ROOT_URL = "https://raw.githubusercontent.com/apache/hadoop/rel/release-3.5.0/"
MODULES = ("hadoop-client-api", "hadoop-client-runtime")
# Select whole artifacts before shading; never edit an existing JAR. Keep the
# YARN client module: Master.getMasterPrincipal(local) calls YarnClientUtils.
# Only the unneeded interactive-shell transport and remote jobclient are omitted.
EXCLUDED_MODULES = ("hadoop-mapreduce-client-jobclient",)
EXCLUDED_ARTIFACTS = (
    ("org.apache.hadoop", "hadoop-mapreduce-client-jobclient"),
    ("org.eclipse.jetty.websocket", "websocket-client"),
)
REPLACED_CURATOR_ARTIFACTS = tuple(("org.apache.curator", module) for module in CURATOR_MODULES)
CURATOR_VARIANT_ARTIFACTS = tuple((CURATOR_VARIANT_GROUP, module) for module in CURATOR_MODULES)
REQUIRED_MODULES = (
    "hadoop-common", "hadoop-hdfs-client", "hadoop-mapreduce-client-core",
    "hadoop-mapreduce-client-common", "hadoop-yarn-api", "hadoop-yarn-common", "hadoop-yarn-client",
)
UNSUPPORTED_CAPABILITIES = (
    "YARN submission, management CLI and interactive NodeManager container shell",
    "MapReduce jobs submitted to YARN through mapreduce-client-jobclient",
    "Hadoop daemon/web-server and minicluster roles",
    "YARN Timeline delegation-token renewal and cancellation",
)
SUPPORTED_PROFILE = (
    "D2C Spark local[...] execution; local file:// Iceberg warehouse and absolute "
    "local checkpoint paths; Hadoop filesystem and MapReduce input/output APIs"
)
SOURCE_INPUTS = {
    "hadoop-client-modules/hadoop-client-api/pom.xml":
        "957a5d8781d4cd062e85ac384a1b56c6d00329bc0b03414f735c3131cf0b3be3",
    "hadoop-client-modules/hadoop-client-runtime/pom.xml":
        "f5a1580fde7a4fd89513c6ed94027236b6a4e27eaf5147e1e8711d72c468c8de",
    "hadoop-client-modules/hadoop-client/pom.xml":
        "95afbbef1abe1d5bf41d671d9686c17df12a039d1b09cd1541db5d631bab1986",
    "LICENSE.txt":
        "558e26751f0c7f72b79ce8b1be2c03b38629dbb639d6412f940603451f6cecd2",
    "NOTICE.txt":
        "bd8d44431e4ce7a8d03d194d6d11f6d1b3206de594885825ef7ce98ee40c456e",
    "hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/Master.java":
        "6ae3d30539c5ae3872cd1d30ec3d9a6f5cbd21f57488b88da57c08e775fcd979",
    "hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/util/YarnClientUtils.java":
        "357ca3d6e76af5d9ee15519af7251f038a6300f9b05db1ff56f3d30c527d7bc8",
    "hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/api/impl/YarnClientImpl.java":
        "f19610cb8f7d1757e835a85f0bb57f5cd66637edea88630668896a884b330b7b",
    "hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/api/ContainerShellWebSocket.java":
        "9867d26b30d165a703bc5084b4c9468fc76f10b4fa5fcc1d4fbff1f525bfb905",
    "hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/Cluster.java":
        "515316f4044cefe174e9ba34befb07e80672c4b7aba96f0390e92788a65753af",
}
DEPENDENCY_VERSIONS = {
    "com.fasterxml.jackson.core/jackson-core": "2.21.7",
    "com.fasterxml.jackson.core/jackson-databind": "2.21.7",
    "com.fasterxml.jackson.core/jackson-annotations": "2.21",
    "org.jline/jline": "3.30.17",
    "org.jline/jline-builtins": "3.30.17",
    "org.jline/jline-remote-telnet": "3.30.17",
    "com.google.guava/guava": GUAVA_VERSION,
    "org.apache.hadoop.thirdparty/hadoop-shaded-guava": HADOOP_SHADED_GUAVA_VERSION,
    **{f"org.apache.curator/{module}": CURATOR_VERSION for module in CURATOR_MODULES},
    **{f"{CURATOR_VARIANT_GROUP}/{module}": CURATOR_VARIANT_VERSION for module in CURATOR_MODULES},
    "com.google.guava/failureaccess": FAILUREACCESS_VERSION,
}
SOURCE_REVIEWS = {
    "org/apache/hadoop/mapreduce/Cluster": {
        "source": "hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/Cluster.java",
        "review": "Plain public class, not AutoCloseable. Its public synchronized close() throws IOException and closes the selected ClientProtocolProvider. Construct before try and close in finally; preserve the actual local getFileSystem bootstrap check.",
    },
    "org/apache/hadoop/mapred/Master": {
        "source": "hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/Master.java",
        "review": "No explicit static initializer. getMasterPrincipal separates CLASSIC only: LOCAL also calls YarnClientUtils.getRmPrincipal; retain that required utility unchanged.",
        "runtime_requirement": "Load/initialize Master and call getMasterAddress and getMasterPrincipal with framework=local, absent and explicit RM principals.",
    },
    "org/apache/hadoop/yarn/client/util/YarnClientUtils": {
        "source": "hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/util/YarnClientUtils.java",
        "review": "Static initialization creates only an SLF4J Logger and Commons Codec Base64. getRmPrincipal uses configuration, HAUtil and SecurityUtil; no Jetty/WebSocket/CLI call.",
    },
    "org/apache/hadoop/yarn/client/api/impl/YarnClientImpl": {
        "source": "hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/api/impl/YarnClientImpl.java",
        "review": "shellToContainer creates a WebSocketClient for NodeManager's interactive container shell. This remote operation is outside the D2C local profile and is not invoked by Master/YarnClientUtils.",
        "missing_transport_targets": (
            "api/Session", "api/WebSocketException", "client/ClientUpgradeRequest", "client/WebSocketClient",
        ),
    },
    "org/apache/hadoop/yarn/client/api/ContainerShellWebSocket": {
        "source": "hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/api/ContainerShellWebSocket.java",
        "review": "Interactive shell callbacks and run() use Session/RemoteEndpoint. This class is retained with its references visible, but remote interactive-shell transport is unsupported.",
        "missing_transport_targets": (
            "api/Session", "api/RemoteEndpoint", "api/annotations/OnWebSocketClose",
            "api/annotations/OnWebSocketConnect", "api/annotations/OnWebSocketMessage", "api/annotations/WebSocket",
        ),
    },
}
LOCAL_PROVIDER = "org.apache.hadoop.mapred.LocalClientProtocolProvider"
PROTOCOL_SERVICE = "org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider"
FILESYSTEM_PROVIDERS = (
    "org.apache.hadoop.fs.LocalFileSystem", "org.apache.hadoop.fs.viewfs.ViewFileSystem",
    "org.apache.hadoop.fs.HarFileSystem", "org.apache.hadoop.fs.http.HttpFileSystem",
    "org.apache.hadoop.fs.http.HttpsFileSystem", "org.apache.hadoop.hdfs.DistributedFileSystem",
    "org.apache.hadoop.hdfs.web.WebHdfsFileSystem", "org.apache.hadoop.hdfs.web.SWebHdfsFileSystem",
)
