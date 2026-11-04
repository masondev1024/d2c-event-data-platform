"""Pinned inputs and declared capability boundary for the D2C Hadoop assembly."""

UPSTREAM_VERSION = "3.5.0"
VARIANT_VERSION = "3.5.0-d2c-local1"
MAVEN_VERSION = "3.9.15"
SOURCE_ROOT_URL = "https://raw.githubusercontent.com/apache/hadoop/rel/release-3.5.0/"
MODULES = ("hadoop-client-api", "hadoop-client-runtime")
# Select whole modules before shading; never edit an existing JAR. jobclient
# contributes YARN's ClientProtocolProvider; common contributes the local one.
EXCLUDED_MODULES = ("hadoop-yarn-client", "hadoop-mapreduce-client-jobclient")
REQUIRED_MODULES = (
    "hadoop-common", "hadoop-hdfs-client", "hadoop-mapreduce-client-core",
    "hadoop-mapreduce-client-common", "hadoop-yarn-api", "hadoop-yarn-common",
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
}
DEPENDENCY_VERSIONS = {
    "com.fasterxml.jackson.core/jackson-core": "2.21.7",
    "com.fasterxml.jackson.core/jackson-databind": "2.21.7",
    "com.fasterxml.jackson.core/jackson-annotations": "2.21",
}
LOCAL_PROVIDER = "org.apache.hadoop.mapred.LocalClientProtocolProvider"
PROTOCOL_SERVICE = "org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider"
FILESYSTEM_PROVIDERS = (
    "org.apache.hadoop.fs.LocalFileSystem", "org.apache.hadoop.fs.viewfs.ViewFileSystem",
    "org.apache.hadoop.fs.HarFileSystem", "org.apache.hadoop.fs.http.HttpFileSystem",
    "org.apache.hadoop.fs.http.HttpsFileSystem", "org.apache.hadoop.hdfs.DistributedFileSystem",
    "org.apache.hadoop.hdfs.web.WebHdfsFileSystem", "org.apache.hadoop.hdfs.web.SWebHdfsFileSystem",
)
