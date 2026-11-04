/*
 * D2C regression probe for the original Spark 4.2.1 generated protobuf classes.
 * This compiles against, and must load every tested class from, the supplied JAR.
 * No dependency download, network connection, Spark service, or data directory
 * is used. Run separately in the builder (Java 17) and final image (Java 21).
 */
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.apache.spark.status.protobuf.StoreTypes;
import org.apache.spark.status.protobuf.StoreTypes.SparkPlanGraphClusterWrapper;
import org.apache.spark.status.protobuf.StoreTypes.SparkPlanGraphNode;
import org.apache.spark.status.protobuf.StoreTypes.SparkPlanGraphNodeWrapper;
import org.apache.spark.status.protobuf.StoreTypes.SparkPlanGraphNodeWrapper.WrapperCase;

public final class VerifySparkProtobuf {
  private VerifySparkProtobuf() {}

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalStateException(message);
    }
  }

  private static String quote(String value) {
    StringBuilder result = new StringBuilder("\"");
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      if (character == '\\' || character == '"') {
        result.append('\\').append(character);
      } else if (character < 0x20) {
        result.append(String.format("\\u%04x", (int) character));
      } else {
        result.append(character);
      }
    }
    return result.append('"').toString();
  }

  private static Path origin(Class<?> implementation) throws Exception {
    require(implementation.getProtectionDomain().getCodeSource() != null,
        "Missing code source: " + implementation.getName());
    return Path.of(implementation.getProtectionDomain().getCodeSource().getLocation().toURI())
        .toRealPath();
  }

  private static void checkCase(SparkPlanGraphNodeWrapper input, WrapperCase expected,
      SparkPlanGraphNodeWrapper previous) throws Exception {
    // mergeFrom is the method whose original Java 17 compilation uses StoreTypes$1.
    SparkPlanGraphNodeWrapper merged = SparkPlanGraphNodeWrapper.newBuilder(previous)
        .mergeFrom(input).build();
    SparkPlanGraphNodeWrapper expectedMerged = expected == WrapperCase.WRAPPER_NOT_SET
        ? previous : input;
    require(merged.equals(expectedMerged), "mergeFrom payload mismatch: " + expected);
    require(merged.getWrapperCase() == expectedMerged.getWrapperCase(),
        "mergeFrom wrapper case mismatch: " + expected);
    SparkPlanGraphNodeWrapper parsed = SparkPlanGraphNodeWrapper.parseFrom(merged.toByteArray());
    require(parsed.equals(expectedMerged), "Protobuf wire roundtrip mismatch: " + expected);
    require(parsed.getWrapperCase() == expectedMerged.getWrapperCase(),
        "Protobuf wire wrapper case mismatch: " + expected);
  }

  public static void main(String[] arguments) throws Exception {
    require(arguments.length == 2, "Expected arguments: JAVA_MAJOR CORE_JAR");
    int expectedJava = Integer.parseInt(arguments[0]);
    int actualJava = Runtime.version().feature();
    require((expectedJava == 17 || expectedJava == 21) && actualJava == expectedJava,
        "Unexpected Java major: " + actualJava);
    Path expectedJar = Path.of(arguments[1]).toRealPath();
    require(Files.isRegularFile(expectedJar), "Expected a regular core JAR");
    Class<?>[] implementations = {
        StoreTypes.class, SparkPlanGraphNodeWrapper.class,
        SparkPlanGraphNodeWrapper.Builder.class, WrapperCase.class,
        SparkPlanGraphNode.class, SparkPlanGraphClusterWrapper.class
    };
    StringBuilder origins = new StringBuilder("{");
    for (Class<?> implementation : implementations) {
      Path actualJar = origin(implementation);
      require(expectedJar.equals(actualJar), "Wrong core JAR origin: " + implementation.getName());
      if (origins.length() > 1) {
        origins.append(',');
      }
      origins.append(quote(implementation.getName())).append(':').append(quote(actualJar.toString()));
    }
    origins.append('}');

    SparkPlanGraphNodeWrapper node = SparkPlanGraphNodeWrapper.newBuilder().setNode(
        SparkPlanGraphNode.newBuilder().setId(42L).setName("node-\uD55C\uAE00").build()).build();
    SparkPlanGraphNodeWrapper cluster = SparkPlanGraphNodeWrapper.newBuilder().setCluster(
        SparkPlanGraphClusterWrapper.newBuilder().setId(73L).setName("cluster-\uD55C\uAE00").build()).build();
    SparkPlanGraphNodeWrapper unset = SparkPlanGraphNodeWrapper.getDefaultInstance();
    require(node.getWrapperCase() == WrapperCase.NODE, "Node fixture is invalid");
    require(cluster.getWrapperCase() == WrapperCase.CLUSTER, "Cluster fixture is invalid");
    require(unset.getWrapperCase() == WrapperCase.WRAPPER_NOT_SET, "Unset fixture is invalid");
    checkCase(node, WrapperCase.NODE, cluster);
    checkCase(cluster, WrapperCase.CLUSTER, node);
    checkCase(unset, WrapperCase.WRAPPER_NOT_SET, unset);
    checkCase(unset, WrapperCase.WRAPPER_NOT_SET, node);
    checkCase(unset, WrapperCase.WRAPPER_NOT_SET, cluster);

    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (InputStream input = Files.newInputStream(expectedJar)) {
      byte[] buffer = new byte[64 * 1024];
      int count;
      while ((count = input.read(buffer)) != -1) {
        digest.update(buffer, 0, count);
      }
    }
    String proof = "{\"schema_version\":1,\"status\":\"passed\","
        + "\"actual_java_major\":" + actualJava + ",\"expected_java_major\":" + expectedJava + ','
        + "\"java_runtime_version\":" + quote(System.getProperty("java.runtime.version")) + ','
        + "\"java_vendor\":" + quote(System.getProperty("java.vendor")) + ','
        + "\"expected_core_jar\":" + quote(expectedJar.toString()) + ','
        + "\"actual_core_jar\":" + quote(origin(StoreTypes.class).toString()) + ','
        + "\"core_jar_sha256\":" + quote(HexFormat.of().formatHex(digest.digest())) + ','
        + "\"class_code_sources\":" + origins + ','
        + "\"cases\":[\"NODE\",\"CLUSTER\",\"WRAPPER_NOT_SET\"],"
        + "\"merge_cases_verified\":3,\"wire_roundtrips_verified\":3,"
        + "\"unset_merge_preserves_existing\":true}";
    System.out.println("D2C_SPARK_PROTOBUF_PROOF=" + proof);
  }
}
