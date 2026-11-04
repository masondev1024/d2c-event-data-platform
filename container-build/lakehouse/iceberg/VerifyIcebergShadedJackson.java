import java.lang.reflect.Method;

/** Load and exercise both real relocated Jackson implementations in Iceberg. */
public final class VerifyIcebergShadedJackson {
  private static final String[] PREFIXES = {
      "org.apache.iceberg.shaded.com.fasterxml.jackson",
      "org.apache.iceberg.shaded.org.apache.parquet.shaded.com.fasterxml.jackson"
  };

  public static void main(String[] args) throws Exception {
    for (String prefix : PREFIXES) {
      Class<?> corePackage = Class.forName(prefix + ".core.json.PackageVersion");
      Class<?> databindPackage = Class.forName(prefix + ".databind.cfg.PackageVersion");
      String corePackageVersion = corePackage.getField("VERSION").get(null).toString();
      String databindPackageVersion = databindPackage.getField("VERSION").get(null).toString();
      Class<?> factoryClass = Class.forName(prefix + ".core.JsonFactory");
      Object factory = factoryClass.getConstructor().newInstance();
      String factoryVersion = factoryClass.getMethod("version").invoke(factory).toString();
      Class<?> mapperClass = Class.forName(prefix + ".databind.ObjectMapper");
      Object mapper = mapperClass.getConstructor().newInstance();
      String mapperVersion = mapperClass.getMethod("version").invoke(mapper).toString();
      for (String version : new String[] {
          corePackageVersion, databindPackageVersion, factoryVersion, mapperVersion}) {
        if (!version.equals("2.21.7")) {
          throw new AssertionError(prefix + " actual Jackson version " + version + " != 2.21.7");
        }
      }
      Method parse = mapperClass.getMethod("readTree", String.class);
      Method serialize = mapperClass.getMethod("writeValueAsString", Object.class);
      Object tree = parse.invoke(mapper, "{\"marker\":\"PASS\",\"values\":[1,2,3]}");
      Object marker = tree.getClass().getMethod("get", String.class).invoke(tree, "marker");
      Object values = tree.getClass().getMethod("get", String.class).invoke(tree, "values");
      if (!marker.getClass().getMethod("asText").invoke(marker).equals("PASS")
          || !values.getClass().getMethod("size").invoke(values).equals(3)) {
        throw new AssertionError(prefix + " JSON parsing failed");
      }
      String encoded = (String) serialize.invoke(mapper, tree);
      if (!parse.invoke(mapper, encoded).equals(tree)) {
        throw new AssertionError(prefix + " JSON serialization roundtrip failed");
      }
      System.out.println("{\"namespace\":\"" + prefix + "\",\"status\":\"PASS\","
          + "\"core_package_version\":\"" + corePackageVersion + "\","
          + "\"databind_package_version\":\"" + databindPackageVersion + "\","
          + "\"factory_version\":\"" + factoryVersion + "\","
          + "\"mapper_version\":\"" + mapperVersion + "\","
          + "\"json_roundtrip\":\"PASS\"}");
    }
  }
}
