/*
 * Bounded final-image Snappy JNI compatibility/provenance probe.
 * Only synthetic valid data is compressed. No network or service data is used.
 * Java artifact version and the bundled native VERSION resource are separate:
 * Snappy.getNativeLibraryVersion() reads metadata, so successful JNI operations,
 * the actual Linux process mapping and the bundled .so digest are also required.
 */
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.xerial.snappy.Snappy;

public final class VerifySnappyRuntime {
  private static final String VERSION = "1.1.10.9";
  private static final Path FIXED_JAR = Path.of(
      "/opt/spark/jars/org.xerial.snappy_snappy-java-1.1.10.9.jar");
  private static final Path NATIVE_DIRECTORY = Path.of("/tmp/d2c-snappy-native/lib/owned");

  private VerifySnappyRuntime() {}

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalStateException(message);
    }
  }

  private static String sha256(InputStream input) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] buffer = new byte[8192];
    int count;
    while ((count = input.read(buffer)) != -1) {
      digest.update(buffer, 0, count);
    }
    return HexFormat.of().formatHex(digest.digest());
  }

  private static String fileSha256(Path path) throws Exception {
    try (InputStream input = Files.newInputStream(path)) {
      return sha256(input);
    }
  }

  private static Path origin(Class<?> implementation) throws Exception {
    require(implementation.getProtectionDomain().getCodeSource() != null,
        "Missing class code source: " + implementation.getName());
    return Path.of(implementation.getProtectionDomain().getCodeSource().getLocation().toURI())
        .toRealPath();
  }

  private static void rejectNativeOverrides() {
    for (String key : List.of("org.xerial.snappy.use.systemlib",
        "org.xerial.snappy.disable.bundled.libs", "org.xerial.snappy.purejava")) {
      String value = System.getProperty(key);
      require(value == null || value.equals("false"), "Unexpected native override: " + key);
    }
    for (String key : List.of("org.xerial.snappy.lib.path", "org.xerial.snappy.lib.name")) {
      require(System.getProperty(key) == null, "Unexpected native override: " + key);
    }
  }

  private static String[] artifactVersion(JarFile jar) throws Exception {
    String pom = "META-INF/maven/org.xerial.snappy/snappy-java/pom.properties";
    JarEntry properties = jar.getJarEntry(pom);
    if (properties != null) {
      Properties metadata = new Properties();
      try (InputStream input = jar.getInputStream(properties)) {
        metadata.load(input);
      }
      require("org.xerial.snappy".equals(metadata.getProperty("groupId"))
              && "snappy-java".equals(metadata.getProperty("artifactId")),
          "Unexpected Snappy Maven coordinates");
      return new String[] {metadata.getProperty("version"), pom};
    }
    require(jar.getManifest() != null, "Missing Snappy artifact version metadata");
    for (String key : List.of("Implementation-Version", "Bundle-Version")) {
      String value = jar.getManifest().getMainAttributes().getValue(key);
      if (value != null) {
        return new String[] {value, "META-INF/MANIFEST.MF:" + key};
      }
    }
    throw new IllegalStateException("Missing Snappy Java artifact version");
  }

  private static String json(Object value) {
    if (value instanceof String text) {
      StringBuilder result = new StringBuilder("\"");
      for (int index = 0; index < text.length(); index++) {
        char character = text.charAt(index);
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
    if (value instanceof Map<?, ?> map) {
      return "{" + String.join(",", map.entrySet().stream()
          .map(entry -> json(entry.getKey()) + ":" + json(entry.getValue())).toList()) + "}";
    }
    if (value instanceof List<?> list) {
      return "[" + String.join(",", list.stream().map(VerifySnappyRuntime::json).toList()) + "]";
    }
    require(value instanceof Integer || value instanceof Boolean, "Unsupported proof value");
    return value.toString();
  }

  public static void main(String[] arguments) throws Exception {
    require(arguments.length == 4, "Expected: JAVA_MAJOR FIXED_JAR ARCH IMMUTABLE_IMAGE_ID");
    int expectedJava = Integer.parseInt(arguments[0]);
    int actualJava = Runtime.version().feature();
    require(expectedJava == 21 && actualJava == 21, "Final runtime must use Java 21");
    require("Linux".equals(System.getProperty("os.name")), "Native proof requires Linux");
    String machine = System.getProperty("os.arch");
    String arch = switch (machine) {
      case "amd64", "x86_64" -> "amd64";
      case "aarch64", "arm64" -> "arm64";
      default -> throw new IllegalStateException("Unexpected native architecture: " + machine);
    };
    require(arch.equals(arguments[2]), "Native platform mismatch");
    require(arguments[3].matches("sha256:[0-9a-f]{64}"), "Immutable image ID required");
    require(Path.of(arguments[1]).equals(FIXED_JAR) && !Files.isSymbolicLink(FIXED_JAR),
        "Unexpected Snappy JAR path");
    Path expectedJar = FIXED_JAR.toRealPath();
    require(expectedJar.equals(FIXED_JAR) && Files.isRegularFile(expectedJar),
        "Expected the fixed regular Snappy JAR");
    Map<String, String> origins = new LinkedHashMap<>();
    for (Class<?> implementation : List.of(Snappy.class,
        Class.forName("org.xerial.snappy.SnappyNative", false, Snappy.class.getClassLoader()))) {
      Path actualJar = origin(implementation);
      require(expectedJar.equals(actualJar), "Wrong Snappy class origin: " + implementation.getName());
      origins.put(implementation.getName(), actualJar.toString());
    }
    String nativeResource = "org/xerial/snappy/native/Linux/"
        + (arch.equals("amd64") ? "x86_64" : "aarch64") + "/libsnappyjava.so";
    String[] artifact;
    String nativeResourceSha;
    try (JarFile jar = new JarFile(expectedJar.toFile())) {
      artifact = artifactVersion(jar);
      require(VERSION.equals(artifact[0]), "Wrong Java artifact version");
      JarEntry nativeEntry = jar.getJarEntry(nativeResource);
      require(nativeEntry != null && nativeEntry.getSize() > 0
              && nativeEntry.getSize() <= 8 * 1024 * 1024,
          "Missing or unexpectedly large bundled native library");
      try (InputStream input = jar.getInputStream(nativeEntry)) {
        nativeResourceSha = sha256(input);
      }
    }
    rejectNativeOverrides();
    require(System.getProperty("org.xerial.snappy.tempdir") == null,
        "Unexpected Snappy extraction directory override");
    // The parent is the separate 16 MiB exec tmpfs. General /tmp remains noexec.
    // Create only our private child; Docker already created the mount target.
    Files.createDirectory(NATIVE_DIRECTORY,
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    System.setProperty("org.xerial.snappy.tempdir", NATIVE_DIRECTORY.toString());

    byte[] text = "D2C Snappy roundtrip: 한글 • café • 0123456789".getBytes(StandardCharsets.UTF_8);
    byte[] binary = new byte[4096];
    for (int index = 0; index < binary.length; index++) {
      binary[index] = (byte) ((index * 37 + index / 17) & 255);
    }
    for (byte[] input : List.of(text, binary)) {
      byte[] compressed = Snappy.compress(input);
      require(Snappy.isValidCompressedBuffer(compressed), "Invalid synthetic compressed output");
      require(Snappy.uncompressedLength(compressed) == input.length, "Wrong uncompressed length");
      byte[] restored = Snappy.uncompress(compressed);
      require(Arrays.equals(input, restored), "Snappy JNI roundtrip mismatch");
    }
    rejectNativeOverrides();
    require(NATIVE_DIRECTORY.toString().equals(System.getProperty("org.xerial.snappy.tempdir")),
        "Snappy extraction directory changed");
    String nativeVersion = Snappy.getNativeLibraryVersion();
    require(nativeVersion != null && nativeVersion.matches("[0-9]+(?:\\.[0-9]+)+"),
        "Missing native VERSION resource metadata");
    Set<Path> mappedLibraries = new LinkedHashSet<>();
    for (String mapping : Files.readAllLines(Path.of("/proc/self/maps"))) {
      String path = mapping.substring(mapping.lastIndexOf(' ') + 1);
      if (path.endsWith("libsnappyjava.so")) {
        mappedLibraries.add(Path.of(path));
      }
    }
    require(mappedLibraries.size() == 1, "Expected exactly one mapped Snappy JNI library");
    Path library = mappedLibraries.iterator().next();
    require(library.getParent().equals(NATIVE_DIRECTORY) && !Files.isSymbolicLink(library)
            && library.toRealPath().equals(library) && Files.isRegularFile(library),
        "Mapped Snappy library is outside the owned extraction directory");
    String nativeLibrarySha = fileSha256(library);
    require(nativeLibrarySha.equals(nativeResourceSha), "Mapped JNI library differs from fixed JAR resource");

    Map<String, Object> proof = new LinkedHashMap<>();
    proof.put("schema_version", 1);
    proof.put("status", "passed");
    proof.put("actual_java_major", actualJava);
    proof.put("expected_java_major", expectedJava);
    proof.put("java_runtime_version", System.getProperty("java.runtime.version"));
    proof.put("java_vendor", System.getProperty("java.vendor"));
    proof.put("os_name", System.getProperty("os.name"));
    proof.put("actual_arch", arch);
    proof.put("expected_arch", arguments[2]);
    proof.put("image_id", arguments[3]);
    proof.put("expected_jar", FIXED_JAR.toString());
    proof.put("actual_jar", expectedJar.toString());
    proof.put("jar_sha256", fileSha256(expectedJar));
    proof.put("java_artifact_version", artifact[0]);
    proof.put("java_artifact_version_source", artifact[1]);
    proof.put("class_code_sources", origins);
    proof.put("native_backend", "JNI");
    proof.put("native_loaded", true);
    proof.put("native_configuration", Map.of(
        "use_systemlib", System.getProperty("org.xerial.snappy.use.systemlib", "false"),
        "disable_bundled_libs", System.getProperty("org.xerial.snappy.disable.bundled.libs", "false"),
        "purejava", System.getProperty("org.xerial.snappy.purejava", "false"),
        "lib_path", System.getProperty("org.xerial.snappy.lib.path", ""),
        "lib_name", System.getProperty("org.xerial.snappy.lib.name", ""),
        "tempdir", System.getProperty("org.xerial.snappy.tempdir")));
    proof.put("native_library_version", nativeVersion);
    proof.put("native_library_resource", nativeResource);
    proof.put("native_library_path", library.toString());
    proof.put("native_library_sha256", nativeLibrarySha);
    proof.put("native_resource_sha256", nativeResourceSha);
    proof.put("cases", List.of("UTF8", "BINARY"));
    proof.put("roundtrips_verified", 2);
    proof.put("input_bytes", List.of(text.length, binary.length));
    proof.put("uncompressed_lengths_verified", true);
    System.out.println("D2C_SNAPPY_RUNTIME_PROOF=" + json(proof));
  }
}
