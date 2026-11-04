/* Verify complete Guava class semantics in memory; never write a runtime JAR.
 * The pinned builder JDK supplies ASM solely as this build verification tool.
 * Its packages are exported explicitly by the invoking verification command.
 */
import java.io.IOException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.ZipFile;
import jdk.internal.org.objectweb.asm.ClassReader;
import jdk.internal.org.objectweb.asm.ClassWriter;
import jdk.internal.org.objectweb.asm.commons.ClassRemapper;
import jdk.internal.org.objectweb.asm.commons.Remapper;

class VerifyIcebergGuavaClosure {
  private static final String COMMON = "com/google/common/";
  private static final String RELOCATED = "org/apache/iceberg/relocated/com/google/common/";
  private static final String THIRD_PARTY = "com/google/thirdparty/";

  private record Mapping(String original, String relocated) {}

  private static boolean sourceClass(String name) {
    return name.endsWith(".class") && (name.startsWith(COMMON) || name.startsWith(THIRD_PARTY));
  }

  private static boolean bundledClass(String name) {
    return name.endsWith(".class") && (name.startsWith(RELOCATED) || name.startsWith(THIRD_PARTY));
  }

  private static String normalize(String name, List<Mapping> mappings) {
    String result = name;
    for (Mapping mapping : mappings) {
      result = result.replace(mapping.relocated(), mapping.original());
      result = result.replace(mapping.relocated().replace('/', '.'), mapping.original().replace('/', '.'));
    }
    return result;
  }

  private static byte[] canonicalClass(byte[] content, List<Mapping> mappings) {
    ClassReader reader = new ClassReader(content);
    // Recreate the pool from the visited class so equivalent relocation-induced
    // pool indexes, ldc widths and code offsets have one deterministic encoding.
    ClassWriter writer = new ClassWriter(0);
    Remapper remapper = new Remapper() {
      @Override public String map(String internalName) {
        return normalize(internalName, mappings);
      }
      @Override public Object mapValue(Object value) {
        return value instanceof String ? normalize((String) value, mappings) : super.mapValue(value);
      }
    };
    reader.accept(new ClassRemapper(writer, remapper), 0);
    return writer.toByteArray();
  }

  private static Map<String, byte[]> sourceClasses(Path path) throws IOException {
    Map<String, byte[]> entries = new TreeMap<>();
    try (ZipFile jar = new ZipFile(path.toFile())) {
      var iterator = jar.entries();
      while (iterator.hasMoreElements()) {
        var entry = iterator.nextElement();
        if (sourceClass(entry.getName())) {
          byte[] content;
          try (var stream = jar.getInputStream(entry)) {
            content = stream.readAllBytes();
          }
          if (entries.put(entry.getName(), content) != null) {
            throw new IllegalStateException("Duplicate original Guava class: " + entry.getName());
          }
        }
      }
    }
    return entries;
  }

  private static byte[] content(ZipFile jar, String name) throws IOException {
    var entry = jar.getEntry(name);
    if (entry == null) throw new IllegalStateException("Missing Guava class: " + name);
    try (var stream = jar.getInputStream(entry)) {
      return stream.readAllBytes();
    }
  }

  public static void main(String[] arguments) throws Exception {
    if (arguments.length < 5) throw new IllegalArgumentException("Expected fixed Guava/failureaccess, bundle, runtime and original shade mappings");
    List<Mapping> mappings = new ArrayList<>();
    for (int index = 4; index < arguments.length; index++) {
      String[] pair = arguments[index].split("=", -1);
      if (pair.length != 2 || pair[0].isEmpty() || pair[1].isEmpty()) {
        throw new IllegalArgumentException("Invalid original shade mapping");
      }
      mappings.add(new Mapping(pair[0].replace('.', '/'), pair[1].replace('.', '/')));
    }
    mappings.sort(Comparator.comparingInt((Mapping item) -> item.relocated().length()).reversed());
    Map<String, byte[]> originals = sourceClasses(Path.of(arguments[0]));
    for (var entry : sourceClasses(Path.of(arguments[1])).entrySet()) {
      if (originals.put(entry.getKey(), entry.getValue()) != null) {
        throw new IllegalStateException("Duplicate Guava/failureaccess original class: " + entry.getKey());
      }
    }
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    int checked = 0;
    try (ZipFile bundle = new ZipFile(arguments[2]); ZipFile runtime = new ZipFile(arguments[3])) {
      List<String> names = bundle.stream().map(entry -> entry.getName()).filter(VerifyIcebergGuavaClosure::bundledClass).sorted().toList();
      for (String name : names) {
        String original = name.startsWith(RELOCATED) ? COMMON + name.substring(RELOCATED.length()) : name;
        byte[] fixed = originals.get(original);
        if (fixed == null) throw new IllegalStateException("No actual fixed input for retained class: " + name);
        byte[] expected = canonicalClass(fixed, mappings);
        if (!Arrays.equals(expected, canonicalClass(content(bundle, name), mappings))) {
          throw new IllegalStateException("Complete source-bundled Guava class differs from fixed input: " + name);
        }
        if (!Arrays.equals(expected, canonicalClass(content(runtime, name), mappings))) {
          throw new IllegalStateException("Complete final Guava class differs from fixed input: " + name);
        }
        digest.update(original.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        digest.update((byte) 0);
        digest.update(expected);
        checked++;
      }
    }
    if (checked == 0) throw new IllegalStateException("No retained actual Guava classes were verified");
    System.out.println("{\"status\":\"PASS\",\"class_count\":" + checked
        + ",\"complete_canonical_class_sha256\":\"" + HexFormat.of().formatHex(digest.digest())
        + "\",\"jdk_version\":\"" + Runtime.version() + "\"}");
  }
}
