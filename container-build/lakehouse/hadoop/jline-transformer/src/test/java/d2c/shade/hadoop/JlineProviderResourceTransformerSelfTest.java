package d2c.shade.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;

import org.apache.maven.plugins.shade.relocation.Relocator;

/** No test framework or runtime dependency; execute main in the build stage. */
public final class JlineProviderResourceTransformerSelfTest {
    private static final String SHADED = "org.apache.hadoop.shaded.";
    private static final long EARLIER = 1_700_000_000_000L;
    private static final long LATER = EARLIER + 4_000L;
    private static int checks;

    private JlineProviderResourceTransformerSelfTest() { }

    public static void main(String[] args) throws Exception {
        allSixFormatsPreserveEveryOtherByte();
        identicalDuplicatesAndOrderingAreReproducible();
        legacyApiAndStreamOwnership();
        unexpectedInputsFailClosed();
        System.out.println("JlineProviderResourceTransformerSelfTest: PASS; assertions=" + checks);
    }

    private static Map<String, String> providers() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("META-INF/jline/providers/dumb", "org.jline.terminal.impl.DumbTerminalProvider");
        values.put("META-INF/jline/providers/exec", "org.jline.terminal.impl.exec.ExecTerminalProvider");
        values.put("META-INF/jline/providers/ffm", "org.jline.terminal.impl.ffm.FfmTerminalProvider");
        values.put("META-INF/jline/providers/jni", "org.jline.terminal.impl.jni.JniTerminalProvider");
        values.put("META-INF/services/org/jline/terminal/provider/jna", "org.jline.terminal.impl.jna.JnaTerminalProvider");
        values.put("META-INF/services/org/jline/terminal/provider/jansi", "org.jline.terminal.impl.jansi.JansiTerminalProvider");
        return values;
    }

    private static String[] resourceText(String path, String implementation) {
        // The class name also occurs in a comment: global string replacement
        // would violate this contract. Mixed line endings must also survive.
        String before = "# License mentions " + implementation + " unchanged.\r\n"
                + "! UTF-8 comment: \uac80\uc99d\r\n \t\n";
        before += path.startsWith("META-INF/services/") ? "\tclass \t= \t" : "\t ";
        String after = " \t\r\n# trailing comment\n";
        return new String[] {before + implementation + after, before + SHADED + implementation + after};
    }

    private static void allSixFormatsPreserveEveryOtherByte() throws Exception {
        JlineProviderResourceTransformer transformer = new JlineProviderResourceTransformer();
        check(!transformer.hasTransformedResource(), "empty transformer");
        ClassRelocator relocator = new ClassRelocator(SHADED);
        Map<String, byte[]> expected = new LinkedHashMap<>();
        for (Map.Entry<String, String> provider : providers().entrySet()) {
            check(transformer.canTransformResource(provider.getKey()), "claim exact provider path");
            String[] text = resourceText(provider.getKey(), provider.getValue());
            transformer.processResource(provider.getKey(), input(text[0]), Collections.singletonList(relocator), EARLIER);
            expected.put(provider.getKey(), text[1].getBytes(StandardCharsets.UTF_8));
        }
        check(transformer.hasTransformedResource(), "resources accumulated");
        Map<String, OutputEntry> actual = unpack(emit(transformer));
        check(actual.keySet().equals(expected.keySet()), "exact six output paths, no renamed descriptor");
        for (Map.Entry<String, byte[]> item : expected.entrySet()) {
            check(Arrays.equals(item.getValue(), actual.get(item.getKey()).bytes), "only class token changed");
            check(actual.get(item.getKey()).time == EARLIER, "upstream entry timestamp preserved");
        }
        check(relocator.calls.equals(new ArrayList<>(providers().values())), "actual class relocator used once per token");
        check(!transformer.hasTransformedResource(), "emitted resources cleared");
        check(!transformer.canTransformResource("META-INF/services/com.example.Provider"), "ordinary service left alone");
        check(!transformer.canTransformResource("META-INF/jline/providers-extra/dumb"), "unrelated namespace left alone");
        check(!transformer.canTransformResource(null), "null is not a claimed resource");
    }

    private static void identicalDuplicatesAndOrderingAreReproducible() throws Exception {
        List<Map.Entry<String, String>> order = new ArrayList<>(providers().entrySet());
        JlineProviderResourceTransformer first = new JlineProviderResourceTransformer();
        JlineProviderResourceTransformer second = new JlineProviderResourceTransformer();
        for (Map.Entry<String, String> entry : order) {
            String source = resourceText(entry.getKey(), entry.getValue())[0];
            first.processResource(entry.getKey(), input(source), relocating(), EARLIER);
            first.processResource(entry.getKey(), input(source), relocating(), LATER);
        }
        Collections.reverse(order);
        for (Map.Entry<String, String> entry : order) {
            String source = resourceText(entry.getKey(), entry.getValue())[0];
            second.processResource(entry.getKey(), input(source), relocating(), LATER);
            second.processResource(entry.getKey(), input(source), relocating(), EARLIER);
        }
        byte[] firstJar = emit(first);
        check(Arrays.equals(firstJar, emit(second)), "resource and duplicate traversal order do not affect output");
        Map<String, OutputEntry> entries = unpack(firstJar);
        check(entries.size() == 6, "identical duplicates merged once");
        for (OutputEntry entry : entries.values()) {
            check(entry.time == LATER, "duplicate timestamp is deterministic maximum");
        }
    }

    @SuppressWarnings("deprecation")
    private static void legacyApiAndStreamOwnership() throws Exception {
        String path = "META-INF/jline/providers/dumb";
        String source = providers().get(path); // No final newline is a supported original form.
        TrackingInput stream = new TrackingInput(source.getBytes(StandardCharsets.UTF_8));
        JlineProviderResourceTransformer transformer = new JlineProviderResourceTransformer();
        transformer.processResource(path, stream, relocating());
        check(!stream.closed, "input remains owned by Shade");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream jar = new JarOutputStream(bytes)) {
            transformer.modifyOutputStream(jar);
            jar.putNextEntry(new JarEntry("after-transformer"));
            jar.write(1);
            jar.closeEntry();
        }
        Map<String, OutputEntry> entries = unpack(bytes.toByteArray());
        check(entries.containsKey("after-transformer"), "output stream remains open");
        check(entries.get(path).time == 0L, "three-argument API has deterministic epoch timestamp");
        check(Arrays.equals(entries.get(path).bytes, (SHADED + source).getBytes(StandardCharsets.UTF_8)),
                "missing final newline is preserved");
    }

    private static void unexpectedInputsFailClosed() throws Exception {
        String dumb = "META-INF/jline/providers/dumb";
        String implementation = providers().get(dumb);
        String legacy = "META-INF/services/org/jline/terminal/provider/jna";
        String legacyImplementation = providers().get(legacy);
        for (String unknown : Arrays.asList("META-INF/jline/providers/new-provider", "META-INF/jline/providers",
                "META-INF/jline/providers/../dumb", "META-INF/services/org/jline/terminal/provider/new-provider")) {
            check(new JlineProviderResourceTransformer().canTransformResource(unknown), "unknown JLine path claimed for rejection");
            rejects(() -> process(unknown, implementation), "unknown provider resource");
        }
        rejects(() -> process("META-INF/unrelated", implementation), "direct unknown path rejected");
        for (String malformed : Arrays.asList("", "# license only\n! comment\n", "class=" + implementation,
                implementation + "\n" + implementation, implementation + " # inline comment",
                implementation.replace('.', '/'), SHADED + implementation, "\ufeff" + implementation,
                "org.jline.terminal.impl.exec.ExecTerminalProvider", implementation + "\nother.property=value")) {
            rejects(() -> process(dumb, malformed), "malformed or mismatched modern content");
        }
        for (String malformed : Arrays.asList(legacyImplementation, "class:" + legacyImplementation,
                "Class=" + legacyImplementation, "class=", "class=" + legacyImplementation + "\nclass=" + legacyImplementation,
                "class=" + legacyImplementation + "\nextra=value", "class=" + legacyImplementation + " # comment")) {
            rejects(() -> process(legacy, malformed), "malformed legacy properties content");
        }
        rejects(() -> new JlineProviderResourceTransformer().processResource(dumb,
                new ByteArrayInputStream(new byte[] {(byte) 0xc3, (byte) 0x28}), relocating(), EARLIER), "invalid UTF-8 rejected");
        rejects(() -> new JlineProviderResourceTransformer().processResource(dumb, null, relocating(), EARLIER), "missing input rejected");
        rejects(() -> new JlineProviderResourceTransformer().processResource(dumb, input(implementation), null, EARLIER), "no relocators rejected");
        rejects(() -> new JlineProviderResourceTransformer().processResource(dumb, input(implementation), Collections.emptyList(), EARLIER), "empty relocators rejected");
        for (String escaped : Arrays.asList("", "com.example.", SHADED + SHADED, "../")) {
            rejects(() -> new JlineProviderResourceTransformer().processResource(dumb, input(implementation),
                    Collections.singletonList(new ClassRelocator(escaped)), EARLIER), "relocation escape rejected");
        }
        JlineProviderResourceTransformer duplicates = new JlineProviderResourceTransformer();
        duplicates.processResource(dumb, input(implementation), relocating(), EARLIER);
        rejects(() -> duplicates.processResource(dumb, input(implementation + "\n"), relocating(), LATER),
                "different whitespace is a conflicting duplicate");
        rejects(() -> duplicates.processResource(dumb, input("# another license\n" + implementation), relocating(), LATER),
                "different comments are a conflicting duplicate");
        byte[] oversized = new byte[65 * 1024];
        Arrays.fill(oversized, (byte) ' ');
        rejects(() -> new JlineProviderResourceTransformer().processResource(dumb,
                new ByteArrayInputStream(oversized), relocating(), EARLIER), "oversized resource rejected");
    }

    private static void process(String path, String content) throws IOException {
        new JlineProviderResourceTransformer().processResource(path, input(content), relocating(), EARLIER);
    }

    private static ByteArrayInputStream input(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    private static List<Relocator> relocating() {
        return Collections.singletonList(new ClassRelocator(SHADED));
    }

    private static byte[] emit(JlineProviderResourceTransformer transformer) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream jar = new JarOutputStream(bytes)) {
            transformer.modifyOutputStream(jar);
        }
        return bytes.toByteArray();
    }

    private static Map<String, OutputEntry> unpack(byte[] jarBytes) throws IOException {
        Map<String, OutputEntry> entries = new LinkedHashMap<>();
        try (JarInputStream jar = new JarInputStream(new ByteArrayInputStream(jarBytes))) {
            for (JarEntry entry; (entry = jar.getNextJarEntry()) != null;) {
                ByteArrayOutputStream content = new ByteArrayOutputStream();
                byte[] buffer = new byte[4096];
                for (int count; (count = jar.read(buffer)) != -1;) {
                    content.write(buffer, 0, count);
                }
                check(!entries.containsKey(entry.getName()), "no duplicate output ZIP entries");
                entries.put(entry.getName(), new OutputEntry(content.toByteArray(), entry.getTime()));
            }
        }
        return entries;
    }

    private static void rejects(Throwing action, String message) throws Exception {
        try {
            action.run();
        } catch (IOException expected) {
            checks++;
            return;
        }
        throw new AssertionError(message + " did not fail");
    }

    private static void check(boolean condition, String message) {
        checks++;
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private interface Throwing {
        void run() throws Exception;
    }

    private static final class OutputEntry {
        final byte[] bytes;
        final long time;

        OutputEntry(byte[] bytes, long time) {
            this.bytes = bytes;
            this.time = time;
        }
    }

    private static final class TrackingInput extends ByteArrayInputStream {
        boolean closed;

        TrackingInput(byte[] bytes) {
            super(bytes);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class ClassRelocator implements Relocator {
        private final String prefix;
        final List<String> calls = new ArrayList<>();

        ClassRelocator(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public boolean canRelocateClass(String name) {
            return name.startsWith("org.jline.");
        }

        @Override
        public String relocateClass(String name) {
            calls.add(name);
            return prefix + name;
        }

        @Override
        public boolean canRelocatePath(String path) {
            throw new AssertionError("provider filenames must not be relocated");
        }

        @Override
        public String relocatePath(String path) {
            throw new AssertionError("provider filenames must not be relocated");
        }

        @Override
        public String applyToSourceContent(String content) {
            throw new AssertionError("comments and whitespace must not be globally rewritten");
        }
    }
}
