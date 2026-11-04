package d2c.shade.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.maven.plugins.shade.relocation.Relocator;
import org.apache.maven.plugins.shade.resource.ReproducibleResourceTransformer;

/**
 * Relocate only the implementation token in the six JLine 3.30.17 provider files.
 *
 * <p>These are JLine's own resource formats, including two slash-named legacy
 * properties files. They are not ordinary Java ServiceLoader descriptors. Put
 * this transformer before Shade's general ServicesResourceTransformer. Unknown
 * entries in either JLine provider namespace fail the build for review.
 *
 * <p>No resource name, comment, license, whitespace, or line ending is rewritten.
 * Maven's actual class relocator computes the token and must produce Hadoop's
 * expected shaded class name. This class is a build plugin, not runtime code.
 */
public final class JlineProviderResourceTransformer implements ReproducibleResourceTransformer {
    private static final String MODERN_ROOT = "META-INF/jline/providers";
    private static final String LEGACY_ROOT = "META-INF/services/org/jline/terminal/provider";
    private static final String SHADED_PREFIX = "org.apache.hadoop.shaded.";
    private static final int MAX_RESOURCE_BYTES = 64 * 1024;
    private static final String CLASS_NAME = "[A-Za-z_$][A-Za-z0-9_$]*(?:\\.[A-Za-z_$][A-Za-z0-9_$]*)+";
    private static final Pattern MODERN_LINE = Pattern.compile("([ \\t]*)(" + CLASS_NAME + ")([ \\t]*)");
    private static final Pattern LEGACY_LINE = Pattern.compile("([ \\t]*class[ \\t]*=[ \\t]*)(" + CLASS_NAME + ")([ \\t]*)");
    private static final Map<String, String> PROVIDERS;

    static {
        Map<String, String> providers = new LinkedHashMap<>();
        providers.put(MODERN_ROOT + "/dumb", "org.jline.terminal.impl.DumbTerminalProvider");
        providers.put(MODERN_ROOT + "/exec", "org.jline.terminal.impl.exec.ExecTerminalProvider");
        providers.put(MODERN_ROOT + "/ffm", "org.jline.terminal.impl.ffm.FfmTerminalProvider");
        providers.put(MODERN_ROOT + "/jni", "org.jline.terminal.impl.jni.JniTerminalProvider");
        providers.put(LEGACY_ROOT + "/jna", "org.jline.terminal.impl.jna.JnaTerminalProvider");
        providers.put(LEGACY_ROOT + "/jansi", "org.jline.terminal.impl.jansi.JansiTerminalProvider");
        PROVIDERS = Collections.unmodifiableMap(providers);
    }

    private final Map<String, ResourceContent> resources = new TreeMap<>();

    @Override
    public boolean canTransformResource(String resource) {
        return inNamespace(resource, MODERN_ROOT) || inNamespace(resource, LEGACY_ROOT);
    }

    private static boolean inNamespace(String resource, String root) {
        return resource != null && (resource.equals(root) || resource.startsWith(root + "/"));
    }

    /** The legacy Shade API supplies no timestamp; use a deterministic epoch. */
    @Override
    @SuppressWarnings("deprecation")
    public void processResource(String resource, InputStream input, List<Relocator> relocators) throws IOException {
        processResource(resource, input, relocators, 0L);
    }

    @Override
    public void processResource(String resource, InputStream input, List<Relocator> relocators, long time)
            throws IOException {
        String expectedClass = PROVIDERS.get(resource);
        if (expectedClass == null) {
            throw new IOException("Unknown JLine provider resource: " + resource);
        }
        if (input == null) {
            throw new IOException("Missing input for JLine provider resource: " + resource);
        }
        byte[] source = readResource(input, resource);
        String text = decode(source, resource);
        Token token = parseToken(text, resource);
        if (!expectedClass.equals(token.value)) {
            throw new IOException("Unexpected provider implementation in " + resource);
        }
        String relocated = relocate(expectedClass, relocators, resource);
        byte[] transformed = (text.substring(0, token.start) + relocated + text.substring(token.end))
                .getBytes(StandardCharsets.UTF_8);
        long reproducibleTime = time < 0 ? 0L : time;
        ResourceContent previous = resources.get(resource);
        if (previous != null) {
            if (!Arrays.equals(previous.source, source) || !Arrays.equals(previous.transformed, transformed)) {
                throw new IOException("Conflicting duplicate JLine provider resource: " + resource);
            }
            // Identical content from multiple dependencies has one deterministic
            // timestamp, independent of dependency traversal order.
            previous.time = Math.max(previous.time, reproducibleTime);
        } else {
            resources.put(resource, new ResourceContent(source, transformed, reproducibleTime));
        }
    }

    private static byte[] readResource(InputStream input, String resource) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        for (int count; (count = input.read(buffer)) != -1;) {
            if (count == 0) {
                continue;
            }
            if (count > MAX_RESOURCE_BYTES - bytes.size()) {
                throw new IOException("Oversized JLine provider resource: " + resource);
            }
            bytes.write(buffer, 0, count);
        }
        // Shade owns the input stream. Do not close it here.
        return bytes.toByteArray();
    }

    private static String decode(byte[] source, String resource) throws IOException {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(source)).toString();
        } catch (CharacterCodingException exc) {
            throw new IOException("Invalid UTF-8 JLine provider resource: " + resource, exc);
        }
    }

    private static Token parseToken(String text, String resource) throws IOException {
        Pattern syntax = resource.startsWith(MODERN_ROOT + "/") ? MODERN_LINE : LEGACY_LINE;
        Token found = null;
        int offset = 0;
        while (offset < text.length()) {
            int end = offset;
            while (end < text.length() && text.charAt(end) != '\r' && text.charAt(end) != '\n') {
                end++;
            }
            String line = text.substring(offset, end);
            int first = 0;
            while (first < line.length() && (line.charAt(first) == ' ' || line.charAt(first) == '\t')) {
                first++;
            }
            boolean commentOrBlank = first == line.length() || line.charAt(first) == '#' || line.charAt(first) == '!';
            if (!commentOrBlank) {
                if (found != null) {
                    throw new IOException("Multiple provider lines in " + resource);
                }
                Matcher matcher = syntax.matcher(line);
                if (!matcher.matches()) {
                    throw new IOException("Malformed JLine provider line in " + resource);
                }
                found = new Token(offset + matcher.start(2), offset + matcher.end(2), matcher.group(2));
            }
            offset = end;
            if (offset < text.length() && text.charAt(offset) == '\r') {
                offset++;
            }
            if (offset < text.length() && text.charAt(offset) == '\n') {
                offset++;
            }
        }
        if (found == null) {
            throw new IOException("Missing provider implementation in " + resource);
        }
        return found;
    }

    private static String relocate(String original, List<Relocator> relocators, String resource) throws IOException {
        if (relocators != null) {
            for (Relocator relocator : relocators) {
                if (relocator != null && relocator.canRelocateClass(original)) {
                    String relocated = relocator.relocateClass(original);
                    if (!(SHADED_PREFIX + original).equals(relocated)) {
                        throw new IOException("JLine provider relocation escaped the expected Hadoop namespace: " + resource);
                    }
                    return relocated;
                }
            }
        }
        throw new IOException("No Maven class relocator matched JLine provider resource: " + resource);
    }

    @Override
    public boolean hasTransformedResource() {
        return !resources.isEmpty();
    }

    @Override
    public void modifyOutputStream(JarOutputStream output) throws IOException {
        for (Map.Entry<String, ResourceContent> item : resources.entrySet()) {
            JarEntry entry = new JarEntry(item.getKey());
            entry.setTime(item.getValue().time);
            output.putNextEntry(entry);
            output.write(item.getValue().transformed);
            output.closeEntry();
        }
        resources.clear();
        // Shade owns the output stream. Do not close it here.
    }

    private static final class Token {
        final int start;
        final int end;
        final String value;

        Token(int start, int end, String value) {
            this.start = start;
            this.end = end;
            this.value = value;
        }
    }

    private static final class ResourceContent {
        final byte[] source;
        final byte[] transformed;
        long time;

        ResourceContent(byte[] source, byte[] transformed, long time) {
            this.source = source;
            this.transformed = transformed;
            this.time = time;
        }
    }
}
