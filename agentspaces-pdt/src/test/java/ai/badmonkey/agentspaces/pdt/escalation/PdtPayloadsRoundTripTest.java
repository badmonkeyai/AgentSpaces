/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.pdt.escalation;

import ai.badmonkey.agentspaces.common.codec.CanonicalMaps;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.values.PayloadTree;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PD-E-02 and PD-E-03 (srad/agentspaces-pdt.md §3.7): a payload spilled into a tree
 * by {@link PayloadTree}, the engine of {@link PdtPayloads}, comes back byte for
 * byte, over 10,000 generated CBOR payloads (maps, arrays, byte strings, records in
 * declared field order, nested, 1 B to 2 MiB) and over every record type the
 * examples declare, each encoded with {@code CborCodec} as a space encodes an entry.
 */
class PdtPayloadsRoundTripTest {

    private static final int CASES = 10_000;
    private static final int LARGE_CASES = 100;
    private static final int SMALL_CAP = 64 * 1024;
    private static final int LARGE_CAP = 2 * 1024 * 1024;

    private final CborCodec codec = CborCodec.defaultCodec();

    /** A record as the examples declare them: fields in declared order, a large collection among them. */
    public record Doc(String id, Map<String, String> fields, List<String> tags, byte[] blob, int revision) {
    }

    public record Nested(String name, Doc doc, List<Doc> history, double score) {
    }

    @Test
    void tenThousandGeneratedPayloadsReassembleByteIdentical() {
        Random random = new Random(42);
        NodeStore store = NodeStore.inMemory();
        long bytes = 0;
        int spilled = 0;
        int[] kinds = new int[4];
        for (int i = 0; i < CASES; i++) {
            int cap = i < LARGE_CASES ? LARGE_CAP : SMALL_CAP;
            int target = (int) Math.exp(random.nextDouble() * Math.log(cap));
            Object value = generate(random, Math.max(1, target), 0);
            byte[] payload = codec.toBytes(value);
            PayloadTree.Written written = PayloadTree.write(payload, store);
            assertThat(Cids.parse(written.root()).codec()).as("case " + i + ": a dag-cbor root").isEqualTo(Cids.DAG_CBOR);
            byte[] back = PayloadTree.read(written.root(), store, ai.badmonkey.agentspaces.pdt.values.NodeLoader.of(store));
            assertThat(Arrays.equals(back, payload)).as("case " + i + ": " + payload.length + " bytes of "
                    + value.getClass().getSimpleName() + " reassemble byte for byte").isTrue();
            assertThat(store.complete(written.root())).as("case " + i + ": the tree is complete").isTrue();
            bytes += payload.length;
            String kind = NodeCodec.kindOf(store.get(written.root()).orElseThrow());
            kinds[switch (kind) {
                case Kind.X -> 0;
                case Kind.M -> 1;
                case Kind.VR -> 2;
                default -> 3;
            }]++;
            if (!Kind.X.equals(kind) || written.nodes().size() > 1) {
                spilled++;
            }
        }
        System.out.printf("round trip: %d payloads, %d bytes, %d spilled into trees; roots x=%d m=%d vr=%d b=%d; store %d nodes%n",
                CASES, bytes, spilled, kinds[0], kinds[1], kinds[2], kinds[3], store.size());
        assertThat(spilled).as("the generator reached the spill rules").isGreaterThan(1_000);
        assertThat(kinds[1]).as("map roots").isGreaterThan(0);
        assertThat(kinds[2]).as("array roots").isGreaterThan(0);
        assertThat(kinds[3]).as("blob roots").isGreaterThan(0);
    }

    @Test
    void recordsInDeclaredFieldOrderReassembleByteIdenticalAndShareTheirLargeFields() {
        Random random = new Random(7);
        NodeStore store = NodeStore.inMemory();
        Map<String, String> fields = new TreeMap<>();
        for (int i = 0; i < 3_000; i++) {
            fields.put("field-" + i, text(random, 80));
        }
        List<String> tags = List.of("a", "b", "c");
        byte[] blob = new byte[300_000];
        random.nextBytes(blob);
        Doc v1 = new Doc("doc-1", fields, tags, blob, 1);
        byte[] p1 = codec.toBytes(v1);
        assertThat(p1[0] & 0xff).as("a record is an indefinite map").isEqualTo(0xbf);
        PayloadTree.Written w1 = PayloadTree.write(p1, store);
        assertThat(PayloadTree.read(w1.root(), store, ai.badmonkey.agentspaces.pdt.values.NodeLoader.of(store))).isEqualTo(p1);
        assertThat(NodeCodec.kindOf(store.get(w1.root()).orElseThrow())).as("the declared order rides an x envelope").isEqualTo(Kind.X);

        Map<String, String> changed = new TreeMap<>(fields);
        changed.put("field-1500", "changed");
        Doc v2 = new Doc("doc-1", changed, tags, blob, 2);
        byte[] p2 = codec.toBytes(v2);
        PayloadTree.Written w2 = PayloadTree.write(p2, store);
        assertThat(PayloadTree.read(w2.root(), store, ai.badmonkey.agentspaces.pdt.values.NodeLoader.of(store))).isEqualTo(p2);
        long delta = w2.bytes();
        System.out.printf("record: v1 %d bytes in %d nodes; one field changed: delta %d bytes in %d nodes%n",
                w1.bytes(), w1.nodes().size(), delta, w2.nodes().size());
        assertThat(delta).as("a one-field change in a 300 KiB record ships the path, under 16 KiB").isLessThan(16 * 1024);

        Nested nested = new Nested("n", v1, List.of(v1, v2), 0.5);
        byte[] p3 = codec.toBytes(nested);
        PayloadTree.Written w3 = PayloadTree.write(p3, store);
        assertThat(PayloadTree.read(w3.root(), store, ai.badmonkey.agentspaces.pdt.values.NodeLoader.of(store))).isEqualTo(p3);
        assertThat(w3.bytes()).as("the nested docs are shared with the versions already stored").isLessThan(p3.length / 4);
    }

    @Test
    void everyExampleRecordTypeRoundTrips() throws IOException {
        Path examples = Path.of("..", "examples").toAbsolutePath().normalize();
        assertThat(Files.isDirectory(examples)).as(examples.toString()).isTrue();
        Map<String, List<String[]>> records = new TreeMap<>();
        try (Stream<Path> files = Files.walk(examples)) {
            for (Path file : files.filter(p -> p.toString().endsWith(".java") && p.toString().contains("/src/main/java/")).toList()) {
                records.putAll(declaredRecords(Files.readString(file)));
            }
        }
        assertThat(records.size()).as("record types found under examples/*/src/main/java").isGreaterThan(40);
        Random random = new Random(11);
        NodeStore store = NodeStore.inMemory();
        int cases = 0;
        for (Map.Entry<String, List<String[]>> record : records.entrySet()) {
            for (int size : new int[] {64, 4_096, 100_000}) {
                Object value = instance(record.getValue(), records, random, size, 0);
                byte[] payload = codec.toBytes(value);
                PayloadTree.Written written = PayloadTree.write(payload, store);
                byte[] back = PayloadTree.read(written.root(), store, ai.badmonkey.agentspaces.pdt.values.NodeLoader.of(store));
                assertThat(Arrays.equals(back, payload)).as(record.getKey() + " at " + payload.length + " bytes").isTrue();
                cases++;
            }
        }
        System.out.printf("examples: %d record types, %d round trips%n", records.size(), cases);
    }

    // ------------------------------------------------------------ generators

    /** A value whose CBOR is about {@code budget} bytes: scalars, byte strings, lists, sorted maps, and ordered maps (records). */
    private Object generate(Random random, int budget, int depth) {
        int choice = random.nextInt(depth > 3 || budget < 16 ? 5 : 9);
        switch (choice) {
            case 0:
                return text(random, Math.max(0, budget - 3));
            case 1:
                return random.nextBoolean() ? (Object) random.nextLong() : (Object) random.nextInt(1 << 20);
            case 2:
                return random.nextBoolean() ? (Object) random.nextDouble() : (Object) random.nextBoolean();
            case 3: {
                byte[] bytes = new byte[Math.max(0, budget - 5)];
                random.nextBytes(bytes);
                return bytes;
            }
            case 4:
                return random.nextInt(4) == 0 ? List.of() : Map.of();
            case 5:
            case 6: {
                int n = 1 + random.nextInt(Math.min(64, Math.max(1, budget / 8)));
                List<Object> list = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    list.add(generate(random, Math.max(1, budget / n), depth + 1));
                }
                return list;
            }
            case 7: {
                int n = 1 + random.nextInt(Math.min(256, Math.max(1, budget / 12)));
                Map<String, Object> map = new LinkedHashMap<>();
                for (int i = 0; i < n; i++) {
                    map.put("k" + random.nextInt(1_000_000), generate(random, Math.max(1, budget / n - 8), depth + 1));
                }
                return map;
            }
            default: {
                // a record: an insertion-ordered map with keys the codec writes as given
                int n = 1 + random.nextInt(Math.min(24, Math.max(1, budget / 12)));
                CanonicalMaps.Verbatim ordered = new CanonicalMaps.Verbatim();
                for (int i = 0; i < n; i++) {
                    ordered.put(text(random, 3 + random.nextInt(9)), generate(random, Math.max(1, budget / n - 8), depth + 1));
                }
                return ordered;
            }
        }
    }

    private static String text(Random random, int length) {
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = (char) ('a' + random.nextInt(26));
        }
        return new String(chars);
    }

    private static final Pattern RECORD = Pattern.compile("record\\s+([A-Z][A-Za-z0-9]*)\\s*\\(([^)]*)\\)");

    /** The records a source file declares: name to its components as {type, name} pairs. */
    private static Map<String, List<String[]>> declaredRecords(String source) {
        Map<String, List<String[]>> out = new LinkedHashMap<>();
        Matcher m = RECORD.matcher(source);
        while (m.find()) {
            List<String[]> components = new ArrayList<>();
            String body = m.group(2).replaceAll("@\\w+(\\([^)]*\\))?", "").trim();
            if (!body.isEmpty()) {
                for (String component : splitTopLevel(body)) {
                    String trimmed = component.trim().replaceAll("\\s+", " ");
                    int space = trimmed.lastIndexOf(' ');
                    if (space > 0) {
                        components.add(new String[] {trimmed.substring(0, space).trim(), trimmed.substring(space + 1).trim()});
                    }
                }
            }
            out.put(m.group(1), components);
        }
        return out;
    }

    private static List<String> splitTopLevel(String body) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '<') {
                depth++;
            } else if (c == '>') {
                depth--;
            } else if (c == ',' && depth == 0) {
                parts.add(body.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(body.substring(start));
        return parts;
    }

    /** An instance of a declared record as the codec would write it: fields in declared order, values by type. */
    private Object instance(List<String[]> components, Map<String, List<String[]>> records, Random random, int size, int depth) {
        CanonicalMaps.Verbatim value = new CanonicalMaps.Verbatim();
        int per = Math.max(1, size / Math.max(1, components.size()));
        for (String[] component : components) {
            value.put(component[1], valueOf(component[0], records, random, per, depth));
        }
        return value;
    }

    private Object valueOf(String type, Map<String, List<String[]>> records, Random random, int size, int depth) {
        String t = type.trim();
        if (t.startsWith("List<") || t.startsWith("Set<") || t.startsWith("Collection<") || t.endsWith("[]") && !t.equals("byte[]")) {
            String inner = t.endsWith("[]") ? t.substring(0, t.length() - 2) : t.substring(t.indexOf('<') + 1, t.lastIndexOf('>'));
            int n = Math.max(1, Math.min(200, size / 16));
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                list.add(valueOf(inner, records, random, Math.max(1, size / n), depth + 1));
            }
            return list;
        }
        if (t.startsWith("Map<")) {
            String[] kv = splitTopLevel(t.substring(4, t.lastIndexOf('>'))).toArray(new String[0]);
            int n = Math.max(1, Math.min(200, size / 16));
            Map<String, Object> map = new LinkedHashMap<>();
            for (int i = 0; i < n; i++) {
                map.put("key-" + i, valueOf(kv.length > 1 ? kv[1] : "String", records, random, Math.max(1, size / n), depth + 1));
            }
            return map;
        }
        switch (t) {
            case "String":
                return text(random, Math.max(1, size));
            case "int": case "Integer": case "short": case "long": case "Long":
                return random.nextInt(1 << 20);
            case "double": case "Double": case "float": case "Float":
                return random.nextDouble();
            case "boolean": case "Boolean":
                return random.nextBoolean();
            case "byte[]": {
                byte[] bytes = new byte[Math.max(1, size)];
                random.nextBytes(bytes);
                return bytes;
            }
            case "Instant":
                return "2026-11-08T00:00:00Z";
            case "Duration":
                return "PT15M";
            default:
                if (records.containsKey(t) && depth < 2) {
                    return instance(records.get(t), records, random, size, depth + 1);
                }
                return text(random, Math.max(1, Math.min(size, 32)));
        }
    }
}
