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
package ai.badmonkey.agentspaces.examples.pdt;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The Python client against a Java fleet over tcp (srad/agentspaces-pdt.md PD-Q-INT-01,
 * PD-Q-INT-03, PD-C-04, PD-E-06): a Java writer and a Python writer swap one ref at the
 * same time and converge on one root; a Java writer escalates a large entry of an
 * opted-in space to a tree and the Python member reads it through its facade; and
 * 1,000 generated maps and ten files have the same roots in both languages. The
 * interpreter is {@code $PYTHON} or {@code /tmp/aspace-venv4/bin/python}; without one the
 * tests are skipped by an assumption, so the surefire count shows it.
 */
class PythonInteropTest {

    private static final Pattern ROOT_LIST = Pattern.compile("\"(maps|files)\":\\[([^\\]]*)\\]");

    /** A record as the examples declare them: a large map field among small ones. */
    public record Report(String id, Map<String, String> fields, int revision) {
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    private static boolean await(Duration limit, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + limit.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(100);
        }
        return condition.getAsBoolean();
    }

    private static Map<String, String> fields(int n, long seed) {
        Random random = new Random(seed);
        Map<String, String> fields = new TreeMap<>();
        for (int i = 0; i < n; i++) {
            char[] chars = new char[80];
            for (int c = 0; c < chars.length; c++) {
                chars[c] = (char) ('a' + random.nextInt(26));
            }
            fields.put("field-" + i, new String(chars));
        }
        return fields;
    }

    private static List<String> roots(String answer, String which) {
        Matcher matcher = ROOT_LIST.matcher(answer);
        while (matcher.find()) {
            if (matcher.group(1).equals(which)) {
                List<String> out = new ArrayList<>();
                for (String item : matcher.group(2).split(",")) {
                    if (!item.isBlank()) {
                        out.add(item.replace("\"", "").trim());
                    }
                }
                return out;
            }
        }
        return List.of();
    }

    @Test
    @Timeout(300)
    void aJavaWriterAndThePythonWriterSwapOneRefAtTheSameTimeAndConverge(@TempDir Path dir) throws Exception {
        assumeTrue(PythonReader.available(), "the Python reader is unavailable: " + PythonReader.whyUnavailable());
        int port = freePort();
        try (PersistentValuesFleet.Peer curator = PersistentValuesFleet.start("curator", port, List.of(), dir.resolve("curator"));
             PythonReader python = new PythonReader("127.0.0.1", port)) {
            python.start(PersistentValuesFleet.FOUNDING, "facts", PersistentValuesFleet.REF_SPACE, "content");
            assertThat(await(Duration.ofSeconds(30), () -> curator.runtime().membership().allMembers().size() == 1)).isTrue();

            // PD-Q-INT-03: ten swaps on each side, interleaved, on the facts ref
            Thread javaWriter = new Thread(() -> {
                for (int i = 0; i < 10; i++) {
                    final int n = i;
                    curator.facts().swap(m -> m.assoc("java-" + n, "j" + n));
                }
            }, "java-writer");
            javaWriter.start();
            for (int i = 0; i < 10; i++) {
                python.swap("py-" + i, "p" + i);
            }
            javaWriter.join();

            // both sides converge on one root (PD-Q-INT-03): the same deref on either side
            String[] last = {null, null};
            assertThat(await(Duration.ofSeconds(120), () -> {
                try {
                    last[1] = python.derefRoot();
                    last[0] = curator.facts().deref().root();
                    return last[0].equals(last[1]);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            })).as("java root " + last[0] + " python root " + last[1] + " java " + curator.pdt().stats()).isTrue();
            PMap<String, String> converged = curator.facts().deref();
            List<String> missing = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                if (converged.get("java-" + i).isEmpty()) {
                    missing.add("java-" + i);
                }
                if (converged.get("py-" + i).isEmpty()) {
                    missing.add("py-" + i);
                }
            }
            System.out.println("PD-Q-INT-03: both sides at " + last[0] + " with " + converged.count() + " of 20 keys"
                    + (missing.isEmpty() ? "" : ", missing " + missing) + "; java " + curator.pdt().stats());
            assertThat(missing).as("every key of both writers survives the merge").isEmpty();
            System.out.println("PD-Q-INT-03: both sides at " + last[0] + " with " + converged.count() + " keys; java "
                    + curator.pdt().stats());
        }
    }

    @Test
    @Timeout(300)
    void aJavaWriterEscalatesToATreeAndThePythonMemberReadsItThroughItsFacade(@TempDir Path dir) throws Exception {
        assumeTrue(PythonReader.available(), "the Python reader is unavailable: " + PythonReader.whyUnavailable());
        int port = freePort();
        try (PersistentValuesFleet.Peer curator = PersistentValuesFleet.start("curator", port, List.of(), dir.resolve("curator"));
             PythonReader python = new PythonReader("127.0.0.1", port)) {
            python.start(PersistentValuesFleet.FOUNDING, "facts", PersistentValuesFleet.REF_SPACE, "content");
            PeerId pythonId = PeerId.of(python.peerId());
            // PD-E-06: the writer takes the tree seam only once the Python member advertises the trees
            assertThat(await(Duration.ofSeconds(60), () -> curator.content().advertisesTrees(pythonId)))
                    .as("the Python member advertises trees=1").isTrue();

            Report report = new Report("r-1", fields(3_000, 1), 1);
            int payloadBytes = CborCodec.defaultCodec().toBytes(report).length;
            assertThat(payloadBytes).isGreaterThan((int) PersistentValuesFleet.ESCALATE_ABOVE);
            curator.plans().write(report, Lease.of(Duration.ofHours(1)));
            String schema = curator.plans().schemaNameOf(Report.class);
            String ref = curator.plans().signedState(curator.plans().readAllEntries(
                    ai.badmonkey.agentspaces.api.space.Template.of(Report.class), 4).get(0).entryId()).orElseThrow()
                    .record().payloadRef();
            assertThat(Cids.parse(ref).codec()).as("the Java writer escalated to a tree").isEqualTo(Cids.DAG_CBOR);

            String[] answer = {""};
            assertThat(await(Duration.ofSeconds(120), () -> {
                try {
                    answer[0] = python.read(PersistentValuesFleet.PLANS_SPACE, schema);
                    return answer[0].contains("\"id\":\"r-1\"") && answer[0].contains("\"bytes\":" + payloadBytes);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            })).as("python read: " + answer[0]).isTrue();
            assertThat(answer[0]).contains("\"payloadRef\":\"" + ref + "\"").contains("\"revision\":1")
                    .contains("\"fields\":{\"entries\":3000}");
            assertThat(answer[0]).as("the facade reassembled it").containsPattern("\"payloadsRead\":[1-9]");
            System.out.println("PD-E-01 Python reader: " + answer[0].substring(0, Math.min(300, answer[0].length())));
        }
    }

    @Test
    @Timeout(300)
    void aThousandGeneratedMapsAndTenFilesHaveTheSameRootsInJavaAndPython(@TempDir Path dir) throws Exception {
        assumeTrue(PythonReader.available(), "the Python reader is unavailable: " + PythonReader.whyUnavailable());
        Random random = new Random(33);
        List<Map<String, Object>> maps = new ArrayList<>();
        NodeStore store = NodeStore.inMemory();
        List<String> javaMapRoots = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Map<String, Object> entries = new LinkedHashMap<>();
            int keys = 1 + random.nextInt(60);
            for (int k = 0; k < keys; k++) {
                String key = "k" + random.nextInt(10_000);
                Object value = switch (random.nextInt(5)) {
                    case 0 -> random.nextInt(1_000_000);
                    case 1 -> "v" + random.nextLong();
                    case 2 -> random.nextBoolean();
                    case 3 -> List.of(random.nextInt(100), random.nextInt(100), random.nextInt(100));
                    default -> {
                        byte[] raw = new byte[1 + random.nextInt(40)];
                        random.nextBytes(raw);
                        yield raw;
                    }
                };
                entries.put(key, value);
            }
            maps.add(entries);
            PMap<String, Object> value = PMap.empty(store, Codec.of(String.class), Codec.of(Object.class));
            for (Map.Entry<String, Object> e : entries.entrySet()) {
                value = value.assoc(e.getKey(), e.getValue());
            }
            javaMapRoots.add(value.root());
        }
        List<byte[]> files = new ArrayList<>();
        List<String> javaFileRoots = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            byte[] data = new byte[1024 + random.nextInt(1_000_000)];
            random.nextBytes(data);
            files.add(data);
            javaFileRoots.add(PBlob.of(data, store).root());
        }
        Path spec = dir.resolve("roots.cbor");
        Files.write(spec, CborCodec.defaultCodec().toBytes(Map.of("maps", maps, "files", files)));

        int port = freePort();
        try (PersistentValuesFleet.Peer curator = PersistentValuesFleet.start("curator", port, List.of(), dir.resolve("curator"));
             PythonReader python = new PythonReader("127.0.0.1", port)) {
            python.start(PersistentValuesFleet.FOUNDING, "facts", PersistentValuesFleet.REF_SPACE, "content");
            String answer = python.roots(spec);
            List<String> pythonMapRoots = roots(answer, "maps");
            List<String> pythonFileRoots = roots(answer, "files");
            assertThat(pythonMapRoots).as("PD-Q-INT-01: 1,000 map roots").hasSize(1000).isEqualTo(javaMapRoots);
            assertThat(pythonFileRoots).as("PD-Q-INT-01: ten file roots").hasSize(10).isEqualTo(javaFileRoots);
            System.out.println("PD-Q-INT-01: 1,000 maps and 10 files, every root equal in Java and Python");
        }
    }
}
