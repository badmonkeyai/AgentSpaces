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
package ai.badmonkey.agentspaces.content.tree.sync;

import ai.badmonkey.agentspaces.common.crypto.Digests;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * PDT 2 phase 5, P2-K-01: the Java negentropy reproduces every message of every round that the reference
 * implementation wrote into {@code agentspaces-spec/negentropy-vectors.json} (phase 0), the outcome of each case and
 * measurement, and both protocol-version replies of the upstream suite.
 */
class NegentropyConformanceTest {

    private static final Path VECTORS = Path.of("..", "..", "agentspaces-spec", "negentropy-vectors.json");
    private static final long TIMESTAMP_BASE = 1792320000L;
    private static Map<String, Object> vectors;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void load() throws Exception {
        if (Boolean.getBoolean("golden.required")) {
            assertThat(VECTORS).as("the negentropy vectors are required (golden.required=true)").exists();
        }
        assumeTrue(Files.exists(VECTORS), "negentropy-vectors.json not present");
        vectors = new ObjectMapper().readValue(VECTORS.toFile(), Map.class);
    }

    /** Item i of a universe, by the recipe of the vectors. */
    private static Negentropy.Item item(String universe, int perTimestamp, int i) {
        byte[] id = Digests.sha256(("aspace:negentropy:" + universe + ":" + i).getBytes(StandardCharsets.UTF_8));
        return new Negentropy.Item(TIMESTAMP_BASE + i / perTimestamp, id);
    }

    @SuppressWarnings("unchecked")
    private static Negentropy.Storage members(Map<String, Object> c, String side) {
        String universe = (String) c.get("universe");
        int perTimestamp = 1;
        for (Map<String, Object> u : (List<Map<String, Object>>) ((Map<String, Object>) vectors.get("recipe")).get("universes")) {
            if (universe.equals(u.get("name"))) {
                perTimestamp = ((Number) u.get("perTimestamp")).intValue();
            }
        }
        List<Negentropy.Item> items = new ArrayList<>();
        for (List<Number> range : (List<List<Number>>) c.get(side)) {
            for (int i = range.get(0).intValue(); i < range.get(1).intValue(); i++) {
                items.add(item(universe, perTimestamp, i));
            }
        }
        return Negentropy.Storage.of(items);
    }

    /** One session, client first; every message as hex in order, and the sorted outcome. */
    private record Run(List<String> messages, List<String> have, List<String> need, int largest) {
        int rounds() {
            return messages.size() / 2;
        }

        long bytes() {
            return messages.stream().mapToLong(m -> m.length() / 2).sum();
        }
    }

    private static Run run(Map<String, Object> c) {
        int limit = ((Number) c.get("frameSizeLimit")).intValue();
        Negentropy client = new Negentropy(members(c, "client"), limit);
        Negentropy server = new Negentropy(members(c, "server"), limit);
        List<String> messages = new ArrayList<>();
        List<byte[]> have = new ArrayList<>();
        List<byte[]> need = new ArrayList<>();
        int largest = 0;
        byte[] query = client.initiate();
        while (true) {
            assertThat(messages.size() / 2).as("within the 64 rounds of P2-K-04").isLessThan(64);
            messages.add(HexFormat.of().formatHex(query));
            byte[] reply = server.reconcile(query);
            messages.add(HexFormat.of().formatHex(reply));
            largest = Math.max(largest, Math.max(query.length, reply.length));
            Optional<byte[]> next = client.reconcile(reply, have, need);
            if (next.isEmpty()) {
                break;
            }
            query = next.get();
        }
        return new Run(messages, sorted(have), sorted(need), largest);
    }

    private static List<String> sorted(List<byte[]> ids) {
        return ids.stream().map(HexFormat.of()::formatHex).sorted().toList();
    }

    @Test
    @SuppressWarnings("unchecked")
    void everyMessageOfEveryCaseMatchesTheReference() {
        List<Map<String, Object>> cases = (List<Map<String, Object>>) vectors.get("cases");
        assertThat(cases).hasSize(10);
        for (Map<String, Object> c : cases) {
            Run run = run(c);
            List<String> expected = ((List<Map<String, Object>>) c.get("messages")).stream()
                    .map(m -> (String) m.get("hex")).toList();
            assertThat(run.messages()).as((String) c.get("name")).containsExactlyElementsOf(expected);
            assertThat(run.rounds()).as(c.get("name") + " rounds").isEqualTo(((Number) c.get("rounds")).intValue());
            assertThat(run.bytes()).as(c.get("name") + " bytes").isEqualTo(((Number) c.get("bytes")).longValue());
            assertThat(run.have()).as(c.get("name") + " have").containsExactlyElementsOf((List<String>) c.get("have"));
            assertThat(run.need()).as(c.get("name") + " need").containsExactlyElementsOf((List<String>) c.get("need"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void theMeasurementsAtTheScaleOfP2K02MatchTheReference() {
        for (Map<String, Object> m : (List<Map<String, Object>>) vectors.get("measurements")) {
            Run run = run(m);
            assertThat(run.rounds()).as(m.get("name") + " rounds").isEqualTo(((Number) m.get("rounds")).intValue());
            assertThat(run.bytes()).as(m.get("name") + " bytes").isEqualTo(((Number) m.get("bytes")).longValue());
            assertThat(run.have()).as(m.get("name") + " have").hasSize(((Number) m.get("have")).intValue());
            assertThat(run.need()).as(m.get("name") + " need").hasSize(((Number) m.get("need")).intValue());
            assertThat(run.largest()).as(m.get("name") + " largest message").isEqualTo(((Number) m.get("largestMessage")).intValue());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void aResponderAnswersTheUpstreamProtocolVersionVectors() {
        for (Map<String, Object> u : (List<Map<String, Object>>) vectors.get("upgrade")) {
            List<Negentropy.Item> items = new ArrayList<>();
            for (List<Object> item : (List<List<Object>>) u.get("server")) {
                items.add(new Negentropy.Item(((Number) item.get(0)).longValue(), HexFormat.of().parseHex((String) item.get(1))));
            }
            Negentropy server = new Negentropy(Negentropy.Storage.of(items), 0);
            byte[] reply = server.reconcile(HexFormat.of().parseHex((String) u.get("message")));
            assertThat(HexFormat.of().formatHex(reply)).as((String) u.get("name")).isEqualTo(u.get("reply"));
        }
    }

    @Test
    void malformedMessagesAreRefusedByName() {
        Negentropy server = new Negentropy(Negentropy.Storage.of(List.of()), 0);
        assertThatThrownBy(() -> server.reconcile(new byte[]{0x61, 0x00})).hasMessageContaining("prematurely");
        assertThatThrownBy(() -> server.reconcile(new byte[]{0x70})).hasMessageContaining("version byte");
        assertThatThrownBy(() -> server.reconcile(new byte[]{0x61, 0x00, 0x21})).hasMessageContaining("bound key too long");
        assertThatThrownBy(() -> server.reconcile(new byte[]{0x61, 0x00, 0x00, 0x07})).hasMessageContaining("unexpected mode");
        assertThatThrownBy(() -> server.reconcile(new byte[]{0x61, 0x00, 0x00, 0x02, 0x05})).hasMessageContaining("runs past");
        byte[] wide = new byte[12];
        wide[0] = 0x61;
        java.util.Arrays.fill(wide, 1, 11, (byte) 0xff);
        assertThatThrownBy(() -> server.reconcile(wide)).hasMessageContaining("63 bits");
        Negentropy client = new Negentropy(Negentropy.Storage.of(List.of()), 0);
        client.initiate();
        assertThatThrownBy(() -> client.reconcile(new byte[]{0x62}, new ArrayList<>(), new ArrayList<>()))
                .hasMessageContaining("unsupported negentropy protocol version 2");
    }
}
