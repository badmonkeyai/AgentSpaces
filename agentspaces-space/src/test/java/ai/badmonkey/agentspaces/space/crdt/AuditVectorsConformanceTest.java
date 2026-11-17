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
package ai.badmonkey.agentspaces.space.crdt;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Reads audit-vectors.json (SPEC annex I.6; srad/replication-by-entry.md RE-L-05, second half) through the trie this
 * replica runs: each set rebuilt from its slots, one put at a time for half of them and through the builder for the
 * other half, gives the listed lines, count, stream hash and every listed node; each descent, walked through the two
 * sets' own tries, asks and answers exactly the listed nodes and finds exactly the listed ids; and every body decodes
 * under the strict reader to its nodes and back to its bytes. The conformance harness reads the same file with Java and
 * with an independent Python fold ({@code tools/conformance/audit}).
 */
class AuditVectorsConformanceTest {

    private static final List<Path> PATHS = List.of(
            Path.of("..", "tools", "golden", "audit-vectors.json"),
            Path.of("..", "..", "agentspaces-spec", "audit-vectors.json"));
    private static final boolean REQUIRED = Boolean.getBoolean("golden.required");
    private static final HexFormat HEX = HexFormat.of();
    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final String STREAM = "space:tasks";
    private static JsonNode doc;

    @BeforeAll
    static void load() throws Exception {
        Path found = PATHS.stream().filter(Files::exists).findFirst().orElse(null);
        if (REQUIRED) {
            assertThat(found).as("audit-vectors.json is required (golden.required=true) but none of " + PATHS
                    + " exists").isNotNull();
        }
        assumeTrue(found != null, "audit-vectors.json not present");
        doc = new ObjectMapper().readTree(found.toFile());
    }

    @Test
    void everySetGivesItsLinesStreamHashAndNodes() {
        int nodes = 0;
        for (JsonNode set : doc.get("sets")) {
            String name = set.get("name").asText();
            HashTrie<String> trie = trie(set.get("slots"), (name.hashCode() & 1) == 0);
            trie.checkShape();
            String joined = new String(trie.linesUnder(new byte[0]), StandardCharsets.UTF_8);
            assertThat(joined.isEmpty() ? List.of() : List.of(joined.split("\n"))).as(name + " lines")
                    .isEqualTo(texts(set.get("lines")));
            assertThat(trie.auditedCount()).as(name + " count").isEqualTo(set.get("count").asInt());
            assertThat(HEX.formatHex(trie.streamHash())).as(name + " stream hash")
                    .isEqualTo(set.get("stream_hash").asText());
            for (JsonNode n : set.get("nodes")) {
                AuditNode node = trie.node(path(n.get("path").asText()));
                String at = name + " at '" + n.get("path").asText() + "'";
                assertThat(node.kind().name().toLowerCase(java.util.Locale.ROOT)).as(at).isEqualTo(n.get("kind").asText());
                assertThat(node.count()).as(at).isEqualTo(n.get("count").asLong());
                assertThat(HEX.formatHex(node.preimage())).as(at).isEqualTo(n.get("preimage").asText());
                if (node.kind() != AuditNode.Kind.EMPTY) {
                    assertThat(HEX.formatHex(node.fingerprint())).as(at).isEqualTo(n.get("fingerprint").asText());
                }
                nodes++;
            }
        }
        assertThat(doc.get("sets").size()).isGreaterThanOrEqualTo(30);
        assertThat(nodes).isGreaterThan(900);
    }

    @Test
    void everyDescentAsksAndFindsWhatItLists() {
        Map<String, HashTrie<String>> tries = new HashMap<>();
        doc.get("sets").forEach(set -> tries.put(set.get("name").asText(), trie(set.get("slots"), true)));
        for (JsonNode d : doc.get("descents")) {
            String name = d.get("name").asText();
            HashTrie<String> asker = tries.get(d.get("asker").asText());
            HashTrie<String> partner = tries.get(d.get("partner").asText());
            Set<String> ids = new TreeSet<>((a, b) -> Arrays.compareUnsigned(a.getBytes(StandardCharsets.UTF_8),
                    b.getBytes(StandardCharsets.UTF_8)));
            JsonNode exchanges = d.get("exchanges");
            assertThat(exchanges.isEmpty()).as(name + ": no exchange exactly when the hashes match")
                    .isEqualTo(Arrays.equals(asker.streamHash(), partner.streamHash()));
            for (int k = 0; k < exchanges.size(); k++) {
                JsonNode exchange = exchanges.get(k);
                String at = name + " exchange " + k;
                Bodies.Digest request = body(exchange.get("request_body_cbor").asText());
                Bodies.Digest answer = body(exchange.get("answer_body_cbor").asText());
                List<Bodies.TreeNode> answers = answer.tree().get(STREAM);
                if (k == 0) {
                    assertThat(request.hashes().get(STREAM)).as(at).isEqualTo(asker.streamHash());
                    assertThat(request.tree()).as(at + ": the opening carries no tree").isNull();
                    assertThat(answers).hasSize(1);
                    assertThat(answers.get(0).path()).isEmpty();
                } else {
                    List<Bodies.TreeNode> asked = request.tree().get(STREAM);
                    assertThat(asked).as(at).hasSize(exchange.get("requests").size());
                    for (int i = 0; i < asked.size(); i++) {
                        AuditNode mine = asker.node(asked.get(i).path());
                        assertThat(asked.get(i).count()).as(at + ": the asker's own count").isEqualTo(mine.count());
                        assertThat(answers.get(i).path()).as(at + ": answered in the order asked")
                                .isEqualTo(asked.get(i).path());
                    }
                    assertThat(request.tree().get(STREAM)).extracting(n -> pathText(n.path()))
                            .containsExactlyElementsOf(texts(exchange.get("requests"), "path"));
                }
                for (Bodies.TreeNode a : answers) {
                    AuditNode theirs = partner.node(a.path());
                    if (a.nodes() != null) {
                        assertThat(a.nodes()).as(at).isEqualTo(theirs.preimage());
                    } else if (!a.lines().isEmpty()) {
                        assertThat(a.lines()).as(at + ": every partner line under the path")
                                .isEqualTo(new String(partner.linesUnder(a.path()), StandardCharsets.UTF_8));
                        String own = new String(asker.linesUnder(a.path()), StandardCharsets.UTF_8);
                        Set<String> mine = new HashSet<>(own.isEmpty() ? List.of() : Arrays.asList(own.split("\n")));
                        for (String line : a.lines().split("\n")) {
                            if (!mine.contains(line)) {
                                ids.add(line.substring(2, line.lastIndexOf('=')));
                            }
                        }
                    } else {
                        assertThat(theirs.count()).as(at + ": the empty text answers an empty node").isZero();
                    }
                }
            }
            assertThat(new ArrayList<>(ids)).as(name + " ids").isEqualTo(texts(d.get("ids")));
        }
        assertThat(doc.get("descents").size()).isGreaterThanOrEqualTo(9);
    }

    private static Bodies.Digest body(String hex) {
        byte[] bytes = HEX.parseHex(hex);
        Bodies.Digest body = CODEC.fromWire(bytes, Bodies.Digest.class);
        assertThat(CODEC.toBytes(body)).as("a body re-encodes to its own bytes").isEqualTo(bytes);
        return body;
    }

    private static HashTrie<String> trie(JsonNode slots, boolean stepwise) {
        List<Slot<String>> built = new ArrayList<>();
        for (JsonNode s : slots) {
            String id = s.get("id").asText();
            Slot<String> slot = Slot.empty(EntryId.of(id));
            if (!s.get("state").isNull()) {
                slot = slot.withState(TrieSupport.state(id), s.get("state").asText());
            }
            if (!s.get("claim").isNull()) {
                slot = slot.withClaim("claim", s.get("claim").asText());
            }
            if (!s.get("dropped").isNull()) {
                slot = slot.withDropped(s.get("dropped").asText());
            }
            slot = slot.withLapsed(s.get("lapsed").asBoolean());
            if (!s.get("acks").isEmpty()) {
                slot = slot.withAcks(texts(s.get("acks")));
            }
            built.add(slot);
        }
        if (stepwise) {
            HashTrie<String> trie = HashTrie.empty();
            for (Slot<String> slot : built) {
                trie = trie.put(slot);
            }
            return trie;
        }
        HashTrie.Builder<String> builder = HashTrie.builder();
        built.forEach(builder::put);
        return builder.build();
    }

    private static List<String> texts(JsonNode array) {
        List<String> out = new ArrayList<>();
        array.forEach(n -> out.add(n.asText()));
        return out;
    }

    private static List<String> texts(JsonNode array, String field) {
        List<String> out = new ArrayList<>();
        array.forEach(n -> out.add(n.get(field).asText()));
        return out;
    }

    private static byte[] path(String hex) {
        byte[] out = new byte[hex.length()];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Character.digit(hex.charAt(i), 16);
        }
        return out;
    }

    private static String pathText(byte[] path) {
        StringBuilder out = new StringBuilder();
        for (byte b : path) {
            out.append(Character.forDigit(b, 16));
        }
        return out.toString();
    }
}
