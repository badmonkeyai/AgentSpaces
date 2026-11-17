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
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static ai.badmonkey.agentspaces.space.crdt.TrieSupport.HEX;
import static ai.badmonkey.agentspaces.space.crdt.TrieSupport.path;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-checks the trie against an independent Python fold of SPEC annex I.6. The fixture
 * {@code hash-trie-fixture.txt} and its generator sit beside each other in the test resources; each set lists its
 * slots (states, claims, shard-dropped lines, acknowledgements, a lapse flag), the stream hash the fold computed, and
 * the fingerprint and count at every node of the audited tree plus one path under each leaf.
 *
 * <p>Provisional: phase 4's vector generator writes {@code audit-vectors.json}, and this fixture gives way to it. It
 * is not a normative vector, and {@code golden.json} is untouched.
 */
class HashTrieFixtureTest {

    @Test
    void everySetMatchesThePythonFold() throws IOException {
        int sets = 0;
        int nodes = 0;
        try (InputStream in = Objects.requireNonNull(getClass().getResourceAsStream("hash-trie-fixture.txt"));
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String name = null;
            List<Slot<String>> slots = new ArrayList<>();
            HashTrie<String> trie = null;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#") || line.isBlank()) {
                    continue;
                }
                String[] f = line.split(" ");
                switch (f[0]) {
                    case "set" -> {
                        name = f[1];
                        slots.clear();
                        trie = null;
                    }
                    case "slot" -> slots.add(slot(f));
                    case "stream" -> {
                        // Half the sets are built one put at a time, half by the builder, so both paths are checked.
                        trie = (name.hashCode() & 1) == 0 ? stepwise(slots) : built(slots);
                        trie.checkShape();
                        assertThat(trie.size()).as(name).isEqualTo(slots.size());
                        assertThat(HEX.formatHex(trie.streamHash())).as(name + " stream hash").isEqualTo(f[1]);
                    }
                    case "node" -> {
                        String hexPath = f[1].equals(".") ? "" : f[1];
                        AuditNode node = trie.node(path(hexPath));
                        assertThat(node.count()).as(name + " count at " + f[1]).isEqualTo(Long.parseLong(f[2]));
                        if (f[3].equals("-")) {
                            assertThat(node.kind()).as(name + " at " + f[1]).isEqualTo(AuditNode.Kind.EMPTY);
                        } else {
                            assertThat(HEX.formatHex(node.fingerprint())).as(name + " fingerprint at " + f[1])
                                    .isEqualTo(f[3]);
                            assertThat(node.kind() == AuditNode.Kind.INNER).as(name + " kind at " + f[1])
                                    .isEqualTo(node.count() > HashTrie.LEAF_MAX && hexPath.length() < 64);
                        }
                        nodes++;
                    }
                    case "end" -> sets++;
                    default -> throw new IllegalStateException("unknown fixture line: " + line);
                }
            }
        }
        assertThat(sets).isEqualTo(17);
        assertThat(nodes).isGreaterThan(600);
    }

    private static Slot<String> slot(String[] f) {
        String id = text(f[1]);
        Slot<String> slot = Slot.empty(EntryId.of(id));
        if (!f[2].equals("-")) {
            slot = slot.withState(TrieSupport.state(id), f[2]);
        }
        if (!f[3].equals("-")) {
            slot = slot.withClaim("claim", f[3]);
        }
        if (!f[4].equals("-")) {
            slot = slot.withDropped(f[4]);
        }
        slot = slot.withLapsed(f[5].equals("1"));
        if (!f[6].equals("-")) {
            slot = slot.withAcks(Arrays.stream(f[6].split(",")).map(HashTrieFixtureTest::text).toList());
        }
        return slot;
    }

    private static String text(String hex) {
        return new String(HEX.parseHex(hex), StandardCharsets.UTF_8);
    }

    private static HashTrie<String> stepwise(List<Slot<String>> slots) {
        HashTrie<String> trie = HashTrie.empty();
        for (Slot<String> slot : slots) {
            trie = trie.put(slot);
        }
        return trie;
    }

    private static HashTrie<String> built(List<Slot<String>> slots) {
        HashTrie.Builder<String> builder = HashTrie.builder();
        slots.forEach(builder::put);
        return builder.build();
    }
}
