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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static ai.badmonkey.agentspaces.space.crdt.TrieSupport.stateSlot;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the audit's line reads cost (srad/replication-by-entry.md §3.2; SPEC annex I.6), the trie's side of the Java
 * parity with the TypeScript adversarial review of 2026-11-15 (reviews/RBE-R2-REGRESSION.md §4.1). ADV-TS-4: a partner
 * that answers a request by nodes because its lines pass the budget walked up to the budget's worth of lines first and
 * threw them away; exact byte totals per node answer it with no walk. ADV-TS-3: an asker compared a partner's lines
 * with its whole own set under the path; {@link HashTrie#holdsLine} reads the one slot a line names.
 */
class TrieLineCostTest {

    private static HashTrie<String> trieOf(int n) {
        HashTrie.Builder<String> builder = HashTrie.builder();
        for (int i = 0; i < n; i++) {
            builder.put(stateSlot("entry-" + i, Integer.toHexString(i * 7919)));
        }
        return builder.build();
    }

    /**
     * ADV-TS-4: lines that pass the bound are known to without a walk. Before, a request at the root of 5,000 entries
     * under half their bytes gathered about half the slots, and one hostile body of 256 such requests did it 256 times.
     */
    @Test
    void aBoundedLineBuildKnownToFailGathersNoSlot() {
        HashTrie<String> trie = trieOf(5_000);
        List<byte[]> paths = List.of(new byte[0], new byte[] {3}, new byte[] {3, 9});
        for (byte[] path : paths) {
            byte[] whole = trie.linesUnder(path);
            assertThat(whole.length).as("lines under %s", Arrays.toString(path)).isGreaterThan(64);
            long before = HashTrie.slotsGathered();
            assertThat(trie.linesUnder(path, whole.length / 2)).isNull();
            assertThat(trie.linesUnder(path, whole.length - 1L)).isNull();
            assertThat(HashTrie.slotsGathered() - before).as("slots gathered for answers known to pass the bound")
                    .isZero();
        }
    }

    /**
     * The byte totals stay exact through any history: puts, replacements, removals, acknowledgements equal to a held
     * line, claims, lapses, ids with non-ASCII and control characters, splits and collapses, and batches built afresh
     * and path by path. Each check asks a bounded build at exactly its lines' size and one byte under, at the root and
     * two levels down, against the whole build.
     */
    @Test
    void theByteTotalsStayExactThroughAnyHistory() {
        Random random = new Random(20261116);
        List<String> pool = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            String id = switch (i % 5) {
                case 0 -> "entry-" + i;
                case 1 -> "é-" + i + "-ü";
                case 2 -> "😀-" + i;   // above U+FFFF
                case 3 -> "-" + i;         // U+E000, ordered against the line above by bytes
                default -> i % 25 == 4 ? "bad\u0001-" + i : "x" + i;
            };
            pool.add(id);
        }
        HashTrie<String> trie = HashTrie.empty();
        int checks = 0;
        for (int step = 0; step < 4_000; step++) {
            if (step % 97 == 96) {
                // A batch, built afresh when it is large and path by path when it is small.
                HashTrie.Builder<String> builder = trie.toBuilder();
                int n = 1 + random.nextInt(random.nextBoolean() ? 6 : 200);
                for (int i = 0; i < n; i++) {
                    EntryId id = EntryId.of(pool.get(random.nextInt(pool.size())));
                    builder.update(id, s -> change(random, s));
                }
                trie = builder.build();
            } else {
                EntryId id = EntryId.of(pool.get(random.nextInt(pool.size())));
                trie = random.nextInt(6) == 0 ? trie.remove(id) : trie.update(id, s -> change(random, s));
            }
            if (step % 13 == 0) {
                trie.checkShape();
                for (byte[] path : List.of(new byte[0], new byte[] {(byte) (step % 16)},
                        new byte[] {(byte) (step % 16), (byte) (step / 16 % 16)})) {
                    byte[] whole = trie.linesUnder(path);
                    assertThat(trie.linesUnder(path, whole.length)).as("step %d: at the bound", step).isEqualTo(whole);
                    if (whole.length > 0) {
                        assertThat(trie.linesUnder(path, whole.length - 1L)).as("step %d: one byte under", step)
                                .isNull();
                    }
                    checks++;
                }
            }
        }
        assertThat(checks).isGreaterThan(900);
        assertThat(trie.size()).as("the history ends with a trie past the threshold").isGreaterThan(32);
    }

    /**
     * ADV-TS-3: {@link HashTrie#holdsLine} names exactly what the comparison with every own line under the path named,
     * for hostile lines too (other ids, other hashes, outside the path, malformed, of an id holding a control
     * character, of a lapsed slot), and it gathers no slot.
     */
    @Test
    void holdsLineAnswersAsTheComparisonWithEveryOwnLine() {
        Random random = new Random(7);
        for (int round = 0; round < 40; round++) {
            HashTrie<String> trie = HashTrie.empty();
            List<String> ids = new ArrayList<>();
            int n = 1 + random.nextInt(round < 20 ? 40 : 600);
            for (int i = 0; i < n; i++) {
                String id = (i % 9 == 8 ? "c\u0002" : "id") + "-" + round + "-" + i;
                ids.add(id);
                Slot<String> slot = stateSlot(id, Integer.toHexString(random.nextInt(4)));
                if (random.nextInt(3) == 0) {
                    slot = slot.withClaim("claim", Integer.toHexString(random.nextInt(4)));
                }
                if (random.nextInt(5) == 0) {
                    slot = slot.withAck("e:" + id + "=" + Integer.toHexString(random.nextInt(4)));
                }
                if (random.nextInt(7) == 0) {
                    slot = slot.withLapsed(true);
                }
                trie = trie.put(slot);
            }
            byte[] path = random.nextBoolean() ? new byte[0] : new byte[] {(byte) random.nextInt(16)};
            Set<String> mine = new HashSet<>();
            String own = new String(trie.linesUnder(path), StandardCharsets.UTF_8);
            if (!own.isEmpty()) {
                mine.addAll(Arrays.asList(own.split("\n")));
            }
            List<String> theirs = new ArrayList<>(mine);
            for (int i = 0; i < 60; i++) {
                String id = random.nextInt(5) == 0 ? "stranger-" + i : ids.get(random.nextInt(ids.size()));
                String kind = random.nextBoolean() ? "e:" : "c:";
                theirs.add(switch (random.nextInt(4)) {
                    case 0 -> kind + id + "=" + Integer.toHexString(random.nextInt(4));
                    case 1 -> kind + id + "=x=" + i;
                    case 2 -> "a:" + id + "=0";
                    default -> kind + "=" + i;
                });
            }
            long before = HashTrie.slotsGathered();
            for (String line : theirs) {
                assertThat(trie.holdsLine(line, path)).as("round %d: %s", round, line).isEqualTo(mine.contains(line));
            }
            assertThat(HashTrie.slotsGathered() - before).as("slots gathered to compare %d lines", theirs.size())
                    .isZero();
        }
    }

    /** A random change to one slot, as {@code HashTriePropertyTest} makes them. */
    private static Slot<String> change(Random random, Slot<String> slot) {
        String id = slot.id().value();
        String h = Integer.toString(random.nextInt(3));
        if (slot.isEmpty() && random.nextInt(4) != 0) {
            return slot.withState(TrieSupport.state(id), h);
        }
        return switch (random.nextInt(9)) {
            case 0, 1, 2 -> slot.withState(TrieSupport.state(id), h);
            case 3 -> slot.withClaim("claim", h);
            case 4 -> slot.withoutClaim();
            case 5 -> slot.withDropped(random.nextBoolean() ? h : null);
            case 6 -> slot.withAck((random.nextBoolean() ? "e:" : "c:") + id + "=" + h);
            case 7 -> slot.acks().isEmpty() ? slot.withoutState() : slot.withoutAck(slot.acks().get(0));
            default -> slot.withLapsed(!slot.lapsed());
        };
    }
}
