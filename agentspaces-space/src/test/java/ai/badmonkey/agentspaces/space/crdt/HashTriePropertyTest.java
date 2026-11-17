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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static ai.badmonkey.agentspaces.space.crdt.TrieSupport.HEX;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * RE-M-05 and RE-Q-REL-01 (srad/replication-by-entry.md): random histories of puts, replacements, removals, batches
 * and bulk removals over a pool of ids, some of them ground to share a three-nibble key prefix so the trie splits and
 * collapses below the root as well as at it. Each history alternates growing and shrinking phases, so the counts
 * sweep across the threshold again and again.
 *
 * <p>After every step: the trie's contents equal a {@link HashMap} model, the stored tree has annex I.6's canonical
 * shape, and its stream hash equals the reference fold over the model. Across the whole history, equal audited sets
 * have equal roots and different sets different roots. At the end of each phase, the model's slots are put into an
 * empty trie in a shuffled order among junk that is put and taken out again, and that second history must reach the
 * same root. Seeded, so a failure reproduces exactly.
 */
class HashTriePropertyTest {

    private static final int STEPS = 2_400;
    private static final int PHASE = 300;
    private static final byte[] DEEP = TrieSupport.path("3a7");

    @ParameterizedTest
    @ValueSource(longs = {1L, 20261114L, 0x5eedL})
    void rootsAreEqualExactlyWhenTheAuditedSetsAre(long seed) {
        Random random = new Random(seed);
        List<String> pool = new ArrayList<>(TrieSupport.ground("3a7", 60, "deep"));
        for (int i = 0; pool.size() < 130; i++) {
            pool.add("id-" + i + (i % 11 == 0 ? "é" : ""));
        }
        Map<EntryId, Slot<String>> model = new HashMap<>();
        Map<Set<String>, String> rootOfSet = new HashMap<>();
        Map<String, Set<String>> setOfRoot = new HashMap<>();
        HashTrie<String> trie = HashTrie.empty();
        String previousRoot = HEX.formatHex(trie.streamHash());
        Set<String> previousSet = Set.of();
        int collapses = 0;
        int splits = 0;
        int maxSize = 0;
        int deepInner = 0;

        for (int step = 0; step < STEPS; step++) {
            boolean growing = (step / PHASE) % 2 == 0;
            int sizeBefore = trie.size();
            int op = random.nextInt(100);
            if (op < 6) {
                trie = batch(random, pool, model, trie);
            } else if (op < 7 && !model.isEmpty()) {
                trie = bulkRemove(random, model, trie);
            } else if (op < (growing ? 90 : 35)) {
                EntryId id = EntryId.of(pool.get(random.nextInt(pool.size())));
                Slot<String> next = change(random, model.getOrDefault(id, Slot.empty(id)));
                trie = trie.put(next);
                putModel(model, next);
            } else {
                EntryId id = model.isEmpty() ? EntryId.of(pool.get(0))
                        : new ArrayList<>(model.keySet()).get(random.nextInt(model.size()));
                trie = trie.remove(id);
                model.remove(id);
            }
            if (sizeBefore <= HashTrie.LEAF_MAX && trie.size() > HashTrie.LEAF_MAX) {
                splits++;
            }
            if (sizeBefore > HashTrie.LEAF_MAX && trie.size() <= HashTrie.LEAF_MAX) {
                collapses++;
            }
            maxSize = Math.max(maxSize, trie.size());
            if (trie.node(DEEP).kind() == AuditNode.Kind.INNER) {
                deepInner++;
            }

            trie.checkShape();
            assertContentsEqual(trie, model, seed, step);
            Set<String> audited = TrieSupport.auditedSet(model.values());
            String root = HEX.formatHex(trie.streamHash());
            assertThat(root).as("seed %d step %d: the reference fold", seed, step)
                    .isEqualTo(HEX.formatHex(TrieSupport.referenceStreamHash(model.values())));
            String seenRoot = rootOfSet.putIfAbsent(audited, root);
            assertThat(seenRoot == null ? root : seenRoot).as("seed %d step %d: equal sets, equal roots", seed, step)
                    .isEqualTo(root);
            Set<String> seenSet = setOfRoot.putIfAbsent(root, audited);
            assertThat(seenSet == null ? audited : seenSet).as("seed %d step %d: equal roots, equal sets", seed, step)
                    .isEqualTo(audited);
            assertThat(root.equals(previousRoot)).as("seed %d step %d: the root moves with the set", seed, step)
                    .isEqualTo(audited.equals(previousSet));
            previousRoot = root;
            previousSet = audited;

            if ((step + 1) % PHASE == 0) {
                HashTrie<String> other = otherHistory(random, model);
                other.checkShape();
                assertThat(other).as("seed %d step %d: another history", seed, step).isEqualTo(trie);
                assertThat(HEX.formatHex(other.streamHash())).isEqualTo(root);
            }
        }
        // The histories did what they are for: they crossed the threshold both ways, many times, and went deep.
        assertThat(splits).as("splits at the root").isGreaterThan(5);
        assertThat(collapses).as("collapses at the root").isGreaterThan(5);
        assertThat(maxSize).isGreaterThan(90);
        assertThat(deepInner).as("steps with an inner node three nibbles down").isGreaterThan(50);
        assertThat(deepInner).as("and steps without one").isLessThan(STEPS - 50);
        assertThat(rootOfSet.size()).isGreaterThan(STEPS / 3);
    }

    /** A random change to one slot: any line added, replaced or dropped, the lapse flag flipped, or a fresh slot. */
    private static Slot<String> change(Random random, Slot<String> slot) {
        String id = slot.id().value();
        // A small alphabet of hashes, so different histories keep landing on the same lines and sets.
        String h = Integer.toString(random.nextInt(3));
        if (slot.isEmpty() && random.nextInt(4) != 0) {
            // Most fresh ids start with a state, as entries do; the rest start from any line or none.
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

    private static void putModel(Map<EntryId, Slot<String>> model, Slot<String> slot) {
        if (slot.isEmpty()) {
            model.remove(slot.id());
        } else {
            model.put(slot.id(), slot);
        }
    }

    /** A batch of changes and removals made through the builder, as a merged PULL_RESP would be. */
    private static HashTrie<String> batch(Random random, List<String> pool, Map<EntryId, Slot<String>> model,
                                          HashTrie<String> trie) {
        HashTrie.Builder<String> builder = trie.toBuilder();
        int n = 1 + random.nextInt(random.nextBoolean() ? 8 : 80);
        for (int i = 0; i < n; i++) {
            EntryId id = EntryId.of(pool.get(random.nextInt(pool.size())));
            if (random.nextInt(4) == 0) {
                builder.remove(id);
                model.remove(id);
            } else {
                builder.update(id, s -> change(random, s));
                Slot<String> now = builder.get(id);
                if (now == null) {
                    model.remove(id);
                } else {
                    model.put(id, now);
                }
            }
        }
        return builder.build();
    }

    /** Drops a random part of the model at once, as {@code without} and the tombstone collection do. */
    private static HashTrie<String> bulkRemove(Random random, Map<EntryId, Slot<String>> model,
                                               HashTrie<String> trie) {
        HashTrie.Builder<String> builder = trie.toBuilder();
        for (EntryId id : new ArrayList<>(model.keySet())) {
            if (random.nextInt(8) == 0) {
                builder.remove(id);
                model.remove(id);
            }
        }
        return builder.build();
    }

    /** The model's slots reached by another order, among junk slots and earlier versions taken back out. */
    private static HashTrie<String> otherHistory(Random random, Map<EntryId, Slot<String>> model) {
        List<Slot<String>> slots = new ArrayList<>(model.values());
        Collections.shuffle(slots, random);
        HashTrie<String> trie = HashTrie.empty();
        List<EntryId> junk = new ArrayList<>();
        for (Slot<String> slot : slots) {
            if (random.nextInt(4) == 0) {
                trie = trie.put(change(random, slot));
            }
            if (random.nextInt(3) == 0) {
                EntryId j = EntryId.of("junk-" + junk.size());
                junk.add(j);
                trie = trie.put(TrieSupport.stateSlot(j.value(), "j"));
            }
            trie = trie.put(slot);
        }
        Collections.shuffle(junk, random);
        for (EntryId j : junk) {
            trie = trie.remove(j);
        }
        return trie;
    }

    private static void assertContentsEqual(HashTrie<String> trie, Map<EntryId, Slot<String>> model, long seed,
                                            int step) {
        assertThat(trie.size()).as("seed %d step %d: size", seed, step).isEqualTo(model.size());
        Map<EntryId, Slot<String>> seen = new HashMap<>();
        for (Slot<String> slot : trie) {
            seen.put(slot.id(), slot);
        }
        assertThat(seen).as("seed %d step %d: contents", seed, step).isEqualTo(model);
        for (Map.Entry<EntryId, Slot<String>> e : model.entrySet()) {
            assertThat(trie.get(e.getKey())).isEqualTo(e.getValue());
        }
    }
}
