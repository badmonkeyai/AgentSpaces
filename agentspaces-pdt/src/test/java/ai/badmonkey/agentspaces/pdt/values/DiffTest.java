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
package ai.badmonkey.agentspaces.pdt.values;

import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** PD-V-07: {@link Diff} finds added, removed, and changed keys and visits a number of nodes proportional to the changes. */
class DiffTest {

    private static final Codec<Integer> INTS = Codec.of(Integer.class);
    private static final Codec<String> STRINGS = Codec.of(String.class);

    private static PMap<Integer, Integer> build(NodeStore store, int n) {
        Editor<Integer, Integer> editor = PMap.empty(store, INTS, INTS).editor();
        for (int i = 0; i < n; i++) {
            editor.assoc(i, i);
        }
        return editor.done().value();
    }

    @Test
    void addedRemovedAndChangedAreExact() {
        NodeStore store = NodeStore.inMemory();
        PMap<String, Integer> base = PMap.empty(store, STRINGS, INTS).assoc("a", 1).assoc("b", 2).assoc("c", 3);
        PMap<String, Integer> mine = base.assoc("d", 4).dissoc("a").assoc("b", 20);
        Diff<String, Integer> diff = mine.diff(base);
        assertThat(diff.added()).containsExactly(Map.entry("d", 4));
        assertThat(diff.removed()).containsExactly(Map.entry("a", 1));
        assertThat(diff.changed()).containsExactly(Map.entry("b", new Diff.Change<>(2, 20)));
        assertThat(diff.isEmpty()).isFalse();
        assertThat(base.diff(base).isEmpty()).isTrue();
        assertThat(base.diff(mine).added()).containsExactly(Map.entry("a", 1));
        assertThat(base.diff(mine).removed()).containsExactly(Map.entry("d", 4));
        assertThat(base.diff(PMap.empty(store, STRINGS, INTS)).added()).hasSize(3);
        assertThat(PMap.empty(store, STRINGS, INTS).diff(base).removed()).hasSize(3);
    }

    @Test
    void costIsProportionalToChangesTimesDepth() {
        NodeStore store = NodeStore.inMemory();
        PMap<Integer, Integer> base = build(store, 100_000);
        int depth = base.depth();
        PMap<Integer, Integer> mine = base.assoc(123_456, 1).assoc(500, -500).dissoc(77_777).assoc(123_457, 2);
        Diff<Integer, Integer> diff = mine.diff(base);
        assertThat(diff.added()).containsOnlyKeys(123_456, 123_457);
        assertThat(diff.removed()).containsOnlyKeys(77_777);
        assertThat(diff.changed()).containsExactly(Map.entry(500, new Diff.Change<>(500, -500)));
        // Four changes, each a path of at most depth nodes on each side, plus a few inlined subtrees.
        assertThat(diff.nodesVisited()).isLessThanOrEqualTo(4 * depth * 2 + 8);
        assertThat(diff.nodesVisited()).isLessThan(60);

        // A cold diff reads the same number of nodes through the loader.
        AtomicInteger loads = new AtomicInteger();
        NodeLoader counting = cid -> {
            loads.incrementAndGet();
            return store.get(cid).orElseThrow();
        };
        PMap<Integer, Integer> coldMine = PMap.load(store, counting, mine.root(), INTS, INTS);
        PMap<Integer, Integer> coldBase = PMap.load(store, counting, base.root(), INTS, INTS);
        Diff<Integer, Integer> cold = coldMine.diff(coldBase);
        assertThat(cold.added()).containsOnlyKeys(123_456, 123_457);
        assertThat(loads.get()).isLessThanOrEqualTo(diff.nodesVisited() + 8);
        assertThat(base.diff(base).nodesVisited()).isZero();
    }

    @Test
    void randomDiffsMatchABruteForceComparison() {
        NodeStore store = NodeStore.inMemory();
        Random random = new Random(42);
        for (int round = 0; round < 200; round++) {
            Map<Integer, Integer> baseModel = new HashMap<>();
            Map<Integer, Integer> mineModel = new HashMap<>();
            PMap<Integer, Integer> base = PMap.empty(store, INTS, INTS);
            int n = random.nextInt(300);
            for (int i = 0; i < n; i++) {
                int key = random.nextInt(500);
                base = base.assoc(key, i);
                baseModel.put(key, i);
            }
            mineModel.putAll(baseModel);
            PMap<Integer, Integer> mine = base;
            int changes = random.nextInt(40);
            for (int i = 0; i < changes; i++) {
                int key = random.nextInt(600);
                if (random.nextInt(3) == 0) {
                    mine = mine.dissoc(key);
                    mineModel.remove(key);
                } else {
                    mine = mine.assoc(key, -1 - i);
                    mineModel.put(key, -1 - i);
                }
            }
            Map<Integer, Integer> added = new LinkedHashMap<>();
            Map<Integer, Integer> removed = new LinkedHashMap<>();
            Map<Integer, Diff.Change<Integer>> changed = new LinkedHashMap<>();
            for (Map.Entry<Integer, Integer> e : mineModel.entrySet()) {
                Integer before = baseModel.get(e.getKey());
                if (before == null) {
                    added.put(e.getKey(), e.getValue());
                } else if (!before.equals(e.getValue())) {
                    changed.put(e.getKey(), new Diff.Change<>(before, e.getValue()));
                }
            }
            for (Map.Entry<Integer, Integer> e : baseModel.entrySet()) {
                if (!mineModel.containsKey(e.getKey())) {
                    removed.put(e.getKey(), e.getValue());
                }
            }
            Diff<Integer, Integer> diff = mine.diff(base);
            assertThat(diff.added()).as("round %d added", round).isEqualTo(added);
            assertThat(diff.removed()).as("round %d removed", round).isEqualTo(removed);
            assertThat(diff.changed()).as("round %d changed", round).isEqualTo(changed);
        }
    }
}
