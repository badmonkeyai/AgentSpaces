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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PD-Q-TST-02 and PD-V-01, PD-V-02, PD-V-03: 10,000 seeded cases per kind. A key set
 * reached through any insertion order, through any superset and removal sequence,
 * and through a transient has one root; a vector of a given content reached through
 * any conj, pop, and assoc sequence has one root.
 */
class CanonicalLayoutPropertyTest {

    private static final int CASES = 10_000;
    private static final Codec<String> STRINGS = Codec.of(String.class);
    private static final Codec<Integer> INTS = Codec.of(Integer.class);

    private static int valueOf(String key) {
        return key.hashCode() & 0xffff;
    }

    /** A target key set, a superset to insert first, and the insertion order of the superset. */
    private record Case(List<String> target, List<String> superset) {
    }

    private static Case nextCase(Random random) {
        int n = random.nextInt(40);
        int extra = random.nextInt(12);
        Set<String> target = new LinkedHashSet<>();
        while (target.size() < n) {
            target.add("k" + random.nextInt(400));
        }
        Set<String> superset = new LinkedHashSet<>(target);
        while (superset.size() < n + extra) {
            superset.add("k" + random.nextInt(400));
        }
        List<String> order = new ArrayList<>(superset);
        Collections.shuffle(order, random);
        return new Case(new ArrayList<>(target), order);
    }

    @Test
    void mapsHaveOneRootPerKeySet() {
        Random random = new Random(42);
        NodeStore store = NodeStore.inMemory();
        int withRemovals = 0;
        for (int i = 0; i < CASES; i++) {
            Case c = nextCase(random);
            List<String> sorted = new ArrayList<>(c.target());
            Collections.sort(sorted);
            PMap<String, Integer> reference = PMap.empty(store, STRINGS, INTS);
            for (String key : sorted) {
                reference = reference.assoc(key, valueOf(key));
            }
            String expected = reference.root();

            // Insert the superset in a random order, then remove what is not in the target in a random order.
            PMap<String, Integer> built = PMap.empty(store, STRINGS, INTS);
            for (String key : c.superset()) {
                built = built.assoc(key, valueOf(key));
            }
            List<String> toRemove = new ArrayList<>(c.superset());
            toRemove.removeAll(c.target());
            Collections.shuffle(toRemove, random);
            for (String key : toRemove) {
                built = built.dissoc(key);
            }
            withRemovals += toRemove.isEmpty() ? 0 : 1;
            assertThat(built.root()).as("case %d persistent", i).isEqualTo(expected);
            assertThat(built.count()).isEqualTo(c.target().size());

            // The same through a transient, with the removals interleaved.
            Editor<String, Integer> editor = PMap.empty(store, STRINGS, INTS).editor();
            List<String> ops = new ArrayList<>(c.superset());
            for (String key : ops) {
                editor.assoc(key, valueOf(key));
                if (!toRemove.isEmpty() && random.nextInt(3) == 0) {
                    String gone = toRemove.get(random.nextInt(toRemove.size()));
                    if (editor.get(gone).isPresent()) {
                        editor.dissoc(gone);
                    }
                }
            }
            for (String key : toRemove) {
                editor.dissoc(key);
            }
            assertThat(editor.done().value().root()).as("case %d transient", i).isEqualTo(expected);
        }
        assertThat(withRemovals).isGreaterThan(CASES / 2);
    }

    @Test
    void setsHaveOneRootPerElementSet() {
        Random random = new Random(43);
        NodeStore store = NodeStore.inMemory();
        for (int i = 0; i < CASES; i++) {
            Case c = nextCase(random);
            List<String> sorted = new ArrayList<>(c.target());
            Collections.sort(sorted);
            PSet<String> reference = PSet.empty(store, STRINGS);
            for (String key : sorted) {
                reference = reference.conj(key);
            }
            String expected = reference.root();

            PSet<String> built = PSet.empty(store, STRINGS);
            for (String key : c.superset()) {
                built = built.conj(key);
            }
            List<String> toRemove = new ArrayList<>(c.superset());
            toRemove.removeAll(c.target());
            Collections.shuffle(toRemove, random);
            for (String key : toRemove) {
                built = built.disj(key);
            }
            assertThat(built.root()).as("case %d", i).isEqualTo(expected);
            assertThat(built.count()).isEqualTo(c.target().size());

            SetEditor<String> editor = PSet.empty(store, STRINGS).editor();
            for (String key : c.superset()) {
                editor.conj(key);
            }
            for (String key : toRemove) {
                editor.disj(key);
            }
            assertThat(editor.done().value().root()).as("case %d transient", i).isEqualTo(expected);
        }
    }

    @Test
    void vectorsHaveOneRootPerContent() {
        Random random = new Random(44);
        NodeStore store = NodeStore.inMemory();
        for (int i = 0; i < CASES; i++) {
            int n = random.nextInt(i % 100 == 0 ? 1200 : 70);
            List<Integer> target = new ArrayList<>();
            for (int j = 0; j < n; j++) {
                target.add(random.nextInt(1000));
            }
            PVector<Integer> reference = PVector.empty(store, INTS);
            for (int item : target) {
                reference = reference.conj(item);
            }
            String expected = reference.root();

            // Overshoot with extra elements and wrong values, then pop back and repair with assoc.
            PVector<Integer> built = PVector.empty(store, INTS);
            int extra = random.nextInt(40);
            List<Integer> wrong = new ArrayList<>();
            for (int j = 0; j < n; j++) {
                boolean lie = random.nextInt(4) == 0;
                built = built.conj(lie ? -1 : target.get(j));
                if (lie) {
                    wrong.add(j);
                }
            }
            for (int j = 0; j < extra; j++) {
                built = built.conj(-2);
            }
            for (int j = 0; j < extra; j++) {
                built = built.pop();
            }
            Collections.shuffle(wrong, random);
            for (int j : wrong) {
                built = built.assoc(j, target.get(j));
            }
            assertThat(built.root()).as("case %d persistent, n=%d", i, n).isEqualTo(expected);

            VectorEditor<Integer> editor = PVector.empty(store, INTS).editor();
            for (int j = 0; j < n; j++) {
                editor.conj(-1);
            }
            for (int j = 0; j < extra; j++) {
                editor.conj(-2);
            }
            for (int j = 0; j < extra; j++) {
                editor.pop();
            }
            for (int j = 0; j < n; j++) {
                editor.assoc(j, target.get(j));
            }
            assertThat(editor.done().value().root()).as("case %d transient, n=%d", i, n).isEqualTo(expected);
        }
    }
}
