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

import ai.badmonkey.agentspaces.pdt.Embedded;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** PD-V-02: {@link PSet} as a map of null values with conj, disj, contains, and equality by root. */
class PSetTest {

    private static final Codec<String> STRINGS = Codec.of(String.class);
    private static final Codec<Integer> INTS = Codec.of(Integer.class);

    @Test
    void conjDisjContainsAndCount() {
        PSet<String> empty = PSet.empty(NodeStore.inMemory(), STRINGS);
        PSet<String> one = empty.conj("a");
        PSet<String> two = one.conj("b");
        assertThat(empty.count()).isZero();
        assertThat(empty.isEmpty()).isTrue();
        assertThat(one.contains("a")).isTrue();
        assertThat(one.contains("b")).isFalse();
        assertThat(two.count()).isEqualTo(2);
        assertThat(two.conj("a")).isSameAs(two);
        assertThat(two.disj("zz")).isSameAs(two);
        assertThat(two.disj("a").contains("a")).isFalse();
        assertThat(two.contains("a")).isTrue();
        assertThat(two.toSet()).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void valuesAreCborNullOnTheWire() {
        NodeStore store = NodeStore.inMemory();
        PSet<String> set = PSet.empty(store, STRINGS).conj("a");
        Map<String, Object> node = NodeCodec.decode(set.root(), store.get(set.root()).orElseThrow());
        List<?> entries = (List<?>) node.get("e");
        assertThat(entries).hasSize(2);
        assertThat(((Embedded) entries.get(1)).bytes()).containsExactly(0xf6);
        assertThat(node.get("cnt")).isEqualTo(1L);
    }

    @Test
    void equalityIsByRootAndASetEqualsTheMapOfNulls() {
        NodeStore store = NodeStore.inMemory();
        PSet<Integer> a = PSet.empty(store, INTS).conj(1).conj(2).conj(3);
        PSet<Integer> b = PSet.empty(store, INTS).conj(3).conj(1).conj(2).conj(9).disj(9);
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a.disj(1)).isNotEqualTo(b);
        PSet<Integer> loaded = PSet.load(store, a.root(), INTS);
        assertThat(loaded.count()).isEqualTo(3);
        assertThat(loaded.contains(2)).isTrue();
        assertThat(loaded.toSet()).isEqualTo(a.toSet());
    }

    @Test
    void randomOperationsMatchAHashSet() {
        NodeStore store = NodeStore.inMemory();
        Random random = new Random(42);
        Set<Integer> model = new HashSet<>();
        PSet<Integer> set = PSet.empty(store, INTS);
        for (int i = 0; i < 10_000; i++) {
            int element = random.nextInt(2000);
            if (random.nextBoolean()) {
                model.add(element);
                set = set.conj(element);
            } else {
                model.remove(element);
                set = set.disj(element);
            }
        }
        assertThat(set.count()).isEqualTo(model.size());
        assertThat(set.toSet()).isEqualTo(model);
    }

    @Test
    void diffAndMergeAreElementWise() {
        NodeStore store = NodeStore.inMemory();
        PSet<String> base = PSet.empty(store, STRINGS).conj("a").conj("b").conj("c");
        PSet<String> mine = base.conj("d").disj("a");
        PSet<String> theirs = base.conj("e").disj("b").conj("d");
        PSet.SetDiff<String> diff = mine.diff(base);
        assertThat(diff.added()).containsExactly("d");
        assertThat(diff.removed()).containsExactly("a");
        assertThat(base.diff(base).isEmpty()).isTrue();
        PSet<String> merged = mine.merge3(base, theirs);
        assertThat(merged.toSet()).containsExactlyInAnyOrder("c", "d", "e");
        assertThat(merged).isEqualTo(theirs.merge3(base, mine));
    }

    @Test
    void editorBatchesChanges() {
        NodeStore store = NodeStore.inMemory();
        PSet<Integer> base = PSet.empty(store, INTS).conj(1).conj(2);
        SetEditor<Integer> editor = base.editor();
        for (int i = 3; i < 100; i++) {
            editor.conj(i);
        }
        editor.disj(1);
        assertThat(editor.contains(50)).isTrue();
        assertThat(editor.count()).isEqualTo(98);
        SetEditor.Result<Integer> result = editor.done();
        assertThat(result.value().count()).isEqualTo(98);
        assertThat(result.nodes()).isNotEmpty();
        assertThat(result.nodes()).containsKey(result.value().root());
        assertThat(base.count()).isEqualTo(2);
    }
}
