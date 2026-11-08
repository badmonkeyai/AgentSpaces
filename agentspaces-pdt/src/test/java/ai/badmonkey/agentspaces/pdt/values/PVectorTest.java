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

import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** PD-V-03, PD-V-04: {@link PVector} operations, the tail rule, path copying, loading, and equality. */
class PVectorTest {

    private static final Codec<Integer> INTS = Codec.of(Integer.class);
    private static final Codec<String> STRINGS = Codec.of(String.class);

    private static PVector<Integer> range(NodeStore store, int n) {
        VectorEditor<Integer> editor = PVector.empty(store, INTS).editor();
        for (int i = 0; i < n; i++) {
            editor.conj(i);
        }
        return editor.done().value();
    }

    @Test
    void conjNthPopAssocAndCountNeverChangeTheReceiver() {
        PVector<String> empty = PVector.empty(NodeStore.inMemory(), STRINGS);
        PVector<String> one = empty.conj("a");
        PVector<String> two = one.conj("b");
        PVector<String> changed = two.assoc(0, "A");
        PVector<String> popped = changed.pop();
        assertThat(empty.count()).isZero();
        assertThat(empty.isEmpty()).isTrue();
        assertThat(empty.peek()).isEmpty();
        assertThat(one.nth(0)).isEqualTo("a");
        assertThat(two.nth(1)).isEqualTo("b");
        assertThat(two.peek()).contains("b");
        assertThat(changed.nth(0)).isEqualTo("A");
        assertThat(two.nth(0)).isEqualTo("a");
        assertThat(popped.count()).isEqualTo(1);
        assertThat(popped.toList()).containsExactly("A");
        assertThat(changed.toList()).containsExactly("A", "b");
        assertThat(two.assoc(1, "b")).isSameAs(two);
        assertThat(two.update(1, s -> s + "!").nth(1)).isEqualTo("b!");
        assertThatThrownBy(() -> two.nth(2)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> two.assoc(5, "x")).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(empty::pop).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> two.conj(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void conjAndPopWriteOneRootNodeWhileTheTailHasRoom() {
        NodeStore store = NodeStore.inMemory();
        PVector<Integer> thousand = range(store, 1000);
        assertThat(thousand.node().tailoff()).isEqualTo(992);
        PVector<Integer> more = thousand.conj(1000);
        Map<String, byte[]> written = more.flush();
        assertThat(written).hasSize(1);
        assertThat(NodeCodec.kindOf(written.values().iterator().next())).isEqualTo(Kind.VR);
        assertThat(thousand.pop().flush()).hasSize(1);
        assertThat(thousand.assoc(995, -1).flush()).hasSize(1);
        assertThat(more.nth(1000)).isEqualTo(1000);
        assertThat(more.count()).isEqualTo(1001);
    }

    @Test
    void aFullTailPushesALeafAndAPath() {
        NodeStore store = NodeStore.inMemory();
        PVector<Integer> v = range(store, 1024);
        assertThat(v.node().tail).hasSize(32);
        Map<String, byte[]> written = v.conj(1024).flush();
        // A new leaf, the copied trie root (shift 5), and the vector root.
        assertThat(written).hasSize(3);
        PVector<Integer> grown = range(store, 1056);
        assertThat(grown.node().shift).isEqualTo(5);
        Map<String, byte[]> overflow = grown.conj(1056).flush();
        // The leaf, its new path node, the new two-child trie root, and the vector root.
        assertThat(overflow).hasSize(4);
        PVector<Integer> taller = grown.conj(1056);
        assertThat(taller.node().shift).isEqualTo(10);
        assertThat(taller.depth()).isEqualTo(4);
        assertThat(taller.nth(1056)).isEqualTo(1056);
        assertThat(taller.nth(0)).isEqualTo(0);
        assertThat(taller.pop().node().shift).isEqualTo(5);
        assertThat(taller.pop().root()).isEqualTo(grown.root());
    }

    @Test
    void assocCopiesAtMostThePath() {
        NodeStore store = NodeStore.inMemory();
        PVector<Integer> v = range(store, 1000);
        int shift = v.node().shift;
        // The path: shift / 5 trie nodes above the leaf, the leaf, and the vector root.
        assertThat(v.assoc(5, -1).flush()).hasSize(shift / 5 + 2);
        PVector<Integer> big = range(store, 40_000);
        assertThat(big.node().shift).isEqualTo(15);
        assertThat(big.assoc(12_345, -1).flush()).hasSize(big.node().shift / 5 + 2);
        assertThat(big.assoc(12_345, -1).nth(12_345)).isEqualTo(-1);
        assertThat(big.nth(12_345)).isEqualTo(12_345);
    }

    @Test
    void loadReadsNodesOnDemandAndEqualityIsByRoot() {
        NodeStore store = NodeStore.inMemory();
        PVector<Integer> built = range(store, 5000);
        PVector<Integer> loaded = PVector.load(store, built.root(), INTS);
        assertThat(loaded.count()).isEqualTo(5000);
        assertThat(loaded.nth(4321)).isEqualTo(4321);
        assertThat(loaded.nth(4999)).isEqualTo(4999);
        assertThat(loaded).isEqualTo(built);
        assertThat(loaded.hashCode()).isEqualTo(built.hashCode());
        assertThat(loaded.toList()).isEqualTo(built.toList());
        PVector<Integer> other = PVector.empty(NodeStore.inMemory(), INTS);
        for (int i = 0; i < 5000; i++) {
            other = other.conj(i);
        }
        assertThat(other).isEqualTo(built);
        assertThat(other.pop()).isNotEqualTo(built);
        assertThat(store.complete(loaded.conj(1).root())).isTrue();
    }

    @Test
    void randomOperationsMatchAnArrayList() {
        NodeStore store = NodeStore.inMemory();
        Random random = new Random(42);
        List<Integer> model = new ArrayList<>();
        PVector<Integer> v = PVector.empty(store, INTS);
        for (int i = 0; i < 30_000; i++) {
            int op = random.nextInt(10);
            if (op < 6 || model.isEmpty()) {
                model.add(i);
                v = v.conj(i);
            } else if (op < 8) {
                model.remove(model.size() - 1);
                v = v.pop();
            } else {
                int index = random.nextInt(model.size());
                model.set(index, -i);
                v = v.assoc(index, -i);
            }
            if (i % 5000 == 0) {
                assertThat(v.toList()).isEqualTo(model);
            }
        }
        assertThat(v.count()).isEqualTo(model.size());
        assertThat(v.toList()).isEqualTo(model);
        PVector<Integer> straight = PVector.empty(store, INTS);
        for (int item : model) {
            straight = straight.conj(item);
        }
        assertThat(v.root()).isEqualTo(straight.root());
        PVector<Integer> emptied = v;
        while (!emptied.isEmpty()) {
            emptied = emptied.pop();
        }
        assertThat(emptied.root()).isEqualTo(PVector.empty(store, INTS).root());
    }

    @Test
    void diffReportsIndexRanges() {
        NodeStore store = NodeStore.inMemory();
        PVector<Integer> base = range(store, 1000);
        PVector<Integer> mine = base.assoc(3, -3).assoc(700, -700).assoc(701, -701).conj(1000).conj(1001);
        VectorDiff diff = mine.diff(base);
        assertThat(diff.changed()).containsExactly(new VectorDiff.Range(3, 4), new VectorDiff.Range(700, 702));
        assertThat(diff.appended()).isEqualTo(new VectorDiff.Range(1000, 1002));
        assertThat(diff.removed().isEmpty()).isTrue();
        assertThat(diff.nodesVisited()).isLessThan(20);
        VectorDiff shorter = base.pop().pop().diff(base);
        assertThat(shorter.changed()).isEmpty();
        assertThat(shorter.removed()).isEqualTo(new VectorDiff.Range(998, 1000));
        assertThat(base.diff(base).isEmpty()).isTrue();
        PVector<Integer> tall = range(store, 40_000);
        VectorDiff grown = tall.diff(base);
        assertThat(grown.changed()).isEmpty();
        assertThat(grown.appended()).isEqualTo(new VectorDiff.Range(1000, 40_000));
        VectorDiff edited = tall.assoc(20_000, -1).diff(tall);
        assertThat(edited.changed()).containsExactly(new VectorDiff.Range(20_000, 20_001));
        assertThat(edited.nodesVisited()).isLessThanOrEqualTo(2 * 5);
    }
}
