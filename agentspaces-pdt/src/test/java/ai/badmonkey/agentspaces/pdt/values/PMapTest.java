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

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeUnavailableException;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** PD-V-01, PD-V-04, PD-V-06, PD-V-08: the operations of {@link PMap}, lazy roots, loading, and equality. */
class PMapTest {

    private static final Codec<String> STRINGS = Codec.of(String.class);
    private static final Codec<Integer> INTS = Codec.of(Integer.class);

    @Test
    void assocGetDissocAndCountNeverChangeTheReceiver() {
        NodeStore store = NodeStore.inMemory();
        PMap<String, Integer> empty = PMap.empty(store, STRINGS, INTS);
        PMap<String, Integer> one = empty.assoc("a", 1);
        PMap<String, Integer> two = one.assoc("b", 2);
        PMap<String, Integer> replaced = two.assoc("a", 10);
        PMap<String, Integer> less = replaced.dissoc("b");
        assertThat(empty.count()).isZero();
        assertThat(empty.get("a")).isEmpty();
        assertThat(one.get("a")).contains(1);
        assertThat(one.count()).isEqualTo(1);
        assertThat(two.get("b")).contains(2);
        assertThat(two.count()).isEqualTo(2);
        assertThat(replaced.get("a")).contains(10);
        assertThat(replaced.count()).isEqualTo(2);
        assertThat(two.get("a")).contains(1);
        assertThat(less.get("b")).isEmpty();
        assertThat(less.count()).isEqualTo(1);
        assertThat(replaced.containsKey("b")).isTrue();
        assertThat(less.containsKey("b")).isFalse();
        assertThat(two.assoc("a", 1)).isSameAs(two);
        assertThat(two.dissoc("zz")).isSameAs(two);
        assertThat(empty.dissoc("a")).isSameAs(empty);
        assertThat(empty.isEmpty()).isTrue();
    }

    @Test
    void nullKeysAndValuesAreRefused() {
        PMap<String, Integer> map = PMap.empty(NodeStore.inMemory(), STRINGS, INTS);
        assertThatThrownBy(() -> map.assoc(null, 1)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> map.assoc("a", null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> map.get(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void rootIsLazyCachedAndWritesEachNodeOnce() {
        NodeStore store = NodeStore.inMemory();
        PMap<String, Integer> map = PMap.empty(store, STRINGS, INTS).assoc("a", 1).assoc("b", 2);
        assertThat(store.size()).isZero();
        String root = map.root();
        assertThat(Cids.parse(root).codec()).isEqualTo(Cids.DAG_CBOR);
        assertThat(store.size()).isEqualTo(1);
        assertThat(map.root()).isEqualTo(root);
        assertThat(map.raw().flush()).isEmpty();
        assertThat(store.size()).isEqualTo(1);
        Map<String, Object> node = NodeCodec.decode(root, store.get(root).orElseThrow());
        assertThat(node.get("cnt")).isEqualTo(2L);
        assertThat(node.get("k")).isEqualTo("m");
        PMap<String, Integer> three = map.assoc("c", 3);
        assertThat(store.size()).isEqualTo(1);
        three.root();
        assertThat(store.size()).isEqualTo(2);
        assertThat(store.complete(three.root())).isTrue();
    }

    @Test
    void emptyMapHasAFixedRoot() {
        PMap<String, Integer> empty = PMap.empty(NodeStore.inMemory(), STRINGS, INTS);
        String root = empty.root();
        byte[] bytes = empty.store().get(root).orElseThrow();
        Map<String, Object> node = NodeCodec.decode(root, bytes);
        assertThat(node.keySet()).containsExactly("c", "d", "e", "k", "n", "cnt");
        assertThat(PMap.empty(NodeStore.inMemory(), INTS, INTS).root()).isEqualTo(root);
    }

    @Test
    void equalityIsByRootAcrossStoresAndCodecs() {
        PMap<String, Integer> a = PMap.empty(NodeStore.inMemory(), STRINGS, INTS).assoc("x", 1).assoc("y", 2);
        PMap<String, Integer> b = PMap.empty(NodeStore.inMemory(), STRINGS, INTS).assoc("y", 2).assoc("x", 1);
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a.assoc("z", 3)).isNotEqualTo(b);
        assertThat(a.diff(b).isEmpty()).isTrue();
    }

    @Test
    void loadReadsTheCountFromTheRootAndNodesOnDemand() {
        NodeStore store = NodeStore.inMemory();
        Map<Integer, Integer> model = new HashMap<>();
        Editor<Integer, Integer> editor = PMap.empty(store, INTS, INTS).editor();
        Random random = new Random(42);
        for (int i = 0; i < 5000; i++) {
            int key = random.nextInt(100_000);
            editor.assoc(key, i);
            model.put(key, i);
        }
        PMap<Integer, Integer> built = editor.done().value();
        PMap<Integer, Integer> loaded = PMap.load(store, built.root(), INTS, INTS);
        assertThat(loaded.count()).isEqualTo(model.size());
        for (Map.Entry<Integer, Integer> entry : model.entrySet()) {
            assertThat(loaded.get(entry.getKey())).contains(entry.getValue());
        }
        assertThat(loaded.get(-1)).isEmpty();
        assertThat(loaded).isEqualTo(built);
        PMap<Integer, Integer> derived = loaded.assoc(-1, -1).dissoc(model.keySet().iterator().next());
        assertThat(derived.count()).isEqualTo(model.size());
        assertThat(derived.root()).isNotEqualTo(built.root());
        assertThat(store.complete(derived.root())).isTrue();
    }

    @Test
    void loadRefusesAMissingOrWrongRoot() {
        NodeStore store = NodeStore.inMemory();
        String absent = Cids.of(new byte[] {1}, Cids.DAG_CBOR);
        assertThatThrownBy(() -> PMap.load(store, absent, STRINGS, INTS))
                .isInstanceOf(NodeUnavailableException.class)
                .hasMessageContaining(absent);
        String vector = PVector.empty(store, INTS).conj(1).root();
        assertThatThrownBy(() -> PMap.load(store, vector, STRINGS, INTS))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("expected a map node");
    }

    @Test
    void iterationIsInSlotOrderAndStable() {
        NodeStore store = NodeStore.inMemory();
        PMap<String, Integer> a = PMap.empty(store, STRINGS, INTS);
        PMap<String, Integer> b = PMap.empty(store, STRINGS, INTS);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            keys.add("key" + i);
        }
        for (String key : keys) {
            a = a.assoc(key, key.length());
        }
        for (int i = keys.size() - 1; i >= 0; i--) {
            b = b.assoc(keys.get(i), keys.get(i).length());
        }
        assertThat(a.keys()).isEqualTo(b.keys());
        assertThat(a.keys()).hasSize(200).containsExactlyInAnyOrderElementsOf(keys);
        assertThat(a.toMap()).containsOnlyKeys(keys);
        assertThat(PMap.load(store, a.root(), STRINGS, INTS).keys()).isEqualTo(a.keys());
        int seen = 0;
        for (Map.Entry<String, Integer> entry : a) {
            assertThat(entry.getValue()).isEqualTo(entry.getKey().length());
            seen++;
        }
        assertThat(seen).isEqualTo(200);
    }

    @Test
    void updateAndUpdateInWorkOnNestedPlainValues() {
        NodeStore store = NodeStore.inMemory();
        PMap<String, Integer> counts = PMap.empty(store, STRINGS, INTS).assoc("a", 1);
        assertThat(counts.update("a", v -> v + 1).get("a")).contains(2);
        assertThat(counts.update("b", v -> v == null ? 100 : v).get("b")).contains(100);
        assertThat(counts.update("a", v -> null).containsKey("a")).isFalse();

        Codec<Object> objects = Codec.of(Object.class);
        PMap<String, Object> nested = PMap.empty(store, STRINGS, objects)
                .assoc("doc", Map.of("title", "t", "tags", List.of("x", "y")));
        PMap<String, Object> updated = nested.updateIn(List.of("doc", "tags", 1), v -> "z");
        Map<?, ?> doc = (Map<?, ?>) updated.get("doc").orElseThrow();
        assertThat(doc.get("tags")).isEqualTo(List.of("x", "z"));
        assertThat(doc.get("title")).isEqualTo("t");
        PMap<String, Object> created = nested.updateIn(List.of("new", "deep", "leaf"), v -> 1);
        assertThat(created.get("new")).contains(Map.of("deep", Map.of("leaf", 1)));
        assertThat(nested.updateIn(List.of("doc"), v -> "flat").get("doc")).contains("flat");
        assertThatThrownBy(() -> nested.updateIn(List.of(), v -> v)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void randomOperationsMatchAHashMap() {
        NodeStore store = NodeStore.inMemory();
        Random random = new Random(42);
        Map<Integer, Integer> model = new HashMap<>();
        PMap<Integer, Integer> map = PMap.empty(store, INTS, INTS);
        for (int i = 0; i < 20_000; i++) {
            int key = random.nextInt(3000);
            if (random.nextInt(3) == 0) {
                model.remove(key);
                map = map.dissoc(key);
            } else {
                model.put(key, i);
                map = map.assoc(key, i);
            }
            if (i % 997 == 0) {
                assertThat(map.count()).isEqualTo(model.size());
            }
        }
        assertThat(map.count()).isEqualTo(model.size());
        assertThat(map.toMap()).isEqualTo(model);
        PMap<Integer, Integer> straight = PMap.empty(store, INTS, INTS);
        for (Map.Entry<Integer, Integer> entry : model.entrySet()) {
            straight = straight.assoc(entry.getKey(), entry.getValue());
        }
        assertThat(map.root()).isEqualTo(straight.root());
    }
}
