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
import ai.badmonkey.agentspaces.pdt.Embedded;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PD-N-04 and PD-V-01 at the values layer (srad/agentspaces-pdt.md §6.7 threat 2): a
 * stored tree that is well formed node by node but wrong as a tree, deeper than the
 * bounds, narrower than its root claims, or laid out where the key digests do not
 * put it, ends in a {@link NodeFormatException} naming the CID and the rule, never in
 * a stack overflow, an index error, or a second root for the same value.
 */
class MalformedTreeTest {

    private static final Codec<String> STRINGS = Codec.of(String.class);
    private static final Codec<Object> OBJECTS = Codec.of(Object.class);

    private static String put(NodeStore store, Map<String, Object> node) {
        return store.put(NodeCodec.encode(node), Cids.DAG_CBOR);
    }

    private static Map<String, Object> mapNode(long datamap, long nodemap, List<Object> entries, List<Object> children, Long cnt) {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("k", "m");
        node.put("d", datamap);
        node.put("n", nodemap);
        node.put("e", entries);
        node.put("c", children);
        if (cnt != null) {
            node.put("cnt", cnt);
        }
        return node;
    }

    private static Embedded embed(Object value) {
        return new Embedded(OBJECTS.encode(value));
    }

    private static int slotOf(String key, int level) {
        return ChampNode.index(KeyHasher.SHA256.hash(STRINGS.encode(key)), level);
    }

    @Test
    void aMapChainDeeperThanTheCollisionLevelIsRefusedNotOverflowed() {
        NodeStore store = NodeStore.inMemory();
        String leaf = put(store, mapNode(1L << slotOf("a", 0), 0, List.of(embed("a"), embed(1)), List.of(), null));
        String next = leaf;
        for (int i = 0; i < 100; i++) {
            next = put(store, mapNode(0, 1, List.of(), List.of(new Link(next)), null));
        }
        String root = put(store, mapNode(0, 1, List.of(), List.of(new Link(next)), 1L));
        PMap<String, Object> map = PMap.load(store, root, STRINGS, OBJECTS);
        assertThat(map.count()).isEqualTo(1);
        assertThatThrownBy(map::keys).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("map node at level " + ChampNode.COLLISION_LEVEL);
        assertThatThrownBy(map::toMap).isInstanceOf(NodeFormatException.class);
        assertThatThrownBy(map::depth).isInstanceOf(NodeFormatException.class);
        PMap<String, Object> empty = PMap.empty(store, STRINGS, OBJECTS);
        assertThatThrownBy(() -> map.diff(empty)).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("level " + ChampNode.COLLISION_LEVEL);
        assertThatThrownBy(() -> empty.diff(map)).isInstanceOf(NodeFormatException.class);
        assertThatThrownBy(() -> store.complete(root)).isInstanceOf(NodeFormatException.class).hasMessageContaining("tree deeper than 64 levels");
    }

    @Test
    void aSpilledBlobChainDeeperThanSixtyFourLevelsIsRefusedNotOverflowed() {
        NodeStore store = NodeStore.inMemory();
        String chunk = store.put(new byte[] {7}, Cids.RAW);
        String next = chunk;
        for (int i = 0; i < 100; i++) {
            Map<String, Object> b = new LinkedHashMap<>();
            b.put("k", "b");
            b.put("h", (long) (i + 1));
            b.put("c", List.of(new Link(next)));
            b.put("z", List.of(1L));
            next = put(store, b);
        }
        String root = put(store, mapNode(1L << slotOf("blob", 0), 0,
                List.of(embed("blob"), new Embedded(Spill.linkItem(next))), List.of(), 1L));
        PMap<String, Object> map = PMap.load(store, root, STRINGS, OBJECTS);
        assertThatThrownBy(() -> map.get("blob")).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("tree deeper than 64 levels");
        assertThatThrownBy(() -> store.complete(root)).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("tree deeper than 64 levels");
    }

    @Test
    void aSpilledMapChainDeeperThanSixtyFourLevelsIsRefusedNotOverflowed() {
        NodeStore store = NodeStore.inMemory();
        // Each level is a one-entry map root whose value is the next level, a chain of spilled maps.
        String next = put(store, mapNode(1L << slotOf("v", 0), 0, List.of(embed("v"), embed(0)), List.of(), 1L));
        for (int i = 0; i < 100; i++) {
            next = put(store, mapNode(1L << slotOf("v", 0), 0, List.of(embed("v"), new Embedded(Spill.linkItem(next))), List.of(), 1L));
        }
        PMap<String, Object> map = PMap.load(store, next, STRINGS, OBJECTS);
        assertThatThrownBy(() -> map.get("v")).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("spilled values nested deeper than 64 levels");
    }

    @Test
    void anInlineKeyOutsideTheSlotItsDigestNamesIsRefusedOnLoad() {
        NodeStore store = NodeStore.inMemory();
        int named = slotOf("a", 1);
        int wrong = (named + 1) % 32;
        String child = put(store, mapNode(1L << wrong, 0, List.of(embed("a"), embed(1)), List.of(), null));
        String root = put(store, mapNode(0, 1L << 7, List.of(), List.of(new Link(child)), 1L));
        PMap<String, Object> map = PMap.load(store, root, STRINGS, OBJECTS);
        assertThatThrownBy(map::keys).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("sits in slot " + wrong + " at level 1 where its key digest names slot " + named)
                .hasMessageContaining(child);
        // The root's own inline keys are checked at level 0.
        int rootNamed = slotOf("a", 0);
        String badRoot = put(store, mapNode(1L << ((rootNamed + 1) % 32), 0, List.of(embed("a"), embed(1)), List.of(), 1L));
        assertThatThrownBy(() -> PMap.load(store, badRoot, STRINGS, OBJECTS)).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("at level 0");
        // A correctly placed child loads and the key is found.
        String good = put(store, mapNode(1L << named, 0, List.of(embed("a"), embed(1)), List.of(), null));
        String goodRoot = put(store, mapNode(0, 1L << slotOf("a", 0), List.of(), List.of(new Link(good)), 1L));
        PMap<String, Object> fine = PMap.load(store, goodRoot, STRINGS, OBJECTS);
        assertThat(fine.keys()).containsExactly("a");
        assertThat(fine.get("a")).contains(1);
    }

    @Test
    void aCountOnAChildOrACollisionNodeAboveTheCollisionLevelIsRefused() {
        NodeStore store = NodeStore.inMemory();
        String counted = put(store, mapNode(1L << slotOf("a", 1), 0, List.of(embed("a"), embed(1)), List.of(), 1L));
        String root = put(store, mapNode(0, 1L << slotOf("a", 0), List.of(), List.of(new Link(counted)), 1L));
        assertThatThrownBy(() -> PMap.load(store, root, STRINGS, OBJECTS).keys()).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("'cnt' on a node that is not a map root").hasMessageContaining(counted);
        Map<String, Object> mc = new LinkedHashMap<>();
        mc.put("k", "mc");
        mc.put("e", List.of(embed("a"), embed(1)));
        String collision = put(store, mc);
        String over = put(store, mapNode(0, 1L << slotOf("a", 0), List.of(), List.of(new Link(collision)), 1L));
        assertThatThrownBy(() -> PMap.load(store, over, STRINGS, OBJECTS).keys()).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("collision node at level 1");
    }

    @Test
    void aVectorRootWhoseShiftDoesNotMatchItsCountIsRefused() {
        NodeStore store = NodeStore.inMemory();
        Map<String, Object> vr = new LinkedHashMap<>();
        vr.put("k", "vr");
        vr.put("n", 3L);
        vr.put("s", 25L);
        vr.put("r", null);
        vr.put("t", List.of(embed(1), embed(2), embed(3)));
        String root = put(store, vr);
        assertThatThrownBy(() -> PVector.load(store, root, OBJECTS)).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("shift 25 where the count 3 implies 5");
        assertThat(VecRoot.shiftFor(0)).isEqualTo(5);
        assertThat(VecRoot.shiftFor(1056)).isEqualTo(5);
        assertThat(VecRoot.shiftFor(1057)).isEqualTo(10);
        assertThat(VecRoot.shiftFor(32800)).isEqualTo(10);
        assertThat(VecRoot.shiftFor(32801)).isEqualTo(15);
        // The real shapes at both boundaries decode.
        VectorEditor<Object> editor = PVector.empty(store, OBJECTS).editor();
        for (int i = 0; i < 32801; i++) {
            editor.conj(i);
        }
        PVector<Object> grown = editor.done().value();
        assertThat(PVector.load(store, grown.root(), OBJECTS).nth(32800)).isEqualTo(32800);
        PVector<Object> shrunk = grown.pop();
        assertThat(PVector.load(store, shrunk.root(), OBJECTS).count()).isEqualTo(32800);
    }

    @Test
    void aVectorTrieNarrowerThanItsCountIsRefusedByName() {
        NodeStore store = NodeStore.inMemory();
        List<Object> thin = new ArrayList<>();
        thin.add(embed(0));
        Map<String, Object> leaf = new LinkedHashMap<>();
        leaf.put("k", "v");
        leaf.put("e", thin);
        String thinLeaf = put(store, leaf);
        Map<String, Object> interior = new LinkedHashMap<>();
        interior.put("k", "v");
        interior.put("c", List.of(new Link(thinLeaf)));
        String trie = put(store, interior);
        List<Object> tail = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            tail.add(embed(i));
        }
        Map<String, Object> vr = new LinkedHashMap<>();
        vr.put("k", "vr");
        vr.put("n", 40L);
        vr.put("s", 5L);
        vr.put("r", new Link(trie));
        vr.put("t", tail);
        PVector<Object> vector = PVector.load(store, put(store, vr), OBJECTS);
        assertThatThrownBy(() -> vector.nth(5)).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("vector leaf with 1 items").hasMessageContaining(thinLeaf);
        assertThatThrownBy(vector::toList).isInstanceOf(NodeFormatException.class);

        // A full leaf under an interior node that is one child short of the count.
        List<Object> full = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            full.add(embed(i));
        }
        leaf.put("e", full);
        interior.put("c", List.of(new Link(put(store, leaf))));
        String narrow = put(store, interior);
        vr.put("n", 65L);
        vr.put("r", new Link(narrow));
        vr.put("t", List.of(embed(64)));
        PVector<Object> short65 = PVector.load(store, put(store, vr), OBJECTS);
        assertThat(short65.nth(3)).isEqualTo(3);
        assertThatThrownBy(() -> short65.nth(40)).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("vector node has 1 slots where index 1 was expected").hasMessageContaining(narrow);
    }
}
