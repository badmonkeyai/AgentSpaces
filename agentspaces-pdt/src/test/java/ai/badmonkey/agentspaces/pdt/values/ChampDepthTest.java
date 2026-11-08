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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PD-V-01 and PD-N-04: the depth of a CHAMP trie of 2^16 keys stays far under the
 * 64-level bound, and a test-only hasher that gives several keys one digest drives
 * them through all 52 levels into a collision node, which inlines again when one
 * key remains.
 */
class ChampDepthTest {

    private static final Codec<String> STRINGS = Codec.of(String.class);
    private static final Codec<Integer> INTS = Codec.of(Integer.class);

    /** Keys that start with "c" share one digest; every other key hashes normally. */
    private static final KeyHasher COLLIDING = bytes -> {
        if (bytes.length > 1 && bytes[1] == 'c') {
            return new byte[32];
        }
        return KeyHasher.SHA256.hash(bytes);
    };

    @Test
    void indexReadsFiveBitsBigEndian() {
        byte[] digest = new byte[32];
        digest[0] = (byte) 0b10110010;
        digest[1] = (byte) 0b11000000;
        assertThat(ChampNode.index(digest, 0)).isEqualTo(0b10110);
        assertThat(ChampNode.index(digest, 1)).isEqualTo(0b01011);
        assertThat(ChampNode.index(digest, 2)).isEqualTo(0);
        digest[31] = (byte) 0b00000001;
        assertThat(ChampNode.index(digest, 51)).isEqualTo(0b10000);
        assertThat(ChampNode.COLLISION_LEVEL).isEqualTo(52);
    }

    @Test
    void sixtyFourThousandKeysStayShallow() {
        NodeStore store = NodeStore.inMemory();
        Editor<Integer, Integer> editor = PMap.empty(store, INTS, INTS).editor();
        for (int i = 0; i < 1 << 16; i++) {
            editor.assoc(i, i);
        }
        PMap<Integer, Integer> map = editor.done().value();
        assertThat(map.count()).isEqualTo(1 << 16);
        int depth = map.depth();
        assertThat(depth).isLessThanOrEqualTo(NodeStore.MAX_DEPTH);
        assertThat(depth).isLessThanOrEqualTo(8);
        assertThat(store.complete(map.root())).isTrue();
        assertThat(map.get(12345)).contains(12345);
    }

    @Test
    void equalDigestsReachTheCollisionNodeAndInlineBack() {
        NodeStore store = NodeStore.inMemory();
        PMap<String, Integer> map = PMap.withHasher(Backing.of(store), COLLIDING, STRINGS, INTS);
        for (int i = 0; i < 100; i++) {
            map = map.assoc("n" + i, i);
        }
        List<String> colliding = List.of("c-a", "c-b", "c-c", "c-d", "c-e");
        for (int i = 0; i < colliding.size(); i++) {
            map = map.assoc(colliding.get(i), 1000 + i);
        }
        assertThat(map.count()).isEqualTo(105);
        assertThat(map.depth()).isEqualTo(ChampNode.COLLISION_LEVEL + 1);
        String root = map.root();
        List<String> kinds = new ArrayList<>();
        for (String cid : store.reachable(root)) {
            kinds.add(NodeCodec.kindOf(store.get(cid).orElseThrow()));
        }
        assertThat(kinds).contains(Kind.MC);
        for (int i = 0; i < colliding.size(); i++) {
            assertThat(map.get(colliding.get(i))).contains(1000 + i);
        }
        assertThat(map.get("c-z")).isEmpty();
        assertThat(map.keys()).containsAll(colliding);

        // Overwrite inside the collision node and reload through the store.
        PMap<String, Integer> changed = map.assoc("c-c", 7);
        assertThat(changed.get("c-c")).contains(7);
        assertThat(map.get("c-c")).contains(1002);
        PMap<String, Integer> reloaded = PMap.withHasher(Backing.of(store), COLLIDING, STRINGS, INTS);
        reloaded = new PMap<>(RawMap.load(Backing.of(store), COLLIDING, changed.root()), STRINGS, INTS);
        assertThat(reloaded.get("c-a")).contains(1000);
        assertThat(reloaded.get("c-c")).contains(7);
        assertThat(reloaded.count()).isEqualTo(105);

        // Remove all but one colliding key: the collision node inlines and the path collapses.
        PMap<String, Integer> shrunk = map;
        for (String key : colliding.subList(1, colliding.size())) {
            shrunk = shrunk.dissoc(key);
        }
        assertThat(shrunk.count()).isEqualTo(101);
        assertThat(shrunk.depth()).isLessThan(10);
        assertThat(shrunk.get("c-a")).contains(1000);
        PMap<String, Integer> direct = PMap.withHasher(Backing.of(store), COLLIDING, STRINGS, INTS);
        for (int i = 0; i < 100; i++) {
            direct = direct.assoc("n" + i, i);
        }
        direct = direct.assoc("c-a", 1000);
        assertThat(shrunk.root()).isEqualTo(direct.root());

        // The collision node's kind is on the wire as "mc" with keys in byte order.
        Map<String, Object> decoded = null;
        for (String cid : store.reachable(root)) {
            byte[] bytes = store.get(cid).orElseThrow();
            if (Kind.MC.equals(NodeCodec.kindOf(bytes))) {
                decoded = NodeCodec.decode(cid, bytes);
            }
        }
        assertThat(decoded).isNotNull();
        assertThat(((List<?>) decoded.get("e"))).hasSize(10);
    }
}
