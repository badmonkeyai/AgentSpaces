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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.Embedded;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** PD-N-05: values over 1 KiB spill to {@code x} nodes, maps, arrays, and byte strings over 4 KiB become sub-trees, and reads reassemble the same bytes. */
class SpillTest {

    private static final Codec<String> STRINGS = Codec.of(String.class);
    private static final Codec<Object> OBJECTS = Codec.of(Object.class);
    private static final Codec<byte[]> BYTES = Codec.of(byte[].class);
    private static final CborCodec CBOR = CborCodec.defaultCodec();

    private static Set<String> kinds(NodeStore store, String root) {
        Set<String> kinds = new HashSet<>();
        for (String cid : store.reachable(root)) {
            byte[] bytes = store.get(cid).orElseThrow();
            kinds.add(Cids.parse(cid).codec() == Cids.RAW ? "raw" : NodeCodec.kindOf(bytes));
        }
        return kinds;
    }

    private static Map<String, Object> bigMap(int entries, Random random) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < entries; i++) {
            map.put("field" + i, random.nextInt());
        }
        return map;
    }

    @Test
    void linkItemsRoundTrip() {
        String cid = Cids.of(new byte[] {1, 2, 3}, Cids.DAG_CBOR);
        byte[] item = Spill.linkItem(cid);
        assertThat(item).hasSize(41);
        assertThat(item[0] & 0xff).isEqualTo(0xd8);
        assertThat(item[1]).isEqualTo((byte) 0x2a);
        assertThat(Spill.isLinkItem(item)).isTrue();
        assertThat(Spill.linkCid(item)).isEqualTo(cid);
        assertThat(Spill.isLinkItem(CBOR.toBytes("text"))).isFalse();
        assertThat(NodeCodec.skip(item, 0)).isEqualTo(41);
    }

    @Test
    void aValueOverOneKiBLeavesItsNode() {
        NodeStore store = NodeStore.inMemory();
        String small = "s".repeat(1000);
        String large = "L".repeat(2000);
        PMap<String, String> map = PMap.empty(store, STRINGS, STRINGS).assoc("small", small).assoc("large", large);
        String root = map.root();
        assertThat(kinds(store, root)).containsExactlyInAnyOrder(Kind.M, Kind.X);
        assertThat(store.size()).isEqualTo(2);
        Map<String, Object> node = NodeCodec.decode(root, store.get(root).orElseThrow());
        List<?> entries = (List<?>) node.get("e");
        int links = 0;
        for (int i = 1; i < entries.size(); i += 2) {
            if (Spill.isLinkItem(((Embedded) entries.get(i)).bytes())) {
                links++;
            }
        }
        assertThat(links).isEqualTo(1);
        assertThat(map.get("large")).contains(large);
        assertThat(map.get("small")).contains(small);
        PMap<String, String> loaded = PMap.load(store, root, STRINGS, STRINGS);
        assertThat(loaded.get("large")).contains(large);
        assertThat(loaded.toMap()).isEqualTo(map.toMap());
        // The same large value under two keys is one x node.
        PMap<String, String> twice = map.assoc("again", large);
        assertThat(twice.raw().flush()).hasSize(1);
    }

    @Test
    void aMapOverFourKiBBecomesASubTreeAndReadsBackByteForByte() {
        NodeStore store = NodeStore.inMemory();
        Random random = new Random(42);
        Map<String, Object> doc = bigMap(600, random);
        byte[] original = CBOR.toBytes(doc);
        assertThat(original.length).isGreaterThan(Spill.TREE_LIMIT);
        assertThat(original[0] & 0xff).isEqualTo(0xbf);
        PMap<String, Object> map = PMap.empty(store, STRINGS, OBJECTS).assoc("doc", doc);
        String root = map.root();
        assertThat(kinds(store, root)).containsExactlyInAnyOrder(Kind.M);
        assertThat(store.size()).isGreaterThan(2);
        Map<String, Object> rootNode = NodeCodec.decode(root, store.get(root).orElseThrow());
        List<?> entries = (List<?>) rootNode.get("e");
        byte[] slot = ((Embedded) entries.get(1)).bytes();
        assertThat(Spill.isLinkItem(slot)).isTrue();
        Map<String, Object> subRoot = NodeCodec.decode(Spill.linkCid(slot), store.get(Spill.linkCid(slot)).orElseThrow());
        assertThat(subRoot.get("cnt")).isEqualTo(600L);

        Object back = map.get("doc").orElseThrow();
        assertThat(back).isEqualTo(doc);
        assertThat(map.valueCodec().encode(back)).isEqualTo(original);
        PMap<String, Object> loaded = PMap.load(store, root, STRINGS, OBJECTS);
        assertThat(loaded.valueCodec().encode(loaded.get("doc").orElseThrow())).isEqualTo(original);

        // A change inside the nested map copies the nested path and the outer path only.
        Set<String> before = store.reachable(root);
        Map<String, Object> changed = new LinkedHashMap<>(doc);
        changed.put("field7", "changed");
        PMap<String, Object> next = map.assoc("doc", changed);
        Set<String> delta = new HashSet<>(store.reachable(next.root()));
        delta.removeAll(before);
        assertThat(delta.size()).isLessThanOrEqualTo(4);
        assertThat(next.get("doc")).contains(changed);
        assertThat(map.get("doc")).contains(doc);
    }

    @Test
    void anArrayOverFourKiBBecomesAVectorSubTree() {
        NodeStore store = NodeStore.inMemory();
        List<Object> items = new ArrayList<>();
        Random random = new Random(42);
        for (int i = 0; i < 2000; i++) {
            items.add(random.nextInt(1_000_000));
        }
        byte[] original = CBOR.toBytes(items);
        assertThat(original.length).isGreaterThan(Spill.TREE_LIMIT);
        PMap<String, Object> map = PMap.empty(store, STRINGS, OBJECTS).assoc("items", items);
        String root = map.root();
        assertThat(kinds(store, root)).containsExactlyInAnyOrder(Kind.M, Kind.VR, Kind.V);
        assertThat(map.get("items")).contains(items);
        assertThat(map.valueCodec().encode(map.get("items").orElseThrow())).isEqualTo(original);
        PMap<String, Object> loaded = PMap.load(store, root, STRINGS, OBJECTS);
        assertThat(loaded.valueCodec().encode(loaded.get("items").orElseThrow())).isEqualTo(original);

        // The same list as a vector element spills the same way.
        PVector<Object> vector = PVector.empty(store, OBJECTS).conj(items).conj("tail");
        assertThat(kinds(store, vector.root())).contains(Kind.VR, Kind.V);
        assertThat(vector.nth(0)).isEqualTo(items);
        assertThat(PVector.load(store, vector.root(), OBJECTS).nth(0)).isEqualTo(items);
    }

    @Test
    void midSizedValuesStayInXNodes() {
        NodeStore store = NodeStore.inMemory();
        Random random = new Random(7);
        Map<String, Object> mid = bigMap(200, random);
        byte[] midBytes = CBOR.toBytes(mid);
        assertThat(midBytes.length).isBetween(Spill.VALUE_LIMIT + 1, Spill.TREE_LIMIT);
        PMap<String, Object> map = PMap.empty(store, STRINGS, OBJECTS).assoc("mid", mid);
        assertThat(kinds(store, map.root())).containsExactlyInAnyOrder(Kind.M, Kind.X);
        assertThat(map.get("mid")).contains(mid);
        // A byte string between 1 KiB and 4 KiB is an x node too.
        byte[] small = new byte[3000];
        random.nextBytes(small);
        PMap<String, byte[]> bytes = PMap.empty(store, STRINGS, BYTES).assoc("b", small);
        assertThat(kinds(store, bytes.root())).containsExactlyInAnyOrder(Kind.M, Kind.X);
        assertThat(bytes.get("b")).contains(small);
    }

    /** PD-N-05, the byte-string half: above 4 KiB a byte string becomes a blob sub-tree of §3.3. */
    @Test
    void aByteStringOverFourKiBBecomesABlobSubTree() {
        NodeStore store = NodeStore.inMemory();
        byte[] oneChunk = new byte[10_000];
        new Random(42).nextBytes(oneChunk);
        PMap<String, byte[]> map = PMap.empty(store, STRINGS, BYTES).assoc("blob", oneChunk);
        String root = map.root();
        assertThat(kinds(store, root)).as("below the minimum chunk size the blob is one raw chunk")
                .containsExactlyInAnyOrder(Kind.M, "raw");
        Map<String, Object> rootNode = NodeCodec.decode(root, store.get(root).orElseThrow());
        byte[] slot = ((Embedded) ((List<?>) rootNode.get("e")).get(1)).bytes();
        assertThat(Spill.isLinkItem(slot)).isTrue();
        assertThat(Spill.linkCid(slot)).isEqualTo(Cids.of(oneChunk, Cids.RAW));
        assertThat(map.get("blob")).contains(oneChunk);
        assertThat(PMap.load(store, root, STRINGS, BYTES).get("blob")).contains(oneChunk);
        assertThat(store.complete(root)).isTrue();

        byte[] big = new byte[300 * 1024];
        new Random(43).nextBytes(big);
        PMap<String, byte[]> wide = map.assoc("big", big);
        String wideRoot = wide.root();
        assertThat(kinds(store, wideRoot)).containsExactlyInAnyOrder(Kind.M, Kind.B, "raw");
        assertThat(wide.get("big")).contains(big);
        assertThat(PMap.load(store, wideRoot, STRINGS, BYTES).get("big")).contains(big);
        assertThat(store.complete(wideRoot)).isTrue();
        // The blob root equals the root PBlob.of gives the same bytes, so the layout is the one of §3.3.
        String blobRoot = Spill.linkCid(((Embedded) ((List<?>) NodeCodec.decode(wideRoot, store.get(wideRoot).orElseThrow())
                .get("e")).get(wideIndex(store, wideRoot, "big"))).bytes());
        assertThat(blobRoot).isEqualTo(ai.badmonkey.agentspaces.pdt.blob.PBlob.of(big, NodeStore.inMemory()).root());

        // An edit of one byte in the big value rewrites one chunk and the path above it, and the old value stays readable.
        byte[] edited = big.clone();
        edited[150_000] ^= 0x5a;
        Set<String> before = store.reachable(wideRoot);
        PMap<String, byte[]> next = wide.assoc("big", edited);
        Set<String> delta = new HashSet<>(store.reachable(next.root()));
        delta.removeAll(before);
        assertThat(delta.size()).isLessThanOrEqualTo(4);
        assertThat(next.get("big")).contains(edited);
        assertThat(wide.get("big")).contains(big);

        // A vector element and a byte string inside a spilled map follow the same rule.
        PVector<byte[]> vector = PVector.empty(store, BYTES).conj(big).conj(oneChunk);
        assertThat(kinds(store, vector.root())).contains(Kind.VR, Kind.B, "raw");
        assertThat(PVector.load(store, vector.root(), BYTES).nth(0)).isEqualTo(big);
        Map<String, Object> doc = new LinkedHashMap<>(bigMap(600, new Random(1)));
        doc.put("payload", oneChunk);
        PMap<String, Object> nested = PMap.empty(store, STRINGS, OBJECTS).assoc("doc", doc);
        assertThat(kinds(store, nested.root())).containsExactlyInAnyOrder(Kind.M, "raw");
        assertThat(nested.valueCodec().encode(nested.get("doc").orElseThrow())).isEqualTo(CBOR.toBytes(doc));
    }

    private static int wideIndex(NodeStore store, String root, String key) {
        List<?> entries = (List<?>) NodeCodec.decode(root, store.get(root).orElseThrow()).get("e");
        byte[] wanted = STRINGS.encode(key);
        for (int i = 0; i < entries.size(); i += 2) {
            if (java.util.Arrays.equals(((Embedded) entries.get(i)).bytes(), wanted)) {
                return i + 1;
            }
        }
        throw new AssertionError("key " + key + " is not inline in the root");
    }

    /** PD-N-04 at the value layer: a value whose x node would pass 1 MiB is refused at assoc, by size. */
    @Test
    void aValueWhoseXNodeWouldPassOneMebibyteIsRefusedAtAssoc() {
        NodeStore store = NodeStore.inMemory();
        String text = "t".repeat(2 * 1024 * 1024);
        PMap<String, String> map = PMap.empty(store, STRINGS, STRINGS);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> map.assoc("big", text))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CBOR bytes exceeds the " + Spill.MAX_X_VALUE)
                .hasMessageContaining("1 MiB");
        String fits = "t".repeat(1_000_000);
        PMap<String, String> held = map.assoc("fits", fits);
        assertThat(kinds(store, held.root())).containsExactlyInAnyOrder(Kind.M, Kind.X);
        assertThat(store.get(Spill.linkCid(((Embedded) ((List<?>) NodeCodec.decode(held.root(),
                store.get(held.root()).orElseThrow()).get("e")).get(1)).bytes())).orElseThrow().length)
                .isBetween(1_000_000, NodeCodec.MAX_NODE_BYTES);
        assertThat(held.get("fits")).contains(fits);
        // A byte string of the same size is a blob and needs no x node.
        byte[] bytes = new byte[2 * 1024 * 1024];
        new Random(42).nextBytes(bytes);
        PMap<String, byte[]> blobs = PMap.empty(store, STRINGS, BYTES).assoc("big", bytes);
        assertThat(blobs.get("big")).contains(bytes);
        assertThat(kinds(store, blobs.root())).containsExactlyInAnyOrder(Kind.M, Kind.B, "raw");
    }

    /** A bare link item in a value or key slot names a spilled node, so the values layer refuses one as data. */
    @Test
    void aBareLinkItemIsRefusedAsAValueOrAKey() {
        NodeStore store = NodeStore.inMemory();
        String cid = Cids.of(new byte[] {9}, Cids.RAW);
        Codec<String> links = new Codec<>() {
            @Override
            public byte[] encode(String value) {
                return Spill.linkItem(value);
            }

            @Override
            public String decode(byte[] bytes) {
                return Spill.linkCid(bytes);
            }
        };
        PMap<String, String> map = PMap.empty(store, STRINGS, links);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> map.assoc("ref", cid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bare link item");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> map.editor().assoc("ref", cid))
                .isInstanceOf(IllegalArgumentException.class);
        PMap<String, String> keyed = PMap.empty(store, links, STRINGS);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> keyed.assoc(cid, "text"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("key must not be a bare link item");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> PVector.empty(store, links).conj(cid))
                .isInstanceOf(IllegalArgumentException.class);
        // A CID held as text is ordinary data.
        PMap<String, String> text = PMap.empty(store, STRINGS, STRINGS).assoc("ref", cid);
        assertThat(text.get("ref")).contains(cid);
        assertThat(store.reachable(text.root())).hasSize(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void nestedSpillsShareUnchangedSubtrees() {
        NodeStore store = NodeStore.inMemory();
        Random random = new Random(42);
        Map<String, Object> outer = new LinkedHashMap<>();
        for (int i = 0; i < 30; i++) {
            outer.put("section" + i, bigMap(300, random));
        }
        byte[] original = CBOR.toBytes(outer);
        PMap<String, Object> map = PMap.empty(store, STRINGS, OBJECTS).assoc("doc", outer);
        String root = map.root();
        int nodes = store.size();
        assertThat(nodes).isGreaterThan(30);
        assertThat(map.valueCodec().encode(map.get("doc").orElseThrow())).isEqualTo(original);

        Map<String, Object> changed = new LinkedHashMap<>(outer);
        Map<String, Object> section = new LinkedHashMap<>((Map<String, Object>) outer.get("section3"));
        section.put("field1", "edited");
        changed.put("section3", section);
        PMap<String, Object> next = map.assoc("doc", changed);
        Set<String> delta = new HashSet<>(store.reachable(next.root()));
        delta.removeAll(store.reachable(root));
        assertThat(delta.size()).isLessThanOrEqualTo(6);
        assertThat(next.valueCodec().encode(next.get("doc").orElseThrow())).isEqualTo(CBOR.toBytes(changed));
    }
}
