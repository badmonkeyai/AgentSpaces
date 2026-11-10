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
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.golden.PdtVectors;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PDT 2 phase 6, group R (srad/agentspaces-pdt2.md §3.7): the RRB operations of {@link PVector},
 * the commit rule, the reader of relaxed trees, and the packed form.
 */
class RrbVectorTest {

    private static final Codec<Integer> INTS = Codec.of(Integer.class);
    private static final int MILLION = 1_000_000;

    private static PVector<Integer> range(NodeStore store, int from, int to) {
        VectorEditor<Integer> editor = PVector.empty(store, INTS).editor();
        for (int i = from; i < to; i++) {
            editor.conj(i);
        }
        return editor.done().value();
    }

    private static List<Integer> list(int from, int to) {
        List<Integer> out = new ArrayList<>();
        for (int i = from; i < to; i++) {
            out.add(i);
        }
        return out;
    }

    private static String cid(byte[] node) {
        return Cids.of(node, Cids.DAG_CBOR);
    }

    private static long bytes(Map<String, byte[]> nodes) {
        return nodes.values().stream().mapToLong(b -> b.length).sum();
    }

    @Test
    void spliceInsertAtSliceAndConcatKeepTheElementsAndNeverChangeTheReceiver() {
        NodeStore store = NodeStore.inMemory();
        PVector<Integer> v = range(store, 0, 1000);
        PVector<Integer> inserted = v.insertAt(500, -1);
        List<Integer> expected = list(0, 1000);
        expected.add(500, -1);
        assertThat(inserted.toList()).isEqualTo(expected);
        assertThat(inserted.count()).isEqualTo(1001);
        assertThat(inserted.nth(500)).isEqualTo(-1);
        assertThat(inserted.nth(1000)).isEqualTo(999);
        assertThat(v.toList()).isEqualTo(list(0, 1000));

        PVector<Integer> spliced = v.splice(10, 300, List.of(-5, -6, -7));
        List<Integer> afterSplice = list(0, 10);
        afterSplice.addAll(List.of(-5, -6, -7));
        afterSplice.addAll(list(310, 1000));
        assertThat(spliced.toList()).isEqualTo(afterSplice);

        assertThat(v.slice(33, 777).toList()).isEqualTo(list(33, 777));
        assertThat(v.slice(5, 5).isEmpty()).isTrue();
        assertThat(v.concat(range(store, 1000, 1500)).toList()).isEqualTo(list(0, 1500));
        assertThat(v.concat(PVector.empty(store, INTS))).isSameAs(v);

        assertThatThrownBy(() -> v.insertAt(1001, 1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> v.splice(990, 11, List.of())).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> v.slice(10, 5)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> v.insertAt(1, null)).isInstanceOf(NullPointerException.class);
    }

    /** P2-R-02: an operation at a million elements creates fewer than 3 x depth nodes. */
    @Test
    void aSpliceAnInsertionAndAConcatAtAMillionElementsCreateFewerThanThreeTimesTheDepthInNodes() {
        NodeStore store = NodeStore.inMemory();
        PVector<Integer> big = range(store, 0, MILLION);
        big.root();
        int depth = big.depth();
        assertThat(depth).isEqualTo(5);

        PVector<Integer> inserted = big.insertAt(MILLION / 2 + 7, -1);
        assertThat(inserted.flush().size()).as("insertAt").isLessThan(3 * inserted.depth());
        assertThat(inserted.nth(MILLION / 2 + 7)).isEqualTo(-1);
        assertThat(inserted.nth(MILLION / 2 + 8)).isEqualTo(MILLION / 2 + 7);

        PVector<Integer> spliced = inserted.splice(123_456, 40, List.of(-2, -3));
        assertThat(spliced.flush().size()).as("splice").isLessThan(3 * spliced.depth());
        assertThat(spliced.count()).isEqualTo(MILLION + 1 - 40 + 2);

        PVector<Integer> left = big.slice(0, MILLION / 2 + 3);
        PVector<Integer> right = big.slice(MILLION / 2 + 3, MILLION);
        left.flush();
        right.flush();
        PVector<Integer> joined = left.concat(right);
        assertThat(joined.flush().size()).as("concat").isLessThan(3 * joined.depth());
        assertThat(joined.toList()).isEqualTo(big.toList());

        PVector<Integer> again = spliced.insertAt(900_000, -9);
        assertThat(again.flush().size()).as("an insertion into a relaxed tree").isLessThan(3 * again.depth());
    }

    /**
     * P2-R-03 as the build chose (§10.10): a relaxed vector commits as it stands, and its packed form
     * is the {@code vr} vector of the same elements, over 10,000 random edit sequences.
     */
    @Test
    void thePackedRootOfEveryRandomEditSequenceIsTheVrRootOfItsElements() {
        Random random = new Random(46);
        NodeStore store = NodeStore.inMemory();
        for (int sequence = 0; sequence < 10_000; sequence++) {
            int n = random.nextInt(sequence % 10 == 0 ? 3000 : 300);
            PVector<Integer> v = range(store, 0, n);
            List<Integer> model = list(0, n);
            int edits = 1 + random.nextInt(4);
            for (int e = 0; e < edits; e++) {
                int op = random.nextInt(6);
                int size = model.size();
                if (op == 0 || size == 0) {
                    int at = random.nextInt(size + 1);
                    v = v.insertAt(at, -e - 1);
                    model.add(at, -e - 1);
                } else if (op == 1) {
                    int at = random.nextInt(size);
                    int del = random.nextInt(Math.min(size - at, 70) + 1);
                    List<Integer> add = list(10_000 + e * 100, 10_000 + e * 100 + random.nextInt(40));
                    v = v.splice(at, del, add);
                    model.subList(at, at + del).clear();
                    model.addAll(at, add);
                } else if (op == 2) {
                    int from = random.nextInt(size + 1);
                    int to = from + random.nextInt(size - from + 1);
                    v = v.slice(from, to);
                    model = new ArrayList<>(model.subList(from, to));
                } else if (op == 3) {
                    int m = random.nextInt(200);
                    v = v.concat(range(store, 20_000, 20_000 + m));
                    model.addAll(list(20_000, 20_000 + m));
                } else if (op == 4) {
                    v = v.conj(-100 - e);
                    model.add(-100 - e);
                } else if (size > 0) {
                    v = v.pop();
                    model.remove(size - 1);
                }
            }
            assertThat(v.count()).isEqualTo(model.size());
            if (sequence % 50 == 0) {
                assertThat(v.toList()).as("sequence %d", sequence).isEqualTo(model);
            }
            String vr = vectorOf(store, model).root();
            assertThat(v.packed().root()).as("sequence %d", sequence).isEqualTo(vr);
            assertThat(v.packed().relaxed()).isFalse();
            assertThat(v).isEqualTo(vectorOf(store, model));
            assertThat(v.hashCode()).isEqualTo(vectorOf(store, model).hashCode());
            PVector<Integer> reloaded = PVector.load(store, v.root(), INTS);
            assertThat(reloaded.count()).isEqualTo(model.size());
            if (!model.isEmpty()) {
                int probe = random.nextInt(model.size());
                assertThat(reloaded.nth(probe)).isEqualTo(model.get(probe));
            }
        }
    }

    private static PVector<Integer> vectorOf(NodeStore store, List<Integer> elements) {
        VectorEditor<Integer> editor = PVector.empty(store, INTS).editor();
        elements.forEach(editor::conj);
        return editor.done().value();
    }

    /** P2-R-01: a relaxed tree writes an rr root and rv nodes with z; a dense result writes vr. */
    @Test
    void aRelaxedTreeWritesAnRrRootAndRvNodesWithSizesAndADenseResultWritesVr() {
        NodeStore store = NodeStore.inMemory();
        PVector<Integer> v = range(store, 0, 5000);
        PVector<Integer> inserted = v.insertAt(100, -1);
        assertThat(inserted.relaxed()).isTrue();
        Map<String, byte[]> written = inserted.flush();
        assertThat(NodeCodec.kindOf(store.get(inserted.root()).orElseThrow())).isEqualTo(Kind.RR);
        long rv = written.values().stream().filter(b -> Kind.RV.equals(NodeCodec.kindOf(b))).count();
        assertThat(rv).as("the relaxed nodes on the path").isPositive();
        for (byte[] node : written.values()) {
            if (Kind.RV.equals(NodeCodec.kindOf(node))) {
                assertThat(NodeCodec.decode(null, node)).containsKey("z");
            }
        }
        PVector<Integer> appended = v.insertAt(5000, 5000).splice(5001, 0, List.of(5001, 5002));
        assertThat(appended.relaxed()).as("an edit at the end keeps the dense form").isFalse();
        assertThat(NodeCodec.kindOf(store.get(appended.root()).orElseThrow())).isEqualTo(Kind.VR);
        assertThat(appended.root()).isEqualTo(range(store, 0, 5003).root());
        PVector<Integer> prefix = v.slice(0, 4321);
        assertThat(prefix.relaxed()).as("a prefix of a dense vector is dense").isFalse();
        assertThat(prefix.root()).isEqualTo(range(store, 0, 4321).root());
        assertThat(inserted.splice(100, 1, List.of()).packed().root()).isEqualTo(v.root());
    }

    /** P2-R-04: a reader indexes a relaxed tree that another port wrote through its size tables. */
    @Test
    void aReaderIndexesTheRelaxedVectorOfTheGoldenFileThroughItsSizeTable() {
        NodeStore store = NodeStore.inMemory();
        byte[] root = PdtVectors.rrRelaxed().encode();
        store.put(root, Cids.DAG_CBOR);
        store.put(PdtVectors.rvRelaxed().encode(), Cids.DAG_CBOR);
        for (int[] leaf : new int[][] {{0, 32}, {32, 40}, {40, 72}, {72, 96}}) {
            store.put(PdtVectors.rrLeaf(leaf[0], leaf[1]), Cids.DAG_CBOR);
        }
        String cid = cid(root);
        PVector<Integer> v = PVector.load(store, cid, INTS);
        assertThat(v.relaxed()).isTrue();
        assertThat(v.count()).isEqualTo(PdtVectors.RR_ELEMENTS);
        assertThat(v.toList()).isEqualTo(list(0, PdtVectors.RR_ELEMENTS));
        assertThat(v.nth(39)).isEqualTo(39);
        assertThat(v.nth(40)).isEqualTo(40);
        assertThat(v.nth(97)).isEqualTo(97);
        assertThat(v.root()).isEqualTo(cid);
        assertThat(v.packed().root()).isEqualTo(cid(PdtVectors.vectorOf(PdtVectors.RR_ELEMENTS)));
        assertThat(v.conj(100).toList()).isEqualTo(list(0, 101));
        assertThat(v.insertAt(35, -1).nth(35)).isEqualTo(-1);
    }

    @Test
    void theReaderRefusesAnRvNodeWithoutSizesASizeTableOnADenseNodeAndAnOverfullChild() {
        NodeStore store = NodeStore.inMemory();
        List<Link> leaves = new ArrayList<>();
        for (int[] leaf : new int[][] {{0, 32}, {32, 64}, {64, 72}}) {
            byte[] bytes = PdtVectors.rrLeaf(leaf[0], leaf[1]);
            store.put(bytes, Cids.DAG_CBOR);
            leaves.add(new Link(cid(bytes)));
        }
        assertThatThrownBy(() -> nth(store, rv(leaves, null), 72, 0))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("a dense interior node is a v node");
        assertThatThrownBy(() -> nth(store, rv(leaves, List.of(32L, 64L, 72L)), 72, 0))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("dense");
        assertThatThrownBy(() -> nth(store, rv(leaves, List.of(16L, 64L, 72L)), 72, 0))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("more than");
        assertThat(nth(store, rv(List.of(leaves.get(2), leaves.get(0)), List.of(8L, 40L)), 40, 8)).isEqualTo(0);
    }

    /**
     * P2-R-02 under adversarial edits (PDT 2 phase 6 review): insertions at one index and prepends
     * split the nodes at the seam evenly, so the tree stays within one level of its packed depth
     * where a split that fills its left node would add a level every 500 insertions.
     */
    @Test
    void insertionsAtOneIndexAndPrependsKeepTheTreeWithinOneLevelOfItsPackedDepth() {
        NodeStore store = NodeStore.inMemory();
        PVector<Integer> base = range(store, 0, 100_000);
        PVector<Integer> fixed = base;
        PVector<Integer> front = base;
        for (int k = 1; k <= 5000; k++) {
            fixed = fixed.insertAt(1000, -k);
            front = front.insertAt(0, -k);
            assertThat(fixed.node().shift).as("insertion %d at index 1000", k)
                    .isLessThanOrEqualTo(VecRoot.shiftFor((int) fixed.count()) + VecNode.BITS);
            assertThat(front.node().shift).as("prepend %d", k)
                    .isLessThanOrEqualTo(VecRoot.shiftFor((int) front.count()) + VecNode.BITS);
        }
        List<Integer> expected = list(0, 1000);
        for (int k = 5000; k >= 1; k--) {
            expected.add(-k);
        }
        expected.addAll(list(1000, 100_000));
        assertThat(PVector.load(store, fixed.root(), INTS).toList()).isEqualTo(expected);
        assertThat(front.nth(0)).isEqualTo(-5000);
        assertThat(front.nth(5000)).isEqualTo(0);
        assertThat(front.packed().root()).isEqualTo(vectorOf(store, front.toList()).root());
    }

    /**
     * P2-R-02 and P2-R-03 over one long history (PDT 2 phase 6 review): 6,000 random insertions,
     * splices, deletions, rotations, and appends on one vector keep its elements, its depth within
     * one level of the packed depth, and a packed root equal to the {@code vr} root, and the
     * committed tree reloads.
     */
    @Test
    void oneVectorThroughSixThousandRandomEditsKeepsItsElementsAndItsDepth() {
        Random random = new Random(52);
        NodeStore store = NodeStore.inMemory();
        PVector<Integer> v = range(store, 0, 20_000);
        List<Integer> model = list(0, 20_000);
        int next = 100_000;
        for (int op = 0; op < 6_000; op++) {
            int size = model.size();
            int kind = random.nextInt(6);
            if (kind == 0 || size < 64) {
                int at = random.nextBoolean() ? 0 : random.nextInt(size + 1);
                v = v.insertAt(at, next);
                model.add(at, next++);
            } else if (kind == 1) {
                int at = random.nextInt(size);
                int del = random.nextInt(Math.min(size - at, 40) + 1);
                v = v.splice(at, del, List.of());
                model.subList(at, at + del).clear();
            } else if (kind == 2) {
                int k = random.nextInt(size);
                v = v.slice(k, size).concat(v.slice(0, k));
                List<Integer> rotated = new ArrayList<>(model.subList(k, size));
                rotated.addAll(model.subList(0, k));
                model = rotated;
            } else if (kind == 3) {
                int at = random.nextInt(size + 1);
                List<Integer> add = list(next, next + random.nextInt(70));
                next += add.size();
                v = v.splice(at, 0, add);
                model.addAll(at, add);
            } else if (kind == 4) {
                v = v.insertAt(Math.min(size, 777), next);
                model.add(Math.min(size, 777), next++);
            } else {
                v = v.conj(next);
                model.add(next++);
            }
            assertThat(v.node().shift).as("edit %d", op)
                    .isLessThanOrEqualTo(VecRoot.shiftFor((int) v.count()) + VecNode.BITS);
            if (op % 500 == 0) {
                assertThat(v.toList()).as("edit %d", op).isEqualTo(model);
            }
        }
        assertThat(PVector.load(store, v.root(), INTS).toList()).isEqualTo(model);
        assertThat(v.packed().root()).isEqualTo(vectorOf(store, model).root());
    }

    /**
     * P2-R-04 (PDT 2 phase 6 review): every node of a relaxed tree holds the elements its parent
     * counts under it, so a size table or a root count that disagrees with the nodes below it is
     * refused by name before an index or an iteration reads past it.
     */
    @Test
    void theReaderRefusesASizeTableOrARootCountThatDisagreesWithTheNodesBelowIt() {
        NodeStore store = NodeStore.inMemory();
        List<Link> leaves = new ArrayList<>();
        for (int[] leaf : new int[][] {{0, 32}, {32, 64}, {64, 72}}) {
            byte[] bytes = PdtVectors.rrLeaf(leaf[0], leaf[1]);
            store.put(bytes, Cids.DAG_CBOR);
            leaves.add(new Link(cid(bytes)));
        }
        // a size table that counts fewer elements than a leaf holds, then more
        assertThatThrownBy(() -> nth(store, rv(leaves, List.of(30L, 62L, 70L)), 70, 0))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("its parent counts");
        assertThatThrownBy(() -> nth(store, rv(List.of(leaves.get(2), leaves.get(0)), List.of(16L, 48L)), 48, 0))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("its parent counts");
        // a root that counts more elements than its trie's size table
        assertThatThrownBy(() -> nth(store, rv(List.of(leaves.get(2), leaves.get(0)), List.of(8L, 40L)), 50, 0))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("its parent counts");
        // a dense node whose children cannot hold what the root counts, then whose last leaf is too long
        byte[] dense = NodeCodec.encode(Map.of(NodeCodec.KIND_KEY, Kind.V, "c", leaves.subList(0, 2)));
        assertThatThrownBy(() -> nth(store, dense, 70, 0))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("its parent counts");
        assertThatThrownBy(() -> nth(store, dense, 40, 35))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("its parent counts");
        assertThatThrownBy(() -> PVector.load(store, rrOver(store, dense, 40), INTS).toList())
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("its parent counts");
    }

    /**
     * P2-R-04 (PDT 2 phase 6 review): a tree another port wrote at the deepest shift a reader reads,
     * with thin nodes beside one full path, packs when an insertion would split its root, so a
     * writer never commits a shift past 30.
     */
    @Test
    void anInsertionThatWouldDeepenATreePastShiftThirtyPacksIt() {
        NodeStore store = NodeStore.inMemory();
        byte[] one = PdtVectors.rrLeaf(1000, 1001);
        String thin = store.put(one, Cids.DAG_CBOR);
        String path = store.put(PdtVectors.rrLeaf(0, 32), Cids.DAG_CBOR);
        long pathCount = 32;
        for (int level = VecNode.BITS; level <= 30; level += VecNode.BITS) {
            List<Link> children = new ArrayList<>();
            List<Long> sizes = new ArrayList<>();
            children.add(new Link(path));
            sizes.add(pathCount);
            for (int j = 1; j < VecNode.WIDTH; j++) {
                children.add(new Link(thin));
                sizes.add(pathCount + j);
            }
            path = store.put(rv(children, sizes), Cids.DAG_CBOR);
            pathCount += VecNode.WIDTH - 1;
            thin = store.put(NodeCodec.encode(Map.of(NodeCodec.KIND_KEY, Kind.V, "c", List.of(new Link(thin)))), Cids.DAG_CBOR);
        }
        Map<String, Object> root = new LinkedHashMap<>();
        root.put(NodeCodec.KIND_KEY, Kind.RR);
        root.put("n", pathCount + 1);
        root.put("s", 30L);
        root.put("r", new Link(path));
        root.put("t", List.of(new Embedded(CborCodec.defaultCodec().toBytes(-1))));
        PVector<Integer> v = PVector.load(store, store.put(NodeCodec.encode(root), Cids.DAG_CBOR), INTS);
        List<Integer> model = v.toList();
        assertThat(model).hasSize((int) pathCount + 1);

        PVector<Integer> inserted = v.insertAt(0, -7);
        model.add(0, -7);
        assertThat(inserted.node().shift).isLessThanOrEqualTo(30);
        assertThat(inserted.relaxed()).as("the packed form").isFalse();
        assertThat(PVector.load(store, inserted.root(), INTS).toList()).isEqualTo(model);
    }

    private static byte[] rv(List<Link> children, List<Long> sizes) {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put(NodeCodec.KIND_KEY, Kind.RV);
        node.put("c", children);
        if (sizes != null) {
            node.put("z", sizes);
        }
        return NodeCodec.encode(node);
    }

    private static Integer nth(NodeStore store, byte[] trie, int trieCount, int index) {
        return PVector.load(store, rrOver(store, trie, trieCount), INTS).nth(index);
    }

    /** An rr root at shift 5 over a trie, counting {@code trieCount} elements in it and one in the tail. */
    private static String rrOver(NodeStore store, byte[] trie, int trieCount) {
        store.put(trie, Cids.DAG_CBOR);
        Map<String, Object> root = new LinkedHashMap<>();
        root.put(NodeCodec.KIND_KEY, Kind.RR);
        root.put("n", (long) trieCount + 1);
        root.put("s", 5L);
        root.put("r", new Link(cid(trie)));
        root.put("t", List.of(new Embedded(CborCodec.defaultCodec().toBytes(-1))));
        return store.put(NodeCodec.encode(root), Cids.DAG_CBOR);
    }

    /**
     * P2-Q-PRF-07 and the choice of §10.10: an insertion in the middle of a million elements ships
     * under 8 KB as the relaxed tree commits it, and the packed form rewrites every leaf to its right.
     */
    @Test
    void anInsertionInAMillionElementsShipsUnderEightKilobytesWhileItsPackedFormRewritesTheRightHalf() {
        NodeStore store = NodeStore.inMemory();
        PVector<Integer> big = range(store, 0, MILLION);
        big.root();
        PVector<Integer> inserted = big.insertAt(MILLION / 2, -1);
        Map<String, byte[]> relaxedNodes = inserted.flush();
        long relaxedBytes = bytes(relaxedNodes);
        assertThat(relaxedBytes).isLessThan(8 * 1024);
        PVector<Integer> packed = inserted.packed();
        Map<String, byte[]> packedNodes = packed.flush();
        long packedBytes = bytes(packedNodes);
        assertThat(packedNodes.size()).as("every leaf right of the insertion changes").isGreaterThan(MILLION / 2 / 32);
        System.out.printf("PDT 2 phase 6 (P2-Q-PRF-07): insertion at %d of %d: relaxed commit %d nodes %d bytes; "
                + "packed commit %d nodes %d bytes%n", MILLION / 2, MILLION, relaxedNodes.size(), relaxedBytes,
                packedNodes.size(), packedBytes);
    }

    @Test
    void spilledElementsTravelThroughSplicesAndAReload() {
        NodeStore store = NodeStore.inMemory();
        Codec<String> strings = Codec.of(String.class);
        VectorEditor<String> editor = PVector.empty(store, strings).editor();
        List<String> model = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            String big = i + ":" + "x".repeat(1500);
            editor.conj(big);
            model.add(big);
        }
        PVector<String> v = editor.done().value().insertAt(150, "y".repeat(3000)).splice(10, 5, List.of("z".repeat(2000)));
        model.add(150, "y".repeat(3000));
        model.subList(10, 15).clear();
        model.add(10, "z".repeat(2000));
        assertThat(v.relaxed()).isTrue();
        assertThat(PVector.load(store, v.root(), strings).toList()).isEqualTo(model);
        assertThat(PVector.load(store, v.packed().root(), strings).toList()).isEqualTo(model);
    }

    /** P2-R-04: an rr root whose tail another port left empty reads, appends, and pops. */
    @Test
    void anRrRootWithAnEmptyTailReadsAppendsAndPops() {
        NodeStore store = NodeStore.inMemory();
        List<Link> leaves = new ArrayList<>();
        for (int[] leaf : new int[][] {{0, 20}, {20, 52}}) {
            byte[] bytes = PdtVectors.rrLeaf(leaf[0], leaf[1]);
            store.put(bytes, Cids.DAG_CBOR);
            leaves.add(new Link(cid(bytes)));
        }
        byte[] trie = rv(leaves, List.of(20L, 52L));
        store.put(trie, Cids.DAG_CBOR);
        Map<String, Object> root = new LinkedHashMap<>();
        root.put(NodeCodec.KIND_KEY, Kind.RR);
        root.put("n", 52L);
        root.put("s", 5L);
        root.put("r", new Link(cid(trie)));
        root.put("t", List.of());
        byte[] bytes = NodeCodec.encode(root);
        String cid = store.put(bytes, Cids.DAG_CBOR);
        PVector<Integer> v = PVector.load(store, cid, INTS);
        assertThat(v.toList()).isEqualTo(list(0, 52));
        assertThat(v.conj(52).toList()).isEqualTo(list(0, 53));
        assertThat(v.pop().toList()).isEqualTo(list(0, 51));
        assertThat(v.packed().root()).isEqualTo(range(store, 0, 52).root());
    }

    @Test
    void anEditorSplicesInPlaceAndItsResultMatchesThePersistentOperations() {
        NodeStore store = NodeStore.inMemory();
        PVector<Integer> v = range(store, 0, 2000);
        VectorEditor<Integer> editor = v.editor();
        editor.insertAt(1000, -1).splice(10, 20, List.of(-2, -3)).conj(-4).insertAt(0, -5);
        assertThat(editor.count()).isEqualTo(2000 + 1 - 20 + 2 + 1 + 1);
        assertThat(editor.nth(0)).isEqualTo(-5);
        PVector<Integer> done = editor.done().value();
        PVector<Integer> persistent = v.insertAt(1000, -1).splice(10, 20, List.of(-2, -3)).conj(-4).insertAt(0, -5);
        assertThat(done.toList()).isEqualTo(persistent.toList());
        assertThat(done).isEqualTo(persistent);
        assertThat(v.toList()).isEqualTo(list(0, 2000));
    }

    @Test
    void aDiffOfARelaxedVectorReportsTheShiftedRange() {
        NodeStore store = NodeStore.inMemory();
        PVector<Integer> v = range(store, 0, 300);
        VectorDiff diff = v.insertAt(290, -1).diff(v);
        assertThat(diff.changed()).containsExactly(new VectorDiff.Range(290, 300));
        assertThat(diff.appended()).isEqualTo(new VectorDiff.Range(300, 301));
        assertThat(v.insertAt(10, -1).splice(10, 1, List.of()).diff(v).isEmpty()).isTrue();
    }
}
