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
package ai.badmonkey.agentspaces.pdt.blob;

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.Embedded;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link BlobTree} (PD-B-02): the boundary rule, fanout statistics over 100,000 entries, the bounds per node, seek, and the degenerate roots. */
class BlobTreeTest {

    private static final int ENTRIES = 100_000;

    /** Synthetic chunk entries with seeded random digests and lengths between the chunker's bounds. */
    static List<BlobTree.Entry> synthetic(int count, long seed) {
        Random random = new Random(seed);
        List<BlobTree.Entry> entries = new ArrayList<>(count);
        byte[] digest = new byte[32];
        for (int i = 0; i < count; i++) {
            random.nextBytes(digest);
            int length = FastCdc.MIN_SIZE + random.nextInt(FastCdc.MAX_SIZE - FastCdc.MIN_SIZE + 1);
            entries.add(new BlobTree.Entry(Cids.ofDigest(digest, Cids.RAW), length));
        }
        return entries;
    }

    private static BlobTree.Entry withPrefix(int b0, int b1, int b2, int b3) {
        byte[] digest = new byte[32];
        digest[0] = (byte) b0;
        digest[1] = (byte) b1;
        digest[2] = (byte) b2;
        digest[3] = (byte) b3;
        return new BlobTree.Entry(Cids.ofDigest(digest, Cids.RAW), 1);
    }

    @Test
    void boundaryReadsTheFirstFourDigestBytesBigEndian() {
        assertThat(withPrefix(0, 0, 0, 0).boundary()).isTrue();
        assertThat(withPrefix(0x07, 0xFF, 0xFF, 0xFF).boundary()).isTrue();
        assertThat(withPrefix(0x08, 0, 0, 0).boundary()).isFalse();
        assertThat(withPrefix(0xFF, 0, 0, 0).boundary()).isFalse();
        assertThat(BlobTree.BOUNDARY).isEqualTo(1L << 27);
    }

    @Test
    void groupingKeepsEveryNodeWithinTheBoundsAndNearTheAverageFanout() {
        List<BlobTree.Entry> level = synthetic(ENTRIES, 42);
        List<List<BlobTree.Entry>> nodes = BlobTree.group(level);
        int min = Integer.MAX_VALUE;
        int max = 0;
        for (int i = 0; i < nodes.size(); i++) {
            int size = nodes.get(i).size();
            assertThat(size).isLessThanOrEqualTo(BlobTree.MAX_FANOUT);
            if (i < nodes.size() - 1) {
                assertThat(size).isGreaterThanOrEqualTo(BlobTree.MIN_FANOUT);
                // Every node but the last closes on a boundary or on the maximum.
                assertThat(nodes.get(i).get(size - 1).boundary() || size == BlobTree.MAX_FANOUT).isTrue();
            }
            min = Math.min(min, size);
            max = Math.max(max, size);
        }
        double mean = (double) ENTRIES / nodes.size();
        System.out.println("BlobTree fanout over " + ENTRIES + " entries: " + nodes.size() + " nodes, mean "
                + String.format("%.2f", mean) + ", min " + min + ", max " + max);
        assertThat(mean).isBetween(28.0, 42.0);
        assertThat(nodes.stream().mapToInt(List::size).sum()).isEqualTo(ENTRIES);
        List<BlobTree.Entry> flat = new ArrayList<>();
        nodes.forEach(flat::addAll);
        assertThat(flat).isEqualTo(level);
    }

    @Test
    void buildWritesEveryInteriorNodeOnceAndTheTreeReadsBack() {
        NodeStore store = NodeStore.inMemory();
        List<BlobTree.Entry> level = synthetic(ENTRIES, 42);
        long total = level.stream().mapToLong(BlobTree.Entry::length).sum();
        BlobTree tree = BlobTree.build(level, store);
        assertThat(tree.size()).isEqualTo(total);
        assertThat(tree.height()).isBetween(3, 4);
        assertThat(tree.writtenNodes()).isEqualTo(store.size());
        assertThat(tree.writtenBytes()).isEqualTo(store.bytesUsed());
        assertThat(Cids.parse(tree.root()).structured()).isTrue();
        System.out.println("BlobTree over " + ENTRIES + " entries: height " + tree.height() + ", "
                + tree.writtenNodes() + " interior nodes, " + tree.writtenBytes() + " bytes");

        BlobTree again = BlobTree.build(level, store);
        assertThat(again.root()).isEqualTo(tree.root());
        assertThat(again.writtenNodes()).isZero();

        BlobTree opened = BlobTree.open(store, tree.root());
        assertThat(opened.size()).isEqualTo(total);
        assertThat(opened.height()).isEqualTo(tree.height());
        assertThat(opened.chunks()).isEqualTo(level);
        assertThat(store.complete(tree.root())).isFalse();
        assertThat(store.missing(tree.root())).hasSize(ENTRIES);
    }

    @Test
    void seekDescendsTheSizeSumsToTheRightChunk() {
        NodeStore store = NodeStore.inMemory();
        List<BlobTree.Entry> level = synthetic(20_000, 42);
        BlobTree tree = BlobTree.open(store, BlobTree.build(level, store).root());
        long[] starts = new long[level.size()];
        long running = 0;
        for (int i = 0; i < level.size(); i++) {
            starts[i] = running;
            running += level.get(i).length();
        }
        long sum = running;
        Random random = new Random(42);
        for (int probe = 0; probe < 1_000; probe++) {
            long offset = Math.floorMod(random.nextLong(), sum);
            int expected = 0;
            while (expected < starts.length - 1 && starts[expected + 1] <= offset) {
                expected++;
            }
            BlobTree.Located located = tree.seek(offset);
            assertThat(located.cid()).isEqualTo(level.get(expected).cid());
            assertThat(located.start()).isEqualTo(starts[expected]);
            assertThat(located.length()).isEqualTo(level.get(expected).length());
        }
        assertThat(tree.seek(0).start()).isZero();
        assertThat(tree.seek(sum - 1).cid()).isEqualTo(level.get(level.size() - 1).cid());
        assertThatThrownBy(() -> tree.seek(sum)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> tree.seek(-1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void emptyAndSingleChunkBlobsAreTheChunk() {
        NodeStore store = NodeStore.inMemory();
        BlobTree empty = BlobTree.build(List.of(), store);
        assertThat(empty.root()).isEqualTo(BlobTree.EMPTY_ROOT);
        assertThat(empty.root()).isEqualTo("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku");
        assertThat(empty.size()).isZero();
        assertThat(empty.height()).isZero();
        assertThat(empty.chunks()).isEmpty();
        assertThat(BlobTree.open(store, empty.root()).chunks()).isEmpty();

        byte[] chunk = new byte[1000];
        new Random(42).nextBytes(chunk);
        String cid = store.put(chunk, Cids.RAW);
        BlobTree single = BlobTree.build(List.of(new BlobTree.Entry(cid, chunk.length)), store);
        assertThat(single.root()).isEqualTo(cid);
        assertThat(single.height()).isZero();
        assertThat(single.writtenNodes()).isZero();
        BlobTree opened = BlobTree.open(store, cid);
        assertThat(opened.size()).isEqualTo(1000);
        assertThat(opened.chunks()).containsExactly(new BlobTree.Entry(cid, 1000));
        assertThat(opened.seek(999).cid()).isEqualTo(cid);
    }

    @Test
    void interiorNodeEncodesAsKindBWithHeightLinksAndSizes() {
        List<BlobTree.Entry> entries = synthetic(5, 1);
        byte[] bytes = BlobTree.encodeNode(1, entries);
        Map<String, Object> node = NodeCodec.decode(bytes);
        assertThat(node.get("k")).isEqualTo("b");
        assertThat(node.get("h")).isEqualTo(1L);
        assertThat(NodeCodec.links(bytes)).containsExactly(entries.stream().map(BlobTree.Entry::cid).toArray(String[]::new));
        assertThat(node.get("z")).isEqualTo(entries.stream().map(BlobTree.Entry::length).toList());
    }

    @Test
    void openRefusesAnAbsentRootOrAnotherKind() {
        NodeStore store = NodeStore.inMemory();
        String absent = Cids.ofDigest(new byte[32], Cids.DAG_CBOR);
        assertThatThrownBy(() -> BlobTree.open(store, absent))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("not in the store");
        String x = store.put(NodeCodec.encode(Map.of("k", "x", "v", new Embedded(new byte[] {0x01}))), Cids.DAG_CBOR);
        assertThatThrownBy(() -> BlobTree.open(store, x))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("kind 'x'");
    }

    /** PD-B-02, PD-N-04: a crafted interior node whose byte counts lie is refused by name, never read misaligned. */
    @Test
    void interiorByteCountsThatLieAreRefusedByName() {
        NodeStore store = NodeStore.inMemory();
        Random random = new Random(42);
        List<String> chunks = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            byte[] bytes = new byte[100];
            random.nextBytes(bytes);
            chunks.add(store.put(bytes, Cids.RAW));
        }
        List<BlobTree.Entry> lying = new ArrayList<>();
        for (String cid : chunks) {
            lying.add(new BlobTree.Entry(cid, 1000));
        }
        byte[] node = BlobTree.encodeNode(1, lying);
        String root = store.put(node, Cids.DAG_CBOR);
        PBlob blob = PBlob.load(store, root);
        assertThat(blob.size()).as("the root's claim is all a reader knows before it reads a chunk").isEqualTo(4000);
        assertThatThrownBy(() -> blob.read(1500, 10)).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("chunk of 100 bytes where the tree claims 1000")
                .hasMessageContaining(chunks.get(1));
        assertThatThrownBy(() -> blob.read(150, 10)).isInstanceOf(NodeFormatException.class).hasMessageContaining(chunks.get(0));
        assertThatThrownBy(() -> blob.bytes()).isInstanceOf(NodeFormatException.class);
        assertThatThrownBy(() -> blob.channel().read(java.nio.ByteBuffer.allocate(10))).isInstanceOf(NodeFormatException.class);
        assertThatThrownBy(() -> blob.splice(0, 1, new byte[] {1})).isInstanceOf(NodeFormatException.class);

        // Byte counts that overflow a long are refused at the root.
        List<BlobTree.Entry> overflow = List.of(new BlobTree.Entry(chunks.get(0), Long.MAX_VALUE), new BlobTree.Entry(chunks.get(1), Long.MAX_VALUE));
        String overflowRoot = store.put(BlobTree.encodeNode(1, overflow), Cids.DAG_CBOR);
        assertThatThrownBy(() -> PBlob.load(store, overflowRoot)).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("byte counts overflow").hasMessageContaining(overflowRoot);

        // A child node whose sum differs from what its parent claims is refused when the walk reaches it.
        List<BlobTree.Entry> honest = new ArrayList<>();
        for (String cid : chunks) {
            honest.add(new BlobTree.Entry(cid, 100));
        }
        String child = store.put(BlobTree.encodeNode(1, honest), Cids.DAG_CBOR);
        String parent = store.put(BlobTree.encodeNode(2, List.of(new BlobTree.Entry(child, 999))), Cids.DAG_CBOR);
        PBlob mismatched = PBlob.load(store, parent);
        assertThatThrownBy(mismatched::chunks).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("holds 400 bytes where its parent claims 999").hasMessageContaining(child);
        assertThatThrownBy(() -> mismatched.tree().seek(5)).isInstanceOf(NodeFormatException.class).hasMessageContaining(child);
        // The honest tree reads back whole.
        PBlob good = PBlob.load(store, store.put(BlobTree.encodeNode(2, List.of(new BlobTree.Entry(child, 400))), Cids.DAG_CBOR));
        assertThat(good.bytes()).hasSize(400);
    }

    @Test
    void orderChangesTheRoot() {
        NodeStore store = NodeStore.inMemory();
        List<BlobTree.Entry> level = new ArrayList<>(synthetic(500, 42));
        String root = BlobTree.build(level, store).root();
        BlobTree.Entry first = level.get(0);
        level.set(0, level.get(1));
        level.set(1, first);
        assertThat(BlobTree.build(level, store).root()).isNotEqualTo(root);
    }
}
