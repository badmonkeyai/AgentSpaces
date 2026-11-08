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
package ai.badmonkey.agentspaces.pdt;

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link NodeStore}: put, get, verification (PD-N-06), pins, completeness (PD-S-01, PD-S-06), reachability, and the depth bound. */
class NodeStoreTest {

    private static final Random RANDOM = new Random(42);

    private static byte[] chunk(int size) {
        byte[] bytes = new byte[size];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    private static Map<String, Object> blobNode(int height, List<String> children, List<Object> sizes) {
        List<Object> links = new ArrayList<>();
        for (String child : children) {
            links.add(new Link(child));
        }
        return Map.of("k", "b", "h", height, "c", links, "z", sizes);
    }

    @Test
    void putComputesTheCidAndGetReturnsTheBytes() {
        NodeStore store = NodeStore.inMemory();
        byte[] bytes = "chunk".getBytes(StandardCharsets.UTF_8);
        String cid = store.put(bytes, Cids.RAW);
        assertThat(cid).isEqualTo(Cids.of(bytes, Cids.RAW));
        assertThat(store.has(cid)).isTrue();
        assertThat(store.has(Cids.of(new byte[0], Cids.RAW))).isFalse();
        assertThat(store.get(cid)).contains(bytes);
        assertThat(store.get(Cids.of(new byte[0], Cids.RAW))).isEmpty();
        assertThat(store.bytesUsed()).isEqualTo(bytes.length);
        assertThat(store.size()).isEqualTo(1);
        assertThat(store.backend().kind()).isEqualTo("memory");
    }

    @Test
    void putVerifiedAcceptsAMatchAndRefusesAMismatch() {
        NodeStore store = NodeStore.inMemory();
        byte[] bytes = NodeCodec.encode(Map.of("k", "mc", "e", List.of()));
        String cid = Cids.of(bytes, Cids.DAG_CBOR);
        store.putVerified(cid, bytes);
        assertThat(store.has(cid)).isTrue();
        String other = Cids.of(new byte[] {9}, Cids.DAG_CBOR);
        assertThatThrownBy(() -> store.putVerified(other, bytes))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("does not match the claimed CID")
                .hasMessageContaining(other);
        assertThat(store.has(other)).isFalse();
        // The codec byte is part of the identity: raw bytes under a dag-cbor CID of the same digest are refused.
        String wrongCodec = Cids.of(bytes, Cids.RAW);
        assertThatThrownBy(() -> store.putVerified(wrongCodec, new byte[] {1}))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("does not match");
        assertThatThrownBy(() -> store.putVerified("nonsense", bytes))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("CID refused");
    }

    @Test
    void putAllVerifiesEachEntryAndStopsAtTheFirstMismatch() {
        NodeStore store = NodeStore.inMemory();
        byte[] a = new byte[] {1};
        byte[] b = new byte[] {2};
        Map<String, byte[]> batch = new LinkedHashMap<>();
        batch.put(Cids.of(a, Cids.RAW), a);
        batch.put(Cids.of(b, Cids.RAW), a);
        assertThatThrownBy(() -> store.putAll(batch)).isInstanceOf(NodeFormatException.class);
        assertThat(store.has(Cids.of(a, Cids.RAW))).isTrue();
        assertThat(store.has(Cids.of(b, Cids.RAW))).isFalse();
        store.putAll(Map.of(Cids.of(b, Cids.RAW), b));
        assertThat(store.size()).isEqualTo(2);
    }

    @Test
    void getVerifiesWhatTheBackendHolds() {
        MemoryBlockStore backend = new MemoryBlockStore();
        NodeStore store = new NodeStore(backend, InstantSource.system());
        String cid = Cids.of(new byte[] {1}, Cids.RAW);
        backend.put(cid, new byte[] {2}, 0L);
        assertThatThrownBy(() -> store.get(cid))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("does not match");
    }

    @Test
    void putRefusesAStructuredNodeOverOneMebibyte() {
        NodeStore store = NodeStore.inMemory();
        byte[] big = new byte[NodeCodec.MAX_NODE_BYTES + 1];
        assertThatThrownBy(() -> store.put(big, Cids.DAG_CBOR))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("1 MiB");
        // A raw chunk is bounded by the chunker, and the store takes it.
        assertThat(store.has(store.put(big, Cids.RAW))).isTrue();
    }

    @Test
    void pinsCountReferences() {
        NodeStore store = NodeStore.inMemory();
        String root = Cids.of(new byte[] {1}, Cids.DAG_CBOR);
        assertThat(store.pinCount(root)).isZero();
        assertThat(store.pin(root)).isEqualTo(1);
        assertThat(store.pin(root)).isEqualTo(2);
        assertThat(store.pinned()).containsExactly(root);
        assertThat(store.unpin(root)).isEqualTo(1);
        assertThat(store.pinCount(root)).isEqualTo(1);
        assertThat(store.unpin(root)).isZero();
        assertThat(store.pinned()).isEmpty();
        assertThat(store.unpin(root)).isEqualTo(-1);
    }

    /** PD-S-01: pin and unpin are single atomic map operations, so a pin is never lost to a concurrent unpin. */
    @Test
    void pinsAndUnpinsNeverLoseAReferenceUnderContention() throws Exception {
        NodeStore store = NodeStore.inMemory();
        String root = Cids.of(new byte[] {2}, Cids.DAG_CBOR);
        int threads = 8;
        int rounds = 2000;
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        java.util.concurrent.atomic.AtomicInteger lost = new java.util.concurrent.atomic.AtomicInteger();
        try {
            for (int round = 0; round < rounds; round++) {
                java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(threads);
                List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    futures.add(pool.submit(() -> {
                        barrier.await();
                        store.pin(root);
                        if (store.pinCount(root) < 1) {
                            lost.incrementAndGet();
                        }
                        store.unpin(root);
                        return null;
                    }));
                }
                for (java.util.concurrent.Future<?> future : futures) {
                    future.get();
                }
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(lost.get()).as("a pin read as zero right after its own pin() returned").isZero();
        assertThat(store.pinCount(root)).isZero();
        assertThat(store.pinned()).isEmpty();
        assertThat(store.pinGeneration()).isEqualTo((long) threads * rounds);
    }

    @Test
    void completeAndReachableOverATwoLevelBlobTree() {
        NodeStore store = NodeStore.inMemory();
        List<String> chunks = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            chunks.add(store.put(chunk(100 + i), Cids.RAW));
        }
        String left = store.put(NodeCodec.encode(blobNode(1, chunks.subList(0, 2), List.of(100, 101))), Cids.DAG_CBOR);
        String right = store.put(NodeCodec.encode(blobNode(1, chunks.subList(2, 4), List.of(102, 103))), Cids.DAG_CBOR);
        String root = store.put(NodeCodec.encode(blobNode(2, List.of(left, right), List.of(201, 205))), Cids.DAG_CBOR);

        assertThat(store.complete(root)).isTrue();
        assertThat(store.missing(root)).isEmpty();
        assertThat(store.reachable(root)).containsExactly(root, left, right, chunks.get(0), chunks.get(1), chunks.get(2), chunks.get(3));
        assertThat(store.reachable(left)).containsExactly(left, chunks.get(0), chunks.get(1));
        assertThat(store.reachable(chunks.get(3))).containsExactly(chunks.get(3));

        store.backend().remove(chunks.get(2));
        assertThat(store.complete(root)).isFalse();
        assertThat(store.missing(root)).containsExactly(chunks.get(2));
        assertThat(store.reachable(root)).hasSize(7);
        assertThat(store.complete(left)).isTrue();
        assertThat(store.complete(right)).isFalse();

        store.backend().remove(right);
        assertThat(store.missing(root)).containsExactly(right);
        assertThat(store.reachable(root)).containsExactly(root, left, right, chunks.get(0), chunks.get(1));
        assertThat(store.complete(Cids.of(new byte[] {7}, Cids.DAG_CBOR))).isFalse();
    }

    @Test
    void sharedSubtreesAreVisitedOnce() {
        NodeStore store = NodeStore.inMemory();
        String leaf = store.put(chunk(10), Cids.RAW);
        String a = store.put(NodeCodec.encode(blobNode(1, List.of(leaf, leaf), List.of(10, 10))), Cids.DAG_CBOR);
        String root = store.put(NodeCodec.encode(blobNode(2, List.of(a, a), List.of(20, 20))), Cids.DAG_CBOR);
        assertThat(store.reachable(root)).containsExactly(root, a, leaf);
        assertThat(store.complete(root)).isTrue();
    }

    @Test
    void walkRefusesATreeDeeperThan64Levels() {
        NodeStore store = NodeStore.inMemory();
        String cid = store.put(NodeCodec.encode(Map.of("k", "v", "e", List.of())), Cids.DAG_CBOR);
        for (int i = 0; i < 63; i++) {
            cid = store.put(NodeCodec.encode(Map.of("k", "v", "c", List.of(new Link(cid)))), Cids.DAG_CBOR);
        }
        // 64 levels: the leaf and 63 wrappers.
        assertThat(store.complete(cid)).isTrue();
        assertThat(store.reachable(cid)).hasSize(64);
        String tooDeep = store.put(NodeCodec.encode(Map.of("k", "v", "c", List.of(new Link(cid)))), Cids.DAG_CBOR);
        assertThatThrownBy(() -> store.complete(tooDeep))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("tree deeper than 64 levels");
        assertThatThrownBy(() -> store.reachable(tooDeep))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("tree deeper than 64 levels");
    }

    @Test
    void walkRefusesAMalformedNodeByCid() {
        NodeStore store = NodeStore.inMemory();
        String bad = store.put(new byte[] {(byte) 0xa1, 0x61, 0x6b, 0x61, 0x71}, Cids.DAG_CBOR);
        assertThatThrownBy(() -> store.complete(bad))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("unknown kind 'q'")
                .hasMessageContaining(bad);
    }

    @Test
    void theClockMarksTheBackend() {
        MemoryBlockStore backend = new MemoryBlockStore();
        NodeStore store = new NodeStore(backend, InstantSource.fixed(java.time.Instant.ofEpochMilli(1234L)));
        String cid = store.put(new byte[] {1}, Cids.RAW);
        assertThat(backend.lastAccessMillis(cid)).isEqualTo(1234L);
        assertThat(store.clock().millis()).isEqualTo(1234L);
    }
}
