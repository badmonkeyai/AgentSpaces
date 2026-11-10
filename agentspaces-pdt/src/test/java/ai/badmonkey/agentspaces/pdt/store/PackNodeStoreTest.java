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
package ai.badmonkey.agentspaces.pdt.store;

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link PackNodeStore} (PD-S-02): round trip, index rebuild, torn tail, compaction, bytes. */
class PackNodeStoreTest {

    private static final Random RANDOM = new Random(42);

    private static Map<String, byte[]> fill(PackNodeStore store, int count, int size) {
        Map<String, byte[]> nodes = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            byte[] bytes = TreeFixtures.chunk(RANDOM, size);
            String cid = Cids.of(bytes, Cids.RAW);
            assertThat(store.put(cid, bytes, 1000 + i)).isTrue();
            nodes.put(cid, bytes);
        }
        return nodes;
    }

    private static void assertHolds(PackNodeStore store, Map<String, byte[]> nodes) {
        for (Map.Entry<String, byte[]> entry : nodes.entrySet()) {
            assertThat(store.get(entry.getKey(), 5000)).as(entry.getKey()).contains(entry.getValue());
        }
    }

    /** PD-S-02: the production pack ceiling is 256 MiB; the tests below roll packs at small ceilings. */
    @Test
    void theDefaultPackCeilingIsTwoHundredFiftySixMebibytes() {
        assertThat(PackNodeStore.Options.defaults().maxPackBytes()).isEqualTo(256L * 1024 * 1024);
        assertThat(PackNodeStore.DEFAULT_MAX_PACK_BYTES).isEqualTo(256L * 1024 * 1024);
    }

    @Test
    void roundTripsNodesAndReportsSizes(@TempDir Path dir) throws IOException {
        try (PackNodeStore store = PackNodeStore.open(dir)) {
            assertThat(store.kind()).isEqualTo("pack");
            assertThat(store.size()).isZero();
            assertThat(store.bytesUsed()).isZero();
            assertThat(store.rebuiltOnOpen()).isTrue();
            Map<String, byte[]> nodes = fill(store, 50, 300);
            assertThat(store.size()).isEqualTo(50);
            assertThat(store.bytesUsed()).isEqualTo(50 * 300L);
            assertThat(store.cids()).containsExactlyInAnyOrderElementsOf(nodes.keySet());
            assertHolds(store, nodes);
            String first = nodes.keySet().iterator().next();
            assertThat(store.lastAccessMillis(first)).isEqualTo(5000);
            assertThat(store.lastAccessMillis("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")).isEqualTo(-1);
            assertThat(store.get("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", 1)).isEmpty();
        }
    }

    @Test
    void putOfAHeldCidMarksItServedAndStoresNothing(@TempDir Path dir) throws IOException {
        try (PackNodeStore store = PackNodeStore.open(dir)) {
            byte[] bytes = TreeFixtures.chunk(RANDOM, 100);
            String cid = Cids.of(bytes, Cids.RAW);
            assertThat(store.put(cid, bytes, 10)).isTrue();
            long length = store.packLength(store.packIds().get(0));
            assertThat(store.put(cid, bytes, 20)).isFalse();
            assertThat(store.lastAccessMillis(cid)).isEqualTo(20);
            assertThat(store.packLength(store.packIds().get(0))).isEqualTo(length);
            assertThat(store.bytesUsed()).isEqualTo(100);
        }
    }

    @Test
    void removeFreesBytesAndAllowsAPutAgain(@TempDir Path dir) throws IOException {
        try (PackNodeStore store = PackNodeStore.open(dir)) {
            byte[] bytes = TreeFixtures.chunk(RANDOM, 100);
            String cid = Cids.of(bytes, Cids.RAW);
            assertThat(store.remove(cid)).isEqualTo(-1);
            store.put(cid, bytes, 10);
            fill(store, 5, 100);
            assertThat(store.remove(cid)).isEqualTo(100);
            assertThat(store.get(cid, 1)).isEmpty();
            assertThat(store.lastAccessMillis(cid)).isEqualTo(-1);
            assertThat(store.bytesUsed()).isEqualTo(500);
            assertThat(store.put(cid, bytes, 30)).isTrue();
            assertThat(store.get(cid, 1)).contains(bytes);
            assertThat(store.bytesUsed()).isEqualTo(600);
        }
    }

    @Test
    void cleanCloseWritesTheIndexAndReopenLoadsIt(@TempDir Path dir) throws IOException {
        Map<String, byte[]> nodes;
        try (PackNodeStore store = PackNodeStore.open(dir)) {
            nodes = fill(store, 40, 200);
            String removed = nodes.keySet().iterator().next();
            store.remove(removed);
            nodes.remove(removed);
            store.get(nodes.keySet().iterator().next(), 777);
        }
        assertThat(Files.exists(dir.resolve(PackNodeStore.INDEX_FILE))).isTrue();
        try (PackNodeStore store = PackNodeStore.open(dir)) {
            assertThat(store.rebuiltOnOpen()).isFalse();
            assertThat(Files.exists(dir.resolve(PackNodeStore.INDEX_FILE))).as("index deleted after load").isFalse();
            assertThat(store.size()).isEqualTo(39);
            assertThat(store.bytesUsed()).isEqualTo(39 * 200L);
            assertThat(store.lastAccessMillis(nodes.keySet().iterator().next())).isEqualTo(777);
            assertHolds(store, nodes);
        }
    }

    @Test
    void restartWithoutCloseRebuildsTheIndexFromThePacks(@TempDir Path dir) throws IOException {
        PackNodeStore crashed = PackNodeStore.open(dir, PackNodeStore.Options.defaults().withMaxPackBytes(4096));
        Map<String, byte[]> nodes = fill(crashed, 60, 150);
        List<String> cids = new ArrayList<>(nodes.keySet());
        for (int i = 0; i < 10; i++) {
            crashed.remove(cids.get(i * 3));
            nodes.remove(cids.get(i * 3));
        }
        assertThat(crashed.packIds().size()).isGreaterThan(1);
        crashed.abandon();
        try (PackNodeStore store = PackNodeStore.open(dir, PackNodeStore.Options.defaults().withMaxPackBytes(4096))) {
            assertThat(store.rebuiltOnOpen()).isTrue();
            assertThat(store.size()).isEqualTo(50);
            assertThat(store.bytesUsed()).isEqualTo(50 * 150L);
            assertThat(store.cids()).containsExactlyInAnyOrderElementsOf(nodes.keySet());
            assertThat(store.lastAccessMillis(cids.get(1))).as("write-time access survives a rebuild").isEqualTo(1001);
            assertHolds(store, nodes);
            for (int i = 0; i < 10; i++) {
                assertThat(store.get(cids.get(i * 3), 1)).isEmpty();
            }
        }
    }

    @Test
    void aStaleIndexFileIsIgnoredWhenThePacksChanged(@TempDir Path dir) throws IOException {
        Map<String, byte[]> nodes;
        try (PackNodeStore store = PackNodeStore.open(dir)) {
            nodes = fill(store, 10, 100);
        }
        Path index = dir.resolve(PackNodeStore.INDEX_FILE);
        byte[] stale = Files.readAllBytes(index);
        try (PackNodeStore store = PackNodeStore.open(dir)) {
            assertThat(store.rebuiltOnOpen()).isFalse();
            nodes.putAll(fill(store, 5, 100));
            store.abandon();
        }
        Files.write(index, stale);
        try (PackNodeStore store = PackNodeStore.open(dir)) {
            assertThat(store.rebuiltOnOpen()).isTrue();
            assertThat(store.size()).isEqualTo(15);
            assertHolds(store, nodes);
        }
    }

    @Test
    void aTornTailIsIgnoredAndTruncated(@TempDir Path dir) throws IOException {
        Map<String, byte[]> nodes;
        PackNodeStore store = PackNodeStore.open(dir);
        nodes = fill(store, 20, 100);
        store.abandon();
        Path pack = dir.resolve("pack-00000001.pack");
        long full = Files.size(pack);
        try (FileChannel channel = FileChannel.open(pack, StandardOpenOption.WRITE)) {
            channel.truncate(full - 17);
        }
        List<String> cids = new ArrayList<>(nodes.keySet());
        String torn = cids.get(19);
        try (PackNodeStore reopened = PackNodeStore.open(dir)) {
            assertThat(reopened.rebuiltOnOpen()).isTrue();
            assertThat(reopened.size()).isEqualTo(19);
            assertThat(reopened.get(torn, 1)).isEmpty();
            nodes.remove(torn);
            assertHolds(reopened, nodes);
            assertThat(Files.size(pack)).as("truncated to the last whole record").isLessThan(full - 17);
            byte[] bytes = TreeFixtures.chunk(RANDOM, 100);
            String cid = Cids.of(bytes, Cids.RAW);
            reopened.put(cid, bytes, 1);
            nodes.put(cid, bytes);
            reopened.abandon();
        }
        try (PackNodeStore again = PackNodeStore.open(dir)) {
            assertThat(again.size()).isEqualTo(20);
            assertHolds(again, nodes);
        }
    }

    @Test
    void aCorruptRecordStopsTheScanAtThatRecord(@TempDir Path dir) throws IOException {
        PackNodeStore store = PackNodeStore.open(dir);
        Map<String, byte[]> nodes = fill(store, 5, 50);
        store.abandon();
        Path pack = dir.resolve("pack-00000001.pack");
        long record = Files.size(pack) / 5;
        try (FileChannel channel = FileChannel.open(pack, StandardOpenOption.WRITE)) {
            channel.write(java.nio.ByteBuffer.wrap(new byte[] {(byte) 0xFF}), record * 3 + 40);
        }
        List<String> cids = new ArrayList<>(nodes.keySet());
        try (PackNodeStore reopened = PackNodeStore.open(dir)) {
            assertThat(reopened.size()).isEqualTo(3);
            assertThat(reopened.get(cids.get(2), 1)).contains(nodes.get(cids.get(2)));
            assertThat(reopened.get(cids.get(3), 1)).isEmpty();
            assertThat(reopened.get(cids.get(4), 1)).isEmpty();
        }
    }

    @Test
    void packsRollAtTheConfiguredSize(@TempDir Path dir) throws IOException {
        try (PackNodeStore store = PackNodeStore.open(dir, PackNodeStore.Options.defaults().withMaxPackBytes(2000))) {
            Map<String, byte[]> nodes = fill(store, 30, 300);
            assertThat(store.packIds().size()).isGreaterThanOrEqualTo(6);
            for (long id : store.packIds()) {
                assertThat(store.packLength(id)).isLessThanOrEqualTo(2000);
            }
            assertHolds(store, nodes);
            assertThat(store.packBytes()).isGreaterThan(30 * 300L);
        }
    }

    @Test
    void compactionKeepsEveryLiveNodeAndDropsTheDeadBytes(@TempDir Path dir) throws IOException {
        PackNodeStore.Options options = PackNodeStore.Options.defaults().withMaxPackBytes(8192);
        int expected;
        try (PackNodeStore store = PackNodeStore.open(dir, options)) {
            Map<String, byte[]> nodes = fill(store, 100, 200);
            List<Long> before = store.packIds();
            assertThat(before.size()).isGreaterThan(2);
            long firstPack = before.get(0);
            List<String> inFirst = new ArrayList<>();
            for (String cid : nodes.keySet()) {
                if (store.packOf(cid) == firstPack) {
                    inFirst.add(cid);
                }
            }
            assertThat(inFirst.size()).isGreaterThan(3);
            int toRemove = inFirst.size() / 2;
            for (int i = 0; i < toRemove; i++) {
                store.remove(inFirst.get(i));
                nodes.remove(inFirst.get(i));
                assertThat(store.packIds()).as("under half dead keeps the pack").contains(firstPack);
            }
            store.remove(inFirst.get(toRemove));
            nodes.remove(inFirst.get(toRemove));
            assertThat(store.packIds()).as("over half dead compacts the pack").doesNotContain(firstPack);
            assertThat(Files.exists(dir.resolve("pack-00000001.pack"))).isFalse();
            for (int i = toRemove + 1; i < inFirst.size(); i++) {
                assertThat(store.packOf(inFirst.get(i))).isNotEqualTo(firstPack);
            }
            assertThat(store.size()).isEqualTo(nodes.size());
            assertThat(store.bytesUsed()).isEqualTo(nodes.size() * 200L);
            assertHolds(store, nodes);
            assertThat(store.packBytes()).isLessThan(100 * 250L);
            expected = nodes.size();
        }
        try (PackNodeStore store = PackNodeStore.open(dir, options)) {
            assertThat(store.rebuiltOnOpen()).isFalse();
            assertThat(store.size()).isEqualTo(expected);
        }
    }

    /**
     * PDT 2 phase 1, P2-V-03: a meta record lives in the packs beside the nodes, the latest one of
     * a name wins, a clean close and a crash both keep it, compaction carries it, and it is no node:
     * {@code cids}, {@code size}, and {@code bytesUsed} never count it, so a collector never sweeps it.
     */
    @Test
    void metaRecordsSurviveCloseCrashAndCompactionAndAreNoNodes(@TempDir Path dir) throws IOException {
        PackNodeStore.Options options = PackNodeStore.Options.defaults().withMaxPackBytes(8192);
        Map<String, byte[]> nodes;
        try (PackNodeStore store = PackNodeStore.open(dir, options)) {
            store.putMeta("pins", new byte[]{1});
            store.putMeta("pins", new byte[]{2});
            store.putMeta("retention:72", new byte[]{3, 3});
            nodes = fill(store, 2, 200);
            assertThat(store.meta("pins")).contains(new byte[]{2});
            assertThat(store.meta("absent")).isEmpty();
            assertThat(store.metaNames()).containsExactlyInAnyOrder("pins", "retention:72");
            assertThat(store.cids()).containsExactlyInAnyOrderElementsOf(nodes.keySet());
            assertThat(store.size()).isEqualTo(2);
            assertThat(store.bytesUsed()).isEqualTo(400);
        }
        PackNodeStore reopened = PackNodeStore.open(dir, options);
        assertThat(reopened.rebuiltOnOpen()).isFalse();
        assertThat(reopened.meta("pins")).as("through the index file").contains(new byte[]{2});
        reopened.putMeta("pins", new byte[]{4});
        reopened.abandon();
        PackNodeStore rebuilt = PackNodeStore.open(dir, options);
        assertThat(rebuilt.rebuiltOnOpen()).isTrue();
        assertThat(rebuilt.meta("pins")).as("through a rebuild, the latest record").contains(new byte[]{4});
        assertThat(rebuilt.meta("retention:72")).contains(new byte[]{3, 3});
        assertThat(rebuilt.size()).isEqualTo(2);
        long first = rebuilt.packIds().get(0);
        assertThat(rebuilt.compact(first)).isTrue();
        assertThat(rebuilt.packIds()).doesNotContain(first);
        assertThat(rebuilt.meta("retention:72")).as("compaction carries the live meta record").contains(new byte[]{3, 3});
        assertThat(rebuilt.meta("pins")).contains(new byte[]{4});
        assertHolds(rebuilt, nodes);
        rebuilt.close();
        assertThatThrownBy(() -> rebuilt.putMeta("bad name", new byte[0])).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void compactingTheCurrentPackRollsFirst(@TempDir Path dir) throws IOException {
        try (PackNodeStore store = PackNodeStore.open(dir)) {
            Map<String, byte[]> nodes = fill(store, 10, 100);
            List<String> cids = new ArrayList<>(nodes.keySet());
            for (int i = 0; i < 6; i++) {
                store.remove(cids.get(i));
                nodes.remove(cids.get(i));
            }
            assertThat(store.packIds()).containsExactly(2L);
            assertThat(store.packDeadBytes(2L)).isZero();
            assertThat(store.size()).isEqualTo(4);
            assertHolds(store, nodes);
            assertThat(store.packBytes()).isLessThan(10 * 150L);
        }
    }

    @Test
    void explicitCompactionAndSyncWork(@TempDir Path dir) throws IOException {
        try (PackNodeStore store = PackNodeStore.open(dir)) {
            Map<String, byte[]> nodes = fill(store, 10, 100);
            String gone = nodes.keySet().iterator().next();
            store.remove(gone);
            nodes.remove(gone);
            assertThat(store.compact(1L)).isTrue();
            assertThat(store.compact(1L)).isFalse();
            assertThat(store.packIds()).containsExactly(2L);
            store.sync();
            assertHolds(store, nodes);
        }
    }

    @Test
    void worksUnderNodeStoreForTreesAndCompleteness(@TempDir Path dir) throws IOException {
        try (PackNodeStore backend = PackNodeStore.open(dir, PackNodeStore.Options.defaults().withMaxPackBytes(4096))) {
            NodeStore store = new NodeStore(backend, InstantSource.system());
            String root = TreeFixtures.putTree(store, RANDOM, 64, 8, 100);
            assertThat(store.complete(root)).isTrue();
            assertThat(store.reachable(root)).hasSize(64 + 8 + 1);
            String leaf = store.reachable(root).stream().filter(c -> Cids.parse(c).codec() == Cids.RAW).findFirst().orElseThrow();
            backend.remove(leaf);
            assertThat(store.missing(root)).containsExactly(leaf);
        }
    }

    @Test
    void aClosedStoreRefusesUse(@TempDir Path dir) throws IOException {
        PackNodeStore store = PackNodeStore.open(dir);
        store.close();
        store.close();
        assertThatThrownBy(() -> store.get("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", 1))
                .isInstanceOf(IllegalStateException.class);
    }
}
