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

import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.NodeUnavailableException;
import ai.badmonkey.agentspaces.pdt.kinds.BoNode;
import ai.badmonkey.agentspaces.pdt.values.NodeLoader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verified windows (PDT 2 phase 4, P2-B-03, P2-Q-REL-05; srad/agentspaces-pdt2.md §3.5, S6 of §6.4): a window of a
 * blob delivers each 16 KiB chunk group only after its chaining value verifies up the {@code bo} outboard to the
 * identity, so a group whose bytes differ from the identity is refused and the groups around it are delivered.
 */
class VerifiedWindowTest {

    private static final int GROUP = BoNode.GROUP_BYTES;

    private final NodeStore store = NodeStore.inMemory();

    private static byte[] seeded(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    /** The blob's tree in the store, claimed by the outboard of {@code claimed}. */
    private PBlob claimed(PBlob tree, byte[] claimed) throws IOException {
        BaoOutboard.Built outboard = BaoOutboard.of(new ByteArrayInputStream(claimed));
        store.putAll(outboard.nodes());
        return PBlob.load(store, tree.root()).claiming(BlobIdentity.of(outboard));
    }

    private static byte[] read(PBlob blob, long offset, long length) throws IOException {
        try (InputStream in = blob.openVerified(offset, length)) {
            return in.readAllBytes();
        }
    }

    @Test
    void everyWindowOfAnHonestBlobReadsItsBytes() throws Exception {
        byte[] bytes = seeded(1024 * 1024 + 5000, 1);
        PBlob blob = PBlob.of(bytes, store);
        PBlob verified = claimed(blob, bytes);
        long[][] windows = {{0, 10}, {GROUP - 1, 2}, {500_000, 300_000}, {bytes.length - 100, 100}, {0, bytes.length},
                {bytes.length, 0}, {123_456, 1}, {bytes.length - 5000, 10_000}};
        for (long[] w : windows) {
            int end = (int) Math.min(bytes.length, w[0] + w[1]);
            assertThat(read(verified, w[0], w[1])).as("window %d+%d", w[0], w[1]).isEqualTo(Arrays.copyOfRange(bytes, (int) w[0], end));
        }
    }

    @Test
    void aGroupWithOneCorruptByteIsRefusedAndTheGroupsAroundItAreDelivered() throws Exception {
        byte[] bytes = seeded(1024 * 1024, 2);
        PBlob honest = PBlob.of(bytes, store);
        long corrupt = 20L * GROUP + 77;
        PBlob fetched = honest.splice(corrupt, 1, new byte[]{(byte) (bytes[(int) corrupt] ^ 1)});
        PBlob verified = claimed(fetched, bytes);

        assertThat(read(verified, 19L * GROUP, GROUP)).isEqualTo(Arrays.copyOfRange(bytes, 19 * GROUP, 20 * GROUP));
        assertThat(read(verified, 21L * GROUP, GROUP)).isEqualTo(Arrays.copyOfRange(bytes, 21 * GROUP, 22 * GROUP));
        assertThatThrownBy(() -> read(verified, 20L * GROUP + 100, 10)).isInstanceOf(IOException.class)
                .hasMessageContaining("chunk group 20").hasMessageContaining("does not verify");

        try (InputStream in = verified.openVerified(18L * GROUP, 5L * GROUP)) {
            byte[] delivered = in.readNBytes(2 * GROUP);
            assertThat(delivered).as("groups 18 and 19 verify and arrive").isEqualTo(Arrays.copyOfRange(bytes, 18 * GROUP, 20 * GROUP));
            assertThatThrownBy(in::read).isInstanceOf(IOException.class).hasMessageContaining("chunk group 20");
        }
    }

    @Test
    void aGroupThatSpansTwoChunksAndFailsFailsAgainOnTheNextRead() throws Exception {
        // PDT 2 phase 4, P2-B-03: after a refusal the stream refuses again with the same IOException, wherever the
        // chunks split the group, and never delivers the group
        byte[] bytes = seeded(1024 * 1024, 9);
        PBlob blob = PBlob.of(bytes, store);
        long seam = 0;
        for (BlobTree.Entry chunk : blob.chunks()) {
            seam += chunk.length();
            if (seam % GROUP != 0) {
                break;
            }
        }
        long group = seam / GROUP;
        byte[] claimed = bytes.clone();
        claimed[(int) seam] ^= 1;
        PBlob verified = claimed(blob, claimed);
        try (InputStream in = verified.openVerified(group * GROUP, GROUP)) {
            assertThatThrownBy(in::read).isInstanceOf(IOException.class).hasMessageContaining("chunk group " + group);
            assertThatThrownBy(in::read).isInstanceOf(IOException.class).hasMessageContaining("chunk group " + group);
        }
    }

    @Test
    void aChunkThatCannotBeHadFailsTheReadWithAnIOExceptionAndEveryLaterReadAgain() throws Exception {
        // PDT 2 phase 4 gate, P2-B-03: a chunk the loader cannot have fails the read as an IOException, and the next
        // read throws it again, wherever the chunks split the group; the chunk cursor has moved past the group's start
        byte[] bytes = seeded(1024 * 1024, 10);
        PBlob written = PBlob.of(bytes, store);
        BlobIdentity identity = BlobIdentity.of(written.outboard());
        long seam = 0;
        String second = null;
        for (BlobTree.Entry chunk : written.chunks()) {
            seam += chunk.length();
            if (seam % GROUP != 0) {
                break;
            }
        }
        long group = seam / GROUP;
        long at = 0;
        for (BlobTree.Entry chunk : written.chunks()) {
            if (at == seam) {
                second = chunk.cid();
                break;
            }
            at += chunk.length();
        }
        String unavailableOnce = second;
        NodeStore reader = NodeStore.inMemory();
        java.util.concurrent.atomic.AtomicBoolean refused = new java.util.concurrent.atomic.AtomicBoolean();
        NodeLoader remote = cid -> {
            if (cid.equals(unavailableOnce) && refused.compareAndSet(false, true)) {
                throw new NodeUnavailableException(cid, List.of());
            }
            byte[] node = store.get(cid).orElseThrow(() -> new NodeUnavailableException(cid, List.of()));
            reader.putVerified(cid, node);
            return node;
        };
        PBlob lazy = PBlob.load(reader, remote, written.root()).claiming(identity);
        try (InputStream in = lazy.openVerified(group * GROUP, GROUP)) {
            assertThatThrownBy(in::read).isInstanceOf(IOException.class).hasMessageContaining("chunk group " + group);
            assertThatThrownBy(in::read).isInstanceOf(IOException.class).hasMessageContaining("chunk group " + group);
        }
    }

    @Test
    void aBlobOfOneGroupVerifiesAgainstTheRootHashItself() throws Exception {
        byte[] bytes = seeded(5000, 3);
        PBlob blob = PBlob.of(bytes, store);
        assertThat(read(claimed(blob, bytes), 100, 200)).isEqualTo(Arrays.copyOfRange(bytes, 100, 300));
        PBlob other = PBlob.of(seeded(5000, 4), store);
        assertThatThrownBy(() -> read(claimed(other, bytes), 0, 10)).isInstanceOf(IOException.class)
                .hasMessageContaining("chunk group 0");
        assertThat(read(claimed(PBlob.empty(store), new byte[0]), 0, 0)).isEmpty();
    }

    @Test
    void anOutboardOfOtherContentFailsAtTheRootBeforeAnyGroupIsRead() throws Exception {
        byte[] bytes = seeded(600_000, 5);
        PBlob blob = PBlob.of(bytes, store);
        BaoOutboard.Built mine = BaoOutboard.of(new ByteArrayInputStream(bytes));
        BaoOutboard.Built theirs = BaoOutboard.of(new ByteArrayInputStream(seeded(600_000, 6)));
        store.putAll(mine.nodes());
        store.putAll(theirs.nodes());
        PBlob mixed = blob.claiming(new BlobIdentity(mine.identity(), theirs.root()));
        assertThatThrownBy(() -> mixed.openVerified(0, 10)).isInstanceOf(IOException.class)
                .hasMessageContaining("does not chain to the identity");
        assertThatThrownBy(() -> blob.openVerified(0, 10)).as("no claim").isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pdt.blake3");
    }

    @Test
    void aWindowAcrossTheSeamOfATwoLevelOutboardVerifiesThroughTheUpperNode() throws Exception {
        // PDT 2 phase 4 measures, P2-Q-PRF-05 (owner ruling of 2026-11-09, srad/agentspaces-pdt2.md §10.14): past 16
        // leaves of 8 MiB the root links an upper node of 16 leaves and a short last leaf
        long leaf = 8L << 20;
        int size = (int) (16 * leaf) + 2 * GROUP;
        byte[] bytes = seeded(size, 10);
        PBlob honest = PBlob.of(bytes, store);
        PBlob verified = claimed(honest, bytes);
        BoNode root = BoNode.decode(null, store.get(verified.claim().orElseThrow().bao()).orElseThrow());
        assertThat(root.children()).as("an upper node over 128 MiB and a leaf of two groups").hasSize(2);
        BoNode upper = BoNode.decode(null, store.get(root.children().get(0).cid()).orElseThrow());
        assertThat(upper.size()).isEqualTo(16 * leaf);
        assertThat(upper.children()).hasSize(16);
        int seam = (int) (16 * leaf);
        assertThat(read(verified, seam - 1000, 2 * GROUP)).isEqualTo(Arrays.copyOfRange(bytes, seam - 1000, seam - 1000 + 2 * GROUP));
        int inner = (int) (5 * leaf);
        assertThat(read(verified, inner - 3000, 6000)).as("a seam between two leaves under the upper node")
                .isEqualTo(Arrays.copyOfRange(bytes, inner - 3000, inner + 3000));
        long corrupt = inner + 5;
        PBlob fetched = honest.splice(corrupt, 1, new byte[]{(byte) (bytes[(int) corrupt] ^ 1)});
        PBlob bad = fetched.claiming(verified.claim().orElseThrow());
        assertThat(read(bad, inner - GROUP, GROUP)).isEqualTo(Arrays.copyOfRange(bytes, inner - GROUP, inner));
        assertThatThrownBy(() -> read(bad, inner, 10)).isInstanceOf(IOException.class)
                .hasMessageContaining("chunk group " + inner / GROUP);
    }

    @Test
    void aWindowAcrossTheSplitOfAnOutboardPastOneLeafVerifiesThroughBothChildren() throws Exception {
        int size = (BoNode.LEAF_GROUPS + 2) * GROUP;
        byte[] bytes = seeded(size, 7);
        PBlob honest = PBlob.of(bytes, store);
        PBlob verified = claimed(honest, bytes);
        BoNode root = BoNode.decode(null, store.get(verified.claim().orElseThrow().bao()).orElseThrow());
        assertThat(root.children()).as("a full leaf and a leaf of two groups").hasSize(2);
        long seam = (long) BoNode.LEAF_GROUPS * GROUP;
        assertThat(read(verified, seam - 1000, 2 * GROUP)).isEqualTo(Arrays.copyOfRange(bytes, (int) seam - 1000, (int) seam - 1000 + 2 * GROUP));
        assertThat(read(verified, seam - 1000, 3 * GROUP)).as("a window clipped to the end of the blob").hasSize(2 * GROUP + 1000);
        long corrupt = seam + GROUP + 5;
        PBlob fetched = honest.splice(corrupt, 1, new byte[]{(byte) (bytes[(int) corrupt] ^ 1)});
        PBlob bad = fetched.claiming(verified.claim().orElseThrow());
        assertThat(read(bad, seam, GROUP)).isEqualTo(Arrays.copyOfRange(bytes, (int) seam, (int) seam + GROUP));
        assertThatThrownBy(() -> read(bad, seam + GROUP, 10)).isInstanceOf(IOException.class)
                .hasMessageContaining("chunk group " + (BoNode.LEAF_GROUPS + 1));
    }

    @Test
    void aLazyReaderLoadsOnlyTheWindowsChunksTheirPathAndTheOutboardOnTheirPath() throws Exception {
        byte[] bytes = seeded(8 << 20, 8);
        PBlob written = PBlob.of(bytes, store);
        BlobIdentity identity = BlobIdentity.of(written.outboard());
        NodeStore reader = NodeStore.inMemory();
        AtomicLong loaded = new AtomicLong();
        NodeLoader remote = cid -> {
            byte[] node = store.get(cid).orElseThrow(() -> new NodeUnavailableException(cid, List.of()));
            reader.putVerified(cid, node);
            loaded.addAndGet(node.length);
            return node;
        };
        PBlob lazy = PBlob.load(reader, remote, written.root()).claiming(identity);
        long offset = 3_000_000;
        int length = 300_000;
        assertThat(read(lazy, offset, length)).isEqualTo(Arrays.copyOfRange(bytes, (int) offset, (int) offset + length));
        long outboard = 0;
        for (byte[] node : written.outboard().nodes().values()) {
            outboard += node.length;
        }
        System.out.printf("P2-B-03: a %d-byte window of an %d-byte blob loaded %d bytes (outboard %d bytes)%n",
                length, bytes.length, loaded.get(), outboard);
        assertThat(loaded.get()).as("the window, a chunk of overhang at each end, the path, and the outboard")
                .isLessThan(length + 2L * FastCdc.MAX_SIZE + 64 * 1024 + outboard);
        assertThat(reader.size()).isLessThan(written.chunks().size() / 4);
    }
}
