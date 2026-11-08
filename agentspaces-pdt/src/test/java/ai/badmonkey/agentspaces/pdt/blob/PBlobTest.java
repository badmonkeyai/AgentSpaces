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
import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link PBlob} (PD-B-03, PD-B-04): round trips, ranged reads across chunk boundaries,
 * splices that share unchanged nodes, file publishing against a previous value, the
 * append and one-byte edit costs of PD-Q-PRF-04 and PD-Q-PRF-02 at 64 MiB, and the
 * 64 MiB heap bound of PD-B-04 in a forked JVM.
 */
class PBlobTest {

    private static final int MIB = 1024 * 1024;

    private static byte[] random(int size, long seed) {
        byte[] data = new byte[size];
        new Random(seed).nextBytes(data);
        return data;
    }

    private static Set<String> cids(PBlob blob) {
        Set<String> cids = new HashSet<>();
        blob.chunks().forEach(e -> cids.add(e.cid()));
        return cids;
    }

    @Test
    void roundTripsAndChunksAsTheChunkerCuts() {
        NodeStore store = NodeStore.inMemory();
        byte[] data = random(MIB + 4321, 42);
        PBlob blob = PBlob.of(data, store);
        assertThat(blob.size()).isEqualTo(data.length);
        assertThat(blob.bytes()).isEqualTo(data);
        List<Integer> cuts = FastCdc.cuts(data);
        assertThat(blob.chunks()).hasSize(cuts.size());
        long sum = 0;
        for (int i = 0; i < cuts.size(); i++) {
            sum += blob.chunks().get(i).length();
            assertThat(sum).isEqualTo((long) cuts.get(i));
        }
        assertThat(blob.writtenChunkBytes()).isEqualTo(data.length);
        assertThat(blob.writtenBytes()).isEqualTo(store.bytesUsed());
        assertThat(store.complete(blob.root())).isTrue();
        assertThat(PBlob.of(data, store).root()).isEqualTo(blob.root());
        assertThat(PBlob.of(data, store).writtenBytes()).isZero();
        PBlob loaded = PBlob.load(store, blob.root());
        assertThat(loaded.size()).isEqualTo(data.length);
        assertThat(loaded.bytes()).isEqualTo(data);
    }

    @Test
    void emptyAndTinyBlobsAreSingleChunks() {
        NodeStore store = NodeStore.inMemory();
        PBlob empty = PBlob.empty(store);
        assertThat(empty.size()).isZero();
        assertThat(empty.root()).isEqualTo(BlobTree.EMPTY_ROOT);
        assertThat(empty.bytes()).isEmpty();
        assertThat(PBlob.of(new byte[0], store).root()).isEqualTo(BlobTree.EMPTY_ROOT);
        byte[] tiny = "hello".getBytes();
        PBlob blob = PBlob.of(tiny, store);
        assertThat(blob.root()).isEqualTo(Cids.of(tiny, Cids.RAW));
        assertThat(blob.bytes()).isEqualTo(tiny);
        assertThat(empty.append(tiny).root()).isEqualTo(blob.root());
        assertThat(blob.splice(0, 5, new byte[0]).root()).isEqualTo(BlobTree.EMPTY_ROOT);
    }

    @Test
    void rangedReadsCrossChunkBoundaries() throws IOException {
        NodeStore store = NodeStore.inMemory();
        byte[] data = random(2 * MIB, 42);
        PBlob blob = PBlob.of(data, store);
        long firstCut = blob.chunks().get(0).length();
        byte[] across = blob.read(firstCut - 10, 20);
        assertThat(across).isEqualTo(Arrays.copyOfRange(data, (int) firstCut - 10, (int) firstCut + 10));
        Random random = new Random(42);
        for (int i = 0; i < 200; i++) {
            int offset = random.nextInt(data.length);
            int length = random.nextInt(300_000);
            byte[] expected = Arrays.copyOfRange(data, offset, Math.min(data.length, offset + length));
            assertThat(blob.read(offset, length)).isEqualTo(expected);
        }
        try (InputStream in = blob.open(0, data.length)) {
            assertThat(in.readAllBytes()).isEqualTo(data);
        }
        assertThat(blob.read(data.length, 10)).isEmpty();
        assertThat(blob.open(5, 0).read()).isEqualTo(-1);
        assertThatThrownBy(() -> blob.open(data.length + 1, 1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void channelSeeksAndReadsAndRefusesWrites() throws IOException {
        NodeStore store = NodeStore.inMemory();
        byte[] data = random(MIB + 77, 42);
        PBlob blob = PBlob.of(data, store);
        try (SeekableByteChannel channel = blob.channel()) {
            assertThat(channel.size()).isEqualTo(data.length);
            ByteBuffer buffer = ByteBuffer.allocate(100_000);
            channel.position(500_000);
            int read = 0;
            while (buffer.hasRemaining()) {
                int n = channel.read(buffer);
                if (n < 0) {
                    break;
                }
                read += n;
            }
            assertThat(read).isEqualTo(100_000);
            assertThat(buffer.array()).isEqualTo(Arrays.copyOfRange(data, 500_000, 600_000));
            assertThat(channel.position()).isEqualTo(600_000);
            channel.position(data.length);
            assertThat(channel.read(ByteBuffer.allocate(1))).isEqualTo(-1);
            assertThatThrownBy(() -> channel.write(ByteBuffer.allocate(1))).isInstanceOf(NonWritableChannelException.class);
            assertThatThrownBy(() -> channel.truncate(1)).isInstanceOf(NonWritableChannelException.class);
            assertThat(channel.isOpen()).isTrue();
        }
    }

    @Test
    void spliceInsertsDeletesAndReplacesWhileSharingUnchangedChunks() {
        NodeStore store = NodeStore.inMemory();
        byte[] data = random(4 * MIB, 42);
        PBlob blob = PBlob.of(data, store);
        int chunks = blob.chunks().size();

        byte[] insert = random(1000, 7);
        PBlob inserted = blob.splice(2 * MIB, 0, insert);
        byte[] expected = new byte[data.length + insert.length];
        System.arraycopy(data, 0, expected, 0, 2 * MIB);
        System.arraycopy(insert, 0, expected, 2 * MIB, insert.length);
        System.arraycopy(data, 2 * MIB, expected, 2 * MIB + insert.length, data.length - 2 * MIB);
        assertThat(inserted.size()).isEqualTo(expected.length);
        assertThat(inserted.bytes()).isEqualTo(expected);
        Set<String> shared = cids(inserted);
        shared.retainAll(cids(blob));
        assertThat(shared.size()).isGreaterThanOrEqualTo(chunks - 3);
        assertThat(inserted.writtenBytes()).isLessThan(3 * FastCdc.MAX_SIZE);
        assertThat(blob.bytes()).isEqualTo(data);

        PBlob deleted = inserted.splice(2 * MIB, insert.length, new byte[0]);
        assertThat(deleted.root()).isEqualTo(blob.root());
        assertThat(deleted.writtenBytes()).isZero();

        byte[] replacement = random(500_000, 9);
        PBlob replaced = blob.splice(MIB, 300_000, replacement);
        byte[] expected2 = new byte[data.length - 300_000 + replacement.length];
        System.arraycopy(data, 0, expected2, 0, MIB);
        System.arraycopy(replacement, 0, expected2, MIB, replacement.length);
        System.arraycopy(data, MIB + 300_000, expected2, MIB + replacement.length, data.length - MIB - 300_000);
        assertThat(replaced.bytes()).isEqualTo(expected2);
        assertThat(PBlob.of(expected2, store).root()).isEqualTo(replaced.root());

        PBlob truncated = blob.splice(MIB, data.length - MIB, new byte[0]);
        assertThat(truncated.bytes()).isEqualTo(Arrays.copyOf(data, MIB));
        assertThat(truncated.root()).isEqualTo(PBlob.of(Arrays.copyOf(data, MIB), store).root());

        assertThatThrownBy(() -> blob.splice(data.length + 1, 0, new byte[0])).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> blob.splice(data.length - 1, 2, new byte[0])).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void editedBlobsHaveTheCanonicalRootOfTheirContent() {
        NodeStore store = NodeStore.inMemory();
        byte[] data = random(3 * MIB, 42);
        PBlob blob = PBlob.of(data, store);
        byte[] tail = random(MIB / 2, 3);
        PBlob appended = blob.append(tail);
        byte[] whole = Arrays.copyOf(data, data.length + tail.length);
        System.arraycopy(tail, 0, whole, data.length, tail.length);
        assertThat(appended.bytes()).isEqualTo(whole);
        assertThat(appended.root()).isEqualTo(PBlob.of(whole, store).root());
        PBlob streamed = blob.append(new ByteArrayInputStream(tail));
        assertThat(streamed.root()).isEqualTo(appended.root());
        assertThat(streamed.writtenBytes()).isZero();
        PBlob viaStream = PBlob.of(new ByteArrayInputStream(whole), NodeStore.inMemory());
        assertThat(viaStream.root()).isEqualTo(appended.root());
    }

    @Test
    void ofPathWithPreviousSharesUnchangedChunks(@TempDir Path dir) throws IOException {
        NodeStore store = NodeStore.inMemory();
        byte[] data = random(6 * MIB, 42);
        Path file = dir.resolve("blob.bin");
        Files.write(file, data);
        PBlob first = PBlob.of(file, store);
        assertThat(first.root()).isEqualTo(PBlob.of(data, store).root());
        assertThat(PBlob.of(file, store, first)).isSameAs(first);

        byte[] edited = data.clone();
        edited[3 * MIB + 5] ^= 0x7F;
        Files.write(file, edited);
        PBlob second = PBlob.of(file, store, first);
        assertThat(second.bytes()).isEqualTo(edited);
        assertThat(second.root()).isEqualTo(first.splice(3 * MIB + 5, 1, new byte[] {edited[3 * MIB + 5]}).root());
        assertThat(second.writtenBytes()).isLessThan(FastCdc.MAX_SIZE + 20_000);

        byte[] grown = new byte[data.length + 12_345];
        System.arraycopy(data, 0, grown, 0, 2 * MIB);
        new Random(5).nextBytes(grown);
        System.arraycopy(data, 0, grown, 0, 2 * MIB);
        System.arraycopy(data, 2 * MIB, grown, 2 * MIB + 12_345, data.length - 2 * MIB);
        Files.write(file, grown);
        PBlob third = PBlob.of(file, store, first);
        assertThat(third.bytes()).isEqualTo(grown);
        assertThat(third.root()).isEqualTo(PBlob.of(grown, NodeStore.inMemory()).root());
        assertThat(third.writtenBytes()).isLessThan(3 * FastCdc.MAX_SIZE);

        Files.write(file, new byte[0]);
        assertThat(PBlob.of(file, store, first).root()).isEqualTo(BlobTree.EMPTY_ROOT);
        Files.write(file, Arrays.copyOf(data, 100));
        PBlob shrunk = PBlob.of(file, store, first);
        assertThat(shrunk.bytes()).isEqualTo(Arrays.copyOf(data, 100));
        assertThat(shrunk.root()).isEqualTo(Cids.of(Arrays.copyOf(data, 100), Cids.RAW));
    }

    /**
     * PD-Q-PRF-04 at 64 MiB. An append rewrites the old last chunk, because that chunk
     * closed at the end of the stream and the bytes after it now decide its cut, and
     * then chunks the appended bytes; so the cost is the appended bytes plus the old
     * last chunk (at most 256 KiB) plus the interior path, and the layout stays
     * canonical. The row reads that way; this stream's last chunk is 103,975 bytes, and
     * the test asserts the structural bound and prints the figure.
     */
    @Test
    void appendingOneMebibyteToSixtyFourCostsTheTailChunkAndThePath() {
        NodeStore store = NodeStore.inMemory();
        byte[] data = random(64 * MIB, 42);
        PBlob blob = PBlob.of(data, store);
        long lastOld = blob.chunks().get(blob.chunks().size() - 1).length();
        byte[] tail = random(MIB, 43);
        PBlob appended = blob.append(tail);
        System.out.println("PD-Q-PRF-04 at 64 MiB: 1 MiB append wrote " + appended.writtenBytes() + " bytes ("
                + appended.writtenChunkBytes() + " in " + appended.writtenChunks() + " chunks, "
                + appended.writtenNodeBytes() + " interior); last old chunk was " + lastOld
                + " bytes; 1.1 MiB is " + (long) (1.1 * MIB));
        assertThat(appended.size()).isEqualTo(65L * MIB);
        assertThat(appended.writtenChunkBytes()).isEqualTo(MIB + lastOld);
        assertThat(appended.writtenNodeBytes()).isLessThan(16 * 1024L);
        assertThat(appended.writtenBytes()).isLessThanOrEqualTo(MIB + FastCdc.MAX_SIZE + 16 * 1024L);
        assertThat(appended.root()).isEqualTo(PBlob.of(concat(data, tail), NodeStore.inMemory()).root());
        assertThat(appended.read(64 * MIB - 100, 200))
                .isEqualTo(concat(Arrays.copyOfRange(data, 64 * MIB - 100, 64 * MIB), Arrays.copyOf(tail, 100)));
        assertThat(appended.read(65 * MIB - 1000, 1000)).isEqualTo(Arrays.copyOfRange(tail, MIB - 1000, MIB));
    }

    @Test
    void appendingAfterAMaximumSizedChunkKeepsThatChunk() {
        NodeStore store = NodeStore.inMemory();
        byte[] flat = new byte[FastCdc.MAX_SIZE];
        Arrays.fill(flat, (byte) 0x41);
        PBlob blob = PBlob.of(flat, store);
        assertThat(blob.chunks()).hasSize(1);
        byte[] tail = random(100_000, 44);
        PBlob appended = blob.append(tail);
        assertThat(appended.writtenChunkBytes()).isEqualTo(tail.length);
        assertThat(appended.chunks().get(0).cid()).isEqualTo(blob.root());
        assertThat(appended.root()).isEqualTo(PBlob.of(concat(flat, tail), NodeStore.inMemory()).root());
    }

    /**
     * PD-Q-PRF-02 at 64 MiB. The cost of a one-byte edit is structural: the chunk
     * holding the byte is rewritten, at most one neighbour follows while the cuts
     * realign, and the interior path above them is rewritten; so the cost is bounded
     * by the lengths of those two old chunks plus under 16 KiB of interior nodes, and
     * in the worst case by two maximum chunks. The row's 128 KiB figure holds at the
     * median chunk (about 76 KiB) and fails inside a chunk longer than 128 KiB, which
     * this stream contains; the test edits one such chunk on purpose and prints the
     * figures the record cites.
     */
    @Test
    void aOneByteEditCostsTheEditedChunkAndItsPath() {
        NodeStore store = NodeStore.inMemory();
        byte[] data = random(64 * MIB, 42);
        PBlob blob = PBlob.of(data, store);
        List<BlobTree.Entry> chunks = blob.chunks();
        long[] lengths = new long[chunks.size()];
        long[] ends = new long[chunks.size()];
        int largest = 0;
        int over128 = 0;
        for (int i = 0; i < chunks.size(); i++) {
            lengths[i] = chunks.get(i).length();
            ends[i] = (i == 0 ? 0 : ends[i - 1]) + lengths[i];
            if (lengths[i] > lengths[largest]) {
                largest = i;
            }
            if (lengths[i] > 128 * 1024) {
                over128++;
            }
        }
        long[] sorted = lengths.clone();
        Arrays.sort(sorted);
        System.out.println("PD-Q-PRF-02 at 64 MiB: " + chunks.size() + " chunks, median " + sorted[sorted.length / 2]
                + " bytes, max " + sorted[sorted.length - 1] + " bytes, " + over128 + " chunks over 128 KiB");
        assertThat(sorted[sorted.length / 2]).as("the median chunk stays under 128 KiB").isLessThan(128 * 1024L);
        assertThat(lengths[largest]).as("this stream has a chunk over 128 KiB, so the row as written cannot hold everywhere")
                .isGreaterThan(128 * 1024L);

        Random random = new Random(42);
        List<Integer> offsets = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            offsets.add(random.nextInt(data.length));
        }
        offsets.add((int) (ends[largest] - lengths[largest] / 2));
        long worst = 0;
        long worstInLargest = 0;
        for (int offset : offsets) {
            byte[] one = {(byte) (data[offset] ^ 0x55)};
            PBlob edited = blob.splice(offset, 1, one);
            int i = Arrays.binarySearch(ends, offset);
            int holding = i >= 0 ? i + 1 : -i - 1;
            long bound = lengths[holding] + (holding + 1 < lengths.length ? lengths[holding + 1] : 0);
            worst = Math.max(worst, edited.writtenBytes());
            System.out.println("PD-Q-PRF-02 at 64 MiB: one-byte edit at " + offset + " in a chunk of " + lengths[holding]
                    + " bytes wrote " + edited.writtenBytes() + " bytes (" + edited.writtenChunks() + " chunks, "
                    + edited.writtenNodeBytes() + " interior)");
            assertThat(edited.writtenChunks()).isLessThanOrEqualTo(2);
            assertThat(edited.writtenChunkBytes()).isLessThanOrEqualTo(bound);
            assertThat(edited.writtenNodeBytes()).isLessThan(16 * 1024L);
            assertThat(edited.writtenBytes()).isLessThanOrEqualTo(2L * FastCdc.MAX_SIZE + 16 * 1024L);
            assertThat(edited.size()).isEqualTo(data.length);
            assertThat(edited.read(offset, 1)).isEqualTo(one);
            assertThat(edited.read(Math.max(0, offset - 50), 50)).isEqualTo(Arrays.copyOfRange(data, Math.max(0, offset - 50), Math.max(0, offset - 50) + 50));
            Set<String> shared = cids(edited);
            shared.retainAll(cids(blob));
            assertThat(shared.size()).isGreaterThanOrEqualTo(blob.chunks().size() - 2);
            if (holding == largest) {
                worstInLargest = edited.writtenBytes();
            }
        }
        System.out.println("PD-Q-PRF-02 at 64 MiB: worst of " + offsets.size() + " edits " + worst + " bytes; the edit in the largest chunk wrote " + worstInLargest);
        assertThat(worstInLargest).as("an edit inside a chunk over 128 KiB costs more than 128 KiB").isGreaterThan(128 * 1024L);
    }

    /**
     * PD-B-04: {@code of(InputStream)} chunks a stream of any size within 64 MiB of
     * heap. A forked JVM capped at {@code -Xmx64m} streams 300 MiB through
     * {@link HeapBoundedChunkRun} into a store that keeps counts and drops bytes; the
     * root and the chunk count must equal the same stream chunked in this JVM.
     */
    @Test
    void chunksAStreamLargerThanTheHeapWithinSixtyFourMebibytes() throws Exception {
        int mib = 300;
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        ProcessBuilder builder = new ProcessBuilder(java, "-Xmx64m", "-cp", classPath, HeapBoundedChunkRun.class.getName(), String.valueOf(mib));
        builder.redirectErrorStream(true);
        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes());
        int exit = process.waitFor();
        System.out.println("PD-B-04 fork: " + output.trim());
        assertThat(exit).as("the forked run under -Xmx64m: " + output).isZero();
        HeapBoundedChunkRun.Result child = HeapBoundedChunkRun.parse(output);
        assertThat(child.maxHeap()).isLessThanOrEqualTo(64L * MIB);
        HeapBoundedChunkRun.Result mine = HeapBoundedChunkRun.run(mib);
        assertThat(child.root()).isEqualTo(mine.root());
        assertThat(child.chunks()).isEqualTo(mine.chunks());
        assertThat(child.bytes()).isEqualTo((long) mib * MIB);
        assertThat(child.chunks()).isBetween((long) mib * MIB / FastCdc.MAX_SIZE, (long) mib * MIB / FastCdc.MIN_SIZE);
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}
