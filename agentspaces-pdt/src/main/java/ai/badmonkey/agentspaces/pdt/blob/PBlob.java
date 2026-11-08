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
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeStore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A persistent byte sequence (srad/agentspaces-pdt.md §3.3; PD-B-03, PD-B-04): the
 * bytes are cut by {@link FastCdc}, each chunk is a {@code raw} node, and a
 * {@link BlobTree} indexes the chunks. A value is immutable; {@link #splice} and
 * {@link #append} return a new value that shares every unchanged chunk and interior
 * node with this one.
 *
 * <p>An edit re-chunks from the start of the chunk holding the first changed byte
 * and stops at the first new cut that coincides with an old cut past the edit, after
 * which every old chunk returns unchanged (PD-B-03): because a FastCDC cut depends on
 * the 64 bytes before it and nothing earlier, the cuts realign within a chunk or two.
 * An append after a chunk the maximum size closed starts after that chunk, since a
 * cut by the maximum does not depend on the bytes that follow it.
 *
 * <p>{@link #of(Path, NodeStore)} and {@link #of(InputStream, NodeStore)} chunk a
 * stream of any length through {@link FastCdc}'s bounded buffer and write only the
 * chunks the store lacks; {@link #of(Path, NodeStore, PBlob)} compares the file with
 * a previous value, finds the changed middle, and edits it in as one splice, so the
 * new value shares every unchanged chunk and interior node (PD-B-04).
 *
 * <p>{@link #writtenBytes()} reports what the operation that produced a value wrote
 * to the store that the store lacked, chunks and interior nodes together: the delta
 * the tests of PD-B-06 and PD-Q-PRF-02/04 measure.
 *
 * <p>Every chunk read checks the chunk's length against the entry the tree holds for
 * it ({@link BlobTree#chunk(BlobTree.Entry)}), so a crafted interior node whose byte
 * counts lie ends in a {@link NodeFormatException} naming the chunk, before any
 * misaligned read.
 */
public final class PBlob {

    private final NodeStore store;
    private final BlobTree tree;
    private final long writtenChunkBytes;
    private final int writtenChunks;
    private long[] starts;

    private PBlob(NodeStore store, BlobTree tree, long writtenChunkBytes, int writtenChunks) {
        this.store = store;
        this.tree = tree;
        this.writtenChunkBytes = writtenChunkBytes;
        this.writtenChunks = writtenChunks;
    }

    // ------------------------------------------------------------------ factories

    /** The empty blob, whose root is the empty chunk. */
    public static PBlob empty(NodeStore store) {
        return new PBlob(store, BlobTree.build(List.of(), store), 0, 0);
    }

    /**
     * Opens a blob the store already holds.
     *
     * @param store the store
     * @param root  the root CID
     * @return the blob
     * @throws NodeFormatException when the root is absent or is not a blob
     */
    public static PBlob load(NodeStore store, String root) {
        return new PBlob(store, BlobTree.open(store, root), 0, 0);
    }

    /**
     * A blob of the bytes in memory.
     *
     * @param bytes the content
     * @param store the store
     * @return the blob
     */
    public static PBlob of(byte[] bytes, NodeStore store) {
        Objects.requireNonNull(bytes, "bytes");
        return of(new ByteArrayInputStream(bytes), store);
    }

    /**
     * A blob of a stream, chunked through a bounded buffer. The caller closes the stream.
     *
     * @param in    the content
     * @param store the store
     * @return the blob
     */
    public static PBlob of(InputStream in, NodeStore store) {
        Objects.requireNonNull(in, "in");
        Objects.requireNonNull(store, "store");
        Writer writer = new Writer(store);
        Iterator<byte[]> chunks = FastCdc.chunks(in);
        List<BlobTree.Entry> entries = new ArrayList<>();
        while (chunks.hasNext()) {
            entries.add(writer.write(chunks.next()));
        }
        return new PBlob(store, BlobTree.build(entries, store), writer.bytes, writer.count);
    }

    /**
     * A blob of a file.
     *
     * @param file  the file
     * @param store the store
     * @return the blob
     * @throws UncheckedIOException when the file cannot be read
     */
    public static PBlob of(Path file, NodeStore store) {
        Objects.requireNonNull(file, "file");
        try (InputStream in = Files.newInputStream(file)) {
            return of(in, store);
        } catch (IOException e) {
            throw new UncheckedIOException("reading " + file, e);
        }
    }

    /**
     * A blob of a file that shares every unchanged chunk and interior node with a
     * previous value: the common prefix and suffix of the file and the previous value
     * stay, and the middle is spliced in with one edit.
     *
     * @param file     the file
     * @param store    the store
     * @param previous the previous value, or null to chunk from scratch; a value held
     *                 in another store is treated as null
     * @return the blob; {@code previous} itself when the file equals it
     * @throws UncheckedIOException when the file cannot be read
     */
    public static PBlob of(Path file, NodeStore store, PBlob previous) {
        Objects.requireNonNull(file, "file");
        Objects.requireNonNull(store, "store");
        if (previous == null || previous.store != store) {
            return of(file, store);
        }
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            long newSize = channel.size();
            long prefix = previous.commonPrefix(channel, newSize);
            if (prefix == newSize && prefix == previous.size()) {
                return previous;
            }
            long suffix = previous.commonSuffix(channel, newSize, prefix);
            long insertLength = newSize - prefix - suffix;
            channel.position(prefix);
            InputStream middle = new BoundedChannelStream(channel, insertLength);
            return previous.edit(prefix, previous.size() - prefix - suffix, middle, insertLength);
        } catch (IOException e) {
            throw new UncheckedIOException("reading " + file, e);
        }
    }

    // ------------------------------------------------------------------ reads

    /** The size in bytes. */
    public long size() {
        return tree.size();
    }

    /** The root CID. */
    public String root() {
        return tree.root();
    }

    /** The tree. */
    public BlobTree tree() {
        return tree;
    }

    /** The store. */
    public NodeStore store() {
        return store;
    }

    /** The chunks in order. */
    public List<BlobTree.Entry> chunks() {
        return tree.chunks();
    }

    /** The chunk and interior bytes the operation that produced this value wrote to the store. */
    public long writtenBytes() {
        return writtenChunkBytes + tree.writtenBytes();
    }

    /** The chunk bytes the operation that produced this value wrote to the store. */
    public long writtenChunkBytes() {
        return writtenChunkBytes;
    }

    /** The interior node bytes the operation that produced this value wrote to the store. */
    public long writtenNodeBytes() {
        return tree.writtenBytes();
    }

    /** How many chunks the operation that produced this value wrote to the store. */
    public int writtenChunks() {
        return writtenChunks;
    }

    /**
     * Opens a range for reading. Chunks load from the store as the stream advances,
     * each verified against its CID.
     *
     * @param offset the first byte
     * @param length how many bytes, clipped to the end of the blob
     * @return the stream
     */
    public InputStream open(long offset, long length) {
        checkRange(offset, length);
        return new RangeStream(offset, Math.min(length, size() - offset));
    }

    /**
     * Opens the whole blob as a read-only channel.
     *
     * @return the channel; {@code write} and {@code truncate} throw {@link NonWritableChannelException}
     */
    public SeekableByteChannel channel() {
        return new ReadChannel();
    }

    /**
     * Reads a range into memory.
     *
     * @param offset the first byte
     * @param length how many bytes, clipped to the end of the blob
     * @return the bytes
     */
    public byte[] read(long offset, int length) {
        checkRange(offset, length);
        int actual = (int) Math.min(length, size() - offset);
        byte[] out = new byte[actual];
        int done = 0;
        try (InputStream in = open(offset, actual)) {
            while (done < actual) {
                int n = in.read(out, done, actual - done);
                if (n < 0) {
                    break;
                }
                done += n;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return done == actual ? out : Arrays.copyOf(out, done);
    }

    /** The whole content in memory; for small blobs and tests. */
    public byte[] bytes() {
        if (size() > Integer.MAX_VALUE - 16) {
            throw new IllegalStateException("blob of " + size() + " bytes does not fit one array");
        }
        return read(0, (int) size());
    }

    private void checkRange(long offset, long length) {
        if (offset < 0 || offset > size()) {
            throw new IndexOutOfBoundsException("offset " + offset + " outside a blob of " + size() + " bytes");
        }
        if (length < 0) {
            throw new IllegalArgumentException("negative length " + length);
        }
    }

    // ------------------------------------------------------------------ edits

    /**
     * A new value with a range replaced.
     *
     * @param offset       the first byte to replace, at most {@link #size()}
     * @param removeLength how many bytes to drop, at most {@code size() - offset}
     * @param insert       the bytes to put in their place
     * @return the new value
     */
    public PBlob splice(long offset, long removeLength, byte[] insert) {
        Objects.requireNonNull(insert, "insert");
        return edit(offset, removeLength, new ByteArrayInputStream(insert), insert.length);
    }

    /**
     * A new value with bytes added at the end.
     *
     * @param bytes the bytes
     * @return the new value
     */
    public PBlob append(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        return edit(size(), 0, new ByteArrayInputStream(bytes), bytes.length);
    }

    /**
     * A new value with a stream added at the end. The caller closes the stream.
     *
     * @param in the bytes
     * @return the new value
     */
    public PBlob append(InputStream in) {
        Objects.requireNonNull(in, "in");
        return edit(size(), 0, in, -1);
    }

    /**
     * The edit every change goes through.
     *
     * @param offset       the first changed byte
     * @param removeLength the old bytes dropped
     * @param insert       the new bytes
     * @param insertLength their count, or -1 when unknown, which is allowed only when nothing follows the edit
     */
    private PBlob edit(long offset, long removeLength, InputStream insert, long insertLength) {
        long oldSize = size();
        if (offset < 0 || offset > oldSize) {
            throw new IndexOutOfBoundsException("offset " + offset + " outside a blob of " + oldSize + " bytes");
        }
        if (removeLength < 0 || offset + removeLength > oldSize) {
            throw new IllegalArgumentException("cannot remove " + removeLength + " bytes at " + offset
                    + " from a blob of " + oldSize + " bytes");
        }
        if (insertLength < 0 && offset + removeLength != oldSize) {
            throw new IllegalArgumentException("an insert of unknown length must reach the end of the blob");
        }
        List<BlobTree.Entry> old = chunks();
        long[] ends = starts();
        int first = firstChunk(offset, old, ends);
        long chunkStart = first == 0 ? 0 : ends[first - 1];
        long editEnd = insertLength < 0 ? Long.MAX_VALUE : offset + insertLength;
        long delta = insertLength < 0 ? 0 : insertLength - removeLength;

        InputStream prefix = open(chunkStart, offset - chunkStart);
        InputStream suffix = open(offset + removeLength, oldSize - offset - removeLength);
        InputStream stream = new SequenceInputStream(new SequenceInputStream(prefix, insert), suffix);

        Writer writer = new Writer(store);
        List<BlobTree.Entry> fresh = new ArrayList<>();
        Iterator<byte[]> chunks = FastCdc.chunks(stream);
        long position = chunkStart;
        int resumeAt = old.size();
        while (chunks.hasNext()) {
            byte[] chunk = chunks.next();
            fresh.add(writer.write(chunk));
            position += chunk.length;
            if (position >= editEnd) {
                int k = oldChunkEndingAt(position - delta, ends, first);
                if (k >= 0) {
                    resumeAt = k + 1;
                    break;
                }
            }
        }
        List<BlobTree.Entry> level0 = new ArrayList<>(first + fresh.size() + old.size() - resumeAt);
        level0.addAll(old.subList(0, first));
        level0.addAll(fresh);
        level0.addAll(old.subList(resumeAt, old.size()));
        return new PBlob(store, BlobTree.build(level0, store), writer.bytes, writer.count);
    }

    /** The index of the chunk re-chunking starts at: the one holding the offset, or the one before an append. */
    private static int firstChunk(long offset, List<BlobTree.Entry> old, long[] ends) {
        if (old.isEmpty()) {
            return 0;
        }
        if (offset >= ends[ends.length - 1]) {
            BlobTree.Entry last = old.get(old.size() - 1);
            return last.length() >= FastCdc.MAX_SIZE ? old.size() : old.size() - 1;
        }
        int i = Arrays.binarySearch(ends, offset);
        return i >= 0 ? i + 1 : -i - 1;
    }

    /** The index of the old chunk that ends at a position, or -1 when no chunk at or after {@code from} does. */
    private static int oldChunkEndingAt(long position, long[] ends, int from) {
        int i = Arrays.binarySearch(ends, from, ends.length, position);
        return i >= 0 ? i : -1;
    }

    /** The end offset of each chunk, cumulative. */
    private synchronized long[] starts() {
        if (starts == null) {
            List<BlobTree.Entry> chunks = chunks();
            long[] ends = new long[chunks.size()];
            long sum = 0;
            for (int i = 0; i < ends.length; i++) {
                sum += chunks.get(i).length();
                ends[i] = sum;
            }
            starts = ends;
        }
        return starts;
    }

    /** The length of the longest common prefix of this value and a file. */
    private long commonPrefix(FileChannel channel, long newSize) throws IOException {
        long common = 0;
        long limit = Math.min(size(), newSize);
        for (BlobTree.Entry entry : chunks()) {
            if (common >= limit) {
                break;
            }
            byte[] mine = tree.chunk(entry);
            int compare = (int) Math.min(mine.length, limit - common);
            byte[] theirs = readAt(channel, common, compare);
            int same = Arrays.mismatch(mine, 0, compare, theirs, 0, compare);
            if (same >= 0) {
                return common + same;
            }
            common += compare;
        }
        return common;
    }

    /** The length of the longest common suffix past the prefix. */
    private long commonSuffix(FileChannel channel, long newSize, long prefix) throws IOException {
        long limit = Math.min(size(), newSize) - prefix;
        long common = 0;
        List<BlobTree.Entry> chunks = chunks();
        long[] ends = starts();
        for (int i = chunks.size() - 1; i >= 0 && common < limit; i--) {
            byte[] mine = tree.chunk(chunks.get(i));
            int compare = (int) Math.min(mine.length, limit - common);
            long myStart = ends[i] - compare;
            long theirStart = newSize - common - compare;
            byte[] theirs = readAt(channel, theirStart, compare);
            int mineFrom = mine.length - compare;
            int same = 0;
            while (same < compare && mine[mineFrom + compare - 1 - same] == theirs[compare - 1 - same]) {
                same++;
            }
            if (same < compare) {
                return common + same;
            }
            common += compare;
            if (myStart == 0) {
                break;
            }
        }
        return common;
    }

    private static byte[] readAt(FileChannel channel, long position, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        long at = position;
        while (buffer.hasRemaining()) {
            int n = channel.read(buffer, at);
            if (n < 0) {
                throw new IOException("file shorter than expected at " + at);
            }
            at += n;
        }
        return buffer.array();
    }

    // ------------------------------------------------------------------ helpers

    /** Writes chunks the store lacks and counts them. */
    private static final class Writer {
        private final NodeStore store;
        long bytes;
        int count;

        Writer(NodeStore store) {
            this.store = store;
        }

        BlobTree.Entry write(byte[] chunk) {
            NodeStore.Stored stored = store.prepare(chunk, Cids.RAW);
            String cid = stored.cid();
            if (!store.has(cid)) {
                store.putVerified(cid, stored.bytes());
                bytes += chunk.length;
                count++;
            }
            return new BlobTree.Entry(cid, chunk.length);
        }
    }

    /** A stream over a range, one chunk in memory at a time. */
    private final class RangeStream extends InputStream {
        private long remaining;
        private int index;
        private byte[] chunk;
        private int inChunk;

        RangeStream(long offset, long length) {
            this.remaining = length;
            if (length > 0) {
                long[] ends = starts();
                int i = Arrays.binarySearch(ends, offset);
                index = i >= 0 ? i + 1 : -i - 1;
                long start = index == 0 ? 0 : ends[index - 1];
                inChunk = (int) (offset - start);
            }
        }

        private boolean ready() {
            if (remaining <= 0) {
                return false;
            }
            if (chunk == null || inChunk >= chunk.length) {
                if (chunk != null) {
                    index++;
                    inChunk = 0;
                }
                chunk = tree.chunk(chunks().get(index));
            }
            return true;
        }

        @Override
        public int read() {
            if (!ready()) {
                return -1;
            }
            remaining--;
            return chunk[inChunk++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            Objects.checkFromIndexSize(off, len, b.length);
            if (len == 0) {
                return 0;
            }
            if (!ready()) {
                return -1;
            }
            int n = (int) Math.min(Math.min(len, chunk.length - inChunk), remaining);
            System.arraycopy(chunk, inChunk, b, off, n);
            inChunk += n;
            remaining -= n;
            return n;
        }

        @Override
        public int available() {
            return (int) Math.min(remaining, Integer.MAX_VALUE);
        }
    }

    /** A read-only seekable channel over the whole blob. */
    private final class ReadChannel implements SeekableByteChannel {
        private long position;
        private boolean open = true;
        private int index = -1;
        private byte[] chunk;

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (!open) {
                throw new IOException("channel closed");
            }
            if (position >= size()) {
                return -1;
            }
            int total = 0;
            long[] ends = starts();
            while (dst.hasRemaining() && position < size()) {
                int i = Arrays.binarySearch(ends, position);
                int at = i >= 0 ? i + 1 : -i - 1;
                if (at != index) {
                    chunk = tree.chunk(chunks().get(at));
                    index = at;
                }
                long start = at == 0 ? 0 : ends[at - 1];
                int inChunk = (int) (position - start);
                int n = Math.min(dst.remaining(), chunk.length - inChunk);
                dst.put(chunk, inChunk, n);
                position += n;
                total += n;
            }
            return total;
        }

        @Override
        public int write(ByteBuffer src) {
            throw new NonWritableChannelException();
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public SeekableByteChannel position(long newPosition) {
            if (newPosition < 0) {
                throw new IllegalArgumentException("negative position " + newPosition);
            }
            position = newPosition;
            return this;
        }

        @Override
        public long size() {
            return PBlob.this.size();
        }

        @Override
        public SeekableByteChannel truncate(long size) {
            throw new NonWritableChannelException();
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
            chunk = null;
        }
    }

    /** A stream over a bounded range of a channel, from its current position. */
    private static final class BoundedChannelStream extends InputStream {
        private final FileChannel channel;
        private long remaining;

        BoundedChannelStream(FileChannel channel, long length) {
            this.channel = channel;
            this.remaining = length;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : one[0] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            Objects.checkFromIndexSize(off, len, b.length);
            if (len == 0) {
                return 0;
            }
            if (remaining <= 0) {
                return -1;
            }
            int want = (int) Math.min(len, remaining);
            int n = channel.read(ByteBuffer.wrap(b, off, want));
            if (n < 0) {
                throw new IOException("file shorter than expected");
            }
            remaining -= n;
            return n;
        }
    }
}
