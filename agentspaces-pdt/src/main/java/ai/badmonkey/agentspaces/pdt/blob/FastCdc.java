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

import ai.badmonkey.agentspaces.common.crypto.Digests;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * The content-defined chunker (srad/agentspaces-pdt.md §3.3, PD-B-01): FastCDC in the
 * per-byte form of the 2016 paper with normalized chunking level 2 and cut-point
 * skipping, pinned so that the Java, Python, and TypeScript chunkers cut the vector
 * {@code pdt_fastcdc_cuts} at the same offsets.
 *
 * <p>The algorithm, exactly:
 * <ul>
 *   <li>Sizes: minimum 16 KiB, average 64 KiB, maximum 256 KiB.</li>
 *   <li>Gear table: {@code GEAR[i]} is the big-endian unsigned 64-bit integer held in
 *       the first eight bytes of {@code sha256} of the single byte {@code i}.</li>
 *   <li>Rolling hash: {@code h = (h << 1) + GEAR[b]} over unsigned 64-bit arithmetic,
 *       starting from zero at index {@code MIN} of the chunk; the first {@code MIN}
 *       bytes are skipped (cut-point skipping).</li>
 *   <li>Masks: {@code 0xFFFFC00000000000} (the top 18 bits) while the chunk is
 *       shorter than the average, {@code 0xFFFC000000000000} (the top 14 bits) after.</li>
 *   <li>Cut: the chunk ends at the first index {@code i >= MIN} where, after folding
 *       byte {@code i} into {@code h}, {@code (h & mask) == 0}; the chunk is the
 *       {@code i} bytes before that index. When no index matches, the chunk ends at
 *       {@code MAX}, or at the end of the input when fewer bytes remain. An input of
 *       at most {@code MIN} bytes is one chunk.</li>
 * </ul>
 *
 * <p>Because the hash shifts left, the test at any index depends only on the 64 bytes
 * that end there, so a cut is a local property of the content: an edit moves the cuts
 * of the chunk it touches and the chunks after it realign with the old cuts.
 *
 * <p>The streaming forms read through a buffer of twice the maximum chunk size, so a
 * stream of any length chunks within a fixed heap (PD-B-04).
 */
public final class FastCdc {

    /** The minimum chunk size, 16 KiB. */
    public static final int MIN_SIZE = 16 * 1024;

    /** The average (normal) chunk size, 64 KiB. */
    public static final int AVG_SIZE = 64 * 1024;

    /** The maximum chunk size, 256 KiB. */
    public static final int MAX_SIZE = 256 * 1024;

    /** The mask used while the chunk is shorter than the average: the top 18 bits. */
    public static final long MASK_S = 0xFFFFC00000000000L;

    /** The mask used once the chunk reaches the average: the top 14 bits. */
    public static final long MASK_L = 0xFFFC000000000000L;

    /** The Gear table, derived from sha256 of each byte value. */
    static final long[] GEAR = gear();

    private FastCdc() {
    }

    private static long[] gear() {
        long[] table = new long[256];
        for (int i = 0; i < 256; i++) {
            byte[] digest = Digests.sha256(new byte[] {(byte) i});
            long value = 0;
            for (int j = 0; j < 8; j++) {
                value = (value << 8) | (digest[j] & 0xFFL);
            }
            table[i] = value;
        }
        return table;
    }

    /**
     * The length of the chunk that starts at an offset.
     *
     * @param data   the bytes
     * @param offset where the chunk starts
     * @param length how many bytes follow the offset, at least 0
     * @return the chunk length, between 1 and {@link #MAX_SIZE}, or 0 when length is 0
     */
    public static int cut(byte[] data, int offset, int length) {
        Objects.requireNonNull(data, "data");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException("range " + offset + "+" + length + " outside " + data.length + " bytes");
        }
        int n = length;
        if (n <= MIN_SIZE) {
            return n;
        }
        int normal = AVG_SIZE;
        if (n >= MAX_SIZE) {
            n = MAX_SIZE;
        } else if (n <= normal) {
            normal = n;
        }
        long h = 0;
        int i = MIN_SIZE;
        for (; i < normal; i++) {
            h = (h << 1) + GEAR[data[offset + i] & 0xFF];
            if ((h & MASK_S) == 0) {
                return i;
            }
        }
        for (; i < n; i++) {
            h = (h << 1) + GEAR[data[offset + i] & 0xFF];
            if ((h & MASK_L) == 0) {
                return i;
            }
        }
        return i;
    }

    /**
     * The cut points of a byte array: the end offset of every chunk, ascending, the last
     * equal to the array length. An empty array has no cut points.
     *
     * @param data the bytes
     * @return the cut points
     */
    public static List<Integer> cuts(byte[] data) {
        Objects.requireNonNull(data, "data");
        List<Integer> cuts = new ArrayList<>();
        int offset = 0;
        while (offset < data.length) {
            int length = cut(data, offset, data.length - offset);
            offset += length;
            cuts.add(offset);
        }
        return cuts;
    }

    /**
     * The chunks of a byte array, in order; an empty array has no chunks.
     *
     * @param data the bytes
     * @return the chunks, each a fresh array
     */
    public static List<byte[]> chunksOf(byte[] data) {
        List<byte[]> chunks = new ArrayList<>();
        int start = 0;
        for (int end : cuts(data)) {
            chunks.add(Arrays.copyOfRange(data, start, end));
            start = end;
        }
        return chunks;
    }

    /**
     * Chunks a stream through a bounded buffer. The iterator reads the stream as it
     * advances and wraps an {@link IOException} in an {@link UncheckedIOException}; the
     * caller closes the stream.
     *
     * @param in the stream
     * @return the chunks in order, each a fresh array
     */
    public static Iterator<byte[]> chunks(InputStream in) {
        Objects.requireNonNull(in, "in");
        return new Chunker(Channels.newChannel(in));
    }

    /**
     * Chunks a channel through a bounded buffer. The caller closes the channel.
     *
     * @param channel the channel
     * @return the chunks in order, each a fresh array
     */
    public static Iterator<byte[]> chunks(ReadableByteChannel channel) {
        Objects.requireNonNull(channel, "channel");
        return new Chunker(channel);
    }

    /** The streaming chunker: a buffer of twice the maximum size, filled ahead of each cut. */
    private static final class Chunker implements Iterator<byte[]> {

        private final ReadableByteChannel channel;
        private final byte[] buffer = new byte[2 * MAX_SIZE];
        private int start;
        private int end;
        private boolean eof;

        Chunker(ReadableByteChannel channel) {
            this.channel = channel;
        }

        private void fill() {
            if (start > 0) {
                System.arraycopy(buffer, start, buffer, 0, end - start);
                end -= start;
                start = 0;
            }
            ByteBuffer target = ByteBuffer.wrap(buffer, end, buffer.length - end);
            try {
                while (!eof && end - start < MAX_SIZE && target.hasRemaining()) {
                    int read = channel.read(target);
                    if (read < 0) {
                        eof = true;
                    } else {
                        end += read;
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("chunking a stream", e);
            }
        }

        @Override
        public boolean hasNext() {
            if (end - start < MAX_SIZE && !eof) {
                fill();
            }
            return end > start;
        }

        @Override
        public byte[] next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            int length = cut(buffer, start, end - start);
            byte[] chunk = Arrays.copyOfRange(buffer, start, start + length);
            start += length;
            return chunk;
        }
    }
}
