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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.crypto.Digests;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.Objects;

/**
 * The hashing rules of a content root (srad/large-networks-bulk-data.md §3.1 and
 * §6.3): each piece is sha2-256; the table is the piece hashes in order; the root
 * is a CIDv1 (multibase base32, multicodec raw, multihash sha2-256) over the
 * table. A piece verifies against its table entry; a table verifies against the
 * root; a root is bound by the signature on the manifest entry.
 */
public final class ContentRoots {

    /** The CIDv1 prefix: version 1, codec raw (0x55), multihash sha2-256 (0x12), 32 bytes (0x20). */
    private static final byte[] CID_PREFIX = {0x01, 0x55, 0x12, 0x20};

    /** Default piece size (decision 3). */
    public static final int DEFAULT_PIECE_SIZE = 1024 * 1024;

    /** Piece size suggested for a fleet with a windowed datagram binding (LB-M-04). */
    public static final int LOSSY_PIECE_SIZE = 256 * 1024;

    private ContentRoots() {
    }

    /**
     * Computes the root of a piece table.
     *
     * @param table the piece hashes, 32 bytes each
     * @return the CIDv1 string, beginning {@code b}
     */
    public static String rootOf(byte[] table) {
        Objects.requireNonNull(table, "table");
        if (table.length % Manifest.HASH_BYTES != 0) {
            throw new IllegalArgumentException("table of " + table.length + " bytes is not a multiple of 32");
        }
        byte[] digest = Digests.sha256(table);
        byte[] cid = new byte[CID_PREFIX.length + digest.length];
        System.arraycopy(CID_PREFIX, 0, cid, 0, CID_PREFIX.length);
        System.arraycopy(digest, 0, cid, CID_PREFIX.length, digest.length);
        return Multibase.base32(cid);
    }

    /** Whether a reference string names a content root rather than a legacy single block (LB-E-01). */
    public static boolean isRoot(String ref) {
        return ref != null && ref.length() > 1 && ref.charAt(0) == Multibase.BASE32_PREFIX;
    }

    /**
     * Hashes a channel in pieces.
     *
     * @param in        the bytes, read to the end
     * @param pieceSize the piece size
     * @return the table and the size read
     * @throws IOException on a read failure
     */
    public static Hashed hash(ReadableByteChannel in, int pieceSize) throws IOException {
        Objects.requireNonNull(in, "in");
        java.io.ByteArrayOutputStream table = new java.io.ByteArrayOutputStream();
        ByteBuffer buffer = ByteBuffer.allocate(pieceSize);
        long size = 0;
        while (true) {
            buffer.clear();
            while (buffer.hasRemaining()) {
                int n = in.read(buffer);
                if (n < 0) {
                    break;
                }
            }
            int length = buffer.position();
            if (length == 0) {
                break;
            }
            size += length;
            table.writeBytes(Digests.sha256(Arrays.copyOf(buffer.array(), length)));
            if (length < pieceSize) {
                break;
            }
        }
        byte[] bytes = table.toByteArray();
        return new Hashed(bytes, size, rootOf(bytes));
    }

    /** As {@link #hash(ReadableByteChannel, int)} for a stream. */
    public static Hashed hash(InputStream in, int pieceSize) throws IOException {
        return hash(java.nio.channels.Channels.newChannel(in), pieceSize);
    }

    /** As {@link #hash(ReadableByteChannel, int)} for bytes in memory. */
    public static Hashed hash(byte[] bytes, int pieceSize) {
        try {
            return hash(new java.io.ByteArrayInputStream(bytes), pieceSize);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /**
     * Verifies a piece against its table entry.
     *
     * @param table the piece table
     * @param index the piece index
     * @param piece the bytes
     * @return whether the piece hashes to the table entry
     */
    public static boolean verifies(byte[] table, int index, byte[] piece) {
        if (index < 0 || (long) (index + 1) * Manifest.HASH_BYTES > table.length) {
            return false;
        }
        byte[] expected = Arrays.copyOfRange(table, index * Manifest.HASH_BYTES, (index + 1) * Manifest.HASH_BYTES);
        return java.security.MessageDigest.isEqual(expected, Digests.sha256(piece));
    }

    /** A hashed artifact: its table, its size, and its root. */
    public record Hashed(byte[] table, long size, String root) {
        /** The piece count for a piece size. */
        public int pieces(int pieceSize) {
            return size == 0 ? 0 : (int) ((size + pieceSize - 1) / pieceSize);
        }
    }
}
