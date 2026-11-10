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
import org.bouncycastle.crypto.digests.Blake3Digest;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * BLAKE3 for blob identities and Bao outboards (PDT 2 phase 0, P2-B-02; srad/agentspaces-pdt2.md
 * §3.5). A blob's identity is the BLAKE3 hash of its whole content, named as a CIDv1 of codec
 * {@code raw} over multihash 0x1e, so two publishers of one file share one identity whatever
 * their chunking. The hash is BouncyCastle's {@link Blake3Digest} (P2-B-04).
 *
 * <p>An outboard needs the chaining values of subtrees, which the digest keeps private, so this
 * class also carries the BLAKE3 compression function and computes the chaining value of a chunk
 * group ({@link #groupCv}) and of a parent ({@link #parent}) as the BLAKE3 specification defines
 * them. The root a tree of those values reaches equals the digest; {@code Blake3Test} checks it at
 * every boundary. The compression reads each full block as sixteen little-endian words at once and
 * updates its chaining value in place (PDT 2 phase 4, P2-B-05), since the outboard hashes every byte
 * of a published blob through it.
 */
public final class Blake3 {

    /** The bytes of a BLAKE3 hash and of a chaining value. */
    public static final int DIGEST_BYTES = 32;

    /** The multihash code of BLAKE3. */
    public static final int MULTIHASH = 0x1e;

    /** The bytes of one BLAKE3 chunk. */
    public static final int CHUNK_BYTES = 1024;

    private static final int BLOCK_BYTES = 64;
    private static final VarHandle WORDS = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private static final int CHUNK_START = 1;
    private static final int CHUNK_END = 2;
    private static final int PARENT = 4;
    private static final int ROOT = 8;

    private static final int[] IV = {
        0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A, 0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19
    };

    private static final int[] PERMUTATION = {2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8};

    /** The message word order of each of the seven rounds. */
    private static final int[][] SCHEDULE = schedule();

    private Blake3() {
    }

    /** The BLAKE3 hash of bytes, by BouncyCastle (P2-B-04). */
    public static byte[] digest(byte[] data) {
        Objects.requireNonNull(data, "data");
        Blake3Digest digest = new Blake3Digest(DIGEST_BYTES * 8);
        digest.update(data, 0, data.length);
        byte[] out = new byte[DIGEST_BYTES];
        digest.doFinal(out, 0);
        return out;
    }

    /**
     * The identity CID of a BLAKE3 hash: {@code 0x01 0x55 0x1e 0x20} and the digest, base32.
     *
     * @param digest the 32-byte hash
     * @return the CIDv1 text, which starts {@code bafkr4i}
     */
    public static String identity(byte[] digest) {
        return Cids.identity(digest); // PDT 2 phase 4, P2-B-01: Cids names the identity
    }

    /**
     * The chaining value of the chunks in {@code data[offset, offset + length)}, the first of which
     * is chunk {@code firstChunk} of its blob, as a non-root subtree: the value a parent above
     * the group combines.
     *
     * @param data       the bytes
     * @param offset     where the group starts
     * @param length     the group's bytes, 0 to any multiple of {@link #CHUNK_BYTES} plus a remainder
     * @param firstChunk the blob-wide index of the group's first chunk
     * @return the 32-byte chaining value
     */
    public static byte[] groupCv(byte[] data, int offset, int length, long firstChunk) {
        Objects.checkFromIndexSize(offset, length, data.length);
        return toBytes(subtree(data, offset, length, firstChunk, false));
    }

    /**
     * The chaining value of a parent, or the hash of the tree when it is the root.
     *
     * @param left  the left child's chaining value
     * @param right the right child's chaining value
     * @param root  whether the parent is the root of the whole tree
     * @return 32 bytes
     */
    public static byte[] parent(byte[] left, byte[] right, boolean root) {
        return toBytes(parentWords(toWords(left), toWords(right), root));
    }

    /**
     * The hash of bytes computed through the tree of chunk chaining values; equal to
     * {@link #digest} by the BLAKE3 specification, which tests check.
     */
    static byte[] treeRoot(byte[] data) {
        return toBytes(subtree(data, 0, data.length, 0, true));
    }

    // ------------------------------------------------------------------ the tree

    /** The chaining value (or root hash) of the chunks in a range, split as BLAKE3 splits a tree. */
    private static int[] subtree(byte[] data, int offset, int length, long firstChunk, boolean root) {
        long chunks = Math.max(1, (length + (long) CHUNK_BYTES - 1) / CHUNK_BYTES);
        if (chunks == 1) {
            return chunk(data, offset, length, firstChunk, root);
        }
        long leftChunks = Long.highestOneBit(chunks - 1);
        int leftBytes = (int) (leftChunks * CHUNK_BYTES);
        int[] left = subtree(data, offset, leftBytes, firstChunk, false);
        int[] right = subtree(data, offset + leftBytes, length - leftBytes, firstChunk + leftChunks, false);
        return parentWords(left, right, root);
    }

    private static int[] chunk(byte[] data, int offset, int length, long counter, boolean root) {
        int[] cv = IV.clone();
        int blocks = Math.max(1, (length + BLOCK_BYTES - 1) / BLOCK_BYTES);
        int[] block = new int[16];
        for (int b = 0; b < blocks; b++) {
            int start = offset + b * BLOCK_BYTES;
            int blockLength = Math.min(BLOCK_BYTES, length - b * BLOCK_BYTES);
            loadBlock(data, start, blockLength, block);
            int flags = (b == 0 ? CHUNK_START : 0) | (b == blocks - 1 ? CHUNK_END | (root ? ROOT : 0) : 0);
            compress(cv, block, counter, blockLength, flags);
        }
        return cv;
    }

    private static int[] parentWords(int[] left, int[] right, boolean root) {
        int[] block = new int[16];
        System.arraycopy(left, 0, block, 0, 8);
        System.arraycopy(right, 0, block, 8, 8);
        int[] cv = IV.clone();
        compress(cv, block, 0, BLOCK_BYTES, PARENT | (root ? ROOT : 0));
        return cv;
    }

    // ------------------------------------------------------------------ the compression function

    private static int[][] schedule() {
        int[][] schedule = new int[7][16];
        for (int i = 0; i < 16; i++) {
            schedule[0][i] = i;
        }
        for (int r = 1; r < 7; r++) {
            for (int i = 0; i < 16; i++) {
                schedule[r][i] = schedule[r - 1][PERMUTATION[i]];
            }
        }
        return schedule;
    }

    /** The BLAKE3 compression of one block; the first eight words of its output replace {@code cv}. */
    private static void compress(int[] cv, int[] m, long counter, int blockLength, int flags) {
        int s0 = cv[0], s1 = cv[1], s2 = cv[2], s3 = cv[3], s4 = cv[4], s5 = cv[5], s6 = cv[6], s7 = cv[7];
        int s8 = IV[0], s9 = IV[1], s10 = IV[2], s11 = IV[3];
        int s12 = (int) counter, s13 = (int) (counter >>> 32), s14 = blockLength, s15 = flags;
        for (int[] w : SCHEDULE) {
            // columns
            s0 += s4 + m[w[0]]; s12 = Integer.rotateRight(s12 ^ s0, 16); s8 += s12; s4 = Integer.rotateRight(s4 ^ s8, 12);
            s0 += s4 + m[w[1]]; s12 = Integer.rotateRight(s12 ^ s0, 8); s8 += s12; s4 = Integer.rotateRight(s4 ^ s8, 7);
            s1 += s5 + m[w[2]]; s13 = Integer.rotateRight(s13 ^ s1, 16); s9 += s13; s5 = Integer.rotateRight(s5 ^ s9, 12);
            s1 += s5 + m[w[3]]; s13 = Integer.rotateRight(s13 ^ s1, 8); s9 += s13; s5 = Integer.rotateRight(s5 ^ s9, 7);
            s2 += s6 + m[w[4]]; s14 = Integer.rotateRight(s14 ^ s2, 16); s10 += s14; s6 = Integer.rotateRight(s6 ^ s10, 12);
            s2 += s6 + m[w[5]]; s14 = Integer.rotateRight(s14 ^ s2, 8); s10 += s14; s6 = Integer.rotateRight(s6 ^ s10, 7);
            s3 += s7 + m[w[6]]; s15 = Integer.rotateRight(s15 ^ s3, 16); s11 += s15; s7 = Integer.rotateRight(s7 ^ s11, 12);
            s3 += s7 + m[w[7]]; s15 = Integer.rotateRight(s15 ^ s3, 8); s11 += s15; s7 = Integer.rotateRight(s7 ^ s11, 7);
            // diagonals
            s0 += s5 + m[w[8]]; s15 = Integer.rotateRight(s15 ^ s0, 16); s10 += s15; s5 = Integer.rotateRight(s5 ^ s10, 12);
            s0 += s5 + m[w[9]]; s15 = Integer.rotateRight(s15 ^ s0, 8); s10 += s15; s5 = Integer.rotateRight(s5 ^ s10, 7);
            s1 += s6 + m[w[10]]; s12 = Integer.rotateRight(s12 ^ s1, 16); s11 += s12; s6 = Integer.rotateRight(s6 ^ s11, 12);
            s1 += s6 + m[w[11]]; s12 = Integer.rotateRight(s12 ^ s1, 8); s11 += s12; s6 = Integer.rotateRight(s6 ^ s11, 7);
            s2 += s7 + m[w[12]]; s13 = Integer.rotateRight(s13 ^ s2, 16); s8 += s13; s7 = Integer.rotateRight(s7 ^ s8, 12);
            s2 += s7 + m[w[13]]; s13 = Integer.rotateRight(s13 ^ s2, 8); s8 += s13; s7 = Integer.rotateRight(s7 ^ s8, 7);
            s3 += s4 + m[w[14]]; s14 = Integer.rotateRight(s14 ^ s3, 16); s9 += s14; s4 = Integer.rotateRight(s4 ^ s9, 12);
            s3 += s4 + m[w[15]]; s14 = Integer.rotateRight(s14 ^ s3, 8); s9 += s14; s4 = Integer.rotateRight(s4 ^ s9, 7);
        }
        cv[0] = s0 ^ s8;
        cv[1] = s1 ^ s9;
        cv[2] = s2 ^ s10;
        cv[3] = s3 ^ s11;
        cv[4] = s4 ^ s12;
        cv[5] = s5 ^ s13;
        cv[6] = s6 ^ s14;
        cv[7] = s7 ^ s15;
    }

    /** One block as little-endian words, zero-padded past {@code length}. */
    private static void loadBlock(byte[] data, int start, int length, int[] block) {
        if (length == BLOCK_BYTES) {
            for (int w = 0; w < 16; w++) {
                block[w] = (int) WORDS.get(data, start + (w << 2));
            }
            return;
        }
        java.util.Arrays.fill(block, 0);
        for (int i = 0; i < length; i++) {
            block[i >>> 2] |= (data[start + i] & 0xff) << ((i & 3) << 3);
        }
    }

    private static int[] toWords(byte[] bytes) {
        if (bytes.length != DIGEST_BYTES) {
            throw new IllegalArgumentException("a chaining value holds 32 bytes, found " + bytes.length);
        }
        int[] words = new int[8];
        for (int i = 0; i < DIGEST_BYTES; i++) {
            words[i >>> 2] |= (bytes[i] & 0xff) << ((i & 3) << 3);
        }
        return words;
    }

    private static byte[] toBytes(int[] words) {
        byte[] out = new byte[DIGEST_BYTES];
        for (int i = 0; i < DIGEST_BYTES; i++) {
            out[i] = (byte) (words[i >>> 2] >>> ((i & 3) << 3));
        }
        return out;
    }
}
