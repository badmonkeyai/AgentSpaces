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
package ai.badmonkey.agentspaces.content.tree;

import ai.badmonkey.agentspaces.common.codec.Base32;
import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.crypto.Digests;

import java.util.Arrays;
import java.util.Objects;

/**
 * CIDv1 names for nodes (srad/agentspaces-pdt.md §3.1, PD-N-02): the string is
 * {@code "b"} followed by lower-case base32 of the bytes {@code 0x01, codec, 0x12,
 * 0x20} and the sha2-256 digest, the form {@code ContentRoots} already uses for a
 * content root with codec {@code raw}. A structured node uses codec {@code dag-cbor}
 * ({@link #DAG_CBOR}); a blob chunk uses {@code raw} ({@link #RAW}). {@link #parse}
 * refuses any other codec or multihash by name (PD-N-04).
 *
 * <p>The class lives in the content module because the tree exchange verifies
 * every received node against its CID here (PD-X-04) and the content module
 * must not depend on {@code agentspaces-pdt} (srad/agentspaces-pdt.md §6.9);
 * the pdt module names its nodes through the same class.
 */
public final class Cids {

    /** Multicodec {@code dag-cbor}: every structured node. */
    public static final int DAG_CBOR = 0x71;

    /** Multicodec {@code raw}: every blob chunk. */
    public static final int RAW = 0x55;

    /** Multihash code of sha2-256, the only multihash the design uses. */
    public static final int SHA2_256 = 0x12;

    /** The sha2-256 digest length in bytes. */
    public static final int DIGEST_LENGTH = 32;

    /** The length of a binary CID: version, codec, multihash code, digest length, digest. */
    public static final int BINARY_LENGTH = 4 + DIGEST_LENGTH;

    private static final int VERSION = 0x01;

    /**
     * A parsed CID.
     *
     * @param codec  {@link #DAG_CBOR} or {@link #RAW}
     * @param digest the 32-byte sha2-256 digest
     */
    public record Parsed(int codec, byte[] digest) {

        /** Validates the parts. */
        public Parsed {
            codecName(codec);
            Objects.requireNonNull(digest, "digest");
            if (digest.length != DIGEST_LENGTH) {
                throw new IllegalArgumentException("digest of " + digest.length + " bytes; sha2-256 has " + DIGEST_LENGTH);
            }
            digest = digest.clone();
        }

        /** The digest, copied. */
        @Override
        public byte[] digest() {
            return digest.clone();
        }

        /** Whether the CID names a structured node. */
        public boolean structured() {
            return codec == DAG_CBOR;
        }

        /** The CID string of these parts. */
        public String cid() {
            return ofDigest(digest, codec);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Parsed other && codec == other.codec && Arrays.equals(digest, other.digest);
        }

        @Override
        public int hashCode() {
            return 31 * codec + Arrays.hashCode(digest);
        }

        @Override
        public String toString() {
            return cid();
        }
    }

    private Cids() {
    }

    /**
     * Names bytes.
     *
     * @param bytes the node or chunk bytes
     * @param codec {@link #DAG_CBOR} or {@link #RAW}
     * @return the CID string
     * @throws IllegalArgumentException on a codec the design does not use
     */
    public static String of(byte[] bytes, int codec) {
        Objects.requireNonNull(bytes, "bytes");
        codecName(codec);
        return ofDigest(Digests.sha256(bytes), codec);
    }

    /**
     * Names a digest already computed.
     *
     * @param sha256 the 32-byte sha2-256 digest
     * @param codec  {@link #DAG_CBOR} or {@link #RAW}
     * @return the CID string
     * @throws IllegalArgumentException on a codec the design does not use or a digest of the wrong length
     */
    public static String ofDigest(byte[] sha256, int codec) {
        return Multibase.base32(binaryOf(sha256, codec));
    }

    /**
     * Parses a CID string.
     *
     * @param cid the string
     * @return the codec and digest
     * @throws IllegalArgumentException on a missing multibase prefix, a version other than 1,
     *                                  an unknown codec or multihash, or a digest of the wrong length
     */
    public static Parsed parse(String cid) {
        Objects.requireNonNull(cid, "cid");
        if (cid.length() < 2 || cid.charAt(0) != Multibase.BASE32_PREFIX) {
            throw new IllegalArgumentException("not a base32 CIDv1 (no 'b' prefix): " + cid);
        }
        byte[] binary;
        try {
            binary = Base32.decode(cid.substring(1));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("not a base32 CIDv1: " + cid, e);
        }
        return parseBinary(binary, cid);
    }

    /** Whether a string parses as a CID the design uses. */
    public static boolean isCid(String s) {
        if (s == null) {
            return false;
        }
        try {
            parse(s);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * The binary CID a link carries after its leading zero byte.
     *
     * @param cid the string
     * @return the 36 bytes {@code 0x01, codec, 0x12, 0x20, digest}
     * @throws IllegalArgumentException as {@link #parse}
     */
    public static byte[] binary(String cid) {
        Parsed parsed = parse(cid);
        return binaryOf(parsed.digest, parsed.codec);
    }

    /**
     * The string of a binary CID.
     *
     * @param binary the 36 bytes
     * @return the CID string
     * @throws IllegalArgumentException as {@link #parse}
     */
    public static String fromBinary(byte[] binary) {
        Objects.requireNonNull(binary, "binary");
        return parseBinary(binary, null).cid();
    }

    /**
     * The multicodec name, for messages.
     *
     * @param codec the code
     * @return {@code dag-cbor} or {@code raw}
     * @throws IllegalArgumentException on any other code
     */
    public static String codecName(int codec) {
        return switch (codec) {
            case DAG_CBOR -> "dag-cbor";
            case RAW -> "raw";
            default -> throw new IllegalArgumentException(
                    "unknown codec 0x" + Integer.toHexString(codec) + "; this design uses dag-cbor (0x71) and raw (0x55)");
        };
    }

    private static byte[] binaryOf(byte[] sha256, int codec) {
        Objects.requireNonNull(sha256, "sha256");
        codecName(codec);
        if (sha256.length != DIGEST_LENGTH) {
            throw new IllegalArgumentException("digest of " + sha256.length + " bytes; sha2-256 has " + DIGEST_LENGTH);
        }
        byte[] out = new byte[BINARY_LENGTH];
        out[0] = VERSION;
        out[1] = (byte) codec;
        out[2] = SHA2_256;
        out[3] = DIGEST_LENGTH;
        System.arraycopy(sha256, 0, out, 4, DIGEST_LENGTH);
        return out;
    }

    private static Parsed parseBinary(byte[] binary, String text) {
        String shown = text == null ? "binary CID of " + binary.length + " bytes" : text;
        if (binary.length < 4) {
            throw new IllegalArgumentException("CID too short: " + shown);
        }
        if ((binary[0] & 0xff) != VERSION) {
            throw new IllegalArgumentException("unknown CID version " + (binary[0] & 0xff) + " (only CIDv1): " + shown);
        }
        int codec = binary[1] & 0xff;
        if (codec != DAG_CBOR && codec != RAW) {
            throw new IllegalArgumentException(
                    "unknown codec 0x" + Integer.toHexString(codec) + " (only dag-cbor 0x71 and raw 0x55): " + shown);
        }
        int multihash = binary[2] & 0xff;
        if (multihash != SHA2_256) {
            throw new IllegalArgumentException(
                    "unknown multihash 0x" + Integer.toHexString(multihash) + " (only sha2-256 0x12): " + shown);
        }
        int length = binary[3] & 0xff;
        if (length != DIGEST_LENGTH || binary.length != BINARY_LENGTH) {
            throw new IllegalArgumentException("sha2-256 digest length must be 32 bytes: " + shown);
        }
        return new Parsed(codec, Arrays.copyOfRange(binary, 4, BINARY_LENGTH));
    }
}
