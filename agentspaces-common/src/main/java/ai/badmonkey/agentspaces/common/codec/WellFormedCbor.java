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
package ai.badmonkey.agentspaces.common.codec;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Walks the raw bytes of a CBOR item before Jackson reads them, in one of two modes.
 *
 * <p>The lenient mode (HLC round five, 2026-11-08, decision E8) reads what this node persisted
 * before the wire encoding and refuses:
 *
 * <ul>
 *   <li>every tag except the three Jackson writes for big numbers as values: tag 2 or 3 over
 *       a definite-length byte string (a {@link java.math.BigInteger}) and tag 4 over a
 *       two-element array of an integer exponent and an integer or bignum mantissa (a
 *       {@link java.math.BigDecimal});</li>
 *   <li>a tag in map key position, since no encoder writes one;</li>
 *   <li>a text string, map keys and each chunk of an indefinite-length text string included,
 *       whose bytes are not well-formed shortest-form UTF-8 (RFC 3629), which also refuses an
 *       encoded surrogate and a code point above U+10FFFF;</li>
 *   <li>an item that is not well-formed (RFC 8949 section 3), and nesting deeper than the
 *       limit Jackson enforces after this walk.</li>
 * </ul>
 *
 * <p>The strict mode checks every document a receiver reads from the network against the item
 * rules S1 to S12 of the wire encoding (HLC round six, 2026-11-08, owner ruling F3): one item and
 * no trailing byte, at most the nesting limit with the two-element array of a decimal counted as
 * a level as Jackson counts it, the shortest head everywhere, integers inside a Java {@code long}
 * and bignums only outside it in their minimal magnitude form, normalized decimals with an
 * exponent inside an {@code int} less its minimum, definite byte strings and arrays, indefinite
 * maps with unique text keys, text chunked exactly as Jackson chunks it, float32 and float64 with
 * one NaN per width, and only {@code false}, {@code true} and {@code null} among the simple values.
 * Both modes refuse a bignum past {@link #BIGNUM_BYTES} bytes, as Jackson does after them.
 * A document that passes holds the one encoding of its value that every implementation writes.
 */
final class WellFormedCbor {

    /** Jackson writes a text longer than this many UTF-16 code units as an indefinite-length text. */
    static final int TEXT_CHUNK = 3996;

    /**
     * The longest bignum byte string, a leading {@code 00} included (HLC round seven, 2026-11-08,
     * S9): Jackson's {@code StreamReadConstraints} read a number of at most 1000 bytes, alone or as
     * a decimal's mantissa, so the walk refuses a longer one in both modes and every read, the item
     * check a persistent-tree node relies on included, refuses what Jackson refuses.
     */
    static final int BIGNUM_BYTES = 1000;

    /** The largest decimal exponent magnitude: -(2^31-1) to 2^31-1, so Java negates it safely. */
    private static final long EXPONENT_LIMIT = Integer.MAX_VALUE;

    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

    private final byte[] in;
    private final int maxDepth;
    private final boolean strict;
    private int pos;

    private WellFormedCbor(byte[] in, int maxDepth, boolean strict) {
        this.in = in;
        this.maxDepth = maxDepth;
        this.strict = strict;
    }

    /**
     * Checks the first CBOR item of {@code input} in the lenient mode.
     *
     * @param input    the encoded bytes
     * @param maxDepth the deepest container nesting accepted
     * @throws IOException naming the first defect and its offset
     */
    static void check(byte[] input, int maxDepth) throws IOException {
        new WellFormedCbor(input, maxDepth, false).item(0);
    }

    /**
     * Checks that {@code input} holds exactly one item in the wire encoding (S1 to S12).
     *
     * @param input    the encoded bytes
     * @param maxDepth the deepest container nesting accepted
     * @throws IOException naming the first defect and its offset
     */
    static void checkCanonical(byte[] input, int maxDepth) throws IOException {
        WellFormedCbor walk = new WellFormedCbor(input, maxDepth, true);
        walk.item(0);
        if (walk.pos != input.length) {
            throw defect(walk.pos, "a byte after the document's one item (S1)");
        }
    }

    /** Reads one item, its head at {@link #pos}, inside {@code depth} containers. */
    private void item(int depth) throws IOException {
        int start = pos;
        int initial = next();
        int major = initial >>> 5;
        int info = initial & 0x1F;
        if (info == 31) {
            indefinite(start, major, depth);
            return;
        }
        if (major == 7) {
            simple(start, info);
            return;
        }
        long argument = argument(start, info);
        switch (major) {
            case 0, 1 -> {
                if (strict && argument < 0) {
                    throw defect(start, "an integer outside a Java long is a bignum (S4)");
                }
            }
            case 2 -> skip(start, argument);
            case 3 -> {
                int from = pos;
                utf8(start, argument);
                if (strict && utf16Length(from, pos) > TEXT_CHUNK) {
                    throw defect(start, "a text past " + TEXT_CHUNK + " UTF-16 units is chunked (S6)");
                }
            }
            case 4 -> {
                int inner = enter(start, depth);
                long items = count(start, argument, 1);
                for (long i = 0; i < items; i++) {
                    item(inner);
                }
            }
            case 5 -> {
                if (strict) {
                    throw defect(start, "a map is indefinite-length (S8)");
                }
                int inner = enter(start, depth);
                long pairs = count(start, argument, 2);
                for (long i = 0; i < pairs; i++) {
                    key(inner);
                    item(inner);
                }
            }
            default -> tagged(start, argument, depth);
        }
    }

    /**
     * A map key: any untagged item in the lenient mode, a text string in the strict mode. No
     * encoder writes a tagged key, so a tag in key position is refused, the big-number tags
     * included (HLC round five, 2026-11-08, decision E8).
     *
     * @return the key's text in the strict mode, else null
     */
    private String key(int depth) throws IOException {
        int start = pos;
        int head = peek(pos);
        if (head >>> 5 == 6) {
            throw defect(pos, "a tag in map key position is refused");
        }
        if (strict && head >>> 5 != 3) {
            throw defect(pos, "a map key is a text string (S8)");
        }
        item(depth);
        return strict ? textOf(start) : null;
    }

    /** The text of the (already checked) text item at {@code start}, chunks joined. */
    private String textOf(int start) {
        int head = in[start] & 0xFF;
        if ((head & 0x1F) != 31) {
            int at = start;
            long length = argumentAt(at);
            int from = at + headLength(head);
            return new String(in, from, (int) length, StandardCharsets.UTF_8);
        }
        StringBuilder text = new StringBuilder();
        int at = start + 1;
        while ((in[at] & 0xFF) != 0xFF) {
            int chunkHead = in[at] & 0xFF;
            int length = (int) argumentAt(at);
            int from = at + headLength(chunkHead);
            text.append(new String(in, from, length, StandardCharsets.UTF_8));
            at = from + length;
        }
        return text.toString();
    }

    /** An indefinite-length string, array, or map (RFC 8949 section 3.2). */
    private void indefinite(int start, int major, int depth) throws IOException {
        switch (major) {
            case 2, 3 -> {
                if (strict && major == 2) {
                    throw defect(start, "a byte string is definite-length (S5)");
                }
                List<int[]> chunks = new ArrayList<>();
                while (peek(start) != 0xFF) {
                    int chunk = pos;
                    int head = next();
                    if (head >>> 5 != major || (head & 0x1F) == 31) {
                        throw defect(chunk,
                                "a chunk of an indefinite-length string is a definite string of its kind");
                    }
                    long length = argument(chunk, head & 0x1F);
                    int from = pos;
                    if (major == 3) {
                        utf8(chunk, length);
                    } else {
                        skip(chunk, length);
                    }
                    chunks.add(new int[]{from, pos});
                }
                if (strict) {
                    chunking(start, chunks);
                }
            }
            case 4 -> {
                if (strict) {
                    throw defect(start, "an array is definite-length (S7)");
                }
                int inner = enter(start, depth);
                while (peek(start) != 0xFF) {
                    item(inner);
                }
            }
            case 5 -> {
                int inner = enter(start, depth);
                Set<String> keys = strict ? new HashSet<>() : null;
                while (peek(start) != 0xFF) {
                    int at = pos;
                    String key = key(inner);
                    if (strict && !keys.add(key)) {
                        throw defect(at, "a key occurs twice in one map (S8)");
                    }
                    item(inner);
                }
            }
            case 7 -> throw defect(start, "a break outside an indefinite-length item");
            default -> throw defect(start, "major type " + major + " has no indefinite length");
        }
        pos++; // the break
    }

    /**
     * The chunk boundaries Jackson writes (S6): only a text past {@link #TEXT_CHUNK} UTF-16 units
     * is chunked; while more than that remain the next chunk takes {@link #TEXT_CHUNK} units, or
     * one fewer when its last unit would be a high surrogate, and the rest forms the last chunk.
     */
    private void chunking(int start, List<int[]> chunks) throws IOException {
        StringBuilder joined = new StringBuilder();
        int[] lengths = new int[chunks.size()];
        for (int i = 0; i < chunks.size(); i++) {
            String chunk = new String(in, chunks.get(i)[0], chunks.get(i)[1] - chunks.get(i)[0],
                    StandardCharsets.UTF_8);
            lengths[i] = chunk.length();
            joined.append(chunk);
        }
        int remaining = joined.length();
        if (remaining <= TEXT_CHUNK) {
            throw defect(start, "a text of " + TEXT_CHUNK + " UTF-16 units or fewer is definite (S6)");
        }
        int offset = 0;
        for (int i = 0; i < lengths.length; i++) {
            int expected = remaining > TEXT_CHUNK
                    ? (Character.isHighSurrogate(joined.charAt(offset + TEXT_CHUNK - 1)) ? TEXT_CHUNK - 1 : TEXT_CHUNK)
                    : remaining;
            if (lengths[i] != expected || (i == lengths.length - 1) != (remaining == expected)) {
                throw defect(start, "a text chunked other than as Jackson chunks it (S6)");
            }
            offset += expected;
            remaining -= expected;
        }
    }

    /**
     * A tagged item: only the big-number tags Jackson writes, each over the item it writes (HLC
     * round five, 2026-11-08, decision E8); in the strict mode each in its one form (S9).
     */
    private void tagged(int start, long tag, int depth) throws IOException {
        int content = peek(start);
        if (tag == 2 || tag == 3) {
            if (content >>> 5 != 2 || (content & 0x1F) == 31) {
                throw defect(start, "tag " + tag + " encloses a definite-length byte string");
            }
            int from = pos;
            item(depth);
            if (pos - from - headLength(content) > BIGNUM_BYTES) {
                throw defect(start, "a bignum holds at most " + BIGNUM_BYTES + " bytes (S9)");
            }
            if (strict) {
                bignum(start, tag, from);
            }
        } else if (tag == 4) {
            if (content != 0x82) {
                throw defect(start, "tag 4 encloses a two-element array");
            }
            // HLC round six, 2026-11-08 (S2): Jackson counts the decimal's array as a level
            int inner = strict ? enter(start, depth) : depth;
            pos++;
            int exponent = peek(start);
            if (exponent >>> 5 > 1) {
                throw defect(pos, "the exponent of tag 4 is an integer");
            }
            int exponentAt = pos;
            item(inner);
            int mantissa = peek(start);
            if (mantissa >>> 5 > 1 && mantissa != 0xC2 && mantissa != 0xC3) {
                throw defect(pos, "the mantissa of tag 4 is an integer or a bignum");
            }
            int mantissaAt = pos;
            item(inner);
            if (strict) {
                decimal(start, exponentAt, mantissaAt);
            }
        } else {
            throw defect(start, "tag " + Long.toUnsignedString(tag)
                    + " is refused; only the big-number tags 2, 3, and 4 decode");
        }
    }

    /**
     * S9: a bignum holds {@code BigInteger.toByteArray()} of a magnitude outside a Java long,
     * above 2^63-1 under tag 2 and above 2^63 under tag 3.
     */
    private void bignum(int start, long tag, int item) throws IOException {
        int head = in[item] & 0xFF;
        int from = item + headLength(head);
        int length = pos - from;
        if (length == 0 || (in[from] & 0x80) != 0
                || (length > 1 && in[from] == 0 && (in[from + 1] & 0x80) == 0)) {
            throw defect(start, "a bignum holds its magnitude in minimal two's complement (S9)");
        }
        byte[] magnitude = java.util.Arrays.copyOfRange(in, from, pos);
        BigInteger value = new BigInteger(magnitude);
        BigInteger floor = tag == 2 ? LONG_MAX : LONG_MAX.add(BigInteger.ONE);
        if (value.compareTo(floor) <= 0) {
            throw defect(start, "a value inside a Java long is a plain integer (S9)");
        }
    }

    /** S9: a normalized decimal, its exponent inside -(2^31-1) to 2^31-1. */
    private void decimal(int start, int exponentAt, int mantissaAt) throws IOException {
        long exponent = signedAt(exponentAt);
        if (exponent < -EXPONENT_LIMIT || exponent > EXPONENT_LIMIT) {
            throw defect(start, "a decimal exponent lies inside -(2^31-1) to 2^31-1 (S9)");
        }
        int mantissaHead = in[mantissaAt] & 0xFF;
        BigInteger mantissa;
        if (mantissaHead >>> 5 == 6) {
            int content = mantissaAt + 1;
            int from = content + headLength(in[content] & 0xFF);
            BigInteger magnitude = new BigInteger(1, java.util.Arrays.copyOfRange(in, from, pos));
            mantissa = mantissaHead == 0xC3 ? magnitude.negate() : magnitude;
        } else {
            mantissa = BigInteger.valueOf(signedAt(mantissaAt));
        }
        boolean zero = mantissa.signum() == 0;
        if (zero ? exponent != 0 : mantissa.mod(BigInteger.TEN).signum() == 0) {
            throw defect(start, "a decimal is normalized: no trailing zero, and zero is [0, 0] (S9)");
        }
    }

    /** The value of the plain integer at {@code at}, already checked to fit a long. */
    private long signedAt(int at) {
        long argument = argumentAt(at);
        return (in[at] & 0xFF) >>> 5 == 1 ? -1 - argument : argument;
    }

    /**
     * Major type 7 (S10, S11): a simple value or a float. The lenient mode refuses only a
     * two-byte simple value below 32; the strict mode keeps false, true, null, float32 and
     * float64, each NaN as Java's constants hold it.
     */
    private void simple(int start, int info) throws IOException {
        if (!strict) {
            long argument = argument(start, info);
            if (info == 24 && argument < 32) {
                throw defect(start, "a two-byte simple value below 32");
            }
            return;
        }
        switch (info) {
            case 20, 21, 22 -> {
            }
            case 26 -> {
                int bits = (int) argument(start, info);
                if (Float.isNaN(Float.intBitsToFloat(bits)) && bits != Float.floatToIntBits(Float.NaN)) {
                    throw defect(start, "a float32 NaN other than 7FC00000 (S10)");
                }
            }
            case 27 -> {
                long bits = argument(start, info);
                if (Double.isNaN(Double.longBitsToDouble(bits)) && bits != Double.doubleToLongBits(Double.NaN)) {
                    throw defect(start, "a float64 NaN other than 7FF8000000000000 (S10)");
                }
            }
            case 25 -> throw defect(start, "a half float (S10)");
            default -> throw defect(start, "a simple value other than false, true and null (S11)");
        }
    }

    /**
     * The element count of a definite array or map, refused when the input cannot hold it, since
     * every item takes at least one byte; {@code perElement} is 2 for a map's key and value.
     */
    private long count(int start, long argument, int perElement) throws IOException {
        if (argument < 0 || argument > (long) (in.length - pos) / perElement) {
            throw defect(start, "a container holds more items than the input");
        }
        return argument;
    }

    private int enter(int start, int depth) throws IOException {
        if (depth >= maxDepth) {
            throw defect(start, "nesting deeper than " + maxDepth);
        }
        return depth + 1;
    }

    /**
     * The argument of a head whose additional information is {@code info} (RFC 8949 section 3);
     * the strict mode refuses a head wider than its argument needs (S3).
     */
    private long argument(int start, int info) throws IOException {
        if (info < 24) {
            return info;
        }
        int width = switch (info) {
            case 24 -> 1;
            case 25 -> 2;
            case 26 -> 4;
            case 27 -> 8;
            default -> throw defect(start, "reserved additional information " + info);
        };
        long value = 0;
        for (int i = 0; i < width; i++) {
            value = (value << 8) | next();
        }
        boolean isFloat = (in[start] & 0xFF) >>> 5 == 7;
        if (strict && !isFloat) {
            long floor = switch (width) {
                case 1 -> 24;
                case 2 -> 0x100;
                case 4 -> 0x10000;
                default -> 0x100000000L;
            };
            if (Long.compareUnsigned(value, floor) < 0) {
                throw defect(start, "a head wider than its argument needs (S3)");
            }
        }
        return value;
    }

    /** The argument of the (already checked) head at {@code at}. */
    private long argumentAt(int at) {
        int info = in[at] & 0x1F;
        if (info < 24) {
            return info;
        }
        long value = 0;
        for (int i = 1; i < headLength(in[at] & 0xFF); i++) {
            value = (value << 8) | (in[at + i] & 0xFF);
        }
        return value;
    }

    /** The bytes a head takes, its initial byte included. */
    private static int headLength(int initial) {
        return switch (initial & 0x1F) {
            case 24 -> 2;
            case 25 -> 3;
            case 26 -> 5;
            case 27 -> 9;
            default -> 1;
        };
    }

    /** The UTF-16 length of the (already checked) UTF-8 bytes {@code from} to {@code to}. */
    private int utf16Length(int from, int to) {
        int units = 0;
        for (int i = from; i < to; i++) {
            int b = in[i] & 0xFF;
            if ((b & 0xC0) != 0x80) {
                units += b >= 0xF0 ? 2 : 1;
            }
        }
        return units;
    }

    /** Skips {@code length} content bytes. */
    private void skip(int start, long length) throws IOException {
        if (length < 0 || length > in.length - pos) {
            throw defect(start, "a string runs past the end of the input");
        }
        pos += (int) length;
    }

    /** Checks {@code length} content bytes as well-formed shortest-form UTF-8 (RFC 3629 section 4). */
    private void utf8(int start, long length) throws IOException {
        int from = pos;
        skip(start, length);
        int i = from;
        while (i < pos) {
            int b = in[i] & 0xFF;
            if (b < 0x80) {
                i++;
                continue;
            }
            int count;
            int low = 0x80;
            int high = 0xBF;
            if (b >= 0xC2 && b <= 0xDF) {
                count = 1;
            } else if (b == 0xE0) {
                count = 2;
                low = 0xA0;
            } else if (b == 0xED) {
                count = 2;
                high = 0x9F;
            } else if (b >= 0xE1 && b <= 0xEF) {
                count = 2;
            } else if (b == 0xF0) {
                count = 3;
                low = 0x90;
            } else if (b == 0xF4) {
                count = 3;
                high = 0x8F;
            } else if (b >= 0xF1 && b <= 0xF3) {
                count = 3;
            } else {
                throw defect(i, "invalid UTF-8 lead byte");
            }
            if (count > pos - i - 1) {
                throw defect(i, "truncated UTF-8 sequence");
            }
            int second = in[i + 1] & 0xFF;
            if (second < low || second > high) {
                throw defect(i, "overlong, surrogate, or out-of-range UTF-8 sequence");
            }
            for (int k = 2; k <= count; k++) {
                int c = in[i + k] & 0xFF;
                if (c < 0x80 || c > 0xBF) {
                    throw defect(i, "invalid UTF-8 continuation byte");
                }
            }
            i += count + 1;
        }
    }

    private int next() throws IOException {
        if (pos >= in.length) {
            throw defect(pos, "truncated item");
        }
        return in[pos++] & 0xFF;
    }

    private int peek(int start) throws IOException {
        if (pos >= in.length) {
            throw defect(start, "truncated item");
        }
        return in[pos] & 0xFF;
    }

    private static IOException defect(int offset, String rule) {
        return new IOException("CBOR item not accepted at offset " + offset + ": " + rule);
    }
}
