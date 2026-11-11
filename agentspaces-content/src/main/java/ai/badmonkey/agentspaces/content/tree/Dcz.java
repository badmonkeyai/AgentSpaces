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

import ai.badmonkey.agentspaces.common.crypto.Digests;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * The {@code dcz} field of an intra-chunk delta in {@code NODES.deltas} (PDT 2 phase 4, P2-Z-01, P2-Z-04, P2-F-02;
 * srad/agentspaces-pdt2.md §3.8): one chunk written as the change from another chunk, its base.
 *
 * <p>The field is a leading format byte ({@link #ZSTD} {@code 'd'}; {@link #VCDIFF} {@code 'v'} is reserved for
 * P2-Z-05 and refused by name), then the 40-byte header RFC 9842 gives the {@code dcz} content encoding (the
 * skippable-frame magic {@code 5e 2a 4d 18 20 00 00 00} and the base's sha-256), then exactly one zstd frame
 * (RFC 8878) compressed with the base as a raw-content dictionary, as {@code zstd --patch-from} does. Java uses
 * {@code zstd-jni} (decision 12), whose frames over a known size carry the content size in a single segment, so the
 * window equals the output.
 *
 * <p>The reader trusts no size it has not checked (threat 5 of §6.7, P2-Q-SEC-05). Before it decompresses it reads
 * the zstd frame header and refuses, each by name, a frame without a content size, a content size or a window past
 * {@link #MAX_OUTPUT} (the chunk bound of PD-B-01, 256 KiB), a dictionary id, a reserved header bit, and trailing
 * bytes; it then decompresses into a buffer of exactly the declared size, so no frame writes past it. It also
 * refuses a base past the chunk bound and a base that starts with the zstd dictionary magic, which zstd would load
 * as a formatted dictionary; a writer makes no frame over such a base. The caller verifies the result against the
 * chunk's CID before it stores anything.
 */
public final class Dcz {

    /** The format byte of a zstd frame (P2-F-02). */
    public static final byte ZSTD = 'd';

    /** The format byte reserved for a VCDIFF frame (RFC 3284; P2-Z-05, P2-F-02), which this release does not read. */
    public static final byte VCDIFF = 'v';

    /** The most bytes a frame may rebuild and the widest window it may declare: the chunk bound of PD-B-01. */
    public static final int MAX_OUTPUT = 256 * 1024;

    /** The format byte and the RFC 9842 header before the zstd frame. */
    public static final int HEADER_BYTES = 1 + 8 + 32;

    /** The zstd level a writer compresses at; any level decodes the same way. */
    static final int LEVEL = 3;

    private static final byte[] DCZ_MAGIC = {0x5e, 0x2a, 0x4d, 0x18, 0x20, 0x00, 0x00, 0x00};
    private static final int ZSTD_MAGIC = 0xFD2FB528;
    private static final int DICTIONARY_MAGIC = 0xEC30A437;
    /** A zstd frame's magic, header descriptor, and at least one block header byte. */
    private static final int MIN_ZSTD_BYTES = 6;

    private Dcz() {
    }

    /**
     * Writes the field that rebuilds {@code target} from {@code base}.
     *
     * @param base   the chunk the receiver holds
     * @param target the chunk to send
     * @return the field, or empty when zstd cannot carry this pair: a chunk past {@link #MAX_OUTPUT}, a base that
     *         starts with the zstd dictionary magic, or a platform without the native zstd library
     */
    public static Optional<byte[]> encode(byte[] base, byte[] target) {
        Objects.requireNonNull(base, "base");
        Objects.requireNonNull(target, "target");
        if (base.length > MAX_OUTPUT || target.length > MAX_OUTPUT || dictionaryMagic(base)) {
            return Optional.empty();
        }
        byte[] zstd;
        try (ZstdCompressCtx ctx = new ZstdCompressCtx()) {
            ctx.setLevel(LEVEL).setChecksum(false).setContentSize(true).setDictID(false);
            ctx.loadDict(base);
            zstd = ctx.compress(target);
        } catch (RuntimeException | LinkageError e) {
            return Optional.empty(); // no frame: the chunk travels whole
        }
        byte[] field = new byte[HEADER_BYTES + zstd.length];
        field[0] = ZSTD;
        System.arraycopy(DCZ_MAGIC, 0, field, 1, DCZ_MAGIC.length);
        System.arraycopy(Digests.sha256(base), 0, field, 1 + DCZ_MAGIC.length, 32);
        System.arraycopy(zstd, 0, field, HEADER_BYTES, zstd.length);
        return Optional.of(field);
    }

    /**
     * Rebuilds the chunk a field writes over its base.
     *
     * @param base  the base chunk, verified against its CID by the caller
     * @param field the {@code dcz} field
     * @return the rebuilt bytes, which the caller verifies against the chunk's CID
     * @throws IllegalArgumentException naming the rule the field breaks
     */
    public static byte[] decode(byte[] base, byte[] field) {
        Objects.requireNonNull(base, "base");
        Objects.requireNonNull(field, "field");
        if (field.length < HEADER_BYTES + MIN_ZSTD_BYTES) {
            throw refused("a field of " + field.length + " bytes holds no format byte, RFC 9842 header, and zstd frame");
        }
        if (field[0] == VCDIFF) {
            throw refused("a VCDIFF frame (format byte 'v'), which this release does not read;"
                    + " send a zstd frame (format byte 'd') or the whole node");
        }
        if (field[0] != ZSTD) {
            throw refused(String.format("unknown format byte 0x%02x; this release reads 'd' (zstd)", field[0] & 0xff));
        }
        if (!Arrays.equals(field, 1, 1 + DCZ_MAGIC.length, DCZ_MAGIC, 0, DCZ_MAGIC.length)) {
            throw refused("no RFC 9842 dcz header after the format byte");
        }
        if (base.length > MAX_OUTPUT) {
            throw refused("a base of " + base.length + " bytes is past the chunk bound of " + MAX_OUTPUT + " bytes");
        }
        if (dictionaryMagic(base)) {
            throw refused("a base that starts with the zstd dictionary magic loads as no raw dictionary");
        }
        if (!Arrays.equals(field, 1 + DCZ_MAGIC.length, HEADER_BYTES, Digests.sha256(base), 0, 32)) {
            throw refused("the frame names another base than the one held");
        }
        int size = contentSize(field);
        byte[] out = new byte[size];
        try (ZstdDecompressCtx ctx = new ZstdDecompressCtx()) {
            if (Zstd.findFrameCompressedSize(field, HEADER_BYTES) != field.length - HEADER_BYTES) {
                throw refused("the field holds more than one zstd frame");
            }
            ctx.loadDict(base);
            int written = ctx.decompressByteArray(out, 0, out.length, field, HEADER_BYTES, field.length - HEADER_BYTES);
            if (written != size) {
                throw refused("the frame wrote " + written + " bytes where its header declared " + size);
            }
        } catch (RuntimeException e) {
            if (e instanceof IllegalArgumentException refusal && refusal.getMessage().startsWith("dcz:")) {
                throw refusal;
            }
            throw refused("the field is not one zstd frame that opens over the base: " + e.getMessage());
        } catch (LinkageError e) {
            throw refused("zstd is not available on this platform: " + e.getMessage());
        }
        return out;
    }

    /** The content size the zstd frame header declares, after the bound checks of the class comment. */
    private static int contentSize(byte[] field) {
        int at = HEADER_BYTES;
        int magic = (field[at] & 0xff) | (field[at + 1] & 0xff) << 8 | (field[at + 2] & 0xff) << 16 | (field[at + 3] & 0xff) << 24;
        if (magic != ZSTD_MAGIC) {
            throw refused("no zstd frame after the RFC 9842 header");
        }
        int descriptor = field[at + 4] & 0xff;
        boolean singleSegment = (descriptor & 0x20) != 0;
        if ((descriptor & 0x08) != 0) {
            throw refused("the zstd frame header sets its reserved bit");
        }
        if ((descriptor & 0x03) != 0) {
            throw refused("the zstd frame names a dictionary id; a base is a raw dictionary with none");
        }
        int p = at + 5;
        long window = -1;
        if (!singleSegment) {
            int w = field[p++] & 0xff;
            long windowBase = 1L << (10 + (w >>> 3));
            window = windowBase + (windowBase / 8) * (w & 0x07);
        }
        int sizeBytes = switch (descriptor >>> 6) {
            case 0 -> singleSegment ? 1 : 0;
            case 1 -> 2;
            case 2 -> 4;
            default -> 8;
        };
        if (sizeBytes == 0) {
            throw refused("the zstd frame declares no content size");
        }
        if (p + sizeBytes > field.length) {
            throw refused("the zstd frame header is cut short");
        }
        long size = 0;
        for (int i = 0; i < sizeBytes; i++) {
            size |= (field[p + i] & 0xffL) << (8 * i);
        }
        if (sizeBytes == 2) {
            size += 256;
        }
        if (size < 0 || size > MAX_OUTPUT) {
            throw refused("a content size of " + Long.toUnsignedString(size) + " bytes is past the chunk bound of "
                    + MAX_OUTPUT + " bytes");
        }
        if (singleSegment) {
            window = size;
        }
        if (window > MAX_OUTPUT) {
            throw refused("a window of " + window + " bytes is past the bound of " + MAX_OUTPUT + " bytes");
        }
        return (int) size;
    }

    private static boolean dictionaryMagic(byte[] base) {
        return base.length >= 4
                && ((base[0] & 0xff) | (base[1] & 0xff) << 8 | (base[2] & 0xff) << 16 | (base[3] & 0xff) << 24) == DICTIONARY_MAGIC;
    }

    private static IllegalArgumentException refused(String why) {
        return new IllegalArgumentException("dcz: " + why);
    }
}
