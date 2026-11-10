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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * HLC round five, 2026-11-08 (decision E8): outside stamp positions every implementation reads
 * CBOR alike. An indefinite-length text string decodes, since Jackson writes any string longer
 * than 3996 characters that way. A tag decodes only when an encoder writes it: Jackson writes
 * tags 2 and 3 for a {@link BigInteger} and tag 4 for a {@link BigDecimal}, and the codec refuses
 * every other tag. Invalid or overlong UTF-8 in any text string, a map key or a chunk included,
 * does not decode.
 */
class CborCodecWellFormedTest {

    private final CborCodec codec = CborCodec.defaultCodec();

    record Note(String title, Map<String, String> tags) {
    }

    record Amounts(BigInteger count, BigDecimal price, BigInteger debt) {
    }

    record Sized(long size) {
    }

    @Test
    void anIndefiniteLengthTextStringOutsideAStampDecodes() {
        String text = "x" + "😀".repeat(3000) + "é".repeat(1000);
        byte[] bytes = codec.toBytes(new Note(text, Map.of("k", "v")));
        assertThat(bytes).as("Jackson writes a long string as indefinite-length text")
                .containsSubsequence(new byte[]{(byte) 0x7F});
        assertThat(codec.fromBytes(bytes, Note.class).title()).isEqualTo(text);

        byte[] chunked = note(cat(new byte[]{0x7F, 0x63}, ascii("abc"), new byte[]{0x62}, ascii("de"),
                new byte[]{(byte) 0xFF}));
        assertThat(codec.fromBytes(chunked, Note.class).title()).isEqualTo("abcde");
    }

    @Test
    void theTagsJacksonWritesForBigNumbersDecode() {
        // HLC round six, 2026-11-08 (JC-1): a tag 2 bignum holds a value past a Java long
        Amounts amounts = new Amounts(BigInteger.ONE.shiftLeft(64), new BigDecimal("9.99"),
                new BigInteger("-18446744073709551617"));
        byte[] bytes = codec.toBytes(amounts);
        String hex = HexFormat.of().formatHex(bytes);
        assertThat(hex).as("tag 2 over a byte string").contains("c249010000000000000000");
        assertThat(hex).as("tag 4 over [exponent, mantissa]").contains("c482211903e7");
        assertThat(hex).as("tag 3 over a byte string").contains("c349");
        assertThat(codec.fromBytes(bytes, Amounts.class)).isEqualTo(amounts);
    }

    @Test
    void everyOtherTagIsRefused() {
        Map<String, byte[]> notes = new LinkedHashMap<>();
        notes.put("tag 0 over the title", note(cat(new byte[]{(byte) 0xC0}, text("t"))));
        notes.put("tag 32 over the title", note(cat(new byte[]{(byte) 0xD8, 0x20}, text("t"))));
        notes.put("tag 42 over the title", note(cat(new byte[]{(byte) 0xD8, 0x2A}, text("t"))));
        notes.put("tag 55799 over the whole item", cat(new byte[]{(byte) 0xD9, (byte) 0xD9, (byte) 0xF7},
                note(text("t"))));
        notes.put("tag 1 over a map key", cat(new byte[]{(byte) 0xBF, (byte) 0xC1}, text("title"),
                text("t"), text("tags"), new byte[]{(byte) 0xA0, (byte) 0xFF}));
        notes.put("tag 2 over a text string", note(cat(new byte[]{(byte) 0xC2}, text("t"))));
        notes.put("tag 4 over a map", cat(new byte[]{(byte) 0xBF}, text("title"), text("t"), text("tags"),
                new byte[]{(byte) 0xC4, (byte) 0xA0, (byte) 0xFF}));
        for (Map.Entry<String, byte[]> e : notes.entrySet()) {
            assertThatThrownBy(() -> codec.fromBytes(e.getValue(), Note.class)).as(e.getKey())
                    .isInstanceOf(UncheckedIOException.class);
        }
        Map<String, byte[]> amounts = new LinkedHashMap<>();
        amounts.put("tag 4 over a three-element array", cat(new byte[]{(byte) 0xBF}, text("price"),
                new byte[]{(byte) 0xC4, (byte) 0x83, 0x21, 0x01, 0x01, (byte) 0xFF}));
        amounts.put("tag 4 over a text exponent", cat(new byte[]{(byte) 0xBF}, text("price"),
                new byte[]{(byte) 0xC4, (byte) 0x82}, text("e"), new byte[]{0x01, (byte) 0xFF}));
        amounts.put("tag 4 over a tag 0 mantissa", cat(new byte[]{(byte) 0xBF}, text("price"),
                new byte[]{(byte) 0xC4, (byte) 0x82, 0x21, (byte) 0xC0, 0x01, (byte) 0xFF}));
        amounts.put("tag 2 over an integer", cat(new byte[]{(byte) 0xBF}, text("count"),
                new byte[]{(byte) 0xC2, 0x01, (byte) 0xFF}));
        for (Map.Entry<String, byte[]> e : amounts.entrySet()) {
            assertThatThrownBy(() -> codec.fromBytes(e.getValue(), Amounts.class)).as(e.getKey())
                    .isInstanceOf(UncheckedIOException.class);
        }
        assertThatThrownBy(() -> codec.fromBytes(cat(new byte[]{(byte) 0xBF}, text("size"),
                new byte[]{(byte) 0xC1, 0x05, (byte) 0xFF}), Sized.class))
                .as("tag 1 over an integer").isInstanceOf(UncheckedIOException.class);
        assertThat(codec.fromBytes(cat(new byte[]{(byte) 0xBF}, text("size"), new byte[]{0x05, (byte) 0xFF}),
                Sized.class).size()).as("the same item untagged").isEqualTo(5);
    }

    /**
     * HLC round five, 2026-11-08 (decision E8): no encoder writes a tagged map key, so a tag in
     * key position does not decode, the big-number tags included. Jackson would otherwise read a
     * tag 2 key's byte string as the key text.
     */
    @Test
    void aTagInMapKeyPositionIsRefusedEvenForTheBigNumberTags() {
        Map<String, String> keys = new LinkedHashMap<>();
        keys.put("tag 2 key in a definite map", "a1c241016178");
        keys.put("tag 4 key in a definite map", "a1c48200016178");
        keys.put("tag 3 key in a definite map", "a1c341006178");
        keys.put("tag 2 key in an indefinite map", "bfc241016178ff");
        keys.put("tag 4 key in an indefinite map", "bfc48200016178ff");
        for (Map.Entry<String, String> e : keys.entrySet()) {
            byte[] bytes = HexFormat.of().parseHex(e.getValue());
            assertThatThrownBy(() -> WellFormedCbor.check(bytes, 64)).as(e.getKey())
                    .isInstanceOf(java.io.IOException.class);
            assertThatThrownBy(() -> codec.fromBytes(bytes, Object.class)).as(e.getKey())
                    .isInstanceOf(UncheckedIOException.class);
        }
        assertThat(codec.fromBytes(HexFormat.of().parseHex("a16161c24101"), Object.class))
                .as("the same tag 2 item as a value decodes").isEqualTo(Map.of("a", BigInteger.ONE));
    }

    @Test
    void invalidOrOverlongUtf8IsRefusedInEveryTextString() {
        Map<String, byte[]> bad = new LinkedHashMap<>();
        bad.put("overlong C1 81 for 'A'", new byte[]{(byte) 0xC1, (byte) 0x81});
        bad.put("overlong C0 80 for NUL", new byte[]{(byte) 0xC0, (byte) 0x80});
        bad.put("overlong E0 81 81", new byte[]{(byte) 0xE0, (byte) 0x81, (byte) 0x81});
        bad.put("overlong F0 80 81 81", new byte[]{(byte) 0xF0, (byte) 0x80, (byte) 0x81, (byte) 0x81});
        bad.put("an encoded surrogate ED A0 80", new byte[]{(byte) 0xED, (byte) 0xA0, (byte) 0x80});
        bad.put("above U+10FFFF, F4 90 80 80", new byte[]{(byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80});
        bad.put("a lone continuation byte", new byte[]{(byte) 0x80});
        bad.put("a truncated sequence", new byte[]{(byte) 0xE2, (byte) 0x82});
        bad.put("byte FF", new byte[]{(byte) 0xFF});
        for (Map.Entry<String, byte[]> e : bad.entrySet()) {
            byte[] utf8 = cat(ascii("a"), e.getValue(), ascii("b"));
            byte[] item = cat(new byte[]{(byte) (0x60 | utf8.length)}, utf8);
            assertThatThrownBy(() -> codec.fromBytes(note(item), Note.class)).as("value: " + e.getKey())
                    .isInstanceOf(UncheckedIOException.class);
            assertThatThrownBy(() -> codec.fromBytes(cat(new byte[]{(byte) 0xBF}, text("title"), text("t"),
                    text("tags"), new byte[]{(byte) 0xBF}, item, text("v"), new byte[]{(byte) 0xFF, (byte) 0xFF}),
                    Note.class)).as("map key: " + e.getKey()).isInstanceOf(UncheckedIOException.class);
            assertThatThrownBy(() -> codec.fromBytes(note(cat(new byte[]{0x7F, 0x61}, ascii("z"), item,
                    new byte[]{(byte) 0xFF})), Note.class)).as("chunk: " + e.getKey())
                    .isInstanceOf(UncheckedIOException.class);
        }
        byte[] euro = "€".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> codec.fromBytes(note(cat(new byte[]{0x7F, 0x41}, new byte[]{euro[0]},
                new byte[]{0x62}, new byte[]{euro[1], euro[2]}, new byte[]{(byte) 0xFF})), Note.class))
                .as("a chunk boundary inside a code point (RFC 8949 section 3.2.3)")
                .isInstanceOf(UncheckedIOException.class);
        assertThat(codec.fromBytes(note(text("a€😀é")), Note.class).title())
                .as("valid UTF-8 of every length decodes").isEqualTo("a€😀é");
    }

    /**
     * Items that are not well-formed CBOR (RFC 8949 section 3) fail in the walk, whatever Jackson
     * would make of them.
     */
    @Test
    void anItemThatIsNotWellFormedIsRefused() {
        Map<String, byte[]> malformed = new LinkedHashMap<>();
        malformed.put("a break in place of the title", note(new byte[]{(byte) 0xFF}));
        malformed.put("a byte-string chunk in indefinite text", note(cat(new byte[]{0x7F, 0x41}, ascii("a"),
                new byte[]{(byte) 0xFF})));
        malformed.put("an indefinite chunk in indefinite text", note(cat(new byte[]{0x7F, 0x7F, 0x61}, ascii("a"),
                new byte[]{(byte) 0xFF, (byte) 0xFF})));
        malformed.put("reserved additional information 28", note(new byte[]{0x7C}));
        malformed.put("an indefinite-length integer", note(new byte[]{0x1F}));
        malformed.put("a two-byte simple value below 32", note(new byte[]{(byte) 0xF8, 0x10}));
        malformed.put("an array claiming 2^63 items", note(new byte[]{(byte) 0x9B, (byte) 0x80, 0, 0, 0, 0, 0, 0, 0}));
        malformed.put("a map claiming more pairs than the input", note(new byte[]{(byte) 0xB8, (byte) 0xF0}));
        malformed.put("a text running past the end", cat(new byte[]{(byte) 0xBF}, text("title"),
                new byte[]{0x78, 0x40}, ascii("short")));
        for (Map.Entry<String, byte[]> e : malformed.entrySet()) {
            assertThatThrownBy(() -> WellFormedCbor.check(e.getValue(), 64)).as(e.getKey())
                    .isInstanceOf(java.io.IOException.class);
            assertThatThrownBy(() -> codec.fromBytes(e.getValue(), Note.class)).as(e.getKey())
                    .isInstanceOf(UncheckedIOException.class);
        }
    }

    /** The item {@code {"title": <title>, "tags": {}}} with the given raw title item. */
    private static byte[] note(byte[] titleItem) {
        return cat(new byte[]{(byte) 0xBF}, text("title"), titleItem, text("tags"),
                new byte[]{(byte) 0xA0, (byte) 0xFF});
    }

    private static byte[] text(String s) {
        return CanonicalStamps.textItem(s);
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] cat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

    /**
     * The well-formedness walk refuses no depth Jackson accepts, and a hostile depth fails as
     * undecodable instead of overflowing the stack.
     */
    @Test
    void theCheckAcceptsTheDepthJacksonAcceptsAndNeverOverflowsTheStack() {
        assertThat(codec.fromBytes(nestedArrays(64), Object.class)).isNotNull();
        assertThatThrownBy(() -> codec.fromBytes(nestedArrays(65), Object.class))
                .isInstanceOf(UncheckedIOException.class);
        assertThatThrownBy(() -> codec.fromBytes(nestedArrays(1_000_000), Object.class))
                .isInstanceOf(UncheckedIOException.class);
    }

    /** {@code depth} one-element arrays around the integer 1. */
    private static byte[] nestedArrays(int depth) {
        byte[] bytes = new byte[depth + 1];
        java.util.Arrays.fill(bytes, (byte) 0x81);
        bytes[depth] = 0x01;
        return bytes;
    }
}
