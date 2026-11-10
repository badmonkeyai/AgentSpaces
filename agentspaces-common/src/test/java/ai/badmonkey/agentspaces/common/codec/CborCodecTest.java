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

import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CborCodecTest {

    record Sample(String name, int priority, Instant at, Duration ttl,
                  Map<String, String> tags, byte[] payload,
                  PeerId issuer, HlcTimestamp stamp) {
    }

    record SampleV2(String name, int priority, Instant at, Duration ttl,
                    Map<String, String> tags, byte[] payload,
                    PeerId issuer, HlcTimestamp stamp, String extraField) {
    }

    @Test
    void recordsRoundTripThroughCbor() {
        CborCodec codec = CborCodec.defaultCodec();
        Sample sample = new Sample("hello", 3, Instant.parse("2026-08-26T14:02:11Z"),
                Duration.ofMinutes(15), Map.of("kind", "demo"), new byte[]{1, 2, 3},
                PeerId.of("zAbC"), new HlcTimestamp(42L, 1, "n1"));

        Sample decoded = codec.fromBytes(codec.toBytes(sample), Sample.class);

        assertThat(decoded.name()).isEqualTo(sample.name());
        assertThat(decoded.priority()).isEqualTo(sample.priority());
        assertThat(decoded.at()).isEqualTo(sample.at());
        assertThat(decoded.ttl()).isEqualTo(sample.ttl());
        assertThat(decoded.tags()).isEqualTo(sample.tags());
        assertThat(decoded.payload()).isEqualTo(sample.payload());
        assertThat(decoded.issuer()).isEqualTo(sample.issuer());
        assertThat(decoded.stamp()).isEqualTo(sample.stamp());
    }

    record Stamped(HlcTimestamp stamp) {
    }

    record StampedAs(Object stamp) {
    }

    /**
     * HLC logical counter, 2026-11-08 (Rust team report): the codec decodes a stamp only from
     * its canonical string form and refuses a map, an array, a number, a byte string or a
     * non-canonical string in its place. Through
     * the record's constructor, Jackson truncated a fractional counter in a map.
     */
    @Test
    void aStampDecodesOnlyFromItsCanonicalString() {
        CborCodec codec = CborCodec.defaultCodec();
        assertThat(codec.fromBytes(codec.toBytes(new StampedAs("5:65536:n")), Stamped.class).stamp())
                .isEqualTo(new HlcTimestamp(5, 65_536, "n"));
        for (Object hostile : java.util.List.<Object>of(
                Map.of("physical", 5, "logical", 1.5, "node", "n"),
                Map.of("physical", 5, "logical", -1, "node", "n"),
                java.util.List.of(5, 1, "n"), 5, 1.5,
                // HLC canonical stamps, 2026-11-08: a CBOR byte string is no stamp either.
                "5:0:n".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                "007:0:n")) {
            byte[] bytes = codec.toBytes(new StampedAs(hostile));
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> codec.fromBytes(bytes, Stamped.class))
                    .as(String.valueOf(hostile)).isInstanceOf(java.io.UncheckedIOException.class);
        }
    }

    /**
     * HLC round four, 2026-11-08 (decision D3): a stamp on the wire is a definite-length,
     * untagged CBOR text string whose bytes are shortest-form UTF-8 under the shortest length
     * header. Jackson's CBOR parser decoded overlong UTF-8, unwrapped tags, and joined
     * indefinite-length chunks silently, so each of these items decoded to the canonical stamp
     * and a signature over the re-encoding verified. The codec now refuses every one of them.
     */
    @Test
    void aStampDecodesOnlyFromADefiniteUntaggedShortestFormTextString() {
        CborCodec codec = CborCodec.defaultCodec();
        byte[] honest = codec.toBytes(new Stamped(new HlcTimestamp(5, 0, "nA")));
        byte[] item = cat(new byte[]{0x66}, ascii("5:0:nA"));
        assertThat(codec.fromBytes(honest, Stamped.class).stamp()).isEqualTo(new HlcTimestamp(5, 0, "nA"));
        Map<String, byte[]> hostile = new java.util.LinkedHashMap<>();
        hostile.put("overlong C1 81 for 'A' in the node", cat(new byte[]{0x67}, ascii("5:0:n"),
                new byte[]{(byte) 0xC1, (byte) 0x81}));
        hostile.put("a UTF-8 encoded surrogate in the node", cat(new byte[]{0x68}, ascii("5:0:n"),
                new byte[]{(byte) 0xED, (byte) 0xA0, (byte) 0x80}));
        hostile.put("indefinite-length text, one chunk", cat(new byte[]{0x7F}, item, new byte[]{(byte) 0xFF}));
        hostile.put("indefinite-length text, two chunks", cat(new byte[]{0x7F, 0x63}, ascii("5:0"),
                new byte[]{0x63}, ascii(":nA"), new byte[]{(byte) 0xFF}));
        hostile.put("tag 0", cat(new byte[]{(byte) 0xC0}, item));
        hostile.put("tag 32", cat(new byte[]{(byte) 0xD8, 0x20}, item));
        hostile.put("a byte string", cat(new byte[]{0x46}, ascii("5:0:nA")));
        hostile.put("a non-shortest length header", cat(new byte[]{0x78, 0x06}, ascii("5:0:nA")));
        for (Map.Entry<String, byte[]> e : hostile.entrySet()) {
            byte[] bytes = splice(honest, item, e.getValue());
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> codec.fromBytes(bytes, Stamped.class))
                    .as(e.getKey()).isInstanceOf(java.io.UncheckedIOException.class);
        }
    }

    /** The same rule holds for a stamp inside a list and a map, where Jackson reads it from a nested token. */
    @Test
    void aNestedStampIsJudgedOnItsOwnBytes() {
        CborCodec codec = CborCodec.defaultCodec();
        record Nested(java.util.List<HlcTimestamp> stamps, Map<String, HlcTimestamp> byName) {
        }
        Nested value = new Nested(java.util.List.of(new HlcTimestamp(7, 1, "nA")),
                Map.of("k", new HlcTimestamp(7, 1, "nA")));
        byte[] honest = codec.toBytes(value);
        assertThat(codec.fromBytes(honest, Nested.class)).isEqualTo(value);
        byte[] item = cat(new byte[]{0x66}, ascii("7:1:nA"));
        byte[] tagged = splice(honest, item, cat(new byte[]{(byte) 0xC0}, item));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> codec.fromBytes(tagged, Nested.class))
                .isInstanceOf(java.io.UncheckedIOException.class);
    }

    private static byte[] ascii(String text) {
        return text.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    }

    private static byte[] cat(byte[]... parts) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

    /** Replaces the first occurrence of {@code from} in {@code bytes}, which must hold it. */
    private static byte[] splice(byte[] bytes, byte[] from, byte[] to) {
        for (int i = 0; i + from.length <= bytes.length; i++) {
            if (java.util.Arrays.equals(bytes, i, i + from.length, from, 0, from.length)) {
                return cat(java.util.Arrays.copyOfRange(bytes, 0, i), to,
                        java.util.Arrays.copyOfRange(bytes, i + from.length, bytes.length));
            }
        }
        throw new AssertionError("the stamp item was not found");
    }

    @Test
    void unknownFieldsAreIgnoredOnRead() {
        CborCodec codec = CborCodec.defaultCodec();
        SampleV2 newer = new SampleV2("hello", 3, Instant.EPOCH, Duration.ZERO,
                Map.of(), new byte[0], PeerId.of("zAbC"),
                new HlcTimestamp(1L, 0, "n1"), "from-the-future");

        Sample decoded = codec.fromBytes(codec.toBytes(newer), Sample.class);

        assertThat(decoded.name()).isEqualTo("hello");
    }

    @Test
    void serializationIsDeterministicForEqualValues() {
        CborCodec codec = CborCodec.defaultCodec();
        Sample a = new Sample("x", 1, Instant.EPOCH, Duration.ofSeconds(1),
                Map.of("k", "v"), new byte[]{9}, PeerId.of("zZz"), new HlcTimestamp(1L, 0, "n"));
        Sample b = new Sample("x", 1, Instant.EPOCH, Duration.ofSeconds(1),
                Map.of("k", "v"), new byte[]{9}, PeerId.of("zZz"), new HlcTimestamp(1L, 0, "n"));

        assertThat(codec.toBytes(a)).isEqualTo(codec.toBytes(b));
    }

    record WithList(String name, java.util.List<Integer> values) {
    }

    /** SPEC §9 / v0.1.8 canonical form: arrays are definite-length (RFC 8949 deterministic preference), maps stay indefinite-length. */
    @Test
    void listsEncodeAsDefiniteLengthArraysAndRecordsAsIndefiniteMaps() {
        CborCodec codec = CborCodec.defaultCodec();
        java.util.HexFormat hex = java.util.HexFormat.of();

        assertThat(hex.formatHex(codec.toBytes(java.util.List.of(1, 2, 3)))).isEqualTo("83010203");
        assertThat(hex.formatHex(codec.toBytes(java.util.List.of()))).isEqualTo("80");

        // A record is an indefinite-length map (0xBF ... 0xFF) with fields in
        // declared order; a list-valued field inside it is a definite array.
        byte[] record = codec.toBytes(new WithList("v", java.util.List.of(1, 2, 3)));
        assertThat(hex.formatHex(record)).isEqualTo("bf646e616d6561766676616c75657383010203ff");
    }

    @Test
    void jsonViewIsAvailableForLogs() {
        String json = CborCodec.defaultCodec().toJson(Map.of("k", "v"));

        assertThat(json).contains("\"k\"").contains("\"v\"");
    }
}
