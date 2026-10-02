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
