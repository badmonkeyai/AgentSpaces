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
package ai.badmonkey.agentspaces.agent.pdt;

import ai.badmonkey.agentspaces.common.codec.CanonicalMaps;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * HLC round six, 2026-11-08 (owner rulings F1, F3 and R1): a head travels as an entry payload and
 * decodes as a protocol record, from its own wire encoding alone. Its attrs map text to text,
 * except the reserved {@code pdt.issued} and {@code pdt.issuer}, whose value may be any
 * application value; one that is null or no text reads as absent there, so the head falls back
 * to its entry's own stamp and issuer together. A null or non-text value under any other key
 * makes the head undecodable, and a byte string is never read as base64 text.
 */
class PRefWireTest {

    private static final Path VECTORS = Path.of("..", "tools", "wire-encoding", "vectors.json");
    private static final String ROOT = "bafyreigq3u2qzvyq5r6dpq2nqq4cujdxa6wmljnrrjz4ns2nk2o3clj5vy";

    private final CborCodec codec = CborCodec.defaultCodec();

    @TestFactory
    Stream<DynamicTest> everyPRefVectorDecodesAsItsVerdictSays() throws Exception {
        assertThat(VECTORS).as("the wire-encoding vectors ship with the repository").exists();
        JsonNode vectors = new ObjectMapper().readTree(Files.readAllBytes(VECTORS)).get("vectors");
        return StreamSupport.stream(vectors.spliterator(), false)
                .filter(v -> v.get("carrier").asText().equals("pref"))
                .map(v -> DynamicTest.dynamicTest(
                        v.get("id").asText() + " " + v.get("verdict").asText() + ": " + v.get("why").asText(), () -> {
                            byte[] bytes = HexFormat.of().parseHex(v.get("hex").asText());
                            if (v.get("verdict").asText().equals("accept")) {
                                PRef head = codec.fromDocument(bytes, PRef.class);
                                assertThat(codec.toBytes(head)).as("re-encodes to its own bytes").isEqualTo(bytes);
                            } else {
                                assertThatThrownBy(() -> codec.fromDocument(bytes, PRef.class))
                                        .isInstanceOf(UncheckedIOException.class);
                            }
                        }));
    }

    /** A head whose attrs are written as given, in the RFC 8949 key order. */
    private byte[] head(Map<String, ?> attrs) {
        CanonicalMaps.Verbatim fields = new CanonicalMaps.Verbatim();
        fields.put("v", 1);
        fields.put("name", "facts");
        fields.put("kind", PRef.KIND_MAP);
        fields.put("root", ROOT);
        fields.put("base", List.of());
        fields.put("seq", 1);
        fields.put("size", 0);
        fields.put("mode", PRef.MODE_MULTI);
        fields.put("sealing", PRef.SEALING_NONE);
        CanonicalMaps.Verbatim sorted = new CanonicalMaps.Verbatim(CanonicalMaps.sorted(attrs));
        fields.put("attrs", sorted);
        return codec.toBytes(fields);
    }

    @Test
    void aReservedAttrThatIsNoTextReadsAsAbsentAndTheHeadDecodes() {
        Object[] values = {null, 5, true, 1.5, new byte[]{(byte) 0xcc, 0x4b, (byte) 0xe2, (byte) 0xfd, (byte) 0xaa, 0x69},
                List.of("zEvil/app"), new CanonicalMaps.Verbatim(Map.of("zEvil", "app"))};
        for (Object value : values) {
            java.util.Map<String, Object> attrs = new java.util.HashMap<>();
            attrs.put(PRef.ISSUED_ATTR, "99999999999999:0:zEvil");
            attrs.put(PRef.ISSUER_ATTR, value);
            attrs.put("origin", "test");
            byte[] bytes = head(attrs);
            PRef head = codec.fromDocument(bytes, PRef.class);
            assertThat(head.attrs()).as("%s", value).containsKey(PRef.ISSUER_ATTR);
            assertThat(head.issuerAttr()).as("a %s pdt.issuer reads as absent, never as base64 text", value).isNull();
            assertThat(head.issuedAttr()).isEqualTo("99999999999999:0:zEvil");
            assertThat(head.violation()).isEmpty();
            assertThat(codec.toBytes(head)).as("%s re-encodes unchanged", value).isEqualTo(bytes);
        }
    }

    @Test
    void aNullOrNonTextValueUnderAnyOtherKeyMakesTheHeadUndecodable() {
        Object[] values = {null, 5, true, 1.5, new byte[]{1}, List.of("x"), new CanonicalMaps.Verbatim(Map.of("a", "b"))};
        for (Object value : values) {
            java.util.Map<String, Object> attrs = new java.util.HashMap<>();
            attrs.put("origin", value);
            byte[] bytes = head(attrs);
            assertThatThrownBy(() -> codec.fromDocument(bytes, PRef.class)).as("%s", value)
                    .isInstanceOf(UncheckedIOException.class);
        }
    }

    @Test
    void theAttrsBoundCountsKeysAndTextValues() {
        // R1: the UTF-8 bytes of every key and of every text value; a reserved value that is no
        // text counts its key alone
        int fits = PRef.MAX_ATTRS_BYTES - "a".length() - PRef.ISSUER_ATTR.length();
        Map<String, Object> atBound = new java.util.HashMap<>();
        atBound.put("a", "x".repeat(fits));
        atBound.put(PRef.ISSUER_ATTR, List.of("y".repeat(100)));
        assertThat(codec.fromDocument(head(atBound), PRef.class).violation()).isEmpty();
        Map<String, Object> over = new java.util.HashMap<>(atBound);
        over.put("a", "x".repeat(fits + 1));
        assertThat(codec.fromDocument(head(over), PRef.class).violation()).hasValueSatisfying(
                v -> assertThat(v).contains("attrs larger"));
    }
}
