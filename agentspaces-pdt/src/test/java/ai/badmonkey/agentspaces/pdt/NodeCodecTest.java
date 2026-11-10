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
package ai.badmonkey.agentspaces.pdt;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.tree.Cids;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link NodeCodec}: the DAG-CBOR form of §3.1 at the byte level (PD-N-01, PD-N-03),
 * embedded splice and recovery, and each refusal of PD-N-04 named by its rule.
 */
class NodeCodecTest {

    private static final CborCodec CBOR = CborCodec.defaultCodec();

    private static Embedded embed(Object value) {
        return new Embedded(CBOR.toBytes(value));
    }

    private static Link linkTo(String text) {
        return new Link(Cids.of(text.getBytes(StandardCharsets.UTF_8), Cids.DAG_CBOR));
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }

    @Test
    void tinySpillNodeMatchesHandAssembledDagCbor() {
        Map<String, Object> node = new HashMap<>();
        node.put("v", new Embedded(bytes(0x01)));
        node.put("k", "x");
        byte[] encoded = NodeCodec.encode(node);
        // map(2) "k" "x" "v" 1
        assertThat(encoded).isEqualTo(bytes(0xa2, 0x61, 0x6b, 0x61, 0x78, 0x61, 0x76, 0x01));
        assertThat(NodeCodec.decode(encoded)).containsExactly(
                Map.entry("k", "x"), Map.entry("v", new Embedded(bytes(0x01))));
    }

    @Test
    void linkIsTag42OverZeroAndTheBinaryCid() {
        Link link = linkTo("child");
        Map<String, Object> node = Map.of("k", "v", "c", List.of(link));
        byte[] encoded = NodeCodec.encode(node);
        byte[] expected = concat(
                bytes(0xa2),
                bytes(0x61, 0x63),                       // "c"
                bytes(0x81, 0xd8, 0x2a, 0x58, 0x25, 0x00), // [ tag 42 ( bytes(37) 0x00 ...
                Cids.binary(link.cid()),
                bytes(0x61, 0x6b, 0x61, 0x76));          // "k": "v"
        assertThat(encoded).isEqualTo(expected);
        assertThat(NodeCodec.links(encoded)).containsExactly(link.cid());
        assertThat(NodeCodec.kindOf(encoded)).isEqualTo("v");
        assertThat(NodeCodec.decode(encoded).get("c")).isEqualTo(List.of(link));
    }

    @Test
    void keysSortByLengthThenBytes() {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("cnt", 2L);
        node.put("n", 0);
        node.put("k", "m");
        node.put("e", List.of(embed("a"), embed(1), embed("b"), embed(2)));
        node.put("d", 5);
        node.put("c", List.of());
        byte[] encoded = NodeCodec.encode(node);
        assertThat(encoded[0]).isEqualTo((byte) 0xa6);
        assertThat(new ArrayList<>(NodeCodec.decode(encoded).keySet())).containsExactly("c", "d", "e", "k", "n", "cnt");
        Map<String, Object> decoded = NodeCodec.decode(encoded);
        assertThat(decoded.get("d")).isEqualTo(5L);
        assertThat(decoded.get("cnt")).isEqualTo(2L);
        assertThat(decoded.get("c")).isEqualTo(List.of());
        assertThat(NodeCodec.compareKeys("k", "cnt")).isNegative();
        assertThat(NodeCodec.compareKeys("n", "k")).isPositive();
    }

    @Test
    void encodingIsIndependentOfInputMapOrder() {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("n", 1);
        a.put("k", "vr");
        a.put("s", 5);
        a.put("r", null);
        a.put("t", List.of(embed("x")));
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("t", List.of(embed("x")));
        b.put("r", null);
        b.put("s", 5L);
        b.put("k", "vr");
        b.put("n", 1L);
        assertThat(NodeCodec.encode(a)).isEqualTo(NodeCodec.encode(b));
        Map<String, Object> decoded = NodeCodec.decode(NodeCodec.encode(a));
        assertThat(decoded).containsEntry("r", null);
        assertThat(NodeCodec.links(NodeCodec.encode(a))).isEmpty();
    }

    @Test
    void embeddedItemsSpliceInAndRecoverThroughCborCodec() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("name", "pdt");
        value.put("tags", List.of("a", "b", "c"));
        value.put("ratio", 1.5d);
        value.put("count", 123456789L);
        Map<String, Object> node = Map.of("k", "m", "d", 1, "n", 0,
                "e", List.of(embed("key"), embed(value)), "c", List.of(), "cnt", 1);
        byte[] encoded = NodeCodec.encode(node);
        byte[] rawValue = CBOR.toBytes(value);
        assertThat(indexOf(encoded, rawValue)).isGreaterThan(0);
        List<?> entries = (List<?>) NodeCodec.decode(encoded).get("e");
        assertThat(entries).hasSize(2);
        assertThat(((Embedded) entries.get(0)).bytes()).isEqualTo(CBOR.toBytes("key"));
        assertThat(((Embedded) entries.get(1)).bytes()).isEqualTo(rawValue);
        @SuppressWarnings("unchecked")
        Map<String, Object> recovered = CBOR.fromBytes(((Embedded) entries.get(1)).bytes(), Map.class);
        assertThat(recovered).containsEntry("name", "pdt").containsEntry("ratio", 1.5d);
    }

    /** The skipper walks every CBOR form, indefinite ones, half floats, and any tag included; the codec decides what a node may hold. */
    @Test
    void skipperHandlesIndefiniteFormsFloatsAndTags() {
        byte[] indefiniteMap = bytes(0xbf, 0x61, 0x61, 0x01, 0x61, 0x62, 0x9f, 0x01, 0x02, 0xff, 0xff);
        byte[] indefiniteText = bytes(0x7f, 0x62, 0x61, 0x62, 0x61, 0x63, 0xff);
        byte[] halfFloat = bytes(0xf9, 0x3c, 0x00);
        byte[] tagged = bytes(0xc1, 0x1a, 0x51, 0x4b, 0x67, 0xb0);
        byte[] bigUint = bytes(0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff);
        for (byte[] item : List.of(indefiniteMap, indefiniteText, halfFloat, tagged, bigUint)) {
            assertThat(NodeCodec.skip(concat(item, bytes(0x00)), 0)).isEqualTo(item.length);
        }
        // a tag is no nesting level (rule S2), and a long chain of tags costs no stack
        byte[] chain = new byte[100_001];
        Arrays.fill(chain, (byte) 0xc1);
        chain[chain.length - 1] = 0x01;
        assertThat(NodeCodec.skip(chain, 0)).isEqualTo(chain.length);
    }

    /**
     * HLC round six, 2026-11-08 (owner ruling F2, change JD-4): every embedded item is a document
     * in the wire encoding, tags 2, 3 and 4 included, so each document vector embeds in a node,
     * encoded and decoded, exactly when the wire encoding accepts it. A node therefore never
     * carries an item that no other implementation reads to the same value.
     */
    @TestFactory
    Stream<DynamicTest> everyDocumentVectorEmbedsInANodeExactlyWhenTheWireEncodingAcceptsIt() throws Exception {
        Path vectors = Path.of("..", "tools", "wire-encoding", "vectors.json");
        assertThat(vectors).as("the wire-encoding vectors ship with the repository").exists();
        JsonNode all = new ObjectMapper().readTree(Files.readAllBytes(vectors)).get("vectors");
        return StreamSupport.stream(all.spliterator(), false)
                .filter(v -> v.get("carrier").asText().equals("document"))
                .map(v -> DynamicTest.dynamicTest(v.get("id").asText() + " " + v.get("verdict").asText() + ": "
                        + v.get("why").asText(), () -> {
                    byte[] item = HexFormat.of().parseHex(v.get("hex").asText());
                    // the hand-assembled node {k: "x", v: item}, as a peer that writes any item would send it
                    byte[] raw = concat(bytes(0xa2, 0x61, 0x6b, 0x61, 0x78, 0x61, 0x76), item);
                    if (v.get("verdict").asText().equals("accept")) {
                        assertThat(NodeCodec.encode(Map.of("k", "x", "v", new Embedded(item)))).isEqualTo(raw);
                        assertThat(NodeCodec.decode(raw).get("v")).isEqualTo(new Embedded(item));
                    } else {
                        assertThatThrownBy(() -> NodeCodec.encode(Map.of("k", "x", "v", new Embedded(item))))
                                .isInstanceOf(NodeFormatException.class);
                        assertThatThrownBy(() -> NodeCodec.decode(raw)).isInstanceOf(NodeFormatException.class);
                    }
                }));
    }

    /**
     * HLC round six, 2026-11-08 (change JD-4, wire encoding 2.4): the item rules judge an embedded
     * item by its own bytes in every slot, so the node's own map and entry list add no nesting
     * level. Each document vector embeds as a map node's key, as its value, and as a vector
     * node's element exactly when the wire encoding accepts it, and the codec reads every node it
     * writes. The decoder once counted the entry list as a level and refused the 64-level vectors
     * {@code doc-44} and {@code doc-53} in a list slot that the encoder had written.
     */
    @TestFactory
    Stream<DynamicTest> everyDocumentVectorEmbedsInEveryListSlotExactlyWhenTheWireEncodingAcceptsIt() throws Exception {
        Path vectors = Path.of("..", "tools", "wire-encoding", "vectors.json");
        JsonNode all = new ObjectMapper().readTree(Files.readAllBytes(vectors)).get("vectors");
        byte[] text = CBOR.toBytes("kk");
        return StreamSupport.stream(all.spliterator(), false)
                .filter(v -> v.get("carrier").asText().equals("document"))
                .map(v -> DynamicTest.dynamicTest(v.get("id").asText() + " " + v.get("verdict").asText() + ": "
                        + v.get("why").asText(), () -> {
                    byte[] item = HexFormat.of().parseHex(v.get("hex").asText());
                    // the hand-assembled nodes {e: [kk, item], k: "mc"}, {e: [item, kk], k: "mc"} and {c: [], e: [item], k: "v"}
                    Map<String, byte[]> raws = new LinkedHashMap<>();
                    raws.put("as a map value", concat(bytes(0xa2, 0x61, 0x65, 0x82), text, item, bytes(0x61, 0x6b, 0x62, 0x6d, 0x63)));
                    raws.put("as a map key", concat(bytes(0xa2, 0x61, 0x65, 0x82), item, text, bytes(0x61, 0x6b, 0x62, 0x6d, 0x63)));
                    raws.put("as a vector element", concat(bytes(0xa3, 0x61, 0x63, 0x80, 0x61, 0x65, 0x81), item, bytes(0x61, 0x6b, 0x61, 0x76)));
                    Map<String, Map<String, Object>> nodes = Map.of(
                            "as a map value", Map.of("k", "mc", "e", List.of(new Embedded(text), new Embedded(item))),
                            "as a map key", Map.of("k", "mc", "e", List.of(new Embedded(item), new Embedded(text))),
                            "as a vector element", Map.of("k", "v", "c", List.of(), "e", List.of(new Embedded(item))));
                    for (Map.Entry<String, byte[]> raw : raws.entrySet()) {
                        Map<String, Object> node = nodes.get(raw.getKey());
                        if (v.get("verdict").asText().equals("accept")) {
                            assertThat(NodeCodec.encode(node)).as(raw.getKey()).isEqualTo(raw.getValue());
                            assertThat(NodeCodec.decode(raw.getValue())).as(raw.getKey()).isEqualTo(node);
                            assertThat(NodeCodec.links(raw.getValue())).as(raw.getKey()).isEmpty();
                        } else {
                            assertThatThrownBy(() -> NodeCodec.encode(node)).as(raw.getKey())
                                    .isInstanceOf(NodeFormatException.class);
                            assertThatThrownBy(() -> NodeCodec.decode(raw.getValue())).as(raw.getKey())
                                    .isInstanceOf(NodeFormatException.class);
                            assertThatThrownBy(() -> NodeCodec.links(raw.getValue())).as(raw.getKey())
                                    .isInstanceOf(NodeFormatException.class);
                        }
                    }
                }));
    }

    /** HLC round six, 2026-11-08 (owner ruling F2): Java's big numbers embed and recover as the values they are. */
    @Test
    void bigNumbersEmbedUnderTheirTagsAndRecoverThroughCborCodec() {
        java.math.BigInteger big = java.math.BigInteger.TWO.pow(70).negate();
        java.math.BigDecimal decimal = new java.math.BigDecimal("-12.5");
        List<Object> items = List.of(embed(big), embed(decimal));
        assertThat(items.get(0)).extracting(e -> ((Embedded) e).bytes()[0]).isEqualTo((byte) 0xc3);
        assertThat(items.get(1)).extracting(e -> ((Embedded) e).bytes()[0]).isEqualTo((byte) 0xc4);
        List<?> decoded = (List<?>) NodeCodec.decode(NodeCodec.encode(Map.of("k", "v", "e", items))).get("e");
        assertThat(decoded).isEqualTo(items);
        assertThat(CBOR.fromBytes(((Embedded) decoded.get(0)).bytes(), java.math.BigInteger.class)).isEqualTo(big);
        assertThat(CBOR.fromBytes(((Embedded) decoded.get(1)).bytes(), java.math.BigDecimal.class)).isEqualTo(decimal);
    }

    @Test
    void refusesAnEmbeddedItemWithTrailingBytes() {
        assertThatThrownBy(() -> NodeCodec.encode(Map.of("k", "x", "v", new Embedded(bytes(0x01, 0x02)))))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("trailing bytes");
    }

    @Test
    void refusesANodeOverOneMebibyte() {
        byte[] big = new byte[NodeCodec.MAX_NODE_BYTES];
        new Random(42).nextBytes(big);
        byte[] item = concat(bytes(0x5a, 0x00, 0x10, 0x00, 0x00), big);
        assertThatThrownBy(() -> NodeCodec.encode(Map.of("k", "x", "v", new Embedded(item))))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("exceeds 1048576 bytes (1 MiB)");
        byte[] raw = concat(bytes(0xa2, 0x61, 0x6b, 0x61, 0x78, 0x61, 0x76), item);
        assertThatThrownBy(() -> NodeCodec.decode("bafyexample", raw))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("exceeds 1048576 bytes (1 MiB)")
                .hasMessageContaining("bafyexample");
        // One byte under the bound is fine.
        byte[] fits = Arrays.copyOf(big, NodeCodec.MAX_NODE_BYTES - 7 - 5);
        byte[] fitsItem = concat(bytes(0x5a), bytes(fits.length >>> 24, fits.length >>> 16, fits.length >>> 8, fits.length), fits);
        assertThat(NodeCodec.encode(Map.of("k", "x", "v", new Embedded(fitsItem)))).hasSize(NodeCodec.MAX_NODE_BYTES);
    }

    @Test
    void refusesMoreThan1024Links() {
        List<Object> links = new ArrayList<>();
        for (int i = 0; i <= NodeCodec.MAX_LINKS; i++) {
            links.add(linkTo("child-" + i));
        }
        assertThatThrownBy(() -> NodeCodec.encode(Map.of("k", "v", "c", links)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("more than 1024 links");
        byte[] exactly = NodeCodec.encode(Map.of("k", "v", "c", links.subList(0, NodeCodec.MAX_LINKS)));
        assertThat(NodeCodec.links(exactly)).hasSize(NodeCodec.MAX_LINKS);
        // Hand-assembled: 1025 links in the array.
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        raw.writeBytes(bytes(0xa2, 0x61, 0x63, 0x99, 0x04, 0x01));
        for (Object link : links) {
            raw.writeBytes(bytes(0xd8, 0x2a, 0x58, 0x25, 0x00));
            raw.writeBytes(((Link) link).binary());
        }
        raw.writeBytes(bytes(0x61, 0x6b, 0x61, 0x76));
        assertThatThrownBy(() -> NodeCodec.decode(raw.toByteArray()))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("more than 1024 links");
        assertThatThrownBy(() -> NodeCodec.links(raw.toByteArray()))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("more than 1024 links");
    }

    @Test
    void refusesNestingDeeperThan64Levels() {
        byte[] deep = new byte[65];
        Arrays.fill(deep, (byte) 0x81);
        deep[64] = 0x01;
        // 64 arrays around an integer: allowed.
        byte[] ok = concat(deep);
        ok = Arrays.copyOfRange(ok, 0, 65);
        assertThat(NodeCodec.decode(NodeCodec.encode(Map.of("k", "x", "v", new Embedded(ok)))).get("v"))
                .isEqualTo(new Embedded(ok));
        // 65 arrays: refused.
        byte[] tooDeep = concat(bytes(0x81), ok);
        assertThatThrownBy(() -> NodeCodec.encode(Map.of("k", "x", "v", new Embedded(tooDeep))))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("nesting deeper than 64 levels");
        byte[] raw = concat(bytes(0xa2, 0x61, 0x6b, 0x61, 0x78, 0x61, 0x76), tooDeep);
        assertThatThrownBy(() -> NodeCodec.decode(raw))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("nesting deeper than 64 levels");
    }

    @Test
    void refusesAnUnknownKindByName() {
        assertThatThrownBy(() -> NodeCodec.encode(Map.of("k", "q")))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("unknown kind 'q'");
        byte[] raw = bytes(0xa1, 0x61, 0x6b, 0x61, 0x71);
        assertThatThrownBy(() -> NodeCodec.decode(raw))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("unknown kind 'q'");
        assertThat(NodeCodec.kindOf(raw)).isEqualTo("q");
    }

    @Test
    void refusesAReservedKindByName() {
        // PDT 2 phase 0 (P2-F-04): s stays reserved for PSortedMap, tm and ec join it; rr has a schema now
        assertThat(Kind.reserved("s")).isTrue();
        assertThat(Kind.reserved("tm")).isTrue();
        assertThat(Kind.reserved("ec")).isTrue();
        assertThat(Kind.reserved("rr")).isFalse();
        assertThat(Kind.known("s")).isFalse();
        assertThat(Kind.known("m")).isTrue();
        assertThatThrownBy(() -> NodeCodec.decode(bytes(0xa1, 0x61, 0x6b, 0x62, 0x74, 0x6d)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("kind 'tm' is reserved");
    }

    @Test
    void refusesAKeyOutsideTheSchema() {
        assertThatThrownBy(() -> NodeCodec.encode(Map.of("k", "x", "v", embed(1), "z", 1)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("key 'z' outside the schema of kind 'x'");
        // {"k":"x","q":1,"v":1} hand-assembled.
        byte[] raw = bytes(0xa3, 0x61, 0x6b, 0x61, 0x78, 0x61, 0x71, 0x01, 0x61, 0x76, 0x01);
        assertThatThrownBy(() -> NodeCodec.decode(raw))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("key 'q' outside the schema of kind 'x'");
    }

    @Test
    void refusesALinkWithAnUnknownCodecOrMultihash() {
        byte[] badCodec = new byte[36];
        badCodec[0] = 0x01;
        badCodec[1] = 0x70;
        badCodec[2] = 0x12;
        badCodec[3] = 0x20;
        byte[] raw = concat(bytes(0xa2, 0x61, 0x63, 0x81, 0xd8, 0x2a, 0x58, 0x25, 0x00), badCodec, bytes(0x61, 0x6b, 0x61, 0x76));
        assertThatThrownBy(() -> NodeCodec.decode(raw))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("unknown codec 0x70");
        byte[] badHash = badCodec.clone();
        badHash[1] = 0x71;
        badHash[2] = 0x13;
        byte[] raw2 = concat(bytes(0xa2, 0x61, 0x63, 0x81, 0xd8, 0x2a, 0x58, 0x25, 0x00), badHash, bytes(0x61, 0x6b, 0x61, 0x76));
        assertThatThrownBy(() -> NodeCodec.decode(raw2))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("unknown multihash 0x13");
        // A different tag, and a link without the identity byte.
        byte[] raw3 = concat(bytes(0xa2, 0x61, 0x63, 0x81, 0xd8, 0x2b, 0x58, 0x25, 0x00), Cids.binary(linkTo("x").cid()), bytes(0x61, 0x6b, 0x61, 0x76));
        assertThatThrownBy(() -> NodeCodec.decode(raw3))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("tag 43");
        byte[] raw4 = concat(bytes(0xa2, 0x61, 0x63, 0x81, 0xd8, 0x2a, 0x58, 0x25, 0x01), Cids.binary(linkTo("x").cid()), bytes(0x61, 0x6b, 0x61, 0x76));
        assertThatThrownBy(() -> NodeCodec.decode(raw4))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("0x00 identity byte");
    }

    @Test
    void refusesNonCanonicalBytes() {
        // Keys out of order: "v" before "k".
        assertThatThrownBy(() -> NodeCodec.decode(bytes(0xa2, 0x61, 0x76, 0x01, 0x61, 0x6b, 0x61, 0x78)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("out of canonical order");
        // Duplicate key.
        assertThatThrownBy(() -> NodeCodec.decode(bytes(0xa2, 0x61, 0x6b, 0x61, 0x78, 0x61, 0x6b, 0x61, 0x78)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("duplicate key 'k'");
        // Indefinite map.
        assertThatThrownBy(() -> NodeCodec.decode(bytes(0xbf, 0x61, 0x6b, 0x61, 0x78, 0xff)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("indefinite length");
        // Non-shortest integer: {"d": 24 as 0x18 0x05, "k": "m"}.
        assertThatThrownBy(() -> NodeCodec.decode(bytes(0xa2, 0x61, 0x64, 0x18, 0x05, 0x61, 0x6b, 0x61, 0x6d)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("shortest form");
        // Trailing bytes.
        assertThatThrownBy(() -> NodeCodec.decode(bytes(0xa1, 0x61, 0x6b, 0x61, 0x78, 0x00)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("trailing bytes");
        // Truncated.
        assertThatThrownBy(() -> NodeCodec.decode(bytes(0xa2, 0x61, 0x6b, 0x61, 0x78, 0x61)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("truncated");
        // No kind.
        assertThatThrownBy(() -> NodeCodec.decode(bytes(0xa0)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("no 'k' field");
        // A float in a structural field.
        assertThatThrownBy(() -> NodeCodec.decode(bytes(0xa2, 0x61, 0x64, 0xf9, 0x3c, 0x00, 0x61, 0x6b, 0x61, 0x6d)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("major type 0");
    }

    @Test
    void refusesWrongValueTypesOnEncode() {
        assertThatThrownBy(() -> NodeCodec.encode(Map.of("k", "m", "d", "five")))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("'d' must be an unsigned integer");
        assertThatThrownBy(() -> NodeCodec.encode(Map.of("k", "m", "d", -1)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("'d' must be an unsigned integer");
        assertThatThrownBy(() -> NodeCodec.encode(Map.of("k", "v", "c", List.of("notalink"))))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("'c' must hold a link");
        assertThatThrownBy(() -> NodeCodec.encode(Map.of("k", "x", "v", "text")))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("'v' must hold an embedded CBOR item");
        assertThatThrownBy(() -> NodeCodec.encode(Map.of("v", embed(1))))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("no 'k' field");
    }

    @Test
    void blobInteriorNodeRoundTripsWithSizes() {
        Link a = new Link(Cids.of(new byte[] {1}, Cids.RAW));
        Link b = new Link(Cids.of(new byte[] {2, 3}, Cids.RAW));
        Map<String, Object> node = Map.of("k", "b", "h", 1, "c", List.of(a, b), "z", List.of(1, 2L));
        byte[] encoded = NodeCodec.encode(node);
        Map<String, Object> decoded = NodeCodec.decode(encoded);
        assertThat(decoded.get("h")).isEqualTo(1L);
        assertThat(decoded.get("z")).isEqualTo(List.of(1L, 2L));
        assertThat(NodeCodec.links(encoded)).containsExactly(a.cid(), b.cid());
        assertThat(a.codec()).isEqualTo(Cids.RAW);
        assertThat(a.structured()).isFalse();
    }

    @Test
    void largeIntegersUseTheShortestForm() {
        byte[] encoded = NodeCodec.encode(Map.of("k", "vr", "n", 0x1_0000_0000L, "s", 300, "t", List.of()));
        // "k" "vr" | "n" 1a? no: 2^32 needs 8 bytes (0x1b) | "s" 300 needs 2 bytes (0x19) | "t" []
        byte[] expected = concat(
                bytes(0xa4, 0x61, 0x6b, 0x62, 0x76, 0x72),
                bytes(0x61, 0x6e, 0x1b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00),
                bytes(0x61, 0x73, 0x19, 0x01, 0x2c),
                bytes(0x61, 0x74, 0x80));
        assertThat(encoded).isEqualTo(expected);
        Map<String, Object> decoded = NodeCodec.decode(encoded);
        assertThat(decoded.get("n")).isEqualTo(0x1_0000_0000L);
        assertThat(decoded.get("s")).isEqualTo(300L);
    }

    @Test
    void linksFastPathReturnsEmptyForARawChunk() {
        byte[] chunk = bytes(0xa2, 0x61, 0x63, 0x81);
        String cid = Cids.of(chunk, Cids.RAW);
        assertThat(NodeCodec.links(cid, chunk)).isEmpty();
        assertThatThrownBy(() -> NodeCodec.links(chunk)).isInstanceOf(NodeFormatException.class);
    }

    @Test
    void decodeWithCidNamesTheCidInARefusal() {
        String cid = Cids.of(new byte[] {0}, Cids.DAG_CBOR);
        assertThatThrownBy(() -> NodeCodec.decode(cid, bytes(0xa1, 0x61, 0x6b, 0x61, 0x71)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("unknown kind 'q'")
                .hasMessageContaining(cid)
                .satisfies(e -> {
                    NodeFormatException nfe = (NodeFormatException) e;
                    assertThat(nfe.cid()).isEqualTo(cid);
                    assertThat(nfe.rule()).isEqualTo("unknown kind 'q'");
                });
    }

    @Test
    void schemaKeysListEachKind() {
        assertThat(NodeCodec.schemaKeys("m")).containsExactlyInAnyOrder("d", "n", "e", "c", "cnt");
        assertThat(NodeCodec.schemaKeys("mc")).containsExactly("e");
        assertThat(NodeCodec.schemaKeys("v")).containsExactlyInAnyOrder("c", "e");
        assertThat(NodeCodec.schemaKeys("vr")).containsExactlyInAnyOrder("n", "s", "r", "t");
        assertThat(NodeCodec.schemaKeys("b")).containsExactlyInAnyOrder("h", "c", "z");
        assertThat(NodeCodec.schemaKeys("x")).containsExactly("v");
        // PDT 2 phase 0 (P2-D-01, P2-R-01, P2-T-01, P2-V-01, P2-B-02): the seven level-2 kinds
        assertThat(NodeCodec.schemaKeys("db")).containsExactlyInAnyOrder("v", "s", "me", "mt", "bf", "i", "t");
        assertThat(NodeCodec.schemaKeys("dn")).containsExactlyInAnyOrder("l", "x", "e", "c");
        assertThat(NodeCodec.schemaKeys("rr")).containsExactlyInAnyOrder("n", "s", "r", "t");
        assertThat(NodeCodec.schemaKeys("rv")).containsExactlyInAnyOrder("c", "z");
        assertThat(NodeCodec.schemaKeys("t")).containsExactlyInAnyOrder("r", "v");
        assertThat(NodeCodec.schemaKeys("vg")).containsExactlyInAnyOrder("r", "b", "i", "t", "p");
        assertThat(NodeCodec.schemaKeys("bo")).containsExactlyInAnyOrder("g", "n", "p", "c");
        assertThat(Kind.all()).containsExactlyInAnyOrder("m", "mc", "v", "vr", "b", "x", "sn",
                "db", "dn", "rr", "rv", "t", "vg", "bo");
    }

    /** PD-N-04: a spilled-value link in an embedded slot is a link of the node and counts against the bound. */
    @Test
    void spilledLinksInEmbeddedSlotsCountTowardTheLinkBound() {
        List<Object> entries = new ArrayList<>();
        for (int i = 0; i <= NodeCodec.MAX_LINKS; i++) {
            entries.add(embed("key-" + i));
            entries.add(new Embedded(concat(bytes(0xd8, 0x2a, 0x58, 0x25, 0x00), linkTo("spill-" + i).binary())));
        }
        assertThatThrownBy(() -> NodeCodec.encode(Map.of("k", "mc", "e", entries)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("more than 1024 links");
        byte[] exactly = NodeCodec.encode(Map.of("k", "mc", "e", entries.subList(0, 2 * NodeCodec.MAX_LINKS)));
        assertThat(NodeCodec.links(exactly)).hasSize(NodeCodec.MAX_LINKS);
        assertThat(NodeCodec.decode(exactly).get("k")).isEqualTo("mc");
        // Hand-assembled over the bound: the decoder and links() refuse it by the same rule.
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        raw.writeBytes(bytes(0xa2, 0x61, 0x65, 0x99));
        raw.writeBytes(bytes(entries.size() >>> 8 & 0xff, entries.size() & 0xff));
        for (Object item : entries) {
            raw.writeBytes(((Embedded) item).bytes());
        }
        raw.writeBytes(bytes(0x61, 0x6b, 0x62, 0x6d, 0x63));
        byte[] over = raw.toByteArray();
        assertThatThrownBy(() -> NodeCodec.decode(over)).isInstanceOf(NodeFormatException.class).hasMessageContaining("more than 1024 links");
        assertThatThrownBy(() -> NodeCodec.links(over)).isInstanceOf(NodeFormatException.class).hasMessageContaining("more than 1024 links");
    }

    /** On a map node the entry list alternates key and value; links() reads the value slots only. */
    @Test
    void linksReadsValueSlotsOnlyOnMapNodes() {
        byte[] linkItem = concat(bytes(0xd8, 0x2a, 0x58, 0x25, 0x00), linkTo("spilled").binary());
        byte[] node = NodeCodec.encode(Map.of("k", "m", "d", 1, "n", 0,
                "e", List.of(embed("plain"), new Embedded(linkItem), embed("key"), embed("value")), "c", List.of()));
        assertThat(NodeCodec.links(node)).containsExactly(linkTo("spilled").cid());
        byte[] leaf = NodeCodec.encode(Map.of("k", "v", "e", List.of(new Embedded(linkItem), embed(1), new Embedded(linkItem))));
        assertThat(NodeCodec.links(leaf)).hasSize(2);
    }

    /**
     * HLC round six, 2026-11-08 (owner ruling F2): tag 42 stands only as a bare link item in a
     * value slot (PD-N-05). A key never spills, so the codec refuses a link item in a key slot of
     * a map node in both directions, as every reader refuses tag 42 in any other document.
     */
    @Test
    void refusesALinkItemInAKeySlotOfAMapNode() {
        byte[] linkItem = concat(bytes(0xd8, 0x2a, 0x58, 0x25, 0x00), linkTo("spilled").binary());
        for (String kind : List.of("m", "mc")) {
            Map<String, Object> node = new HashMap<>();
            node.put("k", kind);
            node.put("e", List.of(new Embedded(linkItem), embed("value")));
            if (kind.equals("m")) {
                node.putAll(Map.of("d", 1, "n", 0, "c", List.of()));
            }
            assertThatThrownBy(() -> NodeCodec.encode(node)).as(kind).isInstanceOf(NodeFormatException.class)
                    .hasMessageContaining("key");
            Map<String, Object> keyedByText = new HashMap<>(node);
            keyedByText.put("e", List.of(embed("ke"), embed("value")));
            byte[] honest = NodeCodec.encode(keyedByText);
            // the same node with the link item spliced into the key slot in place of the text "ke"
            byte[] key = CBOR.toBytes("ke");
            int at = indexOf(honest, key);
            byte[] forged = concat(Arrays.copyOfRange(honest, 0, at), linkItem,
                    Arrays.copyOfRange(honest, at + key.length, honest.length));
            assertThatThrownBy(() -> NodeCodec.decode(forged)).as(kind).isInstanceOf(NodeFormatException.class)
                    .hasMessageContaining("key");
            assertThatThrownBy(() -> NodeCodec.links(forged)).as(kind).isInstanceOf(NodeFormatException.class);
        }
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= haystack.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }
}
