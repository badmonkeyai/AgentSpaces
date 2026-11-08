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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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

    @Test
    void skipperHandlesIndefiniteFormsFloatsAndTagsInsideEmbeddedItems() {
        byte[] indefiniteMap = bytes(0xbf, 0x61, 0x61, 0x01, 0x61, 0x62, 0x9f, 0x01, 0x02, 0xff, 0xff);
        byte[] indefiniteText = bytes(0x7f, 0x62, 0x61, 0x62, 0x61, 0x63, 0xff);
        byte[] halfFloat = bytes(0xf9, 0x3c, 0x00);
        byte[] tagged = bytes(0xc1, 0x1a, 0x51, 0x4b, 0x67, 0xb0);
        byte[] bigUint = bytes(0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff);
        List<Object> items = List.of(new Embedded(indefiniteMap), new Embedded(indefiniteText),
                new Embedded(halfFloat), new Embedded(tagged), new Embedded(bigUint));
        byte[] encoded = NodeCodec.encode(Map.of("k", "mc", "e", items));
        assertThat(NodeCodec.decode(encoded).get("e")).isEqualTo(items);
        assertThat(NodeCodec.skip(indefiniteMap, 0)).isEqualTo(indefiniteMap.length);
        assertThat(NodeCodec.skip(concat(halfFloat, bytes(0x00)), 0)).isEqualTo(3);
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
        assertThat(Kind.reserved("s")).isTrue();
        assertThat(Kind.reserved("rr")).isTrue();
        assertThat(Kind.known("s")).isFalse();
        assertThat(Kind.known("m")).isTrue();
        assertThatThrownBy(() -> NodeCodec.decode(bytes(0xa1, 0x61, 0x6b, 0x62, 0x72, 0x72)))
                .isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("kind 'rr' is reserved");
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
        assertThat(Kind.all()).containsExactlyInAnyOrder("m", "mc", "v", "vr", "b", "x", "sn");
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
                "e", List.of(new Embedded(linkItem), embed("plain"), embed("key"), new Embedded(linkItem)), "c", List.of()));
        assertThat(NodeCodec.links(node)).containsExactly(linkTo("spilled").cid());
        byte[] leaf = NodeCodec.encode(Map.of("k", "v", "e", List.of(new Embedded(linkItem), embed(1), new Embedded(linkItem))));
        assertThat(NodeCodec.links(leaf)).hasSize(2);
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
