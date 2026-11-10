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

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PDT 2 phase 0 (P2-D-01, P2-T-01, P2-V-01, P2-B-02, P2-R-01): the field types the seven
 * level-2 kinds add to {@link NodeCodec}, at the byte level: datoms with their value grammar,
 * Fugue runs, the {@code db} index map, CID text, and strict UTF-8 text.
 */
class Pdt2NodeFieldsTest {

    private static final HexFormat HEX = HexFormat.of();

    private static Link linkTo(String text) {
        return new Link(Cids.of(text.getBytes(StandardCharsets.UTF_8), Cids.DAG_CBOR));
    }

    private static Map<String, Object> leaf(List<Datom> datoms) {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("k", Kind.DN);
        node.put("l", 0L);
        node.put("x", "eavt");
        node.put("e", datoms);
        return node;
    }

    private static byte[] bytes(String hex) {
        return HEX.parseHex(hex.replace(" ", ""));
    }

    @Test
    void aDatomLeafEncodesAsTheCanonicalBytes() {
        byte[] encoded = NodeCodec.encode(leaf(List.of(new Datom(1, ":a", 2L, 3))));
        // {"e": [[1, ":a", 2, 3]], "k": "dn", "l": 0, "x": "eavt"}: keys e, k, l, x in DAG-CBOR order
        assertThat(HEX.formatHex(encoded)).isEqualTo("a4" + "6165" + "81" + "8401623a610203"
                + "616b" + "62646e" + "616c" + "00" + "6178" + "6465617674");
        Map<String, Object> decoded = NodeCodec.decode(encoded);
        assertThat(decoded.get("e")).isEqualTo(List.of(new Datom(1, ":a", 2L, 3)));
    }

    @Test
    void everyDatomValueTagRoundTrips() {
        Link spilled = new Link(Cids.of(new byte[5000], Cids.RAW));
        List<Datom> datoms = List.of(
                new Datom(3, ":v/bigint", Datom.Tagged.bigint(BigInteger.TWO.pow(70)), 536870913),
                new Datom(3, ":v/bool", true, 536870913),
                new Datom(3, ":v/bytes", new byte[]{0, 1, 2}, 536870913),
                new Datom(3, ":v/float", 1.5d, 536870913),
                new Datom(3, ":v/instant", Datom.Tagged.instant(1792320000000L), 536870913),
                new Datom(3, ":v/int", -7L, 536870913),
                new Datom(3, ":v/keyword", ":status/active", 536870913),
                new Datom(3, ":v/spill", spilled, 536870913),
                new Datom(3, ":v/text", "Ada ✓", 536870913),
                new Datom(3, ":v/uuid", Datom.Tagged.uuid(new UUID(0x0123456789abcdefL, 0xfedcba9876543210L)), -536870914));
        byte[] encoded = NodeCodec.encode(leaf(datoms));
        assertThat(NodeCodec.decode(encoded).get("e")).isEqualTo(datoms);
        assertThat(NodeCodec.encode(NodeCodec.decode(encoded))).isEqualTo(encoded);
        assertThat(NodeCodec.links(encoded)).as("a spilled datom value is a link of the node").containsExactly(spilled.cid());
        // a negative transaction (a retraction) is major type 1; the float is float64
        assertThat(HEX.formatHex(encoded)).contains("3a20000001").contains("fb3ff8000000000000");
    }

    @Test
    void theDatomValueGrammarRefusesEveryOtherItem() {
        String head = "a4 6165 81 84 01 62 3a61";
        String tail = " 03 616b 62646e 616c 00 6178 6465617674";
        Map<String, String> refused = new LinkedHashMap<>();
        refused.put("f97e00", "half");
        refused.put("fa3fc00000", "float32");
        refused.put("fb7ff8000000000000", "not finite");
        refused.put("fb7ff0000000000000", "not finite");
        refused.put("1802", "shortest");
        refused.put("f6", "null");
        refused.put("bf6161 01 ff", "major type 5");
        refused.put("a1616101", "major type 5");
        refused.put("82 6171 01", "unknown value tag 'q'");
        refused.put("82 6175 43010203", "16 bytes");
        refused.put("82 6169 6178", "integer");
        refused.put("82 616e 42 0001", "minimal");
        refused.put("82 616e 40", "minimal");
        refused.put("83 6169 01 02", "two items");
        refused.put("c2 41 01", "tag 2");
        refused.put("7f 6161 ff", "indefinite");
        refused.put("62 c328", "UTF-8");
        for (Map.Entry<String, String> e : refused.entrySet()) {
            assertThatThrownBy(() -> NodeCodec.decode(bytes(head + e.getKey() + tail))).as(e.getKey())
                    .isInstanceOf(NodeFormatException.class).hasMessageContaining(e.getValue());
        }
        assertThatThrownBy(() -> NodeCodec.decode(bytes("a4 6165 81 83 01 62 3a61 02" + " 616b 62646e 616c 00 6178 6465617674")))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("four items");
        assertThatThrownBy(() -> NodeCodec.decode(bytes("a4 6165 81 84 01 01 02 03" + " 616b 62646e 616c 00 6178 6465617674")))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("major type 3");
    }

    @Test
    void aDatomRefusesAValueOutsideTheGrammarOnTheWayIn() {
        assertThatThrownBy(() -> new Datom(1, ":a", 1.5f, 1)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Float");
        assertThatThrownBy(() -> new Datom(1, ":a", Double.NaN, 1)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("finite");
        assertThatThrownBy(() -> new Datom(1, ":a", null, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Datom.Tagged("u", new byte[15])).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16 bytes");
        assertThatThrownBy(() -> new Datom.Tagged("n", new byte[]{0, 1})).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minimal");
        assertThat(new Datom(1, ":a", 7, 1).v()).as("an Integer reads as the Long it encodes").isEqualTo(7L);
        assertThat(Datom.Tagged.bigint(BigInteger.valueOf(-129)).inner()).isEqualTo(new byte[]{(byte) 0xff, 0x7f});
    }

    @Test
    void aDatomCarriesASignedEntityAndANonZeroTransaction() {
        // PDT 2 phase 0 (P2-D-01, §3.1.1): e is an int, written under its signed head, and tx is negative
        // for a retraction, so tx 0 has no retraction form and no datom carries it
        Datom negative = new Datom(-5, ":a", 1L, 3);
        byte[] encoded = NodeCodec.encode(leaf(List.of(negative)));
        assertThat(HEX.formatHex(encoded)).contains("8424623a610103");
        assertThat(NodeCodec.decode(encoded).get("e")).isEqualTo(List.of(negative));
        assertThatThrownBy(() -> new Datom(1, ":a", 1L, 0)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tx");
        byte[] zeroTx = NodeCodec.encode(leaf(List.of(new Datom(1, ":a", 1L, 3))));
        zeroTx[HEX.formatHex(zeroTx).indexOf("8401623a610103") / 2 + 6] = 0x00;
        assertThatThrownBy(() -> NodeCodec.decode(zeroTx)).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("tx");
    }

    @Test
    void textRunsRoundTripAndRefuseTheirOwnRules() {
        TextRun first = new TextRun("alice", 0, null, TextRun.RIGHT, 4, false, "Ab😀c");
        TextRun deleted = new TextRun("alice", 4, new TextRun.CharId("alice", 1), TextRun.LEFT, 2, true, "");
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("k", Kind.T);
        node.put("r", List.of(first, deleted));
        node.put("v", 1L);
        byte[] encoded = NodeCodec.encode(node);
        assertThat(NodeCodec.decode(encoded).get("r")).isEqualTo(List.of(first, deleted));
        assertThat(NodeCodec.links(encoded)).isEmpty();
        // {"k": "t", "r": [["alice", 4, ["alice", 1], "L", 2, true, ""]], "v": 1}
        assertThat(NodeCodec.decode(bytes("a3 616b 6174 6172 81 87 65616c696365 04 82 65616c696365 01 614c 02 f5 60 6176 01")).get("r"))
                .isEqualTo(List.of(deleted));

        assertThatThrownBy(() -> new TextRun("alice", 0, null, "X", 1, false, "a"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("side");
        assertThatThrownBy(() -> new TextRun("alice", 0, null, TextRun.RIGHT, 1, true, "a"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("deleted run");
        assertThatThrownBy(() -> new TextRun("alice", 0, null, TextRun.RIGHT, 2, false, "😀"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("code points");
        assertThatThrownBy(() -> new TextRun("", 0, null, TextRun.RIGHT, 1, false, "a"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("replica");
        assertThatThrownBy(() -> new TextRun("alice", 0, null, TextRun.RIGHT, 0, false, ""))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("length");
        // a parent of three items, and a side the grammar does not name, on the way out of the bytes
        assertThatThrownBy(() -> NodeCodec.decode(bytes("a3 616b 6174 6172 81 87 65616c696365 04 83 65616c696365 01 01 614c 02 f5 60 6176 01")))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("parent");
        assertThatThrownBy(() -> NodeCodec.decode(bytes("a3 616b 6174 6172 81 87 65616c696365 04 f6 6158 02 f5 60 6176 01")))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("side");
    }

    @Test
    void theDbIndexMapHoldsExactlyTheThreeIndexLinksInOrder() {
        Map<String, Link> index = new LinkedHashMap<>();
        index.put("eavt", linkTo("eavt"));
        index.put("aevt", linkTo("aevt"));
        index.put("avet", linkTo("avet"));
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("k", Kind.DB);
        node.put("v", 1L);
        node.put("s", new Embedded(CborCodec.defaultCodec().toBytes(Map.of())));
        node.put("me", 2L);
        node.put("mt", 536870914L);
        node.put("bf", 128L);
        node.put("i", index);
        node.put("t", linkTo("tail"));
        byte[] encoded = NodeCodec.encode(node);
        Map<String, Object> decoded = NodeCodec.decode(encoded);
        assertThat(List.copyOf(((Map<?, ?>) decoded.get("i")).keySet())).isEqualTo(List.of("aevt", "avet", "eavt"));
        assertThat(NodeCodec.links(encoded)).containsExactly(linkTo("aevt").cid(), linkTo("avet").cid(),
                linkTo("eavt").cid(), linkTo("tail").cid());
        assertThat(NodeCodec.encode(decoded)).isEqualTo(encoded);

        index.remove("avet");
        assertThatThrownBy(() -> NodeCodec.encode(node)).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("aevt, avet and eavt");
        index.put("avet", linkTo("avet"));
        index.put("vaet", linkTo("vaet"));
        assertThatThrownBy(() -> NodeCodec.encode(node)).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("aevt, avet and eavt");
        // {"i": {"avet": link, "aevt": link, "eavt": link}} out of order, inside an otherwise minimal db node
        String link = "d82a5825" + "00" + HEX.formatHex(linkTo("x").binary());
        assertThatThrownBy(() -> NodeCodec.decode(bytes("a2 6169 a3 6461766574" + link + "6461657674" + link + "6465617674" + link + " 616b 626462")))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("aevt, avet and eavt");
    }

    @Test
    void cidTextFieldsAndStrictTextRefuseWhatTheyCannotHold() {
        Map<String, Object> vg = new LinkedHashMap<>();
        vg.put("k", Kind.VG);
        vg.put("r", "not-a-cid");
        assertThatThrownBy(() -> NodeCodec.encode(vg)).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("'r' must hold a CID");
        vg.put("r", linkTo("root").cid());
        vg.put("b", List.of(linkTo("base").cid()));
        vg.put("i", "peer/writer");
        vg.put("t", "1792320000000:0:golden");
        vg.put("p", List.of(linkTo("vg")));
        byte[] encoded = NodeCodec.encode(vg);
        assertThat(NodeCodec.decode(encoded)).isEqualTo(vg);
        assertThat(NodeCodec.links(encoded)).as("a vg node links only earlier vg nodes, never a root (6.10, rule 4)")
                .containsExactly(linkTo("vg").cid());
        // {"k": "dn", "x": <the byte 0xff as text>}: text is well-formed UTF-8 or refused
        assertThatThrownBy(() -> NodeCodec.decode(bytes("a2 616b 62646e 6178 61ff"))).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("UTF-8");
    }

    @Test
    void aBaoNodeCarriesItsChainingValuesAsOneByteString() {
        Map<String, Object> bo = new LinkedHashMap<>();
        bo.put("k", Kind.BO);
        bo.put("g", 4L);
        bo.put("n", 32768L);
        bo.put("p", new byte[64]);
        bo.put("c", List.of());
        byte[] encoded = NodeCodec.encode(bo);
        assertThat(HEX.formatHex(encoded)).startsWith("a5616380616704").contains("61705840");
        assertThat(NodeCodec.links(encoded)).isEmpty();
    }
}
