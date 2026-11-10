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
package ai.badmonkey.agentspaces.pdt.kinds;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.Datom;
import ai.badmonkey.agentspaces.pdt.Embedded;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PDT 2 phase 0 (P2-D-01, P2-R-01, P2-V-01, P2-B-02): the typed readers of the level-2 kinds
 * round-trip their nodes and refuse every rule that spans fields, each by name.
 */
class KindsTest {

    private static final CborCodec CBOR = CborCodec.defaultCodec();

    private static Link linkTo(String text) {
        return new Link(Cids.of(text.getBytes(StandardCharsets.UTF_8), Cids.DAG_CBOR));
    }

    private static byte[] raw(Map<String, Object> node) {
        return NodeCodec.encode(node);
    }

    private static Map<String, Object> node(Object... keysAndValues) {
        Map<String, Object> node = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            node.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return node;
    }

    // ------------------------------------------------------------------ db and dn

    private static DbRoot db(Link tail) {
        return new DbRoot(new Embedded(CBOR.toBytes(Map.of(":person/name", Map.of(":db/index", true)))),
                2, 536870914L, DbRoot.BRANCHING, linkTo("eavt"), linkTo("aevt"), linkTo("avet"), tail);
    }

    @Test
    void aDbRootRoundTripsAndLinksItsIndexesAndItsTail() {
        DbRoot root = db(linkTo("tail"));
        byte[] bytes = root.encode();
        assertThat(DbRoot.decode(null, bytes)).isEqualTo(root);
        assertThat(NodeCodec.links(bytes)).containsExactly(linkTo("aevt").cid(), linkTo("avet").cid(),
                linkTo("eavt").cid(), linkTo("tail").cid());
        DbRoot folded = db(null);
        assertThat(DbRoot.decode(null, folded.encode()).tail()).as("a folded database has no tail").isNull();
    }

    @Test
    void aDbRootRefusesAnotherVersionAMissingKeyAndATinyBranchingFactor() {
        Map<String, Object> node = NodeCodec.decode(db(null).encode());
        Map<String, Object> v2 = new LinkedHashMap<>(node);
        v2.put("v", 2L);
        assertThatThrownBy(() -> DbRoot.decode(null, raw(v2))).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("db root version 2");
        Map<String, Object> noTail = new LinkedHashMap<>(node);
        noTail.remove("t");
        assertThatThrownBy(() -> DbRoot.decode(null, raw(noTail))).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("lacks 't'");
        Map<String, Object> tiny = new LinkedHashMap<>(node);
        tiny.put("bf", 1L);
        assertThatThrownBy(() -> DbRoot.decode(null, raw(tiny))).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("branching factor");
        assertThatThrownBy(() -> DbRoot.decode(null, raw(node("k", Kind.DN, "l", 0L, "x", "eavt", "e", List.of()))))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("kind 'dn' where a 'db' node belongs");
    }

    @Test
    void aDbSchemaIsAMapWithTextKeysOrASpilledLink() {
        // PDT 2 phase 0 (P2-D-01, srad/agentspaces-pdt2.md §3.1.1): s is the schema map, keyword keys as text
        assertThatThrownBy(() -> new DbRoot(new Embedded(CBOR.toBytes("schema")), 0, 0, DbRoot.BRANCHING,
                linkTo("eavt"), linkTo("aevt"), linkTo("avet"), null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("schema");
        assertThatThrownBy(() -> new DbRoot(new Embedded(new byte[]{(byte) 0xbf, 0x01, (byte) 0xf5, (byte) 0xff}), 0, 0,
                DbRoot.BRANCHING, linkTo("eavt"), linkTo("aevt"), linkTo("avet"), null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("schema");
        Map<String, Object> node = NodeCodec.decode(db(null).encode());
        Map<String, Object> textSchema = new LinkedHashMap<>(node);
        textSchema.put("s", new Embedded(CBOR.toBytes("schema")));
        assertThatThrownBy(() -> DbRoot.decode(null, raw(textSchema))).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("schema");
        byte[] binary = linkTo("schema").binary();
        byte[] item = new byte[5 + binary.length];
        item[0] = (byte) 0xd8;
        item[1] = 0x2a;
        item[2] = 0x58;
        item[3] = 0x25;
        System.arraycopy(binary, 0, item, 5, binary.length);
        DbRoot spilled = new DbRoot(new Embedded(item), 0, 0, DbRoot.BRANCHING, linkTo("eavt"), linkTo("aevt"),
                linkTo("avet"), null);
        assertThat(NodeCodec.links(spilled.encode())).as("a spilled schema is a link of the root")
                .contains(linkTo("schema").cid());
        assertThat(DbRoot.decode(null, spilled.encode())).isEqualTo(spilled);
    }

    @Test
    void dnLeavesBranchesAndTailsKeepTheirShapes() {
        Datom a = new Datom(1, ":person/age", 36L, 536870913);
        Datom b = new Datom(1, ":person/name", "Ada", 536870913);
        DnNode leaf = DnNode.leaf(DnNode.EAVT, List.of(a, b));
        assertThat(DnNode.decode(null, leaf.encode())).isEqualTo(leaf);
        assertThat(NodeCodec.decode(leaf.encode())).as("a leaf has no 'c' key").doesNotContainKey("c");
        DnNode branch = DnNode.branch(1, DnNode.EAVT, List.of(b), List.of(new Link(Cids.of(leaf.encode(), Cids.DAG_CBOR))));
        assertThat(DnNode.decode(null, branch.encode())).isEqualTo(branch);
        DnNode tail = DnNode.tail(List.of(new Datom(1, ":person/age", 36L, -536870914), new Datom(1, ":person/age", 37L, 536870914)));
        assertThat(DnNode.decode(null, tail.encode())).isEqualTo(tail);

        assertThatThrownBy(() -> DnNode.decode(null, raw(node("k", Kind.DN, "l", 0L, "x", "vaet", "e", List.of(a)))))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("index 'vaet'");
        assertThatThrownBy(() -> DnNode.decode(null, raw(node("k", Kind.DN, "l", 1L, "x", "eavt", "e", List.of(a, b), "c", List.of(linkTo("one"))))))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("one link per key");
        assertThatThrownBy(() -> DnNode.decode(null, raw(node("k", Kind.DN, "l", 0L, "x", "eavt", "e", List.of(a), "c", List.of(linkTo("one"))))))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("a leaf carries no 'c'");
        assertThatThrownBy(() -> DnNode.decode(null, raw(node("k", Kind.DN, "l", 1L, "x", "eavt", "e", List.of(a)))))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("lacks 'c'");
        assertThatThrownBy(() -> DnNode.decode(null, raw(node("k", Kind.DN, "l", 1L, "x", "tail", "e", List.of(a), "c", List.of(linkTo("one"))))))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("a tail is a leaf");
        assertThatThrownBy(() -> DnNode.decode(null, raw(node("k", Kind.DN, "l", 0L, "x", "tail", "e", List.of()))))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("an empty tail");
    }

    // ------------------------------------------------------------------ rr and rv

    @Test
    void rrRootsAndRvNodesRoundTripAndCheckTheirSizes() {
        List<Embedded> tail = new ArrayList<>();
        for (int i = 96; i < 100; i++) {
            tail.add(new Embedded(CBOR.toBytes(i)));
        }
        RrRoot root = new RrRoot(100, 5, linkTo("rv"), tail);
        assertThat(RrRoot.decode(null, root.encode())).isEqualTo(root);
        RvNode relaxed = new RvNode(List.of(linkTo("a"), linkTo("b"), linkTo("c"), linkTo("d")), List.of(32L, 40L, 72L, 96L));
        assertThat(RvNode.decode(null, relaxed.encode())).isEqualTo(relaxed);
        RvNode dense = new RvNode(List.of(linkTo("a"), linkTo("b")), null);
        assertThat(NodeCodec.decode(dense.encode())).as("z only on a relaxed node").doesNotContainKey("z");
        assertThat(RvNode.decode(null, dense.encode())).isEqualTo(dense);

        assertThatThrownBy(() -> new RvNode(List.of(linkTo("a"), linkTo("b")), List.of(40L, 40L)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("strictly increasing");
        assertThatThrownBy(() -> new RvNode(List.of(linkTo("a")), List.of(1L, 2L)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("one size per child");
        assertThatThrownBy(() -> new RvNode(List.of(), null)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1 to 32 children");
        assertThatThrownBy(() -> new RrRoot(3, 5, null, tail)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tail");
        assertThatThrownBy(() -> new RrRoot(4, 5, linkTo("rv"), tail)).as("a trie behind a link holds an element")
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("trie");
        List<Embedded> long33 = new ArrayList<>();
        for (int i = 0; i < 33; i++) {
            long33.add(new Embedded(CBOR.toBytes(i)));
        }
        assertThatThrownBy(() -> new RrRoot(133, 5, linkTo("rv"), long33)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 32");
        assertThatThrownBy(() -> new RrRoot(100, 7, linkTo("rv"), tail)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shift");
        assertThatThrownBy(() -> RvNode.decode(null, raw(node("k", Kind.RV, "z", List.of(1L)))))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("lacks 'c'");
    }

    // ------------------------------------------------------------------ vg

    @Test
    void aVgNodeRoundTripsAndRefusesABadIssuerOrStamp() {
        String writer = "zQ3shPeer/writer";
        VgNode first = new VgNode(linkTo("r1").cid(), List.of(), writer, "1792320000000:0:golden", List.of());
        VgNode second = new VgNode(linkTo("r2").cid(), List.of(linkTo("r1").cid()), writer, "1792320000001:0:golden",
                List.of(new Link(Cids.of(first.encode(), Cids.DAG_CBOR))));
        assertThat(VgNode.decode(null, second.encode())).isEqualTo(second);
        assertThat(NodeCodec.links(second.encode())).containsExactly(Cids.of(first.encode(), Cids.DAG_CBOR));

        assertThatThrownBy(() -> new VgNode(linkTo("r").cid(), List.of(), "no-slash", "1:0:n", List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("issuer");
        assertThatThrownBy(() -> new VgNode(linkTo("r").cid(), List.of(), writer, "01:0:n", List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("issued");
        assertThatThrownBy(() -> new VgNode("not-a-cid", List.of(), writer, "1:0:n", List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("by CID");
        List<String> nine = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            nine.add(linkTo("b" + i).cid());
        }
        assertThatThrownBy(() -> new VgNode(linkTo("r").cid(), nine, writer, "1:0:n", List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("8 bases");
        assertThatThrownBy(() -> new VgNode(linkTo("r").cid(), List.of(), writer, "1:0:n", List.of(linkTo("p"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("one earlier vg node per base");
        assertThatThrownBy(() -> new VgNode(linkTo("r").cid(), List.of(linkTo("b").cid()), writer, "1:0:n",
                List.of(new Link(Cids.of(new byte[1], Cids.RAW)))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dag-cbor");
    }

    @Test
    void aVgNodeNamesItsRootAndBasesInCanonicalBase32() {
        // PDT 2 phase 0 (P2-V-01, gate finding G2): a CID's base32 text ends in two padding bits,
        // which canonical text holds at zero; Python, TypeScript and Ruby refuse other padding, so
        // Java does too, or one vg node would read in Java and fail in every other port.
        String writer = "zQ3shPeer/writer";
        String root = linkTo("r").cid();
        String padded = padded(root);
        // PDT 2 phase 1 (P2-X-09): the content swarm's own CID check now refuses the padding too
        assertThat(Cids.parse(padded).digest()).as("the padded text names the same bytes").isEqualTo(Cids.parse(root).digest());
        assertThat(Cids.isCid(padded)).as("content's strict parse refuses the padding bits").isFalse();
        assertThatThrownBy(() -> new VgNode(padded, List.of(), writer, "1:0:n", List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("by CID");
        assertThatThrownBy(() -> new VgNode(root, List.of(padded), writer, "1:0:n", List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("by CID");
        assertThatThrownBy(() -> raw(node("k", Kind.VG, "b", List.of(), "i", writer, "p", List.of(), "r", padded, "t", "1:0:n")))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("must hold a CID as text");
        byte[] good = new VgNode(root, List.of(root), writer, "1:0:n", List.of()).encode();
        byte[] badRoot = good.clone();
        int r = lastIndexOf(badRoot, root.getBytes(StandardCharsets.US_ASCII));
        System.arraycopy(padded.getBytes(StandardCharsets.US_ASCII), 0, badRoot, r, padded.length());
        byte[] badBase = good.clone();
        int b = indexOf(badBase, root.getBytes(StandardCharsets.US_ASCII));
        System.arraycopy(padded.getBytes(StandardCharsets.US_ASCII), 0, badBase, b, padded.length());
        assertThat(b).isLessThan(r);
        for (byte[] bytes : List.of(badRoot, badBase)) {
            assertThatThrownBy(() -> NodeCodec.decode(bytes)).isInstanceOf(NodeFormatException.class)
                    .hasMessageContaining("must hold a CID as text");
        }
    }

    /** The same CID text with a padding bit set in its last character. */
    private static String padded(String cid) {
        String alphabet = "abcdefghijklmnopqrstuvwxyz234567";
        int last = alphabet.indexOf(cid.charAt(cid.length() - 1));
        assertThat(last & 3).as("canonical padding").isZero();
        return cid.substring(0, cid.length() - 1) + alphabet.charAt(last | 1);
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        for (int i = 0; i + needle.length <= haystack.length; i++) {
            if (java.util.Arrays.equals(haystack, i, i + needle.length, needle, 0, needle.length)) {
                return i;
            }
        }
        throw new AssertionError("needle not found");
    }

    private static int lastIndexOf(byte[] haystack, byte[] needle) {
        for (int i = haystack.length - needle.length; i >= 0; i--) {
            if (java.util.Arrays.equals(haystack, i, i + needle.length, needle, 0, needle.length)) {
                return i;
            }
        }
        throw new AssertionError("needle not found");
    }

    // ------------------------------------------------------------------ bo

    @Test
    void aBoNodeHoldsTheParentsItsSizeImplies() {
        BoNode single = new BoNode(5000, new byte[0], List.of());
        assertThat(BoNode.decode(null, single.encode())).isEqualTo(single);
        BoNode seven = new BoNode(100 * 1024, new byte[6 * BoNode.PARENT_BYTES], List.of());
        assertThat(BoNode.decode(null, seven.encode())).isEqualTo(seven);
        assertThat(BoNode.groups(0)).isEqualTo(1);
        assertThat(BoNode.groups(16384)).isEqualTo(1);
        assertThat(BoNode.groups(16385)).isEqualTo(2);

        assertThatThrownBy(() -> new BoNode(100 * 1024, new byte[5 * BoNode.PARENT_BYTES], List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("7 groups take 6 parents");
        long split = (long) BoNode.LEAF_GROUPS * BoNode.GROUP_BYTES + 1;
        assertThatThrownBy(() -> new BoNode(split, new byte[0], List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("2 children");
        BoNode upper = new BoNode(split, new byte[BoNode.PARENT_BYTES], List.of(linkTo("l"), linkTo("r")));
        assertThat(BoNode.decode(null, upper.encode())).isEqualTo(upper);
        assertThat(BoNode.childSizes(split)).containsExactly((long) BoNode.LEAF_GROUPS * BoNode.GROUP_BYTES, 1L);
        Map<String, Object> wrongGroup = NodeCodec.decode(single.encode());
        wrongGroup = new LinkedHashMap<>(wrongGroup);
        wrongGroup.put("g", 6L);
        Map<String, Object> finalWrongGroup = wrongGroup;
        assertThatThrownBy(() -> BoNode.decode(null, raw(finalWrongGroup))).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("group size 2^6");
    }

    @Test
    void aFullBoLeafFitsTheNodeBound() {
        long size = (long) BoNode.LEAF_GROUPS * BoNode.GROUP_BYTES;
        byte[] encoded = new BoNode(size, new byte[(BoNode.LEAF_GROUPS - 1) * BoNode.PARENT_BYTES], List.of()).encode();
        assertThat(encoded.length).isLessThanOrEqualTo(NodeCodec.MAX_NODE_BYTES);
    }

    @Test
    void aBoLeafCoversEightMibAndAnUpperNodeLinksSixteenBlocks() {
        // PDT 2 phase 4 measures, P2-Q-PRF-05 (owner ruling of 2026-11-09, srad/agentspaces-pdt2.md §10.14): a leaf
        // covers 8 MiB in about 32 KiB, so a 2 MiB window across a leaf boundary pays two leaves and a short upper path
        long leaf = 8L << 20;
        assertThat((long) BoNode.LEAF_GROUPS * BoNode.GROUP_BYTES).isEqualTo(leaf);
        assertThat(new BoNode(leaf, new byte[511 * BoNode.PARENT_BYTES], List.of()).encode()).hasSize(32_728);
        assertThat(BoNode.childSizes(leaf)).as("a full leaf").isEmpty();
        assertThat(BoNode.childSizes(leaf + 1)).containsExactly(leaf, 1L);
        assertThat(BoNode.childSizes(16 * leaf)).hasSize(16).containsOnly(leaf);
        assertThat(BoNode.childSizes(16 * leaf + 1)).containsExactly(16 * leaf, 1L);
        assertThat(BoNode.childSizes(10L << 30)).as("10 GiB: five blocks of 2 GiB").hasSize(5).containsOnly(2L << 30);
        List<Link> sixteen = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            sixteen.add(linkTo("leaf" + i));
        }
        byte[] upper = new BoNode(16 * leaf, new byte[15 * BoNode.PARENT_BYTES], sixteen).encode();
        assertThat(upper.length).as("an upper node costs a few links and parents").isLessThan(1_700);
    }
}
