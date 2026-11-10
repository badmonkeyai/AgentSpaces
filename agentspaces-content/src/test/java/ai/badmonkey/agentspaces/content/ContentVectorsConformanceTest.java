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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Holding;
import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.tree.Cids;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * SPEC v0.1.23 annex G: the content-swarm vectors decode into the real records,
 * the manifest's root recomputes from its table, and every vector re-encodes
 * byte for byte, so the Python and TypeScript ports can pin the same bytes.
 */
class ContentVectorsConformanceTest {

    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static final boolean GOLDEN_REQUIRED = Boolean.getBoolean("golden.required");
    private static Map<String, Object> golden;
    private final CborCodec codec = CborCodec.defaultCodec();

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void loadVectors() throws Exception {
        Path found = GOLDEN_PATHS.stream().filter(Files::exists).findFirst().orElse(null);
        if (GOLDEN_REQUIRED) {
            assertThat(found).as("golden.json is required (golden.required=true) but none of "
                    + GOLDEN_PATHS + " exists").isNotNull();
        }
        assumeTrue(found != null, "golden.json not present");
        golden = new ObjectMapper().readValue(found.toFile(), Map.class);
        assumeTrue(golden.containsKey("content_root_cidv1"), "golden.json predates the content vectors");
    }

    private byte[] hex(String name) {
        return HexFormat.of().parseHex((String) golden.get(name));
    }

    private <T> T roundTrip(String name, Class<T> type) {
        byte[] bytes = hex(name);
        T value = codec.fromBytes(bytes, type);
        assertThat(codec.toBytes(value)).as(name + " re-encodes byte for byte").isEqualTo(bytes);
        return value;
    }

    @Test
    void theInlineManifestRecomputesItsRoot() {
        Manifest m = roundTrip("content_manifest_inline_cbor", Manifest.class);
        assertThat(m.violation()).isEmpty();
        assertThat(m.root()).isEqualTo(golden.get("content_root_cidv1"));
        assertThat(ContentRoots.rootOf(m.table())).isEqualTo(m.root());
        assertThat(m.pieces()).isEqualTo(3);
        assertThat(m.pieceLength(2)).isEqualTo(100);
        assertThat(m.attrs()).containsEntry("run", "42");
        // the pieces of the vector: piece i is filled with (i + 1)
        for (int i = 0; i < 3; i++) {
            byte[] piece = new byte[m.pieceLength(i)];
            java.util.Arrays.fill(piece, (byte) (i + 1));
            assertThat(ContentRoots.verifies(m.table(), i, piece)).as("piece " + i).isTrue();
        }
    }

    @Test
    void theReferencedManifestNamesItsTableBlock() {
        Manifest m = roundTrip("content_manifest_tableref_cbor", Manifest.class);
        assertThat(m.violation()).isEmpty();
        assertThat(m.table()).isNull();
        assertThat(m.tableRef()).startsWith("z");
        assertThat(m.pieces()).isEqualTo(65);
        assertThat(m.sealing()).isEqualTo(Manifest.SEALING_GROUP_KEY);
        assertThat(m.keyEpoch()).isEqualTo(3L);
    }

    @Test
    void holdingsDecodeTheirBitfields() {
        Holding partial = roundTrip("content_holding_partial_cbor", Holding.class);
        BitSet have = partial.decodeHave(3);
        assertThat(have.get(0)).isTrue();
        assertThat(have.get(1)).isFalse();
        assertThat(have.get(2)).isTrue();
        assertThat(partial.count()).isEqualTo(2);
        assertThat(partial.via()).containsExactly(PeerId.of("zB"));
        Holding complete = roundTrip("content_holding_complete_cbor", Holding.class);
        assertThat(complete.complete()).isTrue();
        assertThat(complete.decodeHave(3).cardinality()).isEqualTo(3);
    }

    /** PDT 2 phase 1, P2-X-09: every message vector and the manifest record pass the strict receive path. */
    @Test
    void everyMessageVectorPassesTheStrictReceivePath() {
        Map<String, Class<?>> types = new java.util.LinkedHashMap<>();
        types.put("content_piece_want_cbor", Messages.PieceWant.class);
        types.put("content_piece_cbor", Messages.Piece.class);
        types.put("content_have_cbor", Messages.Have.class);
        types.put("content_cancel_cbor", Messages.Cancel.class);
        types.put("content_reject_cbor", Messages.Reject.class);
        types.put("content_manifest_want_cbor", Messages.ManifestWant.class);
        types.put("content_manifest_cbor", Messages.ManifestReply.class);
        types.put("content_node_want_cbor", Messages.NodeWant.class);
        types.put("content_nodes_cbor", Messages.Nodes.class);
        types.put("content_tree_want_cbor", Messages.TreeWant.class);
        types.put("content_tree_end_cbor", Messages.TreeEnd.class);
        types.forEach((name, type) -> assertThat(Messages.decode(codec, hex(name), type)).as(name).isPresent());
        Messages.ManifestReply reply = Messages.decode(codec, hex("content_manifest_cbor"), Messages.ManifestReply.class).orElseThrow();
        assertThat(codec.fromWire(reply.record(), Manifest.class).root()).isEqualTo(golden.get("content_root_cidv1"));
    }

    @Test
    void everyMessageDecodesWithItsTag() {
        assertThat(roundTrip("content_piece_want_cbor", Messages.PieceWant.class).t()).isEqualTo(Messages.PIECE_WANT);
        Messages.Piece piece = roundTrip("content_piece_cbor", Messages.Piece.class);
        assertThat(piece.t()).isEqualTo(Messages.PIECE);
        assertThat(piece.bytes()).hasSize(16);
        assertThat(roundTrip("content_have_cbor", Messages.Have.class).t()).isEqualTo(Messages.HAVE);
        assertThat(roundTrip("content_cancel_cbor", Messages.Cancel.class).t()).isEqualTo(Messages.CANCEL);
        assertThat(roundTrip("content_reject_cbor", Messages.Reject.class).reason()).isEqualTo(Messages.REASON_OVER_BUDGET);
        assertThat(roundTrip("content_manifest_want_cbor", Messages.ManifestWant.class).t()).isEqualTo(Messages.MANIFEST_WANT);
        Messages.ManifestReply reply = roundTrip("content_manifest_cbor", Messages.ManifestReply.class);
        assertThat(reply.record()).isEqualTo(hex("content_manifest_inline_cbor"));
        for (String name : List.of("content_piece_want_cbor", "content_piece_cbor", "content_have_cbor",
                "content_cancel_cbor", "content_reject_cbor", "content_manifest_want_cbor", "content_manifest_cbor")) {
            assertThat(Messages.tagOf(codec, hex(name))).as(name).isPresent();
        }
    }

    @Test
    void theTreeManifestCarriesItsLayoutAndTheOthersDoNot() {
        assumeTrue(golden.containsKey("content_manifest_tree_cbor"), "golden.json predates the tree vectors");
        Manifest tree = roundTrip("content_manifest_tree_cbor", Manifest.class);
        assertThat(tree.violation()).isEmpty();
        assertThat(tree.layout()).isEqualTo(Manifest.LAYOUT_TREE);
        assertThat(tree.treeLayout()).isTrue();
        assertThat(tree.root()).startsWith("bafyrei").hasSize(59);
        assertThat(Cids.parse(tree.root()).codec()).isEqualTo(Cids.DAG_CBOR);
        assertThat(Cids.of(new byte[]{(byte) 0xa0}, Cids.DAG_CBOR)).isEqualTo(tree.root());
        Manifest inline = roundTrip("content_manifest_inline_cbor", Manifest.class);
        assertThat(inline.layout()).isNull();
        assertThat(inline.treeLayout()).isFalse();
        // the tree vector is the inline vector with a new root and the trailing layout; nothing else moved
        Manifest expected = new Manifest(inline.v(), tree.root(), inline.size(), inline.pieceSize(), inline.pieces(),
                inline.table(), inline.tableRef(), inline.tableRefs(), inline.name(), inline.mediaType(), inline.sealing(),
                inline.keyEpoch(), inline.hashing(), inline.origin(), inline.created(), inline.attrs(), Manifest.LAYOUT_TREE);
        assertThat(tree).isEqualTo(expected);
        assertThat(codec.toBytes(expected)).isEqualTo(hex("content_manifest_tree_cbor"));
        assertThat(roundTrip("content_manifest_tableref_cbor", Manifest.class).layout()).isNull();
    }

    @Test
    void theTreeExchangeMessagesDecodeWithinTheirBounds() {
        assumeTrue(golden.containsKey("content_node_want_cbor"), "golden.json predates the tree vectors");
        String treeRoot = Cids.of(new byte[]{(byte) 0xa0}, Cids.DAG_CBOR);
        String emptyChunk = Cids.of(new byte[0], Cids.RAW);
        Messages.NodeWant want = roundTrip("content_node_want_cbor", Messages.NodeWant.class);
        assertThat(want.t()).isEqualTo(Messages.NODE_WANT);
        assertThat(want.req()).isEqualTo(42);
        assertThat(want.cids()).containsExactly(treeRoot, emptyChunk);
        assertThat(want.violation()).isEmpty();

        Messages.Nodes nodes = roundTrip("content_nodes_cbor", Messages.Nodes.class);
        assertThat(nodes.t()).isEqualTo(Messages.NODES);
        assertThat(nodes.req()).isEqualTo(42);
        assertThat(nodes.plaintext()).isTrue();
        assertThat(nodes.sealed()).isNull();
        assertThat(nodes.epoch()).isNull();
        assertThat(nodes.nodes()).hasSize(2);
        assertThat(Cids.of(nodes.nodes().get(0), Cids.DAG_CBOR)).as("node 0 is the asked-for root").isEqualTo(treeRoot);
        assertThat(Cids.of(nodes.nodes().get(1), Cids.RAW)).as("node 1 is the asked-for chunk").isEqualTo(emptyChunk);
        assertThat(nodes.missing()).containsExactly((String) golden.get("content_root_cidv1"));
        assertThat(nodes.violation()).isEmpty();

        Messages.TreeWant treeWant = roundTrip("content_tree_want_cbor", Messages.TreeWant.class);
        assertThat(treeWant.t()).isEqualTo(Messages.TREE_WANT);
        assertThat(treeWant.req()).isEqualTo(43);
        assertThat(treeWant.root()).isEqualTo(treeRoot);
        assertThat(treeWant.haves()).containsExactly(emptyChunk);
        assertThat(treeWant.budget()).isEqualTo(Messages.MAX_TREE_BUDGET);
        assertThat(treeWant.sketch()).isNull();
        assertThat(treeWant.violation()).isEmpty();

        Messages.TreeEnd end = roundTrip("content_tree_end_cbor", Messages.TreeEnd.class);
        assertThat(end.t()).isEqualTo(Messages.TREE_END);
        assertThat(end.req()).isEqualTo(43);
        assertThat(end.frontier()).containsExactly(emptyChunk);
        assertThat(end.reason()).isEqualTo(Messages.END_BUDGET);
        assertThat(end.violation()).isEmpty();

        for (String name : List.of("content_node_want_cbor", "content_nodes_cbor", "content_tree_want_cbor",
                "content_tree_end_cbor")) {
            assertThat(Messages.tagOf(codec, hex(name))).as(name).isPresent();
            byte[] bytes = hex(name);
            assertThat(bytes[1]).as(name + " has 't' first").isEqualTo((byte) 0x61);
            assertThat(bytes[2]).isEqualTo((byte) 't');
        }
    }
}
