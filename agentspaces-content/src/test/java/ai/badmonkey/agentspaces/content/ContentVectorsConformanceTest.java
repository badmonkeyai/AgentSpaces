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
}
