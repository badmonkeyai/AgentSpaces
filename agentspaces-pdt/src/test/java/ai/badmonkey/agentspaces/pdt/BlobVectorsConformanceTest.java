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
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.content.Messages;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.content.tree.Dcz;
import ai.badmonkey.agentspaces.pdt.golden.BlobVectors;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The golden vectors of PDT 2 phase 4 ({@link BlobVectors}), read from {@code golden.json} as
 * {@code PdtVectorsConformanceTest} reads the others, with {@code -Dgolden.required=true} failing when the file is
 * absent: the {@code dcz} field names its base and opens to the target, and the level-2 {@code NODES} reads strictly
 * and keeps its bytes (P2-Z-01, P2-Z-04, P2-F-02).
 */
class BlobVectorsConformanceTest {

    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static Map<String, Object> golden;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void loadVectors() throws Exception {
        Path found = GOLDEN_PATHS.stream().filter(Files::exists).findFirst().orElse(null);
        if (Boolean.getBoolean("golden.required")) {
            assertThat(found).as("golden.json is required (golden.required=true)").isNotNull();
        }
        assumeTrue(found != null, "golden.json not present");
        golden = new ObjectMapper().readValue(found.toFile(), Map.class);
    }

    private static String vector(String name) {
        Object value = golden.get(name);
        assertThat(value).as(name + " is in golden.json").isInstanceOf(String.class);
        return (String) value;
    }

    @Test
    void theDczFieldNamesItsBaseAndOpensToTheTarget() {
        String[] parts = vector("pdt_dcz_frame").split(",");
        assertThat(parts).hasSize(3);
        byte[] base = HexFormat.of().parseHex(parts[0]);
        byte[] target = HexFormat.of().parseHex(parts[1]);
        byte[] field = HexFormat.of().parseHex(parts[2]);
        assertThat(base).isEqualTo(BlobVectors.base());
        assertThat(target).isEqualTo(BlobVectors.target());
        assertThat(field[0]).as("P2-F-02: the zstd format byte").isEqualTo(Dcz.ZSTD);
        assertThat(Arrays.copyOfRange(field, 1, 9)).containsExactly(0x5e, 0x2a, 0x4d, 0x18, 0x20, 0x00, 0x00, 0x00);
        assertThat(Arrays.copyOfRange(field, 9, 41)).as("RFC 9842: the base's sha-256").isEqualTo(Digests.sha256(base));
        assertThat(Dcz.decode(base, field)).isEqualTo(target);
        assertThat(2 * field.length).as("under half the chunk, so a holder may send it").isLessThan(target.length);
        assertThat(parts[2]).as("Java's zstd at the pinned version writes the same field").isEqualTo(HexFormat.of().formatHex(BlobVectors.field()));
    }

    @Test
    void theLevelTwoNodesCarriesItsDeltaLastAndReadsStrictly() {
        byte[] wire = HexFormat.of().parseHex(vector("content_nodes_deltas_cbor"));
        CborCodec codec = CborCodec.defaultCodec();
        Messages.Nodes nodes = Messages.decode(codec, wire, Messages.Nodes.class).orElseThrow();
        assertThat(codec.toBytes(nodes)).isEqualTo(wire);
        assertThat(nodes.violation()).isEmpty();
        assertThat(nodes.nodes()).isEmpty();
        assertThat(nodes.deltas()).hasSize(1);
        Messages.NodeDelta delta = nodes.deltas().get(0);
        String[] frame = vector("pdt_dcz_frame").split(",");
        assertThat(delta.cid()).isEqualTo(Cids.of(HexFormat.of().parseHex(frame[1]), Cids.RAW));
        assertThat(delta.base()).isEqualTo(Cids.of(HexFormat.of().parseHex(frame[0]), Cids.RAW));
        assertThat(HexFormat.of().formatHex(delta.dcz())).isEqualTo(frame[2]);
        String hex = HexFormat.of().formatHex(wire);
        assertThat(hex.indexOf(HexFormat.of().formatHex("deltas".getBytes())))
                .as("rule 14: the appended field comes after the six fields of level 1")
                .isGreaterThan(hex.indexOf(HexFormat.of().formatHex("missing".getBytes())));
        assertThat(BlobVectors.vectors()).containsEntry("content_nodes_deltas_cbor", vector("content_nodes_deltas_cbor"));
    }
}
