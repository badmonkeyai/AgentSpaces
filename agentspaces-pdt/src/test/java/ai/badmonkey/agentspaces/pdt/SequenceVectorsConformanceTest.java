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

import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.golden.PdtVectors;
import ai.badmonkey.agentspaces.pdt.golden.SequenceVectors;
import ai.badmonkey.agentspaces.pdt.kinds.TextLeaf;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.pdt.values.PText;
import ai.badmonkey.agentspaces.pdt.values.PVector;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The golden vectors of PDT 2 phase 6 ({@link SequenceVectors}), read from {@code golden.json} as
 * {@code PdtVectorsConformanceTest} reads the earlier ones, with {@code -Dgolden.required=true}
 * failing when the file is absent: the relaxed tree reads and packs, the text edits follow Fugue,
 * and a text value reads inline and spilled.
 */
class SequenceVectorsConformanceTest {

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

    private static String[] parts(String name) {
        Object value = golden.get(name);
        assertThat(value).as(name + " is in golden.json").isInstanceOf(String.class);
        return ((String) value).split(",");
    }

    private static byte[] hex(String text) {
        return HexFormat.of().parseHex(text);
    }

    /** P2-R-03, P2-R-04: the relaxed tree indexes through its size tables, and its packed root is the vr root. */
    @Test
    void theSplicedVectorReadsThroughItsSizeTablesAndPacksToTheVrRootOfItsElements() {
        String[] splice = parts("pdt_rr_splice");
        NodeStore store = NodeStore.inMemory();
        for (int i = 2; i < splice.length; i++) {
            byte[] node = hex(splice[i]);
            assertThat(NodeCodec.encode(NodeCodec.decode(node))).isEqualTo(node);
            store.put(node, Cids.DAG_CBOR);
        }
        assertThat(Cids.of(hex(splice[2]), Cids.DAG_CBOR)).isEqualTo(splice[0]);
        assertThat(NodeCodec.kindOf(hex(splice[2]))).isEqualTo(Kind.RR);
        PVector<Integer> read = PVector.load(store, splice[0], Codec.of(Integer.class));
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < SequenceVectors.SPLICE_ELEMENTS; i++) {
            expected.add(i);
        }
        expected.add(SequenceVectors.SPLICE_AT, SequenceVectors.SPLICE_ELEMENT);
        assertThat(read.toList()).isEqualTo(expected);
        assertThat(read.nth(SequenceVectors.SPLICE_AT)).isEqualTo(SequenceVectors.SPLICE_ELEMENT);
        assertThat(read.packed().root()).isEqualTo(splice[1]);
        assertThat(store.reachable(splice[0])).hasSize(splice.length - 2);
    }

    /** P2-T-01, P2-T-02: replaying the edits gives both leaves and their union byte for byte. */
    @Test
    void theTextEditsFollowFugueAndMeetInTheirUnion() {
        String[] edits = parts("pdt_text_edits");
        TextLeaf alice = TextLeaf.decode(null, hex(edits[0]));
        TextLeaf bob = TextLeaf.decode(null, hex(edits[1]));
        TextLeaf merged = TextLeaf.decode(null, hex(edits[2]));
        assertThat(SequenceVectors.textAlice().leaf()).isEqualTo(alice);
        assertThat(SequenceVectors.textBob().leaf()).isEqualTo(bob);
        assertThat(alice.union(bob)).isEqualTo(merged);
        assertThat(merged.text()).isEqualTo(edits[3]).isEqualTo("Jello; big world!");
        assertThat(alice.text()).isEqualTo("Jello; world");
        assertThat(bob.text()).isEqualTo("Hello; big world!");
    }

    /** P2-T-02: the value form reads strictly as the leaf of pdt_text_runs, and the node form is no value. */
    @Test
    void theTextValueReadsAsTheRunsLeafAndOnlyInItsValueForm() {
        byte[] value = hex(parts("pdt_text_value")[0]);
        assertThat(value[0] & 0xff).as("an indefinite map (rule S8)").isEqualTo(0xbf);
        PText text = PText.codec("carol").decode(value);
        assertThat(text.leaf()).isEqualTo(PdtVectors.textRuns());
        assertThat(PText.codec("carol").encode(text)).isEqualTo(value);
        assertThatThrownBy(() -> PText.codec("carol").decode(PdtVectors.textRuns().encode()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** P2-T-02: the large text spilled to a t node and the small one stayed inline. */
    @Test
    void theSpilledMapHoldsTheNoteInlineAndTheDocumentInATNode() {
        String[] spill = parts("pdt_text_spill");
        assertThat(spill).hasSize(2);
        NodeStore store = NodeStore.inMemory();
        String root = store.put(hex(spill[0]), Cids.DAG_CBOR);
        store.put(hex(spill[1]), Cids.DAG_CBOR);
        assertThat(NodeCodec.kindOf(hex(spill[0]))).isEqualTo(Kind.M);
        assertThat(NodeCodec.kindOf(hex(spill[1]))).isEqualTo(Kind.T);
        assertThat(TextLeaf.decode(null, hex(spill[1])).text()).isEqualTo("x".repeat(1100));
        PMap<String, PText> map = PMap.load(store, root, Codec.of(String.class), PText.codec("carol"));
        assertThat(map.get("doc").orElseThrow().toString()).isEqualTo("x".repeat(1100));
        assertThat(map.get("note").orElseThrow().leaf()).isEqualTo(PdtVectors.textRuns());
        assertThat(root).isEqualTo(SequenceVectors.textMap(NodeStore.inMemory()).root());
    }

    /** P2-T-02: an agent's replica id. */
    @Test
    void theReplicaVectorIsTheAgentsReplica() {
        String[] replica = parts("pdt_text_replica");
        assertThat(replica[0]).isEqualTo(PdtVectors.GOLDEN_WRITER);
        assertThat(PText.replicaOf(AgentId.parse(replica[0]))).isEqualTo(replica[1]).hasSize(PText.REPLICA_LENGTH);
    }
}
