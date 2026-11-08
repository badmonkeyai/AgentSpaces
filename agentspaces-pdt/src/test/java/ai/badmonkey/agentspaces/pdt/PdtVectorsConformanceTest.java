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

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.golden.PdtVectors;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The golden vectors of PD-N-07 (srad/agentspaces-pdt.md §3.1): every {@code pdt_*}
 * field of {@code golden.json} equals a fresh computation by {@link PdtVectors}, and
 * the node vectors decode as the kinds they promise. The file is looked up as
 * {@code ContentVectorsConformanceTest} does: the vendored copy first, then the
 * specification kit beside the repository; {@code -Dgolden.required=true} fails
 * when neither exists.
 */
class PdtVectorsConformanceTest {

    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static final boolean GOLDEN_REQUIRED = Boolean.getBoolean("golden.required");
    private static Map<String, Object> golden;
    private static Map<String, String> fresh;

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
        assumeTrue(golden.containsKey("pdt_map_empty"), "golden.json predates the pdt vectors");
        fresh = PdtVectors.vectors();
    }

    private String vector(String name) {
        Object value = golden.get(name);
        assertThat(value).as(name + " is in golden.json").isInstanceOf(String.class);
        return (String) value;
    }

    private byte[] node(String name) {
        return HexFormat.of().parseHex(vector(name));
    }

    @Test
    void everyPdtVectorEqualsAFreshComputation() {
        for (Map.Entry<String, String> entry : fresh.entrySet()) {
            assertThat(vector(entry.getKey())).as(entry.getKey()).isEqualTo(entry.getValue());
        }
        assertThat(golden.keySet().stream().filter(k -> k.startsWith("pdt_")).toList())
                .as("no pdt_* field in golden.json lacks a generator")
                .containsExactlyInAnyOrderElementsOf(fresh.keySet());
    }

    @Test
    void theMapVectorsDecodeAsMapRoots() {
        Map<String, Object> empty = NodeCodec.decode(node("pdt_map_empty"));
        assertThat(empty.get("k")).isEqualTo(Kind.M);
        assertThat(empty.get("cnt")).isEqualTo(0L);
        assertThat(empty.get("d")).isEqualTo(0L);
        assertThat(empty.get("n")).isEqualTo(0L);

        Map<String, Object> small = NodeCodec.decode(node("pdt_map_small"));
        assertThat(small.get("k")).isEqualTo(Kind.M);
        assertThat(small.get("cnt")).isEqualTo(2L);
        assertThat((List<?>) small.get("e")).hasSize(4);
        assertThat(small.get("n")).isEqualTo(0L);

        Map<String, Object> deep = NodeCodec.decode(node("pdt_map_deep"));
        assertThat(deep.get("k")).isEqualTo(Kind.M);
        assertThat(deep.get("cnt")).isEqualTo(100L);
        assertThat((List<?>) deep.get("c")).as("100 keys over 32 slots force child nodes").isNotEmpty();
        assertThat(NodeCodec.links(node("pdt_map_deep"))).hasSameSizeAs((List<?>) deep.get("c"));
    }

    @Test
    void theVectorVectorsDecodeAsVectorRoots() {
        Map<String, Object> tail = NodeCodec.decode(node("pdt_vector_tail"));
        assertThat(tail.get("k")).isEqualTo(Kind.VR);
        assertThat(tail.get("n")).isEqualTo(10L);
        assertThat(tail.get("r")).isNull();
        assertThat((List<?>) tail.get("t")).hasSize(10);

        Map<String, Object> deep = NodeCodec.decode(node("pdt_vector_deep"));
        assertThat(deep.get("k")).isEqualTo(Kind.VR);
        assertThat(deep.get("n")).isEqualTo(1100L);
        assertThat(deep.get("s")).isEqualTo(10L);
        assertThat(deep.get("r")).isInstanceOf(Link.class);
        assertThat((List<?>) deep.get("t")).hasSize(1100 - ((1100 - 1) >>> 5 << 5));
    }

    @Test
    void theDagCborNodeIsTheSmallMapRoot() {
        assertThat(vector("pdt_node_dagcbor")).isEqualTo(vector("pdt_map_small"));
        assertThat(Cids.of(node("pdt_node_dagcbor"), Cids.DAG_CBOR)).startsWith("bafyrei");
        // Every embedded item is a scalar: no indefinite form and no float anywhere in the node.
        String hex = vector("pdt_node_dagcbor");
        assertThat(hex).doesNotContain("bf").doesNotContain("fa");
    }

    /** The node with a map-valued and a float-valued entry carries CborCodec's forms inside, as §10.2 records. */
    @Test
    void theEmbeddedNodeCarriesAnIndefiniteMapAndAThirtyTwoBitFloat() {
        byte[] bytes = node("pdt_node_embedded");
        Map<String, Object> decoded = NodeCodec.decode(bytes);
        assertThat(decoded.get("k")).isEqualTo(Kind.M);
        assertThat(decoded.get("cnt")).isEqualTo(2L);
        List<?> entries = (List<?>) decoded.get("e");
        assertThat(entries).hasSize(4);
        boolean indefiniteMap = false;
        boolean float32 = false;
        for (int i = 1; i < entries.size(); i += 2) {
            byte[] item = ((Embedded) entries.get(i)).bytes();
            indefiniteMap |= (item[0] & 0xff) == 0xbf && (item[item.length - 1] & 0xff) == 0xff;
            float32 |= (item[0] & 0xff) == 0xfa && item.length == 5;
        }
        assertThat(indefiniteMap).as("the map value is embedded in the indefinite form").isTrue();
        assertThat(float32).as("the float value is embedded as a 32-bit float").isTrue();
    }

    /**
     * The sealed envelope of §3.8 (PD-P-05): an {@code sn} node over the small map root,
     * sealed under the fixed test key at epoch 3, that opens back to {@code pdt_map_small}
     * and refuses another epoch's AAD and another key.
     */
    @Test
    void theSealedNodeVectorOpensToTheSmallMapRootUnderTheTestKey() {
        byte[] sealed = node("pdt_sealed_node");
        byte[] plain = node("pdt_map_small");
        assertThat(ai.badmonkey.agentspaces.pdt.seal.NodeSealer.isSealed(sealed)).isTrue();
        ai.badmonkey.agentspaces.pdt.seal.NodeSealer.Envelope envelope = ai.badmonkey.agentspaces.pdt.seal.NodeSealer.decode(sealed);
        assertThat(envelope.epoch()).isEqualTo(PdtVectors.SEALED_TEST_EPOCH);
        assertThat(envelope.links()).as("the small map root links to nothing").isEmpty();
        byte[] key = ai.badmonkey.agentspaces.pdt.seal.NodeSealer.nodeKey(
                ai.badmonkey.agentspaces.common.crypto.GroupKey.fromBytes(PdtVectors.SEALED_TEST_KEY));
        // the nonce is keyed (HMAC-SHA256 under the node key, PD-P-06): the unkeyed digest prefix
        // of the first build would let a keyless holder confirm a guessed plaintext
        assertThat(envelope.nonce()).isEqualTo(ai.badmonkey.agentspaces.pdt.seal.NodeSealer.nonce(key, plain));
        assertThat(envelope.nonce()).isNotEqualTo(java.util.Arrays.copyOf(
                ai.badmonkey.agentspaces.common.crypto.Digests.sha256(plain), 12));
        assertThat(envelope.ciphertext()).hasSize(plain.length + 16);
        assertThat(ai.badmonkey.agentspaces.pdt.seal.NodeSealer.open(key, envelope)).contains(plain);
        assertThat(ai.badmonkey.agentspaces.pdt.seal.NodeSealer.open(key, new ai.badmonkey.agentspaces.pdt.seal.NodeSealer.Envelope(
                4, envelope.nonce(), envelope.ciphertext(), envelope.links()))).as("another epoch's AAD").isEmpty();
        byte[] other = key.clone();
        other[0] ^= 1;
        assertThat(ai.badmonkey.agentspaces.pdt.seal.NodeSealer.open(other, envelope)).as("another key").isEmpty();
        assertThat(Cids.of(sealed, Cids.DAG_CBOR)).startsWith("bafyrei");
        // NodeCodec reads the envelope as a node of kind sn with no links: what a keyless holder walks
        assertThat(NodeCodec.kindOf(sealed)).isEqualTo(Kind.SN);
        assertThat(NodeCodec.links(sealed)).isEmpty();
    }

    /** Each node vector decodes and re-encodes to the same bytes: one canonical form per node. */
    @Test
    void everyNodeVectorRoundTripsThroughDecodeAndEncode() {
        for (String name : List.of("pdt_map_empty", "pdt_map_small", "pdt_map_deep", "pdt_vector_tail", "pdt_vector_deep",
                "pdt_node_dagcbor", "pdt_node_embedded", "pdt_sealed_node")) {
            byte[] bytes = node(name);
            assertThat(NodeCodec.encode(NodeCodec.decode(bytes))).as(name).isEqualTo(bytes);
        }
    }

    @Test
    void theBlobAndCutVectorsNameRawChunksAndAscendingOffsets() {
        String[] blob = vector("pdt_blob_small").split(",");
        assertThat(blob.length).isGreaterThanOrEqualTo(2);
        for (int i = 1; i < blob.length; i++) {
            assertThat(Cids.parse(blob[i]).codec()).as("chunk " + i).isEqualTo(Cids.RAW);
        }
        if (blob.length == 2) {
            assertThat(blob[0]).as("a one-chunk blob is its chunk").isEqualTo(blob[1]);
        } else {
            assertThat(Cids.parse(blob[0]).codec()).isEqualTo(Cids.DAG_CBOR);
        }

        String[] cuts = vector("pdt_fastcdc_cuts").split(",");
        assertThat(cuts).hasSize(PdtVectors.CUTS_RECORDED);
        long previous = 0;
        for (String cut : cuts) {
            long offset = Long.parseLong(cut);
            assertThat(offset - previous).isBetween((long) ai.badmonkey.agentspaces.pdt.blob.FastCdc.MIN_SIZE,
                    (long) ai.badmonkey.agentspaces.pdt.blob.FastCdc.MAX_SIZE);
            previous = offset;
        }
    }

    @Test
    void theRefVectorsDecodeAsHeads() {
        CborCodec codec = CborCodec.defaultCodec();
        PRef full = codec.fromBytes(node("pdt_ref_cbor"), PRef.class);
        assertThat(full.violation()).isEmpty();
        assertThat(full.name()).isEqualTo("facts");
        assertThat(full.kind()).isEqualTo(PRef.KIND_MAP);
        assertThat(full.root()).isEqualTo(Cids.of(node("pdt_map_deep"), Cids.DAG_CBOR));
        assertThat(full.base()).containsExactly(Cids.of(node("pdt_map_small"), Cids.DAG_CBOR));
        assertThat(full.seq()).isEqualTo(2);
        assertThat(full.size()).isEqualTo(100);
        assertThat(full.nodes()).hasSize(2);
        assertThat(full.nodes().get(0)).isEqualTo(node("pdt_map_deep"));
        assertThat(Cids.of(full.nodes().get(1), Cids.DAG_CBOR))
                .as("the second inline node is the first child of the root")
                .isEqualTo(NodeCodec.links(full.root(), full.nodes().get(0)).get(0));
        assertThat(full.mode()).isEqualTo(PRef.MODE_MULTI);
        assertThat(full.sealing()).isEqualTo(PRef.SEALING_NONE);
        assertThat(full.keyEpoch()).isEqualTo(3L);
        assertThat(full.attrs()).containsExactlyEntriesOf(Map.of("origin", "golden"));
        assertThat(full).isEqualTo(PdtVectors.ref());
        assertThat(codec.toBytes(full)).isEqualTo(node("pdt_ref_cbor"));

        byte[] minBytes = node("pdt_ref_min_cbor");
        PRef min = codec.fromBytes(minBytes, PRef.class);
        assertThat(min.violation()).isEmpty();
        assertThat(min.root()).isEqualTo(Cids.of(node("pdt_map_empty"), Cids.DAG_CBOR));
        assertThat(min.base()).isEmpty();
        assertThat(min.seq()).isEqualTo(1);
        assertThat(min.size()).isZero();
        assertThat(min.nodes()).isNull();
        assertThat(min.keyEpoch()).isNull();
        assertThat(min.attrs()).isNull();
        String text = new String(minBytes, java.nio.charset.StandardCharsets.ISO_8859_1);
        assertThat(text).doesNotContain("nodes").doesNotContain("keyEpoch").doesNotContain("attrs");
        assertThat(min).isEqualTo(PdtVectors.refMin());
        assertThat(codec.toBytes(min)).isEqualTo(minBytes);
    }
}
