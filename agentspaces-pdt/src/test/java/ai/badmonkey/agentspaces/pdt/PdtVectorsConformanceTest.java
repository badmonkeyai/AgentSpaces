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

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.agent.content.SignedPin;
import ai.badmonkey.agentspaces.agent.content.StorePin;
import ai.badmonkey.agentspaces.agent.content.StoreReceipt;
import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.golden.PdtVectors;
import ai.badmonkey.agentspaces.pdt.kinds.BoNode;
import ai.badmonkey.agentspaces.pdt.kinds.DbRoot;
import ai.badmonkey.agentspaces.pdt.kinds.DnNode;
import ai.badmonkey.agentspaces.pdt.kinds.RrRoot;
import ai.badmonkey.agentspaces.pdt.kinds.RvNode;
import ai.badmonkey.agentspaces.pdt.kinds.TextLeaf;
import ai.badmonkey.agentspaces.pdt.kinds.VgNode;
import ai.badmonkey.agentspaces.pdt.values.DatomOrder;
import ai.badmonkey.agentspaces.pdt.values.NodeLoader;
import ai.badmonkey.agentspaces.pdt.values.PDb;
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
                .containsExactlyInAnyOrderElementsOf(fresh.keySet().stream().filter(k -> k.startsWith("pdt_")).toList());
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
        assertThat(full.attrs()).isEqualTo(Map.of("origin", "golden"));
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
    // ------------------------------------------------------------------ PDT 2 phase 0 (srad/agentspaces-pdt2.md §8)

    private String[] parts(String name) {
        return vector(name).split(",");
    }

    private static byte[] hex(String text) {
        return HexFormat.of().parseHex(text);
    }

    private static String cid(byte[] node) {
        return Cids.of(node, Cids.DAG_CBOR);
    }

    /** P2-D-01: the golden database's root links its three index leaves and its tail, each in its index order. */
    @Test
    void theDatabaseVectorsDecodeAsTheGoldenDatabase() {
        String[] root = parts("pdt_db_root");
        assertThat(root).hasSize(3);
        DbRoot db = DbRoot.decode(null, hex(root[0]));
        assertThat(db.encode()).isEqualTo(hex(root[0]));
        assertThat(db.branching()).isEqualTo(DbRoot.BRANCHING);
        assertThat(db.maxEid()).isEqualTo(2);
        assertThat(db.maxTx()).isEqualTo(PdtVectors.GOLDEN_TX2);
        assertThat(db.eavt().cid()).isEqualTo(cid(node("pdt_dn_leaf")));
        assertThat(db.aevt().cid()).isEqualTo(cid(hex(root[1])));
        assertThat(db.avet().cid()).isEqualTo(cid(hex(root[2])));
        assertThat(db.tail().cid()).as("the tail travels linked from the root (§10.4, phase 0)").isEqualTo(cid(node("pdt_dn_tail")));

        DnNode eavt = DnNode.decode(null, node("pdt_dn_leaf"));
        DnNode aevt = DnNode.decode(null, hex(root[1]));
        DnNode avet = DnNode.decode(null, hex(root[2]));
        assertThat(eavt.datoms()).isSortedAccordingTo(java.util.Comparator.comparingLong(Datom::e).thenComparing(Datom::a));
        assertThat(aevt.datoms()).isSortedAccordingTo(java.util.Comparator.comparing(Datom::a).thenComparingLong(Datom::e));
        assertThat(avet.datoms()).extracting(Datom::v).containsExactly("Ada", "Grace");
        assertThat(aevt.datoms()).containsExactlyInAnyOrderElementsOf(eavt.datoms());

        DnNode tail = DnNode.decode(null, node("pdt_dn_tail"));
        assertThat(tail.index()).isEqualTo(DnNode.TAIL);
        assertThat(tail.datoms()).extracting(Datom::tx).containsExactly(-PdtVectors.GOLDEN_TX2, PdtVectors.GOLDEN_TX2);
        for (byte[] bytes : List.of(node("pdt_dn_leaf"), node("pdt_dn_tail"), hex(root[1]), hex(root[2]))) {
            assertThat(DnNode.decode(null, bytes).encode()).isEqualTo(bytes);
        }
    }

    @Test
    void theBranchVectorKeysEachChildByItsLargestDatom() {
        String[] branch = parts("pdt_dn_branch");
        DnNode node = DnNode.decode(null, hex(branch[0]));
        assertThat(node.level()).isEqualTo(1);
        assertThat(node.children()).extracting(Link::cid).containsExactly(cid(hex(branch[1])), cid(hex(branch[2])));
        for (int i = 0; i < 2; i++) {
            List<Datom> child = DnNode.decode(null, hex(branch[i + 1])).datoms();
            assertThat(node.datoms().get(i)).isEqualTo(child.get(child.size() - 1));
        }
        assertThat(node.encode()).isEqualTo(hex(branch[0]));
    }

    @Test
    void theDatomValuesVectorHoldsOneDatomPerValueForm() {
        DnNode leaf = DnNode.decode(null, node("pdt_datom_values"));
        assertThat(leaf.encode()).isEqualTo(node("pdt_datom_values"));
        java.util.Set<String> forms = new java.util.TreeSet<>();
        for (Datom datom : leaf.datoms()) {
            Object v = datom.v();
            forms.add(v instanceof Datom.Tagged tagged ? "tag " + tagged.tag()
                    : v instanceof String text ? (text.startsWith(":") ? "keyword" : "text") : v.getClass().getSimpleName());
        }
        assertThat(forms).containsExactlyInAnyOrder("Boolean", "Double", "Link", "Long", "byte[]", "keyword", "tag i",
                "tag n", "tag u", "text");
        assertThat(NodeCodec.links(node("pdt_datom_values"))).hasSize(1);
        assertThat(leaf.datoms().get(leaf.datoms().size() - 1).tx()).as("a retraction").isNegative();
    }

    /** P2-S-02: the pin and the receipt read strictly from their own bytes and keep them. */
    @Test
    void theStoreRecordVectorsReadStrictlyAndReEncode() {
        CborCodec codec = CborCodec.defaultCodec();
        StorePin pin = codec.fromDocument(node("pdt_store_pin_cbor"), StorePin.class);
        assertThat(pin.violation()).isEmpty();
        assertThat(pin.root()).isEqualTo(Cids.of(node("pdt_map_deep"), Cids.DAG_CBOR));
        assertThat(pin.haves()).containsExactly(Cids.of(node("pdt_map_small"), Cids.DAG_CBOR));
        assertThat(codec.toBytes(pin)).isEqualTo(node("pdt_store_pin_cbor"));
        StoreReceipt receipt = codec.fromDocument(node("pdt_store_receipt_cbor"), StoreReceipt.class);
        assertThat(receipt.violation()).isEmpty();
        assertThat(receipt.root()).isEqualTo(pin.root());
        assertThat(receipt.holder()).isNotEqualTo(pin.requester());
        assertThat(codec.toBytes(receipt)).isEqualTo(node("pdt_store_receipt_cbor"));
    }

    /**
     * PDT 2 follow-ups, 2026-11-09, F1: the signed pin reads strictly, keeps its bytes, carries its requester's
     * consent for its group alone, and {@code pdt_signed_pin_consent_cbor} is the consent the requester signed. Each
     * is one hex string, as every {@code _cbor} vector is, so every port's round trip of the {@code _cbor} vectors
     * reads both.
     */
    @Test
    void theSignedPinVectorCarriesItsRequestersConsent() {
        CborCodec codec = CborCodec.defaultCodec();
        byte[] bytes = HexFormat.of().parseHex(vector("pdt_signed_pin_cbor"));
        SignedPin signed = codec.fromWire(bytes, SignedPin.class);
        assertThat(signed.violation()).isEmpty();
        assertThat(codec.toBytes(signed)).isEqualTo(bytes);
        assertThat(signed.consented(PdtVectors.SIGNED_PIN_GROUP)).isTrue();
        assertThat(signed.consented(ai.badmonkey.agentspaces.common.id.GroupId.of("zOther"))).isFalse();
        assertThat(signed.pin().requester()).isEqualTo(PdtVectors.signedPinRequester().peerId());
        assertThat(signed.pin().root()).isEqualTo(Cids.of(node("pdt_map_deep"), Cids.DAG_CBOR));
        assertThat(signed.left()).isEqualTo(signed.pin().copies()).isEqualTo(2);
        assertThat(SignedPin.consentBytes(signed.pin(), PdtVectors.SIGNED_PIN_GROUP, signed.notAfter()))
                .isEqualTo(HexFormat.of().parseHex(vector("pdt_signed_pin_consent_cbor")));
        assertThat(codec.toBytes(signed.forward().forward()).length).as("a forward changes left alone")
                .isEqualTo(bytes.length);
    }

    /**
     * PDT 2 follow-ups, 2026-11-09, F3 (P2-X-01): the tree manifest of the small blob carries no piece table, since
     * its root names every piece, reads strictly, keeps its bytes, and passes every manifest rule.
     */
    @Test
    void theTreeManifestVectorNamesTheSmallBlobWithoutAPieceTable() {
        CborCodec codec = CborCodec.defaultCodec();
        byte[] bytes = node("pdt_manifest_tree_cbor");
        Manifest manifest = codec.fromWire(bytes, Manifest.class);
        assertThat(manifest.violation()).isEmpty();
        assertThat(manifest.treeLayout()).isTrue();
        assertThat(manifest.table()).isNull();
        assertThat(manifest.tableRef()).isNull();
        assertThat(manifest.root()).isEqualTo(vector("pdt_blob_small").split(",")[0]);
        assertThat(Cids.parse(manifest.root()).codec()).isEqualTo(Cids.DAG_CBOR);
        assertThat(manifest.size()).isEqualTo(PdtVectors.SMALL_BLOB_BYTES);
        assertThat(codec.toBytes(manifest)).isEqualTo(bytes);
        assertThat(manifest.withLayout(null).violation()).contains("exactly one of table and tableRef must be present");
    }

    /** P2-B-02: the outboard vectors hash to their identities through their root parents. */
    @Test
    void theOutboardVectorsReachTheirIdentities() {
        assertThat(vector("pdt_blake3_identity")).startsWith("bafkr4i");
        String[] small = parts("pdt_bo_small");
        BoNode smallNode = BoNode.decode(null, hex(small[0]));
        assertThat(smallNode.size()).isEqualTo(PdtVectors.SMALL_BLOB_BYTES);
        assertThat(smallNode.children()).isEmpty();
        assertThat(rootHash(smallNode)).isEqualTo(identityDigest(small[1]));
        assertThat(smallNode.encode()).isEqualTo(hex(small[0]));

        String[] split = parts("pdt_bo_split");
        assertThat(split).hasSize(4);
        BoNode splitNode = BoNode.decode(null, hex(split[0]));
        assertThat(splitNode.size()).isEqualTo(PdtVectors.SPLIT_STREAM_BYTES);
        assertThat(splitNode.children()).extracting(Link::cid).containsExactly(split[2], split[3]);
        assertThat(splitNode.parents()).as("two children, one parent above the cut").hasSize(BoNode.PARENT_BYTES);
        assertThat(rootHash(splitNode)).isEqualTo(identityDigest(split[1]));
    }

    private static byte[] rootHash(BoNode node) {
        byte[] parents = node.parents();
        return ai.badmonkey.agentspaces.pdt.blob.Blake3.parent(java.util.Arrays.copyOfRange(parents, 0, 32),
                java.util.Arrays.copyOfRange(parents, 32, 64), true);
    }

    private static byte[] identityDigest(String identity) {
        byte[] binary = ai.badmonkey.agentspaces.common.codec.Multibase.decode(identity);
        assertThat(java.util.Arrays.copyOf(binary, 4)).isEqualTo(new byte[]{0x01, 0x55, 0x1e, 0x20});
        return java.util.Arrays.copyOfRange(binary, 4, binary.length);
    }

    /** P2-R-01: the relaxed tree indexes through its size table to the elements of the packed vector. */
    @Test
    void theRelaxedVectorHoldsTheElementsOfThePackedOne() {
        String[] relaxed = parts("pdt_rr_relaxed");
        RrRoot root = RrRoot.decode(null, hex(relaxed[0]));
        RvNode interior = RvNode.decode(null, hex(relaxed[1]));
        assertThat(root.trie().cid()).isEqualTo(cid(hex(relaxed[1])));
        assertThat(interior.sizes()).containsExactly(32L, 40L, 72L, 96L);
        List<Embedded> elements = new java.util.ArrayList<>();
        long from = 0;
        for (int i = 0; i < interior.children().size(); i++) {
            long to = interior.sizes().get(i);
            byte[] leaf = PdtVectors.rrLeaf((int) from, (int) to);
            assertThat(interior.children().get(i).cid()).isEqualTo(cid(leaf));
            for (Object element : (List<?>) NodeCodec.decode(leaf).get("e")) {
                elements.add((Embedded) element);
            }
            from = to;
        }
        elements.addAll(root.tail());
        assertThat(elements).hasSize((int) root.count());
        for (int i = 0; i < elements.size(); i++) {
            assertThat(CborCodec.defaultCodec().fromBytes(elements.get(i).bytes(), Integer.class)).isEqualTo(i);
        }
        Map<String, Object> packed = NodeCodec.decode(node("pdt_rr_packed"));
        assertThat(packed.get("k")).as("the packed form is the vr root (§3.7)").isEqualTo(Kind.VR);
        assertThat(packed.get("n")).isEqualTo((long) PdtVectors.RR_ELEMENTS);
        assertThat(root.encode()).isEqualTo(hex(relaxed[0]));
        assertThat(interior.encode()).isEqualTo(hex(relaxed[1]));
    }

    /** P2-T-01: the text vectors read in Fugue order and merge without interleaving. */
    @Test
    void theTextVectorsReadAndMergeInFugueOrder() {
        TextLeaf runs = TextLeaf.decode(null, node("pdt_text_runs"));
        assertThat(runs.text()).isEqualTo("Ab\ud83d\ude00c\u00e9!");
        assertThat(runs.encode()).isEqualTo(node("pdt_text_runs"));
        String[] merge = parts("pdt_text_merge");
        TextLeaf alice = TextLeaf.decode(null, hex(merge[0]));
        TextLeaf bob = TextLeaf.decode(null, hex(merge[1]));
        TextLeaf merged = TextLeaf.decode(null, hex(merge[2]));
        assertThat(alice.union(bob)).isEqualTo(merged);
        assertThat(bob.union(alice)).isEqualTo(merged);
        assertThat(merged.text()).isEqualTo(merge[3]).isEqualTo("bXYc");
    }

    /** P2-V-01: the chain links the deep version's vg node to its base's, and the head names the first. */
    @Test
    void theVersionGraphVectorsChainAndTheHeadNamesItsNode() {
        String[] chain = parts("pdt_vg_chain");
        VgNode deep = VgNode.decode(null, hex(chain[0]));
        VgNode small = VgNode.decode(null, hex(chain[1]));
        assertThat(deep.root()).isEqualTo(cid(node("pdt_map_deep")));
        assertThat(deep.bases()).containsExactly(cid(node("pdt_map_small")));
        assertThat(deep.previous()).extracting(Link::cid).containsExactly(cid(hex(chain[1])));
        assertThat(small.root()).isEqualTo(cid(node("pdt_map_small")));
        assertThat(small.previous()).isEmpty();
        assertThat(NodeCodec.links(hex(chain[0]))).as("a vg node links no root").containsExactly(cid(hex(chain[1])));

        byte[] headBytes = node("pdt_ref_ancestry_cbor");
        PRef head = CborCodec.defaultCodec().fromDocument(headBytes, PRef.class);
        assertThat(head.v()).as("ancestry needs no new record version (rule 9)").isEqualTo(1);
        assertThat(head.violation()).isEmpty();
        assertThat(ai.badmonkey.agentspaces.pdt.refs.Ancestry.read(head, hex(chain[0]))).contains(deep);
        assertThat(CborCodec.defaultCodec().toBytes(head)).isEqualTo(headBytes);
    }

    /** PDT 2 phase 1, P2-V-01: a merge version's vg node links its bases' nodes in the order of its bases. */
    @Test
    void theMergeVersionLinksEachBaseNodeInTheOrderOfItsBases() {
        String[] chain = parts("pdt_vg_chain");
        byte[] bytes = hex(vector("pdt_vg_merge"));
        VgNode merge = VgNode.decode(null, bytes);
        assertThat(merge).isEqualTo(ai.badmonkey.agentspaces.pdt.golden.PdtVectors.vgMerge());
        assertThat(merge.root()).isEqualTo(cid(node("pdt_map_empty")));
        assertThat(merge.bases()).containsExactly(cid(node("pdt_map_deep")), cid(node("pdt_map_small")));
        assertThat(merge.previous()).extracting(Link::cid).containsExactly(cid(hex(chain[0])), cid(hex(chain[1])));
        assertThat(NodeCodec.encode(NodeCodec.decode(bytes))).isEqualTo(bytes);
    }

    /** Every PDT 2 node vector decodes and re-encodes through NodeCodec to its own bytes. */
    @Test
    void everyPdt2NodeVectorRoundTripsThroughNodeCodec() {
        List<String> nodes = new java.util.ArrayList<>(List.of(vector("pdt_dn_leaf"), vector("pdt_dn_tail"),
                vector("pdt_datom_values"), vector("pdt_text_runs"), vector("pdt_rr_packed")));
        for (String name : List.of("pdt_db_root", "pdt_dn_branch", "pdt_rr_relaxed", "pdt_vg_chain")) {
            nodes.addAll(List.of(parts(name)));
        }
        nodes.add(parts("pdt_bo_small")[0]);
        nodes.add(parts("pdt_bo_split")[0]);
        nodes.addAll(List.of(parts("pdt_text_merge")).subList(0, 3));
        for (String node : nodes) {
            assertThat(NodeCodec.encode(NodeCodec.decode(hex(node)))).isEqualTo(hex(node));
        }
    }

    /** PDT 2 phase 2, P2-D-03: the version 2 head of a db ref reads strictly and passes every bound. */
    @Test
    void theVersionTwoHeadNamesTheGoldenDatabase() {
        byte[] bytes = node("pdt_ref_v2_cbor");
        PRef head = CborCodec.defaultCodec().fromWire(bytes, PRef.class);
        assertThat(head.v()).isEqualTo(PRef.VERSION_2);
        assertThat(head.kind()).isEqualTo(PRef.KIND_DB);
        assertThat(head.violation()).isEmpty();
        assertThat(head.root()).isEqualTo(cid(hex(parts("pdt_db_root")[0])));
        assertThat(CborCodec.defaultCodec().toBytes(head)).isEqualTo(bytes);
    }

    /**
     * PDT 2 phase 2, P2-D-09: the sample database reads back node by node, its indexes hold the 1,000
     * folded datoms, and every answer is the leaf of the matching datoms of the database with its tail applied.
     */
    @Test
    void theSampleDatabaseReadsBackAndEveryAnswerFollowsItsIndex() {
        String[] sample = parts("pdt_db_sample");
        NodeStore store = NodeStore.inMemory();
        for (int i = 1; i < sample.length; i++) {
            store.put(hex(sample[i]), Cids.DAG_CBOR);
        }
        assertThat(store.complete(sample[0])).as("the vector holds every node the root reaches").isTrue();
        PDb db = PDb.load(store, NodeLoader.of(store), sample[0]);
        assertThat(db.node().maxEid()).isEqualTo(126);
        assertThat(db.index(DnNode.EAVT).count()).isEqualTo(1000);
        assertThat(db.index(DnNode.AVET).count()).as("email, two friends, and name per entity").isEqualTo(500);
        assertThat(db.tail()).hasSize(3);
        List<Datom> live = db.changesSince(PDb.empty(store, NodeLoader.of(store))).added();
        assertThat(live).hasSize(1001);
        Map<String, java.util.function.Predicate<Datom>> queries = new java.util.LinkedHashMap<>();
        queries.put("eavt_7", d -> d.e() == 7);
        queries.put("eavt_126", d -> d.e() == 126);
        queries.put("aevt_name", d -> d.a().equals(":p/name"));
        queries.put("avet_name_ada", d -> d.a().equals(":p/name") && d.v().equals("Ada"));
        queries.put("avet_friend_9", d -> d.a().equals(":p/friend") && d.v().equals(9L));
        queries.put("entity_42", d -> d.e() == 42);
        queries.put("lookup_email_p42", d -> d.a().equals(":p/email") && d.v().equals("p42@example.org"));
        queries.put("avet_name_person7", d -> d.a().equals(":p/name") && d.v().equals("Person 7"));
        Map<String, DnNode> answers = new java.util.LinkedHashMap<>();
        for (String pair : vector("pdt_db_sample_answers").split(",")) {
            String[] kv = pair.split("=", 2);
            answers.put(kv[0], DnNode.decode(null, hex(kv[1])));
        }
        assertThat(answers.keySet()).containsExactlyElementsOf(queries.keySet());
        for (Map.Entry<String, java.util.function.Predicate<Datom>> q : queries.entrySet()) {
            DnNode answer = answers.get(q.getKey());
            List<Datom> expected = new java.util.ArrayList<>(live.stream().filter(q.getValue()).toList());
            expected.sort(DatomOrder.of(answer.index()));
            assertThat(answer.datoms()).as(q.getKey()).isEqualTo(expected);
        }
        assertThat(answers.get("eavt_7").datoms()).extracting(Datom::v).contains("Ada").doesNotContain("Person 7");
        assertThat(answers.get("avet_friend_9").datoms()).extracting(Datom::e).containsExactly(7L, 8L);
        assertThat(answers.get("lookup_email_p42").datoms()).extracting(Datom::e).containsExactly(42L);
        assertThat(answers.get("avet_name_person7").datoms()).isEmpty();
    }
}
