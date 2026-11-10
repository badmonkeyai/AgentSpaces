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
package ai.badmonkey.agentspaces.pdt.golden;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.agent.content.SignedPin;
import ai.badmonkey.agentspaces.agent.content.StorePin;
import ai.badmonkey.agentspaces.agent.content.StoreReceipt;
import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.Datom;
import ai.badmonkey.agentspaces.pdt.Embedded;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.TextRun;
import ai.badmonkey.agentspaces.pdt.blob.BaoOutboard;
import ai.badmonkey.agentspaces.pdt.blob.Blake3;
import ai.badmonkey.agentspaces.pdt.blob.BlobTree;
import ai.badmonkey.agentspaces.pdt.blob.FastCdc;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.kinds.BoNode;
import ai.badmonkey.agentspaces.pdt.kinds.DbRoot;
import ai.badmonkey.agentspaces.pdt.kinds.DnNode;
import ai.badmonkey.agentspaces.pdt.kinds.RrRoot;
import ai.badmonkey.agentspaces.pdt.kinds.RvNode;
import ai.badmonkey.agentspaces.pdt.kinds.TextLeaf;
import ai.badmonkey.agentspaces.pdt.kinds.VgNode;
import ai.badmonkey.agentspaces.pdt.refs.Ancestry;
import ai.badmonkey.agentspaces.pdt.seal.NodeSealer;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.DatomOrder;
import ai.badmonkey.agentspaces.pdt.values.Editor;
import ai.badmonkey.agentspaces.pdt.values.NodeLoader;
import ai.badmonkey.agentspaces.pdt.values.PDb;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.pdt.values.PSortedMap;
import ai.badmonkey.agentspaces.pdt.values.PVector;
import ai.badmonkey.agentspaces.pdt.values.VectorEditor;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * The golden vectors of srad/agentspaces-pdt.md PD-N-07 that phase 1 can produce:
 * the root node bytes of fixed maps and vectors, the chunk layout of a seeded blob,
 * and the cut points of the seeded 8 MiB stream. {@code tools/golden/GoldenVectors}
 * calls {@link #vectors()} by reflection and writes each entry as a field of
 * {@code golden.json}; {@code PdtVectorsConformanceTest} recomputes every entry.
 * The Python and TypeScript ports read the same fields.
 *
 * <p>Inputs, so another language can reproduce them:
 * <ul>
 *   <li>{@code pdt_map_empty}: the root node of the empty map (hex).</li>
 *   <li>{@code pdt_map_small}: the root node of {@code {"a": 1, "b": "two"}} with text
 *       keys and the values as canonical CBOR items (hex).</li>
 *   <li>{@code pdt_map_deep}: the root node of the map {@code "k0".."k99"} to the
 *       unsigned integer {@code 0..99}, which has child nodes (hex).</li>
 *   <li>{@code pdt_vector_tail}: the root node of the vector {@code [0..9]} (hex); every
 *       element sits in the tail.</li>
 *   <li>{@code pdt_vector_deep}: the root node of the vector {@code [0..1099]} (hex), a
 *       two-level trie plus the tail.</li>
 *   <li>{@code pdt_blob_small}: the root CID of a 100 KiB stream from
 *       {@code java.util.Random(42).nextBytes}, then the chunk CIDs in order, comma
 *       separated.</li>
 *   <li>{@code pdt_fastcdc_cuts}: the first 20 cut points (chunk end offsets, decimal,
 *       comma separated) of the 8 MiB stream from {@code java.util.Random(42).nextBytes}.</li>
 *   <li>{@code pdt_node_dagcbor}: the bytes of {@code pdt_map_small}'s root node, the node
 *       the IPLD DAG-CBOR reference decoder must accept (PD-N-03): every embedded item is
 *       a scalar.</li>
 *   <li>{@code pdt_node_embedded}: the root node of {@code {"rec": {"x": 1, "y": "two"},
 *       "f": 1.5f}} (hex), a node with a map-valued and a float-valued entry. Its own
 *       layer is strict DAG-CBOR and any RFC 8949 decoder reads it, but the embedded map
 *       keeps {@code CborCodec}'s indefinite-length form ({@code bf..ff}) and the float
 *       its 32-bit form ({@code fa}), so a strict DAG-CBOR decoder refuses it; the
 *       TypeScript check of PD-N-03 records both outcomes (§10.2).</li>
 *   <li>{@code pdt_ref_cbor}: the canonical CBOR of a {@code PRef} head (§3.6, PD-R-01)
 *       named {@code facts}, kind {@code map}, root = the root CID of {@code pdt_map_deep},
 *       base = [the root CID of {@code pdt_map_small}], seq 2, size 100, two inline nodes
 *       (the {@code pdt_map_deep} root node and its first child), mode {@code multi},
 *       sealing {@code none}, keyEpoch 3, attrs {@code {"origin": "golden"}} (hex).</li>
 *   <li>{@code pdt_ref_min_cbor}: the same record type with no optional field: name
 *       {@code facts}, kind {@code map}, root = the root CID of {@code pdt_map_empty}, no
 *       base, seq 1, size 0, no nodes, mode {@code multi}, sealing {@code none}, no
 *       keyEpoch, no attrs (hex); the three optional fields are absent from the map.</li>
 *   <li>{@code pdt_sealed_node}: the sealed envelope of {@code pdt_map_small}'s root node
 *       in the sealed-nodes mode of §3.8 (PD-P-05) under the fixed test content key
 *       {@link #SEALED_TEST_KEY} (the bytes {@code 0x00..0x1f}) at epoch {@link #SEALED_TEST_EPOCH}
 *       (3): per-epoch key = HKDF-SHA256(key, salt empty, info {@code aspace:pdt:node}), nonce =
 *       the first 12 bytes of HMAC-SHA256(per-epoch key, plaintext), AAD {@code aspace:pdt:node|3},
 *       AES-256-GCM-SIV, and the {@code sn} node {@code {k, ct, ep, ln, nc}} in canonical order
 *       (hex). Its CID is {@code Cids.of(envelope, DAG_CBOR)}; the plaintext has no links, so
 *       {@code ln} is empty. The name carries no {@code _cbor} suffix because the envelope is a
 *       definite-length DAG-CBOR node, which the wire-form round-trip loops of the clients
 *       (every {@code *_cbor} key) would re-encode in the indefinite Jackson form.</li>
 * </ul>
 * {@code pdt_map_collision} is omitted: an {@code mc} node needs two keys whose sha2-256
 * digests agree on all 256 bits, which no deterministic input produces; the
 * {@code mc} layout is covered by {@code ChampDepthTest} with a substitute hasher.
 *
 * <p>PDT 2 phase 0 adds the vectors of P2-D-01, P2-S-02, P2-B-02, P2-R-01, P2-T-01, and P2-V-01
 * (srad/agentspaces-pdt2.md §8), listed by {@link #pdt2Vectors()} and added to {@code golden.json}
 * by {@code tools/golden/Pdt2GoldenVectors.java} without touching any other entry. Their inputs:
 * <ul>
 *   <li>The golden database (P2-D-01): schema {@code {":person/name" {":db/index" true}}} as an
 *       embedded {@code CborCodec} map; transaction {@code 536870913} (DataScript's tx0 plus one)
 *       asserts {@code [1 :person/age 36]}, {@code [1 :person/name "Ada"]}, {@code [2 :person/age 45]},
 *       {@code [2 :person/name "Grace"]}; transaction {@code 536870914} retracts {@code [2 :person/age 45]}
 *       (transaction {@code -536870914}) and asserts {@code [2 :person/age 46]}, and stays unfolded in
 *       the tail. Max-eid 2, max-tx 536870914, branching factor 128. Attributes are text in their
 *       keyword print form.</li>
 *   <li>{@code pdt_dn_leaf}: the EAVT leaf of the golden database (hex): the four datoms of the
 *       first transaction in EAVT order.</li>
 *   <li>{@code pdt_dn_tail}: the tail of the golden database (hex): the retraction, then the
 *       assertion.</li>
 *   <li>{@code pdt_db_root}: the {@code db} root of the golden database (hex), then the AEVT leaf (the
 *       same datoms in AEVT order) and the AVET leaf (the two name datoms, the one indexed attribute),
 *       comma separated; its EAVT link names {@code pdt_dn_leaf} and its tail link {@code pdt_dn_tail}.</li>
 *   <li>{@code pdt_dn_branch}: an EAVT branch at level 1 over two leaves (hex), then the two leaves,
 *       comma separated: entity 1's two datoms and entity 2's two datoms of the first transaction; the
 *       branch keys are each leaf's largest datom.</li>
 *   <li>{@code pdt_datom_values}: an EAVT leaf (hex) of entity 3 at transaction 536870913 with one
 *       datom per value form, attributes {@code :v/bigint} (2^70), {@code :v/bool} (true),
 *       {@code :v/bytes} (00 01 02), {@code :v/float} (1.5), {@code :v/instant} (1792320000000),
 *       {@code :v/int} (-7), {@code :v/keyword} ({@code :status/active}), {@code :v/ref} (2),
 *       {@code :v/spill} (a link to the raw chunk of 5,000 zero bytes), {@code :v/text}
 *       ({@code "Ada ✓"}), {@code :v/uuid} (0123456789abcdef fedcba9876543210), the last one a
 *       retraction at transaction -536870914.</li>
 *   <li>{@code pdt_store_pin_cbor}: the canonical CBOR of a {@code StorePin} (hex): root = the root
 *       CID of {@code pdt_map_deep}, kind {@code tree}, lease 86400, copies 2, requester = the peer id
 *       of the Ed25519 public key {@code 0x00..0x1f}, haves = [the root CID of {@code pdt_map_small}],
 *       attrs {@code {"purpose": "golden"}}.</li>
 *   <li>{@code pdt_store_receipt_cbor}: the canonical CBOR of a {@code StoreReceipt} (hex) for the
 *       same root: holder = the peer id of the public key {@code 0x20..0x3f}, bytes 12345, until
 *       1792406400000, blind true.</li>
 *   <li>{@code pdt_blake3_identity}: the BLAKE3 identity CID of the 8 MiB stream of
 *       {@code pdt_fastcdc_cuts}.</li>
 *   <li>{@code pdt_bo_small}: the {@code bo} node (hex) of the 100 KiB stream of {@code pdt_blob_small}
 *       (seven groups, six parents), then its BLAKE3 identity CID, comma separated.</li>
 *   <li>{@code pdt_bo_split}: the root {@code bo} node (hex) of the first 2^23 + 5000 bytes of the
 *       {@code java.util.Random(42).nextBytes} stream (2^9 + 1 groups: a full leaf of 32,728 bytes and a
 *       leaf of one group; PDT 2 phase 4 measures, P2-Q-PRF-05), then its BLAKE3 identity CID, then the
 *       CID of each child, comma separated.</li>
 *   <li>{@code pdt_rr_relaxed}: an {@code rr} root (hex) then its one {@code rv} node (hex), comma
 *       separated, for the elements 0..99 as embedded {@code CborCodec} integers: the {@code rv} node
 *       links four {@code v} leaves holding 0..31, 32..39, 40..71, and 72..95, with sizes
 *       [32, 40, 72, 96], and the tail holds 96..99.</li>
 *   <li>{@code pdt_rr_packed}: the root node (hex) of the same elements packed, which is the
 *       {@code vr} root of the vector 0..99 (§3.7).</li>
 *   <li>{@code pdt_text_runs}: a {@code t} node (hex) with runs {@code [alice 0 null R 4 false "Ab😀c"]},
 *       {@code [alice 4 [alice 1] L 2 true ""]}, {@code [bob 0 [alice 3] R 2 false "é!"]}, whose text
 *       reads {@code "Ab😀cé!"}.</li>
 *   <li>{@code pdt_text_merge}: alice's leaf, bob's leaf, and their union (hex each), then the merged
 *       text, comma separated. From {@code [alice 0 null R 2 false "ac"]}, alice inserts {@code b}
 *       between a and c ({@code [alice 2 [alice 1] L 1 false "b"]}) while bob inserts {@code XY} there
 *       ({@code [bob 0 [alice 1] L 2 false "XY"]}) and deletes {@code a}; the union reads
 *       {@code "bXYc"}.</li>
 *   <li>{@code pdt_vg_chain}: the {@code vg} node (hex) of the {@code pdt_map_deep} version, then the
 *       {@code vg} node of its base, the {@code pdt_map_small} version, comma separated: issuer = the
 *       peer id of {@code pdt_store_pin_cbor}'s requester plus {@code /writer}, stamps
 *       {@code 1792320000001:0:golden} and {@code 1792320000000:0:golden}.</li>
 *   <li>{@code pdt_ref_ancestry_cbor}: the canonical CBOR of a {@code v} 1 head (hex) named
 *       {@code facts}, kind {@code map}, root = {@code pdt_map_deep}'s root, base = [{@code pdt_map_small}'s
 *       root], seq 2, size 100, mode {@code multi}, sealing {@code none}, attrs
 *       {@code {"pdt.ancestry": the CID of the first node of pdt_vg_chain}}.</li>
 *   <li>{@code pdt_vg_merge} (PDT 2 phase 1, P2-V-01): the {@code vg} node (hex) of the
 *       {@code pdt_map_empty} version with bases [{@code pdt_map_deep}'s root, {@code pdt_map_small}'s
 *       root] and links to their {@code pdt_vg_chain} nodes in the same order, issuer as above, stamp
 *       {@code 1792320000002:0:golden}: a writer links, in the order of the bases, the {@code vg} node
 *       of each base it has one for.</li>
 *   <li>{@code pdt_ref_v2_cbor} (PDT 2 phase 2, P2-D-03, rule 8 of §6.10.3): the canonical CBOR of a
 *       {@code v} 2 head (hex) named {@code graph}, kind {@code db}, root = the {@code db} root of
 *       {@code pdt_db_root}, no base, seq 1, size 2 (its transactions since DataScript's tx0), mode
 *       {@code linear}, sealing {@code none}, no optional field.</li>
 *   <li>{@code pdt_db_sample} (PDT 2 phase 2, P2-D-09): the root CID of the sample database, then every
 *       node it reaches (hex), the root first and each node before its children, links in node order,
 *       comma separated. Schema {@code {":p/email" {":db/unique" ":db.unique/identity"}, ":p/friend"
 *       {":db/cardinality" ":db.cardinality/many", ":db/valueType" ":db.type/ref"}, ":p/name"
 *       {":db/index" true}, ":p/tag" {":db/cardinality" ":db.cardinality/many"}}}. Transaction
 *       {@code 536870913} gives each entity e of 1..125 eight datoms, 1,000 in all, folded into the
 *       three indexes at branching factor 128 by {@code PSortedMap.of}: {@code :p/active} (e % 3 == 0),
 *       {@code :p/age} (20 + 7e % 50), {@code :p/email} ("p" e "@example.org"), {@code :p/friend} twice
 *       (1 + e % 125 and 1 + (e + 1) % 125), {@code :p/name} ("Person " e), {@code :p/score} (e / 2.0),
 *       {@code :p/tag} ({@code :tag/even} or {@code :tag/odd}). AVET holds the indexed attributes
 *       {@code :p/email}, {@code :p/friend}, and {@code :p/name}, as DataScript indexes a unique, a ref,
 *       and a {@code :db/index} attribute. Transaction {@code 536870914} stays in the tail: it retracts
 *       {@code [7 :p/name "Person 7"]} and asserts {@code [7 :p/name "Ada"]} and
 *       {@code [126 :p/name "Newcomer"]}. Max-eid 126, max-tx 536870914.</li>
 *   <li>{@code pdt_db_sample_answers} (P2-D-09): {@code name=hex} pairs, comma separated, each hex a
 *       {@code dn} leaf of the index named holding the answer's datoms in that index's order, over the
 *       sample with its tail applied: {@code eavt_7} = {@code datoms(eavt, 7)}; {@code eavt_126} =
 *       {@code datoms(eavt, 126)}; {@code aevt_name} = {@code datoms(aevt, :p/name)};
 *       {@code avet_name_ada} = {@code datoms(avet, :p/name, "Ada")}; {@code avet_friend_9} =
 *       {@code datoms(avet, :p/friend, 9)}, the entities that name 9; {@code entity_42} =
 *       {@code entity(42)}, its datoms in EAVT order; {@code lookup_email_p42} =
 *       {@code lookup(:p/email, "p42@example.org")}, the AVET datom whose entity it returns;
 *       {@code avet_name_person7} = {@code datoms(avet, :p/name, "Person 7")}, empty since the tail
 *       retracted it.</li>
 *   <li>{@code pdt_signed_pin_cbor} (PDT 2 follow-ups, 2026-11-09, F1): the canonical CBOR of a
 *       {@code SignedPin} (hex). The requester is the peer of the Ed25519 key whose 32-byte seed counts {@code 0x40..0x5f};
 *       the pin's terms are root = the root CID of {@code pdt_map_deep}, kind {@code tree}, lease 86400,
 *       copies 2, haves = [the root CID of {@code pdt_map_small}], attrs {@code {"pdt.stores": the holder of
 *       pdt_store_receipt_cbor}}; group {@code zGolden}, notAfter 1792323600000, left 2. The consent is
 *       {@code {purpose: "agentspaces.store-pin", group, pin, notAfter}} in that order, and the signature is
 *       RFC 8032 Ed25519 over it.</li>
 *   <li>{@code pdt_signed_pin_consent_cbor} (F1): the canonical CBOR of the consent the requester of
 *       {@code pdt_signed_pin_cbor} signed (hex), so a port checks its signing input apart from the record.</li>
 *   <li>{@code pdt_manifest_tree_cbor} (PDT 2 follow-ups, 2026-11-09, F3; P2-X-01): the canonical CBOR of a
 *       {@code layout: tree} {@code Manifest} (hex) without a piece table: root = the root CID of
 *       {@code pdt_blob_small}, size 102400, piece size 65536, 2 pieces, table and tableRef null, name
 *       {@code blob-small.bin}, media type {@code application/octet-stream}, sealing {@code none}, hashing
 *       {@code plain}, origin the requester of {@code pdt_store_pin_cbor}, created 1792320000000, no attrs.</li>
 *   <li>{@code pdt_rr_splice}, {@code pdt_text_edits}, {@code pdt_text_value}, {@code pdt_text_spill}, and
 *       {@code pdt_text_replica} (PDT 2 phase 6): see {@link SequenceVectors}.</li>
 *   <li>{@code pdt_dcz_frame} and {@code content_nodes_deltas_cbor} (PDT 2 phase 4): see {@link BlobVectors}.</li>
 *   <li>{@code pdt_push_bundle_cbor} and {@code pdt_riblt_symbols} (PDT 2 phase 5): see {@link PushVectors}.</li>
 * </ul>
 */
public final class PdtVectors {

    /** The seed of every stream the vectors read. */
    public static final long SEED = 42;

    /** The size of the {@code pdt_blob_small} stream. */
    public static final int SMALL_BLOB_BYTES = 100 * 1024;

    /** The size of the {@code pdt_fastcdc_cuts} stream. */
    public static final int CUTS_STREAM_BYTES = 8 * 1024 * 1024;

    /** How many cut points {@code pdt_fastcdc_cuts} records. */
    public static final int CUTS_RECORDED = 20;

    /** The fixed content key of {@code pdt_sealed_node}: the bytes {@code 0x00..0x1f}. */
    public static final byte[] SEALED_TEST_KEY = sealedTestKey();

    /** The epoch of {@code pdt_sealed_node}. */
    public static final long SEALED_TEST_EPOCH = 3;

    private PdtVectors() {
    }

    /** The vectors in emission order, each value hex or the text its name promises. */
    public static Map<String, String> vectors() {
        Map<String, String> out = new LinkedHashMap<>();
        out.put("pdt_map_empty", hex(mapEmpty()));
        byte[] small = mapSmall();
        out.put("pdt_map_small", hex(small));
        out.put("pdt_map_deep", hex(mapDeep()));
        out.put("pdt_vector_tail", hex(vectorTail()));
        out.put("pdt_vector_deep", hex(vectorDeep()));
        out.put("pdt_blob_small", blobSmall());
        out.put("pdt_fastcdc_cuts", fastCdcCuts());
        out.put("pdt_node_dagcbor", hex(small));
        out.put("pdt_node_embedded", hex(nodeEmbedded()));
        out.put("pdt_ref_cbor", hex(refCbor()));
        out.put("pdt_ref_min_cbor", hex(refMinCbor()));
        out.put("pdt_sealed_node", hex(sealedNode()));
        out.putAll(pdt2Vectors());
        return out;
    }

    /** The PDT 2 phase 0 and phase 1 vectors (srad/agentspaces-pdt2.md §8), in emission order. */
    public static Map<String, String> pdt2Vectors() {
        Map<String, String> out = new LinkedHashMap<>();
        // P2-D-01
        out.put("pdt_db_root", hex(dbRoot().encode()) + "," + hex(goldenIndex(DnNode.AEVT).encode()) + ","
                + hex(goldenIndex(DnNode.AVET).encode()));
        out.put("pdt_dn_leaf", hex(goldenIndex(DnNode.EAVT).encode()));
        out.put("pdt_dn_branch", dnBranch());
        out.put("pdt_dn_tail", hex(goldenTail().encode()));
        out.put("pdt_datom_values", hex(datomValues().encode()));
        // P2-S-02
        out.put("pdt_store_pin_cbor", hex(CborCodec.defaultCodec().toBytes(storePin())));
        out.put("pdt_store_receipt_cbor", hex(CborCodec.defaultCodec().toBytes(storeReceipt())));
        // P2-B-02
        out.put("pdt_blake3_identity", Blake3.identity(Blake3.digest(seeded(CUTS_STREAM_BYTES))));
        BaoOutboard.Built small = outboard(SMALL_BLOB_BYTES);
        out.put("pdt_bo_small", hex(small.nodes().get(small.root())) + "," + small.identity());
        BaoOutboard.Built split = outboard(SPLIT_STREAM_BYTES);
        BoNode splitRoot = BoNode.decode(split.root(), split.nodes().get(split.root()));
        out.put("pdt_bo_split", hex(split.nodes().get(split.root())) + "," + split.identity() + ","
                + splitRoot.children().stream().map(Link::cid).collect(Collectors.joining(",")));
        // P2-R-01
        out.put("pdt_rr_relaxed", hex(rrRelaxed().encode()) + "," + hex(rvRelaxed().encode()));
        out.put("pdt_rr_packed", hex(vectorOf(RR_ELEMENTS)));
        // P2-T-01
        out.put("pdt_text_runs", hex(textRuns().encode()));
        TextLeaf merged = textAlice().union(textBob());
        out.put("pdt_text_merge", hex(textAlice().encode()) + "," + hex(textBob().encode()) + "," + hex(merged.encode())
                + "," + merged.text());
        // P2-V-01
        out.put("pdt_vg_chain", hex(vgDeep().encode()) + "," + hex(vgSmall().encode()));
        out.put("pdt_ref_ancestry_cbor", hex(CborCodec.defaultCodec().toBytes(refAncestry())));
        // PDT 2 phase 1, P2-V-01: a merge version's chain node links each base's node in the order of its bases
        out.put("pdt_vg_merge", hex(vgMerge().encode()));
        // PDT 2 phase 2, P2-D-03: every head of a db ref is version 2 of kind db
        out.put("pdt_ref_v2_cbor", hex(CborCodec.defaultCodec().toBytes(refV2())));
        // PDT 2 phase 2, P2-D-09: the sample database the Python and TypeScript readers answer from
        NodeStore sampleStore = NodeStore.inMemory();
        String sampleRoot = dbSample(sampleStore).root();
        out.put("pdt_db_sample", sampleRoot + "," + reachable(sampleStore, sampleRoot).stream().map(PdtVectors::hex)
                .collect(Collectors.joining(",")));
        out.put("pdt_db_sample_answers", dbSampleAnswers().entrySet().stream()
                .map(e -> e.getKey() + "=" + hex(e.getValue().encode())).collect(Collectors.joining(",")));
        // PDT 2 follow-ups, 2026-11-09, F1
        SignedPin signed = signedPin();
        out.put("pdt_signed_pin_cbor", hex(CborCodec.defaultCodec().toBytes(signed)));
        out.put("pdt_signed_pin_consent_cbor", hex(SignedPin.consentBytes(signed.pin(), SIGNED_PIN_GROUP, signed.notAfter())));
        // PDT 2 follow-ups, 2026-11-09, F3 (P2-X-01)
        out.put("pdt_manifest_tree_cbor", hex(CborCodec.defaultCodec().toBytes(treeManifest())));
        // PDT 2 phase 6, groups R and T: the vectors of SequenceVectors
        out.putAll(SequenceVectors.vectors());
        // PDT 2 phase 4, group Z: the vectors of BlobVectors, one of them a content_* message
        out.putAll(BlobVectors.vectors());
        // PDT 2 phase 5, groups P and K: the vectors of PushVectors
        out.putAll(PushVectors.vectors());
        return out;
    }

    /** The manifest of {@code pdt_manifest_tree_cbor}: the small blob's tree, without a piece table. */
    public static Manifest treeManifest() {
        return new Manifest(Manifest.VERSION, blobSmall().split(",")[0], SMALL_BLOB_BYTES, 64 * 1024, 2, null, null, null,
                "blob-small.bin", "application/octet-stream", Manifest.SEALING_NONE, null, Manifest.HASHING_PLAIN,
                GOLDEN_REQUESTER, 1792320000000L, null, Manifest.LAYOUT_TREE);
    }

    // ------------------------------------------------------------------ PDT 2 follow-ups, 2026-11-09, F1

    /** The group of {@code pdt_signed_pin_cbor}. */
    public static final GroupId SIGNED_PIN_GROUP = GroupId.of("zGolden");

    /** The requester of {@code pdt_signed_pin_cbor}: the key whose seed counts {@code 0x40..0x5f}. */
    public static PeerIdentity signedPinRequester() {
        return PeerIdentity.of(Ed25519.fromSeed(counting(64)));
    }

    /** The pin of {@code pdt_signed_pin_cbor}. */
    public static SignedPin signedPin() {
        PeerIdentity requester = signedPinRequester();
        StorePin terms = new StorePin(StorePin.VERSION, ref().root(), StorePin.KIND_TREE, 86400, 2, requester.peerId(),
                List.of(ref().base().get(0)), Map.of("pdt.stores", GOLDEN_HOLDER.value()));
        return SignedPin.sign(terms, SIGNED_PIN_GROUP, 1792323600000L, requester);
    }

    // ------------------------------------------------------------------ PDT 2 phase 2, P2-D-03 and P2-D-09

    /** The head of {@code pdt_ref_v2_cbor}: the golden database at version 2. */
    public static PRef refV2() {
        String root = Cids.of(dbRoot().encode(), Cids.DAG_CBOR);
        return new PRef(PRef.VERSION_2, "graph", PRef.KIND_DB, root, List.of(), 1, GOLDEN_TX2 - PDb.TX0, null,
                PRef.MODE_LINEAR, PRef.SEALING_NONE, null, null);
    }

    /** The sample's 1,000 folded datoms, in EAVT order. */
    public static List<Datom> dbSampleDatoms() {
        List<Datom> out = new ArrayList<>();
        for (long e = 1; e <= 125; e++) {
            out.add(new Datom(e, ":p/active", e % 3 == 0, GOLDEN_TX1));
            out.add(new Datom(e, ":p/age", 20 + (7 * e) % 50, GOLDEN_TX1));
            out.add(new Datom(e, ":p/email", "p" + e + "@example.org", GOLDEN_TX1));
            out.add(new Datom(e, ":p/friend", 1 + e % 125, GOLDEN_TX1));
            out.add(new Datom(e, ":p/friend", 1 + (e + 1) % 125, GOLDEN_TX1));
            out.add(new Datom(e, ":p/name", "Person " + e, GOLDEN_TX1));
            out.add(new Datom(e, ":p/score", e / 2.0, GOLDEN_TX1));
            out.add(new Datom(e, ":p/tag", e % 2 == 0 ? ":tag/even" : ":tag/odd", GOLDEN_TX1));
        }
        out.sort(DatomOrder.EAVT);
        return out;
    }

    /** The sample's unfolded tail, in transaction order. */
    public static List<Datom> dbSampleTail() {
        return List.of(new Datom(7, ":p/name", "Person 7", -GOLDEN_TX2), new Datom(7, ":p/name", "Ada", GOLDEN_TX2),
                new Datom(126, ":p/name", "Newcomer", GOLDEN_TX2));
    }

    private static final Set<String> SAMPLE_INDEXED = Set.of(":p/email", ":p/friend", ":p/name");

    /** The schema of the sample. */
    public static Map<String, Object> dbSampleSchema() {
        return Map.of(":p/email", Map.of(":db/unique", ":db.unique/identity"),
                ":p/friend", Map.of(":db/cardinality", ":db.cardinality/many", ":db/valueType", ":db.type/ref"),
                ":p/name", Map.of(":db/index", true),
                ":p/tag", Map.of(":db/cardinality", ":db.cardinality/many"));
    }

    /** The sample database, written into a store. */
    public static PDb dbSample(NodeStore store) {
        List<Datom> datoms = dbSampleDatoms();
        List<Datom> aevt = new ArrayList<>(datoms);
        aevt.sort(DatomOrder.AEVT);
        List<Datom> avet = new ArrayList<>(datoms.stream().filter(d -> SAMPLE_INDEXED.contains(d.a())).toList());
        avet.sort(DatomOrder.AVET);
        int bf = (int) DbRoot.BRANCHING;
        String eavtRoot = PSortedMap.of(store, DnNode.EAVT, DatomOrder.EAVT, bf, datoms).root();
        String aevtRoot = PSortedMap.of(store, DnNode.AEVT, DatomOrder.AEVT, bf, aevt).root();
        String avetRoot = PSortedMap.of(store, DnNode.AVET, DatomOrder.AVET, bf, avet).root();
        PDb.Writer writer = PDb.writer(store, NodeLoader.of(store));
        return writer.root(CborCodec.defaultCodec().toBytes(dbSampleSchema()), 126, GOLDEN_TX2, eavtRoot, aevtRoot,
                avetRoot, writer.tail(dbSampleTail()));
    }

    /** The answers of {@code pdt_db_sample_answers}, each a leaf of the index it reads, over the sample with its tail applied. */
    public static Map<String, DnNode> dbSampleAnswers() {
        TreeSet<Datom> eav = new TreeSet<>(DatomOrder.EAVT);
        eav.addAll(dbSampleDatoms());
        for (Datom op : dbSampleTail()) {
            eav.removeIf(d -> d.e() == op.e() && d.a().equals(op.a()) && d.v().equals(op.v()));
            if (op.tx() > 0) {
                eav.add(op);
            }
        }
        Map<String, DnNode> out = new LinkedHashMap<>();
        out.put("eavt_7", answer(eav, DnNode.EAVT, d -> d.e() == 7));
        out.put("eavt_126", answer(eav, DnNode.EAVT, d -> d.e() == 126));
        out.put("aevt_name", answer(eav, DnNode.AEVT, d -> d.a().equals(":p/name")));
        out.put("avet_name_ada", answer(eav, DnNode.AVET, d -> d.a().equals(":p/name") && d.v().equals("Ada")));
        out.put("avet_friend_9", answer(eav, DnNode.AVET, d -> d.a().equals(":p/friend") && d.v().equals(9L)));
        out.put("entity_42", answer(eav, DnNode.EAVT, d -> d.e() == 42));
        out.put("lookup_email_p42", answer(eav, DnNode.AVET, d -> d.a().equals(":p/email") && d.v().equals("p42@example.org")));
        out.put("avet_name_person7", answer(eav, DnNode.AVET, d -> d.a().equals(":p/name") && d.v().equals("Person 7")));
        return out;
    }

    private static DnNode answer(Collection<Datom> datoms, String index, Predicate<Datom> match) {
        List<Datom> hits = new ArrayList<>(datoms.stream().filter(match).toList());
        hits.sort(DatomOrder.of(index));
        return DnNode.leaf(index, hits);
    }

    /** Every node a root reaches, the root first and each node before its children, each once. */
    private static List<byte[]> reachable(NodeStore store, String root) {
        List<byte[]> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        Deque<String> stack = new ArrayDeque<>(List.of(root));
        while (!stack.isEmpty()) {
            String cid = stack.pop();
            if (!seen.add(cid)) {
                continue;
            }
            byte[] node = rootNode(store, cid);
            out.add(node);
            List<String> links = NodeCodec.links(cid, node);
            for (int i = links.size() - 1; i >= 0; i--) {
                stack.push(links.get(i));
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ PDT 2 phase 0, P2-D-01

    /** DataScript's first transaction id plus one: the golden database's first transaction. */
    public static final long GOLDEN_TX1 = 536870913L;

    /** The golden database's second transaction, which stays in the tail. */
    public static final long GOLDEN_TX2 = 536870914L;

    private static final Datom AGE_1 = new Datom(1, ":person/age", 36L, GOLDEN_TX1);
    private static final Datom NAME_1 = new Datom(1, ":person/name", "Ada", GOLDEN_TX1);
    private static final Datom AGE_2 = new Datom(2, ":person/age", 45L, GOLDEN_TX1);
    private static final Datom NAME_2 = new Datom(2, ":person/name", "Grace", GOLDEN_TX1);

    /** The golden database's index leaf for {@code eavt}, {@code aevt}, or {@code avet}. */
    public static DnNode goldenIndex(String index) {
        return switch (index) {
            case DnNode.EAVT -> DnNode.leaf(index, List.of(AGE_1, NAME_1, AGE_2, NAME_2));
            case DnNode.AEVT -> DnNode.leaf(index, List.of(AGE_1, AGE_2, NAME_1, NAME_2));
            case DnNode.AVET -> DnNode.leaf(index, List.of(NAME_1, NAME_2));
            default -> throw new IllegalArgumentException("no golden index " + index);
        };
    }

    /** The golden database's tail: the second transaction. */
    public static DnNode goldenTail() {
        return DnNode.tail(List.of(new Datom(2, ":person/age", 45L, -GOLDEN_TX2), new Datom(2, ":person/age", 46L, GOLDEN_TX2)));
    }

    /** The golden database's root. */
    public static DbRoot dbRoot() {
        Embedded schema = new Embedded(CborCodec.defaultCodec().toBytes(Map.of(":person/name", Map.of(":db/index", true))));
        return new DbRoot(schema, 2, GOLDEN_TX2, DbRoot.BRANCHING, link(goldenIndex(DnNode.EAVT).encode()),
                link(goldenIndex(DnNode.AEVT).encode()), link(goldenIndex(DnNode.AVET).encode()), link(goldenTail().encode()));
    }

    private static String dnBranch() {
        DnNode first = DnNode.leaf(DnNode.EAVT, List.of(AGE_1, NAME_1));
        DnNode second = DnNode.leaf(DnNode.EAVT, List.of(AGE_2, NAME_2));
        DnNode branch = DnNode.branch(1, DnNode.EAVT, List.of(NAME_1, NAME_2), List.of(link(first.encode()), link(second.encode())));
        return hex(branch.encode()) + "," + hex(first.encode()) + "," + hex(second.encode());
    }

    /** The leaf of {@code pdt_datom_values}: one datom per value form. */
    public static DnNode datomValues() {
        return DnNode.leaf(DnNode.EAVT, List.of(
                new Datom(3, ":v/bigint", Datom.Tagged.bigint(BigInteger.TWO.pow(70)), GOLDEN_TX1),
                new Datom(3, ":v/bool", true, GOLDEN_TX1),
                new Datom(3, ":v/bytes", new byte[]{0, 1, 2}, GOLDEN_TX1),
                new Datom(3, ":v/float", 1.5d, GOLDEN_TX1),
                new Datom(3, ":v/instant", Datom.Tagged.instant(1792320000000L), GOLDEN_TX1),
                new Datom(3, ":v/int", -7L, GOLDEN_TX1),
                new Datom(3, ":v/keyword", ":status/active", GOLDEN_TX1),
                new Datom(3, ":v/ref", 2L, GOLDEN_TX1),
                new Datom(3, ":v/spill", new Link(Cids.of(new byte[5000], Cids.RAW)), GOLDEN_TX1),
                new Datom(3, ":v/text", "Ada \u2713", GOLDEN_TX1),
                new Datom(3, ":v/uuid", Datom.Tagged.uuid(new UUID(0x0123456789abcdefL, 0xfedcba9876543210L)), -GOLDEN_TX2)));
    }

    // ------------------------------------------------------------------ PDT 2 phase 0, P2-S-02

    /** The requester of {@code pdt_store_pin_cbor}: the peer of the public key {@code 0x00..0x1f}. */
    public static final PeerId GOLDEN_REQUESTER = PeerId.fromPublicKey(counting(0));

    /** The holder of {@code pdt_store_receipt_cbor}: the peer of the public key {@code 0x20..0x3f}. */
    public static final PeerId GOLDEN_HOLDER = PeerId.fromPublicKey(counting(32));

    /** The pin of {@code pdt_store_pin_cbor}. */
    public static StorePin storePin() {
        return new StorePin(StorePin.VERSION, ref().root(), StorePin.KIND_TREE, 86400, 2, GOLDEN_REQUESTER,
                List.of(ref().base().get(0)), Map.of("purpose", "golden"));
    }

    /** The receipt of {@code pdt_store_receipt_cbor}. */
    public static StoreReceipt storeReceipt() {
        return new StoreReceipt(StoreReceipt.VERSION, ref().root(), GOLDEN_HOLDER, 12345, 1792406400000L, true);
    }

    private static byte[] counting(int from) {
        byte[] key = new byte[32];
        for (int i = 0; i < key.length; i++) {
            key[i] = (byte) (from + i);
        }
        return key;
    }

    // ------------------------------------------------------------------ PDT 2 phase 0, P2-B-02

    /**
     * The size of the {@code pdt_bo_split} stream: one full leaf of 2^9 groups (8 MiB, about 32 KiB of parents) and
     * one more group (PDT 2 phase 4 measures, P2-Q-PRF-05, §10.14; the phase 0 leaf held 2^14 groups).
     */
    public static final long SPLIT_STREAM_BYTES = (long) BoNode.LEAF_GROUPS * BoNode.GROUP_BYTES + 5000;

    /** The outboard of the first {@code size} bytes of the seeded stream, read in pieces. */
    public static BaoOutboard.Built outboard(long size) {
        try {
            return BaoOutboard.of(new SeededStream(size));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static byte[] seeded(int size) {
        byte[] bytes = new byte[size];
        new Random(SEED).nextBytes(bytes);
        return bytes;
    }

    /**
     * The {@code java.util.Random(42).nextBytes} stream, produced 64 KiB at a time so a long
     * stream costs no array of its size; {@code nextBytes} uses each int whole, so blocks of a multiple
     * of four bytes concatenate to the bytes of one call.
     */
    private static final class SeededStream extends java.io.InputStream {
        private final Random random = new Random(SEED);
        private final byte[] block = new byte[64 * 1024];
        private long remaining;
        private int at = block.length;

        SeededStream(long size) {
            this.remaining = size;
        }

        @Override
        public int read() {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (remaining == 0) {
                return -1;
            }
            if (at == block.length) {
                random.nextBytes(block);
                at = 0;
            }
            int n = (int) Math.min(Math.min(len, block.length - at), remaining);
            System.arraycopy(block, at, b, off, n);
            at += n;
            remaining -= n;
            return n;
        }
    }

    // ------------------------------------------------------------------ PDT 2 phase 0, P2-R-01

    /** The elements of the RRB vectors: 0..99. */
    public static final int RR_ELEMENTS = 100;

    /** The leaves of {@code pdt_rr_relaxed}: element ranges 0..31, 32..39, 40..71, 72..95. */
    private static final int[][] RR_LEAVES = {{0, 32}, {32, 40}, {40, 72}, {72, 96}};

    /** A {@code v} leaf over the elements {@code [from, to)}. */
    public static byte[] rrLeaf(int from, int to) {
        List<Embedded> elements = new java.util.ArrayList<>();
        for (int i = from; i < to; i++) {
            elements.add(new Embedded(CborCodec.defaultCodec().toBytes(i)));
        }
        Map<String, Object> node = new LinkedHashMap<>();
        node.put(NodeCodec.KIND_KEY, Kind.V);
        node.put("e", elements);
        return NodeCodec.encode(node);
    }

    /** The relaxed interior node of {@code pdt_rr_relaxed}. */
    public static RvNode rvRelaxed() {
        List<Link> children = new java.util.ArrayList<>();
        List<Long> sizes = new java.util.ArrayList<>();
        for (int[] leaf : RR_LEAVES) {
            children.add(link(rrLeaf(leaf[0], leaf[1])));
            sizes.add((long) leaf[1]);
        }
        return new RvNode(children, sizes);
    }

    /** The root of {@code pdt_rr_relaxed}. */
    public static RrRoot rrRelaxed() {
        List<Embedded> tail = new java.util.ArrayList<>();
        for (int i = 96; i < RR_ELEMENTS; i++) {
            tail.add(new Embedded(CborCodec.defaultCodec().toBytes(i)));
        }
        return new RrRoot(RR_ELEMENTS, RrRoot.BITS, link(rvRelaxed().encode()), tail);
    }

    // ------------------------------------------------------------------ PDT 2 phase 0, P2-T-01

    /** The leaf of {@code pdt_text_runs}. */
    public static TextLeaf textRuns() {
        return new TextLeaf(List.of(
                new TextRun("alice", 0, null, TextRun.RIGHT, 4, false, "Ab\ud83d\ude00c"),
                new TextRun("alice", 4, new TextRun.CharId("alice", 1), TextRun.LEFT, 2, true, ""),
                new TextRun("bob", 0, new TextRun.CharId("alice", 3), TextRun.RIGHT, 2, false, "\u00e9!")));
    }

    /** Alice's leaf of {@code pdt_text_merge}: "ac" with "b" inserted between. */
    public static TextLeaf textAlice() {
        return new TextLeaf(List.of(
                new TextRun("alice", 0, null, TextRun.RIGHT, 2, false, "ac"),
                new TextRun("alice", 2, new TextRun.CharId("alice", 1), TextRun.LEFT, 1, false, "b")));
    }

    /** Bob's leaf of {@code pdt_text_merge}: "ac" with "XY" inserted between and "a" deleted. */
    public static TextLeaf textBob() {
        return new TextLeaf(List.of(
                new TextRun("alice", 0, null, TextRun.RIGHT, 1, true, ""),
                new TextRun("alice", 1, new TextRun.CharId("alice", 0), TextRun.RIGHT, 1, false, "c"),
                new TextRun("bob", 0, new TextRun.CharId("alice", 1), TextRun.LEFT, 2, false, "XY")));
    }

    // ------------------------------------------------------------------ PDT 2 phase 0, P2-V-01

    /** The issuer of the {@code pdt_vg_chain} nodes. */
    public static final String GOLDEN_WRITER = GOLDEN_REQUESTER.value() + "/writer";

    /** The {@code vg} node of the {@code pdt_map_small} version: no bases. */
    public static VgNode vgSmall() {
        return new VgNode(ref().base().get(0), List.of(), GOLDEN_WRITER, "1792320000000:0:golden", List.of());
    }

    /** The {@code vg} node of the {@code pdt_map_deep} version, whose base is the small one. */
    public static VgNode vgDeep() {
        PRef ref = ref();
        return new VgNode(ref.root(), ref.base(), GOLDEN_WRITER, "1792320000001:0:golden", List.of(link(vgSmall().encode())));
    }

    /**
     * The {@code vg} node of {@code pdt_vg_merge} (PDT 2 phase 1, P2-V-01): the {@code pdt_map_empty}
     * version merged from the deep and the small versions, bases in that order, one link per base in
     * the same order, as {@code Refs} writes it.
     */
    public static VgNode vgMerge() {
        PRef ref = ref();
        return new VgNode(Cids.of(mapEmpty(), Cids.DAG_CBOR), List.of(ref.root(), ref.base().get(0)), GOLDEN_WRITER,
                "1792320000002:0:golden", List.of(link(vgDeep().encode()), link(vgSmall().encode())));
    }

    /** The head of {@code pdt_ref_ancestry_cbor}: a {@code v} 1 head naming its {@code vg} node. */
    public static PRef refAncestry() {
        PRef ref = ref();
        PRef head = new PRef(PRef.VERSION, "facts", PRef.KIND_MAP, ref.root(), ref.base(), 2, 100, null, PRef.MODE_MULTI,
                PRef.SEALING_NONE, null, null);
        return Ancestry.with(head, Cids.of(vgDeep().encode(), Cids.DAG_CBOR));
    }

    private static Link link(byte[] node) {
        return new Link(Cids.of(node, Cids.DAG_CBOR));
    }

    /** The sealed envelope of {@link #mapSmall()} under {@link #SEALED_TEST_KEY} at {@link #SEALED_TEST_EPOCH}. */
    public static byte[] sealedNode() {
        byte[] nodeKey = NodeSealer.nodeKey(GroupKey.fromBytes(SEALED_TEST_KEY));
        return NodeSealer.seal(nodeKey, SEALED_TEST_EPOCH, mapSmall(), Cids.DAG_CBOR);
    }

    private static byte[] sealedTestKey() {
        byte[] key = new byte[GroupKey.KEY_LENGTH];
        for (int i = 0; i < key.length; i++) {
            key[i] = (byte) i;
        }
        return key;
    }

    /** The head record of {@code pdt_ref_cbor}: two inline nodes and every optional field. */
    public static PRef ref() {
        NodeStore store = NodeStore.inMemory();
        Editor<String, Object> editor = PMap.empty(store, Codec.of(String.class), Codec.of(Object.class)).editor();
        for (int i = 0; i < 100; i++) {
            editor.assoc("k" + i, i);
        }
        String deepRoot = editor.done().value().root();
        byte[] deepNode = rootNode(store, deepRoot);
        String firstChild = NodeCodec.links(deepRoot, deepNode).get(0);
        byte[] childNode = rootNode(store, firstChild);
        String smallRoot = PMap.empty(store, Codec.of(String.class), Codec.of(Object.class))
                .assoc("a", 1).assoc("b", "two").root();
        return new PRef(PRef.VERSION, "facts", PRef.KIND_MAP, deepRoot, List.of(smallRoot), 2, 100,
                List.of(deepNode, childNode), PRef.MODE_MULTI, PRef.SEALING_NONE, 3L, Map.of("origin", "golden"));
    }

    /** The head record of {@code pdt_ref_min_cbor}: no optional field. */
    public static PRef refMin() {
        NodeStore store = NodeStore.inMemory();
        String emptyRoot = PMap.empty(store, Codec.of(String.class), Codec.of(Object.class)).root();
        return new PRef(PRef.VERSION, "facts", PRef.KIND_MAP, emptyRoot, List.of(), 1, 0, null, PRef.MODE_MULTI,
                PRef.SEALING_NONE, null, null);
    }

    /** The canonical CBOR of {@link #ref()}. */
    public static byte[] refCbor() {
        return CborCodec.defaultCodec().toBytes(ref());
    }

    /** The canonical CBOR of {@link #refMin()}. */
    public static byte[] refMinCbor() {
        return CborCodec.defaultCodec().toBytes(refMin());
    }

    /** The root node bytes of {@code {"rec": {"x": 1, "y": "two"}, "f": 1.5f}}. */
    public static byte[] nodeEmbedded() {
        NodeStore store = NodeStore.inMemory();
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("x", 1);
        rec.put("y", "two");
        PMap<String, Object> map = PMap.empty(store, Codec.of(String.class), Codec.of(Object.class))
                .assoc("rec", rec)
                .assoc("f", 1.5f);
        return rootNode(store, map.root());
    }

    /** The root node bytes of the empty map. */
    public static byte[] mapEmpty() {
        NodeStore store = NodeStore.inMemory();
        return rootNode(store, PMap.empty(store, Codec.of(String.class), Codec.of(Object.class)).root());
    }

    /** The root node bytes of {@code {"a": 1, "b": "two"}}. */
    public static byte[] mapSmall() {
        NodeStore store = NodeStore.inMemory();
        PMap<String, Object> map = PMap.empty(store, Codec.of(String.class), Codec.of(Object.class))
                .assoc("a", 1)
                .assoc("b", "two");
        return rootNode(store, map.root());
    }

    /** The root node bytes of {@code "k0".."k99"} to {@code 0..99}. */
    public static byte[] mapDeep() {
        NodeStore store = NodeStore.inMemory();
        Editor<String, Object> editor = PMap.empty(store, Codec.of(String.class), Codec.of(Object.class)).editor();
        for (int i = 0; i < 100; i++) {
            editor.assoc("k" + i, i);
        }
        return rootNode(store, editor.done().value().root());
    }

    /** The root node bytes of {@code [0..9]}. */
    public static byte[] vectorTail() {
        return vectorOf(10);
    }

    /** The root node bytes of {@code [0..1099]}. */
    public static byte[] vectorDeep() {
        return vectorOf(1100);
    }

    /** The root node bytes of the vector {@code [0..count)}. */
    public static byte[] vectorOf(int count) {
        NodeStore store = NodeStore.inMemory();
        VectorEditor<Object> editor = PVector.empty(store, Codec.of(Object.class)).editor();
        for (int i = 0; i < count; i++) {
            editor.conj(i);
        }
        return rootNode(store, editor.done().value().root());
    }

    /** The root CID of the seeded 100 KiB blob, then its chunk CIDs, comma separated. */
    public static String blobSmall() {
        byte[] bytes = new byte[SMALL_BLOB_BYTES];
        new Random(SEED).nextBytes(bytes);
        PBlob blob = PBlob.of(bytes, NodeStore.inMemory());
        return blob.root() + "," + blob.chunks().stream().map(BlobTree.Entry::cid).collect(Collectors.joining(","));
    }

    /** The first 20 cut points of the seeded 8 MiB stream, decimal, comma separated. */
    public static String fastCdcCuts() {
        byte[] bytes = new byte[CUTS_STREAM_BYTES];
        new Random(SEED).nextBytes(bytes);
        List<Integer> cuts = FastCdc.cuts(bytes);
        return cuts.subList(0, Math.min(CUTS_RECORDED, cuts.size())).stream()
                .map(String::valueOf).collect(Collectors.joining(","));
    }

    private static byte[] rootNode(NodeStore store, String root) {
        return store.get(root).orElseThrow(() -> new IllegalStateException("root " + root + " was not flushed"));
    }

    private static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }
}
