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

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.blob.BlobTree;
import ai.badmonkey.agentspaces.pdt.blob.FastCdc;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.seal.NodeSealer;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.Editor;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.pdt.values.PVector;
import ai.badmonkey.agentspaces.pdt.values.VectorEditor;

import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
        return out;
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

    private static byte[] vectorOf(int count) {
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
