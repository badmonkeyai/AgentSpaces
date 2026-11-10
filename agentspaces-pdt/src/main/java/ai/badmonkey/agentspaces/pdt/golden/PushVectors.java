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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.content.tree.sync.Riblt;
import ai.badmonkey.agentspaces.pdt.refs.PushBundle;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@code pdt_*} golden vectors of PDT 2 phase 5 (srad/agentspaces-pdt2.md §3.4, §3.6, §6.10.4), which
 * {@link PdtVectors#pdt2Vectors()} lists after the vectors of phases 6 and 4:
 * <ul>
 *   <li>{@code pdt_push_bundle_cbor} (P2-P-01, rule 16): the canonical CBOR of a {@link PushBundle} (hex) of seq 3,
 *       part 0 of 1, whose one node is the {@code pdt_map_small} node and whose root is that node's CID.</li>
 *   <li>{@code pdt_riblt_symbols} (P2-K-05): three hex strings, comma separated: the session key {@code 00..0f}; the
 *       ids, the sha-256 of the UTF-8 text {@code "aspace:riblt:<i>"} for i in 0 to 9, concatenated; and the wire
 *       form of the first 12 coded symbols of those ids under that key, concatenated (sum, checksum as 8
 *       little-endian bytes, count as a negentropy varint).</li>
 * </ul>
 * The sketch messages of the same phase, {@code content_tree_want_sketch_cbor} and
 * {@code content_tree_end_sketch_cbor}, are checked by {@code agentspaces-content}'s {@code SketchVectorsTest}.
 */
public final class PushVectors {

    /** The session key of {@code pdt_riblt_symbols}. */
    static final byte[] RIBLT_KEY = HexFormat.of().parseHex("000102030405060708090a0b0c0d0e0f");

    private PushVectors() {
    }

    /** The phase 5 vectors in emission order. */
    public static Map<String, String> vectors() {
        Map<String, String> out = new LinkedHashMap<>();
        out.put("pdt_push_bundle_cbor", HexFormat.of().formatHex(CborCodec.defaultCodec().toBytes(pushBundle())));
        out.put("pdt_riblt_symbols", ribltSymbols());
        return out;
    }

    /** The bundle of {@code pdt_push_bundle_cbor}. */
    public static PushBundle pushBundle() {
        byte[] node = PdtVectors.mapSmall();
        return new PushBundle(PushBundle.VERSION, Cids.of(node, Cids.DAG_CBOR), 3, 0, 1, List.of(node));
    }

    private static String ribltSymbols() {
        List<byte[]> ids = new ArrayList<>();
        ByteArrayOutputStream idBytes = new ByteArrayOutputStream();
        for (int i = 0; i < 10; i++) {
            byte[] id = Digests.sha256(("aspace:riblt:" + i).getBytes(StandardCharsets.UTF_8));
            ids.add(id);
            idBytes.writeBytes(id);
        }
        ByteArrayOutputStream symbols = new ByteArrayOutputStream();
        new Riblt.Encoder(RIBLT_KEY, ids).next(12).forEach(s -> s.write(symbols));
        HexFormat hex = HexFormat.of();
        return hex.formatHex(RIBLT_KEY) + "," + hex.formatHex(idBytes.toByteArray()) + "," + hex.formatHex(symbols.toByteArray());
    }
}
