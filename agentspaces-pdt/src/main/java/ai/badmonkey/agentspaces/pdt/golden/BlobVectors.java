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
import ai.badmonkey.agentspaces.content.Messages;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.content.tree.Dcz;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * The golden vectors of PDT 2 phase 4 (srad/agentspaces-pdt2.md §8; P2-Z-01, P2-Z-04, P2-F-02, rule 14 of §6.10.3),
 * which {@link PdtVectors#pdt2Vectors()} lists after the phase 6 ones:
 * <ul>
 *   <li>{@code pdt_dcz_frame}: the base chunk, the target chunk, and the {@code dcz} field Java writes for them (hex
 *       each, comma separated). The base is the first 4,096 bytes of the {@code java.util.Random(42).nextBytes}
 *       stream; the target is the base with bytes 1,000 to 1,003 replaced by the ASCII {@code PDT2} and the ASCII
 *       {@code phase four} appended (4,106 bytes). The field is the format byte {@code 'd'} (0x64), the RFC 9842
 *       header ({@code 5e 2a 4d 18 20 00 00 00} and the base's sha-256), and one zstd frame with the base as its raw
 *       dictionary, a declared content size, and no dictionary id or checksum. A port opens the frame over the base
 *       and gets the target byte for byte; it need not reproduce the frame, since zstd's compressed bytes depend on
 *       the encoder (Java's are zstd 1.5.7 through {@code zstd-jni} at level 3).</li>
 *   <li>{@code content_nodes_deltas_cbor}: the wire CBOR (hex) of a level-2 {@code NODES} (tag 9), req 7, an empty
 *       {@code nodes} list, no {@code sealed}, {@code epoch}, or {@code missing}, and {@code deltas} holding one entry
 *       {@code {cid, base, dcz}}: the {@code raw} CID of the target, the {@code raw} CID of the base, and the field of
 *       {@code pdt_dcz_frame}, in that order after the six fields of level 1.</li>
 * </ul>
 */
public final class BlobVectors {

    /** The bytes of the base chunk. */
    public static final int BASE_BYTES = 4096;

    /** Where the target replaces four bytes of the base. */
    public static final int EDIT_AT = 1000;

    private static final byte[] EDIT = "PDT2".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] APPENDED = "phase four".getBytes(StandardCharsets.US_ASCII);

    private BlobVectors() {
    }

    /** The vectors in emission order. */
    public static Map<String, String> vectors() {
        Map<String, String> out = new LinkedHashMap<>();
        byte[] base = base();
        byte[] target = target();
        byte[] field = field();
        out.put("pdt_dcz_frame", hex(base) + "," + hex(target) + "," + hex(field));
        out.put("content_nodes_deltas_cbor", hex(CborCodec.defaultCodec().toBytes(nodes())));
        return out;
    }

    /** The base chunk of {@code pdt_dcz_frame}. */
    public static byte[] base() {
        byte[] base = new byte[BASE_BYTES];
        new Random(PdtVectors.SEED).nextBytes(base);
        return base;
    }

    /** The target chunk of {@code pdt_dcz_frame}. */
    public static byte[] target() {
        byte[] base = base();
        byte[] target = new byte[base.length + APPENDED.length];
        System.arraycopy(base, 0, target, 0, base.length);
        System.arraycopy(EDIT, 0, target, EDIT_AT, EDIT.length);
        System.arraycopy(APPENDED, 0, target, base.length, APPENDED.length);
        return target;
    }

    /** The {@code dcz} field of {@code pdt_dcz_frame}. */
    public static byte[] field() {
        return Dcz.encode(base(), target()).orElseThrow(() -> new IllegalStateException("zstd made no frame"));
    }

    /** The {@code NODES} of {@code content_nodes_deltas_cbor}. */
    public static Messages.Nodes nodes() {
        Messages.NodeDelta delta = new Messages.NodeDelta(Cids.of(target(), Cids.RAW), Cids.of(base(), Cids.RAW), field());
        return new Messages.Nodes(Messages.NODES, 7, List.of(), null, null, null, List.of(delta));
    }

    private static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }
}
