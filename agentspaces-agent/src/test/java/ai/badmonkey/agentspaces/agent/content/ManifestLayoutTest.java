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
package ai.badmonkey.agentspaces.agent.content;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PD-X-09: the optional trailing {@code layout} of a manifest leaves the 0.5.0
 * bytes untouched when absent, travels when set, and admits only
 * {@code pieces} and {@code tree}.
 */
class ManifestLayoutTest {

    private final CborCodec codec = CborCodec.defaultCodec();
    private final PeerId holder = PeerId.of("zHolder");

    /** A CIDv1 over sha2-256 in its one base32 text. */
    private static String cid(int codec, byte[] content) {
        byte[] digest = Digests.sha256(content);
        byte[] binary = new byte[4 + digest.length];
        binary[0] = 0x01;
        binary[1] = (byte) codec;
        binary[2] = 0x12;
        binary[3] = 0x20;
        System.arraycopy(digest, 0, binary, 4, digest.length);
        return Multibase.base32(binary);
    }

    /** A blob tree's root: a dag-cbor CID, the only root a tree manifest names. */
    private static final String TREE_ROOT = cid(0x71, new byte[]{(byte) 0xa0});

    private Manifest manifest(String layout) {
        return new Manifest(1, TREE_ROOT, 3 * 65_536 + 1, 65_536, 4, new byte[4 * 32], null, null,
                "m.bin", "application/octet-stream", Manifest.SEALING_NONE, null, Manifest.HASHING_PLAIN,
                holder, 1_000L, Map.of("run", "42"), layout);
    }

    @Test
    void aManifestWithoutALayoutKeepsItsBytesAndMeansPieces() {
        Manifest legacyShape = new Manifest(1, TREE_ROOT, 3 * 65_536 + 1, 65_536, 4, new byte[4 * 32], null, null,
                "m.bin", "application/octet-stream", Manifest.SEALING_NONE, null, Manifest.HASHING_PLAIN,
                holder, 1_000L, Map.of("run", "42"));
        byte[] bytes = codec.toBytes(legacyShape);
        assertThat(legacyShape.layout()).isNull();
        assertThat(legacyShape.treeLayout()).isFalse();
        assertThat(codec.toBytes(manifest(null))).isEqualTo(bytes);
        assertThat(new String(bytes, java.nio.charset.StandardCharsets.ISO_8859_1)).doesNotContain("layout");
        Manifest back = codec.fromBytes(bytes, Manifest.class);
        assertThat(back.layout()).isNull();
        assertThat(back).isEqualTo(legacyShape.withTableCopy());
        assertThat(legacyShape.violation()).isEmpty();
    }

    @Test
    void aTreeLayoutTravelsAndRoundTrips() {
        Manifest tree = manifest(Manifest.LAYOUT_TREE);
        assertThat(tree.treeLayout()).isTrue();
        assertThat(tree.violation()).isEmpty();
        byte[] bytes = codec.toBytes(tree);
        assertThat(new String(bytes, java.nio.charset.StandardCharsets.ISO_8859_1)).contains("layout").contains("tree");
        assertThat(bytes.length).isGreaterThan(codec.toBytes(manifest(null)).length);
        Manifest back = codec.fromBytes(bytes, Manifest.class);
        assertThat(back.layout()).isEqualTo(Manifest.LAYOUT_TREE);
        assertThat(back).isEqualTo(tree.withTableCopy());
        assertThat(back).isNotEqualTo(manifest(null).withTableCopy());
        assertThat(manifest(Manifest.LAYOUT_PIECES).violation()).isEmpty();
        assertThat(manifest(Manifest.LAYOUT_PIECES).treeLayout()).isFalse();
        assertThat(manifest(null).withLayout(Manifest.LAYOUT_TREE)).isEqualTo(tree);
        assertThat(tree.withLayout(null)).isEqualTo(manifest(null));
    }

    /**
     * PDT 2 phase 2, P2-X-01, as the owner ruled on 2026-11-09 (PDT 2 follow-ups, F3): a tree manifest carries
     * its tree root, which names every piece, so it may travel without a piece table; a flat manifest still
     * needs exactly one of the table and the table reference, and a tree manifest may not carry both.
     */
    @Test
    void aTreeManifestNeedsNoPieceTableAndAFlatOneStillDoes() {
        Manifest tableless = new Manifest(1, TREE_ROOT, 3 * 65_536 + 1, 65_536, 4, null, null, null,
                "m.bin", "application/octet-stream", Manifest.SEALING_NONE, null, Manifest.HASHING_PLAIN,
                holder, 1_000L, Map.of("run", "42"), Manifest.LAYOUT_TREE);
        assertThat(tableless.violation()).isEmpty();
        assertThat(codec.fromWire(codec.toBytes(tableless), Manifest.class)).isEqualTo(tableless);
        assertThat(tableless.withLayout(null).violation()).contains("exactly one of table and tableRef must be present");
        assertThat(tableless.withLayout(Manifest.LAYOUT_PIECES).violation())
                .contains("exactly one of table and tableRef must be present");
        Manifest both = new Manifest(1, TREE_ROOT, 3 * 65_536 + 1, 65_536, 4, new byte[4 * 32], "bref", null,
                "m.bin", "application/octet-stream", Manifest.SEALING_NONE, null, Manifest.HASHING_PLAIN,
                holder, 1_000L, Map.of("run", "42"), Manifest.LAYOUT_TREE);
        assertThat(both.violation()).contains("at most one of table and tableRef may be present");
        assertThat(manifest(Manifest.LAYOUT_TREE).violation()).as("a tree manifest may still carry its table").isEmpty();
    }

    /**
     * PDT 2 follow-ups, 2026-11-09, F3: a tree manifest names a blob tree, whose root is a dag-cbor CID over
     * sha2-256, so a table-less tree manifest can never stand for a flat root (a raw CID over a piece table)
     * and send a flat fetch after a table it does not have.
     */
    @Test
    void aTreeManifestNamesADagCborRootAndNoOther() {
        String flatRoot = cid(0x55, new byte[64]);
        for (String root : new String[]{flatRoot, "broot", TREE_ROOT.toUpperCase(java.util.Locale.ROOT),
                TREE_ROOT.substring(0, TREE_ROOT.length() - 1), "z" + TREE_ROOT.substring(1)}) {
            Manifest tree = new Manifest(1, root, 3 * 65_536 + 1, 65_536, 4, null, null, null, "m.bin",
                    "application/octet-stream", Manifest.SEALING_NONE, null, Manifest.HASHING_PLAIN, holder, 1_000L,
                    null, Manifest.LAYOUT_TREE);
            assertThat(tree.violation()).as(root).contains("a tree manifest's root is a dag-cbor CID");
        }
        Manifest flat = new Manifest(1, flatRoot, 3 * 65_536 + 1, 65_536, 4, new byte[4 * 32], null, null, "m.bin",
                "application/octet-stream", Manifest.SEALING_NONE, null, Manifest.HASHING_PLAIN, holder, 1_000L, null);
        assertThat(flat.violation()).as("a flat manifest keeps its rules").isEmpty();
        assertThat(flat.withLayout(Manifest.LAYOUT_TREE).violation()).contains("a tree manifest's root is a dag-cbor CID");
    }

    @Test
    void validationRefusesAnyOtherLayout() {
        assertThat(manifest("blocks").violation()).contains("unknown layout blocks");
        assertThat(manifest("").violation()).contains("unknown layout ");
        assertThat(manifest("Tree").violation()).contains("unknown layout Tree");
    }
}
