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

    private Manifest manifest(String layout) {
        return new Manifest(1, "broot", 3 * 65_536 + 1, 65_536, 4, new byte[4 * 32], null, null,
                "m.bin", "application/octet-stream", Manifest.SEALING_NONE, null, Manifest.HASHING_PLAIN,
                holder, 1_000L, Map.of("run", "42"), layout);
    }

    @Test
    void aManifestWithoutALayoutKeepsItsBytesAndMeansPieces() {
        Manifest legacyShape = new Manifest(1, "broot", 3 * 65_536 + 1, 65_536, 4, new byte[4 * 32], null, null,
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

    @Test
    void validationRefusesAnyOtherLayout() {
        assertThat(manifest("blocks").violation()).contains("unknown layout blocks");
        assertThat(manifest("").violation()).contains("unknown layout ");
        assertThat(manifest("Tree").violation()).contains("unknown layout Tree");
    }
}
