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
package ai.badmonkey.agentspaces.agent.pdt;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PD-R-01: the head record round-trips through the canonical codec, omits its
 * optional fields when null, tags name and root, and names each violated bound.
 */
class PRefTest {

    private static final String ROOT = "bafyreigq3u2qzvyq5r6dpq2nqq4cujdxa6wmljnrrjz4ns2nk2o3clj5vy";
    private static final String BASE = "bafyreihstcpt3qhkqw2kzczkq7hslg4wq4rh3ukcn6f22ndpxq3crdi6wq";

    private final CborCodec codec = CborCodec.defaultCodec();

    private static PRef full() {
        return new PRef(1, "facts", PRef.KIND_MAP, ROOT, List.of(BASE), 2, 100,
                List.of(new byte[]{(byte) 0xa0}, new byte[]{1, 2, 3}), PRef.MODE_MULTI, PRef.SEALING_NONE,
                7L, Map.of("origin", "test"));
    }

    private static PRef min() {
        return new PRef(1, "facts", PRef.KIND_MAP, ROOT, List.of(), 1, 0, null, PRef.MODE_MULTI,
                PRef.SEALING_NONE, null, null);
    }

    @Test
    void aFullHeadRoundTripsAndKeepsItsInlineNodes() {
        PRef head = full();
        assertThat(head.violation()).isEmpty();
        byte[] bytes = codec.toBytes(head);
        PRef back = codec.fromBytes(bytes, PRef.class);
        assertThat(back).isEqualTo(head);
        assertThat(back.hashCode()).isEqualTo(head.hashCode());
        assertThat(back.nodes()).hasSize(2);
        assertThat(back.inlineBytes()).isEqualTo(4);
        assertThat(back.keyEpoch()).isEqualTo(7L);
        assertThat(back.attrs()).containsEntry("origin", "test");
    }

    @Test
    void theMinimalHeadOmitsNodesKeyEpochAndAttrsFromTheWire() {
        PRef head = min();
        assertThat(head.violation()).isEmpty();
        byte[] bytes = codec.toBytes(head);
        String text = new String(bytes, StandardCharsets.ISO_8859_1);
        assertThat(text).doesNotContain("nodes").doesNotContain("keyEpoch").doesNotContain("attrs");
        assertThat(text).contains("name").contains("kind").contains("root").contains("base").contains("seq")
                .contains("size").contains("mode").contains("sealing");
        PRef back = codec.fromBytes(bytes, PRef.class);
        assertThat(back).isEqualTo(head);
        assertThat(back.nodes()).isNull();
        assertThat(back.inlineBytes()).isZero();
        assertThat(back.base()).isEmpty();
        assertThat(full().withoutNodes().nodes()).isNull();
        assertThat(full().withoutNodes().withNodes(List.of(new byte[]{9})).nodes()).hasSize(1);
    }

    @Test
    void tagsCarryTheNameAndTheRoot() {
        assertThat(full().tags()).containsExactlyInAnyOrderEntriesOf(Map.of("name", "facts", "root", ROOT));
        assertThat(PRef.NAME_TAG).isEqualTo("name");
        assertThat(PRef.ROOT_TAG).isEqualTo("root");
    }

    @Test
    void everyBoundIsNamedByViolation() {
        PRef base = min();
        assertThat(new PRef(2, "facts", "map", ROOT, null, 1, 0, null, "multi", "none", null, null).violation())
                .contains("unsupported PRef version 2");
        assertThat(new PRef(1, "", "map", ROOT, null, 1, 0, null, "multi", "none", null, null).violation())
                .contains("empty name");
        assertThat(new PRef(1, "n".repeat(257), "map", ROOT, null, 1, 0, null, "multi", "none", null, null).violation())
                .contains("name longer than 256 bytes");
        assertThat(new PRef(1, "facts", "graph", ROOT, null, 1, 0, null, "multi", "none", null, null).violation())
                .contains("unknown kind graph");
        assertThat(new PRef(1, "facts", "map", "", null, 1, 0, null, "multi", "none", null, null).violation())
                .contains("empty root");
        List<String> nine = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            nine.add(BASE);
        }
        assertThat(new PRef(1, "facts", "map", ROOT, nine, 1, 0, null, "multi", "none", null, null).violation())
                .contains("more than 8 bases");
        assertThat(new PRef(1, "facts", "map", ROOT, null, -1, 0, null, "multi", "none", null, null).violation())
                .contains("negative seq");
        assertThat(new PRef(1, "facts", "map", ROOT, null, 1, -1, null, "multi", "none", null, null).violation())
                .contains("negative size");
        assertThat(new PRef(1, "facts", "map", ROOT, null, 1, 0, List.of(new byte[0]), "multi", "none", null, null).violation())
                .contains("empty inline node");
        assertThat(new PRef(1, "facts", "map", ROOT, null, 1, 0, List.of(new byte[32 * 1024 + 1]), "multi", "none", null, null).violation())
                .contains("inline nodes larger than 32768 bytes");
        assertThat(new PRef(1, "facts", "map", ROOT, null, 1, 0, List.of(new byte[16 * 1024], new byte[16 * 1024]), "multi", "none", null, null).violation())
                .isEmpty();
        assertThat(new PRef(1, "facts", "map", ROOT, null, 1, 0, null, "", "none", null, null).violation())
                .contains("empty mode");
        assertThat(new PRef(1, "facts", "map", ROOT, null, 1, 0, null, "multi", "", null, null).violation())
                .contains("empty sealing");
        assertThat(new PRef(1, "facts", "map", ROOT, null, 1, 0, null, "multi", "none", -1L, null).violation())
                .contains("negative keyEpoch");
        Map<String, String> many = new HashMap<>();
        for (int i = 0; i < 33; i++) {
            many.put("k" + i, "v");
        }
        assertThat(new PRef(1, "facts", "map", ROOT, null, 1, 0, null, "multi", "none", null, many).violation())
                .contains("more than 32 attrs");
        assertThat(new PRef(1, "facts", "map", ROOT, null, 1, 0, null, "multi", "none", null, Map.of("k", "v".repeat(4096))).violation())
                .contains("attrs larger than 4096 bytes");
        // the open sets: an unknown mode or sealing is kept
        assertThat(new PRef(1, "facts", "map", ROOT, null, 1, 0, null, "future-mode", "future-seal", null, null).violation())
                .isEmpty();
        assertThat(base.violation()).isEmpty();
    }

    @Test
    void theRootAndEveryBaseObeyTheCidGrammarAndSeqModeAndSealingAreBounded() {
        // a root that is any non-CID string is named, so no reader ever parses it (PD-P-04, §6.7 row 6)
        assertThat(new PRef(1, "facts", "map", "not-a-cid", null, 1, 0, null, "multi", "none", null, null).violation())
                .contains("root is not a CID");
        assertThat(new PRef(1, "facts", "map", "b" + "a".repeat(58), null, 1, 0, null, "multi", "none", null, null).violation())
                .contains("root is not a CID");
        assertThat(new PRef(1, "facts", "map", ROOT.toUpperCase(), null, 1, 0, null, "multi", "none", null, null).violation())
                .contains("root is not a CID");
        assertThat(new PRef(1, "facts", "map", ROOT, List.of("bafy"), 1, 0, null, "multi", "none", null, null).violation())
                .contains("base root is not a CID");
        assertThat(PRef.isCid(ROOT)).isTrue();
        assertThat(PRef.isCid(BASE)).isTrue();
        assertThat(PRef.isCid("bafkreigq3u2qzvyq5r6dpq2nqq4cujdxa6wmljnrrjz4ns2nk2o3clj5vy")).as("raw codec").isTrue();
        assertThat(PRef.isCid("bafybeigq3u2qzvyq5r6dpq2nqq4cujdxa6wmljnrrjz4ns2nk2o3clj5vy")).as("dag-pb codec").isFalse();
        assertThat(PRef.isCid(ROOT + "a")).isFalse();
        assertThat(PRef.isCid(ROOT.substring(0, 58))).isFalse();
        assertThat(PRef.isCid(null)).isFalse();
        // seq saturates below 2^53, so a successor of the largest head never wraps negative
        assertThat(new PRef(1, "facts", "map", ROOT, null, PRef.MAX_SEQ, 0, null, "multi", "none", null, null).violation()).isEmpty();
        assertThat(new PRef(1, "facts", "map", ROOT, null, PRef.MAX_SEQ + 1, 0, null, "multi", "none", null, null).violation())
                .contains("seq above " + PRef.MAX_SEQ);
        assertThat(new PRef(1, "facts", "map", ROOT, null, Long.MAX_VALUE, 0, null, "multi", "none", null, null).violation())
                .contains("seq above " + PRef.MAX_SEQ);
        // the open sets stay open but short
        assertThat(new PRef(1, "facts", "map", ROOT, null, 1, 0, null, "m".repeat(33), "none", null, null).violation())
                .contains("mode longer than 32 bytes");
        assertThat(new PRef(1, "facts", "map", ROOT, null, 1, 0, null, "multi", "s".repeat(33), null, null).violation())
                .contains("sealing longer than 32 bytes");
        assertThat(new PRef(1, "facts", "map", ROOT, null, 1, 0, null, "m".repeat(32), "s".repeat(32), null, null).violation())
                .isEmpty();
        // the reserved attrs a rewrite carries read back by name
        PRef rewritten = min().withAttrs(Map.of(PRef.ISSUED_ATTR, "7:0:peer", PRef.ISSUER_ATTR, "zPeer/app"));
        assertThat(rewritten.issuedAttr()).isEqualTo("7:0:peer");
        assertThat(rewritten.issuerAttr()).isEqualTo("zPeer/app");
        assertThat(min().issuedAttr()).isNull();
        assertThat(rewritten.withAttrs(null).attrs()).isNull();
    }
}
