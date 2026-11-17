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
package ai.badmonkey.agentspaces.peering.wire;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The annex I.6 body vectors of golden.json (srad/replication-by-entry.md RE-L-05, second half): each carrier to accept
 * decodes under the strict reader to what it names and re-encodes to its own bytes, the signed fetch frame verifies,
 * the root's 384 bytes hash to the annex's printed stream hash, and each carrier to refuse is refused by the reader a
 * {@code wire.ae=2} member uses. The trie's own vectors are in audit-vectors.json (agentspaces-space).
 */
class AuditBodyVectorsTest {

    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static final boolean GOLDEN_REQUIRED = Boolean.getBoolean("golden.required");
    private static final CborCodec codec = CborCodec.defaultCodec();
    private static final String STREAM = "space:tasks";
    private static Map<String, Object> golden;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void load() throws Exception {
        Path found = GOLDEN_PATHS.stream().filter(Files::exists).findFirst().orElse(null);
        if (GOLDEN_REQUIRED) {
            assertThat(found).as("golden.json is required but none of " + GOLDEN_PATHS + " exists").isNotNull();
        }
        assumeTrue(found != null, "golden.json not present");
        golden = new ObjectMapper().readValue(found.toFile(), Map.class);
        if (GOLDEN_REQUIRED) {
            assertThat(golden).containsKey("digest_tree_request_body_cbor");
        }
        assumeTrue(golden.containsKey("digest_tree_request_body_cbor"), "golden.json predates the audit vectors");
    }

    private static byte[] hex(String name) {
        return HexFormat.of().parseHex((String) golden.get(name));
    }

    private static <T> T read(String name, Class<T> type) {
        T body = codec.fromWire(hex(name), type);
        assertThat(codec.toBytes(body)).as(name + " re-encodes to its own bytes").isEqualTo(hex(name));
        return body;
    }

    @Test
    void theTreeBodiesCarryRequestsAndBothFormsOfAnswer() throws Exception {
        Bodies.Digest request = read("digest_tree_request_body_cbor", Bodies.Digest.class);
        assertThat(request.digests()).as("written null, as annex I.1 has it").isNull();
        assertThat(request.budget()).isEqualTo(1L << 20);
        assertThat(request.tree().get(STREAM)).extracting(Bodies.TreeNode::count).containsExactly(12L, 40L);
        assertThat(request.tree().get(STREAM).get(1).path()).containsExactly(0x0a, 0x03);
        assertThat(request.carriesAudit()).isTrue();

        Bodies.Digest nodes = read("digest_tree_nodes_body_cbor", Bodies.Digest.class);
        Bodies.TreeNode root = nodes.tree().get(STREAM).get(0);
        assertThat(root.path()).isEmpty();
        assertThat(root.nodes()).hasSize(384);
        assertThat(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(root.nodes())))
                .as("annex I.6's stream hash of the 33 slots e00 to e32")
                .isEqualTo("988d034b3abe2292c17478e2f646d845710c3ce40c30dfd9732598be8c2560cb");
        assertThat(nodes.budget()).as("an answer carries no budget").isNull();

        Bodies.Digest lines = read("digest_tree_lines_body_cbor", Bodies.Digest.class);
        Bodies.TreeNode leaf = lines.tree().get(STREAM).get(0);
        assertThat(leaf.lines()).isEqualTo("c:a=2\ne:a=1");
        assertThat(HexFormat.of().formatHex(Arrays.copyOf(MessageDigest.getInstance("SHA-256")
                .digest(leaf.lines().getBytes(StandardCharsets.UTF_8)), 16)))
                .as("annex I.6's fingerprint of the slot a").isEqualTo("b5af992c6ac029d376fa9dec750a5c4b");
        assertThat(lines.tree().get(STREAM).get(1).lines()).as("an empty node answers the empty text").isEmpty();
    }

    @Test
    void theFetchTheBudgetedGetSpaceAndTheRemainderDecode() {
        Bodies.Digest ids = read("digest_ids_body_cbor", Bodies.Digest.class);
        assertThat(ids.ids().get(STREAM)).containsExactly("11111111-2222-3333-4444-555555555555", "entry-150");
        assertThat(ids.budget()).isEqualTo(65_536L);
        Optional<Envelope> frame = new WireCodec(codec).decode(hex("digest_ids_frame_cbor"));
        assertThat(frame).as("the frame verifies").isPresent();
        assertThat(frame.get().kind()).isEqualTo(Envelope.Kind.DIGEST);
        assertThat(frame.get().body()).isEqualTo(hex("digest_ids_body_cbor"));

        Bodies.Digest getSpace = read("digest_get_space_budget_body_cbor", Bodies.Digest.class);
        assertThat(getSpace.digests().get(STREAM)).as("the empty digest asks for the whole space").isEmpty();
        assertThat(getSpace.budget()).isEqualTo(1L << 20);

        Bodies.PullResp whole = read("pull_resp_body_cbor", Bodies.PullResp.class);
        assertThat(whole.remainder()).as("an unbudgeted answer, as before v0.1.35").isNull();
        Bodies.PullResp cut = read("pull_resp_remainder_body_cbor", Bodies.PullResp.class);
        assertThat(cut.remainder()).containsEntry(STREAM, 3L);
        assertThat(cut.deltas().get(STREAM)).isEqualTo(whole.deltas().get(STREAM));
        assertThat(hex("pull_resp_remainder_body_cbor")).as("the remainder only adds bytes after the deltas")
                .startsWith(Arrays.copyOf(hex("pull_resp_body_cbor"), hex("pull_resp_body_cbor").length - 1));
    }

    @Test
    void everyCarrierThatBreaksAFormRuleIsRefused() {
        List<String> digests = List.of("adv_digest_tree_path_nibble_body", "adv_digest_tree_path_long_body",
                "adv_digest_tree_nodes_length_body", "adv_digest_tree_nodes_few_body",
                "adv_digest_tree_two_forms_body", "adv_digest_tree_no_form_body", "adv_digest_tree_lines_order_body",
                "adv_digest_tree_lines_empty_line_body", "adv_digest_ids_twice_body", "adv_digest_budget_zero_body");
        for (String name : digests) {
            assertThatThrownBy(() -> codec.fromWire(hex(name), Bodies.Digest.class)).as(name)
                    .isInstanceOf(RuntimeException.class);
        }
        assertThatThrownBy(() -> codec.fromWire(hex("adv_pull_resp_remainder_negative_body"), Bodies.PullResp.class))
                .isInstanceOf(RuntimeException.class);
        assertThat(golden.keySet().stream().filter(k -> k.startsWith("adv_digest_") || k.startsWith("adv_pull_resp_")))
                .as("every annex I.6 carrier to refuse is checked here").hasSize(digests.size() + 1);
    }
}
