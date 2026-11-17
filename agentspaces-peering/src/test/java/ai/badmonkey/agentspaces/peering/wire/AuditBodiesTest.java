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
import com.fasterxml.jackson.annotation.JsonInclude;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code DIGEST} and {@code PULL_RESP} fields of SPEC annex I.6 on the wire (srad/replication-by-entry.md RE-T-01,
 * RE-F-01, RE-F-02): each is omitted when absent, so the bodies of v0.1.34 keep their bytes, and the strict reader
 * refuses every carrier that breaks a form rule. Each malformed carrier is written by a twin of the records with no
 * form rules, beside a well-formed carrier written the same way, so the refusal is the rule's and not the encoding's.
 */
class AuditBodiesTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();

    private static byte[] nodesOf(long... counts) {
        byte[] nodes = new byte[384];
        for (int c = 0; c < 16; c++) {
            long count = c < counts.length ? counts[c] : 0;
            if (count > 0) {
                nodes[c * 24] = (byte) (c + 1); // a fingerprint that is not all zeros
            }
            for (int i = 7; i >= 0; i--) {
                nodes[c * 24 + 16 + i] = (byte) count;
                count >>>= 8;
            }
        }
        return nodes;
    }

    /** The records' twins with no form rules, so a test can write any carrier in exactly the records' encoding. */
    private record RawNode(byte[] path,
                           @JsonInclude(JsonInclude.Include.NON_NULL) Long count,
                           @JsonInclude(JsonInclude.Include.NON_NULL) byte[] nodes,
                           @JsonInclude(JsonInclude.Include.NON_NULL) String lines) {
    }

    private record RawDigest(Map<String, byte[]> digests,
                             @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, byte[]> hashes,
                             @JsonInclude(JsonInclude.Include.NON_NULL) List<String> want,
                             @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, List<RawNode>> tree,
                             @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, List<String>> ids,
                             @JsonInclude(JsonInclude.Include.NON_NULL) Long budget,
                             @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, byte[]> sketch) {
    }

    private record RawPullResp(Map<String, byte[]> deltas,
                               @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, Long> remainder) {
    }

    private record RawExtra(Map<String, byte[]> digests, Map<String, byte[]> trees) {
    }

    /** A tree node with the fields given by name. */
    private static RawNode node(byte[] path, Object... fields) {
        Long count = null;
        byte[] nodes = null;
        String lines = null;
        for (int i = 0; i < fields.length; i += 2) {
            switch ((String) fields[i]) {
                case "count" -> count = (Long) fields[i + 1];
                case "nodes" -> nodes = (byte[]) fields[i + 1];
                case "lines" -> lines = (String) fields[i + 1];
                default -> throw new IllegalArgumentException((String) fields[i]);
            }
        }
        return new RawNode(path, count, nodes, lines);
    }

    private static byte[] treeBody(List<RawNode> nodes) {
        return CODEC.toBytes(new RawDigest(null, null, null, Map.of("space:a", nodes), null, null, null));
    }

    private static byte[] idsBody(List<String> ids) {
        return CODEC.toBytes(new RawDigest(null, null, null, null, Map.of("space:a", ids), null, null));
    }

    private static byte[] budgetBody(long budget) {
        return CODEC.toBytes(new RawDigest(null, null, null, null, null, budget, null));
    }

    private static void refused(byte[] carrier) {
        assertThatThrownBy(() -> CODEC.fromWire(carrier, Bodies.Digest.class))
                .as("a carrier that breaks a form rule is undecodable")
                .isInstanceOf(RuntimeException.class);
    }

    private static Bodies.Digest decoded(byte[] carrier) {
        return CODEC.fromWire(carrier, Bodies.Digest.class);
    }

    @Test
    void aBodyWithoutTheNewFieldsKeepsTheBytesOfV0134() {
        Map<String, byte[]> digests = Map.of("space:a", "e:x=1\n".getBytes(StandardCharsets.UTF_8));
        byte[] old = CODEC.toBytes(new Bodies.Digest(digests));
        assertThat(CODEC.toBytes(new Bodies.Digest(digests, null, null, null, null, null, null))).isEqualTo(old);
        assertThat(old).as("the three fields of v0.1.17, absent ones omitted")
                .isEqualTo(CODEC.toBytes(new RawDigest(digests, null, null, null, null, null, null)));

        byte[] hashed = CODEC.toBytes(new Bodies.Digest(null, Map.of("space:a", new byte[32]), null));
        assertThat(new String(hashed, StandardCharsets.ISO_8859_1)).doesNotContain("tree", "ids", "budget",
                "sketch");
        assertThat(CODEC.toBytes(new Bodies.PullResp(Map.of("space:a", new byte[]{1}))))
                .isEqualTo(CODEC.toBytes(new RawPullResp(Map.of("space:a", new byte[]{1}), null)));
    }

    @Test
    void everyNewFieldTravelsAndReadsBack() {
        Bodies.Digest body = new Bodies.Digest(null, null, List.of("peers"),
                Map.of("space:a", List.of(Bodies.TreeNode.request(new byte[]{3, 15}, 7),
                        Bodies.TreeNode.inner(new byte[0], nodesOf(20, 13)),
                        Bodies.TreeNode.lines(new byte[]{1}, "c:x=2\ne:x=1"),
                        Bodies.TreeNode.lines(new byte[]{2}, ""))),
                Map.of("space:a", List.of("x", "y")), 1_048_576L, Map.of("space:a", new byte[]{9}));
        Bodies.Digest back = decoded(CODEC.toBytes(body));
        assertThat(back.carriesAudit()).isTrue();
        assertThat(back.tree().get("space:a")).hasSize(4);
        assertThat(back.tree().get("space:a").get(0).count()).isEqualTo(7);
        assertThat(back.tree().get("space:a").get(2).lines()).isEqualTo("c:x=2\ne:x=1");
        assertThat(back.ids()).containsEntry("space:a", List.of("x", "y"));
        assertThat(back.budget()).isEqualTo(1_048_576L);

        Bodies.PullResp resp = new Bodies.PullResp(Map.of("space:a", new byte[]{1}), Map.of("space:a", 3L));
        assertThat(CODEC.fromWire(CODEC.toBytes(resp), Bodies.PullResp.class).remainder())
                .containsEntry("space:a", 3L);
    }

    @Test
    void nodesHoldExactly384BytesSummingAbove32WithEmptyChildrenZeroed() {
        assertThat(decoded(treeBody(List.of(node(new byte[0], "nodes", nodesOf(20, 13)))))).isNotNull();
        refused(treeBody(List.of(node(new byte[0], "nodes", java.util.Arrays.copyOf(nodesOf(20, 13), 383)))));
        refused(treeBody(List.of(node(new byte[0], "nodes", java.util.Arrays.copyOf(nodesOf(20, 13), 385)))));
        refused(treeBody(List.of(node(new byte[0], "nodes", nodesOf(16, 16)))));
        byte[] stray = nodesOf(20, 13);
        stray[2 * 24 + 5] = 1; // child 2 has the count 0 and a fingerprint that is not zeros
        refused(treeBody(List.of(node(new byte[0], "nodes", stray))));
    }

    @Test
    void linesAreNonEmptyFreeOfControlCharactersAndStrictlyAscending() {
        assertThat(decoded(treeBody(List.of(node(new byte[0], "lines", "c:x=2\ne:x=1"))))).isNotNull();
        assertThat(decoded(treeBody(List.of(node(new byte[0], "lines", ""))))).isNotNull();
        refused(treeBody(List.of(node(new byte[0], "lines", "\nc:x=2\ne:x=1"))));
        refused(treeBody(List.of(node(new byte[0], "lines", "c:x=2\n\ne:x=1"))));
        refused(treeBody(List.of(node(new byte[0], "lines", "c:x=2\ne:x=1\n"))));
        refused(treeBody(List.of(node(new byte[0], "lines", "c:x=2\ne:x\t=1"))));
        refused(treeBody(List.of(node(new byte[0], "lines", "c:x=2\ne:x\u007F=1"))));
        refused(treeBody(List.of(node(new byte[0], "lines", "e:x=1\nc:x=2"))));
        refused(treeBody(List.of(node(new byte[0], "lines", "e:x=1\ne:x=1"))));
    }

    @Test
    void aPathHoldsAtMost64NibblesAndANodeExactlyOneOfItsForms() {
        assertThat(decoded(treeBody(List.of(node(new byte[64], "count", 3L))))).isNotNull();
        refused(treeBody(List.of(node(new byte[65], "count", 3L))));
        refused(treeBody(List.of(node(new byte[]{0x10}, "count", 3L))));
        refused(treeBody(List.of(node(new byte[]{1}))));
        refused(treeBody(List.of(node(new byte[]{1}, "count", 3L, "lines", ""))));
        refused(treeBody(List.of(node(new byte[]{1}, "count", -1L))));
        refused(treeBody(List.of(node(new byte[]{1}, "nodes", nodesOf(20, 13), "lines", ""))));
    }

    @Test
    void aRequestCarriesAtMost256NodesAndAFetchAtMost4096DistinctIds() {
        List<RawNode> nodes = new ArrayList<>();
        for (int i = 0; i < 256; i++) {
            nodes.add(node(new byte[]{(byte) (i >> 4), (byte) (i & 15)}, "count", 0L));
        }
        assertThat(decoded(treeBody(nodes))).isNotNull();
        nodes.add(node(new byte[]{0}, "count", 0L));
        refused(treeBody(nodes));

        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 4096; i++) {
            ids.add("id-" + i);
        }
        assertThat(decoded(idsBody(ids))).isNotNull();
        List<String> more = new ArrayList<>(ids);
        more.add("id-4096");
        refused(idsBody(more));
        refused(idsBody(List.of("x", "y", "x")));
        refused(idsBody(List.of("")));
    }

    @Test
    void aBudgetIsPositiveAndARemainderNeverNegative() {
        assertThat(decoded(budgetBody(1L)).budget()).isEqualTo(1L);
        refused(budgetBody(0L));
        refused(budgetBody(-5L));
        assertThatThrownBy(() -> CODEC.fromWire(CODEC.toBytes(new RawPullResp(Map.of(), Map.of("space:a", -1L))),
                Bodies.PullResp.class)).isInstanceOf(RuntimeException.class);
        assertThat(CODEC.fromWire(CODEC.toBytes(new RawPullResp(Map.of(), Map.of("space:a", 0L))),
                Bodies.PullResp.class).remainder()).containsEntry("space:a", 0L);
    }

    @Test
    void aFieldTheSchemaDoesNotNameStillRefusesTheBody() {
        refused(CODEC.toBytes(new RawExtra(null, Map.of())));
    }
}
