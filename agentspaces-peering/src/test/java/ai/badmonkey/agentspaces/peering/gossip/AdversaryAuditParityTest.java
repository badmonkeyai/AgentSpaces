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
package ai.badmonkey.agentspaces.peering.gossip;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Java parity with the TypeScript adversarial review of 2026-11-15 (reviews/RBE-R2-REGRESSION.md §4.1, ADV-TS-1 to
 * ADV-TS-3; SPEC annex I.6): the same hostile partner answers, scripted against the asker's side of {@link TrieAudit}.
 * Every body goes through the strict reader, so each is one the wire would carry. ADV-TS-4 is the partner's side and
 * lives in the trie ({@code HashTrieTest} in agentspaces-space).
 */
class AdversaryAuditParityTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final String STREAM = "space:t";

    private final AtomicLong now = new AtomicLong(1_000_000);
    private final List<Bodies.Digest> sent = new ArrayList<>();
    private final PeerId partner = PeerIdentity.generate().peerId();

    /** An asker's state: nothing at every path unless {@code count} says otherwise, its own lines held by id. */
    static final class Asker implements ReconcilableState {
        final long count;
        final Set<String> held;
        final AtomicInteger linesBuilt = new AtomicInteger();

        Asker(long count, Set<String> held) {
            this.count = count;
            this.held = held;
        }

        @Override
        public byte[] digest() {
            return new byte[0];
        }

        @Override
        public byte[] deltaFor(byte[] remoteDigest) {
            return new byte[0];
        }

        @Override
        public void applyDelta(byte[] delta) {
        }

        @Override
        public byte[] streamHash() {
            return new byte[32];
        }

        @Override
        public ReconcilableState.AuditNode auditNode(byte[] path) {
            return count == 0 ? new ReconcilableState.AuditNode(0, false, new byte[0])
                    : new ReconcilableState.AuditNode(count, true, new byte[384]);
        }

        @Override
        public byte[] auditLines(byte[] path) {
            // What the old comparison built for every lines answer: the whole own set under the path.
            linesBuilt.incrementAndGet();
            return String.join("\n", new java.util.TreeSet<>(held)).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public Set<String> auditHeld(byte[] path, Collection<String> lines) {
            Set<String> out = new HashSet<>();
            for (String line : lines) {
                if (held.contains(line)) {
                    out.add(line);
                }
            }
            return out;
        }
    }

    private TrieAudit audit(ReconcilableState state) {
        GossipBus.FrameSender sender = new GossipBus.FrameSender() {
            @Override
            public void sendRumor(PeerId to, Bodies.Rumor rumor) {
            }

            @Override
            public void sendDigest(PeerId to, Bodies.Digest digest) {
                sent.add(CODEC.fromWire(CODEC.toBytes(digest), Bodies.Digest.class));
            }

            @Override
            public void sendPullResp(PeerId to, Bodies.PullResp resp) {
            }
        };
        TrieAudit audit = new TrieAudit(Map.of(STREAM, state), sender, CODEC, now::get, new Random(7));
        audit.capable(p -> true);
        return audit;
    }

    /** The request bodies sent from {@code mark} on. */
    private List<List<Bodies.TreeNode>> requests(int mark) {
        List<List<Bodies.TreeNode>> out = new ArrayList<>();
        for (Bodies.Digest d : sent.subList(mark, sent.size())) {
            if (d.tree() != null && d.tree().containsKey(STREAM)) {
                out.add(d.tree().get(STREAM));
            }
        }
        return out;
    }

    /** The fetch ids sent from {@code mark} on. */
    private List<String> fetched(int mark) {
        List<String> out = new ArrayList<>();
        for (Bodies.Digest d : sent.subList(mark, sent.size())) {
            if (d.ids() != null && d.ids().containsKey(STREAM)) {
                out.addAll(d.ids().get(STREAM));
            }
        }
        return out;
    }

    /** An inner preimage whose sixteen children each carry one fingerprint and one count. */
    private static byte[] level(byte[] childFingerprint, long count) {
        byte[] nodes = new byte[384];
        for (int c = 0; c < 16; c++) {
            System.arraycopy(childFingerprint, 0, nodes, c * 24, 16);
            long left = count;
            for (int i = 7; i >= 0; i--) {
                nodes[c * 24 + 16 + i] = (byte) left;
                left >>>= 8;
            }
        }
        return nodes;
    }

    /** A consistent two-level partner: a root of 16 children of 1,600, each an inner node of 16 children of 100. */
    private static byte[][] partnerTree() {
        byte[] child = level(new byte[] {5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5}, 100);
        byte[] root = level(Arrays.copyOf(Digests.sha256(child), 16), 1_600);
        return new byte[][] {root, child};
    }

    private void onTree(TrieAudit audit, Bodies.TreeNode... nodes) {
        audit.onTree(partner, Map.of(STREAM, List.of(nodes)), null, false);
    }

    /**
     * ADV-TS-2: a hostile partner sends a root in each of 200 bodies within one opening and never answers a request.
     * Each root started the descent afresh with its exchange count at zero, so the 64-exchange bound never held and
     * every root drew a fresh body of requests. One opening admits one root per stream.
     */
    @Test
    void aRootInEveryBodyDoesNotKeepTheAskerDescendingPastOneOpeningsBound() {
        TrieAudit audit = audit(new Asker(0, Set.of()));
        Bodies.TreeNode root = Bodies.TreeNode.inner(new byte[0], partnerTree()[0]);
        audit.opening(partner);
        for (int i = 0; i < 200; i++) {
            onTree(audit, root);
        }
        assertThat(requests(0)).as("one opening admits one root per stream").hasSize(1);
        assertThat(audit.refusals().get("unsolicited-answer")).isEqualTo(199L);

        // Each new opening is owed one root, and an opening whose root never came lends no credit to a later burst.
        for (int i = 0; i < 10; i++) {
            audit.opening(partner);
        }
        int mark = sent.size();
        for (int i = 0; i < 20; i++) {
            onTree(audit, root);
        }
        assertThat(requests(mark)).as("at most the last openings are owed a root").hasSize(TrieAudit.ROOTS_OWED_MAX);
    }

    /** Two openings in flight, a round and a continuation's just after it, still read both roots with no refusal. */
    @Test
    void twoOpeningsInFlightStillReadBothRoots() {
        TrieAudit audit = audit(new Asker(0, Set.of()));
        Bodies.TreeNode root = Bodies.TreeNode.inner(new byte[0], partnerTree()[0]);
        audit.opening(partner);
        audit.opening(partner);
        onTree(audit, root);
        onTree(audit, root);
        assertThat(requests(0)).hasSize(2);
        assertThat(audit.refusals()).doesNotContainKey("unsolicited-answer");
    }

    /** An opening that idled out is owed nothing. */
    @Test
    void anOpeningThatIdledOutAdmitsNoRoot() {
        TrieAudit audit = audit(new Asker(0, Set.of()));
        audit.opening(partner);
        now.addAndGet(TrieAudit.IDLE_MILLIS);
        onTree(audit, Bodies.TreeNode.inner(new byte[0], partnerTree()[0]));
        assertThat(requests(0)).isEmpty();
        assertThat(audit.refusals().get("unsolicited-answer")).isEqualTo(1L);
    }

    /**
     * ADV-TS-1: after a root, the partner never answers the 16 requests in flight and sends 200 bodies, each an answer
     * at a path the asker never asked. Each body re-sent every request in flight, uncounted, past the 64-exchange
     * bound. A body whose every answer was refused moves nothing; the descent still takes the partner's real answers.
     */
    @Test
    void bodiesOfRefusedAnswersMidDescentDrawNoRequests() {
        TrieAudit audit = audit(new Asker(0, Set.of()));
        byte[][] tree = partnerTree();
        audit.opening(partner);
        onTree(audit, Bodies.TreeNode.inner(new byte[0], tree[0]));
        List<List<Bodies.TreeNode>> first = requests(0);
        assertThat(first).as("the root drew the first requests").hasSize(1);
        assertThat(first.get(0)).hasSize(16);

        int mark = sent.size();
        for (int i = 0; i < 200; i++) {
            byte[] path = {(byte) (i % 16), (byte) ((i >> 4) % 16), 3};
            onTree(audit, Bodies.TreeNode.lines(path, "e:x" + i + "=1"));
        }
        assertThat(requests(mark)).as("request bodies 200 refused bodies drew").isEmpty();
        assertThat(audit.refusals().get("unsolicited-answer")).isEqualTo(200L);

        // The descent is still there for the partner's real answers, which move it on.
        List<Bodies.TreeNode> answers = new ArrayList<>();
        for (Bodies.TreeNode request : first.get(0)) {
            answers.add(Bodies.TreeNode.inner(request.path(), tree[1]));
        }
        mark = sent.size();
        onTree(audit, answers.toArray(new Bodies.TreeNode[0]));
        assertThat(requests(mark)).as("the real answers drew the next level").hasSize(1);
        assertThat(requests(mark).get(0)).hasSize(Bodies.Digest.TREE_NODES_MAX);
        assertThat(audit.refusals()).doesNotContainKeys("fingerprint-mismatch", "shape-mismatch");
    }

    /**
     * ADV-TS-3: a member of many entries meets a partner of few, whose root is a leaf of lines. Comparing the lines
     * built the asker's whole own set under the path every round. Each line is looked up in the one slot it names,
     * so the cost follows the answer; the ids fetched are the ones the old comparison named.
     */
    @Test
    void comparingAPartnersLinesReadsWhatTheAnswerNamesNotTheOwnSet() {
        Asker asker = new Asker(200_000, Set.of("e:a=1", "e:b=2", "c:b=9"));
        TrieAudit audit = audit(asker);
        audit.opening(partner);
        onTree(audit, Bodies.TreeNode.lines(new byte[0], "c:b=9\ne:a=1\ne:b=3\ne:d=4\nnot-a-line"));
        assertThat(asker.linesBuilt.get()).as("own line sets built to compare one small answer").isZero();
        assertThat(fetched(0)).as("ids whose line the asker lacks, in answer order").containsExactly("b", "d");
    }
}
