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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Phase 4 wire adversary (srad/replication-by-entry.md §3.2; SPEC annex I.6): a hostile partner's answers against the
 * asker's descent, and a hostile asker's requests against the partner. Every body goes through the strict reader, so
 * each is one the wire would carry.
 */
class AdversaryAuditDescentTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final String STREAM = "space:t";

    private final AtomicLong now = new AtomicLong(1_000_000);

    /** A state that holds nothing at every path, or a fixed large node when {@code big}. */
    static class Stub implements ReconcilableState {
        final ReconcilableState.AuditNode node;
        final byte[] lines;
        final AtomicInteger linesBuilt = new AtomicInteger();

        Stub(ReconcilableState.AuditNode node, byte[] lines) {
            this.node = node;
            this.lines = lines;
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
            return Digests.sha256(node.preimage());
        }

        @Override
        public ReconcilableState.AuditNode auditNode(byte[] path) {
            if (path.length > 64) {
                // As HashTrie.node and every real trie do.
                throw new IllegalArgumentException("a path holds at most 64 nibbles: " + path.length);
            }
            return node;
        }

        @Override
        public byte[] auditLines(byte[] path) {
            linesBuilt.incrementAndGet();
            return lines;
        }
    }

    record Sent(PeerId to, Object body) {
    }

    private GossipBus bus(PeerId self, List<Sent> out, Stub state) {
        GossipBus.FrameSender sender = new GossipBus.FrameSender() {
            @Override
            public void sendRumor(PeerId to, Bodies.Rumor rumor) {
            }

            @Override
            public void sendDigest(PeerId to, Bodies.Digest digest) {
                out.add(new Sent(to, CODEC.fromWire(CODEC.toBytes(digest), Bodies.Digest.class)));
            }

            @Override
            public void sendPullResp(PeerId to, Bodies.PullResp resp) {
                out.add(new Sent(to, CODEC.fromWire(CODEC.toBytes(resp), Bodies.PullResp.class)));
            }
        };
        GossipBus bus = new GossipBus(n -> List.of(), sender, CODEC, 3, 6, now::get, new java.util.Random(7));
        bus.audit(p -> true);
        bus.reconcile(STREAM, state);
        return bus;
    }

    private static Stub empty() {
        return new Stub(new ReconcilableState.AuditNode(0, false, new byte[0]), new byte[0]);
    }

    /** An inner preimage whose sixteen children all carry one fingerprint and the count 1000. */
    private static byte[] innerOf(byte[] childFingerprint) {
        byte[] nodes = new byte[384];
        for (int c = 0; c < 16; c++) {
            System.arraycopy(childFingerprint, 0, nodes, c * 24, 16);
            long count = 1000;
            for (int i = 7; i >= 0; i--) {
                nodes[c * 24 + 16 + i] = (byte) count;
                count >>>= 8;
            }
        }
        return nodes;
    }

    /** One preimage per depth, 0 to 64, each child's fingerprint that of the next depth's: a consistent hostile tree. */
    private static byte[][] hostileChain() {
        byte[][] byDepth = new byte[65][];
        byDepth[64] = innerOf(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        for (int d = 63; d >= 0; d--) {
            byDepth[d] = innerOf(Arrays.copyOf(Digests.sha256(byDepth[d + 1]), 16));
        }
        return byDepth;
    }

    private static Bodies.Digest answers(Bodies.Digest requests, byte[][] chain) {
        List<Bodies.TreeNode> nodes = new ArrayList<>();
        for (Bodies.TreeNode request : requests.tree().get(STREAM)) {
            nodes.add(Bodies.TreeNode.inner(request.path(), chain[request.path().length]));
        }
        return new Bodies.Digest(null, null, null, Map.of(STREAM, nodes), null, null, null);
    }

    private static Bodies.Digest lastRequests(List<Sent> out) {
        for (int i = out.size() - 1; i >= 0; i--) {
            if (out.get(i).body() instanceof Bodies.Digest d && d.tree() != null && d.tree().containsKey(STREAM)) {
                return d;
            }
        }
        return null;
    }

    /**
     * A hostile partner answers every request with an inner node whose sixteen children all differ from the asker's
     * and all hash right. Nothing on the asker's side bounds the descent: it keeps asking 256 nodes per exchange while
     * its queue of pending children grows by about 3,840 per exchange, for as long as the partner answers. The
     * partner's own 64-exchange session bound protects only the partner. A bounded asker stops within 64 exchanges.
     */
    @Test
    void aHostilePartnerCannotKeepTheAskerDescendingForever() {
        PeerId asker = PeerIdentity.generate().peerId();
        PeerId hostile = PeerIdentity.generate().peerId();
        List<Sent> out = new ArrayList<>();
        GossipBus bus = bus(asker, out, empty());
        byte[][] chain = hostileChain();

        bus.antiEntropyWith(hostile); // the asker opens a round, so the root is expected
        bus.onDigest(hostile, new Bodies.Digest(null, null, null,
                Map.of(STREAM, List.of(Bodies.TreeNode.inner(new byte[0], chain[0]))), null, null, null));
        int exchanges = 0;
        for (int i = 0; i < 200; i++) {
            Bodies.Digest requests = lastRequests(out);
            if (requests == null) {
                break;
            }
            out.clear();
            exchanges++;
            bus.onDigest(hostile, answers(requests, chain));
        }
        assertThat(exchanges).as("request bodies the asker sent to one hostile partner in one descent")
                .isLessThanOrEqualTo(TrieAudit.EXCHANGES_MAX);
    }

    /**
     * A hostile partner leads the asker one level per exchange, through consistent fingerprints, to an inner node at
     * path length 64. A node at 64 nibbles is a leaf whatever its count (annex I.6), so such an answer is malformed;
     * the asker must drop and count it rather than throw while it builds a 65-nibble child path.
     */
    @Test
    void anInnerNodeAtSixtyFourNibblesIsDroppedNotThrown() {
        PeerId asker = PeerIdentity.generate().peerId();
        PeerId hostile = PeerIdentity.generate().peerId();
        List<Sent> out = new ArrayList<>();
        GossipBus bus = bus(asker, out, empty());
        // A chain where only child 0 is set, 40 slots, so each exchange asks exactly one node one level deeper.
        byte[][] chain = new byte[65][];
        chain[64] = single(new byte[] {9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9});
        for (int d = 63; d >= 0; d--) {
            chain[d] = single(Arrays.copyOf(Digests.sha256(chain[d + 1]), 16));
        }
        bus.antiEntropyWith(hostile);
        bus.onDigest(hostile, new Bodies.Digest(null, null, null,
                Map.of(STREAM, List.of(Bodies.TreeNode.inner(new byte[0], chain[0]))), null, null, null));
        for (int i = 0; i < 63; i++) {
            Bodies.Digest requests = lastRequests(out);
            assertThat(requests).as("the asker asks level " + (i + 1)).isNotNull();
            out.clear();
            bus.onDigest(hostile, answers(requests, chain));
        }
        // The asker's own exchange bound now ends the descent at its 64th answer, the one at depth 63, so the chain
        // never reaches a request at 64 nibbles; the descent ends counted instead. Were a request at 64 nibbles ever
        // sent, its inner answer must still be dropped and counted, not thrown.
        Bodies.Digest last = lastRequests(out);
        if (last == null) {
            assertThat(bus.auditSignals().refusals()).as("the bound ended the descent")
                    .containsKey("descent-capped");
            return;
        }
        assertThat(last.tree().get(STREAM).get(0).path()).hasSize(64);
        assertThatCode(() -> bus.onDigest(hostile, answers(last, chain)))
                .as("an inner node at 64 nibbles is a malformed answer, dropped and counted")
                .doesNotThrowAnyException();
        assertThat(bus.auditSignals().refusals()).containsKey("shape-mismatch");
    }

    /**
     * The bound holds when the hostile tree is consistent too: every count the sum of its children's, every fingerprint
     * that of the next depth's preimage, branching in two for ten levels and then running on one child each to the
     * deepest path. Nothing in it is refused, so only the asker's own exchange bound ends the descent.
     */
    @Test
    void aConsistentHostileTreeStillEndsAtTheExchangeBound() {
        PeerId asker = PeerIdentity.generate().peerId();
        PeerId hostile = PeerIdentity.generate().peerId();
        List<Sent> out = new ArrayList<>();
        GossipBus bus = bus(asker, out, empty());
        byte[][] chain = new byte[65][];
        long[] counts = new long[65];
        chain[64] = branch(new byte[] {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, 40, 1);
        counts[64] = 40;
        for (int d = 63; d >= 0; d--) {
            int children = d < 10 ? 2 : 1;
            chain[d] = branch(Arrays.copyOf(Digests.sha256(chain[d + 1]), 16), counts[d + 1], children);
            counts[d] = counts[d + 1] * children;
        }
        bus.antiEntropyWith(hostile);
        bus.onDigest(hostile, new Bodies.Digest(null, null, null,
                Map.of(STREAM, List.of(Bodies.TreeNode.inner(new byte[0], chain[0]))), null, null, null));
        int exchanges = 0;
        long nodesAsked = 0;
        for (int i = 0; i < 200; i++) {
            Bodies.Digest requests = lastRequests(out);
            if (requests == null) {
                break;
            }
            out.clear();
            exchanges++;
            nodesAsked += requests.tree().get(STREAM).size();
            bus.onDigest(hostile, answers(requests, chain));
        }
        assertThat(exchanges).as("request bodies one descent sent").isLessThan(TrieAudit.EXCHANGES_MAX);
        assertThat(nodesAsked).isLessThanOrEqualTo((long) TrieAudit.EXCHANGES_MAX * Bodies.Digest.TREE_NODES_MAX);
        assertThat(bus.auditSignals().refusals()).containsKey("descent-capped")
                .doesNotContainKeys("shape-mismatch", "fingerprint-mismatch");
    }

    /** An inner preimage whose first {@code children} children carry one fingerprint and one count. */
    private static byte[] branch(byte[] fingerprint, long count, int children) {
        byte[] nodes = new byte[384];
        for (int c = 0; c < children; c++) {
            System.arraycopy(fingerprint, 0, nodes, c * 24, 16);
            long left = count;
            for (int i = 7; i >= 0; i--) {
                nodes[c * 24 + 16 + i] = (byte) left;
                left >>>= 8;
            }
        }
        return nodes;
    }

    private static byte[] single(byte[] fingerprint) {
        byte[] nodes = new byte[384];
        System.arraycopy(fingerprint, 0, nodes, 0, 16);
        nodes[16 + 7] = 40;
        return nodes;
    }

    /**
     * A hostile asker opens a session and sends one body of 256 requests for the root path with the count 0, under a
     * budget a quarter of the partner's count allows but its lines pass. The partner builds every line of the space
     * for each of the 256 requests and discards each, since nodes answer them. Repeated paths in one body, or at least
     * the line building, should be bounded; one body should not cost 256 walks of the whole space.
     */
    @Test
    void oneRequestBodyDoesNotDrawHundredsOfWholeSpaceLineBuilds() {
        PeerId partner = PeerIdentity.generate().peerId();
        PeerId hostile = PeerIdentity.generate().peerId();
        List<Sent> out = new ArrayList<>();
        // 100,000 slots: count * 4 = 400,000 <= 1 MiB, and 1.1 MiB of lines pass it.
        byte[] preimage = innerOf(new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1});
        Stub big = new Stub(new ReconcilableState.AuditNode(100_000, true, preimage), new byte[1_150_000]);
        GossipBus bus = bus(partner, out, big);

        bus.onDigest(hostile, new Bodies.Digest(null, Map.of(STREAM, new byte[32]), null));
        assertThat(out).as("the opening drew a root and a session").isNotEmpty();
        List<Bodies.TreeNode> asks = new ArrayList<>();
        for (int i = 0; i < 256; i++) {
            asks.add(Bodies.TreeNode.request(new byte[0], 0));
        }
        out.clear();
        bus.onDigest(hostile, new Bodies.Digest(null, null, null, Map.of(STREAM, asks), null, 1L << 20, null));
        assertThat(big.linesBuilt.get()).as("whole-space line builds one request body drew").isLessThanOrEqualTo(2);
    }
    /**
     * A member that has not yet read its partner's card opens a round with text hashes, while the partner, which holds
     * the asker's card, compares them with its trie hashes. Two replicas holding the same space then never compare
     * equal: the partner answers a root, opens a session for an asker that never asked, and the asker drops the root as
     * unsolicited. The round does nothing for the space until the asker reads the card. Here the states are identical,
     * so a sound round sends nothing back at all.
     */
    @Test
    void anAskerOnTheTextFormIsNotAnsweredWithRootsForAnEqualSpace() {
        PeerId a = PeerIdentity.generate().peerId();
        PeerId b = PeerIdentity.generate().peerId();
        byte[] preimage = innerOf(new byte[] {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2});
        List<Sent> toB = new ArrayList<>();
        List<Sent> toA = new ArrayList<>();
        Stub same = new Stub(new ReconcilableState.AuditNode(16_000, true, preimage), new byte[0]);
        GossipBus busA = bus(a, toB, same);
        GossipBus busB = bus(b, toA, same);
        busA.audit(p -> false);       // A has not read B's card yet
        busA.hashedDigests(p -> true); // but knows B reads the v0.1.17 hashed form
        busB.audit(p -> true);         // B holds A's card, which carries wire.ae=2

        busA.antiEntropyWith(b);
        for (Sent s : toB) {
            busB.onDigest(a, (Bodies.Digest) s.body());
        }
        assertThat(toA).as("an equal space draws no answer").isEmpty();
    }

    /**
     * The same card race with spaces that differ: the partner answers a root, and the asker, which opened the round in
     * the text form, answers it by its full text digest as it answers a want, so the stream is reconciled within the
     * round rather than dropped as unsolicited until the cards spread.
     */
    @Test
    void anAskerOnTheTextFormAnswersARootByItsTextDigest() {
        PeerId a = PeerIdentity.generate().peerId();
        PeerId b = PeerIdentity.generate().peerId();
        List<Sent> toB = new ArrayList<>();
        List<Sent> toA = new ArrayList<>();
        Stub mine = new Stub(new ReconcilableState.AuditNode(16_000, true,
                innerOf(new byte[] {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3})), new byte[0]) {
            @Override
            public byte[] digest() {
                return "e:x=1\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
        };
        Stub theirs = new Stub(new ReconcilableState.AuditNode(16_000, true,
                innerOf(new byte[] {4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4})), new byte[0]);
        GossipBus busA = bus(a, toB, mine);
        GossipBus busB = bus(b, toA, theirs);
        busA.audit(p -> false);
        busA.hashedDigests(p -> true);
        busB.audit(p -> true);

        busA.antiEntropyWith(b);
        List<Sent> opening = new ArrayList<>(toB);
        toB.clear();
        for (Sent s : opening) {
            busB.onDigest(a, (Bodies.Digest) s.body());
        }
        assertThat(toA).as("b answered a root").hasSize(1);
        busA.onDigest(b, (Bodies.Digest) toA.get(0).body());
        assertThat(toB).as("a answered the root by its text digest").hasSize(1);
        Bodies.Digest pull = (Bodies.Digest) toB.get(0).body();
        assertThat(pull.digests()).containsKey(STREAM);
        assertThat(pull.carriesAudit()).as("nothing of annex I.6 toward a card a has not read").isFalse();
        assertThat(busA.auditSignals().refusals()).containsKey("text-fallback")
                .doesNotContainKey("unsolicited-answer");
    }

    /** A tree list holding a null node is refused by the strict reader, not handed to the audit. */
    @Test
    void aNullTreeNodeDoesNotDecode() {
        java.util.Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("digests", null);
        java.util.List<Object> nodes = new java.util.ArrayList<>();
        nodes.add(null);
        body.put("tree", java.util.Map.of(STREAM, nodes));
        byte[] bytes = CODEC.toBytes(body);
        Bodies.Digest read = null;
        try {
            read = CODEC.fromWire(bytes, Bodies.Digest.class);
        } catch (RuntimeException expected) {
            return;
        }
        Bodies.Digest decoded = read;
        PeerId partner = PeerIdentity.generate().peerId();
        GossipBus bus = bus(partner, new ArrayList<>(), empty());
        assertThatCode(() -> bus.onDigest(PeerIdentity.generate().peerId(), decoded))
                .as("a body the strict reader let through is handled without throwing").doesNotThrowAnyException();
    }
}
