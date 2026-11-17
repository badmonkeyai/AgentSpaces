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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The hint {@code wire.ae=2} and the stale-card rules (srad/replication-by-entry.md RE-T-02, RE-T-08, RE-Q-REL-07,
 * RE-Q-MNT-03): a node advertises the hint as a value in its hints map, reads each member's from its current card,
 * and an anti-entropy body that does not decode, or that carries the audit's fields to a node off the hint, is dropped
 * and counted under one key with no strike against its sender.
 */
class AuditHintTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zAudit");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zAudit",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "audit", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed, boolean audit) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed).audit(audit).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    @Test
    void theHintIsAValueInTheHintsMapAndEachMembersIsReadFromItsCard() throws IOException {
        PeerNode a = node("a", 1, true);
        PeerNode b = node("b", 2, false);
        PeerNode c = node("c", 3, true);
        assertThat(a.wireHints()).containsEntry(PeerNode.AE_HINT, PeerNode.AE_LEVEL);
        assertThat(b.wireHints()).doesNotContainKey(PeerNode.AE_HINT);
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        GroupRuntime rc = c.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(8);
        assertThat(ra.gossip().auditing()).isTrue();
        assertThat(rb.gossip().auditing()).as("a node off the hint takes no part").isFalse();
        assertThat(ra.gossip().audits(c.peerId())).as("both on wire.ae=2").isTrue();
        assertThat(ra.gossip().audits(b.peerId())).as("b's card lacks the hint").isFalse();
        assertThat(rc.gossip().audits(b.peerId())).isFalse();
        assertThat(rb.gossip().audits(a.peerId())).as("b audits nobody").isFalse();
        assertThat(ra.gossip().auditSessions()).isEqualTo(4);
        assertThat(ra.gossip().auditBudget()).isEqualTo(1L << 20);
    }

    @Test
    void anUndecodableDigestIsDroppedAndCountedOnceWithNoStrike() throws IOException {
        PeerNode a = node("a", 1, true);
        PeerNode b = node("b", 2, true);
        a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(6);
        long dropped = b.framesDropped();
        // A field no schema names, as a later form would send to a reader of this one: undecodable.
        Map<String, Object> body = new HashMap<>();
        body.put("digests", null);
        body.put("treez", Map.of());
        for (int i = 0; i < 3; i++) {
            assertThat(network.inject("a", "b", signedFromA(a, b, Envelope.Kind.DIGEST, CODEC.toBytes(body), i))).isTrue();
        }
        assertThat(rb.gossip().auditSignals().refusals()).containsEntry("undecodable", 3L);
        assertThat(b.framesDropped() - dropped).as("a verified sender's frame is not a door drop").isZero();
        assertThat(b.strikes(a.peerId())).as("a stale card is nobody's misbehaviour").isZero();
        // A PULL_RESP that carries a remainder below zero is undecodable too, and counted under the same key.
        assertThat(network.inject("a", "b", signedFromA(a, b, Envelope.Kind.PULL_RESP,
                CODEC.toBytes(Map.of("deltas", Map.of(), "remainder", Map.of("x", -1L))), 9))).isTrue();
        assertThat(rb.gossip().auditSignals().refusals()).containsEntry("undecodable", 4L);
        assertThat(b.strikes(a.peerId())).isZero();
    }

    @Test
    void aNodeOffTheHintDropsTheAuditsFieldsWholeAndCountsThem() throws IOException {
        PeerNode a = node("a", 1, true);
        PeerNode b = node("b", 2, false);
        a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(6);
        Bodies.Digest stale = new Bodies.Digest(null, null, null,
                Map.of("space:x", List.of(Bodies.TreeNode.request(new byte[]{1}, 3))), null, 4096L, null);
        assertThat(network.inject("a", "b", signedFromA(a, b, Envelope.Kind.DIGEST, CODEC.toBytes(stale), 0))).isTrue();
        assertThat(rb.gossip().auditSignals().refusals()).containsEntry("lacks-hint", 1L);
        assertThat(b.strikes(a.peerId())).isZero();
    }

    private byte[] signedFromA(PeerNode a, PeerNode b, Envelope.Kind kind, byte[] body, int logical) {
        return TestFrames.signed(a.identity(), groupId, kind, b.peerId(), clock, logical, body);
    }
}
