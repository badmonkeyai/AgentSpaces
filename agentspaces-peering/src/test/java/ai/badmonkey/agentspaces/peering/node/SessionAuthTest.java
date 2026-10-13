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
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Session attestation on unattested connections (spec §5.6, v0.1.17; PERF2 §3.7): two
 * SESSION-mode nodes on a fabric that attests nobody exchange one signed hello each way and
 * then authenticate frames by MAC; a SIGNED-mode node in the same group never hears a hello
 * and keeps every frame signed both ways; an attested connection keeps the §5.6 bare form.
 */
class SessionAuthTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zSession");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zSession",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "session", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed, PeerNode.ChannelAuth mode, boolean attested) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed)
                .channelAuth(mode).build();
        node.listen(network.register(address), address);
        if (attested) {
            network.attest(address, node.peerId());
        }
        nodes.add(node);
        return node;
    }

    private GroupRuntime join(PeerNode node, String seed) {
        return node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seed == null ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", seed, 0)));
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    @Test
    void sessionModeNodesSignOneHelloEachWayThenAuthenticateByMac() throws Exception {
        PeerNode a = node("a", 1, PeerNode.ChannelAuth.SESSION, false);
        PeerNode b = node("b", 2, PeerNode.ChannelAuth.SESSION, false);
        GroupRuntime ra = join(a, null);
        GroupRuntime rb = join(b, "a");
        tickAll(6);
        assertThat(ra.membership().allMembers()).extracting(GroupMembership.Member::id).containsExactly(b.peerId());
        assertThat(rb.membership().allMembers()).extracting(GroupMembership.Member::id).containsExactly(a.peerId());
        long hellosA = a.traffic().sentByKind().getOrDefault(Envelope.Kind.SESSION_HELLO, 0L);
        long hellosB = b.traffic().sentByKind().getOrDefault(Envelope.Kind.SESSION_HELLO, 0L);
        assertThat(hellosA).as("one opening hello, or an opening and an answer when both opened at once").isBetween(1L, 2L);
        assertThat(hellosB).isBetween(1L, 2L);
        assertThat(hellosA + hellosB).as("one session: at most an opening each and one answer").isLessThanOrEqualTo(3L);
        assertThat(a.channelModes()).containsEntry(b.peerId(), "session");
        assertThat(b.channelModes()).containsEntry(a.peerId(), "session");
        assertThat(a.sessionFramesSent()).isGreaterThan(0);
        assertThat(b.sessionFramesSent()).isGreaterThan(0);
        assertThat(a.bareFramesSent()).as("nothing travels bare on an unattested connection").isZero();

        // From here on the data plane costs no signatures at all.
        List<String> atB = new CopyOnWriteArrayList<>();
        rb.gossip().onStream("news", (from, id, payload) -> atB.add(new String(payload, StandardCharsets.UTF_8)));
        long signedBefore = a.signedFramesSent();
        long sessionBefore = a.sessionFramesSent();
        for (int i = 0; i < 20; i++) {
            ra.gossip().publish("news", "n" + i, ("item-" + i).getBytes(StandardCharsets.UTF_8));
        }
        tickAll(2);
        assertThat(atB).hasSize(20);
        assertThat(a.signedFramesSent() - signedBefore).as("no further signatures").isZero();
        assertThat(a.sessionFramesSent() - sessionBefore).isGreaterThanOrEqualTo(20L);
        assertThat(b.framesDropped()).isZero();
    }

    @Test
    void aSignedModeMemberNeverHearsAHelloAndKeepsEveryFrameSigned() throws Exception {
        PeerNode session = node("a", 1, PeerNode.ChannelAuth.SESSION, false);
        PeerNode classic = node("b", 2, PeerNode.ChannelAuth.SIGNED, false);
        GroupRuntime rs = join(session, null);
        GroupRuntime rc = join(classic, "a");
        tickAll(6);
        assertThat(rc.membership().allMembers()).extracting(GroupMembership.Member::id).containsExactly(session.peerId());
        assertThat(session.traffic().sentByKind()).doesNotContainKey(Envelope.Kind.SESSION_HELLO);
        assertThat(classic.traffic().receivedByKind()).doesNotContainKey(Envelope.Kind.SESSION_HELLO);
        assertThat(session.sessionFramesSent()).isZero();
        assertThat(classic.framesDropped()).isZero();
        assertThat(session.channelModes()).containsEntry(classic.peerId(), "signed");
        List<String> atClassic = new CopyOnWriteArrayList<>();
        rc.gossip().onStream("news", (from, id, payload) -> atClassic.add(new String(payload, StandardCharsets.UTF_8)));
        rs.gossip().publish("news", "n", "signed as ever".getBytes(StandardCharsets.UTF_8));
        assertThat(atClassic).containsExactly("signed as ever");
    }

    @Test
    void anAttestedConnectionKeepsTheBareFormInSessionMode() throws Exception {
        PeerNode a = node("a", 1, PeerNode.ChannelAuth.SESSION, true);
        PeerNode b = node("b", 2, PeerNode.ChannelAuth.SESSION, true);
        join(a, null);
        join(b, "a");
        tickAll(6);
        assertThat(a.traffic().sentByKind()).doesNotContainKey(Envelope.Kind.SESSION_HELLO);
        assertThat(a.bareFramesSent()).isGreaterThan(0);
        assertThat(a.sessionFramesSent()).isZero();
        assertThat(a.channelModes()).containsEntry(b.peerId(), "attested");
    }

    @Test
    void aSessionFrameCountsOnlyOnItsOwnConnectionUnderItsOwnKey() throws Exception {
        PeerNode a = node("a", 1, PeerNode.ChannelAuth.SESSION, false);
        PeerNode b = node("b", 2, PeerNode.ChannelAuth.SESSION, false);
        PeerNode c = node("c", 3, PeerNode.ChannelAuth.SESSION, false);
        GroupRuntime ra = join(a, null);
        join(b, "a");
        join(c, "a");
        tickAll(6);
        assertThat(a.channelModes()).containsEntry(b.peerId(), "session").containsEntry(c.peerId(), "session");
        // A session frame on C's connection naming B as its sender: A's session there attests C
        // alone, so it is dropped.
        long droppedBefore = a.framesDropped();
        var wire = new ai.badmonkey.agentspaces.peering.wire.WireCodec();
        var forged = new Envelope(PeerNode.WIRE_VERSION, groupId, Envelope.Kind.RUMOR, b.peerId(), a.peerId(),
                new ai.badmonkey.agentspaces.common.hlc.HlcTimestamp(clock.millis(), 0, "x"),
                ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec().toBytes(
                        new ai.badmonkey.agentspaces.peering.wire.Bodies.Rumor("news", "f", 6, new byte[]{1})));
        List<String> atA = new CopyOnWriteArrayList<>();
        ra.gossip().onStream("news", (from, id, payload) -> atA.add(from.display()));
        c.connectionFor(a.peerId()).send(wire.encodeSession(forged, new byte[32]));
        assertThat(a.framesDropped()).as("names B over C's session").isEqualTo(droppedBefore + 1);
        // Naming C itself under a key that is not the session's fails the tag.
        var asC = new Envelope(PeerNode.WIRE_VERSION, groupId, Envelope.Kind.RUMOR, c.peerId(), a.peerId(),
                new ai.badmonkey.agentspaces.common.hlc.HlcTimestamp(clock.millis(), 1, "x"), forged.body());
        c.connectionFor(a.peerId()).send(wire.encodeSession(asC, new byte[32]));
        assertThat(a.framesDropped()).as("wrong key").isEqualTo(droppedBefore + 2);
        // And a session frame on a fresh connection, which has no session at all, is dropped too.
        var fresh = network.register("x").dial("a");
        fresh.send(wire.encodeSession(asC, new byte[32]));
        assertThat(a.framesDropped()).as("no session on that connection").isEqualTo(droppedBefore + 3);
        fresh.close();
        assertThat(atA).isEmpty();
    }
}
