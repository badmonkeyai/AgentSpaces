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
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
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
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ROBUSTNESS.md §3.2, the never-joiner: a node with a good identity that sends every frame kind
 * over a real connection without ever being admitted. The posture is generous and cautious:
 * nothing throws, nothing is struck, the stranger gets exactly the bootstrap answers (a PING is
 * acknowledged under the bootstrap budget) and no session, no pull partner, no connection
 * entry, no data; the members' own traffic is untouched; and the same identity, introducing
 * itself properly afterwards, is admitted like anyone else.
 */
class RobustnessNeverJoinerTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zStranger");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zStranger",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "stranger", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final CborCodec codec = CborCodec.defaultCodec();
    private final WireCodec wire = new WireCodec(codec);

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed, PeerIdentity identity, PeerNode.ChannelAuth mode) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).channelAuth(mode).build();
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

    private byte[] frame(PeerIdentity from, PeerNode to, Envelope.Kind kind, Object body) {
        byte[] bodyBytes = codec.toBytes(body);
        Envelope envelope = new Envelope(WireCodec.WIRE_VERSION, groupId, kind, from.peerId(), to.peerId(),
                new HlcTimestamp(clock.millis(), 0, from.peerId().value()), bodyBytes);
        return wire.encode(envelope, from);
    }

    @Test
    void aStrangerSendingEveryKindIsNeitherServedNorStruckAndIsAdmittedOnceItIntroducesItself() throws Exception {
        PeerNode a = node("a", 1, PeerIdentity.generate(), PeerNode.ChannelAuth.SESSION);
        PeerNode b = node("b", 2, PeerIdentity.generate(), PeerNode.ChannelAuth.SESSION);
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(6);
        assertThat(ra.membership().allMembers()).hasSize(1);
        List<String> newsAtA = new CopyOnWriteArrayList<>();
        List<String> newsAtB = new CopyOnWriteArrayList<>();
        ra.gossip().onStream("news", (from, id, data) -> newsAtA.add(new String(data, StandardCharsets.UTF_8)));
        rb.gossip().onStream("news", (from, id, data) -> newsAtB.add(new String(data, StandardCharsets.UTF_8)));
        long droppedBefore = a.framesDropped();
        PeerNode.Traffic aBefore = a.traffic();

        // The stranger: a good identity on a raw connection, never introduced.
        PeerIdentity stranger = PeerIdentity.generate();
        Transport raw = network.register("s");
        TransportConnection toA = raw.dial("a");
        List<byte[]> heard = new CopyOnWriteArrayList<>();
        toA.onReceive(heard::add);

        byte[] payload = "from a stranger".getBytes(StandardCharsets.UTF_8);
        byte[] hash = Digests.sha256(payload);
        Bodies.Rumor rumor = new Bodies.Rumor("news", "s1", 6, payload);
        byte[] innerFrame = frame(stranger, a, Envelope.Kind.RUMOR, rumor);
        List<Object[]> kinds = List.of(
                new Object[] {Envelope.Kind.PING, new Bodies.Ping(7L)},
                new Object[] {Envelope.Kind.PING_REQ, new Bodies.PingReq(b.peerId(), 8L)},
                new Object[] {Envelope.Kind.ACK, new Bodies.Ack(9L, null)},
                new Object[] {Envelope.Kind.RUMOR, rumor},
                new Object[] {Envelope.Kind.DIGEST, new Bodies.Digest(Map.of("news", hash))},
                new Object[] {Envelope.Kind.DIGEST, new Bodies.Digest(null, Map.of("news", hash), null)},
                new Object[] {Envelope.Kind.DIGEST, new Bodies.Digest(null, null, List.of("news"))},
                new Object[] {Envelope.Kind.PULL_RESP, new Bodies.PullResp(Map.of("news", payload))},
                new Object[] {Envelope.Kind.HAVE, new Bodies.Have(List.of(new Bodies.HaveItem("news", hash)))},
                new Object[] {Envelope.Kind.GRAFT, new Bodies.Graft("news", hash)},
                new Object[] {Envelope.Kind.PRUNE, new Bodies.Prune("news", hash)},
                new Object[] {Envelope.Kind.BATCH, new Bodies.Batch(List.of(codec.toBytes(new Envelope(
                        WireCodec.WIRE_VERSION, groupId, Envelope.Kind.RUMOR, stranger.peerId(), a.peerId(),
                        new HlcTimestamp(clock.millis(), 1, "s"), codec.toBytes(rumor)))))},
                new Object[] {Envelope.Kind.SESSION_HELLO, new Bodies.SessionHello(new byte[32], new byte[32], a.peerId(), null)},
                new Object[] {Envelope.Kind.CHANNEL_HELLO, new Bodies.ChannelHello(true, a.peerId())},
                new Object[] {Envelope.Kind.RELAY_FRAME, new Bodies.RelayFrame(b.peerId(), innerFrame)},
                new Object[] {Envelope.Kind.PIPE_DATA, payload});
        for (Object[] kind : kinds) {
            toA.send(frame(stranger, a, (Envelope.Kind) kind[0], kind[1]));
            clock.advance(Duration.ofMillis(10));
        }
        tickAll(3);

        assertThat(ra.membership().allMembers()).as("no admission without an introduction").hasSize(1);
        assertThat(a.channelModes()).as("no connection entry for the stranger").doesNotContainKey(stranger.peerId());
        assertThat(newsAtA).as("no data from a non-member").isEmpty();
        assertThat(newsAtB).as("nothing forwarded on the stranger's behalf").isEmpty();
        assertThat(a.framesDropped()).as("well-formed frames are ignored, not dropped as malformed").isEqualTo(droppedBefore);
        assertThat(a.quarantined(stranger.peerId())).as("a stranger is not a misbehaving member").isFalse();
        long acks = heard.stream().filter(f -> wire.decode(f).map(e -> e.kind() == Envelope.Kind.ACK).orElse(false)).count();
        assertThat(acks).as("the bootstrap path answers the ping and nothing else").isEqualTo(1);
        assertThat(heard.size() - acks).as("no other answer: no pull, no digest, no hello, no graft answer").isZero();
        PeerNode.Traffic aAfter = a.traffic();
        assertThat(aAfter.sentByKind().getOrDefault(Envelope.Kind.SESSION_HELLO, 0L))
                .as("no session opened toward a stranger")
                .isEqualTo(aBefore.sentByKind().getOrDefault(Envelope.Kind.SESSION_HELLO, 0L));

        // The members are untouched.
        ra.gossip().publish("news", "m1", "members only".getBytes(StandardCharsets.UTF_8));
        assertThat(newsAtB).containsExactly("members only");
        assertThat(b.channelModes()).containsEntry(a.peerId(), "session");

        // The same identity introduces itself properly and is admitted like anyone else.
        toA.close();
        PeerNode s = node("s2", 3, stranger, PeerNode.ChannelAuth.SESSION);
        GroupRuntime rs = s.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(6);
        assertThat(ra.membership().member(stranger.peerId())).as("admitted on its introduction").isPresent();
        assertThat(rs.membership().allMembers()).hasSize(2);
        List<String> newsAtS = new CopyOnWriteArrayList<>();
        rs.gossip().onStream("news", (from, id, data) -> newsAtS.add(new String(data, StandardCharsets.UTF_8)));
        ra.gossip().publish("news", "m2", "welcome".getBytes(StandardCharsets.UTF_8));
        tickAll(2);
        assertThat(newsAtS).contains("welcome");
    }

    @Test
    void malformedBodiesFromAStrangerAreDroppedWithoutAStrikeOrAnException() throws Exception {
        PeerNode a = node("a", 1, PeerIdentity.generate(), PeerNode.ChannelAuth.SIGNED);
        a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        tickAll(2);
        PeerIdentity stranger = PeerIdentity.generate();
        TransportConnection toA = network.register("s").dial("a");
        long droppedBefore = a.framesDropped();
        int sent = 0;
        for (Envelope.Kind kind : Envelope.Kind.values()) {
            byte[] garbage = new byte[] {(byte) 0xff, 0x00, 0x7f, (byte) 0xa5, 0x01};
            Envelope envelope = new Envelope(WireCodec.WIRE_VERSION, groupId, kind, stranger.peerId(), a.peerId(),
                    new HlcTimestamp(clock.millis(), sent, "s"), garbage);
            toA.send(wire.encode(envelope, stranger));
            sent++;
            // And the same frame with its signature broken.
            byte[] broken = wire.encode(envelope, stranger);
            broken[broken.length - 1] ^= 0x55;
            toA.send(broken);
            sent++;
        }
        tickAll(2);
        assertThat(a.framesDropped() - droppedBefore).as("every broken signature was dropped at the door")
                .isGreaterThanOrEqualTo(Envelope.Kind.values().length);
        assertThat(a.quarantined(stranger.peerId())).isFalse();
        assertThat(a.channelModes()).doesNotContainKey(stranger.peerId());
    }
}
