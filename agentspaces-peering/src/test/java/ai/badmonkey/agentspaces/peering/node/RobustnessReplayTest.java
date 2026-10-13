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
 * ROBUSTNESS.md §3.4 and §3.5: every frame a member sent, replayed exactly, replayed after the
 * staleness window, replayed over a stranger's connection, and delivered reversed, shuffled or
 * doubled. Nothing is delivered twice, nothing is answered twice, no honest member is struck,
 * and a session is never reset by a replayed hello.
 */
class RobustnessReplayTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zReplay");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zReplay",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "replay", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed, PeerNode.ChannelAuth mode) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed).channelAuth(mode).build();
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

    private static long kind(PeerNode node, Envelope.Kind kind) {
        return node.traffic().sentByKind().getOrDefault(kind, 0L);
    }

    @Test
    void everyFrameReplayedExactlyLateOrOverAStrangersLinkChangesNothing() throws Exception {
        PeerNode a = node("a", 1, PeerNode.ChannelAuth.SESSION);
        PeerNode b = node("b", 2, PeerNode.ChannelAuth.SESSION);
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        List<byte[]> aToB = new CopyOnWriteArrayList<>();
        network.tap("a", "b", aToB::add);
        tickAll(8);
        assertThat(b.channelModes()).containsEntry(a.peerId(), "session");
        List<String> newsAtB = new CopyOnWriteArrayList<>();
        rb.gossip().onStream("news", (from, id, payload) -> newsAtB.add(new String(payload, StandardCharsets.UTF_8)));
        for (int i = 0; i < 5; i++) {
            ra.gossip().publish("news", "n" + i, ("item-" + i).getBytes(StandardCharsets.UTF_8));
        }
        tickAll(4); // probes, digests, hellos, announcements: every kind A sends B is on the tap
        assertThat(newsAtB).hasSize(5);
        List<byte[]> captured = List.copyOf(aToB);
        assertThat(captured.size()).isGreaterThan(10);
        long acksFromB = kind(b, Envelope.Kind.ACK);
        long digestsFromB = kind(b, Envelope.Kind.DIGEST);
        long pullsFromB = kind(b, Envelope.Kind.PULL_RESP);
        long hellosFromB = kind(b, Envelope.Kind.SESSION_HELLO);
        long droppedAtB = b.framesDropped();

        // Exact replay of everything, in order: deduplicated by stamp, answered by nobody.
        for (byte[] frame : captured) {
            assertThat(network.inject("a", "b", frame)).isTrue();
        }
        assertThat(newsAtB).as("no item delivered twice").hasSize(5);
        assertThat(kind(b, Envelope.Kind.ACK)).as("a replayed probe earns no second ACK").isEqualTo(acksFromB);
        assertThat(kind(b, Envelope.Kind.PULL_RESP)).as("a replayed digest earns no second pull").isEqualTo(pullsFromB);
        assertThat(kind(b, Envelope.Kind.SESSION_HELLO)).as("a replayed hello opens nothing").isEqualTo(hellosFromB);
        assertThat(b.channelModes()).as("the session stands").containsEntry(a.peerId(), "session");
        assertThat(rb.membership().member(a.peerId())).isPresent();
        assertThat(b.quarantined(a.peerId())).as("replays never strike the signer").isFalse();

        // Reversed, then doubled, then shuffled: the same.
        List<byte[]> reversed = new ArrayList<>(captured);
        java.util.Collections.reverse(reversed);
        for (byte[] frame : reversed) {
            network.inject("a", "b", frame);
        }
        for (byte[] frame : captured) {
            network.inject("a", "b", frame);
            network.inject("a", "b", frame);
        }
        List<byte[]> shuffled = new ArrayList<>(captured);
        java.util.Collections.shuffle(shuffled, new java.util.Random(7));
        for (byte[] frame : shuffled) {
            network.inject("a", "b", frame);
        }
        assertThat(newsAtB).hasSize(5);
        assertThat(kind(b, Envelope.Kind.ACK)).isEqualTo(acksFromB);
        assertThat(b.quarantined(a.peerId())).isFalse();

        // Over a stranger's connection: session frames are refused there (no session), signed
        // frames are replays; either way nothing changes and nothing is struck.
        var strangerLink = network.register("x").dial("b");
        long droppedBeforeStranger = b.framesDropped();
        for (byte[] frame : captured) {
            strangerLink.send(frame);
        }
        assertThat(newsAtB).hasSize(5);
        assertThat(b.framesDropped()).as("session frames on a link with no session are dropped at the door")
                .isGreaterThan(droppedBeforeStranger);
        assertThat(b.channelModes()).containsEntry(a.peerId(), "session");
        assertThat(b.quarantined(a.peerId())).isFalse();

        // After the staleness window: dropped before any side effect, still no strike.
        clock.advance(Duration.ofMinutes(6));
        for (byte[] frame : captured) {
            network.inject("a", "b", frame);
        }
        assertThat(newsAtB).hasSize(5);
        assertThat(kind(b, Envelope.Kind.ACK)).isEqualTo(acksFromB);
        assertThat(b.quarantined(a.peerId())).isFalse();

        // And the link still works both ways afterwards.
        ra.gossip().publish("news", "n5", "after".getBytes(StandardCharsets.UTF_8));
        tickAll(3);
        assertThat(newsAtB).contains("after");
        assertThat(rb.membership().member(a.peerId())).isPresent();
        assertThat(ra.membership().member(b.peerId())).isPresent();
    }

    @Test
    void heldTrafficReleasedInAnyOrderConvergesWithoutDuplicatesOrStrikes() throws Exception {
        PeerNode a = node("a", 1, PeerNode.ChannelAuth.SESSION);
        PeerNode b = node("b", 2, PeerNode.ChannelAuth.SESSION);
        PeerNode c = node("c", 3, PeerNode.ChannelAuth.SESSION);
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        GroupRuntime rc = c.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(8);
        List<String> newsAtC = new CopyOnWriteArrayList<>();
        rc.gossip().onStream("news", (from, id, payload) -> newsAtC.add(new String(payload, StandardCharsets.UTF_8)));
        for (SimNetwork.Order order : SimNetwork.Order.values()) {
            newsAtC.clear();
            network.hold("a", "c");
            network.hold("b", "c");
            for (int i = 0; i < 6; i++) {
                ra.gossip().publish("news", order + "-" + i, (order + "-" + i).getBytes(StandardCharsets.UTF_8));
                rb.gossip().publish("news", order + "-b" + i, (order + "-b" + i).getBytes(StandardCharsets.UTF_8));
            }
            tickAll(3); // probes and announcements pile up behind the hold too
            assertThat(newsAtC).as("held: nothing arrived").isEmpty();
            network.release("a", "c", order, 11);
            network.release("b", "c", order, 11);
            tickAll(4);
            assertThat(newsAtC).as("released " + order).hasSize(12).doesNotHaveDuplicates();
            assertThat(c.quarantined(a.peerId())).isFalse();
            assertThat(c.quarantined(b.peerId())).isFalse();
            assertThat(rc.membership().allMembers()).hasSize(2);
        }
    }
}
