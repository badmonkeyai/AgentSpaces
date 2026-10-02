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
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Relay forwarding (spec §5.4): a peer that only dials out (the NAT-restricted
 * posture) advertises no endpoints; frames addressed to it route through a
 * RELAY-role member and arrive verified, attributed to the true origin.
 */
class RelayTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private GroupAdvertisement groupAd;

    @BeforeEach
    void setUp() {
        GroupId groupId = GroupId.of("zRelayGroup");
        groupAd = new GroupAdvertisement("aspace://zRelayGroup",
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(1), "relay-group",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                new GroupAdvertisement.GossipParameters(3, Duration.ofSeconds(1)));
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode listeningNode(String address, long seed,
                                   Set<PeerAdvertisement.PeerRole> roles) throws IOException {
        return listeningNode(address, seed, roles, PeerIdentity.generate());
    }

    private PeerNode listeningNode(String address, long seed,
                                   Set<PeerAdvertisement.PeerRole> roles,
                                   PeerIdentity identity) throws IOException {
        PeerNode node = PeerNode.builder(identity)
                .clock(clock).randomSeed(seed).roles(roles).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    /** An inner frame from {@code origin} to {@code target}, as a NAT peer would sign it. */
    private byte[] inner(PeerNode origin, PeerNode target, Envelope.Kind kind, Object body, int logical) {
        return TestFrames.signed(origin.identity(), groupAd.group(), kind, target.peerId(),
                clock, logical, body);
    }

    /** A RELAY_FRAME carrying {@code innerFrame}, signed by {@code relay}, delivered straight to {@code target}. */
    private void relayInto(String targetAddress, PeerNode target, PeerNode relay, byte[] innerFrame,
                           int logical) throws IOException {
        TransportConnection injector = network.register("injector-" + logical).dial(targetAddress);
        injector.send(TestFrames.signed(relay.identity(), groupAd.group(), Envelope.Kind.RELAY_FRAME,
                target.peerId(), clock, logical, new Bodies.RelayFrame(target.peerId(), innerFrame)));
    }

    /** A dial-only node: it can reach others, nobody can dial it. */
    private PeerNode natNode(String address, long seed) {
        PeerNode node = PeerNode.builder(PeerIdentity.generate())
                .clock(clock).randomSeed(seed).build();
        node.transport(network.register(address));
        nodes.add(node);
        return node;
    }

    private static List<PeerAdvertisement.Endpoint> seed(String address) {
        return List.of(new PeerAdvertisement.Endpoint("mem", address, 0));
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    /** Moves the clock past every stamp the nodes have issued, so hand-built frames are never exact replays. */
    private void freshMillisecond() {
        clock.advance(Duration.ofSeconds(1));
    }

    @Test
    void framesForAnUndialablePeerRouteThroughTheRelay() throws Exception {
        PeerNode relay = listeningNode("relay", 1,
                Set.of(PeerAdvertisement.PeerRole.RELAY));
        PeerNode alice = listeningNode("alice", 2, Set.of());
        PeerNode nat = natNode("nat", 3);

        relay.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime aliceRuntime =
                alice.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("relay"));
        GroupRuntime natRuntime =
                nat.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("relay"));
        tickAll(4);

        // Alice knows the NAT peer as a member with no endpoints.
        assertThat(aliceRuntime.membership().member(nat.peerId()))
                .hasValueSatisfying(m -> assertThat(m.endpoints()).isEmpty());

        List<PeerId> senders = new CopyOnWriteArrayList<>();
        List<String> payloads = new CopyOnWriteArrayList<>();
        natRuntime.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> {
            senders.add(from);
            payloads.add(new String(body, StandardCharsets.UTF_8));
        });

        aliceRuntime.send(nat.peerId(), Envelope.Kind.PIPE_DATA,
                "hello through the relay".getBytes(StandardCharsets.UTF_8));

        assertThat(payloads).containsExactly("hello through the relay");
        assertThat(senders).containsExactly(alice.peerId());
    }

    /** Spec §5.4: without a RELAY-role member a frame for an unreachable peer has nowhere to go; the NAT peer is partitioned from its target so its own dial-outs cannot open a direct path. */
    @Test
    void nonRelayMembersDoNotForward() throws Exception {
        PeerNode plain = listeningNode("plain", 1, Set.of());
        PeerNode alice = listeningNode("alice", 2, Set.of());
        PeerNode nat = natNode("nat", 3);
        // Under an advancing clock the NAT peer's refreshed advertisement and
        // probes would let it dial alice directly, giving alice a cached link
        // back to it. Cut that pair explicitly: alice can reach nat only
        // through a relay, and there is none.
        network.partition("nat", "alice");

        plain.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime aliceRuntime =
                alice.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("plain"));
        GroupRuntime natRuntime =
                nat.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("plain"));
        tickAll(4);

        List<String> payloads = new CopyOnWriteArrayList<>();
        natRuntime.onKind(Envelope.Kind.PIPE_DATA, (from, body) ->
                payloads.add(new String(body, StandardCharsets.UTF_8)));

        assertThat(aliceRuntime.membership().member(nat.peerId()))
                .as("alice knows the NAT peer, learned through plain").isPresent();

        // No member declares RELAY, so the frame has nowhere to go and is dropped.
        aliceRuntime.send(nat.peerId(), Envelope.Kind.PIPE_DATA,
                "undeliverable".getBytes(StandardCharsets.UTF_8));

        assertThat(payloads).isEmpty();
    }

    @Test
    void repliesFromTheNatPeerReachTheOriginDirectly() throws Exception {
        PeerNode relay = listeningNode("relay", 1,
                Set.of(PeerAdvertisement.PeerRole.RELAY));
        PeerNode alice = listeningNode("alice", 2, Set.of());
        PeerNode nat = natNode("nat", 3);

        relay.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime aliceRuntime =
                alice.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("relay"));
        GroupRuntime natRuntime =
                nat.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("relay"));
        tickAll(4);

        List<String> aliceSaw = new CopyOnWriteArrayList<>();
        aliceRuntime.onKind(Envelope.Kind.PIPE_DATA, (from, body) ->
                aliceSaw.add(new String(body, StandardCharsets.UTF_8)));
        natRuntime.onKind(Envelope.Kind.PIPE_DATA, (from, body) ->
                // Reply to whoever sent: the NAT peer can dial Alice directly.
                natRuntime.send(from, Envelope.Kind.PIPE_DATA,
                        "pong".getBytes(StandardCharsets.UTF_8)));

        aliceRuntime.send(nat.peerId(), Envelope.Kind.PIPE_DATA,
                "ping".getBytes(StandardCharsets.UTF_8));

        assertThat(aliceSaw).containsExactly("pong");
    }

    /** Spec §5.4 / TECH-SPEC §3.1: the relay carries the origin's signed frame verbatim; a tampered inner frame is dropped. */
    @Test
    void aRelayCannotTamperWithTheInnerFrame() throws Exception {
        PeerNode relay = listeningNode("relay", 1, Set.of(PeerAdvertisement.PeerRole.RELAY));
        PeerNode alice = listeningNode("alice", 2, Set.of());
        PeerNode nat = natNode("nat", 3);
        relay.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime aliceRuntime =
                alice.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("relay"));
        nat.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("relay"));
        tickAll(4);

        List<PeerId> senders = new CopyOnWriteArrayList<>();
        List<String> payloads = new CopyOnWriteArrayList<>();
        aliceRuntime.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> {
            senders.add(from);
            payloads.add(new String(body, StandardCharsets.UTF_8));
        });

        freshMillisecond();
        byte[] genuine = inner(nat, alice, Envelope.Kind.PIPE_DATA,
                "from nat".getBytes(StandardCharsets.UTF_8), 0);
        byte[] tampered = genuine.clone();
        tampered[tampered.length / 2] ^= 0x01;

        relayInto("alice", alice, relay, tampered, 1);
        assertThat(payloads).as("a relay that alters what it carries delivers nothing").isEmpty();

        relayInto("alice", alice, relay, genuine, 2);
        assertThat(payloads).containsExactly("from nat");
        assertThat(senders).as("attributed to the origin, not the relay").containsExactly(nat.peerId());
    }

    /** Spec §5.4: one relay hop only; an inner frame that is itself a RELAY_FRAME is refused at the target. */
    @Test
    void relayFramesAreNeverReRelayed() throws Exception {
        PeerNode relay = listeningNode("relay", 1, Set.of(PeerAdvertisement.PeerRole.RELAY));
        PeerNode alice = listeningNode("alice", 2, Set.of());
        PeerNode nat = natNode("nat", 3);
        relay.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime aliceRuntime =
                alice.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("relay"));
        nat.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("relay"));
        tickAll(4);

        List<String> payloads = new CopyOnWriteArrayList<>();
        aliceRuntime.onKind(Envelope.Kind.PIPE_DATA, (from, body) ->
                payloads.add(new String(body, StandardCharsets.UTF_8)));

        // nat -> (RELAY_FRAME to alice wrapping) -> (RELAY_FRAME to alice wrapping PIPE_DATA)
        freshMillisecond();
        byte[] innermost = inner(nat, alice, Envelope.Kind.PIPE_DATA,
                "two hops".getBytes(StandardCharsets.UTF_8), 0);
        byte[] nested = inner(nat, alice, Envelope.Kind.RELAY_FRAME,
                new Bodies.RelayFrame(alice.peerId(), innermost), 1);

        relayInto("alice", alice, relay, nested, 2);
        assertThat(payloads).isEmpty();
    }

    /** Spec v0.1.9 + ASF-022: a relayed inner frame re-enters dispatch, so a revoked origin is refused at the target. */
    @Test
    void aRelayedFrameFromARevokedOriginIsRefused() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        GroupId groupId = GroupId.of("zRelayFounded");
        groupAd = new GroupAdvertisement("aspace://zRelayFounded", founderId.peerId(), groupId,
                Instant.EPOCH, Duration.ofDays(1), "relay-founded",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        PeerNode relay = listeningNode("relay", 1, Set.of(PeerAdvertisement.PeerRole.RELAY), founderId);
        PeerNode alice = listeningNode("alice", 2, Set.of());
        PeerNode nat = natNode("nat", 3);
        GroupRuntime founderRuntime =
                relay.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime aliceRuntime =
                alice.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("relay"));
        nat.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("relay"));
        tickAll(4);

        List<PeerId> senders = new CopyOnWriteArrayList<>();
        aliceRuntime.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> senders.add(from));

        // The trust root revokes the NAT peer; alice learns it over gossip.
        assertThat(founderRuntime.revoke(nat.peerId(), "compromised")).isPresent();
        tickAll(4);
        assertThat(aliceRuntime.revocations().revoked(nat.peerId())).isTrue();

        // A relay that still carries the revoked origin's (validly signed) frame gets it refused.
        freshMillisecond();
        relayInto("alice", alice, relay, inner(nat, alice, Envelope.Kind.PIPE_DATA,
                "still here".getBytes(StandardCharsets.UTF_8), 0), 1);
        assertThat(senders).isEmpty();

        // While the same path carries an admitted origin's frame just fine.
        relayInto("alice", alice, relay, inner(relay, alice, Envelope.Kind.PIPE_DATA,
                "hello".getBytes(StandardCharsets.UTF_8), 2), 3);
        assertThat(senders).containsExactly(relay.peerId());
    }
}
