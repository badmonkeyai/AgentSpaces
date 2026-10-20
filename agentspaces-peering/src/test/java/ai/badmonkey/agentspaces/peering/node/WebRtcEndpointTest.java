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
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
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
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SRAD srad/agentspaces-js.md JS-W-08 and JS-W-11, the node side: a peer that accepts
 * {@code webrtc} advertises {@code Endpoint("webrtc", "sig", p)}; a dialer with a {@code webrtc}
 * binding never dials that endpoint from inside a send (the binding is signalled,
 * {@link Transport#signalled()}), sends through a relay instead, and dials it with the member's
 * PeerID ({@link Transport#dial(String, PeerId)}) from its tick once more than the threshold of
 * frames crossed the relay in a minute, backing off after a failure; a dialer without the
 * binding skips the endpoint (SPEC §5.5). The binding itself lives in
 * agentspaces-transport-webrtc; this fake records what the node asked of it.
 */
class WebRtcEndpointTest {

    /** A {@code webrtc}-scheme binding that records its dials; its "sig" address carries no destination. */
    static final class RecordingWebRtc implements Transport {
        final List<PeerId> targets = new CopyOnWriteArrayList<>();
        final List<String> targetlessDials = new CopyOnWriteArrayList<>();
        volatile String listenedOn;

        @Override
        public String scheme() {
            return "webrtc";
        }

        @Override
        public Set<Capability> capabilities() {
            return Set.of(Capability.ENCRYPTED, Capability.ATTESTING, Capability.RELIABLE, Capability.ORDERED);
        }

        @Override
        public boolean signalled() {
            return true;
        }

        @Override
        public TransportConnection dial(String address) throws IOException {
            targetlessDials.add(address);
            throw new IOException("a webrtc dial needs the target member: " + address);
        }

        @Override
        public TransportConnection dial(String address, PeerId target) throws IOException {
            targets.add(target);
            throw new IOException("signalling to " + target.display() + " not answered in this test");
        }

        @Override
        public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) {
            listenedOn = bindAddress;
            return () -> {
            };
        }
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zWebRtcEndpoint");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zWebRtcEndpoint",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "webrtc-endpoint", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(long seed) {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed).build();
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
    void anAcceptorAdvertisesTheSigEndpointAfterItsStreamEndpoints() throws Exception {
        PeerNode a = node(1);
        RecordingWebRtc acceptor = new RecordingWebRtc();
        a.listen(network.register("a"), "a");
        a.listen(acceptor, "sig");
        assertThat(acceptor.listenedOn).isEqualTo("sig");
        assertThat(a.advertisedEndpoints()).containsExactly(
                new PeerAdvertisement.Endpoint("mem", "a", 0),
                new PeerAdvertisement.Endpoint("webrtc", "sig", 1));
        assertThat(a.transportCapabilities("webrtc")).contains(Transport.Capability.ATTESTING);
    }

    @Test
    void aSendNeverDialsTheSigEndpointAndADialerWithoutTheBindingSkipsIt() throws Exception {
        PeerNode a = node(1);
        a.listen(network.register("a"), "a", 1);
        a.listen(new RecordingWebRtc(), "sig", 0); // preferred over the stream endpoint
        PeerNode withBinding = node(2);
        withBinding.listen(network.register("b"), "b");
        RecordingWebRtc dialer = new RecordingWebRtc();
        withBinding.transport(dialer);
        PeerNode without = node(3);
        without.listen(network.register("c"), "c");
        GroupRuntime atA = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime atB = withBinding.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        GroupRuntime atC = without.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(3);
        assertThat(atB.membership().member(a.peerId())).hasValueSatisfying(m ->
                assertThat(m.endpoints()).contains(new PeerAdvertisement.Endpoint("webrtc", "sig", 0)));
        // Cut the cached links so each dialer must reach A afresh from its advertised endpoints.
        network.partition("a", "b");
        network.partition("a", "c");
        List<String> atASaw = new CopyOnWriteArrayList<>();
        atA.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> atASaw.add(new String(body, StandardCharsets.UTF_8)));
        atB.send(a.peerId(), Envelope.Kind.PIPE_DATA, "from b".getBytes(StandardCharsets.UTF_8));
        atC.send(a.peerId(), Envelope.Kind.PIPE_DATA, "from c".getBytes(StandardCharsets.UTF_8));
        network.heal();
        atB.send(a.peerId(), Envelope.Kind.PIPE_DATA, "from b again".getBytes(StandardCharsets.UTF_8));
        assertThat(dialer.targets).as("a signalled endpoint is never dialed from inside a send (JS-W-11)").isEmpty();
        assertThat(dialer.targetlessDials).as("a member's endpoint is never dialed without its PeerID").isEmpty();
        assertThat(atASaw).as("the send fell through to the mem endpoint once it was reachable")
                .contains("from b again");
    }

    @Test
    void aNodeRelayedPastTheThresholdDialsTheSigEndpointFromItsTickAndBacksOffAfterAFailure() throws Exception {
        PeerNode seed = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(1)
                .roles(Set.of(PeerAdvertisement.PeerRole.RELAY)).build();
        nodes.add(seed);
        seed.listen(network.register("s"), "s");
        PeerNode a = node(2);
        a.listen(network.register("a"), "a", 1);
        a.listen(new RecordingWebRtc(), "sig", 0);
        PeerNode b = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(3)
                .relayOffload(new PeerNode.RelayOffload(10, Duration.ofMinutes(5))).build();
        nodes.add(b);
        b.transport(network.register("b")); // dial-only on the fabric
        RecordingWebRtc dialer = new RecordingWebRtc();
        b.transport(dialer);
        List<PeerAdvertisement.Endpoint> seeds = List.of(new PeerAdvertisement.Endpoint("mem", "s", 0));
        GroupRuntime atSeed = seed.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime atA = a.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        GroupRuntime atB = b.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        network.partition("a", "b"); // A's stream endpoint is out of B's reach: the relay carries B's frames
        tickAll(4);
        assertThat(atB.membership().member(a.peerId())).hasValueSatisfying(m ->
                assertThat(m.endpoints()).contains(new PeerAdvertisement.Endpoint("webrtc", "sig", 0)));
        List<String> atASaw = new CopyOnWriteArrayList<>();
        atA.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> atASaw.add(new String(body, StandardCharsets.UTF_8)));
        // B's probes and gossip to A cross the relay too, so the count holds the application's
        // eleven frames and more; what the rule reads is the count, whatever its composition.
        for (int i = 0; i < 11; i++) {
            atB.send(a.peerId(), Envelope.Kind.PIPE_DATA, ("b" + i).getBytes(StandardCharsets.UTF_8));
        }
        tickAll(1);
        assertThat(b.relayedWithInLastMinute(groupId, a.peerId())).isGreaterThanOrEqualTo(11);
        awaitDials(dialer, 1);
        assertThat(dialer.targets).as("past the threshold the tick dialed, with the member's PeerID")
                .containsExactly(a.peerId());
        assertThat(atASaw).as("the relay carried every frame meanwhile; the failed dial lost none")
                .hasSize(11);
        assertThat(atSeed.membership().member(b.peerId())).isPresent();
        assertThat(seed.relayStats(groupId).get(a.peerId()).framesForwarded()).isGreaterThanOrEqualTo(11);
        // Inside the backoff the relay keeps carrying and nothing dials, however heavy the traffic.
        for (int i = 0; i < 20; i++) {
            atB.send(a.peerId(), Envelope.Kind.PIPE_DATA, ("more" + i).getBytes(StandardCharsets.UTF_8));
        }
        tickAll(60);
        assertThat(dialer.targets).hasSize(1);
        assertThat(b.relayOffloadStats().get(groupId)).isEqualTo(new PeerNode.OffloadStats(1, 0, 1));
        // Five minutes on, still-heavy traffic earns one more attempt, not a storm.
        clock.advance(Duration.ofMinutes(4));
        for (int i = 0; i < 11; i++) {
            atB.send(a.peerId(), Envelope.Kind.PIPE_DATA, ("later" + i).getBytes(StandardCharsets.UTF_8));
        }
        tickAll(2);
        awaitDials(dialer, 2);
        assertThat(dialer.targets).containsExactly(a.peerId(), a.peerId());
        assertThat(b.relayOffloadStats().get(groupId)).isEqualTo(new PeerNode.OffloadStats(2, 0, 2));
        assertThat(atASaw).hasSize(42);
    }

    /** The offload dials on a thread of its own, so a tick returns before the recording binding is asked. */
    private static void awaitDials(RecordingWebRtc dialer, int count) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (dialer.targets.size() < count) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("expected " + count + " dials, saw " + dialer.targets);
            }
            Thread.sleep(10);
        }
    }
}
