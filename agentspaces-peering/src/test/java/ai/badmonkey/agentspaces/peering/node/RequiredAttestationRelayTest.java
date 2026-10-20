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
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SPEC §5.6 (v0.1.21), SRAD srad/agentspaces-js.md JS-N-03 and JS-Q-SEC-05: under required
 * attestation a node admits the inner frame of a {@code RELAY_FRAME} only when it is a fully
 * signed {@code PIPE_DATA} of {@code aspace:cap/rtc-signal} addressed to the node itself; every
 * other relayed inner frame stays dropped, and a signal still needs its origin to be a member.
 */
class RequiredAttestationRelayTest {

    /** The head of a {@code PIPE_DATA} body, as {@code CapabilityPipes} encodes it. */
    private record PipeFrame(String capability, byte[] payload) {
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zAttestedRelay");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zAttestedRelay",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "attested-relay", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            new GroupAdvertisement.GossipParameters(3, Duration.ofSeconds(1)));

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    /** A listening node whose fabric channel attests it, so its own frames pass required attestation. */
    private PeerNode attestedListener(String address, long seed, boolean requireAttestation,
                                      Set<PeerAdvertisement.PeerRole> roles) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).roles(roles)
                .requireAttestation(requireAttestation).build();
        network.attest(address, identity.peerId());
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    /** A dial-only node, attested on its own channel: the browser behind a relay of scenario S7. */
    private PeerNode attestedNatNode(String address, long seed) {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        network.attest(address, identity.peerId());
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

    private byte[] pipe(String capability, String payload) {
        return TestFrames.body(new PipeFrame(capability, payload.getBytes(StandardCharsets.UTF_8)));
    }

    /** A RELAY_FRAME from {@code relay}, on a channel attested for the relay, carrying {@code inner} to {@code target}. */
    private void relayInto(String targetAddress, PeerNode target, PeerNode relay, byte[] inner, int logical)
            throws IOException {
        String injector = "injector-" + logical;
        network.attest(injector, relay.peerId());
        TransportConnection connection = network.register(injector).dial(targetAddress);
        connection.send(TestFrames.signed(relay.identity(), groupId, Envelope.Kind.RELAY_FRAME,
                target.peerId(), clock, logical, new Bodies.RelayFrame(target.peerId(), inner)));
    }

    private record Fleet(PeerNode target, PeerNode relay, PeerNode origin, GroupRuntime atTarget,
                         List<String> signals, List<PeerId> signalOrigins) {
    }

    private Fleet fleet(boolean requireAttestation) throws IOException {
        PeerNode target = attestedListener("target", 1, requireAttestation, Set.of());
        PeerNode relay = attestedListener("relay", 2, false, Set.of(PeerAdvertisement.PeerRole.RELAY));
        PeerNode origin = attestedNatNode("origin", 3);
        GroupRuntime atTarget = target.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        relay.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("target"));
        origin.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("target"));
        tickAll(4);
        assertThat(atTarget.membership().member(origin.peerId())).as("the origin joined over an attested channel")
                .isPresent();
        assertThat(atTarget.membership().member(relay.peerId())).isPresent();
        List<String> signals = new CopyOnWriteArrayList<>();
        List<PeerId> origins = new CopyOnWriteArrayList<>();
        atTarget.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> {
            origins.add(from);
            signals.add(new String(CborCodec.defaultCodec().fromBytes(body, PipeFrame.class).payload(),
                    StandardCharsets.UTF_8));
        });
        clock.advance(Duration.ofSeconds(1)); // past every stamp issued so far: hand-built frames are fresh
        return new Fleet(target, relay, origin, atTarget, signals, origins);
    }

    @Test
    void aRelayedSignalAddressedToTheNodeIsAdmittedUnderRequiredAttestation() throws Exception {
        Fleet f = fleet(true);
        byte[] inner = TestFrames.signed(f.origin().identity(), groupId, Envelope.Kind.PIPE_DATA,
                f.target().peerId(), clock, 1, pipe(PeerNode.RTC_SIGNAL_CAPABILITY, "OFFER"));
        relayInto("target", f.target(), f.relay(), inner, 2);
        assertThat(f.signals()).containsExactly("OFFER");
        assertThat(f.signalOrigins()).containsExactly(f.origin().peerId());
    }

    @Test
    void everyOtherRelayedInnerFrameStaysDroppedUnderRequiredAttestation() throws Exception {
        Fleet f = fleet(true);
        // Another capability's pipe frame, addressed to the node.
        relayInto("target", f.target(), f.relay(), TestFrames.signed(f.origin().identity(), groupId,
                Envelope.Kind.PIPE_DATA, f.target().peerId(), clock, 1,
                pipe("aspace:cap/aggregate", "share")), 2);
        // A signal addressed to someone else (the relay), carried to the node.
        relayInto("target", f.target(), f.relay(), TestFrames.signed(f.origin().identity(), groupId,
                Envelope.Kind.PIPE_DATA, f.relay().peerId(), clock, 3,
                pipe(PeerNode.RTC_SIGNAL_CAPABILITY, "OFFER-for-relay")), 4);
        // A rumor, the kind a relay injection would use to refresh a view.
        relayInto("target", f.target(), f.relay(), TestFrames.signed(f.origin().identity(), groupId,
                Envelope.Kind.RUMOR, f.target().peerId(), clock, 5,
                new Bodies.Rumor("news", "n1", 3, "injected".getBytes(StandardCharsets.UTF_8))), 6);
        // A pipe body that does not decode as a pipe frame at all.
        relayInto("target", f.target(), f.relay(), TestFrames.signed(f.origin().identity(), groupId,
                Envelope.Kind.PIPE_DATA, f.target().peerId(), clock, 7, new byte[]{(byte) 0xff}), 8);
        assertThat(f.signals()).isEmpty();
        assertThat(f.atTarget().membership().member(f.origin().peerId()))
                .as("a dropped relayed frame never evicts: the relay's channel attests the relay").isPresent();
    }

    @Test
    void aRelayedSignalFromANonMemberIsStillIgnored() throws Exception {
        Fleet f = fleet(true);
        PeerIdentity stranger = PeerIdentity.generate();
        byte[] inner = TestFrames.signed(stranger, groupId, Envelope.Kind.PIPE_DATA,
                f.target().peerId(), clock, 1, pipe(PeerNode.RTC_SIGNAL_CAPABILITY, "OFFER"));
        relayInto("target", f.target(), f.relay(), inner, 2);
        assertThat(f.signals()).as("ASF-003: the data plane serves admitted members only").isEmpty();
    }

    @Test
    void withoutRequiredAttestationRelayedFramesPassAsBefore() throws Exception {
        Fleet f = fleet(false);
        relayInto("target", f.target(), f.relay(), TestFrames.signed(f.origin().identity(), groupId,
                Envelope.Kind.PIPE_DATA, f.target().peerId(), clock, 1,
                pipe("aspace:cap/aggregate", "share")), 2);
        relayInto("target", f.target(), f.relay(), TestFrames.signed(f.origin().identity(), groupId,
                Envelope.Kind.PIPE_DATA, f.target().peerId(), clock, 3,
                pipe(PeerNode.RTC_SIGNAL_CAPABILITY, "OFFER")), 4);
        assertThat(f.signals()).containsExactly("share", "OFFER");
    }
}
