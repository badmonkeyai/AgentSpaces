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
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Channel-attested frames end to end (spec §5.6), deterministically on a
 * SimNetwork that stands in for a TLS transport: nodes in ATTESTED mode
 * announce over attested connections, elide envelope signatures toward peers
 * that announced, and interoperate untouched with SIGNED-mode nodes and with
 * unattested channels.
 */
class ChannelAuthTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private GroupAdvertisement groupAd;

    @BeforeEach
    void setUp() {
        GroupId groupId = GroupId.of("zChannelGroup");
        groupAd = new GroupAdvertisement("aspace://zChannelGroup",
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(1), "channel-group",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                new GroupAdvertisement.GossipParameters(3, Duration.ofSeconds(1)));
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode newNode(String simAddress, long seed, PeerNode.ChannelAuth mode,
                             boolean attested) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate())
                .clock(clock)
                .randomSeed(seed)
                .channelAuth(mode)
                .build();
        node.listen(network.register(simAddress), simAddress);
        if (attested) {
            network.attest(simAddress, node.peerId());
        }
        nodes.add(node);
        return node;
    }

    private static List<PeerAdvertisement.Endpoint> seed(String address) {
        return List.of(new PeerAdvertisement.Endpoint("mem", address, 0));
    }

    private void join(PeerNode node, List<PeerAdvertisement.Endpoint> seeds) {
        node.joinGroup(groupAd,
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                seeds);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            for (PeerNode node : nodes) {
                node.tick();
            }
            clock.advance(Duration.ofSeconds(1));
        }
    }

    @Test
    void attestedNodesNegotiateAndElideEnvelopeSignatures() throws Exception {
        PeerNode a = newNode("a", 1, PeerNode.ChannelAuth.ATTESTED, true);
        PeerNode b = newNode("b", 2, PeerNode.ChannelAuth.ATTESTED, true);
        join(a, List.of());
        join(b, seed("a"));
        tickAll(6);

        // Both directions converged over frames, and after the CHANNEL_HELLO
        // exchange the bulk of them traveled bare.
        assertThat(a.group(groupAd.group()).orElseThrow().membership().allMembers())
                .extracting(GroupMembership.Member::id)
                .contains(b.peerId());
        assertThat(b.group(groupAd.group()).orElseThrow().membership().allMembers())
                .extracting(GroupMembership.Member::id)
                .contains(a.peerId());
        assertThat(a.bareFramesSent()).isGreaterThan(0);
        assertThat(b.bareFramesSent()).isGreaterThan(0);
        assertThat(a.channelModes()).containsEntry(b.peerId(), "attested");
        assertThat(b.channelModes()).containsEntry(a.peerId(), "attested");
    }

    @Test
    void aSignedModePeerNeverReceivesBareFramesAndStillConverges() throws Exception {
        PeerNode attested = newNode("a", 1, PeerNode.ChannelAuth.ATTESTED, true);
        PeerNode classic = newNode("b", 2, PeerNode.ChannelAuth.SIGNED, true);
        join(attested, List.of());
        join(classic, seed("a"));
        tickAll(6);

        // The SIGNED-mode node announces nothing, so the ATTESTED node keeps
        // signing toward it; the fleet still converges.
        assertThat(attested.bareFramesSent()).isZero();
        assertThat(classic.bareFramesSent()).isZero();
        assertThat(attested.channelModes()).containsEntry(classic.peerId(), "signed");
        assertThat(classic.group(groupAd.group()).orElseThrow().membership().allMembers())
                .extracting(GroupMembership.Member::id)
                .contains(attested.peerId());
    }

    @Test
    void unattestedChannelsStaySignedEvenInAttestedMode() throws Exception {
        // Both nodes opt in, but the fabric attests neither (a plain TCP
        // world): no announcement fires and every frame stays signed.
        PeerNode a = newNode("a", 1, PeerNode.ChannelAuth.ATTESTED, false);
        PeerNode b = newNode("b", 2, PeerNode.ChannelAuth.ATTESTED, false);
        join(a, List.of());
        join(b, seed("a"));
        tickAll(6);

        assertThat(a.bareFramesSent()).isZero();
        assertThat(b.bareFramesSent()).isZero();
        assertThat(a.signedFramesSent()).isGreaterThan(0);
        assertThat(a.group(groupAd.group()).orElseThrow().membership().allMembers())
                .extracting(GroupMembership.Member::id)
                .contains(b.peerId());
    }

    @Test
    void defaultModeIsSignedAndSendsNoAnnouncements() throws Exception {
        PeerNode a = newNode("a", 1, PeerNode.ChannelAuth.SIGNED, true);
        PeerNode b = newNode("b", 2, PeerNode.ChannelAuth.SIGNED, true);
        join(a, List.of());
        join(b, seed("a"));
        tickAll(4);

        assertThat(a.bareFramesSent()).isZero();
        assertThat(b.bareFramesSent()).isZero();
        assertThat(a.channelModes().values()).allMatch("signed"::equals);
    }

    // ------------------------------------------------------------ §5.6 negatives

    private final CborCodec codec = CborCodec.defaultCodec();
    private final WireCodec wire = new WireCodec(codec);

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static boolean isBare(WireCodec.Signed frame) {
        return frame.senderPublicKey() == null && frame.signature() == null;
    }

    private GroupRuntime runtimeOf(PeerNode node) {
        return node.group(groupAd.group()).orElseThrow();
    }

    /** Registers a PIPE_DATA handler on the node; entries are {@code <fromPeerId>:<utf8 body>}. */
    private List<String> pipeDataInbox(PeerNode node) {
        List<String> inbox = new CopyOnWriteArrayList<>();
        runtimeOf(node).onKind(Envelope.Kind.PIPE_DATA, (from, body) ->
                inbox.add(from.value() + ":" + new String(body, StandardCharsets.UTF_8)));
        return inbox;
    }

    /**
     * A scripted peer with a real identity, admitted to the host's group over
     * a raw SimNetwork connection that this test drives frame by frame. The
     * fabric attests the connection for whichever PeerID the test names (or
     * none), so every {@code (attested, announced, from)} combination of SPEC
     * §5.6 can be produced deliberately. Frames the host sends back are kept
     * in {@link #received} as their raw signed wrapper, so "bare" versus
     * "signed" is observable per frame.
     */
    private final class Ghost {
        final PeerIdentity identity = PeerIdentity.generate();
        final String address;
        final TransportConnection connection;
        final List<WireCodec.Signed> received = new CopyOnWriteArrayList<>();
        private int logical;

        Ghost(String address, String hostAddress) throws IOException {
            this.address = address;
            this.connection = network.register(address).dial(hostAddress);
            this.connection.onReceive(frame ->
                    received.add(codec.fromBytes(frame, WireCodec.Signed.class)));
        }

        PeerId peerId() {
            return identity.peerId();
        }

        /** Makes the host's transport attest this connection for {@code peer}. */
        void attestedAs(PeerId peer) {
            network.attest(address, peer);
        }

        Envelope envelope(Envelope.Kind kind, PeerId to, byte[] body) {
            return new Envelope(PeerNode.WIRE_VERSION, groupAd.group(), kind, peerId(), to,
                    new HlcTimestamp(clock.millis(), ++logical, peerId().value()), body);
        }

        void sendSigned(Envelope.Kind kind, PeerId to, byte[] body) throws IOException {
            connection.send(wire.encode(envelope(kind, to, body), identity));
        }

        void sendBare(Envelope.Kind kind, PeerId to, byte[] body) throws IOException {
            connection.send(wire.encodeBare(envelope(kind, to, body)));
        }

        void hello(PeerNode host, boolean acceptsBare, PeerId echo) throws IOException {
            sendSigned(Envelope.Kind.CHANNEL_HELLO, host.peerId(),
                    codec.toBytes(new Bodies.ChannelHello(acceptsBare, echo)));
        }

        /**
         * Introduces this peer exactly as a joining node does: an unaddressed,
         * signed peers-stream rumor carrying its signed PeerAdvertisement. The
         * OPEN group admits it, and the host caches this connection for it.
         */
        void introduceTo(PeerNode host) throws IOException {
            PeerAdvertisement ad = new PeerAdvertisement(
                    "aspace://" + groupAd.group().value() + "/peer/" + peerId().value(),
                    peerId(), groupAd.group(), clock.instant(), Duration.ofMinutes(10),
                    List.of(new PeerAdvertisement.Endpoint("mem", address, 0)),
                    Set.of(), Map.of());
            byte[] adBytes = codec.toBytes(ad);
            byte[] payload = codec.toBytes(new PeerNode.SignedPeerAd(
                    adBytes, identity.rawPublicKey(), identity.sign(adBytes)));
            Bodies.Rumor rumor = new Bodies.Rumor("peers",
                    "peer:" + peerId().value() + ":" + clock.millis(), 6, payload);
            sendSigned(Envelope.Kind.RUMOR, null, codec.toBytes(rumor));
            assertThat(runtimeOf(host).membership().member(peerId()))
                    .as("the scripted peer is admitted to the host's group")
                    .isPresent();
        }

        /** The wrapper of the PIPE_DATA frame the host sent us carrying {@code marker}. */
        WireCodec.Signed pipeData(byte[] marker) {
            for (int i = received.size() - 1; i >= 0; i--) {
                WireCodec.Signed frame = received.get(i);
                if (frame.envelope().kind() == Envelope.Kind.PIPE_DATA
                        && Arrays.equals(frame.envelope().body(), marker)) {
                    return frame;
                }
            }
            throw new AssertionError("the host never sent the marked PIPE_DATA frame");
        }
    }

    /** SPEC §5.6 frame relaxation: a bare frame is accepted only when the connection's attested PeerID equals {@code from}; attested for someone else means dropped. */
    @Test
    void aBareFrameFromAPeerTheConnectionDidNotAttestIsDropped() throws Exception {
        PeerNode a = newNode("a", 1, PeerNode.ChannelAuth.ATTESTED, true);
        join(a, List.of());
        List<String> inbox = pipeDataInbox(a);
        Ghost ghost = new Ghost("ghost", "a");
        ghost.attestedAs(PeerIdentity.generate().peerId()); // attested, but for another identity
        ghost.introduceTo(a);

        ghost.sendBare(Envelope.Kind.PIPE_DATA, a.peerId(), bytes("impostor"));
        assertThat(inbox).as("attested-for-someone-else is not attested-for-from").isEmpty();

        // A signed frame over the very same connection is fine: attestation
        // only ever adds an acceptance path, never removes one.
        ghost.sendSigned(Envelope.Kind.PIPE_DATA, a.peerId(), bytes("signed"));
        assertThat(inbox).containsExactly(ghost.peerId().value() + ":signed");

        // Once the fabric attests the connection for the actual sender, the
        // bare path opens.
        ghost.attestedAs(ghost.peerId());
        ghost.sendBare(Envelope.Kind.PIPE_DATA, a.peerId(), bytes("attested now"));
        assertThat(inbox).containsExactly(
                ghost.peerId().value() + ":signed",
                ghost.peerId().value() + ":attested now");
    }

    /** SPEC §5.6 frame relaxation: on an unattested connection a bare frame is dropped by every receiver, signed frames still flow. */
    @Test
    void aBareFrameOnAnUnattestedConnectionIsDropped() throws Exception {
        PeerNode a = newNode("a", 1, PeerNode.ChannelAuth.ATTESTED, true);
        join(a, List.of());
        List<String> inbox = pipeDataInbox(a);
        Ghost ghost = new Ghost("plain", "a"); // never attested
        ghost.introduceTo(a);

        ghost.sendBare(Envelope.Kind.PIPE_DATA, a.peerId(), bytes("bare over plain tcp"));
        assertThat(inbox).isEmpty();

        ghost.sendSigned(Envelope.Kind.PIPE_DATA, a.peerId(), bytes("signed over plain tcp"));
        assertThat(inbox).containsExactly(ghost.peerId().value() + ":signed over plain tcp");
        // And the host, which attests nothing about this channel, never announced.
        assertThat(ghost.received)
                .noneMatch(frame -> frame.envelope().kind() == Envelope.Kind.CHANNEL_HELLO);
    }

    /** SPEC §5.6 negotiation: the announcement is signed and echoes the attested remote; a receiver honors it only when the echo names itself. */
    @Test
    void aHelloWhoseEchoNamesAnotherPeerIsIgnored() throws Exception {
        PeerNode a = newNode("a", 1, PeerNode.ChannelAuth.ATTESTED, true);
        join(a, List.of());
        GroupRuntime ra = runtimeOf(a);
        Ghost ghost = new Ghost("ghost", "a");
        ghost.attestedAs(ghost.peerId());
        ghost.introduceTo(a);

        // The host attests us, so it announced toward us: a signed CHANNEL_HELLO
        // whose echo is our own PeerID.
        WireCodec.Signed announcement = ghost.received.stream()
                .filter(frame -> frame.envelope().kind() == Envelope.Kind.CHANNEL_HELLO)
                .findFirst().orElseThrow();
        assertThat(isBare(announcement)).as("CHANNEL_HELLO itself is always signed").isFalse();
        Bodies.ChannelHello hostHello =
                codec.fromBytes(announcement.envelope().body(), Bodies.ChannelHello.class);
        assertThat(hostHello.acceptsBare()).isTrue();
        assertThat(hostHello.attestedRemote()).isEqualTo(ghost.peerId());

        // Our announcement echoes the wrong peer: not bound to this channel.
        ghost.hello(a, true, PeerIdentity.generate().peerId());
        assertThat(a.channelModes()).containsEntry(ghost.peerId(), "signed");
        byte[] first = bytes("after a mis-echoed hello");
        ra.send(ghost.peerId(), Envelope.Kind.PIPE_DATA, first);
        assertThat(isBare(ghost.pipeData(first))).isFalse();

        // A correct echo flips the channel, and the next frame travels bare
        // and verifies only against the host's attestation.
        ghost.hello(a, true, a.peerId());
        assertThat(a.channelModes()).containsEntry(ghost.peerId(), "attested");
        byte[] second = bytes("after a correct hello");
        ra.send(ghost.peerId(), Envelope.Kind.PIPE_DATA, second);
        WireCodec.Signed bare = ghost.pipeData(second);
        assertThat(isBare(bare)).isTrue();
        assertThat(wire.decode(codec.toBytes(bare), Optional.of(a.peerId()))).isPresent();
        assertThat(wire.decode(codec.toBytes(bare), Optional.empty())).isEmpty();
    }

    /** SPEC §5.6 negotiation: the announcement counts only when the connection's attestation names the announcer. */
    @Test
    void aHelloFromAConnectionNotAttestedForTheAnnouncerIsIgnored() throws Exception {
        PeerNode a = newNode("a", 1, PeerNode.ChannelAuth.ATTESTED, true);
        join(a, List.of());
        GroupRuntime ra = runtimeOf(a);

        // (i) Unattested connection, correct echo.
        Ghost unattested = new Ghost("g1", "a");
        unattested.introduceTo(a);
        unattested.hello(a, true, a.peerId());
        assertThat(a.channelModes()).containsEntry(unattested.peerId(), "signed");
        byte[] m1 = bytes("to the unattested peer");
        ra.send(unattested.peerId(), Envelope.Kind.PIPE_DATA, m1);
        assertThat(isBare(unattested.pipeData(m1))).isFalse();

        // (ii) Connection attested for a third party, correct echo.
        Ghost mismatched = new Ghost("g2", "a");
        mismatched.attestedAs(PeerIdentity.generate().peerId());
        mismatched.introduceTo(a);
        mismatched.hello(a, true, a.peerId());
        assertThat(a.channelModes()).containsEntry(mismatched.peerId(), "signed");
        byte[] m2 = bytes("to the mismatched peer");
        ra.send(mismatched.peerId(), Envelope.Kind.PIPE_DATA, m2);
        assertThat(isBare(mismatched.pipeData(m2))).isFalse();

        assertThat(a.bareFramesSent()).isZero();
    }

    /** SPEC §5.6 negotiation: {@code acceptsBare=false} withdraws the announcement, and the sender returns to signed frames. */
    @Test
    void acceptsBareFalseRevokesElision() throws Exception {
        PeerNode a = newNode("a", 1, PeerNode.ChannelAuth.ATTESTED, true);
        join(a, List.of());
        GroupRuntime ra = runtimeOf(a);
        Ghost ghost = new Ghost("ghost", "a");
        ghost.attestedAs(ghost.peerId());
        ghost.introduceTo(a);

        ghost.hello(a, true, a.peerId());
        assertThat(a.channelModes()).containsEntry(ghost.peerId(), "attested");
        byte[] elided = bytes("elided");
        ra.send(ghost.peerId(), Envelope.Kind.PIPE_DATA, elided);
        assertThat(isBare(ghost.pipeData(elided))).isTrue();

        ghost.hello(a, false, a.peerId());
        assertThat(a.channelModes()).containsEntry(ghost.peerId(), "signed");
        long bareBefore = a.bareFramesSent();
        byte[] signedAgain = bytes("signed again");
        ra.send(ghost.peerId(), Envelope.Kind.PIPE_DATA, signedAgain);
        assertThat(isBare(ghost.pipeData(signedAgain))).isFalse();
        assertThat(a.bareFramesSent()).isEqualTo(bareBefore);
    }

    /** SPEC §5.6 negotiation: a CHANNEL_HELLO whose body does not decode is ignored and leaves the channel usable. */
    @Test
    void aMalformedHelloBodyIsIgnoredWithoutBreakingTheChannel() throws Exception {
        PeerNode a = newNode("a", 1, PeerNode.ChannelAuth.ATTESTED, true);
        join(a, List.of());
        Ghost ghost = new Ghost("ghost", "a");
        ghost.attestedAs(ghost.peerId());
        ghost.introduceTo(a);

        ghost.sendSigned(Envelope.Kind.CHANNEL_HELLO, a.peerId(), new byte[]{9, 9, 9});
        assertThat(a.channelModes()).containsEntry(ghost.peerId(), "signed");

        ghost.hello(a, true, a.peerId());
        assertThat(a.channelModes()).containsEntry(ghost.peerId(), "attested");
    }

    /** SPEC §5.6: after negotiation a frame the sender elided is actually accepted by the receiver, and no signed fallback was used. */
    @Test
    void bareFramesAreActuallyAcceptedAfterNegotiation() throws Exception {
        PeerNode a = newNode("a", 1, PeerNode.ChannelAuth.ATTESTED, true);
        PeerNode b = newNode("b", 2, PeerNode.ChannelAuth.ATTESTED, true);
        join(a, List.of());
        join(b, seed("a"));
        tickAll(6);
        assertThat(a.channelModes()).containsEntry(b.peerId(), "attested");

        List<String> inbox = pipeDataInbox(b);
        long signedBefore = a.signedFramesSent();
        long bareBefore = a.bareFramesSent();

        runtimeOf(a).send(b.peerId(), Envelope.Kind.PIPE_DATA, bytes("over the attested channel"));

        assertThat(inbox).containsExactly(a.peerId().value() + ":over the attested channel");
        assertThat(a.bareFramesSent()).as("exactly one bare frame carried it").isEqualTo(bareBefore + 1);
        assertThat(a.signedFramesSent()).as("no signed frame was needed").isEqualTo(signedBefore);
    }
}
