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
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The dispatch gate in front of every handler (spec §11, ASF-003/010/020,
 * WS5): exact replays, stale stamps, frames addressed elsewhere, frames for
 * unjoined groups, and the whole data plane for non-members are dropped
 * before any side effect; witnessed misbehavior on authenticated frames
 * accumulates strikes into a local quarantine that lapses on its own.
 */
class DispatchGateTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zGate");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zGate",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "gate", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private PeerNode a;
    private PeerNode b;
    private GroupRuntime ra;
    private GroupRuntime rb;
    private final List<String> newsAtB = new CopyOnWriteArrayList<>();
    private TransportConnection injector;
    private final List<byte[]> injectorInbox = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws IOException {
        a = node("a", 1);
        b = node("b", 2);
        ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(4);
        assertThat(rb.membership().member(a.peerId())).isPresent();
        rb.gossip().onStream("news", (from, id, payload) ->
                newsAtB.add(new String(payload, StandardCharsets.UTF_8)));
        injector = network.register("injector").dial("b");
        injector.onReceive(injectorInbox::add);
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed).build();
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

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private Bodies.Rumor news(String text) {
        return new Bodies.Rumor("news", "news:" + text, 3, bytes(text));
    }

    /** A frame from the admitted member A to B, stamped at the given offset from now. */
    private byte[] fromA(GroupId group, Envelope.Kind kind, PeerId to, Duration stampOffset,
                         int logical, Object body) {
        HlcTimestamp stamp = new HlcTimestamp(
                clock.millis() + stampOffset.toMillis(), logical, a.peerId().value());
        return TestFrames.signed(a.identity(), group, kind, to, stamp, body);
    }

    /** Spec §11 (ASF-010): an exact replay of a captured frame has no second effect. */
    @Test
    void aReplayedFrameHasNoEffect() throws Exception {
        byte[] frame = fromA(groupId, Envelope.Kind.RUMOR, b.peerId(), Duration.ZERO, 0, news("once"));

        injector.send(frame);
        injector.send(frame);

        assertThat(newsAtB).containsExactly("once");
    }

    /** Spec §11 (ASF-010): a frame older than the staleness window is dropped; a merely delayed one is not. */
    @Test
    void aStaleFrameIsDropped() throws Exception {
        injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(),
                Duration.ofMinutes(-6), 0, news("stale")));
        assertThat(newsAtB).isEmpty();

        injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(),
                Duration.ofMinutes(-4), 0, news("delayed")));
        assertThat(newsAtB).containsExactly("delayed");
    }

    /** Spec §11 (ASF-010): the signed destination binds a frame to its recipient; others drop it. */
    @Test
    void aFrameAddressedToAnotherPeerIsDropped() throws Exception {
        injector.send(fromA(groupId, Envelope.Kind.RUMOR, PeerIdentity.generate().peerId(),
                Duration.ZERO, 0, news("misdirected")));
        assertThat(newsAtB).isEmpty();

        injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(), Duration.ZERO, 1, news("for-b")));
        assertThat(newsAtB).containsExactly("for-b");
    }

    /** Spec §5.1/§11 (ASF-003): a peer outside the view gets no data plane and leaves no state behind. */
    @Test
    void aNonMemberGetsNoDataPlane() throws Exception {
        PeerIdentity stranger = PeerIdentity.generate();
        List<Envelope.Kind> served = new CopyOnWriteArrayList<>();
        rb.onKind(Envelope.Kind.QUERY, (from, body) -> served.add(Envelope.Kind.QUERY));
        rb.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> served.add(Envelope.Kind.PIPE_DATA));
        rb.onKind(Envelope.Kind.BLOCK_WANT, (from, body) -> served.add(Envelope.Kind.BLOCK_WANT));
        int logical = 0;

        injector.send(TestFrames.signed(stranger, groupId, Envelope.Kind.RUMOR, b.peerId(),
                clock, logical++, news("from-a-stranger")));
        injector.send(TestFrames.signed(stranger, groupId, Envelope.Kind.DIGEST, b.peerId(),
                clock, logical++, new Bodies.Digest(Map.of("revocations", new byte[0]))));
        injector.send(TestFrames.signed(stranger, groupId, Envelope.Kind.PULL_RESP, b.peerId(),
                clock, logical++, new Bodies.PullResp(Map.of("revocations", bytes("x")))));
        injector.send(TestFrames.signed(stranger, groupId, Envelope.Kind.QUERY, b.peerId(),
                clock, logical++, bytes("q")));
        injector.send(TestFrames.signed(stranger, groupId, Envelope.Kind.PIPE_DATA, b.peerId(),
                clock, logical++, bytes("p")));
        injector.send(TestFrames.signed(stranger, groupId, Envelope.Kind.BLOCK_WANT, b.peerId(),
                clock, logical++, bytes("w")));

        assertThat(newsAtB).as("a non-peers rumor is not delivered").isEmpty();
        assertThat(served).as("no kind handler ran").isEmpty();
        assertThat(injectorInbox).as("no digest was answered").isEmpty();
        assertThat(b.channelModes()).as("no connection was cached for the stranger")
                .doesNotContainKey(stranger.peerId());
        assertThat(rb.membership().member(stranger.peerId())).isEmpty();

        // The one liveness courtesy: a PING is answered on the incoming connection alone.
        injector.send(TestFrames.signed(stranger, groupId, Envelope.Kind.PING, b.peerId(),
                clock, logical++, new Bodies.Ping(42L)));
        assertThat(injectorInbox).hasSize(1);
        assertThat(b.channelModes()).doesNotContainKey(stranger.peerId());
    }

    /**
     * Spec §11 (ASF-020): a stamp further past the HLC drift ceiling than the skew margin is
     * witnessed misbehavior; enough of them quarantine. HLC canonical stamps, 2026-11-08: each
     * such drop is counted.
     */
    @Test
    void aFarFutureStampIsWitnessedMisbehavior() throws Exception {
        Duration beyondCeiling = Duration.ofMillis(HybridLogicalClock.MAX_DRIFT_MILLIS
                + PeerNode.FAR_FUTURE_SKEW_MARGIN_MILLIS + 1);
        for (int i = 0; i < PeerNode.STRIKE_THRESHOLD - 1; i++) {
            injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(), beyondCeiling, i, news("n" + i)));
        }
        assertThat(newsAtB).as("every far-future frame was dropped").isEmpty();
        assertThat(b.quarantined(a.peerId())).as("one strike short").isFalse();
        assertThat(b.framesFarFuture()).as("every drop is counted").isEqualTo(PeerNode.STRIKE_THRESHOLD - 1);
        assertThat(b.framesFarFutureSkewed()).isZero();

        injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(), beyondCeiling, 99, news("last")));
        assertThat(b.quarantined(a.peerId())).isTrue();
        assertThat(b.channelModes()).as("the link is cut").doesNotContainKey(a.peerId());

        injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(), Duration.ZERO, 0, news("honest")));
        assertThat(newsAtB).as("a quarantined peer's honest frames are refused too").isEmpty();
    }

    /**
     * HLC logical counter, 2026-11-08 (Rust team report): a member's frame stamped just under
     * the drift ceiling with the logical counter at its maximum froze B's clock on that stamp,
     * so every frame B sent afterwards repeated it and A dropped them as replays. The frame is
     * now refused before the clock merge, struck and counted; a counter at exactly
     * LOGICAL_LIMIT is honest and delivered.
     */
    @Test
    void aMemberFrameWithALogicalCounterAboveTheLimitIsRefusedAndStruck() throws Exception {
        List<String> newsAtA = new CopyOnWriteArrayList<>();
        ra.gossip().onStream("news", (from, id, payload) ->
                newsAtA.add(new String(payload, StandardCharsets.UTF_8)));
        Duration nearCeiling = Duration.ofMillis(HybridLogicalClock.MAX_DRIFT_MILLIS - 1_000);

        injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(), nearCeiling,
                Integer.MAX_VALUE, news("frozen")));

        assertThat(newsAtB).as("the hostile frame was refused").isEmpty();
        assertThat(b.strikes(a.peerId())).as("struck once, not sentenced").isEqualTo(1);
        assertThat(b.framesLogicalRefused()).isEqualTo(1);
        for (int i = 0; i < 3; i++) {
            rb.gossip().publish("news", "from-b-" + i, bytes("b" + i));
        }
        assertThat(newsAtA).as("B's clock still advances, so A drops none of its frames as replays")
                .containsExactly("b0", "b1", "b2");

        injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(), Duration.ZERO,
                HybridLogicalClock.LOGICAL_LIMIT, news("at-limit")));
        assertThat(newsAtB).as("a counter at the limit is honest").containsExactly("at-limit");
        assertThat(b.strikes(a.peerId())).isEqualTo(1);
        assertThat(b.framesLogicalRefused()).isEqualTo(1);
    }

    /**
     * HLC logical counter, 2026-11-08 (Rust team report): in an OPEN group a newcomer's own
     * introduction admits it and merges its stamp, so anyone could freeze a member's clock.
     * The introduction is now refused before admission, struck and counted; the same newcomer
     * with an honest stamp is admitted.
     */
    @Test
    void anIntroductionWithALogicalCounterAboveTheLimitIsRefusedAndStruck() throws Exception {
        List<String> newsAtA = new CopyOnWriteArrayList<>();
        ra.gossip().onStream("news", (from, id, payload) ->
                newsAtA.add(new String(payload, StandardCharsets.UTF_8)));
        PeerIdentity newcomer = PeerIdentity.generate();
        ai.badmonkey.agentspaces.common.codec.CborCodec codec =
                ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec();
        byte[] adBytes = codec.toBytes(new PeerAdvertisement(
                "aspace://" + groupId.value() + "/peer/" + newcomer.peerId().value(),
                newcomer.peerId(), groupId, clock.instant(), Duration.ofMinutes(10),
                List.of(new PeerAdvertisement.Endpoint("mem", "nowhere", 0)),
                java.util.Set.of(), Map.of()));
        Bodies.Rumor introduction = new Bodies.Rumor("peers",
                "peer:" + newcomer.peerId().value() + ":" + clock.millis(), 6,
                codec.toBytes(new PeerNode.SignedPeerAd(adBytes, newcomer.rawPublicKey(),
                        newcomer.sign(adBytes))));
        HlcTimestamp hostile = new HlcTimestamp(
                clock.millis() + HybridLogicalClock.MAX_DRIFT_MILLIS - 1_000, Integer.MAX_VALUE,
                newcomer.peerId().value());

        injector.send(TestFrames.signed(newcomer, groupId, Envelope.Kind.RUMOR, b.peerId(),
                hostile, introduction));

        assertThat(rb.membership().member(newcomer.peerId())).as("not admitted").isEmpty();
        assertThat(b.strikes(newcomer.peerId())).isEqualTo(1);
        assertThat(b.framesLogicalRefused()).isEqualTo(1);
        for (int i = 0; i < 3; i++) {
            rb.gossip().publish("news", "from-b-" + i, bytes("b" + i));
        }
        assertThat(newsAtA).as("B's clock still advances, so A drops none of its frames as replays")
                .containsExactly("b0", "b1", "b2");

        injector.send(TestFrames.signed(newcomer, groupId, Envelope.Kind.RUMOR, b.peerId(),
                clock, HybridLogicalClock.LOGICAL_LIMIT, introduction));
        assertThat(rb.membership().member(newcomer.peerId()))
                .as("the same introduction with an honest stamp admits it").isPresent();
        assertThat(b.framesLogicalRefused()).isEqualTo(1);
    }

    /**
     * HLC logical counter, 2026-11-08 (Rust team report), the quarantine probe: A's frame
     * stamped at exactly the drift ceiling with the counter at LOGICAL_LIMIT is honest, and B
     * accepts it. B's clock merged it and carried one millisecond past the ceiling, so every
     * frame B sent next failed the far-future check (ASF-020) at the other members, which
     * struck the honest B until quarantine. The merge now clamps inside the ceiling, the carry
     * lands on it, and no member strikes B.
     */
    @Test
    void noMemberStrikesTheReceiverOfAFrameAtTheDriftCeilingWithTheCounterAtTheLimit() throws Exception {
        PeerNode c = node("c", 3);
        GroupRuntime rc = c.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(4);
        assertThat(rc.membership().member(b.peerId())).isPresent();
        List<String> newsAtC = new CopyOnWriteArrayList<>();
        rc.gossip().onStream("news", (from, id, payload) ->
                newsAtC.add(new String(payload, StandardCharsets.UTF_8)));
        int strikesOfBAtA = a.strikes(b.peerId());
        int strikesOfBAtC = c.strikes(b.peerId());

        injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(),
                Duration.ofMillis(HybridLogicalClock.MAX_DRIFT_MILLIS), HybridLogicalClock.LOGICAL_LIMIT,
                news("at-ceiling")));
        assertThat(newsAtB).as("the frame at the ceiling is honest").containsExactly("at-ceiling");
        assertThat(b.strikes(a.peerId())).isZero();
        assertThat(b.framesLogicalRefused()).isZero();

        List<String> fromB = new ArrayList<>();
        for (int i = 0; i < PeerNode.STRIKE_THRESHOLD + 2; i++) {
            rb.gossip().publish("news", "from-b-" + i, bytes("b" + i));
            fromB.add("b" + i);
        }
        assertThat(newsAtC).as("C delivers every frame B sent after the merge").containsAll(fromB);
        assertThat(c.strikes(b.peerId())).as("C never struck B").isEqualTo(strikesOfBAtC);
        assertThat(a.strikes(b.peerId())).as("A never struck B").isEqualTo(strikesOfBAtA);
        assertThat(c.quarantined(b.peerId())).isFalse();
        assertThat(a.quarantined(b.peerId())).isFalse();
    }

    /**
     * HLC canonical stamps, 2026-11-08 (decision B): a stamp past the drift ceiling by no more
     * than the skew margin is what an honest peer sends when an attacker dragged its clock to
     * its own ceiling and the receiver's wall runs a little behind. The receiver drops and counts
     * each such frame and strikes nobody, however many arrive; one millisecond further is struck.
     */
    @Test
    void aStampWithinTheSkewMarginPastTheCeilingIsDroppedAndCountedButNeverStruck() throws Exception {
        for (int i = 0; i < PeerNode.STRIKE_THRESHOLD + 2; i++) {
            injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(),
                    Duration.ofMillis(HybridLogicalClock.MAX_DRIFT_MILLIS + 1 + i), 0, news("skewed" + i)));
        }
        injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(), Duration.ofMillis(
                HybridLogicalClock.MAX_DRIFT_MILLIS + PeerNode.FAR_FUTURE_SKEW_MARGIN_MILLIS), 0, news("edge")));

        assertThat(newsAtB).as("every frame past the ceiling was dropped").isEmpty();
        assertThat(b.framesFarFutureSkewed()).isEqualTo(PeerNode.STRIKE_THRESHOLD + 3);
        assertThat(b.strikes(a.peerId())).as("within the margin nobody is struck").isZero();
        assertThat(b.quarantined(a.peerId())).isFalse();
        assertThat(b.framesFarFuture()).isZero();

        injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(), Duration.ofMillis(
                HybridLogicalClock.MAX_DRIFT_MILLIS + PeerNode.FAR_FUTURE_SKEW_MARGIN_MILLIS + 1), 0, news("past")));
        assertThat(b.strikes(a.peerId())).as("past the margin the sender is struck").isEqualTo(1);
        assertThat(b.framesFarFuture()).isEqualTo(1);
        assertThat(b.framesFarFutureSkewed()).isEqualTo(PeerNode.STRIKE_THRESHOLD + 3);

        injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(), Duration.ZERO, 0, news("honest")));
        assertThat(newsAtB).containsExactly("honest");
    }

    /**
     * HLC canonical stamps, 2026-11-08 (decision B), the skew probe: A drags B's clock to B's
     * drift ceiling with an honest stamp at the ceiling, and C's wall runs one second behind B's.
     * Every frame B sends next lands one second past C's ceiling. Before the skew margin C struck
     * the honest B for each and quarantined it; now C drops and counts them and never strikes B,
     * while a frame B signs ten seconds past C's ceiling still strikes.
     */
    @Test
    void theSkewProbeAReceiverOneSecondBehindNeverStrikesAPeerDraggedToItsCeiling() throws Exception {
        PeerNode c = PeerNode.builder(PeerIdentity.generate())
                .clock(() -> clock.instant().minusSeconds(1)).randomSeed(3).build();
        c.listen(network.register("c"), "c");
        nodes.add(c);
        GroupRuntime rc = c.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(6);
        assertThat(rc.membership().member(b.peerId())).isPresent();
        assertThat(c.strikes(b.peerId())).isZero();

        injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(),
                Duration.ofMillis(HybridLogicalClock.MAX_DRIFT_MILLIS), HybridLogicalClock.LOGICAL_LIMIT,
                news("drag")));
        assertThat(newsAtB).as("the frame at B's ceiling is honest").containsExactly("drag");
        long skewedBefore = c.framesFarFutureSkewed();
        for (int i = 0; i < PeerNode.STRIKE_THRESHOLD + 2; i++) {
            rb.gossip().publish("news", "from-b-" + i, bytes("b" + i));
        }
        tickAll(3);

        assertThat(c.framesFarFutureSkewed()).as("C drops B's frames within the margin and counts them")
                .isGreaterThan(skewedBefore);
        assertThat(c.strikes(b.peerId())).as("C never strikes B").isZero();
        assertThat(c.quarantined(b.peerId())).isFalse();
        assertThat(c.framesFarFuture()).isZero();

        TransportConnection toC = network.register("injector-c").dial("c");
        long wallAtC = clock.millis() - 1_000;
        HlcTimestamp tooFar = new HlcTimestamp(wallAtC + HybridLogicalClock.MAX_DRIFT_MILLIS + 10_000,
                0, b.peerId().value());
        toC.send(TestFrames.signed(b.identity(), groupId, Envelope.Kind.RUMOR, c.peerId(), tooFar,
                news("ten-seconds-past")));
        assertThat(c.strikes(b.peerId())).as("ten seconds past C's ceiling is still struck").isEqualTo(1);
        assertThat(c.framesFarFuture()).isEqualTo(1);
    }

    /** WS5: a verified member sending bodies its kind cannot carry accumulates strikes into quarantine. */
    @Test
    void aMalformedBodyFromAMemberStrikes() throws Exception {
        for (int i = 0; i < PeerNode.STRIKE_THRESHOLD; i++) {
            injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(), Duration.ZERO, i,
                    new byte[]{9, 9, 9}));
        }

        assertThat(b.quarantined(a.peerId())).isTrue();
        assertThat(newsAtB).isEmpty();
    }

    /** WS5 via {@code GroupRuntime.reportMisbehavior}: reports quarantine locally, and the sentence lapses. */
    @Test
    void reportedMisbehaviorQuarantinesAndTheSentenceLapses() throws Exception {
        ra.gossip().publish("news", "pre", bytes("before"));
        assertThat(newsAtB).containsExactly("before");

        for (int i = 0; i < PeerNode.STRIKE_THRESHOLD; i++) {
            rb.reportMisbehavior(a.peerId(), "witnessed #" + i);
        }
        assertThat(b.quarantined(a.peerId())).isTrue();
        assertThat(b.channelModes()).doesNotContainKey(a.peerId());

        ra.gossip().publish("news", "during", bytes("during"));
        tickAll(2);
        ra.gossip().publish("news", "during-2", bytes("during-2"));
        assertThat(newsAtB).as("nothing from A lands while quarantined").containsExactly("before");
        assertThat(rb.membership().member(a.peerId()))
                .as("quarantine is not eviction; liveness simply stops being credited").isPresent();

        clock.advance(PeerNode.QUARANTINE.plusSeconds(1));
        assertThat(b.quarantined(a.peerId())).as("the local sentence lapsed").isFalse();
        tickAll(3);
        ra.gossip().publish("news", "after", bytes("after"));
        // Since v0.1.17 A's first probe after the sentence carries the items it forwarded
        // meanwhile and B grafts them: gossip catches up once the sentence is over, which is
        // not a breach of it. Only the order and the absence of duplicates are fixed.
        assertThat(newsAtB).containsSubsequence("before", "after").doesNotHaveDuplicates();
        assertThat(newsAtB.get(0)).isEqualTo("before");
    }

    /** Spec §11 (ASF-010) reversed: a stale or replayed frame is never a strike, so replaying a victim cannot quarantine it. */
    @Test
    void replaysAndStaleFramesNeverStrikeTheOriginalSigner() throws Exception {
        byte[] frame = fromA(groupId, Envelope.Kind.RUMOR, b.peerId(), Duration.ZERO, 0, news("victim"));
        for (int i = 0; i < PeerNode.STRIKE_THRESHOLD * 2; i++) {
            injector.send(frame);
            injector.send(fromA(groupId, Envelope.Kind.RUMOR, b.peerId(), Duration.ofMinutes(-6), i,
                    news("old" + i)));
        }

        assertThat(b.quarantined(a.peerId())).isFalse();
        assertThat(newsAtB).containsExactly("victim");
    }

    /** Spec §4.4/§5.1/§10.1: a seed answering a join-by-GroupID with an advertisement whose id does not re-derive is ignored; the join fails and no runtime exists. */
    @Test
    void aForgedFoundingAdvertisementFromASeedIsRefused() throws Exception {
        PeerIdentity evil = PeerIdentity.generate();
        PeerIdentity founderId = PeerIdentity.generate();
        SignedGroupAdvertisement genuine = GroupFounding.found(founderId, "fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults(), clock.instant(), Duration.ofDays(1));
        GroupId wanted = genuine.advertisement().group();
        // The seed's own INVITE group, relabelled with the wanted GroupID: the
        // signature is genuine (the seed's), the id simply is not this document's.
        SignedGroupAdvertisement evilOwn = GroupFounding.found(evil, "fleet",
                GroupAdvertisement.MembershipPolicy.INVITE, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults(), clock.instant(), Duration.ofDays(1));
        GroupAdvertisement e = evilOwn.advertisement();
        SignedGroupAdvertisement forged = new SignedGroupAdvertisement(new GroupAdvertisement(
                "aspace://" + wanted.value(), e.issuer(), wanted, e.issued(), e.ttl(), e.name(),
                e.membershipPolicy(), e.defaultStrategy(), e.gossip()),
                evilOwn.founderPublicKey(), evilOwn.signature());

        AtomicInteger wants = new AtomicInteger();
        ai.badmonkey.agentspaces.peering.wire.WireCodec wire =
                new ai.badmonkey.agentspaces.peering.wire.WireCodec();
        network.register("evil").listen("evil", connection -> connection.onReceive(frame ->
                wire.decode(frame).ifPresent(envelope -> {
                    if (envelope.kind() == Envelope.Kind.GROUP_AD_WANT) {
                        wants.incrementAndGet();
                        try {
                            connection.send(TestFrames.signed(evil, envelope.group(),
                                    Envelope.Kind.GROUP_AD, envelope.from(), clock,
                                    wants.get(), forged));
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    }
                })));

        PeerNode newcomer = node("c", 3);
        assertThatThrownBy(() -> newcomer.joinGroup(wanted, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "evil", 0)), Duration.ofMillis(300)))
                .isInstanceOf(java.util.concurrent.TimeoutException.class);

        assertThat(wants).as("the seed was asked").hasValue(1);
        assertThat(newcomer.group(wanted)).as("no runtime was created").isEmpty();
        assertThat(newcomer.channelModes()).as("nothing was cached").isEmpty();
    }

    /** Spec §11: with a frozen nanosecond source and capacity 20, the 21st signed frame from a member is dropped and struck; refill follows the source. */
    @Test
    void aFloodingMemberIsThrottledByItsTokenBucketAndStruck() throws Exception {
        AtomicLong nanos = new AtomicLong(1_000_000_000L);
        PeerNode c = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(3)
                .rateLimit(20, 10, nanos::get).build();
        c.listen(network.register("c"), "c");
        nodes.add(c);
        GroupRuntime rc = c.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(4);
        assertThat(rc.membership().member(a.peerId())).isPresent();
        List<String> newsAtC = new CopyOnWriteArrayList<>();
        rc.gossip().onStream("news", (from, id, payload) ->
                newsAtC.add(new String(payload, StandardCharsets.UTF_8)));
        TransportConnection toC = network.register("injector-c").dial("c");
        // Let A's bucket at C refill to capacity, then freeze the source.
        nanos.addAndGet(Duration.ofMinutes(1).toNanos());

        for (int i = 0; i < 20; i++) {
            toC.send(fromA(groupId, Envelope.Kind.RUMOR, c.peerId(), Duration.ZERO, i, news("n" + i)));
        }
        assertThat(newsAtC).hasSize(20);
        assertThat(c.strikes(a.peerId())).isZero();

        toC.send(fromA(groupId, Envelope.Kind.RUMOR, c.peerId(), Duration.ZERO, 20, news("n20")));
        assertThat(newsAtC).as("the 21st frame in a frozen instant is dropped").hasSize(20);
        assertThat(c.strikes(a.peerId())).as("a rate-limit breach is witnessed misbehavior").isEqualTo(1);

        // One second later ten tokens are back.
        nanos.addAndGet(Duration.ofSeconds(1).toNanos());
        for (int i = 21; i < 31; i++) {
            toC.send(fromA(groupId, Envelope.Kind.RUMOR, c.peerId(), Duration.ZERO, i, news("n" + i)));
        }
        assertThat(newsAtC).hasSize(30);
        toC.send(fromA(groupId, Envelope.Kind.RUMOR, c.peerId(), Duration.ZERO, 31, news("n31")));
        assertThat(newsAtC).hasSize(30);
        assertThat(c.strikes(a.peerId())).isEqualTo(2);
    }
}
