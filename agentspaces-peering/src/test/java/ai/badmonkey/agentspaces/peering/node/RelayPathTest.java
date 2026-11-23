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
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Measurement;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.SimWait;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.measure.Wait;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Relay paths (srad/large-networks-bulk-data.md §3.7, group R): two dial-only
 * kits behind two dial-only relays that both hang off one listening relay at
 * headquarters, so a frame between the kits crosses three relays, every frame
 * of the flow on the same pinned path; a loop is refused; a drop signals the
 * origin, which forgets the path.
 */
class RelayPathTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private GroupAdvertisement groupAd;
    private GroupId groupId;

    @BeforeEach
    void setUp() {
        groupId = GroupId.of("zRelayPaths");
        groupAd = new GroupAdvertisement("aspace://zRelayPaths",
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(1), "relay-paths",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                new GroupAdvertisement.GossipParameters(3, Duration.ofSeconds(1)));
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode listening(String address, long seed, Set<PeerAdvertisement.PeerRole> roles) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed).roles(roles).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    /** A node nobody can dial: it only dials out. */
    private PeerNode dialOnly(String address, long seed, Set<PeerAdvertisement.PeerRole> roles) {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed).roles(roles).build();
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

    private static final Set<PeerAdvertisement.PeerRole> RELAY = Set.of(PeerAdvertisement.PeerRole.RELAY);

    /** HQ listens; RA and RB are relays that only dial out to HQ; A1 and B1 are kits that only dial their site's relay. */
    private record Fleet(PeerNode hq, PeerNode ra, PeerNode rb, PeerNode a1, PeerNode b1,
                         GroupRuntime a1Runtime, GroupRuntime b1Runtime) {
    }

    private Fleet nestedNat() throws IOException {
        PeerNode hq = listening("hq", 1, RELAY);
        PeerNode ra = listening("ra", 2, RELAY);
        PeerNode rb = listening("rb", 3, RELAY);
        PeerNode a1 = dialOnly("a1", 4, Set.of());
        PeerNode b1 = dialOnly("b1", 5, Set.of());
        // Site relays listen for their kits but cannot reach each other or be reached by the other site.
        network.partition("ra", "rb");
        network.partition("a1", "rb");
        network.partition("a1", "hq");
        network.partition("a1", "b1");
        network.partition("b1", "ra");
        network.partition("b1", "hq");
        hq.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        ra.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("hq"));
        rb.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("hq"));
        GroupRuntime a1Runtime = a1.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("ra"));
        GroupRuntime b1Runtime = b1.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("rb"));
        // membership and the reach hints cross four hops (b1, rb, hq, ra, a1) before a path can be computed
        for (int i = 0; i < 200 && !(a1.viaOf(groupId, b1.peerId()).contains(rb.peerId())
                && b1.viaOf(groupId, a1.peerId()).contains(ra.peerId())
                && a1.relayLinksOf(groupId, hq.peerId()).containsAll(List.of(ra.peerId(), rb.peerId()))
                && b1.relayLinksOf(groupId, hq.peerId()).containsAll(List.of(ra.peerId(), rb.peerId()))); i++) {
            tickAll(1);
        }
        assertThat(a1.viaOf(groupId, b1.peerId())).as("a1 heard b1's reach hint").containsExactly(rb.peerId());
        assertThat(a1.relayLinksOf(groupId, hq.peerId())).as("a1 heard hq's links").contains(ra.peerId(), rb.peerId());
        assertThat(a1.relayLinksOf(groupId, ra.peerId())).as("a1 heard ra's links").contains(hq.peerId());
        assertThat(b1.relayLinksOf(groupId, rb.peerId())).as("b1 heard rb's links").contains(hq.peerId());
        return new Fleet(hq, ra, rb, a1, b1, a1Runtime, b1Runtime);
    }

    private static long forwarded(PeerNode relay, GroupId group) {
        PeerNode.RelayStats stats = relay.relayStats().get(group);
        return stats == null ? 0 : stats.framesForwarded() + stats.forwardedToRelay();
    }

    @Test
    void aFrameCrossesThreeRelaysOnOnePinnedPathAndTheReplyComesBack() throws Exception {
        Fleet f = nestedNat();
        assertThat(f.a1Runtime().membership().member(f.b1().peerId())).as("the view carries b1").isPresent();
        assertThat(f.a1Runtime().viaRelays()).containsExactly(f.ra().peerId());
        assertThat(f.b1Runtime().viaRelays()).containsExactly(f.rb().peerId());

        List<String> atB1 = new CopyOnWriteArrayList<>();
        List<String> atA1 = new CopyOnWriteArrayList<>();
        f.b1Runtime().onKind(Envelope.Kind.PIPE_DATA, (from, body) -> {
            atB1.add(new String(body, StandardCharsets.UTF_8));
            f.b1Runtime().send(from, Envelope.Kind.PIPE_DATA, "reply".getBytes(StandardCharsets.UTF_8));
        });
        f.a1Runtime().onKind(Envelope.Kind.PIPE_DATA, (from, body) -> atA1.add(new String(body, StandardCharsets.UTF_8)));

        for (int i = 0; i < 10; i++) {
            f.a1Runtime().send(f.b1().peerId(), Envelope.Kind.PIPE_DATA, ("hello " + i).getBytes(StandardCharsets.UTF_8));
        }
        tickAll(2);
        assertThat(atB1).hasSize(10);
        assertThat(atA1).hasSize(10);
        assertThat(f.a1().pinnedPath(groupId, f.b1().peerId()))
                .contains(List.of(f.ra().peerId(), f.hq().peerId(), f.rb().peerId()));
        assertThat(f.b1().pinnedPath(groupId, f.a1().peerId()))
                .contains(List.of(f.rb().peerId(), f.hq().peerId(), f.ra().peerId()));
        assertThat(forwarded(f.ra(), groupId)).isGreaterThanOrEqualTo(20);
        assertThat(forwarded(f.hq(), groupId)).isGreaterThanOrEqualTo(20);
        assertThat(forwarded(f.rb(), groupId)).isGreaterThanOrEqualTo(20);
        assertThat(f.a1Runtime().hopsTo(f.b1().peerId())).isEqualTo(3);
        assertThat(f.a1Runtime().hopsTo(f.ra().peerId())).isZero();
        // LB-R-09: each relay sees the frames as pipe data; LB-R-12: the origin's offload counts the
        // flow at the first hop of its pinned path, as it counts a one-hop flow
        assertThat(f.hq().relayBytesByKind(groupId)).containsKey("PIPE_DATA");
        assertThat(f.hq().relayBytesByKind(groupId).get("PIPE_DATA")).isGreaterThan(0L);
        assertThat(f.a1().relayedWithInLastMinute(groupId, f.b1().peerId())).isGreaterThanOrEqualTo(10);
        // W09c (phase-6 gate, EL-L-03): the first pin gave the pins participant a deadline where it had none, so it
        // signalled the participant; frames on a pinned path signal nothing more.
        String pins = groupId.value() + "/pins";
        long pinWakes = f.a1().loopStats().wakeCalls().getOrDefault(pins, 0L);
        assertThat(pinWakes).as("the first pin signals the pins participant").isPositive();
        for (int i = 0; i < 10; i++) {
            f.a1Runtime().send(f.b1().peerId(), Envelope.Kind.PIPE_DATA, ("again " + i).getBytes(StandardCharsets.UTF_8));
        }
        tickAll(2);
        assertThat(atB1).hasSize(20);
        assertThat(f.a1().loopStats().wakeCalls().getOrDefault(pins, 0L)).as("no wake per frame on a held pin")
                .isEqualTo(pinWakes);
    }

    @Test
    void equalCostPathsConvergeOnOneRelayAndStick() throws Exception {
        PeerNode hq1 = listening("hq1", 1, RELAY);
        PeerNode hq2 = listening("hq2", 6, RELAY);
        PeerNode ra = listening("ra", 2, RELAY);
        PeerNode rb = listening("rb", 3, RELAY);
        PeerNode a1 = dialOnly("a1", 4, Set.of());
        PeerNode b1 = dialOnly("b1", 5, Set.of());
        network.partition("ra", "rb");
        for (String kit : List.of("a1", "b1")) {
            network.partition(kit, "hq1");
            network.partition(kit, "hq2");
        }
        network.partition("a1", "rb");
        network.partition("b1", "ra");
        network.partition("a1", "b1");
        hq1.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        hq2.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("hq1"));
        ra.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("hq1"));
        rb.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("hq1"));
        GroupRuntime a1Runtime = a1.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("ra"));
        GroupRuntime b1Runtime = b1.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("rb"));
        for (int i = 0; i < 200 && !(a1.viaOf(groupId, b1.peerId()).contains(rb.peerId())
                && a1.relayLinksOf(groupId, ra.peerId()).containsAll(List.of(hq1.peerId(), hq2.peerId()))
                && a1.relayLinksOf(groupId, rb.peerId()).containsAll(List.of(hq1.peerId(), hq2.peerId()))); i++) {
            tickAll(1); // membership, the mesh (ra and rb to both hq relays), and the hints cross the chain
        }
        assertThat(a1.viaOf(groupId, b1.peerId())).containsExactly(rb.peerId());
        assertThat(a1.relayLinksOf(groupId, ra.peerId())).contains(hq1.peerId(), hq2.peerId());
        assertThat(a1.relayLinksOf(groupId, rb.peerId())).contains(hq1.peerId(), hq2.peerId());

        List<String> atB1 = new CopyOnWriteArrayList<>();
        b1Runtime.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> atB1.add("x"));
        long hq1Before = forwarded(hq1, groupId);
        long hq2Before = forwarded(hq2, groupId);
        for (int i = 0; i < 20; i++) {
            a1Runtime.send(b1.peerId(), Envelope.Kind.PIPE_DATA, "frame".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(atB1).hasSize(20);
        long viaHq1 = forwarded(hq1, groupId) - hq1Before; // the sends deliver inline, so nothing else moved
        long viaHq2 = forwarded(hq2, groupId) - hq2Before;
        assertThat(viaHq1 + viaHq2).isEqualTo(20);
        assertThat(Math.min(viaHq1, viaHq2)).as("one middle relay carried the whole flow").isZero();
        assertThat(a1.pinnedPath(groupId, b1.peerId())).isPresent();
        assertThat(a1.pinnedPath(groupId, b1.peerId()).get()).hasSize(3);
    }

    @Test
    void aDropAtTheMiddleSignalsTheOriginWhichForgetsThePath() throws Exception {
        Fleet f = nestedNat();
        f.b1Runtime().onKind(Envelope.Kind.PIPE_DATA, (from, body) -> { });
        f.a1Runtime().send(f.b1().peerId(), Envelope.Kind.PIPE_DATA, "one".getBytes(StandardCharsets.UTF_8));
        assertThat(f.a1().pinnedPath(groupId, f.b1().peerId())).isPresent();

        // the wire between hq and rb is cut and hq cannot redial rb
        network.partition("hq", "rb");
        f.hq().dropLinksTo(f.rb().peerId());
        f.a1Runtime().send(f.b1().peerId(), Envelope.Kind.PIPE_DATA, "two".getBytes(StandardCharsets.UTF_8));
        PeerNode.RelayStats hqStats = f.hq().relayStats().get(groupId);
        assertThat(hqStats.droppedUnreachable()).isGreaterThanOrEqualTo(1);
        assertThat(f.a1().unreachableSignalsReceived()).as("the signal reached the origin").isGreaterThanOrEqualTo(1);
        assertThat(f.a1().pinnedPath(groupId, f.b1().peerId())).as("the path is forgotten").isEmpty();
    }

    /**
     * A signal names a failed step, not a dead relay. a1 reaches rb (a listener) through ra in
     * one hop, but ra cannot reach rb, so ra signals a1 with rb as the failed hop. The pin to b1
     * runs ra, hq, rb and never takes the step from ra to rb, so it stays. Forgetting every pin
     * through rb made the restart test below fail about 1 run in 10: the last frame arrived on
     * the pin, then a signal about a1's own probe of rb landed in the same tick and wiped it.
     */
    @Test
    void aSignalAboutOneStepKeepsAPinThatReachesTheHopFromElsewhere() throws Exception {
        Fleet f = nestedNat();
        f.b1Runtime().onKind(Envelope.Kind.PIPE_DATA, (from, body) -> { });
        f.a1Runtime().send(f.b1().peerId(), Envelope.Kind.PIPE_DATA, "one".getBytes(StandardCharsets.UTF_8));
        List<PeerId> toB1 = List.of(f.ra().peerId(), f.hq().peerId(), f.rb().peerId());
        assertThat(f.a1().pinnedPath(groupId, f.b1().peerId())).contains(toB1);

        // past ra's ten-second signal window for a1 and rb, which the setup's gossip may have used
        clock.advance(Duration.ofSeconds(11));
        long signals = f.a1().unreachableSignalsReceived();
        f.a1Runtime().send(f.rb().peerId(), Envelope.Kind.PIPE_DATA, "probe".getBytes(StandardCharsets.UTF_8));
        assertThat(f.a1().unreachableSignalsReceived()).as("ra could not reach rb and said so")
                .isEqualTo(signals + 1);
        assertThat(f.a1().pinnedPath(groupId, f.rb().peerId())).as("the path to rb is forgotten").isEmpty();
        assertThat(f.a1().pinnedPath(groupId, f.b1().peerId())).as("the pin to b1 never steps from ra to rb")
                .contains(toB1);
    }

    @Test
    void crossesStepNamesOnlyTheStepFromTheSignallerToTheFailedHop() {
        PeerId ra = PeerId.of("zRa");
        PeerId hq = PeerId.of("zHq");
        PeerId rb = PeerId.of("zRb");
        PeerId b1 = PeerId.of("zB1");
        List<PeerId> path = List.of(ra, hq, rb);
        assertThat(PeerNode.crossesStep(path, b1, ra, hq)).isTrue();
        assertThat(PeerNode.crossesStep(path, b1, hq, rb)).isTrue();
        assertThat(PeerNode.crossesStep(path, b1, rb, b1)).as("the last relay to the target").isTrue();
        assertThat(PeerNode.crossesStep(path, b1, ra, rb)).as("ra never steps to rb here").isFalse();
        assertThat(PeerNode.crossesStep(path, b1, b1, ra)).as("a signaller off the path").isFalse();
        assertThat(PeerNode.crossesStep(List.of(ra), rb, ra, rb)).as("a one-hop path").isTrue();
    }

    @Test
    void aMalformedPathIsRefusedAndCounted() throws Exception {
        Fleet f = nestedNat();
        PeerId self = f.hq().peerId();
        assertThat(RelayPaths.wellFormed(List.of(f.ra().peerId(), self, f.rb().peerId()), 1, self, f.b1().peerId(), 4)).isTrue();
        assertThat(RelayPaths.wellFormed(List.of(f.ra().peerId(), self, f.ra().peerId()), 1, self, f.b1().peerId(), 4))
                .as("a repeated relay").isFalse();
        assertThat(RelayPaths.wellFormed(List.of(f.ra().peerId(), self, f.rb().peerId()), 2, self, f.b1().peerId(), 4))
                .as("the hop does not name the receiver").isFalse();
        assertThat(RelayPaths.wellFormed(List.of(f.ra().peerId(), self, f.rb().peerId(), f.b1().peerId()), 1, self, f.b1().peerId(), 4))
                .as("the target on the path").isFalse();
        assertThat(RelayPaths.wellFormed(List.of(f.ra().peerId(), self), 1, self, f.b1().peerId(), 1))
                .as("over the hop bound").isFalse();
    }

    @Test
    void shortestPathBreaksTiesOnTheLowestHashSoEveryOriginAgrees() {
        PeerId x = PeerId.of("zX");
        PeerId y = PeerId.of("zY");
        PeerId left = PeerId.of("zLeft");
        PeerId right = PeerId.of("zRight");
        RelayPaths.Graph graph = new RelayPaths.Graph(Set.of(x, y, left, right),
                Map.of(x, Set.of(left, right), y, Set.of(left, right)), Map.of());
        List<PeerId> first = RelayPaths.shortest(graph, Set.of(x), Set.of(y), 4).orElseThrow();
        List<PeerId> again = RelayPaths.shortest(graph, Set.of(x), Set.of(y), 4).orElseThrow();
        assertThat(first).hasSize(3).startsWith(x).endsWith(y);
        assertThat(again).isEqualTo(first);
        assertThat(RelayPaths.shortest(graph, Set.of(x), Set.of(y), 2)).as("too long for the bound").isEmpty();
        assertThat(RelayPaths.shortest(graph, Set.of(x), Set.of(x), 4).orElseThrow()).containsExactly(x);
    }

    /** LB-Q-REL-07: the middle relay restarts; the flow repins and every frame arrives, in order, before and after. */
    @Test
    void aPinnedPathSurvivesARestartOfTheMiddleRelayAndDeliversInOrder() throws Exception {
        Fleet f = nestedNat();
        List<Integer> got = new CopyOnWriteArrayList<>();
        f.b1Runtime().onKind(Envelope.Kind.PIPE_DATA, (from, body) -> got.add(Integer.parseInt(new String(body, StandardCharsets.UTF_8))));
        for (int i = 0; i < 50; i++) {
            f.a1Runtime().send(f.b1().peerId(), Envelope.Kind.PIPE_DATA, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }
        tickAll(2);
        assertThat(got).containsExactlyElementsOf(java.util.stream.IntStream.range(0, 50).boxed().toList());

        PeerIdentity hqIdentity = f.hq().identity();
        f.hq().close();
        nodes.remove(f.hq());
        tickAll(3);
        PeerNode hq = PeerNode.builder(hqIdentity).clock(clock).randomSeed(1).roles(RELAY).build();
        hq.listen(network.register("hq"), "hq");
        nodes.add(hq);
        hq.joinGroup(groupAd, GroupMembership.Config.defaults(), seed("ra"));
        int[] sent = {50};
        // Typically 73 to 81 ticks (median 76 over 160 runs, slowest 125): the restarted hq starts with
        // an empty view and drops a1's frames for b1 as a non-member's until b1's advertisement reaches
        // it through rb, and a1 and ra may have let b1 go while hq was down and must admit it again;
        // after that, one frame a tick, since each frame waits for the one before it. The root cause of
        // the old failure was a signal about one step wiping a pin that never takes it (the test above).
        // fm: (a) ceiling 400 ticks (policy: was 120, under the slowest convergences seen; 400 is clearly
        // broken); the ticks it took are recorded as peering.relay.pin-after-restart.
        Measurement took = SimWait.ticks("peering.relay.pin-after-restart", 400, () -> {
            if (sent[0] < 100) {
                f.a1Runtime().send(f.b1().peerId(), Envelope.Kind.PIPE_DATA,
                        String.valueOf(sent[0]).getBytes(StandardCharsets.UTF_8));
            }
            tickAll(1);
            if (sent[0] < 100 && got.contains(sent[0])) {
                sent[0]++;
            }
        }, () -> got.size() >= 100, Wait.with().diagnostics(() -> "got " + got.size() + " frames, next to send "
                + sent[0] + ", pin " + f.a1().pinnedPath(groupId, f.b1().peerId())));
        // a frame sent while the relay was down may be lost; every frame after the repin arrives, and in order
        List<Integer> after = got.subList(50, got.size());
        assertThat(after).isSorted();
        assertThat(got).contains(99);
        assertThat(f.a1().pinnedPath(groupId, f.b1().peerId()))
                .contains(List.of(f.ra().peerId(), hq.peerId(), f.rb().peerId()));
        System.out.printf("LB-Q-REL-07: %d frames after the restart, in order, on the repinned path, in %.0f ticks%n",
                after.size(), took.value());
    }

    /** LB-Q-PRF-07: a path over a graph of 64 relays computes in well under a millisecond. */
    @Test
    void aPathOverSixtyFourRelaysComputesUnderAMillisecond() {
        java.util.Random random = new java.util.Random(64);
        List<PeerId> relays = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            relays.add(PeerIdentity.generate().peerId());
        }
        Map<PeerId, Set<PeerId>> links = new java.util.HashMap<>();
        for (int i = 0; i < 64; i++) {
            Set<PeerId> out = new java.util.HashSet<>();
            out.add(relays.get((i + 1) % 64));
            for (int k = 0; k < 3; k++) {
                out.add(relays.get(random.nextInt(64)));
            }
            out.remove(relays.get(i));
            links.put(relays.get(i), out);
        }
        RelayPaths.Graph graph = new RelayPaths.Graph(Set.copyOf(relays), links, Map.of());
        Set<PeerId> start = Set.of(relays.get(0));
        Set<PeerId> goal = Set.of(relays.get(37));
        for (int i = 0; i < 200; i++) {
            RelayPaths.shortest(graph, start, goal, 4); // warm
        }
        int runs = 2_000;
        long begin = System.nanoTime();
        java.util.Optional<List<PeerId>> path = java.util.Optional.empty();
        for (int i = 0; i < runs; i++) {
            path = RelayPaths.shortest(graph, start, goal, 4);
        }
        double micros = (System.nanoTime() - begin) / 1000.0 / runs;
        System.out.printf("LB-Q-PRF-07: a path over 64 relays in %.1f microseconds (%s)%n", micros,
                path.map(p -> p.size() + " hops").orElse("no path within four hops"));
        // fm: (b) a performance claim, recorded as peering.relay.path-compute; the 1 ms of LB-Q-PRF-07 stays as
        // the requirement's own bound, far above the few microseconds it takes
        Measure.record("peering.relay.path-compute", Unit.US, Direction.LOWER, micros,
                Rec.with().kind(Kind.PERF).params(Params.of("relays", 64)));
        assertThat(micros).as("LB-Q-PRF-07").isLessThan(1000.0);
    }
}
