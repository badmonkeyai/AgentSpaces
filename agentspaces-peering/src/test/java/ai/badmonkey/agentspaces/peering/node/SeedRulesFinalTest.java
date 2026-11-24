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
import ai.badmonkey.agentspaces.common.time.TimeSource;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.LinkFilter;
import ai.badmonkey.agentspaces.test.LinkFrame;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The seed and wake rules of SPEC v0.1.37 §9.4 as the core builds them in its final round (2026-11-23, the same in
 * every client):
 *
 * <ul>
 *   <li>a timer wake more than 30 s late with no wall step (a frozen process) is a wake: the seed is PINGed again,
 *   and a member past its TTL only because this node was frozen gets a probe, not a drop;</li>
 *   <li>bytes of a frame still arriving answer the dead-link rule's PING and a wake's PING;</li>
 *   <li>the dead-link rule watches every link dialed to a seed, a listening node's too;</li>
 *   <li>the lost-member retry runs on a fixed cycling table with a spread;</li>
 *   <li>seed dials run off the loop, each on a virtual thread of its own, so a blackholed connect holds no round.</li>
 * </ul>
 *
 * <p>Everything runs on virtual time over the in-memory fabric: no sockets, no sleeps.
 */
@Timeout(120)
class SeedRulesFinalTest {

    private static final Duration TICK = Duration.ofMillis(250);

    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
    private final SimNetwork network = new SimNetwork();
    private final Wire wire = new Wire();
    private final GroupId groupId = GroupId.of("zSeedRulesFinal");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zSeedRulesFinal",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "seed-rules",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> listeners = new ArrayList<>();
    private final Map<String, GroupRuntime> runtimes = new HashMap<>();

    /** Every frame of the fabric, by link and kind with its instants; a silenced address neither sends nor hears. */
    private final class Wire implements LinkFilter {
        final Map<String, List<Long>> times = new ConcurrentHashMap<>();
        final Set<String> silent = new CopyOnWriteArraySet<>();

        @Override
        public Verdict decide(LinkFrame frame) {
            times.computeIfAbsent(frame.from() + ">" + frame.to() + ":" + frame.kind(),
                    k -> new CopyOnWriteArrayList<>()).add(time.monoMillis());
            return silent.contains(frame.from()) || silent.contains(frame.to()) ? Verdict.drop() : Verdict.pass();
        }

        List<Long> instants(String from, String to, String kind, long fromMillis) {
            return times.getOrDefault(from + ">" + to + ":" + kind, List.of()).stream()
                    .filter(t -> t >= fromMillis).distinct().toList();
        }
    }

    @BeforeEach
    void wire() {
        network.filter(null, null, wire);
    }

    @AfterEach
    void tearDown() throws Exception {
        nodes.forEach(PeerNode::close);
        for (AutoCloseable listener : listeners) {
            listener.close();
        }
    }

    private PeerNode node(String name, boolean listening, TimeSource view, String... seeds) throws IOException {
        return node(name, listening, view, network.register(name), seeds);
    }

    private PeerNode node(String name, boolean listening, TimeSource view, Transport transport, String... seeds)
            throws IOException {
        return node(name, listening, view, transport, java.util.function.UnaryOperator.identity(), seeds);
    }

    private PeerNode node(String name, boolean listening, TimeSource view, Transport transport,
                          java.util.function.UnaryOperator<PeerNode.Builder> options, String... seeds)
            throws IOException {
        PeerNode node = options.apply(PeerNode.builder(PeerIdentity.generate()).clock(view).time(view)
                .randomSeed(name.hashCode())).build();
        if (listening) {
            node.listen(transport, name);
        } else {
            node.transport(transport); // dial-only: nobody can dial it back
        }
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String seed : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", seed, 0));
        }
        runtimes.put(name, node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints));
        node.startTicking(TICK);
        nodes.add(node);
        return node;
    }

    private boolean sees(String host, PeerNode other) {
        return runtimes.get(host).membership().member(other.peerId()).isPresent();
    }

    private long until(Duration limit, Duration step, BooleanSupplier condition) {
        time.runDue();
        long start = time.monoMillis();
        while (!condition.getAsBoolean()) {
            if (time.monoMillis() - start > limit.toMillis()) {
                return -1;
            }
            time.advance(step);
        }
        return time.monoMillis() - start;
    }

    private static void assertSlot(long wait, long slot, String what) {
        assertThat(wait).as(what + ": the " + slot + " ms slot, +/-10 %")
                .isBetween(Math.round(slot * 0.9) - 1, Math.round(slot * 1.1) + 1);
    }

    // ---- decision 3: a frozen process is a wake

    @Test
    void aSixtySecondFreezeIsAWakeTheSeedIsPingedAndNoMemberIsDroppedForTheFreeze() throws Exception {
        PeerNode s1 = node("s1", true, time);
        PeerNode s2 = node("s2", true, time, "s1");
        VirtualTime.NodeTime view = time.unwatchedNode();
        PeerNode host = node("h", true, view, "s1", "s2");
        assertThat(until(Duration.ofSeconds(30), Duration.ofMillis(100),
                () -> sees("h", s1) && sees("h", s2) && sees("s1", host) && sees("s2", host))).isNotNegative();
        GroupMembership membership = runtimes.get("h").membership();
        long reprobes0 = membership.lateReprobes();
        // SIGSTOP for a minute: the node's threads stop and its frames are lost, while its monotonic clock and its
        // wall clock both count on, so no wall step is raised.
        wire.silent.add("h");
        view.freeze();
        time.advance(Duration.ofSeconds(60));
        wire.silent.remove("h");
        // The thaw: what the members send arrives a moment after the node's first run, as over any real link (the
        // fabric would otherwise answer inline, inside that run).
        network.hold("s1", "h");
        network.hold("s2", "h");
        view.resume();
        time.runDue();
        boolean keptAtFirstRun = sees("h", s1) && sees("h", s2);
        network.release("s1", "h", SimNetwork.Order.FIFO, 1);
        network.release("s2", "h", SimNetwork.Order.FIFO, 1);
        long woke = time.monoMillis();
        boolean keptBoth = keptAtFirstRun;
        for (int i = 0; i < 600; i++) { // 6 s in 10 ms steps: the first run, and the probes' whole span
            keptBoth &= sees("h", s1) && sees("h", s2);
            time.advance(Duration.ofMillis(10));
        }
        long back = until(Duration.ofSeconds(10), Duration.ofMillis(10), () -> sees("s1", host) && sees("s2", host));
        List<Long> pings = wire.instants("h", "s1", "PING", woke);
        PeerNode.SeedLinkStats stats = host.seedLinkStats();
        System.out.printf("[final] 60 s freeze: kept both %s, late reprobes %d, wake PINGs %d sent and %d adopted,"
                        + " resumes %d, late wakes %d, first PING to s1 %s ms after the thaw, back in both views %d ms"
                        + " later%n", keptBoth, membership.lateReprobes() - reprobes0, stats.wakePings(),
                membership.wakePingsAdopted(), stats.resumes(), host.lateWakes(),
                pings.isEmpty() ? "none" : pings.get(0) - woke, back);
        assertThat(host.wallStepStats().steps()).as("wall and monotonic time moved together: no wall step").isZero();
        assertThat(keptBoth).as("a member past its TTL only because this node was frozen is probed, not dropped")
                .isTrue();
        assertThat(membership.lateReprobes() - reprobes0).as("each member past its TTL got a probe").isEqualTo(2);
        assertThat(host.lateWakes()).as("the late wake reached the node").isEqualTo(1);
        assertThat(stats.wakePings())
                .as("the seed is PINGed again, as at any wake: the probe's PING to it becomes the wake's").isEqualTo(1);
        assertThat(stats.resumes()).as("a wake, counted as the clients count it").isEqualTo(1);
        assertThat(pings).isNotEmpty();
        assertThat(pings.get(0) - woke).as("at once").isLessThanOrEqualTo(10);
        assertThat(back).as("its peers, which dropped it, hold it again after the resume").isNotNegative();
    }

    // ---- decision 4: bytes of a frame still arriving answer

    @Test
    void bytesOfAFrameStillArrivingAnswerTheDeadLinkPing() throws Exception {
        PeerNode s1 = node("s1", true, time);
        PeerNode s2 = node("s2", true, time, "s1");
        PeerNode host = node("d", false, time, "s1", "s2");
        assertThat(until(Duration.ofSeconds(60), Duration.ofMillis(100),
                () -> sees("d", s1) && sees("d", s2) && sees("s1", host) && sees("s2", host))).isNotNegative();
        network.oneWay("s1", "d"); // no whole frame reaches d from s1 from now on
        long pings0 = host.seedLinkStats().seedPings();
        long pinged = until(Duration.ofSeconds(60), Duration.ofMillis(100),
                () -> host.seedLinkStats().seedPings() > pings0);
        assertThat(pinged).as("a link silent for 45 s is asked").isBetween(0L, 45_100L);
        // s1 is writing a large frame over a slow link: its bytes trickle in through the answer window.
        for (int i = 0; i < 4; i++) {
            time.advance(Duration.ofMillis(500));
            assertThat(network.progress("s1", "d")).isTrue();
        }
        time.advance(Duration.ofMillis(1_000));
        System.out.printf("[final] bytes still arriving: links closed %d after the answer window%n",
                host.seedLinkStats().linksSilent());
        assertThat(host.seedLinkStats().linksSilent()).as("bytes still arriving are the seed's answer").isZero();
        // The bytes stop: the next quiet bound asks again, and silence closes the link.
        long asked = host.seedLinkStats().seedPings();
        long again = until(Duration.ofSeconds(60), Duration.ofMillis(100), () -> host.seedLinkStats().seedPings() > asked);
        long closed = until(Duration.ofSeconds(10), Duration.ofMillis(100), () -> host.seedLinkStats().linksSilent() > 0);
        System.out.printf("[final] bytes stopped: asked again after %d ms, closed %d ms later%n", again, closed);
        assertThat(again).as("asked again 45 s after the last bytes").isBetween(40_000L, 45_100L);
        assertThat(closed).as("then closed 2 s after the unanswered PING").isBetween(1_900L, 2_100L);
    }

    @Test
    void bytesOfAFrameStillArrivingAnswerAWakesPingToTheSeed() throws Exception {
        PeerNode s1 = node("s1", true, time);
        PeerNode s2 = node("s2", true, time, "s1");
        VirtualTime.NodeTime view = time.unwatchedNode();
        PeerNode host = node("h", true, view, "s1", "s2");
        assertThat(until(Duration.ofSeconds(30), Duration.ofMillis(100),
                () -> sees("h", s1) && sees("h", s2) && sees("s1", host) && sees("s2", host))).isNotNegative();
        view.suspend();
        time.advance(Duration.ofMinutes(3));
        network.oneWay("s1", "h"); // s1's whole frames do not arrive; its bytes do
        view.resume();
        time.advance(Duration.ofMillis(1_000));
        assertThat(network.progress("s1", "h")).isTrue();
        time.advance(Duration.ofMillis(1_100));
        GroupMembership membership = runtimes.get("h").membership();
        System.out.printf("[final] a wake's PING answered by bytes: wake drops %d, byte answers %d%n",
                membership.wakeDrops(), membership.wakeBytesAnswers());
        assertThat(host.seedLinkStats().wakePings()).isEqualTo(1);
        assertThat(membership.wakeBytesAnswers()).as("the seed is writing: its bytes answer the wake").isEqualTo(1);
        assertThat(membership.wakeDrops()).as("no fail-over from a seed that answers with bytes").isZero();
        assertThat(sees("h", s1)).isTrue();
    }

    // ---- decision 5: the dead-link rule on every link dialed to a seed

    @Test
    void aListeningNodePingsItsSeedLinkSilentFor45sAndClosesItAfter2s() throws Exception {
        PeerNode s1 = node("s1", true, time);
        PeerNode s2 = node("s2", true, time, "s1");
        PeerNode host = node("n", true, time, "s1", "s2");
        assertThat(until(Duration.ofSeconds(60), Duration.ofMillis(100),
                () -> sees("n", s1) && sees("n", s2) && sees("s1", host) && sees("s2", host))).isNotNegative();
        time.advance(Duration.ofMinutes(10));
        PeerNode.SeedLinkStats quiet = host.seedLinkStats();
        assertThat(quiet.linksSilent()).as("a seed link that answers is never closed").isZero();
        // The link n dialed to s1 goes dead one way. s2's indirect probes vouch for s1, so membership keeps it: only
        // the dead-link rule can find the half-open link.
        network.oneWay("s1", "n");
        long pinged = until(Duration.ofSeconds(60), Duration.ofMillis(100),
                () -> host.seedLinkStats().seedPings() > quiet.seedPings());
        long closed = until(Duration.ofSeconds(10), Duration.ofMillis(100),
                () -> host.seedLinkStats().linksSilent() > 0);
        System.out.printf("[final] listening node, seed link dead one way: PING after %d ms, closed %d ms later%n",
                pinged, closed);
        assertThat(pinged).as("a seed link silent for 45 s is asked, on a node that listens too")
                .isBetween(0L, 45_100L);
        assertThat(closed).as("and closed 2 s after an unanswered PING").isBetween(1_900L, 2_100L);
    }

    // ---- decision 6: the lost-member retry on a fixed cycling table

    @Test
    void aLostMemberIsRetriedOnTheCyclingTableWithItsSpread() throws Exception {
        PeerNode a = node("a", true, time);
        PeerNode b = node("b", true, time, "a");
        assertThat(until(Duration.ofSeconds(30), Duration.ofMillis(100), () -> sees("a", b) && sees("b", a)))
                .isNotNegative();
        wire.silent.add("b"); // b is gone for good, as far as a can tell
        assertThat(until(Duration.ofMinutes(2), Duration.ofMillis(100), () -> !sees("a", b))).isNotNegative();
        long lost = time.monoMillis();
        time.advance(Duration.ofMinutes(14));
        List<Long> intros = wire.instants("a", "b", "RUMOR", lost);
        List<Long> waits = new ArrayList<>();
        for (int i = 1; i < intros.size(); i++) {
            waits.add(intros.get(i) - intros.get(i - 1));
        }
        System.out.printf("[final] lost-member retries %d after %d ms; waits %s%n", intros.size(),
                intros.isEmpty() ? -1 : intros.get(0) - lost, waits);
        long[] slots = {10_000, 20_000, 30_000, 60_000, 120_000, 300_000, 5_000, 10_000, 20_000};
        assertThat(waits.size()).as("the table wraps: never stuck at its longest wait").isGreaterThanOrEqualTo(
                slots.length);
        assertSlot(intros.get(0) - lost, 5_000, "the first try one table step after the loss");
        for (int i = 0; i < slots.length; i++) {
            assertSlot(waits.get(i), slots[i], "wait " + (i + 1) + " (5, 10, 20, 30 s, 1, 2, 5 min, then 5 s again)");
        }
        assertThat(waits.stream().distinct().count()).as("the waits are spread, not exact").isGreaterThan(5);
    }

    /**
     * The cross-client check of 2026-11-23 (decision 6 with a wall step): the table's waits are local periods. An NTP
     * step of the wall clock back an hour, between two tries, must not hold the next try an hour (the schedule was
     * kept on the wall clock); it comes on the table as before.
     */
    @Test
    void aBackwardWallStepBetweenTwoTriesDoesNotHoldTheLostMemberTable() throws Exception {
        PeerNode a = node("a", true, time);
        PeerNode b = node("b", true, time, "a");
        assertThat(until(Duration.ofSeconds(30), Duration.ofMillis(100), () -> sees("a", b) && sees("b", a)))
                .isNotNegative();
        wire.silent.add("b");
        assertThat(until(Duration.ofMinutes(2), Duration.ofMillis(100), () -> !sees("a", b))).isNotNegative();
        long lost = time.monoMillis();
        time.advance(Duration.ofSeconds(6));
        assertThat(wire.instants("a", "b", "RUMOR", lost)).as("the first try, one 5 s step after the loss").hasSize(1);
        time.stepWall(Duration.ofHours(-1)); // NTP steps the wall back an hour
        time.advance(Duration.ofSeconds(60));
        List<Long> intros = wire.instants("a", "b", "RUMOR", lost);
        System.out.printf("[final] lost-member tries across a backward wall step: %s (lost at %d)%n", intros, lost);
        assertThat(intros.size()).as("the 10 s and 20 s steps still come, an hour of wall step or not")
                .isGreaterThanOrEqualTo(3);
        assertSlot(intros.get(1) - intros.get(0), 10_000, "the wait across the step");
        assertSlot(intros.get(2) - intros.get(1), 20_000, "the wait after it");
    }

    // ---- decision 7: seed dials off the loop

    /** A transport over the fabric whose dials to one address are refused at once, or blackholed for 5 s. */
    private final class Blackhole implements Transport {
        final Transport base;
        final String address;
        volatile boolean blackhole;
        /** A slow connect: the dial waits 5 s, then connects (a SYN-ACK that comes late). */
        volatile boolean slow;
        /** Dials to the address connect at once (neither refused, slow nor blackholed). */
        volatile boolean pass;
        final List<Long> dialStarts = new CopyOnWriteArrayList<>();
        final List<Thread> dialThreads = new CopyOnWriteArrayList<>();
        /** The links a slow connect returned, and whether each was closed. */
        final Map<TransportConnection, Boolean> slowLinks = new ConcurrentHashMap<>();

        Blackhole(Transport base, String address) {
            this.base = base;
            this.address = address;
        }

        @Override
        public String scheme() {
            return base.scheme();
        }

        @Override
        public Set<Capability> capabilities() {
            return base.capabilities();
        }

        @Override
        public TransportConnection dial(String to) throws IOException {
            if (!to.equals(address)) {
                return base.dial(to);
            }
            dialStarts.add(time.monoMillis());
            dialThreads.add(Thread.currentThread());
            if (slow) {
                try {
                    time.sleep(Duration.ofSeconds(5));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                TransportConnection link = base.dial(to);
                slowLinks.put(link, false);
                return new TransportConnection() {
                    @Override
                    public String remoteAddress() {
                        return link.remoteAddress();
                    }

                    @Override
                    public void send(byte[] frame) throws IOException {
                        link.send(frame);
                    }

                    @Override
                    public void onReceive(Consumer<byte[]> receiver) {
                        link.onReceive(receiver);
                    }

                    @Override
                    public void onClose(Runnable listener) {
                        link.onClose(listener);
                    }

                    @Override
                    public void close() {
                        slowLinks.put(link, true);
                        link.close();
                    }
                };
            }
            if (blackhole) {
                try {
                    time.sleep(Duration.ofSeconds(5)); // no SYN-ACK: the connect timeout
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new IOException("connect timed out: " + to);
            }
            if (pass) {
                return base.dial(to);
            }
            throw new IOException("refused: " + to);
        }

        @Override
        public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException {
            return base.listen(bindAddress, onAccept);
        }
    }

    @Test
    void aBlackholedSeedConnectHoldsNeitherTheLoopNorTheRoundsNextSeed() throws Exception {
        PeerNode s = node("s", true, time);
        Blackhole transport = new Blackhole(network.register("h"), "b");
        PeerNode host = node("h", false, time, transport, "b", "s");
        assertThat(until(Duration.ofSeconds(5), Duration.ofMillis(10), () -> sees("h", s))).isNotNegative();
        time.advance(Duration.ofSeconds(10)); // a link that holds: its end redials at once
        transport.blackhole = true;
        wire.silent.add("s");
        long lost = time.monoMillis();
        runtimes.get("h").membership().evict(s.peerId());
        time.advance(Duration.ofSeconds(8));
        List<Long> toS = wire.instants("h", "s", "RUMOR", lost);
        List<Long> toB = transport.dialStarts.stream().filter(t -> t >= lost).toList();
        System.out.printf("[final] blackholed b: dials to b at %s, to s at %s (from %d)%n", toB, toS, lost);
        assertThat(toB).as("the round dials b first").isNotEmpty();
        assertThat(toB.get(0)).isEqualTo(lost);
        assertThat(toS).as("the lost seed s goes last").isNotEmpty();
        assertThat(toS.get(0) - toB.get(0)).as("b's blackholed connect costs the 250 ms stagger, not its 5 s timeout")
                .isEqualTo(SeedRedial.STAGGER_MILLIS);
        Thread loopThread = transport.dialThreads.get(transport.dialThreads.size() - 1);
        assertThat(loopThread.isVirtual()).as("each dial on a virtual thread of its own").isTrue();
    }

    /**
     * The cross-client check of 2026-11-23 (decision 7 with a close): a seed dial off the loop whose connect completes
     * after the node closed leaves no link open. Before, the late link was attached to the closed node (after its
     * close had closed every link), introduced itself on it and stayed open with a reader nobody served.
     */
    @Test
    void aSeedDialThatConnectsAfterTheNodeClosedLeavesNoLinkOpen() throws Exception {
        PeerNode s = node("s", true, time);
        Blackhole transport = new Blackhole(network.register("h"), "s");
        transport.pass = true;
        PeerNode host = node("h", false, time, transport, "s");
        assertThat(until(Duration.ofSeconds(5), Duration.ofMillis(10), () -> sees("h", s))).isNotNegative();
        time.advance(Duration.ofSeconds(10));
        transport.slow = true;
        int before = transport.dialStarts.size();
        runtimes.get("h").membership().evict(s.peerId());
        assertThat(until(Duration.ofSeconds(30), Duration.ofMillis(10), () -> transport.dialStarts.size() > before))
                .as("the redial starts, off the loop").isNotNegative();
        host.close();
        time.advance(Duration.ofSeconds(6)); // the slow connect completes after the close
        assertThat(until(Duration.ofSeconds(5), Duration.ofMillis(10), () -> !transport.slowLinks.isEmpty()))
                .isNotNegative();
        System.out.printf("[final] slow connects after the close: %s%n", transport.slowLinks.values());
        assertThat(until(Duration.ofSeconds(5), Duration.ofMillis(10),
                () -> !transport.slowLinks.containsValue(false)))
                .as("every link a dial brought after the close was closed").isNotNegative();
    }

    // ---- the carried items of the close-out (2026-11-23): sends and the last resort

    /**
     * A send that needs a dial runs it off the loop (SPEC v0.1.37 §9.4, as the seed and lost-member dials): a member
     * the loop sends to with no live link and a blackholed endpoint (its connect sleeps the 5 s timeout) costs the
     * loop nothing; the frames wait behind one dial on a virtual thread of its own. Before, {@code connectionTo}
     * dialed on the sender's thread, so the loop sat in each 5 s connect. Once the endpoint answers, the dial brings
     * the link.
     */
    @Test
    void aSendToAMemberWithNoLinkDialsOffTheLoop() throws Exception {
        PeerNode c = node("c", true, time);
        Blackhole toB = new Blackhole(network.register("a"), "b");
        toB.blackhole = true; // a's connects to b get no SYN-ACK
        PeerNode a = node("a", true, time, toB, "c");
        PeerNode b = node("b", true, time, new Blackhole(network.register("b"), "a"), "c"); // b's dials to a refused
        assertThat(until(Duration.ofSeconds(60), Duration.ofMillis(50), () -> sees("a", b) && sees("b", a)
                && sees("a", c) && toB.dialStarts.size() >= 3)).as("a learns b through c and dials it").isNotNegative();
        long from = time.monoMillis();
        time.advance(Duration.ofSeconds(30));
        NodeLoop.Stats stats = a.loop().stats();
        PeerNode.SendDialStats sends = a.sendDialStats();
        List<Long> dials = toB.dialStarts.stream().filter(t -> t >= from).toList();
        System.out.printf("[close-out] blackholed b: %d dials in 30 s, loop lateness max %.1f ms (p99 %.1f ms),"
                        + " sends %s, a sees b %s%n", dials.size(), stats.latenessMaxNanos() / 1e6,
                stats.latenessP99Nanos() / 1e6, sends, sees("a", b));
        assertThat(dials).as("a keeps trying b").isNotEmpty();
        assertThat(stats.latenessMaxNanos()).as("no run of a's loop waited out a 5 s connect")
                .isLessThan(Duration.ofSeconds(1).toNanos());
        assertThat(sends.dials()).as("the dials ran off the loop, one per member at a time").isPositive();
        assertThat(toB.dialThreads.get(toB.dialThreads.size() - 1).isVirtual())
                .as("each on a virtual thread of its own").isTrue();
        assertThat(sees("a", b)).as("b stays in a's view (c vouches for it)").isTrue();

        toB.blackhole = false;
        toB.pass = true; // b's endpoint answers again
        assertThat(until(Duration.ofSeconds(30), Duration.ofMillis(50), () -> a.connectedTo(b.peerId())))
                .as("a send's dial brings the link").isNotNegative();
        assertThat(a.sendDialStats().dropped()).isZero();
    }

    /**
     * The last resort runs on its own fixed cycling table (house rule, in the lost-member table's style): the first
     * round at once, then 2, 4, 8 and 10 minutes, then 2 minutes again, each wait spread by a tenth. Before, it
     * doubled from 2 minutes to a 10 minute cap (4, 8, 10, 10, ... a quarter either way) and stayed there.
     */
    @Test
    void theLastResortRunsOnItsOwnCyclingTable() throws Exception {
        PeerNode a = node("a", true, time, network.register("a"),
                o -> o.retireAfter(Duration.ofMinutes(1)).postRetirement(true));
        PeerNode b = node("b", true, time, "a");
        assertThat(until(Duration.ofSeconds(30), Duration.ofMillis(100), () -> sees("a", b) && sees("b", a)))
                .isNotNegative();
        wire.silent.add("b"); // gone for good, as far as a can tell
        assertThat(until(Duration.ofMinutes(2), Duration.ofMillis(100), () -> !sees("a", b))).isNotNegative();
        long lost = time.monoMillis();
        time.advance(Duration.ofSeconds(90)); // retired a minute after the loss; its lost-member tries are over
        assertThat(a.retired(groupId)).as("b retired").containsExactly(b.peerId());
        long from = time.monoMillis();
        time.advance(Duration.ofMinutes(40));
        List<Long> rounds = wire.instants("a", "b", "RUMOR", from);
        List<Long> waits = new ArrayList<>();
        for (int i = 1; i < rounds.size(); i++) {
            waits.add(rounds.get(i) - rounds.get(i - 1));
        }
        System.out.printf("[close-out] last resort: first round %d ms after the loss, %d rounds, waits %s%n",
                rounds.isEmpty() ? -1 : rounds.get(0) - lost, rounds.size(), waits);
        assertThat(a.lastResortIntroductions()).as("the retirement list was tried").isPositive();
        assertThat(rounds.get(0) - lost).as("the first round once the view was empty 2 minutes")
                .isBetween(PeerNode.LAST_RESORT_AFTER_MILLIS, PeerNode.LAST_RESORT_AFTER_MILLIS + 1_000L);
        long[] slots = {120_000, 240_000, 480_000, 600_000, 120_000, 240_000};
        assertThat(waits.size()).as("the table wraps: never stuck at its longest wait").isGreaterThanOrEqualTo(
                slots.length);
        for (int i = 0; i < slots.length; i++) {
            assertSlot(waits.get(i), slots[i], "last-resort wait " + (i + 1) + " (2, 4, 8, 10 min, then 2 again)");
        }
    }
}
