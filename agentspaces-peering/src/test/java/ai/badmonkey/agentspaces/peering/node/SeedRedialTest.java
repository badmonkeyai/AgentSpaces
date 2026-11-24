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
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The seed redial of the core, as every client has it (owner decisions of 2026-11-23; srad/agentspaces-event-loop.md
 * §10.36): the table 5, 10, 20, 30 s with its wrap and +/-10 % spread, reset only by a link that held 5 s or by a
 * wake; the round of seed dials in list order 250 ms apart, the first to answer winning, the losers closed, the lost
 * seed last; a wake that PINGs the seed and fails over 2 s later, or dials at once with nobody in view; the dead-link
 * rule of a dial-only node; one link per seed however many rounds; one dial per seed at the join.
 *
 * <p>Everything runs on virtual time over the in-memory fabric: no sockets, no sleeps. A "silent" seed accepts a link
 * and never sends a frame, as a frozen process or a filtering middlebox does.
 */
@Timeout(120)
class SeedRedialTest {

    private static final Duration TICK = Duration.ofMillis(250);

    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
    private final SimNetwork network = new SimNetwork();
    private final Wire wire = new Wire();
    private final GroupId groupId = GroupId.of("zSeedRedial");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zSeedRedial",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "seed-redial",
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

        /** The distinct instants (ms) a kind went on a link from {@code fromMillis} on. */
        List<Long> instants(String from, String to, String kind, long fromMillis) {
            return times.getOrDefault(from + ">" + to + ":" + kind, List.of()).stream()
                    .filter(t -> t >= fromMillis).distinct().toList();
        }

        long count(String from, String to, String kind) {
            return times.getOrDefault(from + ">" + to + ":" + kind, List.of()).size();
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
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(view).time(view)
                .randomSeed(name.hashCode()).build();
        Transport transport = network.register(name);
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

    /** A seed that accepts every link and never sends a frame. */
    private void silentSeed(String name) throws IOException {
        listeners.add(network.register(name).listen(name, connection -> { }));
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

    private static List<Long> gaps(List<Long> instants) {
        List<Long> out = new ArrayList<>();
        for (int i = 1; i < instants.size(); i++) {
            out.add(instants.get(i) - instants.get(i - 1));
        }
        return out;
    }

    private static void assertSlot(long wait, long slot, String what) {
        assertThat(wait).as(what + ": the " + slot + " ms slot, +/-10 %")
                .isBetween(Math.round(slot * 0.9) - 1, Math.round(slot * 1.1) + 1);
    }

    // ---- (a) and (b): the table, the stagger, the leak and the join's single dial

    @Test
    void silentSeedsAreDialledOnTheTableInListOrder250msApartWithOneLinkEachAndOneDialAtTheJoin() throws Exception {
        silentSeed("s1");
        silentSeed("s2");
        long start = time.monoMillis();
        PeerNode host = node("h", true, time, "s1", "s2");
        assertThat(wire.count("h", "s1", "RUMOR")).as("the join dials each seed once, not twice (f)").isEqualTo(1);
        time.runDue();
        assertThat(wire.count("h", "s1", "RUMOR")).as("the lost participant's first run adds no dial (f)")
                .isEqualTo(1);
        time.advance(Duration.ofMinutes(4));

        List<Long> s1 = wire.instants("h", "s1", "RUMOR", start);
        List<Long> s2 = wire.instants("h", "s2", "RUMOR", start);
        System.out.printf("[seed-redial] rounds at %s; second seed %s%n", s1, s2);
        assertThat(s1.get(0)).as("the join dials at once").isEqualTo(start);
        assertThat(s1).hasSameSizeAs(s2);
        for (int i = 0; i < s1.size(); i++) {
            assertThat(s2.get(i) - s1.get(i)).as("a silent first seed costs the 250 ms stagger, in list order")
                    .isEqualTo(SeedRedial.STAGGER_MILLIS);
        }
        // Each round fails once its last dial has waited the stagger; the table's wait runs from there.
        long[] slots = {5_000, 10_000, 20_000, 30_000, 5_000, 10_000, 20_000};
        List<Long> waits = gaps(s1).stream().map(g -> g - 2 * SeedRedial.STAGGER_MILLIS).toList();
        System.out.printf("[seed-redial] table waits %s%n", waits);
        assertThat(waits.size()).as("never gives up (d)").isGreaterThanOrEqualTo(slots.length);
        for (int i = 0; i < slots.length; i++) {
            assertSlot(waits.get(i), slots[i], "wait " + i + " (5, 10, 20, 30 s, then wrapping)");
        }
        assertThat(waits.stream().distinct().count()).as("the waits are spread, not exact").isGreaterThan(4);

        assertThat(host.dialedLinkCount()).as("one link per seed after " + s1.size() + " rounds (f)")
                .isLessThanOrEqualTo(2);
        PeerNode.SeedLinkStats stats = host.seedLinkStats();
        assertThat(stats.seedDials()).isEqualTo(s1.size() + s2.size());
        assertThat(stats.lateSeedChannels()).isZero();
    }

    @Test
    void aRefusedSeedStartsTheNextAtOnceAndUnansweredIntroductionsCloseAfter47s() throws Exception {
        silentSeed("s2"); // s1 has no listener: every dial is refused at once
        long start = time.monoMillis();
        PeerNode host = node("h", true, time, "s1", "s2");
        time.advance(Duration.ofSeconds(2));
        assertThat(wire.instants("h", "s2", "RUMOR", start)).as("the refusal starts the next dial at once")
                .containsExactly(start);
        assertThat(host.dialedLinkCount()).isEqualTo(1);
        // One silent seed's link is replaced at each round, and the last is closed 47 s after its dial at most.
        time.advance(Duration.ofSeconds(60));
        assertThat(host.dialedLinkCount()).isLessThanOrEqualTo(1);
    }

    @Test
    void theFirstSeedToAnswerWinsTheLoserIsClosedAndTheNextIsNeverDialled() throws Exception {
        silentSeed("s1");
        PeerNode s2 = node("s2", true, time);
        silentSeed("s3");
        long start = time.monoMillis();
        PeerNode host = node("h", true, time, "s1", "s2", "s3");
        long joined = until(Duration.ofSeconds(5), Duration.ofMillis(10), () -> sees("h", s2));
        System.out.printf("[seed-redial] silent first seed: joined through the second in %d ms%n", joined);
        assertThat(joined).as("a silent first seed costs 250 ms").isBetween(0L, SeedRedial.STAGGER_MILLIS + 50);
        assertThat(wire.instants("h", "s2", "RUMOR", start).get(0) - start).isEqualTo(SeedRedial.STAGGER_MILLIS);
        time.advance(Duration.ofSeconds(30));
        assertThat(wire.count("h", "s3", "RUMOR")).as("the seeds not yet started are never dialled").isZero();
        PeerNode.SeedLinkStats stats = host.seedLinkStats();
        assertThat(stats.lateSeedChannels()).as("s1's link, still waiting, closed when s2 answered").isEqualTo(1);
        assertThat(stats.seedDials()).isEqualTo(2);
        assertThat(wire.count("h", "s1", "RUMOR")).as("linked: no more rounds").isEqualTo(1);
    }

    @Test
    void theLostSeedIsDialledLastInEveryRound() throws Exception {
        silentSeed("a");
        PeerNode b = node("b", true, time);
        silentSeed("c");
        PeerNode host = node("h", false, time, "a", "b", "c");
        assertThat(until(Duration.ofSeconds(5), Duration.ofMillis(10), () -> sees("h", b))).isNotNegative();
        time.advance(Duration.ofSeconds(10)); // a link that holds: its end redials at once
        wire.silent.add("b");
        long lost = time.monoMillis();
        runtimes.get("h").membership().evict(b.peerId());
        time.advance(Duration.ofSeconds(20));
        List<Long> a = wire.instants("h", "a", "RUMOR", lost);
        List<Long> c = wire.instants("h", "c", "RUMOR", lost);
        List<Long> bb = wire.instants("h", "b", "RUMOR", lost);
        System.out.printf("[seed-redial] after losing b: a %s, c %s, b %s%n", a, c, bb);
        assertThat(a.get(0)).as("a link that held 10 s ended: redial at once").isEqualTo(lost);
        for (int i = 0; i < 2; i++) {
            assertThat(c.get(i) - a.get(i)).as("round " + i + ": a, then c").isEqualTo(SeedRedial.STAGGER_MILLIS);
            assertThat(bb.get(i) - a.get(i)).as("round " + i + ": the lost seed b last")
                    .isEqualTo(2 * SeedRedial.STAGGER_MILLIS);
        }
        assertThat(host.seedLinkStats().seedDials()).isGreaterThanOrEqualTo(2 + 6);
    }

    @Test
    void aFourSecondLinkWaitsTheNextStepAndASixSecondLinkResets() throws Exception {
        PeerNode s = node("s", true, time);
        PeerNode host = node("h", false, time, "s");
        assertThat(until(Duration.ofSeconds(5), Duration.ofMillis(10), () -> sees("h", s))).isNotNegative();
        long linked = time.monoMillis();
        time.advance(Duration.ofMillis(4_000 - (time.monoMillis() - linked)));
        long ended = time.monoMillis();
        runtimes.get("h").membership().evict(s.peerId()); // a 4 s link ends
        time.advance(Duration.ofSeconds(8));
        List<Long> after = wire.instants("h", "s", "RUMOR", ended);
        System.out.printf("[seed-redial] a 4 s link ended at %d: redials %s%n", ended, after);
        assertThat(after).isNotEmpty();
        assertSlot(after.get(0) - ended, 5_000, "a 4 s link waits the next step");
        assertThat(host.seedTablePosition(groupId)).as("and the position moved on").isEqualTo(1);

        long back = until(Duration.ofSeconds(30), Duration.ofMillis(10), () -> sees("h", s));
        assertThat(back).as("s answers the redial").isNotNegative();
        time.advance(Duration.ofSeconds(6));
        long ended6 = time.monoMillis();
        runtimes.get("h").membership().evict(s.peerId()); // a 6 s link ends
        time.runDue();
        List<Long> at6 = wire.instants("h", "s", "RUMOR", ended6);
        System.out.printf("[seed-redial] a 6 s link ended at %d: redials %s%n", ended6, at6);
        assertThat(at6).as("a 6 s link resets: the redial goes at once").containsExactly(ended6);
        assertThat(host.seedTablePosition(groupId)).as("the table is back at its first wait").isZero();
    }

    @Test
    void aLoneFounderWithNothingToDialSchedulesNoRounds() throws Exception {
        PeerNode founder = node("f", true, time);
        time.advance(Duration.ofSeconds(1));
        long before = lostRuns(founder);
        time.advance(Duration.ofMinutes(10));
        long runs = lostRuns(founder) - before;
        System.out.printf("[seed-redial] a lone founder, 10 idle minutes: %d lost runs%n", runs);
        assertThat(runs).as("no seed and no locator: the table schedules no empty rounds").isLessThanOrEqualTo(2);
        assertThat(founder.seedLinkStats().seedDials()).isZero();
    }

    private static long lostRuns(PeerNode node) {
        return node.loopStats().dueRuns().entrySet().stream().filter(e -> e.getKey().endsWith("/lost"))
                .mapToLong(Map.Entry::getValue).sum();
    }

    // ---- (c): the wake

    @Test
    void aWakeWhileLinkedPingsTheSeedAndFailsOverAt2sHoweverYoungTheLink() throws Exception {
        PeerNode s1 = node("s1", true, time);
        PeerNode s2 = node("s2", true, time, "s1");
        VirtualTime.NodeTime view = time.unwatchedNode();
        PeerNode host = node("h", true, view, "s1", "s2");
        assertThat(until(Duration.ofSeconds(30), Duration.ofMillis(100),
                () -> sees("h", s1) && sees("h", s2) && sees("s1", host) && sees("s2", host))).isNotNegative();
        view.suspend(); // the link to s1 is young in monotonic time: the sleep does not count
        wire.silent.add("s1");
        time.advance(Duration.ofMinutes(3));
        view.resume();
        long woke = time.monoMillis();
        long left = until(Duration.ofSeconds(10), Duration.ofMillis(10), () -> !sees("h", s1));
        List<Long> pings = wire.instants("h", "s1", "PING", woke);
        System.out.printf("[seed-redial] wake while linked: PING %s ms after the wake, s1 left after %d ms%n",
                pings.isEmpty() ? "none" : pings.get(0) - woke, left);
        assertThat(pings).isNotEmpty();
        assertThat(pings.get(0) - woke).as("the wake PINGs the seed at once").isLessThanOrEqualTo(10);
        assertThat(left).as("no answer in the 2 s ping timeout: failed over at once, no indirect round")
                .isBetween(2_000L, 2_100L);
        assertThat(sees("h", s2)).as("the other seed is kept").isTrue();
        PeerNode.SeedLinkStats stats = host.seedLinkStats();
        assertThat(stats.wakePings()).isEqualTo(1);
        assertThat(stats.linksSilent()).isEqualTo(1);
        assertThat(stats.resumes()).isEqualTo(1);
        time.advance(Duration.ofSeconds(1));
        assertThat(host.seedLinkPeer(groupId)).as("the fail-over made s2 the seed link").isEqualTo(s2.peerId());
    }

    @Test
    void aWakeWithNoLinkRedialsAtOnceAndResetsTheTable() throws Exception {
        silentSeed("s1");
        VirtualTime.NodeTime view = time.unwatchedNode();
        PeerNode host = node("h", true, view, "s1");
        time.advance(Duration.ofSeconds(20)); // rounds at 0, 5 and 15 s: waiting the 20 s slot, the 30 s one next
        assertThat(host.seedTablePosition(groupId)).as("three waits taken").isEqualTo(3);
        view.suspend();
        time.advance(Duration.ofMinutes(3));
        view.resume();
        long woke = time.monoMillis();
        time.advance(Duration.ofSeconds(8));
        List<Long> after = wire.instants("h", "s1", "RUMOR", woke);
        System.out.printf("[seed-redial] wake with no link at %d: rounds %s%n", woke, after);
        assertThat(after).as("the wake dials at once, and again after the table's first wait").hasSizeGreaterThan(1);
        assertThat(after.get(0)).as("the wake dials at once").isEqualTo(woke);
        assertSlot(after.get(1) - after.get(0) - SeedRedial.STAGGER_MILLIS, 5_000,
                "the table restarts at its first wait");
        assertThat(host.seedLinkStats().resumes()).isEqualTo(1);
    }

    // ---- (e): the dead-link rule of a dial-only node

    @Test
    void aDialOnlyNodePingsALinkSilentFor45sAndClosesItAfter2sButAnswersKeepIt() throws Exception {
        PeerNode s1 = node("s1", true, time);
        PeerNode s2 = node("s2", true, time, "s1");
        PeerNode host = node("d", false, time, "s1", "s2");
        assertThat(until(Duration.ofSeconds(60), Duration.ofMillis(100),
                () -> sees("d", s1) && sees("d", s2) && sees("s1", host) && sees("s2", host))).isNotNegative();
        time.advance(Duration.ofMinutes(10)); // quiet: whatever arrives answers, nothing is closed
        PeerNode.SeedLinkStats quiet = host.seedLinkStats();
        System.out.printf("[seed-redial] dial-only, 10 quiet minutes: %s%n", quiet);
        assertThat(quiet.linksSilent()).as("a link that answers is never closed").isZero();

        network.oneWay("s1", "d"); // the link to s1 goes dead one way; s1 stays alive through s2's indirect probes
        long cut = time.monoMillis();
        long pinged = until(Duration.ofSeconds(60), Duration.ofMillis(100),
                () -> host.seedLinkStats().seedPings() > quiet.seedPings());
        long closed = until(Duration.ofSeconds(10), Duration.ofMillis(100),
                () -> host.seedLinkStats().linksSilent() > 0);
        System.out.printf("[seed-redial] dead one way: PING after %d ms, closed %d ms later%n", pinged, closed);
        assertThat(pinged).as("a link silent for 45 s is asked").isBetween(0L, 45_100L);
        assertThat(closed).as("and closed 2 s after an unanswered PING").isBetween(1_900L, 2_100L);
        assertThat(cut).isPositive();
    }
}
