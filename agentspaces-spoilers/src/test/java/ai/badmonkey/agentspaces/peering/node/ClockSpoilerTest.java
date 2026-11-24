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
import ai.badmonkey.agentspaces.common.time.TimeSource;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.LinkFilter;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The phase-3 clock spoilers of srad/agentspaces-event-loop.md §5.1, on virtual time: EL-SP-04 (one node's wall clock
 * steps back 5 minutes, later forward 4), EL-SP-05 (one node freezes for 5 s), EL-SP-13 (one node's wall clock steps
 * forward 2 hours and back after 5 minutes) and EL-SP-16 (one node's monotonic clock stops for 2 hours, a laptop
 * suspend). Ten members, every node on its own loop ({@link PeerNode#startTicking}), nobody calling {@code tick()}; the
 * target node runs on a node view of the fleet's {@link VirtualTime} ({@link VirtualTime.NodeTime}) whose wall clock
 * steps, or which is held, alone. While a node is held its frames are dropped both ways, as a stopped process's are.
 * Each scenario is PREVENTED or REPAIRED as the SRAD states and its figure goes to the measurements store.
 */
@Timeout(900)
class ClockSpoilerTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final int SIZE = 10;
    private static final String TARGET = "m" + (SIZE - 1);

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final Map<String, PeerNode> nodes = new LinkedHashMap<>();
    private final Map<String, GroupRuntime> runtimes = new LinkedHashMap<>();
    private final GroupId groupId = GroupId.of("zClockSpoiler");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zClockSpoiler",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "clock-spoiler", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private VirtualTime.NodeTime target;
    /** Departures each node saw, by the member that left. */
    private final Map<String, AtomicLong> departures = new LinkedHashMap<>();

    @AfterEach
    void tearDown() {
        nodes.values().forEach(PeerNode::close);
    }

    private void member(String name, String seed, TimeSource source) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).time(source).randomSeed(name.hashCode()).build();
        node.listen(network.register(name), name);
        nodes.put(name, node);
        GroupRuntime rt = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seed == null ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", seed, 0)));
        runtimes.put(name, rt);
        node.startTicking(TICK);
    }

    /** Ten members, the last on its own node view; formed, then idle for two minutes. */
    private void fleet() throws IOException {
        target = time.unwatchedNode();
        for (int i = 0; i < SIZE; i++) {
            String name = "m" + i;
            member(name, i == 0 ? null : "m0", name.equals(TARGET) ? target : time);
        }
        long waited = 0;
        while (!formed()) {
            time.advance(TICK);
            waited += TICK.toMillis();
            assertThat(waited).as("the group forms within 120 s").isLessThan(120_000);
        }
        time.advance(Duration.ofMinutes(2));
        assertThat(formed()).as("formed and quiet").isTrue();
    }

    private boolean formed() {
        return runtimes.values().stream().allMatch(rt -> rt.membership().allMembers().size() == SIZE - 1);
    }

    /** Whether the target is in every other view and holds every other member. */
    private boolean targetEverywhere() {
        PeerId id = nodes.get(TARGET).peerId();
        for (Map.Entry<String, GroupRuntime> e : runtimes.entrySet()) {
            if (e.getKey().equals(TARGET)) {
                if (e.getValue().membership().allMembers().size() != SIZE - 1) {
                    return false;
                }
            } else if (e.getValue().membership().member(id).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /** Members missing from any view, summed over the views (0 when every view is whole). */
    private int missing() {
        int n = 0;
        for (GroupRuntime rt : runtimes.values()) {
            n += SIZE - 1 - rt.membership().allMembers().size();
        }
        return n;
    }

    private long sum(java.util.function.ToLongFunction<PeerNode> f) {
        return nodes.values().stream().mapToLong(f).sum();
    }

    private long backstopFound() {
        return sum(n -> n.loopStats().backstopFoundWork());
    }

    /** Drops every frame to and from the target while it is held. */
    private List<AutoCloseable> cutTarget() {
        List<AutoCloseable> cuts = new ArrayList<>();
        cuts.add(network.filter(TARGET, null, frame -> LinkFilter.Verdict.drop()));
        cuts.add(network.filter(null, TARGET, frame -> LinkFilter.Verdict.drop()));
        return cuts;
    }

    private static void close(List<AutoCloseable> cuts) throws Exception {
        for (AutoCloseable c : cuts) {
            c.close();
        }
    }

    /** Advances in ticks until the target is in every view; returns the wait in ms, failing past the bound. */
    private long untilTargetEverywhere(long boundMillis) {
        long start = time.millis();
        while (!targetEverywhere()) {
            time.advance(TICK);
            assertThat(time.millis() - start).as("back in every view within " + boundMillis + " ms")
                    .isLessThanOrEqualTo(boundMillis);
        }
        return time.millis() - start;
    }

    private static Rec rec(Object... params) {
        return Rec.with().params(Params.of(params)).kind(Kind.PERF);
    }

    /**
     * EL-SP-04, PREVENTED: a back step of 5 minutes and, 2 minutes later, a forward step of 4 on one node, its
     * monotonic clock untouched. Both stay inside the 300 s frame-age window and the 10-minute HLC drift bound, so the
     * loop is tested alone. No member expires early on any node (before phase 3 the forward step expired every member
     * of the stepped node's view at once, its TTL judged on the wall clock), no catch-up runs, no frame is refused, and
     * each step asks every participant again in the same instant ({@code core.wallstep.rerun}).
     */
    @Test
    void wallStepsBackAndForwardMoveNoLocalPeriod() throws Exception {
        fleet();
        PeerNode stepped = nodes.get(TARGET);
        long stale0 = sum(PeerNode::framesStale) + sum(PeerNode::framesFarFuture);
        long found0 = backstopFound();
        long pings0 = runtimes.get(TARGET).membership().probeStats().directPings();
        long runs0 = stepped.loopStats().runs();

        target.stepWall(Duration.ofMinutes(-5));
        time.runDue();
        long rerunBack = stepped.wallStepStats().lastRerunNanos();
        assertThat(stepped.wallStepStats().steps()).isEqualTo(1);
        assertThat(rerunBack).as("every participant asked again in the instant of the step").isZero();
        assertThat(stepped.wallStepStats().resumeRejoins()).as("a back step is no absence").isZero();
        int missingAfterBack = 0;
        for (int s = 0; s < 120; s++) {
            time.advance(Duration.ofSeconds(1));
            missingAfterBack = Math.max(missingAfterBack, missing());
        }

        target.stepWall(Duration.ofMinutes(4));
        time.runDue();
        long rerunForward = stepped.wallStepStats().lastRerunNanos();
        assertThat(stepped.wallStepStats().steps()).isEqualTo(2);
        assertThat(stepped.wallStepStats().resumeRejoins())
                .as("a 4-minute forward step is longer than the 45 s member TTL: EL-C-14 re-introduces")
                .isEqualTo(1);
        int missingAfterForward = 0;
        for (int s = 0; s < 120; s++) {
            time.advance(Duration.ofSeconds(1));
            missingAfterForward = Math.max(missingAfterForward, missing());
        }
        long runs = stepped.loopStats().runs() - runs0;
        long pings = runtimes.get(TARGET).membership().probeStats().directPings() - pings0;

        Measure.record("core.wallstep.rerun", Unit.MS, Direction.LOWER, rerunForward / 1_000_000L,
                rec("step", "4min", "harness", "virtual"));
        System.out.printf("EL-SP-04: reruns %d ns back, %d ns forward; missing members after back %d, after"
                        + " forward %d; stepped node: %d runs, %d direct pings over 4 minutes%n",
                rerunBack, rerunForward, missingAfterBack, missingAfterForward, runs, pings);
        assertThat(missingAfterBack).as("no member leaves any view after the back step").isZero();
        assertThat(missingAfterForward).as("no member leaves any view after the forward step").isZero();
        assertThat(sum(PeerNode::framesStale) + sum(PeerNode::framesFarFuture) - stale0)
                .as("no frame is refused").isZero();
        assertThat(backstopFound() - found0).as("no backstop run found work").isZero();
        assertThat(rerunForward).isZero();
        // No catch-up: in a quiet group the stepped node's loop runs at the pace it ran before; a catch-up after the
        // forward step would show as a burst of rounds (four minutes of probe turns at once).
        assertThat(pings).as("the resume probe of EL-C-14 adds one probe per member, no more")
                .isLessThanOrEqualTo(SIZE - 1 + 4 * 60 / 4);
    }

    /**
     * EL-SP-05, REPAIRED: one node freezes for 5 s (its threads stop and its frames are lost; its monotonic clock counts
     * on, as in a stop-the-world pause). After the thaw each overdue participant runs once, no wall step is raised,
     * and the node is back in every view within the band ({@code core.resume.rejoin}, the freeze shape).
     */
    @Test
    void aFiveSecondFreezeRunsEachOverdueParticipantOnce() throws Exception {
        fleet();
        PeerNode frozen = nodes.get(TARGET);
        long found0 = backstopFound();
        List<AutoCloseable> cuts = cutTarget();
        target.freeze();
        time.advance(Duration.ofSeconds(5));
        long runsBefore = frozen.loopStats().runs();
        target.resume();
        close(cuts);
        time.runDue();
        long thawRuns = frozen.loopStats().runs() - runsBefore;
        long rejoin = untilTargetEverywhere(GroupMembership.Config.defaults().memberTtl().toMillis());
        time.advance(Duration.ofMinutes(1));
        Measure.record("core.resume.rejoin", Unit.MS, Direction.LOWER, rejoin,
                rec("members", SIZE, "absence", "freeze-5s", "harness", "virtual"));
        System.out.printf("EL-SP-05: %d runs at the thaw; back in every view %d ms after it; wall steps %d%n",
                thawRuns, rejoin, frozen.wallStepStats().steps());
        assertThat(thawRuns).as("the overdue participants run once, together, not a round each").isBetween(1L, 3L);
        assertThat(frozen.wallStepStats().steps()).as("wall and monotonic time moved together").isZero();
        assertThat(targetEverywhere()).isTrue();
        assertThat(missing()).isZero();
        assertThat(backstopFound() - found0).as("no backstop run found work").isZero();
    }

    /**
     * EL-SP-16, REPAIRED: one node's monotonic clock stops for 2 hours while its wall clock and its peers move on (a
     * laptop suspend). Its peers drop it; the wall step is raised on its first timer wake after the resume; it
     * re-introduces itself to its seed and members and probes each member once (EL-C-14), and is back in every view
     * within the band ({@code core.resume.rejoin}, goal 5 s, limit the member TTL plus the backstop).
     */
    @Test
    void aTwoHourSuspendIsRaisedAsAStepAndTheNodeRejoinsAtOnce() throws Exception {
        fleet();
        PeerNode laptop = nodes.get(TARGET);
        List<AutoCloseable> cuts = cutTarget();
        target.suspend();
        time.advance(Duration.ofHours(2));
        assertThat(runtimes.entrySet().stream().filter(e -> !e.getKey().equals(TARGET))
                .noneMatch(e -> e.getValue().membership().member(laptop.peerId()).isPresent()))
                .as("its peers dropped it while it slept").isTrue();
        assertThat(runtimes.get(TARGET).membership().allMembers()).as("its own view, frozen with it, is whole")
                .hasSize(SIZE - 1);
        long probes0 = runtimes.get(TARGET).membership().resumeProbes();
        long wakePings0 = laptop.seedLinkStats().wakePings();
        close(cuts);
        target.resume();
        time.runDue();
        assertThat(laptop.wallStepStats().steps()).as("the step is raised at the first wake after the resume")
                .isEqualTo(1);
        assertThat(laptop.wallStepStats().resumeRejoins()).isEqualTo(1);
        long rerun = laptop.wallStepStats().lastRerunNanos();
        long rejoin = untilTargetEverywhere(GroupMembership.Config.defaults().memberTtl().toMillis() + 1_000);
        // The seed link's member, when the node holds one, is probed by the wake's PING and the resume probes then
        // skip it (SPEC v0.1.37 §9.4, decision 3): one probe of each member, of either kind (§10.51).
        long wakePings = laptop.seedLinkStats().wakePings() - wakePings0;
        long probes = runtimes.get(TARGET).membership().resumeProbes() - probes0 + wakePings;
        time.advance(Duration.ofMinutes(1));
        Measure.record("core.resume.rejoin", Unit.MS, Direction.LOWER, rejoin,
                rec("members", SIZE, "absence", "suspend-2h", "harness", "virtual"));
        Measure.record("core.wallstep.rerun", Unit.MS, Direction.LOWER, rerun / 1_000_000L,
                rec("step", "suspend-2h", "harness", "virtual"));
        System.out.printf("EL-SP-16: back in every view %d ms after the resume; %d probes (%d by the wake's PING);"
                + " rerun %d ns%n", rejoin, probes, wakePings, rerun);
        assertThat(probes).as("each member probed once, the seed link's by the wake's PING").isEqualTo(SIZE - 1);
        assertThat(wakePings).as("at most the one seed link").isLessThanOrEqualTo(1);
        assertThat(rejoin).as("§5.4 goal: at most 2 ping timeouts plus 1 s").isLessThanOrEqualTo(5_000);
        assertThat(targetEverywhere()).isTrue();
        assertThat(missing()).isZero();
    }

    /**
     * Time adversary of phase 3 (§10.18), EL-SP-16 at a shorter shape: a suspend longer than the failed-probe path that
     * drops a silent member (about 8 s) but shorter than the member TTL (45 s). Its peers drop it while it sleeps; its
     * own view, frozen with it, is whole, so nothing in it lapses at the resume. It must still be back in every view
     * within the §5.4 goal, as after a 2-hour suspend.
     */
    @Test
    void aSuspendShorterThanTheMemberTtlButLongerThanTheProbePathRejoinsAtOnce() throws Exception {
        fleet();
        PeerNode laptop = nodes.get(TARGET);
        List<AutoCloseable> cuts = cutTarget();
        target.suspend();
        time.advance(Duration.ofSeconds(40));
        boolean dropped = runtimes.entrySet().stream().filter(e -> !e.getKey().equals(TARGET))
                .noneMatch(e -> e.getValue().membership().member(laptop.peerId()).isPresent());
        close(cuts);
        target.resume();
        time.runDue();
        long rejoin = untilTargetEverywhere(Duration.ofMinutes(3).toMillis());
        Measure.record("core.resume.rejoin", Unit.MS, Direction.LOWER, rejoin,
                rec("members", SIZE, "absence", "suspend-40s", "harness", "virtual"));
        System.out.printf("suspend-40s: peers dropped it %s; back in every view %d ms after the resume;"
                        + " %d resume rejoins%n", dropped, rejoin, laptop.wallStepStats().resumeRejoins());
        assertThat(dropped).as("its peers dropped it while it slept").isTrue();
        assertThat(rejoin).as("§5.4 goal: at most 2 ping timeouts plus 1 s").isLessThanOrEqualTo(5_000);
        assertThat(missing()).isZero();
    }

    /**
     * EL-SP-13, REPAIRED as today: one node's wall clock steps forward 2 hours and back after 5 minutes. While stepped,
     * its peers refuse its far-future frames and it refuses theirs as stale, so it leaves their views and they leave
     * its. Frame age and the HLC stay on wall time and their rules are unchanged (EL-C-05, EL-C-11), so the stepped
     * node's HLC keeps the lead it took until its wall clock passes it again; its peers take its frames once the lead
     * is inside their drift ceiling (10 minutes plus the skew margin), and it is back in every view then. The figure is
     * recorded; no catch-up runs at the correction.
     *
     * <p>The prompt rejoin is FUTURE row EL-F-08 (owner, 2026-11-22). The 2 hours of virtual time cost about 60 to 90 s
     * of CPU, so this runs in the soak profile ({@code -Psoak}, at phase gates), not in every build.
     */
    @Tag("soak")
    @Test
    @DisplayName("EL-SP-13 (FUTURE EL-F-08): a two-hour forward step, corrected, rejoins once the clock lead is inside the drift ceiling")
    void aTwoHourForwardStepCorrectedRejoinsOnceTheClockLeadIsInsideTheDriftCeiling() throws Exception {
        fleet();
        PeerNode stepped = nodes.get(TARGET);
        long stale0 = sum(PeerNode::framesStale);
        long future0 = sum(PeerNode::framesFarFuture);
        target.stepWall(Duration.ofHours(2));
        time.runDue();
        time.advance(Duration.ofMinutes(5));
        long refusedStale = sum(PeerNode::framesStale) - stale0;
        long refusedFuture = sum(PeerNode::framesFarFuture) - future0;
        boolean leftViews = runtimes.entrySet().stream().filter(e -> !e.getKey().equals(TARGET))
                .noneMatch(e -> e.getValue().membership().member(stepped.peerId()).isPresent());
        long runs0 = stepped.loopStats().runs();
        target.stepWall(Duration.ofHours(-2));
        time.runDue();
        long correctionRuns = stepped.loopStats().runs() - runs0;
        long correctedAt = time.millis();
        // The lead falls inside the drift ceiling at about 2 h less 10 minutes; a peer that struck the node for its
        // far-future frames holds it in quarantine (10 minutes) from its last strike.
        long bound = Duration.ofHours(2).plusMinutes(2).toMillis();
        long rejoin = untilTargetEverywhere(bound);
        Measure.record("core.resume.rejoin", Unit.MS, Direction.LOWER, rejoin,
                rec("members", SIZE, "absence", "step-2h-corrected", "harness", "virtual"));
        System.out.printf("EL-SP-13: while stepped %d stale and %d far-future refusals, left every view %s;"
                        + " %d runs at the correction; back in every view %d ms after it%n",
                refusedStale, refusedFuture, leftViews, correctionRuns, rejoin);
        assertThat(refusedStale).as("it refuses its peers' frames as stale while stepped").isPositive();
        assertThat(refusedFuture).as("its peers refuse its far-future frames").isPositive();
        assertThat(leftViews).as("it leaves their views while stepped").isTrue();
        assertThat(correctionRuns).as("the correction asks every participant once; no catch-up").isBetween(1L, 2L);
        // The HLC lead it took (2 h) falls as its wall clock runs; its peers take its frames once the lead is inside
        // their 10-minute drift ceiling and skew margin and their quarantine of it (10 minutes) has run out.
        long leadAtCorrection = Duration.ofHours(2).toMillis();
        assertThat(rejoin).as("not before the lead is inside the drift ceiling")
                .isGreaterThanOrEqualTo(leadAtCorrection - 600_000 - PeerNode.FAR_FUTURE_SKEW_MARGIN_MILLIS);
        assertThat(rejoin).as("no later than that plus the quarantine and a minute")
                .isLessThanOrEqualTo(leadAtCorrection - 600_000 + PeerNode.QUARANTINE.toMillis() + 60_000);
        assertThat(time.millis() - correctedAt).isEqualTo(rejoin);
    }

    /**
     * C01 of EL-A-03 (srad/agentspaces-event-loop.md §10.33): a wall step during a stretch. The stepped node's loop
     * runs at a x2 stretch (the pool at its max); its wall clock steps back 5 minutes and, 2 minutes later, forward 4.
     * PREVENTED: each step asks every participant again in its instant, the stretch in force is neither reset nor
     * doubled by a step (local periods are on the monotonic base, EL-C-05), the forward step past the member TTL runs
     * the resume probe once, no member leaves any view, no wire-visible deadline outran its refresh, and no backstop
     * run found work. Then the stretch is lifted and the loop is back at x1 at its next run.
     */
    @Test
    void c01AWallStepDuringAStretchKeepsTheStretchAndDropsNoMember() throws Exception {
        fleet();
        PeerNode stepped = nodes.get(TARGET);
        NodeLoop loop = stepped.loop();
        long found0 = backstopFound();
        loop.requestStretch(2);
        time.advance(TICK);
        assertThat(loop.stretch()).as("the stretch applies at the next run").isEqualTo(2);
        long unrefreshed0 = loop.stats().stretchUnrefreshed();

        target.stepWall(Duration.ofMinutes(-5));
        time.runDue();
        assertThat(stepped.wallStepStats().lastRerunNanos()).as("the back step asks everyone at once").isZero();
        int missing = 0;
        for (int s = 0; s < 120; s++) {
            loop.requestStretch(2); // the pool stays at its max for the whole scenario
            time.advance(Duration.ofSeconds(1));
            missing = Math.max(missing, missing());
        }
        assertThat(loop.stretch()).as("a back step does not reset the stretch").isEqualTo(2);

        target.stepWall(Duration.ofMinutes(4));
        time.runDue();
        assertThat(stepped.wallStepStats().lastRerunNanos()).as("the forward step asks everyone at once").isZero();
        assertThat(stepped.wallStepStats().resumeRejoins()).as("a 4-minute step is past the TTL: one resume probe")
                .isEqualTo(1);
        for (int s = 0; s < 120; s++) {
            loop.requestStretch(2);
            time.advance(Duration.ofSeconds(1));
            missing = Math.max(missing, missing());
        }
        assertThat(loop.stretch()).as("nor does a forward step").isEqualTo(2);
        long unrefreshed = loop.stats().stretchUnrefreshed() - unrefreshed0;
        System.out.printf("C01: x2 stretch through a 5 min back step and a 4 min forward step: missing %d, "
                + "stretch-unrefreshed %d, backstop found %d%n", missing, unrefreshed, backstopFound() - found0);
        assertThat(missing).as("no member left any view").isZero();
        assertThat(unrefreshed).as("no wire-visible deadline outran its refresh").isZero();
        assertThat(backstopFound() - found0).as("no backstop run found work").isZero();

        loop.requestStretch(1);
        time.advance(TICK);
        assertThat(loop.stretch()).as("lifted at the next run").isEqualTo(1);
        for (int s = 0; s < 60; s++) {
            time.advance(Duration.ofSeconds(1));
            assertThat(missing()).isZero();
        }
    }
}
