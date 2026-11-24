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

import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.spoilers.Action;
import ai.badmonkey.agentspaces.spoilers.Script;
import ai.badmonkey.agentspaces.spoilers.ScriptRunner;
import ai.badmonkey.agentspaces.spoilers.Trigger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EL-SP-31 on virtual time, and the targeted relay's spoilers (srad/agentspaces-event-loop.md §3.12, EL-P-13 to
 * EL-P-15, EL-E-05): a dial-only peer ({@link ThinSimPeer}, which answers PINGs and never reissues its card) linked to
 * one member of a quiet group of ten, twelve and sixteen members, idle 30 minutes after a 2-minute settle; then its
 * holder falls silent toward it (REPAIRED: every view drops it), and a peer that names a victim as its holder (the
 * victim's extra frames are counted).
 */
@Timeout(1_800)
class TargetedRelaySpoilerTest {

    private static final Duration STEP = Duration.ofMillis(250);
    private static final Duration PERIOD = Duration.ofSeconds(1);

    private static void step(RobustnessFleet fleet, ScriptRunner runner) {
        for (PeerNode node : List.copyOf(fleet.nodes.values())) {
            node.tick();
        }
        if (runner != null) {
            runner.tick();
        }
        fleet.clock.advance(STEP);
    }

    private static boolean everyViewHolds(RobustnessFleet fleet, PeerId peer) {
        return fleet.runtimes.values().stream().allMatch(r -> r.membership().member(peer).isPresent());
    }

    private static boolean noViewHolds(RobustnessFleet fleet, PeerId peer) {
        return fleet.runtimes.values().stream().noneMatch(r -> r.membership().member(peer).isPresent());
    }

    /** A quiet group of {@code java} members, the thin peer dialling m0 and naming {@code holders}, settled 2 minutes. */
    private static RobustnessFleet thinGroup(String name, int java, ThinSimPeer[] thin, String holder) throws Exception {
        return thinGroup(name, java, thin, holder, true);
    }

    private static RobustnessFleet thinGroup(String name, int java, ThinSimPeer[] thin, String holder, boolean settle)
            throws Exception {
        RobustnessFleet fleet = new RobustnessFleet(name);
        for (int i = 0; i < java; i++) {
            fleet.add("m" + i, i == 0 ? null : "m0", PeerNode.ChannelAuth.SIGNED, GroupMembership.Config.defaults(),
                    true);
        }
        int steps = 0;
        while (fleet.runtimes.values().stream().anyMatch(r -> r.membership().size() != java - 1)) {
            step(fleet, null);
            assertThat(++steps).as("the Java members form within 120 s").isLessThan(480);
        }
        thin[0] = new ThinSimPeer(fleet.network, "thin", fleet.groupId, fleet.clock);
        thin[0].dial("m0", fleet.id("m0"), List.of(fleet.id(holder)));
        while (!everyViewHolds(fleet, thin[0].peerId())) {
            step(fleet, null);
            assertThat(++steps).as("every member admits the thin peer").isLessThan(960);
        }
        for (int i = 0; settle && i < 2 * 60 * 4; i++) { // the 2-minute settle
            step(fleet, null);
        }
        return fleet;
    }

    /**
     * EL-SP-31 (virtual): PREVENTED at each size; the probes of the thin peer go to its seed as PING_REQs. Ten members
     * run in every build; twelve and sixteen (about 150 s of CPU between them) run in the soak profile
     * ({@code -Psoak}, at phase gates), below.
     */
    @ParameterizedTest
    @ValueSource(ints = {10})
    void aDialOnlyPeerStaysInEveryViewOfAQuietGroupFor30Minutes(int members) throws Exception {
        dialOnlyPeerFor30Minutes(members);
    }

    /** EL-SP-31 (virtual) at the larger sizes, in the soak profile. */
    @Tag("soak")
    @ParameterizedTest
    @ValueSource(ints = {12, 16})
    void aDialOnlyPeerStaysInEveryViewOfALargerQuietGroupFor30Minutes(int members) throws Exception {
        dialOnlyPeerFor30Minutes(members);
    }

    private void dialOnlyPeerFor30Minutes(int members) throws Exception {
        ThinSimPeer[] thin = new ThinSimPeer[1];
        RobustnessFleet fleet = thinGroup("ElSp31v" + members, members - 1, thin, "m0");
        try {
            PeerId seed = fleet.id("m0");
            Map<String, Long> reqsBefore = new java.util.HashMap<>();
            fleet.runtimes.forEach((n, r) -> reqsBefore.put(n,
                    r.membership().probeStats().holderPingReqsByRelay().getOrDefault(seed, 0L)));
            int departures = 0;
            boolean held = true;
            for (int i = 0; i < 30 * 60 * 4; i++) {
                step(fleet, null);
                boolean now = everyViewHolds(fleet, thin[0].peerId());
                departures += held && !now ? 1 : 0;
                held = now;
            }
            long ttl = fleet.runtimes.get("m1").membership().memberTtlMillis();
            long cycle = fleet.runtimes.get("m1").membership().quietProbeCycleMillis();
            double perMinute = 0;
            for (Map.Entry<String, GroupRuntime> e : fleet.runtimes.entrySet()) {
                if (e.getKey().equals("m0")) {
                    continue;
                }
                GroupMembership.ProbeStats stats = e.getValue().membership().probeStats();
                assertThat(stats.holderPingReqsByRelay()).as("%s asks the seed alone", e.getKey())
                        .containsOnlyKeys(seed);
                assertThat(stats.randomPingReqs()).as("%s: no PING to the thin peer went nowhere", e.getKey()).isZero();
                assertThat(stats.dropsAfterHolderRound()).isZero();
                perMinute += (stats.holderPingReqsByRelay().get(seed) - reqsBefore.get(e.getKey())) / 30.0;
            }
            perMinute /= members - 2;
            Measure.record("core.thin-peer.departures", Unit.COUNT, Direction.LOWER, departures,
                    Rec.with().params(Params.of("members", members, "harness", "virtual")).kind(Kind.COUNT));
            Measure.record("core.membership.member-ttl", Unit.MS, Direction.LOWER, ttl,
                    Rec.with().params(Params.of("members", members, "harness", "virtual")).kind(Kind.PERF));
            Measure.record("core.thin-peer.holder-pingreqs", Unit.COUNT, Direction.LOWER, perMinute,
                    Rec.with().params(Params.of("members", members, "harness", "virtual")).kind(Kind.COUNT));
            System.out.printf("EL-SP-31 (virtual): %d members, thin-peer departures %d, member TTL %d ms, quiet cycle"
                    + " %d ms, holder PING_REQs %.2f per minute per non-seed member, PINGs it answered %s%n",
                    members, departures, ttl, cycle, perMinute, thin[0].pingsFrom().values());
            assertThat(departures).as("PREVENTED: the thin peer never left a view").isZero();
            assertThat(thin[0].pingsFrom().keySet()).as("every PING came over its one link").containsExactly(seed);
            assertThat(perMinute).as("about one holder PING_REQ per quiet cycle").isBetween(
                    0.8 * 60_000.0 / cycle, 1.5 * 60_000.0 / cycle);
        } finally {
            thin[0].close();
            fleet.close();
        }
    }

    /** The holder falls silent toward the thin peer (a script drops m0's frames to it): REPAIRED everywhere. */
    @Test
    void whenItsHolderFallsSilentEveryViewDropsTheThinPeerAndNoCopyBringsItBack() throws Exception {
        ThinSimPeer[] thin = new ThinSimPeer[1];
        RobustnessFleet fleet = thinGroup("ElSp31silent", 9, thin, "m0");
        try {
            Script script = Script.of("thin").seed(3101)
                    .rule("holder-silent").when(Trigger.frame().from("m0").to("thin")).act(Action.drop()).always()
                    .build();
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, PERIOD, PeerNode.ChannelAuth.SIGNED,
                    script);
            long start = fleet.clock.millis();
            long bound = fleet.runtimes.values().stream().mapToLong(r -> r.membership().memberTtlMillis()
                    + r.membership().quietProbeCycleMillis()).max().orElseThrow();
            while (!noViewHolds(fleet, thin[0].peerId())) {
                step(fleet, runner);
                assertThat(fleet.clock.millis() - start).as("dropped within the member TTL and one cycle")
                        .isLessThanOrEqualTo(bound);
            }
            long detected = fleet.clock.millis() - start;
            int returns = 0;
            for (int i = 0; i < 5 * 60 * 4; i++) {
                step(fleet, runner);
                returns += noViewHolds(fleet, thin[0].peerId()) ? 0 : 1;
            }
            runner.finish();
            Measure.record("core.thin-peer.holder-silent-drop", Unit.MS, Direction.LOWER, detected,
                    Rec.with().params(Params.of("members", 10, "harness", "virtual")).kind(Kind.PERF));
            System.out.printf("EL-SP-31 holder silent: the thin peer left every view %d ms after its holder fell"
                    + " silent (bound %d ms)%n", detected, bound);
            assertThat(returns).as("not re-admitted from a copy of its card").isZero();
            assertThat(fleet.runtimes.values()).as("the Java members keep one another")
                    .allSatisfy(r -> assertThat(r.membership().size()).isEqualTo(8));
        } finally {
            thin[0].close();
            fleet.close();
        }
    }

    /**
     * Seam tr02d (srad/agentspaces-event-loop.md §10.33, §10.37; TR-2): a routine holder round that fails escalates
     * to an indirect round that asks the target's declared holders first, then random members. Each non-seed member's
     * first PING_REQ to the thin peer's holder is lost once, so each holder round fails; the escalated round asks the
     * holder again and keeps the peer. Random members hold no link to the thin peer, so without the declared holders
     * an escalated round reached it only when its random draw happened to include the seed, and the others dropped it.
     */
    @Test
    void anEscalatedRoundAsksTheDeclaredHolderFirstSoOneLostRelayDropsNobody() throws Exception {
        ThinSimPeer[] thin = new ThinSimPeer[1];
        RobustnessFleet fleet = thinGroup("Tr02d", 9, thin, "m0");
        try {
            Script.RuleBuilder rules = null;
            Script.Builder script = Script.of("thin").seed(3102);
            for (int i = 1; i < 9; i++) {
                Script.RuleBuilder r = (rules == null ? script.rule("lose-m" + i) : rules.rule("lose-m" + i))
                        .when(Trigger.frame(Envelope.Kind.PING_REQ).from("m" + i).to("m0")).act(Action.drop())
                        .times(1);
                rules = r;
            }
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, PERIOD, PeerNode.ChannelAuth.SIGNED,
                    rules.build());
            Map<String, Long> escalations0 = new java.util.HashMap<>();
            fleet.runtimes.forEach((n, r) -> escalations0.put(n, r.membership().probeStats().escalations()));
            int departures = 0;
            boolean held = true;
            long bound = 10 * 60 * 4;
            int steps = 0;
            while (true) {
                step(fleet, runner);
                boolean now = everyViewHolds(fleet, thin[0].peerId());
                departures += held && !now ? 1 : 0;
                held = now;
                boolean allLost = true;
                for (int i = 1; i < 9; i++) {
                    allLost &= runner.report().fired("lose-m" + i) > 0;
                }
                if (allLost) {
                    break;
                }
                assertThat(++steps).as("every member's holder round lost one PING_REQ within 10 minutes")
                        .isLessThan((int) bound);
            }
            for (int i = 0; i < 30 * 4; i++) { // the last escalation's ping timeout and a margin
                step(fleet, runner);
                boolean now = everyViewHolds(fleet, thin[0].peerId());
                departures += held && !now ? 1 : 0;
                held = now;
            }
            runner.finish();
            long escalated = 0;
            for (Map.Entry<String, GroupRuntime> e : fleet.runtimes.entrySet()) {
                GroupMembership.ProbeStats stats = e.getValue().membership().probeStats();
                escalated += stats.escalations() - escalations0.get(e.getKey());
                assertThat(stats.dropsAfterHolderRound()).as("%s dropped nobody after a holder round", e.getKey())
                        .isZero();
            }
            System.out.printf("tr02d: %d escalated rounds after one lost PING_REQ per member, %d departures%n",
                    escalated, departures);
            assertThat(escalated).as("each lost relay escalated").isGreaterThanOrEqualTo(8);
            assertThat(departures).as("PREVENTED: the escalated round asked the holder, so no view dropped the peer")
                    .isZero();
        } finally {
            thin[0].close();
            fleet.close();
        }
    }

    /**
     * C02 (srad/agentspaces-event-loop.md EL-A-03, §10.37): the watchdog re-initialises the holder's loop while
     * another member's holder round toward its dial-only peer is in flight. The holder's next dispatch is lost at the
     * instant m3 sends its PING_REQ, so its loop stops until the watchdog finds the missed wake. Here the fleet runs on
     * its loops alone (no hand-driven tick that would mask the stop). PREVENTED: the relay is answered (a PING_REQ is
     * served as it arrives, not by the loop), no view drops the thin peer, one re-initialisation, no escalation.
     */
    @Test
    void c02AHoldersLoopReinitialisedDuringATargetedRelayDropsNobody() throws Exception {
        ThinSimPeer[] thin = new ThinSimPeer[1];
        RobustnessFleet fleet = thinGroup("C02", 9, thin, "m0");
        try {
            ai.badmonkey.agentspaces.common.time.Watchdog wd = fleet.clock.watchdog().markScripted();
            long detections0 = wd.stats().detections();
            GroupMembership m3 = fleet.runtimes.get("m3").membership();
            PeerNode holder = fleet.nodes.get("m0");
            long reqs0 = m3.probeStats().holderPingReqs();
            int steps = 0;
            while (m3.probeStats().holderPingReqs() == reqs0) {
                fleet.clock.advance(Duration.ofMillis(10));
                assertThat(++steps).as("m3 sends a holder round within 5 minutes").isLessThan(30_000);
            }
            long generation0 = holder.loop().generation();
            holder.loop().dropNextDispatch(); // the holder's loop stops here, mid-relay
            holder.loop().wakeAll();
            int departures = 0;
            boolean held = true;
            for (int i = 0; i < 2 * 60 * 4; i++) { // two minutes on the loops alone
                fleet.clock.advance(STEP);
                boolean now = everyViewHolds(fleet, thin[0].peerId());
                departures += held && !now ? 1 : 0;
                held = now;
            }
            GroupMembership.ProbeStats stats = m3.probeStats();
            System.out.printf("C02: holder generation %d -> %d, detections %d, m3 holder PING_REQs %d -> %d,"
                    + " escalations %d, departures %d%n", generation0, holder.loop().generation(),
                    wd.stats().detections() - detections0, reqs0, stats.holderPingReqs(), stats.escalations(),
                    departures);
            assertThat(holder.loop().generation()).as("the holder's loop was re-initialised once")
                    .isEqualTo(generation0 + 1);
            assertThat(wd.detections()).extracting(ai.badmonkey.agentspaces.common.time.Watchdog.Detection::kind)
                    .contains("missed-wake");
            assertThat(departures).as("PREVENTED: no view dropped the thin peer").isZero();
            assertThat(fleet.runtimes.values()).as("and no member dropped another")
                    .allSatisfy(r -> assertThat(r.membership().size()).isEqualTo(9));
            assertThat(stats.dropsAfterHolderRound()).isZero();
        } finally {
            thin[0].close();
            fleet.close();
        }
    }

    /**
     * A thin peer that names a member it never dialled as its holder (a victim, here m5): the others' holder rounds go
     * to the victim, whose relayed PINGs go nowhere; each failed round escalates to the victim and random members, and
     * only a round that happens to draw the seed keeps the peer, so the misnamed peer leaves the views within a few
     * cycles. Recorded from its admission: the victim's PING_REQs per cycle while the peer was still in a view, and how
     * long the peer lasted. Bounded: the victim's load ends with the peer.
     */
    @Test
    void aThinPeerNamingAVictimCostsTheVictimBoundedPingReqsUntilItLeaves() throws Exception {
        ThinSimPeer[] thin = new ThinSimPeer[1];
        RobustnessFleet fleet = thinGroup("ElSp31victim", 9, thin, "m5", false);
        try {
            PeerNode victim = fleet.nodes.get("m5");
            long before = victim.traffic().receivedByKind().getOrDefault(Envelope.Kind.PING_REQ, 0L);
            long start = fleet.clock.millis();
            long lastHeldAt = start;
            long reqsWhileHeld = 0;
            int steps = 10 * 60 * 4;
            for (int i = 0; i < steps; i++) {
                step(fleet, null);
                boolean heldByOthers = fleet.runtimes.entrySet().stream().anyMatch(e -> !e.getKey().equals("m0")
                        && e.getValue().membership().member(thin[0].peerId()).isPresent());
                if (heldByOthers) { // the seed holds its link and keeps it; the others reach it only through m5
                    lastHeldAt = fleet.clock.millis();
                    reqsWhileHeld = victim.traffic().receivedByKind().getOrDefault(Envelope.Kind.PING_REQ, 0L)
                            - before;
                }
            }
            long reqs = victim.traffic().receivedByKind().getOrDefault(Envelope.Kind.PING_REQ, 0L) - before;
            long cycle = fleet.runtimes.get("m5").membership().quietProbeCycleMillis();
            long lasted = lastHeldAt - start;
            double perCycle = lasted == 0 ? 0 : reqsWhileHeld / Math.max(1.0, lasted / (double) Math.max(1, cycle));
            Measure.record("core.thin-peer.victim-pingreqs", Unit.COUNT, Direction.LOWER, perCycle,
                    Rec.with().params(Params.of("members", 10, "harness", "virtual")).kind(Kind.COUNT));
            System.out.printf("EL-SP-31 victim holder: the misnamed thin peer lasted %d ms in a non-seed view; the victim"
                    + " received %d PING_REQs in that time (%.1f per %d ms cycle) and %d in all 10 minutes%n", lasted,
                    reqsWhileHeld, perCycle, cycle, reqs);
            assertThat(reqs - reqsWhileHeld).as("the victim's load ends with the peer").isLessThanOrEqualTo(8L);
        } finally {
            thin[0].close();
            fleet.close();
        }
    }
}
