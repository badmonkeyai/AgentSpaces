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

import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftLog;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 0 of srad/agentspaces-event-loop.md on today's tick loop, on virtual time: the detection, newcomer and Raft
 * baselines the later phases compare against (EL-M-03, EL-M-04; the scenarios EL-SP-01, EL-SP-09 and EL-SP-10). Every
 * node is driven as the scheduled ticker drives it at the starter's 250 ms, one {@link PeerNode#tick()} per node per
 * step and the fleet clock moved 250 ms; a direct tick never takes the idle divider's rest (PN:4053-4080), so these
 * figures are today's protocol at the full tick rate, and {@code IdleCostIT} records the same series over TCP with the
 * real ticker and its divider. Each figure is recorded in the measurements store; the protocol bounds stay hard
 * assertions (class (c)).
 */
class EventLoopBaselineTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final Duration PERIOD = Duration.ofSeconds(1); // the gossip period, which the runner counts in

    private static void step(RobustnessFleet fleet, ScriptRunner runner) {
        for (PeerNode node : List.copyOf(fleet.nodes.values())) {
            node.tick();
        }
        if (runner != null) {
            runner.tick();
        }
        fleet.clock.advance(TICK);
    }

    private static RobustnessFleet idleFleet(String name, int size) throws Exception {
        RobustnessFleet fleet = new RobustnessFleet(name);
        for (int i = 0; i < size; i++) {
            fleet.add("m" + i, i == 0 ? null : "m0", PeerNode.ChannelAuth.SIGNED, GroupMembership.Config.defaults(),
                    true);
        }
        int steps = 0;
        while (!fleet.nodes.values().stream().allMatch(n -> n.group(fleet.groupId).orElseThrow().membership()
                .allMembers().size() == size - 1)) {
            step(fleet, null);
            assertThat(++steps).as("the fleet forms within 120 s of fleet time").isLessThan(480);
        }
        for (int i = 0; i < 480; i++) { // two idle minutes: the probe interval stretches as far as a quiet group may
            step(fleet, null);
        }
        return fleet;
    }

    /** EL-SP-01 on today's loop: a member stops answering anything; the time until no other view holds it. */
    @ParameterizedTest
    @ValueSource(ints = {2, 10})
    void aSilentMemberLeavesEveryViewWithinTheMemberTtl(int size) throws Exception {
        RobustnessFleet fleet = idleFleet("ElSilent" + size, size);
        try {
            String silent = "m" + (size - 1);
            PeerId gone = fleet.id(silent);
            Script script = Script.of(silent).seed(2101)
                    .rule("silent-out").when(Trigger.frame().from(silent)).act(Action.drop()).always()
                    .rule("silent-in").when(Trigger.frame().to(silent)).act(Action.drop()).always()
                    .build();
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, PERIOD, PeerNode.ChannelAuth.SIGNED,
                    script);
            long start = fleet.clock.millis();
            int steps = 0;
            while (anyHolds(fleet, silent, gone)) {
                step(fleet, runner);
                assertThat(++steps).as("removed within 200 s of fleet time").isLessThan(800);
            }
            long detected = fleet.clock.millis() - start;
            int returns = 0;
            for (int i = 0; i < 240; i++) { // a minute more: it must not come back while silent
                step(fleet, runner);
                returns += anyHolds(fleet, silent, gone) ? 1 : 0;
            }
            runner.finish();
            Measure.record("core.detect.silent-member", Unit.MS, Direction.LOWER, detected,
                    Rec.with().params(Params.of("members", size, "harness", "virtual")).kind(Kind.PERF));
            System.out.printf("EL-SP-01 baseline: %d members, silent member gone from every view after %d ms%n",
                    size, detected);
            long ttl = GroupMembership.Config.defaults().memberTtl().toMillis();
            assertThat(detected).as("SPEC §5.2: a silent member leaves within its TTL and one tick")
                    .isLessThanOrEqualTo(ttl + TICK.toMillis());
            assertThat(returns).as("a silent member is not re-admitted from a copy of its card").isZero();
        } finally {
            fleet.close();
        }
    }

    private static boolean anyHolds(RobustnessFleet fleet, String silent, PeerId gone) {
        for (var e : fleet.runtimes.entrySet()) {
            if (!e.getKey().equals(silent) && e.getValue().membership().member(gone).isPresent()) {
                return true;
            }
        }
        return false;
    }

    /** EL-SP-09 on today's loop: every member idle, a newcomer admitted; admission to the targeted round. */
    @Test
    void aNewcomerGetsItsTargetedRoundOnTheTickAfterItsAdmission() throws Exception {
        int size = 4;
        RobustnessFleet fleet = idleFleet("ElNewcomer", size);
        try {
            List<Integer> before = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                before.add(fleet.nodes.get("m" + i).newcomerRoundMillis().size());
            }
            fleet.add("late", "m0", PeerNode.ChannelAuth.SIGNED, GroupMembership.Config.defaults(), true);
            int steps = 0;
            while (!roundEverywhere(fleet, size, before)) {
                step(fleet, null);
                assertThat(++steps).as("every member admits the newcomer within 60 s of fleet time").isLessThan(240);
            }
            double[] rounds = new double[size];
            for (int i = 0; i < size; i++) {
                List<Long> list = fleet.nodes.get("m" + i).newcomerRoundMillis();
                rounds[i] = list.get(list.size() - 1);
            }
            Measure.samples("core.newcomer.targeted-round", Unit.MS, Direction.LOWER, rounds,
                    Rec.with().params(Params.of("members", size, "harness", "virtual")).kind(Kind.PERF));
            System.out.printf("EL-SP-09 baseline: targeted round after admission %s ms%n",
                    java.util.Arrays.toString(rounds));
            for (double r : rounds) {
                assertThat(r).as("SPEC §5.3: the targeted round on the tick after the admission")
                        .isLessThanOrEqualTo(TICK.toMillis());
            }
        } finally {
            fleet.close();
        }
    }

    private static boolean roundEverywhere(RobustnessFleet fleet, int size, List<Integer> before) {
        for (int i = 0; i < size; i++) {
            if (fleet.nodes.get("m" + i).newcomerRoundMillis().size() <= before.get(i)) {
                return false;
            }
        }
        return true;
    }

    /** EL-SP-10 on today's loop: a three-member ordered log idles; its leader falls silent; a follower calls an election. */
    @Test
    void aFollowerCallsAnElectionAfterItsLeaderFallsSilent() throws Exception {
        RobustnessFleet fleet = new RobustnessFleet("ElRaft");
        CborCodec codec = CborCodec.defaultCodec();
        try {
            List<PeerIdentity> identities = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                PeerIdentity identity = PeerIdentity.generate();
                identities.add(identity);
                fleet.identities.put("r" + i, identity);
            }
            List<PeerId> ids = identities.stream().map(PeerIdentity::peerId).toList();
            List<RaftLog> logs = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                fleet.add("r" + i, i == 0 ? null : "r0", PeerNode.ChannelAuth.SIGNED,
                        GroupMembership.Config.defaults(), true);
                GroupRuntime runtime = fleet.runtimes.get("r" + i);
                RaftLog log = new RaftLog(new CapabilityPipes(runtime, codec), codec, ids.get(i), ids, fleet.clock,
                        1000 + i, (command, index) -> { });
                CapabilityRuntime capabilities = new CapabilityRuntime(runtime,
                        new DiscoveryService(runtime, new AdCache(codec, fleet.clock), codec, ids.get(i)),
                        identities.get(i));
                capabilities.register(log); // driven by the node's tick, as in production (CR:116)
                logs.add(log);
            }
            int steps = 0;
            while (logs.stream().filter(RaftLog::isLeader).count() != 1) {
                step(fleet, null);
                assertThat(++steps).as("a leader within 60 s of fleet time").isLessThan(240);
            }
            for (int i = 0; i < 40; i++) { // ten idle seconds under a live leader
                step(fleet, null);
            }
            int leader = 0;
            while (!logs.get(leader).isLeader()) {
                leader++;
            }
            long term = logs.get(leader).term();
            String silent = "r" + leader;
            Script script = Script.of(silent).seed(2110)
                    .rule("leader-silent").when(Trigger.frame(Envelope.Kind.PIPE_DATA).from(silent)).act(Action.drop()).always()
                    .build();
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, PERIOD, PeerNode.ChannelAuth.SIGNED,
                    script);
            long start = fleet.clock.millis();
            int followerA = (leader + 1) % 3;
            int followerB = (leader + 2) % 3;
            long called = -1;
            long elected = -1;
            steps = 0;
            while (elected < 0) {
                step(fleet, runner);
                assertThat(++steps).as("a new leader within 30 s of fleet time").isLessThan(120);
                if (called < 0 && (logs.get(followerA).term() > term || logs.get(followerB).term() > term)) {
                    called = fleet.clock.millis() - start;
                }
                if (logs.get(followerA).isLeader() || logs.get(followerB).isLeader()) {
                    elected = fleet.clock.millis() - start;
                }
            }
            runner.finish();
            Measure.record("core.raft.election-after-silence", Unit.MS, Direction.LOWER, called,
                    Rec.with().params(Params.of("members", 3, "harness", "virtual", "until", "called")).kind(Kind.PERF));
            Measure.record("core.raft.election-after-silence", Unit.MS, Direction.LOWER, elected,
                    Rec.with().params(Params.of("members", 3, "harness", "virtual", "until", "elected")).kind(Kind.PERF));
            System.out.printf("EL-SP-10 baseline: election called %d ms, new leader %d ms after the leader fell silent%n",
                    called, elected);
            assertThat(called).as("an election within the longest election timeout (9 ticks) and one tick")
                    .isLessThanOrEqualTo(10 * TICK.toMillis());
        } finally {
            fleet.close();
        }
    }
}
