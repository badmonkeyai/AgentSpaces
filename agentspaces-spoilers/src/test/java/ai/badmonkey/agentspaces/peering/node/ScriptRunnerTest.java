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

import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.spoilers.Action;
import ai.badmonkey.agentspaces.spoilers.Ceiling;
import ai.badmonkey.agentspaces.spoilers.Script;
import ai.badmonkey.agentspaces.spoilers.ScriptRunner;
import ai.badmonkey.agentspaces.spoilers.Trigger;
import ai.badmonkey.agentspaces.test.FleetInvariants;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The harness's own tests (srad/spoiler-peers.md SP-C-02 to SP-C-10, SP-Q-REL-01, SP-Q-REL-04, SP-Q-MNT-02) on a
 * real {@link RobustnessFleet} of three SESSION members: rules fire on the frames they name, holds end on their
 * release trigger after the triggering frame, every rule that never fired fails the run by name, a hold past its
 * ceiling fails with the last events, a rule the link's channel-auth mode forbids is refused at load, and a replay
 * of the same script on the same fleet gives the same firing sequence.
 */
class ScriptRunnerTest {

    private static final Duration PERIOD = Duration.ofSeconds(1); // RobustnessFleet.tickAll advances one second
    private static final java.util.Map<String, ai.badmonkey.agentspaces.identity.PeerIdentity> IDENTITIES = java.util.Map.of(
            "a", ai.badmonkey.agentspaces.identity.PeerIdentity.generate(),
            "b", ai.badmonkey.agentspaces.identity.PeerIdentity.generate(),
            "c", ai.badmonkey.agentspaces.identity.PeerIdentity.generate());

    private static RobustnessFleet fleet() throws Exception {
        RobustnessFleet fleet = new RobustnessFleet("Spoil");
        // the same identities in every fleet of this class, so the members order each other the same way in a replay
        IDENTITIES.forEach(fleet.identities::put);
        // no lazy push: every rumor travels eagerly, so which links carry it does not depend on the members'
        // random identities, and a replay on a fresh fleet sees the same frames
        for (String[] m : new String[][] {{"a", null}, {"b", "a"}, {"c", "a"}}) {
            fleet.add(m[0], m[1], PeerNode.ChannelAuth.SESSION,
                    ai.badmonkey.agentspaces.peering.membership.GroupMembership.Config.defaults(), false);
        }
        assertThat(fleet.settle(30)).as("the fleet forms").isTrue();
        return fleet;
    }

    private static Script holdNewsToB() {
        return Script.of("a").seed(5)
                // every news rumor into b, from a or relayed by c, since gossip forwards what c hears
                .rule("hold-news-to-b")
                .when(Trigger.frame(Envelope.Kind.RUMOR).to("b").stream("news"))
                .act(Action.hold()).always()
                // released by the second news rumor a sends c: the effect of a's next publish, never a timer
                .until(Trigger.frame(Envelope.Kind.RUMOR).from("a").to("c").stream("news").nth(2))
                .ceiling(Ceiling.periods(10))
                .rule("count-news-to-c")
                .when(Trigger.frame(Envelope.Kind.RUMOR).from("a").to("c").stream("news"))
                .act(Action.count()).always()
                .build();
    }

    /** Publishes two items from a under the hold; returns the run's report. */
    private static ScriptRunner.Report holdAndRelease(RobustnessFleet fleet) {
        ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, PERIOD, PeerNode.ChannelAuth.SESSION, holdNewsToB());
        fleet.publish("a", "one");
        assertThat(fleet.news.get("c")).as("c hears at once").contains("one");
        assertThat(fleet.news.get("b")).as("b's copy is held").doesNotContain("one");
        fleet.publish("a", "two");
        assertThat(fleet.news.get("b")).as("the second rumor to c released b's").contains("one", "two");
        return runner.finish();
    }

    @Test
    void aHoldEndsOnItsReleaseFrameAndEveryMatchingRuleFires() throws Exception {
        RobustnessFleet fleet = fleet();
        try {
            ScriptRunner.Report report = holdAndRelease(fleet);
            assertThat(report.fired("hold-news-to-b")).isPositive();
            assertThat(report.fired("count-news-to-c")).isEqualTo(2);
            assertThat(report.releases()).singleElement().satisfies(r -> {
                assertThat(r.rule()).isEqualTo("hold-news-to-b");
                assertThat(r.frames()).isPositive();
                assertThat(r.pastFrameAge()).isZero();
                assertThat(r.cause()).contains("a>c");
            });
            assertThat(report.expectedDrops()).isEmpty();
            fleet.tickAll(5);
            fleet.assertInvariants();
        } finally {
            fleet.close();
        }
    }

    @Test
    void aReplayOfTheScriptFiresTheSameSequence() throws Exception {
        List<String> first;
        RobustnessFleet one = fleet();
        try {
            first = holdAndRelease(one).signatures();
        } finally {
            one.close();
        }
        RobustnessFleet two = fleet();
        try {
            assertThat(holdAndRelease(two).signatures()).as("SP-Q-REL-04").isEqualTo(first);
        } finally {
            two.close();
        }
    }

    @Test
    void theNegativeControlFiresNothingAndTheOracleHolds() throws Exception {
        RobustnessFleet fleet = fleet();
        try {
            FleetInvariants oracle = FleetInvariants.baseline(fleet.probe());
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, PERIOD, PeerNode.ChannelAuth.SESSION,
                    Script.honest("a"));
            fleet.publish("a", "plain");
            fleet.tickAll(5);
            ScriptRunner.Report report = runner.finish();
            assertThat(report.firings()).isEmpty();
            assertThat(fleet.news.get("b")).contains("plain");
            oracle.assertHolds(report.expectedDrops(), true);
        } finally {
            fleet.close();
        }
    }

    @Test
    void aRuleThatNeverFiredFailsTheRunByName() throws Exception {
        RobustnessFleet fleet = fleet();
        try {
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, PERIOD, PeerNode.ChannelAuth.SESSION,
                    Script.of("a").rule("never").when(Trigger.frame(Envelope.Kind.QUERY).from("a").to("b"))
                            .act(Action.drop()).build());
            fleet.tickAll(3);
            assertThatThrownBy(runner::finish).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("rule never never fired").hasMessageContaining("seed");
        } finally {
            fleet.close();
        }
    }

    @Test
    void aHoldPastItsCeilingFailsWithTheRuleAndTheLastEvents() throws Exception {
        RobustnessFleet fleet = fleet();
        try {
            AtomicLong never = new AtomicLong();
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, PERIOD, PeerNode.ChannelAuth.SESSION,
                    Script.of("a").rule("stuck").when(Trigger.frame(Envelope.Kind.RUMOR).to("b").stream("news"))
                            .act(Action.hold()).until(Trigger.counter("never", never::get, 1)).ceiling(Ceiling.periods(2)).build());
            fleet.publish("a", "late");
            fleet.tickAll(1);
            runner.tick();
            fleet.tickAll(2);
            assertThatThrownBy(runner::tick).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("rule stuck").hasMessageContaining("ceiling of 2 periods")
                    .hasMessageContaining("last").hasMessageContaining("firing sequence");
            runner.close();
        } finally {
            fleet.close();
        }
    }

    @Test
    void aHoldWithNoReleaseEndsAtItsCeiling() throws Exception {
        RobustnessFleet fleet = fleet();
        try {
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, PERIOD, PeerNode.ChannelAuth.SESSION,
                    Script.of("a").rule("slow").when(Trigger.frame(Envelope.Kind.RUMOR).to("b").stream("news"))
                            .act(Action.hold()).always().ceiling(Ceiling.periods(2)).build());
            fleet.publish("a", "slow");
            assertThat(fleet.news.get("b")).doesNotContain("slow");
            for (int i = 0; i < 3; i++) {
                fleet.tickAll(1);
                runner.tick();
            }
            assertThat(fleet.news.get("b")).contains("slow");
            assertThat(runner.finish().releases()).singleElement().satisfies(r -> assertThat(r.cause()).contains("ceiling"));
        } finally {
            fleet.close();
        }
    }

    @Test
    void aDropAndADelayActOnlyOnTheFramesTheyName() throws Exception {
        RobustnessFleet fleet = fleet();
        try {
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, PERIOD, PeerNode.ChannelAuth.SESSION,
                    Script.of("a")
                            .rule("lose-to-b").when(Trigger.frame(Envelope.Kind.RUMOR).from("a").to("b").stream("news"))
                            .act(Action.drop())
                            .rule("slow-to-c").when(Trigger.frame(Envelope.Kind.RUMOR).from("a").to("c").stream("news"))
                            .act(Action.delay(Duration.ofSeconds(2)))
                            .build());
            fleet.publish("a", "x");
            assertThat(fleet.news.get("b")).doesNotContain("x");
            assertThat(fleet.news.get("c")).doesNotContain("x");
            fleet.clock.advance(Duration.ofSeconds(2));
            fleet.network.pump();
            assertThat(fleet.news.get("c")).as("delivered on the first pump past its delay").contains("x");
            assertThat(fleet.network.filteredDrops()).isEqualTo(1);
            runner.finish();
        } finally {
            fleet.close();
        }
    }

    @Test
    void aRuleTheLinksChannelAuthForbidsIsRefusedAtLoad() throws Exception {
        RobustnessFleet fleet = fleet();
        try {
            // the spoiler cannot make an honest member send twice: a duplicate on a link into the spoiler is refused
            assertThatThrownBy(() -> ScriptRunner.attach(fleet.network, fleet.clock, PERIOD, PeerNode.ChannelAuth.SESSION,
                    Script.of("a").rule("echo").when(Trigger.frame(Envelope.Kind.RUMOR).from("b").to("a"))
                            .act(Action.duplicate(2)).build()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("rule echo")
                    .hasMessageContaining("INBOUND");
            // a link action needs a frame to act on
            assertThatThrownBy(() -> ScriptRunner.attach(fleet.network, fleet.clock, PERIOD, PeerNode.ChannelAuth.SIGNED,
                    Script.of("a").rule("timed").when(Trigger.afterPeriods(3)).act(Action.drop()).build()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("frame trigger");
        } finally {
            fleet.close();
        }
    }

    @Test
    void theFrameAgeTheRunnerExpectsIsThePeerNodes() throws Exception {
        java.lang.reflect.Field age = PeerNode.class.getDeclaredField("MAX_FRAME_AGE_MILLIS");
        age.setAccessible(true);
        assertThat(ScriptRunner.FRAME_AGE_MILLIS).isEqualTo(age.getLong(null));
    }
}
