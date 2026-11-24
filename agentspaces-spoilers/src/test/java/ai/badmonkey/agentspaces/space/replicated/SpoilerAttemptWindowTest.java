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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.measure.SimWait;
import ai.badmonkey.agentspaces.measure.Wait;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.spoilers.Action;
import ai.badmonkey.agentspaces.spoilers.Ceiling;
import ai.badmonkey.agentspaces.spoilers.Script;
import ai.badmonkey.agentspaces.spoilers.ScriptRunner;
import ai.badmonkey.agentspaces.spoilers.Trigger;
import ai.badmonkey.agentspaces.test.FleetInvariants;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SC-B3, the core form (srad/spoiler-peers.md §3.8 B): a follower takes through the ordered log, and every PIPE_DATA
 * frame it sends to the leader (its claim command among them) is held past the take's own timeout, then released at the
 * hold's ceiling. REPAIRED: the taker waits out the log's 3 s attempt window, which holds past its timeout, so the claim
 * the log commits after the release is adopted, not left to hold the entry for its whole lease with nobody to adopt it.
 *
 * <p>Before, the 3 s was a wall-clock wait (§3.4.4: order asserted, duration measured). The attempt window now waits on
 * the fleet's virtual time (srad/agentspaces-event-loop.md decision 12, EL-W-01, §10.17), and the take runs as a task of
 * that time, so the bounds are hard: the take returns after its own timeout and inside the attempt window.
 */
class SpoilerAttemptWindowTest {

    /** The take's own timeout: shorter than the hold, so only the attempt window can see the commit. */
    private static final Duration TIMEOUT = Duration.ofSeconds(1);
    /** The hold's ceiling in ticks of {@link SpaceFleet#PERIOD} (1.5 s): past the timeout, inside the window. */
    private static final int HOLD_TICKS = 5;
    /** {@code OrderedTakes.ATTEMPT_WINDOW}. */
    private static final Duration ATTEMPT_WINDOW = Duration.ofSeconds(3);

    @Test
    @Timeout(120)
    void aClaimHeldPastTheTakersTimeoutIsAdoptedInsideTheAttemptWindow() throws Exception {
        runHeldClaim(true);
    }

    @Test
    @Timeout(120)
    void theNegativeControlAdoptsInsideTheTimeout() throws Exception {
        runHeldClaim(false);
    }

    /**
     * The stop of a take in flight, as a closing reduce binding sends it (srad/agentspaces-event-loop.md §10.17): the
     * interrupt does not cut the claim's attempt window short and is not swallowed. On a leaderless log the claim is
     * never decided; the take returns empty at exactly the window's end on the fleet's time, with the interrupt kept,
     * and submits nothing more.
     */
    @Test
    @Timeout(60)
    void anInterruptedOrderedTakeWaitsOutItsAttemptWindowExactlyAndKeepsTheInterrupt() throws Exception {
        try (SpaceFleet fleet = new SpaceFleet("AttemptInterrupt", ConflictStrategyType.LEASE_RACE, "desk")) {
            fleet.agentName = "desk";
            SpaceFleet.Member a = fleet.member("a", 1, b -> b.settleWindow(Duration.ZERO));
            fleet.tickAll(2);
            // two voters, one running: no leader, so the claim is never decided
            OrderedTakes ordered = OrderedTakes.over(new CapabilityPipes(a.runtime(), fleet.codec), fleet.codec,
                    a.identity(), "desk", java.util.List.of(a.id(), ai.badmonkey.agentspaces.identity.PeerIdentity
                            .generate().peerId()), fleet.clock, 7, a.space());
            a.space().write(new TaskEntry("job", 1), Lease.of(Duration.ofMinutes(30)));
            java.util.concurrent.atomic.AtomicReference<Thread> taker = new java.util.concurrent.atomic.AtomicReference<>();
            java.util.concurrent.atomic.AtomicBoolean kept = new java.util.concurrent.atomic.AtomicBoolean();
            java.util.concurrent.CompletableFuture<VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>>> take =
                    fleet.clock.start(() -> {
                        taker.set(Thread.currentThread());
                        Optional<TakenEntry<TaskEntry>> t = ordered.take(Template.of(TaskEntry.class),
                                Lease.of(Duration.ofMinutes(5)), Duration.ofSeconds(60));
                        kept.set(Thread.currentThread().isInterrupted());
                        return t;
                    });
            fleet.clock.runDue(); // the take claims and waits on the log, on the fleet's time
            fleet.clock.advance(Duration.ofMillis(500));
            taker.get().interrupt(); // what a closing binding sends
            for (int i = 0; i < 40 && !take.isDone(); i++) {
                fleet.clock.advance(Duration.ofMillis(100));
            }
            assertThat(take).as("the take returned").isDone();
            assertThat(take.join().value()).as("an undecided claim gives nothing").isEmpty();
            assertThat(take.join().took()).as("exactly the attempt window: the interrupt did not cut it short")
                    .isEqualTo(ATTEMPT_WINDOW);
            assertThat(kept).as("and the interrupt was kept for the caller, not swallowed").isTrue();
            assertThat(a.space().readAll(Template.of(TaskEntry.class), 10)).as("the entry is still free")
                    .containsExactly(new TaskEntry("job", 1));
        }
    }

    private void runHeldClaim(boolean spoil) throws Exception {
        try (SpaceFleet fleet = new SpaceFleet(spoil ? "AttemptWindow" : "AttemptWindowControl",
                ConflictStrategyType.LEASE_RACE, "desk")) {
            fleet.agentName = "desk";
            fleet.member("a", 1, b -> b.settleWindow(Duration.ZERO));
            fleet.member("b", 2, b -> b.settleWindow(Duration.ZERO), "a");
            fleet.member("c", 3, b -> b.settleWindow(Duration.ZERO), "a");
            fleet.tickAll(4);
            Map<String, OrderedTakes> logs = new LinkedHashMap<>();
            int seed = 300;
            for (SpaceFleet.Member m : fleet.members.values()) {
                OrderedTakes ordered = OrderedTakes.over(new CapabilityPipes(m.runtime(), fleet.codec), fleet.codec,
                        m.identity(), "desk", fleet.ids(), fleet.clock, seed++, m.space());
                m.group().ordered("desk", ordered);
                logs.put(m.name(), ordered);
            }
            SimWait.ticks("spoiler.attempt-window.leader", 60, fleet::tickAll,
                    () -> logs.values().stream().filter(o -> o.raft().isLeader()).count() == 1, Wait.with());
            String leader = logs.entrySet().stream().filter(e -> e.getValue().raft().isLeader()).findFirst()
                    .orElseThrow().getKey();
            String taker = fleet.members.keySet().stream().filter(n -> !n.equals(leader)).findFirst().orElseThrow();
            fleet.get(leader).space().write(new TaskEntry("order", 1), Lease.of(Duration.ofMinutes(30)));
            fleet.tickAll(2);

            FleetInvariants oracle = FleetInvariants.baseline(fleet.probe(Set.of(), Template.of(FindingEntry.class)));
            Script script = !spoil ? Script.honest(taker) : Script.of(taker).seed(4740)
                    .rule("hold-claim-to-leader")
                    .when(Trigger.frame(Envelope.Kind.PIPE_DATA).from(taker).to(leader))
                    .act(Action.hold()).always()
                    .ceiling(Ceiling.periods(HOLD_TICKS))
                    .build();
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, SpaceFleet.PERIOD,
                    PeerNode.ChannelAuth.SIGNED, script);
            fleet.afterTick.add(runner::tick);

            VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>> take = fleet.callTicking("spoiler.attempt-window.take",
                    (int) ATTEMPT_WINDOW.dividedBy(SpaceFleet.PERIOD) + 1, () -> logs.get(taker)
                            .take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(5)), TIMEOUT));
            ScriptRunner.Report report = runner.finish();
            System.out.printf("SC-B3 %s: taker %s returned after %d ms; firing sequence %s%n", spoil ? "held" : "control",
                    taker, take.took().toMillis(), report.signatures());
            assertThat(take.value()).as("the claim the log committed was adopted").isPresent();
            if (spoil) {
                assertThat(report.fired("hold-claim-to-leader")).as("the claim command was held").isPositive();
                assertThat(report.releases()).as("released at the hold's ceiling")
                        .anySatisfy(r -> assertThat(r.cause()).startsWith("ceiling"));
                assertThat(take.took()).as("past the take's own timeout").isGreaterThan(TIMEOUT);
            } else {
                assertThat(take.took()).as("honestly, inside the timeout").isLessThanOrEqualTo(TIMEOUT);
            }
            assertThat(take.took()).as("inside the attempt window: a hard bound on the fleet's time")
                    .isLessThanOrEqualTo(ATTEMPT_WINDOW);
            assertThat(fleet.get(taker).space().currentClaim(take.value().get().entryId())).as("the taker holds it")
                    .hasValueSatisfying(c -> assertThat(c.holder()).isEqualTo(logs.get(taker).holder()));
            oracle.assertHolds(report.expectedDrops(), true);
        }
    }
}
