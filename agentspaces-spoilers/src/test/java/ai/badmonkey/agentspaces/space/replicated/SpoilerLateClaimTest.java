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

import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Measurement;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.SimWait;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.measure.Wait;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.spoilers.Action;
import ai.badmonkey.agentspaces.spoilers.Ceiling;
import ai.badmonkey.agentspaces.spoilers.ItemKind;
import ai.badmonkey.agentspaces.spoilers.Script;
import ai.badmonkey.agentspaces.spoilers.ScriptRunner;
import ai.badmonkey.agentspaces.spoilers.Trigger;
import ai.badmonkey.agentspaces.test.FleetInvariants;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SC-A1 and the core form of SC-A6 (srad/spoiler-peers.md §3.8 A), on a fleet over {@link SimNetwork}: a late claimer.
 * Taker {@code b}'s claim item is held on every link into taker {@code a} (inside RUMOR, PULL_RESP and BATCH, whoever
 * forwards it) until a frame carrying {@code a}'s completion item for the entry is seen. Before, the race came at a rate
 * from load in the Party Bus fleet test; here the order is the script's, every run.
 *
 * <ul>
 *   <li>LEASE_RACE: {@code b} claims first (its stamp is the lower), so it wins the lattice, but {@code a} never hears of
 *   it inside its window and also judges itself the winner. {@code a} completes; its completion releases the hold and
 *   reaches {@code b} before {@code b} completes, so {@code b} stands down (SPEC §7.4 rule 2): one result, no duplicate.
 *   The row's inferred outcome ("{@code duplicateCompletions == 1}") is the other order; it needs {@code b} to complete
 *   first, which is the SC-A6 form below.</li>
 *   <li>SC-A6, the idempotent take: each worker's claim and completion are held toward the other until the other's own
 *   completion is seen, so both complete. TOLERATE: both results stand, and the duplicate is counted once, where the
 *   lattice's winning claim is held.</li>
 *   <li>The negative control: the same workload with an empty script, one winner inside the window.</li>
 * </ul>
 *
 * <p>Takes settle on the wall clock (§3.4.4), so the order is asserted and the durations measured; the repair after
 * the release is a protocol bound on the fleet clock, asserted on ticks.
 */
class SpoilerLateClaimTest {

    private static final Duration SETTLE = Duration.ofMillis(200);
    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));
    /**
     * The repair bound after a release, in ticks of {@link SpaceFleet#PERIOD}: the rumors travel inline, and what a
     * rumor missed anti-entropy repairs within the gossip's repair window plus a round (S + A + B(1) of the row, with
     * the settle already behind every take). Checked against the code: the default repair window is 3 s, ten ticks.
     */
    private static final int REPAIR_TICKS = 20;

    private static Template<TaskEntry> task() {
        return Template.of(TaskEntry.class);
    }

    private SpaceFleet fleet(String name) throws Exception {
        SpaceFleet fleet = new SpaceFleet(name, ConflictStrategyType.LEASE_RACE, "work");
        fleet.member("w", 1, b -> b.settleWindow(SETTLE));
        fleet.member("a", 2, b -> b.settleWindow(SETTLE), "w");
        fleet.member("b", 3, b -> b.settleWindow(SETTLE), "w");
        fleet.tickAll(4);
        fleet.get("w").space().write(new TaskEntry("contested", 1), MINUTES_30);
        fleet.tickAll(1);
        return fleet;
    }

    private static ScriptRunner arm(SpaceFleet fleet, Script script) {
        ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, SpaceFleet.PERIOD, PeerNode.ChannelAuth.SIGNED,
                script);
        fleet.afterTick.add(runner::tick);
        return runner;
    }

    /** Waits for a rule's first firing, measured: it fires on a frame a take thread sends. */
    private static void awaitFired(ScriptRunner runner, String rule, String metric) {
        Measure.await(metric, Duration.ofSeconds(30), () -> runner.report().fired(rule) > 0,
                Wait.with().diagnostics(() -> "firings " + runner.report().signatures()));
    }

    /** Ticks until every replica holds exactly the given results and no task, within the repair bound; returns the ticks. */
    private static Measurement converge(SpaceFleet fleet, String metric, List<FindingEntry> results) {
        return SimWait.ticks(metric, REPAIR_TICKS, fleet::tickSlowly, () -> fleet.members.values().stream().allMatch(m ->
                        m.space().read(task()).isEmpty()
                        && m.space().readAll(Template.of(FindingEntry.class), 10).stream().sorted(
                                java.util.Comparator.comparing(FindingEntry::summary)).toList().equals(results)),
                Wait.with().diagnostics(() -> fleet.members.values().stream().map(m -> m.name() + " holds "
                        + m.space().readAll(Template.of(FindingEntry.class), 10) + ", stats " + m.space().stats()).toList()
                        .toString()));
    }

    @Test
    @Timeout(120)
    void aLateClaimUnderLeaseRaceIsRepairedWhenTheLatticeWinnerHearsTheDisplacedCompletionFirst() throws Exception {
        try (SpaceFleet fleet = fleet("LateClaim")) {
            SpaceFleet.Member a = fleet.get("a");
            SpaceFleet.Member b = fleet.get("b");
            FleetInvariants oracle = FleetInvariants.baseline(fleet.probe(Set.of(), Template.of(FindingEntry.class)));
            ScriptRunner runner = arm(fleet, Script.of("b").seed(4711)
                    .rule("hold-claim")
                    .when(Trigger.frame(Envelope.Kind.RUMOR, Envelope.Kind.PULL_RESP, Envelope.Kind.BATCH).to("a")
                            .item(ItemKind.CLAIM).attr("holderPeer", b.id().toString()))
                    .act(Action.hold()).always()
                    .until(Trigger.frame().from("a").item(ItemKind.COMPLETION))
                    .ceiling(Ceiling.periods(40))
                    // a marker on the wire, not a look inside b: a frame carrying a's completion reached b
                    .rule("completion-into-b")
                    .when(Trigger.frame(Envelope.Kind.RUMOR, Envelope.Kind.PULL_RESP, Envelope.Kind.BATCH).to("b")
                            .item(ItemKind.COMPLETION).attr("issuerPeer", fleet.get("w").id().toString()))
                    .act(Action.count())
                    .build());

            // b claims first: its stamp is the lower, so its claim wins the lattice everywhere it is heard
            CompletableFuture<Optional<TakenEntry<TaskEntry>>> atB = CompletableFuture.supplyAsync(
                    () -> b.space().take(task(), MINUTES_10, Duration.ZERO));
            awaitFired(runner, "hold-claim", "spoiler.late-claim.held");
            fleet.clock.advance(Duration.ofMillis(10));
            long judging = System.nanoTime();
            Optional<TakenEntry<TaskEntry>> displaced = a.space().take(task(), MINUTES_10, Duration.ZERO);
            Measure.record("spoiler.late-claim.take", Unit.MS, Direction.LOWER, (System.nanoTime() - judging) / 1e6,
                    Rec.with().params(Params.of("strategy", "lease-race")));
            assertThat(displaced).as("a never heard of b's claim inside its window, so it judged itself the winner")
                    .isPresent();
            Optional<TakenEntry<TaskEntry>> winning = Measure.awaitValue("spoiler.late-claim.lattice-winner",
                    Duration.ofSeconds(30), () -> atB.isDone() ? Optional.of(atB.join()) : Optional.empty(), Wait.with());
            assertThat(winning).as("b heard a's later claim and kept the entry: it holds the lattice's winning claim")
                    .isPresent();

            // a completes; the frame carrying its completion releases b's held claim
            a.space().complete(displaced.get(), new FindingEntry("contested", "done by a"), MINUTES_30);
            // the completion travels by rumor, or by the next anti-entropy round when the push skipped b
            SimWait.ticks("spoiler.late-claim.completion-reaches-winner", REPAIR_TICKS, fleet::tickSlowly,
                    () -> runner.report().fired("completion-into-b") > 0, Wait.with());
            // rule 2: the lattice winner that hears of the displaced holder's completion before its own stands down
            assertThatThrownBy(() -> b.space().complete(winning.get(), new FindingEntry("contested", "done by b"),
                    MINUTES_30)).isInstanceOf(LeaseExpiredException.class).hasMessageContaining("completed first");
            assertThat(b.space().outbidAcknowledged()).isEqualTo(1);

            Measurement repair = converge(fleet, "spoiler.late-claim.repair",
                    List.of(new FindingEntry("contested", "done by a")));
            for (SpaceFleet.Member m : fleet.members.values()) {
                assertThat(m.space().duplicateCompletions()).as("nothing done twice at " + m.name()).isZero();
            }
            ScriptRunner.Report report = runner.finish();
            System.out.printf("SC-A1 LEASE_RACE: repaired in %.0f ticks; firing sequence %s%n", repair.value(),
                    report.signatures());
            assertThat(report.signatures().get(0)).as("the claim was held first").startsWith("hold-claim hold");
            assertThat(report.releases()).as("and released by the frame carrying a's completion, not by its ceiling")
                    .singleElement().satisfies(r -> assertThat(r.cause()).startsWith("on "));
            oracle.assertHolds(report.expectedDrops(), true);
        }
    }

    @Test
    @Timeout(120)
    void twoWorkersOnAnIdempotentTakeBothCompleteAndTheDuplicateIsCountedOnce() throws Exception {
        try (SpaceFleet fleet = fleet("Idempotent")) {
            SpaceFleet.Member a = fleet.get("a");
            SpaceFleet.Member b = fleet.get("b");
            FleetInvariants oracle = FleetInvariants.baseline(fleet.probe(Set.of(), Template.of(FindingEntry.class)));
            // each worker's frames toward the other wait until the other's own completion is on the wire
            ScriptRunner runner = arm(fleet, Script.of("b").seed(4712)
                    .rule("hold-b-toward-a")
                    .when(Trigger.frame(Envelope.Kind.RUMOR, Envelope.Kind.PULL_RESP, Envelope.Kind.BATCH).to("a")
                            .item(ItemKind.CLAIM).attr("holderPeer", b.id().toString()))
                    .act(Action.hold()).always()
                    .until(Trigger.frame().from("a").item(ItemKind.COMPLETION))
                    .ceiling(Ceiling.periods(40))
                    .rule("hold-a-toward-b")
                    .when(Trigger.frame(Envelope.Kind.RUMOR, Envelope.Kind.PULL_RESP, Envelope.Kind.BATCH).to("b")
                            .item(ItemKind.CLAIM).attr("holderPeer", a.id().toString()))
                    .act(Action.hold()).always()
                    .until(Trigger.frame().from("b").item(ItemKind.COMPLETION))
                    .ceiling(Ceiling.periods(40))
                    .rule("hold-a-completion-toward-b")
                    .when(Trigger.frame(Envelope.Kind.RUMOR, Envelope.Kind.PULL_RESP, Envelope.Kind.BATCH).to("b")
                            .item(ItemKind.COMPLETION))
                    .act(Action.hold()).always()
                    .until(Trigger.frame().from("b").item(ItemKind.COMPLETION))
                    .ceiling(Ceiling.periods(40))
                    .build());

            Optional<TakenEntry<TaskEntry>> atA = a.space().take(task(), MINUTES_10, Duration.ZERO);
            fleet.clock.advance(Duration.ofMillis(10));
            Optional<TakenEntry<TaskEntry>> atB = b.space().take(task(), MINUTES_10, Duration.ZERO);
            assertThat(atA).as("a judged alone").isPresent();
            assertThat(atB).as("b judged alone: two holders past the window").isPresent();
            a.space().complete(atA.get(), new FindingEntry("contested", "done by a"), MINUTES_30);
            b.space().complete(atB.get(), new FindingEntry("contested", "done by b"), MINUTES_30);

            converge(fleet, "spoiler.idempotent-take.repair", List.of(new FindingEntry("contested", "done by a"),
                    new FindingEntry("contested", "done by b")));
            // a's claim is the earlier, so the lattice's winning claim is a's: the duplicate is counted where it is held
            assertThat(a.space().duplicateCompletions()).as("counted where the winning claim is held").isEqualTo(1);
            assertThat(b.space().duplicateCompletions()).isZero();
            assertThat(fleet.get("w").space().duplicateCompletions()).as("a replica holding no claim judges nothing")
                    .isZero();
            Measure.record("spoiler.idempotent-take.duplicates", Unit.COUNT, Direction.LOWER,
                    fleet.members.values().stream().mapToLong(m -> m.space().duplicateCompletions()).sum(),
                    Rec.with().seed(4712));
            ScriptRunner.Report report = runner.finish();
            System.out.printf("SC-A6 core: firing sequence %s%n", report.signatures());
            oracle.assertHolds(report.expectedDrops(), true);
        }
    }

    @Test
    @Timeout(120)
    void theNegativeControlLeavesOneWinnerInsideTheWindow() throws Exception {
        try (SpaceFleet fleet = fleet("LateClaimControl")) {
            SpaceFleet.Member a = fleet.get("a");
            SpaceFleet.Member b = fleet.get("b");
            FleetInvariants oracle = FleetInvariants.baseline(fleet.probe(Set.of(), Template.of(FindingEntry.class)));
            ScriptRunner runner = arm(fleet, Script.honest("b"));
            Optional<TakenEntry<TaskEntry>> atB = b.space().take(task(), MINUTES_10, Duration.ZERO);
            fleet.clock.advance(Duration.ofMillis(10));
            assertThat(atB).isPresent();
            assertThat(a.space().take(task(), MINUTES_10, Duration.ZERO)).as("the claim arrived in time").isEmpty();
            b.space().complete(atB.get(), new FindingEntry("contested", "done by b"), MINUTES_30);
            converge(fleet, "spoiler.late-claim.control", List.of(new FindingEntry("contested", "done by b")));
            for (SpaceFleet.Member m : fleet.members.values()) {
                assertThat(m.space().duplicateCompletions()).isZero();
                assertThat(m.space().outbidAcknowledged()).isZero();
            }
            ScriptRunner.Report report = runner.finish();
            assertThat(report.firings()).as("no rule, no firing").isEmpty();
            oracle.assertHolds(report.expectedDrops(), true);
        }
    }
}
