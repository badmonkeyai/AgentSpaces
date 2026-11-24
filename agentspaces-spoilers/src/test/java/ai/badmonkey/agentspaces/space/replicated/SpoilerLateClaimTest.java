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
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Measurement;
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
import ai.badmonkey.agentspaces.test.VirtualTime;
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
 * <p>Takes settle on the fleet's virtual time (srad/agentspaces-event-loop.md decision 12, EL-W-01, §10.17): each
 * take runs as a task of that time, so its settle window is a hard bound in virtual time (REPAIRED within exactly the
 * window), no longer a wall duration to measure; the repair after the release is a protocol bound on the fleet clock,
 * asserted on ticks.
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
    /**
     * The bound, in ticks, on the winner's first anti-entropy pull from the one replica that holds a displaced
     * completion, in the scenarios where every rumor of that completion into the winner is lost. The writer cannot hold
     * it: it merged the winner's claim first, so it drops the completion (SPEC §11a.4), and the loser is its only
     * source. Anti-entropy pulls (the opener is the one repaired), so the completion reaches the winner on its first
     * periodic round whose random partner is the loser: one draw in two per round, a round every 1 s period, which
     * lands on every fourth 300 ms tick. Twenty ticks are four rounds, missed one run in sixteen on a fair draw (and
     * in 2 of 8 to 4 of 6 runs of the seeded fleet, whose few reachable draw sequences include four misses in a
     * row). Sixteen rounds: a miss on a fair draw is 2^-16.
     */
    private static final int HOLDER_PULL_TICKS = 16 * 4;
    /**
     * The bound on the same pull when the winner heard the displaced claim before the completion (owner decision 4,
     * 2026-11-23): the winner opens targeted rounds toward each displaced holder while its take is open, at once and
     * then after 1, 2, 4 ... s, so the pull lands on the first targeted round after the completion, within about one
     * period (four ticks); eight ticks leave a round of slack. Its random rounds may come first.
     */
    private static final int TARGETED_PULL_TICKS = 8;

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
            CompletableFuture<VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>>> atB = fleet.clock.start(
                    () -> b.space().take(task(), MINUTES_10, Duration.ZERO));
            fleet.clock.runDue(); // b claims, sends its claim, and settles on the fleet's time
            awaitFired(runner, "hold-claim", "spoiler.late-claim.held");
            fleet.clock.advance(Duration.ofMillis(10));
            VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>> judged = fleet.call(
                    () -> a.space().take(task(), MINUTES_10, Duration.ZERO));
            Optional<TakenEntry<TaskEntry>> displaced = judged.value();
            assertThat(displaced).as("a never heard of b's claim inside its window, so it judged itself the winner")
                    .isPresent();
            assertThat(judged.took()).as("a's take ended exactly at its settle window (decision 12)").isEqualTo(SETTLE);
            assertThat(atB).as("b's window ended inside a's: b started 10 ms earlier").isDone();
            Optional<TakenEntry<TaskEntry>> winning = atB.join().value();
            assertThat(atB.join().took()).isEqualTo(SETTLE);
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

    /**
     * The store race of 2026-11-22 (StorePeerTest, 2 of 2 forked reactors; srad/agentspaces-event-loop.md §10.20), at
     * the space: the loser's completion reaches the lattice winner only by anti-entropy, from replicas that have
     * merged the winner's claim, so it travels under that claim, which does not authenticate it. The rumor that carries
     * the completion with the loser's own claim is lost, with every repair of it (rule {@code drop-completion-rumors}:
     * in the store race the gossip views had dropped the stores). Both scenarios failed before the fix: the winner
     * never noted the completion and wrote a second result.
     *
     * @param claimHeard whether the loser's claim reached the winner before the loser adopted the winner's claim. When
     *                   it did, the winner keeps the claim its own displaced and judges the later completion with it.
     *                   When it did not, no replica holds the loser's claim any more: the loser offers it again, and
     *                   its first offers are lost too (rule {@code drop-loser-claims}, eight periods), so the one that
     *                   arrives is a retry on the evidence of a replica that still offers the entry present.
     */
    private void lateCompletionByAntiEntropy(String name, long seed, boolean claimHeard) throws Exception {
        try (SpaceFleet fleet = fleet(name)) {
            SpaceFleet.Member a = fleet.get("a");
            SpaceFleet.Member b = fleet.get("b");
            FleetInvariants oracle = FleetInvariants.baseline(fleet.probe(Set.of(), Template.of(FindingEntry.class)));
            Script.RuleBuilder rules = Script.of("b").seed(seed)
                    .rule("hold-claim")
                    .when(Trigger.frame(Envelope.Kind.RUMOR, Envelope.Kind.PULL_RESP, Envelope.Kind.BATCH).to("a")
                            .item(ItemKind.CLAIM).attr("holderPeer", b.id().toString()))
                    .act(Action.hold()).always()
                    .until(Trigger.frame().from("a").item(ItemKind.COMPLETION))
                    .ceiling(Ceiling.periods(40))
                    .rule("drop-completion-rumors")
                    .when(Trigger.frame(Envelope.Kind.RUMOR).to("b").item(ItemKind.COMPLETION))
                    .act(Action.drop()).always()
                    .rule("completion-into-b")
                    .when(Trigger.frame(Envelope.Kind.PULL_RESP).to("b").item(ItemKind.COMPLETION)
                            .attr("issuerPeer", fleet.get("w").id().toString()))
                    .act(Action.count());
            if (!claimHeard) {
                rules = rules.rule("drop-loser-claims")
                        .when(Trigger.frame(Envelope.Kind.RUMOR, Envelope.Kind.PULL_RESP, Envelope.Kind.BATCH).to("b")
                                .item(ItemKind.CLAIM).attr("holderPeer", a.id().toString()))
                        .act(Action.drop()).always()
                        .until(Trigger.afterPeriods(8));
            }
            ScriptRunner runner = arm(fleet, rules.build());

            // b claims first: its claim wins the lattice; a never hears of it inside its window
            CompletableFuture<VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>>> atB = fleet.clock.start(
                    () -> b.space().take(task(), MINUTES_10, Duration.ZERO));
            fleet.clock.runDue();
            awaitFired(runner, "hold-claim", "spoiler.late-completion.held");
            fleet.clock.advance(Duration.ofMillis(10));
            Optional<TakenEntry<TaskEntry>> displaced = fleet.call(
                    () -> a.space().take(task(), MINUTES_10, Duration.ZERO)).value();
            assertThat(displaced).as("a judged itself the winner").isPresent();
            Optional<TakenEntry<TaskEntry>> winning = atB.join().value();
            assertThat(winning).as("b holds the lattice's winning claim").isPresent();
            EntryId entryId = winning.get().entryId();

            if (claimHeard) {
                // §10.33 (seam C11): with three members a random round of b's or a's lands on the other within the
                // targeted bound about three times in four, so the bound alone proved nothing (the cut of the targeted
                // round passed). Every member's periodic round is pushed out to 60 s (a raise applies at once), so
                // within TARGETED_PULL_TICKS only the winner's targeted round toward the displaced holder can pull.
                for (SpaceFleet.Member m : fleet.members.values()) {
                    m.runtime().gossip().antiEntropyPeriod(Duration.ofSeconds(60));
                }
            }
            // a completes; the frame carrying its completion releases b's claim into a, which adopts it at once
            a.space().complete(displaced.get(), new FindingEntry("contested", "done by a"), MINUTES_30);
            // a is the completion's one source, and anti-entropy pulls: it reaches b on b's first round with a
            // (HOLDER_PULL_TICKS), a random draw, measured on its own so the bound below is the product's alone
            Measurement pulled = SimWait.ticks("spoiler.late-completion.pulled-from-holder",
                    claimHeard ? TARGETED_PULL_TICKS : HOLDER_PULL_TICKS,
                    fleet::tickSlowly, () -> runner.report().fired("completion-into-b") > 0,
                    Wait.with().diagnostics(() -> "firings " + runner.report().signatures()));
            // from that answer, b pairs the completion with a's displaced claim: at once when the claim was heard
            // first, else when a's offer of it lands (REPAIR_TICKS covers the backoff past drop-loser-claims)
            Measurement learned = SimWait.ticks("spoiler.late-completion.winner-learns-after-pull", REPAIR_TICKS,
                    fleet::tickSlowly, () -> b.space().knowsDisplacedCompletion(entryId),
                    Wait.with().diagnostics(() -> "firings " + runner.report().signatures() + "; a offered its claim "
                            + a.space().displacedReoffers() + " times"));
            if (claimHeard) {
                assertThat(learned.value()).as("the claim was heard first, so the pull itself pairs it, within a tick")
                        .isLessThanOrEqualTo(1);
                assertThat(b.space().displacedPullRounds()).as("owner decision 4: the winner opened targeted rounds"
                        + " toward the displaced holder").isPositive();
            }
            assertThat(a.space().currentClaim(entryId).map(TakeClaim::holder)).as("a adopted b's claim")
                    .isEqualTo(b.space().currentClaim(entryId).map(TakeClaim::holder));
            // rule 2: the winner that learned of the displaced holder's completion before its own stands down
            assertThatThrownBy(() -> b.space().complete(winning.get(), new FindingEntry("contested", "done by b"),
                    MINUTES_30)).isInstanceOf(LeaseExpiredException.class).hasMessageContaining("completed first");
            assertThat(b.space().outbidAcknowledged()).isEqualTo(1);
            if (!claimHeard) {
                assertThat(runner.report().fired("drop-loser-claims")).as("a's first offers were lost").isPositive();
                assertThat(a.space().displacedReoffers()).as("so a offered its claim again").isGreaterThan(1);
            }

            converge(fleet, "spoiler.late-completion.repair", List.of(new FindingEntry("contested", "done by a")));
            for (SpaceFleet.Member m : fleet.members.values()) {
                assertThat(m.space().duplicateCompletions()).as("nothing done twice at " + m.name()).isZero();
            }
            long offers = a.space().displacedReoffers();
            fleet.tickAll(10);
            assertThat(a.space().displacedReoffers()).as("no replica offers the entry present any more, so a stops")
                    .isEqualTo(offers);
            ScriptRunner.Report report = runner.finish();
            System.out.printf("store race at the space (%s): b pulled from a after %.0f ticks and learned %.0f ticks"
                    + " later; a offered its claim %d times; firing sequence %s%n", name, pulled.value(), learned.value(),
                    offers, report.signatures());
            oracle.assertHolds(report.expectedDrops(), true);
        }
    }

    @Test
    @Timeout(120)
    void aDisplacedClaimAndItsCompletionInDifferentFramesStillMakeTheWinnerAcknowledge() throws Exception {
        lateCompletionByAntiEntropy("LateCompletionPaired", 4713, true);
    }

    @Test
    @Timeout(120)
    void aDisplacedHolderOffersItsClaimAgainUntilTheWinnerCanAcknowledge() throws Exception {
        lateCompletionByAntiEntropy("LateCompletionReoffered", 4714, false);
    }

    /**
     * Seam st01 (srad/agentspaces-event-loop.md §10.33, §10.37; SPEC §7.4 rule two): the displaced holder's completion
     * reaches the winner first, under a claim that does not authenticate it, and is kept; the displaced claim comes
     * later, alone, in the loser's re-offer, a rumor. The rumor's own handler pairs the two at once
     * ({@code judgeDisplaced}). Once the completion is in, no anti-entropy answer reaches the winner (periodic rounds
     * 60 s out, every PULL_RESP into it dropped), so nothing else can pair them: before §10.37 the store race
     * scenarios passed without the rumor path, because the winner's targeted round toward the displaced holder (owner
     * decision 4), or any anti-entropy answer carrying a claim of the entry, paired them a moment later. The rumor path
     * is the one that needs no answer from the displaced holder (a dial-only holder may give none).
     */
    @Test
    @Timeout(120)
    void aDisplacedClaimArrivingAloneInARumorIsPairedWithTheKeptCompletionAtOnce() throws Exception {
        try (SpaceFleet fleet = fleet("LateClaimByRumor")) {
            SpaceFleet.Member a = fleet.get("a");
            SpaceFleet.Member b = fleet.get("b");
            ScriptRunner runner = arm(fleet, Script.of("b").seed(4715)
                    .rule("hold-claim")
                    .when(Trigger.frame(Envelope.Kind.RUMOR, Envelope.Kind.PULL_RESP, Envelope.Kind.BATCH).to("a")
                            .item(ItemKind.CLAIM).attr("holderPeer", b.id().toString()))
                    .act(Action.hold()).always()
                    .until(Trigger.frame().from("a").item(ItemKind.COMPLETION))
                    .ceiling(Ceiling.periods(40))
                    .rule("drop-completion-rumors")
                    .when(Trigger.frame(Envelope.Kind.RUMOR).to("b").item(ItemKind.COMPLETION))
                    .act(Action.drop()).always()
                    .rule("completion-into-b")
                    .when(Trigger.frame(Envelope.Kind.PULL_RESP).to("b").item(ItemKind.COMPLETION)
                            .attr("issuerPeer", fleet.get("w").id().toString()))
                    .act(Action.count())
                    .rule("drop-loser-claims")
                    .when(Trigger.frame(Envelope.Kind.RUMOR, Envelope.Kind.PULL_RESP, Envelope.Kind.BATCH).to("b")
                            .item(ItemKind.CLAIM).attr("holderPeer", a.id().toString()))
                    .act(Action.drop()).always()
                    .until(Trigger.afterPeriods(8))
                    .build());
            CompletableFuture<VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>>> atB = fleet.clock.start(
                    () -> b.space().take(task(), MINUTES_10, Duration.ZERO));
            fleet.clock.runDue();
            awaitFired(runner, "hold-claim", "spoiler.late-claim-rumor.held");
            fleet.clock.advance(Duration.ofMillis(10));
            Optional<TakenEntry<TaskEntry>> displaced = fleet.call(
                    () -> a.space().take(task(), MINUTES_10, Duration.ZERO)).value();
            assertThat(displaced).as("a judged itself the winner").isPresent();
            Optional<TakenEntry<TaskEntry>> winning = atB.join().value();
            assertThat(winning).as("b holds the lattice's winning claim").isPresent();
            EntryId entryId = winning.get().entryId();
            a.space().complete(displaced.get(), new FindingEntry("contested", "done by a"), MINUTES_30);
            SimWait.ticks("spoiler.late-claim-rumor.pulled", HOLDER_PULL_TICKS, fleet::tickSlowly,
                    () -> runner.report().fired("completion-into-b") > 0,
                    Wait.with().diagnostics(() -> "firings " + runner.report().signatures()));
            // From here no anti-entropy answer reaches b, so none can pair them: not a periodic round (60 s out),
            // not the winner's targeted round toward a (owner decision 4), not a pull a probe's digests start.
            for (SpaceFleet.Member m : fleet.members.values()) {
                m.runtime().gossip().antiEntropyPeriod(Duration.ofSeconds(60));
            }
            fleet.own(fleet.network.filter(null, "b", frame -> "PULL_RESP".equals(frame.kind())
                    ? ai.badmonkey.agentspaces.test.LinkFilter.Verdict.drop()
                    : ai.badmonkey.agentspaces.test.LinkFilter.Verdict.pass()));
            SimWait.ticks("spoiler.late-claim-rumor.loser-claims-pass", 40, fleet::tickSlowly,
                    () -> runner.report().signatures().stream().anyMatch(sig -> sig.startsWith("drop-loser-claims release")),
                    Wait.with().diagnostics(() -> "firings " + runner.report().signatures()));
            // the loser's next offer, a claim-only rumor, is due once its back-off has passed
            Measurement learned = SimWait.ticks("spoiler.late-claim-rumor.paired", 20, () -> {
                a.space().staleOfferForTest(entryId);
                fleet.tickSlowly();
            }, () -> b.space().knowsDisplacedCompletion(entryId),
                    Wait.with().diagnostics(() -> "a offered its claim " + a.space().displacedReoffers() + " times"));
            System.out.printf("st01: b paired the rumored claim with the kept completion %.0f ticks after the drop"
                    + " rule ended; a offered %d times%n", learned.value(), a.space().displacedReoffers());
            assertThatThrownBy(() -> b.space().complete(winning.get(), new FindingEntry("contested", "done by b"),
                    MINUTES_30)).isInstanceOf(LeaseExpiredException.class).hasMessageContaining("completed first");
            runner.finish();
        }
    }

    /**
     * Seam st02s (srad/agentspaces-event-loop.md §10.33, §10.37; SPEC §7.4): a displaced holder offers its claim
     * again when a replica offers the entry present, the evidence that it has not heard of the completion. A rumor is
     * such an offer: the writer's renewal of the entry reaches the holder as a rumor of the entry, present, and the
     * next offer goes at once. With every anti-entropy round 60 s out, nothing else can bring it.
     */
    @Test
    @Timeout(120)
    void aRumorOfferingTheEntryPresentBringsTheDisplacedHoldersNextOffer() throws Exception {
        try (SpaceFleet fleet = new SpaceFleet("StaleOfferedRumor", ConflictStrategyType.LEASE_RACE, "work")) {
            fleet.member("w", 1, b -> b.settleWindow(SETTLE));
            fleet.member("a", 2, b -> b.settleWindow(SETTLE), "w");
            fleet.tickAll(4);
            ai.badmonkey.agentspaces.api.space.EntryHandle handle = fleet.get("w").space()
                    .write(new TaskEntry("renewed", 1), MINUTES_30);
            fleet.tickAll(2);
            SpaceFleet.Member a = fleet.get("a");
            Optional<TakenEntry<TaskEntry>> taken = fleet.call(
                    () -> a.space().take(task(), MINUTES_10, Duration.ZERO)).value();
            assertThat(taken).isPresent();
            EntryId entryId = taken.get().entryId();
            for (SpaceFleet.Member m : fleet.members.values()) {
                m.runtime().gossip().antiEntropyPeriod(Duration.ofSeconds(60));
            }
            a.space().noteDisplacedOwnForTest(entryId);
            assertThat(a.space().displacedReoffers()).as("the first offer at once").isEqualTo(1);
            fleet.tickAll(5); // past the 1 s back-off
            assertThat(a.space().displacedReoffers()).as("no replica offered the entry present: no offer")
                    .isEqualTo(1);
            handle.renew(Duration.ofHours(1)); // the writer's renewal: a rumor of the entry, present
            Measurement offered = SimWait.ticks("spoiler.stale-offered.reoffer", 2, fleet::tickAll,
                    () -> a.space().displacedReoffers() == 2,
                    Wait.with().diagnostics(() -> "offers " + a.space().displacedReoffers()));
            System.out.printf("st02s: the rumored present state brought the next offer in %.0f ticks%n",
                    offered.value());
        }
    }

    /**
     * C04 of EL-A-03 (srad/agentspaces-event-loop.md §10.37): the store race of 2026-11-22 while the member TTL falls.
     * Twelve members idle ten minutes, so the cycle-bound member TTL rises above its 45 s floor; then nine fall silent
     * at once, and the TTL falls back while the late-claim race between {@code a} and {@code b} runs (the claim heard
     * first, so the winner's targeted rounds toward the displaced holder pull its completion). PREVENTED: none of the
     * three survivors drops another at any tick, the winner acknowledges the displaced holder's completion and stands
     * down, nothing is done twice, and the nine leave every survivor's view.
     */
    @Test
    @Timeout(600)
    void c04AStoreRaceWhileTheMemberTtlFallsIsResolvedAndDropsNoSurvivor() throws Exception {
        try (SpaceFleet fleet = new SpaceFleet("C04", ConflictStrategyType.LEASE_RACE, "work")) {
            fleet.member("w", 1, b -> b.settleWindow(SETTLE));
            fleet.member("a", 2, b -> b.settleWindow(SETTLE), "w");
            fleet.member("b", 3, b -> b.settleWindow(SETTLE), "w");
            for (int i = 0; i < 9; i++) {
                fleet.member("x" + i, 10 + i, b -> b.settleWindow(SETTLE), "w");
            }
            SimWait.ticks("spoiler.c04.formed", 400, fleet::tickAll, () -> fleet.members.values().stream()
                    .allMatch(m -> m.runtime().membership().size() == 11), Wait.with());
            fleet.tickAll(2_000); // ten quiet minutes
            SpaceFleet.Member a = fleet.get("a");
            SpaceFleet.Member b = fleet.get("b");
            List<SpaceFleet.Member> survivors = List.of(fleet.get("w"), a, b);
            long ttlHigh = b.runtime().membership().memberTtlMillis();
            assertThat(ttlHigh).as("the quiet twelve raised the member TTL above its floor").isGreaterThan(45_000L);
            fleet.get("w").space().write(new TaskEntry("contested", 1), MINUTES_30);
            fleet.tickAll(1);
            int[] lost = {0};
            Runnable tick = () -> {
                fleet.tickSlowly();
                for (SpaceFleet.Member m : survivors) {
                    for (SpaceFleet.Member other : survivors) {
                        if (other != m && m.runtime().membership().member(other.id()).isEmpty()) {
                            lost[0]++;
                        }
                    }
                }
            };
            for (int i = 0; i < 9; i++) { // nine fall silent at once
                fleet.members.remove("x" + i).node().close();
            }
            ScriptRunner runner = arm(fleet, Script.of("b").seed(4716)
                    .rule("hold-claim")
                    .when(Trigger.frame(Envelope.Kind.RUMOR, Envelope.Kind.PULL_RESP, Envelope.Kind.BATCH).to("a")
                            .item(ItemKind.CLAIM).attr("holderPeer", b.id().toString()))
                    .act(Action.hold()).always()
                    .until(Trigger.frame().from("a").item(ItemKind.COMPLETION))
                    .ceiling(Ceiling.periods(40))
                    .rule("drop-completion-rumors")
                    .when(Trigger.frame(Envelope.Kind.RUMOR).to("b").item(ItemKind.COMPLETION))
                    .act(Action.drop()).always()
                    .build());
            CompletableFuture<VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>>> atB = fleet.clock.start(
                    () -> b.space().take(task(), MINUTES_10, Duration.ZERO));
            fleet.clock.runDue();
            awaitFired(runner, "hold-claim", "spoiler.c04.held");
            fleet.clock.advance(Duration.ofMillis(10));
            Optional<TakenEntry<TaskEntry>> displaced = fleet.call(
                    () -> a.space().take(task(), MINUTES_10, Duration.ZERO)).value();
            assertThat(displaced).as("a judged itself the winner").isPresent();
            Optional<TakenEntry<TaskEntry>> winning = atB.join().value();
            assertThat(winning).as("b holds the lattice's winning claim").isPresent();
            EntryId entryId = winning.get().entryId();
            a.space().complete(displaced.get(), new FindingEntry("contested", "done by a"), MINUTES_30);
            Measurement learned = SimWait.ticks("spoiler.c04.winner-learns", HOLDER_PULL_TICKS, tick,
                    () -> b.space().knowsDisplacedCompletion(entryId),
                    Wait.with().diagnostics(() -> "firings " + runner.report().signatures()));
            assertThatThrownBy(() -> b.space().complete(winning.get(), new FindingEntry("contested", "done by b"),
                    MINUTES_30)).isInstanceOf(LeaseExpiredException.class).hasMessageContaining("completed first");
            Measurement fell = SimWait.ticks("spoiler.c04.ttl-fell", 1_200, tick,
                    () -> survivors.stream().allMatch(m -> m.runtime().membership().size() == 2
                            && m.runtime().membership().memberTtlMillis() < ttlHigh),
                    Wait.with().diagnostics(() -> survivors.stream().map(m -> m.name() + ": view "
                            + m.runtime().membership().size() + ", TTL " + m.runtime().membership().memberTtlMillis())
                            .toList().toString()));
            for (int i = 0; i < 200; i++) { // a minute more at the fallen TTL
                tick.run();
            }
            runner.finish();
            System.out.printf("C04: TTL %d ms before the fall, %s after; b learned in %.0f ticks; the nine left and"
                    + " the TTL fell within %.0f ticks; survivors lost one another %d times%n", ttlHigh,
                    survivors.stream().map(m -> m.runtime().membership().memberTtlMillis()).toList(), learned.value(),
                    fell.value(), lost[0]);
            assertThat(lost[0]).as("PREVENTED: no survivor dropped another during the fall").isZero();
            for (SpaceFleet.Member m : survivors) {
                assertThat(m.space().duplicateCompletions()).as("nothing done twice at " + m.name()).isZero();
            }
            assertThat(b.space().outbidAcknowledged()).isEqualTo(1);
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

            VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>> takeA = fleet.call(
                    () -> a.space().take(task(), MINUTES_10, Duration.ZERO));
            fleet.clock.advance(Duration.ofMillis(10));
            VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>> takeB = fleet.call(
                    () -> b.space().take(task(), MINUTES_10, Duration.ZERO));
            Optional<TakenEntry<TaskEntry>> atA = takeA.value();
            Optional<TakenEntry<TaskEntry>> atB = takeB.value();
            assertThat(atA).as("a judged alone").isPresent();
            assertThat(atB).as("b judged alone: two holders past the window").isPresent();
            assertThat(List.of(takeA.took(), takeB.took())).as("each judged at exactly its window (decision 12)")
                    .containsOnly(SETTLE);
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
            VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>> takeB = fleet.call(
                    () -> b.space().take(task(), MINUTES_10, Duration.ZERO));
            Optional<TakenEntry<TaskEntry>> atB = takeB.value();
            fleet.clock.advance(Duration.ofMillis(10));
            assertThat(atB).isPresent();
            assertThat(takeB.took()).isEqualTo(SETTLE);
            assertThat(fleet.call(() -> a.space().take(task(), MINUTES_10, Duration.ZERO)).value())
                    .as("the claim arrived in time").isEmpty();
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
