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

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceReduce;
import ai.badmonkey.agentspaces.agent.join.JoinTicket;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftLog;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.measure.Measurement;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.SimWait;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SC-B1 and SC-B4 (srad/spoiler-peers.md §3.8 B), the core forms, on an ORDERED {@code @SpaceReduce} over a fleet on
 * {@link SimNetwork}: two reducers, {@code a} and {@code b}, and a writer {@code c}, the three voters of the log.
 *
 * <ul>
 *   <li>SC-B1, the second reducer: every frame into {@code a} that carries {@code b}'s ticket or {@code b}'s claim is
 *   held, and every frame into {@code b} that carries {@code a}'s, until the first fold's completion is on the wire.
 *   Each reducer therefore writes its own ticket for the key without seeing the other's, and each claims it. Before,
 *   this order came from load (20 of 20 fleet runs, at a rate); here it is the script's. PREVENT under ORDERED: the
 *   log's commit order decides, so every payment folds once fleet-wide.</li>
 *   <li>SC-B4, the duplicate ticket: under ORDERED the second ticket is kept and completed, not withdrawn, so no
 *   committed claim waits on a ticket that is neither live nor completed. The stall it caused waited out
 *   {@code KeyTickets.PENDING_CLAIM_MAX_AGE} (30 s of wall time, 4 of 10 runs); here the drain is measured, and it
 *   must end before that age could have passed once (a c-wall bound: "drained, not timed out"; no counter exists).</li>
 *   <li>The negative control: the same fleet and workload with an empty script.</li>
 * </ul>
 *
 * <p>The reducers' takes run on their binders' threads and the log's attempt window is a wall-clock wait (§3.4.4), so
 * the order is asserted and the durations measured.
 */
class SpoilerOrderedReduceTest {

    /** {@code KeyTickets.PENDING_CLAIM_MAX_AGE}, private there: a stalled key waited this long on the wall clock. */
    private static final Duration PENDING_CLAIM_MAX_AGE = Duration.ofSeconds(30);
    private static final int PAYMENTS = 6;

    public record Payment(String account, long cents) {
    }

    public record Balance(String account, long cents, int payments) {
    }

    @AgentSpec(name = "teller", description = "Keeps a running balance per account, once fleet-wide", goals = {"balance"})
    public static class Teller {
        final AtomicInteger steps = new AtomicInteger();

        @SpaceReduce(space = "ledger", key = "account", mode = SpaceReduce.Mode.ORDERED, name = "teller",
                lease = "30s", pollTimeout = "200ms", accumulatorLease = "1h")
        public Balance apply(Balance balance, Payment payment) {
            steps.incrementAndGet();
            return new Balance(payment.account(), (balance == null ? 0 : balance.cents()) + payment.cents(),
                    (balance == null ? 0 : balance.payments()) + 1);
        }
    }

    private static boolean leads(SpaceFleet.Member m) {
        return m.capabilities().providersOf(RaftLog.TYPE).stream()
                .anyMatch(ad -> "leader".equals(ad.parameters().get("role")) && ad.issuer().equals(m.id()));
    }

    private static boolean settled(SpaceFleet fleet, long cents) {
        for (SpaceFleet.Member m : fleet.members.values()) {
            List<Balance> balances = m.space().readAll(Template.of(Balance.class), 20);
            if (balances.size() != 1 || balances.get(0).cents() != cents || balances.get(0).payments() != PAYMENTS
                    || !m.space().readAll(Template.of(Payment.class), 20).isEmpty()
                    || !m.space().readAll(Template.of(JoinTicket.class), 20).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private static String state(SpaceFleet fleet) {
        StringBuilder out = new StringBuilder();
        for (SpaceFleet.Member m : fleet.members.values()) {
            out.append("\n  ").append(m.name()).append(": balances ")
                    .append(m.space().readAll(Template.of(Balance.class), 20)).append(", payments left ")
                    .append(m.space().readAll(Template.of(Payment.class), 20).size()).append(", tickets ")
                    .append(m.space().readAll(Template.of(JoinTicket.class), 20).size());
        }
        return out.toString();
    }

    /** Runs the workload under a script and returns the tellers' fold counts and the drain's measurement. */
    private record Run(List<Teller> tellers, Measurement drain, ScriptRunner.Report report) {
    }

    private Run run(String name, boolean spoil) throws Exception {
        try (SpaceFleet fleet = new SpaceFleet(name, ConflictStrategyType.LEASE_RACE, "ledger")) {
            fleet.agentName = "teller";
            SpaceFleet.Member a = fleet.member("a", 1, b -> b.settleWindow(Duration.ZERO));
            SpaceFleet.Member b = fleet.member("b", 2, x -> x.settleWindow(Duration.ZERO), "a");
            SpaceFleet.Member c = fleet.member("c", 3, x -> x.settleWindow(Duration.ZERO), "a");
            fleet.tickAll(4);
            int seed = 100;
            List<Teller> tellers = new ArrayList<>();
            for (SpaceFleet.Member m : List.of(a, b, c)) {
                OrderedTakes ordered = OrderedTakes.over(new CapabilityPipes(m.runtime(), fleet.codec), fleet.codec,
                        m.identity(), "teller", fleet.ids(), fleet.clock, seed++, m.space());
                m.group().ordered("ledger", ordered);
            }
            SimWait.ticks("spoiler.ordered-reduce.leader", 60, fleet::tickAll,
                    () -> fleet.members.values().stream().filter(SpoilerOrderedReduceTest::leads).count() == 1, Wait.with());
            FleetInvariants oracle = FleetInvariants.baseline(fleet.probe(Set.of(), Template.of(Balance.class)));
            Script script = !spoil ? Script.honest("b") : Script.of("b").seed(4720)
                    .rule("hold-b-toward-a")
                    .when(Trigger.frame(Envelope.Kind.RUMOR, Envelope.Kind.PULL_RESP, Envelope.Kind.BATCH).to("a")
                            .item(ItemKind.STATE).attr("issuerPeer", b.id().toString()))
                    .act(Action.hold()).always()
                    .until(Trigger.frame().item(ItemKind.COMPLETION))
                    .ceiling(Ceiling.periods(100))
                    .rule("hold-a-toward-b")
                    .when(Trigger.frame(Envelope.Kind.RUMOR, Envelope.Kind.PULL_RESP, Envelope.Kind.BATCH).to("b")
                            .item(ItemKind.STATE).attr("issuerPeer", a.id().toString()))
                    .act(Action.hold()).always()
                    .until(Trigger.frame().item(ItemKind.COMPLETION))
                    .ceiling(Ceiling.periods(100))
                    .build();
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, SpaceFleet.PERIOD,
                    PeerNode.ChannelAuth.SIGNED, script);
            fleet.afterTick.add(runner::tick);
            for (SpaceFleet.Member m : List.of(a, b)) {
                Teller teller = new Teller();
                fleet.own(m.group().bind(teller));
                tellers.add(teller);
            }
            long cents = 0;
            for (int i = 1; i <= PAYMENTS; i++) {
                c.space().write(new Payment("acct-0", i), Lease.of(Duration.ofMinutes(10)));
                cents += i;
            }
            long total = cents;
            // A wall ceiling at the stall's own length: a key stalled behind an unresolvable claim drains only after
            // PENDING_CLAIM_MAX_AGE, so a drain inside it is a drain without the stall (fm: (c-wall), kept hard
            // because no counter of dropped claims exists).
            Measurement drain = SimWait.mixed("spoiler.ordered-reduce.drain", Integer.MAX_VALUE, PENDING_CLAIM_MAX_AGE,
                    () -> {
                        fleet.tickAll();
                        sleep(20);
                    }, fleet.clock::millis, () -> settled(fleet, total),
                    Wait.with().params(Params.of("script", spoil ? "second-reducer" : "honest"))
                            .diagnostics(() -> state(fleet)));
            ScriptRunner.Report report = runner.finish();
            oracle.assertHolds(report.expectedDrops(), true);
            return new Run(tellers, drain, report);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Test
    @Timeout(180)
    void twoOrderedReducersThatCannotSeeEachOthersTicketsFoldEveryPaymentOnce() throws Exception {
        Run run = run("OrderedReduce", true);
        System.out.printf("SC-B1/B4 ORDERED: folds %s, drained in %.0f ticks (%s); firing sequence %s%n",
                run.tellers().stream().map(t -> t.steps.get()).toList(), run.drain().value(), run.drain(),
                run.report().signatures());
        assertThat(run.report().fired("hold-b-toward-a") + run.report().fired("hold-a-toward-b"))
                .as("the reducers' tickets were held from each other").isPositive();
        assertThat(run.tellers().stream().mapToInt(t -> t.steps.get()).sum())
                .as("every payment folded exactly once across the fleet").isEqualTo(PAYMENTS);
    }

    @Test
    @Timeout(180)
    void theNegativeControlFoldsEveryPaymentOnce() throws Exception {
        Run run = run("OrderedReduceControl", false);
        assertThat(run.report().firings()).isEmpty();
        assertThat(run.tellers().stream().mapToInt(t -> t.steps.get()).sum()).isEqualTo(PAYMENTS);
    }
}
