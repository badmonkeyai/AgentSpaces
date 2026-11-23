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
package ai.badmonkey.agentspaces.examples.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.OnEstimate;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.join.JoinTicket;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.examples.workflow.Claims.Claim;
import ai.badmonkey.agentspaces.examples.workflow.Claims.ClaimAssessment;
import ai.badmonkey.agentspaces.examples.workflow.Claims.ClaimClosed;
import ai.badmonkey.agentspaces.examples.workflow.Claims.ClaimantExposure;
import ai.badmonkey.agentspaces.agent.reduce.Reductions;
import ai.badmonkey.agentspaces.examples.workflow.Claims.Payment;
import ai.badmonkey.agentspaces.examples.workflow.Claims.PaymentOrder;
import ai.badmonkey.agentspaces.examples.workflow.Claims.PricedClaim;
import ai.badmonkey.agentspaces.examples.workflow.Claims.QuoteTask;
import ai.badmonkey.agentspaces.examples.workflow.Claims.RegisteredClaim;
import ai.badmonkey.agentspaces.examples.workflow.Claims.RepairEstimate;
import ai.badmonkey.agentspaces.examples.workflow.Claims.ReserveReport;
import ai.badmonkey.agentspaces.examples.workflow.Claims.SeniorReview;
import ai.badmonkey.agentspaces.examples.workflow.ClaimsFlow.Peer;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Wait;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The whole flow over real TCP: four claims in, four cases closed, and the
 * properties each stage promises hold at every replica.
 */
class ClaimsFlowTest {

    /** The join mode of the flow under way, a parameter of each measured wait so the two modes keep apart. */
    private Params mode = Params.none();

    private static int freePort() throws IOException {
        // below the ephemeral range, so no other process takes it before the test binds it (parallel forks)
        return ai.badmonkey.agentspaces.test.Ports.free();
    }

    @Test
    @Timeout(180)
    void fourClaimsFlowThroughEveryStageAndCloseOnce() throws Exception {
        runTheFlow(false, false);
    }

    /**
     * The demo's supervisor, bound from the start with its forty settled rounds, outlasts the plateau between the
     * denials and the paid claim and reports the full average once. It waits those forty seconds out in real time, so
     * it runs in the soak profile ({@code -Psoak}, at phase gates); the default build binds a short-settle supervisor
     * once the estimate has converged and pins the forty rounds in {@link #theDemoSupervisorKeepsItsFortyRoundSettle}.
     */
    @Test
    @Tag("soak")
    @Timeout(180)
    void theDemoSupervisorBoundFromTheStartReportsTheFullAverageOnce() throws Exception {
        runTheFlow(false, true);
    }

    /** The demo's pacing stays forty rounds of about a second: what the soak run above relies on. */
    @Test
    void theDemoSupervisorKeepsItsFortyRoundSettle() throws Exception {
        OnEstimate on = Settlement.Supervisor.class.getMethod("report", PushSumAggregate.Estimate.class)
                .getAnnotation(OnEstimate.class);
        assertThat(on.settle()).isEqualTo("PT40S");
        assertThat(on.tolerance()).isEqualTo(0.01);
    }

    /**
     * The join in ORDERED mode: two joiners on two peers assemble each claim
     * once, through tickets the intake log arbitrates (issue #16 §14.12).
     */
    @Test
    @Timeout(180)
    void twoOrderedJoinersStillAssembleEachClaimOnce() throws Exception {
        runTheFlow(true, false);
    }

    /**
     * @param demoSupervisor bind the demo's supervisor (forty settled rounds) with the staff; otherwise bind
     *                       {@link QuickSupervisor} once stage 8's estimate has converged, so the report follows a
     *                       few rounds later instead of forty
     */
    private void runTheFlow(boolean orderedJoin, boolean demoSupervisor) throws Exception {
        mode = Params.of("join", orderedJoin ? "ordered" : "leased");
        List<PeerIdentity> ids = List.of(PeerIdentity.generate(), PeerIdentity.generate(),
                PeerIdentity.generate());
        List<PeerId> members = ids.stream().map(PeerIdentity::peerId).toList();
        int seedPort = freePort();
        Peer a = ClaimsFlow.startPeer(ids.get(0), "peer-a", seedPort, 0, members, 11);
        Peer b = ClaimsFlow.startPeer(ids.get(1), "peer-b", freePort(), seedPort, members, 12);
        Peer c = ClaimsFlow.startPeer(ids.get(2), "peer-c", freePort(), seedPort, members, 13);
        List<Peer> fleet = List.of(a, b, c);
        AtomicInteger crashes = new AtomicInteger();
        AtomicBoolean armed = new AtomicBoolean(true);
        // Crash the first registration any registrar attempts, and count only the
        // attempt that actually crashed (two registrars race for the same flag).
        Intake.Hook counted = (worker, claim) -> {
            if (armed.compareAndSet(true, false)) {
                crashes.incrementAndGet();
                throw new IllegalStateException("simulated crash in " + worker);
            }
        };
        List<AutoCloseable> staff = new java.util.ArrayList<>(!demoSupervisor
                ? ClaimsFlow.staffWithoutSupervisor(a, b, c, counted, orderedJoin)
                : orderedJoin ? ClaimsFlow.staffWithOrderedJoin(a, b, c, counted) : ClaimsFlow.staff(a, b, c, counted));
        // One witness per peer, beside each peer's reserve sensor and reached the same way, so a
        // stage 8 failure says which replica's reaction never saw which priced claim.
        Map<String, ReserveWitness> witnesses = new java.util.LinkedHashMap<>();
        for (Peer peer : fleet) {
            ReserveWitness witness = new ReserveWitness();
            witnesses.put(peer.name(), witness);
            staff.add(peer.group().bind(witness));
        }
        try {
            // A log leader must exist before a payment can be taken; wait for the election.
            assertThat(await("workflow.claims.leader", () -> fleet.stream().anyMatch(p -> p.ordered().raft().isLeader())
                    ? Optional.of(true) : Optional.<Boolean>empty(), Duration.ofSeconds(40)))
                    .as("a payments log leader is elected").isPresent();

            for (Claim claim : ClaimsFlow.CLAIMS) {
                a.intake().write(claim, Lease.of(Duration.ofHours(1)));
            }

            // Stage 9: every case closes, once, with the expected outcome.
            List<ClaimClosed> closed = await("workflow.claims.closed", () -> {
                List<ClaimClosed> all = c.ledger().readAll(Template.of(ClaimClosed.class), 10);
                return all.size() >= 5 ? Optional.of(all) : Optional.<List<ClaimClosed>>empty();
            }, Duration.ofSeconds(90)).orElseThrow();
            assertThat(closed).hasSize(5);
            Map<String, String> outcomes = closed.stream()
                    .collect(java.util.stream.Collectors.toMap(ClaimClosed::claimId, ClaimClosed::outcome));
            assertThat(outcomes).containsExactlyInAnyOrderEntriesOf(Map.of(
                    "CLM-1", "PAID", "CLM-2", "DENIED", "CLM-3", "DENIED", "CLM-4", "PAID",
                    "CLM-5", "REJECTED"));

            // Stage 1: the crash happened once and every claim was still registered.
            assertThat(crashes.get()).as("the hook crashed exactly one registration").isEqualTo(1);
            assertThat(a.intake().readAll(Template.of(RegisteredClaim.class), 10))
                    .extracting(RegisteredClaim::claimId)
                    .containsExactlyInAnyOrder("CLM-1", "CLM-2", "CLM-3", "CLM-4");
            assertThat(a.intake().readAll(Template.of(Claim.class), 10))
                    .as("claims drained from intake").isEmpty();

            // Stage 3: one assembly per claim, whatever the join mode. The assessments
            // themselves are consumed by the auction take, so the evidence is
            // downstream: every claim priced, and no assessment left unpriced; in
            // ORDERED mode, no ticket left either. The auction awards a claim to one
            // adjuster only while every bid arrives within the settle window (SPEC
            // §7.4); beyond it two adjusters can each price a claim. The completion is
            // idempotent (the price is a pure function of the assessment), so every
            // priced claim of one claim agrees, and every priced claim past the first
            // is a duplicate the space detected and counted where the winning bid is held.
            Map<String, List<PricedClaim>> pricings = a.assessment().readAll(Template.of(PricedClaim.class), 20)
                    .stream().collect(java.util.stream.Collectors.groupingBy(PricedClaim::claimId));
            assertThat(pricings.keySet()).containsExactlyInAnyOrder("CLM-1", "CLM-2", "CLM-3", "CLM-4");
            pricings.forEach((claimId, all) -> assertThat(all).as("every pricing of " + claimId + " agrees")
                    .extracting(PricedClaim::reserve, PricedClaim::recommendation).containsOnly(
                            org.assertj.core.groups.Tuple.tuple(all.get(0).reserve(), all.get(0).recommendation())));
            long countFrom = System.nanoTime();
            assertThat(await("workflow.claims.duplicates-counted", () -> {
                long duplicates = fleet.stream().mapToLong(peer -> peer.assessment().duplicateCompletions()).sum();
                int extra = a.assessment().readAll(Template.of(PricedClaim.class), 20).size() - 4;
                return extra == duplicates ? Optional.of(extra) : Optional.<Integer>empty();
            }, Duration.ofSeconds(30))).as("every second pricing is a counted duplicate completion").isPresent();
            System.out.printf("duplicate pricings: %d (counts agreed after %d ms)%n",
                    a.assessment().readAll(Template.of(PricedClaim.class), 20).size() - 4,
                    java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - countFrom));
            Map<String, PricedClaim> priced = new java.util.HashMap<>();
            pricings.forEach((claimId, all) -> priced.put(claimId, all.get(0)));
            assertThat(a.assessment().readAll(Template.of(ClaimAssessment.class), 10))
                    .as("every assessment was taken and priced").isEmpty();
            if (orderedJoin) {
                for (Peer peer : fleet) {
                    assertThat(await("workflow.claims.tickets-drained", () -> peer.payments().readAll(Template.of(JoinTicket.class), 10)
                                    .isEmpty() ? Optional.of(true) : Optional.<Boolean>empty(),
                            Duration.ofSeconds(30))).as("tickets drained at " + peer.name()).isPresent();
                }
            }
            // The fire claim's assessment came through the ontology instance: the
            // damage estimate and severity were read from the DamageFinding's properties.
            assertThat(priced.get("CLM-4").reserve()).isEqualTo(25_000);   // 36,000 capped at the limit

            // Stage 4: the auction prices each claim once, by an adjuster. Routing to the
            // cheaper bidder (senior for the severe claim, junior for the routine one) is
            // best effort: a lower bid displaces a claim only inside its settle window, so
            // a bid delayed past it by lag loses, as on any real network. The lowest-bid
            // rule itself is pinned under controlled timing in AuctionClusterTest and
            // TakeClaimMergeTest; here a missed route is reported, not failed.
            Map<String, String> cheaper = Map.of("CLM-4", "senior-adjuster", "CLM-1", "junior-adjuster");
            for (PricedClaim p : priced.values()) {
                assertThat(p.pricedBy()).as(p.claimId() + " priced by an adjuster")
                        .isIn("senior-adjuster", "junior-adjuster");
            }
            cheaper.forEach((claimId, bidder) -> {
                if (!bidder.equals(priced.get(claimId).pricedBy())) {
                    System.out.println("auction: " + claimId + " went to " + priced.get(claimId).pricedBy()
                            + ", not the cheaper " + bidder + " (its bid missed the settle window)");
                }
            });
            assertThat(priced.get("CLM-2").recommendation()).isEqualTo(Pricing.DENY);
            assertThat(priced.get("CLM-3").recommendation()).isEqualTo(Pricing.DENY);

            // Stages 5 and 6: the panel split on the large claim and still approved it.
            Optional<VoteCapability.Decision> decision = a.vote().decision("claim:CLM-4");
            assertThat(decision).isPresent();
            assertThat(decision.get().winner()).isEqualTo(Pricing.APPROVE);
            assertThat(decision.get().tally()).containsEntry(Pricing.APPROVE, 2)
                    .containsEntry(Pricing.DENY, 1);

            // Stage 7 and 7a: exactly one payment per approved claim, folded into its
            // claimant's exposure exactly once, visible at every replica; the orders
            // and the payments drained everywhere (the exposure stage consumes each
            // payment as it folds it, after the ledger's reaction has seen it).
            for (Peer peer : fleet) {
                Map<String, ClaimantExposure> exposure = await("workflow.claims.exposure", () -> {
                    Map<String, ClaimantExposure> all = new java.util.HashMap<>();
                    for (String claimant : List.of("A. Okafor", "D. Varga")) {
                        Reductions.current(peer.payments(), ClaimantExposure.class, "exposure", claimant)
                                .ifPresent(e -> all.put(claimant, e.value()));
                    }
                    return all.size() == 2 ? Optional.of(all) : Optional.<Map<String, ClaimantExposure>>empty();
                }, Duration.ofSeconds(30)).orElseThrow();
                assertThat(exposure.get("A. Okafor")).as("exposure at " + peer.name())
                        .isEqualTo(new ClaimantExposure("A. Okafor", 720_000, 1));
                assertThat(exposure.get("D. Varga")).as("exposure at " + peer.name())
                        .isEqualTo(new ClaimantExposure("D. Varga", 2_500_000, 1));
                assertThat(await("workflow.claims.payments-drained", () -> peer.payments().readAll(Template.of(PaymentOrder.class), 10).isEmpty()
                                && peer.payments().readAll(Template.of(Payment.class), 10).isEmpty()
                                ? Optional.of(true) : Optional.<Boolean>empty(),
                        Duration.ofSeconds(30))).as("orders and payments drained at " + peer.name()).isPresent();
            }

            // Stage 3a: the fork wrote one request and three tasks per assessed claim, one
            // shop per peer took its own tasks, and the gather kept the lowest of exactly
            // three quotes; the east shop prices lowest.
            List<RepairEstimate> estimates = await("workflow.claims.estimates", () -> {
                List<RepairEstimate> all = c.ledger().readAll(Template.of(RepairEstimate.class), 10);
                return all.size() >= 4 ? Optional.of(all) : Optional.<List<RepairEstimate>>empty();
            }, Duration.ofSeconds(30)).orElseThrow();
            assertThat(estimates).extracting(RepairEstimate::claimId)
                    .containsExactlyInAnyOrder("CLM-1", "CLM-2", "CLM-3", "CLM-4");
            assertThat(estimates).allSatisfy(e -> {
                assertThat(e.considered()).isEqualTo(3);
                assertThat(e.shop()).isEqualTo("east");
            });
            assertThat(a.intake().readAll(Template.of(QuoteTask.class), 10)).as("every task was taken").isEmpty();
            // Stage 3b: only the severe claim reached the senior reviewer.
            assertThat(c.ledger().readAll(Template.of(SeniorReview.class), 10))
                    .extracting(SeniorReview::claimId).containsExactly("CLM-4");
            // Stage 6a: the panel decided in time, so no reminder escalated.
            assertThat(a.decisions().readAll(Template.of(VoteCapability.Proposal.class), 20))
                    .extracting(VoteCapability.Proposal::proposalId)
                    .noneMatch(id -> id.startsWith("escalate:"));

            // Stage 8: the fleet's average reserve converges on (7200 + 0 + 0 + 25000) / 4.
            // Measured over many runs, the estimate at peer-c is within 2% at most about 3 s after this
            // stage starts (usually under 1 s); 30 s means it is clearly broken, and the time it took is printed.
            double expected = (7_200 + 0 + 0 + 25_000) / 4.0;
            long sensing = System.nanoTime();
            OptionalDouble sensed = await("workflow.settle.estimate", () -> {
                OptionalDouble estimate = c.reserves().estimate(Settlement.RESERVE_EPOCH);
                return estimate.isPresent() && Math.abs(estimate.getAsDouble() - expected) < expected * 0.02
                        ? Optional.of(estimate) : Optional.<OptionalDouble>empty();
            }, Duration.ofSeconds(30), () -> reserveReport(fleet, witnesses))
                    .orElseGet(() -> c.reserves().estimate(Settlement.RESERVE_EPOCH));
            System.out.println("stage 8: the reserve estimate took "
                    + Duration.ofNanos(System.nanoTime() - sensing).toMillis() + " ms to reach " + sensed);
            assertThat(sensed).as(() -> reserveReport(fleet, witnesses)).isPresent();
            assertThat(sensed.getAsDouble()).as(() -> reserveReport(fleet, witnesses))
                    .isCloseTo(expected, within(expected * 0.02));
            // ...and the supervisor reported it exactly once, when it settled.
            // The demo's supervisor waits for forty settled rounds, paced to the gossip period (one a
            // second), so the report comes about forty seconds after the last priced claim moved
            // the estimate: measured at 38 to 43 s after this point over 40 runs. The wait is that
            // deliberate span plus room for its variable part (convergence, under 3 s measured) and
            // for a late claim (an ordered take that lapses holds one for its 30 s lease), so 120 s
            // means clearly broken; the time it took is printed.
            // Otherwise the quick supervisor is bound now, every priced claim in the estimate: three
            // settled rounds (about 3 s) after this point, so 30 s means clearly broken.
            if (!demoSupervisor) {
                staff.add(c.group().bind(new QuickSupervisor()));
            }
            long reporting = System.nanoTime();
            List<ReserveReport> reports = await(demoSupervisor ? "workflow.settle.report" : "workflow.settle.report-bound-late",
                    () -> {
                        List<ReserveReport> all = c.ledger().readAll(Template.of(ReserveReport.class), 10);
                        return all.isEmpty() ? Optional.<List<ReserveReport>>empty() : Optional.of(all);
                    }, Duration.ofSeconds(demoSupervisor ? 120 : 30), () -> reserveReport(fleet, witnesses))
                    .orElseThrow();
            System.out.println("stage 8: the supervisor reported after "
                    + Duration.ofNanos(System.nanoTime() - reporting).toMillis() + " ms: " + reports);
            // Reported once: the supervisor's estimate rule calls back once per epoch, and its one binding is on c.
            // The fixed 1.5 s sleep is gone; the positive marker is the report replicated to every peer, which any
            // second report written with it would have travelled beside, then exactly one is asserted everywhere.
            await("workflow.settle.report-replicated", () -> fleet.stream().allMatch(peer ->
                    !peer.ledger().readAll(Template.of(ReserveReport.class), 10).isEmpty())
                    ? Optional.of(true) : Optional.<Boolean>empty(), Duration.ofSeconds(30));
            for (Peer peer : fleet) {
                assertThat(peer.ledger().readAll(Template.of(ReserveReport.class), 10)).as("reports at " + peer.name())
                        .hasSize(1);
            }
            assertThat(reports.get(0).averageReserve()).as(() -> reserveReport(fleet, witnesses))
                    .isCloseTo(expected, within(expected * 0.05));
        } finally {
            for (AutoCloseable s : staff) {
                s.close();
            }
            fleet.forEach(Peer::close);
        }
    }

    /**
     * Stage 6a: with no panel at all, the reminder's lease lapses and the
     * escalator opens one motion per claim, once, however many replicas saw
     * the lapse.
     */
    @Test
    @Timeout(120)
    void aSilentPanelIsEscalatedOnceWhenTheReminderLapses() throws Exception {
        List<PeerIdentity> ids = List.of(PeerIdentity.generate(), PeerIdentity.generate(),
                PeerIdentity.generate());
        List<PeerId> members = ids.stream().map(PeerIdentity::peerId).toList();
        int seedPort = freePort();
        Peer a = ClaimsFlow.startPeer(ids.get(0), "peer-a", seedPort, 0, members, 31);
        Peer b = ClaimsFlow.startPeer(ids.get(1), "peer-b", freePort(), seedPort, members, 32);
        Peer c = ClaimsFlow.startPeer(ids.get(2), "peer-c", freePort(), seedPort, members, 33);
        List<Peer> fleet = List.of(a, b, c);
        List<AutoCloseable> staff = ClaimsFlow.staffWithoutPanel(a, b, c, Intake.Hook.NONE,
                Duration.ofSeconds(1));
        try {
            // formation, waited for rather than slept (was a fixed 1.5 s): every peer sees the other two
            await("workflow.claims.formed", () -> fleet.stream().allMatch(peer -> members.stream()
                    .filter(id -> !id.equals(peer.identity().peerId()))
                    .allMatch(id -> peer.runtime().membership().member(id).isPresent()))
                    ? Optional.of(true) : Optional.<Boolean>empty(), Duration.ofSeconds(30));
            a.intake().write(ClaimsFlow.CLAIMS.get(0), Lease.of(Duration.ofHours(1)));
            assertThat(await("workflow.claims.escalated", () -> a.vote().proposal("escalate:CLM-1"), Duration.ofSeconds(40)))
                    .as("the lapsed reminder escalated").isPresent();
            // fm: (fixed sleep) an absence window with no positive marker to wait on: no second escalation is written,
            // and nothing follows it in order (srad/feature-measurements.md §3.9)
            Thread.sleep(1500);
            assertThat(a.decisions().readAll(Template.of(VoteCapability.Proposal.class), 20))
                    .filteredOn(p -> p.proposalId().startsWith("escalate:")).hasSize(1);
            assertThat(a.vote().decision("claim:CLM-1")).as("nobody voted").isEmpty();
        } finally {
            for (AutoCloseable s : staff) {
                s.close();
            }
            fleet.forEach(Peer::close);
        }
    }

    /**
     * The supervisor with a test's settle: three rounds of about a second. It is bound once stage 8's estimate holds
     * every priced claim, so the plateau the demo's forty rounds outlast is already behind it.
     */
    @AgentSpec(name = "quick-supervisor", description = "Reports the fleet's average reserve once it settles",
            goals = {"report reserves"})
    public static final class QuickSupervisor {
        @OnEstimate(prefix = "reserve:", settle = "PT3S", tolerance = 0.01, resultSpace = Settlement.LEDGER,
                resultLease = "24h")
        public ReserveReport report(PushSumAggregate.Estimate reserve) {
            return new ReserveReport(reserve.epochId(), reserve.value(), "quick-supervisor");
        }
    }

    /**
     * Records the priced claims the binder hands a reaction on one peer, as the
     * reserve sensor's {@code @SpaceNotify} receives them.
     */
    @AgentSpec(name = "reserve-witness", description = "Records the priced claims a reserve reaction sees",
            goals = {"witness reserves"})
    public static final class ReserveWitness {
        final List<String> seen = new CopyOnWriteArrayList<>();

        @SpaceNotify(space = Pricing.SPACE)
        public void saw(PricedClaim priced) {
            seen.add(priced.claimId() + "=" + priced.reserve());
        }
    }

    /** What every peer saw, held and estimated for stage 8, so a failure names the missing contribution. */
    private static String reserveReport(List<Peer> fleet, Map<String, ReserveWitness> witnesses) {
        StringBuilder report = new StringBuilder("the fleet's reserve state:");
        for (Peer peer : fleet) {
            report.append("\n  ").append(peer.name())
                    .append(": reactions saw ").append(witnesses.get(peer.name()).seen)
                    .append(", holds ").append(peer.assessment().readAll(Template.of(PricedClaim.class), 10).stream()
                            .map(p -> p.claimId() + "=" + p.reserve()).toList())
                    .append(", estimate ").append(peer.reserves().estimate(Settlement.RESERVE_EPOCH));
        }
        return report.toString();
    }

    /**
     * A measured wait (srad/feature-measurements.md §3.5) that keeps the 200 ms poll it had. It returns the value
     * once the probe has one; past the ceiling it fails with the diagnostics, so callers' presence checks always hold.
     */
    private <T> Optional<T> await(String metric, Supplier<Optional<T>> probe, Duration timeout) {
        return await(metric, probe, timeout, () -> "");
    }

    private <T> Optional<T> await(String metric, Supplier<Optional<T>> probe, Duration timeout,
                                  Supplier<String> diagnostics) {
        return Optional.of(Measure.awaitValue(metric, timeout, probe,
                Wait.with().params(mode).poll(Duration.ofMillis(200)).diagnostics(diagnostics)));
    }
}
