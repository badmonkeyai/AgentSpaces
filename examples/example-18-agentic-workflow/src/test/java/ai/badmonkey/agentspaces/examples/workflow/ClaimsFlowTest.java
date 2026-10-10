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

import ai.badmonkey.agentspaces.agent.join.JoinTicket;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
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
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The whole flow over real TCP: four claims in, four cases closed, and the
 * properties each stage promises hold at every replica.
 */
class ClaimsFlowTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(180)
    void fourClaimsFlowThroughEveryStageAndCloseOnce() throws Exception {
        runTheFlow(false);
    }

    /**
     * The join in ORDERED mode: two joiners on two peers assemble each claim
     * once, through tickets the intake log arbitrates (issue #16 §14.12).
     */
    @Test
    @Timeout(180)
    void twoOrderedJoinersStillAssembleEachClaimOnce() throws Exception {
        runTheFlow(true);
    }

    private void runTheFlow(boolean orderedJoin) throws Exception {
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
        List<AutoCloseable> staff = orderedJoin
                ? ClaimsFlow.staffWithOrderedJoin(a, b, c, counted) : ClaimsFlow.staff(a, b, c, counted);
        try {
            // A log leader must exist before a payment can be taken; wait for the election.
            assertThat(await(() -> fleet.stream().anyMatch(p -> p.ordered().raft().isLeader())
                    ? Optional.of(true) : Optional.<Boolean>empty(), Duration.ofSeconds(40)))
                    .as("a payments log leader is elected").isPresent();

            for (Claim claim : ClaimsFlow.CLAIMS) {
                a.intake().write(claim, Lease.of(Duration.ofHours(1)));
            }

            // Stage 9: every case closes, once, with the expected outcome.
            List<ClaimClosed> closed = await(() -> {
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
            // downstream: four priced claims, one per claim, and no assessment left
            // unpriced; in ORDERED mode, no ticket left either.
            Map<String, PricedClaim> priced = a.assessment().readAll(Template.of(PricedClaim.class), 10)
                    .stream().collect(java.util.stream.Collectors.toMap(PricedClaim::claimId, p -> p));
            assertThat(a.assessment().readAll(Template.of(PricedClaim.class), 10)).hasSize(4);
            assertThat(priced.keySet()).containsExactlyInAnyOrder("CLM-1", "CLM-2", "CLM-3", "CLM-4");
            assertThat(a.assessment().readAll(Template.of(ClaimAssessment.class), 10))
                    .as("every assessment was taken and priced").isEmpty();
            if (orderedJoin) {
                for (Peer peer : fleet) {
                    assertThat(await(() -> peer.payments().readAll(Template.of(JoinTicket.class), 10)
                                    .isEmpty() ? Optional.of(true) : Optional.<Boolean>empty(),
                            Duration.ofSeconds(20))).as("tickets drained at " + peer.name()).isPresent();
                }
            }
            // The fire claim's assessment came through the ontology instance: the
            // damage estimate and severity were read from the DamageFinding's properties.
            assertThat(priced.get("CLM-4").reserve()).isEqualTo(25_000);   // 36,000 capped at the limit

            // Stage 4: the auction sent the severe claim to the senior adjuster and the
            // routine one to the junior.
            assertThat(priced.get("CLM-4").pricedBy()).isEqualTo("senior-adjuster");
            assertThat(priced.get("CLM-1").pricedBy()).isEqualTo("junior-adjuster");
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
                Map<String, ClaimantExposure> exposure = await(() -> {
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
                assertThat(await(() -> peer.payments().readAll(Template.of(PaymentOrder.class), 10).isEmpty()
                                && peer.payments().readAll(Template.of(Payment.class), 10).isEmpty()
                                ? Optional.of(true) : Optional.<Boolean>empty(),
                        Duration.ofSeconds(30))).as("orders and payments drained at " + peer.name()).isPresent();
            }

            // Stage 3a: the fork wrote one request and three tasks per assessed claim, one
            // shop per peer took its own tasks, and the gather kept the lowest of exactly
            // three quotes; the east shop prices lowest.
            List<RepairEstimate> estimates = await(() -> {
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
            double expected = (7_200 + 0 + 0 + 25_000) / 4.0;
            OptionalDouble sensed = await(() -> {
                OptionalDouble estimate = c.reserves().estimate(Settlement.RESERVE_EPOCH);
                return estimate.isPresent() && Math.abs(estimate.getAsDouble() - expected) < expected * 0.02
                        ? Optional.of(estimate) : Optional.<OptionalDouble>empty();
            }, Duration.ofSeconds(30)).orElseGet(() -> c.reserves().estimate(Settlement.RESERVE_EPOCH));
            assertThat(sensed).isPresent();
            assertThat(sensed.getAsDouble()).isCloseTo(expected, within(expected * 0.02));
            // ...and the supervisor reported it exactly once, when it settled.
            List<ReserveReport> reports = await(() -> {
                List<ReserveReport> all = c.ledger().readAll(Template.of(ReserveReport.class), 10);
                return all.isEmpty() ? Optional.<List<ReserveReport>>empty() : Optional.of(all);
            }, Duration.ofSeconds(30)).orElseThrow();
            Thread.sleep(1500);
            assertThat(c.ledger().readAll(Template.of(ReserveReport.class), 10)).hasSize(1);
            assertThat(reports.get(0).averageReserve()).isCloseTo(expected, within(expected * 0.05));
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
            Thread.sleep(1500);
            a.intake().write(ClaimsFlow.CLAIMS.get(0), Lease.of(Duration.ofHours(1)));
            assertThat(await(() -> a.vote().proposal("escalate:CLM-1"), Duration.ofSeconds(40)))
                    .as("the lapsed reminder escalated").isPresent();
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

    private static <T> Optional<T> await(Supplier<Optional<T>> probe, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<T> value = probe.get();
            if (value.isPresent()) {
                return value;
            }
            Thread.sleep(200);
        }
        return probe.get();
    }
}
