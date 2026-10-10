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

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.OnEstimate;
import ai.badmonkey.agentspaces.agent.annotation.SpaceReduce;
import ai.badmonkey.agentspaces.agent.annotation.OrderedTake;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.capability.Contribution;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes;
import ai.badmonkey.agentspaces.examples.workflow.Claims.ClaimClosed;
import ai.badmonkey.agentspaces.examples.workflow.Claims.ClaimantExposure;
import ai.badmonkey.agentspaces.examples.workflow.Claims.ClaimDenied;
import ai.badmonkey.agentspaces.examples.workflow.Claims.Payment;
import ai.badmonkey.agentspaces.examples.workflow.Claims.PaymentOrder;
import ai.badmonkey.agentspaces.examples.workflow.Claims.PricedClaim;
import ai.badmonkey.agentspaces.examples.workflow.Claims.Rejected;
import ai.badmonkey.agentspaces.examples.workflow.Claims.ReserveReport;
import java.util.Objects;

/**
 * Stages 7 to 9: the exactly-once payment, the ledger that closes every case,
 * and the sensing that tells the fleet what its average reserve is.
 */
public final class Settlement {

    /** Payment orders are taken here through the ordered log; payments are written back here. */
    public static final String PAYMENTS = "payments";

    /** The terminal records, with long leases. */
    public static final String LEDGER = "ledger";

    /** The push-sum epoch every priced claim's reserve is contributed to. */
    public static final String RESERVE_EPOCH = "reserve:2026-10";

    private Settlement() {
    }

    /**
     * Stage 7: the one irreversible step, completed at most once fleet-wide.
     * The take goes through the space's ordered-log coordinator; the result is
     * written into the same space, atomically with the completion.
     */
    @AgentSpec(name = "payer", description = "Pays approved claims exactly once", goals = {"pay claims"})
    public static final class Payer {
        private final String name;
        private final OrderedTakes ordered;

        public Payer(String name, OrderedTakes ordered) {
            this.name = Objects.requireNonNull(name, "name");
            this.ordered = Objects.requireNonNull(ordered, "ordered");
        }

        @OrderedTake(space = PAYMENTS, lease = "30s", pollTimeout = "2s", resultLease = "1h")
        public Payment pay(PaymentOrder order) {
            return new Payment(order.claimId(), order.claimant(), order.cents(), name,
                    ordered.raft().commitIndex());
        }
    }

    /**
     * Stage 7a: the running exposure per claimant, a fold whose state is an
     * entry. Each payment is taken and completed with the claimant's new
     * exposure as the result, atomically, so a payment is never counted twice;
     * the exposure lives in the payments space beside the payments, tagged by
     * claimant, and any peer reads it with {@code Reductions.current}. The
     * ledger's reaction below still sees every payment: a reaction fires on the
     * write, and the take that follows does not retract it. Bind one, or several;
     * a ticket per claimant serializes them.
     */
    @AgentSpec(name = "exposure", description = "Keeps each claimant's running exposure as payments land",
            goals = {"track exposure"})
    public static final class Exposure {
        @SpaceReduce(space = PAYMENTS, key = "claimant", name = "exposure", lease = "30s", pollTimeout = "300ms",
                accumulatorLease = "24h")
        public ClaimantExposure expose(ClaimantExposure exposure, Payment payment) {
            long paid = (exposure == null ? 0 : exposure.paidCents()) + payment.cents();
            int count = (exposure == null ? 0 : exposure.payments()) + 1;
            return new ClaimantExposure(payment.claimant(), paid, count);
        }
    }

    /** Stage 9: closes every case, paid or denied, with a record that outlives the flow's leases. */
    @AgentSpec(name = "ledger", description = "Closes every claim with a durable record", goals = {"keep the ledger"})
    public static final class Ledger {
        @SpaceNotify(space = PAYMENTS, resultSpace = LEDGER, resultLease = "24h")
        public ClaimClosed onPayment(Payment payment) {
            return new ClaimClosed(payment.claimId(), "PAID", payment.cents() + " cents to "
                    + payment.claimant() + " by " + payment.paidBy() + " (log #" + payment.logIndex() + ")");
        }

        @SpaceNotify(space = LEDGER, resultLease = "24h")
        public ClaimClosed onDenial(ClaimDenied denied) {
            return new ClaimClosed(denied.claimId(), "DENIED", denied.reason());
        }

        @SpaceNotify(space = Intake.SPACE, resultSpace = LEDGER, resultLease = "24h")
        public ClaimClosed onRejection(Rejected rejected) {
            return new ClaimClosed(rejected.claimId(), "REJECTED", rejected.reason());
        }
    }

    /**
     * Stage 8: sensing. Returning a {@link Contribution} adds this value to the
     * named push-sum epoch on this peer's aggregate; the fleet converges on the
     * average with no collector. Bind one sensor per peer.
     */
    @AgentSpec(name = "reserve-sensor", description = "Contributes each reserve to the fleet's average",
            goals = {"sense reserves"})
    public static final class ReserveSensor {
        @SpaceNotify(space = Pricing.SPACE)
        public Contribution sense(PricedClaim priced) {
            return Contribution.to(RESERVE_EPOCH, priced.reserve());
        }
    }

    /**
     * Stage 8: reacts once when the epoch's estimate has settled (unchanged
     * within a tolerance for a number of protocol ticks). The capability
     * evaluates the rule on its own tick; nothing here polls. The peers tick
     * every 250 ms, so twenty ticks is five quiet seconds: longer than the
     * spread between the first and the last priced claim, so the one report
     * carries the average of all of them.
     */
    @AgentSpec(name = "supervisor", description = "Reports the fleet's average reserve once it settles",
            goals = {"report reserves"})
    public static final class Supervisor {
        private final String name;

        public Supervisor(String name) {
            this.name = Objects.requireNonNull(name, "name");
        }

        @OnEstimate(prefix = "reserve:", settleTicks = 20, tolerance = 0.01, resultSpace = LEDGER,
                resultLease = "24h")
        public ReserveReport report(PushSumAggregate.Estimate reserve) {
            return new ReserveReport(reserve.epochId(), reserve.value(), name);
        }
    }
}
