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
import ai.badmonkey.agentspaces.agent.annotation.BidFunction;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.examples.workflow.Claims.ClaimAssessment;
import ai.badmonkey.agentspaces.examples.workflow.Claims.PricedClaim;

/**
 * Stage 4: pricing work routed by cost. The assessment space is an AUCTION
 * space, so every take carries the taker's bid and the lowest bid fleet-wide
 * wins. A senior adjuster is cheap for severe claims and dear for simple ones;
 * a junior adjuster is the reverse. One bidder per space per peer.
 */
public final class Pricing {

    /** Assessments arrive here and are taken at auction; priced claims are written back here. */
    public static final String SPACE = "assessment";

    public static final String APPROVE = "approve";
    public static final String DENY = "deny";

    private Pricing() {
    }

    /**
     * The pricing both adjusters apply; what differs between them is the bid. The
     * reserve is what the fleet expects to pay, so a claim recommended for denial
     * carries none.
     */
    static PricedClaim price(ClaimAssessment a, String by) {
        boolean deny = !a.covered() || a.fraudScore() >= 70;
        double reserve = deny ? 0 : Math.min(a.damageEstimate(), a.coverageLimit());
        return new PricedClaim(a.claimId(), a.claimant(), reserve, deny ? DENY : APPROVE, by);
    }

    @AgentSpec(name = "senior-adjuster", description = "Prices severe claims", goals = {"price claims"})
    public static final class SeniorAdjuster {
        @BidFunction(space = SPACE)
        public double bid(ClaimAssessment a) {
            return "HIGH".equals(a.severity()) ? 10 : 80;
        }

        @SpaceTake(space = SPACE, lease = "30s", pollTimeout = "300ms", resultLease = "1h")
        public PricedClaim price(ClaimAssessment a) {
            return Pricing.price(a, "senior-adjuster");
        }
    }

    @AgentSpec(name = "junior-adjuster", description = "Prices routine claims", goals = {"price claims"})
    public static final class JuniorAdjuster {
        @BidFunction(space = SPACE)
        public double bid(ClaimAssessment a) {
            return "HIGH".equals(a.severity()) ? 90 : 20;
        }

        @SpaceTake(space = SPACE, lease = "30s", pollTimeout = "300ms", resultLease = "1h")
        public PricedClaim price(ClaimAssessment a) {
            return Pricing.price(a, "junior-adjuster");
        }
    }
}
