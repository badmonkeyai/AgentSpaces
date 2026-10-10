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

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.Ballot;
import ai.badmonkey.agentspaces.agent.annotation.OnDecision;
import ai.badmonkey.agentspaces.agent.annotation.CapabilityRef;
import ai.badmonkey.agentspaces.agent.annotation.Propose;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceRef;
import ai.badmonkey.agentspaces.agent.capability.Motion;
import ai.badmonkey.agentspaces.agent.capability.VoteClient;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.examples.workflow.Claims.ClaimDenied;
import ai.badmonkey.agentspaces.examples.workflow.Claims.PaymentOrder;
import ai.badmonkey.agentspaces.examples.workflow.Claims.PricedClaim;
import ai.badmonkey.agentspaces.examples.workflow.Claims.Reminder;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Stages 5 and 6: a decision in the flow. The underwriter opens a vote for
 * every priced claim, one panelist per peer casts a ballot when the proposal
 * appears, and the approver reacts once when the quorum closes.
 */
public final class Decisions {

    /** The vote space; proposals and ballots are entries here. */
    public static final String SPACE = "decisions";

    /** Proposal ids are {@code claim:<claimId>}, so one vote space can carry other decisions too. */
    public static final String PREFIX = "claim:";

    private Decisions() {
    }

    static String claimIdOf(String proposalId) {
        return proposalId.substring(PREFIX.length());
    }

    /**
     * Stage 5: opens the vote, as one annotation. The shape is fixed (the
     * options, the quorum, the lease, and the id rule {@code claim:<claimId>}
     * sit on the annotation); the cue decides only the question. The binder
     * opens once per claim, as this agent, and a redelivered cue opens nothing.
     */
    @AgentSpec(name = "underwriter", description = "Puts every priced claim to the panel",
            goals = {"open decisions"})
    public static final class Underwriter {
        @SpaceRef(SPACE)
        Space decisions;
        private final Lease reminderLease;

        /** @param reminderLease how long the panel has before the flow escalates */
        public Underwriter(Duration reminderLease) {
            this.reminderLease = Lease.of(Objects.requireNonNull(reminderLease, "reminderLease"));
        }

        @Propose(space = Pricing.SPACE, vote = SPACE, prefix = PREFIX, key = "claimId",
                options = {Pricing.APPROVE, Pricing.DENY}, quorum = 3, lease = "10m")
        public String open(PricedClaim priced) {
            // The deadline is a leased entry: when it lapses, the escalator below
            // reacts; when the panel decides first, the escalator finds the
            // decision and does nothing.
            decisions.write(new Reminder(priced.claimId()), reminderLease);
            return "Settle claim " + priced.claimId() + "? The adjuster recommends "
                    + priced.recommendation() + " with a reserve of " + priced.reserve();
        }
    }

    /**
     * Stage 6a: the deadline. A reminder's lease lapsing is the cue; the
     * escalation is a motion, so it opens once per claim however many replicas
     * see the lapse. Bind one escalator.
     */
    @AgentSpec(name = "escalator", description = "Escalates claims the panel has not decided in time",
            goals = {"escalate"})
    public static final class Escalator {
        @CapabilityRef
        VoteClient votes;

        @SpaceNotify(space = SPACE, on = SpaceEvent.Kind.EXPIRED)
        public Motion escalate(Reminder reminder) {
            if (votes.decision(PREFIX + reminder.claimId()).isPresent()) {
                return null;                       // decided in time: nothing to escalate
            }
            return Motion.of("escalate:" + reminder.claimId(), "The panel has not decided claim "
                    + reminder.claimId() + " in time; escalate to the chief underwriter?",
                    List.of("escalate", "wait"), 1, Lease.of(Duration.ofMinutes(10)));
        }
    }

    /**
     * Stage 6: one ballot per proposal. The cue is the proposal itself, so the
     * ballot can never be refused as unknown. The panelist reads the priced
     * claim it is judging, waiting briefly for it to replicate, and abstains
     * if it has not; abstention is not reconsidered later.
     */
    @AgentSpec(name = "panelist", description = "Judges each proposed settlement", goals = {"judge claims"})
    public static final class Panelist {
        @SpaceRef(Pricing.SPACE)
        Space assessments;
        private final double ceiling;

        /** @param ceiling the largest reserve this panelist will approve */
        public Panelist(double ceiling) {
            this.ceiling = ceiling;
        }

        @Ballot(space = SPACE, prefix = PREFIX, lease = "10m")
        public String judge(VoteCapability.Proposal proposal) {
            Optional<PricedClaim> priced = assessments.read(Template.of(PricedClaim.class)
                    .where("claimId", eq(claimIdOf(proposal.proposalId()))), Duration.ofSeconds(5));
            if (priced.isEmpty()) {
                return null;                       // abstain
            }
            boolean approve = Pricing.APPROVE.equals(priced.get().recommendation())
                    && priced.get().reserve() <= ceiling;
            return approve ? Pricing.APPROVE : Pricing.DENY;
        }
    }

    /**
     * Stage 6: fires once per proposal when this replica's tally meets the
     * quorum. A method has one return type, so the approval is the return (a
     * payment order into the payments space) and the denial is a direct write
     * into the ledger through a {@code @SpaceRef}. Bind one approver.
     */
    @AgentSpec(name = "approver", description = "Turns the panel's decision into a payment order or a denial",
            goals = {"settle decisions"})
    public static final class Approver {
        @SpaceRef(Pricing.SPACE)
        Space assessments;
        @SpaceRef(Settlement.LEDGER)
        Space ledger;

        @OnDecision(space = SPACE, prefix = PREFIX, resultSpace = Settlement.PAYMENTS, resultLease = "1h")
        public PaymentOrder settle(VoteCapability.Decision decision) {
            String claimId = claimIdOf(decision.proposalId());
            PricedClaim priced = assessments.read(Template.of(PricedClaim.class)
                    .where("claimId", eq(claimId)), Duration.ofSeconds(5)).orElseThrow();
            if (!Pricing.APPROVE.equals(decision.winner())) {
                ledger.write(new ClaimDenied(claimId, "the panel voted " + decision.tally()
                        + ", counted per " + decision.granularity()), Lease.of(Duration.ofHours(24)));
                return null;
            }
            return new PaymentOrder(claimId, priced.claimant(), Math.round(priced.reserve() * 100));
        }
    }
}
