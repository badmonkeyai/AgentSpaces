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

import ai.badmonkey.agentspaces.agent.Tagged;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.Part;
import ai.badmonkey.agentspaces.agent.annotation.SpaceJoin;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.agent.join.Joined;
import ai.badmonkey.agentspaces.examples.workflow.Claims.Claim;
import ai.badmonkey.agentspaces.examples.workflow.Claims.ClaimAssessment;
import ai.badmonkey.agentspaces.examples.workflow.Claims.CoverageFinding;
import ai.badmonkey.agentspaces.examples.workflow.Claims.FraudFinding;
import ai.badmonkey.agentspaces.examples.workflow.Claims.RegisteredClaim;
import ai.badmonkey.agentspaces.examples.workflow.Claims.Registration;
import ai.badmonkey.agentspaces.examples.workflow.Claims.Rejected;
import java.util.Objects;

/**
 * Stages 1 to 3: the kill-tolerant registrar, three specialists that fan out
 * from every registered claim, and the join that assembles their findings.
 */
public final class Intake {

    /** Claims, registered claims, findings and assemble requests all live here. */
    public static final String SPACE = "intake";

    /** Every policy in this example covers up to this amount. */
    public static final double COVERAGE_LIMIT = 25_000;

    private Intake() {
    }

    /** Something to do before registering. The demo and the test use it to arrange a crash. */
    @FunctionalInterface
    public interface Hook {
        /** Does nothing. */
        Hook NONE = (worker, claim) -> { };

        void beforeRegistering(String worker, Claim claim);
    }

    /**
     * Stage 1, the sequential stage. The take is leased; a crash before the
     * return (the hook throws) lets the lease lapse and the claim reappears for
     * any registrar, so nothing is lost and nothing is coordinated. The return
     * is sealed: a registered claim or a rejection, and the card declares both.
     */
    @AgentSpec(name = "registrar", description = "Registers incoming claims", goals = {"register claims"})
    public static final class Registrar {
        private final String name;
        private final Hook hook;

        public Registrar(String name, Hook hook) {
            this.name = Objects.requireNonNull(name, "name");
            this.hook = Objects.requireNonNull(hook, "hook");
        }

        @SpaceTake(space = SPACE, lease = "3s", pollTimeout = "300ms", resultLease = "1h")
        public Registration register(Claim claim) {
            hook.beforeRegistering(name, claim);
            // The vocabulary's constraints, checked where the data enters: a claim
            // that fails them is quarantined as a Rejected entry, which the ledger
            // closes, rather than thrown away.
            if (claim.claimedAmount() <= 0) {
                return new Rejected(claim.claimId(), "claimedAmount must be positive");
            }
            if (claim.policyId() == null || claim.policyId().isBlank()) {
                return new Rejected(claim.claimId(), "a claim names its policy");
            }
            return new RegisteredClaim(claim.claimId(), claim.policyId(), claim.claimant(),
                    claim.description(), claim.claimedAmount(), name);
        }
    }

    /** Stage 2, fan-out: scores the claim for fraud. */
    @AgentSpec(name = "fraud-screen", description = "Scores each registered claim for fraud",
            goals = {"screen for fraud"})
    public static final class FraudScreen {
        @SpaceNotify(space = SPACE, resultLease = "1h")
        public FraudFinding screen(RegisteredClaim claim) {
            if (claim.description().toLowerCase().contains("staged")) {
                return new FraudFinding(claim.claimId(), 90, "description matches a staging pattern",
                        "fraud-screen");
            }
            if (claim.claimedAmount() > 20_000) {
                return new FraudFinding(claim.claimId(), 40, "large claim", "fraud-screen");
            }
            return new FraudFinding(claim.claimId(), 10, "nothing unusual", "fraud-screen");
        }
    }

    /** Stage 2, fan-out: checks the policy is in force. */
    @AgentSpec(name = "coverage-check", description = "Checks each registered claim against its policy",
            goals = {"check coverage"})
    public static final class CoverageCheck {
        @SpaceNotify(space = SPACE, resultLease = "1h")
        public CoverageFinding check(RegisteredClaim claim) {
            boolean covered = !claim.policyId().endsWith("-LAPSED");
            return new CoverageFinding(claim.claimId(), covered, COVERAGE_LIMIT, "coverage-check");
        }
    }

    /**
     * Stage 2, fan-out: estimates the damage, and emits the finding as an
     * {@link Instance} of the vocabulary's {@code DamageFinding} class rather
     * than as a record of its own, tagged with its key and its class IRI.
     */
    @AgentSpec(name = "damage-estimator", description = "Estimates the damage behind each registered claim",
            goals = {"estimate damage"})
    public static final class DamageEstimator {
        @SpaceNotify(space = SPACE, resultLease = "1h")
        public Tagged<Instance> estimate(RegisteredClaim claim) {
            double estimate = Math.round(claim.claimedAmount() * 0.9);
            Instance finding = Instance.damageFinding(claim.claimId(), estimate,
                    severity(claim.claimedAmount()), "damage-estimator");
            // The tags carry what a template can select on before decoding: the
            // correlation key the join needs, and the class IRI (SPEC §7.2, §11).
            return Tagged.of(finding, "claimId", claim.claimId(), "rdf:type", finding.typeIri());
        }
    }

    /** The severity bands the adjusters bid on. */
    public static String severity(double amount) {
        return amount < 5_000 ? "LOW" : amount < 15_000 ? "MEDIUM" : "HIGH";
    }

    /**
     * Stage 3, fan-in, as one annotation. The binder waits for the four parts
     * of a claim (the registered claim and the three findings), fires once per
     * claim id when all are present, and hands them over as a {@link Joined}
     * bag. The damage finding is keyed by its {@code claimId} tag, which the
     * estimator wrote with {@link Tagged}, since the instance record carries no
     * such component. {@code LOCAL} mode fires once per bound joiner, so the
     * demo binds one; a second test binds two joiners in {@code ORDERED} mode
     * over the payments log and still gets one assessment per claim.
     */
    @AgentSpec(name = "assembler", description = "Joins the findings of a claim into one assessment",
            goals = {"assemble claims"})
    public static final class Assembler {
        private final String name;

        public Assembler(String name) {
            this.name = Objects.requireNonNull(name, "name");
        }

        @SpaceJoin(space = SPACE, key = "claimId", resultSpace = Pricing.SPACE, resultLease = "1h",
                parts = {@Part(RegisteredClaim.class), @Part(FraudFinding.class), @Part(CoverageFinding.class),
                         @Part(value = Instance.class, keyTag = "claimId",
                               tags = "rdf:type=" + Claims.NAMESPACE + Instance.DAMAGE_FINDING)})
        public ClaimAssessment assemble(Joined j) {
            RegisteredClaim claim = j.get(RegisteredClaim.class);
            Instance damage = j.get(Instance.class);
            return new ClaimAssessment(j.key(), claim.claimant(), claim.claimedAmount(),
                    damage.number("estimate"), damage.string("severity"),
                    j.get(FraudFinding.class).fraudScore(), j.get(CoverageFinding.class).covered(),
                    j.get(CoverageFinding.class).coverageLimit(), name);
        }
    }

    /**
     * The same join in {@code ORDERED} mode: the ticket the binder writes when a
     * claim's parts are all present is taken through an ordered-log coordinator,
     * so several joiners on several peers assemble each claim exactly once
     * fleet-wide. A peer hosts one ordered log per group (the capability is one
     * pipe and one advertised role per peer), and this fleet's log is on
     * {@code payments}; so the join lives there, its tickets go through that
     * log, and its parts stay in {@code intake}. The join's space is where its
     * tickets live; its parts may live anywhere.
     */
    @AgentSpec(name = "assembler", description = "Joins the findings of a claim into one assessment, once fleet-wide",
            goals = {"assemble claims"})
    public static final class OrderedAssembler {
        private final String name;

        public OrderedAssembler(String name) {
            this.name = Objects.requireNonNull(name, "name");
        }

        @SpaceJoin(space = Settlement.PAYMENTS, key = "claimId", mode = SpaceJoin.Mode.ORDERED,
                takeLease = "30s", pollTimeout = "500ms", resultSpace = Pricing.SPACE, resultLease = "1h",
                parts = {@Part(value = RegisteredClaim.class, space = SPACE),
                         @Part(value = FraudFinding.class, space = SPACE),
                         @Part(value = CoverageFinding.class, space = SPACE),
                         @Part(value = Instance.class, space = SPACE, keyTag = "claimId")})
        public ClaimAssessment assemble(Joined j) {
            RegisteredClaim claim = j.get(RegisteredClaim.class);
            Instance damage = j.get(Instance.class);
            return new ClaimAssessment(j.key(), claim.claimant(), claim.claimedAmount(),
                    damage.number("estimate"), damage.string("severity"),
                    j.get(FraudFinding.class).fraudScore(), j.get(CoverageFinding.class).covered(),
                    j.get(CoverageFinding.class).coverageLimit(), name);
        }
    }
}
