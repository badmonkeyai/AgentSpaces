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

/**
 * The entries of the claims flow. Each record mirrors a class of a small claims
 * vocabulary ({@link #NAMESPACE}): the record is the class, its components are
 * the class's datatype properties, and an object property to another individual
 * is carried as that individual's id ({@code policyId}), never as a nested
 * object, so a stage can read the referent with a template and the referent can
 * live in its own space under its own lease. Every record carries the
 * {@code claimId}, the one correlation key that follows a case through every
 * stage. On the wire each record is named {@code <fully.qualified.Name>#v1}.
 *
 * <p>The damage finding is the exception: it travels as a generic
 * {@link Instance} of the vocabulary's {@code DamageFinding} class, to show the
 * second way an ontology reaches a space.
 */
public final class Claims {

    /** The vocabulary these records mirror. */
    public static final String NAMESPACE = "https://example.org/claims#";

    private Claims() {
    }

    /** What arrives: a claim against a policy. */
    public record Claim(String claimId, String policyId, String claimant, String description,
                        double claimedAmount) {
    }

    /** Stage 1: what the registrar returns, one of two branches; the card declares both. */
    public sealed interface Registration permits RegisteredClaim, Rejected {
    }

    /** Stage 1: the registrar's output, the cue for every specialist. */
    public record RegisteredClaim(String claimId, String policyId, String claimant,
                                  String description, double claimedAmount, String registeredBy)
            implements Registration {
    }

    /** Stage 1: a claim that failed the vocabulary's constraints; incongruent data, kept where it entered. */
    public record Rejected(String claimId, String reason) implements Registration {
    }

    /** Stage 3a: the fork's request, declaring how many quotes the gather should wait for. */
    public record QuoteRequest(String claimId, int expected) {
    }

    /** Stage 3a: one task per repair shop; the shop field routes it. */
    public record QuoteTask(String claimId, String shop, double estimate) {
    }

    /** Stage 3a: a shop's quote. */
    public record RepairQuote(String claimId, String shop, double amount) {
    }

    /** Stage 3a: the gather's output, the lowest of the quotes asked for. */
    public record RepairEstimate(String claimId, String shop, double amount, int considered) {
    }

    /** Stage 3b: the senior reviewer's note on a severe claim. */
    public record SeniorReview(String claimId, String note) {
    }

    /** Stage 6a: a deadline, which is a leased entry: when it lapses unanswered, the flow escalates. */
    public record Reminder(String claimId) {
    }

    /** Stage 2: the fraud screen's finding. */
    public record FraudFinding(String claimId, int fraudScore, String rationale, String by) {
    }

    /** Stage 2: the coverage check's finding. */
    public record CoverageFinding(String claimId, boolean covered, double coverageLimit, String by) {
    }

    /** Stage 3: the join of the three findings, priced at auction in stage 4. */
    public record ClaimAssessment(String claimId, String claimant, double claimedAmount,
                                  double damageEstimate, String severity, int fraudScore,
                                  boolean covered, double coverageLimit, String assembledBy) {
    }

    /** Stage 4: an adjuster's price and recommendation. */
    public record PricedClaim(String claimId, String claimant, double reserve,
                              String recommendation, String pricedBy) {
    }

    /** Stage 6: the panel approved; pay this, exactly once. */
    public record PaymentOrder(String claimId, String claimant, long cents) {
    }

    /** Stage 6: the panel denied. */
    public record ClaimDenied(String claimId, String reason) {
    }

    /** Stage 7: the payment, confirmed through the ordered log. */
    public record Payment(String claimId, String claimant, long cents, String paidBy, long logIndex) {
    }

    /** Stage 9: the terminal record, written with a long lease into the ledger. */
    public record ClaimClosed(String claimId, String outcome, String summary) {
    }

    /** Stage 7a: a claimant's running exposure, the accumulator of a {@code @SpaceReduce} over payments. */
    public record ClaimantExposure(String claimant, long paidCents, int payments) {
    }

    /** Stage 8: what the fleet thinks the average reserve is. */
    public record ReserveReport(String epoch, double averageReserve, String reportedBy) {
    }
}
