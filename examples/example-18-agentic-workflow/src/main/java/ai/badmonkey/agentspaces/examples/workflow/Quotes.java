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

import ai.badmonkey.agentspaces.agent.Entries;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.Part;
import ai.badmonkey.agentspaces.agent.annotation.SpaceJoin;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.agent.join.Joined;
import ai.badmonkey.agentspaces.examples.workflow.Claims.ClaimAssessment;
import ai.badmonkey.agentspaces.examples.workflow.Claims.QuoteRequest;
import ai.badmonkey.agentspaces.examples.workflow.Claims.QuoteTask;
import ai.badmonkey.agentspaces.examples.workflow.Claims.RepairEstimate;
import ai.badmonkey.agentspaces.examples.workflow.Claims.RepairQuote;
import ai.badmonkey.agentspaces.examples.workflow.Claims.SeniorReview;
import java.util.Comparator;
import java.util.List;

/**
 * Stages 3a and 3b: the fork, the gather, and the route. One assessment forks
 * into one quote request and one task per repair shop; each shop takes only
 * the tasks addressed to it; a join gathers as many quotes as the request
 * asked for and keeps the lowest; and a reviewer sees severe assessments only.
 */
public final class Quotes {

    /** The shops a claim is quoted by, and how each prices against the damage estimate. */
    public static final List<String> SHOPS = List.of("north", "east", "south");

    private Quotes() {
    }

    /**
     * The fork: one cue, several entries, each dispatched as if returned
     * alone. The request says how many quotes to expect, so the gather below
     * waits for exactly that many; the tasks carry the shop so each shop's
     * take sees only its own. The card declares both types.
     */
    @AgentSpec(name = "quote-desk", description = "Asks every repair shop for a quote", goals = {"quote"})
    public static final class QuoteDesk {
        @SpaceNotify(space = Pricing.SPACE, resultSpace = Intake.SPACE, resultLease = "1h",
                produces = {QuoteRequest.class, QuoteTask.class})
        public Entries ask(ClaimAssessment assessment) {
            return Entries.of(new QuoteRequest(assessment.claimId(), SHOPS.size()),
                    new QuoteTask(assessment.claimId(), "north", assessment.damageEstimate()),
                    new QuoteTask(assessment.claimId(), "east", assessment.damageEstimate()),
                    new QuoteTask(assessment.claimId(), "south", assessment.damageEstimate()));
        }
    }

    /** What every shop does once it holds a task; the filters that route tasks are on the shops. */
    static RepairQuote quote(QuoteTask task, String shop, double factor) {
        return new RepairQuote(task.claimId(), shop, Math.round(task.estimate() * factor));
    }

    /** The route by field: a worker deployed for one shop takes only that shop's tasks. */
    @AgentSpec(name = "north-shop", description = "Quotes repairs in the north", goals = {"quote"})
    public static final class NorthShop {
        @SpaceTake(space = Intake.SPACE, where = "shop=north", lease = "30s", pollTimeout = "300ms",
                resultLease = "1h")
        public RepairQuote quote(QuoteTask task) {
            return Quotes.quote(task, "north", 1.1);
        }
    }

    @AgentSpec(name = "east-shop", description = "Quotes repairs in the east", goals = {"quote"})
    public static final class EastShop {
        @SpaceTake(space = Intake.SPACE, where = "shop=east", lease = "30s", pollTimeout = "300ms",
                resultLease = "1h")
        public RepairQuote quote(QuoteTask task) {
            return Quotes.quote(task, "east", 0.9);
        }
    }

    @AgentSpec(name = "south-shop", description = "Quotes repairs in the south", goals = {"quote"})
    public static final class SouthShop {
        @SpaceTake(space = Intake.SPACE, where = "shop=south", lease = "30s", pollTimeout = "300ms",
                resultLease = "1h")
        public RepairQuote quote(QuoteTask task) {
            return Quotes.quote(task, "south", 1.0);
        }
    }

    /**
     * The gather: the request says how many quotes to wait for; the join hands
     * them all over and the method keeps the lowest. A {@code settle} period
     * would end a gather of an unknown number; here the count is known.
     */
    @AgentSpec(name = "quote-gatherer", description = "Keeps the lowest of the quotes asked for",
            goals = {"gather quotes"})
    public static final class QuoteGatherer {
        @SpaceJoin(space = Intake.SPACE, key = "claimId", resultSpace = Settlement.LEDGER, resultLease = "24h",
                parts = {@Part(QuoteRequest.class),
                         @Part(value = RepairQuote.class, countedBy = "QuoteRequest.expected")})
        public RepairEstimate pick(Joined j) {
            List<RepairQuote> quotes = j.all(RepairQuote.class);
            RepairQuote lowest = quotes.stream().min(Comparator.comparingDouble(RepairQuote::amount)).orElseThrow();
            return new RepairEstimate(j.key(), lowest.shop(), lowest.amount(), quotes.size());
        }
    }

    /** The route by field on a reaction: only severe assessments reach the reviewer. */
    @AgentSpec(name = "senior-reviewer", description = "Reviews severe claims", goals = {"review"})
    public static final class SeniorReviewer {
        @SpaceNotify(space = Pricing.SPACE, where = "severity=HIGH", resultSpace = Settlement.LEDGER,
                resultLease = "24h")
        public SeniorReview review(ClaimAssessment assessment) {
            return new SeniorReview(assessment.claimId(), "severe: damage " + assessment.damageEstimate()
                    + " against a limit of " + assessment.coverageLimit());
        }
    }
}
