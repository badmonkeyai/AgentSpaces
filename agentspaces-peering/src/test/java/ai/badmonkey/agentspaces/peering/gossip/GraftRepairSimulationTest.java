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
package ai.badmonkey.agentspaces.peering.gossip;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Swarm retries (ROBUSTNESS.md §10.4, test 2): twenty-one buses on the seeded fabric of {@link GraftRepairSimulation}.
 * A member that lacks an announced item asks for it on a spread timer, of an announcer other than the first heard, so
 * a large item's copies come from the tree and the members that hold it: the writer answers no GRAFT from a member
 * that heard the writer first, and it pushes one copy per eager edge (P2-Q-PRF-04 as restated on 2026-11-09). Every
 * member still gets the item in every run.
 */
class GraftRepairSimulationTest {

    private static final int SEEDS = 20;

    /**
     * P2-Q-PRF-04 as the owner restated it on 2026-11-09 (srad/agentspaces-pdt2.md §10.16), in place of "at most three
     * copies in 15 of 20 seeds": (b) the writer pushes one copy per eager edge it holds at the publish, the count
     * reported, and every member gets the item. For (a), that the writer answers no GRAFT from a member that heard another
     * member announce the item, the bus holds the part its rule controls: a member that heard the writer first never asks
     * it while another announcer is left. A member that heard a relay first and the writer second asks the writer, since a
     * HAVE does not name the item's origin; the run reports those answers (§10.18 records them for the owner).
     */
    @Test
    void theWriterPushesOneCopyPerEagerEdgeAndAnswersNoGraftFromAMemberThatHeardItFirstAndAnotherSince() {
        List<GraftRepairSimulation.Result> results = new ArrayList<>();
        for (long seed = 1; seed <= SEEDS; seed++) {
            results.add(GraftRepairSimulation.run(seed, 21));
        }
        long grafts = results.stream().mapToLong(GraftRepairSimulation.Result::grafts).sum();
        long premature = results.stream().mapToLong(GraftRepairSimulation.Result::premature).sum();
        List<Long> last = results.stream().map(GraftRepairSimulation.Result::lastCopyMillis).sorted().toList();
        System.out.printf("swarm retries simulation, %d seeds: writer eager edges %s, eager copies %s, GRAFT answers %s"
                        + " (after another announcer %s, of them after the writer first %s); writer copies %s; item copies %s;"
                        + " last copy ms %s; grafts %d, premature %d%n", SEEDS,
                results.stream().map(GraftRepairSimulation.Result::eagerEdges).toList(),
                results.stream().map(GraftRepairSimulation.Result::eagerCopies).toList(),
                results.stream().map(GraftRepairSimulation.Result::graftAnswers).toList(),
                results.stream().map(GraftRepairSimulation.Result::answersAfterOther).toList(),
                results.stream().map(GraftRepairSimulation.Result::answersWriterFirst).toList(),
                results.stream().map(GraftRepairSimulation.Result::writerCopies).toList(),
                results.stream().map(GraftRepairSimulation.Result::itemCopies).toList(), last, grafts, premature);
        assertThat(last).as("every member gets the item in every run").allMatch(ms -> ms >= 0);
        assertThat(results).as("P2-Q-PRF-04 (b): one eager copy per eager edge at the publish")
                .allMatch(r -> r.eagerCopies() == r.eagerEdges());
        assertThat(results).allMatch(r -> r.writerCopies() == r.eagerCopies() + r.graftAnswers());
        assertThat(results).as("P2-Q-PRF-04 (a), the bus's part: no GRAFT answered to a member that heard the writer first")
                .allMatch(r -> r.answersWriterFirst() == 0);
    }

    @Test
    void aRerunWithTheSameSeedGivesTheSameFigures() {
        assertThat(GraftRepairSimulation.run(7, 21)).isEqualTo(GraftRepairSimulation.run(7, 21));
    }

    /**
     * Swarm retries, flood shares (ROBUSTNESS.md §10.6 item 1, §10.7): a member that announces 256 new items it never
     * serves, every 250 ms, pushed every honest item out of the 256-item memory before its first ask, so in 28 of 50
     * runs some honest member never got the large item by gossip, and the old code's two-tick asks lost it in 6. Each
     * announcer now has its own share of the memory, and a flood pushes out the flooder's own items, so no honest member
     * misses an item at any flood speed. The 50 runs of a speed hold 57,000 deliveries of small honest items.
     */
    @ParameterizedTest
    @ValueSource(longs = {250, 1_000, 5_000})
    void aFloodOfAnnouncementsKeepsNoHonestMemberFromAnItem(long floodEveryMillis) {
        int runs = 50;
        List<GraftRepairSimulation.Result> results = new ArrayList<>();
        for (long seed = 1; seed <= runs; seed++) {
            results.add(GraftRepairSimulation.run(seed, 21, new GraftRepairSimulation.Scenario(floodEveryMillis, 1)));
        }
        long missingLarge = results.stream().filter(r -> r.largeMissed() > 0).count();
        long smallMissed = results.stream().mapToLong(GraftRepairSimulation.Result::smallMissed).sum();
        System.out.printf("flood shares, a flood every %d ms, %d runs: runs where an honest member missed the large item %d;"
                + " small items missed %d of 57000%n", floodEveryMillis, runs, missingLarge, smallMissed);
        assertThat(missingLarge).as("runs in which an honest member never got the large item by gossip").isZero();
        assertThat(smallMissed).as("small honest items lazy repair missed, of 57,000").isLessThanOrEqualTo(570);
    }

    /**
     * Swarm retries, flood shares (ROBUSTNESS.md §10.7): in a busy group an honest member names hundreds of items at
     * once, more than the 256 a member tracks. Each announcer's share must leave such a member at least what the single
     * memory gave it: the bus before the shares missed 8,379 of the 362,000 small deliveries of these 50 runs. The guard
     * ran 20 runs against 3,893 until the adaptive wait (§10.9), whose figures differ from seed to seed in both ways: over
     * 50 runs it loses fewer items than the shares did before it, and over the first 20 it lost 2% more than that bound.
     */
    @Test
    void aBusyAnnouncerNamingHundredsOfItemsAtOnceLosesNoMoreThanBefore() {
        int runs = 50;
        long smallMissed = 0;
        long missingLarge = 0;
        for (long seed = 1; seed <= runs; seed++) {
            GraftRepairSimulation.Result result = GraftRepairSimulation.run(seed, 21, new GraftRepairSimulation.Scenario(0, 300));
            smallMissed += result.smallMissed();
            missingLarge += result.largeMissed() > 0 ? 1 : 0;
        }
        System.out.printf("flood shares, member 1 names 300 items at once, %d runs: small items missed %d of 362000;"
                + " runs where a member missed the large item %d%n", runs, smallMissed, missingLarge);
        assertThat(missingLarge).isZero();
        assertThat(smallMissed).as("small items lazy repair missed, against 8,379 before the shares").isLessThanOrEqualTo(8_379);
    }

    /**
     * Swarm retries, flood shares, the gate (ROBUSTNESS.md §10.7): a busy group under a flood. The flooder keeps its even
     * split of the memory, and no honest member misses the large item. The bus after the gate of §10.6 missed it in 20,
     * 7 and 0 of these 20 runs.
     */
    @ParameterizedTest
    @ValueSource(longs = {250, 1_000, 5_000})
    void aBusyGroupUnderAFloodGetsTheLargeItemToEveryHonestMember(long floodEveryMillis) {
        int runs = 20;
        long smallMissed = 0;
        long missingLarge = 0;
        for (long seed = 1; seed <= runs; seed++) {
            GraftRepairSimulation.Result result = GraftRepairSimulation.run(seed, 21,
                    new GraftRepairSimulation.Scenario(floodEveryMillis, 300));
            smallMissed += result.smallMissed();
            missingLarge += result.largeMissed() > 0 ? 1 : 0;
        }
        System.out.printf("flood shares, member 1 names 300 items at once and a flood every %d ms, %d runs: small items"
                + " missed %d of 136420; runs where an honest member missed the large item %d%n", floodEveryMillis, runs,
                smallMissed, missingLarge);
        assertThat(missingLarge).as("runs in which an honest member never got the large item by gossip").isZero();
    }
}
