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
package ai.badmonkey.agentspaces.content.tree;

import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PD-X-05: the source rules the piece fetch and the tree fetch share, stated once.
 */
class SourceRankingTest {

    private static final PeerId A = PeerId.of("zA");
    private static final PeerId B = PeerId.of("zB");
    private static final PeerId C = PeerId.of("zC");
    private static final PeerId D = PeerId.of("zD");

    @Test
    void orderIsPreferredThenHopsThenThroughputAndStable() {
        SourceRanking.Source a = new SourceRanking.Source(A);
        SourceRanking.Source b = new SourceRanking.Source(B);
        SourceRanking.Source c = new SourceRanking.Source(C);
        SourceRanking.Source d = new SourceRanking.Source(D);
        a.hops = 2;
        b.hops = 0;
        b.bytesReceived = 100;
        b.srttMillis = 100;
        c.hops = 0;
        c.bytesReceived = 1000;
        c.srttMillis = 100;
        d.hops = -1;
        assertThat(new SourceRanking(Set.of()).order(List.of(a, b, c, d))).containsExactly(c, b, a, d);
        assertThat(new SourceRanking(Set.of(A)).order(List.of(a, b, c, d))).containsExactly(a, c, b, d);
        // ties keep insertion order
        SourceRanking.Source e = new SourceRanking.Source(PeerId.of("zE"));
        SourceRanking.Source f = new SourceRanking.Source(PeerId.of("zF"));
        assertThat(new SourceRanking(null).order(List.of(e, f))).containsExactly(e, f);
        assertThat(new SourceRanking(null).order(List.of(f, e))).containsExactly(f, e);
    }

    @Test
    void windowGrowsOnAnswersHalvesOnTimeoutsAndThreeTimeoutsDropForSixtySeconds() {
        SourceRanking.Source s = new SourceRanking.Source(A);
        assertThat(s.window).isEqualTo(SourceRanking.INITIAL_WINDOW);
        for (int i = 0; i < 10; i++) {
            s.inFlight++;
            s.answered(50, 10);
            s.grow(8);
        }
        assertThat(s.window).isEqualTo(8);
        assertThat(s.successes).isEqualTo(10);
        assertThat(s.inFlight).isZero();
        assertThat(s.srttMillis).isEqualTo(50);
        assertThat(s.bytesReceived).isEqualTo(100);
        assertThat(s.throughput()).isEqualTo(100 * 1000 / 50);

        s.inFlight = 3;
        assertThat(s.timedOut(1_000)).isFalse();
        assertThat(s.window).isEqualTo(4);
        assertThat(s.successes).isZero();
        assertThat(s.inFlight).isEqualTo(2);
        assertThat(s.timedOut(1_000)).isFalse();
        assertThat(s.window).isEqualTo(2);
        assertThat(s.timedOut(1_000)).as("the third consecutive timeout drops the source").isTrue();
        assertThat(s.window).isEqualTo(1);
        assertThat(s.dropped(1_000 + SourceRanking.SOURCE_DROP_MILLIS - 1)).isTrue();
        assertThat(s.dropped(1_000 + SourceRanking.SOURCE_DROP_MILLIS)).isFalse();
        assertThat(s.consecutiveTimeouts).as("reset by the drop").isZero();
        s.inFlight = 1;
        s.answered(10, 0);
        assertThat(s.consecutiveTimeouts).isZero();
        assertThat(s.window).isEqualTo(1);
        assertThat(s.timedOut(5_000)).isFalse();
        assertThat(s.window).as("never below one").isEqualTo(1);
    }

    @Test
    void timeoutIsTheLargerOfTenSecondsAndEightRoundTrips() {
        SourceRanking.Source s = new SourceRanking.Source(A);
        assertThat(s.timeoutAllowanceMillis()).isEqualTo(10_000);
        s.srttMillis = 500;
        assertThat(s.timeoutAllowanceMillis()).isEqualTo(10_000);
        s.srttMillis = 2_000;
        assertThat(s.timeoutAllowanceMillis()).isEqualTo(16_000);
        assertThat(s.timedOut(0, 16_000)).isFalse();
        assertThat(s.timedOut(0, 16_001)).isTrue();
        // the smoothed estimate is seven eighths old, one eighth new
        s.srttMillis = 800;
        s.inFlight = 1;
        s.answered(0, 0);
        assertThat(s.srttMillis).isEqualTo(700);
        s.inFlight = 1;
        s.progressed(1_500, 7);
        assertThat(s.srttMillis).isEqualTo(800);
        assertThat(s.inFlight).as("progress keeps the request in flight").isEqualTo(1);
        assertThat(s.bytesReceived).isEqualTo(7);
    }

    @Test
    void rejectBackOffDoublesPerStrikeUpToSixteenTimesAndStrikesDropForTheFetch() {
        SourceRanking.Source s = new SourceRanking.Source(A);
        long[] expected = {60_000, 120_000, 240_000, 480_000, 960_000, 960_000};
        for (int i = 0; i < expected.length; i++) {
            s.backOff(0);
            assertThat(s.droppedUntilMillis).as("strike " + i).isEqualTo(expected[i]);
            assertThat(s.strikes).isEqualTo(i + 1);
        }
        SourceRanking.Source t = new SourceRanking.Source(B);
        assertThat(t.unopenable()).isFalse();
        assertThat(t.unopenable()).isFalse();
        assertThat(t.unopenable()).isTrue();
        assertThat(t.dropped(Long.MAX_VALUE - 1)).isTrue();
        SourceRanking.Source u = new SourceRanking.Source(C);
        u.dropForever();
        assertThat(u.strikes).isEqualTo(1);
        assertThat(u.dropped(Long.MAX_VALUE - 1)).isTrue();
    }

    @Test
    void fillSamplesTheWindowWhileWanted() {
        SourceRanking.Source s = new SourceRanking.Source(A);
        assertThat(s.fill()).isEmpty();
        s.window = 4;
        s.inFlight = 2;
        s.sampleFill();
        s.inFlight = 4;
        s.sampleFill();
        s.inFlight = 8;
        s.sampleFill();
        assertThat(s.fill()).hasValue((0.5 + 1.0 + 1.0) / 3);
    }
}
