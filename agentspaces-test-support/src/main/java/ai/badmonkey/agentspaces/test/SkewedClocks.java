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
package ai.badmonkey.agentspaces.test;

import java.time.Duration;
import java.time.InstantSource;

/**
 * The skewed clocks of probe P38 (reviews/RBE-REGRESSION-MATRIX.md), one fixture for every module's
 * {@code HorizonLaggingCopyTest} (srad/agentspaces-event-loop.md EL-A-04; it was five diverged copies).
 *
 * <p>Three clocks, all read from one hand-moved {@link TestClock}:
 * <ul>
 *   <li>{@link #reference()}: the actor's, the clock the test moves.</li>
 *   <li>{@link #bridging()}: a bridge {@link #LAG} behind the reference throughout. A replica whose clock trails the
 *   actor's by more than the five-minute frame age is heard by the actor only while its frames carry newer stamps than
 *   its own clock reads: stamps its hybrid logical clock took from members it hears. Cut off together with the bridge,
 *   the lagging replica's stamps follow the bridge's, within five minutes of the actor's, so when a partition heals the
 *   actor hears it again. Alone, its stamps would fall {@link #DRIFTED_LAG} behind and the actor would drop its frames
 *   as too old.</li>
 *   <li>{@link #lagging()}: a replica that starts {@link #LAG} behind and, after {@link #driftTo(Duration)}, drifts
 *   toward a larger lag by half of each {@link #advance(Duration) step}, so it slows as a drifting clock does and never
 *   steps back.</li>
 * </ul>
 *
 * <p>Not thread-safe for writers: one test thread advances and drifts; any thread may read.
 */
public final class SkewedClocks {

    /** The starting lag: inside the five-minute frame age every receiver allows (PeerNode's MAX_FRAME_AGE_MILLIS). */
    public static final Duration LAG = Duration.ofMinutes(4);

    /**
     * The lag the drifted clock reaches while it is cut off: past the five-minute lapse grace and within MAX_DRIFT (ten
     * minutes, SPEC §11). Since the drift floor of SPEC §7.3 an actor collects a withdrawal ten minutes past its lease,
     * and a replica still four minutes behind has dropped its own lapsed copy a minute before that, so nothing is left
     * to re-offer. One that has drifted to eight minutes behind still holds the copy within its grace, and offers it
     * once the healed link has carried the actor's stamps to its clock.
     */
    public static final Duration DRIFTED_LAG = Duration.ofMinutes(8);

    private final TestClock reference;
    private final long bridgeLagMillis;
    private volatile long lagMillis;
    private volatile long lagTargetMillis;
    private final InstantSource bridging;
    private final InstantSource lagging;

    private SkewedClocks(TestClock reference, Duration lag) {
        this.reference = reference;
        this.bridgeLagMillis = lag.toMillis();
        this.lagMillis = lag.toMillis();
        this.lagTargetMillis = lag.toMillis();
        this.bridging = () -> reference.instant().minusMillis(bridgeLagMillis);
        this.lagging = () -> reference.instant().minusMillis(lagMillis);
    }

    /**
     * The probe's clocks: a fresh {@link TestClock}, a bridge and a lagging replica both {@link #LAG} behind it.
     *
     * @return the fixture
     */
    public static SkewedClocks create() {
        return new SkewedClocks(TestClock.create(), LAG);
    }

    /** @return the actor's clock, the one {@link #advance(Duration)} moves */
    public TestClock reference() {
        return reference;
    }

    /** @return the bridge's clock, a fixed lag behind the reference */
    public InstantSource bridging() {
        return bridging;
    }

    /** @return the lagging replica's clock, whose lag drifts toward the target set by {@link #driftTo(Duration)} */
    public InstantSource lagging() {
        return lagging;
    }

    /**
     * Sets the lag the lagging clock drifts to from the next {@link #advance(Duration)} on. A smaller target than the
     * lag now is refused: the drifting clock never runs back.
     *
     * @param target the lag to reach
     */
    public void driftTo(Duration target) {
        long t = target.toMillis();
        if (t < lagMillis) {
            throw new IllegalArgumentException("a drifting clock never runs back: lag " + lagMillis + " ms, target "
                    + t + " ms");
        }
        lagTargetMillis = t;
    }

    /**
     * Moves the reference by {@code step}; the lagging clock moves by half the step while its lag is under the target,
     * so its lag grows by the other half, up to the target.
     *
     * @param step how far the reference moves
     */
    public void advance(Duration step) {
        reference.advance(step);
        lagMillis = Math.min(lagTargetMillis, lagMillis + step.toMillis() / 2);
    }

    /** @return the lagging clock's lag now, in milliseconds */
    public long lagMillis() {
        return lagMillis;
    }
}
