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
package ai.badmonkey.agentspaces.common.time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The wall-step signal (TS-6, srad/agentspaces-event-loop.md EL-C-12) on {@link SystemTime}, judged from synthetic
 * readings, and {@link TimeSources}' rules for turning a clock into a node's time source (EL-C-06). The class timeout
 * is a hang guard only.
 */
@Timeout(30)
class WallStepTest {

    private static final long SECOND = 1_000_000_000L;

    @Test
    void aMoveOfMoreThanOneSecondEitherWayIsAStepAndASlewIsNot() throws Exception {
        SystemTime time = SystemTime.isolated("wall-step-test");
        List<Duration> told = new CopyOnWriteArrayList<>();
        CountDownLatch two = new CountDownLatch(2);
        try (AutoCloseable ignored = time.onWallStep(step -> {
            told.add(step);
            two.countDown();
        })) {
            long wall = 1_000_000_000_000L;
            long mono = 5 * SECOND;
            time.primeWallReading(wall, mono); // the reading steps are judged from
            // 10 s of both: nothing
            assertThat(time.checkWallStep(wall + 10_000, mono + 10 * SECOND)).isNull();
            // a slew of 500 ppm over 10 minutes is 300 ms: not a step
            assertThat(time.checkWallStep(wall + 610_300, mono + 610 * SECOND)).isNull();
            // an NTP step back of 5 minutes
            assertThat(time.checkWallStep(wall + 310_300, mono + 611 * SECOND))
                    .isEqualTo(Duration.ofMillis(-301_000));
            // a suspend of 2 hours: wall moved on, the monotonic clock did not count it
            assertThat(time.checkWallStep(wall + 310_300 + 7_200_000 + 1_000, mono + 612 * SECOND))
                    .isEqualTo(Duration.ofHours(2));
            assertThat(two.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(told).containsExactlyInAnyOrder(Duration.ofMillis(-301_000), Duration.ofHours(2));
            assertThat(time.wallSteps()).isEqualTo(2);
        }
    }

    @Test
    void aListenerRunsOffTheTimerThreadAndAClosedOneIsNotTold() throws Exception {
        SystemTime time = SystemTime.isolated("wall-step-thread-test");
        AtomicLong calls = new AtomicLong();
        CountDownLatch told = new CountDownLatch(1);
        Thread[] on = new Thread[1];
        AutoCloseable kept = time.onWallStep(step -> {
            on[0] = Thread.currentThread();
            calls.incrementAndGet();
            told.countDown();
        });
        AutoCloseable dropped = time.onWallStep(step -> calls.addAndGet(1_000));
        dropped.close();
        time.primeWallReading(0, 0);
        time.checkWallStep(5_000, 0);
        assertThat(told.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(on[0].isVirtual()).isTrue();
        assertThat(calls.get()).isEqualTo(1);
        kept.close();
    }

    /**
     * SPEC v0.1.37 §9.4: a timer wake more than 30 s past the instant it slept to, with no wall step at that wake, is a
     * late wake (a frozen process); one within 30 s, or one that raised a wall step, is not. Listeners run off the timer.
     */
    @Test
    void aTimerWakeMoreThanThirtySecondsLateWithNoWallStepIsALateWake() throws Exception {
        SystemTime time = SystemTime.isolated("late-wake-test");
        List<Duration> told = new CopyOnWriteArrayList<>();
        CountDownLatch once = new CountDownLatch(1);
        Thread[] on = new Thread[1];
        try (AutoCloseable ignored = time.onLateWake(late -> {
            on[0] = Thread.currentThread();
            told.add(late);
            once.countDown();
        })) {
            assertThat(time.checkLateWake(Long.MAX_VALUE, 90 * SECOND, false)).as("it slept to no deadline").isNull();
            assertThat(time.checkLateWake(SECOND, 31 * SECOND, false)).as("30 s late: inside the bound").isNull();
            assertThat(time.checkLateWake(SECOND, 61 * SECOND, true)).as("a wall step is the wake itself").isNull();
            assertThat(time.checkLateWake(SECOND, 61 * SECOND, false)).isEqualTo(Duration.ofSeconds(60));
            assertThat(once.await(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(told).containsExactly(Duration.ofSeconds(60));
        assertThat(on[0].isVirtual()).as("never on the timer thread").isTrue();
        assertThat(time.lateWakes()).isEqualTo(1);
    }

    @Test
    void aRealTimerWakeComparesTheReadings() throws Exception {
        // The timer checks on each wake while anyone listens; with the real clocks there is no step to see, so the
        // check runs and raises nothing (a real step cannot be made in a test).
        SystemTime time = SystemTime.isolated("wall-step-wake-test");
        List<Duration> told = new CopyOnWriteArrayList<>();
        try (AutoCloseable ignored = time.onWallStep(told::add)) {
            CountDownLatch ran = new CountDownLatch(3);
            for (int i = 1; i <= 3; i++) {
                time.schedule(Duration.ofMillis(5L * i), ran::countDown);
            }
            assertThat(ran.await(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(told).isEmpty();
        assertThat(time.wallSteps()).isZero();
    }

    @Test
    void theSystemClockInAnyFormIsTheSystemTimeSource() {
        assertThat(TimeSources.of(InstantSource.system())).isSameAs(TimeSource.system());
        assertThat(TimeSources.of(Clock.systemUTC())).isSameAs(TimeSource.system());
        assertThat(TimeSources.of(Clock.system(ZoneId.of("Asia/Tokyo")))).isSameAs(TimeSource.system());
        assertThat(TimeSources.of(TimeSource.system())).isSameAs(TimeSource.system());
        assertThat(TimeSources.isSystemClock(Clock.offset(Clock.systemUTC(), Duration.ofHours(1)))).isFalse();
    }

    @Test
    void aPlainClockIsFollowedForWallAndLocalTimeAndNeverGoesBackwards() throws Exception {
        long[] wall = {1_000_000L};
        InstantSource plain = new InstantSource() {
            @Override
            public Instant instant() {
                return Instant.ofEpochMilli(wall[0]);
            }
        };
        TimeSource source = TimeSources.of(plain);
        assertThat(TimeSources.followsWall(source)).isTrue();
        assertThat(TimeSources.wallOf(source)).isSameAs(plain);
        assertThat(source.millis()).isEqualTo(1_000_000L);
        assertThat(source.monoMillis()).isEqualTo(1_000_000L);
        wall[0] += 45_000;
        assertThat(source.monoMillis()).isEqualTo(1_045_000L); // a test that moves its clock moves local periods
        wall[0] -= 300_000;
        assertThat(source.millis()).isEqualTo(745_000L);
        assertThat(source.monoMillis()).isEqualTo(1_045_000L); // held, never backwards (TS-1)
        assertThat(source.watchdog()).isSameAs(Watchdog.none());
        // a deadline on its base waits its distance on the system source
        CountDownLatch ran = new CountDownLatch(1);
        source.scheduleAt(source.monoNanos() + 10_000_000L, ran::countDown);
        assertThat(ran.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void aSeparateWallClockBesideATimeSourceKeepsTheSourcesMonotonicBase() {
        InstantSource wall = Clock.fixed(Instant.ofEpochMilli(42), ZoneId.of("UTC"));
        TimeSource composite = TimeSources.withWall(wall, TimeSource.system());
        assertThat(composite.millis()).isEqualTo(42);
        long before = System.nanoTime();
        long mono = composite.monoNanos();
        assertThat(mono).isBetween(before, System.nanoTime());
        assertThat(TimeSources.withWall(TimeSource.system(), TimeSource.system())).isSameAs(TimeSource.system());
    }
}
