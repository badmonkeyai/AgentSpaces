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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Turns the {@link InstantSource} a caller passed into the one {@link TimeSource} a node runs on
 * (srad/agentspaces-event-loop.md §3.1, EL-C-06).
 *
 * <ul>
 *   <li>A {@link TimeSource} is itself.</li>
 *   <li>The system clock ({@link InstantSource#system()}, {@link Clock#systemUTC()} or a system clock in any zone) is
 *       {@link TimeSource#system()}, so a caller that passed it gets monotonic local periods and the wall-step
 *       signal.</li>
 *   <li>Any other plain {@link InstantSource} (a test's {@code TestClock}, an offset clock) is a <em>wall-following</em>
 *       source ({@link #followingWall}): wall time is the clock's, and the monotonic base follows the clock as well,
 *       held from going backwards, so a hand-driven test that moves its clock moves every local period with it, as
 *       before phase 3. Scheduling converts each deadline to a delay on the system source. Such a source has no
 *       watchdog and raises no wall step.</li>
 * </ul>
 */
public final class TimeSources {

    private TimeSources() {
    }

    /**
     * The time source for a clock (see the class comment).
     *
     * @param clock the clock
     * @return the source
     */
    public static TimeSource of(InstantSource clock) {
        Objects.requireNonNull(clock, "clock");
        if (clock instanceof TimeSource source) {
            return source;
        }
        if (isSystemClock(clock)) {
            return TimeSource.system();
        }
        return followingWall(clock, TimeSource.system());
    }

    /**
     * Whether the clock reads the system's wall time with no offset: {@link InstantSource#system()},
     * {@link Clock#systemUTC()}, a system clock in any zone, or {@link TimeSource#system()}.
     *
     * @param clock the clock
     * @return whether it is the system clock
     */
    public static boolean isSystemClock(InstantSource clock) {
        if (clock == TimeSource.system() || clock.equals(InstantSource.system())) {
            return true;
        }
        return clock instanceof Clock c && c.getClass() == Clock.systemUTC().getClass();
    }

    /**
     * Whether the source's monotonic base follows a plain wall clock ({@link #followingWall}), so its local periods move
     * only when that clock does.
     *
     * @param source the source
     * @return whether it follows a wall clock
     */
    public static boolean followsWall(TimeSource source) {
        return source instanceof FollowingWall;
    }

    /**
     * The plain clock a wall-following source reads, or the source itself.
     *
     * @param source the source
     * @return the wall clock
     */
    public static InstantSource wallOf(TimeSource source) {
        return source instanceof FollowingWall f ? f.wall : source;
    }

    /**
     * A source whose wall time and monotonic base both follow a plain clock (the base held from going backwards), with
     * scheduling and sleeps converted to delays on another source (see the class comment).
     *
     * @param wall the clock
     * @param base where tasks and sleeps really wait
     * @return the source
     */
    public static TimeSource followingWall(InstantSource wall, TimeSource base) {
        return new FollowingWall(Objects.requireNonNull(wall, "wall"), Objects.requireNonNull(base, "base"));
    }

    /**
     * A source whose wall time is a clock's and whose monotonic base, scheduling, sleeps, wall steps and watchdog are
     * another source's, for a caller that passes a separate wall clock beside a time source.
     *
     * @param wall the wall clock
     * @param base the rest
     * @return the source
     */
    public static TimeSource withWall(InstantSource wall, TimeSource base) {
        Objects.requireNonNull(wall, "wall");
        Objects.requireNonNull(base, "base");
        if (wall == base) {
            return base;
        }
        return new TimeSource() {
            @Override
            public Instant instant() {
                return wall.instant();
            }

            @Override
            public long millis() {
                return wall.millis();
            }

            @Override
            public long monoNanos() {
                return base.monoNanos();
            }

            @Override
            public Scheduled schedule(Duration delay, Runnable task) {
                return base.schedule(delay, task);
            }

            @Override
            public Scheduled scheduleAt(long monoDeadlineNanos, Runnable task) {
                return base.scheduleAt(monoDeadlineNanos, task);
            }

            @Override
            public Scheduled scheduleOnLoop(long monoDeadlineNanos, Runnable task) {
                return base.scheduleOnLoop(monoDeadlineNanos, task);
            }

            @Override
            public LoopPool loopPool() {
                return base.loopPool();
            }

            @Override
            public void sleep(Duration duration) throws InterruptedException {
                base.sleep(duration);
            }

            @Override
            public boolean waitOn(Object monitor, long monoDeadlineNanos) throws InterruptedException {
                return base.waitOn(monitor, monoDeadlineNanos);
            }

            @Override
            public boolean virtual() {
                return base.virtual();
            }

            @Override
            public AutoCloseable onWallStep(java.util.function.Consumer<Duration> step) {
                return base.onWallStep(step);
            }

            @Override
            public AutoCloseable onLateWake(java.util.function.Consumer<Duration> late) {
                return base.onLateWake(late);
            }

            @Override
            public Watchdog watchdog() {
                return base.watchdog();
            }
        };
    }

    /** See {@link #followingWall}. */
    private static final class FollowingWall implements TimeSource {
        private final InstantSource wall;
        private final TimeSource base;
        private final AtomicLong last = new AtomicLong(Long.MIN_VALUE);

        FollowingWall(InstantSource wall, TimeSource base) {
            this.wall = wall;
            this.base = base;
        }

        @Override
        public Instant instant() {
            return wall.instant();
        }

        @Override
        public long millis() {
            return wall.millis();
        }

        @Override
        public long monoNanos() {
            long now;
            try {
                now = Math.multiplyExact(wall.millis(), 1_000_000L);
            } catch (ArithmeticException e) {
                now = Long.MAX_VALUE - 1;
            }
            long n = now;
            return last.accumulateAndGet(n, Math::max); // never backwards (TS-1)
        }

        @Override
        public Scheduled schedule(Duration delay, Runnable task) {
            return base.schedule(delay, task);
        }

        @Override
        public Scheduled scheduleAt(long monoDeadlineNanos, Runnable task) {
            return convert(monoDeadlineNanos, task, false);
        }

        @Override
        public Scheduled scheduleOnLoop(long monoDeadlineNanos, Runnable task) {
            return convert(monoDeadlineNanos, task, true);
        }

        @Override
        public LoopPool loopPool() {
            return base.loopPool();
        }

        private Scheduled convert(long monoDeadlineNanos, Runnable task, boolean onLoop) {
            if (monoDeadlineNanos == Long.MAX_VALUE) {
                return base.scheduleAt(Long.MAX_VALUE, task);
            }
            long delay = monoDeadlineNanos - monoNanos();
            Scheduled inner;
            if (onLoop) {
                long baseNow = base.monoNanos();
                long at;
                try {
                    at = Math.addExact(baseNow, Math.max(0, delay));
                } catch (ArithmeticException e) {
                    at = Long.MAX_VALUE - 1;
                }
                inner = base.scheduleOnLoop(at, task);
            } else {
                inner = base.schedule(Duration.ofNanos(Math.max(0, delay)), task);
            }
            return new Scheduled() {
                @Override
                public boolean cancel() {
                    return inner.cancel();
                }

                @Override
                public long dueNanos() {
                    return monoDeadlineNanos;
                }

                @Override
                public boolean dispatched() {
                    return inner.dispatched();
                }
            };
        }

        @Override
        public void sleep(Duration duration) throws InterruptedException {
            base.sleep(duration);
        }

        /** The deadline converted to a delay on the base, as {@link #scheduleAt} converts it. */
        @Override
        public boolean waitOn(Object monitor, long monoDeadlineNanos) throws InterruptedException {
            long left = monoDeadlineNanos - monoNanos();
            if (left <= 0) {
                return false;
            }
            long onBase;
            try {
                onBase = Math.addExact(base.monoNanos(), left);
            } catch (ArithmeticException e) {
                onBase = Long.MAX_VALUE;
            }
            return base.waitOn(monitor, onBase);
        }

        @Override
        public boolean virtual() {
            return false;
        }
    }

    /** The furthest ahead a converted deadline is filed: a wall step re-asks every participant anyway (EL-C-12). */
    public static final long MAX_DUE_AHEAD_MILLIS = 3_600_000L;

    /**
     * A wall deadline as a deadline on the monotonic base, converted against both clocks read together, now, and
     * filed at most {@link #MAX_DUE_AHEAD_MILLIS} ahead (srad/agentspaces-event-loop.md §3.6, §10.8). The one
     * converter every registrant whose leases and expiries are wall instants uses.
     *
     * @param monoNowNanos   the monotonic now
     * @param wallNowMillis  the wall now, read with it
     * @param deadlineMillis the wall deadline, or {@link Long#MAX_VALUE} for none
     * @return the deadline on the monotonic base: {@code monoNowNanos} when it has passed, {@link Long#MAX_VALUE}
     *         for none
     */
    public static long monoDue(long monoNowNanos, long wallNowMillis, long deadlineMillis) {
        if (deadlineMillis == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        long ahead = deadlineMillis - wallNowMillis;
        if (ahead <= 0) {
            return monoNowNanos;
        }
        return monoNowNanos + Math.min(ahead, MAX_DUE_AHEAD_MILLIS) * 1_000_000L;
    }
}
