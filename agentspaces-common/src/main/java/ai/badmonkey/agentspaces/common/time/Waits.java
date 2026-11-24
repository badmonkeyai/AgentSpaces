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

import java.time.Duration;
import java.time.InstantSource;
import java.util.Objects;

/**
 * The one take-wait rule and the base every wait is judged on (srad/agentspaces-event-loop.md group W, EL-W-07).
 *
 * <p><b>The rule.</b> A waiting take or read waits for a change or for the next time something it holds falls due, and
 * never longer than {@link #BACKSTOP} between looks. The backstop is a safety net for a change that forgot to signal,
 * never a scheduler. It replaces the binder's gated 1 s cap and ungated 30 s wait and the space's 30 s and 1 s slices.
 *
 * <p><b>The base.</b> A wait's deadline and the wait itself are reckoned on one source ({@link #base}, EL-C-10): a time
 * source of its own (the system's, or a virtual one a test advances), or the system's for a plain clock, whose wall time
 * a test moves by hand and which therefore cannot time a wait.
 */
public final class Waits {

    /** The one backstop between looks of a waiting take or read (EL-W-07). */
    public static final Duration BACKSTOP = Duration.ofSeconds(30);

    /**
     * The look a waiter on a plain hand-moved clock keeps (a compatibility rule, srad/agentspaces-event-loop.md §10.17):
     * a test that moves a {@code TestClock} by hand signals nobody, so a lease it lapses by hand is seen within this.
     */
    public static final Duration HAND_CLOCK_LOOK = Duration.ofSeconds(1);

    private Waits() {
    }

    /**
     * The source a wait is reckoned and parked on, for a component built on {@code clock}: the clock itself when it is a
     * time source of its own (the system's, a real or virtual one, or a node view of one), else the system's.
     *
     * @param clock the component's clock
     * @return the source its waits use
     */
    public static TimeSource base(InstantSource clock) {
        Objects.requireNonNull(clock, "clock");
        if (clock instanceof TimeSource source && !TimeSources.followsWall(source)) {
            return source;
        }
        return TimeSource.system();
    }

    /**
     * Whether nothing but a test's hand moves the clock, so time passing signals no waiter: a plain clock that is not the
     * system's, or a source that follows one.
     *
     * @param clock the clock
     * @return whether it is moved by hand
     */
    public static boolean movedByHand(InstantSource clock) {
        Objects.requireNonNull(clock, "clock");
        if (clock instanceof TimeSource source) {
            return TimeSources.followsWall(source);
        }
        return !TimeSources.isSystemClock(clock);
    }

    /**
     * A monotonic deadline {@code span} from now on the source, saturated.
     *
     * @param time the source
     * @param span how far
     * @return the deadline
     */
    public static long deadline(TimeSource time, Duration span) {
        long nanos = span.isNegative() ? 0 : saturated(span);
        try {
            return Math.addExact(time.monoNanos(), nanos);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Waits on the source for a future to complete, or for the monotonic deadline to pass (srad/agentspaces-event-loop.md
     * group W): a virtual source releases the waiter when an advance passes the deadline, a real one after real time.
     * The future's completion notifies the waiter at once.
     *
     * @param time              the source the wait is reckoned and parked on (see {@link #base})
     * @param future            the future
     * @param monoDeadlineNanos the deadline on the source's monotonic base
     * @return whether the future completed
     * @throws InterruptedException when interrupted while waiting
     */
    public static boolean awaitDone(TimeSource time, java.util.concurrent.CompletableFuture<?> future,
                                    long monoDeadlineNanos) throws InterruptedException {
        Objects.requireNonNull(time, "time");
        Objects.requireNonNull(future, "future");
        if (future.isDone()) {
            return true;
        }
        Object monitor = new Object();
        future.whenComplete((value, failure) -> {
            synchronized (monitor) {
                monitor.notifyAll();
            }
        });
        synchronized (monitor) {
            while (!future.isDone()) {
                if (monoDeadlineNanos - time.monoNanos() <= 0) {
                    return false;
                }
                time.waitOn(monitor, monoDeadlineNanos);
            }
        }
        return true;
    }

    private static long saturated(Duration span) {
        try {
            return span.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }
}
