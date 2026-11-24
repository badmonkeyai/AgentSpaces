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

/**
 * The one time source of a node (srad/agentspaces-event-loop.md §3.1): wall time for anything a peer reads, monotonic
 * time for every local period, and one-shot scheduling on that monotonic base. It is not a wire concept.
 *
 * <p>{@link SystemTime} is the production source, one per process; {@code VirtualTime} in
 * {@code agentspaces-test-support} moves only when a test advances it. Phase 1 of the event-loop work carries the
 * parts the node loop and the flush timer need; phase 3 adds the wall-step signal ({@link #onWallStep}, TS-6),
 * {@link TimeSources} for a plain {@link InstantSource}, and the monitor wait {@link #waitOn} (group W; the
 * {@code Condition} form of §3.1 is not built, since no wait in core waits on a {@code Condition}).
 *
 * <p>Invariants (§3.1):
 * <ul>
 *   <li>TS-1: {@link #millis()} and {@link #instant()} are wall time and may step either way; {@link #monoNanos()}
 *       never goes backwards.</li>
 *   <li>TS-2: a scheduled task runs once, on a virtual thread, never on the timer thread; a cancelled task never
 *       runs, and {@link Scheduled#cancel()} after it ran returns false.</li>
 *   <li>TS-3: tasks due at the same monotonic instant are dispatched in the order they were scheduled.</li>
 *   <li>TS-6: a change of more than {@link #WALL_STEP_THRESHOLD} in (wall - monotonic) between two checks is a wall
 *       step, told to every {@link #onWallStep} listener.</li>
 * </ul>
 */
public interface TimeSource extends InstantSource {

    /**
     * Monotonic nanoseconds: never steps; only differences mean anything.
     *
     * @return the monotonic reading
     */
    long monoNanos();

    /**
     * Runs the task once after the delay, on a virtual thread. A delay of zero or less runs it at once (under virtual
     * time: at the next advance, which may be an advance of zero).
     *
     * @param delay how long from now
     * @param task  the task
     * @return the handle that cancels it
     */
    Scheduled schedule(Duration delay, Runnable task);

    /**
     * Runs the task once at a monotonic deadline; a deadline already past runs at once.
     *
     * @param monoDeadlineNanos the deadline on {@link #monoNanos()}'s base
     * @param task              the task
     * @return the handle that cancels it
     */
    Scheduled scheduleAt(long monoDeadlineNanos, Runnable task);

    /**
     * Runs a node run or a flush once at a monotonic deadline (srad/agentspaces-event-loop.md §3.5, TS-2, decision 2):
     * on the process's {@link SystemTime} the timer hands it to the {@linkplain #loopPool() loop pool}, apart from the
     * application's virtual-thread carriers; a source with no loop pool runs it as {@link #scheduleAt} does. The task
     * must be short: it only nudges a loop or flushes outboxes.
     *
     * @param monoDeadlineNanos the deadline on {@link #monoNanos()}'s base
     * @param task              the task
     * @return the handle that cancels it
     */
    default Scheduled scheduleOnLoop(long monoDeadlineNanos, Runnable task) {
        return scheduleAt(monoDeadlineNanos, task);
    }

    /**
     * The elastic loop pool node runs and flushes execute on (decision 2, EL-O-02), or null when this source has none
     * (a virtual source runs them as tasks of its own, so an advance sees them finish).
     *
     * @return the pool, or null
     */
    default LoopPool loopPool() {
        return null;
    }

    /**
     * Sleeps on this source's monotonic time; virtual time releases the sleeper when an advance passes its deadline.
     *
     * @param duration how long
     * @throws InterruptedException when interrupted
     */
    void sleep(Duration duration) throws InterruptedException;

    /**
     * Waits on a monitor the caller holds until it is notified or the monotonic deadline passes (srad/agentspaces-event-loop.md
     * §3.1, group W): {@code Object.wait} with a deadline on this source's base, so a virtual source releases the waiter
     * when an advance passes the deadline. As with {@code Object.wait}, a return before the deadline may be a notify or
     * a spurious wake; the caller re-checks its condition in a loop.
     *
     * <p>The default waits real time ({@link #monoNanos()} read before and after), which is right for a source whose
     * monotonic base moves with real time.
     *
     * @param monitor           the monitor, held by the caller
     * @param monoDeadlineNanos the deadline on {@link #monoNanos()}'s base
     * @return true when it returned before the deadline, false when the deadline had passed
     * @throws InterruptedException when interrupted while waiting
     */
    default boolean waitOn(Object monitor, long monoDeadlineNanos) throws InterruptedException {
        long left = monoDeadlineNanos - monoNanos();
        if (left <= 0) {
            return false;
        }
        long millis = left / 1_000_000L;
        int nanos = (int) (left % 1_000_000L);
        monitor.wait(millis, nanos); // wait(0, 0) would wait forever; left > 0 keeps at least one of them positive
        return monoDeadlineNanos - monoNanos() > 0;
    }

    /**
     * True when time moves only by a test's advance, so no time passes without a signal.
     *
     * @return whether this source is virtual
     */
    boolean virtual();

    /**
     * Registers a listener told when wall time moved against monotonic time by more than
     * {@link #WALL_STEP_THRESHOLD} between two of this source's checks (TS-6, srad/agentspaces-event-loop.md §3.1,
     * EL-C-12): an NTP step either way, or a suspend, which the monotonic clock does not count. The listener runs once
     * per step, never on the timer thread, with the step's signed size (positive when wall time moved ahead). A slew is
     * not a step. A source that cannot see steps (the default) never calls it.
     *
     * @param step the listener
     * @return closes the registration
     */
    default AutoCloseable onWallStep(java.util.function.Consumer<Duration> step) {
        java.util.Objects.requireNonNull(step, "step");
        return () -> { };
    }

    /** The smallest move of wall time against monotonic time that counts as a wall step (TS-6). */
    Duration WALL_STEP_THRESHOLD = Duration.ofSeconds(1);

    /**
     * Registers a listener told when this source's timer woke more than {@link #LATE_WAKE_THRESHOLD} after the
     * deadline it slept to, and no wall step was raised at that wake (SPEC v0.1.37 §9.4, the wake rule): the process
     * was frozen (SIGSTOP, a long GC or VM pause), or its monotonic clock counted a sleep. Wall and monotonic time moved
     * together, so {@link #onWallStep} sees nothing, yet every peer went on without this process: it is a wake. The
     * listener runs once per late wake, never on the timer thread, with how late the wake was. A source that cannot see
     * its own lateness (the default) never calls it.
     *
     * @param late the listener
     * @return closes the registration
     */
    default AutoCloseable onLateWake(java.util.function.Consumer<Duration> late) {
        java.util.Objects.requireNonNull(late, "late");
        return () -> { };
    }

    /**
     * How late a timer wake must be, past its deadline and with no wall step, to count as a wake: the 30 s keepalive
     * bound, the same bound a forward wall step must pass (SPEC v0.1.37 §9.4).
     */
    Duration LATE_WAKE_THRESHOLD = Duration.ofSeconds(30);

    /**
     * Monotonic milliseconds on this source's base: {@link #monoNanos()} in whole milliseconds, the base every local
     * period of a node is reckoned on (srad/agentspaces-event-loop.md §3.6). Only differences mean anything.
     *
     * @return the reading
     */
    default long monoMillis() {
        return Math.floorDiv(monoNanos(), 1_000_000L);
    }

    /**
     * The watchdog that judges loops on this source (srad/agentspaces-event-loop.md §3.9): the process watchdog for
     * {@link SystemTime}, a virtual-time watchdog for {@code VirtualTime}, and by default {@link Watchdog#none()},
     * because a watchdog must judge deadlines on the same monotonic base its loops arm them on.
     *
     * @return the watchdog
     */
    default Watchdog watchdog() {
        return Watchdog.none();
    }

    /**
     * The process's system time source.
     *
     * @return the one {@link SystemTime}
     */
    static TimeSource system() {
        return SystemTime.INSTANCE;
    }

    /** A scheduled task. */
    interface Scheduled {
        /**
         * Cancels the task.
         *
         * @return true when this call stopped it from running; false when it ran, is running, or was cancelled
         */
        boolean cancel();

        /**
         * The monotonic deadline the task was scheduled for.
         *
         * @return the deadline
         */
        long dueNanos();

        /**
         * Whether the task has been handed to its thread (it may not have begun to run yet): a deadline that fired
         * and waits for a carrier is lateness, not a missed fire (§3.9).
         *
         * @return whether it was dispatched
         */
        default boolean dispatched() {
            return false;
        }
    }
}
