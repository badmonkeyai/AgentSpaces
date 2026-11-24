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

/**
 * Everything the {@link Watchdog} judges (srad/agentspaces-event-loop.md §3.9, EL-H-02): a node loop, a flush timer,
 * the timer thread. Every method reads volatile state and takes no lock (WD-1, NL-9), so the watchdog can judge a
 * loop whose own locks are held by a wedged run.
 */
public interface Watched {

    /** Why the watchdog re-initialises a watched thing, and the kind it is counted under. */
    enum Reason {
        /** Not running, no run since the last tick, and its armed deadline plus the grace has passed. */
        MISSED_FIRE("missed-fire"),
        /** A wake is pending, nothing is running or dispatched, and no run started within the grace of the wake. */
        MISSED_WAKE("missed-wake"),
        /** A run has gone on past the wedge bound. */
        WEDGED("wedged"),
        /** The thread of a run marked started is not alive, or the timer thread died with deadlines queued. */
        DEAD("dead"),
        /** Work has waited on a dispatched drain that never started for the wedge bound. */
        STUCK_DISPATCH("stuck-dispatch");

        private final String id;

        Reason(String id) {
            this.id = id;
        }

        /**
         * The kind's id in counters, logs and the measurements store.
         *
         * @return the id
         */
        public String id() {
            return id;
        }
    }

    /**
     * The thing's name: {@code node:<peer>}, {@code flush:<peer>}, {@code timer}.
     *
     * @return the name
     */
    String name();

    /**
     * Completed runs.
     *
     * @return the count
     */
    long runs();

    /**
     * When the run in progress started, on the time source's monotonic base; 0 when none runs.
     *
     * @return the start
     */
    long runStartedNanos();

    /**
     * When the run in progress last made progress, on the time source's monotonic base: the start of the step it is
     * in now (a node loop's participant), or the run's start for a thing whose run is one step; 0 when none runs.
     * The wedge bound is judged from it, so a run of several slow but healthy steps, none of them stuck, is not
     * wedged; only a step that has not returned within the bound is (srad/agentspaces-event-loop.md §10.10).
     *
     * @return the instant
     */
    default long progressNanos() {
        return runStartedNanos();
    }

    /**
     * The participant or task running now, or "".
     *
     * @return its name
     */
    String runningIn();

    /**
     * The deadline armed now, or {@code Long.MAX_VALUE} when none is.
     *
     * @return the deadline
     */
    long armedNanos();

    /**
     * The thread of the current run, or of the thread that should be alive (the timer), or null.
     *
     * @return the thread
     */
    Thread thread();

    /**
     * Work waiting: a pending wake, queued tasks, a non-empty outbox.
     *
     * @return how much
     */
    int queued();

    /**
     * Whether a run has been handed to a thread and has not started (lateness, not a fault).
     *
     * @return whether one is dispatched
     */
    boolean dispatched();

    /**
     * When the oldest pending wake was raised, or 0 when none is pending.
     *
     * @return the instant on the monotonic base
     */
    default long wakeRaisedNanos() {
        return 0;
    }

    /**
     * When a participant was fenced by a re-initialisation and its old run has not returned, or 0.
     *
     * @return the instant on the monotonic base
     */
    default long fencedSinceNanos() {
        return 0;
    }

    /**
     * Bumped by each re-initialisation.
     *
     * @return the generation
     */
    long generation();

    /**
     * Rebuilds and re-arms (WD-2, WD-3). Runs on the watchdog's thread: it swaps state through atomics and starts
     * threads, never runs a participant, and never blocks on one (WD-1, EL-H-13).
     *
     * @param reason why
     */
    void reinitialise(Reason reason);

    /**
     * Told when the watchdog escalates this thing (WD-5): three re-initialisations within the window, or a fenced
     * participant that has not returned within the bound.
     *
     * @param reason a sentence naming the cause
     */
    default void escalate(String reason) {
    }

    /**
     * Told on each watchdog check, after this thing was judged, for things that adapt per tick: the loop pool grows or
     * shrinks (§3.10, EL-O-03) and a node loop judges its stretch (EL-O-04). It runs on the watchdog's thread, so it
     * takes no lock a participant can hold, never blocks, and runs no participant (WD-1, EL-H-13): it decides, swaps
     * state through atomics or a lock only tried, and at most starts a thread or wakes a loop.
     *
     * @param nowNanos the watchdog's monotonic now
     */
    default void watchdogTick(long nowNanos) {
    }
}
