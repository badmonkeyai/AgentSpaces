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
package ai.badmonkey.agentspaces.api.spi;

/**
 * Work a node's loop runs when it is due (srad/agentspaces-event-loop.md §3.2): a participant runs what is due as of
 * the time it is given and returns its next due time on the monotonic base, or {@link #NEVER}.
 *
 * <p>Invariants:
 * <ul>
 *   <li>DW-1: a run that is not due does nothing and returns its deadline, so calling a participant early is always
 *       safe; the loop's backstop and the public {@code tick()} rely on this.</li>
 *   <li>DW-2: any code that makes a participant's next due earlier calls {@link Registration#wake()} on its
 *       registration, or {@code wake()} on its group when it cannot name the participant.</li>
 *   <li>DW-3: a participant never blocks: it sends, enqueues, arms and returns; blocking work goes to its own virtual
 *       thread.</li>
 *   <li>DW-4: a participant's name is stable, and appears in the loop's statistics and logs.</li>
 *   <li>DW-5: a participant that reads a signal another thread raises keeps its own copy of a generation counter and
 *       compares; it never clears a shared flag.</li>
 * </ul>
 *
 * <p>Stability: SPI, new in 0.7.0.
 */
@FunctionalInterface
public interface DueWork {

    /** The next due time of a participant with nothing scheduled. */
    long NEVER = Long.MAX_VALUE;

    /**
     * Runs what is due as of {@code nowNanos} and returns the next due time.
     *
     * @param nowNanos the loop's monotonic now
     * @return the next due time on the same base, or {@link #NEVER}
     */
    long run(long nowNanos);

    /**
     * Due work that keeps its deadlines on its own wall clock (often an injected {@code InstantSource}), for
     * registrants whose leases and expiries are wall instants. The group runtime converts its result to the loop's
     * monotonic base against both clocks read together (srad/agentspaces-event-loop.md §10.8, §3.6).
     */
    @FunctionalInterface
    interface Wall {

        /**
         * Runs what is due as of {@code nowMillis} on the registrant's clock and returns the next due time.
         *
         * @param nowMillis the registrant's wall now
         * @return the next due time on the same clock, or {@link #NEVER}
         */
        long run(long nowMillis);
    }

    /**
     * What a loop gives a provider that runs as its own participant (a capability provider, srad/agentspaces-event-loop.md
     * EL-T-05): the provider asks to run again when a deadline moves earlier (DW-2), and says when its card changed.
     */
    interface Driver {

        /** Asks the provider's participant to run again on the loop's next run (DW-2). */
        void wake();

        /** Says the provider's {@code describe()} changed, so its card is republished on its next run. */
        void describeChanged();

        /**
         * The loop's monotonic now, in nanoseconds.
         *
         * @return the monotonic now
         */
        long monoNanos();

        /**
         * A wall deadline on the node's clock, converted to the loop's monotonic base.
         *
         * @param wallMillis the wall deadline, or {@link #NEVER}
         * @return the deadline on the monotonic base, or {@link #NEVER}
         */
        long dueAt(long wallMillis);

        /**
         * Whether the provider runs on a started loop, so it keeps its own deadlines in time; false while a host
         * drives the node by hand, when each {@code tick()} is one legacy step (owner answer 4, 2026-11-22).
         *
         * @return whether timed
         */
        boolean timed();

        /**
         * The node's time source, for a provider that arms a one-shot task off the loop (the Raft lease guard,
         * owner answer 2, 2026-11-22) or registers with its watchdog.
         *
         * @return the time source
         */
        ai.badmonkey.agentspaces.common.time.TimeSource time();
    }

    /** A participant's place in a loop; closing it removes the participant. */
    interface Registration extends AutoCloseable {

        /** Asks this one participant to run again on the loop's next run (DW-2). */
        void wake();

        /** Removes the participant; it is not run again once a run in progress ends. */
        @Override
        void close();
    }
}
