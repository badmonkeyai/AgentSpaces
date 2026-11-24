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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-wide loop totals for the end-of-build read (srad/agentspaces-event-loop.md EL-Q-REL-03): every node loop
 * in this process adds the backstop runs that found work (a deadline moved earlier with no wake) unless it was marked
 * scripted ({@code NodeLoop.markScripted()}) by a test that drops a wake on purpose. The test JVM's end prints the total
 * beside the watchdog's ({@code WatchdogTotalsListener}), so the reactor's log carries one line per module whose
 * reading is the requirement's "0 in every test of the reactor".
 */
public final class LoopTotals {

    private static final AtomicLong LOOPS = new AtomicLong();
    private static final AtomicLong FOUND = new AtomicLong();
    private static final AtomicLong SCRIPTED = new AtomicLong();
    private static final Map<String, AtomicLong> FOUND_BY = new ConcurrentHashMap<>();
    /** Distinct participant names kept by name; past this, finds are only counted. */
    private static final int NAMES = 256;

    private LoopTotals() {
    }

    /** A node loop started in this process. */
    public static void loopStarted() {
        LOOPS.incrementAndGet();
    }

    /**
     * A backstop run found work for a participant. A scripted loop's find (a test that drops a wake on purpose) is
     * counted apart, in {@link #scriptedFinds()}, and stays out of the total.
     *
     * @param participant the participant's name
     * @param scripted    whether the loop was marked scripted
     */
    public static void found(String participant, boolean scripted) {
        if (scripted) {
            SCRIPTED.incrementAndGet();
            return;
        }
        FOUND.incrementAndGet();
        AtomicLong n = FOUND_BY.get(participant);
        if (n == null && FOUND_BY.size() < NAMES) {
            n = FOUND_BY.computeIfAbsent(participant, k -> new AtomicLong());
        }
        if (n != null) {
            n.incrementAndGet();
        }
    }

    /**
     * Node loops started in this process.
     *
     * @return the count
     */
    public static long loops() {
        return LOOPS.get();
    }

    /**
     * Backstop runs that found work, over every unscripted loop in this process (EL-Q-REL-03, goal 0).
     *
     * @return the total
     */
    public static long backstopFoundWork() {
        return FOUND.get();
    }

    /**
     * Backstop finds of loops marked scripted: the tests' own dropped wakes, kept out of the total.
     *
     * @return the count
     */
    public static long scriptedFinds() {
        return SCRIPTED.get();
    }

    /**
     * {@link #backstopFoundWork()} by participant name, sorted.
     *
     * @return the counts
     */
    public static Map<String, Long> foundBy() {
        Map<String, Long> out = new TreeMap<>();
        FOUND_BY.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }
}
