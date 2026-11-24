/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.peering.node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The counters behind {@link PeerNode#loopStats()} (srad/agentspaces-event-loop.md EL-M-02, NL-6): runs, wakes, flushes
 * and the lateness of each scheduled run against the time it was scheduled for. Counting is lock-free; the lateness
 * ring and the newcomer figures take a short monitor of their own, never one the protocol holds.
 */
final class LoopMeter {

    private static final int LATENESS_RING = 4096;
    private static final int NEWCOMER_RING = 256;

    private final AtomicLong runs = new AtomicLong();
    private final AtomicLong timerWakes = new AtomicLong();
    private final AtomicLong eventWakes = new AtomicLong();
    private final AtomicLong flushRuns = new AtomicLong();
    private final AtomicLong flushesWithFrames = new AtomicLong();
    private final AtomicLong slowRuns = new AtomicLong();
    private final AtomicLong failedRuns = new AtomicLong();
    private final AtomicLong busyNanos = new AtomicLong();

    private final long[] lateness = new long[LATENESS_RING];
    private long latenessCount;
    private long latenessMax;
    private final long[] flushWait = new long[LATENESS_RING];
    private long flushWaitCount;
    private long flushWaitMax;
    private final long[] newcomer = new long[NEWCOMER_RING];
    private long newcomerCount;

    void ran(long tookNanos) {
        runs.incrementAndGet();
        busyNanos.addAndGet(Math.max(0, tookNanos));
        if (tookNanos > PeerNode.SLOW_RUN_NANOS) {
            slowRuns.incrementAndGet();
        }
    }

    void timerWake(long latenessNanos) {
        timerWakes.incrementAndGet();
        long late = Math.max(0, latenessNanos);
        synchronized (lateness) {
            lateness[(int) (latenessCount % LATENESS_RING)] = late;
            latenessCount++;
            latenessMax = Math.max(latenessMax, late);
        }
    }

    void eventWake() {
        eventWakes.incrementAndGet();
    }

    void failed() {
        failedRuns.incrementAndGet();
    }

    void flushed(boolean foundFrames) {
        flushRuns.incrementAndGet();
        if (foundFrames) {
            flushesWithFrames.incrementAndGet();
        }
    }

    /** The wait of the oldest frame in an outbox that a flush sent (EL-B-04, FT-2). */
    void flushWait(long nanos) {
        long wait = Math.max(0, nanos);
        synchronized (flushWait) {
            flushWait[(int) (flushWaitCount % LATENESS_RING)] = wait;
            flushWaitCount++;
            flushWaitMax = Math.max(flushWaitMax, wait);
        }
    }

    void newcomerRound(long millis) {
        synchronized (newcomer) {
            newcomer[(int) (newcomerCount % NEWCOMER_RING)] = millis;
            newcomerCount++;
        }
    }

    List<Long> newcomerRounds() {
        synchronized (newcomer) {
            int n = (int) Math.min(newcomerCount, NEWCOMER_RING);
            List<Long> out = new ArrayList<>(n);
            for (long i = newcomerCount - n; i < newcomerCount; i++) {
                out.add(newcomer[(int) (i % NEWCOMER_RING)]);
            }
            return List.copyOf(out);
        }
    }

    /** The meter's figures at one moment. */
    record Figures(long runs, long timerWakes, long eventWakes, long flushRuns, long flushesWithFrames, long slowRuns,
                   long failedRuns, long latenessSamples, long latenessP50Nanos, long latenessP99Nanos,
                   long latenessMaxNanos, long busyNanos, long flushWaitSamples, long flushWaitP99Nanos,
                   long flushWaitMaxNanos) {
    }

    Figures snapshot() {
        long[] sorted;
        long count;
        long max;
        synchronized (lateness) {
            count = latenessCount;
            max = latenessMax;
            sorted = Arrays.copyOf(lateness, (int) Math.min(count, LATENESS_RING));
        }
        Arrays.sort(sorted);
        long[] waits;
        long waitMax;
        synchronized (flushWait) {
            waitMax = flushWaitMax;
            waits = Arrays.copyOf(flushWait, (int) Math.min(flushWaitCount, LATENESS_RING));
        }
        Arrays.sort(waits);
        return new Figures(runs.get(), timerWakes.get(), eventWakes.get(), flushRuns.get(),
                flushesWithFrames.get(), slowRuns.get(), failedRuns.get(),
                sorted.length, percentile(sorted, 50), percentile(sorted, 99), max, busyNanos.get(),
                waits.length, percentile(waits, 99), waitMax);
    }

    private static long percentile(long[] sorted, int p) {
        if (sorted.length == 0) {
            return 0;
        }
        int index = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, index))];
    }
}
