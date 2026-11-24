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

import java.util.Arrays;

/**
 * The most recent latencies of one kind, for the loop pool's waits, the timer's dispatch latency and a loop's lateness
 * per watchdog tick (srad/agentspaces-event-loop.md §3.10, EL-X-10, EL-O-01). Recording takes a short monitor of its
 * own, never one a participant holds, so the watchdog may read it (WD-1).
 */
public final class LatencyRing {

    /**
     * Quantiles over the samples a snapshot read.
     *
     * @param samples   samples read
     * @param p50Nanos  the median
     * @param p99Nanos  the 99th percentile
     * @param maxNanos  the largest
     * @param total     samples recorded since the ring was made
     * @param maxEver   the largest sample since the ring was made
     */
    public record Figures(long samples, long p50Nanos, long p99Nanos, long maxNanos, long total, long maxEver) {
        /** No samples. */
        public static final Figures NONE = new Figures(0, 0, 0, 0, 0, 0);
    }

    private final long[] ring;
    private long count;
    private long maxEver;

    /**
     * A ring of the given capacity.
     *
     * @param capacity the most recent samples kept
     */
    public LatencyRing(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity " + capacity);
        }
        this.ring = new long[capacity];
    }

    /**
     * Records one latency; a negative one counts as 0.
     *
     * @param nanos the latency
     */
    public void record(long nanos) {
        long v = Math.max(0, nanos);
        synchronized (ring) {
            ring[(int) (count % ring.length)] = v;
            count++;
            if (v > maxEver) {
                maxEver = v;
            }
        }
    }

    /**
     * Samples recorded since the ring was made.
     *
     * @return the count
     */
    public long count() {
        synchronized (ring) {
            return count;
        }
    }

    /**
     * Quantiles over every sample the ring still holds.
     *
     * @return the figures
     */
    public Figures snapshot() {
        return since(0);
    }

    /**
     * Quantiles over the samples recorded at or after a count (as {@link #count()} returned it), as far as the ring
     * still holds them: the samples of one watchdog tick.
     *
     * @param fromCount the count the window starts at
     * @return the figures
     */
    public Figures since(long fromCount) {
        long[] sorted;
        long total;
        long max;
        synchronized (ring) {
            total = count;
            max = maxEver;
            long from = Math.max(Math.max(0, fromCount), count - ring.length);
            int n = (int) (count - from);
            sorted = new long[n];
            for (int i = 0; i < n; i++) {
                sorted[i] = ring[(int) ((from + i) % ring.length)];
            }
        }
        if (sorted.length == 0) {
            return new Figures(0, 0, 0, 0, total, max);
        }
        Arrays.sort(sorted);
        return new Figures(sorted.length, percentile(sorted, 50), percentile(sorted, 99),
                sorted[sorted.length - 1], total, max);
    }

    /**
     * The p-th percentile of sorted samples, nearest rank.
     *
     * @param sorted the samples, sorted ascending, not empty
     * @param p      the percentile, 0 to 100
     * @return the value
     */
    public static long percentile(long[] sorted, int p) {
        int rank = (int) Math.ceil(p / 100.0 * sorted.length);
        return sorted[Math.max(0, Math.min(sorted.length - 1, rank - 1))];
    }
}
