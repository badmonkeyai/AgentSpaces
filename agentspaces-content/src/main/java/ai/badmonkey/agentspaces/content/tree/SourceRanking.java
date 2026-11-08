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
package ai.badmonkey.agentspaces.content.tree;

import ai.badmonkey.agentspaces.common.id.PeerId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The source rules a piece fetch and a tree fetch share (srad/agentspaces-pdt.md
 * PD-X-05, §6.9): the order sources are asked in (preferred first, then by relay
 * hop count, then by throughput), the per-source window that grows by one on
 * every answer and halves on a timeout, the request timeout of the larger of
 * {@link #MIN_TIMEOUT_MILLIS} and {@link #RTT_MULTIPLE_FOR_TIMEOUT} smoothed round
 * trips, the drop for {@link #SOURCE_DROP_MILLIS} after {@link #TIMEOUTS_BEFORE_DROP}
 * consecutive timeouts, the back-off after a {@code REJECT} for policy or budget
 * ({@link #SOURCE_DROP_MILLIS} times two to the power of the strikes so far, capped
 * at {@link #MAX_BACKOFF_EXPONENT}), and the throughput accounting. These were the
 * rules of {@code FetchSession} (srad/large-networks-bulk-data.md LB-S-04, LB-S-05,
 * LB-R-10) and {@code FetchSession} now reads them from here; the §6.9 drift guard
 * is that no second ranking exists in the module.
 *
 * <p>A session extends {@link Source} with what its unit of work needs (a piece
 * bitfield and assignments, or a set of subtrees) and calls the state transitions
 * here from under its own lock; nothing in this class locks.
 */
public final class SourceRanking {

    /** First window per source (LB-S-04). */
    public static final int INITIAL_WINDOW = 2;
    /** The floor of a request timeout. */
    public static final long MIN_TIMEOUT_MILLIS = 10_000;
    /** A request times out after this many smoothed round trips, when that exceeds the floor. */
    public static final int RTT_MULTIPLE_FOR_TIMEOUT = 8;
    /** Consecutive timeouts that drop a source for {@link #SOURCE_DROP_MILLIS}. */
    public static final int TIMEOUTS_BEFORE_DROP = 3;
    /** How long a dropped source stays out, and the unit of the reject back-off. */
    public static final long SOURCE_DROP_MILLIS = 60_000;
    /** The reject back-off doubles per strike up to this exponent: 60 s, 120 s, 240 s, 480 s, 960 s. */
    public static final int MAX_BACKOFF_EXPONENT = 4;
    /** Unopenable answers (the wrong key, a short range) that drop a source for the rest of a fetch. */
    public static final int UNOPENABLE_BEFORE_DROP = 3;

    /**
     * The per-source state both fetch kinds keep. Fields are plain so a session
     * reads them as its own; every transition that the rules define is a method.
     */
    public static class Source {
        /** The holder. */
        public final PeerId id;
        /** Requests this source may have in flight. */
        public int window = INITIAL_WINDOW;
        /** Requests in flight now. */
        public int inFlight;
        /** Smoothed round trip in milliseconds; -1 before the first answer. */
        public long srttMillis = -1;
        /** Answers since the last timeout. */
        public int successes;
        /** Timeouts since the last answer. */
        public int consecutiveTimeouts;
        /** The source is not asked before this time; {@link Long#MAX_VALUE} means never again this fetch. */
        public long droppedUntilMillis;
        /** Payload bytes this source delivered that verified. */
        public long bytesVerified;
        /** Payload bytes this source delivered, verified or not. */
        public long bytesReceived;
        /** Rejects for policy or budget, and failed verifications, so far. */
        public int strikes;
        /** Relays between this peer and the source: 0 direct, -1 unreachable (LB-R-10). */
        public int hops;
        /** How full the window was, sampled while the source held what the fetch lacked (LB-Q-PRF-04). */
        public long fillSamples;
        /** The sum of the fill samples. */
        public double fillSum;

        /**
         * A source.
         *
         * @param id the holder
         */
        public Source(PeerId id) {
            this.id = Objects.requireNonNull(id, "id");
        }

        /** Whether the source is out at this time. */
        public boolean dropped(long nowMillis) {
            return droppedUntilMillis > nowMillis;
        }

        /** Bytes per second at the smoothed round trip, or the raw byte count before one is known. */
        public long throughput() {
            return srttMillis <= 0 ? bytesReceived : bytesReceived * 1000 / Math.max(1, srttMillis);
        }

        /** The timeout a request to this source gets: the larger of the floor and eight smoothed round trips. */
        public long timeoutAllowanceMillis() {
            return Math.max(MIN_TIMEOUT_MILLIS, srttMillis < 0 ? MIN_TIMEOUT_MILLIS : RTT_MULTIPLE_FOR_TIMEOUT * srttMillis);
        }

        /** Whether a request sent at {@code sentAtMillis} has timed out at {@code nowMillis}. */
        public boolean timedOut(long sentAtMillis, long nowMillis) {
            return nowMillis - sentAtMillis > timeoutAllowanceMillis();
        }

        /**
         * An answer arrived: the round trip folds into the smoothed estimate (seven
         * eighths old, one eighth new), the timeout run resets, the bytes count, and
         * one request leaves flight.
         *
         * @param rttMillis the round trip of the answered request
         * @param bytes     the payload bytes the answer carried
         */
        public void answered(long rttMillis, long bytes) {
            inFlight = Math.max(0, inFlight - 1);
            progressed(rttMillis, bytes);
        }

        /**
         * Part of an answer arrived and the request stays in flight (a {@code TREE_WANT}
         * streams several {@code NODES}): the round trip and the bytes count as in
         * {@link #answered}, and nothing leaves flight.
         *
         * @param rttMillis the time since the request was sent, or since its last part
         * @param bytes     the payload bytes this part carried
         */
        public void progressed(long rttMillis, long bytes) {
            long rtt = Math.max(0, rttMillis);
            srttMillis = srttMillis < 0 ? rtt : (srttMillis * 7 + rtt) / 8;
            consecutiveTimeouts = 0;
            bytesReceived += bytes;
        }

        /**
         * An answer verified: the window grows by one up to the session's maximum
         * and the success run lengthens.
         *
         * @param maxWindow the session's window cap
         */
        public void grow(int maxWindow) {
            successes++;
            if (window < maxWindow) {
                window++;
            }
        }

        /**
         * A request timed out: one request leaves flight, the window halves to no
         * less than one, the success run resets, and after
         * {@link #TIMEOUTS_BEFORE_DROP} in a row the source is dropped for
         * {@link #SOURCE_DROP_MILLIS}.
         *
         * @param nowMillis the time
         * @return true when this timeout dropped the source
         */
        public boolean timedOut(long nowMillis) {
            inFlight = Math.max(0, inFlight - 1);
            window = Math.max(1, window / 2);
            successes = 0;
            consecutiveTimeouts++;
            if (consecutiveTimeouts >= TIMEOUTS_BEFORE_DROP) {
                droppedUntilMillis = nowMillis + SOURCE_DROP_MILLIS;
                consecutiveTimeouts = 0;
                return true;
            }
            return false;
        }

        /**
         * A {@code REJECT} for policy or budget: the source is out for
         * {@link #SOURCE_DROP_MILLIS} times two to the power of its strikes so far
         * (at most {@link #MAX_BACKOFF_EXPONENT}), and the strike counts.
         *
         * @param nowMillis the time
         */
        public void backOff(long nowMillis) {
            droppedUntilMillis = nowMillis + SOURCE_DROP_MILLIS * (1L << Math.min(MAX_BACKOFF_EXPONENT, strikes));
            strikes++;
        }

        /** The source is never asked again in this fetch (a failed verification, LB-X-03, PD-X-04). */
        public void dropForever() {
            strikes++;
            droppedUntilMillis = Long.MAX_VALUE;
        }

        /**
         * An answer that could not be opened (the wrong key, a short range): a strike,
         * and after {@link #UNOPENABLE_BEFORE_DROP} the source is out for the fetch.
         *
         * @return true when this strike dropped the source
         */
        public boolean unopenable() {
            if (++strikes >= UNOPENABLE_BEFORE_DROP) {
                droppedUntilMillis = Long.MAX_VALUE;
                return true;
            }
            return false;
        }

        /** Samples how full the window is now (LB-Q-PRF-04); called while the source holds something wanted. */
        public void sampleFill() {
            if (window > 0) {
                fillSamples++;
                fillSum += Math.min(1.0, (double) inFlight / window);
            }
        }

        /** The mean window fill over the samples, or empty before any. */
        public java.util.OptionalDouble fill() {
            return fillSamples == 0 ? java.util.OptionalDouble.empty() : java.util.OptionalDouble.of(fillSum / fillSamples);
        }
    }

    private final Set<PeerId> prefer;

    /**
     * A ranking.
     *
     * @param prefer sources asked before any other, whatever their hops or throughput
     */
    public SourceRanking(Set<PeerId> prefer) {
        this.prefer = prefer == null ? Set.of() : Set.copyOf(prefer);
    }

    /** The preferred sources. */
    public Set<PeerId> prefer() {
        return prefer;
    }

    /**
     * The order sources are asked in (LB-S-05, LB-R-10, PD-X-05): preferred first,
     * then direct before relayed by hop count (unreachable last), then by
     * throughput, highest first. The sort is stable, so sources that tie keep the
     * order they were added in.
     */
    public Comparator<Source> comparator() {
        return (x, y) -> {
            int px = prefer.contains(x.id) ? 0 : 1;
            int py = prefer.contains(y.id) ? 0 : 1;
            if (px != py) {
                return px - py;
            }
            int hx = x.hops < 0 ? Integer.MAX_VALUE : x.hops;
            int hy = y.hops < 0 ? Integer.MAX_VALUE : y.hops;
            if (hx != hy) {
                return Integer.compare(hx, hy);
            }
            return Long.compare(y.throughput(), x.throughput());
        };
    }

    /**
     * The sources in asking order; a sorted copy.
     *
     * @param sources the sources, in the order they were added
     * @param <S>     the session's source type
     * @return a new list in asking order
     */
    public <S extends Source> List<S> order(Collection<S> sources) {
        List<S> ordered = new ArrayList<>(sources);
        ordered.sort(comparator());
        return ordered;
    }
}
