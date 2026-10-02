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
package ai.badmonkey.agentspaces.common.hlc;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.InstantSource;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HybridLogicalClockTest {

    /** A settable InstantSource for driving the clock deterministically. */
    private static final class FixedSource implements InstantSource {
        final AtomicLong millis = new AtomicLong(1_000_000L);

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis.get());
        }
    }

    @Test
    void timestampsAreStrictlyMonotonicWithinTheSameMillisecond() {
        FixedSource source = new FixedSource();
        HybridLogicalClock clock = new HybridLogicalClock(source, "n1");

        HlcTimestamp first = clock.now();
        HlcTimestamp second = clock.now();
        HlcTimestamp third = clock.now();

        assertThat(first.physical()).isEqualTo(second.physical());
        assertThat(second).isGreaterThan(first);
        assertThat(third).isGreaterThan(second);
        assertThat(third.logical()).isEqualTo(2);
    }

    @Test
    void physicalAdvanceResetsTheLogicalCounter() {
        FixedSource source = new FixedSource();
        HybridLogicalClock clock = new HybridLogicalClock(source, "n1");

        clock.now();
        clock.now();
        source.millis.addAndGet(5);
        HlcTimestamp advanced = clock.now();

        assertThat(advanced.physical()).isEqualTo(1_000_005L);
        assertThat(advanced.logical()).isZero();
    }

    @Test
    void updateWithRemoteAheadJumpsPastTheRemote() {
        FixedSource source = new FixedSource();
        HybridLogicalClock clock = new HybridLogicalClock(source, "n1");

        // A remote that is ahead but within the drift window pulls this clock
        // forward to it (300s < MAX_DRIFT_MILLIS of skew is plausible).
        HlcTimestamp remote = new HlcTimestamp(1_300_000L, 7, "n2");
        HlcTimestamp merged = clock.update(remote);

        assertThat(merged).isGreaterThan(remote);
        assertThat(merged.physical()).isEqualTo(1_300_000L);
        assertThat(merged.logical()).isEqualTo(8);
        assertThat(clock.now()).isGreaterThan(merged);
    }

    @Test
    void updateWithRemoteBehindStillAdvancesLocally() {
        FixedSource source = new FixedSource();
        HybridLogicalClock clock = new HybridLogicalClock(source, "n1");

        HlcTimestamp before = clock.now();
        HlcTimestamp merged = clock.update(new HlcTimestamp(1L, 0, "n2"));

        assertThat(merged).isGreaterThan(before);
    }

    @Test
    void orderingIsTotalAcrossNodes() {
        HlcTimestamp a = new HlcTimestamp(5, 1, "alpha");
        HlcTimestamp b = new HlcTimestamp(5, 1, "beta");

        assertThat(a).isLessThan(b);
        assertThat(a.compareTo(a)).isZero();
    }

    @Test
    void aFarFutureRemoteStampCannotPoisonTheClock() {
        FixedSource source = new FixedSource();
        HybridLogicalClock clock = new HybridLogicalClock(source, "n1");

        // A hostile (or wildly skewed) peer sends physical = Long.MAX_VALUE.
        clock.update(new HlcTimestamp(Long.MAX_VALUE, 0, "evil"));

        // The clock is clamped to within MAX_DRIFT_MILLIS of local wall time,
        // not pinned at the far-future value.
        HlcTimestamp after = clock.now();
        assertThat(after.physical())
                .isLessThanOrEqualTo(source.millis.get() + HybridLogicalClock.MAX_DRIFT_MILLIS);

        // Once wall time advances past the clamp ceiling, stamps track wall time
        // again and the logical counter resets, proving the clock recovered.
        source.millis.set(source.millis.get() + HybridLogicalClock.MAX_DRIFT_MILLIS + 1);
        HlcTimestamp recovered = clock.now();
        assertThat(recovered.physical()).isEqualTo(source.millis.get());
        assertThat(recovered.logical()).isZero();
    }

    @Test
    void logicalCounterSaturatesInsteadOfOverflowing() {
        FixedSource source = new FixedSource();
        HybridLogicalClock clock = new HybridLogicalClock(source, "n1");

        // Merge a remote stamp already at the logical ceiling within the drift
        // window; the next local stamp must saturate rather than overflow to a
        // negative logical (which HlcTimestamp would reject with an exception).
        clock.update(new HlcTimestamp(source.millis.get(), Integer.MAX_VALUE, "peer"));
        HlcTimestamp next = clock.now();
        assertThat(next.logical()).isEqualTo(Integer.MAX_VALUE);
        assertThat(next.logical()).isNotNegative();
    }

    @Test
    void encodedFormRoundTrips() {
        HlcTimestamp ts = new HlcTimestamp(123456789L, 42, "node-a");

        assertThat(HlcTimestamp.parse(ts.encoded())).isEqualTo(ts);
        assertThat(ts.encoded()).isEqualTo("123456789:42:node-a");
    }

    @Test
    void parseRejectsMalformedInput() {
        assertThatThrownBy(() -> HlcTimestamp.parse("nope"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> HlcTimestamp.parse("x:y:z"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HlcTimestamp(1, 1, "has:colon"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
