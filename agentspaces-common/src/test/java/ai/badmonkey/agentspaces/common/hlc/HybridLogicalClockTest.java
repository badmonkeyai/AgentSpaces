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

    /**
     * HLC logical counter, 2026-11-08 (Rust team report): one stamp just under the drift
     * ceiling with the logical counter at its maximum froze the clock for up to ten
     * minutes. A remote counter above LOGICAL_LIMIT never reaches the clock.
     */
    @Test
    void aRemoteStampAboveTheLogicalLimitIsRefusedAndLeavesTheClockUnchanged() {
        FixedSource source = new FixedSource();
        HybridLogicalClock clock = new HybridLogicalClock(source, "n1");
        long wall = source.millis.get();
        assertThat(clock.now()).isEqualTo(new HlcTimestamp(wall, 0, "n1"));

        long nearCeiling = wall + HybridLogicalClock.MAX_DRIFT_MILLIS - 1;
        HlcTimestamp hostile = new HlcTimestamp(nearCeiling, Integer.MAX_VALUE, "evil");
        HlcTimestamp justOver = new HlcTimestamp(nearCeiling, HybridLogicalClock.LOGICAL_LIMIT + 1, "evil");
        assertThat(HybridLogicalClock.isAcceptable(hostile)).isFalse();
        assertThat(HybridLogicalClock.isAcceptable(justOver)).isFalse();
        assertThatThrownBy(() -> clock.update(hostile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("logical counter above 65536");
        assertThatThrownBy(() -> clock.update(justOver))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(clock.now()).as("the refused stamps did not move the clock")
                .isEqualTo(new HlcTimestamp(wall, 1, "n1"));
    }

    /** HLC logical counter, 2026-11-08 (Rust team report): a counter at exactly LOGICAL_LIMIT is honest and merges. */
    @Test
    void aRemoteStampAtTheLogicalLimitIsAccepted() {
        FixedSource source = new FixedSource();
        HybridLogicalClock clock = new HybridLogicalClock(source, "n1");
        HlcTimestamp atLimit = new HlcTimestamp(source.millis.get() + 5_000,
                HybridLogicalClock.LOGICAL_LIMIT, "n2");

        assertThat(HybridLogicalClock.LOGICAL_LIMIT).isEqualTo(65_536);
        assertThat(HybridLogicalClock.isAcceptable(atLimit)).isTrue();
        HlcTimestamp merged = clock.update(atLimit);

        assertThat(merged).isGreaterThan(atLimit);
        assertThat(merged).isEqualTo(new HlcTimestamp(atLimit.physical() + 1, 0, "n1"));
    }

    /** HLC logical counter, 2026-11-08 (Rust team report): past LOGICAL_LIMIT the counter carries into the physical part. */
    @Test
    void theLogicalCounterCarriesIntoThePhysicalPartAtTheLimit() {
        FixedSource source = new FixedSource();
        HybridLogicalClock clock = new HybridLogicalClock(source, "n1");
        long wall = source.millis.get();

        HlcTimestamp last = clock.now();
        while (last.logical() < HybridLogicalClock.LOGICAL_LIMIT) {
            last = clock.now();
        }
        assertThat(last).isEqualTo(new HlcTimestamp(wall, HybridLogicalClock.LOGICAL_LIMIT, "n1"));

        HlcTimestamp carried = clock.now();
        assertThat(carried).isEqualTo(new HlcTimestamp(wall + 1, 0, "n1"));
        assertThat(clock.now()).isEqualTo(new HlcTimestamp(wall + 1, 1, "n1"));
    }

    /** HLC logical counter, 2026-11-08 (Rust team report): after a stamp at the limit, stamps never repeat and always increase. */
    @Test
    void stampsAfterALimitStampAreStrictlyIncreasingAndUnique() {
        FixedSource source = new FixedSource();
        HybridLogicalClock clock = new HybridLogicalClock(source, "n1");
        long nearCeiling = source.millis.get() + HybridLogicalClock.MAX_DRIFT_MILLIS - 1;

        HlcTimestamp previous = clock.update(
                new HlcTimestamp(nearCeiling, HybridLogicalClock.LOGICAL_LIMIT, "n2"));
        int stamps = 3 * HybridLogicalClock.LOGICAL_LIMIT;
        for (int i = 0; i < stamps; i++) {
            HlcTimestamp next = clock.now();
            if (next.compareTo(previous) <= 0 || next.logical() > HybridLogicalClock.LOGICAL_LIMIT) {
                throw new AssertionError("stamp " + i + " " + next + " after " + previous);
            }
            previous = next;
        }
        assertThat(previous.physical()).as("three carries past the merged stamp")
                .isEqualTo(nearCeiling + 3);
    }

    /**
     * HLC logical counter, 2026-11-08 (Rust team report): a remote stamp at exactly the drift
     * ceiling with the counter at LOGICAL_LIMIT carried this clock one millisecond past the
     * ceiling, so a receiver's ASF-020 check struck the honest node that merged it. The merge
     * now clamps a remote physical part one millisecond inside the ceiling, and the carry lands
     * on the ceiling itself.
     */
    @Test
    void aRemoteStampAtTheDriftCeilingWithTheCounterAtTheLimitCarriesOntoTheCeiling() {
        FixedSource source = new FixedSource();
        HybridLogicalClock clock = new HybridLogicalClock(source, "n1");
        long ceiling = source.millis.get() + HybridLogicalClock.MAX_DRIFT_MILLIS;

        HlcTimestamp merged = clock.update(
                new HlcTimestamp(ceiling, HybridLogicalClock.LOGICAL_LIMIT, "n2"));

        assertThat(merged).isEqualTo(new HlcTimestamp(ceiling, 0, "n1"));
        assertThat(clock.now()).isEqualTo(new HlcTimestamp(ceiling, 1, "n1"));
    }

    /** HLC logical counter, 2026-11-08 (Rust team report): the merge clamps a far-future physical part one millisecond inside the ceiling. */
    @Test
    void theMergeClampsAFarFuturePhysicalPartOneMillisecondInsideTheCeiling() {
        FixedSource source = new FixedSource();
        HybridLogicalClock clock = new HybridLogicalClock(source, "n1");
        long ceiling = source.millis.get() + HybridLogicalClock.MAX_DRIFT_MILLIS;

        assertThat(clock.update(new HlcTimestamp(Long.MAX_VALUE, 0, "evil")))
                .isEqualTo(new HlcTimestamp(ceiling - 1, 1, "n1"));
        assertThat(clock.update(new HlcTimestamp(ceiling, 7, "n2")))
                .isEqualTo(new HlcTimestamp(ceiling - 1, 8, "n1"));
    }

    /**
     * HLC logical counter, 2026-11-08 (Rust team report): every implementation reads a stamp
     * with the same strictness, ASCII digits only in the physical and logical parts and a
     * non-empty node. Integer.parseInt and Long.parseLong also took a leading plus sign and
     * non-ASCII digits, which the other implementations refuse.
     */
    @Test
    void parseRefusesASignAFractionAnExponentAnEmptyPartAndNonAsciiDigits() {
        for (String encoded : java.util.List.of("+5:0:n", "5:+0:n", "-5:0:n", "5:-1:n",
                "1.5:0:n", "5:1.5:n", "1e3:0:n", "5:1e3:n", "5::n", ":0:n", "5:0:",
                "\u0665:0:n", "5:\u0660:n", "5:2147483648:n", "9223372036854775808:0:n",
                " 5:0:n", "5:0 :n")) {
            assertThatThrownBy(() -> HlcTimestamp.parse(encoded)).as(encoded)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(HlcTimestamp.parse("5:65537:n"))
                .as("an integer counter above the limit parses, and isAcceptable refuses it")
                .isEqualTo(new HlcTimestamp(5, 65_537, "n"));
        assertThat(HlcTimestamp.parse("0:65536:n")).isEqualTo(new HlcTimestamp(0, 65_536, "n"));
    }

    /**
     * HLC canonical stamps, 2026-11-08: a stamp has exactly one text, so the bytes a signer
     * signed always equal the stamp's re-encoding. Java read "007:00:n" as 7:0:n and verified
     * signatures over the re-encoding while the clients verified over the raw text, which split
     * the claim lattice between them. A leading zero, a node with a control character, and a
     * physical part past 2^63-1 are refused; "0" and 2^63-1 are canonical.
     */
    @Test
    void parseAcceptsOnlyTheCanonicalGrammar() {
        for (String encoded : java.util.List.of("007:0:n", "7:00:n", "00:0:n", "7:01:n", "01:1:n",
                "5:0:a\u0000b", "5:0:\u001f", "5:0:n\u007f", "5:0:\tn", "5:0:n\n", "5:0:a:b",
                "9223372036854775808:0:n", "18446744073709551616:0:n")) {
            assertThatThrownBy(() -> HlcTimestamp.parse(encoded)).as(encoded)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(HlcTimestamp.parse("0:0:n")).isEqualTo(new HlcTimestamp(0, 0, "n"));
        assertThat(HlcTimestamp.parse("10:0:n")).isEqualTo(new HlcTimestamp(10, 0, "n"));
        assertThat(HlcTimestamp.parse("9223372036854775807:65536:n"))
                .isEqualTo(new HlcTimestamp(Long.MAX_VALUE, 65_536, "n"));
        assertThat(HlcTimestamp.parse("5:0:z\u00e9\u0080 n").node())
                .as("only C0 controls and DEL are refused in a node").isEqualTo("z\u00e9\u0080 n");
        for (String encoded : java.util.List.of("0:0:n", "9223372036854775807:65536:zAbC", "5:7:z\u00e9")) {
            assertThat(HlcTimestamp.parse(encoded).encoded()).as("canonical text re-encodes to itself")
                    .isEqualTo(encoded);
        }
    }

    /** HLC canonical stamps, 2026-11-08: no constructed stamp encodes to text that parse refuses. */
    @Test
    void theConstructorRefusesANodeThatParseWouldRefuse() {
        for (String node : java.util.List.of("", "a:b", "a\u0000", "\u001f", "\u007f", "line\n")) {
            assertThatThrownBy(() -> new HlcTimestamp(1, 0, node)).as(node)
                    .isInstanceOf(IllegalArgumentException.class);
        }
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
