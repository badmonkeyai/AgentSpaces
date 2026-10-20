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

package ai.badmonkey.agentspaces.transport.dtls;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The recovery sublayer on a simulated lossy, duplicating, reordering network
 * with no sockets and no DTLS (SRAD DT-Q-TST-01, DT-Q-REL-01): two state
 * machines joined by a seeded event loop on a virtual clock.
 */
class RecoveryTest {

    static final int FRAGMENT = 1_147;
    static final long IDLE = 60_000 * Recovery.MILLI;

    /** A two-ended simulated link. */
    static final class Sim {
        final Random random;
        final double loss;
        final double duplicate;
        final long baseDelay;
        final long jitter;
        long now = 1;
        long seq;
        final PriorityQueue<long[]> events = new PriorityQueue<>((a, b) -> a[0] != b[0]
                ? Long.compare(a[0], b[0]) : Long.compare(a[1], b[1]));
        final List<byte[]> payloads = new ArrayList<>();
        final DtlsCounters countersA = new DtlsCounters();
        final DtlsCounters countersB = new DtlsCounters();
        final Recovery a;
        final Recovery b;
        final List<byte[]> deliveredAtB = new ArrayList<>();
        final List<byte[]> deliveredAtA = new ArrayList<>();
        long datagramsAtoB;
        long acksBtoA;
        long dataBtoA;

        Sim(long seed, double loss, double duplicate, long baseDelay, long jitter, int window) {
            this.random = new Random(seed);
            this.loss = loss;
            this.duplicate = duplicate;
            this.baseDelay = baseDelay;
            this.jitter = jitter;
            this.a = new Recovery(pdu -> route(pdu, 1), FRAGMENT, window, IDLE, () -> now, countersA);
            this.b = new Recovery(pdu -> route(pdu, 0), FRAGMENT, window, IDLE, () -> now, countersB);
        }

        /** Schedules delivery of a PDU to side {@code to} (0 = A, 1 = B), subject to loss and duplication. */
        void route(byte[] pdu, int to) {
            if (to == 1) {
                datagramsAtoB++;
            } else if (pdu[1] == Pdu.ACK) {
                acksBtoA++;
            } else if (pdu[1] == Pdu.DATA) {
                dataBtoA++;
            }
            int copies = random.nextDouble() < duplicate ? 2 : 1;
            for (int i = 0; i < copies; i++) {
                if (random.nextDouble() < loss) {
                    continue;
                }
                long delay = baseDelay + (jitter == 0 ? 0 : (long) (random.nextDouble() * jitter));
                payloads.add(pdu.clone());
                events.add(new long[]{now + delay, seq++, to, payloads.size() - 1});
            }
        }

        /** Runs until {@code done} holds or virtual time passes {@code limit}. */
        void run(java.util.function.BooleanSupplier done, long limit) {
            while (!done.getAsBoolean()) {
                long next = Math.min(a.nextDeadline(), b.nextDeadline());
                long[] event = events.peek();
                if (event != null && event[0] <= next) {
                    events.poll();
                    now = Math.max(now, event[0]);
                    byte[] pdu = payloads.get((int) event[3]);
                    payloads.set((int) event[3], null);
                    if (event[2] == 1) {
                        deliveredAtB.addAll(b.onPdu(pdu, 0, pdu.length));
                    } else {
                        deliveredAtA.addAll(a.onPdu(pdu, 0, pdu.length));
                    }
                } else {
                    if (next == Long.MAX_VALUE) {
                        return;
                    }
                    now = Math.max(now, next);
                    a.onTimer();
                    b.onTimer();
                }
                if (now > limit) {
                    return;
                }
            }
        }
    }

    private static List<byte[]> frames(Random random, int count, int... sizes) {
        List<byte[]> frames = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            byte[] frame = new byte[sizes[i % sizes.length]];
            random.nextBytes(frame);
            frames.add(frame);
        }
        return frames;
    }

    private static void sendAll(Sim sim, Recovery from, List<byte[]> frames, List<byte[]> deliveredAtPeer)
            throws Exception {
        int next = 0;
        long limit = sim.now + 3_600_000 * Recovery.MILLI;
        while (next < frames.size()) {
            if (from.offer(frames.get(next))) {
                next++;
            } else {
                int target = deliveredAtPeer.size() + 1;
                sim.run(() -> deliveredAtPeer.size() >= target || from.backlogBytes() == 0, limit);
            }
        }
    }

    @ParameterizedTest(name = "loss {1}, duplicate {2}, seed {0}")
    @CsvSource({
            "1, 0.0, 0.0",
            "2, 0.10, 0.02",
            "3, 0.30, 0.05",
            "4, 0.30, 0.0"
    })
    void deliversEveryFrameOnceAndInOrderUnderLoss(long seed, double loss, double duplicate) throws Exception {
        Sim sim = new Sim(seed, loss, duplicate, 10 * Recovery.MILLI, 20 * Recovery.MILLI, 64);
        Random random = new Random(seed);
        List<byte[]> frames = frames(random, 400, 1, 200, 1_147, 1_148, 5_000, 64 * 1024);
        frames.add(randomFrame(random, 2 * 1024 * 1024));
        frames.add(randomFrame(random, Recovery.MAX_FRAME));
        sendAll(sim, sim.a, frames, sim.deliveredAtB);
        sim.run(() -> sim.deliveredAtB.size() == frames.size(), sim.now + 3_600_000 * Recovery.MILLI);

        assertThat(sim.deliveredAtB).hasSize(frames.size());
        for (int i = 0; i < frames.size(); i++) {
            assertThat(Arrays.equals(sim.deliveredAtB.get(i), frames.get(i)))
                    .as("frame %d", i).isTrue();
        }
        assertThat(sim.a.failure()).isNull();
        assertThat(sim.b.failure()).isNull();
        if (loss > 0) {
            assertThat(sim.countersA.retransmitted()).isPositive();
        }
    }

    @Test
    void bothDirectionsCarryFramesAtOnce() throws Exception {
        Sim sim = new Sim(7, 0.15, 0.02, 5 * Recovery.MILLI, 10 * Recovery.MILLI, 64);
        Random random = new Random(7);
        List<byte[]> toB = frames(random, 200, 300, 3_000, 40_000);
        List<byte[]> toA = frames(random, 200, 900, 12_000);
        int ia = 0;
        int ib = 0;
        while (ia < toB.size() || ib < toA.size()) {
            if (ia < toB.size() && sim.a.offer(toB.get(ia))) {
                ia++;
            }
            if (ib < toA.size() && sim.b.offer(toA.get(ib))) {
                ib++;
            }
            sim.run(() -> true, sim.now);
            long target = sim.now + 50 * Recovery.MILLI;
            sim.run(() -> sim.now >= target, target + Recovery.MILLI);
        }
        sim.run(() -> sim.deliveredAtB.size() == toB.size() && sim.deliveredAtA.size() == toA.size(),
                sim.now + 3_600_000 * Recovery.MILLI);
        assertThat(sim.deliveredAtB).hasSize(toB.size());
        assertThat(sim.deliveredAtA).hasSize(toA.size());
        for (int i = 0; i < toB.size(); i++) {
            assertThat(sim.deliveredAtB.get(i)).isEqualTo(toB.get(i));
        }
        for (int i = 0; i < toA.size(); i++) {
            assertThat(sim.deliveredAtA.get(i)).isEqualTo(toA.get(i));
        }
        // Acknowledgements ride DATA going the other way when they can.
        assertThat(sim.countersA.pdusSent() + sim.countersB.pdusSent()).isPositive();
    }

    @Test
    void standaloneAcksNumberAtMostOnePerTwoDataPdus() throws Exception {
        Sim sim = new Sim(11, 0.0, 0.0, 2 * Recovery.MILLI, 0, 64);
        List<byte[]> frames = frames(new Random(11), 50, 64 * 1024);
        sendAll(sim, sim.a, frames, sim.deliveredAtB);
        sim.run(() -> sim.deliveredAtB.size() == frames.size(), sim.now + 600_000 * Recovery.MILLI);
        assertThat(sim.deliveredAtB).hasSize(frames.size());
        long data = sim.countersA.pdusSent();
        assertThat(sim.acksBtoA).isLessThanOrEqualTo(data / 2 + 1);
    }

    @Test
    void theWindowBoundsPdusInFlight() throws Exception {
        Sim sim = new Sim(13, 1.0, 0.0, Recovery.MILLI, 0, 16);
        sim.a.offer(new byte[FRAGMENT * 100]);
        assertThat(sim.a.inFlight()).isEqualTo(16);
        assertThat(sim.datagramsAtoB).isEqualTo(16);
    }

    @Test
    void theBacklogRefusesBeyondEightMebibytesUntilItDrains() throws Exception {
        Sim sim = new Sim(17, 1.0, 0.0, Recovery.MILLI, 0, 4);
        assertThat(sim.a.offer(new byte[Recovery.MAX_FRAME])).isTrue();
        // Four fragments left at once (the window); the rest wait as backlog.
        assertThat(sim.a.backlogBytes()).isEqualTo(Recovery.MAX_FRAME - 4L * FRAGMENT);
        assertThat(sim.a.offer(new byte[5 * FRAGMENT])).isFalse();
        assertThat(sim.a.offer(new byte[4 * FRAGMENT])).isTrue();
    }

    @Test
    void oversizeAndEmptyFramesAreRefused() {
        Sim sim = new Sim(19, 0.0, 0.0, Recovery.MILLI, 0, 64);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> sim.a.offer(new byte[Recovery.MAX_FRAME + 1]))
                .isInstanceOf(java.io.IOException.class).hasMessageContaining("exceeds");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> sim.a.offer(new byte[0]))
                .isInstanceOf(java.io.IOException.class);
    }

    @Test
    void probeTimeoutBacksOffAndResetsOnTheNextAcknowledgement() throws Exception {
        Sim sim = new Sim(23, 1.0, 0.0, 5 * Recovery.MILLI, 0, 64);
        sim.a.offer(new byte[100]);
        long first = sim.a.nextDeadline() - sim.now;
        sim.now = sim.a.nextDeadline();
        sim.a.onTimer();
        long second = sim.a.nextDeadline() - sim.now;
        assertThat(second).isEqualTo(2 * first);
        // Heal the link: the retransmission arrives and its acknowledgement resets the backoff.
        Sim healed = new Sim(23, 0.0, 0.0, 5 * Recovery.MILLI, 0, 64);
        healed.a.offer(new byte[100]);
        healed.run(() -> !healed.deliveredAtB.isEmpty() && healed.a.inFlight() == 0,
                healed.now + 10_000 * Recovery.MILLI);
        assertThat(healed.a.smoothedRtt()).isBetween(9 * Recovery.MILLI, 40 * Recovery.MILLI);
    }

    @Test
    void anIdleAssociationKeepsAliveThenFailsWhenThePeerFallsSilent() {
        Sim sim = new Sim(29, 0.0, 0.0, Recovery.MILLI, 0, 64);
        sim.run(() -> sim.now > 120_000 * Recovery.MILLI, 120_001 * Recovery.MILLI);
        // Both ends PING each other, so neither idles out.
        assertThat(sim.a.failure()).isNull();
        assertThat(sim.b.failure()).isNull();

        Sim silent = new Sim(31, 1.0, 0.0, Recovery.MILLI, 0, 64);
        silent.run(() -> silent.a.failure() != null, 120_000 * Recovery.MILLI);
        assertThat(silent.a.failure().code()).isEqualTo(Pdu.CLOSE_IDLE);
        assertThat(silent.now).isBetween(IDLE, IDLE + Recovery.MILLI);
        assertThat(silent.countersA.failedIdle()).isEqualTo(1);
    }

    @Test
    void aReceiverAskedToHoldTooMuchClosesWithTheBoundCode() {
        DtlsCounters counters = new DtlsCounters();
        List<byte[]> sent = new ArrayList<>();
        Recovery receiver = new Recovery(sent::add, FRAGMENT, 64, IDLE, () -> 1L, counters);
        byte[] far = Pdu.data(0, Recovery.MAX_FRAMES_AHEAD, 0, 2, null, new byte[10], 0, 10);
        receiver.onPdu(far, 0, far.length);
        assertThat(receiver.failure().code()).isEqualTo(Pdu.CLOSE_BOUND);
        assertThat(sent.getLast()).isEqualTo(Pdu.close(Pdu.CLOSE_BOUND));
        assertThat(counters.failedBound()).isEqualTo(1);
    }

    @Test
    void malformedFragmentsCloseWithTheProtocolCode() {
        byte[][] cases = {
                // non-final fragments of unequal length
                Pdu.data(0, 0, 0, 3, null, new byte[100], 0, 100),
                Pdu.data(1, 0, 1, 3, null, new byte[99], 0, 99),
        };
        Recovery receiver = new Recovery(pdu -> { }, FRAGMENT, 64, IDLE, () -> 1L, new DtlsCounters());
        receiver.onPdu(cases[0], 0, cases[0].length);
        assertThat(receiver.failure()).isNull();
        receiver.onPdu(cases[1], 0, cases[1].length);
        assertThat(receiver.failure().code()).isEqualTo(Pdu.CLOSE_PROTOCOL);

        Recovery changed = new Recovery(pdu -> { }, FRAGMENT, 64, IDLE, () -> 1L, new DtlsCounters());
        byte[] one = Pdu.data(0, 0, 0, 3, null, new byte[10], 0, 10);
        byte[] two = Pdu.data(1, 0, 1, 4, null, new byte[10], 0, 10);
        changed.onPdu(one, 0, one.length);
        changed.onPdu(two, 0, two.length);
        assertThat(changed.failure().code()).isEqualTo(Pdu.CLOSE_PROTOCOL);

        Recovery longLast = new Recovery(pdu -> { }, FRAGMENT, 64, IDLE, () -> 1L, new DtlsCounters());
        byte[] head = Pdu.data(0, 0, 0, 2, null, new byte[10], 0, 10);
        byte[] tail = Pdu.data(1, 0, 1, 2, null, new byte[11], 0, 11);
        longLast.onPdu(head, 0, head.length);
        longLast.onPdu(tail, 0, tail.length);
        assertThat(longLast.failure().code()).isEqualTo(Pdu.CLOSE_PROTOCOL);
    }

    @Test
    void aPeerCloseEndsTheAssociationWithoutAnEcho() {
        List<byte[]> sent = new ArrayList<>();
        Recovery recovery = new Recovery(sent::add, FRAGMENT, 64, IDLE, () -> 1L, new DtlsCounters());
        byte[] close = Pdu.close(Pdu.CLOSE_NORMAL);
        recovery.onPdu(close, 0, close.length);
        assertThat(recovery.failure().byPeer()).isTrue();
        assertThat(sent).isEmpty();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> recovery.offer(new byte[1]))
                .isInstanceOf(java.io.IOException.class);
    }

    @Test
    void aPathChangeIsValidatedByAnAcknowledgementOfDataSentAfterIt() throws Exception {
        Sim sim = new Sim(37, 0.0, 0.0, Recovery.MILLI, 0, 64);
        sim.a.offer(new byte[10]);
        sim.run(() -> sim.a.inFlight() == 0, sim.now + 1_000 * Recovery.MILLI);
        sim.a.pathChanged();
        assertThat(sim.a.pathValidated()).isFalse();
        sim.a.offer(new byte[10]);
        sim.run(() -> sim.a.pathValidated(), sim.now + 1_000 * Recovery.MILLI);
        assertThat(sim.a.pathValidated()).isTrue();
    }

    private static byte[] randomFrame(Random random, int size) {
        byte[] frame = new byte[size];
        random.nextBytes(frame);
        return frame;
    }
}
