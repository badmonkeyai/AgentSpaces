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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.time.TimeSource;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The flush timer's invariants FT-1 to FT-6 (srad/agentspaces-event-loop.md §3.4, EL-B-01 to EL-B-05) on
 * {@link VirtualTime}, with a recording sink in place of the transport. One stress test runs on the system time source
 * because it is about real threads racing (FT-3); one records {@code core.flush.first-frame-latency} there (EL-B-04).
 * The class-level timeout is a hang guard only.
 */
@Timeout(120)
class CoalescerTest {

    private static final long MS = 1_000_000L;
    private static final Duration WINDOW = Duration.ofMillis(20);
    private static final GroupId GROUP = GroupId.of("zFlush");
    private static final GroupId OTHER_GROUP = GroupId.of("zOther");
    private static final PeerId A = PeerIdentity.generate().peerId();
    private static final PeerId B = PeerIdentity.generate().peerId();

    /** What reached the transport: one entry per frame sent, a BATCH as one entry holding its frames. */
    private record Sent(GroupId group, PeerId to, Envelope.Kind kind, List<byte[]> frames, long atNanos) {
    }

    private static class Sink implements Coalescer.Sink {
        final TimeSource time;
        final List<Sent> sent = new CopyOnWriteArrayList<>();
        volatile Runnable during = () -> { };

        Sink(TimeSource time) {
            this.time = time;
        }

        @Override
        public void single(GroupId group, PeerId to, Envelope.Kind kind, byte[] body) {
            sent.add(new Sent(group, to, kind, List.of(body), time.monoNanos()));
            during.run();
        }

        @Override
        public void batch(GroupId group, PeerId to, List<byte[]> frames) {
            sent.add(new Sent(group, to, Envelope.Kind.BATCH, List.copyOf(frames), time.monoNanos()));
            during.run();
        }

        int frames() {
            return sent.stream().mapToInt(s -> s.frames().size()).sum();
        }
    }

    private static byte[] frame(int producer, int seq) {
        return ByteBuffer.allocate(8).putInt(producer).putInt(seq).array();
    }

    private static void enqueue(Coalescer c, GroupId group, PeerId to, byte[] payload) {
        // The body and the "inner envelope" are the same bytes here: the sink only needs to tell frames apart.
        c.enqueue(group, to, Envelope.Kind.RUMOR, payload, payload);
    }

    /**
     * The adversarial review of phase 1 (srad/agentspaces-event-loop.md §10.10): §10.9 listed as open that a flush
     * thread dying inside one outbox's send leaves that outbox's send lock held by a dead thread. It does not: the
     * lock is released in a {@code finally}, which runs while the error unwinds, so the next flush of that outbox
     * goes through (the class-level timeout is the hang guard).
     */
    @Test
    void aFlushThreadThatDiesInsideASendLeavesTheOutboxUsable() throws Exception {
        VirtualTime time = VirtualTime.create();
        Sink sink = new Sink(time);
        Coalescer c = new Coalescer(time, WINDOW, sink, new LoopMeter());
        enqueue(c, GROUP, A, frame(0, 0));
        sink.during = () -> {
            sink.during = () -> { };
            throw new InternalError("scripted: the flush thread dies inside the send");
        };
        List<Throwable> died = new CopyOnWriteArrayList<>();
        Thread flusher = Thread.ofPlatform().unstarted(c::flushOutboxes);
        flusher.setUncaughtExceptionHandler((t, e) -> died.add(e));
        flusher.start();
        flusher.join();
        assertThat(died).singleElement().isInstanceOf(InternalError.class);
        enqueue(c, GROUP, A, frame(0, 1));
        c.flushOutboxes(); // blocks for good if the dead thread still held the outbox's send lock
        assertThat(sink.sent).hasSize(2);
        assertThat(sink.frames()).isEqualTo(2);
    }

    // FT-1 -----------------------------------------------------------------------------------------------------

    @Test
    void anIdleNodeArmsNoTimerAndOneEnqueueArmsOneThatFlushesEveryOutbox() {
        VirtualTime time = VirtualTime.create();
        LoopMeter meter = new LoopMeter();
        Sink sink = new Sink(time);
        Coalescer c = new Coalescer(time, WINDOW, sink, meter);
        time.advance(Duration.ofMinutes(1));
        assertThat(time.pending()).as("FT-1: an idle node arms no flush timer").isZero();
        assertThat(time.tasksRun()).isZero();

        enqueue(c, GROUP, A, frame(0, 0));
        enqueue(c, GROUP, B, frame(0, 1));
        enqueue(c, GROUP, A, frame(0, 2));
        enqueue(c, OTHER_GROUP, A, frame(0, 3));
        assertThat(time.pending()).as("FT-1: one timer for the node, not one per outbox").isEqualTo(1);
        time.advance(WINDOW);
        assertThat(sink.sent).as("every non-empty outbox flushed by the one timer").hasSize(3);
        assertThat(sink.frames()).isEqualTo(4);
        assertThat(time.pending()).as("the timer fired once and is not re-armed while idle").isZero();
        time.advance(Duration.ofMinutes(1));
        LoopMeter.Figures stats = meter.snapshot();
        assertThat(stats.flushRuns()).as("one flush run in two idle minutes and one burst").isEqualTo(1);
        assertThat(stats.flushesWithFrames()).isEqualTo(1);
    }

    @Test
    void framesToOnePeerInTwoGroupsGoInTwoBatchesEachUnderItsOwnGroup() {
        VirtualTime time = VirtualTime.create();
        Sink sink = new Sink(time);
        Coalescer c = new Coalescer(time, WINDOW, sink, new LoopMeter());
        enqueue(c, GROUP, A, frame(0, 0));
        enqueue(c, GROUP, A, frame(0, 1));
        enqueue(c, OTHER_GROUP, A, frame(1, 0));
        enqueue(c, OTHER_GROUP, A, frame(1, 1));
        time.advance(WINDOW);
        Map<GroupId, Integer> byGroup = new HashMap<>();
        for (Sent s : sink.sent) {
            assertThat(s.kind()).isEqualTo(Envelope.Kind.BATCH);
            byGroup.merge(s.group(), s.frames().size(), Integer::sum);
        }
        assertThat(byGroup).containsEntry(GROUP, 2).containsEntry(OTHER_GROUP, 2);
    }

    // FT-2 -----------------------------------------------------------------------------------------------------

    @Test
    void aFrameWaitsAtMostTheWindowFromItsOwnEnqueue() {
        VirtualTime time = VirtualTime.create();
        LoopMeter meter = new LoopMeter();
        Sink sink = new Sink(time);
        Coalescer c = new Coalescer(time, WINDOW, sink, meter);
        long t0 = time.monoNanos();
        enqueue(c, GROUP, A, frame(0, 0));
        time.advance(Duration.ofMillis(15));
        enqueue(c, GROUP, B, frame(0, 1)); // a later outbox: flushed early by the first frame's timer
        time.advance(Duration.ofMillis(5));
        assertThat(sink.sent).hasSize(2).allSatisfy(s -> assertThat(s.atNanos()).isEqualTo(t0 + 20 * MS));
        enqueue(c, GROUP, A, frame(0, 2));
        long t1 = time.monoNanos();
        time.advance(Duration.ofMillis(19));
        assertThat(sink.sent).as("not before its window").hasSize(2);
        time.advance(Duration.ofMillis(1));
        assertThat(sink.sent).hasSize(3);
        assertThat(sink.sent.get(2).atNanos() - t1).as("FT-2").isEqualTo(WINDOW.toNanos());
        LoopMeter.Figures stats = meter.snapshot();
        assertThat(stats.flushWaitMaxNanos()).as("EL-B-04 on virtual time: the window exactly")
                .isEqualTo(WINDOW.toNanos());
    }

    // FT-3 -----------------------------------------------------------------------------------------------------

    @Test
    void aSizeFlushIsUnchangedAndATimerThatFindsNothingDoesNothing() {
        VirtualTime time = VirtualTime.create();
        LoopMeter meter = new LoopMeter();
        Sink sink = new Sink(time);
        Coalescer c = new Coalescer(time, WINDOW, sink, meter);
        for (int i = 0; i < Coalescer.BATCH_MAX_FRAMES; i++) {
            enqueue(c, GROUP, A, frame(0, i));
        }
        assertThat(sink.sent).as("EL-B-02: the 64th frame flushes at once").hasSize(1);
        assertThat(sink.sent.get(0).frames()).hasSize(Coalescer.BATCH_MAX_FRAMES);
        time.advance(WINDOW);
        assertThat(sink.sent).as("the armed timer found the outbox empty and sent nothing").hasSize(1);
        LoopMeter.Figures stats = meter.snapshot();
        assertThat(stats.flushRuns()).isEqualTo(1);
        assertThat(stats.flushesWithFrames()).isZero();

        byte[] big = new byte[Coalescer.BATCH_MAX_BYTES];
        enqueue(c, GROUP, B, big);
        assertThat(sink.sent).as("the byte bound flushes at once too").hasSize(2);
    }

    @Test
    void noFrameIsLostOrSentTwiceAndEachProducersOrderHoldsUnderRacingFlushes() throws Exception {
        TimeSource time = TimeSource.system();
        Sink sink = new Sink(time);
        Coalescer c = new Coalescer(time, Duration.ofMillis(1), sink, new LoopMeter()); // a short window: many races
        int producers = 8;
        int each = 3_000;
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            int producer = p;
            threads.add(Thread.ofVirtual().start(() -> {
                for (int i = 0; i < each; i++) {
                    enqueue(c, GROUP, producer % 2 == 0 ? A : B, frame(producer, i)); // size flushes race the timer
                }
            }));
        }
        for (Thread t : threads) {
            t.join();
        }
        c.close(); // flushes what is left
        Map<Integer, Integer> next = new HashMap<>();
        int total = 0;
        List<String> problems = new ArrayList<>();
        for (Sent s : sink.sent) {
            for (byte[] f : s.frames()) {
                ByteBuffer b = ByteBuffer.wrap(f);
                int producer = b.getInt();
                int seq = b.getInt();
                PeerId expectedTo = producer % 2 == 0 ? A : B;
                if (!s.to().equals(expectedTo)) {
                    problems.add("frame " + producer + ":" + seq + " went to the wrong peer");
                }
                int want = next.getOrDefault(producer, 0);
                if (seq != want) {
                    problems.add("producer " + producer + " expected " + want + " got " + seq);
                }
                next.put(producer, seq + 1);
                total++;
            }
        }
        assertThat(problems).as("FT-3: per-peer order, no loss, no duplicate").isEmpty();
        assertThat(total).isEqualTo(producers * each);
        assertThat(c.waiting()).isZero();
    }

    // FT-4 -----------------------------------------------------------------------------------------------------

    @Test
    void aSingleFrameFlushSendsTheKindAndBodyKeptBesideItsBytes() {
        VirtualTime time = VirtualTime.create();
        Sink sink = new Sink(time);
        Coalescer c = new Coalescer(time, WINDOW, sink, new LoopMeter());
        byte[] body = {1, 2, 3};
        byte[] inner = {9, 9, 9, 9, 9};
        c.enqueue(GROUP, A, Envelope.Kind.HAVE, body, inner);
        time.advance(WINDOW);
        assertThat(sink.sent).hasSize(1);
        Sent s = sink.sent.get(0);
        assertThat(s.kind()).as("FT-4: the frame's own kind, not a BATCH").isEqualTo(Envelope.Kind.HAVE);
        assertThat(s.frames().get(0)).as("its body, with no decode of the inner envelope").isEqualTo(body);
        assertThat(s.group()).isEqualTo(GROUP);
    }

    // FT-5 -----------------------------------------------------------------------------------------------------

    @Test
    void aFrameEnqueuedDuringTheWalkIsFlushedByTheWalkOrArmsANewTimer() {
        VirtualTime time = VirtualTime.create();
        LoopMeter meter = new LoopMeter();
        Sink sink = new Sink(time);
        Coalescer c = new Coalescer(time, WINDOW, sink, meter);
        enqueue(c, GROUP, A, frame(0, 0));
        enqueue(c, GROUP, B, frame(1, 0));
        // While the timer's walk sends, a reply lands in both outboxes: one the walk has passed, one it has not
        // (the walk's order over the outboxes is not fixed, so both cases occur, one per outbox).
        sink.during = () -> {
            sink.during = () -> { };
            enqueue(c, GROUP, A, frame(0, 1));
            enqueue(c, GROUP, B, frame(1, 1));
        };
        long t0 = time.monoNanos();
        time.advance(WINDOW);
        assertThat(c.armed()).as("FT-5: the walk cleared armed first, so the late enqueue armed a new timer")
                .isTrue();
        time.advance(WINDOW);
        assertThat(sink.frames()).as("no frame stranded").isEqualTo(4);
        assertThat(c.waiting()).isZero();
        assertThat(sink.sent.get(sink.sent.size() - 1).atNanos() - t0).as("the late frame within its own window")
                .isLessThanOrEqualTo(2 * WINDOW.toNanos());
        LoopMeter.Figures stats = meter.snapshot();
        assertThat(stats.flushRuns()).isEqualTo(2);
        time.advance(Duration.ofSeconds(10));
        assertThat(time.pending()).as("and the node is idle again").isZero();
    }

    // FT-6 and EL-B-03 -----------------------------------------------------------------------------------------

    @Test
    void closeCancelsTheTimerFlushesOnceAndAFrameRacingPastTheStopIsSentAtOnce() {
        VirtualTime time = VirtualTime.create();
        Sink sink = new Sink(time);
        Coalescer c = new Coalescer(time, WINDOW, sink, new LoopMeter());
        enqueue(c, GROUP, A, frame(0, 0));
        enqueue(c, GROUP, B, frame(1, 0));
        assertThat(time.pending()).isEqualTo(1);
        c.close();
        assertThat(c.stopped()).as("FT-6: the node sends directly from now on").isTrue();
        assertThat(time.pending()).as("EL-B-03: the timer is cancelled").isZero();
        assertThat(sink.frames()).as("EL-B-03: every outbox flushed once").isEqualTo(2);
        enqueue(c, GROUP, A, frame(0, 1)); // a send that tested the flag just before close
        assertThat(sink.frames()).as("FT-6: never parked in an outbox nobody flushes again").isEqualTo(3);
        assertThat(time.pending()).isZero();
        assertThat(c.waiting()).isZero();
    }

    @Test
    void aLeftGroupsFramesAreDiscardedAndNeverSent() {
        VirtualTime time = VirtualTime.create();
        Sink sink = new Sink(time);
        Coalescer c = new Coalescer(time, WINDOW, sink, new LoopMeter());
        enqueue(c, GROUP, A, frame(0, 0));
        enqueue(c, OTHER_GROUP, A, frame(1, 0));
        c.discard(GROUP);
        time.advance(WINDOW);
        assertThat(sink.sent).hasSize(1);
        assertThat(sink.sent.get(0).group()).isEqualTo(OTHER_GROUP);
    }

    // EL-B-04 on the system time source ------------------------------------------------------------------------

    @Test
    void theFirstFrameIntoAnIdleNodeWaitsAboutTheWindowOnTheSystemTimer() throws Exception {
        TimeSource time = TimeSource.system();
        LoopMeter meter = new LoopMeter();
        int rounds = 20;
        CountDownLatch[] flushed = new CountDownLatch[1];
        Sink sink = new Sink(time) {
            @Override
            public void single(GroupId group, PeerId to, Envelope.Kind kind, byte[] body) {
                super.single(group, to, kind, body);
                flushed[0].countDown();
            }
        };
        Coalescer c = new Coalescer(time, WINDOW, sink, meter);
        double[] waits = new double[rounds];
        for (int i = 0; i < rounds; i++) {
            flushed[0] = new CountDownLatch(1);
            long enqueued = time.monoNanos();
            enqueue(c, GROUP, A, frame(0, i));
            flushed[0].await();
            waits[i] = (sink.sent.get(sink.sent.size() - 1).atNanos() - enqueued) / 1e6;
        }
        Measure.samples("core.flush.first-frame-latency", Unit.MS, Direction.LOWER, waits,
                Rec.with().params(Params.of("harness", "system", "window-ms", 20)).kind(Kind.PERF));
        java.util.Arrays.sort(waits);
        System.out.printf("EL-B-04: first-frame wait over %d idle rounds: median %.2f ms, max %.2f ms%n", rounds,
                waits[rounds / 2], waits[rounds - 1]);
        LoopMeter.Figures stats = meter.snapshot();
        assertThat(stats.flushWaitSamples()).isEqualTo(rounds);
        assertThat(waits[0]).as("a timer never fires before its deadline")
                .isGreaterThanOrEqualTo(WINDOW.toNanos() / 1e6);
    }
}
