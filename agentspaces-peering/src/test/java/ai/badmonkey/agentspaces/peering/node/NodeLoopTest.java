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

import ai.badmonkey.agentspaces.api.spi.DueWork;
import ai.badmonkey.agentspaces.common.time.TimeSource;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link NodeLoop}'s invariants NL-1 to NL-9 (srad/agentspaces-event-loop.md §3.3), the backstop's missed-wake count
 * (EL-Q-REL-03), the run order (EL-L-16) and the registration contract (§3.2), on {@link VirtualTime}. Two stress
 * tests run on the system time source because they are about real threads racing; they assert no timing, and the
 * class-level timeout is a hang guard only.
 */
@Timeout(120)
class NodeLoopTest {

    private static final long MS = 1_000_000L;
    private static final Duration BACKSTOP = NodeLoop.DEFAULT_BACKSTOP;

    private final VirtualTime time = VirtualTime.create();
    private final List<NodeLoop> loops = new ArrayList<>();

    @AfterEach
    void tearDown() {
        loops.forEach(NodeLoop::close);
    }

    private NodeLoop loop(TimeSource source) {
        NodeLoop loop = new NodeLoop("node:test", source, BACKSTOP);
        loops.add(loop);
        return loop;
    }

    /** A participant that counts its runs and returns the deadline the test holds for it. */
    private static final class Probe implements DueWork {
        final AtomicInteger runs = new AtomicInteger();
        final AtomicLong next = new AtomicLong(DueWork.NEVER);
        final List<Long> ranAt = new CopyOnWriteArrayList<>();
        volatile Runnable during = () -> { };

        @Override
        public long run(long nowNanos) {
            runs.incrementAndGet();
            ranAt.add(nowNanos);
            during.run();
            long n = next.get();
            return n;
        }
    }

    // EL-Q-MNT-01 ----------------------------------------------------------------------------------------------

    /**
     * EL-Q-MNT-01: the loop keeps its last 64 participant runs (participant, due, ran at, took, why) and logs them on
     * request. A hundred runs of two participants leave the newest 64, oldest first; the text names each.
     */
    @Test
    void theLoopKeepsTheLastSixtyFourRunsAndLogsThemOnRequest() {
        NodeLoop loop = loop(time);
        Probe a = new Probe();
        Probe b = new Probe();
        DueWork.Registration ra = loop.register("a", NodeLoop.Kind.OTHER, "g", a);
        loop.register("b", NodeLoop.Kind.OTHER, "g", b);
        loop.start();
        time.runDue();
        assertThat(loop.trace()).extracting(NodeLoop.TraceEntry::participant).containsExactly("a", "b");
        assertThat(loop.trace()).as("a new participant is asked at once").extracting(NodeLoop.TraceEntry::cause).containsOnly("asked");
        a.next.set(time.monoNanos() + 10 * MS); // a falls due every 10 ms; b only when asked
        ra.wake();
        time.runDue();
        for (int i = 0; i < 99; i++) {
            a.next.set(time.monoNanos() + 20 * MS);
            time.advance(Duration.ofMillis(10));
        }
        List<NodeLoop.TraceEntry> trace = loop.trace();
        assertThat(trace).as("a fixed ring of the last 64").hasSize(NodeLoop.TRACE_RUNS);
        assertThat(trace).extracting(NodeLoop.TraceEntry::ranAtNanos).isSorted();
        assertThat(trace.get(trace.size() - 1).ranAtNanos()).isEqualTo(time.monoNanos());
        NodeLoop.TraceEntry last = trace.get(trace.size() - 1);
        assertThat(last.participant()).isEqualTo("a");
        assertThat(last.cause()).isEqualTo("due");
        assertThat(last.dueNanos()).as("it ran at the deadline it held").isEqualTo(last.ranAtNanos());
        assertThat(last.tookNanos()).isNotNegative();
        assertThat(last.next()).as("the deadline it returned, set 10 ms before it ran").isEqualTo(last.ranAtNanos() + 10 * MS);
        String text = loop.logTrace();
        assertThat(text.lines()).hasSize(1 + NodeLoop.TRACE_RUNS);
        assertThat(text.lines().findFirst().orElseThrow()).contains("node:test").contains("the last 64 of");
        assertThat(text).contains("a at ").contains("due +0.000 ms").contains(", due, next +10.000 ms");
    }

    // NL-1 -----------------------------------------------------------------------------------------------------

    @Test
    void manyWakesBetweenRunsCostOneRun() {
        NodeLoop loop = loop(time);
        Probe p = new Probe();
        DueWork.Registration reg = loop.register("p", NodeLoop.Kind.OTHER, "g", p);
        loop.start();
        time.runDue();
        long runs = loop.stats().runs();
        for (int i = 0; i < 100; i++) {
            reg.wake();
        }
        assertThat(loop.pendingRun()).as("the wake channel's one slot is full").isTrue();
        time.runDue();
        assertThat(loop.stats().runs() - runs).as("NL-1: a hundred wakes on a full channel are one run").isEqualTo(1);
        assertThat(p.runs.get()).isEqualTo(2);
    }

    @Test
    void wakesDuringARunCauseExactlyOneMoreRun() {
        NodeLoop loop = loop(time);
        Probe p = new Probe();
        AtomicReference<DueWork.Registration> self = new AtomicReference<>();
        p.during = () -> {
            if (p.runs.get() == 1) {
                for (int i = 0; i < 100; i++) {
                    self.get().wake();
                }
            }
        };
        self.set(loop.register("p", NodeLoop.Kind.OTHER, "g", p));
        loop.start();
        time.runDue();
        assertThat(p.runs.get()).as("NL-1: the wakes raised during the run cause one more run").isEqualTo(2);
        assertThat(loop.stats().runs()).isEqualTo(2);
        assertThat(loop.pendingRun()).isFalse();
    }

    // NL-2 -----------------------------------------------------------------------------------------------------

    @Test
    void participantsOfOneNodeNeverRunConcurrently() throws Exception {
        NodeLoop loop = loop(TimeSource.system());
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger most = new AtomicInteger();
        AtomicInteger total = new AtomicInteger();
        List<DueWork.Registration> regs = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            regs.add(loop.register("p" + i, i % 2 == 0 ? NodeLoop.Kind.PROTOCOL : NodeLoop.Kind.OTHER, "g" + (i % 2),
                    now -> {
                        most.accumulateAndGet(inside.incrementAndGet(), Math::max);
                        total.incrementAndGet();
                        inside.decrementAndGet();
                        return now + Duration.ofMillis(3).toNanos();
                    }));
        }
        loop.start();
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            int which = t;
            threads.add(Thread.ofVirtual().start(() -> {
                for (int i = 0; i < 2_000; i++) {
                    switch ((which + i) % 4) {
                        case 0 -> loop.tick();
                        case 1 -> loop.wakeGroup("g" + (i % 2));
                        case 2 -> loop.wakeAll();
                        default -> regs.get(i % regs.size()).wake();
                    }
                }
            }));
        }
        for (Thread t : threads) {
            t.join();
        }
        loop.tick();
        assertThat(most.get()).as("NL-2: participants never run concurrently, across wakes, timers and tick()")
                .isEqualTo(1);
        assertThat(total.get()).isGreaterThan(0);
    }

    // NL-3 -----------------------------------------------------------------------------------------------------

    @Test
    void theTimerIsArmedAtTheEarliestDeadlineCappedByTheBackstop() {
        NodeLoop loop = loop(time);
        long t0 = time.monoNanos();
        Probe soon = new Probe();
        soon.next.set(t0 + 300 * MS);
        Probe later = new Probe();
        later.next.set(t0 + BACKSTOP.toMillis() * MS + 5_000 * MS); // past the backstop
        Probe never = new Probe();
        loop.register("soon", NodeLoop.Kind.PROTOCOL, "g", soon);
        loop.register("later", NodeLoop.Kind.OTHER, "g", later);
        loop.register("never", NodeLoop.Kind.OTHER, "g", never);
        loop.start();
        time.runDue();
        assertThat(loop.armedNanos()).as("NL-3: at the earliest next due").isEqualTo(t0 + 300 * MS);
        soon.next.set(DueWork.NEVER);
        time.advance(Duration.ofMillis(300));
        assertThat(soon.ranAt).as("the participant ran at its deadline exactly").endsWith(t0 + 300 * MS);
        assertThat(loop.armedNanos()).as("NL-3: capped by now + backstop when that is earlier")
                .isEqualTo(t0 + BACKSTOP.toNanos());
        assertThat(later.runs.get()).as("a participant not yet due is not run by a deadline run").isEqualTo(1);
        NodeLoop.Stats stats = loop.stats();
        assertThat(stats.timerWakes()).isEqualTo(1);
        assertThat(stats.latenessMaxNanos()).as("virtual time fires on the deadline").isZero();
    }

    /**
     * EL-Q-REL-03 (§10.39's blind spot, §10.47): a participant that judges "due now" for itself from an input no wake
     * covers does that work on the backstop's sweep and returns the same deadline as before. The sweep found work, and
     * the counter says so; the same work on an asked run is not a missed wake.
     */
    @Test
    void aSweepThatDoesOverdueWorkAndKeepsItsDeadlineCountsAsFindingWork() {
        NodeLoop loop = loop(time).markScripted(); // the find is this test's own: the end-of-build read leaves it out
        AtomicLong ownDue = new AtomicLong(Long.MAX_VALUE); // moved below without a wake: a missed signal
        AtomicLong far = new AtomicLong(DueWork.NEVER);
        AtomicInteger worked = new AtomicInteger();
        DueWork.Registration reg = loop.register("self-judging", NodeLoop.Kind.PROTOCOL, "g", now -> {
            long due = ownDue.get();
            if (due - now <= 0) {
                NodeLoop.foundOverdue(due);
                ownDue.set(Long.MAX_VALUE);
                worked.incrementAndGet();
            }
            return far.get(); // the same deadline on every run
        });
        loop.start();
        far.set(time.monoNanos() + Duration.ofHours(1).toNanos());
        reg.wake();
        time.runDue();
        assertThat(loop.stats().backstopFoundTotal()).isZero();
        ownDue.set(time.monoNanos()); // due now, and nobody woke the participant
        time.advance(BACKSTOP.plusSeconds(1));
        assertThat(worked.get()).as("the sweep did the work").isEqualTo(1);
        assertThat(loop.stats().backstopFoundWork()).as("the sweep found work the loop held for later")
                .containsEntry("self-judging", 1L);
        ownDue.set(time.monoNanos());
        reg.wake(); // the same work, asked for: not a missed wake
        time.runDue();
        assertThat(worked.get()).isEqualTo(2);
        assertThat(loop.stats().backstopFoundTotal()).isEqualTo(1);
        time.advance(BACKSTOP.plusSeconds(1)); // a sweep with nothing overdue
        assertThat(loop.stats().backstopFoundTotal()).isEqualTo(1);
    }

    @Test
    void anIdleLoopWakesOncePerBackstopAndTheBackstopFindsNothing() {
        NodeLoop loop = loop(time);
        Probe a = new Probe();
        Probe b = new Probe();
        loop.register("a", NodeLoop.Kind.PROTOCOL, "g", a);
        loop.register("b", NodeLoop.Kind.OTHER, "g", b);
        loop.start();
        time.runDue();
        long before = loop.stats().runs();
        time.advance(Duration.ofMinutes(1));
        NodeLoop.Stats stats = loop.stats();
        long perMinute = stats.runs() - before;
        long sweeps = Duration.ofMinutes(1).toNanos() / BACKSTOP.toNanos();
        Measure.record("core.idle.backstop-runs", Unit.COUNT, Direction.LOWER, stats.backstopRuns(),
                Rec.with().params(Params.of("harness", "virtual", "participants", 2)).kind(Kind.COUNT));
        assertThat(sweeps).as("decision 3 a after phase 6: at most 4 sweeps a minute").isLessThanOrEqualTo(4);
        assertThat(perMinute).as("one run per backstop period at idle (decision 3 a: 15 s after phase 6)")
                .isEqualTo(sweeps);
        assertThat(stats.backstopRuns()).isEqualTo(sweeps);
        assertThat(stats.backstopFoundTotal()).as("EL-Q-REL-03: no missed wake").isZero();
        assertThat(a.runs.get()).as("the sweep asks every participant (DW-1)").isEqualTo(sweeps + 1);
    }

    // NL-4 -----------------------------------------------------------------------------------------------------

    @Test
    void aThrowingParticipantIsCountedReArmedAtTheBackstopAndTheOthersRun() {
        NodeLoop loop = loop(time);
        long t0 = time.monoNanos();
        Probe after = new Probe();
        loop.register("thrower", NodeLoop.Kind.PROTOCOL, "g", now -> {
            throw new IllegalStateException("expected by the test");
        });
        loop.register("after", NodeLoop.Kind.OTHER, "g", after);
        loop.start();
        time.runDue();
        assertThat(after.runs.get()).as("NL-4: the others run").isEqualTo(1);
        assertThat(loop.stats().participantThrows()).containsEntry("thrower", 1L);
        assertThat(loop.dueOf("thrower")).as("re-armed at the backstop").isEqualTo(t0 + BACKSTOP.toNanos());
        time.advance(BACKSTOP);
        assertThat(loop.stats().participantThrows()).containsEntry("thrower", 2L);
        assertThat(after.runs.get()).isGreaterThanOrEqualTo(2);
    }

    // NL-5 -----------------------------------------------------------------------------------------------------

    @Test
    void closeStopsTheLoopAndCancelsItsTimer() {
        NodeLoop loop = loop(time);
        Probe p = new Probe();
        AtomicReference<Boolean> inside = new AtomicReference<>();
        p.during = () -> inside.set(loop.insideRun());
        DueWork.Registration reg = loop.register("p", NodeLoop.Kind.OTHER, "g", p);
        loop.start();
        time.runDue();
        assertThat(inside.get()).as("NL-5: inside the run, by binding, not by thread name").isTrue();
        assertThat(loop.insideRun()).isFalse();
        assertThat(time.pending()).as("the backstop timer is armed").isEqualTo(1);
        loop.close();
        assertThat(time.pending()).as("close cancels the armed timer").isZero();
        reg.wake();
        loop.tick();
        time.advance(Duration.ofSeconds(10));
        assertThat(p.runs.get()).as("nothing runs after close").isEqualTo(1);
    }

    @Test
    void tickFromInsideARunRunsInlineAndDoesNotReEnterTheCaller() {
        NodeLoop loop = loop(time);
        Probe caller = new Probe();
        Probe other = new Probe();
        caller.during = loop::tick; // an onTick-style runnable that calls tick() (EL-L-16)
        loop.register("caller", NodeLoop.Kind.PROTOCOL, "g", caller);
        loop.register("other", NodeLoop.Kind.OTHER, "g", other);
        loop.start();
        time.runDue();
        assertThat(caller.runs.get()).as("the caller is not re-entered").isEqualTo(1);
        assertThat(other.runs.get()).as("the nested tick ran the other participant, and the outer run once more")
                .isEqualTo(2);
    }

    @Test
    void tickBeforeStartRunsOnTheCallersThreadAndArmsNothing() {
        NodeLoop loop = loop(time);
        AtomicReference<Thread> ranOn = new AtomicReference<>();
        loop.register("p", NodeLoop.Kind.OTHER, "g", now -> {
            ranOn.set(Thread.currentThread());
            return now + 10 * MS;
        });
        loop.tick();
        assertThat(ranOn.get()).isSameAs(Thread.currentThread());
        assertThat(time.pending()).as("a loop never started arms no timer").isZero();
        assertThat(loop.stats().directRuns()).isEqualTo(1);
    }

    // NL-6 -----------------------------------------------------------------------------------------------------

    @Test
    void statsCountEachKindOfRun() {
        AtomicLong real = new AtomicLong();
        NodeLoop loop = new NodeLoop("node:stats", time, BACKSTOP, real::get);
        loops.add(loop);
        long t0 = time.monoNanos();
        Probe timed = new Probe();
        timed.next.set(t0 + 200 * MS);
        Probe slow = new Probe();
        slow.during = () -> real.addAndGet(150 * MS); // a run that took 150 ms of real time
        DueWork.Registration slowReg = loop.register("slow", NodeLoop.Kind.OTHER, "g", slow);
        loop.register("timed", NodeLoop.Kind.PROTOCOL, "g", timed);
        loop.start();
        time.runDue();                       // the first run: an event wake (start), slow
        timed.next.set(DueWork.NEVER);
        time.advance(Duration.ofMillis(200)); // a timer wake
        slow.during = () -> { };
        slowReg.wake();
        time.runDue();                       // an event wake
        loop.tick();                         // a direct run
        time.advance(BACKSTOP);              // a backstop run
        NodeLoop.Stats stats = loop.stats();
        assertThat(stats.runs()).isEqualTo(5);
        assertThat(stats.eventWakes()).isEqualTo(2);
        assertThat(stats.timerWakes()).isEqualTo(2);
        assertThat(stats.directRuns()).isEqualTo(1);
        assertThat(stats.backstopRuns()).isEqualTo(1);
        assertThat(stats.slowRuns()).as("a run over 100 ms of real time is slow (EL-X-10)").isEqualTo(1);
        assertThat(stats.busyNanos()).isEqualTo(150 * MS);
        assertThat(stats.latenessSamples()).isEqualTo(2);
        assertThat(stats.overruns()).isZero();
        assertThat(stats.generation()).isZero();
    }

    // NL-7 -----------------------------------------------------------------------------------------------------

    @Test
    void aWakeForAParticipantAlreadyPassedInThisRunStartsTheNextRun() {
        NodeLoop loop = loop(time);
        Probe first = new Probe();
        Probe second = new Probe();
        AtomicReference<DueWork.Registration> firstReg = new AtomicReference<>();
        second.during = () -> {
            if (second.runs.get() == 1) {
                firstReg.get().wake(); // first's slot in this run has passed
            }
        };
        firstReg.set(loop.register("first", NodeLoop.Kind.PROTOCOL, "g", first));
        loop.register("second", NodeLoop.Kind.OTHER, "g", second);
        loop.start();
        time.runDue();
        assertThat(first.runs.get()).as("NL-7: the wake is seen by the next run").isEqualTo(2);
        assertThat(second.runs.get()).isEqualTo(1);
    }

    @Test
    void aWakeForAParticipantStillAheadInThisRunIsSeenByThisRun() {
        NodeLoop loop = loop(time);
        Probe first = new Probe();
        Probe second = new Probe();
        AtomicReference<DueWork.Registration> secondReg = new AtomicReference<>();
        first.during = () -> {
            if (first.runs.get() == 2) {
                secondReg.get().wake();
            }
        };
        DueWork.Registration firstReg = loop.register("first", NodeLoop.Kind.PROTOCOL, "g", first);
        secondReg.set(loop.register("second", NodeLoop.Kind.OTHER, "g", second));
        loop.start();
        time.runDue();
        firstReg.wake();
        time.runDue();
        assertThat(second.runs.get()).as("NL-7: asked before its slot, run in this run").isEqualTo(2);
        assertThat(loop.stats().runs()).as("plus at most one empty run for the absorbed nudge").isBetween(2L, 3L);
    }

    @Test
    void noWakeIsLostUnderRacingWakers() throws Exception {
        // A backstop of an hour, so only the wakes themselves can deliver the last value: a lost wake would leave
        // the participant behind and the hang guard would end the test.
        NodeLoop loop = new NodeLoop("node:race", TimeSource.system(), Duration.ofHours(1));
        loops.add(loop);
        AtomicLong published = new AtomicLong();
        AtomicLong seen = new AtomicLong();
        int wakers = 8;
        int each = 5_000;
        long last = (long) wakers * each;
        CountDownLatch caughtUp = new CountDownLatch(1);
        AtomicReference<DueWork.Registration> reg = new AtomicReference<>();
        reg.set(loop.register("reader", NodeLoop.Kind.PROTOCOL, "g", now -> {
            long v = published.get();
            seen.set(v);
            if (v == last) {
                caughtUp.countDown();
            }
            return DueWork.NEVER;
        }));
        loop.start();
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < wakers; t++) {
            threads.add(Thread.ofVirtual().start(() -> {
                for (int i = 0; i < each; i++) {
                    published.incrementAndGet(); // the change first, then the wake (DW-2)
                    reg.get().wake();
                }
            }));
        }
        for (Thread t : threads) {
            t.join();
        }
        caughtUp.await();
        assertThat(seen.get()).isEqualTo(last);
        assertThat(loop.stats().backstopFoundTotal()).isZero();
    }

    // NL-8 -----------------------------------------------------------------------------------------------------

    @Test
    void protocolParticipantsOfEveryGroupRunFirstAndGroupsInIdOrder() {
        NodeLoop loop = loop(time);
        List<String> order = new CopyOnWriteArrayList<>();
        loop.register("app-b", NodeLoop.Kind.OTHER, "b", now -> { order.add("app-b"); return DueWork.NEVER; });
        loop.register("gossip-b", NodeLoop.Kind.PROTOCOL, "b", now -> { order.add("gossip-b"); return DueWork.NEVER; });
        loop.register("store-a", NodeLoop.Kind.OTHER, "a", now -> { order.add("store-a"); return DueWork.NEVER; });
        loop.register("members-a", NodeLoop.Kind.PROTOCOL, "a",
                now -> { order.add("members-a"); return DueWork.NEVER; });
        loop.register("gossip-a", NodeLoop.Kind.PROTOCOL, "a", now -> { order.add("gossip-a"); return DueWork.NEVER; });
        loop.register("node", NodeLoop.Kind.OTHER, "", now -> { order.add("node"); return DueWork.NEVER; });
        loop.tick();
        assertThat(order).as("NL-8").containsExactly("members-a", "gossip-a", "gossip-b", "node", "store-a", "app-b");
        assertThat(loop.participants()).containsExactlyElementsOf(order);
    }

    // NL-9 -----------------------------------------------------------------------------------------------------

    @Test
    void theHeartbeatShowsTheRunInProgressAndClearsAtItsEnd() {
        NodeLoop loop = loop(time);
        AtomicReference<String> runningIn = new AtomicReference<>();
        AtomicLong startedAt = new AtomicLong();
        AtomicReference<Thread> thread = new AtomicReference<>();
        loop.register("watched", NodeLoop.Kind.PROTOCOL, "g", now -> {
            runningIn.set(loop.runningIn());
            startedAt.set(loop.runStartedNanos());
            thread.set(loop.runThread() == Thread.currentThread() ? Thread.currentThread() : null);
            return DueWork.NEVER;
        });
        loop.start();
        long beats = loop.heartbeatRuns();
        time.runDue();
        assertThat(runningIn.get()).as("NL-9: the participant running now").isEqualTo("watched");
        assertThat(startedAt.get()).isEqualTo(time.monoNanos());
        assertThat(thread.get()).as("the run's thread").isNotNull();
        assertThat(loop.runningIn()).isEmpty();
        assertThat(loop.runStartedNanos()).isZero();
        assertThat(loop.runThread()).isNull();
        assertThat(loop.heartbeatRuns()).isEqualTo(beats + 1);
        assertThat(loop.armedNanos()).isEqualTo(time.monoNanos() + BACKSTOP.toNanos());
    }

    // The backstop's missed-wake count (EL-Q-REL-03) and the registration contract -----------------------------

    @Test
    void theBackstopCountsAParticipantWhoseDeadlineMovedEarlierWithoutAWake() {
        NodeLoop loop = loop(time).markScripted(); // the dropped wake is this test's own
        long processBefore = ai.badmonkey.agentspaces.common.time.LoopTotals.backstopFoundWork();
        long scriptedBefore = ai.badmonkey.agentspaces.common.time.LoopTotals.scriptedFinds();
        long t0 = time.monoNanos();
        long far = BACKSTOP.toMillis() * 4 * MS; // held well past the sweep, so only the sweep can find the move
        Probe quiet = new Probe();
        quiet.next.set(t0 + far);
        Probe woken = new Probe();
        woken.next.set(t0 + far);
        loop.register("forgot-to-wake", NodeLoop.Kind.OTHER, "g", quiet);
        DueWork.Registration wokenReg = loop.register("woke", NodeLoop.Kind.OTHER, "g", woken);
        loop.start();
        time.runDue();
        quiet.next.set(t0 + 200 * MS);  // moved earlier, no wake: a bug the backstop finds
        woken.next.set(t0 + 200 * MS);  // moved earlier with a wake (DW-2)
        wokenReg.wake();
        time.runDue();
        woken.during = () -> woken.next.set(DueWork.NEVER);
        time.advance(BACKSTOP);
        assertThat(loop.stats().backstopFoundWork()).as("named by participant")
                .containsEntry("forgot-to-wake", 1L).doesNotContainKey("woke");
        assertThat(ai.badmonkey.agentspaces.common.time.LoopTotals.backstopFoundWork() - processBefore)
                .as("a scripted loop's find stays out of the end-of-build total").isZero();
        assertThat(ai.badmonkey.agentspaces.common.time.LoopTotals.scriptedFinds() - scriptedBefore)
                .as("EL-Q-REL-03: the process counts it, apart, as a scripted find").isEqualTo(1);
    }

    @Test
    void aWakeAsksOnlyItsParticipantAndAGroupWakeOnlyItsGroup() {
        NodeLoop loop = loop(time);
        Probe a1 = new Probe();
        Probe a2 = new Probe();
        Probe b1 = new Probe();
        DueWork.Registration a1Reg = loop.register("a1", NodeLoop.Kind.PROTOCOL, "a", a1);
        loop.register("a2", NodeLoop.Kind.OTHER, "a", a2);
        loop.register("b1", NodeLoop.Kind.PROTOCOL, "b", b1);
        loop.start();
        time.runDue();
        a1Reg.wake();
        time.runDue();
        assertThat(List.of(a1.runs.get(), a2.runs.get(), b1.runs.get())).as("EL-L-15: one participant")
                .containsExactly(2, 1, 1);
        loop.wakeGroup("a");
        time.runDue();
        assertThat(List.of(a1.runs.get(), a2.runs.get(), b1.runs.get())).as("EL-L-15: one group")
                .containsExactly(3, 2, 1);
    }

    @Test
    void aParticipantRegisteredAfterStartRunsAtOnceAndAClosedOneNeverAgain() {
        NodeLoop loop = loop(time);
        loop.start();
        Probe late = new Probe();
        DueWork.Registration reg = loop.register("late", NodeLoop.Kind.OTHER, "g", late);
        time.runDue();
        assertThat(late.runs.get()).isEqualTo(1);
        reg.close();
        loop.wakeAll();
        time.advance(Duration.ofSeconds(5));
        assertThat(late.runs.get()).isEqualTo(1);
        assertThat(loop.participants()).isEmpty();
    }

    @Test
    void aDeadlineAtOrBeforeNowRunsAgainNoSoonerThanTheSpinGuard() {
        NodeLoop loop = loop(time);
        Probe eager = new Probe();
        eager.next.set(0); // "run me again at once", forever: a bug that must not spin the loop
        loop.register("eager", NodeLoop.Kind.OTHER, "g", eager);
        loop.start();
        time.advance(Duration.ofMillis(10));
        assertThat(eager.runs.get()).as("at most once per spin-guard interval, plus the first run")
                .isBetween(10, 11);
    }

    @Test
    void anOverrunIsCountedWhenTheTimerFiresWhileARunIsPending() {
        NodeLoop loop = loop(time);
        long t0 = time.monoNanos();
        Probe p = new Probe();
        p.next.set(t0 + 50 * MS);
        DueWork.Registration reg = loop.register("p", NodeLoop.Kind.OTHER, "g", p);
        // Scheduled before the loop arms its timer, so at 50 ms it runs first: a wake leaves a run pending ...
        time.schedule(Duration.ofMillis(50), () -> {
            p.next.set(DueWork.NEVER);
            reg.wake();
        });
        loop.start();
        time.runDue();
        time.advance(Duration.ofMillis(50)); // ... when the 50 ms deadline fires: counted, still one run (§3.10)
        NodeLoop.Stats stats = loop.stats();
        assertThat(stats.overruns()).isEqualTo(1);
        assertThat(p.runs.get()).isEqualTo(2);
        assertThat(stats.runs()).isEqualTo(2);
        assertThat(stats.timerWakes()).as("the absorbed fire is still the run's cause").isEqualTo(1);
    }
}
