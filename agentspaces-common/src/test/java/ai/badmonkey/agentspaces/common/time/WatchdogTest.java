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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The watchdog's checks (srad/agentspaces-event-loop.md §3.9, EL-H-01 to EL-H-08, WD-1 to WD-6) on its manual mode,
 * driven tick by tick against a hand-moved monotonic clock and a scripted {@link Watched}: each check finds its fault
 * at the bound and not before, acts once, and escalates on the decided rule. The threaded mode's own watch (WD-6) and
 * the timer's restart are in {@link SystemTimeTest} and the spoilers (EL-SP-22, EL-SP-23).
 */
@Timeout(60)
class WatchdogTest {

    private static final long MS = 1_000_000L;

    /** A monotonic clock the test moves; nothing is scheduled on it. */
    private static final class HandClock implements TimeSource {
        final AtomicLong now = new AtomicLong(1_000 * MS);

        @Override
        public Instant instant() {
            return Instant.EPOCH.plusNanos(now.get());
        }

        @Override
        public long monoNanos() {
            return now.get();
        }

        @Override
        public Scheduled schedule(Duration delay, Runnable task) {
            throw new UnsupportedOperationException("the watchdog never schedules (EL-H-01)");
        }

        @Override
        public Scheduled scheduleAt(long monoDeadlineNanos, Runnable task) {
            throw new UnsupportedOperationException("the watchdog never schedules (EL-H-01)");
        }

        @Override
        public void sleep(Duration duration) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean virtual() {
            return true;
        }

        void advance(long nanos) {
            now.addAndGet(nanos);
        }
    }

    /** A watched thing whose heartbeat the test sets, recording what the watchdog asks of it. */
    private static final class Fake implements Watched {
        volatile long runs;
        volatile long started;
        volatile String in = "";
        volatile long armed = Long.MAX_VALUE;
        volatile Thread thread;
        volatile int queued;
        volatile boolean dispatched;
        volatile long raised;
        volatile long fenced;
        volatile boolean endsAfterThreadRead;
        final List<Reason> reinits = new CopyOnWriteArrayList<>();
        final List<String> escalations = new CopyOnWriteArrayList<>();
        final CountDownLatch reinitialised = new CountDownLatch(1);

        @Override
        public String name() {
            return "node:fake";
        }

        @Override
        public long runs() {
            return runs;
        }

        @Override
        public long runStartedNanos() {
            return started;
        }

        @Override
        public String runningIn() {
            return in;
        }

        @Override
        public long armedNanos() {
            return armed;
        }

        @Override
        public Thread thread() {
            Thread t = thread;
            if (endsAfterThreadRead) { // the run ends on its own thread, which then ends, right after this read
                endsAfterThreadRead = false;
                runs++;
                started = 0;
                thread = null;
            }
            return t;
        }

        @Override
        public int queued() {
            return queued;
        }

        @Override
        public boolean dispatched() {
            return dispatched;
        }

        @Override
        public long wakeRaisedNanos() {
            return raised;
        }

        @Override
        public long fencedSinceNanos() {
            return fenced;
        }

        @Override
        public long generation() {
            return reinits.size();
        }

        @Override
        public void reinitialise(Reason reason) {
            reinits.add(reason);
            started = 0;
            thread = null;
            queued = 0;
            raised = 0;
            armed = Long.MAX_VALUE;
            reinitialised.countDown();
        }

        @Override
        public void escalate(String reason) {
            escalations.add(reason);
        }

        final List<Long> ticks = new CopyOnWriteArrayList<>();
        volatile boolean tickThrows;

        @Override
        public void watchdogTick(long nowNanos) {
            ticks.add(nowNanos);
            if (tickThrows) {
                throw new IllegalStateException("scripted");
            }
        }
    }

    private final HandClock clock = new HandClock();
    private final Watchdog watchdog = Watchdog.manual(clock, Watchdog.Options.defaults()).markScripted();
    private final Fake fake = new Fake();

    private void tickAfter(long nanos) {
        clock.advance(nanos);
        watchdog.tick();
    }

    @Test
    void aMissedFireIsFoundOnceTheArmedDeadlinePlusTheGraceHasPassedWithNoRun() {
        watchdog.register(fake);
        fake.armed = clock.monoNanos() + 1_000 * MS;
        tickAfter(1_200 * MS);
        assertThat(fake.reinits).as("within the grace: lateness, not a fault").isEmpty();
        tickAfter(100 * MS);
        assertThat(fake.reinits).as("armed + 250 ms passed, no run since the last tick").containsExactly(
                Watched.Reason.MISSED_FIRE);
        assertThat(watchdog.stats().detectionsByKind()).containsEntry("missed-fire", 1L);
        assertThat(watchdog.detections()).singleElement().satisfies(d -> {
            assertThat(d.name()).isEqualTo("node:fake");
            assertThat(d.atNanos()).isEqualTo(clock.monoNanos());
        });
    }

    @Test
    void aRunSinceTheLastTickOrADispatchedRunIsNotAMissedFire() {
        watchdog.register(fake);
        fake.armed = clock.monoNanos() + 100 * MS;
        fake.runs = 1; // it ran, and has not re-armed yet
        tickAfter(2_000 * MS);
        assertThat(fake.reinits).isEmpty();
        fake.dispatched = true; // the fire waits for a carrier: lateness (§3.9)
        tickAfter(2_000 * MS);
        assertThat(fake.reinits).isEmpty();
    }

    @Test
    void aDispatchThatNeverStartsIsFoundAfterTheWedgeBound() {
        watchdog.register(fake);
        fake.dispatched = true;
        fake.queued = 1;
        for (int i = 0; i < 3; i++) {
            tickAfter(2_000 * MS);
        }
        assertThat(fake.reinits).as("first seen, then 4 s: inside the bound").isEmpty();
        tickAfter(2_000 * MS);
        assertThat(fake.reinits).as("6 s waiting on a drain that never starts, past the 5 s bound")
                .containsExactly(Watched.Reason.STUCK_DISPATCH);
    }

    @Test
    void aWakeRaisedAndNeverDispatchedIsFoundWithinThePeriodPlusTheGrace() {
        watchdog.register(fake);
        fake.queued = 1;
        fake.raised = clock.monoNanos();
        tickAfter(200 * MS);
        assertThat(fake.reinits).as("inside the grace").isEmpty();
        tickAfter(100 * MS);
        assertThat(fake.reinits).containsExactly(Watched.Reason.MISSED_WAKE);
    }

    @Test
    void aSlowRunIsCountedOnceAndAWedgedRunIsReinitialisedOnceWithItsParticipantNamed() {
        watchdog.register(fake);
        fake.thread = Thread.currentThread();
        fake.started = clock.monoNanos();
        fake.in = "g/slowpoke";
        tickAfter(150 * MS);
        tickAfter(150 * MS);
        assertThat(watchdog.stats().slowRuns()).as("counted once per run").isEqualTo(1);
        assertThat(watchdog.stats().slowRunsBy()).containsEntry("g/slowpoke", 1L);
        assertThat(fake.reinits).isEmpty();
        long started = fake.started;
        tickAfter(5_000 * MS);
        assertThat(fake.reinits).containsExactly(Watched.Reason.WEDGED);
        assertThat(watchdog.detections()).singleElement().satisfies(d -> assertThat(d.participant())
                .isEqualTo("g/slowpoke"));
        fake.started = started; // the same run, still marked (a fake that does not clear it)
        fake.thread = Thread.currentThread();
        tickAfter(2_000 * MS);
        assertThat(fake.reinits).as("one re-initialisation per wedged run").hasSize(1);
    }

    @Test
    void aDeadThreadWithARunMarkedStartedIsReinitialised() throws Exception {
        watchdog.register(fake);
        Thread dead = Thread.ofVirtual().start(() -> { });
        dead.join();
        fake.thread = dead;
        fake.started = clock.monoNanos();
        tickAfter(1 * MS);
        assertThat(fake.reinits).containsExactly(Watched.Reason.DEAD);
    }

    /**
     * The race the ten-member EL-SP-31 run hit twice (srad/agentspaces-event-loop.md §10.13): a run on a virtual thread
     * of its own ends, and its thread with it, after the check has read the run's start and thread; the loop is armed
     * for its next deadline. That run finished; it is not dead.
     */
    @Test
    void aRunThatEndsWithItsThreadBetweenTheReadsIsNotDead() throws Exception {
        watchdog.register(fake);
        Thread ended = Thread.ofVirtual().start(() -> { });
        ended.join();
        fake.thread = ended;
        fake.started = clock.monoNanos();
        fake.armed = clock.monoNanos() + 30_000 * MS;
        fake.endsAfterThreadRead = true;
        tickAfter(1 * MS);
        assertThat(fake.reinits).as("a finished run is not a dead thread").isEmpty();
        assertThat(watchdog.stats().detections()).isZero();
    }

    @Test
    void threeReinitialisationsWithinTenMinutesEscalateOnceAndTwoDoNot() {
        watchdog.register(fake);
        for (int i = 0; i < 2; i++) {
            fake.queued = 1;
            fake.raised = clock.monoNanos();
            tickAfter(4 * 60_000 * MS);
        }
        assertThat(fake.reinits).hasSize(2);
        assertThat(fake.escalations).as("two in 10 minutes").isEmpty();
        fake.queued = 1;
        fake.raised = clock.monoNanos();
        tickAfter(1_000 * MS);
        assertThat(fake.escalations).as("the third within 10 minutes (WD-5)").hasSize(1);
        fake.queued = 1;
        fake.raised = clock.monoNanos();
        tickAfter(1_000 * MS);
        assertThat(fake.escalations).as("escalated once").hasSize(1);
        assertThat(watchdog.stats().escalations()).isEqualTo(1);
    }

    @Test
    void reinitialisationsSpreadOverMoreThanTheWindowDoNotEscalate() {
        watchdog.register(fake);
        for (int i = 0; i < 4; i++) {
            fake.queued = 1;
            fake.raised = clock.monoNanos();
            tickAfter(6 * 60_000 * MS);
        }
        assertThat(fake.reinits).hasSize(4);
        assertThat(fake.escalations).isEmpty();
    }

    @Test
    void aFencedParticipantThatDoesNotReturnWithinTwoMinutesEscalates() {
        watchdog.register(fake);
        fake.fenced = clock.monoNanos();
        tickAfter(119_000 * MS);
        assertThat(fake.escalations).isEmpty();
        tickAfter(2_000 * MS);
        assertThat(fake.escalations).singleElement().asString().contains("fenced");
    }

    @Test
    void manualModeTicksOnlyWhileSomethingIsRegisteredAndReArmsFromTheEndOfACheck() {
        assertThat(watchdog.nextTickNanos()).isEqualTo(Long.MAX_VALUE);
        Watchdog.Registration r = watchdog.register(fake);
        assertThat(watchdog.nextTickNanos()).isEqualTo(clock.monoNanos() + 2_000 * MS);
        tickAfter(2_100 * MS);
        assertThat(watchdog.nextTickNanos()).as("from the end of the check (EL-T-13)")
                .isEqualTo(clock.monoNanos() + 2_000 * MS);
        r.close();
        assertThat(watchdog.nextTickNanos()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void scriptedFindingsStayOutOfTheProcessTotals() {
        long before = Watchdog.totalDetections();
        watchdog.register(fake);
        fake.queued = 1;
        fake.raised = clock.monoNanos();
        tickAfter(1_000 * MS);
        assertThat(fake.reinits).hasSize(1);
        assertThat(Watchdog.totalDetections()).isEqualTo(before);
    }

    @Test
    void theNoneWatchdogJudgesNothing() {
        Watchdog.Registration r = Watchdog.none().register(fake);
        Watchdog.none().tick();
        r.close();
        assertThat(fake.reinits).isEmpty();
    }

    @Test
    void aThreadedWatchdogStartsWithTheFirstRegistrationAndStopsWithTheLast() throws Exception {
        SystemTime time = SystemTime.isolated("wd-test-timer");
        Watchdog threaded = Watchdog.threaded("wd-test-watchdog", time, Watchdog.Options.defaults()).markScripted();
        assertThat(threaded.thread()).as("nothing registered, no thread").isNull();
        assertThat(time.watchdog()).isSameAs(threaded);
        Fake idle = new Fake();
        Watchdog.Registration r = threaded.register(idle);
        Thread t = threaded.thread();
        assertThat(t).isNotNull();
        assertThat(t.getName()).isEqualTo("wd-test-watchdog");
        assertThat(t.isDaemon()).isTrue();
        assertThat(t.isVirtual()).as("a platform thread on the allowlist (§3.8)").isFalse();
        r.close();
        t.join(Duration.ofSeconds(10));
        assertThat(t.isAlive()).as("stopped with the last registration").isFalse();
        assertThat(threaded.thread()).isNull();
    }

    /**
     * The adversarial review of phase 1 (srad/agentspaces-event-loop.md §10.10, finding W2), a GC-pause model
     * (EL-SP-05's shape) on the threaded watchdog: the hook on the watchdog's own thread stands in for a pause of the
     * whole process, during which no loop could run either. A run under way when the pause began, and a timer that came
     * due during it, were judged wedged and a missed fire on the first check after it, so a healthy node was
     * re-initialised (its participant's thread interrupted) and the detections counted toward the escalation, after
     * every long collection or debugger stop. Now a check that begins past the grace after its park ended judges ages
     * from its own start, and a run that stays stuck is still found one bound after the pause.
     */
    @Test
    void aPauseOfTheProcessIsNotHeldAgainstALoopAndARunStillStuckIsFoundOneBoundAfterIt() throws Exception {
        SystemTime time = SystemTime.isolated("wd-pause-timer");
        Watchdog.Options o = new Watchdog.Options(Duration.ofMillis(100), Duration.ofMillis(50), Duration.ofMillis(20),
                Duration.ofMillis(300), 3, Duration.ofMinutes(10), Duration.ofMinutes(2));
        Watchdog threaded = Watchdog.threaded("wd-pause-watchdog", time, o).markScripted();
        Fake running = new Fake(); // a healthy run under way when the pause began; it ends just after
        Fake armed = new Fake(); // an idle loop whose timer came due inside the pause; it fires just after
        Fake stuck = new Fake(); // a run that never ends: found one wedge bound after the pause
        Thread alive = Thread.currentThread();
        AtomicInteger checks = new AtomicInteger();
        AtomicLong resumedAt = new AtomicLong();
        CountDownLatch settled = new CountDownLatch(1);
        threaded.beforeCheck(() -> {
            int n = checks.incrementAndGet();
            if (n == 2) {
                long start = time.monoNanos();
                for (Fake f : List.of(running, stuck)) {
                    f.thread = alive;
                    f.in = "g/participant";
                    f.started = start;
                }
                armed.armed = start + 50 * MS;
                long until = start + 600 * MS; // the pause: nothing in the process runs for 600 ms
                while (until - time.monoNanos() > 0) {
                    LockSupport.parkNanos(until - time.monoNanos());
                }
                resumedAt.set(time.monoNanos());
            } else if (n == 3) {
                running.started = 0; // the healthy run ended on resume
                running.thread = null;
                armed.runs = armed.runs + 1; // the timer fired on resume and the loop ran
                armed.armed = Long.MAX_VALUE;
            } else if (n == 10) {
                settled.countDown();
            }
        });
        List<Watchdog.Registration> registrations = List.of(threaded.register(running), threaded.register(armed),
                threaded.register(stuck));
        try {
            stuck.reinitialised.await();
            settled.await();
        } finally {
            registrations.forEach(Watchdog.Registration::close);
        }
        assertThat(running.reinits).as("a run under way across a pause is not wedged").isEmpty();
        assertThat(armed.reinits).as("a timer due inside a pause is not a missed fire").isEmpty();
        assertThat(stuck.reinits).as("a run still stuck after the pause is found").containsExactly(
                Watched.Reason.WEDGED);
        assertThat(threaded.detections()).singleElement().satisfies(d -> assertThat(d.atNanos() - resumedAt.get())
                .as("judged from the resume, not the pause's start").isGreaterThanOrEqualTo(300 * MS));
        assertThat(threaded.stats().lateTicks()).isPositive();
    }

    /**
     * The guard on §10.10's pause rule: a host so loaded that every check begins past the grace is load, not a
     * pause, so the judged start does not move with each check, and a run that never ends is still found.
     */
    @Test
    void lateChecksInARowAreLoadNotAPauseAndAStuckRunIsStillFound() throws Exception {
        SystemTime time = SystemTime.isolated("wd-load-timer");
        Watchdog.Options o = new Watchdog.Options(Duration.ofMillis(100), Duration.ofMillis(50), Duration.ofMillis(20),
                Duration.ofMillis(300), 3, Duration.ofMinutes(10), Duration.ofMinutes(2));
        Watchdog threaded = Watchdog.threaded("wd-load-watchdog", time, o).markScripted();
        Fake stuck = new Fake();
        Thread alive = Thread.currentThread();
        AtomicInteger checks = new AtomicInteger();
        threaded.beforeCheck(() -> {
            int n = checks.incrementAndGet();
            if (n == 2) {
                stuck.thread = alive;
                stuck.in = "g/stuck";
                stuck.started = time.monoNanos();
            }
            if (n >= 2 && stuck.reinits.isEmpty()) {
                long until = time.monoNanos() + 80 * MS; // every check from here begins 80 ms late (grace 50 ms)
                while (until - time.monoNanos() > 0) {
                    LockSupport.parkNanos(until - time.monoNanos());
                }
            }
        });
        Watchdog.Registration r = threaded.register(stuck);
        try {
            stuck.reinitialised.await();
        } finally {
            r.close();
        }
        assertThat(stuck.reinits).containsExactly(Watched.Reason.WEDGED);
        assertThat(threaded.stats().lateTicks()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void theTimerRestartKeepsEveryPendingDeadline() throws Exception {
        SystemTime time = SystemTime.isolated("wd-restart-timer");
        CountDownLatch ran = new CountDownLatch(3);
        for (int i = 1; i <= 3; i++) {
            time.schedule(Duration.ofMillis(50L * i), ran::countDown);
        }
        Thread first = time.timerThread();
        time.restartTimer();
        ran.await();
        assertThat(time.timerThread()).isNotSameAs(first);
        first.join(Duration.ofSeconds(10));
        assertThat(first.isAlive()).as("the replaced thread stops").isFalse();
        assertThat(time.timerRestarts()).isEqualTo(1);
    }

    /** A Raft leader as the watchdog sees it (EL-H-09): every value lock-free, the test moves them. */
    private static final class FakeLeader implements Watchdog.LeaseHolder {
        volatile boolean leading = true;
        volatile long issued;
        volatile long span = 1_500 * MS;
        volatile long heartbeatDue;
        final java.util.concurrent.atomic.AtomicInteger refreshes = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.CountDownLatch steppedDown = new java.util.concurrent.CountDownLatch(1);

        @Override
        public String name() {
            return "raft:fake";
        }

        @Override
        public boolean leading() {
            return leading;
        }

        @Override
        public long leaseIssuedNanos() {
            return issued;
        }

        @Override
        public long leaseSpanNanos() {
            return span;
        }

        @Override
        public long heartbeatDueNanos() {
            return heartbeatDue;
        }

        @Override
        public long minElectionTimeoutNanos() {
            return 1_250 * MS;
        }

        @Override
        public void forceRefresh() {
            refreshes.incrementAndGet();
        }

        @Override
        public void stepDown() {
            leading = false;
            steppedDown.countDown();
        }
    }

    /**
     * EL-H-09: a healthy leader is left alone; one whose lease card is past half its span plus the grace, or whose
     * heartbeat is unarmed or late past the grace, is republished; one whose heartbeat is a minimum election timeout
     * overdue is stepped down, off the watchdog's thread; a follower is never judged; a closed registration stops it.
     */
    @Test
    void theLeaseDutyRefreshesAStaleLeaseAndStepsDownALeaderWhoseHeartbeatIsAnElectionTimeoutOverdue()
            throws Exception {
        FakeLeader leader = new FakeLeader();
        Watchdog.Registration registration = watchdog.registerLease(leader);
        leader.issued = clock.monoNanos();
        leader.heartbeatDue = clock.monoNanos() + 250 * MS;
        tickAfter(100 * MS);
        assertThat(leader.refreshes.get()).as("healthy: nothing forced").isZero();
        assertThat(watchdog.leaseForced()).isZero();

        tickAfter(800 * MS); // the card is 900 ms old (within half of 1.5 s plus the grace); the heartbeat is 650 ms late
        assertThat(leader.refreshes.get()).as("a heartbeat late past the 250 ms grace forces a refresh")
                .isEqualTo(1);
        leader.issued = clock.monoNanos();
        leader.heartbeatDue = clock.monoNanos() + 250 * MS;
        tickAfter(200 * MS);
        assertThat(leader.refreshes.get()).as("fresh again").isEqualTo(1);

        leader.heartbeatDue = Long.MAX_VALUE; // the heartbeat deadline was lost
        tickAfter(10 * MS);
        assertThat(leader.refreshes.get()).as("an unarmed heartbeat forces a refresh").isEqualTo(2);
        assertThat(watchdog.leaseForced()).isEqualTo(2);

        leader.heartbeatDue = clock.monoNanos();
        tickAfter(1_250 * MS);
        assertThat(leader.steppedDown.await(10, java.util.concurrent.TimeUnit.SECONDS)).as("stepped down").isTrue();
        assertThat(watchdog.stepdownsForced()).isEqualTo(1);
        assertThat(watchdog.detections()).extracting(Watchdog.Detection::kind).contains("raft-lease", "raft-stepdown");

        tickAfter(5_000 * MS);
        assertThat(watchdog.stepdownsForced()).as("a follower is not judged").isEqualTo(1);
        leader.leading = true;
        registration.close();
        tickAfter(5_000 * MS);
        assertThat(watchdog.stepdownsForced()).as("closed: no longer judged").isEqualTo(1);
    }
    /**
     * §10.16, owner decision 2 (2026-11-22): an operation that holds a leased entry (a bound handler, a connector's
     * operation) is reported by name once it has held past the report bound, and again each time its hold doubles;
     * the report is a slow operation, not a detection of this node's loop.
     */
    @Test
    void aHeldOperationIsReportedByNameAtTheBoundAndEachTimeItsHoldDoubles() {
        Watchdog.Registration registration = watchdog.registerHeld(
                new Watchdog.Held("agent:clerk", "settle", "entry-7", clock.monoNanos()));
        assertThat(watchdog.heldReport()).isEqualTo(Watchdog.DEFAULT_HELD_REPORT);
        tickAfter(59_000 * MS);
        assertThat(watchdog.heldReports()).isZero();
        tickAfter(2_000 * MS); // held 61 s
        assertThat(watchdog.heldReports()).isEqualTo(1);
        tickAfter(60_000 * MS); // 121 s: not yet twice the 61 s it was reported at
        assertThat(watchdog.heldReports()).isEqualTo(1);
        tickAfter(2_000 * MS); // 123 s
        assertThat(watchdog.heldReports()).isEqualTo(2);
        assertThat(watchdog.stats().slowOperations()).isEqualTo(2);
        assertThat(watchdog.detections()).as("a slow operation is not a loop fault").isEmpty();
        registration.close();
        tickAfter(600_000 * MS);
        assertThat(watchdog.heldReports()).as("returned: no longer judged").isEqualTo(2);
        assertThat(Watchdog.none().registerHeld(new Watchdog.Held("a", "m", "e", 0))).isNotNull();
    }

    @Test
    void everyCheckTellsEachWatchedThingItsTickAfterJudgingItAndAThrowingTickHarmsNoOther() {
        Fake other = new Fake();
        watchdog.register(fake);
        watchdog.register(other);
        tickAfter(2_000 * MS);
        assertThat(fake.ticks).containsExactly(clock.monoNanos());
        assertThat(other.ticks).containsExactly(clock.monoNanos());
        fake.tickThrows = true;
        tickAfter(2_000 * MS);
        assertThat(fake.ticks).hasSize(2);
        assertThat(other.ticks).as("the pool's grow and a loop's stretch go on").hasSize(2);
    }
}
