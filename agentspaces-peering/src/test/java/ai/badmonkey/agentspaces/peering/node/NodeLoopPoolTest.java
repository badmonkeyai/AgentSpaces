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
import ai.badmonkey.agentspaces.common.time.LoopPool;
import ai.badmonkey.agentspaces.common.time.SystemTime;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 4 of srad/agentspaces-event-loop.md on one loop: runs and timer fires on the loop pool (decision 2, EL-O-02),
 * the per-participant wake counters (the trace of §10.19's open item), the stretch rule (EL-O-04) and refresh before
 * slowdown (EL-O-05). The stretch tests run on {@link VirtualTime} with the stretch asked directly; the rule is driven
 * on its own clock. One test runs on an isolated system source because it is about the pool's threads; it asserts no
 * timing.
 */
@Timeout(120)
class NodeLoopPoolTest {

    private static final long MS = 1_000_000L;

    private final VirtualTime time = VirtualTime.create();
    private final List<NodeLoop> loops = new ArrayList<>();

    @AfterEach
    void tearDown() {
        loops.forEach(NodeLoop::close);
    }

    private NodeLoop loop(ai.badmonkey.agentspaces.common.time.TimeSource source) {
        NodeLoop loop = new NodeLoop("node:pool-test", source, NodeLoop.DEFAULT_BACKSTOP);
        loops.add(loop);
        return loop;
    }

    // EL-O-02 --------------------------------------------------------------------------------------------------

    @Test
    void onASystemSourceEveryRunAndTimerFireIsOnTheLoopPoolNeverOnACarrier() throws Exception {
        SystemTime system = SystemTime.isolated("pooltest-timer");
        NodeLoop loop = loop(system);
        List<Thread> ranOn = new CopyOnWriteArrayList<>();
        CountDownLatch timed = new CountDownLatch(3);
        loop.register("p", NodeLoop.Kind.PROTOCOL, "g", now -> {
            ranOn.add(Thread.currentThread());
            timed.countDown();
            return now + 20 * MS; // timer-started runs from the second on
        });
        loop.start();
        assertThat(timed.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(ranOn).allSatisfy(t -> {
            assertThat(t.isVirtual()).as("decision 2: loop work is off the application's carriers").isFalse();
            assertThat(t.getName()).startsWith("pooltest-timer-loop-");
        });
        assertThat(loop.stats().timerWakes()).as("the armed timer fired on the pool").isGreaterThanOrEqualTo(2);
        LoopPool pool = system.loopPool();
        assertThat(pool.stats().users()).as("an open node keeps the pool's min").isEqualTo(1);
        loop.close();
        assertThat(pool.stats().users()).isZero();
    }

    @Test
    void onAVirtualSourceRunsAreTasksOfThatSourceSoAnAdvanceSeesThemFinish() {
        NodeLoop loop = loop(time);
        AtomicReference<Thread> on = new AtomicReference<>();
        loop.register("p", NodeLoop.Kind.OTHER, "g", now -> {
            on.set(Thread.currentThread());
            return DueWork.NEVER;
        });
        loop.start();
        time.runDue();
        assertThat(on.get()).isNotNull();
        assertThat(on.get().isVirtual()).isTrue();
    }

    // The per-participant wake counters ----------------------------------------------------------------------

    @Test
    void theLoopCountsPerParticipantRunsByCauseAndTheWakesEachRaiser() {
        NodeLoop loop = loop(time);
        long[] own = {0};
        loop.register("timed", NodeLoop.Kind.PROTOCOL, "g", now -> {
            if (own[0] == 0 || now - own[0] >= 0) {
                own[0] = now + 3_000 * MS; // DW-1: a sweep before its deadline returns the deadline unchanged
            }
            return own[0];
        });
        DueWork.Registration asked = loop.register("asked", NodeLoop.Kind.OTHER, "g", now -> DueWork.NEVER);
        loop.register("other", NodeLoop.Kind.OTHER, "h", now -> DueWork.NEVER);
        loop.start();
        time.runDue();
        time.advance(Duration.ofSeconds(9)); // three timed deadlines; the backstop sweeps the rest
        asked.wake();
        asked.wake();
        time.runDue();
        loop.wakeGroup("h");
        time.runDue();
        loop.wakeAll();
        time.runDue();
        NodeLoop.Stats s = loop.stats();
        assertThat(s.dueRuns()).as("a timer wake's cause").containsEntry("timed", 3L);
        assertThat(s.askedRuns()).as("an event wake's cause, once per run however many wakes; start asks everyone")
                .containsEntry("asked", 3L).containsEntry("other", 3L).containsEntry("timed", 2L);
        assertThat(s.wakeCalls()).containsEntry("asked", 2L).containsEntry("group:h", 1L).containsEntry("all", 1L);
        assertThat(s.dueRuns()).as("a backstop sweep is nobody's cause").doesNotContainKey("other");
    }

    // EL-O-04: the stretch rule ------------------------------------------------------------------------------

    @Test
    void theStretchRuleDoublesAfterThreeTicksOverTheBoundAtThePoolsMaxAndHalvesAfterThirtySecondsInBand() {
        LoopPool.Options o = LoopPool.Options.defaults(10);
        NodeLoop.StretchRule rule = new NodeLoop.StretchRule();
        long now = 0;
        long tick = 2_000 * MS;
        long over = 150 * MS;
        int f = 1;
        for (int i = 0; i < 5; i++) {
            f = rule.judge(now += tick, over, false, o, f);
        }
        assertThat(f).as("over the bound but the pool can still grow: no stretch").isEqualTo(1);
        f = rule.judge(now += tick, over, true, o, f);
        f = rule.judge(now += tick, over, true, o, f);
        assertThat(f).isEqualTo(1);
        f = rule.judge(now += tick, over, true, o, f);
        assertThat(f).as("three ticks over the bound at the max").isEqualTo(2);
        for (int i = 0; i < 3; i++) {
            f = rule.judge(now += tick, over, true, o, f);
        }
        assertThat(f).as("three more: x4").isEqualTo(4);
        for (int i = 0; i < 6; i++) {
            f = rule.judge(now += tick, over, true, o, f);
        }
        assertThat(f).as("the cap").isEqualTo(4);
        long inBand = 10 * MS;
        long cleared = now;
        while (f == 4) {
            f = rule.judge(now += tick, inBand, true, o, f);
        }
        assertThat(now - cleared).as("halved after 30 s in band").isBetween(30_000 * MS, 32_000 * MS);
        assertThat(f).isEqualTo(2);
        f = rule.judge(now += tick, inBand, true, o, f);
        assertThat(f).as("then once a tick: cleared within 30 s plus a period (EL-SP-27)").isEqualTo(1);
        assertThat(now - cleared).isLessThanOrEqualTo(30_000 * MS + 2 * tick);
        f = rule.judge(now += tick, over, true, o, f);
        f = rule.judge(now += tick, inBand, true, o, f);
        f = rule.judge(now += tick, over, true, o, f);
        f = rule.judge(now += tick, over, true, o, f);
        assertThat(f).as("the three ticks over are consecutive").isEqualTo(1);
    }

    // EL-O-04, EL-O-05: the stretch on a loop ----------------------------------------------------------------

    @Test
    void aStretchRefreshesEveryWireVisibleParticipantAndFlushesBeforeAnyPeriodStretches() {
        NodeLoop loop = loop(time);
        List<String> events = new CopyOnWriteArrayList<>();
        List<Long> localRuns = new CopyOnWriteArrayList<>();
        List<Long> leaseRuns = new CopyOnWriteArrayList<>();
        List<Long> fixedRuns = new CopyOnWriteArrayList<>();
        List<Long> cappedRuns = new CopyOnWriteArrayList<>();
        long[] refreshedWith = {0};
        loop.register("local", NodeLoop.Kind.OTHER, "g", now -> {
            localRuns.add(now);
            return now + 1_000 * MS;
        }, NodeLoop.Timing.local(Duration.ZERO));
        loop.register("lease", NodeLoop.Kind.OTHER, "g", now -> {
            leaseRuns.add(now);
            events.add("lease-run");
            return now + 1_000 * MS;
        }, NodeLoop.Timing.wireVisible(Duration.ZERO, period -> {
            refreshedWith[0] = period;
            events.add("refresh");
        }));
        loop.register("fixed", NodeLoop.Kind.PROTOCOL, "g", now -> {
            fixedRuns.add(now);
            return now + 1_000 * MS;
        });
        loop.register("capped", NodeLoop.Kind.OTHER, "g", now -> {
            cappedRuns.add(now);
            return now + 1_000 * MS;
        }, NodeLoop.Timing.local(Duration.ofMillis(1_500)));
        loop.afterRefresh(() -> events.add("flush"));
        loop.start();
        time.runDue();
        time.advance(Duration.ofSeconds(5));
        events.clear();
        int localBefore = localRuns.size();
        int fixedBefore = fixedRuns.size();
        int cappedBefore = cappedRuns.size();
        loop.requestStretch(2);
        time.runDue();
        assertThat(events).as("EL-O-05: the refresh, then the flush, before the stretch applies")
                .startsWith("refresh", "flush");
        assertThat(refreshedWith[0]).as("the refresh is given the stretched period").isEqualTo(2_000 * MS);
        assertThat(loop.stretch()).isEqualTo(2);
        time.advance(Duration.ofSeconds(20));
        assertThat(localRuns.size() - localBefore).as("a local period of 1 s runs every 2 s").isBetween(10, 11);
        assertThat(fixedRuns.size() - fixedBefore).as("a fixed one never stretches (EL-O-08)").isBetween(20, 21);
        assertThat(cappedRuns.size() - cappedBefore).as("a capped one stretches only to its cap, 1.5 s")
                .isBetween(13, 14);
        NodeLoop.Stats s = loop.stats();
        assertThat(s.stretches()).isEqualTo(1);
        assertThat(s.refreshes()).isEqualTo(1);
        assertThat(s.stretchUnrefreshed()).as("core.loop.stretch-unrefreshed").isZero();
        assertThat(s.wireVisible()).containsExactly("lease");
        assertThat(s.stretchable()).containsExactlyInAnyOrder("local", "lease", "capped");
        assertThat(loop.health().state()).as("a stretch in force reads DEGRADED (EL-H-14)")
                .isEqualTo(NodeLoop.HealthState.DEGRADED);

        loop.requestStretch(4);
        time.runDue();
        assertThat(events.stream().filter("refresh"::equals).count()).as("a second step refreshes again").isEqualTo(2);
        assertThat(refreshedWith[0]).isEqualTo(4_000 * MS);

        int restoreFrom = localRuns.size();
        loop.requestStretch(1);
        time.runDue();
        assertThat(loop.stretch()).isEqualTo(1);
        assertThat(localRuns.size()).as("restore asks every stretching participant at once").isGreaterThan(restoreFrom);
        int after = localRuns.size();
        time.advance(Duration.ofSeconds(10));
        assertThat(localRuns.size() - after).as("back to 1 s").isBetween(10, 11);
        assertThat(loop.health().state()).isEqualTo(NodeLoop.HealthState.OK);
        assertThat(loop.stats().restores()).isEqualTo(1);
    }

    /**
     * WD-4, EL-H-06, EL-O-05: a re-initialisation that found the loop stopped past a wire-visible participant's
     * deadline runs that participant's refresh, and flushes, before any participant of the new generation runs; a
     * wire-visible participant whose deadline had not passed is not refreshed. The loop is stopped by a lost nudge
     * (EL-SP-20's fault) and re-initialised as the watchdog would.
     */
    @Test
    void aReinitialisationRefreshesTheOverdueWireVisibleParticipantsBeforeTheFirstRun() {
        NodeLoop loop = loop(time.unwatchedNode()); // the test re-initialises, as the watchdog would
        List<String> events = new CopyOnWriteArrayList<>();
        long[] refreshedWith = {0};
        loop.register("self-ad", NodeLoop.Kind.PROTOCOL, "g", now -> {
            events.add("self-ad-run");
            return now + 30_000 * MS;
        });
        loop.register("lease", NodeLoop.Kind.OTHER, "g", now -> {
            events.add("lease-run");
            return now + 1_000 * MS;
        }, NodeLoop.Timing.wireVisible(Duration.ZERO, period -> {
            refreshedWith[0] = period;
            events.add("lease-refresh");
        }));
        loop.register("card", NodeLoop.Kind.OTHER, "g", now -> {
            events.add("card-run");
            return now + 600_000 * MS;
        }, NodeLoop.Timing.wireVisible(Duration.ZERO, period -> events.add("card-refresh")));
        loop.afterRefresh(() -> events.add("flush"));
        loop.start();
        time.runDue();
        time.advance(Duration.ofSeconds(3));
        long runs = loop.heartbeatRuns();

        loop.dropNextDispatch(); // the next nudge is lost: the loop runs nothing from here
        loop.wakeAll();
        time.advance(Duration.ofSeconds(5));
        assertThat(loop.heartbeatRuns()).as("the loop is stopped").isEqualTo(runs);
        events.clear();

        loop.reinitialise(ai.badmonkey.agentspaces.common.time.Watched.Reason.MISSED_WAKE);
        time.runDue();
        System.out.printf("[wd-4] after the re-initialisation: %s%n", events);
        assertThat(events).as("WD-4: the overdue lease's refresh and the flush come before any run")
                .startsWith("lease-refresh", "flush");
        assertThat(events).as("the card's deadline had not passed: no refresh").doesNotContain("card-refresh");
        assertThat(events).contains("self-ad-run", "lease-run", "card-run");
        assertThat(refreshedWith[0]).as("the refresh is given the period in force").isEqualTo(1_000 * MS);
        assertThat(loop.stats().refreshes()).isEqualTo(1);
    }

    @Test
    void aWireVisibleParticipantWhoseRefreshThrowsKeepsItsUnstretchedPeriod() {
        NodeLoop loop = loop(time);
        List<Long> runs = new CopyOnWriteArrayList<>();
        loop.register("lease", NodeLoop.Kind.OTHER, "g", now -> {
            runs.add(now);
            return now + 1_000 * MS;
        }, NodeLoop.Timing.wireVisible(Duration.ZERO, period -> {
            throw new IllegalStateException("scripted: the renewal failed");
        }));
        loop.start();
        time.runDue();
        time.advance(Duration.ofSeconds(3));
        loop.requestStretch(2);
        time.runDue();
        int before = runs.size();
        time.advance(Duration.ofSeconds(10));
        assertThat(runs.size() - before).as("never stretched without its refresh (EL-O-05)").isBetween(10, 11);
        assertThat(loop.stats().refreshFailures()).isEqualTo(1);
        assertThat(loop.stats().refreshes()).isZero();
    }

    /**
     * Seam r03 (srad/agentspaces-event-loop.md §10.33, §10.37): a wire-visible participant stretches only as far as a
     * refresh of its own covered, and one whose refresh at the stretch in force did not run keeps its unstretched
     * period, uncounted. Two cases: its refresh ran at x2 and threw at x4; and it joined while x2 was in force, so no
     * refresh ran for it. Without the guard the first held the x2 period and both were counted as stretches that went
     * unrefreshed ({@code core.loop.stretch-unrefreshed}), which reads a refresh failure as an unrefreshed stretch.
     */
    @Test
    void aWireVisibleParticipantNotRefreshedAtTheStretchInForceKeepsItsUnstretchedPeriodUncounted() {
        NodeLoop loop = loop(time);
        List<Long> runs = new CopyOnWriteArrayList<>();
        AtomicBoolean failRefresh = new AtomicBoolean();
        loop.register("lease", NodeLoop.Kind.OTHER, "g", now -> {
            runs.add(now);
            return now + 1_000 * MS;
        }, NodeLoop.Timing.wireVisible(Duration.ZERO, period -> {
            if (failRefresh.get()) {
                throw new IllegalStateException("scripted: the renewal failed");
            }
        }));
        loop.start();
        time.runDue();
        time.advance(Duration.ofSeconds(3));
        loop.requestStretch(2);
        time.runDue();
        int before = runs.size();
        time.advance(Duration.ofSeconds(10));
        assertThat(runs.size() - before).as("refreshed at x2: every 2 s").isBetween(5, 6);

        failRefresh.set(true);
        loop.requestStretch(4);
        time.runDue();
        assertThat(loop.stats().refreshFailures()).isEqualTo(1);
        before = runs.size();
        time.advance(Duration.ofSeconds(10));
        assertThat(runs.size() - before).as("its x4 refresh threw: back to its unstretched 1 s (EL-O-05)")
                .isBetween(10, 11);

        List<Long> late = new CopyOnWriteArrayList<>();
        loop.register("late", NodeLoop.Kind.OTHER, "g", now -> {
            late.add(now);
            return now + 1_000 * MS;
        }, NodeLoop.Timing.wireVisible(Duration.ZERO, period -> { }));
        time.runDue();
        int lateBefore = late.size();
        time.advance(Duration.ofSeconds(10));
        assertThat(late.size() - lateBefore).as("joined during the stretch: no refresh ran for it, so 1 s")
                .isBetween(10, 11);
        assertThat(loop.stats().stretchUnrefreshed()).as("neither is counted as a stretch that went unrefreshed")
                .isZero();
    }

    @Test
    void anEarlyRunOfAStretchedParticipantDoesNotPushItsDeadlineOutAgain() {
        NodeLoop loop = loop(time);
        List<Long> runs = new CopyOnWriteArrayList<>();
        long[] own = {0};
        DueWork.Registration reg = loop.register("local", NodeLoop.Kind.OTHER, "g", now -> {
            if (own[0] == 0 || now - own[0] >= 0) {
                runs.add(now);
                own[0] = now + 1_000 * MS; // its own deadline; an early call returns it unchanged (DW-1)
            }
            return own[0];
        }, NodeLoop.Timing.local(Duration.ZERO));
        loop.start();
        time.runDue();
        loop.requestStretch(4);
        time.runDue();
        int first = runs.size();
        time.advance(Duration.ofMillis(1_000)); // its own deadline: it runs, and its next is stretched to 4 s
        assertThat(runs.size()).isEqualTo(first + 1);
        long ranAt = runs.get(runs.size() - 1);
        long due = loop.dueOf("local");
        assertThat(due - ranAt).as("1 s stretched x4").isEqualTo(4_000 * MS);
        for (int i = 0; i < 9; i++) { // asked early, before its own 1 s is up: it does nothing (DW-1)
            time.advance(Duration.ofMillis(100));
            reg.wake();
            time.runDue();
        }
        assertThat(runs.size()).isEqualTo(first + 1);
        assertThat(loop.dueOf("local")).as("asked early nine times, the held deadline stays").isEqualTo(due);
    }

    // The phase-4 review (srad/agentspaces-event-loop.md §10.23) -------------------------------------------------

    /**
     * Review defect R1 on two nodes sharing a one-thread pool: a node woken again by each of its runs (inbound work
     * that never lets up) must not hold the pool's thread from the other node. Before the fix its drain ran until
     * the work stopped, and the other node's run waited for all of it (or for the 5 s stuck bound on a real tick).
     * No timing is asserted: the count of the hot node's runs while the quiet one's waited.
     */
    @Test
    void aNodeWokenByEveryRunCannotHoldTheSharedPoolsThreadFromAnotherNode() throws Exception {
        SystemTime system = SystemTime.isolated("starve-timer");
        LoopPool pool = system.loopPool();
        pool.options(pool.options().withMax(1));
        NodeLoop hot = new NodeLoop("node:hot", system, NodeLoop.DEFAULT_BACKSTOP);
        NodeLoop quiet = new NodeLoop("node:quiet", system, NodeLoop.DEFAULT_BACKSTOP);
        loops.add(hot);
        loops.add(quiet);
        AtomicLong hotRuns = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<DueWork.Registration> self = new AtomicReference<>();
        self.set(hot.register("busy", NodeLoop.Kind.OTHER, "g", now -> {
            if (hotRuns.incrementAndGet() < 300_000 && !stop.get()) {
                self.get().wake(); // more inbound work before this run ends
            }
            return DueWork.NEVER;
        }));
        hot.start();
        long guard = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (hotRuns.get() < 1_000 && System.nanoTime() - guard < 0) {
            Thread.onSpinWait(); // a hang guard only: the hot node reaches 1,000 runs in well under a second
        }
        AtomicLong hotRunsWhenQuietRan = new AtomicLong();
        CountDownLatch ran = new CountDownLatch(1);
        quiet.register("probe", NodeLoop.Kind.PROTOCOL, "h", now -> {
            hotRunsWhenQuietRan.set(hotRuns.get());
            ran.countDown();
            return DueWork.NEVER;
        });
        quiet.start();
        long queuedAt = hotRuns.get();
        try {
            assertThat(ran.await(30, TimeUnit.SECONDS)).as("the quiet node ran").isTrue();
        } finally {
            stop.set(true);
        }
        assertThat(hotRunsWhenQuietRan.get() - queuedAt)
                .as("the hot node's runs while the quiet node's waited for the pool: a few, not all of its work")
                .isLessThan(100);
    }
}
