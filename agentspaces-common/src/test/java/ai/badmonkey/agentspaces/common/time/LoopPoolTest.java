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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The elastic loop pool's rules (srad/agentspaces-event-loop.md §3.10, decision 2, EL-O-02, EL-O-03), judged on a
 * clock the test moves: the pool's real threads run the tasks, and every grow and shrink decision reads the test's
 * clock, so no assertion reads how long anything took. Waits are for a state the pool reaches (a hang guard of 10 s
 * each); the class timeout is a hang guard too.
 */
@Timeout(120)
class LoopPoolTest {

    private static final long MS = 1_000_000L;
    private static final long TICK = 2_000 * MS;

    private final AtomicLong clock = new AtomicLong(1_000_000 * MS);
    private final List<CountDownLatch> gates = new ArrayList<>();
    private final List<AutoCloseable> opened = new ArrayList<>();

    @AfterEach
    void openEverything() throws Exception {
        gates.forEach(g -> {
            while (g.getCount() > 0) {
                g.countDown();
            }
        });
        for (AutoCloseable c : opened) {
            c.close();
        }
    }

    private LoopPool pool(String name, int max) {
        return pool(name, LoopPool.Options.defaults(4).withMax(max));
    }

    /** A pool judged only by the test's own ticks: its watchdog judges nothing, so no real tick reads another clock. */
    private LoopPool pool(String name, LoopPool.Options o) {
        return new LoopPool(name, Watchdog::none, o, clock::get);
    }

    private AutoCloseable open(LoopPool pool) {
        AutoCloseable c = pool.open();
        opened.add(c);
        return c;
    }

    private CountDownLatch gate() {
        CountDownLatch g = new CountDownLatch(1);
        gates.add(g);
        return g;
    }

    private static void until(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("not reached within 10 s (a hang guard): " + what);
            }
            Thread.sleep(2); // a poll for a state another thread reaches; no timing is asserted
        }
    }

    /**
     * One watchdog tick that judged the pool. The tick only tries the pool's lock (WD-1) and skips when a thread holds
     * it, which a worker does for a moment after each run: a test that ticks right after its runs ran could land in
     * that moment, and its tick judged nothing (the p99 test read 0 once, §10.35). The watchdog's next tick judges then;
     * here the test ticks again at the same instant.
     */
    private void judgedTick(LoopPool pool) {
        for (int i = 0; i < 1_000; i++) {
            long skipped = pool.stats().skippedTicks();
            pool.watchdogTick(clock.get());
            if (pool.stats().skippedTicks() == skipped) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("every tick was skipped: the pool's lock stays held");
    }

    private static void blockOn(CountDownLatch gate) {
        try {
            gate.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void theDefaultMaxIsMinOfEightAndMaxOfTwoAndHalfTheCores() {
        assertThat(LoopPool.defaultMax(1)).isEqualTo(2);
        assertThat(LoopPool.defaultMax(4)).isEqualTo(2);
        assertThat(LoopPool.defaultMax(10)).isEqualTo(5);
        assertThat(LoopPool.defaultMax(16)).isEqualTo(8);
        assertThat(LoopPool.defaultMax(64)).isEqualTo(8);
        LoopPool.Options d = LoopPool.Options.defaults(10);
        assertThat(d.min()).isEqualTo(1);
        assertThat(d.idle()).isEqualTo(Duration.ofSeconds(60));
        assertThat(d.growLateness()).isEqualTo(Duration.ofMillis(20));
        assertThat(d.stretchLateness()).isEqualTo(Duration.ofMillis(100));
        assertThat(d.stretchCap()).isEqualTo(4);
    }

    @Test
    void thePropertiesOverrideTheBounds() {
        System.setProperty("agentspaces.loop.pool-max", "3");
        System.setProperty("agentspaces.loop.pool-idle-millis", "5000");
        System.setProperty("agentspaces.loop.stretch-cap", "2");
        try {
            LoopPool.Options o = LoopPool.Options.fromSystemProperties();
            assertThat(o.max()).isEqualTo(3);
            assertThat(o.idle()).isEqualTo(Duration.ofSeconds(5));
            assertThat(o.stretchCap()).isEqualTo(2);
            System.setProperty("agentspaces.loop.pool-max", "0");
            assertThat(LoopPool.Options.fromSystemProperties().max())
                    .as("0 means the default").isEqualTo(LoopPool.defaultMax(Runtime.getRuntime().availableProcessors()));
        } finally {
            System.clearProperty("agentspaces.loop.pool-max");
            System.clearProperty("agentspaces.loop.pool-idle-millis");
            System.clearProperty("agentspaces.loop.stretch-cap");
        }
    }

    @Test
    void theFirstTaskStartsOnePlatformThreadNamedAfterThePoolAndTheMinStaysWhileANodeIsOpen() throws Exception {
        LoopPool pool = pool("test-loop-first", 4);
        assertThat(pool.threads()).as("no thread before any work").isZero();
        open(pool);
        CountDownLatch ran = new CountDownLatch(3);
        List<Thread> on = new java.util.concurrent.CopyOnWriteArrayList<>();
        for (int i = 0; i < 3; i++) {
            pool.execute(() -> {
                on.add(Thread.currentThread());
                ran.countDown();
            });
        }
        assertThat(ran.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(on).allSatisfy(t -> {
            assertThat(t.isVirtual()).as("off the application's carriers (decision 2)").isFalse();
            assertThat(t.isDaemon()).isTrue();
            assertThat(t.getName()).startsWith("test-loop-first-");
        });
        assertThat(pool.threads()).as("growth past the first thread waits for the tick").isEqualTo(1);
        clock.addAndGet(Duration.ofMinutes(5).toNanos());
        judgedTick(pool);
        assertThat(pool.threads()).as("the min stays while a node is open, however long idle").isEqualTo(1);
    }

    @Test
    void itGrowsOneThreadATickWhileMoreRunsAreQueuedThanThreadsServeThemUpToTheMax() throws Exception {
        LoopPool.Options d = LoopPool.Options.defaults(4);
        // A stuck bound past the test's ticks: here the held tasks are a queue to serve, not a wedge.
        LoopPool pool = pool("test-loop-grow", new LoopPool.Options(1, 3, d.idle(), d.growLateness(), d.growTicks(),
                Duration.ofHours(1), d.stretchLateness(), d.stretchTicks(), d.stretchClear(), d.stretchCap()));
        open(pool);
        CountDownLatch g = gate();
        AtomicInteger started = new AtomicInteger();
        for (int i = 0; i < 6; i++) {
            pool.execute(() -> {
                started.incrementAndGet();
                blockOn(g);
            });
        }
        until("the first thread took a task", () -> started.get() == 1);
        for (int tick = 1; tick <= 4; tick++) {
            clock.addAndGet(TICK);
            judgedTick(pool);
            int expected = Math.min(3, Math.max(1, tick)); // the first tick of a queue grows nothing (§10.35)
            assertThat(pool.threads()).as("after tick %d", tick).isEqualTo(expected);
            until("the new thread took a task", () -> started.get() == expected);
        }
        assertThat(pool.stats().grows()).isEqualTo(2);
        assertThat(pool.atMax()).isTrue();
        g.countDown();
        until("every queued task ran", () -> pool.stats().tasks() == 6);
    }

    /**
     * §10.34's observation, §10.35: runs queued at the instant of one tick (co-resident nodes' deadlines falling
     * together on the tick's grid) grow no thread; queued at two ticks running, they do, as the p99 rule judges.
     */
    @Test
    void runsQueuedAtOneTickOnlyGrowNoThreadButQueuedAtTwoTicksRunningTheyDo() throws Exception {
        LoopPool pool = pool("test-loop-two-ticks", 3);
        open(pool);
        CountDownLatch hold = gate();
        CountDownLatch holding = new CountDownLatch(1);
        AtomicInteger ran = new AtomicInteger();
        pool.execute(() -> {
            holding.countDown();
            blockOn(hold);
        });
        holding.await();
        pool.execute(ran::incrementAndGet);
        pool.execute(ran::incrementAndGet);
        judgedTick(pool); // the tick lands in the instant the two were queued
        assertThat(pool.threads()).as("one tick of a momentary queue grows nothing").isEqualTo(1);
        hold.countDown();
        until("the queue drained", () -> ran.get() == 2);
        clock.addAndGet(TICK);
        judgedTick(pool);
        assertThat(pool.threads()).as("the next tick sees no queue: still one thread").isEqualTo(1);

        CountDownLatch hold2 = gate();
        CountDownLatch holding2 = new CountDownLatch(1);
        pool.execute(() -> {
            holding2.countDown();
            blockOn(hold2);
        });
        holding2.await();
        pool.execute(ran::incrementAndGet);
        pool.execute(ran::incrementAndGet);
        judgedTick(pool);
        assertThat(pool.threads()).isEqualTo(1);
        judgedTick(pool); // the same queue at the next tick
        assertThat(pool.threads()).as("queued at two ticks running: one thread more").isEqualTo(2);
        assertThat(pool.stats().grows()).isEqualTo(1);
        hold2.countDown();
        until("the queue drained", () -> ran.get() == 4);
    }

    @Test
    void itGrowsWhenRunsWaitOverTwentyMillisecondsAtP99ForTwoTicks() throws Exception {
        LoopPool pool = pool("test-loop-p99", 4);
        open(pool);
        AtomicInteger done = new AtomicInteger();
        // Runs that waited 30 ms on the test's clock behind a held one, all done by the tick: nothing is queued then,
        // so only the lateness rule can grow the pool.
        for (int tick = 1; tick <= 2; tick++) {
            CountDownLatch hold = gate();
            CountDownLatch holding = new CountDownLatch(1);
            pool.execute(() -> {
                holding.countDown();
                blockOn(hold);
            });
            holding.await();
            int before = done.get();
            for (int i = 0; i < 20; i++) {
                pool.execute(done::incrementAndGet);
            }
            clock.addAndGet(30 * MS);
            hold.countDown();
            until("the runs ran", () -> done.get() == before + 20);
            judgedTick(pool);
            assertThat(pool.stats().lastTickP99()).as("tick %d p99", tick).isGreaterThan(20 * MS);
            assertThat(pool.threads()).as("after tick %d", tick).isEqualTo(tick == 1 ? 1 : 2);
        }
        for (int i = 0; i < 5; i++) {
            long before = done.get();
            pool.execute(done::incrementAndGet);
            until("the run ran", () -> done.get() > before); // no wait on the clock now: back in band
        }
        judgedTick(pool);
        assertThat(pool.stats().lastTickP99()).isLessThanOrEqualTo(20 * MS);
        assertThat(pool.threads()).as("in band: no more growth").isEqualTo(2);
    }

    @Test
    void threadsIdleForSixtySecondsEndAtTheNextTickDownToTheMinAndToNoneOnceNoNodeIsOpen() throws Exception {
        LoopPool pool = pool("test-loop-shrink", 3);
        AutoCloseable node = open(pool);
        CountDownLatch g = gate();
        AtomicInteger started = new AtomicInteger();
        for (int i = 0; i < 3; i++) {
            pool.execute(() -> {
                started.incrementAndGet();
                blockOn(g);
            });
        }
        until("one started", () -> started.get() == 1);
        for (int tick = 0; tick < 3; tick++) { // queued at two ticks running grows one a tick (§10.35)
            clock.addAndGet(TICK);
            judgedTick(pool);
        }
        until("three threads run", () -> started.get() == 3);
        g.countDown();
        until("all idle", () -> pool.stats().idleThreads() == 3);
        clock.addAndGet(59_000 * MS);
        judgedTick(pool);
        assertThat(pool.threads()).as("not yet idle 60 s").isEqualTo(3);
        clock.addAndGet(1_000 * MS);
        judgedTick(pool);
        until("every thread idle 60 s ended at the tick, down to the min", () -> pool.threads() == 1);
        judgedTick(pool);
        assertThat(pool.threads()).as("the min stays").isEqualTo(1);
        assertThat(pool.stats().shrinks()).isEqualTo(2);
        node.close();
        judgedTick(pool);
        until("no node open: the last thread ends too", () -> pool.threads() == 0);
        CountDownLatch again = new CountDownLatch(1);
        pool.execute(again::countDown);
        assertThat(again.await(10, TimeUnit.SECONDS)).as("work after that starts a thread again").isTrue();
    }

    @Test
    void aThreadStuckPastTheWedgeBoundDoesNotCountTowardsTheMax() throws Exception {
        LoopPool pool = pool("test-loop-stuck", 2);
        open(pool);
        CountDownLatch wedge = gate();
        CountDownLatch other = gate();
        AtomicInteger started = new AtomicInteger();
        AtomicInteger ran = new AtomicInteger();
        pool.execute(() -> {
            started.incrementAndGet();
            blockOn(wedge);
        });
        pool.execute(() -> {
            started.incrementAndGet();
            blockOn(other);
        });
        until("one started", () -> started.get() == 1);
        for (int tick = 0; tick < 2; tick++) { // the second run has waited past 20 ms for two ticks
            clock.addAndGet(1_000 * MS);
            judgedTick(pool);
        }
        until("both started", () -> started.get() == 2);
        assertThat(pool.atMax()).isTrue();
        for (int i = 0; i < 3; i++) {
            pool.execute(ran::incrementAndGet);
        }
        other.countDown(); // the second thread is free again; the first stays wedged
        until("the free thread ran the queue", () -> ran.get() == 3);
        CountDownLatch wedge2 = gate();
        pool.execute(() -> blockOn(wedge2)); // now both threads are held
        until("both held", () -> pool.stats().idleThreads() == 0 && pool.stats().queued() == 0);
        pool.execute(ran::incrementAndGet);
        clock.addAndGet(6_000 * MS); // past the 5 s stuck bound for both, judged from each one's start
        judgedTick(pool);
        until("a compensating thread ran the queued task", () -> ran.get() == 4);
        assertThat(pool.threads()).as("past the max, in place of the stuck threads").isEqualTo(3);
        assertThat(pool.stats().compensations()).isEqualTo(1);
    }

    @Test
    void aThreadThatDiesOfAnErrorIsReplacedWhileWorkIsQueued() throws Exception {
        LoopPool pool = pool("test-loop-dies", 2);
        open(pool);
        CountDownLatch ran = new CountDownLatch(1);
        pool.execute(() -> {
            throw new InternalError("scripted: the pool thread dies");
        });
        pool.execute(ran::countDown);
        assertThat(ran.await(10, TimeUnit.SECONDS)).as("the next task still runs").isTrue();
        assertThat(pool.threads()).isEqualTo(1);
    }

    @Test
    void aTaskThatThrowsIsLoggedAndTheThreadGoesOn() throws Exception {
        LoopPool pool = pool("test-loop-throws", 2);
        open(pool);
        CountDownLatch ran = new CountDownLatch(1);
        pool.execute(() -> {
            throw new IllegalStateException("scripted");
        });
        pool.execute(ran::countDown);
        assertThat(ran.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(pool.stats().replaced()).isZero();
        assertThat(pool.stats().tasks()).isEqualTo(2);
    }

    @Test
    void theWatchdogTickOnlyTriesThePoolsLock() throws Exception {
        LoopPool pool = pool("test-loop-trylock", 2);
        open(pool);
        java.lang.reflect.Field f = LoopPool.class.getDeclaredField("lock");
        f.setAccessible(true);
        java.util.concurrent.locks.ReentrantLock lock = (java.util.concurrent.locks.ReentrantLock) f.get(pool);
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = gate();
        Thread holder = Thread.ofVirtual().start(() -> {
            lock.lock();
            try {
                held.countDown();
                blockOn(release);
            } finally {
                lock.unlock();
            }
        });
        held.await();
        pool.watchdogTick(clock.get()); // WD-1: returns at once
        assertThat(pool.stats().skippedTicks()).isEqualTo(1);
        release.countDown();
        holder.join();
    }

    /**
     * EL-SP-28's rule on the test's clock (the spoiler runs it on a whole fleet): 20 s of queued runs, then quiet; the
     * pool grows one thread a tick while runs wait, and is back at its min within 60 s plus one period of the
     * burst's end.
     */
    @Test
    void elSp28aBurstGrowsThePoolATickAtATimeAndItIsBackAtItsMinWithinSixtySecondsPlusAPeriod() throws Exception {
        LoopPool pool = pool("test-loop-sp28", 4);
        open(pool);
        AtomicInteger runs = new AtomicInteger();
        List<Integer> threadsPerTick = new ArrayList<>();
        // The burst: each tick, 40 runs land and each takes 50 ms of the test's clock (it moves the clock as it runs),
        // for 10 ticks (20 s).
        for (int tick = 0; tick < 10; tick++) {
            int before = runs.get();
            for (int i = 0; i < 40; i++) {
                pool.execute(() -> {
                    clock.addAndGet(50 * MS / pool.threads()); // more threads share the work: less of the clock each
                    runs.incrementAndGet();
                });
            }
            until("this tick's runs ran", () -> runs.get() == before + 40);
            judgedTick(pool);
            threadsPerTick.add(pool.threads());
        }
        long burstEnd = clock.get();
        for (int i = 1; i < threadsPerTick.size(); i++) {
            assertThat(threadsPerTick.get(i) - threadsPerTick.get(i - 1)).as("at most one thread a tick")
                    .isBetween(0, 1);
        }
        assertThat(threadsPerTick.get(threadsPerTick.size() - 1)).as("the burst grew the pool").isGreaterThan(1);
        int ticks = 0;
        while (pool.threads() > 1) {
            clock.addAndGet(TICK);
            judgedTick(pool);
            ticks++;
            until("the threads the tick told to end have ended", () -> pool.stats().retiring() == 0);
            assertThat(ticks).as("hang guard").isLessThan(200);
        }
        long back = clock.get() - burstEnd;
        assertThat(back).as("back at the min within 60 s plus one period of the burst's end")
                .isLessThanOrEqualTo(60_000 * MS + TICK);
    }

    /**
     * EL-SP-28's soak failure (srad/agentspaces-event-loop.md §10.39, root-caused in §10.47): after a burst, co-resident
     * loops that swept together during it keep sweeping together every backstop, so their runs land at once on every
     * thread, and no thread is idle for 60 s in the literal sense. The surplus then ends only by the busy-under-1%
     * rule, which any tick under pressure restarted: one tick that sampled the runs queued (or one wait past the grow
     * bound) held every surplus thread another 60 s. Pressure now counts as growth does, on two ticks running (EL-O-03,
     * §10.35): a lone tick is a moment, not a load.
     */
    @Test
    void aLoneTickThatSamplesRunsQueuedAfterABurstDoesNotHoldTheSurplusPastSixtySecondsPlusAPeriod() throws Exception {
        LoopPool pool = pool("test-loop-lone", 3);
        open(pool);
        CountDownLatch g = gate();
        AtomicInteger started = new AtomicInteger();
        for (int i = 0; i < 3; i++) {
            pool.execute(() -> {
                started.incrementAndGet();
                blockOn(g);
            });
        }
        until("one started", () -> started.get() == 1);
        for (int tick = 0; tick < 3; tick++) { // queued at two ticks running grows one a tick (§10.35)
            clock.addAndGet(TICK);
            judgedTick(pool);
        }
        until("three threads run", () -> started.get() == 3);
        g.countDown();
        until("all idle", () -> pool.stats().idleThreads() == 3);
        long burstEnd = clock.get();
        long back = -1;
        for (int tick = 1; tick <= 60 && back < 0; tick++) {
            clock.addAndGet(TICK);
            if (tick == 15) { // 30 s after the burst: the co-resident loops' sweeps land together on the tick
                CountDownLatch hold = gate();
                AtomicInteger in = new AtomicInteger();
                for (int i = 0; i < 3; i++) {
                    pool.execute(() -> {
                        in.incrementAndGet();
                        blockOn(hold);
                    });
                }
                until("every thread took a run", () -> in.get() == 3);
                pool.execute(() -> { }); // one more run than threads: queued when the tick samples
                judgedTick(pool);
                assertThat(pool.stats().queued()).as("the tick sampled a run queued").isEqualTo(1);
                hold.countDown();
                until("the threads are idle again", () -> pool.stats().idleThreads() == pool.threads()
                        && pool.stats().queued() == 0);
            } else {
                judgedTick(pool);
            }
            until("the threads told to end have ended", () -> pool.stats().retiring() == 0);
            if (pool.threads() == 1) {
                back = clock.get() - burstEnd;
            }
        }
        assertThat(back).as("back at the min within 60 s plus a period of the burst's end, one lone tick under"
                + " pressure notwithstanding").isBetween(0L, 60_000 * MS + TICK);
    }

    @Test
    void aRunHeldPastTheGrowBoundWhileAnotherWaitsGetsAThreadAtOnceWithoutWaitingForTheTick() throws Exception {
        LoopPool pool = pool("test-loop-prompt", 3);
        open(pool);
        AtomicInteger backlog = new AtomicInteger();
        pool.onBacklog(backlog::incrementAndGet);
        CountDownLatch held = gate();
        CountDownLatch holding = new CountDownLatch(1);
        pool.execute(() -> {
            holding.countDown();
            blockOn(held); // one node's 400 ms participant (EL-SP-08)
        });
        holding.await();
        CountDownLatch other = new CountDownLatch(1);
        pool.execute(other::countDown);
        assertThat(backlog.get()).as("the timer is told to look").isEqualTo(1);
        assertThat(pool.promptCheckAt()).isEqualTo(clock.get() + 20 * MS);
        clock.addAndGet(10 * MS);
        pool.promptCheck(clock.get());
        assertThat(pool.threads()).as("not yet past the grow bound").isEqualTo(1);
        clock.addAndGet(15 * MS);
        pool.promptCheck(clock.get());
        assertThat(other.await(10, TimeUnit.SECONDS)).as("the waiting run ran on a new thread").isTrue();
        assertThat(pool.threads()).isEqualTo(2);
        assertThat(pool.stats().promptGrows()).isEqualTo(1);
        until("the new thread is idle", () -> pool.stats().idleThreads() == 1);
        pool.promptCheck(clock.get());
        assertThat(pool.promptCheckAt()).as("nothing waits: no more checks").isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void shortRunsThatQueueGrowNoThreadPromptlyOnlyAtTheTick() throws Exception {
        LoopPool pool = pool("test-loop-short", 3);
        open(pool);
        CountDownLatch hold = gate();
        CountDownLatch holding = new CountDownLatch(1);
        pool.execute(() -> {
            holding.countDown();
            blockOn(hold);
        });
        holding.await();
        pool.execute(() -> { });
        // the holder started at the clock's now, so it is not held past the bound yet when the queued one is past it
        clock.addAndGet(20 * MS);
        pool.promptCheck(clock.get() + 0);
        assertThat(pool.threads()).as("held by exactly the bound, not past it").isEqualTo(1);
        hold.countDown();
        until("the queue drained", () -> pool.stats().tasks() == 2);
        assertThat(pool.stats().promptGrows()).isZero();
    }

    /**
     * The defect EL-SP-28's soak run found (srad/agentspaces-event-loop.md §10.21): after a burst, idle runs still
     * trickle in, one at a time. A free thread must take them most-recently-idle first, so the surplus threads stay
     * idle and end; handed round the threads in turn, none is ever idle for 60 s and the pool never shrinks.
     */
    @Test
    void aTrickleOfRunsAfterABurstIsTakenByTheMostRecentlyIdleThreadSoTheSurplusStillEnds() throws Exception {
        LoopPool pool = pool("test-loop-trickle", 3);
        open(pool);
        CountDownLatch g = gate();
        AtomicInteger started = new AtomicInteger();
        for (int i = 0; i < 3; i++) {
            pool.execute(() -> {
                started.incrementAndGet();
                blockOn(g);
            });
        }
        until("one started", () -> started.get() == 1);
        for (int tick = 0; tick < 3; tick++) { // queued at two ticks running grows one a tick (§10.35)
            clock.addAndGet(TICK);
            judgedTick(pool);
        }
        until("three threads run", () -> started.get() == 3);
        g.countDown();
        until("all idle", () -> pool.stats().idleThreads() == 3);
        for (int second = 0; second < 64; second++) { // one short run a second, each done before the next
            CountDownLatch ran = new CountDownLatch(1);
            pool.execute(ran::countDown);
            assertThat(ran.await(10, TimeUnit.SECONDS)).isTrue();
            until("the thread is idle again", () -> pool.stats().idleThreads() == pool.threads());
            clock.addAndGet(1_000 * MS);
            if (second % 2 == 1) {
                judgedTick(pool);
                until("the threads told to end have ended", () -> pool.stats().retiring() == 0);
            }
        }
        assertThat(pool.threads()).as("the surplus ended within 60 s plus a period of the burst").isEqualTo(1);
    }

    /**
     * The second defect EL-SP-28's soak run found (§10.21): co-resident nodes' deadlines fall together, so a trickle of
     * short runs lands several at once and touches every thread, and no thread is ever idle 60 s in the literal sense.
     * A thread busy under 1% of the last 60 s, with the waits in band, is idle.
     */
    @Test
    void runsLandingTogetherAfterABurstStillLetTheSurplusEndWithinSixtySecondsPlusAPeriod() throws Exception {
        LoopPool pool = pool("test-loop-aligned", 3);
        open(pool);
        CountDownLatch g = gate();
        AtomicInteger started = new AtomicInteger();
        for (int i = 0; i < 3; i++) {
            pool.execute(() -> {
                started.incrementAndGet();
                blockOn(g);
            });
        }
        until("one started", () -> started.get() == 1);
        for (int tick = 0; tick < 3; tick++) { // queued at two ticks running grows one a tick (§10.35)
            clock.addAndGet(TICK);
            judgedTick(pool);
        }
        until("three threads run", () -> started.get() == 3);
        g.countDown();
        until("all idle", () -> pool.stats().idleThreads() == 3);
        long burstEnd = clock.get();
        long back = -1;
        for (int half = 0; half < 140 && back < 0; half++) { // every 500 ms of the clock, three runs land at once
            CountDownLatch all = new CountDownLatch(3);
            CountDownLatch hold = new CountDownLatch(1);
            for (int i = 0; i < 3; i++) {
                pool.execute(() -> {
                    all.countDown();
                    blockOn(hold); // held together for a moment, so each takes a thread of its own when it can
                });
            }
            all.await(200, TimeUnit.MILLISECONDS);
            hold.countDown();
            until("the threads are idle again", () -> pool.stats().idleThreads() == pool.threads());
            clock.addAndGet(500 * MS);
            if (half % 4 == 3) {
                judgedTick(pool);
                until("the threads told to end have ended", () -> pool.stats().retiring() == 0);
                if (pool.threads() == 1) {
                    back = clock.get() - burstEnd;
                }
            }
        }
        assertThat(back).as("back at the min within 60 s plus a period of the burst's end")
                .isBetween(0L, 60_000 * MS + TICK);
    }

    // The phase-4 review (srad/agentspaces-event-loop.md §10.23) -----------------------------------------------------

    /**
     * Review defect R1: a node's serial executor drained until its queue ran dry, so a node woken again by every run
     * (inbound work that never lets up) held its pool thread for as long as the work lasted. With the pool at its max,
     * every co-resident node's runs waited until the stuck bound let the tick compensate, 5 s or more, and the
     * watchdog then blamed each waiting node with a stuck dispatch that a re-initialisation, on the same pool, could
     * not repair. A drain now hands its thread back after a task while another run waits for the pool.
     */
    @Test
    void aDrainThatNeverRunsDryYieldsItsPoolThreadToAnotherNodesRun() throws Exception {
        LoopPool pool = pool("test-loop-fair", 1);
        open(pool);
        SerialExecutor hot = new SerialExecutor("node:hot", pool);
        SerialExecutor quiet = new SerialExecutor("node:quiet", pool);
        AtomicLong hotRuns = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean();
        Runnable[] again = new Runnable[1];
        again[0] = () -> {
            if (hotRuns.incrementAndGet() < 5_000_000 && !stop.get()) {
                hot.execute(again[0]); // each run is woken again before it ends
            }
        };
        hot.execute(again[0]);
        until("the hot node runs", () -> hotRuns.get() > 1_000);
        AtomicLong hotRunsWhenQuietRan = new AtomicLong();
        CountDownLatch ran = new CountDownLatch(1);
        quiet.execute(() -> {
            hotRunsWhenQuietRan.set(hotRuns.get());
            ran.countDown();
        });
        long queuedAt = hotRuns.get();
        try {
            assertThat(ran.await(10, TimeUnit.SECONDS)).as("the quiet node's run ran").isTrue();
        } finally {
            stop.set(true);
        }
        assertThat(hotRunsWhenQuietRan.get() - queuedAt)
                .as("the hot node's runs after the quiet node's was queued: a few, not until the hot one ran dry")
                .isLessThan(100);
    }

    /**
     * Review defect R2: a node closing while another opened could leave the pool open and unwatched. The closing
     * node's release read the registration after the opening node had replaced it, and closed the new one, so no
     * tick reached the pool again: no grow, no shrink, no compensation for a wedge, and a stale "at max" for the
     * stretch. The registration now follows the count of open nodes, whatever the order.
     */
    @Test
    void aNodeClosingWhileAnotherOpensLeavesThePoolWatched() throws Exception {
        Watchdog dog = Watchdog.manual(TimeSource.system(), Watchdog.Options.defaults());
        LoopPool pool = new LoopPool("test-loop-reg", () -> dog, LoopPool.Options.defaults(4).withMax(2), clock::get);
        AutoCloseable first = pool.open();
        assertThat(dog.registered()).contains(pool.name());
        Thread closer;
        synchronized (pool) { // the closing node is held between its count and its registration
            closer = Thread.ofPlatform().name("test-loop-reg-closer").start(() -> {
                try {
                    first.close();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            until("the closing node waits to drop the registration",
                    () -> closer.getState() == Thread.State.BLOCKED);
            open(pool); // another node opens meanwhile: the count goes from 0 to 1
        }
        closer.join(10_000);
        assertThat(closer.isAlive()).isFalse();
        assertThat(pool.stats().users()).isEqualTo(1);
        assertThat(dog.registered()).as("an open node's pool is judged by the watchdog (EL-H-02, EL-O-03)")
                .contains(pool.name());
    }

    /**
     * Review defect R3: a run handed to the pool while its only idle thread had been woken for another run, and had
     * not taken it yet, armed no prompt check. When the run the thread took was one node's long one, the second
     * waited for all of it, as EL-SP-08 forbids, unless some later run happened to arm the check.
     */
    @Test
    void aSecondRunHandedToTheOneWokenThreadStillGetsThePromptCheck() throws Exception {
        LoopPool pool = pool("test-loop-arm", 3);
        open(pool);
        CountDownLatch warm = new CountDownLatch(1);
        pool.execute(warm::countDown);
        warm.await();
        until("the thread is idle", () -> pool.stats().idleThreads() == 1);
        CountDownLatch held = gate();
        CountDownLatch holding = new CountDownLatch(1);
        CountDownLatch other = new CountDownLatch(1);
        // The two runs are handed over while the test holds the pool's lock (it is reentrant), so the woken thread
        // cannot take the first before the second arrives: the interleaving that missed the check, made certain.
        java.lang.reflect.Field lockField = LoopPool.class.getDeclaredField("lock");
        lockField.setAccessible(true);
        java.util.concurrent.locks.ReentrantLock poolLock = (java.util.concurrent.locks.ReentrantLock) lockField.get(pool);
        poolLock.lock();
        try {
            pool.execute(() -> { // one node's long run (EL-SP-08) ...
                holding.countDown();
                blockOn(held);
            });
            pool.execute(other::countDown); // ... and another's, before the woken thread took the first
        } finally {
            poolLock.unlock();
        }
        holding.await();
        assertThat(pool.promptCheckAt()).as("the timer looks again at the grow bound").isNotEqualTo(Long.MAX_VALUE);
        clock.addAndGet(25 * MS);
        pool.promptCheck(clock.get());
        assertThat(other.await(10, TimeUnit.SECONDS)).as("the waiting run ran on a new thread").isTrue();
        assertThat(pool.stats().promptGrows()).isEqualTo(1);
    }
}
