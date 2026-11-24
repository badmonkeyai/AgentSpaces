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
import ai.badmonkey.agentspaces.common.time.Watchdog;
import ai.badmonkey.agentspaces.common.time.Watched;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link NodeLoop} under its watchdog (srad/agentspaces-event-loop.md §3.9, WD-2 to WD-5, WD-8, EL-H-04 to EL-H-07,
 * EL-H-11, EL-H-14), on {@link VirtualTime}, whose own watchdog ticks as each advance crosses its 2 s period. Each
 * fault here is scripted, so the watchdog is marked scripted and its findings stay out of the process totals; the
 * first test is the unscripted control (R14: an idle loop is never judged faulty).
 */
@Timeout(120)
class NodeLoopWatchdogTest {

    private static final long MS = 1_000_000L;

    private final VirtualTime time = VirtualTime.create();
    private final List<NodeLoop> loops = new ArrayList<>();

    @AfterEach
    void tearDown() {
        loops.forEach(NodeLoop::close);
    }

    private NodeLoop loop() {
        NodeLoop loop = new NodeLoop("node:wd", time, NodeLoop.DEFAULT_BACKSTOP);
        loops.add(loop);
        return loop;
    }

    /** A participant that counts its runs and asks to run every second. */
    private static final class Ticker implements DueWork {
        final AtomicInteger runs = new AtomicInteger();
        final AtomicInteger concurrent = new AtomicInteger();
        final AtomicInteger maxConcurrent = new AtomicInteger();

        @Override
        public long run(long now) {
            int c = concurrent.incrementAndGet();
            maxConcurrent.accumulateAndGet(c, Math::max);
            runs.incrementAndGet();
            concurrent.decrementAndGet();
            return now + 1_000 * MS;
        }
    }

    @Test
    void anIdleLoopIsNeverJudgedFaulty() {
        Watchdog wd = time.watchdog();
        long totals = Watchdog.totalDetections();
        NodeLoop loop = loop();
        loop.register("ticker", NodeLoop.Kind.PROTOCOL, "g", new Ticker());
        loop.start();
        time.advance(Duration.ofMinutes(10));
        assertThat(wd.stats().ticks()).as("the virtual-time watchdog ticked every 2 s").isGreaterThanOrEqualTo(299);
        assertThat(wd.stats().detections()).as("R14: no false positive").isZero();
        assertThat(Watchdog.totalDetections()).isEqualTo(totals);
        assertThat(loop.generation()).isZero();
        assertThat(loop.health().state()).isEqualTo(NodeLoop.HealthState.OK);
        loop.close();
        assertThat(wd.nextTickNanos()).as("a closed loop leaves the watchdog").isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void aLostNudgeIsFoundWithinOnePeriodPlusTheGraceAndTheWakeIsNotLost() {
        Watchdog wd = time.watchdog().markScripted();
        NodeLoop loop = loop();
        Ticker ticker = new Ticker();
        AtomicInteger asked = new AtomicInteger();
        DueWork.Registration reg = loop.register("asked", NodeLoop.Kind.OTHER, "g", now -> {
            asked.incrementAndGet();
            return DueWork.NEVER;
        });
        loop.register("ticker", NodeLoop.Kind.PROTOCOL, "g", ticker);
        loop.start();
        time.runDue();
        int askedBefore = asked.get();
        loop.dropNextDispatch();
        long raised = time.monoNanos();
        reg.wake();
        assertThat(loop.pendingRun()).as("the slot is full and nothing will drain it").isTrue();
        int tickerBefore = ticker.runs.get();
        time.advance(Duration.ofMillis(1_900));
        assertThat(ticker.runs.get()).as("the loop is silent: every later wake is absorbed").isEqualTo(tickerBefore);
        time.advance(Duration.ofMillis(200));
        assertThat(wd.detections()).singleElement().satisfies(d -> {
            assertThat(d.kind()).isEqualTo(Watched.Reason.MISSED_WAKE.id());
            assertThat(d.atNanos() - raised).as("found within one period plus the grace")
                    .isLessThanOrEqualTo(2_250 * MS);
        });
        assertThat(loop.generation()).isEqualTo(1);
        assertThat(asked.get()).as("WD-2: the pending wake survived the re-initialisation")
                .isEqualTo(askedBefore + 1);
        assertThat(ticker.runs.get()).as("the loop runs again").isGreaterThan(tickerBefore);
        assertThat(loop.stats().generation()).isEqualTo(1);
    }

    @Test
    void aDrainThatDiesIsReinitialisedAndEveryParticipantRunsAgain() {
        Watchdog wd = time.watchdog().markScripted();
        NodeLoop loop = loop();
        Ticker ticker = new Ticker();
        AtomicBoolean killNext = new AtomicBoolean();
        loop.register("killer", NodeLoop.Kind.OTHER, "g", now -> {
            if (killNext.getAndSet(false)) {
                throw new InternalError("scripted: the drain's thread dies (EL-SP-20)");
            }
            return now + 1_000 * MS;
        });
        loop.register("ticker", NodeLoop.Kind.PROTOCOL, "g", ticker);
        loop.start();
        time.runDue();
        killNext.set(true);
        assertThatThrownBy(() -> time.advance(Duration.ofSeconds(1)))
                .as("the error escapes the drain and kills its thread").isInstanceOf(AssertionError.class)
                .hasRootCauseInstanceOf(InternalError.class);
        long diedAt = time.monoNanos();
        assertThat(loop.runThread()).as("the run is still marked started on a dead thread").isNotNull();
        assertThat(loop.runThread().isAlive()).isFalse();
        int tickerBefore = ticker.runs.get();
        time.advance(Duration.ofSeconds(3));
        assertThat(wd.detections()).singleElement().satisfies(d -> {
            assertThat(d.kind()).isEqualTo("dead");
            assertThat(d.participant()).isEqualTo("killer");
            assertThat(d.atNanos() - diedAt).isLessThanOrEqualTo(2_000 * MS);
        });
        assertThat(ticker.runs.get()).as("the loop runs again").isGreaterThan(tickerBefore);
        assertThat(loop.runThread()).isNull();
        assertThat(loop.stats().participantThrows()).doesNotContainKey("killer");
    }

    @Test
    void aWedgedParticipantIsFencedTheOthersRunItsLateReturnReArmsNothingAndTheFenceEscalates() {
        Watchdog wd = time.watchdog().markScripted();
        NodeLoop loop = loop();
        List<NodeLoop.Health> healths = new CopyOnWriteArrayList<>();
        loop.onHealth(healths::add);
        Ticker ticker = new Ticker();
        AtomicBoolean wedgeNext = new AtomicBoolean();
        AtomicBoolean latch = new AtomicBoolean();
        AtomicInteger wedgedRuns = new AtomicInteger();
        AtomicInteger inWedged = new AtomicInteger();
        AtomicInteger maxInWedged = new AtomicInteger();
        AtomicLong returned = new AtomicLong();
        loop.register("wedger", NodeLoop.Kind.OTHER, "g", now -> {
            maxInWedged.accumulateAndGet(inWedged.incrementAndGet(), Math::max);
            try {
                wedgedRuns.incrementAndGet();
                if (wedgeNext.getAndSet(false)) {
                    while (!latch.get()) { // a latch that does not open; deaf to the interrupt
                        try {
                            time.sleep(Duration.ofSeconds(1));
                        } catch (InterruptedException e) {
                            // a truly wedged participant ignores it
                        }
                    }
                    returned.set(time.monoNanos());
                    return time.monoNanos() + 1 * MS; // discarded: WD-3
                }
                return now + 10_000 * MS;
            } finally {
                inWedged.decrementAndGet();
            }
        });
        loop.register("ticker", NodeLoop.Kind.PROTOCOL, "g", ticker);
        loop.start();
        time.runDue();
        wedgeNext.set(true);
        long wedgedAt = time.monoNanos();
        loop.wakeAll();
        time.runDue();
        assertThat(loop.runningIn()).isEqualTo("wedger");
        int tickerBefore = ticker.runs.get();
        time.advance(Duration.ofSeconds(8));
        assertThat(wd.detections()).singleElement().satisfies(d -> {
            assertThat(d.kind()).isEqualTo("wedged");
            assertThat(d.participant()).isEqualTo("wedger");
            assertThat(d.atNanos() - wedgedAt).as("within one period after the 5 s bound")
                    .isLessThanOrEqualTo(7_000 * MS);
        });
        assertThat(loop.fencedSinceNanos()).isPositive();
        time.advance(Duration.ofSeconds(30));
        assertThat(ticker.runs.get() - tickerBefore).as("every other participant runs while one is fenced")
                .isGreaterThanOrEqualTo(25);
        time.advance(Duration.ofMinutes(2));
        assertThat(loop.health().state()).as("the fence held past 2 minutes: escalated (WD-5)")
                .isEqualTo(NodeLoop.HealthState.DOWN);
        assertThat(healths).extracting(NodeLoop.Health::state).contains(NodeLoop.HealthState.DOWN);
        int runsBeforeOpen = wedgedRuns.get();
        latch.set(true);
        time.advance(Duration.ofSeconds(3));
        assertThat(returned.get()).as("the old run returned").isPositive();
        assertThat(loop.fencedSinceNanos()).as("the fence lifts when it returns").isZero();
        assertThat(wedgedRuns.get()).as("the new generation asked it again").isGreaterThan(runsBeforeOpen);
        assertThat(loop.dueOf("wedger")).as("its late deadline (now + 1 ms) was discarded (WD-3)")
                .isNotEqualTo(returned.get() + 1 * MS);
        assertThat(maxInWedged.get()).as("NL-2 across generations: never two runs of it at once").isEqualTo(1);
        assertThat(ticker.maxConcurrent.get()).isEqualTo(1);
        assertThat(wd.stats().reinits()).as("one wedge, one re-initialisation").isEqualTo(1);
    }

    /**
     * The adversarial review of phase 1 (srad/agentspaces-event-loop.md §10.10, finding W1): three healthy
     * participants of 3 s each make one 9 s run, and none of them is stuck past the 5 s wedge bound. The watchdog
     * judged the bound from the run's start, so it called the run wedged at its first tick past 5 s, fenced whichever
     * participant was running then and interrupted its thread: a healthy participant interrupted mid-work, counted as
     * a detection toward the escalation. The bound is judged per step now.
     */
    @Test
    void aRunOfSlowButHealthyParticipantsIsNotWedged() {
        Watchdog wd = time.watchdog().markScripted();
        NodeLoop loop = loop();
        java.util.Set<String> slowOnce = java.util.concurrent.ConcurrentHashMap.newKeySet();
        AtomicInteger interrupted = new AtomicInteger();
        AtomicInteger finished = new AtomicInteger();
        for (String name : List.of("a", "b", "c")) {
            loop.register(name, NodeLoop.Kind.PROTOCOL, "g", now -> {
                if (slowOnce.remove(name)) {
                    try {
                        time.sleep(Duration.ofSeconds(3)); // slow, healthy, and well inside the wedge bound
                        finished.incrementAndGet();
                    } catch (InterruptedException e) {
                        interrupted.incrementAndGet();
                        Thread.currentThread().interrupt();
                    }
                }
                return now + 10_000 * MS;
            });
        }
        loop.start();
        time.runDue();
        slowOnce.addAll(List.of("a", "b", "c"));
        loop.wakeAll();
        time.advance(Duration.ofSeconds(12));
        assertThat(wd.detections()).as("a 9 s run of three 3 s participants is slow, not wedged").isEmpty();
        assertThat(interrupted).as("no healthy participant was interrupted").hasValue(0);
        assertThat(finished).as("each slow participant ran to its end").hasValue(3);
        assertThat(loop.generation()).isZero();
        assertThat(wd.stats().slowRuns()).as("the run is still counted slow").isPositive();
    }

    @Test
    void aParticipantThatThrowsThreeTimesInARowIsBackedOffDegradedAndRecoversWhenClean() {
        time.watchdog().markScripted();
        NodeLoop loop = loop();
        List<NodeLoop.Health> healths = new CopyOnWriteArrayList<>();
        loop.onHealth(healths::add);
        AtomicBoolean failing = new AtomicBoolean(true);
        AtomicInteger runs = new AtomicInteger();
        DueWork.Registration reg = loop.register("flaky", NodeLoop.Kind.OTHER, "g", now -> {
            runs.incrementAndGet();
            if (failing.get()) {
                throw new IllegalStateException("expected by the test");
            }
            return DueWork.NEVER;
        });
        loop.start();
        time.runDue();
        time.advance(NodeLoop.DEFAULT_BACKSTOP.multipliedBy(2).plusMillis(100)); // two re-arms at the backstop: throws 2, 3
        assertThat(runs.get()).isEqualTo(3);
        assertThat(loop.health().state()).isEqualTo(NodeLoop.HealthState.DEGRADED);
        assertThat(loop.health().reasons()).singleElement().asString().contains("flaky");
        assertThat(loop.stats().backedOff()).containsExactly("flaky");
        int backedOffRuns = runs.get();
        for (int i = 0; i < 10; i++) {
            reg.wake();
            time.runDue();
        }
        assertThat(runs.get()).as("asked ten times while backed off: not run").isEqualTo(backedOffRuns);
        failing.set(false);
        time.advance(Duration.ofSeconds(2));
        assertThat(loop.health().state()).as("ran clean: OK").isEqualTo(NodeLoop.HealthState.OK);
        assertThat(healths).extracting(NodeLoop.Health::state)
                .containsExactly(NodeLoop.HealthState.DEGRADED, NodeLoop.HealthState.OK);
        assertThat(loop.stats().reregistrations()).isEqualTo(1);
    }

    @Test
    void anApplicationParticipantSlowInThreeOfTenRunsMovesToItsOwnProcess() {
        time.watchdog().markScripted();
        AtomicLong real = new AtomicLong();
        NodeLoop loop = new NodeLoop("node:slow", time, NodeLoop.DEFAULT_BACKSTOP, real::get);
        loops.add(loop);
        AtomicInteger runs = new AtomicInteger();
        List<Boolean> onLoop = new CopyOnWriteArrayList<>();
        loop.register("slow", NodeLoop.Kind.OTHER, "g", now -> {
            runs.incrementAndGet();
            onLoop.add(loop.insideRun());
            real.addAndGet(300 * MS); // 300 ms of scripted work (EL-SP-26)
            return now + 500 * MS;
        });
        Ticker protocol = new Ticker();
        loop.register("protocol", NodeLoop.Kind.PROTOCOL, "g", protocol);
        loop.start();
        time.runDue();
        time.advance(Duration.ofMillis(1_100));
        assertThat(runs.get()).isEqualTo(3);
        assertThat(loop.stats().slowParticipants()).containsEntry("slow", 3L);
        assertThat(loop.stats().offloaded()).as("WD-8: the third slow run in ten moves it").containsExactly("slow");
        assertThat(loop.health().state()).isEqualTo(NodeLoop.HealthState.DEGRADED);
        time.advance(Duration.ofSeconds(5));
        assertThat(runs.get()).as("it keeps running at its own deadline").isGreaterThanOrEqualTo(12);
        assertThat(onLoop.subList(3, onLoop.size())).as("now outside the loop's run, in its own process")
                .containsOnly(false);
        assertThat(onLoop.subList(0, 3)).containsOnly(true);
    }

    /**
     * WD-8 on a hand-driven node (the phase-3 gate's reactor failure, §10.19): a {@code GroupRuntime.onTick} adapter
     * with no tick period, and {@code Content}'s driver, run their work only when {@link NodeLoop#directRun()} says the
     * run is a {@link NodeLoop#tick()}. Once the participant moves to its own process, a tick's request must still read
     * as direct there, or the work never runs again (a content fetch that never left its start).
     */
    @Test
    void aHandDrivenTickStillRunsAnOffloadedParticipantAsATick() {
        time.watchdog().markScripted();
        AtomicLong real = new AtomicLong();
        NodeLoop loop = new NodeLoop("node:handslow", time, NodeLoop.DEFAULT_BACKSTOP, real::get);
        loops.add(loop);
        AtomicInteger worked = new AtomicInteger();
        List<Boolean> onLoop = new CopyOnWriteArrayList<>();
        loop.register("tick:Swarm", NodeLoop.Kind.OTHER, "g", now -> {
            if (loop.directRun()) { // what the onTick adapter without a period and Content's driver ask
                worked.incrementAndGet();
                onLoop.add(loop.insideRun());
                real.addAndGet(300 * MS); // 300 ms of scripted work (EL-SP-26)
            }
            return DueWork.NEVER;
        });
        for (int i = 0; i < 3; i++) {
            loop.tick();
            time.runDue();
        }
        assertThat(loop.stats().offloaded()).as("WD-8: the third slow run in ten moves it").containsExactly("tick:Swarm");
        for (int i = 0; i < 5; i++) {
            loop.tick();
            time.runDue();
        }
        assertThat(worked.get()).as("every tick ran the work, on the loop and then in its own process").isEqualTo(8);
        assertThat(onLoop.subList(3, onLoop.size())).as("the last five outside the loop's run").containsOnly(false);
        assertThat(loop.directRun()).as("the direct mark stays with the request's process").isFalse();
    }

    /**
     * A time source that loses the next drain the loop hands it (a pool that took the task and never ran it), and
     * otherwise passes every call to the virtual time.
     */
    private static final class LosingDrains implements ai.badmonkey.agentspaces.common.time.TimeSource {
        private final VirtualTime base;
        final AtomicBoolean loseNext = new AtomicBoolean();
        final AtomicInteger lost = new AtomicInteger();

        LosingDrains(VirtualTime base) {
            this.base = base;
        }

        @Override
        public java.time.Instant instant() {
            return base.instant();
        }

        @Override
        public long monoNanos() {
            return base.monoNanos();
        }

        @Override
        public Scheduled schedule(Duration delay, Runnable task) {
            if (delay.isZero() && loseNext.getAndSet(false)) {
                lost.incrementAndGet();
                return new Scheduled() { // handed over, never run: the dispatch is stuck
                    @Override
                    public boolean cancel() {
                        return false;
                    }

                    @Override
                    public long dueNanos() {
                        return base.monoNanos();
                    }

                    @Override
                    public boolean dispatched() {
                        return true;
                    }
                };
            }
            return base.schedule(delay, task);
        }

        @Override
        public Scheduled scheduleAt(long monoDeadlineNanos, Runnable task) {
            return base.scheduleAt(monoDeadlineNanos, task);
        }

        @Override
        public void sleep(Duration duration) throws InterruptedException {
            base.sleep(duration);
        }

        @Override
        public boolean waitOn(Object monitor, long monoDeadlineNanos) throws InterruptedException {
            return base.waitOn(monitor, monoDeadlineNanos);
        }

        @Override
        public boolean virtual() {
            return true;
        }

        @Override
        public Watchdog watchdog() {
            return base.watchdog();
        }
    }

    /**
     * Seam wd05 (srad/agentspaces-event-loop.md §10.33, §10.37): the loop's drain is handed to its executor and never
     * runs. The wake is not missed (it was dispatched), no timer is armed, and no run starts, so only the stuck-dispatch
     * judgement can find it: within the wedge bound plus three watchdog periods, after which the loop runs again and the wake that
     * was queued behind the lost drain is not lost. Before §10.37 only {@code WatchdogTest}'s fake {@code Watched}
     * reached this judgement.
     */
    @Test
    void aDrainHandedOverAndNeverRunIsFoundAsAStuckDispatchAndTheLoopRunsAgain() {
        Watchdog wd = time.watchdog().markScripted();
        LosingDrains source = new LosingDrains(time);
        NodeLoop loop = new NodeLoop("node:stuck", source, NodeLoop.DEFAULT_BACKSTOP);
        loops.add(loop);
        Ticker ticker = new Ticker();
        AtomicInteger asked = new AtomicInteger();
        DueWork.Registration reg = loop.register("asked", NodeLoop.Kind.OTHER, "g", now -> {
            asked.incrementAndGet();
            return DueWork.NEVER;
        });
        loop.register("ticker", NodeLoop.Kind.PROTOCOL, "g", ticker);
        loop.start();
        time.runDue();
        int askedBefore = asked.get();
        source.loseNext.set(true);
        long lostAt = time.monoNanos();
        reg.wake();
        assertThat(source.lost).as("the drain was handed over and lost").hasValue(1);
        assertThat(loop.dispatched()).as("a drain is on its way, so the wake is not judged missed").isTrue();
        int tickerBefore = ticker.runs.get();
        time.advance(Duration.ofSeconds(4));
        assertThat(ticker.runs.get()).as("nothing runs while the drain is lost").isEqualTo(tickerBefore);
        assertThat(wd.detections()).as("not yet: the bound is the wedge bound").isEmpty();
        time.advance(Duration.ofSeconds(7));
        assertThat(wd.detections()).singleElement().satisfies(d -> {
            assertThat(d.kind()).isEqualTo(Watched.Reason.STUCK_DISPATCH.id());
            // the first 2 s check still sees the last run's progress; the next starts the count, and the bound is
            // judged at the checks: at most three periods plus the 5 s wedge bound rounded up to a period
            assertThat(d.atNanos() - lostAt).as("found within the wedge bound plus three periods")
                    .isLessThanOrEqualTo(11_000 * MS);
        });
        assertThat(loop.generation()).isEqualTo(1);
        assertThat(asked.get()).as("the queued wake survived the re-initialisation").isEqualTo(askedBefore + 1);
        assertThat(ticker.runs.get()).as("the loop runs again").isGreaterThan(tickerBefore);
    }

    /**
     * Seam wd12 (srad/agentspaces-event-loop.md §10.33, §10.37): a wedged run is re-initialised while a wake waits in
     * the old executor's queue. The re-initialisation moves that queue to the fresh executor ({@code abandon}), so when
     * the wedged participant finally returns, its old drain takes nothing more: no participant ever runs again on the
     * wedged thread. Without the move the old drain ran the queued wake on the old thread, beside the new generation.
     */
    @Test
    void aWedgedDrainThatReturnsLateTakesNothingMoreFromItsOldQueue() {
        Watchdog wd = time.watchdog().markScripted();
        NodeLoop loop = loop();
        AtomicBoolean wedgeNext = new AtomicBoolean();
        AtomicBoolean latch = new AtomicBoolean();
        java.util.concurrent.atomic.AtomicReference<Thread> wedgedThread = new java.util.concurrent.atomic.AtomicReference<>();
        List<Thread> tickerThreads = new CopyOnWriteArrayList<>();
        loop.register("wedger", NodeLoop.Kind.OTHER, "g", now -> {
            if (wedgeNext.getAndSet(false)) {
                wedgedThread.set(Thread.currentThread());
                while (!latch.get()) {
                    try {
                        time.sleep(Duration.ofSeconds(1));
                    } catch (InterruptedException e) {
                        // deaf to the interrupt
                    }
                }
                return time.monoNanos() + 1 * MS;
            }
            return now + 10_000 * MS;
        });
        loop.register("ticker", NodeLoop.Kind.PROTOCOL, "g", now -> {
            tickerThreads.add(Thread.currentThread());
            return now + 1_000 * MS;
        });
        loop.start();
        time.runDue();
        wedgeNext.set(true);
        loop.wakeAll();
        time.runDue();
        assertThat(loop.runningIn()).isEqualTo("wedger");
        time.advance(Duration.ofMillis(1_500)); // the ticker's timer fires: its wake waits in the wedged executor
        assertThat(loop.queued()).as("a wake queued behind the wedged run").isPositive();
        time.advance(Duration.ofSeconds(7));
        assertThat(wd.detections()).extracting(Watchdog.Detection::kind).containsExactly("wedged");
        tickerThreads.clear();
        latch.set(true);
        time.advance(Duration.ofSeconds(5));
        assertThat(tickerThreads).as("the new generation runs").isNotEmpty();
        assertThat(tickerThreads).as("the old drain took nothing more after its late return (WD-2)")
                .doesNotContain(wedgedThread.get());
        assertThat(wd.stats().reinits()).isEqualTo(1);
    }

    @Test
    void aProtocolParticipantIsCountedSlowButNeverMoved() {
        time.watchdog().markScripted();
        AtomicLong real = new AtomicLong();
        NodeLoop loop = new NodeLoop("node:slowproto", time, NodeLoop.DEFAULT_BACKSTOP, real::get);
        loops.add(loop);
        loop.register("membership", NodeLoop.Kind.PROTOCOL, "g", now -> {
            real.addAndGet(300 * MS);
            return now + 500 * MS;
        });
        loop.start();
        time.advance(Duration.ofSeconds(3));
        assertThat(loop.stats().slowParticipants().get("membership")).isGreaterThanOrEqualTo(5L);
        assertThat(loop.stats().offloaded()).isEmpty();
    }
}
